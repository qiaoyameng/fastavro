"""Combine independently authored Avro schemas into one message schema.

The combination never does textual substitutions. Each input schema is first
parsed in isolation so that record, enum and fixed declarations (including
recursive references and aliases) are resolved against their original
namespace scope. Only the resulting declarations and genuine type references
are rewritten, so field names, string defaults, symbols, docs and any other
custom properties are left untouched.
"""

from copy import deepcopy
import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ._schema_common import (
    PRIMITIVES,
    SchemaConflictError,
    SchemaParseException,
)
from .types import Schema

_NAME_PART = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _qualify(name: str, namespace: str) -> str:
    """Resolve a possibly relative name against a namespace scope."""
    if "." in name:
        return name
    if namespace:
        return f"{namespace}.{name}"
    return name


def _validate_dotted_name(name: str):
    parts = name.split(".")
    if not parts or any(not _NAME_PART.fullmatch(part) for part in parts):
        raise SchemaParseException(
            f"renamed type must be a valid Avro name, got {name!r}"
        )


def _collect_declarations(schema: Schema, collected: Dict[str, Dict[str, Any]]):
    """Index every named type declaration of one parsed schema by full name."""
    if isinstance(schema, list):
        for sub_schema in schema:
            _collect_declarations(sub_schema, collected)
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type in ("record", "error", "enum", "fixed"):
            collected[schema["name"]] = schema
        if schema_type in ("record", "error"):
            for field in schema["fields"]:
                _collect_declarations(field["type"], collected)
        elif schema_type == "array":
            _collect_declarations(schema["items"], collected)
        elif schema_type == "map":
            _collect_declarations(schema["values"], collected)


def _format_conflicts(conflicts: List[Dict[str, Any]]) -> str:
    lines = []
    for conflict in conflicts:
        owners = ", ".join(
            f"{fullname!r} from source {source!r}"
            for fullname, source in conflict["sources"]
        )
        lines.append(
            f"{conflict['fullname']!r} is declared by {owners}; provide a "
            "rename mapping for every conflicting declaration"
        )
    return "; ".join(lines)


def _iter_named(schema: Schema):
    if isinstance(schema, list):
        for sub_schema in schema:
            yield from _iter_named(sub_schema)
    elif isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type in ("record", "error", "enum", "fixed"):
            yield schema
        if schema_type in ("record", "error"):
            for field in schema["fields"]:
                yield from _iter_named(field["type"])
        elif schema_type == "array":
            yield from _iter_named(schema["items"])
        elif schema_type == "map":
            yield from _iter_named(schema["values"])


def combine_schemas(
    schemas: List[Schema],
    renames: Optional[Mapping[Any, str]] = None,
    *,
    sources: Optional[List[Any]] = None,
) -> List[Schema]:
    """Combine independent Avro schemas into one union message schema.

    Every input schema is parsed in its own isolated cache so that record,
    enum and fixed declarations, recursive references and aliases are resolved
    against their original namespace scope. Named type declarations and the
    genuine type references that point at them are then rewritten with the
    caller supplied mapping. Field names, string default values, enum symbols,
    docs and unknown extension properties such as custom ``logicalType`` values
    are preserved because no textual substitution is performed.

    Parameters
    ----------
    schemas
        Input schemas. Each one is parsed independently, as if it had been
        authored on its own.
    renames
        Mapping of original full names to new full names. The keys must be the
        fully qualified names as resolved in the source namespace scope. When
        two sources declare the same full name, a ``(source, fullname)`` key
        can be used to disambiguate the declarations. New names may be simple
        names, in which case the original namespace is kept.
    sources
        Optional labels identifying each input schema, used both for
        ``(source, fullname)`` rename keys and in conflict reports. Defaults
        to the index of each schema in ``schemas``.


    Example::

        from fastavro.schema import combine_schemas, parse_schema

        message_schema = parse_schema(
            combine_schemas(
                [event_schema_a, event_schema_b],
                renames={
                    "com.a.Event": "com.a.RenamedEvent",
                    "com.b.Event": "com.b.RenamedEvent",
                },
            )
        )
    """
    # Imported lazily to avoid an import cycle between the schema and combine
    # modules.
    from .schema import parse_schema

    if not isinstance(schemas, (list, tuple)) or not schemas:
        raise ValueError("schemas must be a non-empty list of Avro schemas")

    if sources is not None and len(sources) != len(schemas):
        raise ValueError("sources must have the same length as schemas")

    source_labels = list(sources) if sources is not None else list(
        range(len(schemas))
    )

    # Deep copy so that the input schemas and any caches the caller still
    # holds references to can never be mutated by the combination.
    parsed_sources = []
    declarations_by_source: List[Dict[str, Dict[str, Any]]] = []
    for source_schema in schemas:
        local_named_schemas: Dict[str, Dict[str, Any]] = {}
        copied = deepcopy(source_schema)
        parsed = parse_schema(
            copied,
            local_named_schemas,
            _write_hint=False,
        )
        declarations: Dict[str, Dict[str, Any]] = {}
        _collect_declarations(parsed, declarations)
        parsed_sources.append(parsed)
        declarations_by_source.append(declarations)

    # Which sources declare each original full name.
    owners: Dict[str, List[Tuple[Any, int]]] = {}
    for index, declarations in enumerate(declarations_by_source):
        for fullname in declarations:
            owners.setdefault(fullname, []).append((source_labels[index], index))

    tuple_renames: Dict[Tuple[Any, str], str] = {}
    bare_renames: Dict[str, str] = {}
    for key, new_name in (renames or {}).items():
        if isinstance(key, tuple):
            if len(key) != 2:
                raise ValueError(
                    "rename tuple keys must have the form (source, fullname)"
                )
            tuple_renames[(key[0], key[1])] = new_name
        else:
            bare_renames[key] = new_name

    for fullname in bare_renames:
        if fullname not in owners:
            raise SchemaParseException(
                f"rename mapping refers to an unknown type {fullname!r}"
            )

    for source, fullname in tuple_renames:
        if source not in source_labels:
            raise SchemaParseException(
                f"rename mapping refers to an unknown source {source!r}"
            )
        index = source_labels.index(source)
        if fullname not in declarations_by_source[index]:
            raise SchemaParseException(
                f"source {source!r} does not declare type {fullname!r}"
            )

    def _new_name_for(index: int, fullname: str) -> str:
        source = source_labels[index]
        if (source, fullname) in tuple_renames:
            new_name = tuple_renames[(source, fullname)]
        elif fullname in bare_renames:
            if len(owners[fullname]) > 1:
                raise SchemaConflictError(
                    _format_conflicts(
                        [
                            {
                                "fullname": fullname,
                                "sources": [
                                    (label, label)
                                    for label, _ in owners[fullname]
                                ],
                            }
                        ]
                    )
                )
            new_name = bare_renames[fullname]
        else:
            return fullname

        if "." not in new_name:
            namespace = fullname.rsplit(".", 1)[0] if "." in fullname else ""
            new_name = f"{namespace}.{new_name}" if namespace else new_name
        _validate_dotted_name(new_name)
        return new_name

    # Every same-named declaration across different sources is a conflict and
    # needs an explicit rename for each owning source.
    conflicts = []
    for fullname, owner_list in owners.items():
        if len(owner_list) <= 1:
            continue
        if any(
            (source, fullname) not in tuple_renames
            and fullname not in bare_renames
            for source, _ in owner_list
        ):
            conflicts.append(
                {
                    "fullname": fullname,
                    "sources": [(fullname, source) for source, _ in owner_list],
                }
            )
    if conflicts:
        raise SchemaConflictError(_format_conflicts(conflicts), conflicts)

    # Build the per-source old full name -> new full name mapping.
    renames_by_source: List[Dict[str, str]] = []
    for index, declarations in enumerate(declarations_by_source):
        mapping = {
            fullname: _new_name_for(index, fullname)
            for fullname in declarations
        }
        renames_by_source.append(mapping)

    # Renaming must not create duplicate names inside one source.
    for index, mapping in enumerate(renames_by_source):
        seen: Dict[str, str] = {}
        for old_name, new_name in mapping.items():
            if new_name in seen:
                raise SchemaConflictError(
                    f"renamed types {seen[new_name]!r} and {old_name!r} from "
                    f"source {source_labels[index]!r} both become {new_name!r}",
                    [
                        {
                            "fullname": new_name,
                            "sources": [
                                (old_name, source_labels[index]),
                                (seen[new_name], source_labels[index]),
                            ],
                        }
                    ],
                )
            seen[new_name] = old_name

    rewritten_sources = []
    for index, parsed in enumerate(parsed_sources):
        mapping = renames_by_source[index]
        namespace_by_name = {
            fullname: (fullname.rsplit(".", 1)[0] if "." in fullname else "")
            for fullname in mapping
        }
        rewritten = _rewrite(parsed, mapping, namespace_by_name)
        _check_aliases(rewritten, mapping, source_labels[index])
        rewritten_sources.append(rewritten)

    # The sources become roots of one union, and each root is parsed with its
    # own name scope, so a duplicate name introduced across two different
    # sources would not be rejected by parse_schema. Detect it explicitly.
    new_name_owners: Dict[str, List[Tuple[str, Any]]] = {}
    for index, mapping in enumerate(renames_by_source):
        for old_name, new_name in mapping.items():
            new_name_owners.setdefault(new_name, []).append(
                (old_name, source_labels[index])
            )
    duplicate_conflicts = [
        {
            "fullname": new_name,
            "sources": owners,
        }
        for new_name, owners in new_name_owners.items()
        if len(owners) > 1
    ]
    if duplicate_conflicts:
        raise SchemaConflictError(
            _format_conflicts(duplicate_conflicts), duplicate_conflicts
        )

    # Verify the combined result is a real, parseable Avro schema. References
    # that do not resolve to a declaration surface here as UnknownType.
    parse_schema(deepcopy(rewritten_sources), _write_hint=False)

    return rewritten_sources


def _rewrite(
    schema: Schema,
    mapping: Dict[str, str],
    namespace_by_name: Dict[str, str],
) -> Schema:
    if isinstance(schema, list):
        return [
            _rewrite(sub_schema, mapping, namespace_by_name)
            for sub_schema in schema
        ]

    if not isinstance(schema, dict):
        # A genuine type reference. Parsing has already expanded every
        # reference to its fully qualified name.
        if schema in PRIMITIVES:
            return schema
        if schema not in mapping:
            raise SchemaParseException(
                f"unresolved type reference {schema!r} after renaming"
            )
        return mapping[schema]

    schema_type = schema["type"]
    if schema_type in ("record", "error", "enum", "fixed"):
        rewritten = {
            key: value
            for key, value in schema.items()
            if key not in ("name", "aliases", "fields", "items", "values")
        }
        fullname = schema["name"]
        rewritten["name"] = mapping[fullname]
        if "aliases" in schema:
            namespace = namespace_by_name[fullname]
            # Aliases may point at names that were never declared in this
            # schema (for example a previous version of a type), so only an
            # alias that resolves to a renamed declaration is rewritten.
            rewritten["aliases"] = [
                mapping.get(qualified_alias, qualified_alias)
                for alias in schema["aliases"]
                for qualified_alias in (_qualify(alias, namespace),)
            ]
        if schema_type in ("record", "error"):
            rewritten["fields"] = [
                _rewrite_field(field, mapping, namespace_by_name)
                for field in schema["fields"]
            ]
        return rewritten

    rewritten = dict(schema)
    if schema_type == "array":
        rewritten["items"] = _rewrite(
            schema["items"], mapping, namespace_by_name
        )
    elif schema_type == "map":
        rewritten["values"] = _rewrite(
            schema["values"], mapping, namespace_by_name
        )
    return rewritten


def _rewrite_field(
    field: Dict[str, Any],
    mapping: Dict[str, str],
    namespace_by_name: Dict[str, str],
) -> Dict[str, Any]:
    # Everything except the type is copied verbatim, including field names,
    # string defaults (even when a default happens to equal a type name),
    # field aliases, docs and custom properties.
    rewritten = dict(field)
    rewritten["type"] = _rewrite(field["type"], mapping, namespace_by_name)
    return rewritten


def _check_aliases(
    schema: Schema,
    mapping: Dict[str, str],
    source: Any,
):
    """Re-check alias and declaration name uniqueness after renaming."""
    alias_owners: Dict[str, str] = {}
    declaration_names = set(mapping.values())
    for named in _iter_named(schema):
        fullname = named["name"]
        for alias in named.get("aliases", []):
            if alias in declaration_names and alias != fullname:
                raise SchemaConflictError(
                    f"alias {alias!r} of {fullname!r} in source {source!r} "
                    "collides with another declared type name"
                )
            if alias in alias_owners and alias_owners[alias] != fullname:
                raise SchemaConflictError(
                    f"ambiguous alias {alias!r} in source {source!r}; it is "
                    f"declared by both {alias_owners[alias]!r} and "
                    f"{fullname!r}"
                )
            alias_owners[alias] = fullname
