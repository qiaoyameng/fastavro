"""Static Avro schema compatibility checks (pure Python).

This module decides, without any sample data files, whether data written
with a writer schema can be read back with a reader schema according to
the Avro schema resolution rules. The check is purely structural: it never
invents concrete data values as a proof of (in)compatibility.

The direction of the check matters. ``writer -> reader`` (a reader using
``reader_schema`` consumes data produced with ``writer_schema``) is a
different question than the reverse, and the two directions can disagree.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from ._schema_py import parse_schema
from ._schema_common import PRIMITIVES, SchemaParseException, UnknownType
from .const import NAMED_TYPES
from .types import Schema

COMPATIBLE = "compatible"
INCOMPATIBLE = "incompatible"
UNSUPPORTED = "unsupported"

# Logical types that fastavro itself knows how to read and write. Anything
# outside this set is an unknown extension and yields an "unsupported"
# verdict rather than a guess based on the underlying type.
KNOWN_LOGICAL_TYPES = {
    "decimal",
    "date",
    "time-millis",
    "time-micros",
    "timestamp-millis",
    "timestamp-micros",
    "local-timestamp-millis",
    "local-timestamp-micros",
    "uuid",
}

# Underlying Avro types each known logical type is specified for.
_LOGICAL_TYPE_BASE = {
    "decimal": {"bytes", "fixed"},
    "date": {"int"},
    "time-millis": {"int"},
    "time-micros": {"long"},
    "timestamp-millis": {"long"},
    "timestamp-micros": {"long"},
    "local-timestamp-millis": {"long"},
    "local-timestamp-micros": {"long"},
    "uuid": {"string"},
}

# Primitive promotions allowed by the Avro resolution rules.
_PROMOTIONS = {
    "int": {"long", "float", "double"},
    "long": {"float", "double"},
    "float": {"double"},
    "string": {"bytes"},
    "bytes": {"string"},
}


@dataclass
class CompatibilityIssue:
    """A single finding from a compatibility check.

    Attributes
    ----------
    kind
        One of ``"incompatible"`` or ``"unsupported"``.
    path
        Minimal path to the relevant schema node, e.g.
        ``$.user.tags[]``. ``$`` is the schema root, ``.name`` a record
        field, ``[]`` array items, ``{}`` map values and ``[i]`` the i-th
        union branch.
    message
        Human readable reason.
    """

    kind: str
    path: str
    message: str


@dataclass
class SchemaCompatibilityResult:
    """Outcome of a single-direction compatibility check."""

    issues: List[CompatibilityIssue] = field(default_factory=list)

    @property
    def status(self) -> str:
        """``compatible``, ``incompatible`` or ``unsupported``.

        A definite incompatibility dominates; otherwise any unsupported
        construct means no verdict could be reached.
        """
        if any(issue.kind == INCOMPATIBLE for issue in self.issues):
            return INCOMPATIBLE
        if any(issue.kind == UNSUPPORTED for issue in self.issues):
            return UNSUPPORTED
        return COMPATIBLE

    @property
    def compatible(self) -> bool:
        return self.status == COMPATIBLE

    def __bool__(self) -> bool:
        return self.compatible

    @property
    def paths(self) -> List[str]:
        """Minimal relevant field paths, in order of first appearance."""
        seen: Set[str] = set()
        paths = []
        for issue in self.issues:
            if issue.path not in seen:
                seen.add(issue.path)
                paths.append(issue.path)
        return paths

    @property
    def messages(self) -> List[str]:
        return [
            f"{issue.kind} at {issue.path}: {issue.message}"
            for issue in self.issues
        ]

    def __str__(self) -> str:
        if not self.issues:
            return COMPATIBLE
        return "\n".join(self.messages)


@dataclass
class SchemaPairCompatibility:
    """Both directions of an old -> new schema evolution."""

    new_reader_reads_old_data: SchemaCompatibilityResult
    old_reader_reads_new_data: SchemaCompatibilityResult

    @property
    def compatible(self) -> bool:
        return (
            self.new_reader_reads_old_data.compatible
            and self.old_reader_reads_new_data.compatible
        )

    def __bool__(self) -> bool:
        return self.compatible


def _type_of(schema: Any) -> str:
    if isinstance(schema, dict):
        return schema["type"]
    if isinstance(schema, list):
        return "union"
    return schema


def _describe(schema: Any) -> str:
    if isinstance(schema, dict):
        if schema["type"] in NAMED_TYPES:
            return f"{schema['type']} '{schema.get('name')}'"
        return schema["type"]
    if isinstance(schema, list):
        return "union"
    return repr(schema)


class _Checker:
    def __init__(self, writer_named: Dict[str, Any], reader_named: Dict[str, Any]):
        self.writer_named = writer_named
        self.reader_named = reader_named
        self.issues: List[CompatibilityIssue] = []
        # (writer fullname, reader fullname) pairs already being matched.
        # Named types are global and the check is context free, so a pair
        # seen before is assumed compatible; this also guarantees that
        # recursive schemas terminate.
        self.seen: Set[Tuple[str, str]] = set()

    def _add(self, kind: str, path: str, message: str) -> None:
        self.issues.append(
            CompatibilityIssue(kind=kind, path=path or "$", message=message)
        )

    def _resolve(self, schema: Any, named: Dict[str, Any], path: str, side: str):
        """Resolve a named-type reference string to its definition."""
        if isinstance(schema, str) and schema not in PRIMITIVES:
            definition = named.get(schema)
            if definition is None:
                self._add(
                    UNSUPPORTED,
                    path,
                    f"{side} schema references unknown named type "
                    f"'{schema}'; cannot determine compatibility",
                )
                return None
            return definition
        return schema

    def match(self, writer: Any, reader: Any, path: str = "$") -> None:
        writer = self._resolve(writer, self.writer_named, path, "writer")
        reader = self._resolve(reader, self.reader_named, path, "reader")
        if writer is None or reader is None:
            return

        if isinstance(writer, list):
            # Every possible writer branch must have a place to go.
            for index, branch in enumerate(writer):
                self.match(branch, reader, f"{path}[{index}]")
            return

        if isinstance(reader, list):
            self._match_reader_union(writer, reader, path)
            return

        if not self._match_logical_types(writer, reader, path):
            # An unsupported logical type at this node makes any further
            # verdict about it meaningless.
            return

        writer_type = _type_of(writer)
        reader_type = _type_of(reader)

        if writer_type in NAMED_TYPES and reader_type in NAMED_TYPES:
            self._match_named(writer, reader, path)
        elif writer_type == "array" and reader_type == "array":
            self.match(writer["items"], reader["items"], f"{path}[]")
        elif writer_type == "map" and reader_type == "map":
            self.match(writer["values"], reader["values"], f"{path}{{}}")
        elif writer_type in PRIMITIVES and reader_type in PRIMITIVES:
            if writer_type != reader_type and reader_type not in _PROMOTIONS.get(
                writer_type, ()
            ):
                self._add(
                    INCOMPATIBLE,
                    path,
                    f"writer type '{writer_type}' cannot be read as "
                    f"reader type '{reader_type}'",
                )
        else:
            self._add(
                INCOMPATIBLE,
                path,
                f"writer type '{writer_type}' cannot be read as "
                f"reader type '{reader_type}'",
            )

    def _match_reader_union(self, writer: Any, reader: List[Any], path: str) -> None:
        best: Optional[List[CompatibilityIssue]] = None
        saw_unsupported = False
        for branch in reader:
            trial = _Checker(self.writer_named, self.reader_named)
            trial.seen = set(self.seen)
            trial.match(writer, branch, path)
            if not trial.issues:
                self.seen |= trial.seen
                return
            if any(issue.kind == UNSUPPORTED for issue in trial.issues):
                saw_unsupported = True
            if best is None or len(trial.issues) < len(best):
                best = trial.issues
        if saw_unsupported:
            self._add(
                UNSUPPORTED,
                path,
                f"cannot confirm that writer {_describe(writer)} matches any "
                f"branch of the reader union: {best[0].message}",
            )
        else:
            self._add(
                INCOMPATIBLE,
                path,
                f"writer {_describe(writer)} does not match any branch of "
                f"the reader union",
            )
            # Attach the closest branch's findings for context.
            self.issues.extend(best or [])

    def _match_logical_types(self, writer: Any, reader: Any, path: str) -> bool:
        """Returns False when an unsupported logical type blocks a verdict."""
        writer_lt = writer.get("logicalType") if isinstance(writer, dict) else None
        reader_lt = reader.get("logicalType") if isinstance(reader, dict) else None
        if not writer_lt and not reader_lt:
            return True

        for side, logical_type, node in (
            ("writer", writer_lt, writer),
            ("reader", reader_lt, reader),
        ):
            if not logical_type:
                continue
            if logical_type not in KNOWN_LOGICAL_TYPES:
                self._add(
                    UNSUPPORTED,
                    path,
                    f"{side} uses unknown logical type '{logical_type}'; "
                    f"cannot determine compatibility",
                )
                return False
            base = _type_of(node)
            if base not in _LOGICAL_TYPE_BASE[logical_type]:
                self._add(
                    UNSUPPORTED,
                    path,
                    f"{side} logical type '{logical_type}' is not specified "
                    f"for underlying type '{base}'; cannot determine "
                    f"compatibility",
                )
                return False

        if reader_lt and writer_lt != reader_lt:
            self._add(
                INCOMPATIBLE,
                path,
                f"reader expects logical type '{reader_lt}' but writer "
                f"provides {writer_lt or 'no logical type'}; a matching "
                f"underlying type is not sufficient",
            )
        elif reader_lt == writer_lt == "decimal":
            for attribute, default in (("precision", None), ("scale", 0)):
                writer_value = writer.get(attribute, default)
                reader_value = reader.get(attribute, default)
                if writer_value != reader_value:
                    self._add(
                        INCOMPATIBLE,
                        path,
                        f"decimal {attribute} differs: writer has "
                        f"{writer_value}, reader expects {reader_value}",
                    )
        # writer logical type with no reader logical type is fine: the
        # reader simply sees the raw underlying values.
        return True

    def _match_named(self, writer: dict, reader: dict, path: str) -> None:
        writer_type = writer["type"]
        reader_type = reader["type"]
        if writer_type != reader_type:
            self._add(
                INCOMPATIBLE,
                path,
                f"writer type '{writer_type}' cannot be read as "
                f"reader type '{reader_type}'",
            )
            return

        writer_name = writer["name"]
        reader_name = reader["name"]
        reader_aliases = reader.get("aliases", [])
        if writer_name != reader_name and writer_name not in reader_aliases:
            self._add(
                INCOMPATIBLE,
                path,
                f"writer {writer_type} '{writer_name}' does not match "
                f"reader {reader_type} '{reader_name}' or its aliases",
            )
            return

        pair = (writer_name, reader_name)
        if pair in self.seen:
            # Recursive reference to a pair already being checked.
            return
        self.seen.add(pair)

        if writer_type in ("record", "error"):
            self._match_record(writer, reader, path)
        elif writer_type == "enum":
            missing = [s for s in writer["symbols"] if s not in reader["symbols"]]
            if missing and "default" not in reader:
                self._add(
                    INCOMPATIBLE,
                    path,
                    f"writer enum symbols {missing} are not present in "
                    f"reader enum '{reader_name}' and the reader enum has "
                    f"no default",
                )
        elif writer_type == "fixed":
            if writer["size"] != reader["size"]:
                self._add(
                    INCOMPATIBLE,
                    path,
                    f"fixed size differs: writer has {writer['size']}, "
                    f"reader expects {reader['size']}",
                )

    def _match_record(self, writer: dict, reader: dict, path: str) -> None:
        writer_fields = {f["name"]: f for f in writer["fields"]}
        for reader_field in reader["fields"]:
            field_path = f"{path}.{reader_field['name']}"
            writer_field = writer_fields.get(reader_field["name"])
            if writer_field is None:
                for alias in reader_field.get("aliases", []):
                    if alias in writer_fields:
                        writer_field = writer_fields[alias]
                        break
            if writer_field is None:
                if "default" not in reader_field:
                    self._add(
                        INCOMPATIBLE,
                        field_path,
                        f"reader field '{reader_field['name']}' has no "
                        f"matching writer field and no default",
                    )
                continue
            self.match(writer_field["type"], reader_field["type"], field_path)


def check_reader_can_read(
    writer_schema: Schema, reader_schema: Schema
) -> SchemaCompatibilityResult:
    """Check the writer -> reader direction.

    Returns whether a reader using ``reader_schema`` can read every piece
    of data that conforms to ``writer_schema``, per the Avro schema
    resolution rules. Every possible writer union branch must have a
    matching reader branch.
    """
    writer_named: Dict[str, Any] = {}
    reader_named: Dict[str, Any] = {}
    try:
        parsed_writer = parse_schema(writer_schema, writer_named)
        parsed_reader = parse_schema(reader_schema, reader_named)
    except (SchemaParseException, UnknownType) as error:
        return SchemaCompatibilityResult(
            issues=[
                CompatibilityIssue(
                    kind=UNSUPPORTED,
                    path="$",
                    message=f"schema could not be parsed: {error}",
                )
            ]
        )
    checker = _Checker(writer_named, reader_named)
    checker.match(parsed_writer, parsed_reader)
    return SchemaCompatibilityResult(issues=checker.issues)


def check_writer_can_write(
    reader_schema: Schema, writer_schema: Schema
) -> SchemaCompatibilityResult:
    """Check the reverse (reader -> writer) direction.

    Returns whether data written with ``writer_schema`` can be read by a
    reader still using ``reader_schema``; i.e. the same predicate as
    :func:`check_reader_can_read` with the roles spelled out for the
    opposite direction of an evolution.
    """
    return check_reader_can_read(writer_schema, reader_schema)


def check_schema_pair(
    old_schema: Schema, new_schema: Schema
) -> SchemaPairCompatibility:
    """Check both directions of an old -> new schema evolution."""
    return SchemaPairCompatibility(
        new_reader_reads_old_data=check_reader_can_read(old_schema, new_schema),
        old_reader_reads_new_data=check_reader_can_read(new_schema, old_schema),
    )
