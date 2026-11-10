from copy import deepcopy
from io import BytesIO

import pytest

import fastavro
from fastavro.schema import (
    SchemaConflictError,
    SchemaParseException,
    combine_schemas,
    parse_schema,
)


def _recursive_event_schema(namespace, kind):
    return {
        "type": "record",
        "name": "Event",
        "namespace": namespace,
        "doc": f"recursive event from {kind} source",
        "fields": [
            {"name": "id", "type": "long"},
            {
                # A string default whose text is exactly a type name must not
                # be rewritten like a type reference.
                "name": "marker",
                "type": "string",
                "default": "com.feed.Event",
            },
            {
                "name": "child",
                "type": ["null", "Event"],
                "default": None,
            },
        ],
    }


def test_conflict_is_reported_with_sources_and_not_silently_overwritten():
    schema_a = _recursive_event_schema("com.feed", "a")
    schema_b = _recursive_event_schema("com.feed", "b")

    with pytest.raises(SchemaConflictError) as exc:
        combine_schemas([schema_a, schema_b], sources=["a", "b"])

    error = exc.value
    assert error.conflicts == [
        {
            "fullname": "com.feed.Event",
            "sources": [
                ("com.feed.Event", "a"),
                ("com.feed.Event", "b"),
            ],
        }
    ]
    message = str(error)
    assert "com.feed.Event" in message
    assert "source 'a'" in message and "source 'b'" in message

    # Renaming only one side must still report the unresolved conflict.
    with pytest.raises(SchemaConflictError):
        combine_schemas(
            [schema_a, schema_b],
            renames={("a", "com.feed.Event"): "EventA"},
            sources=["a", "b"],
        )


def test_recursive_records_roundtrip_with_renamed_structures():
    schema_a = _recursive_event_schema("com.feed", "a")
    schema_b = _recursive_event_schema("com.feed", "b")
    snapshot_a = deepcopy(schema_a)
    snapshot_b = deepcopy(schema_b)

    combined_raw = combine_schemas(
        [schema_a, schema_b],
        renames={
            ("a", "com.feed.Event"): "EventA",
            ("b", "com.feed.Event"): "EventB",
        },
        sources=["a", "b"],
    )

    root_a, root_b = combined_raw
    assert root_a["name"] == "com.feed.EventA"
    assert root_b["name"] == "com.feed.EventB"
    # Recursive self references were rewritten, but the string default that
    # merely looks like a type name is preserved verbatim.
    assert root_a["fields"][1]["default"] == "com.feed.Event"
    assert root_a["fields"][2]["type"] == ["null", "com.feed.EventA"]
    assert root_a["doc"] == "recursive event from a source"

    combined = parse_schema(combined_raw)
    records = [
        {"id": 1, "marker": "com.feed.Event", "child": None},
        {
            "id": 2,
            "marker": "com.feed.Event",
            "child": {"id": 3, "marker": "com.feed.Event", "child": None},
        },
    ]

    out = BytesIO()
    fastavro.writer(out, combined, records)
    out.seek(0)
    assert list(fastavro.reader(out)) == records

    # The original input schemas and their strings were not mutated.
    assert schema_a == snapshot_a
    assert schema_b == snapshot_b


def test_same_short_name_in_different_namespaces_keeps_both_structures():
    schema_a = _recursive_event_schema("com.a", "a")
    schema_b = _recursive_event_schema("com.b", "b")

    combined_raw = combine_schemas([schema_a, schema_b], sources=["a", "b"])
    root_a, root_b = combined_raw
    assert root_a["name"] == "com.a.Event"
    assert root_b["name"] == "com.b.Event"
    assert root_a["fields"][2]["type"] == ["null", "com.a.Event"]
    assert root_b["fields"][2]["type"] == ["null", "com.b.Event"]

    records = [
        {"id": 10, "marker": "com.feed.Event", "child": None},
        {"id": 11, "marker": "com.feed.Event", "child": None},
    ]
    out = BytesIO()
    fastavro.writer(out, parse_schema(combined_raw), records)
    out.seek(0)
    assert list(fastavro.reader(out)) == records


def test_relative_alias_is_resolved_in_original_scope_then_rewritten():
    schema = {
        "type": "record",
        "name": "Event",
        "namespace": "com.feed",
        "aliases": ["LegacyEvent"],
        "fields": [
            {
                "name": "kind",
                "type": {
                    "type": "enum",
                    "name": "Kind",
                    "symbols": ["OPEN", "CLOSED"],
                    "default": "OPEN",
                },
            }
        ],
    }

    combined_raw = combine_schemas(
        [schema],
        renames={
            "com.feed.Event": "RenamedEvent",
            "com.feed.Kind": "RenamedKind",
        },
    )
    root = combined_raw[0]
    assert root["name"] == "com.feed.RenamedEvent"
    assert root["aliases"] == ["com.feed.LegacyEvent"]
    assert root["fields"][0]["type"]["name"] == "com.feed.RenamedKind"
    # Enum symbols and defaults must never be text-replaced.
    assert root["fields"][0]["type"]["symbols"] == ["OPEN", "CLOSED"]
    assert root["fields"][0]["type"]["default"] == "OPEN"

    # A reader using the old qualified name as an alias still resolves.
    reader_schema = deepcopy(root)
    del reader_schema["aliases"]
    parsed_writer = parse_schema(deepcopy(combined_raw))
    out = BytesIO()
    fastavro.writer(out, parsed_writer, [{"kind": "CLOSED"}])
    out.seek(0)
    legacy_reader = {
        "type": "record",
        "name": "com.feed.RenamedEvent",
        "aliases": ["com.feed.LegacyEvent"],
        "fields": [
            {
                "name": "kind",
                "type": {
                    "type": "enum",
                    "name": "com.feed.RenamedKind",
                    "symbols": ["OPEN", "CLOSED"],
                },
            }
        ],
    }
    assert list(fastavro.reader(out, legacy_reader)) == [{"kind": "CLOSED"}]


def test_fixed_and_enum_references_and_extension_properties_are_preserved():
    schema = {
        "type": "record",
        "name": "Wrapper",
        "namespace": "com.feed",
        "fields": [
            {
                "name": "code",
                "type": {
                    "type": "fixed",
                    "name": "Code",
                    "size": 4,
                    "logicalType": "some-unknown-logical-type",
                    "x-custom": "keep-me",
                },
            },
            {
                "name": "codes",
                "type": {"type": "array", "items": "Code"},
            },
            {
                "name": "state",
                "type": {
                    "type": "enum",
                    "name": "State",
                    "symbols": ["ON", "OFF"],
                },
            },
        ],
    }

    combined_raw = combine_schemas(
        [schema],
        renames={
            "com.feed.Wrapper": "RenamedWrapper",
            "com.feed.Code": "RenamedCode",
            "com.feed.State": "RenamedState",
        },
    )
    root = combined_raw[0]
    fixed = root["fields"][0]["type"]
    assert fixed["name"] == "com.feed.RenamedCode"
    assert fixed["logicalType"] == "some-unknown-logical-type"
    assert fixed["x-custom"] == "keep-me"
    assert root["fields"][1]["type"]["items"] == "com.feed.RenamedCode"
    assert root["fields"][2]["type"]["name"] == "com.feed.RenamedState"
    parse_schema(deepcopy(combined_raw))


def test_renaming_into_duplicate_names_is_rejected():
    schema = {
        "type": "record",
        "name": "Root",
        "namespace": "com.feed",
        "fields": [
            {
                "name": "a",
                "type": {"type": "record", "name": "Alpha", "fields": []},
            },
            {
                "name": "b",
                "type": {"type": "record", "name": "Beta", "fields": []},
            },
        ],
    }

    with pytest.raises(SchemaConflictError, match="both become"):
        combine_schemas(
            [schema],
            renames={
                "com.feed.Alpha": "Same",
                "com.feed.Beta": "Same",
            },
        )


def test_ambiguous_alias_after_rename_is_rejected():
    schema = {
        "type": "record",
        "name": "Root",
        "namespace": "com.feed",
        "fields": [
            {
                "name": "a",
                "type": {
                    "type": "record",
                    "name": "Alpha",
                    "aliases": ["SharedAlias"],
                    "fields": [],
                },
            },
            {
                "name": "b",
                "type": {
                    "type": "record",
                    "name": "Beta",
                    "aliases": ["SharedAlias"],
                    "fields": [],
                },
            },
        ],
    }

    with pytest.raises(SchemaConflictError, match="ambiguous alias"):
        combine_schemas([schema])


def test_unknown_rename_key_and_caller_cache_are_respected():
    schema = {
        "type": "record",
        "name": "Only",
        "namespace": "com.feed",
        "fields": [],
    }

    with pytest.raises(SchemaParseException, match="unknown type"):
        combine_schemas([schema], renames={"com.feed.Missing": "Whatever"})

    caller_cache = {}
    parse_schema(deepcopy(schema), caller_cache)
    cached_before = deepcopy(caller_cache)
    combine_schemas([schema])
    assert caller_cache == cached_before


def test_bare_fullname_rename_applies_when_there_is_no_conflict():
    schema = {
        "type": "record",
        "name": "Event",
        "namespace": "com.feed",
        "fields": [
            {
                "name": "nested",
                "type": {"type": "fixed", "name": "Tag", "size": 2},
            }
        ],
    }

    combined_raw = combine_schemas(
        [schema], renames={"com.feed.Event": "RenamedEvent"}
    )
    root = combined_raw[0]
    assert root["name"] == "com.feed.RenamedEvent"
    assert root["fields"][0]["type"]["name"] == "com.feed.Tag"


def test_bare_rename_is_rejected_for_ambiguous_fullname():
    schema_a = _recursive_event_schema("com.feed", "a")
    schema_b = _recursive_event_schema("com.feed", "b")

    with pytest.raises(SchemaConflictError):
        combine_schemas([schema_a, schema_b], renames={"com.feed.Event": "X"})


def test_rename_across_sources_into_one_name_is_rejected():
    schema_a = {
        "type": "record",
        "name": "Alpha",
        "namespace": "com.feed",
        "fields": [],
    }
    schema_b = {
        "type": "record",
        "name": "Beta",
        "namespace": "com.feed",
        "fields": [],
    }

    with pytest.raises(SchemaConflictError) as exc:
        combine_schemas(
            [schema_a, schema_b],
            renames={"com.feed.Alpha": "Same", "com.feed.Beta": "Same"},
            sources=["a", "b"],
        )

    conflict = exc.value.conflicts[0]
    assert conflict["fullname"] == "com.feed.Same"
    assert ("com.feed.Alpha", "a") in conflict["sources"]
    assert ("com.feed.Beta", "b") in conflict["sources"]
