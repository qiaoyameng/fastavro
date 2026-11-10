from io import BytesIO

import pytest

import fastavro
from fastavro.read import SchemaResolutionError
from fastavro.compatibility import (
    COMPATIBLE,
    INCOMPATIBLE,
    UNSUPPORTED,
    check_reader_can_read,
    check_writer_can_write,
    check_schema_pair,
)


def record_schema(fields, name="R"):
    return {"type": "record", "name": name, "fields": fields}


def roundtrip(writer_schema, reader_schema, record):
    bio = BytesIO()
    fastavro.schemaless_writer(bio, writer_schema, record)
    bio.seek(0)
    return fastavro.schemaless_reader(bio, writer_schema, reader_schema)


def test_identical_schema_is_compatible_both_directions():
    schema = record_schema([{"name": "a", "type": "int"}])
    pair = check_schema_pair(schema, schema)
    assert pair.new_reader_reads_old_data.status == COMPATIBLE
    assert pair.old_reader_reads_new_data.status == COMPATIBLE
    assert pair.compatible


def test_field_alias_only_helps_reader_direction():
    writer = record_schema([{"name": "id", "type": "long"}])
    reader = record_schema(
        [{"name": "new_id", "type": "long", "aliases": ["id"]}]
    )
    result = check_reader_can_read(writer, reader)
    assert result.status == COMPATIBLE
    # Reverse direction: the alias lives on the reader side only, so a
    # reader expecting "id" cannot find it in the new writer's data.
    reverse = check_reader_can_read(reader, writer)
    assert reverse.status == INCOMPATIBLE
    assert reverse.paths == ["$.id"]
    # The compatible direction really reads.
    assert roundtrip(writer, reader, {"id": 7}) == {"new_id": 7}


def test_missing_field_needs_default():
    writer = record_schema([{"name": "a", "type": "int"}])
    reader_with_default = record_schema(
        [
            {"name": "a", "type": "int"},
            {"name": "b", "type": "string", "default": "x"},
        ]
    )
    reader_without_default = record_schema(
        [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]
    )
    assert check_reader_can_read(writer, reader_with_default).status == COMPATIBLE
    result = check_reader_can_read(writer, reader_without_default)
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.b"]
    assert "no default" in result.messages[0]
    # Writer fields the reader does not know are simply ignored.
    assert check_reader_can_read(reader_without_default, writer).status == COMPATIBLE
    assert roundtrip(writer, reader_with_default, {"a": 1}) == {"a": 1, "b": "x"}


def test_numeric_promotion_is_one_way():
    promotions = [
        ("int", "long"),
        ("int", "float"),
        ("int", "double"),
        ("long", "float"),
        ("long", "double"),
        ("float", "double"),
    ]
    for writer_type, reader_type in promotions:
        writer = record_schema([{"name": "v", "type": writer_type}])
        reader = record_schema([{"name": "v", "type": reader_type}])
        assert check_reader_can_read(writer, reader).status == COMPATIBLE
        reverse = check_reader_can_read(reader, writer)
        assert reverse.status == INCOMPATIBLE
        assert reverse.paths == ["$.v"]


def test_string_bytes_promotion_both_ways():
    string_schema = record_schema([{"name": "v", "type": "string"}])
    bytes_schema = record_schema([{"name": "v", "type": "bytes"}])
    assert check_reader_can_read(string_schema, bytes_schema).status == COMPATIBLE
    assert check_reader_can_read(bytes_schema, string_schema).status == COMPATIBLE


def test_enum_symbols_and_default():
    small = record_schema(
        [{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "B"]}}]
    )
    big = record_schema(
        [
            {
                "name": "e",
                "type": {"type": "enum", "name": "E", "symbols": ["A", "B", "C"]},
            }
        ]
    )
    big_with_default = record_schema(
        [
            {
                "name": "e",
                "type": {
                    "type": "enum",
                    "name": "E",
                    "symbols": ["A", "B"],
                    "default": "A",
                },
            }
        ]
    )
    # Growing an enum is safe for new readers of old data...
    assert check_reader_can_read(small, big).status == COMPATIBLE
    # ...but old readers may see symbols they do not know.
    result = check_reader_can_read(big, small)
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.e"]
    assert "C" in result.messages[0]
    # An enum default on the reader rescues unknown symbols.
    assert check_reader_can_read(big, big_with_default).status == COMPATIBLE
    # Verify the report with a record that hits the incompatible branch.
    with pytest.raises(SchemaResolutionError):
        roundtrip(big, small, {"e": "C"})
    assert roundtrip(big, big_with_default, {"e": "C"}) == {"e": "A"}


def test_fixed_size_and_name():
    fixed4 = record_schema(
        [{"name": "f", "type": {"type": "fixed", "name": "F", "size": 4}}]
    )
    fixed8 = record_schema(
        [{"name": "f", "type": {"type": "fixed", "name": "F", "size": 8}}]
    )
    renamed = record_schema(
        [{"name": "f", "type": {"type": "fixed", "name": "G", "size": 4}}]
    )
    aliased = record_schema(
        [
            {
                "name": "f",
                "type": {
                    "type": "fixed",
                    "name": "G",
                    "size": 4,
                    "aliases": ["F"],
                },
            }
        ]
    )
    assert check_reader_can_read(fixed4, fixed4).status == COMPATIBLE
    for result in (
        check_reader_can_read(fixed4, fixed8),
        check_reader_can_read(fixed8, fixed4),
        check_reader_can_read(fixed4, renamed),
    ):
        assert result.status == INCOMPATIBLE
        assert result.paths == ["$.f"]
    # A named-type alias on the reader reconciles the rename.
    assert check_reader_can_read(fixed4, aliased).status == COMPATIBLE


def test_array_and_map_recursion():
    writer = record_schema(
        [
            {"name": "items", "type": {"type": "array", "items": "int"}},
            {"name": "props", "type": {"type": "map", "values": "int"}},
        ]
    )
    reader = record_schema(
        [
            {"name": "items", "type": {"type": "array", "items": "long"}},
            {"name": "props", "type": {"type": "map", "values": "double"}},
        ]
    )
    assert check_reader_can_read(writer, reader).status == COMPATIBLE
    reverse = check_reader_can_read(reader, writer)
    assert reverse.status == INCOMPATIBLE
    assert reverse.paths == ["$.items[]", "$.props{}"]
    # Container type changes are incompatible.
    array_vs_map = check_reader_can_read(
        record_schema([{"name": "v", "type": {"type": "array", "items": "int"}}]),
        record_schema([{"name": "v", "type": {"type": "map", "values": "int"}}]),
    )
    assert array_vs_map.status == INCOMPATIBLE


def test_union_every_writer_branch_needs_a_place():
    writer = record_schema([{"name": "u", "type": ["null", "string"]}])
    reader = record_schema([{"name": "u", "type": ["null", "string", "int"]}])
    assert check_reader_can_read(writer, reader).status == COMPATIBLE
    reverse = check_reader_can_read(reader, writer)
    assert reverse.status == INCOMPATIBLE
    assert "$.u[2]" in reverse.paths
    # Verify with a small record that triggers the incompatible branch.
    with pytest.raises(SchemaResolutionError):
        roundtrip(reader, writer, {"u": 3})
    # A branch that exists reads fine.
    assert roundtrip(reader, writer, {"u": "hello"}) == {"u": "hello"}


def test_union_against_non_union():
    # Writer union branch with no reader counterpart is incompatible.
    writer = record_schema([{"name": "u", "type": ["null", "int"]}])
    reader = record_schema([{"name": "u", "type": "long"}])
    result = check_reader_can_read(writer, reader)
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.u[0]"]
    # Non-union writer into a union reader works via promotion.
    assert check_reader_can_read(
        record_schema([{"name": "u", "type": "int"}]),
        record_schema([{"name": "u", "type": ["null", "long"]}]),
    ).status == COMPATIBLE


def test_recursive_named_types_terminate():
    def linked_list(value_type):
        return {
            "type": "record",
            "name": "Node",
            "fields": [
                {"name": "value", "type": value_type},
                {"name": "next", "type": ["null", "Node"]},
            ],
        }

    old = linked_list("int")
    new = linked_list("long")
    # Terminates and reports the promotion as compatible.
    assert check_reader_can_read(old, new).status == COMPATIBLE
    # ...and the reverse as incompatible, also without hanging.
    result = check_reader_can_read(new, old)
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.value"]


def test_recursive_incompatibility_is_found_not_truncated():
    writer = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "value", "type": "int"},
            {"name": "next", "type": ["null", "Node"]},
        ],
    }
    reader = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "value", "type": "int"},
            {"name": "next", "type": ["null", "Node"]},
            {"name": "extra", "type": "string"},
        ],
    }
    result = check_reader_can_read(writer, reader)
    assert result.status == INCOMPATIBLE
    # The reader's extra field is already missing at the root record; the
    # recursion guard keeps the nested occurrence from being re-reported.
    assert result.paths == ["$.extra"]


def test_recursive_incompatibility_inside_nested_named_type():
    def child_schema(extra_field):
        fields = [
            {"name": "v", "type": "int"},
            {"name": "next", "type": ["null", "Child"]},
        ]
        if extra_field:
            fields.append({"name": "extra", "type": "string"})
        return {
            "type": "record",
            "name": "Root",
            "fields": [
                {
                    "name": "child",
                    "type": {
                        "type": "record",
                        "name": "Child",
                        "fields": fields,
                    },
                }
            ],
        }

    result = check_reader_can_read(child_schema(False), child_schema(True))
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.child.extra"]


def test_logical_type_constraints():
    decimal = lambda precision, scale: record_schema(
        [
            {
                "name": "amount",
                "type": {
                    "type": "bytes",
                    "logicalType": "decimal",
                    "precision": precision,
                    "scale": scale,
                },
            }
        ]
    )
    plain_bytes = record_schema([{"name": "amount", "type": "bytes"}])
    # Identical logical types are compatible.
    assert check_reader_can_read(decimal(8, 2), decimal(8, 2)).status == COMPATIBLE
    # Same underlying bytes but different decimal parameters are not.
    result = check_reader_can_read(decimal(8, 2), decimal(8, 4))
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.amount"]
    assert "scale" in result.messages[0]
    # A reader logical type cannot be satisfied by raw writer bytes.
    result = check_reader_can_read(plain_bytes, decimal(8, 2))
    assert result.status == INCOMPATIBLE
    assert "logical type" in result.messages[0]
    # Dropping the logical type on the reader is fine.
    assert check_reader_can_read(decimal(8, 2), plain_bytes).status == COMPATIBLE


def test_unknown_logical_type_is_unsupported():
    writer = record_schema([{"name": "v", "type": "string"}])
    reader = record_schema(
        [{"name": "v", "type": {"type": "string", "logicalType": "custom-thing"}}]
    )
    result = check_reader_can_read(writer, reader)
    assert result.status == UNSUPPORTED
    assert not result.compatible
    assert "custom-thing" in result.messages[0]
    assert result.paths == ["$.v"]


def test_logical_type_on_wrong_base_is_unsupported():
    writer = record_schema([{"name": "v", "type": "string"}])
    reader = record_schema(
        [{"name": "v", "type": {"type": "string", "logicalType": "date"}}]
    )
    result = check_reader_can_read(writer, reader)
    assert result.status == UNSUPPORTED


def test_unparseable_schema_is_unsupported():
    result = check_reader_can_read({"type": "does-not-exist"}, "int")
    assert result.status == UNSUPPORTED
    assert "could not be parsed" in result.messages[0]


def test_writer_can_write_is_the_reverse_direction():
    old = record_schema(
        [{"name": "a", "type": "int"}, {"name": "b", "type": "string"}]
    )
    new = record_schema([{"name": "a", "type": "int"}])
    # New reader reads old writer data (extra writer field is ignored).
    assert check_reader_can_read(old, new).status == COMPATIBLE
    # Reverse direction: the old reader still requires "b", which the new
    # writer no longer produces.
    result = check_writer_can_write(old, new)
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.b"]


def test_check_schema_pair_reports_both_directions():
    old = record_schema([{"name": "v", "type": "int"}])
    new = record_schema([{"name": "v", "type": "double"}])
    pair = check_schema_pair(old, new)
    assert pair.new_reader_reads_old_data.status == COMPATIBLE
    assert pair.old_reader_reads_new_data.status == INCOMPATIBLE
    assert not pair.compatible


def test_nested_record_paths_are_minimal():
    writer = record_schema(
        [
            {
                "name": "user",
                "type": record_schema(
                    [{"name": "name", "type": "string"}], name="User"
                ),
            }
        ]
    )
    reader = record_schema(
        [
            {
                "name": "user",
                "type": record_schema(
                    [{"name": "name", "type": "int"}], name="User"
                ),
            }
        ]
    )
    result = check_reader_can_read(writer, reader)
    assert result.status == INCOMPATIBLE
    assert result.paths == ["$.user.name"]


def test_roundtrip_verifies_compatible_report():
    writer = record_schema(
        [
            {"name": "id", "type": "int"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
        ]
    )
    reader = record_schema(
        [
            {"name": "id", "type": "long"},
            {"name": "tags", "type": {"type": "array", "items": "string"}},
            {"name": "extra", "type": "string", "default": "x"},
        ]
    )
    assert check_reader_can_read(writer, reader).status == COMPATIBLE
    record = {"id": 3, "tags": ["a", "b"]}
    assert roundtrip(writer, reader, record) == {
        "id": 3,
        "tags": ["a", "b"],
        "extra": "x",
    }
