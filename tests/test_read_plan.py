"""Tests for the compiled read plans in the pure Python backend.

The pure Python reader compiles the writer/reader schema matching into a
one-time read plan so that per-record reads do not rebuild field and alias
mappings. These tests check that the planned path stays result-compatible
with the generic ``read_data`` path and that schema decisions no longer
scale with the number of records.
"""

from io import BytesIO

import pytest

from fastavro import _read_py, _write_py
from fastavro._read_common import SchemaResolutionError


WRITER_SCHEMA = {
    "type": "record",
    "name": "Node",
    "namespace": "plan",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "label", "type": ["null", "string"]},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attrs", "type": {"type": "map", "values": "long"}},
        {"name": "next", "type": ["null", "plan.Node"]},
        {"name": "extra", "type": "string"},
    ],
}

READER_SCHEMA = {
    "type": "record",
    "name": "Node",
    "namespace": "plan",
    "fields": [
        # alias for the writer's "id" field
        {"name": "identifier", "aliases": ["id"], "type": "long"},
        {"name": "label", "type": ["null", "string"]},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attrs", "type": {"type": "map", "values": "long"}},
        # recursive reference to the reader's own named type
        {"name": "next", "type": ["null", "plan.Node"]},
        # mutable defaults that must not be shared between records
        {
            "name": "extra2",
            "type": {"type": "array", "items": "int"},
            "default": [1, 2],
        },
        {
            "name": "meta",
            "type": {"type": "map", "values": "string"},
            "default": {"a": "b"},
        },
    ],
}


def make_records(count):
    records = []
    for i in range(count):
        record = {
            "id": i,
            "label": None if i % 2 else f"label-{i}",
            "tags": [f"t{i}", "中文"],
            "attrs": {"k": i},
            "next": None,
            "extra": f"e{i}",
        }
        if i % 3 == 0:
            # give some records a recursive child
            record["next"] = {
                "id": -i,
                "label": "child",
                "tags": [],
                "attrs": {},
                "next": None,
                "extra": "child-extra",
            }
        records.append(record)
    return records


def write_file(records, schema=WRITER_SCHEMA):
    bio = BytesIO()
    _write_py.writer(bio, schema, records)
    bio.seek(0)
    return bio


def uncompiled_plan(w_schema, r_schema, named_schemas, options, cache):
    """Replacement for _compile_read_plan that falls back to the generic
    per-record read_data path, for differential testing."""

    def plan(decoder):
        return _read_py.read_data(
            decoder, w_schema, named_schemas, r_schema, options
        )

    return plan


def read_with_reader_schema(raw):
    return list(_read_py.reader(BytesIO(raw), READER_SCHEMA))


def test_plan_differential(monkeypatch):
    """Aliases, recursion, mutable defaults, field reordering and skipped
    writer fields produce identical results on the planned and the generic
    read paths."""
    records = make_records(30)
    raw = write_file(records).getvalue()

    planned_results = read_with_reader_schema(raw)

    monkeypatch.setattr(_read_py, "_compile_read_plan", uncompiled_plan)
    generic_results = read_with_reader_schema(raw)

    assert planned_results == generic_results

    # spot check the actual content
    first = planned_results[0]
    assert first["identifier"] == 0
    assert first["extra2"] == [1, 2]
    assert first["meta"] == {"a": "b"}
    assert "extra" not in first  # writer-only field was skipped
    assert "id" not in first  # renamed via alias
    recursive = planned_results[0]["next"]
    assert recursive["identifier"] == 0
    assert recursive["extra2"] == [1, 2]


def test_mutable_defaults_not_shared_between_records():
    raw = write_file(make_records(4)).getvalue()
    results = read_with_reader_schema(raw)

    assert results[0]["extra2"] == [1, 2]
    results[0]["extra2"].append(999)
    results[0]["meta"]["b"] = "mutated"
    assert results[1]["extra2"] == [1, 2]
    assert results[1]["meta"] == {"a": "b"}

    # reading again also produces fresh defaults
    again = read_with_reader_schema(raw)
    assert again[0]["extra2"] == [1, 2]
    assert again[0]["meta"] == {"a": "b"}


def test_plan_does_not_retain_caller_schema_dicts():
    reader_schema = {
        "type": "record",
        "name": "Row",
        "fields": [
            {"name": "id", "type": "long"},
            {
                "name": "vals",
                "type": {"type": "array", "items": "int"},
                "default": [7],
            },
        ],
    }
    writer_schema = {
        "type": "record",
        "name": "Row",
        "fields": [{"name": "id", "type": "long"}],
    }
    bio = BytesIO()
    _write_py.writer(bio, writer_schema, [{"id": 1}, {"id": 2}])
    bio.seek(0)

    avro_reader = _read_py.reader(bio, reader_schema)
    iterator = iter(avro_reader)
    assert next(iterator)["vals"] == [7]

    # caller mutates their schema after the plan was compiled
    reader_schema["fields"][1]["default"].append(1000)
    reader_schema["fields"][0]["name"] = "renamed"

    assert next(iterator)["vals"] == [7]


def test_plans_isolated_between_reader_schemas():
    raw = write_file(make_records(5)).getvalue()

    other_reader_schema = {
        "type": "record",
        "name": "Node",
        "namespace": "plan",
        "fields": [
            {"name": "other_id", "aliases": ["id"], "type": "long"},
            {"name": "flag", "type": "string", "default": "other"},
        ],
    }

    results_a = read_with_reader_schema(raw)
    results_b = list(_read_py.reader(BytesIO(raw), other_reader_schema))
    results_a_again = read_with_reader_schema(raw)

    assert results_b == [{"other_id": i, "flag": "other"} for i in range(5)]
    assert results_a == results_a_again
    assert results_a[0]["identifier"] == 0


def count_schema_decisions(record_count):
    """Read record_count records and count schema resolution decisions."""
    counts = {"match_schemas": 0, "compile_record_plan": 0}
    real_match_schemas = _read_py.match_schemas
    real_compile_record_plan = _read_py._compile_record_plan

    def counting_match_schemas(*args, **kwargs):
        counts["match_schemas"] += 1
        return real_match_schemas(*args, **kwargs)

    def counting_compile_record_plan(*args, **kwargs):
        counts["compile_record_plan"] += 1
        return real_compile_record_plan(*args, **kwargs)

    _read_py.match_schemas = counting_match_schemas
    _read_py._compile_record_plan = counting_compile_record_plan
    try:
        raw = write_file(make_records(record_count)).getvalue()
        results = read_with_reader_schema(raw)
        assert len(results) == record_count
    finally:
        _read_py.match_schemas = real_match_schemas
        _read_py._compile_record_plan = real_compile_record_plan
    return counts


def test_schema_decisions_do_not_scale_with_record_count():
    small = count_schema_decisions(10)
    large = count_schema_decisions(500)

    assert small["match_schemas"] > 0
    assert small["compile_record_plan"] > 0
    assert large == small


def test_generic_path_schema_decisions_scale_linearly(monkeypatch):
    """Sanity check: the old per-record path really did scale linearly,
    so the constant counts above are meaningful."""
    monkeypatch.setattr(_read_py, "_compile_read_plan", uncompiled_plan)

    small = count_schema_decisions(10)
    large = count_schema_decisions(500)
    assert large["match_schemas"] > small["match_schemas"] * 10


def test_block_reader_matches_reader():
    records = make_records(20)
    raw = write_file(records).getvalue()

    via_reader = read_with_reader_schema(raw)
    via_blocks = [
        record
        for block in _read_py.block_reader(BytesIO(raw), READER_SCHEMA)
        for record in block
    ]
    assert via_reader == via_blocks


def test_schemaless_reader_with_reader_schema():
    record = make_records(1)[0]
    bio = BytesIO()
    _write_py.schemaless_writer(bio, WRITER_SCHEMA, record)
    bio.seek(0)

    result = _read_py.schemaless_reader(bio, WRITER_SCHEMA, READER_SCHEMA)
    assert result["identifier"] == 0
    assert result["extra2"] == [1, 2]
    assert result["meta"] == {"a": "b"}
    assert "extra" not in result


def test_union_branch_matching_and_record_name():
    writer_schema = {
        "type": "record",
        "name": "U",
        "fields": [
            {"name": "value", "type": ["null", "string", "long"]},
        ],
    }
    reader_schema = {
        "type": "record",
        "name": "U",
        "fields": [
            # reordered union branches
            {"name": "value", "type": ["string", "null", "long"]},
        ],
    }
    records = [{"value": None}, {"value": "x"}, {"value": 3}]
    bio = BytesIO()
    _write_py.writer(bio, writer_schema, records)
    bio.seek(0)
    assert list(_read_py.reader(bio, reader_schema)) == records

    # return_record_name still works through the plan
    rec_writer = {
        "type": "record",
        "name": "W",
        "fields": [
            {
                "name": "u",
                "type": [
                    {
                        "type": "record",
                        "name": "A",
                        "fields": [{"name": "x", "type": "int"}],
                    },
                    {
                        "type": "record",
                        "name": "B",
                        "fields": [{"name": "y", "type": "string"}],
                    },
                ],
            }
        ],
    }
    data = [{"u": ("A", {"x": 1})}, {"u": ("B", {"y": "z"})}]
    bio = BytesIO()
    _write_py.writer(bio, rec_writer, data)
    bio.seek(0)
    assert list(_read_py.reader(bio, return_record_name=True)) == data


def test_missing_default_error_points_at_field():
    reader_schema = {
        "type": "record",
        "name": "Node",
        "namespace": "plan",
        "fields": [
            {"name": "identifier", "aliases": ["id"], "type": "long"},
            {"name": "mandatory", "type": "string"},
        ],
    }
    raw = write_file(make_records(2)).getvalue()
    with pytest.raises(
        SchemaResolutionError,
        match="No default value for field mandatory in plan.Node",
    ):
        list(_read_py.reader(BytesIO(raw), reader_schema))


def test_incompatible_field_error():
    reader_schema = {
        "type": "record",
        "name": "Node",
        "namespace": "plan",
        "fields": [
            {"name": "id", "type": "string"},
        ],
    }
    raw = write_file(make_records(2)).getvalue()
    with pytest.raises(SchemaResolutionError, match="Schema mismatch"):
        list(_read_py.reader(BytesIO(raw), reader_schema))


def test_enum_default_and_error_message():
    writer_schema = {
        "type": "record",
        "name": "E",
        "fields": [
            {
                "name": "suit",
                "type": {
                    "type": "enum",
                    "name": "Suit",
                    "symbols": ["SPADES", "HEARTS"],
                },
            }
        ],
    }
    reader_schema = {
        "type": "record",
        "name": "E",
        "fields": [
            {
                "name": "suit",
                "type": {
                    "type": "enum",
                    "name": "Suit",
                    "symbols": ["SPADES"],
                    "default": "SPADES",
                },
            }
        ],
    }
    bio = BytesIO()
    _write_py.writer(bio, writer_schema, [{"suit": "HEARTS"}])
    bio.seek(0)
    assert list(_read_py.reader(bio, reader_schema)) == [{"suit": "SPADES"}]

    reader_schema_no_default = {
        "type": "record",
        "name": "E",
        "fields": [
            {
                "name": "suit",
                "type": {
                    "type": "enum",
                    "name": "Suit",
                    "symbols": ["SPADES"],
                },
            }
        ],
    }
    bio.seek(0)
    with pytest.raises(
        SchemaResolutionError,
        match=r"HEARTS not found in reader symbol list Suit, "
        r"known symbols: \['SPADES'\]",
    ):
        list(_read_py.reader(bio, reader_schema_no_default))


def test_empty_file_with_incompatible_reader_schema_does_not_raise():
    """No records means no schema resolution errors, matching the generic
    path which only resolves schemas when a record is read."""
    reader_schema = {
        "type": "record",
        "name": "Node",
        "namespace": "plan",
        "fields": [{"name": "id", "type": "string"}],
    }
    raw = write_file([]).getvalue()
    assert list(_read_py.reader(BytesIO(raw), reader_schema)) == []
