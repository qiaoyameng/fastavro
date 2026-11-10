import datetime
from decimal import Decimal
from io import BytesIO

import pytest

import fastavro
from fastavro.read import LogicalResolutionError


UTC = datetime.timezone.utc


def write_one(schema, record):
    fo = BytesIO()
    fastavro.schemaless_writer(fo, schema, record)
    fo.seek(0)
    return fo


def field_schema(logical_type, avro_type="long", **extra):
    schema = {"type": avro_type, "logicalType": logical_type}
    schema.update(extra)
    return schema


def record_schema(name, field):
    return {
        "type": "record",
        "name": name,
        "fields": [{"name": "value", "type": field}],
    }


def ts_schema(logical_type):
    return record_schema("R", field_schema(logical_type))


def decimal_schema(kind, precision, scale, size=None, name="R"):
    field = {
        "type": kind,
        "logicalType": "decimal",
        "precision": precision,
        "scale": scale,
    }
    if kind == "fixed":
        field["name"] = "D"
        field["size"] = size
    return record_schema(name, field)


def read_exact(fo, writer_schema, reader_schema):
    return fastavro.schemaless_reader(
        fo,
        writer_schema,
        reader_schema,
        logical_resolution="exact",
    )


def assert_error(writer_schema, reader_schema, record, fragment):
    fo = write_one(writer_schema, record)
    with pytest.raises(LogicalResolutionError) as exc_info:
        read_exact(fo, writer_schema, reader_schema)
    assert fragment in str(exc_info.value)


def test_legacy_ignores_reader_logical_type():
    writer = ts_schema("timestamp-micros")
    reader = ts_schema("timestamp-millis")
    fo = write_one(writer, {"value": 1234567})
    result = fastavro.schemaless_reader(fo, writer, reader)
    # The historical behaviour applies the writer's micros logical type.
    assert result["value"] == datetime.datetime(
        1970, 1, 1, 0, 0, 1, 234567, tzinfo=UTC
    )


def test_exact_without_reader_schema_keeps_writer_logical_type():
    writer = ts_schema("timestamp-micros")
    fo = write_one(writer, {"value": 1234000})
    result = fastavro.schemaless_reader(
        fo, writer, logical_resolution="exact"
    )
    assert result["value"] == datetime.datetime(
        1970, 1, 1, 0, 0, 1, 234000, tzinfo=UTC
    )


def test_micros_to_millis_requires_even_division():
    writer = ts_schema("timestamp-micros")
    reader = ts_schema("timestamp-millis")

    fo = write_one(writer, {"value": 1234000})
    assert read_exact(fo, writer, reader)["value"] == datetime.datetime(
        1970, 1, 1, 0, 0, 1, 234000, tzinfo=UTC
    )

    assert_error(
        writer,
        reader,
        {"value": -1234567},
        "R.value: long-timestamp-micros value -1234567",
    )


def test_millis_to_micros_is_always_exact():
    writer = ts_schema("timestamp-millis")
    reader = ts_schema("timestamp-micros")
    fo = write_one(writer, {"value": -42})
    assert read_exact(fo, writer, reader)["value"] == datetime.datetime(
        1969, 12, 31, 23, 59, 59, 958000, tzinfo=UTC
    )


@pytest.mark.parametrize(
    "writer_type,reader_type",
    [
        ("timestamp-micros", "local-timestamp-micros"),
        ("local-timestamp-millis", "timestamp-millis"),
    ],
)
def test_utc_and_local_timestamps_never_auto_convert(
    writer_type, reader_type
):
    assert_error(
        ts_schema(writer_type),
        ts_schema(reader_type),
        {"value": 1000},
        "UTC and local timestamps have different meanings",
    )


def test_local_timestamp_resolution_stays_naive():
    writer = ts_schema("local-timestamp-micros")
    reader = ts_schema("local-timestamp-millis")
    fo = write_one(writer, {"value": -5000})
    result = read_exact(fo, writer, reader)
    assert result["value"] == datetime.datetime(1969, 12, 31, 23, 59, 59, 995000)
    assert result["value"].tzinfo is None


def test_decimal_scale_increase_is_exact():
    writer = decimal_schema("bytes", 10, 4)
    reader = decimal_schema("bytes", 12, 6)
    fo = write_one(writer, {"value": Decimal("12.3456")})
    assert read_exact(fo, writer, reader)["value"] == Decimal("12.345600")


def test_decimal_scale_decrease_rejects_lost_digits():
    writer = decimal_schema("bytes", 8, 4)
    reader = decimal_schema("bytes", 8, 2)

    fo = write_one(writer, {"value": Decimal("1.2300")})
    assert read_exact(fo, writer, reader)["value"] == Decimal("1.23")

    assert_error(
        writer,
        reader,
        {"value": Decimal("1.2345")},
        "without losing non-zero digits",
    )


def test_decimal_precision_is_checked():
    writer = decimal_schema("bytes", 8, 4)
    reader = decimal_schema("bytes", 6, 6)
    assert_error(
        writer,
        reader,
        {"value": Decimal("12.3456")},
        "exceeds reader precision 6",
    )


def test_fixed_decimal_capacity_is_checked():
    writer = decimal_schema("fixed", 4, 2, size=2)
    reader = decimal_schema("fixed", 4, 3, size=2)
    assert_error(
        writer,
        reader,
        {"value": Decimal("99.99")},
        "does not fit into fixed size 2",
    )


def test_fixed_decimal_successful_resolution():
    writer = decimal_schema("fixed", 6, 3, size=4)
    reader = decimal_schema("fixed", 8, 5, size=4)
    fo = write_one(writer, {"value": Decimal("-99.001")})
    assert read_exact(fo, writer, reader)["value"] == Decimal("-99.00100")


def test_named_fixed_decimal_resolves_by_reference():
    writer = {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "amount",
                "type": {
                    "type": "fixed",
                    "name": "Money",
                    "size": 4,
                    "logicalType": "decimal",
                    "precision": 8,
                    "scale": 2,
                },
            }
        ],
    }
    reader = {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "amount",
                "type": {
                    "type": "fixed",
                    "name": "Money",
                    "size": 4,
                    "logicalType": "decimal",
                    "precision": 9,
                    "scale": 4,
                },
            }
        ],
    }
    fo = write_one(writer, {"amount": Decimal("12.34")})
    assert read_exact(fo, writer, reader) == {"amount": Decimal("12.3400")}


def test_recursive_record_path_is_reported():
    writer = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "ts", "type": field_schema("timestamp-micros")},
            {"name": "next", "type": ["null", "Node"]},
        ],
    }
    reader = {
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "ts", "type": field_schema("timestamp-millis")},
            {"name": "next", "type": ["null", "Node"]},
        ],
    }
    record = {"ts": 1000, "next": {"ts": 2500, "next": None}}
    assert_error(
        writer,
        reader,
        record,
        "Node.next.<union:Node>.Node.ts",
    )


def test_unknown_logical_type_is_not_guessed():
    writer = record_schema("R", field_schema("weird", avro_type="string"))
    assert_error(
        writer,
        writer,
        {"value": "x"},
        "unknown or unsupported logical type string-weird",
    )


def test_reader_without_logical_type_gets_raw_datum():
    writer = ts_schema("timestamp-micros")
    reader = record_schema("R", "long")
    fo = write_one(writer, {"value": 1234})
    assert read_exact(fo, writer, reader) == {"value": 1234}

    writer = decimal_schema("bytes", 6, 2)
    reader = record_schema("R", "bytes")
    fo = write_one(writer, {"value": Decimal("1.23")})
    assert read_exact(fo, writer, reader) == {"value": b"\x7b"}


def test_logical_error_reports_map_and_nested_record_paths():
    map_writer = {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "mapping",
                "type": {
                    "type": "map",
                    "values": field_schema("timestamp-micros"),
                },
            }
        ],
    }
    map_reader = {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "mapping",
                "type": {
                    "type": "map",
                    "values": field_schema("timestamp-millis"),
                },
            }
        ],
    }
    assert_error(
        map_writer,
        map_reader,
        {"mapping": {"a": 1000, "b": 2500}},
        "R.mapping.['b']",
    )

    nested_writer = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "inner",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": [
                        {"name": "when", "type": field_schema("timestamp-micros")}
                    ],
                },
            }
        ],
    }
    nested_reader = {
        "type": "record",
        "name": "Outer",
        "fields": [
            {
                "name": "inner",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": [
                        {"name": "when", "type": field_schema("timestamp-millis")}
                    ],
                },
            }
        ],
    }
    assert_error(
        nested_writer,
        nested_reader,
        {"inner": {"when": 777}},
        "Outer.inner.Inner.when",
    )


def test_logical_error_reports_nested_record_array_and_union_paths():
    writer = {
        "type": "record",
        "name": "Top",
        "fields": [
            {
                "name": "items",
                "type": {
                    "type": "array",
                    "items": field_schema("timestamp-micros"),
                },
            }
        ],
    }
    reader = {
        "type": "record",
        "name": "Top",
        "fields": [
            {
                "name": "items",
                "type": {
                    "type": "array",
                    "items": field_schema("timestamp-millis"),
                },
            }
        ],
    }
    assert_error(
        writer,
        reader,
        {"items": [1000, 2500]},
        "Top.items.[1]",
    )

    union_writer = record_schema(
        "U",
        ["null", field_schema("timestamp-micros")],
    )
    union_reader = record_schema(
        "U",
        ["null", field_schema("timestamp-millis")],
    )
    assert_error(
        union_writer,
        union_reader,
        {"value": 777},
        "U.value.<union:long>",
    )


def test_record_defaults_follow_exact_resolution():
    writer = {"type": "record", "name": "R", "fields": []}
    reader = {
        "type": "record",
        "name": "R",
        "fields": [
            {
                "name": "ts",
                "type": field_schema("timestamp-millis"),
                "default": 1500,
            },
            {
                "name": "amount",
                "type": field_schema(
                    "decimal",
                    avro_type="bytes",
                    precision=6,
                    scale=2,
                ),
                "default": "\u001c",
            },
        ],
    }
    fo = write_one(writer, {})
    result = read_exact(fo, writer, reader)
    assert result["ts"] == datetime.datetime(
        1970, 1, 1, 0, 0, 1, 500000, tzinfo=UTC
    )
    assert result["amount"] == Decimal("0.28")


def test_union_record_defaults_follow_exact_resolution():
    writer = {"type": "record", "name": "R", "fields": []}
    reader = {
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "missing", "type": "null", "default": None},
            {
                "name": "amount",
                "type": [
                    field_schema(
                        "decimal",
                        avro_type="bytes",
                        precision=6,
                        scale=2,
                    ),
                    "null",
                ],
                "default": "\u001c",
            },
        ],
    }
    result = read_exact(write_one(writer, {}), writer, reader)
    assert result["missing"] is None
    assert result["amount"] == Decimal("0.28")


def test_ocf_block_and_schemaless_readers_agree():
    writer_schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {
                "name": "ts",
                "type": field_schema("timestamp-micros"),
            },
            {
                "name": "amount",
                "type": field_schema(
                    "decimal",
                    avro_type="bytes",
                    precision=10,
                    scale=4,
                ),
            },
        ],
    }
    reader_schema = {
        "type": "record",
        "name": "Event",
        "fields": [
            {
                "name": "ts",
                "type": field_schema("timestamp-millis"),
            },
            {
                "name": "amount",
                "type": field_schema(
                    "decimal",
                    avro_type="bytes",
                    precision=12,
                    scale=6,
                ),
            },
        ],
    }
    records = [
        {"ts": 1234000, "amount": Decimal("12.3456")},
        {"ts": -2000000, "amount": Decimal("-0.0001")},
    ]

    fo = BytesIO()
    fastavro.writer(fo, writer_schema, records, sync_interval=40)
    raw = fo.getvalue()

    ocf_result = list(
        fastavro.reader(
            BytesIO(raw), reader_schema, logical_resolution="exact"
        )
    )
    block_result = [
        record
        for block in fastavro.block_reader(
            BytesIO(raw), reader_schema, logical_resolution="exact"
        )
        for record in block
    ]
    schemaless_result = [
        read_exact(write_one(writer_schema, record), writer_schema, reader_schema)
        for record in records
    ]

    assert ocf_result == block_result == schemaless_result
    assert ocf_result[0]["amount"] == Decimal("12.345600")
    assert ocf_result[1]["ts"] == datetime.datetime(
        1969, 12, 31, 23, 59, 58, tzinfo=UTC
    )


def test_successful_values_round_trip_into_old_logical_types():
    writer_schema = {
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "ts", "type": field_schema("timestamp-micros")},
            {
                "name": "amount",
                "type": field_schema(
                    "decimal",
                    avro_type="bytes",
                    precision=10,
                    scale=4,
                ),
            },
        ],
    }
    reader_schema = {
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "ts", "type": field_schema("timestamp-millis")},
            {
                "name": "amount",
                "type": field_schema(
                    "decimal",
                    avro_type="bytes",
                    precision=12,
                    scale=6,
                ),
            },
        ],
    }
    original = {"ts": 1234000, "amount": Decimal("12.3456")}
    original_as_logical = fastavro.schemaless_reader(
        write_one(writer_schema, original), writer_schema
    )

    resolved = read_exact(
        write_one(writer_schema, original), writer_schema, reader_schema
    )
    rewritten = {
        "ts": resolved["ts"],
        "amount": resolved["amount"].quantize(Decimal("0.0001")),
    }
    round_trip = fastavro.schemaless_reader(
        write_one(writer_schema, rewritten), writer_schema
    )
    assert round_trip == original_as_logical


@pytest.mark.parametrize(
    "entrypoint",
    [
        lambda fo, schema: fastavro.schemaless_reader(
            fo, schema, logical_resolution="invalid"
        ),
        lambda fo, schema: fastavro.reader(fo, schema, logical_resolution="invalid"),
    ],
)
def test_invalid_resolution_option_is_rejected(entrypoint):
    schema = record_schema("R", "int")
    with pytest.raises(ValueError, match="logical_resolution"):
        entrypoint(BytesIO(), schema)
