import os
from io import BytesIO

import pytest

import fastavro
import fastavro.ocf as merge_module
from fastavro._read_common import MAGIC, SYNC_SIZE
from fastavro.io.binary_decoder import BinaryDecoder
from fastavro.ocf import OCFMergeError


schema = {
    "type": "record",
    "name": "test_merge",
    "fields": [
        {"name": "str_field", "type": "string"},
        {
            "name": "optional",
            "type": ["string", "null"],
            "default": None,
        },
        {
            "name": "day",
            "type": {"type": "int", "logicalType": "date"},
        },
    ],
}


def make_records(start, count):
    return [
        {
            "str_field": f"record-{start + i}",
            "optional": None if i % 2 else f"v{start + i}",
            "day": start + i,
        }
        for i in range(count)
    ]


def write_container(records, codec="null", metadata=None, sch=schema):
    fo = BytesIO()
    fastavro.writer(
        fo,
        sch,
        records,
        codec=codec,
        metadata=metadata or {},
        sync_interval=40,
    )
    fo.seek(0)
    return fo


def raw_payloads(fo):
    fo.seek(0)
    return [
        (block.num_records, block.payload)
        for block in fastavro.iter_raw_blocks(fo)
    ]


def read_records(fo):
    fo.seek(0)
    return list(fastavro.reader(fo))


@pytest.mark.parametrize("codec", ["null", "deflate"])
def test_merge_preserves_record_sequence_and_payloads(codec):
    records_one = make_records(0, 25)
    records_two = make_records(25, 15)
    records_three = make_records(40, 5)

    first = write_container(records_one, codec=codec, metadata={"env": "ci"})
    second = write_container(records_two, codec=codec, metadata={"env": "ci"})
    third = write_container(
        records_three,
        codec=codec,
        metadata={"env": "ci", "extra": "kept"},
    )

    expected_payloads = (
        raw_payloads(BytesIO(first.getvalue()))
        + raw_payloads(BytesIO(second.getvalue()))
        + raw_payloads(BytesIO(third.getvalue()))
    )

    output = BytesIO()
    fastavro.merge([first, second, third], output)

    # First compare the merged record sequence against the concatenation.
    merged_records = read_records(output)
    assert merged_records == read_records(
        write_container(records_one + records_two + records_three, codec=codec)
    )

    # Then compare the raw, compressed payloads byte for byte.
    assert raw_payloads(output) == expected_payloads


def test_output_uses_its_own_sync_marker_and_header():
    first = write_container(make_records(0, 10))
    second = write_container(make_records(10, 4))

    first.seek(0)
    input_sync = fastavro.reader(first)._header["sync"]
    first.seek(0)

    output = BytesIO()
    fastavro.merge([first, second], output, sync_marker=b"m" * 16)

    output.seek(0)
    reader = fastavro.reader(output)
    assert reader._header["sync"] == b"m" * 16
    assert reader._header["sync"] != input_sync

    # Every block trailer must be the output marker, never an input one.
    blob = output.getvalue()
    probe = BinaryDecoder(BytesIO(blob))
    merge_module._read_container_info(probe.fo)
    trailers = []
    while True:
        try:
            count = probe.read_long()
        except EOFError:
            break
        size = probe.read_long()
        probe.read_fixed(size)
        trailers.append(probe.fo.read(SYNC_SIZE))
    assert trailers == [b"m" * 16] * len(trailers)
    assert trailers


def test_input_headers_are_not_copied_into_the_output():
    first = write_container(make_records(0, 6), metadata={"only_in_first": "x"})
    second = write_container(make_records(6, 6), metadata={"only_in_second": "y"})

    output = BytesIO()
    fastavro.merge([first, second], output)

    # The output starts with a single OCF magic, i.e. one header only.
    blob = output.getvalue()
    assert blob.startswith(MAGIC)
    assert blob.count(MAGIC) == 1

    output.seek(0)
    metadata = fastavro.reader(output).metadata
    assert metadata["only_in_first"] == "x"
    assert metadata["only_in_second"] == "y"


def test_user_metadata_conflicts_are_rejected():
    first = write_container(make_records(0, 3), metadata={"k": "one"})
    second = write_container(make_records(3, 3), metadata={"k": "two"})

    with pytest.raises(OCFMergeError, match="conflicting user metadata"):
        fastavro.merge([first, second], BytesIO())


def test_metadata_argument_overrides_and_reserved_keys_rejected():
    first = write_container(make_records(0, 3), metadata={"k": "one"})

    output = BytesIO()
    fastavro.merge([first], output, metadata={"k": "two", "added": "yes"})
    output.seek(0)
    metadata = fastavro.reader(output).metadata
    assert metadata["k"] == "two"
    assert metadata["added"] == "yes"

    with pytest.raises(OCFMergeError, match="reserved key"):
        fastavro.merge(
            [write_container(make_records(0, 1))],
            BytesIO(),
            metadata={"avro.codec": "deflate"},
        )


def test_codec_mismatch_is_rejected():
    first = write_container(make_records(0, 3), codec="null")
    second = write_container(make_records(3, 3), codec="deflate")

    with pytest.raises(OCFMergeError, match="codec"):
        fastavro.merge([first, second], BytesIO())


@pytest.mark.parametrize(
    "other_schema",
    [
        {
            "type": "record",
            "name": "test_merge_renamed",
            "fields": [
                {"name": "str_field", "type": "string"},
                {"name": "optional", "type": ["string", "null"], "default": None},
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
            ],
        },
        {
            "type": "record",
            "name": "test_merge",
            "fields": [
                {"name": "str_field", "type": "string"},
                {
                    "name": "optional",
                    "type": ["string", "null"],
                    "default": "different",
                },
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
            ],
        },
        {
            "type": "record",
            "name": "test_merge",
            "fields": [
                {"name": "str_field", "type": "string"},
                {"name": "optional", "type": ["string", "null"], "default": None},
                {"name": "day", "type": "int"},
            ],
        },
    ],
)
def test_incomplete_schema_differences_are_rejected(other_schema):
    first = write_container(make_records(0, 3))
    second = write_container(make_records(3, 3), sch=other_schema)

    with pytest.raises(OCFMergeError, match="complete writer schema"):
        fastavro.merge([first, second], BytesIO())


def test_unsupported_codec_is_rejected():
    fo = BytesIO()
    fastavro.writer(fo, schema, make_records(0, 3), codec="bzip2")
    fo.seek(0)

    with pytest.raises(OCFMergeError, match="unsupported codec"):
        fastavro.merge([fo], BytesIO())


def test_missing_header_is_distinct_from_empty_container():
    empty_container = write_container([])
    output = BytesIO()
    fastavro.merge([empty_container], output)
    assert read_records(output) == []

    with pytest.raises(OCFMergeError, match="cannot read header"):
        list(fastavro.iter_raw_blocks(BytesIO(b"")))

    with pytest.raises(OCFMergeError, match="cannot read header"):
        list(fastavro.iter_raw_blocks(BytesIO(b"this is not avro at all!!")))


def _header_and_sync():
    fo = write_container(make_records(0, 2))
    blob = fo.getvalue()
    probe = BytesIO(blob)
    merge_module._read_container_info(probe)
    return blob[: probe.tell()], blob[-16:]


def test_illegal_block_lengths_are_rejected():
    header, sync = _header_and_sync()

    # Positive count followed by a zero size.
    with pytest.raises(OCFMergeError, match="illegal block size"):
        list(fastavro.iter_raw_blocks(BytesIO(header + b"\x02\x00" + sync)))

    # Negative count followed by a zero size.
    with pytest.raises(OCFMergeError, match="illegal block size"):
        list(fastavro.iter_raw_blocks(BytesIO(header + b"\x01\x00" + sync)))


def test_truncated_blocks_are_rejected():
    header, sync = _header_and_sync()

    with pytest.raises(OCFMergeError, match="missing block size"):
        list(fastavro.iter_raw_blocks(BytesIO(header + b"\x02")))

    with pytest.raises(OCFMergeError, match="payload is shorter"):
        list(fastavro.iter_raw_blocks(BytesIO(header + b"\x02\x0aabc")))

    with pytest.raises(OCFMergeError, match="missing sync marker"):
        list(
            fastavro.iter_raw_blocks(
                BytesIO(header + b"\x02\x02" + b"\x02" + sync[:10])
            )
        )


def test_wrong_sync_marker_is_rejected():
    header, sync = _header_and_sync()
    wrong_sync = bytearray(sync)
    wrong_sync[-1] ^= 0xFF

    with pytest.raises(OCFMergeError, match="expected sync marker not found"):
        list(
            fastavro.iter_raw_blocks(
                BytesIO(header + b"\x02\x02" + b"\x02" + bytes(wrong_sync))
            )
        )


def test_java_style_negative_count_blocks_are_supported():
    header, sync = _header_and_sync()
    payload = b"\x02"  # one record, the int value 1 in zigzag
    blob = header + b"\x01" + bytes([len(payload) * 2]) + payload + sync

    blocks = list(fastavro.iter_raw_blocks(BytesIO(blob)))
    assert len(blocks) == 1
    assert blocks[0].num_records == 1
    assert blocks[0].payload == payload


def test_merge_to_path_is_atomic_and_published_only_on_success(tmp_path):
    first = tmp_path / "first.avro"
    second = tmp_path / "second.avro"
    target = tmp_path / "merged.avro"

    with open(first, "wb") as fo:
        fastavro.writer(fo, schema, make_records(0, 4))
    with open(second, "wb") as fo:
        fastavro.writer(fo, schema, make_records(4, 4))

    fastavro.merge([str(first), str(second)], str(target))
    assert read_records(open(target, "rb")) == read_records(
        write_container(make_records(0, 8))
    )

    original_bytes = target.read_bytes()

    # Break the sync marker of the second input.
    corrupted = bytearray(second.read_bytes())
    corrupted[-1] ^= 0xFF
    second.write_bytes(bytes(corrupted))

    with pytest.raises(OCFMergeError, match="expected sync marker"):
        fastavro.merge([str(first), str(second)], str(target))

    assert target.read_bytes() == original_bytes
    assert not [
        name for name in os.listdir(tmp_path) if name.startswith("tmp")
    ]


def test_failed_merge_never_creates_the_output(tmp_path):
    empty = tmp_path / "empty.bin"
    empty.write_bytes(b"")
    target = tmp_path / "should_not_exist.avro"

    with pytest.raises(OCFMergeError, match="cannot read header"):
        fastavro.merge([str(empty)], str(target))

    assert not target.exists()


def test_merge_requires_inputs_and_valid_sync_marker():
    with pytest.raises(OCFMergeError, match="at least one input"):
        fastavro.merge([], BytesIO())

    with pytest.raises(OCFMergeError, match="sync marker must be 16 bytes"):
        fastavro.merge(
            [write_container(make_records(0, 1))],
            BytesIO(),
            sync_marker=b"too-short",
        )


def test_inputs_do_not_need_to_be_seekable():
    class ForwardOnly(BytesIO):
        def seekable(self):
            return False

        def seek(self, *args):
            raise OSError("forward only")

    first = ForwardOnly(write_container(make_records(0, 4)).getvalue())
    second = ForwardOnly(write_container(make_records(4, 3)).getvalue())

    output = BytesIO()
    fastavro.merge([first, second], output)

    assert read_records(output) == read_records(
        write_container(make_records(0, 7))
    )


def test_only_container_framing_is_validated():
    # A syntactically valid frame around an undecodable payload is still
    # accepted: record values are intentionally not decoded while merging.
    header, sync = _header_and_sync()
    invalid_payload = b"\xff\xff\xff"
    framed = (
        header
        + b"\x02"
        + bytes([len(invalid_payload) * 2])
        + invalid_payload
        + sync
    )

    output = BytesIO()
    fastavro.merge([BytesIO(framed)], output)
    assert raw_payloads(output) == [(1, invalid_payload)]
