import json
import os

import pytest

import fastavro

from ocf_index import (
    INDEX_VERSION,
    IndexBuildError,
    IndexError_,
    Query,
    build_index,
    default_sidecar_path,
    load_index,
    scan,
    between,
    column,
    eq,
    ge,
    gt,
    is_null,
    isnot_null,
    le,
    lt,
    ne,
)
from ocf_index.scan import (
    FALLBACK_BLOCK_MISMATCH,
    FALLBACK_CORRUPT_SIDECAR,
    FALLBACK_FILE_MISMATCH,
    FALLBACK_MISSING_SIDECAR,
    FALLBACK_SCHEMA_MISMATCH,
    FALLBACK_UNSUPPORTED_QUERY,
)
from ocf_index_test_fixtures import (
    ALL_RECORDS,
    SCHEMA,
    all_records,
    count_blocks,
    expected_matching,
    load_sidecar,
    write_multiblock_avro,
    write_sidecar,
)


@pytest.fixture()
def avro_file(tmp_path):
    path = str(tmp_path / "measurements.avro")
    write_multiblock_avro(path)
    yield path
    sidecar = default_sidecar_path(path)
    if os.path.exists(sidecar):
        os.remove(sidecar)


@pytest.fixture()
def indexed_file(avro_file):
    build_index(avro_file)
    return avro_file


def test_index_records_one_entry_per_physical_block(avro_file):
    assert count_blocks(avro_file) == 4
    index = build_index(avro_file)
    assert len(index.blocks) == 4
    assert index.version == INDEX_VERSION
    # Only nullable int/string unions are indexable; `id` is a plain long.
    assert index.indexed_fields == ("score", "label")

    with open(avro_file, "rb") as handle:
        physical = list(fastavro.block_reader(handle))
    for entry, block in zip(index.blocks, physical):
        assert entry.offset == block.offset
        assert entry.size == block.size
        assert entry.record_count == block.num_records


def test_index_does_not_modify_ocf(avro_file):
    before = open(avro_file, "rb").read()
    build_index(avro_file)
    after = open(avro_file, "rb").read()
    assert before == after
    assert os.path.exists(default_sidecar_path(avro_file))


def test_block_statistics_capture_min_max_and_nulls(indexed_file):
    index = load_index(default_sidecar_path(indexed_file))
    blocks = index.blocks

    all_null = blocks[0].fields["score"]
    assert all_null.min is None and all_null.max is None
    assert all_null.null_count == 2

    low = blocks[1].fields["score"]
    assert (low.min, low.max) == (0, 19)
    assert low.null_count == 0

    high = blocks[2].fields["score"]
    assert (high.min, high.max) == (20, 50)

    mixed = blocks[3].fields["score"]
    assert (mixed.min, mixed.max) == (60, 70)
    assert mixed.null_count == 1

    label_nulls = blocks[3].fields["label"]
    assert (label_nulls.min, label_nulls.max) == ("q", "r")
    assert label_nulls.null_count == 1


def test_scan_without_index_is_full_scan(avro_file):
    sidecar = default_sidecar_path(avro_file)
    if os.path.exists(sidecar):
        os.remove(sidecar)
    result = scan(avro_file, Query((gt("score", 30),)))
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_MISSING_SIDECAR
    assert result.decoded_blocks == 4
    assert result.total_records == len(ALL_RECORDS)
    assert result.records == expected_matching(result_query_gt_30())


def result_query_gt_30():
    return Query((gt("score", 30),))


def test_indexed_scan_skips_blocks_but_keeps_all_matches(indexed_file):
    query = Query((gt("score", 30),))
    result = scan(indexed_file, query)
    assert result.used_index is True
    assert result.fallback_reason is None
    # Block 0 (all null) and block 1 (max 19) are provably empty for > 30.
    assert result.skipped_blocks == 2
    assert result.decoded_blocks == 2
    assert result.records == expected_matching(query)
    assert [row["id"] for row in result.records] == [7, 8, 10, 11]


def test_full_scan_and_indexed_scan_return_identical_records(indexed_file):
    sidecar = default_sidecar_path(indexed_file)
    os.remove(sidecar)
    query = Query((ge("score", 10), lt("score", 61)))
    full = scan(indexed_file, query)
    build_index(indexed_file)
    indexed = scan(indexed_file, query)
    assert full.records == indexed.records
    assert full.decoded_blocks == 4
    assert indexed.decoded_blocks < full.decoded_blocks


@pytest.mark.parametrize(
    "query",
    [
        Query((eq("score", 20),)),
        Query((ne("score", 20),)),
        Query((lt("score", 20),)),
        Query((le("score", 20),)),
        Query((gt("score", 20),)),
        Query((ge("score", 20),)),
        between("score", 19, 50),
        between("score", 20, 20),
        Query((is_null("score"),)),
        Query((isnot_null("score"),)),
        Query((ge("label", "n"), le("label", "z"))),
        Query((gt("score", 0), isnot_null("label"))),
        Query(()),
    ],
)
def test_boundary_equality_and_range_predicates(indexed_file, query):
    result = scan(indexed_file, query)
    assert result.used_index is True
    assert result.records == expected_matching(query)


def test_equality_on_boundary_value_does_not_skip_block(indexed_file):
    # 20 is the min of block 2: the block must be decoded, not pruned.
    result = scan(indexed_file, Query((eq("score", 20),)))
    assert result.decoded_blocks >= 1
    assert [row["id"] for row in result.records] == [6]


def test_all_null_block_skipped_for_comparison_but_kept_for_isnull(indexed_file):
    comparison = scan(indexed_file, gt("score", 0))
    assert all(row["id"] != 1 for row in comparison.records)
    assert comparison.records == expected_matching(Query((gt("score", 0),)))

    nulls = scan(indexed_file, is_null("score"))
    assert nulls.used_index is True
    # Only blocks 0 and 3 contain nulls for score.
    assert nulls.decoded_blocks == 2
    assert sorted(row["id"] for row in nulls.records) == [1, 2, 9]


def test_exhaustive_correctness_across_literals(indexed_file):
    literals = list(range(-5, 76))
    for literal in literals:
        for builder in (eq, ne, lt, le, gt, ge):
            query = Query((builder("score", literal),))
            result = scan(indexed_file, query)
            assert result.records == expected_matching(query), (
                builder.__name__,
                literal,
            )


def test_corrupt_sidecar_checksum_falls_back_to_full_scan(indexed_file):
    sidecar = default_sidecar_path(indexed_file)
    payload = load_sidecar(sidecar)
    payload["blocks"][1]["fields"]["score"]["min"] = -9999
    write_sidecar(sidecar, payload)

    with pytest.raises(IndexError_):
        load_index(sidecar)

    query = Query((gt("score", 30),))
    result = scan(indexed_file, query)
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_CORRUPT_SIDECAR
    assert result.decoded_blocks == 4
    assert result.records == expected_matching(query)


def test_truncated_sidecar_falls_back_to_full_scan(indexed_file):
    sidecar = default_sidecar_path(indexed_file)
    with open(sidecar, "r", encoding="utf-8") as handle:
        text = handle.read()
    with open(sidecar, "w", encoding="utf-8") as handle:
        handle.write(text[: len(text) // 2])

    result = scan(indexed_file, eq("score", 20))
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_CORRUPT_SIDECAR
    assert result.records == expected_matching(Query((eq("score", 20),)))


def test_sidecar_from_another_file_is_rejected(avro_file, tmp_path):
    build_index(avro_file)
    other = str(tmp_path / "other.avro")
    write_multiblock_avro(other)
    # Reuse the first file's sidecar against a different data file.
    foreign = default_sidecar_path(other)
    with open(default_sidecar_path(avro_file), "rb") as source:
        data = source.read()
    with open(foreign, "wb") as dest:
        dest.write(data)

    result = scan(other, gt("score", 30), sidecar_path=foreign)
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_FILE_MISMATCH
    assert result.decoded_blocks == 4
    assert result.records == expected_matching(Query((gt("score", 30),)))


def test_schema_fingerprint_mismatch_forces_full_scan(indexed_file, tmp_path):
    sidecar = default_sidecar_path(indexed_file)
    payload = load_sidecar(sidecar)
    payload["schema_fingerprint"] = "0" * len(payload["schema_fingerprint"])
    payload["checksum"] = recompute_checksum(payload)
    write_sidecar(sidecar, payload)

    result = scan(indexed_file, gt("score", 30))
    assert result.fallback_reason == FALLBACK_SCHEMA_MISMATCH
    assert result.used_index is False
    assert result.records == expected_matching(Query((gt("score", 30),)))


def test_block_layout_mismatch_even_with_valid_identity(indexed_file):
    sidecar = default_sidecar_path(indexed_file)
    payload = load_sidecar(sidecar)
    # Keep file hash/schema intact but lie about a block's record count.
    payload["blocks"][0]["record_count"] += 1
    payload["checksum"] = recompute_checksum(payload)
    write_sidecar(sidecar, payload)

    result = scan(indexed_file, gt("score", 30))
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_BLOCK_MISMATCH
    assert result.decoded_blocks == 4
    assert result.records == expected_matching(Query((gt("score", 30),)))


def test_block_layout_mismatch_forces_full_scan(avro_file, tmp_path):
    build_index(avro_file)
    # Append records to the same file: identity hash changes and layout drifts.
    with open(avro_file, "a+b") as handle:
        fastavro.writer(handle, None, [{"id": 99, "score": 99, "label": "zz"}])

    result = scan(avro_file, gt("score", 30))
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_FILE_MISMATCH
    assert result.decoded_blocks == count_blocks(avro_file)
    expected = [
        record
        for record in all_records(avro_file)
        if record.get("score") is not None and record["score"] > 30
    ]
    assert result.records == expected


def test_unsupported_query_field_falls_back(indexed_file):
    # id is a non-nullable long, which is deliberately not indexable here.
    result = scan(indexed_file, gt("id", 5))
    assert result.used_index is False
    assert result.fallback_reason == FALLBACK_UNSUPPORTED_QUERY
    assert result.decoded_blocks == 4
    assert [row["id"] for row in result.records] == [6, 7, 8, 9, 10, 11]


def test_build_index_rejects_explicit_unsupported_field(avro_file):
    with pytest.raises(IndexBuildError):
        build_index(avro_file, ["id"])


def test_partial_field_index_still_supports_subset(avro_file):
    index = build_index(avro_file, ["score"])
    assert index.indexed_fields == ("score",)
    result = scan(avro_file, eq("score", 50))
    assert result.used_index is True
    assert [row["id"] for row in result.records] == [8]

    # A field absent from the index cannot be pruned and triggers full scan.
    fallback = scan(avro_file, is_null("label"))
    assert fallback.fallback_reason == FALLBACK_UNSUPPORTED_QUERY
    assert fallback.decoded_blocks == 4


def test_query_builder_helpers_match_manual_predicates():
    built = Query((column("score").ge(10),))
    manual = Query((ge("score", 10),))
    assert built.predicates == manual.predicates
    assert between("score", 1, 5).predicates == (
        ge("score", 1),
        le("score", 5),
    )


def test_predicate_literal_type_validation():
    with pytest.raises(ValueError):
        eq("score", 1.5)
    with pytest.raises(ValueError):
        between("score", 1, "2")


def test_deflate_codec_round_trip_with_pruning(tmp_path):
    path = str(tmp_path / "compressed.avro")
    write_multiblock_avro(path, codec="deflate")
    build_index(path)
    result = scan(path, between("score", 20, 50))
    assert result.used_index is True
    assert result.records == expected_matching(between("score", 20, 50))


def recompute_checksum(payload):
    from ocf_index.index import _compute_checksum  # noqa: PLC2701

    payload.pop("checksum", None)
    rebuilt = {
        "version": payload["version"],
        "file_hash": payload["file_hash"],
        "schema_fingerprint": payload["schema_fingerprint"],
        "schema_canonical": payload["schema_canonical"],
        "indexed_fields": payload["indexed_fields"],
        "blocks": payload["blocks"],
    }
    return _compute_checksum(rebuilt)
