"""Shared fixtures for the OCF companion-index tests."""

import json
import os

import fastavro
from fastavro.write import Writer

# Deliberately small interval is *not* used; blocks are produced explicitly
# with Writer.dump() so each group below maps to exactly one OCF block.
NO_AUTO_FLUSH = 10**9

SCHEMA = {
    "type": "record",
    "name": "Measurement",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "score", "type": ["null", "int"]},
        {"name": "label", "type": ["null", "string"]},
    ],
}

# A second, structurally different schema used to prove schema binding.
OTHER_SCHEMA = {
    "type": "record",
    "name": "Measurement",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "score", "type": ["null", "long"]},
        {"name": "label", "type": ["null", "string"]},
        {"name": "extra", "type": ["null", "int"]},
    ],
}

# Block 0: fully null block for the indexed columns.
# Block 1: low value range.
# Block 2: high value range (boundary values 20 / 50 shared with ranges).
# Block 3: mixed nulls and a different label value domain.
BLOCK_RECORDS = [
    [
        {"id": 1, "score": None, "label": None},
        {"id": 2, "score": None, "label": None},
    ],
    [
        {"id": 3, "score": 0, "label": "a"},
        {"id": 4, "score": 10, "label": "b"},
        {"id": 5, "score": 19, "label": "m"},
    ],
    [
        {"id": 6, "score": 20, "label": "n"},
        {"id": 7, "score": 35, "label": "p"},
        {"id": 8, "score": 50, "label": "z"},
    ],
    [
        {"id": 9, "score": None, "label": "q"},
        {"id": 10, "score": 60, "label": None},
        {"id": 11, "score": 70, "label": "r"},
    ],
]

ALL_RECORDS = [record for block in BLOCK_RECORDS for record in block]


def write_multiblock_avro(path, schema=SCHEMA, block_records=None, codec="null"):
    """Write one OCF block per nested list in ``block_records``."""
    block_records = BLOCK_RECORDS if block_records is None else block_records
    with open(path, "wb") as handle:
        writer = Writer(handle, schema, sync_interval=NO_AUTO_FLUSH, codec=codec)
        for group in block_records:
            for record in group:
                writer.write(record)
            writer.dump()
        writer.flush()


def all_records(path):
    with open(path, "rb") as handle:
        return list(fastavro.reader(handle))


def load_sidecar(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_sidecar(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def expected_matching(query):
    return [record for record in ALL_RECORDS if query.evaluate(record)]


def count_blocks(path):
    with open(path, "rb") as handle:
        return len(list(fastavro.block_reader(handle)))


def file_size(path):
    return os.path.getsize(path)
