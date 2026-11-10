"""End-to-end demonstration of OCF companion block pruning.

Run with::

    python examples/ocf_index_demo.py

The script writes one OCF file with several deliberately separated value
domains (including an all-null block), builds the companion index and then
runs the same query with and without the index, comparing the decoded block
counts while proving that both return exactly the same records.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastavro.write import Writer

from ocf_index import Query, build_index, default_sidecar_path, gt, scan

SCHEMA = {
    "type": "record",
    "name": "Event",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": ["null", "int"]},
        {"name": "bucket", "type": ["null", "string"]},
    ],
}

# One nested list becomes one physical OCF block.
BLOCKS = [
    [{"id": 1, "amount": None, "bucket": None}],
    [{"id": 2, "amount": 0, "bucket": "a"}, {"id": 3, "amount": 9, "bucket": "a"}],
    [{"id": 4, "amount": 10, "bucket": "b"}, {"id": 5, "amount": 19, "bucket": "b"}],
    [{"id": 6, "amount": 20, "bucket": "c"}, {"id": 7, "amount": 99, "bucket": "c"}],
]


def write_file(path):
    with open(path, "wb") as handle:
        writer = Writer(handle, SCHEMA, sync_interval=10**9)
        for group in BLOCKS:
            for record in group:
                writer.write(record)
            writer.dump()
        writer.flush()


def main():
    workdir = tempfile.mkdtemp(prefix="ocf-index-demo-")
    data_path = os.path.join(workdir, "events.avro")
    write_file(data_path)

    query = Query((gt("amount", 15),))

    full = scan(data_path, query)
    print("without index:")
    print(f"  mode={full.used_index!r} reason={full.fallback_reason!r}")
    print(f"  decoded_blocks={full.decoded_blocks}/{full.total_blocks}")
    print(f"  matched_ids={[row['id'] for row in full.records]}")

    index = build_index(data_path)
    print(f"\nbuilt sidecar at {default_sidecar_path(data_path)}")
    print(f"  indexed_fields={index.indexed_fields} blocks={len(index.blocks)}")

    pruned = scan(data_path, query)
    print("\nwith index:")
    print(f"  mode={pruned.used_index!r} reason={pruned.fallback_reason!r}")
    print(
        f"  decoded_blocks={pruned.decoded_blocks}/{pruned.total_blocks} "
        f"skipped={pruned.skipped_blocks}"
    )
    print(f"  matched_ids={[row['id'] for row in pruned.records]}")

    assert full.records == pruned.records
    assert pruned.decoded_blocks < full.decoded_blocks
    print("\nOK: identical records, fewer blocks decoded.")


if __name__ == "__main__":
    main()
