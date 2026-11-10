# OCF Companion Block Index

Optional sidecar index for block-level pruning of Avro Object Container Files
(OCF), implemented in pure Python on top of `fastavro`. The standard OCF
bytes are never modified: a JSON sidecar is written next to the data file.

## What is indexed

Top-level record fields whose type is a nullable union of `null` with exactly
one of `int`, `long` or `string`, e.g. `["null", "int"]`. For every OCF block
the sidecar stores:

- block `offset` and `size` in the data file,
- `record_count`,
- per-field `min`, `max` and `null_count`.

Non-nullable fields, nested fields and other physical types are not
indexable. Explicitly requesting an unsupported field raises
`IndexBuildError`; querying an unsupported field falls back to a full scan.

## Predicates

Predicates are conjunctions (logical AND) of:

- comparisons `eq`, `ne`, `lt`, `le`, `gt`, `ge` with `int`/`str` literals,
- inclusive ranges via `between(field, low, high)`,
- `is_null` / `isnot_null`.

Comparisons use SQL-style null semantics: `None` never satisfies a
comparison, only an explicit `is_null`.

## Safety model

Statistics can only prove that a block **cannot** match. The pruning test is
conservative:

- comparisons use block `min`/`max` (boundary equality keeps the block),
- `is_null` needs `null_count > 0`, `isnot_null` needs at least one non-null,
- any missing statistic or unknown type keeps the block.

Every candidate block is still fully decoded and each returned record is
re-evaluated by the predicate, so pruning removes I/O/CPU work but never
changes the result set.

## Identity binding and fallbacks

A sidecar is used only when all of the following validate:

1. its embedded SHA-256 checksum matches (truncation/tamper detection),
2. the SHA-256 of the complete OCF file matches `file_hash`,
3. the SHA-256 fingerprint of the file's writer schema matches
   `schema_fingerprint` (canonical form stored alongside),
4. stored blocks line up 1:1 with physical blocks (offset/size/count).

Any failure - missing or corrupt sidecar, another file's statistics, schema
drift, block-layout drift, or an unsupported query - triggers a full scan and
is reported in `ScanResult.fallback_reason`. Incomplete results are never
returned.

## Usage

```python
from ocf_index import Query, build_index, gt, scan

build_index("events.avro")                 # writes events.avro.ocf-index.json
result = scan("events.avro", Query((gt("amount", 15),)))
result.records          # matching records, re-validated per record
result.used_index       # True when pruning was applied
result.decoded_blocks   # blocks actually decoded
result.skipped_blocks   # blocks proven not to match
result.fallback_reason  # None, or the reason a full scan was chosen
```

Run `python examples/ocf_index_demo.py` for an end-to-end comparison of block
counts with and without the index, and `pytest test_ocf_index.py` for the
correctness, boundary and corruption tests.
