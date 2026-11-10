"""Block-pruning scan on top of a companion index.

The scan has exactly two modes:

* ``indexed``  - a sidecar is present, fully validated against the data file
  and its writer schema, and used to skip blocks that cannot match;
* ``full_scan``- every block is decoded.

In both modes every record that is returned has been re-evaluated against the
predicate, so pruning can only remove work, never results.  Any condition that
makes pruning unsafe (missing/corrupt sidecar, wrong file or schema identity,
misaligned blocks, unsupported field or predicate) triggers a full scan and is
reported through :class:`ScanResult`.
"""

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional, Sequence, Tuple

import fastavro

from .index import (
    BlockEntry,
    Index,
    IndexError_,
    default_sidecar_path,
    hash_file,
    indexable_fields,
    load_index,
    writer_schema_fingerprint,
)
from .predicates import Predicate, Query

#: Fallback reason codes reported on :class:`ScanResult`.
FALLBACK_MISSING_SIDECAR = "missing_sidecar"
FALLBACK_CORRUPT_SIDECAR = "corrupt_sidecar"
FALLBACK_FILE_MISMATCH = "file_identity_mismatch"
FALLBACK_SCHEMA_MISMATCH = "writer_schema_mismatch"
FALLBACK_BLOCK_MISMATCH = "block_layout_mismatch"
FALLBACK_UNSUPPORTED_QUERY = "unsupported_query"
FALLBACK_INDEX_ERROR = "index_error"


@dataclass
class ScanResult:
    """Outcome of a pruning scan."""

    records: List[Mapping[str, Any]] = field(default_factory=list)
    used_index: bool = False
    total_blocks: int = 0
    decoded_blocks: int = 0
    skipped_blocks: int = 0
    total_records: int = 0
    fallback_reason: Optional[str] = None


def scan(
    avro_path: str,
    query: Optional[Any] = None,
    *,
    sidecar_path: Optional[str] = None,
) -> ScanResult:
    """Return records matching ``query`` using block pruning when safe.

    Parameters
    ----------
    avro_path:
        OCF data file.
    query:
        Conjunctive predicate.  ``None`` (or an empty query) means return
        every record.
    sidecar_path:
        Explicit sidecar location.  When ``None`` the conventional
        :func:`default_sidecar_path` location is used; if it is absent the
        scan simply runs without an index.
    """
    query = _coerce_query(query)

    with open(avro_path, "rb") as handle:
        reader = fastavro.reader(handle)
        writer_schema = reader.writer_schema
        canonical, schema_fp = writer_schema_fingerprint(writer_schema)
        handle.seek(0)
        physical_blocks = list(fastavro.block_reader(handle))

    result = ScanResult(total_blocks=len(physical_blocks))

    index = _try_load_index(avro_path, query, writer_schema, canonical, schema_fp,
                            sidecar_path, result)
    if index is None:
        _run_full_scan(physical_blocks, query, result)
        return result

    if not _blocks_align(index.blocks, physical_blocks):
        result.fallback_reason = FALLBACK_BLOCK_MISMATCH
        _run_full_scan(physical_blocks, query, result)
        return result

    result.used_index = True
    for entry, block in zip(index.blocks, physical_blocks):
        stats_for_block = _block_stats_for_query(entry, query)
        if query.can_skip_block(stats_for_block):
            result.skipped_blocks += 1
            continue
        result.decoded_blocks += 1
        for record in block:
            result.total_records += 1
            if query.evaluate(record):
                result.records.append(record)
    return result


def _coerce_query(query: Optional[Any]) -> Query:
    """Accept a Query, a single Predicate or None as a conjunction."""
    if query is None:
        return Query(())
    if isinstance(query, Predicate):
        return Query((query,))
    if isinstance(query, Query):
        return query
    raise TypeError("query must be a Query, Predicate or None")


def _try_load_index(
    avro_path: str,
    query: Query,
    writer_schema: Mapping[str, Any],
    canonical: str,
    schema_fp: str,
    sidecar_path: Optional[str],
    result: ScanResult,
) -> Optional[Index]:
    """Validate every precondition; return None (with a reason) on failure."""
    supported = indexable_fields(writer_schema)
    for name in query.fields:
        if name not in supported:
            result.fallback_reason = FALLBACK_UNSUPPORTED_QUERY
            return None

    path = sidecar_path or default_sidecar_path(avro_path)
    try:
        index = load_index(path)
    except FileNotFoundError:
        # No index exists yet (both default and explicit paths).
        result.fallback_reason = FALLBACK_MISSING_SIDECAR
        return None
    except IndexError_:
        result.fallback_reason = FALLBACK_CORRUPT_SIDECAR
        return None
    except OSError:
        result.fallback_reason = FALLBACK_MISSING_SIDECAR
        return None

    if index.version != 1:
        result.fallback_reason = FALLBACK_CORRUPT_SIDECAR
        return None

    if index.schema_canonical != canonical or index.schema_fingerprint != schema_fp:
        result.fallback_reason = FALLBACK_SCHEMA_MISMATCH
        return None

    # Every queried field must actually be present in this index.
    if not set(query.fields).issubset(index.indexed_fields):
        result.fallback_reason = FALLBACK_UNSUPPORTED_QUERY
        return None

    try:
        current_hash = hash_file(avro_path)
    except OSError:
        result.fallback_reason = FALLBACK_INDEX_ERROR
        return None
    if current_hash != index.file_hash:
        result.fallback_reason = FALLBACK_FILE_MISMATCH
        return None
    return index


def _blocks_align(
    entries: Sequence[BlockEntry], physical_blocks: Sequence[Any]
) -> bool:
    if len(entries) != len(physical_blocks):
        return False
    for entry, block in zip(entries, physical_blocks):
        if (
            entry.offset != block.offset
            or entry.size != block.size
            or entry.record_count != block.num_records
        ):
            return False
    return True


def _block_stats_for_query(
    entry: BlockEntry, query: Query
) -> Mapping[str, Mapping[str, Any]]:
    stats: dict = {}
    for name in query.fields:
        field_stats = entry.fields.get(name)
        if field_stats is None:
            continue
        stats[name] = {
            "record_count": entry.record_count,
            "null_count": field_stats.null_count,
            "min": field_stats.min,
            "max": field_stats.max,
        }
    return stats


def _run_full_scan(
    physical_blocks: Sequence[Any], query: Query, result: ScanResult
) -> None:
    result.used_index = False
    for block in physical_blocks:
        result.decoded_blocks += 1
        for record in block:
            result.total_records += 1
            if query.evaluate(record):
                result.records.append(record)


def explain(avro_path: str, query: Query) -> Tuple[bool, Optional[str]]:
    """Return ``(would_use_index, fallback_reason)`` without collecting rows."""
    with open(avro_path, "rb") as handle:
        reader = fastavro.reader(handle)
        writer_schema = reader.writer_schema
        canonical, schema_fp = writer_schema_fingerprint(writer_schema)
    probe = ScanResult()
    index = _try_load_index(
        avro_path, query, writer_schema, canonical, schema_fp, None, probe
    )
    return index is not None, probe.fallback_reason
