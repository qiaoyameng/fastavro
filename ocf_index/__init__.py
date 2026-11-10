"""Optional companion block index for Avro Object Container Files (OCF).

The package is intentionally read-only with respect to the OCF file: a
sidecar index is written next to the data file and the standard OCF bytes are
never modified.  The index only stores *summary* information (block position,
record count and min/max/null_count for a few top-level fields).  It can
therefore only be used to prove that a block cannot match; every candidate
block is still fully decoded and every returned record is re-validated
against the predicate.  Whenever pruning cannot be proven safe, the reader
falls back to a full scan so results are never incomplete.
"""

from .index import (
    INDEX_VERSION,
    IndexBuildError,
    IndexError_,
    build_index,
    default_sidecar_path,
    load_index,
)
from .predicates import (
    OPS,
    Predicate,
    Query,
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
from .scan import ScanResult, scan

__all__ = [
    "INDEX_VERSION",
    "IndexBuildError",
    "IndexError_",
    "build_index",
    "load_index",
    "default_sidecar_path",
    "OPS",
    "Predicate",
    "Query",
    "between",
    "column",
    "eq",
    "ge",
    "gt",
    "is_null",
    "isnot_null",
    "le",
    "lt",
    "ne",
    "ScanResult",
    "scan",
]
