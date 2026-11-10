"""Static Avro schema compatibility checks.

Decide whether a reader schema can read all data a writer schema can
produce, without needing any sample data files. The two directions of a
schema evolution are checked independently::

    from fastavro.compatibility import check_reader_can_read, check_schema_pair

    result = check_reader_can_read(old_schema, new_schema)
    if not result.compatible:
        print(result.status)     # "incompatible" or "unsupported"
        print(result.paths)      # minimal relevant field paths
        print(result.messages)   # human readable reasons

    both = check_schema_pair(old_schema, new_schema)
    both.new_reader_reads_old_data   # writer -> reader direction
    both.old_reader_reads_new_data   # reverse direction
"""

from ._compatibility_py import (
    COMPATIBLE,
    INCOMPATIBLE,
    UNSUPPORTED,
    KNOWN_LOGICAL_TYPES,
    CompatibilityIssue,
    SchemaCompatibilityResult,
    SchemaPairCompatibility,
    check_reader_can_read,
    check_writer_can_write,
    check_schema_pair,
)

__all__ = [
    "COMPATIBLE",
    "INCOMPATIBLE",
    "UNSUPPORTED",
    "KNOWN_LOGICAL_TYPES",
    "CompatibilityIssue",
    "SchemaCompatibilityResult",
    "SchemaPairCompatibility",
    "check_reader_can_read",
    "check_writer_can_write",
    "check_schema_pair",
]
