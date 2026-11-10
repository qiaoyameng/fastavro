from typing import Any

LEGACY: str
EXACT: str

class LogicalResolutionError(Exception): ...

def resolve_logical_value(
    raw: Any,
    writer_schema: Any,
    reader_schema: Any,
    path: str,
) -> Any: ...

def resolve_logical_default(
    default: Any,
    reader_schema: Any,
    path: str,
) -> Any: ...
