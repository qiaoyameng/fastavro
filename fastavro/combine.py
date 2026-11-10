try:
    from . import _combine
except ImportError:
    from . import _combine_py as _combine  # type: ignore

from ._schema_common import SchemaConflictError

combine_schemas = _combine.combine_schemas

__all__ = ["combine_schemas", "SchemaConflictError"]
