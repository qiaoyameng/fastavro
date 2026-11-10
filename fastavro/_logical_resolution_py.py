"""Auditable logical type resolution between a writer and a reader schema.

The default behaviour of fastavro is to ignore the reader schema's logical
type and always convert the raw datum using the writer schema. When
``logical_resolution="exact"`` is requested, the logical type declared on the
reader schema is taken into account and every conversion has to be exact.
"""

from decimal import localcontext

from .const import MLS_PER_SECOND
from ._logical_readers_py import (
    read_timestamp_millis,
    read_timestamp_micros,
    read_local_timestamp_millis,
    read_local_timestamp_micros,
    read_decimal,
)

LEGACY = "legacy"
EXACT = "exact"

_TIMESTAMP_READERS = {
    "long-timestamp-millis": read_timestamp_millis,
    "long-timestamp-micros": read_timestamp_micros,
    "long-local-timestamp-millis": read_local_timestamp_millis,
    "long-local-timestamp-micros": read_local_timestamp_micros,
}

_UTC_TYPES = {
    "long-timestamp-millis",
    "long-timestamp-micros",
}

_LOCAL_TYPES = {
    "long-local-timestamp-millis",
    "long-local-timestamp-micros",
}


class LogicalResolutionError(Exception):
    """Raised when an exact logical type conversion cannot be performed."""


def _logical_key(schema):
    if not isinstance(schema, dict):
        return None
    logical_type = schema.get("logicalType")
    if not logical_type:
        return None
    return f"{schema['type']}-{logical_type}"


def _resolve_timestamp(raw, writer_schema, reader_schema, path):
    writer_key = _logical_key(writer_schema)
    reader_key = _logical_key(reader_schema)

    if reader_key is None:
        # The reader dropped the logical annotation; per the Avro spec the raw
        # long is returned unchanged instead of guessing a conversion.
        return raw

    if reader_key not in _TIMESTAMP_READERS:
        raise LogicalResolutionError(
            f"{path}: cannot resolve logical type {writer_key} "
            f"to {reader_key}"
        )

    if (writer_key in _UTC_TYPES) != (reader_key in _UTC_TYPES):
        raise LogicalResolutionError(
            f"{path}: cannot automatically convert between "
            f"{writer_key} and {reader_key}; UTC and local timestamps "
            "have different meanings"
        )

    if writer_key == reader_key:
        return _TIMESTAMP_READERS[writer_key](
            raw, writer_schema, reader_schema
        )

    writer_is_micros = writer_key.endswith("micros")
    reader_is_micros = reader_key.endswith("micros")

    if writer_is_micros and not reader_is_micros:
        # micros -> millis is only exact when no sub-millisecond data is lost
        if raw % MLS_PER_SECOND != 0:
            raise LogicalResolutionError(
                f"{path}: {writer_key} value {raw} cannot be converted to "
                f"{reader_key} without losing precision"
            )
        return _TIMESTAMP_READERS[reader_key](
            raw // MLS_PER_SECOND, reader_schema, reader_schema
        )

    # millis -> micros always stays exact (1000 microseconds per millisecond)
    return _TIMESTAMP_READERS[reader_key](
        raw * MLS_PER_SECOND, reader_schema, reader_schema
    )


def _unscaled_to_decimal(unscaled, scale, precision):
    with localcontext() as context:
        context.prec = max(precision, 1)
        return context.create_decimal(unscaled).scaleb(-scale, context)


def _resolve_decimal(raw, writer_schema, reader_schema, path):
    writer_key = _logical_key(writer_schema)
    reader_key = _logical_key(reader_schema)

    if reader_key is None:
        # The reader dropped the logical annotation; return the raw bytes.
        return raw

    if reader_key not in ("bytes-decimal", "fixed-decimal"):
        raise LogicalResolutionError(
            f"{path}: cannot resolve logical type {writer_key} "
            f"to {reader_key}"
        )

    writer_scale = writer_schema.get("scale", 0)
    reader_scale = reader_schema.get("scale", 0)
    reader_precision = reader_schema["precision"]

    unscaled = int.from_bytes(raw, byteorder="big", signed=True)

    if writer_scale == reader_scale:
        new_unscaled = unscaled
    elif reader_scale > writer_scale:
        # Increasing the scale has to be an exact multiplication.
        new_unscaled = unscaled * (10 ** (reader_scale - writer_scale))
    else:
        shift = writer_scale - reader_scale
        dropped = unscaled % (10**shift) if unscaled >= 0 else (
            (-unscaled) % (10**shift)
        )
        if dropped != 0:
            raise LogicalResolutionError(
                f"{path}: decimal value cannot be converted from scale "
                f"{writer_scale} to scale {reader_scale} without losing "
                "non-zero digits"
            )
        new_unscaled = unscaled // (10**shift)

    if reader_key == "fixed-decimal":
        size = reader_schema["size"]
        limit = 1 << (8 * size - 1)
        if new_unscaled >= limit or new_unscaled < -limit:
            raise LogicalResolutionError(
                f"{path}: decimal unscaled value {new_unscaled} does not fit "
                f"into fixed size {size} of the reader schema"
            )

    if new_unscaled != 0 and len(str(abs(new_unscaled))) > reader_precision:
        raise LogicalResolutionError(
            f"{path}: decimal value {new_unscaled} exceeds reader precision "
            f"{reader_precision}"
        )

    return _unscaled_to_decimal(new_unscaled, reader_scale, reader_precision)


def resolve_logical_value(raw, writer_schema, reader_schema, path):
    """Convert ``raw`` using the reader logical type in exact mode.

    ``raw`` is the decoded plain Avro datum (an int or a bytes-like object).
    """
    writer_key = _logical_key(writer_schema)

    if writer_key is None:
        # The writer carries no logical type, so there is nothing to convert
        # and the reader logical type must not be guessed.
        return raw

    if reader_schema is None:
        # No reader schema was supplied; keep the writer's own logical type.
        reader_schema = writer_schema

    if writer_key in _TIMESTAMP_READERS:
        return _resolve_timestamp(raw, writer_schema, reader_schema, path)

    if writer_key in ("bytes-decimal", "fixed-decimal"):
        return _resolve_decimal(raw, writer_schema, reader_schema, path)

    raise LogicalResolutionError(
        f"{path}: unknown or unsupported logical type {writer_key}; "
        "no conversion was attempted"
    )


def resolve_logical_default(default, reader_schema, path):
    """Apply the reader logical type to a raw record default value."""
    reader_key = _logical_key(reader_schema)

    if reader_key in _TIMESTAMP_READERS:
        if not isinstance(default, int) or isinstance(default, bool):
            raise LogicalResolutionError(
                f"{path}: default value {default!r} is not an integer "
                f"for logical type {reader_key}"
            )
        return _TIMESTAMP_READERS[reader_key](
            default, reader_schema, reader_schema
        )

    if reader_key in ("bytes-decimal", "fixed-decimal"):
        if isinstance(default, str):
            raw_default = default.encode("iso-8859-1")
        elif isinstance(default, (bytes, bytearray)):
            raw_default = bytes(default)
        else:
            raise LogicalResolutionError(
                f"{path}: default value {default!r} is not bytes "
                f"for logical type {reader_key}"
            )
        return read_decimal(raw_default, reader_schema, reader_schema)

    if reader_key is None:
        return default

    raise LogicalResolutionError(
        f"{path}: unknown or unsupported logical type {reader_key}; "
        "no conversion was attempted"
    )
