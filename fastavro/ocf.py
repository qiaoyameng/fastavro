"""Merge Avro Object Container Files without recompressing their blocks.

Unlike a record-by-record ``reader`` -> ``writer`` copy, this module only
validates the container framing of each input file and copies each block's
record count together with its already compressed payload byte for byte. The
individual record values inside the blocks are never decoded or validated.

The first version only supports the ``null`` and ``deflate`` codecs and every
input container must use the exact same complete writer schema.
"""

import json
import os
import tempfile
from dataclasses import dataclass
from os import PathLike
from typing import IO, Dict, Iterator, List, Optional, Sequence, Union

from ._read_common import HEADER_SCHEMA, SYNC_SIZE
from .io.binary_decoder import BinaryDecoder
from .io.binary_encoder import BinaryEncoder
from ._read_py import read_data
from .schema import parse_schema
from ._write_py import write_header

__all__ = ["OCFMergeError", "merge", "iter_raw_blocks"]

# Codecs whose on-disk block payload can be copied without re-compression.
SUPPORTED_CODECS = ("null", "deflate")

# Header metadata entries managed by the container format itself.
_RESERVED_METADATA = ("avro.schema", "avro.codec")


class OCFMergeError(ValueError):
    """Raised when Avro OCF containers cannot be merged.

    This covers missing or unreadable headers, unsupported or mismatched
    codecs, incompatible schemas, conflicting user metadata and any framing
    problem such as an illegal block length, a truncated block or a wrong
    sync marker.
    """


@dataclass
class _ContainerInfo:
    schema: object
    codec: str
    user_metadata: Dict[str, bytes]
    sync_marker: bytes


@dataclass
class RawBlock:
    """A single, still compressed Avro block.

    .. attribute:: num_records

        Number of records stored in the block.

    .. attribute:: payload

        The block payload exactly as it appears on disk, i.e. the compressed
        (or, for the ``null`` codec, raw) bytes. It is never decoded.

    .. attribute:: codec

        The codec used when the block was written.
    """

    num_records: int
    payload: bytes
    codec: str


def _read_container_info(fo: IO[bytes]) -> _ContainerInfo:
    """Read the OCF header and return the information needed for merging.

    An empty stream is not the same as a container with a missing header: an
    empty (or header-truncated) stream raises an :class:`OCFMergeError`, while
    a complete header followed by zero blocks is a valid empty container.
    """
    decoder = BinaryDecoder(fo)
    try:
        header = read_data(decoder, HEADER_SCHEMA, {}, None, {})
    except EOFError as exc:
        raise OCFMergeError(
            "cannot read header - input is empty or is not an Avro OCF file"
        ) from exc

    meta = header["meta"]
    raw_schema = meta.get("avro.schema")
    if raw_schema is None:
        raise OCFMergeError("header is missing the avro.schema entry")

    try:
        schema = parse_schema(json.loads(raw_schema), {}, _force=True)
    except (ValueError, TypeError) as exc:
        raise OCFMergeError(f"invalid writer schema in header: {exc}") from exc

    codec_bytes = meta.get("avro.codec")
    codec = codec_bytes.decode() if codec_bytes is not None else "null"

    user_metadata = {
        key: value
        for key, value in meta.items()
        if key not in _RESERVED_METADATA
    }

    return _ContainerInfo(
        schema=schema,
        codec=codec,
        user_metadata=user_metadata,
        sync_marker=header["sync"],
    )


def iter_raw_blocks(fo: IO[bytes]) -> Iterator[RawBlock]:
    """Yield the raw, compressed blocks of an Avro OCF stream.

    The stream is read from its current position, so it must either still be
    positioned at the start of the container or be rewound by the caller. The
    header is read first and then each block is scanned. Only the container
    framing (block count, block size, payload length and sync marker) is
    validated; the payload is never decompressed and no record values are
    decoded.

    Both count encodings allowed by the Avro specification are accepted: a
    positive count and a negative count (whose absolute value is the real
    number of records). Both forms must be followed by an explicit block size,
    which is how fastavro writes containers. The payload is kept verbatim so
    that every compressed byte is preserved on output.
    """
    info = _read_container_info(fo)
    if info.codec not in SUPPORTED_CODECS:
        raise OCFMergeError(
            f"unsupported codec: {info.codec!r}; only {SUPPORTED_CODECS} can "
            "be merged without re-compression"
        )
    yield from _scan_blocks(fo, info.sync_marker, info.codec)


def _scan_blocks(
    fo: IO[bytes], sync_marker: bytes, codec: str
) -> Iterator[RawBlock]:
    """Validate block framing and yield each untouched compressed payload.

    The stream is read from its current position, i.e. immediately after the
    header. Record values are never decoded; only the count, size, payload
    length and sync marker of every block are checked.
    """
    decoder = BinaryDecoder(fo)
    while True:
        try:
            block_count = decoder.read_long()
        except EOFError:
            return

        if block_count == 0:
            raise OCFMergeError("illegal block: block count must not be zero")

        if block_count < 0:
            num_records = -block_count
        else:
            num_records = block_count

        try:
            block_size = decoder.read_long()
        except EOFError as exc:
            raise OCFMergeError("truncated block: missing block size") from exc
        if block_size <= 0:
            raise OCFMergeError(
                f"illegal block size: {block_size} must be positive"
            )

        try:
            payload = decoder.read_fixed(block_size)
        except EOFError as exc:
            raise OCFMergeError(
                "truncated block: payload is shorter than the declared size"
            ) from exc

        marker = fo.read(SYNC_SIZE)
        if len(marker) < SYNC_SIZE:
            raise OCFMergeError("truncated block: missing sync marker")
        if marker != sync_marker:
            raise OCFMergeError("expected sync marker not found")

        yield RawBlock(num_records, payload, codec)


def _check_compatibility(
    containers: Sequence[_ContainerInfo],
) -> _ContainerInfo:
    """Validate schema, codec and user metadata across all containers."""
    base = containers[0]

    for index, container in enumerate(containers[1:], start=1):
        if container.schema != base.schema:
            raise OCFMergeError(
                f"schema of input {index} does not match the complete writer "
                "schema of the first input"
            )
        if container.codec != base.codec:
            raise OCFMergeError(
                f"codec of input {index} is {container.codec!r}, expected "
                f"{base.codec!r}"
            )
        for key, value in container.user_metadata.items():
            existing = base.user_metadata.get(key)
            if existing is not None and existing != value:
                raise OCFMergeError(
                    f"conflicting user metadata for key {key!r} between inputs"
                )
            base.user_metadata.setdefault(key, value)

    if base.codec not in SUPPORTED_CODECS:
        raise OCFMergeError(
            f"unsupported codec: {base.codec!r}; only {SUPPORTED_CODECS} can "
            "be merged without re-compression"
        )

    return base


def _check_metadata_keys(metadata: Optional[Dict[str, str]]) -> None:
    if metadata:
        for key in metadata:
            if key in _RESERVED_METADATA:
                raise OCFMergeError(
                    f"user metadata must not override reserved key {key!r}"
                )


def _build_metadata(
    container: _ContainerInfo,
    metadata: Optional[Dict[str, str]],
) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for key, value in container.user_metadata.items():
        try:
            merged[key] = value.decode()
        except UnicodeDecodeError as exc:
            raise OCFMergeError(
                f"user metadata for key {key!r} is not valid UTF-8"
            ) from exc
    if metadata:
        merged.update(metadata)
    return merged


def _strip_schema_markers(schema: object) -> object:
    if isinstance(schema, dict):
        return {
            key: _strip_schema_markers(value)
            for key, value in schema.items()
            if key not in ("__fastavro_parsed", "__named_schemas")
        }
    if isinstance(schema, list):
        return [_strip_schema_markers(item) for item in schema]
    return schema


def merge(
    inputs: Sequence[Union[str, PathLike, IO[bytes]]],
    output: Union[str, PathLike, IO[bytes]],
    *,
    metadata: Optional[Dict[str, str]] = None,
    sync_marker: Optional[bytes] = None,
) -> None:
    """Merge several Avro OCF files into one container without re-compression.

    Each input block's record count and compressed payload are copied verbatim
    into the output, so the ``null`` and ``deflate`` codecs do not pay for a
    second compression pass. Only the container framing is validated; record
    values are not decoded or checked.

    All inputs must share the exact same complete writer schema (the comparison
    uses fully parsed schemas, not short fingerprints, so differing defaults or
    logical types are rejected) and the same codec. User metadata entries with
    the same key must carry the same value in every input; the optional
    ``metadata`` argument is added on top and may override the merged values.

    When ``output`` is a path, the result is first written to a temporary file
    in the same directory and the destination is only replaced after every
    input has been fully validated and copied, so a failure leaves any existing
    output file untouched. When ``output`` is a file-like object it is written
    directly and the caller is responsible for cleanup on failure. Inputs are
    scanned in a single forward pass, so the input streams do not need to be
    seekable. Each input object must still be positioned at the start of its
    container; pass a separate open file or a rewound stream to merge the same
    data more than once.

    Parameters
    ----------
    inputs
        Paths or binary file-like objects of the Avro OCF containers to merge,
        in the order their records should appear in the output.
    output
        Path or binary file-like object to write the merged container to.
    metadata
        Additional user metadata to include in the output header. The reserved
        ``avro.schema`` and ``avro.codec`` keys may not be used.
    sync_marker
        Optional 16 byte sync marker for the output. When omitted a random one
        is generated. Input sync markers are never reused.
    """
    if not inputs:
        raise OCFMergeError("at least one input container is required")

    if sync_marker is not None and (
        not isinstance(sync_marker, (bytes, bytearray))
        or len(sync_marker) != SYNC_SIZE
    ):
        raise OCFMergeError(f"sync marker must be {SYNC_SIZE} bytes")

    _check_metadata_keys(metadata)

    path_output = isinstance(output, (str, PathLike))
    output_path = os.fspath(output) if path_output else None

    if path_output:
        directory = os.path.dirname(output_path) or "."
        temporary = tempfile.NamedTemporaryFile(
            mode="wb", dir=directory, delete=False
        )
        out: IO[bytes] = temporary
        temporary_path: Optional[str] = temporary.name
    else:
        out = output  # type: ignore[assignment]
        temporary_path = None

    opened_files: List[IO[bytes]] = []
    try:
        readers: List[IO[bytes]] = []
        containers: List[_ContainerInfo] = []

        for item in inputs:
            if isinstance(item, (str, PathLike)):
                try:
                    fo = open(item, "rb")
                except OSError as exc:
                    raise OCFMergeError(
                        f"cannot open input {os.fspath(item)!r}: {exc}"
                    ) from exc
                opened_files.append(fo)
            else:
                fo = item
            readers.append(fo)
            containers.append(_read_container_info(fo))

        base = _check_compatibility(containers)

        output_metadata = _build_metadata(base, metadata)
        chosen_marker = (
            bytes(sync_marker) if sync_marker else os.urandom(SYNC_SIZE)
        )

        encoder = BinaryEncoder(out)
        header_metadata = dict(output_metadata)
        header_metadata["avro.schema"] = json.dumps(
            _strip_schema_markers(base.schema)
        )
        header_metadata["avro.codec"] = base.codec
        write_header(encoder, header_metadata, chosen_marker)

        # Single forward pass: the header of every stream was just consumed,
        # so its blocks follow immediately without any rewinding.
        for container, fo in zip(containers, readers):
            for block in _scan_blocks(
                fo, container.sync_marker, container.codec
            ):
                encoder.write_long(block.num_records)
                encoder.write_long(len(block.payload))
                out.write(block.payload)
                out.write(chosen_marker)

        out.flush()

        if path_output:
            os.replace(temporary_path, output_path)
            temporary_path = None
    finally:
        for fo in opened_files:
            fo.close()
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
