"""Build, serialise and validate the companion block index.

The index is a JSON sidecar stored next to the OCF file.  It never touches
the OCF bytes themselves.  A sidecar is only considered usable for a given
data file when *all* of the following hold:

* its embedded checksum matches (detects truncation / tampering),
* the SHA-256 of the complete OCF file matches ``file_hash``,
* the SHA-256 fingerprint of the file's writer schema matches
  ``schema_fingerprint`` (the canonical form is stored as well),
* the stored blocks line up 1:1 with the physical blocks in the file
  (offset, size and record count).

If any check fails the caller must fall back to a full scan; a sidecar built
for another file can therefore never be applied silently.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import fastavro
from fastavro.schema import fingerprint, to_parsing_canonical_form

#: Bump when the on-disk sidecar format changes incompatibly.
INDEX_VERSION = 1

_SIDECAR_SUFFIX = ".ocf-index.json"
_HASH_CHUNK = 1 << 23  # 8 MiB
_SUPPORTED_TYPES = frozenset({"int", "long", "string"})


class IndexBuildError(ValueError):
    """Raised when an index cannot be built for the requested fields."""


class IndexError_(ValueError):
    """Raised when a sidecar is corrupt or otherwise unusable."""


def default_sidecar_path(avro_path: str) -> str:
    """Return the conventional sidecar path for an OCF file."""
    return avro_path + _SIDECAR_SUFFIX


@dataclass(frozen=True)
class FieldStats:
    """Min/max/null_count for one field within one block."""

    min: Optional[Any]
    max: Optional[Any]
    null_count: int

    def to_json(self) -> Dict[str, Any]:
        return {"min": self.min, "max": self.max, "null_count": self.null_count}

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "FieldStats":
        if set(data) != {"min", "max", "null_count"}:
            raise IndexError_("invalid field statistics entry")
        if not isinstance(data["null_count"], int) or data["null_count"] < 0:
            raise IndexError_("invalid null_count in index")
        return cls(data["min"], data["max"], data["null_count"])


@dataclass(frozen=True)
class BlockEntry:
    """Location, size and per-field statistics for one OCF block."""

    offset: int
    size: int
    record_count: int
    fields: Mapping[str, FieldStats]

    def to_json(self) -> Dict[str, Any]:
        return {
            "offset": self.offset,
            "size": self.size,
            "record_count": self.record_count,
            "fields": {name: stats.to_json() for name, stats in self.fields.items()},
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> "BlockEntry":
        try:
            offset = data["offset"]
            size = data["size"]
            record_count = data["record_count"]
            raw_fields = data["fields"]
        except (KeyError, TypeError) as exc:
            raise IndexError_("invalid block entry") from exc
        if not (
            isinstance(offset, int)
            and isinstance(size, int)
            and isinstance(record_count, int)
            and offset >= 0
            and size >= 0
            and record_count >= 0
            and isinstance(raw_fields, dict)
        ):
            raise IndexError_("invalid block entry values")
        fields = {
            name: FieldStats.from_json(raw)
            for name, raw in raw_fields.items()
        }
        return cls(offset, size, record_count, fields)


@dataclass(frozen=True)
class Index:
    """A validated-in-shape companion index for one OCF file."""

    version: int
    file_hash: str
    schema_fingerprint: str
    schema_canonical: str
    indexed_fields: Tuple[str, ...]
    blocks: Tuple[BlockEntry, ...]
    checksum: str

    def field_names(self) -> Tuple[str, ...]:
        return self.indexed_fields


def _nullable_underlying_type(field_schema: Any) -> Optional[str]:
    """Return the non-null type name for a nullable union, else None."""
    if not isinstance(field_schema, list):
        return None
    non_null = [member for member in field_schema if member != "null"]
    if len(non_null) != 1:
        return None
    member = non_null[0]
    if isinstance(member, str) and member in _SUPPORTED_TYPES:
        return member
    return None


def indexable_fields(schema: Mapping[str, Any]) -> Dict[str, str]:
    """Map top-level field name -> physical type for indexable fields."""
    result: Dict[str, str] = {}
    if schema.get("type") != "record":
        raise IndexBuildError("top-level schema must be an Avro record")
    for field in schema.get("fields", []):
        physical = _nullable_underlying_type(field.get("type"))
        if physical is not None:
            result[field["name"]] = physical
    return result


def _resolve_fields(
    schema: Mapping[str, Any], requested: Optional[Sequence[str]]
) -> Tuple[str, ...]:
    available = indexable_fields(schema)
    if requested is None:
        return tuple(available)
    chosen: List[str] = []
    for name in requested:
        if name in chosen:
            continue
        if name not in available:
            raise IndexBuildError(
                f"field {name!r} is not indexable; first version only supports "
                "nullable unions of null with int/long/string"
            )
        chosen.append(name)
    return tuple(chosen)


class _HashingReader:
    """File wrapper that hashes every byte read while supporting seeking."""

    def __init__(self, fo: Any) -> None:
        self._fo = fo
        self._hasher = hashlib.sha256()

    def read(self, num_bytes: int = -1) -> bytes:
        chunk = self._fo.read(num_bytes)
        if chunk:
            self._hasher.update(chunk)
        return chunk

    def seek(self, pos: int, whence: int = 0) -> int:
        return self._fo.seek(pos, whence)

    def tell(self) -> int:
        return self._fo.tell()

    @property
    def digest_hex(self) -> str:
        return self._hasher.hexdigest()


def _summarise_block(
    block: Any, fields: Tuple[str, ...]
) -> Dict[str, FieldStats]:
    stats: Dict[str, FieldStats] = {}
    accum: Dict[str, Dict[str, Any]] = {
        name: {"min": None, "max": None, "null_count": 0} for name in fields
    }
    for record in block:
        for name in fields:
            value = record.get(name)
            slot = accum[name]
            if value is None:
                slot["null_count"] += 1
                continue
            if slot["min"] is None or value < slot["min"]:
                slot["min"] = value
            if slot["max"] is None or value > slot["max"]:
                slot["max"] = value
    for name in fields:
        slot = accum[name]
        stats[name] = FieldStats(slot["min"], slot["max"], slot["null_count"])
    return stats


def build_index(
    avro_path: str,
    fields: Optional[Sequence[str]] = None,
    *,
    sidecar_path: Optional[str] = None,
) -> Index:
    """Build a companion index for an OCF file and write its sidecar.

    Parameters
    ----------
    avro_path:
        Path to the standard OCF data file (never modified).
    fields:
        Top-level field names to index.  When ``None`` every indexable
        top-level field is used.  Explicitly requesting an unsupported field
        raises :class:`IndexBuildError`.
    sidecar_path:
        Where to write the JSON sidecar.  Defaults to
        :func:`default_sidecar_path`.
    """
    destination = sidecar_path or default_sidecar_path(avro_path)
    with open(avro_path, "rb") as raw:
        reader = fastavro.reader(raw)
        writer_schema = reader.writer_schema
        canonical = to_parsing_canonical_form(writer_schema)
        schema_fp = fingerprint(canonical, "SHA-256")
        chosen = _resolve_fields(writer_schema, fields)

        raw.seek(0)
        hashing = _HashingReader(raw)
        block_entries: List[BlockEntry] = []
        for block in fastavro.block_reader(hashing):
            field_stats = _summarise_block(block, chosen)
            block_entries.append(
                BlockEntry(
                    offset=block.offset,
                    size=block.size,
                    record_count=block.num_records,
                    fields=field_stats,
                )
            )

        # Reading every block has streamed the complete file through hashing.
        file_hash = hashing.digest_hex

    index = Index(
        version=INDEX_VERSION,
        file_hash=file_hash,
        schema_fingerprint=schema_fp,
        schema_canonical=canonical,
        indexed_fields=chosen,
        blocks=tuple(block_entries),
        checksum="",
    )
    checksum = _compute_checksum(_checksum_payload(index))
    object.__setattr__(index, "checksum", checksum)

    with open(destination, "w", encoding="utf-8") as out:
        json.dump(_index_to_json(index), out, indent=2, sort_keys=True)
        out.write("\n")
    return index


def _checksum_payload(index: Index) -> Dict[str, Any]:
    return {
        "version": index.version,
        "file_hash": index.file_hash,
        "schema_fingerprint": index.schema_fingerprint,
        "schema_canonical": index.schema_canonical,
        "indexed_fields": list(index.indexed_fields),
        "blocks": [block.to_json() for block in index.blocks],
    }


def _compute_checksum(payload: Mapping[str, Any]) -> str:
    serialised = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def _index_to_json(index: Index) -> Dict[str, Any]:
    payload = _checksum_payload(index)
    payload["checksum"] = index.checksum
    return payload


def load_index(sidecar_path: str) -> Index:
    """Load and structurally validate a sidecar, verifying its checksum."""
    # open() errors (notably FileNotFoundError) are deliberately not wrapped
    # so callers can distinguish a missing sidecar from a corrupt one.
    with open(sidecar_path, "r", encoding="utf-8") as handle:
        try:
            data = json.load(handle)
        except json.JSONDecodeError as exc:
            raise IndexError_(f"invalid sidecar JSON: {exc}") from exc
    return index_from_dict(data)


def index_from_dict(data: Mapping[str, Any]) -> Index:
    if not isinstance(data, dict):
        raise IndexError_("sidecar root must be a JSON object")
    required = {
        "version",
        "file_hash",
        "schema_fingerprint",
        "schema_canonical",
        "indexed_fields",
        "blocks",
        "checksum",
    }
    missing = required.difference(data)
    if missing:
        raise IndexError_(f"sidecar missing keys: {sorted(missing)}")

    checksum = data["checksum"]
    if not isinstance(checksum, str):
        raise IndexError_("invalid checksum")
    if data["version"] != INDEX_VERSION:
        raise IndexError_(
            f"unsupported index version {data['version']!r}, "
            f"expected {INDEX_VERSION}"
        )
    for name in ("file_hash", "schema_fingerprint", "schema_canonical"):
        if not isinstance(data[name], str):
            raise IndexError_(f"invalid {name}")
    indexed_fields = data["indexed_fields"]
    if not isinstance(indexed_fields, list) or not all(
        isinstance(name, str) for name in indexed_fields
    ):
        raise IndexError_("invalid indexed_fields")
    raw_blocks = data["blocks"]
    if not isinstance(raw_blocks, list):
        raise IndexError_("invalid blocks list")

    blocks = tuple(BlockEntry.from_json(entry) for entry in raw_blocks)
    index = Index(
        version=data["version"],
        file_hash=data["file_hash"],
        schema_fingerprint=data["schema_fingerprint"],
        schema_canonical=data["schema_canonical"],
        indexed_fields=tuple(indexed_fields),
        blocks=blocks,
        checksum=checksum,
    )
    expected = _compute_checksum(_checksum_payload(index))
    if not _hashes_equal(expected, checksum):
        raise IndexError_("sidecar checksum mismatch; index is corrupt")
    return index


def _hashes_equal(expected: str, actual: str) -> bool:
    # Constant-time comparison; sidecars are local but this keeps tamper
    # handling uniform and avoids leaking through timing.
    if len(expected) != len(actual):
        return False
    result = 0
    for left, right in zip(expected, actual):
        result |= ord(left) ^ ord(right)
    return result == 0


def hash_file(path: str) -> str:
    """Return the SHA-256 hex digest of a complete file."""
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(_HASH_CHUNK)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def writer_schema_fingerprint(schema: Mapping[str, Any]) -> Tuple[str, str]:
    """Return (canonical form, SHA-256 fingerprint) for a writer schema."""
    canonical = to_parsing_canonical_form(schema)
    return canonical, fingerprint(canonical, "SHA-256")
