"""Small, dependency-free canonical JSON helpers for replay evidence."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Mapping


def utc_iso_z(value: datetime) -> str:
    """Format a datetime as a UTC ISO-8601 value ending in ``Z``."""

    if value.tzinfo is None:
        raise ValueError("replay timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def canonicalize(value: Any) -> Any:
    """Convert supported values into deterministic JSON-safe values.

    Mappings are sorted, lists retain their order, finite floats are rounded with
    Python's documented ``.15g`` representation, and non-finite numbers fail
    rather than silently changing replay evidence.
    """

    if isinstance(value, Mapping):
        return {str(key): canonicalize(item) for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [canonicalize(item) for item in value]
    if isinstance(value, datetime):
        return utc_iso_z(value)
    if isinstance(value, Enum):
        return canonicalize(value.value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats are not valid replay evidence")
        return float(format(value, ".15g"))
    if value is None or isinstance(value, (bool, int, str)):
        return value
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Encode canonical JSON as sorted, compact UTF-8 bytes."""

    return json.dumps(canonicalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256_canonical(value: Any) -> str:
    """Return the SHA-256 hash of canonical JSON bytes."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()
