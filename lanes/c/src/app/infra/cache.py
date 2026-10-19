"""Small in-memory cache helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Generic, TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class _CacheEntry(Generic[T]):
    value: T
    expires_at: datetime | None = None


class TTLCache(Generic[T]):
    """Tiny TTL cache for dedupe and snapshot storage."""

    def __init__(self) -> None:
        self._store: dict[str, _CacheEntry[T]] = {}

    def set(self, key: str, value: T, ttl_seconds: int | None = None) -> None:
        expires_at = None
        if ttl_seconds is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        self._store[key] = _CacheEntry(value=value, expires_at=expires_at)

    def get(self, key: str) -> T | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.expires_at and datetime.now(timezone.utc) >= entry.expires_at:
            self._store.pop(key, None)
            return None
        return entry.value

    def pop(self, key: str) -> T | None:
        entry = self._store.pop(key, None)
        return None if entry is None else entry.value

    def items(self) -> dict[str, T]:
        result: dict[str, T] = {}
        for key in list(self._store):
            value = self.get(key)
            if value is not None:
                result[key] = value
        return result
