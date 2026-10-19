"""Error throttling helpers."""

from __future__ import annotations

from app.infra.cache import TTLCache


class ErrorThrottler:
    """Deduplicate repeated error alerts for a fixed TTL."""

    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = ttl_seconds
        self._cache: TTLCache[bool] = TTLCache()

    def should_send(self, key: str) -> bool:
        """Return True when the alert should be emitted."""

        if self._cache.get(key):
            return False
        self._cache.set(key, True, ttl_seconds=self._ttl_seconds)
        return True
