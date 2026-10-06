"""Async rate limiter primitives."""

from __future__ import annotations

import asyncio
import random
from time import monotonic


class AsyncRateLimiter:
    """Simple minimum-delay limiter with optional jitter."""

    def __init__(self, min_delay_ms: int = 100, jitter_min_ms: int = 0, jitter_max_ms: int = 0) -> None:
        self._min_delay = min_delay_ms / 1000
        self._jitter_min = jitter_min_ms / 1000
        self._jitter_max = jitter_max_ms / 1000
        self._lock = asyncio.Lock()
        self._last_called = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = monotonic()
            sleep_for = max(0.0, (self._last_called + self._min_delay) - now)
            if self._jitter_max > 0:
                sleep_for += random.uniform(self._jitter_min, self._jitter_max)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            self._last_called = monotonic()
