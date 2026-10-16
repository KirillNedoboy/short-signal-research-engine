"""Shared request scheduling and retry logic."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, TypeVar

from app.infra.rate_limiter import AsyncRateLimiter


T = TypeVar("T")


class RequestScheduler:
    """Concurrency-limited request scheduler for exchange and Telegram APIs."""

    def __init__(
        self,
        max_concurrency: int,
        jitter_min_ms: int = 100,
        jitter_max_ms: int = 300,
        min_delay_ms: int = 350,
    ) -> None:
        self._min_delay_ms = min_delay_ms
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._rate_limiter = AsyncRateLimiter(
            min_delay_ms=min_delay_ms,
            jitter_min_ms=jitter_min_ms,
            jitter_max_ms=jitter_max_ms,
        )

    async def schedule(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a sync function on a worker thread with retries."""

        async with self._semaphore:
            return await self._run_with_retry(func, *args, **kwargs)

    @property
    def min_delay_ms(self) -> int:
        """Return configured minimum spacing between scheduled requests."""

        return self._min_delay_ms

    async def _run_with_retry(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Run a sync provider call with bounded, rate-limited retries."""
        for attempt in range(3):
            await self._rate_limiter.acquire()
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            except Exception:
                if attempt == 2:
                    raise
                await asyncio.sleep(min(0.5 * (2**attempt), 4.0))
        raise RuntimeError("request retry loop exited unexpectedly")
