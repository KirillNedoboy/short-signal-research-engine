"""Bounded outcome scheduling outside the live scan consumers."""

from __future__ import annotations

import asyncio

from .outcome_worker import OutcomeWorker


class OutcomeScheduler:
    def __init__(self, *, max_concurrency: int = 1) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self.is_background = True
        self.live_slots_reserved = 1

    async def run(self, job):
        async with self._semaphore:
            return await job()


# ``OutcomeScheduler`` is retained as the old semaphore adapter; the real
# bounded lifecycle worker is exported under its explicit name.
__all__ = ["OutcomeScheduler", "OutcomeWorker"]

