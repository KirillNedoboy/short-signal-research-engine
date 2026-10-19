"""Delivery wake-up boundary; network delivery is deliberately external."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from .contracts import SnapshotQuality
from .delivery_worker import DeliveryWorker as DurableDeliveryWorker


@dataclass(frozen=True, slots=True)
class PersistedDecision:
    payload: Any
    actionable: bool
    data_quality: str


@dataclass(frozen=True, slots=True)
class DeliveryStats:
    persisted: int = 0
    queued: int = 0
    sent: int = 0
    dropped: int = 0


class DeliveryWorker:
    """Wake a configured delivery adapter after durable persistence.

    The worker has no Telegram dependency and never performs a signal decision.
    A production adapter may enqueue a durable outbox ID; this class only
    preserves the separation and bounded queue semantics.
    """

    def __init__(self, *, enqueue: Callable[[Any], Any | Awaitable[Any]], poll_sec: float = 1.0, maxsize: int = 256) -> None:
        self._enqueue = enqueue
        self._poll_sec = max(0.001, float(poll_sec))
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=maxsize)
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._stats = DeliveryStats()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="lane-c-delivery-wake")

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def enqueue_after_persist(self, item: Any) -> bool:
        if self._stopping:
            return False
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._stats = DeliveryStats(self._stats.persisted, self._stats.queued, self._stats.sent, self._stats.dropped + 1)
            return False
        self._stats = DeliveryStats(self._stats.persisted + 1, self._stats.queued + 1, self._stats.sent, self._stats.dropped)
        self._wake.set()
        return True

    async def persist_decision(self, payload: Any, *, data_quality: str) -> PersistedDecision:
        quality = str(data_quality).upper()
        actionable = quality in {SnapshotQuality.COMPLETE.value, "VALID", "OK"}
        result = PersistedDecision(payload=payload, actionable=actionable, data_quality=quality)
        if actionable:
            await self.enqueue_after_persist(payload)
        return result

    def stats(self) -> DeliveryStats:
        return self._stats

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_sec)
            except asyncio.TimeoutError:
                continue
            self._wake.clear()
            while not self._queue.empty():
                item = await self._queue.get()
                try:
                    result = self._enqueue(item)
                    if asyncio.iscoroutine(result):
                        await result
                finally:
                    self._queue.task_done()
                    self._stats = DeliveryStats(self._stats.persisted, max(0, self._stats.queued - 1), self._stats.sent, self._stats.dropped)


# ``DeliveryWorker`` above remains the old wake-only adapter for callers that
# still enqueue a persisted ID.  New composition roots must import the
# explicitly named durable worker so a review cannot confuse wake and send
# responsibilities.
__all__ = [
    "DeliveryStats",
    "DeliveryWorker",
    "DurableDeliveryWorker",
    "PersistedDecision",
]

