"""Bounded in-process queue for noncritical research/coverage payloads.

Root events, event state, admission evaluations, signal rows, provenance,
and the delivery outbox stay synchronous; only research-class payloads
(coverage cycles and similar) flow through this queue so a storage pause can
never block a P0/P1 scan task.  Overflow drops the newest research payload
and increments a counter instead of applying backpressure to the trading
path.  Persistence itself still goes through the existing repository and
research storage router; no second archive format exists here.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from app.runtime.blocking import run_blocking

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ResearchJob:
    name: str
    write: Callable[[], Any | Awaitable[Any]]


class ResearchQueue:
    """One bounded producer/consumer queue drained in batches off the hot path."""

    def __init__(
        self,
        *,
        maxsize: int = 2000,
        batch_size: int = 25,
        on_error: Callable[[Exception, str], None] | None = None,
        shutdown_timeout_sec: float = 5.0,
        shutdown_max_batches: int = 1,
    ) -> None:
        if maxsize < 1 or batch_size < 1:
            raise ValueError("research queue bounds must be positive")
        if shutdown_timeout_sec <= 0 or shutdown_max_batches < 1:
            raise ValueError("research shutdown bounds must be positive")
        self._maxsize = int(maxsize)
        self._batch_size = int(batch_size)
        self._on_error = on_error
        self._shutdown_timeout_sec = float(shutdown_timeout_sec)
        self._shutdown_max_batches = int(shutdown_max_batches)
        self._queue: deque[ResearchJob] = deque()
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._enqueued_total = 0
        self._written_total = 0
        self._dropped_total = 0
        self._errors_total = 0

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="research-queue")

    async def stop(self) -> None:
        """Drain what already fit, then stop; never blocks past one batch set."""
        self._stopping = True
        self._wake.set()
        task, self._task = self._task, None
        if task is None:
            self._drop_remaining("shutdown bound")
            return
        try:
            await asyncio.wait_for(task, timeout=self._shutdown_timeout_sec)
        except asyncio.TimeoutError:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self._drop_remaining("shutdown bound")

    # -------------------------------------------------------------- producer

    def submit(
        self,
        name: str | Callable[[], Any | Awaitable[Any]],
        write: Callable[[], Any | Awaitable[Any]] | None = None,
    ) -> bool:
        """Enqueue one research payload; ``False`` when the queue is full."""
        if write is None and callable(name):
            # Compatibility with the former BoundedResearchQueue one-argument
            # producer API while new callers provide a stable job name.
            write = name
            name = getattr(write, "__name__", "research")
        if write is None:
            raise TypeError("research queue submit requires a write callback")
        name = str(name)
        if self._stopping:
            self._dropped_total += 1
            return False
        if len(self._queue) >= self._maxsize:
            self._dropped_total += 1
            LOGGER.warning(
                "research queue full; dropping payload | name=%s dropped_total=%s",
                name,
                self._dropped_total,
            )
            return False
        self._queue.append(ResearchJob(name=name, write=write))
        self._enqueued_total += 1
        self._wake.set()
        return True

    # -------------------------------------------------------------- consumer

    def stats(self) -> dict[str, int]:
        return {
            "depth": len(self._queue),
            "maxsize": self._maxsize,
            "enqueued_total": self._enqueued_total,
            "written_total": self._written_total,
            "dropped_total": self._dropped_total,
            "errors_total": self._errors_total,
        }

    async def drain_once(self) -> int:
        """Process at most one bounded batch; returns written job count."""
        written = 0
        while self._queue and written < self._batch_size:
            job = self._queue.popleft()
            try:
                if inspect.iscoroutinefunction(job.write):
                    result = job.write()
                else:
                    result = await run_blocking(job.write)
                if inspect.isawaitable(result):
                    await result
                self._written_total += 1
                written += 1
            except Exception as exc:  # noqa: BLE001 - research cannot crash runtime
                self._errors_total += 1
                LOGGER.warning(
                    "research write failed | name=%s error=%s",
                    job.name,
                    type(exc).__name__,
                )
                if self._on_error is not None:
                    callback_result = self._on_error(exc, job.name)
                    if inspect.isawaitable(callback_result):
                        await callback_result
        return written

    async def _run(self) -> None:
        while not self._stopping:
            if not self._queue:
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue
            await self.drain_once()
        # Final bounded drain for already-accepted payloads.  Anything left
        # after the explicit shutdown budget is dropped and counted; process
        # teardown must never wait on an unbounded research backlog.
        for _ in range(self._shutdown_max_batches):
            if not self._queue:
                break
            await self.drain_once()
        self._drop_remaining("shutdown bound")

    def _drop_remaining(self, reason: str) -> None:
        if not self._queue:
            return
        dropped = len(self._queue)
        self._queue.clear()
        self._dropped_total += dropped
        LOGGER.warning(
            "research queue dropped pending jobs | reason=%s dropped=%s total=%s",
            reason,
            dropped,
            self._dropped_total,
        )
