"""Bounded outcome maturation independent of live scan and its HTTP slots."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

_BACKOFF_SEC = (2, 5, 10, 30, 60)


class OutcomeWorker:
    """Bounded, due-aware loop that owns all outcome persistence work."""

    def __init__(
        self,
        *,
        shadow_scheduler: Any,
        outcome_tracker: Any,
        signal_batch_size: int = 25,
        observation_batch_size: int = 25,
        idle_poll_sec: float = 5.0,
        clock: Callable[[], datetime] | None = None,
        fetcher: Any = None,
        include_shadow_v2: bool = False,
    ) -> None:
        if signal_batch_size < 1 or observation_batch_size < 1:
            raise ValueError("outcome batch sizes must be positive")
        self._shadow_scheduler = shadow_scheduler
        self._outcome_tracker = outcome_tracker
        self._signal_batch_size = int(signal_batch_size)
        self._observation_batch_size = int(observation_batch_size)
        self._idle_poll_sec = max(1.0, float(idle_poll_sec))
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._fetcher = fetcher
        self._include_shadow_v2 = include_shadow_v2
        self._tracker_first = False
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()
        self._stopping = False
        self._heartbeat_at: datetime | None = None
        self._last_duration_ms = 0.0
        self._processed_total = 0
        self._error_total = 0
        self._retry_total = 0
        self._oldest_overdue_sec = 0.0
        self._due_total = 0
        self._backlog: dict[str, float | int] = {}

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="outcome-worker")

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def wake(self) -> None:
        self._wake.set()

    def stats(self) -> dict[str, Any]:
        return {
            "heartbeat_at": (
                self._heartbeat_at.isoformat() if self._heartbeat_at else None
            ),
            "due_total": self._due_total,
            "processed_total": self._processed_total,
            "oldest_overdue_sec": round(self._oldest_overdue_sec, 1),
            "retries": self._retry_total,
            "errors": self._error_total,
            "last_duration_ms": round(self._last_duration_ms, 1),
            "rate_limit": dict(self._fetcher.metrics) if self._fetcher is not None else {},
            "cooldown_remaining_sec": self._fetcher.cooldown_remaining if self._fetcher is not None else 0,
            "backlog": dict(self._backlog),
        }

    async def _run(self) -> None:
        consecutive_failures = 0
        while not self._stopping:
            if self._fetcher is not None and self._fetcher.cooldown_remaining:
                await asyncio.sleep(self._fetcher.cooldown_remaining)
            iteration_started = time.monotonic()
            try:
                await self._run_iteration()
                consecutive_failures = 0
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - worker must survive errors
                self._error_total += 1
                consecutive_failures += 1
                LOGGER.warning(
                    "outcome worker iteration failed | error=%s",
                    consecutive_failures,
                    exc_info=True,
                )
            self._heartbeat_at = self._clock()
            self._last_duration_ms = (time.monotonic() - iteration_started) * 1000.0
            if consecutive_failures:
                delay = _BACKOFF_SEC[min(consecutive_failures - 1, len(_BACKOFF_SEC) - 1)]
            elif self._due_total:
                delay = min(0.5, self._idle_poll_sec)
            else:
                delay = self._idle_poll_sec
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _run_iteration(self) -> None:
        with self._fetcher.cycle() if self._fetcher is not None else nullcontext():
            await self._process_iteration()

    async def _process_iteration(self) -> None:
        now = self._clock()
        self._tracker_first = not self._tracker_first
        async def update_tracker():
            return await self._outcome_tracker.update_due_outcomes(
                now=now, signal_limit=self._signal_batch_size,
                observation_limit=self._observation_batch_size,
            )
        if self._tracker_first:
            updated = await update_tracker()
            shadow_stats = await self._shadow_scheduler.run_cycle(now=now)
        else:
            shadow_stats = await self._shadow_scheduler.run_cycle(now=now)
            updated = await update_tracker()
        remaining_due = int(getattr(shadow_stats, "remaining_due", 0) or 0)
        backlog = (
            self._fetcher.repository.outcome_tracker_backlog(
                now=now, include_shadow_v2=self._include_shadow_v2
            )
            if self._fetcher is not None else None
        )
        self._backlog = dict(backlog or {})
        if backlog is not None:
            remaining_due = int(backlog["due_total"])
        self._processed_total += (
            int(getattr(shadow_stats, "runtime_processed", 0))
            + int(getattr(shadow_stats, "legacy_processed", 0))
            + int(getattr(shadow_stats, "v2_processed", 0))
            + int(getattr(shadow_stats, "live_root_processed", 0))
            + int(updated or 0)
        )
        self._retry_total += int(getattr(shadow_stats, "retries", 0))
        self._error_total += int(getattr(shadow_stats, "errors", 0))
        self._oldest_overdue_sec = (
            float(backlog["oldest_overdue_sec"]) if backlog is not None
            else float(getattr(shadow_stats, "oldest_overdue_seconds", 0.0) or 0.0)
        ) if remaining_due else 0.0
        self._due_total = remaining_due
