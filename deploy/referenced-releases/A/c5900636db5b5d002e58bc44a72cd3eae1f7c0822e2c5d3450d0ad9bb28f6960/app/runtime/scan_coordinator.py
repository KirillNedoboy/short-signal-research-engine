"""Event-driven scan window coordination with explicit terminal accounting.

One coordinator owns minute-window scheduling: persisted active symbols are
P0 tasks, the current top-N shortlist fills P1 tasks, and a window is only
complete when every scheduled ``(window_id, symbol)`` pair has recorded
exactly one terminal status.  Task evaluation itself stays in the runtime;
this module never touches market data, strategy, or storage directly.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum, StrEnum
from typing import Any

LOGGER = logging.getLogger(__name__)

#: Rolling sample window for published completion statistics.
WINDOW_SAMPLE_SIZE = 60

#: Bounded number of recent windows kept for watchdog evidence.
WINDOWS_RETAINED = 8

#: Per-task stage spans published as rolling p50/p95/p99 evidence.
SPAN_METRICS = ("feature_ms", "strategy_ms", "db_write_ms", "outbox_enqueue_ms")


class ScanPriority(IntEnum):
    """Coordinator task priorities; P0 (active events) preempts P1 (scan)."""

    P0 = 0
    P1 = 1


class ScanTerminalStatus(StrEnum):
    """Closed set: exactly one of these ends every scheduled scan task."""

    SCANNED_OK = "SCANNED_OK"
    NO_SETUP = "NO_SETUP"
    INCOMPLETE_DATA = "INCOMPLETE_DATA"
    STALE_DATA = "STALE_DATA"
    RATE_LIMITED = "RATE_LIMITED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    SCAN_ERROR = "SCAN_ERROR"


@dataclass(frozen=True, slots=True)
class ScanTask:
    """One immutable unit of scheduled evaluation work."""

    task_id: str
    window_id: str
    symbol: str
    priority: ScanPriority
    snapshot_as_of: datetime
    deadline_monotonic: float
    attempt: int = 1


def _percentiles(values: list[float]) -> dict[str, float]:
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
    ordered = sorted(values)

    def at(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
        return round(ordered[index], 3)

    return {"p50": at(0.50), "p95": at(0.95), "p99": at(0.99)}


class ScanCoordinator:
    """Priority task queues plus per-window terminal accounting."""

    def __init__(
        self,
        *,
        p0_deadline_sec: float = 15.0,
        p1_deadline_sec: float = 30.0,
        clock: Any = time.monotonic,
    ) -> None:
        if p0_deadline_sec <= 0 or p1_deadline_sec <= 0:
            raise ValueError("scan deadlines must be positive")
        self._p0_deadline_sec = float(p0_deadline_sec)
        self._p1_deadline_sec = float(p1_deadline_sec)
        self._clock = clock
        self._queues: dict[ScanPriority, deque[ScanTask]] = {
            ScanPriority.P0: deque(),
            ScanPriority.P1: deque(),
        }
        self._wake = asyncio.Event()
        self._scheduled: dict[str, set[tuple[str, str]]] = {}
        self._terminal: dict[str, dict[tuple[str, str], ScanTerminalStatus]] = {}
        self._status_counts: Counter[str] = Counter()
        self._enqueued_at: dict[str, float] = {}
        self._queue_waits_ms: dict[str, list[float]] = {}
        self._window_opened_at: dict[str, float] = {}
        self._window_completion_ms: deque[float] = deque(maxlen=WINDOW_SAMPLE_SIZE)
        self._completed_events: dict[str, asyncio.Event] = {}
        self._window_spans: dict[str, dict[str, list[float]]] = {}
        self._span_rings: dict[str, deque[float]] = {}
        self._in_flight: set[str] = set()
        self._p0_deadline_misses = 0
        self._coalesced_notifications = 0
        self._stopping = False

    # ------------------------------------------------------------- scheduling

    def open_window(
        self,
        window_id: str,
        snapshot_as_of: datetime,
        *,
        active_symbols: tuple[str, ...],
        shortlist_symbols: tuple[str, ...],
    ) -> int:
        """Enqueue one P0 per active symbol and one P1 per remaining shortlist symbol.

        Duplicate ``(window_id, symbol)`` keys are coalesced: an active symbol
        is never evaluated twice inside one window.
        """
        if self._stopping:
            return 0
        if window_id in self._scheduled:
            return len(self._scheduled[window_id])
        now = self._clock()
        self._scheduled[window_id] = set()
        self._terminal[window_id] = {}
        self._queue_waits_ms[window_id] = []
        self._window_spans[window_id] = {}
        self._window_opened_at[window_id] = now
        self._completed_events[window_id] = asyncio.Event()
        keys = self._scheduled[window_id]
        count = 0
        for priority, symbols in (
            (ScanPriority.P0, active_symbols),
            (ScanPriority.P1, shortlist_symbols),
        ):
            deadline = now + (
                self._p0_deadline_sec
                if priority is ScanPriority.P0
                else self._p1_deadline_sec
            )
            for symbol in symbols:
                key = (window_id, symbol.upper())
                if key in keys:
                    self._coalesced_notifications += 1
                    continue
                keys.add(key)
                task = ScanTask(
                    task_id=f"{window_id}:{symbol.upper()}",
                    window_id=window_id,
                    symbol=symbol.upper(),
                    priority=priority,
                    snapshot_as_of=snapshot_as_of,
                    deadline_monotonic=deadline,
                )
                self._enqueued_at[task.task_id] = now
                self._queues[priority].append(task)
                count += 1
        self._wake.set()
        LOGGER.info(
            "Scan window opened | window=%s p0=%s p1=%s total=%s",
            window_id,
            len(active_symbols),
            len(shortlist_symbols),
            count,
        )
        if not keys:
            # Zero scheduled symbols: ``record_terminal`` can never fire and
            # the completion event would stay unset, hanging every waiter on
            # ``wait_window_complete`` forever.  Set the event immediately so
            # the scan loop proceeds to the next window instead of stalling.
            self._completed_events[window_id].set()
            LOGGER.warning(
                "Scan window opened empty | window=%s scheduled=0 "
                "(completion event set immediately to prevent deadlock)",
                window_id,
            )
        return count

    async def acquire(self) -> ScanTask | None:
        """Return the next non-expired task (P0 first); ``None`` after stop.

        Expired queued tasks are drained here and marked
        ``DEADLINE_EXCEEDED`` so window accounting can never strand a task.
        """
        while not self._stopping:
            now = self._clock()
            task = self._pop_ready(now)
            if task is not None:
                return task
            if any(self._queues[priority] for priority in ScanPriority):
                # Everything still queued has expired; finalize the stragglers.
                for priority in ScanPriority:
                    queue = self._queues[priority]
                    while queue:
                        self._finalize_expiry(queue.popleft())
                continue
            self._wake.clear()
            await self._wake.wait()
        return None

    def _pop_ready(self, now: float) -> ScanTask | None:
        for priority in (ScanPriority.P0, ScanPriority.P1):
            queue = self._queues[priority]
            while queue:
                task = queue[0]
                if task.deadline_monotonic <= now:
                    queue.popleft()
                    self._finalize_expiry(task)
                    continue
                queue.popleft()
                enqueued = self._enqueued_at.pop(task.task_id, now)
                waits = self._queue_waits_ms.get(task.window_id)
                if waits is not None:
                    waits.append(max(0.0, (now - enqueued) * 1000.0))
                self._in_flight.add(task.task_id)
                return task
        return None

    def _finalize_expiry(self, task: ScanTask) -> None:
        self._enqueued_at.pop(task.task_id, None)
        self.record_terminal(task, ScanTerminalStatus.DEADLINE_EXCEEDED)

    def expire_pending(self, now: float | None = None) -> int:
        """Mark every queued task past its deadline terminal; returns count."""
        current = now if now is not None else self._clock()
        expired = 0
        for priority in ScanPriority:
            queue = self._queues[priority]
            snapshot = list(queue)
            for task in snapshot:
                if task.deadline_monotonic <= current:
                    queue.remove(task)
                    self._finalize_expiry(task)
                    expired += 1
        return expired

    # ------------------------------------------------------------- accounting

    def record_terminal(
        self, task: ScanTask, status: ScanTerminalStatus
    ) -> bool:
        """Record exactly one terminal status per ``(window_id, symbol)``.

        Returns ``False`` when the key was already terminal (duplicate
        completion is coalesced, never double-counted).
        """
        terminal = self._terminal.get(task.window_id)
        if terminal is None:
            return False
        key = (task.window_id, task.symbol)
        if key in terminal:
            return False
        terminal[key] = status
        self._status_counts[str(status)] += 1
        self._enqueued_at.pop(task.task_id, None)
        self._in_flight.discard(task.task_id)
        if status is ScanTerminalStatus.DEADLINE_EXCEEDED and (
            task.priority is ScanPriority.P0
        ):
            self._p0_deadline_misses += 1
        window_keys = self._scheduled.get(task.window_id, set())
        if window_keys and set(terminal) >= window_keys:
            completion_ms = (
                self._clock() - self._window_opened_at[task.window_id]
            ) * 1000.0
            self._window_completion_ms.append(completion_ms)
            event = self._completed_events.get(task.window_id)
            if event is not None:
                event.set()
            LOGGER.info(
                "Scan window complete | window=%s scheduled=%s duration_ms=%.1f statuses=%s",
                task.window_id,
                len(window_keys),
                completion_ms,
                dict(Counter(terminal.values())),
            )
            self._prune_windows()
        return True

    def terminal_statuses(self, window_id: str) -> dict[str, ScanTerminalStatus]:
        """Symbol -> terminal status for one tracked window."""
        terminal = self._terminal.get(window_id, {})
        return {symbol: status for (_window, symbol), status in terminal.items()}

    def record_task_spans(self, task: ScanTask, spans: dict[str, float]) -> None:
        """Attach per-stage millisecond spans to one task's terminal record."""
        ring = self._span_rings
        for name, value_ms in spans.items():
            if name not in SPAN_METRICS:
                continue
            ring.setdefault(name, deque(maxlen=WINDOW_SAMPLE_SIZE * 64)).append(
                max(0.0, float(value_ms))
            )
            per_window = self._window_spans.get(task.window_id)
            if per_window is not None:
                per_window.setdefault(name, []).append(max(0.0, float(value_ms)))

    def span_percentiles_ms(self, name: str) -> dict[str, float]:
        values = self._span_rings.get(name)
        return _percentiles(list(values or ()))

    def _prune_windows(self) -> None:
        for window_id in list(self._scheduled)[:-WINDOWS_RETAINED]:
            self._scheduled.pop(window_id, None)
            self._terminal.pop(window_id, None)
            self._queue_waits_ms.pop(window_id, None)
            self._window_opened_at.pop(window_id, None)
            self._completed_events.pop(window_id, None)
            self._window_spans.pop(window_id, None)

    async def wait_window_complete(self, window_id: str) -> None:
        """Block until every scheduled task in ``window_id`` is terminal."""
        event = self._completed_events.get(window_id)
        if event is None:
            return
        await event.wait()

    def is_window_complete(self, window_id: str) -> bool:
        keys = self._scheduled.get(window_id)
        terminal = self._terminal.get(window_id)
        return bool(keys) and bool(terminal) and set(terminal) >= keys

    def missing_tasks(self, window_id: str) -> tuple[str, ...]:
        keys = self._scheduled.get(window_id, set())
        terminal = self._terminal.get(window_id, {})
        return sorted(
            symbol for window, symbol in keys if (window, symbol) not in terminal
        )

    def scheduled_count(self, window_id: str) -> int:
        return len(self._scheduled.get(window_id, ()))

    def outstanding_count(self) -> int:
        return sum(len(queue) for queue in self._queues.values())

    def status_counts(self) -> dict[str, int]:
        return dict(self._status_counts)

    @property
    def p0_deadline_misses(self) -> int:
        return self._p0_deadline_misses

    def window_completion_percentiles_ms(self) -> dict[str, float]:
        return _percentiles(list(self._window_completion_ms))

    def window_completion_samples(self) -> list[float]:
        return list(self._window_completion_ms)

    def queue_wait_percentiles_ms(self, window_id: str | None = None) -> dict[str, float]:
        if window_id is not None:
            return _percentiles(list(self._queue_waits_ms.get(window_id, ())))
        merged: list[float] = []
        for waits in self._queue_waits_ms.values():
            merged.extend(waits)
        return _percentiles(merged)

    def recent_windows(self) -> tuple[str, ...]:
        return tuple(self._scheduled)

    def evidence_document(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "window_completion_ms": self.window_completion_percentiles_ms(),
            "queue_wait_ms": self.queue_wait_percentiles_ms(),
            "task_spans_ms": {
                name: self.span_percentiles_ms(name) for name in SPAN_METRICS
            },
            "terminal_status_counts": self.status_counts(),
            "p0_deadline_misses": self._p0_deadline_misses,
            "coalesced_notifications": self._coalesced_notifications,
            "outstanding_tasks": self.outstanding_count(),
            "windows_tracked": len(self._scheduled),
        }

    async def stop(self) -> None:
        self._stopping = True
        self._wake.set()

    def start(self) -> None:
        self._stopping = False
