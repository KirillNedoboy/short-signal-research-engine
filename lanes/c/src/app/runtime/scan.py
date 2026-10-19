"""Small C facade over the selected A priority scan coordinator.

The adapter keeps C's launcher-facing names while delegating queue ordering,
deadline accounting, duplicate terminal handling, and window evidence to the
reviewed A runtime implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .scan_coordinator import (
    ScanCoordinator as _ScanCoordinator,
    ScanPriority,
    ScanTask as _ScanTask,
    ScanTerminalStatus as TerminalStatus,
)


@dataclass(frozen=True, slots=True)
class ScanTask:
    task_id: str
    window_id: str
    symbol: str
    priority: ScanPriority
    snapshot_asof: datetime
    deadline: float


class ScanWindowCoordinator:
    """C naming facade with A terminal-accounting semantics."""

    def __init__(self, *, p0_deadline_sec: float = 15.0, p1_deadline_sec: float = 30.0) -> None:
        self._delegate = _ScanCoordinator(
            p0_deadline_sec=p0_deadline_sec,
            p1_deadline_sec=p1_deadline_sec,
        )
        self._tasks: dict[str, _ScanTask] = {}

    def open(
        self,
        window_id: str,
        snapshot_asof: datetime,
        *,
        active: tuple[str, ...],
        shortlist: tuple[str, ...],
    ) -> int:
        return self._delegate.open_window(
            window_id,
            snapshot_asof,
            active_symbols=active,
            shortlist_symbols=shortlist,
        )

    async def acquire(self) -> ScanTask | None:
        task = await self._delegate.acquire()
        if task is None:
            return None
        self._tasks[task.task_id] = task
        return ScanTask(
            task_id=task.task_id,
            window_id=task.window_id,
            symbol=task.symbol,
            priority=task.priority,
            snapshot_asof=task.snapshot_as_of,
            deadline=task.deadline_monotonic,
        )

    def complete(self, task: ScanTask, status: TerminalStatus) -> bool:
        source = self._tasks.pop(task.task_id, None)
        if source is None:
            return False
        return self._delegate.record_terminal(source, status)

    def complete_for(self, window_id: str) -> bool:
        return self._delegate.is_window_complete(window_id)

    def status_counts(self) -> dict[str, int]:
        return self._delegate.status_counts()

    async def stop(self) -> None:
        await self._delegate.stop()


__all__ = ["ScanPriority", "ScanTask", "ScanWindowCoordinator", "TerminalStatus"]

