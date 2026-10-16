from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.runtime.scan_coordinator import (
    ScanCoordinator,
    ScanPriority,
    ScanTask,
    ScanTerminalStatus,
)

_AS_OF = datetime(2026, 9, 10, 12, 1, tzinfo=UTC)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _coordinator(**overrides) -> ScanCoordinator:
    kwargs = {"p0_deadline_sec": 15.0, "p1_deadline_sec": 30.0}
    kwargs.update(overrides)
    return ScanCoordinator(**kwargs)


def test_terminal_statuses_are_exact_closed_set() -> None:
    assert {status.value for status in ScanTerminalStatus} == {
        "SCANNED_OK",
        "NO_SETUP",
        "INCOMPLETE_DATA",
        "STALE_DATA",
        "RATE_LIMITED",
        "DEADLINE_EXCEEDED",
        "SCAN_ERROR",
    }


def test_scan_task_fields_are_exact() -> None:
    task = ScanTask(
        task_id="w:A",
        window_id="w",
        symbol="A",
        priority=ScanPriority.P1,
        snapshot_as_of=_AS_OF,
        deadline_monotonic=1.0,
    )
    assert {
        "task_id",
        "window_id",
        "symbol",
        "priority",
        "snapshot_as_of",
        "deadline_monotonic",
        "attempt",
    } == set(task.__dataclass_fields__)


def test_100_symbol_window_produces_exactly_100_terminal_results() -> None:
    coordinator = _coordinator()
    shortlist = tuple(f"SYM{index:03d}USDT" for index in range(100))
    count = coordinator.open_window(
        "w1", _AS_OF, active_symbols=(), shortlist_symbols=shortlist
    )
    assert count == 100

    async def run() -> None:
        for _ in range(100):
            task = await coordinator.acquire()
            assert task is not None
            coordinator.record_terminal(task, ScanTerminalStatus.SCANNED_OK)
        assert coordinator.is_window_complete("w1")

    asyncio.run(run())
    assert coordinator.status_counts() == {"SCANNED_OK": 100}
    assert list(coordinator.missing_tasks("w1")) == []


def test_duplicate_active_and_shortlist_keys_are_coalesced_once() -> None:
    coordinator = _coordinator()
    count = coordinator.open_window(
        "w1",
        _AS_OF,
        active_symbols=("ACTUSDT",),
        shortlist_symbols=("ACTUSDT", "BBBUSDT"),
    )
    # ACTUSDT is scheduled exactly once, as P0.
    assert count == 2
    assert coordinator.scheduled_count("w1") == 2
    assert len(coordinator._queues[ScanPriority.P0]) == 1
    assert len(coordinator._queues[ScanPriority.P1]) == 1
    assert coordinator._coalesced_notifications == 1


def test_p0_always_dequeued_before_queued_p1() -> None:
    coordinator = _coordinator()
    coordinator.open_window(
        "w1",
        _AS_OF,
        active_symbols=(),
        shortlist_symbols=("P1USDT", "P1BUSDT"),
    )
    coordinator.open_window(
        "w2", _AS_OF, active_symbols=("ACTUSDT",), shortlist_symbols=()
    )

    async def run() -> list[ScanPriority]:
        priorities = []
        for _ in range(3):
            task = await coordinator.acquire()
            assert task is not None
            priorities.append(task.priority)
            coordinator.record_terminal(task, ScanTerminalStatus.SCANNED_OK)
        return priorities

    priorities = asyncio.run(run())
    assert priorities == [
        ScanPriority.P0,
        ScanPriority.P1,
        ScanPriority.P1,
    ]


def test_expired_tasks_finalize_deadline_exceeded_not_no_setup() -> None:
    clock = _Clock()
    coordinator = _coordinator(p0_deadline_sec=15.0, p1_deadline_sec=30.0, clock=clock)
    coordinator.open_window(
        "w1", _AS_OF, active_symbols=("ACTUSDT",), shortlist_symbols=("P1USDT",)
    )
    clock.now += 16  # the P0 deadline passed while it sat queued

    async def run() -> ScanTask:
        task = await coordinator.acquire()  # P0 expires -> drained internally
        assert task is not None and task.symbol == "P1USDT"
        return task

    survivor = asyncio.run(run())
    statuses = coordinator.terminal_statuses("w1")
    assert statuses["ACTUSDT"] is ScanTerminalStatus.DEADLINE_EXCEEDED
    assert coordinator.p0_deadline_misses == 1
    coordinator.record_terminal(survivor, ScanTerminalStatus.NO_SETUP)
    assert coordinator.is_window_complete("w1")


def test_duplicate_terminal_record_is_rejected() -> None:
    coordinator = _coordinator()
    coordinator.open_window(
        "w1", _AS_OF, active_symbols=(), shortlist_symbols=("AUSDT",)
    )

    async def run() -> ScanTask:
        task = await coordinator.acquire()
        assert task is not None
        return task

    task = asyncio.run(run())
    assert coordinator.record_terminal(task, ScanTerminalStatus.SCANNED_OK) is True
    assert coordinator.record_terminal(task, ScanTerminalStatus.NO_SETUP) is False
    assert coordinator.status_counts() == {"SCANNED_OK": 1}


def test_stale_task_terminal_status_is_distinct_from_no_setup() -> None:
    # The coordinator never classifies data; it only accounts statuses the
    # runtime records.  STALE_DATA must be an admissible terminal status that
    # still completes a window.
    coordinator = _coordinator()
    coordinator.open_window(
        "w1", _AS_OF, active_symbols=(), shortlist_symbols=("AUSDT", "BUSDT")
    )

    async def run() -> None:
        first = await coordinator.acquire()
        second = await coordinator.acquire()
        assert first is not None and second is not None
        coordinator.record_terminal(first, ScanTerminalStatus.STALE_DATA)
        coordinator.record_terminal(second, ScanTerminalStatus.NO_SETUP)

    asyncio.run(run())
    assert coordinator.is_window_complete("w1")
    statuses = coordinator.terminal_statuses("w1")
    assert statuses["AUSDT"] is not statuses["BUSDT"]


def test_wait_window_complete_resolves_after_last_terminal() -> None:
    coordinator = _coordinator()
    coordinator.open_window(
        "w1", _AS_OF, active_symbols=(), shortlist_symbols=("AUSDT", "BUSDT")
    )

    async def run() -> None:
        waiter = asyncio.create_task(coordinator.wait_window_complete("w1"))
        first = await coordinator.acquire()
        second = await coordinator.acquire()
        assert first is not None and second is not None
        coordinator.record_terminal(first, ScanTerminalStatus.SCANNED_OK)
        assert not waiter.done()
        coordinator.record_terminal(second, ScanTerminalStatus.SCANNED_OK)
        await asyncio.wait_for(waiter, timeout=1)

    asyncio.run(run())


def test_explicit_expiry_pass_finalizes_queued_work() -> None:
    clock = _Clock()
    coordinator = _coordinator(clock=clock)
    coordinator.open_window(
        "w1", _AS_OF, active_symbols=(), shortlist_symbols=tuple(f"S{i}USDT" for i in range(5))
    )
    clock.now += 100
    expired = coordinator.expire_pending()
    assert expired == 5
    assert coordinator.is_window_complete("w1")
    assert coordinator.outstanding_count() == 0


def test_reopening_same_window_is_idempotent() -> None:
    coordinator = _coordinator()
    first = coordinator.open_window(
        "w1", _AS_OF, active_symbols=("AUSDT",), shortlist_symbols=("BUSDT",)
    )
    second = coordinator.open_window(
        "w1", _AS_OF, active_symbols=("AUSDT", "CUSDT"), shortlist_symbols=("BUSDT",)
    )
    assert first == second == 2


def test_empty_window_completes_immediately_without_deadlock() -> None:
    """Regression: a window with zero scheduled symbols must not hang.

    ``record_terminal`` sets the completion event only when the terminal set
    covers the scheduled keys; with an empty key set that path never fires.
    ``open_window`` therefore has to resolve the event itself (or every
    ``wait_window_complete`` waiter stalls the scan loop forever).
    """
    coordinator = _coordinator()
    count = coordinator.open_window(
        "w-empty", _AS_OF, active_symbols=(), shortlist_symbols=()
    )
    assert count == 0

    async def run() -> None:
        # Bounded wait: pre-fix code never resolves and asyncio.wait_for
        # raises TimeoutError here, failing the test.
        await asyncio.wait_for(
            coordinator.wait_window_complete("w-empty"), timeout=0.25
        )

    asyncio.run(run())


def test_window_completion_percentiles_published() -> None:
    clock = _Clock()
    coordinator = _coordinator(clock=clock)
    coordinator.open_window(
        "w1", _AS_OF, active_symbols=(), shortlist_symbols=("AUSDT", "BUSDT")
    )

    async def run() -> None:
        first = await coordinator.acquire()
        clock.now += 0.5
        second = await coordinator.acquire()
        assert first is not None and second is not None
        coordinator.record_terminal(first, ScanTerminalStatus.SCANNED_OK)
        clock.now += 1.0
        coordinator.record_terminal(second, ScanTerminalStatus.SCANNED_OK)

    asyncio.run(run())
    stats = coordinator.window_completion_percentiles_ms()
    assert stats["p50"] == pytest.approx(1500.0, abs=5)
    document = coordinator.evidence_document()
    assert document["terminal_status_counts"] == {"SCANNED_OK": 2}
    assert "task_spans_ms" in document
