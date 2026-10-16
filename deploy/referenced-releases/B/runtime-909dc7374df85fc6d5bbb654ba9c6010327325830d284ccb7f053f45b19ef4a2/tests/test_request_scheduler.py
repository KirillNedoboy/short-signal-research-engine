from __future__ import annotations

import asyncio
import threading
import time

import pytest

from app.infra.request_scheduler import (
    MAX_CONSECUTIVE_P1_BEFORE_P2,
    RequestDeadlineExceeded,
    RequestPriority,
    RequestScheduler,
)


def test_request_scheduler_defaults_to_bybit_safe_min_delay() -> None:
    scheduler = RequestScheduler(max_concurrency=4)

    assert scheduler.min_delay_ms == 350


class _CountingLimiter:
    def __init__(self) -> None:
        self.acquires = 0

    async def acquire(self) -> None:
        self.acquires += 1


def test_retry_reacquires_shared_rate_limiter_before_each_attempt() -> None:
    calls = 0

    def flaky_operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("ErrCode: 10006 Too many visits")
        return "ok"

    async def run() -> tuple[str, int, int]:
        scheduler = RequestScheduler(
            max_concurrency=1,
            min_delay_ms=0,
            jitter_min_ms=0,
            jitter_max_ms=0,
        )
        limiter = _CountingLimiter()
        scheduler._rate_limiter = limiter
        result = await scheduler.schedule(flaky_operation)
        return result, calls, limiter.acquires

    result, operation_calls, limiter_acquires = asyncio.run(run())

    assert result == "ok"
    assert operation_calls == 2
    assert limiter_acquires == 2


def _scheduler(max_concurrency: int = 4) -> RequestScheduler:
    return RequestScheduler(
        max_concurrency=max_concurrency,
        min_delay_ms=0,
        jitter_min_ms=0,
        jitter_max_ms=0,
    )


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("condition not reached")
        await asyncio.sleep(0.005)


def test_priority_values_are_exact() -> None:
    assert (
        RequestPriority.P0.value == 0
        and RequestPriority.P1.value == 1
        and RequestPriority.P2.value == 2
    )


def test_p0_uses_reserved_lane_before_queued_p1_work() -> None:
    async def run() -> list[int]:
        scheduler = _scheduler(max_concurrency=2)  # one reserved P0, one shared
        release = threading.Event()
        order_done: list[int] = []

        def hold() -> None:
            release.wait(5)

        def work(token: int) -> None:
            order_done.append(token)

        # Occupy only the shared lane; the reserved P0 lane stays free.
        shared_blocker = asyncio.create_task(
            scheduler.schedule(hold, priority=RequestPriority.P2, operation="hold")
        )
        await _wait_until(lambda: scheduler._shared_free == 0)
        p1_task = asyncio.create_task(
            scheduler.schedule(work, 2, priority=RequestPriority.P1, operation="p1")
        )
        await asyncio.sleep(0.02)
        p0_task = asyncio.create_task(
            scheduler.schedule(work, 3, priority=RequestPriority.P0, operation="p0")
        )
        # P0 completes through the reserved lane while P1 is still queued.
        await asyncio.wait_for(p0_task, timeout=5)
        assert order_done == [3]
        release.set()
        await asyncio.gather(shared_blocker, p1_task)
        return order_done

    order_done = asyncio.run(run())
    assert order_done == [3, 2]


def test_shared_lanes_bound_p1_runs_before_admitting_p2() -> None:
    async def run() -> list[str]:
        scheduler = _scheduler(max_concurrency=2)  # one reserved P0, one shared
        events: list[str] = []
        release = threading.Event()

        def blocking(name: str) -> str:
            events.append(f"start-{name}")
            release.wait(5)
            return name

        shared_blocker = asyncio.create_task(
            scheduler.schedule(
                lambda: blocking("shared"),
                priority=RequestPriority.P2,
                operation="b0",
            )
        )
        p0_blocker = asyncio.create_task(
            scheduler.schedule(
                lambda: blocking("p0"), priority=RequestPriority.P0, operation="b1"
            )
        )
        await _wait_until(lambda: len(events) == 2)
        queued = [
            asyncio.create_task(
                scheduler.schedule(
                    lambda: events.append("p1"),
                    priority=RequestPriority.P1,
                    operation="p1",
                )
            )
            for _ in range(MAX_CONSECUTIVE_P1_BEFORE_P2)
        ]
        p2 = asyncio.create_task(
            scheduler.schedule(
                lambda: events.append("p2"),
                priority=RequestPriority.P2,
                operation="p2late",
            )
        )
        await asyncio.sleep(0.05)  # let queue state settle
        assert "p2" not in events  # nothing runs while both lanes are held
        release.set()
        await asyncio.gather(shared_blocker, p0_blocker, *queued, p2)
        return events

    events = asyncio.run(run())
    p2_index = events.index("p2")
    p1_indices = [i for i, item in enumerate(events) if item == "p1"]
    assert len(p1_indices) == MAX_CONSECUTIVE_P1_BEFORE_P2
    # The late P2 request is admitted only after the consecutive-P1 budget,
    # so it can never precede a queued P1.
    assert p2_index > max(p1_indices)


def test_deadline_expiry_stops_retry_loop_and_is_typed() -> None:
    async def run() -> None:
        scheduler = _scheduler()
        calls = 0

        def failing() -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("transient")

        deadline = time.monotonic() + 0.3
        with pytest.raises(RequestDeadlineExceeded) as exc_info:
            await scheduler.schedule(
                failing,
                priority=RequestPriority.P0,
                operation="bounded",
                deadline_monotonic=deadline,
            )
        assert exc_info.value.operation == "bounded"
        assert exc_info.value.priority is RequestPriority.P0
        assert calls == 1  # another attempt would not fit the deadline

    asyncio.run(run())


def test_admission_after_deadline_raises_without_running_work() -> None:
    async def run() -> None:
        scheduler = _scheduler()

        def forbidden() -> None:  # pragma: no cover - must never run
            raise AssertionError("must not run")

        with pytest.raises(RequestDeadlineExceeded) as exc_info:
            await scheduler.schedule(
                forbidden,
                priority=RequestPriority.P1,
                operation="late",
                deadline_monotonic=time.monotonic() - 1,
            )
        assert exc_info.value.stage == "admission"

    asyncio.run(run())


def test_queue_wait_expiry_discards_ticket_and_counts() -> None:
    async def run() -> dict:
        scheduler = _scheduler(max_concurrency=2)
        release = threading.Event()

        def hold() -> None:
            release.wait(5)

        blockers = [
            asyncio.create_task(
                scheduler.schedule(hold, priority=p, operation="hold")
            )
            for p in (RequestPriority.P0, RequestPriority.P2)
        ]
        await _wait_until(
            lambda: scheduler._p0_free == 0 and scheduler._shared_free == 0
        )
        with pytest.raises(RequestDeadlineExceeded) as exc_info:
            await scheduler.schedule(
                lambda: None,
                priority=RequestPriority.P1,
                operation="queued",
                deadline_monotonic=time.monotonic() + 0.05,
            )
        assert exc_info.value.stage == "queue"
        release.set()
        await asyncio.gather(*blockers)
        return scheduler.stats_snapshot()

    stats = asyncio.run(run())
    assert stats["queued:P1"]["deadline_expired"] == 1
    assert stats["queued:P1"]["attempts"] == 0


def test_stats_keyed_by_operation_and_priority() -> None:
    async def run() -> dict:
        scheduler = _scheduler()
        await scheduler.schedule(
            lambda: None, priority=RequestPriority.P0, operation="alpha"
        )
        await scheduler.schedule(
            lambda: None, priority=RequestPriority.P1, operation="alpha"
        )
        await scheduler.schedule(
            lambda: None, priority=RequestPriority.P2, operation="beta"
        )
        return scheduler.stats_snapshot()

    stats = asyncio.run(run())
    assert stats["alpha:P0"]["completed"] == 1
    assert stats["alpha:P1"]["completed"] == 1
    assert stats["beta:P2"]["attempts"] == 1
