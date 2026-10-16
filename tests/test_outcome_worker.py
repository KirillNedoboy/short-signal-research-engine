from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace

from app.runtime.outcome_worker import OutcomeWorker


def _stats(**overrides):
    base = dict(
        runtime_due_seen=0,
        runtime_processed=0,
        legacy_due_seen=0,
        legacy_processed=0,
        v2_due_seen=0,
        v2_processed=0,
        v2_errors=0,
        live_root_due_seen=0,
        live_root_processed=0,
        live_root_errors=0,
        episodes_deferred=0,
        symbols_fetched=0,
        bybit_requests=0,
        retries=0,
        errors=0,
        oldest_overdue_seconds=0.0,
        remaining_due=0,
        cycle_duration_ms=0.0,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class _ShadowScheduler:
    def __init__(self, stats=None, error: bool = False) -> None:
        self._stats = stats or _stats()
        self._error = error
        self.calls = 0

    async def run_cycle(self, *, now=None):
        self.calls += 1
        if self._error:
            raise RuntimeError("scheduler exploded")
        return self._stats


class _Tracker:
    def __init__(self, delay: float = 0.0, result: int = 0) -> None:
        self.delay = delay
        self._result = result
        self.calls: list[dict] = []

    async def update_due_outcomes(self, *, now=None, signal_limit=25, observation_limit=25):
        self.calls.append(
            {
                "now": now,
                "signal_limit": signal_limit,
                "observation_limit": observation_limit,
            }
        )
        if self.delay:
            await asyncio.sleep(self.delay)
        return self._result


def _worker(scheduler, tracker, **overrides) -> OutcomeWorker:
    kwargs = dict(
        shadow_scheduler=scheduler,
        outcome_tracker=tracker,
        signal_batch_size=7,
        observation_batch_size=9,
        idle_poll_sec=60.0,  # effectively disable idle wake in tests
    )
    kwargs.update(overrides)
    return OutcomeWorker(**kwargs)


def test_worker_batches_are_bounded_by_configuration() -> None:
    tracker = _Tracker()
    worker = _worker(_ShadowScheduler(), tracker)

    async def run() -> None:
        await worker.start()
        await asyncio.sleep(0.05)
        await worker.stop()

    asyncio.run(run())
    assert tracker.calls
    call = tracker.calls[0]
    assert call["signal_limit"] == 7
    assert call["observation_limit"] == 9


def test_slow_p2_outcome_iteration_does_not_block_scans() -> None:
    tracker = _Tracker(delay=0.2)
    worker = _worker(_ShadowScheduler(), tracker)

    async def run() -> bool:
        scan_ran = False

        async def scan_like_task() -> None:
            nonlocal scan_ran
            await asyncio.sleep(0.05)
            scan_ran = True

        await worker.start()
        scan = asyncio.create_task(scan_like_task())
        await scan  # must finish long before the slow outcome iteration
        finished_first = scan_ran and len(tracker.calls) <= 1
        await worker.stop()
        return finished_first

    assert asyncio.run(run())


def test_exceptions_back_off_without_stopping_the_worker() -> None:
    scheduler = _ShadowScheduler(error=True)
    tracker = _Tracker()
    worker = _worker(scheduler, tracker, idle_poll_sec=0.05)

    async def run() -> dict:
        await worker.start()
        await asyncio.sleep(0.2)
        stats = worker.stats()
        await worker.stop()
        return stats

    stats = asyncio.run(run())
    assert stats["errors"] >= 1  # contained, not fatal
    assert scheduler.calls >= 1


def test_remaining_due_wakes_next_iteration_immediately() -> None:
    scheduler = _ShadowScheduler(stats=_stats(remaining_due=5, oldest_overdue_seconds=12.0))
    worker = _worker(scheduler, _Tracker(result=3), idle_poll_sec=60.0)

    async def run() -> dict:
        await worker.start()
        await asyncio.sleep(0.1)  # 0.5s inter-due delay, so ~1 iteration
        stats = worker.stats()
        await worker.stop()
        return stats

    stats = asyncio.run(run())
    assert stats["due_total"] == 5
    assert stats["processed_total"] == 3
    assert stats["oldest_overdue_sec"] == 12.0


def test_restart_resumes_due_work() -> None:
    tracker = _Tracker(result=1)
    scheduler = _ShadowScheduler(stats=_stats(remaining_due=2))
    worker = _worker(scheduler, tracker, idle_poll_sec=60.0)

    async def run() -> tuple[int, int]:
        await worker.start()
        await asyncio.sleep(0.05)
        first = len(tracker.calls)
        await worker.stop()
        # A fresh worker (simulating process restart) resumes due work.
        worker2 = _worker(
            _ShadowScheduler(stats=_stats(remaining_due=1)),
            tracker,
            idle_poll_sec=60.0,
        )
        await worker2.start()
        await asyncio.sleep(0.05)
        second = len(tracker.calls)
        await worker2.stop()
        return first, second

    first, second = asyncio.run(run())
    assert first >= 1
    assert second > first


class _BlockingBacklogRepository:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.thread_daemon = None

    def outcome_tracker_backlog(self, *, now, include_shadow_v2=False):
        self.thread_daemon = threading.current_thread().daemon
        self.started.set()
        time.sleep(0.25)
        return {"due_total": 0, "oldest_overdue_sec": 0.0}


class _FetcherWithBlockingBacklog:
    def __init__(self, repository) -> None:
        self.repository = repository
        self.metrics = {}
        self.cooldown_remaining = 0.0

    @contextmanager
    def cycle(self):
        yield


def test_blocking_history_read_does_not_freeze_loop_or_worker_stop() -> None:
    repository = _BlockingBacklogRepository()
    worker = _worker(
        _ShadowScheduler(),
        _Tracker(),
        fetcher=_FetcherWithBlockingBacklog(repository),
    )

    async def run() -> tuple[float, float]:
        started_at = time.monotonic()
        await worker.start()
        assert await asyncio.to_thread(repository.started.wait, 1.0)
        event_loop_delay = time.monotonic() - started_at
        stop_started_at = time.monotonic()
        await asyncio.wait_for(worker.stop(), timeout=0.1)
        stop_duration = time.monotonic() - stop_started_at
        return event_loop_delay, stop_duration

    event_loop_delay, stop_duration = asyncio.run(run())
    assert event_loop_delay < 0.15
    assert stop_duration < 0.1
    assert repository.thread_daemon is True


class _BlockingSchedulerRepository:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.thread_daemon = None

    def list_shadow_episode_outcomes_due(self, *, now, runtime_limit, legacy_limit):
        self.thread_daemon = threading.current_thread().daemon
        self.started.set()
        time.sleep(0.25)
        return {"runtime": [], "legacy": []}


def test_scheduler_repository_read_does_not_freeze_event_loop() -> None:
    from app.observability.outcome_scheduler import ShadowOutcomeScheduler

    repository = _BlockingSchedulerRepository()
    scheduler = ShadowOutcomeScheduler(
        client=object(),
        repository=repository,
        v2_enabled=False,
    )
    worker = _worker(scheduler, _Tracker())

    async def run() -> float:
        started_at = time.monotonic()
        await worker.start()
        assert await asyncio.to_thread(repository.started.wait, 1.0)
        event_loop_delay = time.monotonic() - started_at
        await worker.stop()
        return event_loop_delay

    event_loop_delay = asyncio.run(run())
    assert event_loop_delay < 0.15
    assert repository.thread_daemon is True
