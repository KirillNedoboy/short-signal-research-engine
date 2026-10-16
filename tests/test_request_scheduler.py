from __future__ import annotations

import asyncio
import threading
import time

import pytest

from app.infra.provider_errors import ProviderRateLimited, RateLimitPolicy
from app.infra.request_scheduler import RequestDeadlineExceeded, RequestPriority, RequestScheduler


def scheduler(**kwargs):
    return RequestScheduler(jitter_min_ms=0, jitter_max_ms=0, min_delay_ms=0,
                            rate_limit_policy=RateLimitPolicy(windows=((0, 0),) * 4), **kwargs)


async def started(event):
    assert await asyncio.to_thread(event.wait, 2)


def test_live_starts_before_queued_fast_and_outcome():
    async def run():
        gate, entered = threading.Event(), threading.Event()
        order = []
        s = scheduler(max_concurrency=1)
        def hold():
            entered.set()
            assert gate.wait(2)
        blocker = asyncio.create_task(s.schedule(hold))
        await started(entered)
        tasks = [asyncio.create_task(s.schedule(lambda name=p.name: order.append(name), priority=p))
                 for p in (RequestPriority.OUTCOME_REPLAY, RequestPriority.FAST_MONITOR, RequestPriority.LIVE_SCAN)]
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(blocker, *tasks)
        assert order == ['LIVE_SCAN', 'FAST_MONITOR', 'OUTCOME_REPLAY']
    asyncio.run(run())


def test_background_and_fast_monitor_cannot_consume_live_reserve():
    async def run():
        gate, entered = threading.Event(), threading.Event()
        s = scheduler(max_concurrency=2)
        def hold():
            entered.set()
            assert gate.wait(2)
        outcome = asyncio.create_task(s.schedule(hold, priority=RequestPriority.OUTCOME_REPLAY))
        await started(entered)
        fast = asyncio.create_task(s.schedule(lambda: 'fast', priority=RequestPriority.FAST_MONITOR))
        try:
            assert await asyncio.wait_for(s.schedule(lambda: 'live'), 1) == 'live'
            assert not fast.done()
        finally:
            gate.set()
        await asyncio.gather(outcome, fast)
    asyncio.run(run())


def test_outcome_rate_limit_does_not_sleep_in_or_block_live_admission():
    async def run():
        s = RequestScheduler(2, 0, 0, 0)
        def limited():
            raise ProviderRateLimited()
        with pytest.raises(ProviderRateLimited):
            await s.schedule(limited, priority=RequestPriority.OUTCOME_REPLAY)
        assert await asyncio.wait_for(s.schedule(lambda: 'live'), 0.5) == 'live'
        metrics = s.stats_snapshot()['metrics']
        assert metrics['bybit_rate_limit_total:OUTCOME_REPLAY'] == 1
        assert metrics.get('live_scan_rate_limit_total', 0) == 0
    asyncio.run(run())


def test_rate_limited_live_retries_have_one_admission_per_send():
    async def run():
        s = scheduler(max_concurrency=2)
        sends = []
        def request():
            sends.append(time.monotonic())
            if len(sends) < 3:
                raise ProviderRateLimited()
            return 'ok'
        assert await s.schedule(request, operation='candles') == 'ok'
        assert len(sends) == 3
        assert s.stats_snapshot()['candles:LIVE_SCAN']['attempts'] == 3
        assert s.stats_snapshot()['metrics']['live_scan_rate_limit_total'] == 2
    asyncio.run(run())


def test_concurrent_outcomes_and_retries_observe_global_start_spacing():
    async def run():
        s = RequestScheduler(4, 0, 0, 20, background_max_requests_per_minute=100)
        times = []
        await asyncio.gather(*(s.schedule(lambda: times.append(time.monotonic()), priority=RequestPriority.OUTCOME_REPLAY) for _ in range(5)))
        assert len(times) == 5
        assert all(b-a >= 0.015 for a, b in zip(times, times[1:]))
    asyncio.run(run())


def test_background_window_is_not_reset_by_new_calls():
    async def run():
        s = scheduler(max_concurrency=2, background_max_requests_per_minute=1)
        await s.schedule(lambda: None, priority=RequestPriority.RESEARCH)
        with pytest.raises(RequestDeadlineExceeded):
            await s.schedule(lambda: pytest.fail('over-budget send'), priority=RequestPriority.OUTCOME_REPLAY,
                             deadline_monotonic=time.monotonic() + 0.02)
        assert await s.schedule(lambda: 'live') == 'live'
    asyncio.run(run())


def test_deadline_bounds_retry_and_discards_expired_ticket():
    async def run():
        s = scheduler(max_concurrency=1)
        calls = []
        def fail():
            calls.append(1)
            raise OSError('temporary')
        with pytest.raises(RequestDeadlineExceeded):
            await s.schedule(fail, deadline_monotonic=time.monotonic() + 0.1)
        assert len(calls) == 1
        with pytest.raises(RequestDeadlineExceeded):
            await s.schedule(lambda: pytest.fail('expired send'), deadline_monotonic=time.monotonic() - 1)
        assert all(v == 0 for v in s.stats_snapshot()['provider_request_queue_depth'].values())
    asyncio.run(run())


def test_cancelled_waiter_does_not_release_running_sync_http_slot():
    async def run():
        s = scheduler(max_concurrency=1)
        gate, entered = threading.Event(), threading.Event()
        def hold():
            entered.set()
            assert gate.wait(2)
        task = asyncio.create_task(s.schedule(hold))
        await started(entered)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        try:
            with pytest.raises(RequestDeadlineExceeded):
                await s.schedule(lambda: pytest.fail('over concurrency'), deadline_monotonic=time.monotonic()+0.02)
        finally:
            gate.set()
        assert await s.schedule(lambda: 'after') == 'after'
    asyncio.run(run())


def test_recovery_bypasses_background_quota_but_uses_global_admission() -> None:
    async def run():
        scheduler_instance = scheduler(max_concurrency=2, background_max_requests_per_minute=1)
        await scheduler_instance.schedule(lambda: None, priority=RequestPriority.RESEARCH)
        assert await scheduler_instance.schedule(
            lambda: "recovered", priority=RequestPriority.RECOVERY, operation="recovered"
        ) == "recovered"
        stats = scheduler_instance.stats_snapshot()
        assert stats["metrics"].get("bybit_rate_limit_total:RECOVERY", 0) == 0
        assert stats["recovered:RECOVERY"]["attempts"] == 1
    asyncio.run(run())


def test_recovery_is_admitted_after_bounded_fast_burst() -> None:
    async def run():
        scheduler_instance = scheduler(max_concurrency=1)
        gate = threading.Event()
        entered = threading.Event()
        order = []

        def blocker():
            entered.set()
            assert gate.wait(2)

        blocker_task = asyncio.create_task(scheduler_instance.schedule(blocker))
        await started(entered)
        tasks = [
            asyncio.create_task(
                scheduler_instance.schedule(
                    lambda index=index: order.append(f"FAST{index}"),
                    priority=RequestPriority.FAST_MONITOR,
                )
            )
            for index in range(9)
        ]
        tasks.append(
            asyncio.create_task(
                scheduler_instance.schedule(
                    lambda: order.append("RECOVERY"),
                    priority=RequestPriority.RECOVERY,
                )
            )
        )
        for _ in range(100):
            if len(scheduler_instance._queue) >= 10:  # noqa: SLF001
                break
            await asyncio.sleep(0.001)
        gate.set()
        await asyncio.gather(blocker_task, *tasks)
        assert order.index("RECOVERY") == 8
    asyncio.run(run())
