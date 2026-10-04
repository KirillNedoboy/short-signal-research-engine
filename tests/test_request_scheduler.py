from __future__ import annotations

import asyncio

from app.infra.request_scheduler import RequestScheduler


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
