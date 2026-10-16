"""One process-wide Bybit admission budget, with priority at HTTP start."""
from __future__ import annotations

import asyncio
import time
from collections import Counter, deque
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, TypeVar

from app.infra.provider_errors import ProviderRateLimited, RateLimitPolicy
from app.infra.rate_limiter import StartRateLimiter

T = TypeVar("T")
RETRY_ATTEMPT_WINDOW_SEC = 1.0


class RequestPriority(IntEnum):
    LIVE_SCAN = 0
    FAST_MONITOR = 1
    OUTCOME_REPLAY = 2
    RESEARCH = 3


request_class: ContextVar[RequestPriority] = ContextVar("request_class", default=RequestPriority.LIVE_SCAN)
request_deadline: ContextVar[float | None] = ContextVar("request_deadline", default=None)


class RequestDeadlineExceeded(RuntimeError):
    def __init__(self, operation: str, priority: RequestPriority, *, stage: str) -> None:
        self.operation, self.priority, self.stage = operation, priority, stage
        super().__init__(f"request deadline exceeded for {operation} (priority={priority.name}, stage={stage})")


@dataclass(eq=False)
class _Ticket:
    priority: RequestPriority
    operation: str
    placed_at: float


class RequestScheduler:
    """Background never reserves live capacity or a place ahead of live in the limiter."""
    def __init__(self, max_concurrency: int, jitter_min_ms: int = 100,
                 jitter_max_ms: int = 300, min_delay_ms: int = 350, *,
                 outcome_max_concurrency: int = 1, background_max_requests_per_minute: int = 10,
                 rate_limit_policy: RateLimitPolicy | None = None) -> None:
        if max_concurrency < 1 or outcome_max_concurrency < 1 or background_max_requests_per_minute < 1:
            raise ValueError("request budgets must be positive")
        self._max_concurrency = max_concurrency
        self._outcome_capacity = outcome_max_concurrency
        self._background_limit = background_max_requests_per_minute
        self._min_delay_ms = min_delay_ms
        self._limiter = StartRateLimiter(min_delay_ms, jitter_min_ms, jitter_max_ms)
        self._condition = asyncio.Condition()
        self._queue: list[_Ticket] = []
        self._in_flight: Counter[RequestPriority] = Counter()
        self._background_starts: deque[float] = deque()
        self._cooldown: dict[RequestPriority, float] = {}
        self._global_cooldown = 0.0
        self._stats: dict[str, Counter] = {}
        self._metrics: Counter[str] = Counter({
            "bybit_rate_limit_total": 0,
            "live_scan_rate_limit_total": 0,
            **{f"bybit_rate_limit_total:{priority.name}": 0 for priority in RequestPriority},
        })
        self._policy = rate_limit_policy or RateLimitPolicy()
        self._executions: set[asyncio.Task] = set()

    @property
    def min_delay_ms(self) -> int:
        return self._min_delay_ms

    def stats_snapshot(self) -> dict[str, dict[str, float | int]]:
        return {**{key: dict(value) for key, value in self._stats.items()},
                "metrics": dict(self._metrics),
                "provider_request_queue_depth": {p.name: sum(t.priority == p for t in self._queue) for p in RequestPriority},
                "in_flight": {p.name: self._in_flight[p] for p in RequestPriority}}

    def _flow(self, operation: str, priority: RequestPriority) -> Counter:
        return self._stats.setdefault(f"{operation}:{priority.name}", Counter())

    async def schedule(self, func: Callable[..., T], *args: Any,
                       priority: RequestPriority | None = None, operation: str = "unknown",
                       deadline_monotonic: float | None = None, **kwargs: Any) -> T:
        priority = request_class.get() if priority is None else priority
        deadline_monotonic = request_deadline.get() if deadline_monotonic is None else deadline_monotonic
        stats = self._flow(operation, priority)
        stats["queued"] += 1
        # Background retries are persisted by the outcome owner, never multiplied here.
        attempts = 1 if priority >= RequestPriority.OUTCOME_REPLAY else self._policy.max_attempts
        for attempt in range(1, attempts + 1):
            try:
                await self._admit(priority, operation, deadline_monotonic)
                stats["attempts"] += 1
                execution = asyncio.create_task(self._execute(func, args, kwargs, priority))
                self._executions.add(execution)
                execution.add_done_callback(self._completed_execution)
                result = await asyncio.shield(execution)
                stats["completed"] += 1
                return result
            except RequestDeadlineExceeded:
                stats["deadline_expired"] += 1
                raise
            except ProviderRateLimited as exc:
                exc.attempt = attempt
                self._metrics["bybit_rate_limit_total"] += 1
                self._metrics[f"bybit_rate_limit_total:{priority.name}"] += 1
                if priority is RequestPriority.LIVE_SCAN:
                    self._metrics["live_scan_rate_limit_total"] += 1
                exc.scheduler_state = {
                    "scheduler_queue_depth": len(self._queue),
                    "live_scan_in_flight": self._in_flight[RequestPriority.LIVE_SCAN],
                    "outcome_in_flight": self._in_flight[RequestPriority.OUTCOME_REPLAY],
                }
                delay = self._policy.delay(attempt, exc.retry_after)
                if exc.global_scope:
                    self._global_cooldown = max(self._global_cooldown, time.monotonic() + delay)
                else:
                    self._cooldown[priority] = max(self._cooldown.get(priority, 0), time.monotonic() + delay)
                if attempt == attempts:
                    stats["errors"] += 1
                    raise
            except Exception as exc:
                stats["errors"] += 1
                # Bybit permanent errors must not be retried as rate limits.
                if getattr(exc, "status_code", None) is not None or getattr(exc, "ret_code", None) is not None:
                    raise
                if attempt >= min(attempts, 3):
                    raise
                delay = min(0.5 * 2 ** (attempt - 1), 4)
            if deadline_monotonic is not None and time.monotonic() + delay + RETRY_ATTEMPT_WINDOW_SEC > deadline_monotonic:
                stats["deadline_expired"] += 1
                raise RequestDeadlineExceeded(operation, priority, stage="retry") from None
            await asyncio.sleep(delay)
        raise RuntimeError("unreachable retry state")

    def _completed_execution(self, task: asyncio.Task) -> None:
        self._executions.discard(task)
        if not task.cancelled():
            task.exception()  # Consume an exception even when its caller was cancelled.

    async def _execute(self, func, args, kwargs, priority):
        try:
            return await asyncio.to_thread(func, *args, **kwargs)
        finally:
            async with self._condition:
                self._in_flight[priority] -= 1
                self._condition.notify_all()

    def _has_capacity(self, priority: RequestPriority) -> bool:
        total = sum(self._in_flight.values())
        if total >= self._max_concurrency:
            return False
        if priority != RequestPriority.LIVE_SCAN and self._max_concurrency > 1:
            non_live = total - self._in_flight[RequestPriority.LIVE_SCAN]
            if non_live >= self._max_concurrency - 1:
                return False
        return priority != RequestPriority.OUTCOME_REPLAY or self._in_flight[priority] < self._outcome_capacity

    def _ready_at(self, priority: RequestPriority, now: float) -> float:
        ready = max(self._limiter.next_start, self._global_cooldown, self._cooldown.get(priority, 0))
        if priority >= RequestPriority.OUTCOME_REPLAY:
            while self._background_starts and self._background_starts[0] <= now - 60:
                self._background_starts.popleft()
            if len(self._background_starts) >= self._background_limit:
                ready = max(ready, self._background_starts[0] + 60)
        return ready

    async def _admit(self, priority: RequestPriority, operation: str, deadline: float | None) -> None:
        ticket = _Ticket(priority, operation, time.monotonic())
        async with self._condition:
            self._queue.append(ticket)
            self._condition.notify_all()
            try:
                while True:
                    now = time.monotonic()
                    if deadline is not None and now >= deadline:
                        raise RequestDeadlineExceeded(operation, priority, stage="admission")
                    eligible = [t for t in self._queue if self._has_capacity(t.priority)]
                    ready = [t for t in eligible if self._ready_at(t.priority, now) <= now]
                    chosen = min(ready, key=lambda t: (t.priority, t.placed_at), default=None)
                    if chosen is ticket:
                        self._limiter.reserve(now)
                        self._in_flight[priority] += 1
                        if priority >= RequestPriority.OUTCOME_REPLAY:
                            self._background_starts.append(now)
                        self._flow(operation, priority)["queue_wait_ms_total"] += (now - ticket.placed_at) * 1000
                        return
                    wake_times = [self._ready_at(t.priority, now) for t in eligible if self._ready_at(t.priority, now) > now]
                    if deadline is not None:
                        wake_times.append(deadline)
                    timeout = max(0.0001, min(wake_times) - now) if wake_times else None
                    try:
                        await asyncio.wait_for(self._condition.wait(), timeout)
                    except asyncio.TimeoutError:
                        pass
            finally:
                self._queue.remove(ticket)
                self._condition.notify_all()
