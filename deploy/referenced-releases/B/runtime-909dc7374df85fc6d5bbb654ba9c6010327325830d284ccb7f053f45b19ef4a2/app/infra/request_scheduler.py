"""Shared priority-aware request scheduling and retry logic."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, TypeVar

from app.infra.rate_limiter import AsyncRateLimiter

T = TypeVar("T")

#: Shared P1/P2 lanes serve at most this many consecutive P1 requests before
#: one waiting P2 request is admitted, bounding outcome/backfill starvation.
MAX_CONSECUTIVE_P1_BEFORE_P2 = 8

#: Conservative minimum wall time an attempt still needs to be worthwhile.
RETRY_ATTEMPT_WINDOW_SEC = 1.0

_MAX_ATTEMPTS = 3


class RequestPriority(IntEnum):
    """Global REST admission priorities; lower value is served first."""

    P0 = 0
    P1 = 1
    P2 = 2


class RequestDeadlineExceeded(RuntimeError):
    """A scheduled request could not admit or retry within its deadline."""

    def __init__(
        self,
        operation: str,
        priority: RequestPriority,
        *,
        stage: str,
    ) -> None:
        self.operation = operation
        self.priority = priority
        self.stage = stage
        super().__init__(
            f"request deadline exceeded for {operation} "
            f"(priority={priority.name}, stage={stage})"
        )


@dataclass(slots=True)
class _Ticket:
    priority: RequestPriority
    operation: str
    placed_at: float
    granted: asyncio.Future[None]
    lane: str | None = None

    @property
    def acquired(self) -> bool:
        return self.lane is not None and self.granted.done()


@dataclass(slots=True)
class _FlowStats:
    """Bounded per-(operation, priority) admission evidence."""

    queued: int = 0
    attempts: int = 0
    completed: int = 0
    errors: int = 0
    deadline_expired: int = 0
    queue_wait_ms_total: float = 0.0
    queue_wait_ms_max: float = 0.0
    limiter_wait_ms_total: float = 0.0
    limiter_wait_ms_max: float = 0.0

    def snapshot(self) -> dict[str, float | int]:
        return {
            "queued": self.queued,
            "attempts": self.attempts,
            "completed": self.completed,
            "errors": self.errors,
            "deadline_expired": self.deadline_expired,
            "queue_wait_ms_total": round(self.queue_wait_ms_total, 3),
            "queue_wait_ms_max": round(self.queue_wait_ms_max, 3),
            "queue_wait_ms_mean": (
                round(self.queue_wait_ms_total / self.completed, 3)
                if self.completed
                else 0.0
            ),
            "limiter_wait_ms_total": round(self.limiter_wait_ms_total, 3),
            "limiter_wait_ms_max": round(self.limiter_wait_ms_max, 3),
            "limiter_wait_ms_mean": (
                round(self.limiter_wait_ms_total / self.attempts, 3)
                if self.attempts
                else 0.0
            ),
        }


class RequestScheduler:
    """Concurrency-limited request scheduler for exchange and Telegram APIs.

    One reserved P0 lane and ``max_concurrency - 1`` shared P1/P2 lanes feed a
    single global start-rate limiter; every exchange REST call passes through
    this object so no worker can create an independent Bybit budget.
    """

    def __init__(
        self,
        max_concurrency: int,
        jitter_min_ms: int = 100,
        jitter_max_ms: int = 300,
        min_delay_ms: int = 350,
    ) -> None:
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be positive")
        self._min_delay_ms = min_delay_ms
        self._p0_capacity = 1 if max_concurrency >= 2 else 0
        self._p0_free = self._p0_capacity
        self._shared_free = max_concurrency - self._p0_capacity
        self._rate_limiter = AsyncRateLimiter(
            min_delay_ms=min_delay_ms,
            jitter_min_ms=jitter_min_ms,
            jitter_max_ms=jitter_max_ms,
        )
        self._p0_queue: deque[_Ticket] = deque()
        self._shared_queues: dict[RequestPriority, deque[_Ticket]] = {
            RequestPriority.P1: deque(),
            RequestPriority.P2: deque(),
        }
        self._consecutive_p1_grants = 0
        self._stats: dict[tuple[str, int], _FlowStats] = {}

    @property
    def min_delay_ms(self) -> int:
        """Return configured minimum spacing between scheduled requests."""

        return self._min_delay_ms

    def stats_snapshot(self) -> dict[str, dict[str, float | int]]:
        """Return bounded admission counters keyed by ``operation:priority``."""

        return {
            f"{operation}:P{int(priority)}": stats.snapshot()
            for (operation, priority), stats in sorted(self._stats.items())
        }

    async def schedule(
        self,
        func: Callable[..., T],
        *args: Any,
        priority: RequestPriority = RequestPriority.P1,
        operation: str = "unknown",
        deadline_monotonic: float | None = None,
        **kwargs: Any,
    ) -> T:
        """Run a sync function on a worker thread with retries under one lane."""

        stats = self._flow(operation, priority)
        stats.queued += 1
        if deadline_monotonic is not None and time.monotonic() >= (
            deadline_monotonic
        ):
            stats.deadline_expired += 1
            raise RequestDeadlineExceeded(operation, priority, stage="admission")
        ticket: _Ticket | None = None
        try:
            ticket = await self._acquire_lane(
                priority, operation, deadline_monotonic
            )
            result = await self._run_with_retry(
                func,
                args,
                kwargs,
                operation=operation,
                priority=priority,
                deadline_monotonic=deadline_monotonic,
            )
            stats.completed += 1
            return result
        except RequestDeadlineExceeded:
            stats.deadline_expired += 1
            raise
        except Exception:
            stats.errors += 1
            raise
        finally:
            if ticket is not None and ticket.acquired:
                self._release_lane(ticket)

    async def _run_with_retry(
        self,
        func: Callable[..., T],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        operation: str,
        priority: RequestPriority,
        deadline_monotonic: float | None,
    ) -> T:
        """Run a sync provider call with bounded, rate-limited retries."""
        stats = self._flow(operation, priority)
        for attempt in range(_MAX_ATTEMPTS):
            if deadline_monotonic is not None and time.monotonic() >= (
                deadline_monotonic
            ):
                raise RequestDeadlineExceeded(
                    operation, priority, stage=f"attempt:{attempt}"
                )
            limiter_started = time.monotonic()
            await self._rate_limiter.acquire()
            limiter_wait_ms = (time.monotonic() - limiter_started) * 1000.0
            stats.limiter_wait_ms_total += limiter_wait_ms
            stats.limiter_wait_ms_max = max(
                stats.limiter_wait_ms_max, limiter_wait_ms
            )
            stats.attempts += 1
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            except Exception:
                if attempt == _MAX_ATTEMPTS - 1:
                    raise
                backoff = min(0.5 * (2**attempt), 4.0)
                if (
                    deadline_monotonic is not None
                    and time.monotonic()
                    + backoff
                    + RETRY_ATTEMPT_WINDOW_SEC
                    > deadline_monotonic
                ):
                    raise RequestDeadlineExceeded(
                        operation, priority, stage=f"retry:{attempt + 1}"
                    ) from None
                await asyncio.sleep(backoff)
        raise RuntimeError("request retry loop exited unexpectedly")

    def _flow(self, operation: str, priority: RequestPriority) -> _FlowStats:
        key = (operation, int(priority))
        stats = self._stats.get(key)
        if stats is None:
            stats = _FlowStats()
            self._stats[key] = stats
        return stats

    async def _acquire_lane(
        self,
        priority: RequestPriority,
        operation: str,
        deadline_monotonic: float | None,
    ) -> _Ticket:
        loop = asyncio.get_running_loop()
        ticket = _Ticket(
            priority=priority,
            operation=operation,
            placed_at=time.monotonic(),
            granted=loop.create_future(),
        )
        self._enqueue(ticket)
        self._dispatch()
        timeout: float | None = None
        if deadline_monotonic is not None:
            timeout = max(0.0, deadline_monotonic - time.monotonic())
        try:
            if timeout is None:
                await ticket.granted
            else:
                await asyncio.wait_for(ticket.granted, timeout)
        except asyncio.TimeoutError:
            if ticket.acquired:
                # Grant landed in the same tick as the timeout; return the lane.
                self._release_lane(ticket)
            else:
                self._discard(ticket)
            raise RequestDeadlineExceeded(
                operation, priority, stage="queue"
            ) from None
        except asyncio.CancelledError:
            if ticket.acquired:
                self._release_lane(ticket)
            else:
                self._discard(ticket)
            raise
        stats = self._flow(operation, priority)
        waited_ms = (time.monotonic() - ticket.placed_at) * 1000.0
        stats.queue_wait_ms_total += waited_ms
        stats.queue_wait_ms_max = max(stats.queue_wait_ms_max, waited_ms)
        return ticket

    def _enqueue(self, ticket: _Ticket) -> None:
        if ticket.priority is RequestPriority.P0 and self._p0_capacity:
            self._p0_queue.append(ticket)
            return
        # With a single lane budget there is no room to reserve a lane;
        # P0 admission then shares the P1 queue rather than starving.
        queue_priority = (
            RequestPriority.P1
            if ticket.priority is RequestPriority.P0
            else ticket.priority
        )
        self._shared_queues[queue_priority].append(ticket)

    def _discard(self, ticket: _Ticket) -> None:
        for queue in (self._p0_queue, *self._shared_queues.values()):
            try:
                queue.remove(ticket)
            except ValueError:
                continue
            return

    def _dispatch(self) -> None:
        while self._p0_free > 0 and self._p0_queue:
            self._grant(self._p0_queue.popleft(), lane="p0")
        while self._shared_free > 0:
            ticket = self._select_shared()
            if ticket is None:
                break
            self._grant(ticket, lane="shared")

    def _select_shared(self) -> _Ticket | None:
        p1 = self._shared_queues[RequestPriority.P1]
        p2 = self._shared_queues[RequestPriority.P2]
        if self._p0_capacity == 0 and self._p0_queue:
            return self._p0_queue.popleft()
        if p1 and self._consecutive_p1_grants < MAX_CONSECUTIVE_P1_BEFORE_P2:
            return p1.popleft()
        if p2:
            return p2.popleft()
        if p1:
            return p1.popleft()
        return None

    def _grant(self, ticket: _Ticket, *, lane: str) -> None:
        if lane == "p0":
            self._p0_free -= 1
        else:
            self._shared_free -= 1
            if ticket.priority is RequestPriority.P1:
                self._consecutive_p1_grants += 1
            else:
                self._consecutive_p1_grants = 0
        ticket.lane = lane
        if not ticket.granted.done():
            ticket.granted.set_result(None)

    def _release_lane(self, ticket: _Ticket) -> None:
        if ticket.lane == "p0":
            self._p0_free += 1
        elif ticket.lane == "shared":
            self._shared_free += 1
        ticket.lane = None
        self._dispatch()
