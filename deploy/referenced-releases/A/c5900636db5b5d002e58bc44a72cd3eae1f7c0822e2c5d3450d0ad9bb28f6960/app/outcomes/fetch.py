"""Outcome-only single-flight candles and durable provider deferrals."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import Counter, OrderedDict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from app.config import AppConfig
from app.infra.provider_errors import OutcomeBudgetUnavailable, ProviderRateLimited, RateLimitPolicy
from app.infra.request_scheduler import RequestPriority

LOGGER = logging.getLogger(__name__)


class OutcomeCandleFetcher:
    def __init__(self, client, repository, config: AppConfig | None = None, *, clock=None, monotonic=None):
        self.client = client
        self.repository = repository
        self.config = config or AppConfig()
        self.policy = RateLimitPolicy.from_config(self.config)
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.monotonic = monotonic or time.monotonic
        self._cache = OrderedDict()
        self._inflight: dict[tuple, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(self.config.outcome_max_concurrency)
        self._cycle_depth = 0
        self._requests = 0
        self._cooldown_until = 0.0
        self._failures = 0
        self.metrics = Counter({
            "outcome_deferred_rate_limit_total": 0,
            "outcome_retry_success_total": 0,
            "outcome_retry_exhausted_total": 0,
        })

    @contextmanager
    def cycle(self):
        if not self._cycle_depth:
            self._requests = 0
        self._cycle_depth += 1
        try:
            yield
        finally:
            self._cycle_depth -= 1

    @property
    def cooldown_remaining(self):
        return max(0.0, self._cooldown_until - self.monotonic())

    async def fetch_klines(self, symbol, interval, *, limit=240, start_ms=None, end_ms=None, **_kwargs):
        key = (symbol, interval, start_ms, end_ms, limit)
        cached = self._cache.get(key)
        if cached is not None:
            expires, rows = cached
            if self.monotonic() < expires:
                self._cache.move_to_end(key)
                return [list(row) for row in rows]
            del self._cache[key]
        task = self._inflight.get(key)
        if task is None:
            if len(self._inflight) >= self.config.outcome_max_inflight_keys:
                raise OutcomeBudgetUnavailable("outcome in-flight capacity reached")
            if self.cooldown_remaining:
                raise OutcomeBudgetUnavailable("outcome cooldown")
            task = asyncio.create_task(self._fetch(key))
            self._inflight[key] = task
            task.add_done_callback(lambda done: self._finish(key, done))
        rows = await asyncio.shield(task)
        return [list(row) for row in rows]
    async def _fetch(self, key):
        symbol, interval, start, end, limit = key
        async with self._semaphore:
            if self.cooldown_remaining:
                raise OutcomeBudgetUnavailable("outcome cooldown")
            if self._requests >= self.config.outcome_max_requests_per_cycle:
                raise OutcomeBudgetUnavailable("outcome request budget unavailable")
            self._requests += 1
            try:
                raw = await self.client.fetch_klines(
                    symbol, interval, start_ms=start, end_ms=end, limit=limit,
                    priority=RequestPriority.OUTCOME_REPLAY,
                )
            except ProviderRateLimited as exc:
                self._failures += 1
                delay = min(self.config.outcome_rate_limit_max_backoff_sec,
                            self.config.outcome_rate_limit_backoff_sec * 2 ** min(self._failures - 1, 10))
                self._cooldown_until = self.monotonic() + max(delay, exc.retry_after)
                raise
        self._failures = max(0, self._failures - 1)
        rows = tuple(tuple(row) for row in raw)
        self._cache[key] = (self.monotonic() + self.config.outcome_cache_ttl_sec, rows)
        self._cache.move_to_end(key)
        while len(self._cache) > self.config.outcome_cache_max_entries:
            self._cache.popitem(last=False)
        return rows

    def _finish(self, key, task):
        self._inflight.pop(key, None)
        if not task.cancelled():
            task.exception()


    def defer(self, task_key: str, exc: ProviderRateLimited, *, symbol: str, interval: str):
        previous = self.repository.outcome_fetch_retry(task_key)
        attempt = int(previous.get("provider_attempt_count", 0)) + 1
        exhausted = attempt >= self.policy.max_total_attempts
        now = self.clock()
        retry_at = None if exhausted else now + timedelta(seconds=self.policy.delay(attempt, exc.retry_after))
        data = {
            "data_status": "deferred", "data_reason": "provider_rate_limited", "retryable": True,
            "provider_code": exc.provider_code, "provider_attempt_count": attempt,
            "automatic_retry_exhausted": exhausted,
        }
        self.repository.defer_outcome_fetch(task_key, data, next_attempt_at=retry_at, now=now)
        self.metrics["outcome_deferred_rate_limit_total"] += 1
        if exhausted and not previous.get("automatic_retry_exhausted"):
            self.metrics["outcome_retry_exhausted_total"] += 1
        event = {
            "event": "provider_rate_limit_deferred", "symbol": symbol, "interval": interval,
            "request_class": RequestPriority.OUTCOME_REPLAY.name, "attempt": attempt,
            "max_attempts": self.policy.max_total_attempts,
            "retry_at": retry_at.isoformat() if retry_at else None, "provider_code": exc.provider_code,
            "scheduler_queue_depth": 0, "live_scan_in_flight": 0,
            "outcome_in_flight": len(self._inflight), **exc.scheduler_state,
        }
        LOGGER.warning(json.dumps(event, separators=(",", ":")))
        return data, retry_at, now

    def recovered(self, task_key):
        if self.repository.recover_outcome_fetch(task_key):
            self.metrics["outcome_retry_success_total"] += 1

    async def close(self):
        # Do not cancel shared producers while a sync HTTP call is still running.
        if self._inflight:
            await asyncio.gather(*self._inflight.values(), return_exceptions=True)
