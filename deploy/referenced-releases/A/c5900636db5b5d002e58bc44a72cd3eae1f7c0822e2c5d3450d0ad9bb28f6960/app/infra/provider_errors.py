"""Safe provider error metadata and the single rate-limit retry policy."""
from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Mapping


class ProviderRateLimited(RuntimeError):
    retryable = True
    data_reason = "provider_rate_limited"

    def __init__(self, provider_code: int = 10006, *, retry_after: float = 0.0,
                 global_scope: bool = False) -> None:
        self.provider_code = provider_code
        self.retry_after = retry_after
        self.global_scope = global_scope
        self.attempt = 1
        self.scheduler_state: dict[str, int] = {}
        super().__init__(f"RATE_LIMITED provider_code={provider_code}")


class OutcomeBudgetUnavailable(RuntimeError):
    """No request was sent; leave the task due without recording API failure."""


@dataclass(frozen=True)
class RateLimitPolicy:
    windows: tuple[tuple[float, float], ...] = ((2, 4), (8, 12), (30, 45), (90, 120))
    max_attempts: int = 5
    max_total_attempts: int = 8
    deferred_minutes: float = 5
    deferred_max_minutes: float = 60

    @classmethod
    def from_config(cls, config) -> RateLimitPolicy:
        return cls(
            windows=tuple(tuple(pair) for pair in config.outcome_rate_limit_retry_windows_sec),
            max_attempts=config.outcome_rate_limit_max_attempts,
            max_total_attempts=config.outcome_max_total_attempts,
            deferred_minutes=config.outcome_deferred_retry_minutes,
            deferred_max_minutes=config.outcome_deferred_max_retry_minutes,
        )

    def delay(self, attempt: int, retry_after: float = 0.0) -> float:
        if attempt < self.max_attempts:
            low, high = self.windows[min(attempt - 1, len(self.windows) - 1)]
            delay = random.uniform(low, high)
        else:
            delay = 60 * min(self.deferred_max_minutes,
                             self.deferred_minutes * 2 ** min(attempt - self.max_attempts, 10))
        return max(delay, retry_after)


def retry_after_seconds(headers: Mapping | None, *, now: datetime | None = None) -> float:
    """Accept seconds/HTTP-date and Bybit's epoch-ms reset; never shorten a ban."""
    now = now or datetime.now(timezone.utc)
    values = {str(k).lower(): str(v) for k, v in (headers or {}).items()}
    delays = [0.0]
    value = values.get("retry-after")
    if value:
        try:
            delays.append(float(value))
        except ValueError:
            try:
                parsed = parsedate_to_datetime(value)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                delays.append((parsed - now).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    reset = values.get("x-bapi-limit-reset-timestamp")
    if reset:
        try:
            delays.append(float(reset) / 1000 - now.timestamp())
        except ValueError:
            pass
    return max(d for d in delays if math.isfinite(d))
