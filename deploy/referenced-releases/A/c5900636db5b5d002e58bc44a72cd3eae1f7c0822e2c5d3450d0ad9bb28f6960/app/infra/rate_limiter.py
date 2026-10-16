"""Start-spacing clock; admission order belongs exclusively to the scheduler."""
from __future__ import annotations

import random


class StartRateLimiter:
    def __init__(self, min_delay_ms: int, jitter_min_ms: int, jitter_max_ms: int) -> None:
        self.next_start = 0.0
        self._delay = min_delay_ms / 1000
        self._jitter = (jitter_min_ms / 1000, jitter_max_ms / 1000)

    def reserve(self, now: float) -> None:
        self.next_start = now + self._delay + random.uniform(*self._jitter)
