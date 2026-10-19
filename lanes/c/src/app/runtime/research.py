"""Bounded noncritical research queue; overflow is observable and nonblocking."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Any, Awaitable, Callable

from .research_queue import ResearchQueue


class BoundedResearchQueue:
    def __init__(self, *, maxsize: int = 256) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be positive")
        self._maxsize = maxsize
        self._queue: deque[Callable[[], Any | Awaitable[Any]]] = deque()
        self.dropped_total = 0

    def submit(self, job: Callable[[], Any | Awaitable[Any]]) -> bool:
        if len(self._queue) >= self._maxsize:
            self.dropped_total += 1
            return False
        self._queue.append(job)
        return True

    async def drain_once(self) -> int:
        count = 0
        while self._queue and count < self._maxsize:
            result = self._queue.popleft()()
            if asyncio.iscoroutine(result):
                await result
            count += 1
        return count


# Keep the former one-argument queue available for compatibility.  New code
# should use ResearchQueue.submit(name, write) for observable job counters.
__all__ = ["BoundedResearchQueue", "ResearchQueue"]

