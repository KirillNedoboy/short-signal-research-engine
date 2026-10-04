"""Per-symbol ownership for logically atomic runtime state transitions."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator


@dataclass
class _Lane:
    lock: asyncio.Lock
    users: int = 0
    owner: asyncio.Task[object] | None = None
    contention: int = 0


class SymbolMutationCoordinator:
    """Serialize read-modify-write transitions for one canonical symbol key."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._lanes: dict[str, _Lane] = {}
        self._logger = logger or logging.getLogger(__name__)

    @staticmethod
    def _key(symbol: str) -> str:
        return symbol.upper()

    @asynccontextmanager
    async def mutation(self, symbol: str) -> AsyncIterator[None]:
        key = self._key(symbol)
        lane = self._lanes.get(key)
        if lane is None:
            lane = _Lane(asyncio.Lock())
            self._lanes[key] = lane
        current = asyncio.current_task()
        if lane.owner is current:
            raise RuntimeError(f"reentrant symbol mutation for {key}")
        lane.users += 1
        started = time.monotonic()
        if lane.lock.locked():
            lane.contention += 1
        acquired = False
        try:
            await lane.lock.acquire()
            acquired = True
            lane.owner = current
            waited = time.monotonic() - started
            if waited:
                self._logger.debug(
                    "symbol mutation lane acquired | symbol=%s wait_seconds=%.6f contention=%s",
                    key,
                    waited,
                    lane.contention,
                )
            yield
        finally:
            if acquired:
                lane.owner = None
                lane.lock.release()
            lane.users -= 1
            if lane.users == 0 and not lane.lock.locked():
                self._lanes.pop(key, None)
