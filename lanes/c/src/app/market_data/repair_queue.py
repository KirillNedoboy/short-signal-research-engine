"""Bounded ranked continuity-repair demand and conservative spare-capacity gate."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class _Entry:
    rank: int
    sequence: int
    attempts: int = 0
    due: float = 0.0
    inflight: bool = False


class RepairQueue:
    def __init__(self, capacity: int = 150, cooldown: float = 60.0) -> None:
        self.capacity = capacity
        self.cooldown = cooldown
        self._entries: dict[str, _Entry] = {}
        self._sequence = 0
        self.overflow = 0

    @property
    def queued(self) -> int:
        return sum(not entry.inflight for entry in self._entries.values())

    def offer(self, blocked: list[str], ranked: list[str], now: float) -> int:
        ranks = {str(symbol).upper(): index for index, symbol in enumerate(ranked)}
        demand = list(dict.fromkeys(str(symbol).upper() for symbol in blocked))
        for symbol in demand:
            if symbol in self._entries:
                self._entries[symbol].rank = ranks.get(symbol, len(ranks))
        added = 0
        for symbol in sorted(demand, key=lambda item: ranks.get(item, len(ranks))):
            if symbol in self._entries:
                continue
            rank = ranks.get(symbol, len(ranks))
            if len(self._entries) >= self.capacity:
                pending = [(name, entry) for name, entry in self._entries.items()
                           if not entry.inflight]
                worst = max(pending, key=lambda item: (item[1].rank, item[1].sequence),
                            default=None)
                if worst is None or worst[1].rank <= rank:
                    self.overflow += 1
                    continue
                del self._entries[worst[0]]
                self.overflow += 1  # displaced demand must be re-offered next window
            self._entries[symbol] = _Entry(rank, self._sequence, due=now)
            self._sequence += 1
            added += 1
        return added

    def pop(self, now: float) -> str | None:
        choices = ((symbol, entry) for symbol, entry in self._entries.items()
                   if not entry.inflight and entry.due <= now)
        selected = min(choices, key=lambda item: (item[1].attempts, item[1].rank,
                                                   item[1].sequence), default=None)
        if selected is None:
            return None
        symbol, entry = selected
        entry.inflight = True
        return symbol

    def finish(self, symbol: str, now: float, *, ready: bool) -> None:
        entry = self._entries.get(symbol)
        if entry is None:
            return
        if ready:
            del self._entries[symbol]
        else:
            entry.inflight = False
            entry.attempts += 1
            entry.due = now + self.cooldown

    def next_due(self, now: float) -> float | None:
        return min((entry.due for entry in self._entries.values() if not entry.inflight),
                   default=None)

    def reconcile(self, blocked: list[str]) -> None:
        blocked_set = {str(symbol).upper() for symbol in blocked}
        for symbol, entry in tuple(self._entries.items()):
            if symbol not in blocked_set and not entry.inflight:
                del self._entries[symbol]

    def clear(self) -> None:
        self._entries.clear()


class RecoveryBudget:
    """Bounded recovery rate with a conservative startup burst."""
    def __init__(self, *, bootstrap_limit: int = 8) -> None:
        if bootstrap_limit < 1:
            raise ValueError("bootstrap recovery limit must be positive")
        self.limit = bootstrap_limit
        self._steady_limit = 8
        self._starts: deque[float] = deque()
        self._backoff_until = 0.0

    def set_measured_limit(self, limit: int, *, measured_spare_per_minute: int) -> None:
        if limit < 1 or measured_spare_per_minute < limit:
            raise ValueError("recovery grant exceeds measured shared-IP spare capacity")
        self.limit = limit

    def _prune(self, now: float) -> None:
        while self._starts and self._starts[0] <= now - 60:
            self._starts.popleft()

    def allowed(self, now: float, scheduler_stats: dict) -> bool:
        self._prune(now)
        for name in ("LIVE_SCAN", "FAST_MONITOR"):
            if scheduler_stats.get("provider_request_queue_depth", {}).get(name, 0):
                return False
        return now >= self._backoff_until and len(self._starts) < self.limit

    def next_due(self, now: float) -> float:
        self._prune(now)
        return max(self._backoff_until,
                   self._starts[0] + 60 if len(self._starts) >= self.limit else now)

    def record(self, now: float) -> None:
        self._prune(now)
        self._starts.append(now)

    def rate_limited(self, now: float) -> None:
        self.limit = self._steady_limit
        self._backoff_until = max(self._backoff_until, now + 60)
