"""Bounded runtime warmup coverage state."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Iterable


READINESS_FRACTION = 0.95


@dataclass(slots=True)
class WarmupCoverage:
    """Track actionable coverage without treating failures as readiness."""

    _targets: set[str] = field(default_factory=set)
    _ready: set[str] = field(default_factory=set)
    _failed: set[str] = field(default_factory=set)
    _state: str = "WARMING_UP"

    def begin(self, symbols: Iterable[str], *, ready: Iterable[str] = ()) -> None:
        self._targets = {str(symbol).upper() for symbol in symbols}
        self._ready = {
            str(symbol).upper() for symbol in ready if str(symbol).upper() in self._targets
        }
        self._failed = set()
        self._refresh_state()

    def mark_ready(self, symbol: str) -> None:
        normalized = str(symbol).upper()
        if normalized not in self._targets:
            return
        self._failed.discard(normalized)
        self._ready.add(normalized)
        self._refresh_state()

    def mark_waiting(self, symbol: str) -> None:
        """Keep a symbol retryable while waiting for current WS state."""
        normalized = str(symbol).upper()
        if normalized not in self._targets:
            return
        self._failed.discard(normalized)
        self._ready.discard(normalized)
        self._refresh_state()

    def mark_failed(self, symbol: str) -> None:
        normalized = str(symbol).upper()
        if normalized not in self._targets:
            return
        self._ready.discard(normalized)
        self._failed.add(normalized)
        self._refresh_state()

    def add_targets(self, symbols: Iterable[str]) -> None:
        self._targets.update(str(symbol).upper() for symbol in symbols)
        self._refresh_state()

    @property
    def pending_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._targets - self._ready - self._failed))

    @property
    def target_symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._targets))

    @property
    def required_ready(self) -> int:
        """Minimum non-empty actionable coverage for runtime READY."""
        return max(1, math.ceil(len(self._targets) * READINESS_FRACTION))

    def snapshot(self) -> dict[str, int | str]:
        return {
            "state": self._state,
            "total": len(self._targets),
            "ready": len(self._ready),
            "pending": len(self.pending_symbols),
            "failed": len(self._failed),
        }

    def _refresh_state(self) -> None:
        ready = len(self._ready)
        self._state = (
            "READY"
            if self._targets and ready >= self.required_ready
            else "WARMING_UP"
        )
