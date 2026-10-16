"""Health counters for the live loop."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ServiceHealth:
    """Runtime counters exposed to logs and alerts."""

    cycles: int = 0
    signals_sent: int = 0
    errors: int = 0
    strategy_observation_write_failures: int = 0
    last_cycle_started_at: datetime | None = None
    last_cycle_finished_at: datetime | None = None
    last_error_at: datetime | None = None

    def on_cycle_start(self) -> None:
        self.cycles += 1
        self.last_cycle_started_at = datetime.now(timezone.utc)

    def on_cycle_finish(self) -> None:
        self.last_cycle_finished_at = datetime.now(timezone.utc)

    def on_signal(self, count: int = 1) -> None:
        self.signals_sent += count

    def on_error(self) -> None:
        self.errors += 1
        self.last_error_at = datetime.now(timezone.utc)

    def on_strategy_observation_write_failure(self, count: int = 1) -> None:
        self.strategy_observation_write_failures += count
