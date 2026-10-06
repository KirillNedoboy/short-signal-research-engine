"""Health counters and component liveness projection for the live loop."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _status(last_seen: datetime | None, now: datetime, stale_after_sec: float) -> str:
    if last_seen is None:
        return "UNKNOWN"
    return "HEALTHY" if (now - last_seen).total_seconds() <= stale_after_sec else "STALE"


@dataclass(slots=True)
class ServiceHealth:
    """Runtime counters and independent component heartbeats."""

    cycles: int = 0
    signals_sent: int = 0
    errors: int = 0
    strategy_observation_write_failures: int = 0
    last_cycle_started_at: datetime | None = None
    last_cycle_finished_at: datetime | None = None
    last_error_at: datetime | None = None
    process_last_seen: datetime | None = None
    full_scan_last_complete: datetime | None = None
    fast_monitor_last_complete: datetime | None = None
    market_data_last_healthy: datetime | None = None
    outbox_last_progress: datetime | None = None
    outbox_last_observed: datetime | None = None
    last_scan_duration_ms: float | None = None
    last_event_loop_lag_ms: float | None = None

    def mark_process(self, at: datetime | None = None) -> None:
        self.process_last_seen = at or datetime.now(timezone.utc)

    def mark_scan(self, at: datetime | None = None, *, duration_ms: float | None = None) -> None:
        self.full_scan_last_complete = at or datetime.now(timezone.utc)
        if duration_ms is not None:
            self.last_scan_duration_ms = float(duration_ms)

    def mark_fast_monitor(self, at: datetime | None = None) -> None:
        self.fast_monitor_last_complete = at or datetime.now(timezone.utc)

    def mark_market_data(self, at: datetime | None = None) -> None:
        self.market_data_last_healthy = at or datetime.now(timezone.utc)

    def mark_outbox(self, at: datetime | None = None) -> None:
        self.outbox_last_progress = at or datetime.now(timezone.utc)

    def mark_outbox_observed(self, at: datetime | None = None) -> None:
        self.outbox_last_observed = at or datetime.now(timezone.utc)

    def mark_event_loop_lag(self, lag_ms: float) -> None:
        self.last_event_loop_lag_ms = float(lag_ms)

    def snapshot(
        self,
        *,
        now: datetime | None = None,
        process_stale_after_sec: float = 120.0,
        scan_stale_after_sec: float = 300.0,
        market_data_stale_after_sec: float = 120.0,
        outbox_stale_after_sec: float = 300.0,
    ) -> dict[str, object]:
        """Return a secret-free component status projection."""
        observed_at = now or datetime.now(timezone.utc)
        outbox_last_seen = max(
            (
                timestamp
                for timestamp in (self.outbox_last_progress, self.outbox_last_observed)
                if timestamp is not None
            ),
            default=None,
        )
        outbox_status = _status(
            outbox_last_seen,
            observed_at,
            outbox_stale_after_sec,
        )
        if self.outbox_last_progress is None and self.outbox_last_observed is None:
            outbox_status = "UNKNOWN"
        return {
            "process_heartbeat": _status(self.process_last_seen, observed_at, process_stale_after_sec),
            "scan_heartbeat": _status(self.full_scan_last_complete, observed_at, scan_stale_after_sec),
            "market_data_health": _status(self.market_data_last_healthy, observed_at, market_data_stale_after_sec),
            "outbox_health": "STALLED" if outbox_status == "STALE" else outbox_status,
            "last_scan_duration_ms": self.last_scan_duration_ms,
            "last_event_loop_lag_ms": self.last_event_loop_lag_ms,
            "process_last_seen": _iso(self.process_last_seen),
            "full_scan_last_complete": _iso(self.full_scan_last_complete),
            "fast_monitor_last_complete": _iso(self.fast_monitor_last_complete),
            "market_data_last_healthy": _iso(self.market_data_last_healthy),
            "outbox_last_progress": _iso(self.outbox_last_progress),
            "outbox_last_observed": _iso(self.outbox_last_observed),
        }

    def on_cycle_start(self) -> None:
        self.cycles += 1
        now = datetime.now(timezone.utc)
        self.last_cycle_started_at = now
        self.mark_process(now)

    def on_cycle_finish(self) -> None:
        self.last_cycle_finished_at = datetime.now(timezone.utc)

    def on_signal(self, count: int = 1) -> None:
        self.signals_sent += count

    def on_error(self) -> None:
        self.errors += 1
        self.last_error_at = datetime.now(timezone.utc)

    def on_strategy_observation_write_failure(self, count: int = 1) -> None:
        self.strategy_observation_write_failures += count
