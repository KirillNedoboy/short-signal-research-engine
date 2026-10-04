"""Bounded, priority-aware maturation for canonical shadow episodes."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from app.market.candles import klines_to_frame
from app.observability.root_detector_shadow import evaluate_shadow_outcome


logger = logging.getLogger(__name__)

HORIZONS: tuple[tuple[str, int], ...] = (
    ("15m", 15),
    ("30m", 30),
    ("1h", 60),
    ("4h", 240),
    ("12h", 720),
    ("24h", 1440),
)
OUTCOME_METHOD_VERSION = "ROOT_SHADOW_OUTCOME_V2"


def terminalize_due_horizons(*, anchor_time: datetime, market_asof: datetime, outcome: dict[str, Any]) -> dict[str, Any]:
    """Mark due horizons with no persisted result as explicit DATA_GAP."""
    result = dict(outcome)
    horizons = {label: dict(value or {}) for label, value in (outcome.get("horizons") or {}).items()}
    anchor = _utc(anchor_time)
    asof = _utc(market_asof)
    missing_due = False
    for label, minutes in HORIZONS:
        horizon = horizons.setdefault(label, {})
        if horizon.get("price") is None and asof >= anchor + timedelta(minutes=minutes):
            horizon.setdefault("status", "DATA_GAP")
            missing_due = True
    if missing_due and result.get("outcome_status") == "PARTIAL":
        result["outcome_status"] = "DATA_GAP"
    result["horizons"] = horizons
    return result


def quota_for_rate(
    incoming_rate_per_hour: float,
    *,
    scan_interval_sec: int,
    minimum: int,
    maximum: int,
    capacity_margin: float = 2.0,
) -> int:
    """Convert measured incoming episode rate to a bounded per-cycle quota."""
    cycles_per_hour = 3600 / max(1, scan_interval_sec)
    required = math.ceil(max(0.0, incoming_rate_per_hour) * max(1.0, capacity_margin) / cycles_per_hour)
    return max(minimum, min(maximum, required))


@dataclass(slots=True)
class SchedulerStats:
    runtime_due_seen: int = 0
    runtime_processed: int = 0
    legacy_due_seen: int = 0
    legacy_processed: int = 0
    v2_due_seen: int = 0
    v2_processed: int = 0
    v2_errors: int = 0
    live_root_due_seen: int = 0
    live_root_processed: int = 0
    live_root_errors: int = 0
    episodes_deferred: int = 0
    symbols_fetched: int = 0
    bybit_requests: int = 0
    retries: int = 0
    errors: int = 0
    oldest_overdue_seconds: float = 0.0
    remaining_due: int = 0
    cycle_duration_ms: float = 0.0

    @property
    def episodes_processed(self) -> int:
        return self.runtime_processed + self.legacy_processed

    @property
    def episodes_per_request(self) -> float:
        return self.episodes_processed / self.bybit_requests if self.bybit_requests else 0.0

    def to_summary(self, *, v2_enabled: bool) -> dict[str, int | float | bool]:
        """Return the canonical named scheduler summary used by logging/tests."""
        return {
            "runtime_due_seen": self.runtime_due_seen,
            "runtime_processed": self.runtime_processed,
            "legacy_due_seen": self.legacy_due_seen,
            "legacy_processed": self.legacy_processed,
            "symbols_fetched": self.symbols_fetched,
            "bybit_requests": self.bybit_requests,
            "v2_enabled": bool(v2_enabled),
            "v2_due_seen": self.v2_due_seen,
            "v2_processed": self.v2_processed,
            "v2_errors": self.v2_errors,
            "live_root_due_seen": self.live_root_due_seen,
            "live_root_processed": self.live_root_processed,
            "live_root_errors": self.live_root_errors,
            "episodes_deferred": self.episodes_deferred,
            "episodes_per_request": self.episodes_per_request,
            "remaining_due": self.remaining_due,
            "oldest_overdue_seconds": self.oldest_overdue_seconds,
            "retries": self.retries,
            "errors": self.errors,
            "cycle_duration_ms": self.cycle_duration_ms,
        }


def emit_scheduler_summary(stats: SchedulerStats, *, v2_enabled: bool) -> dict[str, int | float | bool]:
    """Emit one name-safe structured summary and return the same object."""
    summary = stats.to_summary(v2_enabled=v2_enabled)
    rendered = " ".join(f"{key}={value}" for key, value in summary.items())
    logger.info("Shadow outcome scheduler | %s", rendered, extra={"scheduler_summary": summary})
    return summary


def next_outcome_due_at(
    anchor_time: datetime,
    market_asof: datetime,
    outcome: dict[str, Any],
    *,
    data_gap_retry_minutes: int = 30,
) -> datetime | None:
    """Return the next immutable horizon deadline, never a tight retry loop."""

    anchor = _utc(anchor_time)
    asof = _utc(market_asof)
    status = str(outcome.get("outcome_status") or "")
    if status in {"MATURE", "DATA_GAP", "RETRYABLE_ERROR"}:
        return None
    horizons = outcome.get("horizons") or {}
    for label, minutes in HORIZONS:
        value = (horizons.get(label) or {}).get("price")
        if value is not None:
            continue
        target = anchor + timedelta(minutes=minutes)
        if target <= asof and status == "DATA_GAP":
            return asof + timedelta(minutes=max(1, data_gap_retry_minutes))
        return target
    return None


class ShadowOutcomeScheduler:
    """Process runtime episodes first, then a bounded legacy quota."""

    def __init__(
        self,
        *,
        client: Any,
        repository: Any,
        runtime_batch_size: int = 25,
        legacy_batch_size: int = 10,
        data_gap_retry_minutes: int = 30,
        outcome_code_version: str = "unknown",
        scan_interval_sec: int = 60,
        runtime_min_batch: int = 25,
        runtime_max_batch: int = 100,
        capacity_margin: float = 2.0,
        v2_enabled: bool,
    ) -> None:
        self._client = client
        self._repository = repository
        self._runtime_batch_size = max(0, runtime_batch_size)
        self._legacy_batch_size = max(0, legacy_batch_size)
        self._data_gap_retry_minutes = max(1, data_gap_retry_minutes)
        self._outcome_code_version = outcome_code_version
        self._scan_interval_sec = scan_interval_sec
        self._runtime_min_batch = runtime_min_batch
        self._runtime_max_batch = runtime_max_batch
        self._capacity_margin = capacity_margin
        self._v2_enabled = bool(v2_enabled)

    async def run_cycle(self, *, now: datetime | None = None) -> SchedulerStats:
        started = time.perf_counter()
        asof = _utc(now or datetime.now(timezone.utc))
        stats = SchedulerStats()
        try:
            runtime_batch_size = self._runtime_batch_size
            rate_fn = getattr(self._repository, "estimate_shadow_episode_rate", None)
            if rate_fn is not None:
                runtime_batch_size = quota_for_rate(
                    float(rate_fn(now=asof)), scan_interval_sec=self._scan_interval_sec,
                    minimum=self._runtime_min_batch, maximum=self._runtime_max_batch,
                    capacity_margin=self._capacity_margin,
                )
            batches = self._repository.list_shadow_episode_outcomes_due(
                now=asof,
                runtime_limit=runtime_batch_size,
                legacy_limit=self._legacy_batch_size,
            )
            runtime_rows = list(batches.get("runtime") or [])
            legacy_rows = list(batches.get("legacy") or [])
            stats.runtime_due_seen = len(runtime_rows)
            stats.legacy_due_seen = len(legacy_rows)
            v2_rows = []
            if self._v2_enabled:
                v2_due_fn = getattr(self._repository, "list_root_detector_shadow_v2_outcomes_due", None)
                if v2_due_fn is None:
                    raise RuntimeError("V2 scheduler enabled but repository lacks V2 outcomes query")
                try:
                    v2_rows = list(v2_due_fn(now=asof, limit=runtime_batch_size))
                except Exception:
                    stats.v2_errors += 1
                    raise
            stats.v2_due_seen = len(v2_rows)
            live_root_rows = []
            live_due_fn = getattr(self._repository, "list_current_root_outcomes_due", None)
            if live_due_fn is not None:
                live_root_rows = list(live_due_fn(now=asof, limit=runtime_batch_size))
            stats.live_root_due_seen = len(live_root_rows)
            rows = [("runtime", row) for row in runtime_rows] + [("legacy", row) for row in legacy_rows] + [("v2", row) for row in v2_rows] + [("live_root", row) for row in live_root_rows]
            due_times = [_utc(row["next_due_at"]) for _, row in rows if row.get("next_due_at")]
            if due_times:
                stats.oldest_overdue_seconds = max(0.0, (asof - min(due_times)).total_seconds())
            grouped = _group_by_symbol(rows)
            for symbol, symbol_rows in grouped.items():
                try:
                    frame = await self._fetch_group(symbol, symbol_rows, asof, stats)
                except Exception:
                    stats.errors += len(symbol_rows)
                    stats.v2_errors += sum(1 for cohort, _ in symbol_rows if cohort == "v2")
                    stats.live_root_errors += sum(1 for cohort, _ in symbol_rows if cohort == "live_root")
                    stats.retries += len(symbol_rows)
                    for cohort, row in symbol_rows:
                        self._record_error(row, asof, stats)
                    logger.exception("shadow outcome grouped fetch failed symbol=%s", symbol)
                    continue
                for cohort, row in symbol_rows:
                    self._process_row(cohort, row, frame, asof, stats)
            count_due = getattr(self._repository, "count_shadow_episode_outcomes_due", None)
            stats.remaining_due = int(count_due(now=asof)) if count_due is not None else 0
            count_live_due = getattr(self._repository, "count_current_root_outcomes_due", None)
            if count_live_due is not None:
                stats.remaining_due += int(count_live_due(now=asof))
            stats.episodes_deferred = max(0, stats.remaining_due - stats.errors)
        except Exception:
            stats.errors += 1
            logger.exception("shadow outcome scheduler cycle failed")
        finally:
            stats.cycle_duration_ms = (time.perf_counter() - started) * 1000.0
            emit_scheduler_summary(stats, v2_enabled=self._v2_enabled)
        return stats

    async def _fetch_group(self, symbol: str, rows: list[tuple[str, dict[str, Any]]], asof: datetime, stats: SchedulerStats) -> pd.DataFrame:
        anchors = [_utc(row["anchor_time"]) for _, row in rows]
        start = min(anchors)
        end = max(anchors) + timedelta(hours=24)
        limit = min(1000, max(20, math.ceil((end - start).total_seconds() / 300) + 10))
        stats.symbols_fetched += 1
        stats.bybit_requests += 1
        raw = await self._client.fetch_klines(
            symbol, "5", limit=limit,
            start_ms=int(start.timestamp() * 1000),
            end_ms=int(min(end, asof).timestamp() * 1000),
        )
        if isinstance(raw, pd.DataFrame):
            frame = raw
        elif raw and isinstance(raw[0], dict) and "timestamp" in raw[0]:
            frame = pd.DataFrame(raw)
        else:
            frame = klines_to_frame(raw)
        if not frame.empty and "close_time" not in frame.columns and "timestamp" in frame.columns:
            frame = frame.copy()
            frame["close_time"] = pd.to_datetime(frame["timestamp"], utc=True) + timedelta(minutes=5)
        return frame

    def _process_row(self, cohort: str, row: dict[str, Any], frame: pd.DataFrame, asof: datetime, stats: SchedulerStats) -> None:
        try:
            outcome = evaluate_shadow_outcome(
                observed_at=_utc(row["anchor_time"]),
                entry_price=float(row["anchor_price"]),
                event_high=row.get("event_high"),
                frame_5m=frame,
                market_asof=asof,
            )
            outcome = terminalize_due_horizons(
                anchor_time=_utc(row["anchor_time"]), market_asof=asof, outcome=outcome,
            )
            outcome["anchor_type"] = "LIVE_ROOT" if cohort == "live_root" else "SHADOW_V2_ROOT" if cohort == "v2" else "EPISODE_FIRST_SEEN"
            outcome["outcome_code_version"] = self._outcome_code_version
            next_due = next_outcome_due_at(
                _utc(row["anchor_time"]), asof, outcome,
                data_gap_retry_minutes=self._data_gap_retry_minutes,
            )
            if cohort == "v2":
                persisted = self._repository.record_root_detector_shadow_v2_outcome_attempt(
                    row["shadow_v2_root_id"], outcome=outcome, next_due_at=next_due, computed_at=asof,
                )
                if persisted is False:
                    raise RuntimeError("V2 outcome persistence returned failure")
            elif cohort == "live_root":
                persisted = self._repository.record_live_root_outcome_attempt(
                    root_event_id=row["root_event_id"], outcome=outcome, next_due_at=next_due, computed_at=asof,
                )
                if persisted is False:
                    raise RuntimeError("LIVE_ROOT outcome persistence returned failure")
            else:
                self._repository.record_root_detector_shadow_episode_outcome_attempt(
                    row["episode_id"], outcome=outcome, next_due_at=next_due, computed_at=asof,
                )
            if cohort == "runtime":
                stats.runtime_processed += 1
            elif cohort == "v2":
                stats.v2_processed += 1
            elif cohort == "live_root":
                stats.live_root_processed += 1
            else:
                stats.legacy_processed += 1
        except Exception:
            stats.errors += 1
            stats.retries += 1
            if cohort == "v2":
                stats.v2_errors += 1
            elif cohort == "live_root":
                stats.live_root_errors += 1
            try:
                self._record_error(row, asof, stats)
            except Exception:
                logger.exception("shadow outcome error bookkeeping failed episode_id=%s", row.get("episode_id"))
            logger.exception("shadow outcome computation failed episode_id=%s", row.get("episode_id"))

    def _record_error(self, row: dict[str, Any], asof: datetime, stats: SchedulerStats) -> None:
        attempt = int(row.get("attempt_count") or 0)
        delay = min(60, 2 ** min(attempt, 6))
        if row.get("shadow_v2_root_id"):
            self._repository.record_root_detector_shadow_v2_outcome_attempt(
                row["shadow_v2_root_id"],
                outcome={"outcome_status": "RETRYABLE_ERROR", "outcome_method_version": OUTCOME_METHOD_VERSION},
                next_due_at=asof + timedelta(minutes=delay), computed_at=asof,
                error="OUTCOME_SCHEDULER_ERROR",
            )
        elif row.get("root_event_id"):
            self._repository.record_live_root_outcome_attempt(
                root_event_id=row["root_event_id"],
                outcome={"outcome_status": "RETRYABLE_ERROR", "outcome_method_version": OUTCOME_METHOD_VERSION, "anchor_type": "LIVE_ROOT"},
                next_due_at=asof + timedelta(minutes=delay), computed_at=asof,
                error="OUTCOME_SCHEDULER_ERROR",
            )
        else:
            self._repository.record_root_detector_shadow_episode_outcome_attempt(
                row["episode_id"],
                outcome={"outcome_status": "RETRYABLE_ERROR", "outcome_method_version": OUTCOME_METHOD_VERSION},
                next_due_at=asof + timedelta(minutes=delay), computed_at=asof,
                error="OUTCOME_SCHEDULER_ERROR",
            )


def _group_by_symbol(rows: list[tuple[str, dict[str, Any]]]) -> dict[str, list[tuple[str, dict[str, Any]]]]:
    grouped: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for cohort, row in rows:
        grouped.setdefault(str(row["symbol"]).upper(), []).append((cohort, row))
    return grouped


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
