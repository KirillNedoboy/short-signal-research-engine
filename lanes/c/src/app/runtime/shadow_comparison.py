"""Bounded Lane C shadow capture and same-input comparison helpers.

The live runtime owns market acquisition and admission.  This module is only a
best-effort evidence boundary: it receives an already captured market snapshot,
the state immediately before evaluation, and the candidate result.  It never
opens a database, starts a process, calls a provider, or sends a notification.

Raw input is deliberately separate from comparison aggregates.  When sampling,
the byte budget, or a :class:`ShadowControl` request disables raw capture, the
small aggregate stream remains available and records the resulting UNKNOWN
reason.  That distinction is important for a shadow report: a missing input is
not evidence of parity.
"""

from __future__ import annotations

import dataclasses
import json
import math
import os
import threading
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_MAX_BYTES = 1 * 1024 * 1024 * 1024
DEFAULT_MAX_RECORD_BYTES = 256 * 1024
DEFAULT_MAX_AGGREGATE_BYTES = 64 * 1024 * 1024
RAW_FILENAME = "shadow-inputs.jsonl"
AGGREGATE_FILENAME = "shadow-aggregates.jsonl"

# Capture data is allowed to contain strategy metadata and exchange evidence,
# but never runtime credentials or filesystem details.  Configuration is
# filtered by the strategy allowlist below as a second, independent guard.
_SENSITIVE_KEY_PARTS = (
    "token",
    "secret",
    "password",
    "credential",
    "api_key",
    "apikey",
    "authorization",
    "cookie",
    "chat_id",
    "database_url",
    "db_url",
    "dsn",
    "private_key",
    "access_key",
    "endpoint",
    "file_path",
    "filepath",
    "path",
    "cwd",
    "home",
)

# Kept in this module so a recorder can be imported without importing the
# application config loader.  The set mirrors app.replay.config's explicit
# strategy fingerprint contract.  Unknown fields are excluded by construction.
STRATEGY_CONFIG_FIELDS = frozenset(
    {
        "event_ret_15m_min", "event_ret_1h_min", "event_ret_4h_min",
        "pullback_min_pct", "pullback_max_pct", "pullback_hold_vwap_min",
        "pullback_hold_range_floor_pct", "short_zone_mode",
        "short_zone_range_low_pct", "short_zone_range_high_pct",
        "short_zone_atr_low_mult", "short_zone_atr_high_mult",
        "dist_to_vwap_min", "event_dist_to_vwap_min", "upper_wick_min",
        "rejection_min", "vol_zscore_min", "event_dist_to_ema20_atr_min",
        "dist_to_ema20_atr_bonus", "rsi_bonus_level", "ret_1h_bonus_level",
        "ret_4h_bonus_level", "range_atr_bonus_level", "signal_expiry_minutes",
        "max_signal_age_minutes", "derivatives_enabled", "max_spread_pct",
        "max_slippage_pct", "min_orderbook_depth_usdt_1pct",
        "min_orderbook_depth_usdt_2pct", "enable_watch_candidates",
        "watch_min_score", "min_public_signal_grade", "send_grade_c_to_telegram",
        "grade_c_mode", "enable_squeeze_guard", "squeeze_guard_mode",
        "retest_not_failed_penalty", "shallow_pullback_penalty",
        "weak_rejection_penalty", "combined_early_short_risk_penalty",
        "climax_min_signal_score", "climax_min_public_grade", "climax_grade_a_score",
        "volume_climax_unwind_enabled", "volume_climax_min_ret_15m_pct",
        "volume_climax_min_volume_ratio", "volume_climax_min_volume_zscore",
        "volume_climax_min_ret_5m_pct", "volume_climax_max_oi_change_5m_pct",
        "volume_climax_min_rejection_pct",
        "volume_climax_max_entry_distance_below_high_pct",
        "volume_climax_confirmation_window_minutes",
        "volume_climax_min_closed_candles_after_high",
        "volume_climax_max_lifetime_minutes", "low_volume_extension_enabled",
        "low_volume_min_price_extension_pct",
        "low_volume_max_current_previous_volume_ratio",
        "low_volume_max_volume_efficiency_ratio", "low_volume_min_rejection_pct",
        "low_volume_max_entry_distance_below_high_pct",
        "low_volume_max_minutes_after_high",
        "low_volume_min_closed_candles_after_high",
        "low_volume_require_close_below_breakout",
        "low_volume_require_lower_high_or_failed_retest",
        "low_volume_require_microstructure_break",
        "low_volume_require_closed_candles_only",
        "low_volume_require_equal_volume_windows",
        "low_volume_block_price_acceleration_resumed",
        "low_volume_block_active_short_squeeze",
        "low_volume_high_liquidity_risk_mode", "low_volume_current_ret5_gate_enabled",
        "climax_max_spread_pct", "climax_max_slippage_pct",
        "climax_min_depth_1pct_usdt", "climax_min_depth_2pct_usdt",
        "trapped_longs_reversal_enabled", "trapped_longs_live_delivery_enabled",
        "trapped_longs_min_oi_change_15m_pct", "trapped_longs_min_closed_candles",
        "trapped_longs_max_lifetime_minutes", "trapped_longs_new_high_tolerance_pct",
        "trapped_longs_min_rejection_pct", "trapped_longs_min_signal_score",
        "trapped_longs_min_public_grade", "trapped_longs_max_spread_pct",
        "trapped_longs_max_slippage_pct", "trapped_longs_min_depth_1pct_usdt",
        "trapped_longs_min_depth_2pct_usdt",
    }
)

# These flags select an already allowlisted evaluator branch.  They are kept
# separate from the fingerprint payload because the repository's established
# strategy fingerprint intentionally excludes runtime scheduling controls.  A
# replay still needs the branch selection to reproduce the captured call.
REPLAY_EXECUTION_FIELDS = frozenset(
    {
        "climax_short_enabled",
        "climax_fast_monitor_enabled",
        "volume_climax_lifecycle_shadow_enabled",
        "climax_fresh_recheck_attempts",
        "climax_fresh_recheck_retry_delay_sec",
        "low_volume_block_new_high_before_delivery",
        "low_volume_confirmation_window_minutes",
        "low_volume_frozen_initial_extension_enabled",
        "low_volume_frozen_initial_extension_shadow_only",
        "trapped_longs_reversal_enabled",
    }
)


class _DropValue:
    pass


_DROP = _DropValue()


def _key_is_sensitive(key: object) -> bool:
    normalized = str(key).strip().casefold().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def _enum_value(value: Enum) -> Any:
    return value.value


def _safe_jsonable(value: Any, *, key: str | None = None) -> Any:
    """Convert supported values to JSON while dropping unsafe fields.

    Returning ``_DROP`` instead of a redaction string prevents a consumer from
    mistaking a redacted credential/path for a real replay input.
    """

    if key is not None and _key_is_sensitive(key):
        return _DROP
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, (datetime, date)):
        if isinstance(value, datetime) and value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.isoformat()
    if isinstance(value, Enum):
        return _safe_jsonable(_enum_value(value), key=key)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Path):
        return _DROP
    if isinstance(value, bytes):
        return _DROP
    if dataclasses.is_dataclass(value):
        output: dict[str, Any] = {}
        for item in dataclasses.fields(value):
            child = _safe_jsonable(getattr(value, item.name), key=item.name)
            if child is not _DROP:
                output[item.name] = child
        return output
    if isinstance(value, Mapping):
        output = {}
        for raw_key, raw_value in value.items():
            child_key = str(raw_key)
            child = _safe_jsonable(raw_value, key=child_key)
            if child is not _DROP:
                output[child_key] = child
        return output
    if isinstance(value, (tuple, list, set, frozenset)):
        output = []
        for item in value:
            child = _safe_jsonable(item)
            if child is not _DROP:
                output.append(child)
        return output
    # Numpy scalar values are common in pandas rows.  Avoid importing numpy as
    # a runtime dependency by using its stable scalar conversion hook.
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _safe_jsonable(item(), key=key)
        except (TypeError, ValueError):
            return None
    return _DROP


def _json_bytes(value: Any) -> bytes:
    safe = _safe_jsonable(value)
    if safe is _DROP:
        safe = None
    return json.dumps(
        safe,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if dataclasses.is_dataclass(value):
        return {item.name: getattr(value, item.name) for item in dataclasses.fields(value)}
    if value is None:
        return {}
    return {
        name: getattr(value, name)
        for name in dir(value)
        if not name.startswith("_") and not callable(getattr(value, name, None))
    }


def _get(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _frame_payload(frame_holder: Any) -> dict[str, Any] | None:
    if frame_holder is None:
        return None
    source = _get(frame_holder, "source")
    captured = _get(frame_holder, "captured_at_utc", _get(frame_holder, "captured_at"))
    available = _get(
        frame_holder,
        "max_input_availability_time_utc",
        _get(frame_holder, "max_input_availability_time"),
    )
    frame = _get(frame_holder, "frame", frame_holder)
    # A previously serialized frame is already safe and should remain readable.
    if isinstance(frame, Mapping) and "rows" in frame:
        payload = dict(frame)
        if source is not None:
            payload.setdefault("source", source)
        if captured is not None:
            payload.setdefault("captured_at_utc", captured)
        if available is not None:
            payload.setdefault("max_input_availability_time_utc", available)
        safe = _safe_jsonable(payload)
        return None if safe is _DROP else safe
    if not hasattr(frame, "columns") or not hasattr(frame, "to_dict"):
        safe = _safe_jsonable(frame)
        if safe is _DROP:
            return None
        return {"value": safe}
    try:
        columns = [str(item) for item in frame.columns]
        rows = frame.to_dict(orient="records")
        index = list(frame.index)
        payload = {
            "columns": columns,
            "rows": rows,
            "index": index,
            "index_name": getattr(frame.index, "name", None),
            "source": source,
            "captured_at_utc": captured,
            "max_input_availability_time_utc": available,
        }
    except Exception:
        return None
    safe = _safe_jsonable(payload)
    return None if safe is _DROP else safe


def _market_snapshot_payload(snapshot: Any) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    frame_holder = _get(snapshot, "frame_1m", _get(snapshot, "frame"))
    payload = {
        "symbol": _get(snapshot, "symbol"),
        "frame_1m": _frame_payload(frame_holder),
        "derivatives": deepcopy(_get(snapshot, "derivatives", {})) or {},
        "liquidity": deepcopy(_get(snapshot, "liquidity")),
        "captured_at_utc": _get(snapshot, "captured_at_utc", _get(snapshot, "asof")),
        "max_input_availability_time_utc": _get(
            snapshot,
            "max_input_availability_time_utc",
            _get(snapshot, "max_input_availability_time"),
        ),
    }
    ticker = _get(snapshot, "ticker_group")
    if ticker is not None:
        payload["ticker_group"] = _safe_jsonable(ticker)
    safe = _safe_jsonable(payload)
    return None if safe is _DROP else safe


def _strategy_config_payload(config: Any) -> dict[str, Any]:
    source = _as_mapping(config)
    return {
        key: _safe_jsonable(source[key], key=key)
        for key in sorted(STRATEGY_CONFIG_FIELDS)
        if key in source and _safe_jsonable(source[key], key=key) is not _DROP
    }


def _context_payload(context: Any) -> dict[str, Any]:
    source = _as_mapping(context)
    allowed = (
        "decision_at",
        "decision_at_utc",
        "fast_monitor",
        "poll_sequence",
        "input_quality",
        "final_recheck_captured",
        "final_recheck_required",
        "final_recheck_status",
        "final_recheck_reason",
        "schedule_classification",
        "trapped_attempt_created_at",
        "trapped_confirmation_expires_at",
    )
    output: dict[str, Any] = {}
    for name in allowed:
        if name in source:
            safe = _safe_jsonable(source[name], key=name)
            if safe is not _DROP:
                output[name] = safe
    config = source.get("strategy_config", source.get("config"))
    payload = source.get("strategy_config_payload")
    if payload is None and config is not None:
        payload = _strategy_config_payload(config)
    elif payload is not None:
        payload = _strategy_config_payload(payload)
    output["strategy_config_payload"] = payload or {}
    if config is not None:
        config_source = _as_mapping(config)
        output["evaluation_flags"] = {
            name: _safe_jsonable(config_source[name], key=name)
            for name in sorted(REPLAY_EXECUTION_FIELDS)
            if name in config_source
            and _safe_jsonable(config_source[name], key=name) is not _DROP
        }
    elif isinstance(source.get("evaluation_flags"), Mapping):
        output["evaluation_flags"] = _safe_jsonable(source["evaluation_flags"]) or {}
    fingerprint = source.get(
        "strategy_config_fingerprint",
        source.get("config_fingerprint", source.get("strategy_config_hash")),
    )
    if fingerprint is not None:
        safe = _safe_jsonable(fingerprint, key="strategy_config_fingerprint")
        if safe is not _DROP:
            output["strategy_config_fingerprint"] = safe
    # Preserve only an explicitly captured fresh snapshot.  This is usually
    # supplied by the pipeline integration when low-volume admission rechecks.
    recheck = source.get("final_recheck")
    if recheck is not None:
        output["final_recheck"] = _market_snapshot_payload(recheck)
    elif source.get("final_recheck_snapshot") is not None:
        output["final_recheck"] = _market_snapshot_payload(source["final_recheck_snapshot"])
    return output


def _result_payload(result: Any) -> dict[str, Any] | None:
    if result is None:
        return None
    safe = _safe_jsonable(deepcopy(result))
    if safe is _DROP:
        return None
    if isinstance(safe, dict):
        return safe
    return {"value": safe}


def _decision_from_result(result: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    if not result:
        return None
    decision = result.get("decision")
    return decision if isinstance(decision, Mapping) else None


def _identity(result: Mapping[str, Any] | None) -> dict[str, Any] | None:
    decision = _decision_from_result(result)
    if decision is None:
        return None
    return {
        name: decision.get(name)
        for name in (
            "symbol",
            "event_id",
            "strategy_type",
            "strategy_subtype",
            "model_version",
        )
    }


@dataclass(slots=True)
class ShadowCaptureStats:
    seen: int = 0
    captured: int = 0
    sampled_out: int = 0
    dropped_record_bytes: int = 0
    dropped_budget: int = 0
    dropped_control: int = 0
    dropped_oversize: int = 0
    unknown_inputs: int = 0
    unknown_final_recheck: int = 0
    serialization_errors: int = 0
    write_errors: int = 0
    aggregate_captured: int = 0
    aggregate_dropped: int = 0
    bytes_written: int = 0
    aggregate_bytes_written: int = 0
    redacted_fields: int = 0

    def as_dict(self) -> dict[str, int]:
        return dataclasses.asdict(self)


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    status: str
    comparisons: Mapping[str, bool]
    unknown_reasons: tuple[str, ...] = ()
    schedule_comparison: str = "UNSUPPORTED"
    historical_telegram_comparison: str = "UNSUPPORTED"

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "comparisons": dict(self.comparisons),
            "unknown_reasons": list(self.unknown_reasons),
            "schedule_comparison": self.schedule_comparison,
            "historical_telegram_comparison": self.historical_telegram_comparison,
        }


def compare_results(
    old: Mapping[str, Any] | None,
    candidate: Mapping[str, Any] | None,
    *,
    input_status: str = "captured",
    final_recheck_required: bool = False,
    final_recheck_captured: bool = True,
    error: str | None = None,
) -> ComparisonResult:
    """Compare semantic replay results and refuse to turn missing evidence into PASS."""

    unknown: list[str] = []
    if input_status != "captured":
        unknown.append(f"input_{input_status}")
    if error:
        unknown.append("replay_error")
    if final_recheck_required and not final_recheck_captured:
        unknown.append("final_fresh_recheck_not_captured")
    projections = {
        "features": (old or {}).get("features") == (candidate or {}).get("features"),
        "state": (old or {}).get("state") == (candidate or {}).get("state"),
        "decision": (old or {}).get("decision") == (candidate or {}).get("decision"),
        "evaluations": (old or {}).get("evaluations") == (candidate or {}).get("evaluations"),
        "identity": (old or {}).get("identity") == (candidate or {}).get("identity"),
        "actionable": (old or {}).get("actionable") == (candidate or {}).get("actionable"),
        "blocked_reason": (old or {}).get("blocked_reason") == (candidate or {}).get("blocked_reason"),
    }
    if unknown:
        status = "UNKNOWN"
    elif not all(projections.values()):
        status = "MISMATCH"
    else:
        status = "PASS"
    return ComparisonResult(
        status=status,
        comparisons=projections,
        unknown_reasons=tuple(dict.fromkeys(unknown)),
    )


class ShadowCaptureRecorder:
    """Best-effort bounded writer for raw shadow inputs and aggregate evidence."""

    def __init__(
        self,
        root: str | Path,
        *,
        sample_every: int = 1,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_record_bytes: int = DEFAULT_MAX_RECORD_BYTES,
        max_aggregate_bytes: int = DEFAULT_MAX_AGGREGATE_BYTES,
        control: Any | None = None,
    ) -> None:
        if sample_every < 1:
            raise ValueError("sample_every must be positive")
        if max_bytes < 0 or max_record_bytes < 1 or max_aggregate_bytes < 1:
            raise ValueError("capture byte limits must be non-negative/positive")
        self.root = Path(root)
        self.raw_root = self.root / "raw"
        self.comparison_root = self.root / "comparisons"
        self.raw_path = self.raw_root / RAW_FILENAME
        self.aggregate_path = self.comparison_root / AGGREGATE_FILENAME
        self.sample_every = int(sample_every)
        self.max_bytes = int(max_bytes)
        self.max_record_bytes = int(max_record_bytes)
        self.max_aggregate_bytes = int(max_aggregate_bytes)
        self.control = control
        self.stats = ShadowCaptureStats()
        self._raw_capture_enabled = True
        self._raw_disabled_reason: str | None = None
        self._control_disable_notified = False
        self._lock = threading.Lock()

    @property
    def raw_capture_enabled(self) -> bool:
        return self._raw_capture_enabled

    @property
    def raw_disabled_reason(self) -> str | None:
        return self._raw_disabled_reason

    def stop_raw_capture(self, reason: str = "requested") -> None:
        """Stop only raw writes; aggregate comparison accounting remains active."""

        self._raw_capture_enabled = False
        self._raw_disabled_reason = reason
        disable = getattr(self.control, "disable_raw_capture", None)
        if callable(disable) and not self._control_disable_notified:
            self._control_disable_notified = True
            try:
                disable(reason=reason)
            except TypeError:
                try:
                    disable()
                except Exception:
                    self.stats.write_errors += 1
            except Exception:
                # Storage/control failures never re-enable raw capture or
                # escape into the P0 evaluation path.
                self.stats.write_errors += 1

    disable_raw_capture = stop_raw_capture

    def _poll_control(self) -> None:
        if self.control is None:
            return
        poll = getattr(self.control, "poll_control", None)
        if callable(poll):
            try:
                action = poll()
            except Exception:
                self.stats.write_errors += 1
                action = None
            if action in {"disable_raw_capture", "raw_capture_disabled"}:
                self.stop_raw_capture("control")
        enabled = getattr(self.control, "raw_capture_enabled", None)
        if enabled is False:
            self.stop_raw_capture("control")

    def _ensure_dirs(self) -> None:
        self.raw_root.mkdir(parents=True, exist_ok=True)
        self.comparison_root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _input_completeness(market: Mapping[str, Any] | None, before: Any) -> tuple[str, list[str]]:
        reasons: list[str] = []
        if not market:
            reasons.append("market_snapshot_missing")
        elif not market.get("frame_1m"):
            reasons.append("frame_1m_missing")
        if before is None:
            reasons.append("before_state_missing")
        return ("captured" if not reasons else "unknown", reasons)

    def _build_record(
        self,
        before_state: Any,
        decision_market: Any,
        result: Any,
        context: Any,
    ) -> dict[str, Any]:
        # Copy while still on the caller's side of the queue.  State lifecycle
        # code is allowed to mutate its working copy after this method returns.
        before_copy = deepcopy(before_state)
        market_copy = deepcopy(decision_market)
        result_copy = deepcopy(result)
        context_copy = deepcopy(context) if context is not None else {}
        market = _market_snapshot_payload(market_copy)
        context_payload = _context_payload(context_copy)
        result_payload = _result_payload(result_copy)
        input_status, reasons = self._input_completeness(market, before_copy)
        decision = _decision_from_result(result_payload)
        subtype = decision.get("strategy_subtype") if decision else None
        # The final recheck may veto the original decision, leaving the
        # terminal result without a decision subtype.  Preserve the caller's
        # explicit requirement bit so a missing fresh snapshot remains
        # UNKNOWN instead of being mistaken for a complete capture.
        final_required = bool(
            context_payload.get("final_recheck_required")
            or subtype == "LOW_VOLUME_EXTENSION_FAILURE"
        )
        final_captured = bool(
            context_payload.get("final_recheck_captured")
            or context_payload.get("final_recheck")
        )
        if final_required and not final_captured:
            self.stats.unknown_final_recheck += 1
        if input_status != "captured":
            self.stats.unknown_inputs += 1
        if final_required and not final_captured:
            reasons.append("final_fresh_recheck_not_captured")
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "shadow_capture",
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "input_status": input_status,
            "input_unknown_reasons": list(dict.fromkeys(reasons)),
            "symbol": (market or {}).get("symbol") or _get(before_copy, "symbol"),
            "market": market,
            "before_state": _safe_jsonable(before_copy),
            "fast_monitor": {
                "enabled": bool(context_payload.get("fast_monitor", False)),
                "poll_sequence": context_payload.get("poll_sequence"),
            },
            "strategy_config": {
                "fingerprint": context_payload.get("strategy_config_fingerprint"),
                "payload": context_payload.get("strategy_config_payload", {}),
            },
            "context": context_payload,
            "candidate_result": result_payload,
            "final_recheck": context_payload.get("final_recheck"),
            "final_recheck_required": final_required,
            "final_recheck_captured": final_captured,
            "candidate_identity": _identity(result_payload),
        }

    def _write_line(self, path: Path, encoded: bytes, limit: int) -> bool:
        try:
            current = path.stat().st_size if path.exists() else 0
            if current + len(encoded) > limit:
                return False
            with path.open("ab") as handle:
                handle.write(encoded)
            return True
        except (OSError, ValueError):
            self.stats.write_errors += 1
            return False

    def _aggregate(self, record: Mapping[str, Any], *, status: str, reasons: Sequence[str] = ()) -> None:
        aggregate = {
            "schema_version": SCHEMA_VERSION,
            "kind": "shadow_aggregate",
            "at_utc": datetime.now(timezone.utc).isoformat(),
            "symbol": record.get("symbol"),
            "input_status": record.get("input_status", "unknown"),
            "candidate_identity": record.get("candidate_identity"),
            "candidate_actionable": bool(
                (record.get("candidate_result") or {}).get("actionable", False)
            ),
            "final_recheck_required": bool(record.get("final_recheck_required")),
            "final_recheck_captured": bool(record.get("final_recheck_captured")),
            "status": status,
            "unknown_reasons": list(reasons) or list(record.get("input_unknown_reasons", ())),
            "raw_capture_enabled": self.raw_capture_enabled,
            "raw_disabled_reason": self.raw_disabled_reason,
        }
        try:
            encoded = _json_bytes(aggregate) + b"\n"
        except (TypeError, ValueError):
            self.stats.serialization_errors += 1
            return
        try:
            self._ensure_dirs()
        except OSError:
            self.stats.write_errors += 1
            self.stats.aggregate_dropped += 1
            return
        if self._write_line(self.aggregate_path, encoded, self.max_aggregate_bytes):
            self.stats.aggregate_captured += 1
            self.stats.aggregate_bytes_written += len(encoded)
        else:
            self.stats.aggregate_dropped += 1

    def record(
        self,
        before_state: Any,
        decision_market: Any,
        result: Any,
        context: Any | None = None,
    ) -> bool:
        """Queue-friendly capture API used after candidate evaluation.

        ``True`` means a raw record was written.  A ``False`` result can mean
        sampling, a control/byte-budget stop, or serialization failure; the
        aggregate stream retains the reason and the counters expose which case
        occurred.  The method never raises for a best-effort storage failure.
        """

        with self._lock:
            self.stats.seen += 1
            self._poll_control()
            if self.stats.seen % self.sample_every != 0:
                self.stats.sampled_out += 1
                # Build only the tiny aggregate shell; raw fields are not copied
                # when deterministic sampling excludes this event.
                shell = {
                    "symbol": _get(decision_market, "symbol", _get(before_state, "symbol")),
                    "input_status": "sampled_out",
                    "candidate_identity": None,
                    "candidate_result": {"actionable": False},
                    "final_recheck_required": False,
                    "final_recheck_captured": False,
                }
                self._aggregate(shell, status="UNKNOWN", reasons=("input_sampled_out",))
                return False
            try:
                record = self._build_record(before_state, decision_market, result, context)
                encoded = _json_bytes(record) + b"\n"
            except (TypeError, ValueError, OSError, RecursionError):
                self.stats.serialization_errors += 1
                self.stats.dropped_record_bytes += 1
                return False
            if len(encoded) > self.max_record_bytes:
                self.stats.dropped_oversize += 1
                self.stats.dropped_record_bytes += len(encoded)
                self._aggregate(record, status="UNKNOWN", reasons=("raw_record_oversize",))
                return False
            if not self.raw_capture_enabled:
                self.stats.dropped_control += 1
                self._aggregate(record, status="UNKNOWN", reasons=(self.raw_disabled_reason or "raw_capture_disabled",))
                return False
            if self.stats.bytes_written + len(encoded) > self.max_bytes:
                self.stats.dropped_budget += 1
                self.stats.dropped_record_bytes += len(encoded)
                self.stop_raw_capture("byte_budget")
                self._aggregate(record, status="UNKNOWN", reasons=("raw_byte_budget_exhausted",))
                return False
            try:
                self._ensure_dirs()
            except OSError:
                self.stats.write_errors += 1
                self._aggregate(record, status="UNKNOWN", reasons=("capture_directory_unavailable",))
                return False
            if not self._write_line(self.raw_path, encoded, self.max_bytes):
                self.stats.dropped_budget += 1
                self.stats.dropped_record_bytes += len(encoded)
                self.stop_raw_capture("byte_budget")
                self._aggregate(record, status="UNKNOWN", reasons=("raw_write_rejected",))
                return False
            self.stats.captured += 1
            self.stats.bytes_written += len(encoded)
            self._aggregate(record, status="CAPTURED")
            return True

    def record_comparison(self, record: Mapping[str, Any], comparison: ComparisonResult | Mapping[str, Any]) -> None:
        """Append a post-replay aggregate without reopening raw capture."""

        status = comparison.status if isinstance(comparison, ComparisonResult) else str(comparison.get("status", "UNKNOWN"))
        reasons = comparison.unknown_reasons if isinstance(comparison, ComparisonResult) else tuple(comparison.get("unknown_reasons", ()))
        self._aggregate(record, status=status, reasons=reasons)

    def report(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "raw_capture_enabled": self.raw_capture_enabled,
            "raw_disabled_reason": self.raw_disabled_reason,
            "limits": {
                "max_bytes": self.max_bytes,
                "max_record_bytes": self.max_record_bytes,
                "max_aggregate_bytes": self.max_aggregate_bytes,
                "sample_every": self.sample_every,
            },
            "stats": self.stats.as_dict(),
            "paths": {
                "raw_records": str(self.raw_path.name),
                "aggregates": str(self.aggregate_path.name),
            },
            "limitations": [
                "Raw records are sampled and byte bounded.",
                "Missing raw input or an uncaptured fresh final recheck is UNKNOWN.",
                "Historical Telegram scheduling cannot be proven from same-input replay.",
                "No 24-hour shadow completion is claimed by this report.",
            ],
        }


# Short aliases keep the integration seam readable while preserving one
# implementation and one set of counters.
ShadowRecorder = ShadowCaptureRecorder
CaptureRecorder = ShadowCaptureRecorder


__all__ = [
    "AGGREGATE_FILENAME",
    "CaptureRecorder",
    "ComparisonResult",
    "RAW_FILENAME",
    "SCHEMA_VERSION",
    "STRATEGY_CONFIG_FIELDS",
    "ShadowCaptureRecorder",
    "ShadowCaptureStats",
    "ShadowRecorder",
    "compare_results",
]
