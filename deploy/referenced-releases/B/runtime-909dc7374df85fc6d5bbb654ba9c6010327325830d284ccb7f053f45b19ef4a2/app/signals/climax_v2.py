"""Pure independent V2 climax hypotheses."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from math import isfinite, log

import pandas as pd

from app.market.candles import complete_5m_ohlcv
from app.signals.trapped_longs_v2 import ExplanationFactor, OiObservation


@dataclass(frozen=True, slots=True)
class ClimaxV2Input:
    symbol: str
    event_id: str
    event_base: float
    event_high: float
    event_high_at: datetime
    decision_at: datetime
    frame_1m: pd.DataFrame
    oi: tuple[OiObservation, ...]
    config: Mapping[str, float | int]
    frozen_breakout_reference: float | None = None


@dataclass(frozen=True, slots=True)
class ClimaxV2Result:
    identity: str
    actionable: bool
    blockers: tuple[str, ...]
    explanation: dict[str, ExplanationFactor]


def evaluate_volume_climax_v2(value: ClimaxV2Input) -> ClimaxV2Result:
    return _evaluate(value, "VOLUME_CLIMAX_V2")


def evaluate_low_volume_extension_failure_v2(value: ClimaxV2Input) -> ClimaxV2Result:
    return _evaluate(value, "LOW_VOLUME_EXTENSION_FAILURE_V2")


def _evaluate(value: ClimaxV2Input, kind: str) -> ClimaxV2Result:
    decision_at = _utc(value.decision_at)
    high_at = _utc(value.event_high_at)
    blockers: list[str] = []
    explanation: dict[str, ExplanationFactor] = {}
    candles = complete_5m_ohlcv(value.frame_1m, decision_at)
    high_bin = pd.Timestamp(high_at).floor("5min") + pd.Timedelta(minutes=5)
    pre_count = int(value.config.get("pre_leg_candles", 1))
    post_count = int(value.config.get("post_leg_candles", 1))
    history_count = int(value.config.get("robust_history_candles", pre_count))
    explanation["closed_5m"] = ExplanationFactor(len(candles), not candles.empty)
    age_minutes = (decision_at - high_at).total_seconds() / 60
    explanation["event_age"] = ExplanationFactor(
        age_minutes, 0 <= age_minutes <= float(value.config.get("max_age_minutes", 15))
    )
    if candles.empty:
        blockers.append("closed_5m_missing")
    if not explanation["event_age"].passed:
        blockers.append("event_age_invalid")
    if high_bin not in candles.index:
        blockers.append("event_high_bin_missing")
    if blockers:
        return _result(value, kind, blockers, explanation)

    high_position = candles.index.get_loc(high_bin)
    pre = candles.iloc[max(0, high_position - pre_count):high_position]
    post = candles.iloc[high_position:high_position + post_count]
    history = candles.iloc[max(0, high_position - history_count):high_position]
    explanation["event_high_anchored_legs"] = ExplanationFactor(
        {
            "high_close": high_bin.isoformat(),
            "pre_count": len(pre),
            "post_count": len(post),
            "robust_history_count": len(history),
            "pre_end": None if pre.empty else pre.index[-1].isoformat(),
            "post_start": None if post.empty else post.index[0].isoformat(),
        },
        len(pre) == pre_count and len(post) == post_count and len(history) == history_count,
    )
    if len(pre) != pre_count or len(post) != post_count or len(history) != history_count:
        blockers.append("bounded_candle_history_missing")
    selected_volumes = [*pre["volume"].tolist(), *post["volume"].tolist(), *history["volume"].tolist()]
    if not selected_volumes or any(not isfinite(float(volume)) or float(volume) <= 0 for volume in selected_volumes):
        blockers.append("volume_missing_or_invalid")
    if blockers:
        return _result(value, kind, blockers, explanation)

    logs = pd.Series([log(float(volume)) for volume in history["volume"]])
    median = float(logs.median())
    mad = float((logs - median).abs().median())
    event_volume = float(post["volume"].mean())
    explanation["robust_log_volume"] = ExplanationFactor(
        {"median": median, "mad": mad, "event_volume": event_volume}, mad > 0
    )
    if kind == "VOLUME_CLIMAX_V2":
        robust_z = None if mad == 0 else (log(event_volume) - median) / (1.4826 * mad)
        robust_passed = robust_z is not None and robust_z >= float(value.config.get("min_volume_z", 2.5))
        explanation["robust_volume_climax"] = ExplanationFactor(robust_z, robust_passed)
        if mad == 0:
            blockers.append("volume_mad_zero")
        elif not robust_passed:
            blockers.append("volume_not_climactic")
    else:
        _evaluate_low_volume(value, pre, post, explanation, blockers)

    _evaluate_oi(value, decision_at, kind, explanation, blockers)
    return _result(value, kind, blockers, explanation)


def _evaluate_low_volume(
    value: ClimaxV2Input,
    pre: pd.DataFrame,
    post: pd.DataFrame,
    explanation: dict[str, ExplanationFactor],
    blockers: list[str],
) -> None:
    extension = (value.event_high - value.event_base) / value.event_base * 100 if value.event_base > 0 else None
    extension_passed = extension is not None and extension >= float(value.config.get("min_extension_pct", 5))
    explanation["event_high_extension"] = ExplanationFactor(extension, extension_passed)
    if not extension_passed:
        blockers.append("extension_missing_or_below_threshold")
    contraction = float(post["volume"].mean()) / float(pre["volume"].mean())
    contraction_passed = contraction <= float(value.config.get("max_contraction_ratio", 0.7))
    explanation["volume_contraction"] = ExplanationFactor(contraction, contraction_passed)
    if not contraction_passed:
        blockers.append("volume_not_contracted")
    reference = value.frozen_breakout_reference
    below_reference = reference is not None and float(post["close"].iloc[-1]) < reference
    explanation["latest_close_below_frozen_reference"] = ExplanationFactor(reference, below_reference)
    if not below_reference:
        blockers.append("close_not_below_frozen_reference")


def _evaluate_oi(
    value: ClimaxV2Input,
    decision_at: datetime,
    kind: str,
    explanation: dict[str, ExplanationFactor],
    blockers: list[str],
) -> None:
    horizon_at = decision_at - timedelta(minutes=int(value.config.get("oi_horizon_minutes", 5)))
    tolerance = timedelta(minutes=float(value.config.get("oi_sample_tolerance_minutes", 5)))
    observations = sorted(value.oi, key=lambda item: _utc(item.observed_at))
    baseline = _asof_observation(observations, horizon_at, tolerance)
    latest = _asof_observation(observations, decision_at, tolerance)
    if baseline is None or latest is None:
        explanation["oi_horizon"] = ExplanationFactor("UNKNOWN", False)
        blockers.append("oi_predicate_not_met")
        return
    initial_value, final_value = float(baseline.value), float(latest.value)
    if not all(isfinite(item) and item > 0 for item in (initial_value, final_value)):
        explanation["oi_horizon"] = ExplanationFactor("UNKNOWN", False)
        blockers.append("oi_predicate_not_met")
        return
    change = (final_value - initial_value) / initial_value * 100
    passed = change <= float(value.config.get("max_oi_change_pct", -1))
    explanation["oi_horizon"] = ExplanationFactor(
        {
            "baseline_at": _utc(baseline.observed_at).isoformat(),
            "decision_at": _utc(latest.observed_at).isoformat(),
            "change_pct": change,
        },
        passed,
    )
    if kind == "VOLUME_CLIMAX_V2" and not passed:
        blockers.append("oi_predicate_not_met")


def _asof_observation(
    observations: list[OiObservation], target: datetime, tolerance: timedelta
) -> OiObservation | None:
    eligible = [item for item in observations if _utc(item.observed_at) <= target]
    if not eligible:
        return None
    selected = eligible[-1]
    if target - _utc(selected.observed_at) > tolerance:
        return None
    return selected


def _result(
    value: ClimaxV2Input,
    kind: str,
    blockers: list[str],
    explanation: dict[str, ExplanationFactor],
) -> ClimaxV2Result:
    return ClimaxV2Result(
        f"{kind.lower()}:{sha256(f'{value.symbol}:{value.event_id}:{kind}'.encode()).hexdigest()[:16]}",
        not blockers,
        tuple(dict.fromkeys(blockers)),
        explanation,
    )


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
