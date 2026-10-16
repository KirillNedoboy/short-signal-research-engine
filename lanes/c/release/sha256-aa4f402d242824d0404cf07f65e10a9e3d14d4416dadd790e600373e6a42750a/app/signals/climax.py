"""Deterministic, non-executing climax short evaluators."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from app.domain import EventState, SymbolFeatures
from app.market.candles import complete_5m_ohlcv, normalize_utc


@dataclass(slots=True)
class ClimaxEvaluation:
    subtype: str | None
    score: int
    grade: str
    metadata: dict[str, Any]
    veto_reasons: list[str]
    data_quality: list[str]

    @property
    def actionable(self) -> bool:
        return self.subtype is not None and not self.veto_reasons and self.grade in {"A", "B"}


@dataclass(slots=True)
class ClimaxEvaluationBundle:
    """All enabled climax branch evaluations and the unchanged selected result."""

    selected: ClimaxEvaluation
    branch_evaluations: dict[str, ClimaxEvaluation]


@dataclass(slots=True)
class VolumeClimaxLifecycle:
    """Shadow lifecycle decision for one root volume-climax event."""

    state: str
    event_revision: int
    root_created_at: datetime
    latest_high: float
    latest_high_at: datetime
    confirmation_started_at: datetime
    last_observed_at: datetime
    veto_reasons: list[str]
    expired: bool = False


def volume_climax_attempt_id(root_event_id: str, event_revision: int) -> str:
    return f"volume_climax:{root_event_id}:r{event_revision}:a1"


def advance_volume_climax_lifecycle(
    *,
    root_created_at: datetime,
    latest_high: float,
    latest_high_at: datetime,
    confirmation_started_at: datetime,
    last_observed_at: datetime,
    event_revision: int,
    current_high: float,
    observed_at: datetime,
    closed_candles_after_high: int,
    max_lifetime_minutes: int,
    confirmation_window_minutes: int,
    min_closed_candles_after_high: int = 2,
    price_acceleration_resumed: bool = False,
    active_short_squeeze: bool = False,
    oi_continuation: bool = False,
    rejection_ok: bool = False,
    liquidity_ok: bool = False,
    entry_distance_ok: bool = False,
) -> VolumeClimaxLifecycle:
    """Advance the bounded shadow lifecycle without mutating the root timestamp.

    A new high starts a new revision and confirmation window, but root lifetime
    always remains anchored to ``root_created_at``. Fallback is deliberately
    hard-gated and cannot be inferred from elapsed time alone.
    """
    if observed_at < root_created_at:
        raise ValueError("observed_at cannot precede root_created_at")
    if current_high <= 0 or latest_high <= 0:
        raise ValueError("high prices must be positive")

    root_age_minutes = (observed_at - root_created_at).total_seconds() / 60
    if root_age_minutes > max_lifetime_minutes:
        return VolumeClimaxLifecycle(
            state="EXPIRED",
            event_revision=event_revision,
            root_created_at=root_created_at,
            latest_high=latest_high,
            latest_high_at=latest_high_at,
            confirmation_started_at=confirmation_started_at,
            last_observed_at=observed_at,
            veto_reasons=["root_lifetime_expired"],
            expired=True,
        )

    if current_high > latest_high:
        return VolumeClimaxLifecycle(
            state="CLIMAX_WATCHING",
            event_revision=event_revision + 1,
            root_created_at=root_created_at,
            latest_high=current_high,
            latest_high_at=observed_at,
            confirmation_started_at=observed_at,
            last_observed_at=observed_at,
            veto_reasons=[],
        )

    reasons: list[str] = []
    confirmation_age_minutes = (observed_at - confirmation_started_at).total_seconds() / 60
    if closed_candles_after_high < min_closed_candles_after_high:
        reasons.append("insufficient_closed_candles_after_high")
    if confirmation_age_minutes < confirmation_window_minutes:
        reasons.append("confirmation_window_open")
    if price_acceleration_resumed:
        reasons.append("price_acceleration_resumed")
    if active_short_squeeze:
        reasons.append("active_short_squeeze")
    if oi_continuation:
        reasons.append("oi_continuation")
    if not rejection_ok:
        reasons.append("rejection_missing")
    if not liquidity_ok:
        reasons.append("liquidity_not_confirmed")
    if not entry_distance_ok:
        reasons.append("entry_distance_not_confirmed")

    return VolumeClimaxLifecycle(
        state="FALLBACK_READY" if not reasons else "CLIMAX_WATCHING",
        event_revision=event_revision,
        root_created_at=root_created_at,
        latest_high=latest_high,
        latest_high_at=latest_high_at,
        confirmation_started_at=confirmation_started_at,
        last_observed_at=observed_at,
        veto_reasons=reasons,
    )


def evaluate_climax(
    state: EventState,
    features: SymbolFeatures,
    frame: pd.DataFrame,
    config: Any,
    *,
    frozen_initial_extension_pct: float | None = None,
    current_ret5_gate_enabled: bool = True,
    strict_closed_candles: bool = False,
) -> ClimaxEvaluation:
    """Evaluate both climax patterns; required gates run before score.

    The optional extension arguments are used only by V3B shadow evaluation;
    defaults preserve the live evaluator semantics exactly.
    """
    return evaluate_climax_bundle(
        state,
        features,
        frame,
        config,
        frozen_initial_extension_pct=frozen_initial_extension_pct,
        current_ret5_gate_enabled=current_ret5_gate_enabled,
        strict_closed_candles=strict_closed_candles,
    ).selected


def evaluate_climax_bundle(
    state: EventState,
    features: SymbolFeatures,
    frame: pd.DataFrame,
    config: Any,
    *,
    frozen_initial_extension_pct: float | None = None,
    current_ret5_gate_enabled: bool = True,
    strict_closed_candles: bool = False,
) -> ClimaxEvaluationBundle:
    """Evaluate every enabled branch while retaining legacy selected behavior."""

    base = _common_metadata(
        state,
        features,
        frame,
        confirmation_window_minutes=getattr(config, "volume_climax_confirmation_window_minutes", 3),
        strict_closed_candles=strict_closed_candles,
    )
    branches: dict[str, ClimaxEvaluation] = {}
    candidates: list[ClimaxEvaluation] = []
    volume_candidate: ClimaxEvaluation | None = None
    if config.volume_climax_unwind_enabled:
        volume_candidate = _volume_climax(base, state, features, config)
        branches["VOLUME_CLIMAX_UNWIND"] = volume_candidate
        candidates.append(volume_candidate)
    if config.low_volume_extension_enabled:
        low_volume_candidate = _low_volume(
            base,
            state,
            features,
            config,
            frozen_initial_extension_pct=frozen_initial_extension_pct,
            current_ret5_gate_enabled=current_ret5_gate_enabled,
        )
        branches["LOW_VOLUME_EXTENSION_FAILURE"] = low_volume_candidate
        candidates.append(low_volume_candidate)
    def with_volume_context(selected: ClimaxEvaluation) -> ClimaxEvaluation:
        if volume_candidate is not None:
            selected.metadata["volume_climax_observed"] = True
            selected.metadata["volume_climax_candidate"] = not volume_candidate.veto_reasons
            selected.metadata["volume_climax_actionable"] = volume_candidate.actionable
            selected.metadata["volume_climax_score"] = volume_candidate.score
            selected.metadata["volume_climax_grade"] = volume_candidate.grade
            selected.metadata["volume_climax_veto_reasons"] = list(volume_candidate.veto_reasons)
            selected.metadata["volume_climax_metadata"] = dict(volume_candidate.metadata)
        return selected

    valid = [item for item in candidates if item.actionable]
    if valid:
        selected = with_volume_context(max(valid, key=lambda item: item.score))
        return ClimaxEvaluationBundle(selected=selected, branch_evaluations=branches)
    vetoed = [item for item in candidates if item.veto_reasons]
    if vetoed:
        selected = with_volume_context(max(vetoed, key=lambda item: item.score))
        return ClimaxEvaluationBundle(selected=selected, branch_evaluations=branches)
    selected = with_volume_context(ClimaxEvaluation(None, 0, "C", base, ["no_climax_admission"], []))
    return ClimaxEvaluationBundle(selected=selected, branch_evaluations=branches)


def _common_metadata(
    state: EventState,
    features: SymbolFeatures,
    frame: pd.DataFrame,
    *,
    confirmation_window_minutes: int = 3,
    strict_closed_candles: bool = False,
) -> dict[str, Any]:
    timestamps = pd.Series(pd.to_datetime(frame["timestamp"], utc=True, errors="coerce"), index=frame.index) if "timestamp" in frame else pd.Series(pd.to_datetime(frame.index, utc=True, errors="coerce"), index=frame.index)
    asof_utc = pd.Timestamp(features.asof).tz_convert("UTC")
    legacy_mask = timestamps <= asof_utc
    valid_timestamps = timestamps.dropna().sort_values()
    interval = valid_timestamps.diff().dropna().median() if len(valid_timestamps) >= 2 else pd.Timedelta(minutes=1)
    if pd.isna(interval) or interval <= pd.Timedelta(0):
        interval = pd.Timedelta(minutes=1)
    close_times = timestamps + interval
    strict_closed_mask = close_times <= asof_utc
    closed_mask = strict_closed_mask if strict_closed_candles else legacy_mask
    latest_closed = frame.loc[closed_mask].copy()
    last_closed_close_time = (
        close_times.loc[strict_closed_mask].max()
        if strict_closed_candles and bool(strict_closed_mask.any())
        else timestamps.loc[legacy_mask].max() if bool(legacy_mask.any()) else None
    )
    high = float(max(state.event_high or 0.0, float(latest_closed["high"].max()) if not latest_closed.empty else 0.0))
    previous_high = float(state.event_high or high)
    previous_window = latest_closed.iloc[-10:-5] if len(latest_closed) >= 10 else latest_closed.iloc[:-5]
    current_window = latest_closed.iloc[-5:] if len(latest_closed) >= 5 else latest_closed
    volume_windows_equal = len(previous_window) == len(current_window) == 5
    previous_leg_volume = float(previous_window["volume"].sum()) if not previous_window.empty else 0.0
    current_leg_volume = float(current_window["volume"].sum()) if not current_window.empty else 0.0
    leg_ratio = current_leg_volume / previous_leg_volume if previous_leg_volume > 0 else 0.0
    distance = ((high - features.price) / high * 100) if high else None
    rejection = float(features.rejection_from_high_pct)
    breakout_reference = previous_high * (1.0 - 0.005)
    event_high_time = pd.Timestamp(normalize_utc(state.event_high_time)) if state.event_high_time else None
    data_after_high = latest_closed.loc[timestamps.loc[latest_closed.index] > event_high_time] if event_high_time is not None else latest_closed.iloc[0:0]
    post_high_high = float(data_after_high["high"].max()) if not data_after_high.empty else None
    if event_high_time is not None:
        confirmation_end = event_high_time + timedelta(minutes=confirmation_window_minutes)
        confirmation_window = data_after_high.loc[timestamps.loc[data_after_high.index] <= confirmation_end]
    else:
        confirmation_window = data_after_high.iloc[0:0]
    post_high_count = len(confirmation_window)
    structural = complete_5m_ohlcv(frame, features.asof) if strict_closed_candles else latest_closed
    structural_timestamps = (
        pd.Series(pd.to_datetime(structural["timestamp"], utc=True, errors="coerce"), index=structural.index)
        if not structural.empty
        else pd.Series(dtype="datetime64[ns, UTC]")
    )
    structural_after_high = structural.loc[structural_timestamps > event_high_time] if event_high_time is not None else structural.iloc[0:0]
    if event_high_time is not None:
        structural_confirmation = structural_after_high.loc[
            structural_timestamps.loc[structural_after_high.index] <= confirmation_end
        ]
        retest_window = structural_after_high.loc[
            structural_timestamps.loc[structural_after_high.index] > confirmation_end
        ]
    else:
        structural_confirmation = structural_after_high.iloc[0:0]
        retest_window = structural_after_high.iloc[0:0]
    post_high_closes = structural_confirmation["close"].tolist() if "close" in structural_confirmation else []
    post_high_high = float(structural_after_high["high"].max()) if not structural_after_high.empty else None
    post_high_retest_high = float(retest_window["high"].max()) if not retest_window.empty else None
    local_prior = latest_closed.iloc[-8:-5] if len(latest_closed) >= 8 else latest_closed.iloc[:-5]
    local_swing_low = float(local_prior["low"].min()) if not local_prior.empty else None
    current_close = float(structural["close"].iloc[-1]) if not structural.empty else features.price
    return {
        "strategy_type": "CLIMAX_EXHAUSTION",
        "model_version": "climax-v1",
        "event_high": high,
        "previous_leg_high": previous_high,
        "current_leg_high": float(frame["high"].iloc[-1]),
        "previous_leg_volume": previous_leg_volume,
        "current_leg_volume": current_leg_volume,
        "current_previous_volume_ratio": leg_ratio,
        "volume_efficiency_ratio": leg_ratio,
        "entry_distance_below_high_pct": distance,
        "price_change_5m": features.ret_5m,
        "price_change_15m": features.ret_15m,
        "oi_change_5m": features.oi_change_pct,
        "volume_zscore": features.vol_zscore_30m,
        "rejection_pct": rejection,
        "funding": features.funding_rate,
        "premium": None,
        "spread_pct": features.spread_pct,
        "slippage_pct": features.slippage_pct,
        "depth_1pct_usdt": features.orderbook_depth_usdt_1pct,
        "depth_2pct_usdt": features.orderbook_depth_usdt_2pct,
        "liquidity_warning": False,
        "volume_windows_equal": volume_windows_equal,
        "volume_window_previous_candles": len(previous_window),
        "volume_window_current_candles": len(current_window),
        "closed_candles_after_high": post_high_count,
        "post_high_closes": post_high_closes,
        "partial_candle_excluded": bool(len(frame) and not bool(closed_mask.iloc[-1])),
        "last_closed_candle_close_time": last_closed_close_time.isoformat() if last_closed_close_time is not None else None,
        "post_high_high": post_high_high,
        "post_high_retest_high": post_high_retest_high,
        "breakout_reference": breakout_reference,
        "local_swing_low": local_swing_low,
        "current_close": current_close,
    }


def _volume_climax(base: dict[str, Any], state: EventState, features: SymbolFeatures, config: Any) -> ClimaxEvaluation:
    data = dict(base)
    data["strategy_subtype"] = "VOLUME_CLIMAX_UNWIND"
    volume_ratio = float(base.get("current_previous_volume_ratio") or _volume_ratio(features.current_volume, state))
    data["volume_ratio"] = volume_ratio
    data["current_previous_volume_ratio"] = volume_ratio
    data["failed_high_pct"] = data["rejection_pct"]
    vetoes: list[str] = []
    quality: list[str] = []
    if features.oi_change_pct is None or features.derivatives_status in {"MISSING", "RATE_LIMITED", "API_ERROR"}:
        vetoes.append("oi_missing_for_volume_climax")
    elif features.oi_change_pct >= 0 and features.ret_5m > 0:
        vetoes.append("price_oi_accelerating_together")
    if volume_ratio < config.volume_climax_min_volume_ratio and features.vol_zscore_30m < config.volume_climax_min_volume_zscore:
        vetoes.append("volume_climax_not_extreme")
    if features.ret_5m < config.volume_climax_min_ret_5m_pct:
        vetoes.append("price_acceleration_below_threshold")
    if features.ret_15m < config.volume_climax_min_ret_15m_pct:
        vetoes.append("pump_below_threshold")
    if data["rejection_pct"] < config.volume_climax_min_rejection_pct:
        vetoes.append("rejection_missing")
    if (data["entry_distance_below_high_pct"] or 999) > config.volume_climax_max_entry_distance_below_high_pct:
        vetoes.append("entry_too_far_from_high")
    score = _score([features.ret_5m >= config.volume_climax_min_ret_5m_pct, volume_ratio >= config.volume_climax_min_volume_ratio or features.vol_zscore_30m >= config.volume_climax_min_volume_zscore, (features.oi_change_pct or 0) < config.volume_climax_max_oi_change_5m_pct, data["rejection_pct"] >= config.volume_climax_min_rejection_pct, features.latest_failed_retest])
    grade = _grade(score)
    if not features.liquidity_available:
        vetoes.append("liquidity_not_confirmed")
    elif _liquidity_blocked(features, config, climax=True):
        vetoes.append("climax_liquidity_block")
    elif features.liquidity_available and _liquidity_warning(features, config):
        data["liquidity_warning"] = True
        grade = "B"
    return ClimaxEvaluation("VOLUME_CLIMAX_UNWIND" if not vetoes and score >= config.climax_min_signal_score else None, score, grade, data, vetoes, quality)


def _low_volume(
    base: dict[str, Any],
    state: EventState,
    features: SymbolFeatures,
    config: Any,
    *,
    frozen_initial_extension_pct: float | None = None,
    current_ret5_gate_enabled: bool = True,
) -> ClimaxEvaluation:
    data = dict(base)
    data["strategy_subtype"] = "LOW_VOLUME_EXTENSION_FAILURE"
    prev_volume = float(base.get("previous_leg_volume") or (state.event_features_snapshot or {}).get("previous_leg_volume") or max(features.current_volume, 1.0))
    current_volume = float(base.get("current_leg_volume") or features.current_volume)
    ratio = current_volume / prev_volume if prev_volume else 0.0
    efficiency = float(base.get("volume_efficiency_ratio") or ratio)
    data.update({"previous_leg_volume": prev_volume, "current_leg_volume": current_volume, "current_previous_volume_ratio": ratio, "volume_efficiency_ratio": efficiency, "failed_high_pct": data["rejection_pct"]})
    vetoes: list[str] = []
    quality: list[str] = []
    closed_count = int(data.get("closed_candles_after_high") or 0)
    post_high_retest_high = data.get("post_high_retest_high")
    high = float(data["event_high"])
    tol = float(config.low_volume_max_new_high_tolerance_pct)
    lower_high = post_high_retest_high is not None and float(post_high_retest_high) <= high * (1.0 + tol / 100.0)
    closes = [float(x) for x in data.get("post_high_closes", [])]
    close_below_breakout = bool(closes) and any(x < float(data["breakout_reference"]) for x in closes)
    micro_break = close_below_breakout
    failed_retest = bool(features.latest_failed_retest)
    if post_high_retest_high is not None:
        # A temporary dip is not enough: price must close materially below the
        # highest post-event retest. This preserves AKE's completed failure and
        # rejects ESPORTS while it is still recovering upward.
        failed_retest = failed_retest or float(data["current_close"]) < float(post_high_retest_high) * 0.997
    data["failed_retest_confirmed"] = failed_retest
    data["microstructure_break_confirmed"] = micro_break
    age_minutes = None
    if state.event_high_time:
        age_minutes = (pd.Timestamp(features.asof).tz_convert("UTC") - pd.Timestamp(state.event_high_time).tz_convert("UTC")).total_seconds() / 60
    data["minutes_after_high"] = age_minutes
    if not state.event_id:
        vetoes.append("no_established_event")
    if not data.get("volume_windows_equal"):
        vetoes.append("incomparable_volume_windows")
    if config.low_volume_require_closed_candles_only and closed_count < config.low_volume_min_closed_candles_after_high:
        vetoes.append("insufficient_closed_candles_after_high")
    if age_minutes is not None and age_minutes > getattr(config, "low_volume_max_minutes_after_high", 15):
        vetoes.append("confirmation_window_expired")
    if data.get("post_high_high") is not None and float(data["post_high_high"]) > high * (1.0 + tol / 100.0):
        vetoes.append("new_high_before_delivery")
    extension_value = features.ret_5m if current_ret5_gate_enabled else frozen_initial_extension_pct
    data["initial_extension_pct"] = frozen_initial_extension_pct
    data["extension_gate_value"] = extension_value
    if extension_value is None:
        vetoes.append("initial_extension_missing")
    elif extension_value < config.low_volume_min_price_extension_pct:
        quality.append("extension_below_threshold")
    if ratio > config.low_volume_max_current_previous_volume_ratio or efficiency > config.low_volume_max_volume_efficiency_ratio:
        vetoes.append("second_leg_volume_confirmed")
    if config.low_volume_require_close_below_breakout and not close_below_breakout:
        vetoes.append("close_not_below_breakout_reference")
    if config.low_volume_require_lower_high_or_failed_retest and not (lower_high and failed_retest):
        vetoes.append("lower_high_or_failed_retest_missing")
    if config.low_volume_require_microstructure_break and not micro_break:
        vetoes.append("microstructure_break_missing")
    if features.oi_change_pct is not None and features.oi_change_pct >= 1.0:
        vetoes.append("oi_accelerating_up")
    if config.low_volume_block_active_short_squeeze and features.oi_change_pct is not None and features.oi_change_pct < 0 and features.ret_5m > config.low_volume_min_price_extension_pct * 0.5 and not failed_retest:
        vetoes.append("active_short_squeeze")
    if config.low_volume_block_price_acceleration_resumed and features.ret_5m > 0 and not failed_retest:
        vetoes.append("price_acceleration_resumed")
    if data["rejection_pct"] < config.low_volume_min_rejection_pct:
        vetoes.append("rejection_missing")
    if (data["entry_distance_below_high_pct"] or 999) > config.low_volume_max_entry_distance_below_high_pct:
        vetoes.append("entry_too_far_from_high")
    score = _score([bool(state.event_id), ratio <= config.low_volume_max_current_previous_volume_ratio, efficiency <= config.low_volume_max_volume_efficiency_ratio, close_below_breakout, lower_high and failed_retest, micro_break, features.oi_change_pct is None or features.oi_change_pct < 0])
    grade = _grade(score)
    if not features.liquidity_available:
        vetoes.append("liquidity_not_confirmed")
    elif _liquidity_blocked(features, config, climax=True):
        vetoes.append("climax_liquidity_block")
    elif features.liquidity_available and _liquidity_warning(features, config):
        data["liquidity_warning"] = True
        if config.low_volume_high_liquidity_risk_mode == "block":
            vetoes.append("high_liquidity_risk")
        else:
            grade = "B"
            score = min(score, config.climax_min_signal_score)
    if features.oi_change_pct is None:
        data["oi_confirmation_state"] = "unavailable"
        grade = "B"
    elif features.oi_change_pct < 0:
        data["oi_confirmation_state"] = "possible_short_covering"
        if grade == "A":
            grade = "B"
    elif features.oi_change_pct < features.ret_5m:
        data["oi_confirmation_state"] = "slow"
    else:
        data["oi_confirmation_state"] = "flat"
    return ClimaxEvaluation("LOW_VOLUME_EXTENSION_FAILURE" if not vetoes and score >= config.climax_min_signal_score else None, score, grade, data, vetoes, quality)




def evaluate_climax_shadow(
    state: EventState,
    features: SymbolFeatures,
    frame: pd.DataFrame,
    config: Any,
) -> ClimaxEvaluation:
    """Evaluate the V3B frozen-extension variant without changing live rules."""
    snapshot = state.event_features_snapshot or {}
    frozen = snapshot.get("initial_extension_pct")
    try:
        frozen_value = float(frozen) if frozen is not None else None
    except (TypeError, ValueError):
        frozen_value = None
    return evaluate_climax(
        state,
        features,
        frame,
        config,
        frozen_initial_extension_pct=frozen_value,
        current_ret5_gate_enabled=False,
        strict_closed_candles=True,
    )


def _volume_ratio(current: float, state: EventState) -> float:
    prior = float((state.event_features_snapshot or {}).get("previous_leg_volume") or 0.0)
    return current / prior if prior > 0 else 3.0


def _score(flags: list[bool]) -> int:
    return min(100, int(sum(flags) * 15 + 10))


def _grade(score: int) -> str:
    return "A" if score >= 85 else "B" if score >= 70 else "C"


def _liquidity_blocked(features: SymbolFeatures, config: Any, climax: bool = False) -> bool:
    spread = config.climax_max_spread_pct if climax else config.max_spread_pct
    slip = config.climax_max_slippage_pct if climax else config.max_slippage_pct
    d1 = config.climax_min_depth_1pct_usdt if climax else config.min_orderbook_depth_usdt_1pct
    d2 = config.climax_min_depth_2pct_usdt if climax else config.min_orderbook_depth_usdt_2pct
    return bool((features.spread_pct is not None and features.spread_pct > spread) or (features.slippage_pct is not None and features.slippage_pct > slip) or (features.orderbook_depth_usdt_1pct is not None and features.orderbook_depth_usdt_1pct < d1) or (features.orderbook_depth_usdt_2pct is not None and features.orderbook_depth_usdt_2pct < d2))


def _liquidity_warning(features: SymbolFeatures, config: Any) -> bool:
    return bool((features.orderbook_depth_usdt_1pct or 0) < config.min_orderbook_depth_usdt_1pct or (features.orderbook_depth_usdt_2pct or 0) < config.min_orderbook_depth_usdt_2pct)
