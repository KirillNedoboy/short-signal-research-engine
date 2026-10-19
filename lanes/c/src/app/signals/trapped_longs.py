"""Independent TRAPPED_LONGS_REVERSAL evaluator and lifecycle."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd

from app.domain import EventState, SymbolFeatures

TRAPPED_LONGS_REVERSAL = "TRAPPED_LONGS_REVERSAL"
TRAPPED_LONGS_MODEL_VERSION = "trapped-longs-v1"


@dataclass(slots=True)
class TrappedLongsEvaluation:
    subtype: str | None
    score: int
    grade: str
    metadata: dict[str, Any]
    veto_reasons: list[str]
    data_quality: list[str]

    @property
    def actionable(self) -> bool:
        return self.subtype == TRAPPED_LONGS_REVERSAL and not self.veto_reasons and self.grade in {"A", "B"}


@dataclass(slots=True)
class TrappedLongsLifecycle:
    state: str
    event_revision: int
    root_created_at: datetime
    breakout_at: datetime
    last_observed_at: datetime
    veto_reasons: list[str]
    expired: bool = False


def _cfg(config: Any, name: str, default: Any) -> Any:
    return getattr(config, name, default)


def evaluate_trapped_longs_reversal(
    state: EventState,
    features: SymbolFeatures,
    frame: pd.DataFrame,
    config: Any,
    *,
    attempt_created_at: datetime | None = None,
    confirmation_expires_at: datetime | None = None,
    decision_time: datetime | None = None,
) -> TrappedLongsEvaluation:
    """Evaluate only the trapped-long reversal contract; never mutates ``state``."""
    metadata: dict[str, Any] = {"strategy_type": "TRAPPED_LONGS", "strategy_subtype": TRAPPED_LONGS_REVERSAL, "model_version": TRAPPED_LONGS_MODEL_VERSION}
    vetoes: list[str] = []
    reference = (state.event_features_snapshot or {}).get("breakout_reference")
    if reference is None or not state.event_id:
        vetoes.append("breakout_reference_missing" if reference is None else "no_established_event")
        reference = float(reference or 0)
    reference = float(reference)
    metadata["breakout_reference"] = reference
    event_high = float(state.event_high or 0)
    metadata["event_high"] = event_high
    if reference <= 0 or event_high <= 0:
        vetoes.append("invalid_breakout_reference")

    asof = pd.Timestamp(features.asof).tz_convert("UTC")
    if "timestamp" in frame and not frame.empty:
        timestamps = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
        closed = frame.loc[(timestamps + pd.Timedelta(minutes=5)) <= asof]
        breakout_time = pd.Timestamp(state.event_high_time or state.event_start_time or features.asof)
        breakout_time = breakout_time.tz_localize("UTC") if breakout_time.tzinfo is None else breakout_time.tz_convert("UTC")
        after_breakout = closed.loc[timestamps.loc[closed.index] > breakout_time]
    else:
        after_breakout = frame.iloc[0:0]
    closed_count = len(after_breakout)
    metadata["closed_structural_candles"] = closed_count
    if closed_count < int(_cfg(config, "trapped_longs_min_closed_candles", 2)):
        vetoes.append("insufficient_closed_structural_candles")

    observed_high = max([event_high, float(features.last_high or 0)] + ([float(after_breakout["high"].max())] if not after_breakout.empty else []))
    tolerance = float(_cfg(config, "trapped_longs_new_high_tolerance_pct", 0.30))
    if observed_high > event_high * (1 + tolerance / 100):
        vetoes.append("new_high_before_delivery")
    breakout = observed_high > reference
    if not breakout:
        vetoes.append("breakout_not_confirmed")
    close = float(features.last_close or features.price)
    if close >= reference:
        vetoes.append("close_not_below_breakout_reference")
    if not features.latest_failed_retest:
        vetoes.append("failed_retest_missing")
    rejection = float(features.rejection_from_high_pct or 0)
    if rejection < float(_cfg(config, "trapped_longs_min_rejection_pct", 1.0)):
        vetoes.append("rejection_below_threshold")

    oi = features.oi_change_15m
    metadata["oi_change_15m_pct"] = oi
    if oi is None or features.derivatives_status in {"MISSING", "RATE_LIMITED", "API_ERROR"}:
        vetoes.append("oi_missing")
    elif oi < float(_cfg(config, "trapped_longs_min_oi_change_15m_pct", 1.0)):
        vetoes.append("oi_below_threshold")

    if not features.liquidity_available:
        vetoes.append("liquidity_unavailable")
    else:
        if features.spread_pct is None or features.slippage_pct is None or features.orderbook_depth_usdt_1pct is None or features.orderbook_depth_usdt_2pct is None:
            vetoes.append("liquidity_incomplete")
        elif (features.spread_pct > _cfg(config, "trapped_longs_max_spread_pct", .80) or features.slippage_pct > _cfg(config, "trapped_longs_max_slippage_pct", 1.0) or features.orderbook_depth_usdt_1pct < _cfg(config, "trapped_longs_min_depth_1pct_usdt", 5000) or features.orderbook_depth_usdt_2pct < _cfg(config, "trapped_longs_min_depth_2pct_usdt", 10000)):
            vetoes.append("liquidity_block")

    if (
        attempt_created_at is not None
        and confirmation_expires_at is not None
        and decision_time is not None
    ):
        vetoes.extend(
            trapped_longs_expiry_reasons(
                attempt_created_at=attempt_created_at,
                confirmation_expires_at=confirmation_expires_at,
                decision_time=decision_time,
            )
        )

    flags = [bool(state.event_id), breakout, closed_count >= 2, close < reference, bool(features.latest_failed_retest), oi is not None and oi >= 1.0, not any(r == "new_high_before_delivery" for r in vetoes)]
    score = min(100, 10 + 15 * sum(flags))
    grade = "A" if score >= 85 else "B" if score >= 70 else "C"
    metadata.update({"close_below_breakout_reference": close < reference, "failed_retest_confirmed": bool(features.latest_failed_retest), "no_new_high": "new_high_before_delivery" not in vetoes, "rejection_pct": rejection, "failed_retest_high": float(features.last_high or event_high), "decision_price": float(features.price), "entry_reference": reference, "failed_retest_quality": "PREDICATE_SHAPE_ONLY", "oi_sequence_classification": "WEAK_DIRECTIONAL_OI_INFERENCE" if oi is not None else "OI_UNKNOWN"})
    metadata.update(compute_trapped_longs_shadow_metrics(decision_price=float(features.price), breakout_reference=reference, failed_retest_high=float(features.last_high or event_high), event_high=event_high, atr=float(features.atr_14 or 0), breakout_failure_time=None, failed_retest_time=None, decision_time=decision_time, event_high_time=state.event_high_time))
    admitted = not vetoes and score >= int(_cfg(config, "trapped_longs_min_signal_score", 70)) and grade in {"A", "B"}
    return TrappedLongsEvaluation(TRAPPED_LONGS_REVERSAL if admitted else None, score, grade, metadata, vetoes, [])


def trapped_longs_expiry_reasons(
    *,
    attempt_created_at: datetime,
    confirmation_expires_at: datetime,
    decision_time: datetime,
) -> list[str]:
    """Return stable Strategy-4 expiry blockers without changing the window."""
    reasons: list[str] = []
    if confirmation_expires_at <= attempt_created_at:
        reasons.append("BORN_EXPIRED_ATTEMPT")
    if decision_time >= confirmation_expires_at:
        reasons.append("CONFIRMATION_WINDOW_EXPIRED")
    return reasons


def compute_trapped_longs_shadow_metrics(
    *,
    decision_price: float,
    breakout_reference: float,
    failed_retest_high: float | None,
    event_high: float,
    atr: float | None,
    breakout_failure_time: datetime | None,
    failed_retest_time: datetime | None,
    decision_time: datetime | None,
    event_high_time: datetime | None,
) -> dict[str, Any]:
    """Return bounded, non-admission metrics for Strategy-4 shadow review."""
    def pct(level: float | None) -> float | None:
        return None if level in (None, 0) else (level - decision_price) / level * 100

    def atr_distance(level: float | None) -> float | None:
        return None if level in (None, 0) or not atr else (level - decision_price) / atr

    def minutes_between(start: datetime | None, end: datetime | None) -> float | None:
        if start is None or end is None:
            return None
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end.tzinfo is None:
            end = end.replace(tzinfo=timezone.utc)
        return (end - start).total_seconds() / 60

    breakout_pct = pct(breakout_reference)
    breakout_atr = atr_distance(breakout_reference)
    retest_pct = pct(failed_retest_high)
    retest_atr = atr_distance(failed_retest_high)
    event_pct = pct(event_high)
    event_atr = atr_distance(event_high)
    max_distance = max(abs(x) for x in (breakout_pct, retest_pct, event_pct) if x is not None)
    max_atr = max(abs(x) for x in (breakout_atr, retest_atr, event_atr) if x is not None)
    classification = "ENTRY_FRESH" if max_distance <= 0.5 and max_atr <= 0.5 else (
        "ENTRY_HEAVILY_CHASED" if max_distance >= 3.0 or max_atr >= 2.0 else "ENTRY_EXTENDED"
    )
    quality = max(0.0, min(100.0, 100.0 - max_distance * 12.0 - max_atr * 8.0))
    return {
        "distance_from_breakout_pct": breakout_pct,
        "distance_from_breakout_atr": breakout_atr,
        "distance_from_retest_high_pct": retest_pct,
        "distance_from_retest_high_atr": retest_atr,
        "distance_from_event_high_pct": event_pct,
        "distance_from_event_high_atr": event_atr,
        "event_high_to_decision_minutes": minutes_between(event_high_time, decision_time),
        "breakout_failure_to_failed_retest_minutes": minutes_between(breakout_failure_time, failed_retest_time),
        "failed_retest_to_decision_minutes": minutes_between(failed_retest_time, decision_time),
        "favorable_move_completed_pct": event_pct,
        "entry_classification": classification,
        "shadow_quality_score": round(quality, 2),
    }


def classify_trapped_longs_oi_sequence(
    *,
    price_before: float | None,
    price_breakout: float | None,
    price_failure: float | None,
    oi_before: float | None,
    oi_breakout: float | None,
    oi_failure: float | None,
    oi_decision: float | None,
    retained_increase_pct: float = 1.0,
) -> str:
    """Classify historical OI evidence; this never participates in admission."""
    values = (price_before, price_breakout, price_failure, oi_before, oi_breakout, oi_failure, oi_decision)
    if any(value is None or value <= 0 for value in values):
        return "OI_UNKNOWN"
    assert all(value is not None for value in values)
    p_before, p_breakout, p_failure, o_before, o_breakout, o_failure, o_decision = (float(value) for value in values)
    breakout_price_up = p_breakout > p_before
    breakout_oi_up = o_breakout > o_before
    retained_floor = o_before * (1 + retained_increase_pct / 100)
    if breakout_price_up and breakout_oi_up and p_failure < p_breakout:
        if o_failure >= retained_floor and o_decision >= retained_floor:
            return "STRONG_TRAPPED_LONG_EVIDENCE"
        if o_failure < retained_floor or o_decision < retained_floor:
            return "OI_UNWOUND_AFTER_FAILURE"
    return "WEAK_DIRECTIONAL_OI_INFERENCE"


def advance_trapped_longs_lifecycle(*, root_created_at: datetime, breakout_at: datetime, observed_at: datetime, event_revision: int, breakout_confirmed: bool, oi_confirmed: bool, closed_candles: int, close_below_reference: bool, failed_retest: bool, no_new_high: bool, liquidity_ok: bool, rejection_ok: bool, max_lifetime_minutes: int = 15) -> TrappedLongsLifecycle:
    if observed_at < root_created_at:
        raise ValueError("observed_at cannot precede root_created_at")
    base = dict(event_revision=event_revision, root_created_at=root_created_at, breakout_at=breakout_at, last_observed_at=observed_at)
    if observed_at - root_created_at > timedelta(minutes=max_lifetime_minutes):
        return TrappedLongsLifecycle(state="EXPIRED", veto_reasons=["root_lifetime_expired"], expired=True, **base)
    checks = [(breakout_confirmed, "breakout_not_confirmed"), (oi_confirmed, "oi_not_confirmed"), (closed_candles >= 2, "insufficient_closed_structural_candles"), (close_below_reference, "close_not_below_breakout_reference"), (failed_retest, "failed_retest_missing"), (no_new_high, "new_high_before_delivery"), (liquidity_ok, "liquidity_not_confirmed"), (rejection_ok, "rejection_missing")]
    reasons = [reason for passed, reason in checks if not passed]
    return TrappedLongsLifecycle(state="ADMITTED" if not reasons else "RETEST_IN_PROGRESS", veto_reasons=reasons, **base)


def trapped_longs_root_event_id(symbol: str, breakout_identity: str) -> str:
    return f"trapped_longs:{symbol}:{breakout_identity}"


def _identity(kind: str, symbol: str, breakout_identity: str, revision: int) -> str:
    return f"trapped_longs:{kind}:{symbol}:{breakout_identity}:r{revision}"


def trapped_longs_attempt_id(symbol: str, breakout_identity: str, revision: int = 1) -> str:
    return _identity("attempt", symbol, breakout_identity, revision)


def trapped_longs_evaluation_id(symbol: str, breakout_identity: str, revision: int = 1) -> str:
    return _identity("evaluation", symbol, breakout_identity, revision)


def trapped_longs_admission_id(symbol: str, breakout_identity: str, revision: int = 1) -> str:
    return _identity("admission", symbol, breakout_identity, revision)
