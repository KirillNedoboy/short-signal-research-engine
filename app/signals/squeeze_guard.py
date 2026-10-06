"""Explainable squeeze-risk scoring for short candidates."""

from __future__ import annotations

from app.config import AppConfig
from app.domain import SqueezeGuardResult, SymbolFeatures


def evaluate_squeeze_guard(features: SymbolFeatures, config: AppConfig) -> SqueezeGuardResult:
    """Return squeeze-risk score, reasons, and configured action."""

    reasons: list[str] = []
    data_quality: list[str] = list(features.data_quality_warnings)
    score = 0

    if features.funding_rate is None:
        data_quality.append("derivatives_missing")
    elif features.funding_rate < 0 and features.ret_1h >= config.event_ret_1h_min:
        reasons.append("funding_negative_trap")
        score += 12

    if features.oi_change_15m is None and features.oi_change_1h is None:
        data_quality.append("oi_missing")
    else:
        if (features.oi_change_15m or 0) > 0 and features.ret_15m > 0:
            reasons.append("oi_rising_with_price")
            score += 6
        if (features.oi_change_1h or 0) > 0 and features.ret_1h > 0:
            reasons.append("oi_rising_with_price")
            score += 5

    pullback = features.pullback_from_high_pct or 0.0
    if 0 < pullback < config.pullback_min_pct:
        reasons.append("shallow_pullback")
        score += 6
    if not features.latest_failed_retest:
        reasons.append("retest_not_failed")
        score += 6
    if features.upper_wick_ratio < config.upper_wick_min and features.rejection_from_high_pct < config.rejection_min:
        reasons.append("weak_rejection")
        score += 6
    if (features.distance_to_event_high_pct or 0.0) < 1.0:
        reasons.append("price_near_high")
        score += 6
    if features.recent_high_breakout:
        reasons.append("second_leg_pump")
        score += 12

    if not features.liquidity_available:
        data_quality.append("liquidity_missing")
    else:
        if features.spread_pct is not None and features.spread_pct > config.max_spread_pct:
            reasons.append("spread_too_wide")
            score += 8 if features.spread_pct <= config.max_spread_pct * 1.5 else 14
        if features.slippage_pct is not None and features.slippage_pct > config.max_slippage_pct:
            reasons.append("slippage_too_high")
            score += 8
        if (
            features.orderbook_depth_usdt_1pct is not None
            and features.orderbook_depth_usdt_1pct < config.min_orderbook_depth_usdt_1pct
        ):
            reasons.append("thin_orderbook")
            score += 10
        if (
            features.orderbook_depth_usdt_2pct is not None
            and features.orderbook_depth_usdt_2pct < config.min_orderbook_depth_usdt_2pct
        ):
            reasons.append("low_liquidity")
            score += 8

    score = min(score, 100)
    if score >= 34:
        level = "EXTREME"
    elif score >= 24:
        level = "HIGH"
    elif score >= 12:
        level = "MEDIUM"
    else:
        level = "LOW"

    action = "NONE"
    block_signal = False
    force_watch = False
    if not config.enable_squeeze_guard or level == "LOW":
        action = "NONE"
    elif config.squeeze_guard_mode == "warn_only":
        action = "WARNING"
    elif config.squeeze_guard_mode == "score_penalty":
        action = "SCORE_PENALTY"
    elif config.squeeze_guard_mode == "watch_only":
        action = "WATCH_ONLY"
        force_watch = True
    elif config.squeeze_guard_mode == "block_extreme" and level == "EXTREME":
        action = "BLOCKED"
        block_signal = True
    elif config.squeeze_guard_mode == "block_extreme":
        action = "WARNING"

    return SqueezeGuardResult(
        score=score,
        level=level,
        reasons=_dedupe(reasons),
        data_quality=_dedupe(data_quality),
        action=action,
        block_signal=block_signal,
        force_watch=force_watch,
    )


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
