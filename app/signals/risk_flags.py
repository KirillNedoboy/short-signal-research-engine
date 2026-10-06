"""Risk penalties for early or continuation-heavy shorts."""

from __future__ import annotations

from app.config import AppConfig
from app.domain import SymbolFeatures


def evaluate_risk_flags(features: SymbolFeatures, config: AppConfig) -> tuple[list[str], int, bool]:
    """Return human-readable risk flags, penalty points, and breakout risk."""

    flags: list[str] = []
    penalty = 0
    breakout_risk = False

    pullback = features.pullback_from_high_pct or 0.0
    shallow = 0 < pullback < config.pullback_min_pct
    weak_rejection = features.upper_wick_ratio < config.upper_wick_min and features.rejection_from_high_pct < config.rejection_min
    retest_not_failed = not features.latest_failed_retest
    price_near_high = (features.distance_to_event_high_pct or 0.0) < 1.0 or pullback < 0.5

    if shallow:
        flags.append("Pullback is still shallow.")
        penalty += config.shallow_pullback_penalty
    if price_near_high:
        flags.append("Price is still too close to the event high.")
        penalty += max(4, config.shallow_pullback_penalty // 2)
    if weak_rejection:
        flags.append("Rejection candle is weak.")
        penalty += config.weak_rejection_penalty
    if features.dist_to_vwap_pct < (config.dist_to_vwap_min + 1.0):
        flags.append("VWAP stretch buffer is thin.")
        penalty += 6
    if features.recent_high_breakout:
        flags.append("Recent high was just broken.")
        penalty += 12
        breakout_risk = True
    if features.latest_body_atr_ratio > 1.0:
        flags.append("Continuation body is still too large.")
        penalty += 10
        breakout_risk = True
    if retest_not_failed:
        flags.append("Retest failure is not fully confirmed.")
        penalty += config.retest_not_failed_penalty
    if shallow and weak_rejection and retest_not_failed and price_near_high:
        penalty += config.combined_early_short_risk_penalty

    if not features.liquidity_available:
        flags.append("Liquidity confirmation unavailable.")
        penalty += 10
    else:
        if features.spread_pct is not None and features.spread_pct > config.max_spread_pct:
            flags.append("Orderbook spread is too wide.")
            breakout_risk = True
        if features.slippage_pct is not None and features.slippage_pct > config.max_slippage_pct:
            flags.append("Estimated slippage is too high.")
            breakout_risk = True
        if (
            features.orderbook_depth_usdt_1pct is not None
            and features.orderbook_depth_usdt_1pct < config.min_orderbook_depth_usdt_1pct
        ):
            flags.append("Orderbook depth within 1% is too thin.")
            breakout_risk = True
        if (
            features.orderbook_depth_usdt_2pct is not None
            and features.orderbook_depth_usdt_2pct < config.min_orderbook_depth_usdt_2pct
        ):
            flags.append("Orderbook depth within 2% is too thin.")
            breakout_risk = True

    return flags, min(penalty, 40), breakout_risk
