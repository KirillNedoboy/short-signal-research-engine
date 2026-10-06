"""Signal scoring logic."""

from __future__ import annotations

from app.config import AppConfig
from app.domain import ShortZone, SymbolFeatures
from app.signals.risk_flags import evaluate_risk_flags


def score_setup(
    features: SymbolFeatures,
    zone: ShortZone,
    config: AppConfig,
) -> tuple[int, dict[str, float], list[str], bool]:
    """Return score, breakdown, flags, and breakout risk."""

    stretch = _scale(features.dist_to_vwap_pct, 6.0, 12.0, 10.0) + _scale(features.dist_to_ema20_atr, 2.0, 4.0, 5.0)
    exhaustion = (
        _scale(features.upper_wick_ratio, 0.12, 0.25, 8.0)
        + _scale(features.rejection_from_high_pct, 0.8, 1.6, 8.0)
        + _scale(1 - features.close_position_in_range, 0.25, 0.8, 2.0)
        + (2.0 if features.latest_failed_retest else 0.0)
    )
    volume = _scale(features.vol_zscore_30m, 0.4, 2.0, 6.0) + _scale(features.range_atr_ratio, 1.3, 2.2, 4.0)
    event_quality = max(
        _scale(features.ret_15m, 6.0, 12.0, 5.0),
        _scale(features.ret_1h, 8.0, 15.0, 5.0),
        _scale(features.ret_4h, 20.0, 35.0, 5.0),
    ) + _scale(features.event_range_pct or 0.0, 6.0, 22.0, 10.0)
    pullback_maturity = _ideal_band(features.pullback_from_high_pct or 0.0, ideal=3.5, width=3.5, max_points=10.0)
    pullback_maturity += _scale(features.dist_to_vwap_pct, config.dist_to_vwap_min, config.dist_to_vwap_min + 4.0, 5.0)
    zone_quality = _zone_score(features.price, zone.low, zone.high)
    derivatives_bonus = 0.0
    if features.oi_change_15m and features.oi_change_15m > 0:
        derivatives_bonus += 2.0
    if features.oi_change_1h and features.oi_change_1h > 0:
        derivatives_bonus += 2.0
    if features.funding_rate and features.funding_rate > 0:
        derivatives_bonus += 1.0

    flags, penalty_points, breakout_risk = evaluate_risk_flags(features, config)
    raw_total = stretch + exhaustion + volume + event_quality + pullback_maturity + zone_quality + derivatives_bonus
    total = int(round(max(0.0, min(100.0, raw_total - penalty_points))))

    breakdown = {
        "stretch": round(stretch, 2),
        "exhaustion": round(exhaustion, 2),
        "volume": round(volume, 2),
        "event_quality": round(event_quality, 2),
        "pullback_maturity": round(pullback_maturity, 2),
        "zone_quality": round(zone_quality, 2),
        "derivatives_bonus": round(derivatives_bonus, 2),
        "penalties": round(-penalty_points, 2),
        "raw_total": round(raw_total, 2),
    }
    return total, breakdown, flags, breakout_risk


def _scale(value: float, start: float, end: float, max_points: float) -> float:
    if value <= start:
        return 0.0
    if value >= end:
        return max_points
    return ((value - start) / (end - start)) * max_points


def _ideal_band(value: float, ideal: float, width: float, max_points: float) -> float:
    distance = abs(value - ideal)
    if distance >= width:
        return 0.0
    return (1 - (distance / width)) * max_points


def _zone_score(price: float, zone_low: float, zone_high: float) -> float:
    if zone_high <= zone_low:
        return 0.0
    position = (price - zone_low) / (zone_high - zone_low)
    return _ideal_band(position, ideal=0.55, width=0.55, max_points=10.0)
