"""Candle shape helpers."""

from __future__ import annotations


def candle_shape(open_price: float, high: float, low: float, close: float) -> dict[str, float]:
    """Return wick and body metrics for a candle."""

    total_range = max(high - low, 1e-9)
    body = abs(close - open_price)
    upper_wick = max(high - max(open_price, close), 0.0)
    lower_wick = max(min(open_price, close) - low, 0.0)
    rejection = ((high - close) / high) * 100 if high else 0.0
    close_position = (close - low) / total_range
    return {
        "upper_wick_ratio": upper_wick / total_range,
        "lower_wick_ratio": lower_wick / total_range,
        "body_pct": body / total_range,
        "rejection_from_high_pct": rejection,
        "close_position_in_range": close_position,
    }
