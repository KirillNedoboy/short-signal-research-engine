from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any


def climax_config(**overrides: Any) -> SimpleNamespace:
    values: dict[str, Any] = dict(
        volume_climax_unwind_enabled=True,
        low_volume_extension_enabled=True,
        climax_min_signal_score=70,
        volume_climax_min_ret_5m_pct=3.0,
        volume_climax_min_ret_15m_pct=12.0,
        volume_climax_min_volume_ratio=3.0,
        volume_climax_min_volume_zscore=2.5,
        volume_climax_max_oi_change_5m_pct=-1.0,
        volume_climax_min_rejection_pct=2.0,
        volume_climax_max_entry_distance_below_high_pct=20.0,
        low_volume_min_price_extension_pct=5.0,
        low_volume_max_current_previous_volume_ratio=0.70,
        low_volume_max_volume_efficiency_ratio=0.70,
        low_volume_min_rejection_pct=2.0,
        low_volume_max_entry_distance_below_high_pct=15.0,
        low_volume_max_minutes_after_high=15,
        low_volume_min_closed_candles_after_high=2,
        low_volume_confirmation_window_minutes=3,
        low_volume_max_new_high_tolerance_pct=0.30,
        low_volume_require_close_below_breakout=True,
        low_volume_require_lower_high_or_failed_retest=True,
        low_volume_require_microstructure_break=True,
        low_volume_require_closed_candles_only=True,
        low_volume_require_equal_volume_windows=True,
        low_volume_block_price_acceleration_resumed=True,
        low_volume_block_new_high_before_delivery=True,
        low_volume_block_active_short_squeeze=True,
        low_volume_high_liquidity_risk_mode="block",
        climax_max_spread_pct=0.80,
        climax_max_slippage_pct=1.00,
        climax_min_depth_1pct_usdt=5000,
        climax_min_depth_2pct_usdt=10000,
        max_spread_pct=0.30,
        max_slippage_pct=0.35,
        min_orderbook_depth_usdt_1pct=30000,
        min_orderbook_depth_usdt_2pct=60000,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def ready_low_volume_inputs(make_features, make_event_state, make_frame, *, frame_prices=None, **feature_overrides):
    state = make_event_state(
        event_high=115.0,
        event_high_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
    )
    values = dict(
        asof=datetime(2026, 4, 13, 12, 10, tzinfo=timezone.utc),
        price=112.0,
        ret_5m=4.0,
        rejection_from_high_pct=3.0,
        current_volume=100.0,
        oi_change_pct=-5.0,
        derivatives_status="OK",
        latest_failed_retest=True,
    )
    values.update(feature_overrides)
    features = make_features(**values)
    prices = frame_prices or [110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 114.0, 113.0, 112.0, 111.0, 110.5]
    frame = make_frame(prices)
    frame.loc[frame.index[1:6], "volume"] = 1000.0
    frame.loc[frame.index[6:11], "volume"] = 100.0
    return state, features, frame
