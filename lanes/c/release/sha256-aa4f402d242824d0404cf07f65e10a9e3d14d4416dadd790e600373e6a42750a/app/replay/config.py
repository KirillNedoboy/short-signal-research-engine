"""Decision-only configuration contract shared by runtime and replay."""

from __future__ import annotations

from typing import Any, Mapping

from app.replay.canonical import sha256_canonical


# Deliberately explicit: adding a runtime field never makes it replay evidence by
# accident.  Credentials, endpoints, chat IDs, delivery toggles and scheduling
# controls are therefore absent by construction.
STRATEGY_CONFIG_FIELDS = frozenset(
    {
        "event_ret_15m_min", "event_ret_1h_min", "event_ret_4h_min",
        "pullback_min_pct", "pullback_max_pct", "pullback_hold_vwap_min", "pullback_hold_range_floor_pct",
        "short_zone_mode", "short_zone_range_low_pct", "short_zone_range_high_pct",
        "short_zone_atr_low_mult", "short_zone_atr_high_mult", "dist_to_vwap_min", "event_dist_to_vwap_min",
        "upper_wick_min", "rejection_min", "vol_zscore_min", "event_dist_to_ema20_atr_min",
        "dist_to_ema20_atr_bonus", "rsi_bonus_level", "ret_1h_bonus_level", "ret_4h_bonus_level",
        "range_atr_bonus_level", "signal_expiry_minutes", "max_signal_age_minutes", "derivatives_enabled",
        "max_spread_pct", "max_slippage_pct", "min_orderbook_depth_usdt_1pct", "min_orderbook_depth_usdt_2pct",
        "enable_watch_candidates", "watch_min_score", "min_public_signal_grade", "send_grade_c_to_telegram", "grade_c_mode",
        "enable_squeeze_guard", "squeeze_guard_mode", "retest_not_failed_penalty", "shallow_pullback_penalty",
        "weak_rejection_penalty", "combined_early_short_risk_penalty",
        "climax_min_signal_score", "climax_min_public_grade", "climax_grade_a_score",
        "volume_climax_unwind_enabled", "volume_climax_min_ret_15m_pct", "volume_climax_min_volume_ratio",
        "volume_climax_min_volume_zscore", "volume_climax_min_ret_5m_pct", "volume_climax_max_oi_change_5m_pct",
        "volume_climax_min_rejection_pct", "volume_climax_max_entry_distance_below_high_pct",
        "volume_climax_confirmation_window_minutes", "volume_climax_min_closed_candles_after_high",
        "volume_climax_max_lifetime_minutes", "low_volume_extension_enabled", "low_volume_min_price_extension_pct",
        "low_volume_max_current_previous_volume_ratio", "low_volume_max_volume_efficiency_ratio",
        "low_volume_min_rejection_pct", "low_volume_max_entry_distance_below_high_pct",
        "low_volume_max_minutes_after_high", "low_volume_min_closed_candles_after_high",
        "low_volume_max_new_high_tolerance_pct", "low_volume_require_close_below_breakout",
        "low_volume_require_lower_high_or_failed_retest", "low_volume_require_microstructure_break",
        "low_volume_require_closed_candles_only", "low_volume_require_equal_volume_windows",
        "low_volume_block_price_acceleration_resumed", "low_volume_block_active_short_squeeze",
        "low_volume_high_liquidity_risk_mode", "low_volume_current_ret5_gate_enabled",
        "climax_max_spread_pct", "climax_max_slippage_pct", "climax_min_depth_1pct_usdt", "climax_min_depth_2pct_usdt",
        "trapped_longs_reversal_enabled", "trapped_longs_live_delivery_enabled", "trapped_longs_min_oi_change_15m_pct",
        "trapped_longs_min_closed_candles", "trapped_longs_max_lifetime_minutes", "trapped_longs_new_high_tolerance_pct",
        "trapped_longs_min_rejection_pct", "trapped_longs_min_signal_score", "trapped_longs_min_public_grade",
        "trapped_longs_max_spread_pct", "trapped_longs_max_slippage_pct", "trapped_longs_min_depth_1pct_usdt", "trapped_longs_min_depth_2pct_usdt",
    }
)


def strategy_config_payload(config: Any) -> dict[str, Any]:
    """Return the stable allowlisted decision configuration only."""

    values: Mapping[str, Any]
    if isinstance(config, Mapping):
        values = config
    elif hasattr(config, "model_dump"):
        values = config.model_dump(mode="json")
    else:
        values = vars(config)
    return {key: values[key] for key in sorted(STRATEGY_CONFIG_FIELDS) if key in values}


def strategy_config_fingerprint(config: Any) -> str:
    """Hash precisely the config that affects replay strategy evaluation."""

    return sha256_canonical(strategy_config_payload(config))
