from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

import pytest

from app.signals.climax import _low_volume, evaluate_climax, evaluate_climax_bundle, evaluate_climax_shadow


def _config(**overrides):
    values = dict(
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

        climax_max_spread_pct=0.8,
        climax_max_slippage_pct=1.0,
        climax_min_depth_1pct_usdt=5000,
        climax_min_depth_2pct_usdt=10000,
        max_spread_pct=0.3,
        max_slippage_pct=0.35,
        min_orderbook_depth_usdt_1pct=30000,
        min_orderbook_depth_usdt_2pct=60000,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _ready_low_volume_inputs(make_features, make_event_state, make_frame, *, frame_prices=None, **feature_overrides):
    state = make_event_state(
        event_high=115.0,
        event_high_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
    )
    feature_values = dict(
        asof=datetime(2026, 4, 13, 12, 10, tzinfo=timezone.utc),
        price=112.0,
        ret_5m=4.0,
        rejection_from_high_pct=3.0,
        current_volume=100.0,
        oi_change_pct=-5.0,
        derivatives_status="OK",
        latest_failed_retest=True,
    )
    feature_values.update(feature_overrides)
    features = make_features(**feature_values)
    prices = frame_prices or [110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 114.0, 113.0, 112.0, 111.0, 110.5]
    frame = make_frame(prices)
    frame.loc[frame.index[1:6], "volume"] = 1000.0
    frame.loc[frame.index[6:11], "volume"] = 100.0
    return state, features, frame


def test_low_volume_extension_below_threshold_is_warning_not_veto(make_features, make_event_state, make_frame):
    state, features, frame = _ready_low_volume_inputs(make_features, make_event_state, make_frame)
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert result.actionable
    assert result.subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    assert "extension_below_threshold" not in result.veto_reasons
    assert "extension_below_threshold" in result.data_quality


def test_low_volume_extension_threshold_pass_keeps_existing_behavior(make_features, make_event_state, make_frame):
    state, features, frame = _ready_low_volume_inputs(make_features, make_event_state, make_frame, ret_5m=6.0)
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert result.actionable
    assert "extension_below_threshold" not in result.veto_reasons
    assert "extension_below_threshold" not in result.data_quality


def test_low_volume_extension_warning_does_not_override_missing_liquidity(make_features, make_event_state, make_frame):
    state, features, frame = _ready_low_volume_inputs(make_features, make_event_state, make_frame, liquidity_available=False)
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert not result.actionable
    assert "extension_below_threshold" in result.data_quality
    assert "liquidity_not_confirmed" in result.veto_reasons


def test_low_volume_explicit_bad_liquidity_still_blocks(make_features, make_event_state, make_frame):
    state, features, frame = _ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
        orderbook_depth_usdt_1pct=1000.0,
        orderbook_depth_usdt_2pct=2000.0,
    )
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert not result.actionable
    assert "climax_liquidity_block" in result.veto_reasons


@pytest.mark.parametrize(
    ("feature_overrides", "frame_prices", "expected_blocker"),
    [
        ({"rejection_from_high_pct": 1.0}, None, "rejection_missing"),
        ({}, [110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 115.0, 115.0, 115.0, 115.0, 115.0], "microstructure_break_missing"),
        ({"latest_failed_retest": False, "ret_5m": 7.0}, [110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 115.0, 115.0, 115.0, 115.0, 115.0], "active_short_squeeze"),
    ],
)
def test_low_volume_other_hard_blockers_remain(
    make_features,
    make_event_state,
    make_frame,
    feature_overrides,
    frame_prices,
    expected_blocker,
):
    state, features, frame = _ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
        frame_prices=frame_prices,
        **feature_overrides,
    )
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert not result.actionable
    assert expected_blocker in result.veto_reasons
    if feature_overrides.get("ret_5m", 4.0) < 5.0:
        assert "extension_below_threshold" in result.data_quality


def test_low_volume_confirmation_expiry_still_blocks(make_features, make_event_state, make_frame):
    state, features, frame = _ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
        asof=datetime(2026, 4, 13, 12, 30, tzinfo=timezone.utc),
    )
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert not result.actionable
    assert "confirmation_window_expired" in result.veto_reasons
    assert "extension_below_threshold" in result.data_quality


def test_low_volume_new_high_still_blocks(make_features, make_event_state, make_frame):
    state, features, _ = _ready_low_volume_inputs(make_features, make_event_state, make_frame)
    result = _low_volume(
        {
            "event_high": 115.0,
            "previous_leg_volume": 1000.0,
            "current_leg_volume": 100.0,
            "volume_efficiency_ratio": 0.1,
            "volume_windows_equal": True,
            "closed_candles_after_high": 3,
            "post_high_high": 116.0,
            "post_high_retest_high": 112.0,
            "post_high_closes": [114.0, 112.0],
            "breakout_reference": 114.425,
            "current_close": 112.0,
            "rejection_pct": 3.0,
            "entry_distance_below_high_pct": 2.0,
        },
        state,
        features,
        _config(volume_climax_unwind_enabled=False),
    )
    assert not result.actionable
    assert "new_high_before_delivery" in result.veto_reasons
    assert "extension_below_threshold" in result.data_quality


def test_low_volume_shadow_uses_frozen_extension_when_current_ret5_is_negative(make_features, make_event_state, make_frame):
    state = make_event_state(
        event_high=115.0,
        event_high_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
        event_features_snapshot={"initial_extension_pct": 8.0},
    )
    features = make_features(
        asof=datetime(2026, 4, 13, 12, 10, tzinfo=timezone.utc),
        price=112.0,
        ret_5m=-2.0,
        rejection_from_high_pct=3.0,
        current_volume=100.0,
        oi_change_pct=None,
        derivatives_status="MISSING",
        latest_failed_retest=True,
    )
    frame = make_frame([110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 114.0, 113.0, 112.0, 111.0, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5])
    frame.loc[frame.index[1:6], "volume"] = 1000.0
    frame.loc[frame.index[6:11], "volume"] = 100.0
    result = evaluate_climax_shadow(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert result.metadata["initial_extension_pct"] == 8.0
    assert result.metadata["extension_gate_value"] == 8.0
    assert "extension_below_threshold" not in result.veto_reasons


def test_volume_climax_ake_admits(make_features, make_event_state, make_frame):
    state = make_event_state(event_features_snapshot={"previous_leg_volume": 500.0})
    features = make_features(ret_5m=10.0, ret_15m=15.0, vol_zscore_30m=8.73, oi_change_pct=-5.77, derivatives_status="OK", rejection_from_high_pct=3.0, latest_failed_retest=True, current_volume=3000.0)
    result = evaluate_climax(state, features, make_frame([100 + i * 0.1 for i in range(30)]), _config(low_volume_extension_enabled=False))
    assert result.subtype == "VOLUME_CLIMAX_UNWIND"
    assert result.grade in {"A", "B"}


def test_volume_climax_uses_canonical_five_minute_threshold(make_features, make_event_state, make_frame):
    features = make_features(
        ret_5m=4.0,
        ret_15m=15.0,
        vol_zscore_30m=8.0,
        oi_change_pct=-4.0,
        derivatives_status="OK",
        rejection_from_high_pct=3.0,
    )
    result = evaluate_climax(
        make_event_state(),
        features,
        make_frame([100 + i * 0.1 for i in range(30)]),
        _config(volume_climax_min_ret_5m_pct=5.0, low_volume_extension_enabled=False),
    )
    assert "price_acceleration_below_threshold" in result.veto_reasons


def test_bundle_keeps_every_enabled_branch_when_selected_result_is_rejected(
    make_features,
    make_event_state,
    make_frame,
):
    features = make_features(
        ret_5m=4.0,
        ret_15m=15.0,
        vol_zscore_30m=8.0,
        oi_change_pct=None,
        derivatives_status="MISSING",
        rejection_from_high_pct=3.0,
    )

    bundle = evaluate_climax_bundle(
        make_event_state(),
        features,
        make_frame([100 + index * 0.1 for index in range(30)]),
        _config(),
    )

    assert set(bundle.branch_evaluations) == {
        "VOLUME_CLIMAX_UNWIND",
        "LOW_VOLUME_EXTENSION_FAILURE",
    }
    assert "oi_missing_for_volume_climax" in bundle.branch_evaluations["VOLUME_CLIMAX_UNWIND"].veto_reasons
    assert bundle.selected.subtype is None


def test_bundle_selected_evaluation_preserves_legacy_result(make_features, make_event_state, make_frame):
    state = make_event_state()
    features = make_features(ret_5m=10.0, ret_15m=15.0, vol_zscore_30m=8.0, oi_change_pct=-4.0)
    frame = make_frame([100 + index * 0.1 for index in range(30)])
    config = _config(low_volume_extension_enabled=False)

    bundle = evaluate_climax_bundle(state, features, frame, config)
    legacy = evaluate_climax(state, features, frame, config)

    assert bundle.selected.subtype == legacy.subtype
    assert bundle.selected.score == legacy.score
    assert bundle.selected.grade == legacy.grade
    assert bundle.selected.veto_reasons == legacy.veto_reasons


def test_low_score_volume_observation_is_retained(make_features, make_event_state, make_frame):
    state = make_event_state(event_features_snapshot={"previous_leg_volume": 1000.0})
    features = make_features(
        ret_5m=3.0,
        ret_15m=12.0,
        vol_zscore_30m=1.0,
        oi_change_pct=-0.5,
        derivatives_status="OK",
        rejection_from_high_pct=2.0,
        current_volume=3000.0,
        latest_failed_retest=False,
    )
    result = evaluate_climax(
        state,
        features,
        make_frame([100 + i * 0.1 for i in range(30)]),
        _config(low_volume_extension_enabled=False),
    )
    assert not result.actionable
    assert result.metadata["volume_climax_observed"] is True
    assert result.metadata["volume_climax_candidate"] is True
    assert result.metadata["volume_climax_actionable"] is False
    assert result.metadata["volume_climax_score"] < 70


def test_m1_only_divergence_no_actionable(make_features, make_event_state, make_frame):
    features = make_features(ret_5m=10.0, ret_15m=15.0, vol_zscore_30m=8.0, oi_change_pct=None, derivatives_status="MISSING", rejection_from_high_pct=3.0)
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config(low_volume_extension_enabled=False))
    assert not result.actionable
    assert "oi_missing_for_volume_climax" in result.veto_reasons


def test_price_up_oi_up_is_veto(make_features, make_event_state, make_frame):
    features = make_features(ret_5m=10.0, ret_15m=15.0, vol_zscore_30m=8.0, oi_change_pct=4.0, derivatives_status="OK", rejection_from_high_pct=3.0)
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config(low_volume_extension_enabled=False))
    assert not result.actionable
    assert "price_oi_accelerating_together" in result.veto_reasons


def test_missing_liquidity_blocks_live_volume_candidate(make_features, make_event_state, make_frame):
    features = make_features(
        ret_5m=10.0,
        ret_15m=15.0,
        vol_zscore_30m=8.0,
        oi_change_pct=-4.0,
        derivatives_status="OK",
        rejection_from_high_pct=3.0,
        liquidity_available=False,
    )
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config(low_volume_extension_enabled=False))
    assert not result.actionable
    assert "liquidity_not_confirmed" in result.veto_reasons


def test_volume_candidate_metadata_survives_other_strategy_selection(make_features, make_event_state, make_frame):
    features = make_features(ret_5m=10.0, ret_15m=15.0, vol_zscore_30m=8.0, oi_change_pct=4.0, derivatives_status="OK", rejection_from_high_pct=3.0)
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config())
    assert result.metadata["volume_climax_observed"] is True
    assert result.metadata["volume_climax_candidate"] is False
    assert "volume_climax_metadata" in result.metadata


def test_low_volume_extension_with_missing_oi_is_grade_b(make_features, make_event_state, make_frame):
    state = make_event_state(event_high=115.0, event_high_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc))
    features = make_features(asof=datetime(2026, 4, 13, 12, 10, tzinfo=timezone.utc), price=112.0, ret_5m=6.0, rejection_from_high_pct=3.0, current_volume=100.0, oi_change_pct=None, derivatives_status="MISSING", latest_failed_retest=True)
    prices = [110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 114.0, 113.0, 112.0, 111.0, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5, 110.5]
    frame = make_frame(prices)
    frame.loc[frame.index[1:6], "volume"] = 1000.0
    frame.loc[frame.index[6:11], "volume"] = 100.0
    result = evaluate_climax(state, features, frame, _config(volume_climax_unwind_enabled=False))
    assert result.subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    assert result.grade == "B"
def test_low_volume_rejects_unconfirmed_failed_high(make_features, make_event_state, make_frame):
    features = make_features(ret_5m=7.0, oi_change_pct=-5.0, latest_failed_retest=False, rejection_from_high_pct=4.92)
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config(volume_climax_unwind_enabled=False))
    assert result.subtype is None
    assert "lower_high_or_failed_retest_missing" in result.veto_reasons


def test_low_volume_blocks_active_short_squeeze(make_features, make_event_state, make_frame):
    features = make_features(ret_5m=7.0, oi_change_pct=-5.0, latest_failed_retest=False)
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config(volume_climax_unwind_enabled=False))
    assert "active_short_squeeze" in result.veto_reasons


def test_low_volume_requires_two_closed_candles_after_high(make_features, make_event_state, make_frame):
    features = make_features(asof=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc), ret_5m=6.0, latest_failed_retest=True)
    result = evaluate_climax(make_event_state(), features, make_frame([100 + i * 0.1 for i in range(30)]), _config(volume_climax_unwind_enabled=False))
    assert "insufficient_closed_candles_after_high" in result.veto_reasons
    assert result.metadata["oi_confirmation_state"] == "unavailable"


def test_climax_metadata_excludes_partial_candle(make_features, make_event_state, make_frame):
    start = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    frame = make_frame([100 + i for i in range(30)], start=start)
    features = make_features(
        asof=start + timedelta(minutes=29, seconds=30),
        ret_5m=10.0,
        ret_15m=15.0,
        vol_zscore_30m=8.0,
        oi_change_pct=-4.0,
        derivatives_status="OK",
        rejection_from_high_pct=3.0,
    )
    result = evaluate_climax_shadow(
        make_event_state(event_high_time=start + timedelta(minutes=25)),
        features,
        frame,
        _config(low_volume_extension_enabled=False),
    )
    assert result.metadata["partial_candle_excluded"] is True
    assert result.metadata["last_closed_candle_close_time"] == (start + timedelta(minutes=29)).isoformat()


def test_low_volume_confirmed_second_leg_is_veto(make_features, make_event_state, make_frame):
    state = make_event_state(event_features_snapshot={"previous_leg_volume": 1000.0})
    features = make_features(ret_5m=6.0, rejection_from_high_pct=3.0, current_volume=1000.0, oi_change_pct=4.0, derivatives_status="OK")
    result = evaluate_climax(state, features, make_frame([100 + i * 0.1 for i in range(30)]), _config(volume_climax_unwind_enabled=False))
    assert not result.actionable
    assert "second_leg_volume_confirmed" in result.veto_reasons or "oi_accelerating_up" in result.veto_reasons
