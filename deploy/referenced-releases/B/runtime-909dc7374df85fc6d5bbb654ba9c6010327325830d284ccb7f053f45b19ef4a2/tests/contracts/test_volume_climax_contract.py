from __future__ import annotations

from app.signals.climax import evaluate_climax

from tests.contracts.helpers import climax_config

def _volume_result(make_event_state, make_features, make_frame, **overrides):
    values = dict(
        ret_5m=10.0,
        ret_15m=15.0,
        vol_zscore_30m=8.0,
        oi_change_pct=-4.0,
        derivatives_status="OK",
        rejection_from_high_pct=3.0,
        latest_failed_retest=True,
        current_volume=3000.0,
    )
    values.update(overrides)
    features = make_features(**values)
    return evaluate_climax(
        make_event_state(event_features_snapshot={"previous_leg_volume": 500.0}),
        features,
        make_frame([100 + index * 0.1 for index in range(30)]),
        climax_config(low_volume_extension_enabled=False),
    )


def test_known_actionable_style_fixture_is_volume_climax(make_event_state, make_features, make_frame):
    result = _volume_result(make_event_state, make_features, make_frame)

    assert result.subtype == "VOLUME_CLIMAX_UNWIND"
    assert result.actionable is True
    assert result.score == 85
    assert result.grade == "A"


def test_volume_climax_not_extreme_blocks(make_event_state, make_features, make_frame):
    result = _volume_result(
        make_event_state,
        make_features,
        make_frame,
        vol_zscore_30m=1.0,
        current_volume=500.0,
    )

    assert result.subtype is None
    assert "volume_climax_not_extreme" in result.veto_reasons


def test_volume_climax_pump_threshold_blocks(make_event_state, make_features, make_frame):
    result = _volume_result(make_event_state, make_features, make_frame, ret_15m=11.9)

    assert result.subtype is None
    assert "pump_below_threshold" in result.veto_reasons


def test_volume_climax_dangerous_oi_acceleration_blocks(make_event_state, make_features, make_frame):
    result = _volume_result(make_event_state, make_features, make_frame, oi_change_pct=4.0)

    assert result.subtype is None
    assert "price_oi_accelerating_together" in result.veto_reasons


def test_volume_climax_explicit_bad_liquidity_blocks(make_event_state, make_features, make_frame):
    result = _volume_result(
        make_event_state,
        make_features,
        make_frame,
        orderbook_depth_usdt_1pct=1000.0,
        orderbook_depth_usdt_2pct=2000.0,
    )

    assert result.subtype is None
    assert "climax_liquidity_block" in result.veto_reasons


def test_volume_climax_score_threshold_blocks_public_admission(make_event_state, make_features, make_frame):
    result = _volume_result(
        make_event_state,
        make_features,
        make_frame,
        vol_zscore_30m=1.0,
        ret_5m=3.0,
        ret_15m=12.0,
        oi_change_pct=-0.5,
        latest_failed_retest=False,
    )

    assert result.score < 70
    assert result.grade == "C"
    assert result.subtype is None
    assert result.actionable is False
