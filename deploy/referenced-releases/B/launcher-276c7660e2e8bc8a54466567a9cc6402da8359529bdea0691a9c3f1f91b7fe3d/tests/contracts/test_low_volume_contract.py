from __future__ import annotations

from app.signals.climax import evaluate_climax

from tests.contracts.helpers import climax_config, ready_low_volume_inputs


def _result(make_features, make_event_state, make_frame, **overrides):
    state, features, frame = ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
        **overrides,
    )
    return evaluate_climax(
        state,
        features,
        frame,
        climax_config(volume_climax_unwind_enabled=False),
    )


def test_extension_below_threshold_is_warning_only(make_features, make_event_state, make_frame):
    result = _result(make_features, make_event_state, make_frame, ret_5m=4.0)

    assert result.actionable is True
    assert result.subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    assert "extension_below_threshold" in result.data_quality
    assert "extension_below_threshold" not in result.veto_reasons


def test_rejection_missing_remains_hard(make_features, make_event_state, make_frame):
    result = _result(make_features, make_event_state, make_frame, rejection_from_high_pct=1.0)

    assert result.actionable is False
    assert "rejection_missing" in result.veto_reasons


def test_liquidity_not_confirmed_remains_hard(make_features, make_event_state, make_frame):
    result = _result(make_features, make_event_state, make_frame, liquidity_available=False)

    assert result.actionable is False
    assert "liquidity_not_confirmed" in result.veto_reasons


def test_active_short_squeeze_remains_hard(make_features, make_event_state, make_frame):
    result = _result(
        make_features,
        make_event_state,
        make_frame,
        frame_prices=[100.0 + index for index in range(30)],
        ret_5m=7.0,
        latest_failed_retest=False,
        oi_change_pct=-5.0,
    )

    assert result.actionable is False
    assert "active_short_squeeze" in result.veto_reasons


def test_close_breakout_reference_remains_hard(make_features, make_event_state, make_frame):
    result = _result(
        make_features,
        make_event_state,
        make_frame,
        frame_prices=[110.0, 111.0, 112.0, 113.0, 114.0, 115.0, 115.0, 115.0, 115.0, 115.0, 115.0],
    )

    assert result.actionable is False
    assert "close_not_below_breakout_reference" in result.veto_reasons
    assert "microstructure_break_missing" in result.veto_reasons


def test_micro_break_keeps_current_close_below_breakout_alias(make_features, make_event_state, make_frame):
    result = _result(make_features, make_event_state, make_frame)

    assert result.metadata["microstructure_break_confirmed"] is True
    assert "microstructure_break_missing" not in result.veto_reasons


def test_low_volume_score_and_grade_are_frozen(make_features, make_event_state, make_frame):
    result = _result(make_features, make_event_state, make_frame, ret_5m=6.0)

    assert result.score == 100
    assert result.grade == "B"
    assert result.actionable is True


def test_liquidity_warning_caps_score_and_downgrades_grade(make_features, make_event_state, make_frame):
    state, features, frame = ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
        orderbook_depth_usdt_1pct=20_000.0,
        orderbook_depth_usdt_2pct=100_000.0,
    )
    result = evaluate_climax(
        state,
        features,
        frame,
        climax_config(
            volume_climax_unwind_enabled=False,
            low_volume_high_liquidity_risk_mode="warn",
        ),
    )

    assert result.actionable is True
    assert result.metadata["liquidity_warning"] is True
    assert result.score == 70
    assert result.grade == "B"


def test_independent_event_ids_receive_independent_evaluations(make_features, make_event_state, make_frame):
    first_state, features, frame = ready_low_volume_inputs(make_features, make_event_state, make_frame)
    second_state, _, _ = ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
    )
    second_state.event_id = "ONTUSDT:15m:2:222"

    config = climax_config(volume_climax_unwind_enabled=False)
    first = evaluate_climax(first_state, features, frame, config)
    second = evaluate_climax(second_state, features, frame, config)

    assert first.actionable is True
    assert second.actionable is True
    assert first.subtype == second.subtype == "LOW_VOLUME_EXTENSION_FAILURE"
