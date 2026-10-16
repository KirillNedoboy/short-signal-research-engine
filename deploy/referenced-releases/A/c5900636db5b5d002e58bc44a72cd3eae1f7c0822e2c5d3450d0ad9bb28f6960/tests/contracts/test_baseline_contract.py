from __future__ import annotations

from app.config import AppConfig
from app.domain import EventStatus, ShortZone, SignalType
from app.signals.engine import SignalEngine


def _evaluate(make_event_state, make_features, **overrides):
    state = make_event_state(
        state=EventStatus.PULLBACK_OBSERVED,
        zone_low=110.5,
        zone_high=113.8,
    )
    features = make_features(**overrides)
    return SignalEngine(AppConfig()).analyze(
        state,
        features,
        ShortZone(low=110.5, high=113.8, mode="event_range"),
        features.asof,
    )


def test_non_actionable_baseline_case_is_rejected(make_event_state, make_features):
    evaluation = _evaluate(
        make_event_state,
        make_features,
        dist_to_vwap_pct=4.0,
        vol_zscore_30m=0.2,
        pullback_from_high_pct=1.0,
        rejection_from_high_pct=0.2,
        upper_wick_ratio=0.05,
    )

    assert evaluation.checked is True
    assert evaluation.decision is None
    assert evaluation.grade == "C"
    assert "score_too_low" in evaluation.reject_reasons


def test_baseline_b_confirm_is_stable(make_event_state, make_features):
    evaluation = _evaluate(
        make_event_state,
        make_features,
        upper_wick_ratio=0.17,
        rejection_from_high_pct=1.15,
        close_position_in_range=0.10,
    )

    assert evaluation.decision is not None
    assert evaluation.decision.signal_type is SignalType.CONFIRM
    assert evaluation.decision.actionable is True
    assert evaluation.decision.grade == "B"
    assert 65 <= evaluation.decision.score < 75


def test_baseline_b_aggressive_is_stable(make_event_state, make_features):
    evaluation = _evaluate(
        make_event_state,
        make_features,
        upper_wick_ratio=0.18,
        rejection_from_high_pct=1.20,
        close_position_in_range=0.10,
    )

    assert evaluation.decision is not None
    assert evaluation.decision.signal_type is SignalType.AGGRESSIVE
    assert evaluation.decision.actionable is True
    assert evaluation.decision.grade == "B"
    assert 75 <= evaluation.decision.score < 80


def test_baseline_liquidity_hard_block_remains_non_actionable(make_event_state, make_features):
    evaluation = _evaluate(
        make_event_state,
        make_features,
        orderbook_depth_usdt_1pct=10_000.0,
        orderbook_depth_usdt_2pct=20_000.0,
        spread_pct=0.35,
        slippage_pct=0.30,
    )

    assert evaluation.decision is None
    assert "spread_too_wide" in evaluation.reject_reasons
    assert evaluation.blockers


def test_baseline_continuation_hard_block_remains_non_actionable(make_event_state, make_features):
    evaluation = _evaluate(
        make_event_state,
        make_features,
        recent_high_breakout=True,
        latest_body_atr_ratio=1.2,
    )

    assert evaluation.decision is None
    assert "squeeze_risk" in evaluation.reject_reasons
    assert any("Continuation body" in flag for flag in evaluation.risk_flags)


def test_baseline_dedupe_identity_is_event_and_strategy_specific(
    tmp_path,
    make_event_state,
    make_signal_decision,
    make_signal_provenance,
):
    from app.storage.db import Database
    from app.storage.repository import BotRepository

    database = Database(f"sqlite:///{tmp_path / 'baseline-dedupe.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(strategy_type="BASELINE_PULLBACK", strategy_subtype=None, model_version="baseline-v1")
    repository.save_signal(decision, state, telegram_sent=False, provenance=make_signal_provenance())

    assert repository.has_signal_for_event("ONTUSDT", state.event_id, "", "baseline-v1") is False
    assert repository.has_signal_for_event("ONTUSDT", state.event_id, None, "baseline-v1") is True
    assert repository.has_signal_for_event("ONTUSDT", "ONTUSDT:15m:1:112", None, "baseline-v1") is False
