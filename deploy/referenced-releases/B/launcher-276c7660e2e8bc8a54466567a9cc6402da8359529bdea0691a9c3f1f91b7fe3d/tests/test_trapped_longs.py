from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pandas as pd

from app.domain import EventState, SymbolFeatures
from app.signals.trapped_longs import (
    TRAPPED_LONGS_REVERSAL,
    advance_trapped_longs_lifecycle,
    evaluate_trapped_longs_reversal,
    trapped_longs_admission_id,
    trapped_longs_attempt_id,
    trapped_longs_evaluation_id,
    trapped_longs_root_event_id,
)

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def config(**overrides):
    values = dict(trapped_longs_min_oi_change_15m_pct=1.0, trapped_longs_min_closed_candles=2, trapped_longs_max_lifetime_minutes=15, trapped_longs_new_high_tolerance_pct=.30, trapped_longs_min_rejection_pct=1.0, trapped_longs_min_signal_score=70, trapped_longs_max_spread_pct=.80, trapped_longs_max_slippage_pct=1.0, trapped_longs_min_depth_1pct_usdt=5000, trapped_longs_min_depth_2pct_usdt=10000)
    values.update(overrides)
    return SimpleNamespace(**values)


def features(**overrides):
    values = dict(symbol="ABCUSDT", asof=T0 + timedelta(minutes=10), price=99, ret_5m=-1, ret_15m=2, ret_1h=3, ret_4h=4, vwap=100, dist_to_vwap_pct=1, ema20=100, dist_to_ema20_pct=1, dist_to_ema20_atr=1, rsi_15m=60, upper_wick_ratio=.2, lower_wick_ratio=.1, body_pct=1, rejection_from_high_pct=1.2, close_position_in_range=.2, vol_zscore_30m=1, vol_zscore_1h=1, atr_14=1, range_atr_ratio=1, oi_change_15m=2, oi_change_1h=2, funding_rate=0, open_interest=100, oi_change_pct=2, derivatives_status="OK", latest_failed_retest=True, last_high=105, last_low=98, last_close=99, current_volume=100, spread_pct=.2, slippage_pct=.2, orderbook_depth_usdt_1pct=10000, orderbook_depth_usdt_2pct=20000, liquidity_available=True, event_range_pct=10, last_structural_close_time=T0 + timedelta(minutes=10))
    values.update(overrides)
    return SymbolFeatures(**values)


def state(**snapshot):
    return EventState(symbol="ABCUSDT", event_id="event-1", event_start_time=T0, event_high=105, event_high_time=T0, event_base_price=100, event_features_snapshot={"breakout_reference": 100, **snapshot})


def frame():
    return pd.DataFrame([{"timestamp": T0 + timedelta(minutes=5), "open": 103, "high": 104, "low": 101, "close": 102, "volume": 10}, {"timestamp": T0 + timedelta(minutes=10), "open": 102, "high": 103, "low": 98, "close": 99, "volume": 10}])


def test_trapped_longs_requires_all_reversal_gates_and_scores_actionable():
    result = evaluate_trapped_longs_reversal(state(), features(asof=T0 + timedelta(minutes=15)), frame(), config())
    assert result.subtype == TRAPPED_LONGS_REVERSAL
    assert result.actionable and result.score >= 70 and result.grade in {"A", "B"}
    assert result.metadata["breakout_reference"] == 100


def test_trapped_longs_fails_closed_for_missing_oi_liquidity_or_new_high():
    assert "oi_missing" in evaluate_trapped_longs_reversal(state(), features(oi_change_15m=None, derivatives_status="MISSING"), frame(), config()).veto_reasons
    assert "liquidity_unavailable" in evaluate_trapped_longs_reversal(state(), features(liquidity_available=False), frame(), config()).veto_reasons
    assert "new_high_before_delivery" in evaluate_trapped_longs_reversal(state(), features(last_high=106), frame(), config()).veto_reasons


def test_trapped_longs_lifecycle_is_bounded_and_namespaced():
    lifecycle = advance_trapped_longs_lifecycle(root_created_at=T0, breakout_at=T0, observed_at=T0 + timedelta(minutes=5), event_revision=1, breakout_confirmed=True, oi_confirmed=True, closed_candles=2, close_below_reference=True, failed_retest=True, no_new_high=True, liquidity_ok=True, rejection_ok=True, max_lifetime_minutes=15)
    assert lifecycle.state == "ADMITTED"
    assert trapped_longs_root_event_id("ABCUSDT", "event-1").startswith("trapped_longs:")
    assert trapped_longs_attempt_id("ABCUSDT", "event-1", 1).startswith("trapped_longs:")
    assert trapped_longs_evaluation_id("ABCUSDT", "event-1", 1).startswith("trapped_longs:")
    assert trapped_longs_admission_id("ABCUSDT", "event-1", 1).startswith("trapped_longs:")
    expired = advance_trapped_longs_lifecycle(root_created_at=T0, breakout_at=T0, observed_at=T0 + timedelta(minutes=16), event_revision=1, breakout_confirmed=True, oi_confirmed=True, closed_candles=2, close_below_reference=True, failed_retest=True, no_new_high=True, liquidity_ok=True, rejection_ok=True, max_lifetime_minutes=15)
    assert expired.state == "EXPIRED"


def test_trapped_longs_has_separate_delivery_flag():
    from app.config import AppConfig
    from app.domain import SignalDecision, SignalType
    from app.signals.delivery_policy import live_delivery_enabled
    decision = SignalDecision(symbol="ABCUSDT", event_id="trapped_longs:event-1", signal_type=SignalType.CONFIRM, grade="B", score=70, market_price=99, short_zone_low=98, short_zone_high=100, signal_time=T0, reasons=[], risk_flags=[], features_snapshot={}, score_breakdown={}, strategy_type="TRAPPED_LONGS_REVERSAL", strategy_subtype=TRAPPED_LONGS_REVERSAL)
    assert not live_delivery_enabled(decision, AppConfig())
    assert live_delivery_enabled(decision, AppConfig(trapped_longs_live_delivery_enabled=True))
