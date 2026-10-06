"""Production-derived regression fixtures for LOW_VOLUME_EXTENSION_FAILURE."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from app.config import AppConfig
from app.domain import EventState, EventStatus
from app.signals.climax import evaluate_climax


FIXTURES = json.loads(
    (Path(__file__).parent / "fixtures" / "low_volume_regression_fixtures.json").read_text()
)["fixtures"]


def _case(signal_id: int) -> dict:
    return next(item for item in FIXTURES if item["signal_id"] == signal_id)


def _frame(case: dict) -> pd.DataFrame:
    rows = case["candles_1m"]
    frame = pd.DataFrame(rows)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
    frame["start_ms"] = frame["timestamp"].astype("int64") // 1_000_000
    frame["turnover"] = frame["close"] * frame["volume"]
    return frame.set_index("timestamp", drop=False)


def _state(case: dict) -> EventState:
    event_ts = int(case["event_id"].split(":")[2])
    event_time = datetime.fromtimestamp(event_ts, tz=timezone.utc)
    return EventState(
        symbol=case["symbol"],
        event_id=case["event_id"],
        state=EventStatus.PUMP_DETECTED,
        event_start_time=event_time,
        event_high=case["event_high"],
        event_high_time=event_time,
        event_base_price=case["context"].get("previous_leg_high"),
        event_range_pct=case["context"].get("event_range_pct"),
        trigger_window=case["event_id"].split(":")[1],
    )


def _config() -> AppConfig:
    return AppConfig(
        climax_short_enabled=False,
        volume_climax_unwind_enabled=False,
        low_volume_extension_enabled=True,
        low_volume_high_liquidity_risk_mode="warn",
    )


def _features(make_features, case: dict):
    c = case["context"]
    return make_features(
        symbol=case["symbol"],
        asof=datetime.fromisoformat(c["asof"]),
        price=c["price"],
        ret_5m=c["ret_5m"],
        ret_15m=c["ret_15m"],
        ret_1h=c["ret_1h"],
        ret_4h=c["ret_4h"],
        vwap=c["vwap"],
        dist_to_vwap_pct=c["dist_to_vwap_pct"],
        ema20=c["ema20"],
        dist_to_ema20_pct=c["dist_to_ema20_pct"],
        dist_to_ema20_atr=c["dist_to_ema20_atr"],
        rsi_15m=c["rsi_15m"],
        upper_wick_ratio=c["upper_wick_ratio"],
        lower_wick_ratio=c["lower_wick_ratio"],
        body_pct=c["body_pct"],
        rejection_from_high_pct=c["rejection_from_high_pct"],
        close_position_in_range=c["close_position_in_range"],
        vol_zscore_30m=c["vol_zscore_30m"],
        vol_zscore_1h=c["vol_zscore_1h"],
        atr_14=c["atr_14"],
        range_atr_ratio=c["range_atr_ratio"],
        oi_change_15m=c["oi_change_15m"],
        oi_change_1h=c["oi_change_1h"],
        funding_rate=c["funding_rate"],
        oi_change_pct=c["oi_change_pct"],
        derivatives_status=c["derivatives_status"],
        event_range_pct=c["event_range_pct"],
        pullback_from_high_pct=c["pullback_from_high_pct"],
        distance_to_event_high_pct=c["distance_to_event_high_pct"],
        latest_body_atr_ratio=c["latest_body_atr_ratio"],
        latest_failed_retest=c["latest_failed_retest"],
        last_high=c["last_high"],
        last_low=c["last_low"],
        last_close=c["last_close"],
        current_volume=c["current_volume"],
        spread_pct=c["spread_pct"],
        slippage_pct=c["slippage_pct"],
        orderbook_depth_usdt_1pct=c["orderbook_depth_usdt_1pct"],
        orderbook_depth_usdt_2pct=c["orderbook_depth_usdt_2pct"],
        liquidity_available=c["liquidity_available"],
    )


def test_actual_ake_low_volume_survives_as_grade_b(make_features):
    case = _case(91)
    result = evaluate_climax(_state(case), _features(make_features, case), _frame(case), _config())
    assert result.actionable
    assert result.subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    assert result.grade == "B"
    assert result.metadata["event_high"] == case["event_high"]
    assert result.metadata["liquidity_warning"] is True
    assert result.metadata["failed_retest_confirmed"] is True


def test_actual_first_esports_low_volume_is_vetoed(make_features):
    case = _case(90)
    result = evaluate_climax(_state(case), _features(make_features, case), _frame(case), _config())
    assert not result.actionable
    assert result.subtype is None
    assert set(result.veto_reasons) & {
        "active_short_squeeze",
        "price_acceleration_resumed",
        "lower_high_or_failed_retest_missing",
        "rejection_missing",
    }
    assert result.metadata["failed_retest_confirmed"] is False


def test_actual_volume_climax_fixture_uses_untouched_admission(make_features):
    case = _case(92)
    config = AppConfig(
        climax_short_enabled=False,
        volume_climax_unwind_enabled=True,
        low_volume_extension_enabled=False,
    )
    result = evaluate_climax(_state(case), _features(make_features, case), _frame(case), config)
    assert result.actionable
    assert result.subtype == "VOLUME_CLIMAX_UNWIND"
    assert result.score == 70
    assert result.metadata["volume_ratio"] >= 3.0
    assert result.metadata["volume_ratio"] == result.metadata["current_previous_volume_ratio"]


def test_low_volume_requires_equal_windows_and_closed_confirmation(make_features):
    case = _case(91)
    frame = _frame(case).iloc[:-1].copy()
    frame = frame.iloc[:4]
    result = evaluate_climax(_state(case), _features(make_features, case), frame, _config())
    assert "insufficient_closed_candles_after_high" in result.veto_reasons


def test_formatter_precision_regression():
    from app.signals.formatter import format_signal_message
    from app.domain import SignalDecision, SignalType

    decision = SignalDecision(
        symbol="AKEUSDT", event_id="e", signal_type=SignalType.CONFIRM, grade="B", score=70,
        market_price=0.00169840, short_zone_low=0.0018, short_zone_high=0.00195910,
        signal_time=datetime(2026, 7, 17, 20, 54, tzinfo=timezone.utc), reasons=[], risk_flags=[],
        features_snapshot={}, score_breakdown={}, strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE",
        strategy_metadata={"event_high": 0.00195910, "entry_distance_below_high_pct": 13.3071308254},
    )
    text = format_signal_message(decision, "UTC")
    assert "Event high: 0.00195910" in text
    assert "Event high: 0.00\n" not in text
