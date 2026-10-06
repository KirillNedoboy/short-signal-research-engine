"""Shared test helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from app.domain import EventState, EventStatus, SignalDecision, SignalProvenanceInput, SignalRecord, SignalType, SymbolFeatures


def _make_frame(
    prices: list[float],
    start: datetime | None = None,
    volume_start: float = 1000.0,
) -> pd.DataFrame:
    """Create a simple 1m OHLCV frame for tests."""

    start = start or datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    timestamps = [start + timedelta(minutes=index) for index in range(len(prices))]
    opens = [prices[0], *prices[:-1]]
    highs = [max(open_price, close) * 1.002 for open_price, close in zip(opens, prices, strict=True)]
    lows = [min(open_price, close) * 0.998 for open_price, close in zip(opens, prices, strict=True)]
    volumes = [volume_start + index * 2 for index in range(len(prices))]

    frame = pd.DataFrame(
        {
            "start_ms": [int(timestamp.timestamp() * 1000) for timestamp in timestamps],
            "open": opens,
            "high": highs,
            "low": lows,
            "close": prices,
            "volume": volumes,
            "turnover": [price * volume for price, volume in zip(prices, volumes, strict=True)],
            "timestamp": timestamps,
        }
    )
    frame = frame.set_index("timestamp", drop=False)
    return frame


def _make_features(**overrides: float | bool | str | datetime | None) -> SymbolFeatures:
    """Build a default strong feature snapshot."""

    base = dict(
        symbol="ONTUSDT",
        asof=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
        price=112.0,
        ret_5m=2.2,
        ret_15m=12.0,
        ret_1h=15.0,
        ret_4h=35.0,
        vwap=99.0,
        dist_to_vwap_pct=13.0,
        ema20=102.0,
        dist_to_ema20_pct=9.8,
        dist_to_ema20_atr=4.5,
        rsi_15m=78.5,
        upper_wick_ratio=0.25,
        lower_wick_ratio=0.05,
        body_pct=0.20,
        rejection_from_high_pct=1.6,
        close_position_in_range=0.10,
        vol_zscore_30m=2.0,
        vol_zscore_1h=1.5,
        atr_14=2.0,
        range_atr_ratio=2.2,
        oi_change_15m=None,
        oi_change_1h=None,
        funding_rate=None,
        event_range_pct=22.0,
        pullback_from_high_pct=3.5,
        distance_to_event_high_pct=3.5,
        inside_short_zone_flag=True,
        recent_high_breakout=False,
        latest_body_atr_ratio=0.6,
        latest_failed_retest=True,
        last_high=113.0,
        last_low=111.0,
        last_close=112.0,
        current_volume=2000.0,
        spread_pct=0.05,
        slippage_pct=0.05,
        orderbook_depth_usdt_1pct=100_000.0,
        orderbook_depth_usdt_2pct=200_000.0,
        liquidity_available=True,
    )
    base.update(overrides)
    return SymbolFeatures(**base)


def _make_event_state(**overrides: object) -> EventState:
    """Create a default pump event state."""

    base = dict(
        symbol="ONTUSDT",
        event_id="ONTUSDT:15m:1:111",
        state=EventStatus.PUMP_DETECTED,
        event_start_time=datetime(2026, 4, 13, 11, 30, tzinfo=timezone.utc),
        event_high=115.0,
        event_high_time=datetime(2026, 4, 13, 11, 55, tzinfo=timezone.utc),
        event_base_price=100.0,
        event_range_pct=15.0,
        event_features_snapshot={},
        trigger_window="15m",
        pullback_detected_at=None,
        pullback_depth_pct=None,
        pullback_low_price=None,
        zone_low=None,
        zone_high=None,
        signal_sent_at=None,
        signal_id=None,
        expires_at=datetime(2026, 4, 13, 15, 55, tzinfo=timezone.utc),
        updated_at=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return EventState(**base)


def _make_signal_decision(**overrides: object) -> SignalDecision:
    """Create a default signal decision."""

    base = dict(
        symbol="ONTUSDT",
        event_id="ONTUSDT:15m:1:111",
        signal_type=SignalType.AGGRESSIVE,
        grade="A",
        score=82,
        market_price=112.0,
        short_zone_low=110.5,
        short_zone_high=113.8,
        signal_time=datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc),
        reasons=["Dist to VWAP: +13.0%"],
        risk_flags=[],
        features_snapshot={
            "pullback_from_high_pct": 3.5,
            "dist_to_vwap_pct": 13.0,
            "upper_wick_ratio": 0.25,
            "rejection_from_high_pct": 1.6,
            "vol_zscore_30m": 2.0,
            "dist_to_ema20_atr": 4.5,
            "rsi_15m": 78.5,
            "ret_1h": 15.0,
            "ret_4h": 35.0,
            "range_atr_ratio": 2.2,
            "oi_change_15m": None,
            "oi_change_1h": None,
            "funding_rate": None,
            "signal_vwap": 99.0,
        },
        score_breakdown={"stretch": 15.0, "penalties": 0.0},
    )
    base.update(overrides)
    return SignalDecision(**base)


def _make_signal_provenance(**overrides: object) -> SignalProvenanceInput:
    decision_at = datetime(2026, 4, 13, 12, 5, tzinfo=timezone.utc)
    base = dict(
        strategy_family="BASELINE_PULLBACK",
        strategy_branch="BASELINE_PULLBACK",
        event_id="ONTUSDT:15m:1:111",
        root_event_id=None,
        decision_evaluation_id=None,
        admission_evaluation_id=None,
        code_version="test-code-version",
        config_hash="a" * 64,
        runtime_instance_id="test-runtime",
        runtime_started_at=decision_at - timedelta(minutes=1),
        decision_at=decision_at,
    )
    base.update(overrides)
    return SignalProvenanceInput(**base)


def _make_signal_record(**overrides: object) -> SignalRecord:
    """Create a saved signal record for outcome tests."""

    base = dict(
        id=1,
        symbol="ONTUSDT",
        signal_time=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
        signal_type="Aggressive",
        grade="A",
        score=82,
        market_price=105.0,
        short_zone_low=104.0,
        short_zone_high=106.0,
        event_high=108.0,
        event_base_price=95.0,
        event_range_pct=13.7,
        pullback_from_high_pct=2.8,
        dist_to_vwap_pct=9.0,
        upper_wick_ratio=0.2,
        rejection_from_high_pct=1.0,
        vol_zscore_30m=1.5,
        dist_to_ema20_atr=3.2,
        rsi_15m=74.0,
        ret_1h=10.0,
        ret_4h=22.0,
        range_atr_ratio=1.6,
        oi_change_15m=None,
        oi_change_1h=None,
        funding_rate=None,
        context_json={"signal_vwap": 99.0},
        telegram_sent=True,
        created_at=datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
    )
    base.update(overrides)
    return SignalRecord(**base)


@pytest.fixture
def make_frame():
    return _make_frame


@pytest.fixture
def make_features():
    return _make_features


@pytest.fixture
def make_event_state():
    return _make_event_state


@pytest.fixture
def make_signal_decision():
    return _make_signal_decision


@pytest.fixture
def make_signal_provenance():
    return _make_signal_provenance


@pytest.fixture
def make_signal_record():
    return _make_signal_record
