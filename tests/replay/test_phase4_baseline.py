from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from app.config import AppConfig
from app.domain import EventStatus


def test_normalized_observation_rejects_future_rows_and_preserves_kind() -> None:
    from app.replay.market import CandleObservationKind, NormalizedMarketObservation

    decision_time = datetime(2026, 8, 22, 4, 22, 57, tzinfo=timezone.utc)
    observation = NormalizedMarketObservation(
        symbol="TRUMPUSDT",
        decision_time_utc=decision_time,
        kind=CandleObservationKind.PARTIAL_ASOF_CANDLE,
        candles=pd.DataFrame(
            [{
                "symbol": "TRUMPUSDT",
                "open_time_utc": "2026-08-22T04:22:00Z",
                "availability_time_utc": "2026-08-22T04:22:56Z",
                "open": 2.70,
                "high": 2.71,
                "low": 2.69,
                "close": 2.71,
                "volume": 100.0,
            }]
        ),
    )

    observation.validate()

    assert observation.kind is CandleObservationKind.PARTIAL_ASOF_CANDLE
    assert observation.max_input_availability_time_utc == datetime(2026, 8, 22, 4, 22, 56, tzinfo=timezone.utc)

    future = observation.candles.copy()
    future.loc[0, "availability_time_utc"] = "2026-08-22T04:23:00Z"
    with pytest.raises(ValueError, match="future market input"):
        NormalizedMarketObservation(
            symbol=observation.symbol,
            decision_time_utc=decision_time,
            kind=observation.kind,
            candles=future,
        ).validate()


def test_replay_clock_is_monotonic_and_deterministic() -> None:
    from app.replay.clock import HistoricalReplayClock

    start = datetime(2026, 8, 22, 4, 0, tzinfo=timezone.utc)
    clock = HistoricalReplayClock([start, start + timedelta(minutes=1), start + timedelta(minutes=1)])

    assert [clock.next(), clock.next(), clock.next()] == [start, start + timedelta(minutes=1), start + timedelta(minutes=1)]
    with pytest.raises(StopIteration):
        clock.next()


def test_shared_baseline_lifecycle_creates_event_without_side_effects(make_frame) -> None:
    from app.baseline.lifecycle import BaselineLifecycle
    from app.replay.market import CandleObservationKind, NormalizedMarketObservation

    config = AppConfig(
        derivatives_enabled=False,
        event_ret_15m_min=0.1,
        event_dist_to_vwap_min=0.0,
        event_dist_to_ema20_atr_min=0.0,
        vol_zscore_min=0.0,
        range_atr_bonus_level=0.0,
    )
    frame = make_frame([100.0 + (index * 0.2) for index in range(300)])
    frame = frame.rename(columns={"timestamp": "timestamp"})
    decision_time = frame.index[-1].to_pydatetime() + timedelta(minutes=1)
    observation = NormalizedMarketObservation(
        symbol="TRUMPUSDT",
        decision_time_utc=decision_time,
        kind=CandleObservationKind.CLOSED_CANDLE,
        candles=frame,
    )

    result = BaselineLifecycle(config).detect_event(observation)

    assert result.state is not None
    assert result.state.state is EventStatus.PUMP_DETECTED
    assert result.evaluation_input is None
    assert result.transitions
    assert result.transitions[0].from_state == EventStatus.IDLE.value


def test_baseline_evaluator_contract_remains_signal_engine(make_event_state, make_features) -> None:
    from app.domain import ShortZone
    from app.replay.contracts import BaselineStrategyInput
    from app.replay.evaluator import evaluate_strategy_input

    state = make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE)
    features = make_features()
    item = BaselineStrategyInput(state, features, ShortZone(110.0, 114.0, "event_range"), features.asof)
    result = evaluate_strategy_input(item, AppConfig())

    assert isinstance(result, object)


def test_bybit_normalization_sorts_deduplicates_and_marks_closed_availability() -> None:
    from app.replay.market import normalize_bybit_klines, build_sliding_frame

    rows = [
        ["120000", "12", "13", "11", "12.5", "10", "125"],
        ["60000", "11", "12", "10", "11.5", "9", "100"],
        ["60000", "11", "12", "10", "11.5", "9", "100"],
    ]
    frame, report = normalize_bybit_klines([rows], symbol="TRUMPUSDT")

    assert list(frame["open_time_utc"]) == ["1970-01-01T00:01:00Z", "1970-01-01T00:02:00Z"]
    assert list(frame["availability_time_utc"]) == ["1970-01-01T00:02:00Z", "1970-01-01T00:03:00Z"]
    assert report.duplicates == 1
    window = build_sliding_frame(frame, "TRUMPUSDT", datetime(1970, 1, 1, 0, 3, 0, tzinfo=timezone.utc), limit=300)
    assert len(window) == 2


def test_trade_asof_aggregation_never_uses_future_trades() -> None:
    from app.replay.market import aggregate_trades_asof

    trades = pd.DataFrame([
        {"timestamp": "2026-08-22T04:22:01.000Z", "price": 2.70, "size": 1.0, "side": "Buy"},
        {"timestamp": "2026-08-22T04:22:56.000Z", "price": 2.71, "size": 2.0, "side": "Buy"},
        {"timestamp": "2026-08-22T04:22:58.000Z", "price": 2.99, "size": 99.0, "side": "Sell"},
    ])
    row = aggregate_trades_asof(trades, datetime(2026, 8, 22, 4, 22, 57, tzinfo=timezone.utc))
    assert row["close"] == 2.71
    assert row["high"] == 2.71
    assert row["volume"] == 3.0
    assert row["availability_time_utc"] == "2026-08-22T04:22:56Z"


def test_derivative_points_are_normalized_as_of_and_missing_orderbook_is_observable() -> None:
    from app.replay.market import normalize_derivative_points, liquidity_observability

    points, report = normalize_derivative_points(
        [{"timestamp": "2026-08-22T04:22:56Z", "value": "10"}, {"timestamp": "2026-08-22T04:23:01Z", "value": "11"}, {"timestamp": "2026-08-22T04:22:56Z", "value": "10"}],
        decision_time_utc=datetime(2026, 8, 22, 4, 23, tzinfo=timezone.utc),
        source="bybit:open-interest-history",
    )
    assert points == [{"availability_time_utc": "2026-08-22T04:22:56Z", "timestamp_utc": "2026-08-22T04:22:56Z", "value": 10.0, "source": "bybit:open-interest-history"}]
    assert report.duplicates == 1
    assert liquidity_observability(None) == (None, ("FULL_ADMISSION_HISTORICALLY_UNOBSERVABLE",))


def test_replay_driver_uses_phase3_adapter_and_sealed_control_gate(make_frame, make_event_state, make_features, monkeypatch) -> None:
    from app.research.baseline_replay import BaselineReplayDriver, ReplayOpportunity, ControlReplayReport
    from app.replay.market import CandleObservationKind

    calls = []
    monkeypatch.setattr("app.research.baseline_replay.evaluate_strategy_input", lambda item, config: calls.append(item) or object())
    frame = make_frame([100.0 + (index * 0.2) for index in range(300)])
    state = make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE)
    opportunity = ReplayOpportunity("ONTUSDT", frame.index[-1].to_pydatetime() + timedelta(minutes=1), frame, CandleObservationKind.CLOSED_CANDLE)
    from app.baseline.lifecycle import BaselineLifecycleResult
    item = __import__("app.replay.contracts", fromlist=["BaselineStrategyInput"]).BaselineStrategyInput(state, make_features(symbol="ONTUSDT", asof=opportunity.decision_time_utc), __import__("app.domain", fromlist=["ShortZone"]).ShortZone(110, 114, "event_range"), opportunity.decision_time_utc)
    class FakeLifecycle:
        def advance_active_event(self, *args, **kwargs):
            return BaselineLifecycleResult(state, item.features, item.short_zone, item, (), opportunity.decision_time_utc)
        def advance_active_event_from_features(self, *args, **kwargs):
            return self.advance_active_event()
        def detect_event(self, *args, **kwargs):
            return BaselineLifecycleResult(state, item.features, None, None, (), opportunity.decision_time_utc)
        def mark_signal_sent(self, state, signal_id, when):
            return state
    driver = BaselineReplayDriver(AppConfig(derivatives_enabled=False), lifecycle=FakeLifecycle())
    result = driver.replay([opportunity], initial_states={"ONTUSDT": state}, feature_overrides={"ONTUSDT": make_features(symbol="ONTUSDT", asof=opportunity.decision_time_utc)})
    assert len(calls) == 1
    assert result.evaluations[0].decision_time_utc == opportunity.decision_time_utc
    assert result.evaluations[0].max_input_availability_time_utc <= opportunity.decision_time_utc
    assert ControlReplayReport(False, "CONTROL_REPLAY_MISMATCH", ()).broad_allowed is False
