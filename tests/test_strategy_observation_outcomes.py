from datetime import datetime, timedelta, timezone

from app.outcomes.strategy_observations import evaluate_strategy_observation


UTC = timezone.utc


def test_strategy_observation_outcome_calculates_horizons_excursions_and_timing(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0] * 16, start=start)
    frame.loc[frame.index[3], "low"] = 90.0
    frame.loc[frame.index[4], "high"] = 120.0
    frame.loc[frame.index[1], "close"] = 99.0

    outcome = evaluate_strategy_observation(
        observed_at=start,
        entry_price=100.0,
        event_high=110.0,
        frame_1m=frame,
        now=start + timedelta(minutes=16),
    )

    assert outcome["data_status"] == "complete"
    assert outcome["mfe_pct"] == 10.0
    assert outcome["mae_pct"] == 20.0
    assert outcome["time_to_mfe_minutes"] == 3.0
    assert outcome["time_to_mae_minutes"] == 4.0
    assert outcome["new_high_after_observation"] is True
    assert outcome["horizons"]["1m"]["price"] == 99.0
    assert outcome["horizons"]["1m"]["price_change_pct"] == -1.0
    assert outcome["horizons"]["15m"]["price"] == 100.0


def test_strategy_observation_outcome_marks_partial_coverage_incomplete(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0] * 5, start=start)

    outcome = evaluate_strategy_observation(
        observed_at=start,
        entry_price=100.0,
        event_high=110.0,
        frame_1m=frame,
        now=start + timedelta(minutes=5),
    )

    assert outcome["data_status"] == "incomplete"
    assert outcome["horizons"]["1m"]["price"] is not None
    assert outcome["horizons"]["5m"]["price"] is None
    assert outcome["mfe_pct"] is not None
    assert outcome["new_high_after_observation"] is False


def test_strategy_observation_outcome_keeps_not_yet_mature_data_incomplete(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0], start=start)

    outcome = evaluate_strategy_observation(
        observed_at=start,
        entry_price=100.0,
        event_high=110.0,
        frame_1m=frame,
        now=start,
    )

    assert outcome["data_status"] == "incomplete"
    assert outcome["mfe_pct"] is None
    assert outcome["mae_pct"] is None
    assert outcome["new_high_after_observation"] is None
    assert outcome["data_reason"] == "AWAITING_CLOSED_FUTURE_CANDLES"
    assert all(value["price"] is None for value in outcome["horizons"].values())


def test_strategy_observation_outcome_marks_mature_missing_candles_unknown(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0], start=start)

    outcome = evaluate_strategy_observation(
        observed_at=start,
        entry_price=100.0,
        event_high=110.0,
        frame_1m=frame,
        now=start + timedelta(minutes=20),
    )

    assert outcome["data_status"] == "unknown"
    assert outcome["data_reason"] == "NO_CLOSED_FUTURE_CANDLES"


def test_strategy_observation_outcome_keeps_early_empty_market_data_incomplete(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    empty_frame = make_frame([100.0], start=start).iloc[0:0]

    outcome = evaluate_strategy_observation(
        observed_at=start,
        entry_price=100.0,
        event_high=110.0,
        frame_1m=empty_frame,
        now=start + timedelta(minutes=5),
    )

    assert outcome["data_status"] == "incomplete"
    assert outcome["data_reason"] == "AWAITING_CLOSED_FUTURE_CANDLES"
