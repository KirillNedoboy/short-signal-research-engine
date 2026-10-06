from datetime import datetime, timedelta, timezone

from app.features.builder import FeatureBuilder
from app.market.candles import (
    closed_1m_rows,
    complete_5m_ohlcv,
    normalize_utc,
    validate_closed_1m_frame,
)


UTC = timezone.utc


def test_closed_1m_rows_exclude_the_candle_still_forming(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0, 101.0, 120.0], start=start)

    closed = closed_1m_rows(frame, start + timedelta(minutes=2, seconds=30))

    assert closed["high"].tolist() == frame["high"].iloc[:2].tolist()


def test_complete_5m_ohlcv_requires_all_underlying_closed_rows(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0 + index for index in range(10)], start=start)

    partial = complete_5m_ohlcv(frame, start + timedelta(minutes=9, seconds=30))
    complete = complete_5m_ohlcv(frame, start + timedelta(minutes=10))

    assert partial["timestamp"].tolist() == [start + timedelta(minutes=5)]
    assert complete["timestamp"].tolist() == [
        start + timedelta(minutes=5),
        start + timedelta(minutes=10),
    ]


def test_feature_builder_keeps_partial_5m_structure_out_of_rejection_and_breakout(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0] * 10 + [100.0, 101.0, 102.0, 101.0, 120.0], start=start)
    frame.loc[frame.index[10], "open"] = 120.0
    frame.loc[frame.index[-1], ["open", "high", "low", "close"]] = [120.0, 125.0, 100.0, 110.0]
    builder = FeatureBuilder()

    partial = builder.build("TESTUSDT", frame, market_asof=start + timedelta(minutes=14, seconds=30))
    complete = builder.build("TESTUSDT", frame, market_asof=start + timedelta(minutes=15))

    assert partial.last_high == float(frame["high"].iloc[:14].max())
    assert partial.last_high_time == start + timedelta(minutes=13)
    assert partial.recent_high_breakout is False
    assert partial.latest_failed_retest is False
    assert complete.last_high == 125.0
    assert complete.last_high_time == start + timedelta(minutes=15)
    assert complete.recent_high_breakout is True
    assert complete.latest_failed_retest is True


def test_feature_builder_uses_highest_closed_1m_candle_and_ignores_forming_high(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0] * 20, start=start)
    frame.loc[frame.index[5], "high"] = 150.0
    frame.loc[frame.index[18], "high"] = 110.0
    frame.loc[frame.index[19], "high"] = 200.0
    builder = FeatureBuilder()

    features = builder.build("TESTUSDT", frame, market_asof=start + timedelta(minutes=19, seconds=30))

    assert features.last_high == 150.0
    assert features.last_high_time == start + timedelta(minutes=6)


def test_normalize_utc_treats_naive_sqlite_datetime_as_utc() -> None:
    naive = datetime(2026, 7, 24, 12, 0)

    normalized = normalize_utc(naive)

    assert normalized == datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def test_admission_rejects_forming_candle(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0, 101.0], start=start)

    assert validate_closed_1m_frame(frame, start + timedelta(minutes=1, seconds=30)) == "STALE_MARKET_DATA"


def test_admission_rejects_gap_duplicate_future_and_stale_data(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    asof = start + timedelta(minutes=5)

    gap = make_frame([100.0, 101.0, 102.0], start=start).drop(index=start + timedelta(minutes=1))
    assert validate_closed_1m_frame(gap, asof) == "MARKET_DATA_DISCONTINUITY"

    duplicate = make_frame([100.0, 101.0], start=start)
    import pandas as pd
    duplicate = pd.concat([duplicate, duplicate.iloc[[1]]])
    assert validate_closed_1m_frame(duplicate, asof) == "MARKET_DATA_DISCONTINUITY"

    future = make_frame([100.0, 101.0], start=start)
    future.index = [start, asof + timedelta(minutes=1)]
    future["timestamp"] = future.index
    assert validate_closed_1m_frame(future, asof) == "STALE_MARKET_DATA"

    stale = make_frame([100.0, 101.0], start=start)
    assert validate_closed_1m_frame(stale, asof + timedelta(minutes=20)) == "STALE_MARKET_DATA"
