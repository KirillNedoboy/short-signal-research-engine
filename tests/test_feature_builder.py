from datetime import datetime, timedelta, timezone

from app.features.builder import FeatureBuilder


def test_feature_builder_generates_live_metrics(make_frame) -> None:
    prices = [100 + (index * 0.05) for index in range(260)]
    prices[-20:] = [prices[-21] + (step * 0.5) for step in range(1, 21)]
    frame = make_frame(prices)
    builder = FeatureBuilder()

    features = builder.build("TESTUSDT", frame)

    assert features.ret_15m > 0
    assert features.ret_1h > 0
    assert features.dist_to_vwap_pct > 0
    assert features.atr_14 >= 0
    assert 0 <= features.upper_wick_ratio <= 1
    assert features.symbol == "TESTUSDT"


def test_feature_builder_extracts_derivatives_diagnostics(make_frame) -> None:
    prices = [100 + (index * 0.05) for index in range(260)]
    prices[-20:] = [prices[-21] + (step * 0.5) for step in range(1, 21)]
    frame = make_frame(prices)
    builder = FeatureBuilder()

    features = builder.build(
        "TESTUSDT",
        frame,
        derivatives={
            "derivatives_status": "OK",
            "derivatives_reasons": [],
            "data_quality_warnings": [],
            "open_interest": [
                {"openInterest": "1200"},
                {"openInterest": "1000"},
                {"openInterest": "950"},
                {"openInterest": "900"},
                {"openInterest": "800"},
            ],
            "funding": [{"fundingRate": "0.0005"}],
        },
    )

    assert features.open_interest == 1200.0
    assert features.oi_change_pct == 20.0
    assert features.oi_change_15m == 20.0
    assert features.oi_change_1h == 50.0
    assert features.funding_rate == 0.0005
    assert features.derivatives_status == "OK"
    assert features.derivatives_reasons == []
    assert features.data_quality_warnings == []


def test_feature_builder_highest_closed_candle_with_duplicate_index(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    frame = make_frame([100.0] * 20, start=start)
    frame.iloc[5, frame.columns.get_loc("high")] = 150.0
    frame.iloc[6, frame.columns.get_loc("high")] = 110.0
    frame.iloc[18, frame.columns.get_loc("high")] = 150.0
    frame.iloc[19, frame.columns.get_loc("high")] = 200.0
    index = frame.index.to_list()
    index[6] = index[5]
    frame.index = index

    features = FeatureBuilder().build(
        "TESTUSDT", frame, market_asof=start + timedelta(minutes=19, seconds=30)
    )

    assert features.last_high == 150.0
    assert features.last_high_time == start + timedelta(minutes=6)
