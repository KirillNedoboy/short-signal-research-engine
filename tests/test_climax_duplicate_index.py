"""Fast-monitor climax metadata must select candles by row, not duplicate labels."""
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.signals.climax import evaluate_climax_bundle


@pytest.mark.parametrize("strict_closed_candles", [False, True])
def test_duplicate_datetime_index_keeps_post_high_rows_positional(
    make_frame, make_features, make_event_state, strict_closed_candles
):
    start = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    frame = make_frame([100.0 + i for i in range(15)], start=start)
    # A recycled candle label precedes a different timestamp in the next row.
    # Label-based .loc on the selected index expands this duplicate label.
    index = frame.index.to_list()
    index[8] = index[7]
    frame.index = index
    state = make_event_state(event_high=110.0, event_high_time=start + timedelta(minutes=7))
    features = make_features(asof=start + timedelta(minutes=15), price=114.0)
    config = SimpleNamespace(volume_climax_unwind_enabled=False, low_volume_extension_enabled=False)

    result = evaluate_climax_bundle(
        state, features, frame, config, strict_closed_candles=strict_closed_candles
    ).selected

    assert result.metadata["closed_candles_after_high"] == 3  # 12:08, 12:09, 12:10
    assert result.metadata["post_high_high"] == pytest.approx(frame["high"].iloc[10:15].max() if strict_closed_candles else frame["high"].iloc[8:15].max())
    assert result.metadata["partial_candle_excluded"] is False


def test_duplicate_datetime_index_does_not_count_candles_at_or_before_high(
    make_frame, make_features, make_event_state
):
    start = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    frame = make_frame([100.0 + i for i in range(11)], start=start)
    index = frame.index.to_list()
    index[8] = index[7]
    frame.index = index
    result = evaluate_climax_bundle(
        make_event_state(event_high_time=start + timedelta(minutes=10)),
        make_features(asof=start + timedelta(minutes=11)),
        frame,
        SimpleNamespace(volume_climax_unwind_enabled=False, low_volume_extension_enabled=False),
    ).selected
    assert result.metadata["closed_candles_after_high"] == 0
    assert result.metadata["post_high_high"] is None
    assert result.metadata["post_high_retest_high"] is None
