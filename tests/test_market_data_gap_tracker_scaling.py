"""Old, evicted gaps must not amplify each new closed candle."""
from dataclasses import replace
from datetime import timedelta
from statistics import median
from time import perf_counter

from app.market_data.continuity import GapTracker
from app.market_data.models import REST_BACKFILL_SOURCE
from test_market_data_continuity import BASE, candle


def test_old_gap_cost_is_bounded_by_retained_ring():
    tracker = GapTracker(max_candles_per_symbol=17)
    tracker.observe_closed_candle(candle(0))
    tracker.observe_closed_candle(candle(100_000))
    samples = []
    for minute in range(100_001, 100_031):
        start = perf_counter()
        tracker.observe_closed_candle(candle(minute))
        samples.append((perf_counter() - start) * 1000)
    assert median(samples) < 15, f"old-gap median={median(samples):.2f}ms"
    assert len(tracker.active_gaps) == 0
    assert tracker.get_health().gaps_detected == 1
    assert len(tracker.events) == 1
    assert not tracker.has_gap_since("BTCUSDT", BASE + timedelta(minutes=100_013))
    assert not tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=50_000))


def test_evicted_historical_gap_and_recent_gap_remain_distinct_after_rest_fill():
    tracker = GapTracker(max_candles_per_symbol=5)
    for minute in (0, 50_000, 50_001, 50_002, 50_004):
        tracker.observe_closed_candle(candle(minute))
    assert tracker.get_health().gaps_detected == 2
    assert tracker.has_gap_since("BTCUSDT", BASE + timedelta(minutes=50_001))
    tracker.observe_closed_candle(candle(50_003, source=REST_BACKFILL_SOURCE))
    assert len(tracker.active_gaps) == 0
    assert tracker.get_health().gaps_backfilled == 1
    assert tracker.get_health().gaps_detected == 2
    assert not tracker.has_gap_since("BTCUSDT", BASE + timedelta(minutes=50_001))
    assert not tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=25_000))
    tracker.observe_closed_candle(candle(50_005))
    assert tracker.get_health().gaps_detected == 2


def test_partial_old_gap_fill_does_not_split_or_repeat_detection():
    tracker = GapTracker(max_candles_per_symbol=4)
    for minute in (0, 50_000, 50_001, 50_002, 10_000, 50_003):
        tracker.observe_closed_candle(candle(minute))
    assert len(tracker.active_gaps) == 0
    assert tracker.get_health().gaps_detected == 1
    assert len(tracker.events) == 1


def test_thousand_independent_evicted_gaps_do_not_accumulate_or_count_as_repairs(caplog):
    caplog.set_level("ERROR", logger="app.market_data.continuity")
    tracker = GapTracker(max_candles_per_symbol=17, max_evidence_events=23)
    durations = []
    for batch in range(4):
        start = perf_counter()
        for index in range(batch * 1000, (batch + 1) * 1000):
            tracker.observe_closed_candle(candle(index * 3))
            assert tracker.get_health().active_gaps <= 16
        durations.append(perf_counter() - start)
    health = tracker.get_health()
    assert health.gaps_detected == 3999
    assert health.gaps_backfilled == 0
    assert health.active_gaps <= 16
    assert len(tracker.events) == 23
    assert max(durations) < min(durations) * 2.5, durations
    oldest = tracker.get_candles("BTCUSDT")[0].open_time
    assert not tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=2))
    assert tracker.has_gap_since("BTCUSDT", oldest)
    assert tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=11995))
    tracker.observe_closed_candle(candle(11995, source=REST_BACKFILL_SOURCE))
    assert tracker.get_health().gaps_backfilled == 0
    assert tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=11996))
    tracker.observe_closed_candle(candle(11996, source=REST_BACKFILL_SOURCE))
    assert tracker.get_health().gaps_backfilled == 1
    assert not tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=11995))


def test_many_symbols_keep_gap_lookup_local(caplog):
    caplog.set_level("ERROR", logger="app.market_data.continuity")
    tracker = GapTracker(max_candles_per_symbol=40)
    symbols = [f"S{index}USDT" for index in range(40)]
    for index in range(40):
        for symbol in symbols:
            tracker.observe_closed_candle(replace(candle(index * 2), symbol=symbol))
    assert tracker.get_health().active_gaps == len(symbols) * 39
    start = perf_counter()
    for symbol in symbols:
        tracker.observe_closed_candle(replace(candle(80), symbol=symbol))
    assert perf_counter() - start < 2.0
    assert tracker.get_health().active_gaps == len(symbols) * 39
    assert all(tracker.has_gap_at(symbol, BASE + timedelta(minutes=79)) for symbol in symbols)
    assert not tracker.has_gap_at("UNSEENUSDT", BASE + timedelta(minutes=79))


def test_window_boundary_and_late_duplicate_do_not_resurrect_retired_gap():
    tracker = GapTracker(max_candles_per_symbol=4)
    for minute in (0, 2, 3, 4):
        tracker.observe_closed_candle(candle(minute))
    assert tracker.has_gap_at("BTCUSDT", BASE + timedelta(minutes=1))
    tracker.observe_closed_candle(candle(5))
    assert tracker.active_gaps == ()
    assert tracker.get_symbol_state("BTCUSDT").value == "CONTIGUOUS"
    tracker.observe_closed_candle(candle(1, source=REST_BACKFILL_SOURCE))
    tracker.observe_closed_candle(candle(1, source=REST_BACKFILL_SOURCE))
    assert tracker.active_gaps == ()
    assert tracker.get_health().gaps_backfilled == 0
    assert tracker.get_health().gaps_detected == 1
