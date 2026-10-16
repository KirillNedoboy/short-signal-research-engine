from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from app.market_data.models import ConnectionEpoch, MarketCandle, MarketTickerSnapshot
from app.market_data.parity import (
    KlineClassification,
    ParityMonitor,
    TickerMatchClassification,
    compare_closed_kline,
)

BASE = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)
EPOCH = ConnectionEpoch(0, 1)


def ticker(
    price: str | None,
    *,
    received_at: datetime = BASE,
    exchange_timestamp: datetime | None = BASE,
) -> MarketTickerSnapshot:
    return MarketTickerSnapshot(
        symbol="BTCUSDT",
        last_price=Decimal(price) if price is not None else None,
        mark_price=Decimal(99),
        exchange_timestamp=exchange_timestamp,
        received_at=received_at,
        connection_epoch=EPOCH,
    )


def rest_ticker(price: str | None = "100") -> dict[str, str]:
    row = {"symbol": "BTCUSDT", "markPrice": "99"}
    if price is not None:
        row["lastPrice"] = price
    return row


def field(result, name: str):
    return next(item for item in result.fields if item.symbol == "BTCUSDT" and item.field == name)


def test_exact_value_observed_during_bounded_request_window() -> None:
    monitor = ParityMonitor(ws_symbols=("BTCUSDT",))
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    monitor.observe_ticker(ticker("100", exchange_timestamp=BASE))

    result = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()],
        server_time=BASE,
        response_received_at=BASE + timedelta(milliseconds=200),
        response_received_monotonic=1.2,
    )

    assert field(result, "lastPrice").classification is TickerMatchClassification.EXACT_OBSERVED_IN_WINDOW
    assert result.exact_observed == 2
    assert result.coverage.intersection == ("BTCUSDT",)
    assert monitor.max_high_water_states == 1


def test_exact_carry_in_and_post_anchor_values_are_temporally_classified() -> None:
    monitor = ParityMonitor(ws_symbols=("BTCUSDT",))
    monitor.observe_ticker(ticker("100", received_at=BASE - timedelta(seconds=1), exchange_timestamp=BASE - timedelta(seconds=1)))
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    before = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=BASE,
        response_received_at=BASE + timedelta(milliseconds=100), response_received_monotonic=1.1,
    )
    assert field(before, "lastPrice").classification is TickerMatchClassification.EXACT_NEAREST_BEFORE

    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=2.0)
    monitor.observe_ticker(ticker("100", received_at=BASE + timedelta(milliseconds=50), exchange_timestamp=BASE + timedelta(milliseconds=50)))
    after = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=BASE,
        response_received_at=BASE + timedelta(milliseconds=100), response_received_monotonic=2.1,
    )
    assert field(after, "lastPrice").classification is TickerMatchClassification.EXACT_NEAREST_AFTER


def test_temporal_difference_missing_fields_and_stale_state() -> None:
    monitor = ParityMonitor(ws_symbols=("BTCUSDT",), stale_after_seconds=45)
    monitor.observe_ticker(ticker("101", received_at=BASE - timedelta(seconds=46)))
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    stale = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=None,
        response_received_at=BASE + timedelta(seconds=1), response_received_monotonic=2.0,
    )
    assert field(stale, "lastPrice").classification is TickerMatchClassification.WS_STALE
    assert stale.anchor_source == "REQUEST_RESPONSE_MIDPOINT"

    monitor.observe_ticker(ticker("101", received_at=BASE))
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=3.0)
    different = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=BASE,
        response_received_at=BASE + timedelta(seconds=1), response_received_monotonic=4.0,
    )
    assert field(different, "lastPrice").classification is TickerMatchClassification.TEMPORAL_VALUE_DIFFERENCE
    assert field(different, "bid1Price").classification is TickerMatchClassification.REST_FIELD_UNOBSERVED


def test_unobserved_ws_field_and_buffer_overflow_are_explicit() -> None:
    monitor = ParityMonitor(ws_symbols=("BTCUSDT",), max_states_per_symbol=1, max_total_states=1)
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    monitor.observe_ticker(ticker(None))
    monitor.observe_ticker(ticker("100", received_at=BASE + timedelta(milliseconds=1)))
    result = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=BASE,
        response_received_at=BASE + timedelta(seconds=1), response_received_monotonic=2.0,
    )
    assert result.overflowed is True
    assert {item.classification for item in result.fields} == {TickerMatchClassification.NOT_COMPARABLE}

    clean = ParityMonitor(ws_symbols=("BTCUSDT",))
    clean.observe_ticker(ticker(None))
    clean.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    missing = clean.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=BASE,
        response_received_at=BASE + timedelta(seconds=1), response_received_monotonic=2.0,
    )
    assert field(missing, "lastPrice").classification is TickerMatchClassification.WS_FIELD_UNOBSERVED


def test_universe_differences_are_not_silently_dropped() -> None:
    monitor = ParityMonitor(ws_symbols=("BTCUSDT", "ETHUSDT"))
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    result = monitor.finish_ticker_window(
        rest_rows=[rest_ticker(), {"symbol": "XRPUSDT", "lastPrice": "1"}], server_time=BASE,
        response_received_at=BASE + timedelta(seconds=1), response_received_monotonic=2.0,
    )
    assert result.coverage.rest_only == ("XRPUSDT",)
    assert result.coverage.ws_only == ("ETHUSDT",)


def test_failed_rest_probe_can_abort_and_release_the_bounded_window() -> None:
    monitor = ParityMonitor(ws_symbols=("BTCUSDT",))
    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=1.0)
    monitor.observe_ticker(ticker("100"))
    monitor.abort_ticker_window()

    monitor.begin_ticker_window(request_started_at=BASE, request_started_monotonic=2.0)
    result = monitor.finish_ticker_window(
        rest_rows=[rest_ticker()], server_time=BASE,
        response_received_at=BASE + timedelta(seconds=1), response_received_monotonic=3.0,
    )
    assert result.overflowed is False
    assert result.high_water_states == 0


def closed_candle(*, confirmed: bool = True, close: str = "100") -> MarketCandle:
    return MarketCandle(
        symbol="BTCUSDT", open_time=BASE, close_time=BASE + timedelta(milliseconds=59_999), interval="1",
        open=Decimal(100), high=Decimal(110), low=Decimal(90), close=Decimal(close),
        volume=Decimal(7), turnover=Decimal(700), confirmed=confirmed,
        exchange_timestamp=BASE + timedelta(seconds=59), received_at=BASE + timedelta(minutes=1),
        connection_epoch=EPOCH,
    )


REST_KLINE = [[str(int(BASE.timestamp() * 1000)), "100.0", "110", "90.00", "100", "7.0", "700.00"]]


def test_closed_kline_exact_match_retry_and_true_mismatch() -> None:
    assert compare_closed_kline(closed_candle(), REST_KLINE).classification is KlineClassification.EXACT_MATCH
    assert compare_closed_kline(closed_candle(), [], final_attempt=False).classification is KlineClassification.REST_NOT_YET_VISIBLE
    assert compare_closed_kline(closed_candle(), [], final_attempt=True).classification is KlineClassification.REST_MISSING
    mismatch = compare_closed_kline(closed_candle(close="101"), REST_KLINE)
    assert mismatch.classification is KlineClassification.FIELD_MISMATCH
    assert mismatch.field_differences == ("close",)


def test_forming_ws_candle_cannot_be_final_parity_evidence() -> None:
    try:
        compare_closed_kline(closed_candle(confirmed=False), REST_KLINE)
    except ValueError as exc:
        assert "confirmed" in str(exc)
    else:
        raise AssertionError("forming candle was accepted")


def test_ws_missing_and_malformed_rest_kline_are_explicit() -> None:
    assert compare_closed_kline(None, REST_KLINE).classification is KlineClassification.WS_MISSING
    assert compare_closed_kline(closed_candle(), [[str(int(BASE.timestamp() * 1000)), "bad"]]).classification is KlineClassification.DATA_GAP
