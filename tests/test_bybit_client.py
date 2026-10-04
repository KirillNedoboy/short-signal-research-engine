from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.market.bybit_client import BybitClient, BybitResponseError


class _FakeHTTP:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


def test_bybit_client_passes_timeout(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _build_http(**kwargs):
        captured.update(kwargs)
        return _FakeHTTP(**kwargs)

    monkeypatch.setattr("app.market.bybit_client.HTTP", _build_http)

    BybitClient(scheduler=object(), timeout=27)

    assert captured["timeout"] == 27
    assert captured["testnet"] is False
    assert captured["max_retries"] == 1
    assert captured["retry_codes"] == {-1}


class _StaticScheduler:
    async def schedule(self, operation, **kwargs):
        return operation(**kwargs)


class _ResponseHTTP:
    def __init__(self, responses: dict[str, list[object]]) -> None:
        self._responses = responses

    def _next(self, operation: str, **_kwargs):
        return self._responses[operation].pop(0)

    def get_instruments_info(self, **kwargs):
        return self._next("fetch_instruments", **kwargs)

    def get_tickers(self, **kwargs):
        return self._next("fetch_tickers", **kwargs)

    def get_kline(self, **kwargs):
        return self._next("fetch_klines", **kwargs)

    def get_open_interest(self, **kwargs):
        return self._next("fetch_open_interest", **kwargs)

    def get_funding_rate_history(self, **kwargs):
        return self._next("fetch_funding", **kwargs)

    def get_orderbook(self, **kwargs):
        return self._next("fetch_orderbook", **kwargs)


def _client(responses: dict[str, list[object]]) -> BybitClient:
    client = BybitClient.__new__(BybitClient)
    client._scheduler = _StaticScheduler()
    client._client = _ResponseHTTP(responses)
    return client


def test_fetch_tickers_accepts_success_envelope() -> None:
    client = _client({"fetch_tickers": [{"retCode": 0, "result": {"list": [{"symbol": "TESTUSDT"}]}}]})

    assert asyncio.run(client.fetch_tickers()) == [{"symbol": "TESTUSDT"}]


def test_fetch_tickers_accepts_empty_success_list() -> None:
    client = _client({"fetch_tickers": [{"retCode": 0, "result": {"list": []}}]})

    assert asyncio.run(client.fetch_tickers()) == []


def test_ticker_metadata_retains_server_time_without_changing_legacy_method() -> None:
    payload = {
        "retCode": 0,
        "time": 1_788_609_600_123,
        "result": {"list": [{"symbol": "TESTUSDT"}]},
    }
    client = _client({"fetch_tickers": [payload, payload.copy()]})

    response = asyncio.run(client.fetch_tickers_with_metadata())

    assert response.data == [{"symbol": "TESTUSDT"}]
    assert response.server_time == datetime.fromtimestamp(
        1_788_609_600.123, tz=UTC
    )
    assert asyncio.run(client.fetch_tickers()) == [{"symbol": "TESTUSDT"}]


def test_kline_metadata_uses_existing_scheduler_and_range_parameters() -> None:
    class RecordingHTTP(_ResponseHTTP):
        def __init__(self) -> None:
            super().__init__({})
            self.kwargs = None

        def get_kline(self, **kwargs):
            self.kwargs = kwargs
            return {"retCode": 0, "time": 1_788_609_600_000, "result": {"list": [["1"]]}}

    client = BybitClient.__new__(BybitClient)
    client._scheduler = _StaticScheduler()
    client._client = RecordingHTTP()

    response = asyncio.run(
        client.fetch_klines_with_metadata(
            "BTCUSDT", "1", limit=15, start_ms=1000, end_ms=2000
        )
    )

    assert response.data == [["1"]]
    assert client._client.kwargs == {
        "category": "linear",
        "symbol": "BTCUSDT",
        "interval": "1",
        "limit": 15,
        "start": 1000,
        "end": 2000,
    }


def test_nonzero_bybit_response_code_raises_typed_error() -> None:
    client = _client({"fetch_tickers": [{"retCode": 10001, "retMsg": "bad request", "result": {"list": []}}]})

    with pytest.raises(BybitResponseError, match="fetch_tickers") as exc_info:
        asyncio.run(client.fetch_tickers())

    assert exc_info.value.ret_code == 10001
    assert exc_info.value.ret_msg == "bad request"


def test_missing_result_raises_typed_error() -> None:
    client = _client({"fetch_klines": [{"retCode": 0}]})

    with pytest.raises(BybitResponseError, match="fetch_klines"):
        asyncio.run(client.fetch_klines("TESTUSDT", "1"))


def test_malformed_list_result_raises_typed_error() -> None:
    client = _client({"fetch_funding": [{"retCode": 0, "result": {"list": {}}}]})

    with pytest.raises(BybitResponseError, match="fetch_funding"):
        asyncio.run(client.fetch_funding("TESTUSDT"))


def test_instrument_pagination_validates_second_page() -> None:
    client = _client(
        {
            "fetch_instruments": [
                {"retCode": 0, "result": {"list": [{"symbol": "FIRST"}], "nextPageCursor": "next"}},
                {"retCode": 10006, "retMsg": "rate limited", "result": {"list": []}},
            ]
        }
    )

    with pytest.raises(BybitResponseError, match="fetch_instruments"):
        asyncio.run(client.fetch_instruments())


def test_orderbook_requires_mapping_result() -> None:
    client = _client({"fetch_orderbook": [{"retCode": 0, "result": []}]})

    with pytest.raises(BybitResponseError, match="fetch_orderbook"):
        asyncio.run(client.fetch_orderbook("TESTUSDT"))


def test_bybit_response_error_does_not_include_full_payload() -> None:
    secret_marker = "sensitive-payload-marker"
    client = _client(
        {
            "fetch_open_interest": [
                {"retCode": 10001, "retMsg": "bad request", "result": {"list": [], "token": secret_marker}}
            ]
        }
    )

    with pytest.raises(BybitResponseError) as exc_info:
        asyncio.run(client.fetch_open_interest("TESTUSDT"))

    assert secret_marker not in str(exc_info.value)
