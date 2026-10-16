from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.market.bybit_client import BybitClient, BybitResponseError
from app.infra.provider_errors import ProviderRateLimited, RateLimitPolicy, retry_after_seconds
from app.infra.request_scheduler import RequestPriority, RequestScheduler




class _StaticScheduler:
    async def schedule(self, func, *args, priority=None, operation=None, deadline_monotonic=None, **kwargs):
        return func(*args, **kwargs)


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

    with pytest.raises(ProviderRateLimited) as error:
        asyncio.run(client.fetch_instruments())
    assert error.value.provider_code == 10006


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


@pytest.mark.parametrize("status,code", [(200, 10006), (429, 0)])
def test_real_pybit_send_is_single_attempt_for_outcome(monkeypatch, status, code):
    import json
    import requests
    calls = []
    scheduler = RequestScheduler(2, 0, 0, 0)
    client = BybitClient(scheduler)
    def send(request, **kwargs):
        calls.append(request.url)
        response = requests.Response()
        response.status_code = status
        response.headers["Retry-After"] = "17"
        response._content = json.dumps({"retCode": code, "retMsg": "rate limit", "result": {"list": []}}).encode()
        return response
    monkeypatch.setattr(client._client.client, "send", send)
    with pytest.raises(ProviderRateLimited) as error:
        asyncio.run(client.fetch_klines("ARKUSDT", "1", priority=RequestPriority.OUTCOME_REPLAY))
    assert len(calls) == 1
    assert error.value.retry_after == 17
    assert scheduler.stats_snapshot()["fetch_klines:OUTCOME_REPLAY"]["attempts"] == 1


def test_real_pybit_rate_limit_then_success_reacquires_scheduler(monkeypatch):
    import json
    import requests
    scheduler = RequestScheduler(2, 0, 0, 0, rate_limit_policy=RateLimitPolicy(windows=((0, 0),)*4))
    client = BybitClient(scheduler)
    calls = []
    candles = [["1000", "1", "2", "1", "1", "10", "10"]]
    def send(request, **kwargs):
        calls.append(request.url)
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps({
            "retCode": 10006 if len(calls) < 3 else 0,
            "retMsg": "rate limit" if len(calls) < 3 else "OK",
            "result": {"list": candles},
        }).encode()
        return response
    monkeypatch.setattr(client._client.client, "send", send)
    assert asyncio.run(client.fetch_klines("ARKUSDT", "1")) == candles
    assert len(calls) == scheduler.stats_snapshot()["fetch_klines:LIVE_SCAN"]["attempts"] == 3


def test_retry_after_dates_reset_and_invalid_headers():
    now = datetime(2026, 10, 3, 14, 0, tzinfo=UTC)
    assert retry_after_seconds({"Retry-After": "Sat, 03 Oct 2026 14:02:00 GMT"}, now=now) == 120
    assert retry_after_seconds({"X-Bapi-Limit-Reset-Timestamp": str(int(now.timestamp()*1000)+45000)}, now=now) == 45
    assert retry_after_seconds({"Retry-After": "nan"}, now=now) == 0
