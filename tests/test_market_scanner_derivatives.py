from __future__ import annotations

import asyncio

from app.config import AppConfig
from app.market.scanner import MarketScanner


class _DerivativesClient:
    def __init__(self, *, oi_result=None, funding_result=None, oi_exc: Exception | None = None, funding_exc: Exception | None = None) -> None:
        self.oi_result = oi_result if oi_result is not None else []
        self.funding_result = funding_result if funding_result is not None else []
        self.oi_exc = oi_exc
        self.funding_exc = funding_exc
        self.oi_calls = 0
        self.funding_calls = 0

    async def fetch_open_interest(self, _symbol: str):
        self.oi_calls += 1
        if self.oi_exc is not None:
            raise self.oi_exc
        return self.oi_result

    async def fetch_funding(self, _symbol: str):
        self.funding_calls += 1
        if self.funding_exc is not None:
            raise self.funding_exc
        return self.funding_result


def test_market_scanner_derivatives_disabled_preserves_old_behavior() -> None:
    async def _run() -> dict[str, object]:
        scanner = MarketScanner(client=_DerivativesClient(), config=AppConfig(derivatives_enabled=False))
        return await scanner.fetch_optional_derivatives("TESTUSDT")

    assert asyncio.run(_run()) == {}


def test_market_scanner_fetches_and_caches_derivatives_context() -> None:
    async def _run() -> tuple[dict[str, object], dict[str, object], _DerivativesClient]:
        client = _DerivativesClient(
            oi_result=[
                {"openInterest": "1200"},
                {"openInterest": "1000"},
                {"openInterest": "950"},
                {"openInterest": "900"},
                {"openInterest": "800"},
            ],
            funding_result=[{"fundingRate": "0.0005"}],
        )
        scanner = MarketScanner(client=client, config=AppConfig(derivatives_enabled=True))
        first = await scanner.fetch_optional_derivatives("TESTUSDT")
        second = await scanner.fetch_optional_derivatives("TESTUSDT")
        return first, second, client

    first, second, client = asyncio.run(_run())

    assert first["derivatives_status"] == "OK"
    assert first["data_quality_warnings"] == []
    assert second["derivatives_status"] == "OK"
    assert client.oi_calls == 1
    assert client.funding_calls == 1


def test_market_scanner_classifies_rate_limit_without_crashing_cycle() -> None:
    async def _run() -> dict[str, object]:
        client = _DerivativesClient(oi_exc=RuntimeError("ErrCode: 10006 Too many visits"))
        scanner = MarketScanner(client=client, config=AppConfig(derivatives_enabled=True))
        return await scanner.fetch_optional_derivatives("TESTUSDT")

    result = asyncio.run(_run())

    assert result["derivatives_status"] == "RATE_LIMITED"
    assert "bybit_rate_limit" in result["derivatives_reasons"]
    assert "derivatives_missing" in result["data_quality_warnings"]


def test_market_scanner_classifies_api_error_without_crashing_cycle() -> None:
    async def _run() -> dict[str, object]:
        client = _DerivativesClient(funding_exc=RuntimeError("temporary gateway error"))
        scanner = MarketScanner(client=client, config=AppConfig(derivatives_enabled=True))
        return await scanner.fetch_optional_derivatives("TESTUSDT")

    result = asyncio.run(_run())

    assert result["derivatives_status"] == "API_ERROR"
    assert "derivatives_api_error" in result["derivatives_reasons"]
