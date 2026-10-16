from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd

from app.config import AppConfig
from app.market_data.models import (
    ConnectionEpoch,
    MarketCandle,
    MarketTickerSnapshot,
)
from app.market_data.provider import CanonicalMarketDataProvider
from app.market_data.store import ContinuityStatus

_NOW = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
_RING = 5


class _Scanner:
    def __init__(self, client: object) -> None:
        self.client = client
        self.last_universe_telemetry = None
        self.derivatives_calls = 0

    async def fetch_symbol_frames(self, symbols):  # noqa: ANN001
        return {
            symbol: pd.DataFrame(
                {
                    "timestamp": pd.to_datetime(
                        [
                            (_NOW - timedelta(minutes=k)).isoformat()
                            for k in range(_RING, 0, -1)
                        ],
                        utc=True,
                    ),
                    "open": [1.0] * _RING,
                    "high": [1.0] * _RING,
                    "low": [1.0] * _RING,
                    "close": [1.0] * _RING,
                    "volume": [1.0] * _RING,
                    "turnover": [1.0] * _RING,
                }
            )
            for symbol in symbols
        }

    async def fetch_optional_derivatives(self, symbol):  # noqa: ANN001
        self.derivatives_calls += 1
        return {"symbol": symbol}

    async def fetch_optional_liquidity(self, symbol, price):  # noqa: ANN001, ANN002
        return {"symbol": symbol, "price": price}


class _Client:
    def __init__(self, rows=None) -> None:  # noqa: ANN001
        self.rows = rows or []
        self.calls = 0
        self.gate: asyncio.Event | None = None

    async def fetch_klines(self, symbol, interval, **kwargs):  # noqa: ANN001, ANN002, ANN003
        self.calls += 1
        if self.gate is not None:
            await self.gate.wait()
        return list(self.rows)

    async def fetch_open_interest(self, *args, **kwargs):  # noqa: ANN002, ANN003
        return []


def _candle(minute: int) -> MarketCandle:
    open_time = _NOW - timedelta(minutes=minute)
    return MarketCandle(
        symbol="TESTUSDT",
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        interval="1",
        open=Decimal("2"),
        high=Decimal("3"),
        low=Decimal("1"),
        close=Decimal("2.5"),
        volume=Decimal("100"),
        turnover=Decimal("250"),
        confirmed=True,
        exchange_timestamp=open_time,
        received_at=open_time + timedelta(seconds=3),
        connection_epoch=ConnectionEpoch(0, 1),
    )


def _ticker() -> MarketTickerSnapshot:
    return MarketTickerSnapshot(
        symbol="TESTUSDT",
        connection_epoch=ConnectionEpoch(0, 1),
        last_price=Decimal("2.5"),
        mark_price=Decimal("2.5"),
        index_price=Decimal("2.5"),
        funding_rate=Decimal("0.0001"),
        open_interest=Decimal("999"),
        volume_24h=Decimal("1"),
        turnover_24h=Decimal("1"),
        price_24h_pcnt=Decimal("0"),
        high_price_24h=Decimal("1"),
        low_price_24h=Decimal("1"),
        prev_price_24h=Decimal("1"),
        bid1_price=Decimal("2.5"),
        bid1_size=Decimal("1"),
        ask1_price=Decimal("2.5"),
        ask1_size=Decimal("1"),
        received_at=_NOW,
    )


def _provider(client: _Client, store_mode: str) -> CanonicalMarketDataProvider:
    config = AppConfig(
        canonical_market_data_provider_mode=(
            "HUB_SHADOW" if store_mode == "SHADOW" else "HUB_PREFERRED"
        ),
        market_frame_store_mode=store_mode,
        deep_scan_kline_limit=_RING,
    )
    provider = CanonicalMarketDataProvider(
        scanner=_Scanner(client),
        config=config,
        hub=None,
        continuity=None,
        clock=lambda: _NOW,
    )
    assert provider.frame_store is not None
    provider.frame_store.update_symbols(["TESTUSDT"])
    return provider


def _fill_store(provider: CanonicalMarketDataProvider) -> None:
    store = provider.frame_store
    for minute in range(1, _RING + 1):
        store.observe_closed_candle(_candle(minute))
    store.observe_ticker(_ticker())


def test_preferred_prefetch_returns_store_frames_without_rest() -> None:
    client = _Client()
    provider = _provider(client, "PREFERRED")
    _fill_store(provider)

    async def run() -> dict:
        return await provider.prefetch_historical_frames(["TESTUSDT"])

    frames = asyncio.run(run())
    assert set(frames) == {"TESTUSDT"}
    frozen = frames["TESTUSDT"]
    assert frozen.source == "FRAME_STORE"
    assert len(frozen.frame) >= _RING
    assert client.calls == 0  # store hit consumed no REST
    evidence = provider.evidence_snapshot()
    assert evidence["counters"]["frame_store_hit"] == 1


def test_preferred_skips_stale_symbol_and_records_incomplete() -> None:
    client = _Client()
    provider = _provider(client, "PREFERRED")
    provider.frame_store.observe_ticker(_ticker())

    async def run() -> dict:
        return await provider.prefetch_historical_frames(["TESTUSDT"])

    frames = asyncio.run(run())
    assert frames == {}
    evidence = provider.evidence_snapshot()
    assert evidence["counters"]["frame_store_fallback"] == 1
    assert evidence["counters"]["frame_store_incomplete"] == 1


def test_preferred_fallback_backfills_once_under_concurrency() -> None:
    rows = [
        [
            str(
                int((_NOW - timedelta(minutes=k)).timestamp() * 1000)
            ),
            "2",
            "3",
            "1",
            "2.5",
            "100",
            "250",
        ]
        for k in range(_RING, 0, -1)
    ]
    client = _Client(rows=rows)
    client.gate = asyncio.Event()
    provider = _provider(client, "PREFERRED")
    provider.frame_store.observe_ticker(_ticker())

    async def run() -> tuple[dict, dict]:
        first = asyncio.create_task(provider.prefetch_historical_frames(["TESTUSDT"]))
        await asyncio.sleep(0)  # let the first task register single-flight
        second = asyncio.create_task(provider.prefetch_historical_frames(["TESTUSDT"]))
        await asyncio.sleep(0.01)
        client.gate.set()
        return await first, await second

    frames_a, frames_b = asyncio.run(run())
    assert client.calls == 1  # coalesced single-flight REST fallback
    assert "TESTUSDT" in frames_a and "TESTUSDT" in frames_b
    assert frames_a["TESTUSDT"].source == "FRAME_STORE"


def test_shadow_prefetch_keeps_rest_authoritative_and_records_parity() -> None:
    client = _Client()
    provider = _provider(client, "SHADOW")
    _fill_store(provider)

    async def run() -> dict:
        return await provider.prefetch_historical_frames(["TESTUSDT"])

    frames = asyncio.run(run())
    assert frames["TESTUSDT"].source == "REST"  # REST remains authoritative
    assert client.calls == 0  # frames come from the scanner seam
    evidence = provider.evidence_snapshot()
    assert evidence["counters"].get("frame_store_parity_mismatch", 0) >= 1
    assert "frame_store_hit" not in evidence["counters"]


def test_preferred_capture_uses_store_derivatives_without_rest_enrichment() -> None:
    client = _Client()
    provider = _provider(client, "PREFERRED")
    _fill_store(provider)
    scanner = provider._scanner

    async def run():  # noqa: ANN202
        return await provider.capture_decision_snapshot(
            "TESTUSDT", include_liquidity=False
        )

    decision = asyncio.run(run())
    assert decision is not None
    assert decision.frame_1m.source == "FRAME_STORE"
    assert scanner.derivatives_calls == 0  # OI/funding came from the store frame
    assert decision.derivatives["funding"] == [{"fundingRate": "0.0001"}]


def test_preferred_capture_returns_none_when_frame_is_stale() -> None:
    client = _Client()
    provider = _provider(client, "PREFERRED")

    async def run():  # noqa: ANN202
        return await provider.capture_decision_snapshot("TESTUSDT")

    assert asyncio.run(run()) is None


def test_provider_rechecks_decision_minute_after_rest_crosses_boundary() -> None:
    before = _NOW - timedelta(seconds=1)
    after = _NOW + timedelta(seconds=1)
    clock = {"at": before}
    rest_start = _NOW - timedelta(minutes=2)
    rows = [[str(int(rest_start.timestamp() * 1000)), "2", "3", "1", "2.5", "100", "250"]]

    class CrossingClient(_Client):
        async def fetch_klines(self, symbol, interval, **kwargs):  # noqa: ANN001, ANN003
            result = await super().fetch_klines(symbol, interval, **kwargs)
            await asyncio.sleep(0)
            clock["at"] = after
            return result

    client = CrossingClient(rows=rows)
    provider = _provider(client, "PREFERRED")
    provider._clock = lambda: clock["at"]
    for minute in range(3, 8):
        provider.frame_store.observe_closed_candle(_candle(minute))
    provider.frame_store.observe_ticker(_ticker())

    frame = asyncio.run(provider.decision_market_frame("TESTUSDT"))
    assert client.calls == 1
    assert frame is not None
    assert not frame.actionable
    assert frame.stale_reason == "MISSING_DECISION_CANDLE"
    old_frame = provider.frame_store.peek_snapshot("TESTUSDT", before)
    assert old_frame is not None and not old_frame.actionable
    assert old_frame.stale_reason == "MISSING_DECISION_CANDLE"
    assert old_frame.last_closed_at_utc == rest_start + timedelta(minutes=1)
    assert provider.frame_store.counters["peeks_missing_decision_candle"] >= 1


def test_off_mode_has_no_store_and_prefetch_hits_rest() -> None:
    client = _Client()
    config = AppConfig(market_frame_store_mode="OFF")
    provider = CanonicalMarketDataProvider(
        scanner=_Scanner(client), config=config, clock=lambda: _NOW
    )
    assert provider.frame_store is None

    async def run() -> dict:
        return await provider.prefetch_historical_frames(["TESTUSDT"])

    frames = asyncio.run(run())
    assert frames["TESTUSDT"].source == "REST"


def test_store_continuity_status_visible_for_warming_symbol() -> None:
    client = _Client()
    provider = _provider(client, "PREFERRED")
    frame = provider.frame_store.peek_snapshot("TESTUSDT", _NOW)
    assert frame is not None
    assert frame.continuity_status is ContinuityStatus.WARMING
