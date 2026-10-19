from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.config import AppConfig, CanonicalMarketDataProviderMode
from app.domain import MarketSnapshot
from app.infra.request_scheduler import RequestScheduler
from app.market.bybit_client import BybitClient
from app.market_data.hub import MarketDataHub
from app.market_data.provider import CanonicalMarketDataProvider
from app.runtime.market_runtime import LaneCMarketRuntime


class _HTTPClient:
    def __init__(self, symbols: tuple[str, ...], now: datetime) -> None:
        self.symbols = symbols
        self.now = now
        self.klines_calls: list[str] = []
        self.oi_calls: list[str] = []

    def get_kline(self, **params: object):
        symbol = str(params["symbol"])
        limit = int(params.get("limit", 240))
        self.klines_calls.append(symbol)
        latest = self.now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        rows = []
        for index in range(max(30, limit)):
            opened = latest - timedelta(minutes=max(30, limit) - index - 1)
            value = 100.0 + index * 0.1
            rows.append(
                [
                    str(int(opened.timestamp() * 1000)),
                    str(value),
                    str(value + 0.2),
                    str(value - 0.2),
                    str(value + 0.1),
                    "1000",
                    "100000",
                ]
            )
        return {
            "retCode": 0,
            "retMsg": "OK",
            "time": int(self.now.timestamp() * 1000),
            "result": {"list": list(reversed(rows))},
        }

    def get_open_interest(self, **params: object):
        symbol = str(params["symbol"])
        limit = int(params.get("limit", 5))
        self.oi_calls.append(symbol)
        return {
            "retCode": 0,
            "retMsg": "OK",
            "time": int(self.now.timestamp() * 1000),
            "result": {
                "list": [
                    {
                        "openInterest": str(100 + index),
                        "timestamp": str(int((self.now - timedelta(minutes=15 * index)).timestamp() * 1000)),
                    }
                    for index in range(limit)
                ]
            },
        }


class _Scanner:
    def __init__(self, client: BybitClient, config: AppConfig, *, symbols: tuple[str, ...], now: datetime) -> None:
        self.client = client
        self.config = config
        self.last_universe_telemetry = SimpleNamespace(eligible_symbols=symbols)
        self._symbols = symbols
        self._now = now
        self.fetch_symbol_frames_calls = 0

    async def fetch_market_snapshots(self):
        return [
            MarketSnapshot(
                symbol=symbol,
                last_price=100.0,
                price_24h_pct=1.0,
                turnover_24h=10_000_000.0,
                volume_24h=10_000_000.0,
                mark_price=100.0,
                open_interest=100.0,
                timestamp=self._now,
            )
            for symbol in self._symbols
        ]

    def shortlist(self, snapshots):
        return list(snapshots)

    async def fetch_symbol_frames(self, symbols):
        self.fetch_symbol_frames_calls += 1
        raise AssertionError("HUB runtime must not broad-prefetch REST frames")

    async def fetch_optional_derivatives(self, symbol: str, *, force_refresh: bool = False):
        observed = self._now - timedelta(seconds=1)
        return {
            "symbol": symbol,
            "open_interest": [
                {
                    "openInterest": "101",
                    "timestamp": str(int(observed.timestamp() * 1000)),
                },
                {
                    "openInterest": "100",
                    "timestamp": str(int((observed - timedelta(minutes=15)).timestamp() * 1000)),
                },
            ],
            "funding": [
                {
                    "fundingRate": "0.001",
                    "timestamp": str(int(observed.timestamp() * 1000)),
                }
            ],
            "derivatives_status": "OK",
            "derivatives_reasons": [],
            "data_quality_warnings": [],
        }

    async def fetch_optional_liquidity(self, symbol: str, price: float):
        return {}


class _Connection:
    def __init__(self, symbols: tuple[str, ...], now: datetime) -> None:
        self.symbols = symbols
        self.now = now
        self.messages: asyncio.Queue[str] = asyncio.Queue()

    async def send(self, raw: str) -> None:
        payload = json.loads(raw)
        if payload.get("op") == "ping":
            await self.messages.put(json.dumps({"op": "pong", "req_id": payload.get("req_id")}))
            return
        if payload.get("op") != "subscribe":
            return
        await self.messages.put(
            json.dumps({"op": "subscribe", "success": True, "req_id": payload.get("req_id")})
        )
        closed = self.now.replace(second=0, microsecond=0) - timedelta(minutes=1)
        for topic in payload.get("args", []):
            symbol = str(topic).rsplit(".", 1)[-1]
            if str(topic).startswith("tickers."):
                await self.messages.put(
                    json.dumps(
                        {
                            "topic": topic,
                            "type": "snapshot",
                            "ts": int(self.now.timestamp() * 1000),
                            "data": {
                                "symbol": symbol,
                                "lastPrice": "100",
                                "markPrice": "100",
                                "indexPrice": "100",
                                "fundingRate": "0.002",
                                "openInterest": "100",
                                "volume24h": "10000000",
                                "turnover24h": "10000000",
                                "price24hPcnt": "0.01",
                                "bid1Price": "99.9",
                                "bid1Size": "100",
                                "ask1Price": "100.1",
                                "ask1Size": "100",
                            },
                        }
                    )
                )
            elif str(topic).startswith("kline.1."):
                await self.messages.put(
                    json.dumps(
                        {
                            "topic": topic,
                            "ts": int(self.now.timestamp() * 1000),
                            "data": [
                                {
                                    "symbol": symbol,
                                    "interval": "1",
                                    "start": int(closed.timestamp() * 1000),
                                    "end": int((closed + timedelta(minutes=1)).timestamp() * 1000),
                                    "open": "100",
                                    "high": "100.2",
                                    "low": "99.8",
                                    "close": "100.1",
                                    "volume": "1000",
                                    "turnover": "100000",
                                    "confirm": True,
                                }
                            ],
                        }
                    )
                )

    async def recv(self) -> str:
        return await self.messages.get()

    async def close(self) -> None:
        return None


@pytest.mark.asyncio
async def test_hub_runtime_shares_ws_store_and_progresses_bounded_warmup(tmp_path) -> None:
    now = datetime.now(UTC)
    symbols = tuple(f"S{index:02d}USDT" for index in range(15))
    config = AppConfig(
        autoexecution="OFF",
        canonical_market_data_provider_mode=CanonicalMarketDataProviderMode.HUB_PREFERRED,
        market_data_ws_shadow_enabled=True,
        shortlist_size=len(symbols),
        min_24h_volume=1,
        exclude_btc_eth=False,
        derivatives_enabled=True,
        deep_scan_kline_limit=30,
        market_frame_warmup_max_requests=20,
        market_frame_snapshot_path=str(tmp_path / "market-store.json.gz"),
    )
    raw_http = _HTTPClient(symbols, now)
    scheduler = RequestScheduler(
        max_concurrency=config.max_request_concurrency,
        min_delay_ms=0,
        jitter_min_ms=0,
        jitter_max_ms=0,
    )
    client = BybitClient(scheduler=scheduler, timeout=1)
    client._client = raw_http
    scanner = _Scanner(client, config, symbols=symbols, now=now)

    async def connector(**kwargs: object) -> _Connection:
        return _Connection(symbols, now)

    hub = MarketDataHub(connector=connector, clock=lambda: now)
    provider = CanonicalMarketDataProvider(scanner=scanner, config=config, hub=hub)
    runtime = LaneCMarketRuntime(config=config, provider=provider, scanner=scanner)
    assert runtime.hub is hub
    assert provider.hub is hub

    await runtime.start()
    try:
        await runtime.scan()
        first_warm = runtime._warm_task
        assert first_warm is not None
        await first_warm
        first_calls = len(raw_http.klines_calls)
        assert first_calls == 10

        await runtime.scan()
        second_warm = runtime._warm_task
        assert second_warm is not None
        await second_warm
        assert len(raw_http.klines_calls) == 15

        for _ in range(100):
            await asyncio.sleep(0)
            if runtime.readiness(symbols)[0]:
                break
        assert runtime.readiness(symbols) == (True, "market_coverage_valid")
        frames = await runtime.frames(list(symbols))
        assert set(frames) == set(symbols)
        assert scanner.fetch_symbol_frames_calls == 0
        assert len(raw_http.klines_calls) == first_calls + 5

        await runtime.derivatives_cache.refresh_now(symbols)  # type: ignore[union-attr]
        derivatives = runtime.derivatives(symbols[0])
        assert derivatives is not None
        assert derivatives["derivatives_status"] == "OK"
        assert derivatives["funding"][0]["fundingRate"] == "0.001"
    finally:
        await runtime.stop()

