from __future__ import annotations

import asyncio
import inspect
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pandas as pd
import pytest

from app.config import AppConfig
from app.domain import MarketSnapshot
from app.market_data.continuity import GapState
from app.market_data.models import ConnectionEpoch, HealthStatus, MarketCandle, MarketDataHealth, MarketTickerSnapshot, SubscriptionState
from app.market_data.provider import (
    CanonicalMarketDataProvider,
    CanonicalMarketDataProviderMode,
    FrozenCandleFrame,
)
from app.main import ShortSignalBot
from app.storage.db import Database
from app.storage.repository import BotRepository


class _Scanner:
    def __init__(self) -> None:
        self.client = _Client()
        self.last_universe_telemetry = None
        self.frames = {"AAAUSDT": pd.DataFrame({"close": [1.0, 2.0]})}
        self.derivatives_calls = 0
        self.liquidity_calls = 0

    async def fetch_market_snapshots(self):
        return [
            MarketSnapshot(
                symbol="AAAUSDT", last_price=10.0, price_24h_pct=2.0,
                turnover_24h=100.0, volume_24h=20.0, mark_price=10.1,
                open_interest=3.0, timestamp=datetime.now(UTC),
            )
        ]

    async def fetch_symbol_frames(self, symbols):
        return {symbol: self.frames[symbol] for symbol in symbols if symbol in self.frames}

    def shortlist(self, snapshots):
        return snapshots

    async def fetch_optional_derivatives(self, symbol):
        self.derivatives_calls += 1
        return {"symbol": symbol}

    async def fetch_optional_liquidity(self, symbol, price):
        self.liquidity_calls += 1
        return {"symbol": symbol, "price": price}


class _Client:
    async def fetch_klines(self, *args, **kwargs):
        return []


class _Hub:
    def __init__(self, snapshot: MarketTickerSnapshot | None, *, healthy: bool = True) -> None:
        self.snapshot = snapshot
        self.healthy = healthy
        self.started = False
        self.stopped = False
        self.universe = ()
        self.universe_update_calls = 0
        self.acknowledged = True
        self.health_state: HealthStatus | None = None
        self.current_generation = True
        self.closed_candle = None

    def get_snapshot(self, symbol):
        return self.snapshot if symbol == "AAAUSDT" else None

    def get_health(self):
        return MarketDataHealth(
            state=self.health_state or (HealthStatus.HEALTHY if self.healthy else HealthStatus.STALE),
            updated_at=datetime.now(UTC),
        )

    def get_subscription_state(self):
        topics = frozenset({"tickers.AAAUSDT"})
        acknowledged = topics if self.acknowledged else frozenset()
        return SubscriptionState(topics, acknowledged, frozenset(), frozenset())

    def is_current_ticker_snapshot(self, snapshot):
        return self.current_generation

    def is_current_connection_epoch(self, epoch):
        return self.current_generation

    def get_last_closed_candle(self, symbol):
        return self.closed_candle

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    def update_universe(self, symbols):
        self.universe_update_calls += 1
        self.universe = tuple(symbols)


def _hub_snapshot(*, received_at: datetime | None = None) -> MarketTickerSnapshot:
    now = received_at or datetime.now(UTC)
    return MarketTickerSnapshot(
        symbol="AAAUSDT", last_price=Decimal("11"), price_24h_pcnt=Decimal("0.03"),
        turnover_24h=Decimal("101"), volume_24h=Decimal("21"),
        mark_price=Decimal("11.1"), open_interest=Decimal("4"),
        received_at=now, exchange_timestamp=now,
        connection_epoch=ConnectionEpoch(shard_id=0, generation=1),
    )


class _Continuity:
    def __init__(self, state: GapState = GapState.CONTIGUOUS) -> None:
        self.state = state

    def get_symbol_state(self, symbol: str) -> GapState:
        return self.state


def test_config_provider_modes_default_and_legacy_mapping() -> None:
    assert type(AppConfig().canonical_market_data_provider_mode) is CanonicalMarketDataProviderMode
    assert AppConfig().canonical_market_data_provider_mode == CanonicalMarketDataProviderMode.REST_ONLY
    assert AppConfig(market_data_ws_shadow_enabled=True).canonical_market_data_provider_mode == CanonicalMarketDataProviderMode.HUB_SHADOW
    assert AppConfig(
        market_data_ws_shadow_enabled=True,
        canonical_market_data_provider_mode="HUB_PREFERRED",
    ).canonical_market_data_provider_mode == CanonicalMarketDataProviderMode.HUB_PREFERRED


def test_hub_preferred_replaces_only_a_complete_fresh_acknowledged_group() -> None:
    async def run() -> None:
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=_Hub(_hub_snapshot()), continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        )
        snapshot = await provider.build_scan_snapshot()
        selected = snapshot.snapshots[0]
        assert selected.last_price == 11.0
        assert selected.price_24h_pct == 3.0
        assert snapshot.resolved_tickers["AAAUSDT"].provenance.source == "HUB"

    asyncio.run(run())


@pytest.mark.parametrize("case", ["incomplete", "stale", "future"])
def test_hub_preferred_falls_back_for_incomplete_stale_or_future_group_without_mixing_fields(
    case: str,
) -> None:
    async def run() -> None:
        now = datetime.now(UTC)
        hub_snapshot = _hub_snapshot(
            received_at=now - timedelta(minutes=10) if case == "stale" else now
        )
        if case == "incomplete":
            hub_snapshot = replace(hub_snapshot, mark_price=None)
        if case == "future":
            hub_snapshot = _hub_snapshot(received_at=now + timedelta(minutes=1))
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=_Hub(hub_snapshot), continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        )
        snapshot = await provider.build_scan_snapshot()
        selected = snapshot.snapshots[0]
        assert selected.last_price == 10.0
        assert selected.mark_price == 10.1
        assert snapshot.resolved_tickers["AAAUSDT"].provenance.source == "REST_FALLBACK"
        assert snapshot.resolved_tickers["AAAUSDT"].provenance.reason == "REST_FALLBACK"

    asyncio.run(run())


@pytest.mark.parametrize(
    ("health_state", "acknowledged", "expected_reason"),
    [
        (HealthStatus.RECONNECTING, True, "HUB_NOT_HEALTHY"),
        (HealthStatus.HEALTHY, False, "HUB_UNACKNOWLEDGED"),
    ],
)
def test_hub_preferred_falls_back_until_current_generation_is_healthy_and_acknowledged(
    health_state: HealthStatus, acknowledged: bool, expected_reason: str
) -> None:
    async def run() -> None:
        hub = _Hub(_hub_snapshot())
        hub.health_state = health_state
        hub.acknowledged = acknowledged
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=hub, continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        )
        snapshot = await provider.build_scan_snapshot()
        group = snapshot.resolved_tickers["AAAUSDT"]
        assert group.provenance.source == "REST_FALLBACK"
        assert group.provenance.reason == "REST_FALLBACK"
        assert provider.evidence_snapshot()["details"][-1]["reason"] == expected_reason

    asyncio.run(run())


def test_frozen_candle_frame_defensively_copies_and_rejects_future_availability() -> None:
    captured = datetime.now(UTC)
    source = pd.DataFrame({"close": [1.0]})
    frozen = FrozenCandleFrame(
        symbol="AAAUSDT", frame=source, source="REST", captured_at_utc=captured,
        max_input_availability_time_utc=captured,
    )
    source.loc[0, "close"] = 99.0
    assert frozen.frame.loc[0, "close"] == 1.0
    exposed = frozen.frame
    exposed.loc[0, "close"] = 42.0
    assert frozen.frame.loc[0, "close"] == 1.0
    with pytest.raises(ValueError, match="availability"):
        FrozenCandleFrame(
            symbol="AAAUSDT", frame=source, source="REST", captured_at_utc=captured,
            max_input_availability_time_utc=captured + timedelta(seconds=1),
        )


def test_decision_snapshot_is_defensive_and_captured_after_all_async_reads() -> None:
    class Clock:
        def __init__(self, values):
            self.values = iter(values)

        def __call__(self):
            return next(self.values)

    async def run() -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        scanner = _Scanner()
        clock = Clock([start, start + timedelta(seconds=1), start + timedelta(seconds=2)])
        provider = CanonicalMarketDataProvider(scanner=scanner, config=AppConfig(), clock=clock)
        decision = await provider.capture_decision_snapshot("AAAUSDT")
        assert decision is not None
        # Frame collection consumes the first timestamp; decision capture occurs
        # only after the subsequent derivative and liquidity awaits complete.
        assert decision.captured_at_utc == start + timedelta(seconds=1)
        assert decision.max_input_availability_time_utc <= decision.captured_at_utc
        mutable = decision.derivatives
        mutable["nested"] = ["changed"]
        assert "nested" not in decision.derivatives

    asyncio.run(run())


def test_decision_snapshot_keeps_absent_liquidity_distinct_from_an_empty_result() -> None:
    async def run() -> None:
        provider = CanonicalMarketDataProvider(scanner=_Scanner(), config=AppConfig())
        decision = await provider.capture_decision_snapshot(
            "AAAUSDT", include_liquidity=False
        )
        assert decision is not None
        assert decision.liquidity is None

    asyncio.run(run())


def test_base_snapshot_defers_orderbook_until_enriched_candidate_snapshot() -> None:
    async def run() -> None:
        scanner = _Scanner()
        provider = CanonicalMarketDataProvider(scanner=scanner, config=AppConfig())
        base = await provider.capture_decision_snapshot(
            "AAAUSDT", include_liquidity=False
        )
        assert base is not None
        assert scanner.liquidity_calls == 0
        enriched = await provider.capture_decision_snapshot(
            "AAAUSDT",
            frame_1m=base.frame_1m.frame,
            derivatives=base.derivatives,
        )
        assert enriched is not None
        assert scanner.derivatives_calls == 1
        assert scanner.liquidity_calls == 1

    asyncio.run(run())


def test_runtime_candidate_liquidity_path_enriches_one_base_snapshot_once() -> None:
    async def run() -> None:
        scanner = _Scanner()
        provider = CanonicalMarketDataProvider(scanner=scanner, config=AppConfig())
        base = await provider.capture_decision_snapshot(
            "AAAUSDT", include_liquidity=False
        )
        assert base is not None
        bot = object.__new__(ShortSignalBot)
        bot._market_data_provider = provider
        liquidity = await bot._resolved_liquidity("AAAUSDT", 2.0, base, None)
        assert liquidity["symbol"] == "AAAUSDT"
        assert scanner.liquidity_calls == 1
        assert scanner.derivatives_calls == 1

    asyncio.run(run())


def test_decision_snapshot_carries_defensive_selected_ticker_provenance() -> None:
    async def run() -> None:
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=_Hub(_hub_snapshot()), continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        )
        await provider.build_scan_snapshot()
        decision = await provider.capture_decision_snapshot("AAAUSDT")
        assert decision is not None
        assert decision.ticker_group is not None
        assert decision.ticker_group.provenance.source == "HUB"
        observed = decision.ticker_group.snapshot
        observed.last_price = 999.0
        assert decision.ticker_group.snapshot.last_price == 11.0

    asyncio.run(run())


def test_unchanged_universe_does_not_reset_hub_acknowledgements() -> None:
    hub = _Hub(_hub_snapshot())
    provider = CanonicalMarketDataProvider(
        scanner=_Scanner(), hub=hub, continuity=_Continuity(),
        config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
    )

    provider.update_universe(("AAAUSDT", "BBBUSDT"))
    provider.update_universe(("BBBUSDT", "AAAUSDT"))

    assert hub.universe_update_calls == 1
    assert hub.universe == ("AAAUSDT", "BBBUSDT")


@pytest.mark.parametrize(
    ("current_generation", "continuity_state", "expected_reason"),
    [
        (False, GapState.CONTIGUOUS, "HUB_OLD_GENERATION"),
        (True, GapState.UNINITIALIZED, "HUB_UNINITIALIZED"),
        (True, GapState.GAP_DETECTED, "HUB_GAP_DETECTED"),
    ],
)
def test_hub_preferred_rejects_old_generation_and_continuity_failures(
    current_generation: bool, continuity_state: GapState, expected_reason: str
) -> None:
    async def run() -> None:
        hub = _Hub(_hub_snapshot())
        hub.current_generation = current_generation
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=hub, continuity=_Continuity(continuity_state),
            config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        )
        snapshot = await provider.build_scan_snapshot()
        assert snapshot.resolved_tickers["AAAUSDT"].provenance.source == "REST_FALLBACK"
        assert provider.evidence_snapshot()["details"][-1]["reason"] == expected_reason

    asyncio.run(run())


def test_hub_shadow_records_field_comparison_and_closed_1m_diagnostics() -> None:
    async def run() -> None:
        hub = _Hub(_hub_snapshot())
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=hub, continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_SHADOW"),
        )
        await provider.build_scan_snapshot()
        evidence = provider.evidence_snapshot()
        assert evidence["counters"]["hub_shadow_mismatch"] == 1
        assert evidence["counters"]["hub_closed_1m_shadow_unavailable"] == 1

    asyncio.run(run())


def test_hub_shadow_rejects_closed_1m_from_an_old_generation() -> None:
    async def run() -> None:
        now = datetime.now(UTC)
        hub = _Hub(_hub_snapshot())
        hub.closed_candle = MarketCandle(
            symbol="AAAUSDT", open_time=now - timedelta(minutes=1), close_time=now,
            interval="1", open=Decimal("1"), high=Decimal("1"), low=Decimal("1"),
            close=Decimal("1"), volume=Decimal("1"), turnover=Decimal("1"),
            confirmed=True, exchange_timestamp=now, received_at=now,
            connection_epoch=ConnectionEpoch(0, 1),
        )
        hub.current_generation = False
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=hub, continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_SHADOW"),
        )
        await provider.build_scan_snapshot()
        detail = provider.evidence_snapshot()["details"][-1]
        assert detail["event"] == "hub_closed_1m_shadow_unavailable"
        assert detail["reason"] == "CLOSED_1M_OLD_GENERATION"

    asyncio.run(run())


@pytest.mark.parametrize(
    ("rest_close", "expected"),
    [("1", "EXACT"), ("2", "SOURCE_SEMANTIC_MISMATCH")],
)
def test_hub_shadow_compares_confirmed_1m_identity_and_values_against_rest(
    rest_close: str, expected: str
) -> None:
    async def run() -> None:
        now = datetime.now(UTC)
        open_time = now - timedelta(minutes=1)
        hub = _Hub(_hub_snapshot())
        hub.closed_candle = MarketCandle(
            symbol="AAAUSDT", open_time=open_time, close_time=now,
            interval="1", open=Decimal("1"), high=Decimal("1.5"),
            low=Decimal("0.5"), close=Decimal("1"), volume=Decimal("10"),
            turnover=Decimal("10"), confirmed=True, exchange_timestamp=now,
            received_at=now, connection_epoch=ConnectionEpoch(0, 1),
        )
        frame = pd.DataFrame(
            {
                "start_ms": [int(open_time.timestamp() * 1000)],
                "open": [1.0], "high": [1.5], "low": [0.5],
                "close": [float(rest_close)], "volume": [10.0],
                "turnover": [10.0], "timestamp": [open_time],
            }
        )
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=hub, continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_SHADOW"),
        )
        decision = await provider.capture_decision_snapshot(
            "AAAUSDT", frame_1m=frame, include_liquidity=False
        )
        assert decision is not None
        detail = provider.evidence_snapshot()["details"][-1]
        assert detail["event"] == "hub_closed_1m_shadow_parity"
        assert detail["classification"] == expected
        assert detail["open_time"] == open_time

    asyncio.run(run())


def test_runtime_uses_provider_as_the_only_market_data_facade(tmp_path) -> None:
    class Notifier:
        async def start(self):
            return None

        async def close(self):
            return None

    database = Database(f"sqlite:///{tmp_path / 'provider.sqlite'}")
    database.create_all()
    bot = ShortSignalBot(
        config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        repository=BotRepository(database), scanner=_Scanner(), notifier=Notifier(),
        market_data_hub=_Hub(_hub_snapshot()),
    )
    assert isinstance(bot._market_data_provider, CanonicalMarketDataProvider)
    assert bot._scanner is bot._market_data_provider
    assert bot._shadow_outcome_scheduler._client is bot._market_data_provider


def test_runtime_accepts_injected_provider_and_attaches_continuity_to_hub(tmp_path) -> None:
    class Notifier:
        async def start(self):
            return None

        async def close(self):
            return None

    database = Database(f"sqlite:///{tmp_path / 'injected-provider.sqlite'}")
    database.create_all()
    hub = _Hub(_hub_snapshot())
    provider = CanonicalMarketDataProvider(
        scanner=_Scanner(),
        hub=hub,
        config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
    )
    bot = ShortSignalBot(
        config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        repository=BotRepository(database),
        notifier=Notifier(),
        market_data_hub=hub,
        market_data_provider=provider,
    )
    assert bot._market_data_provider is provider
    assert provider.continuity is bot._market_data_gap_tracker
    assert bot._market_data_hub is hub


def test_enriched_market_snapshot_preserves_frozen_frame_derivatives_and_provenance() -> None:
    async def run() -> None:
        provider = CanonicalMarketDataProvider(
            scanner=_Scanner(), hub=_Hub(_hub_snapshot()), continuity=_Continuity(),
            config=AppConfig(canonical_market_data_provider_mode="HUB_PREFERRED"),
        )
        await provider.build_scan_snapshot()
        base = await provider.capture_decision_snapshot(
            "AAAUSDT", include_liquidity=False
        )
        assert base is not None
        bot = object.__new__(ShortSignalBot)
        bot._market_data_provider = provider
        enriched = await bot._enrich_decision_market_snapshot(
            "AAAUSDT", base, 2.0
        )
        assert enriched.frame_1m.frame.equals(base.frame_1m.frame)
        assert enriched.derivatives == base.derivatives
        assert enriched.ticker_group is not None
        assert enriched.ticker_group.provenance == base.ticker_group.provenance
        assert enriched.liquidity is not None

    asyncio.run(run())


def test_runtime_calls_explicit_provider_snapshot_apis_at_scan_boundaries() -> None:
    assert "build_scan_snapshot" in inspect.getsource(ShortSignalBot.run_cycle)
    assert "prefetch_historical_frames" in inspect.getsource(
        ShortSignalBot._run_fast_monitor
    )
    assert "capture_decision_snapshot" in inspect.getsource(
        ShortSignalBot._evaluate_and_send_climax
    )
