"""End-to-end event-driven window proof on a temporary SQLite database.

Feeds one closed-minute update for 100 deterministic shortlist symbols plus
one persisted active P0 symbol (outside the shortlist), including one stale
frame and one gap repaired by REST fallback, then asserts exactly-once
terminal accounting, P0 precedence, stale-signal suppression, and the single
signal/provenance/outbox transaction.  No network and no Telegram transport.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select

from app.config import AppConfig
from app.domain import EventStatus, MarketSnapshot
from app.main import ShortSignalBot, _actionable_canonical_symbols
from app.market.coverage import ScanUniverseTelemetry
from app.market_data.continuity import GapTracker
from app.market_data.models import (
    ConnectionEpoch,
    HealthStatus,
    MarketCandle,
    MarketDataHealth,
    MarketTickerSnapshot,
    SubscriptionState,
)
from app.market_data.provider import CanonicalMarketDataProvider
from app.runtime.scan_coordinator import ScanTerminalStatus
from app.storage.db import Database
from app.storage.models import (
    MarketScanCycleModel,
    MarketScanSymbolResultModel,
    SignalModel,
    SignalProvenanceModel,
    TelegramDeliveryOutboxModel,
)
from app.storage.repository import BotRepository

_NOW = datetime.now(UTC)
_SHORTLIST = tuple(f"SYM{index:03d}USDT" for index in range(100))
_STALE = "SYM098USDT"
_GAP = "SYM099USDT"
_ACTIVE = "ACTUSDT"


def test_event_window_filter_keeps_only_actionable_canonical_frames() -> None:
    class _Frame:
        def __init__(self, actionable: bool) -> None:
            self.actionable = actionable

    class _Store:
        def peek_snapshot(self, symbol, decision_at):  # noqa: ANN001
            return _Frame(symbol == "READYUSDT")

    assert _actionable_canonical_symbols(
        _Store(),
        ("STALEUSDT", "READYUSDT", "READYUSDT", "MISSINGUSDT"),
        _NOW,
    ) == ("READYUSDT",)


def test_event_runtime_reports_warming_up_partial_coverage(
    tmp_path, make_features, make_event_state  # noqa: ANN001
) -> None:
    async def run() -> None:
        bot, _repository, _database, provider, _client = _build(
            tmp_path, make_features, make_event_state
        )
        store = provider.frame_store
        assert store is not None
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_warmup(*_args, **_kwargs):  # noqa: ANN002, ANN003
            started.set()
            await release.wait()
            return {"seeded": 0, "failed": 0, "skipped": 0, "requests": 0, "timed_out": 0}

        store.warm_symbols = blocked_warmup
        await bot._prepare_event_driven_runtime()
        await asyncio.wait_for(started.wait(), timeout=1.0)
        status = bot.warmup_status()
        assert status["state"] == "WARMING_UP"
        assert status["total"] == 101
        assert status["ready"] == 0
        assert status["pending"] == 101

        release.set()
        warm_task = bot._frame_store_warm_task
        assert warm_task is not None
        await warm_task
        status = bot.warmup_status()
        assert status["state"] == "WARMING_UP"
        assert status["ready"] == 0
        assert status["pending"] == 0
        assert status["failed"] == 101
        for consumer in bot._scan_consumers:
            consumer.cancel()
        await asyncio.gather(*bot._scan_consumers, return_exceptions=True)

    asyncio.run(run())


class _Client:
    """Deterministic REST seam: 300-candle rings, OI history, funding."""

    def __init__(self) -> None:
        self.fallback_calls: list[str] = []
        self.orderbook_calls: list[str] = []

    def _rows(self, count: int, *, skip_minute: int | None = None):
        rows = []
        for minute in range(count, 0, -1):
            if minute == skip_minute:
                continue
            start = _NOW - timedelta(minutes=minute)
            rows.append(
                [
                    str(int(start.timestamp() * 1000)),
                    "100",
                    "100",
                    "100",
                    "100",
                    "1000",
                    "100000",
                ]
            )
        return rows

    async def fetch_klines(
        self,
        symbol,  # noqa: ANN001
        interval,  # noqa: ANN001
        *,
        limit=240,  # noqa: ANN001
        start_ms=None,  # noqa: ANN001
        **kwargs,  # noqa: ANN003
    ):
        symbol = str(symbol).upper()
        if start_ms is None and symbol == _GAP:
            # Warmup supplies a 299-candle ring: one closed minute is missing.
            return self._rows(limit, skip_minute=140)
        if start_ms is not None:
            self.fallback_calls.append(symbol)
        return self._rows(limit)

    async def fetch_open_interest(
        self, symbol, interval="15min", limit=5, **kwargs  # noqa: ANN001, ANN002
    ):
        return [
            {"openInterest": str(1000 + index), "timestamp": str(index)}
            for index in range(limit)
        ]

    async def fetch_funding(self, symbol, limit=1, **kwargs):  # noqa: ANN001, ANN002
        return [{"fundingRate": "0.0001"}]

    async def fetch_orderbook(self, symbol, limit=50, **kwargs):  # noqa: ANN001, ANN002
        self.orderbook_calls.append(str(symbol).upper())
        return {"list": []}


class _TelemetryScanner:
    def __init__(self, client: _Client) -> None:
        self.client = client
        self.derivatives_calls = 0
        self.last_universe_telemetry = ScanUniverseTelemetry(
            exchange_symbols=_SHORTLIST + (_ACTIVE,),
            eligible_symbols=_SHORTLIST,
            excluded=(("ETHUSDT", "UNIVERSE_FILTER"),),
            observed_at=_NOW,
        )

    async def fetch_market_snapshots(self):
        return [
            MarketSnapshot(
                symbol=symbol,
                last_price=100.0,
                price_24h_pct=1.0,
                turnover_24h=10_000_000.0,
                volume_24h=100_000.0,
                mark_price=100.0,
                open_interest=1000.0,
                timestamp=_NOW,
            )
            for symbol in _SHORTLIST
        ]

    def shortlist(self, snapshots):  # noqa: ANN001
        return list(snapshots)[:100]

    async def fetch_symbol_frames(self, symbols):  # noqa: ANN001
        return {}

    async def fetch_optional_derivatives(self, symbol):  # noqa: ANN001
        self.derivatives_calls += 1
        return {}


class _Hub:
    """Minimal hub seam: universe bookkeeping, no transport."""

    def __init__(self) -> None:
        self.universe: tuple[str, ...] = ()
        self.observers: list[object] = []
        self.epoch = ConnectionEpoch(0, 1)

    def add_observer(self, observer):  # noqa: ANN001
        self.observers.append(observer)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    def update_universe(self, symbols) -> None:  # noqa: ANN001
        self.universe = tuple(str(symbol).upper() for symbol in symbols)

    def get_snapshot(self, symbol):  # noqa: ANN001
        return None

    def get_forming_candle(self, symbol):  # noqa: ANN001
        return None

    def get_last_closed_candle(self, symbol):  # noqa: ANN001
        return None

    def get_health(self):  # noqa: ANN001
        return MarketDataHealth(state=HealthStatus.HEALTHY, updated_at=_NOW)

    def get_subscription_state(self):  # noqa: ANN001
        return SubscriptionState(frozenset(), frozenset(), frozenset(), frozenset())

    def is_current_connection_epoch(self, epoch):  # noqa: ANN001
        return epoch == self.epoch


def _candle(symbol: str, *, minute: int, epoch: ConnectionEpoch) -> MarketCandle:
    open_time = _NOW - timedelta(minutes=minute)
    return MarketCandle(
        symbol=symbol,
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        interval="1",
        open=Decimal("100"),
        high=Decimal("100"),
        low=Decimal("100"),
        close=Decimal("100"),
        volume=Decimal("1000"),
        turnover=Decimal("100000"),
        confirmed=True,
        exchange_timestamp=open_time,
        received_at=datetime.now(UTC),
        connection_epoch=epoch,
    )


def _ticker(symbol: str, epoch: ConnectionEpoch, *, stale: bool) -> MarketTickerSnapshot:
    return MarketTickerSnapshot(
        symbol=symbol,
        connection_epoch=epoch,
        last_price=Decimal("100"),
        mark_price=Decimal("100"),
        index_price=Decimal("100"),
        funding_rate=Decimal("0.0001"),
        open_interest=Decimal("1500"),
        volume_24h=Decimal("100000"),
        turnover_24h=Decimal("10000000"),
        price_24h_pcnt=Decimal("0.01"),
        high_price_24h=Decimal("101"),
        low_price_24h=Decimal("99"),
        prev_price_24h=Decimal("100"),
        bid1_price=Decimal("100"),
        bid1_size=Decimal("1"),
        ask1_price=Decimal("100"),
        ask1_size=Decimal("1"),
        received_at=datetime.now(UTC)
        - (timedelta(seconds=120) if stale else timedelta(seconds=3)),
    )


def _feed_closed_minute(store, universe, *, stale_symbol: str | None = None) -> None:
    """Deliver one closed-minute candle + ticker per symbol, as the Hub would."""
    epoch = ConnectionEpoch(0, 1)
    for symbol in universe:
        store.observe_closed_candle(_candle(symbol, minute=1, epoch=epoch))
        store.observe_ticker(
            _ticker(symbol, epoch, stale=(symbol == stale_symbol))
        )


class _FakeNotifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def send_signal(self, message: str) -> bool:
        self.messages.append(message)
        return True

    async def send_alert(self, message: str) -> bool:
        self.messages.append(message)
        return True


def _build(
    tmp_path,  # noqa: ANN001
    make_features,  # noqa: ANN001
    make_event_state,  # noqa: ANN001
) -> tuple[
    ShortSignalBot, BotRepository, Database, CanonicalMarketDataProvider, _Client
]:
    database = Database(f"sqlite:///{tmp_path / 'event-driven.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    client = _Client()
    hub = _Hub()
    config = AppConfig(
        canonical_market_data_provider_mode="HUB_PREFERRED",
        market_frame_store_mode="PREFERRED",
        event_driven_scan_enabled=True,
        background_outcomes_enabled=True,
        shortlist_size=100,
        deep_scan_kline_limit=300,
        derivatives_enabled=True,
        market_frame_snapshot_path=str(tmp_path / "frames.json.gz"),
        p0_scan_deadline_sec=15,
        p1_scan_deadline_sec=30,
        scan_evaluation_consumers=1,
    )
    provider = CanonicalMarketDataProvider(
        scanner=_TelemetryScanner(client),
        config=config,
        hub=hub,
        continuity=GapTracker(),
        clock=lambda: datetime.now(UTC),
    )
    bot = ShortSignalBot(
        config=config,
        repository=repository,
        notifier=_FakeNotifier(),
        market_data_provider=provider,
    )
    # Deterministic strategy seam (the same characterization seam the existing
    # runtime-flow tests use): strong fixture features plus suppressed new-event
    # detection, so exactly the persisted active event can produce a signal
    # without touching thresholds, scoring, grade, or admission.
    anchor = datetime.now(UTC)
    bot._feature_builder.build = lambda symbol, *args, **kwargs: make_features(
        symbol=symbol, asof=anchor, price=112.0
    )
    bot._pump_detector.build_event = lambda *_args, **_kwargs: None
    now = datetime.now(UTC)
    repository.upsert_event_state(
        make_event_state(
            symbol=_ACTIVE,
            event_id=f"{_ACTIVE}:15m:1:111",
            state=EventStatus.PULLBACK_OBSERVED,
            event_start_time=now - timedelta(minutes=45),
            event_high_time=now - timedelta(minutes=30),
            signal_sent_at=None,
            signal_id=None,
            pullback_detected_at=now - timedelta(minutes=10),
            pullback_depth_pct=3.5,
            pullback_low_price=111.0,
            expires_at=now + timedelta(hours=1),
            updated_at=anchor,
        )
    )
    return bot, repository, database, provider, client


async def _manual_window(
    bot: ShortSignalBot,
    provider: CanonicalMarketDataProvider,
    client: _Client,
    window_id: str,
    *,
    feed_stale: bool,
) -> dict[str, ScanTerminalStatus]:
    coordinator = bot._scan_coordinator
    assert coordinator is not None
    store = provider.frame_store
    assert store is not None
    warm_task = getattr(bot, "_frame_store_warm_task", None)
    if warm_task is not None:
        await warm_task
    _feed_closed_minute(
        store,
        provider.hub.universe,
        stale_symbol=_STALE if feed_stale else None,
    )
    boundary = datetime.now(UTC).replace(second=0, microsecond=0)
    shortlist = tuple(
        snapshot.symbol
        for snapshot in provider.shortlist(list(bot._event_universe_snapshots))
    )
    scheduled = coordinator.open_window(
        window_id,
        boundary,
        active_symbols=(_ACTIVE,),
        shortlist_symbols=shortlist,
    )
    assert scheduled == 101
    bot._window_records[window_id] = {"opened_at": boundary}
    statuses: dict[str, ScanTerminalStatus] = {}
    order: list[str] = []
    for _ in range(scheduled):
        task = await coordinator.acquire()
        assert task is not None
        order.append(task.symbol)
        status = await bot._execute_scan_task(task)
        assert coordinator.record_terminal(task, status)
        statuses[task.symbol] = status
    assert order[0] == _ACTIVE  # P0 dequeued before every queued P1
    assert coordinator.is_window_complete(window_id)
    await bot._finalize_event_window(window_id)
    return statuses


def test_event_driven_window_full_contract(
    tmp_path, make_features, make_event_state  # noqa: ANN001
) -> None:
    async def run() -> dict:
        bot, repository, database, provider, client = _build(
            tmp_path, make_features, make_event_state
        )
        await bot._prepare_event_driven_runtime()
        # Cancel the background consumers so this proof drives the exact same
        # task/terminal protocol deterministically.
        for consumer in bot._scan_consumers:
            consumer.cancel()
        await asyncio.gather(*bot._scan_consumers, return_exceptions=True)
        bot._scan_consumers = []

        statuses = await _manual_window(
            bot, provider, client, "window-1", feed_stale=True
        )
        assert len(statuses) == 101
        # PREFERRED mode: the broad per-symbol path never re-fetches OI/funding
        # over REST; the frame store supplies derivatives for all 101 symbols.
        assert provider._scanner.derivatives_calls == 0

        # Stale input cannot produce an actionable result.
        assert statuses[_STALE] in {
            ScanTerminalStatus.STALE_DATA,
            ScanTerminalStatus.INCOMPLETE_DATA,
        }
        # The missing candle was repaired through the bounded REST fallback.
        assert _GAP in client.fallback_calls
        assert statuses[_GAP] in {
            ScanTerminalStatus.SCANNED_OK,
            ScanTerminalStatus.NO_SETUP,
        }
        # The actionable active event yields exactly one signal transaction.
        assert statuses[_ACTIVE] is ScanTerminalStatus.SCANNED_OK
        with database.session() as session:
            assert len(session.scalars(select(SignalModel)).all()) == 1
            assert len(session.scalars(select(SignalProvenanceModel)).all()) == 1
            queued = session.scalars(
                select(TelegramDeliveryOutboxModel)
            ).all()
            assert len(queued) == 1
            assert queued[0].status == "PENDING"
            cycles = session.scalars(select(MarketScanCycleModel)).all()
            assert len(cycles) == 1
            result_rows = session.scalars(
                select(MarketScanSymbolResultModel)
            ).all()
            assert len(result_rows) == 101

        # A second window (restart-like replay) creates no duplicates: the
        # persisted signal_id suppresses a second signal/outbox row.
        await _manual_window(bot, provider, client, "window-2", feed_stale=False)
        with database.session() as session:
            assert (
                session.scalar(select(func.count()).select_from(SignalModel)) == 1
            )
            assert (
                session.scalar(
                    select(func.count()).select_from(TelegramDeliveryOutboxModel)
                )
                == 1
            )
            assert (
                session.scalar(
                    select(func.count()).select_from(SignalProvenanceModel)
                )
                == 1
            )
        await bot._stop_event_driven_runtime()
        return statuses

    statuses = asyncio.run(run())
    assert statuses[_STALE] is not ScanTerminalStatus.SCANNED_OK
