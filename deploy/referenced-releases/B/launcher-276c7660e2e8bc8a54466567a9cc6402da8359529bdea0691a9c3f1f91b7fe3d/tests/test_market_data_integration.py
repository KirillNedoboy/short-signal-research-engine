from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.config import AppConfig
from app.domain import EventStatus, ShortZone
from app.main import ShortSignalBot
from app.market.coverage import ScanUniverseTelemetry
from app.market_data.continuity import GapTracker
from app.signals.climax import evaluate_climax_bundle
from app.signals.engine import SignalEngine
from app.storage.db import Database
from app.storage.repository import BotRepository


class _RecordingHub:
    def __init__(self) -> None:
        self.started = False
        self.stopped = False
        self.universes: list[tuple[str, ...]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def update_universe(self, symbols) -> None:
        self.universes.append(tuple(symbols))


class _UnavailableHub(_RecordingHub):
    async def start(self) -> None:
        self.started = True


class _ReadinessClient:
    async def fetch_klines(self, *_args, **_kwargs):
        return [["ok"]]


class _ReadinessScanner:
    def __init__(self) -> None:
        self.client = _ReadinessClient()


class _ReadinessNotifier:
    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None


def test_market_data_shadow_config_defaults_disabled() -> None:
    assert AppConfig().market_data_ws_shadow_enabled is False
    assert (
        AppConfig(market_data_ws_shadow_enabled=True).market_data_ws_shadow_enabled
        is True
    )


def test_shadow_lifecycle_is_started_and_stopped_without_strategy_getters() -> None:
    async def run() -> None:
        hub = _RecordingHub()
        bot = object.__new__(ShortSignalBot)
        bot._config = AppConfig(market_data_ws_shadow_enabled=True)
        bot._market_data_hub = hub
        bot._notifier = type("Notifier", (), {"close": _close})()
        await bot._start_market_data_shadow()
        assert hub.started is True
        await bot._stop_market_data_shadow()
        assert hub.stopped is True

    asyncio.run(run())


async def _close(_self) -> None:
    return None


def test_rest_universe_is_forwarded_as_control_plane_data() -> None:
    bot = object.__new__(ShortSignalBot)
    hub = _RecordingHub()
    bot._market_data_hub = hub
    telemetry = ScanUniverseTelemetry(
        exchange_symbols=("AAAUSDT",),
        eligible_symbols=("BBBUSD", "AAAUSDT"),
        excluded=(),
        observed_at=datetime.now(timezone.utc),
    )

    bot._publish_market_data_universe(telemetry)

    assert hub.universes == [("BBBUSD", "AAAUSDT")]


def test_strategy_process_source_has_no_market_data_getters() -> None:
    source = ShortSignalBot._process_symbol.__code__.co_names
    assert "get_snapshot" not in source
    assert "get_forming_candle" not in source
    assert "get_last_closed_candle" not in source


def test_strategy_decision_parity_is_unchanged_when_shadow_flag_changes(
    make_event_state, make_features
) -> None:
    state = make_event_state(
        state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8
    )
    features = make_features()
    zone = ShortZone(low=110.5, high=113.8, mode="event_range")
    off = SignalEngine(AppConfig(market_data_ws_shadow_enabled=False)).evaluate(
        state, features, zone, features.asof
    )
    on = SignalEngine(AppConfig(market_data_ws_shadow_enabled=True)).evaluate(
        state, features, zone, features.asof
    )
    assert off == on


def test_all_climax_branch_results_are_unchanged_when_phase6_is_on(
    make_event_state, make_features, make_frame
) -> None:
    state = make_event_state()
    features = make_features()
    frame = make_frame([100 + index * 0.5 for index in range(30)], start=features.asof - timedelta(minutes=29))
    common = {
        "volume_climax_unwind_enabled": True,
        "low_volume_extension_enabled": True,
    }
    off = evaluate_climax_bundle(
        state,
        features,
        frame,
        AppConfig(market_data_ws_shadow_enabled=False, **common),
    )
    on = evaluate_climax_bundle(
        state,
        features,
        frame,
        AppConfig(market_data_ws_shadow_enabled=True, **common),
    )

    assert off == on
    assert set(on.branch_evaluations) == {
        "VOLUME_CLIMAX_UNWIND",
        "LOW_VOLUME_EXTENSION_FAILURE",
    }


def test_strategy_and_lifecycle_packages_do_not_import_phase6_components() -> None:
    root = Path(__file__).parents[1] / "app"
    forbidden = ("ParityMonitor", "GapTracker", "REST_BACKFILL")
    files = [
        path
        for package in ("signals", "baseline", "events")
        for path in (root / package).rglob("*.py")
    ]
    violations = {
        str(path.relative_to(root)): token
        for path in files
        for token in forbidden
        if token in path.read_text(encoding="utf-8")
    }
    assert violations == {}


def test_ws_outage_does_not_block_ready_startup(tmp_path) -> None:
    async def run() -> None:
        database = Database(f"sqlite:///{tmp_path / 'ws-outage.sqlite'}")
        database.create_all()
        hub = _UnavailableHub()
        bot = ShortSignalBot(
            config=AppConfig(market_data_ws_shadow_enabled=True),
            repository=BotRepository(database),
            scanner=_ReadinessScanner(),
            notifier=_ReadinessNotifier(),
            market_data_hub=hub,
        )
        await bot.startup()
        try:
            assert bot._ready is True
            assert bot._lifecycle == "READY"
            assert hub.started is True
        finally:
            await bot.shutdown()

    asyncio.run(run())


def test_live_composition_attaches_gap_tracker_without_backfiller(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'phase6-composition.sqlite'}")
    database.create_all()
    bot = ShortSignalBot(
        config=AppConfig(market_data_ws_shadow_enabled=True),
        repository=BotRepository(database),
        scanner=_ReadinessScanner(),
        notifier=_ReadinessNotifier(),
    )

    assert isinstance(bot._market_data_gap_tracker, GapTracker)
    assert bot._market_data_gap_tracker in bot._market_data_hub._observers
    assert "ClosedCandleBackfiller" not in inspect.getsource(
        __import__("app.main", fromlist=["ShortSignalBot"])
    )


def test_disabled_ws_shadow_does_not_construct_gap_tracker(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'phase6-disabled.sqlite'}")
    database.create_all()
    bot = ShortSignalBot(
        config=AppConfig(market_data_ws_shadow_enabled=False),
        repository=BotRepository(database),
        scanner=_ReadinessScanner(),
        notifier=_ReadinessNotifier(),
    )

    assert bot._market_data_gap_tracker is None
