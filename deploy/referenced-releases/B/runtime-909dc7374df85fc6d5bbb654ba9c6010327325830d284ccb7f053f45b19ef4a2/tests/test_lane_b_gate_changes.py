"""Lane-B gate/runtime changes: VCU re-check parity + per-symbol rate limits.

Covers the two experimental-lane runtime changes:
  (a) the pre-delivery re-check now runs for VOLUME_CLIMAX_UNWIND when
      ``climax_recheck_extended`` is on, and the inner guard compares against
      the DECISION's subtype (no false ``fresh_admission_failed`` for VCU);
  (b) ``climax_event_cooldown_minutes`` and ``max_signals_per_symbol_24h``
      suppress new signals per symbol with a distinct logged veto reason.

Flags are injected with ``object.__setattr__`` because ``app/config.py`` is
owned by another writer and the keys are not declared on AppConfig yet (the
runtime reads them via ``getattr(config, key, default)``).
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.config import AppConfig
from app.domain import EventStatus
from app.main import ShortSignalBot
import app.main as main_mod

UTC = timezone.utc
from app.signals.climax import ClimaxEvaluation, ClimaxEvaluationBundle
from app.storage.db import Database
from app.storage.models import SignalModel, StrategyObservationModel
from app.storage.repository import BotRepository



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


class _RecheckScanner:
    """Scanner exposing the fetch_symbol_frames surface the re-check probes."""
    def __init__(self, fresh_frame) -> None:
        self._fresh_frame = fresh_frame
        self.client = type(
            "_Client", (), {"fetch_klines": lambda *_a, **_k: []}
        )()
    async def fetch_optional_derivatives(self, _symbol: str):
        return {}

    async def fetch_symbol_frames(self, _symbols: list[str]):
        return {"ONTUSDT": self._fresh_frame}


class _LogRecorder:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def _fmt(self, msg, args):
        return str(msg) % args if args else str(msg)

    def info(self, msg, *args):
        self.messages.append(self._fmt(msg, args))

    def warning(self, msg, *args):
        self.messages.append(self._fmt(msg, args))

    def error(self, msg, *args):
        self.messages.append(self._fmt(msg, args))

    def debug(self, msg, *args):
        self.messages.append(self._fmt(msg, args))

    def exception(self, msg, *args):
        self.messages.append(self._fmt(msg, args))

    def setLevel(self, *_args, **_kwargs):
        return None


def _set_flag(config: AppConfig, key: str, value: object) -> None:
    """Preset an undeclared config key (getattr-flag pattern)."""
    object.__setattr__(config, key, value)


def _eval(subtype, metadata, veto):
    return ClimaxEvaluation(
        subtype=subtype,
        score=75,
        grade="B",
        metadata=metadata,
        veto_reasons=veto,
        data_quality=[],
    )


def _bundle(subtype, metadata, veto):
    selected = _eval(subtype, metadata, veto)
    return ClimaxEvaluationBundle(
        selected=selected,
        branch_evaluations={subtype or "NONE": selected},
    )


# --------------------------------------------------------------------------
# (a) VCU pre-delivery re-check parity
# --------------------------------------------------------------------------
def test_vcu_recheck_runs_and_keeps_signal_when_subtype_matches(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    """With climax_recheck_extended on, VCU must run the re-check and — because
    the guard compares against the decision's subtype — a matching actionable
    VCU re-evaluation must still deliver the signal (the old hardcoded
    LOW_VOLUME guard would have vetoed it as fresh_admission_failed)."""

    async def _run():
        database = Database(f"sqlite:///{tmp_path / 'vcu-recheck.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        meta = {
            "strategy_subtype": "VOLUME_CLIMAX_UNWIND",
            "model_version": "climax-v1",
            "event_high": state.event_high,
            "entry_distance_below_high_pct": 0.0,
        }
        initial_features = make_features(asof=state.updated_at, price=state.event_high)
        fresh_features = make_features(
            asof=state.updated_at + timedelta(minutes=1), price=state.event_high
        )
        fresh_frame = make_frame([114.0, 115.0])
        bundles = iter(
            [
                _bundle("VOLUME_CLIMAX_UNWIND", meta, []),
                _bundle("VOLUME_CLIMAX_UNWIND", meta, []),
            ]
        )
        monkeypatch.setattr(
            "app.main.evaluate_climax_bundle",
            lambda *_a, **_k: next(bundles),
            raising=False,
        )
        config = AppConfig(
            climax_short_enabled=True, volume_climax_lifecycle_shadow_enabled=False
        )
        _set_flag(config, "climax_recheck_extended", True)
        bot = ShortSignalBot(
            config=config,
            repository=repository,
            scanner=_RecheckScanner(fresh_frame),
            notifier=_FakeNotifier(),
        )
        bot._feature_builder.build = lambda *_a, **_k: fresh_features
        log = _LogRecorder()
        bot._logger = log
        decision = await bot._evaluate_and_send_climax(
            "ONTUSDT", object(), state, features=initial_features
        )
        with database.session() as session:
            phases = [
                row.evaluation_phase
                for row in session.scalars(select(StrategyObservationModel)).all()
            ]
            signals = len(session.scalars(select(SignalModel)).all())
        return decision, set(phases) | set(log.messages), signals

    decision, observed, signals = asyncio.run(_run())
    assert decision is not None
    assert signals == 1
    assert "PRE_DELIVERY_RECHECK" in observed
    assert not any("fresh_admission_failed" in text for text in observed)


def test_vcu_recheck_disabled_flag_skips_recheck(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    """climax_recheck_extended=False must restore the old VCU behavior: no
    re-check evaluation, no PRE_DELIVERY_RECHECK rows, signal delivered."""

    async def _run():
        database = Database(f"sqlite:///{tmp_path / 'vcu-flag-off.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        meta = {
            "strategy_subtype": "VOLUME_CLIMAX_UNWIND",
            "model_version": "climax-v1",
            "event_high": state.event_high,
            "entry_distance_below_high_pct": 0.0,
        }
        features = make_features(asof=state.updated_at, price=state.event_high)
        monkeypatch.setattr(
            "app.main.evaluate_climax_bundle",
            lambda *_a, **_k: _bundle("VOLUME_CLIMAX_UNWIND", meta, []),
            raising=False,
        )
        config = AppConfig(
            climax_short_enabled=True, volume_climax_lifecycle_shadow_enabled=False
        )
        _set_flag(config, "climax_recheck_extended", False)
        bot = ShortSignalBot(
            config=config,
            repository=repository,
            scanner=_RecheckScanner(make_frame([114.0, 115.0])),
            notifier=_FakeNotifier(),
        )
        decision = await bot._evaluate_and_send_climax(
            "ONTUSDT", object(), state, features=features
        )
        with database.session() as session:
            phases = [
                row.evaluation_phase
                for row in session.scalars(select(StrategyObservationModel)).all()
            ]
        return decision, phases

    decision, phases = asyncio.run(_run())
    assert decision is not None
    assert set(phases) == {"INITIAL"}


# --------------------------------------------------------------------------
# (b) per-symbol cooldown + 24 h cap
# --------------------------------------------------------------------------
class _NoProvider:
    """Makes the re-check block a no-op so tests isolate the cooldown gate."""

    async def capture_decision_snapshot(self, _symbol: str):
        return None


async def _emit_signal(repository, config, make_event_state, make_features, event_id, meta):
    state = repository.upsert_event_state(
        make_event_state(
            event_id=event_id,
            state=EventStatus.PUMP_DETECTED,
            signal_id=None,
            expires_at=datetime(2030, 1, 1, tzinfo=UTC),
        )
    )
    features = make_features(
        asof=datetime.now(UTC), price=state.event_high, symbol="ONTUSDT"
    )
    original = main_mod.evaluate_climax_bundle
    main_mod.evaluate_climax_bundle = lambda *_a, **_k: _bundle(
        "LOW_VOLUME_EXTENSION_FAILURE", meta, []
    )
    try:
        bot = ShortSignalBot(
            config=config,
            repository=repository,
            scanner=_RecheckScanner(None),
            notifier=_FakeNotifier(),
        )
        bot._market_data_provider = _NoProvider()
        bot._logger = _LogRecorder()
        decision = await bot._evaluate_and_send_climax(
            "ONTUSDT", object(), state, features=features
        )
        return decision, bot
    finally:
        main_mod.evaluate_climax_bundle = original


def test_symbol_cooldown_suppresses_second_signal(
    tmp_path, make_event_state, make_features
) -> None:
    async def _run():
        database = Database(f"sqlite:///{tmp_path / 'cooldown.db'}")
        database.create_all()
        repository = BotRepository(database)
        config = AppConfig(
            climax_short_enabled=True, volume_climax_lifecycle_shadow_enabled=False
        )
        meta = {
            "strategy_subtype": "LOW_VOLUME_EXTENSION_FAILURE",
            "model_version": "climax-v1",
            "event_high": 115.0,
            "entry_distance_below_high_pct": 0.0,
        }
        first, _bot = await _emit_signal(
            repository, config, make_event_state, make_features,
            "ONTUSDT:15m:1:111", meta,
        )
        # Same symbol, different event (event-level dedupe cannot catch it).
        second, bot2 = await _emit_signal(
            repository, config, make_event_state, make_features,
            "ONTUSDT:15m:2:112", meta,
        )
        with database.session() as session:
            signals = len(session.scalars(select(SignalModel)).all())
        now = datetime.now(UTC)
        return first, second, signals, bot2, repository, now

    first, second, signals, bot2, repository, now = asyncio.run(_run())
    assert first is not None
    assert second is None
    assert signals == 1
    assert any("climax_event_cooldown" in text for text in bot2._logger.messages)
    # Repository helper semantics (indexed window probe).
    assert repository.has_recent_signal_for_symbol(
        "ONTUSDT", since=now - timedelta(minutes=90)
    )
    assert not repository.has_recent_signal_for_symbol(
        "ONTUSDT", since=now + timedelta(minutes=1)
    )
    assert repository.count_recent_signals_for_symbol(
        "ONTUSDT", since=now - timedelta(hours=24)
    ) == 1


def test_max_signals_per_symbol_24h_cap(
    tmp_path, make_event_state, make_features
) -> None:
    async def _run():
        database = Database(f"sqlite:///{tmp_path / 'cap.db'}")
        database.create_all()
        repository = BotRepository(database)
        config = AppConfig(
            climax_short_enabled=True, volume_climax_lifecycle_shadow_enabled=False
        )
        _set_flag(config, "climax_event_cooldown_minutes", 0)  # cooldown off
        _set_flag(config, "max_signals_per_symbol_24h", 1)
        meta = {
            "strategy_subtype": "LOW_VOLUME_EXTENSION_FAILURE",
            "model_version": "climax-v1",
            "event_high": 115.0,
            "entry_distance_below_high_pct": 0.0,
        }
        first, _bot = await _emit_signal(
            repository, config, make_event_state, make_features,
            "ONTUSDT:15m:1:111", meta,
        )
        second, bot2 = await _emit_signal(
            repository, config, make_event_state, make_features,
            "ONTUSDT:15m:2:112", meta,
        )
        with database.session() as session:
            signals = len(session.scalars(select(SignalModel)).all())
        return first, second, signals, bot2._logger

    first, second, signals, log = asyncio.run(_run())
    assert first is not None
    assert second is None
    assert signals == 1
    assert any("max_signals_per_symbol_24h" in text for text in log.messages)
