"""Tests for the trapped-longs pre-delivery fresh recheck (fail-closed).

Contract: an unavailable or stale fresh snapshot must VETO delivery; a
successful fresh evaluation re-points signal provenance at the recheck
evaluation (admission_evaluation_id != decision_evaluation_id).
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from sqlalchemy import select

from app.config import AppConfig
from app.main import ShortSignalBot
from app.signals.climax import ClimaxEvaluation
from app.signals.trapped_longs import TRAPPED_LONGS_REVERSAL
from app.signals.trapped_longs_v2 import (
    TRAPPED_LONGS_REVERSAL_V2,
    TRAPPED_LONGS_V2_MODEL_VERSION,
    ExplanationFactor,
    TrappedLongsV2Result,
)
from app.storage.db import Database
from app.storage.models import (
    ClimaxEvaluationModel,
    SignalModel,
    SignalProvenanceModel,
    TelegramDeliveryOutboxModel,
)
from app.storage.repository import BotRepository


class _FakeClient:
    async def fetch_klines(self, *_args, **_kwargs):
        return []


class _FramesScanner:
    """Frame-capable scanner; returns the configured frames by symbol."""

    def __init__(self, frames: dict) -> None:
        self.client = _FakeClient()
        self._frames = frames

    async def fetch_symbol_frames(self, _symbols: list[str]):
        return dict(self._frames)

    async def fetch_optional_derivatives(self, _symbol: str):
        return {}


class _Notifier:
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


def _evaluation(subtype: str | None, veto: list[str] | None = None) -> ClimaxEvaluation:
    return ClimaxEvaluation(
        subtype=subtype,
        score=80,
        grade="A",
        metadata={
            "breakout_reference": 100.0,
            "event_high": 115.0,
            "model_version": "trapped-longs-v1",
            "closed_structural_candles": 3,
            "close_below_breakout_reference": True,
            "failed_retest_confirmed": True,
            "no_new_high": True,
        },
        veto_reasons=list(veto or []),
        data_quality=[],
    )


def _run(tmp_path, make_event_state, make_features, make_frame, monkeypatch, scanner, second):
    async def _cycle():
        database = Database(f"sqlite:///{tmp_path / 'trapped-recheck.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        features = make_features(asof=state.updated_at)
        fresh_features = make_features(
            asof=state.updated_at + timedelta(minutes=1), price=111.0
        )
        actionable = _evaluation(TRAPPED_LONGS_REVERSAL)
        evaluations = iter([actionable, second])
        monkeypatch.setattr(
            "app.main.evaluate_trapped_longs_reversal",
            lambda *_args, **_kwargs: next(evaluations),
        )
        bot = ShortSignalBot(
            config=AppConfig(
                trapped_longs_reversal_enabled=True,
                trapped_longs_live_delivery_enabled=True,
            ),
            repository=repository,
            scanner=scanner,
            notifier=_Notifier(),
        )
        bot._feature_builder.build = lambda *_a, **_k: fresh_features
        decision = await bot._evaluate_and_send_trapped_longs(
            state, features, make_frame([112.0, 111.0])
        )
        with database.session() as session:
            signals = session.scalars(select(SignalModel)).all()
            provenance = session.scalars(select(SignalProvenanceModel)).all()
            rechecks = [
                row
                for row in session.scalars(
                    select(ClimaxEvaluationModel).order_by(ClimaxEvaluationModel.id)
                ).all()
                if row.lifecycle_state == "DELIVERY_RECHECK"
            ]
        return decision, signals, provenance, rechecks
    return asyncio.run(_cycle())


def test_trapped_longs_no_fresh_snapshot_saves_no_signal(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    # Frame-capable scanner, but the fresh snapshot is missing (empty result).
    decision, signals, provenance, rechecks = _run(
        tmp_path,
        make_event_state,
        make_features,
        make_frame,
        monkeypatch,
        _FramesScanner({}),
        _evaluation(TRAPPED_LONGS_REVERSAL),
    )
    assert decision is None
    assert signals == []
    assert provenance == []
    assert rechecks == []


def test_trapped_longs_stale_fresh_evaluation_vetoes(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    frame = make_frame([110.0, 111.0])
    decision, signals, provenance, rechecks = _run(
        tmp_path,
        make_event_state,
        make_features,
        make_frame,
        monkeypatch,
        _FramesScanner({"ONTUSDT": frame}),
        _evaluation(None, ["new_high_before_delivery"]),
    )
    assert decision is None
    assert signals == []
    assert provenance == []
    # The vetoed recheck is still ledgered (fail-closed, but never silent).
    assert len(rechecks) == 1
    assert rechecks[0].actionable is False


def test_trapped_longs_successful_fresh_recheck_repoints_provenance(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    frame = make_frame([110.0, 111.0])
    decision, signals, provenance, rechecks = _run(
        tmp_path,
        make_event_state,
        make_features,
        make_frame,
        monkeypatch,
        _FramesScanner({"ONTUSDT": frame}),
        _evaluation(TRAPPED_LONGS_REVERSAL),
    )
    assert decision is not None
    assert len(signals) == 1
    assert len(provenance) == 1
    row = provenance[0]
    assert row.decision_evaluation_id is not None
    assert row.admission_evaluation_id is not None
    assert row.admission_evaluation_id != row.decision_evaluation_id
    assert len(rechecks) == 1
    assert rechecks[0].id == row.admission_evaluation_id


def test_trapped_longs_v2_actionable_creates_live_signal_and_outbox(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    async def _run():
        database = Database(f"sqlite:///{tmp_path / 'trapped-v2-live.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        result = TrappedLongsV2Result(
            identity=state.event_id,
            revision=1,
            actionable=True,
            blockers=(),
            explanation={"closed_candles": ExplanationFactor(2, True)},
        )
        monkeypatch.setattr("app.main.evaluate_trapped_longs_reversal_v2", lambda *a, **k: result)
        bot = ShortSignalBot(
            config=AppConfig(
                trapped_longs_reversal_enabled=False,
                trapped_longs_reversal_v2_enabled=True,
                trapped_longs_v2_live_delivery_enabled=True,
            ),
            repository=repository,
            scanner=_FramesScanner({}),
            notifier=_Notifier(),
        )
        decision = await bot._evaluate_trapped_longs_v2_shadow(
            state, make_features(), make_frame([112.0, 111.0]), derivatives={}
        )
        with database.session() as session:
            signals = session.scalars(select(SignalModel)).all()
            outbox = session.scalars(select(TelegramDeliveryOutboxModel)).all()
            provenance = session.scalars(select(SignalProvenanceModel)).all()
        return decision, signals, outbox, provenance

    decision, signals, outbox, provenance = asyncio.run(_run())
    assert decision is not None
    assert decision.strategy_subtype == TRAPPED_LONGS_REVERSAL_V2
    assert decision.model_version == TRAPPED_LONGS_V2_MODEL_VERSION
    assert len(signals) == len(outbox) == len(provenance) == 1
    assert provenance[0].strategy_branch == TRAPPED_LONGS_REVERSAL


def test_trapped_longs_v2_blocked_creates_no_live_outbox(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    async def _run():
        database = Database(f"sqlite:///{tmp_path / 'trapped-v2-blocked.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        result = TrappedLongsV2Result(
            identity=state.event_id,
            revision=1,
            actionable=False,
            blockers=("oi_sequence_missing",),
            explanation={"oi_sequence": ExplanationFactor(False, False)},
        )
        monkeypatch.setattr("app.main.evaluate_trapped_longs_reversal_v2", lambda *a, **k: result)
        bot = ShortSignalBot(
            config=AppConfig(
                trapped_longs_reversal_enabled=False,
                trapped_longs_reversal_v2_enabled=True,
                trapped_longs_v2_live_delivery_enabled=True,
            ),
            repository=repository,
            scanner=_FramesScanner({}),
            notifier=_Notifier(),
        )
        decision = await bot._evaluate_trapped_longs_v2_shadow(
            state, make_features(), make_frame([112.0, 111.0]), derivatives={}
        )
        with database.session() as session:
            outbox = session.scalars(select(TelegramDeliveryOutboxModel)).all()
        return decision, outbox

    decision, outbox = asyncio.run(_run())
    assert decision is None
    assert outbox == []


def test_trapped_longs_v1_path_is_disabled_without_evaluation(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch
) -> None:
    async def _run_disabled():
        database = Database(f"sqlite:///{tmp_path / 'trapped-v1-disabled.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        bot = ShortSignalBot(
            config=AppConfig(trapped_longs_reversal_enabled=False),
            repository=repository,
            scanner=_FramesScanner({}),
            notifier=_Notifier(),
        )
        monkeypatch.setattr(
            "app.main.evaluate_trapped_longs_reversal",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("V1 evaluator must not run when disabled")
            ),
        )
        return await bot._evaluate_and_send_trapped_longs(
            state, make_features(), make_frame([112.0, 111.0])
        )

    assert asyncio.run(_run_disabled()) is None
