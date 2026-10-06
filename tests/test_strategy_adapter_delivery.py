from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import pandas as pd
from sqlalchemy import select

from app.config import AppConfig, ManualOnlyConfigurationError, require_manual_only
from app.domain import EventStatus, ShortZone, SignalType
from app.main import ShortSignalBot
from app.observability.strategy_observations import (
    ObservationWriteStatus,
    StrategyObservation,
    build_observation_evidence,
    make_observation_idempotency_key,
)
from app.storage.db import Database
from app.storage.models import (
    ClimaxEvaluationModel,
    EventStateModel,
    SignalModel,
    SignalProvenanceModel,
    StrategyObservationModel,
    TelegramDeliveryOutboxModel,
)
from app.storage.repository import BotRepository
from app.strategies.contracts import StrategyEvaluation
from app.strategies.registry import REQUIRED_STRATEGY_TYPES
from app.signals.climax import ClimaxEvaluation, ClimaxEvaluationBundle


class _NoExchangeClient:
    def __init__(self) -> None:
        self.calls = 0
        self.order_calls = 0

    async def fetch_klines(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("exchange/order methods must not be called")

    async def create_order(self, *_args, **_kwargs):
        self.order_calls += 1
        raise AssertionError("manual-only runtime must not place orders")

    async def place_order(self, *_args, **_kwargs):
        self.order_calls += 1
        raise AssertionError("manual-only runtime must not place orders")

    async def cancel_order(self, *_args, **_kwargs):
        self.order_calls += 1
        raise AssertionError("manual-only runtime must not place orders")


class _NoExchangeScanner:
    supports_symbol_frames = True

    def __init__(self) -> None:
        self.client = _NoExchangeClient()
        self.derivative_calls = 0

    async def fetch_optional_derivatives(self, _symbol: str):
        self.derivative_calls += 1
        raise AssertionError("exchange/order methods must not be called")

    async def fetch_symbol_frames(self, _symbols):
        return {}


class _ExplodingNotifier:
    def __init__(self) -> None:
        self.calls = 0

    async def send_signal(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError("blocked delivery must not call notifier")


class _RecordingNotifier:
    def __init__(self) -> None:
        self.calls = 0

    async def send_signal(self, _message: str) -> bool:
        self.calls += 1
        return True


def _bot(database: Database, scanner=None, notifier=None) -> ShortSignalBot:
    return ShortSignalBot(
        config=AppConfig(
            climax_short_enabled=True,
            trapped_longs_reversal_enabled=True,
            climax_fresh_recheck_attempts=1,
            climax_fresh_recheck_retry_delay_sec=0,
        ),
        repository=BotRepository(database),
        scanner=scanner or _NoExchangeScanner(),
        notifier=notifier or object(),
    )


def _evaluation(state, features, *, strategy_type: str, subtype: str, model: str) -> StrategyEvaluation:
    return StrategyEvaluation(
        strategy_type=strategy_type,
        strategy_subtype=subtype,
        model_version=model,
        actionable=True,
        score=82,
        grade="A",
        reasons=["registry_actionable"],
        blockers=[],
        vetoes=[],
        risk_flags=[],
        strategy_metadata={
            "event_high": state.event_high,
            "entry_distance_below_high_pct": 2.6,
            "strategy_subtype": subtype,
        "breakout_reference": 110.0,
        },
        symbol=features.symbol,
        event_id=state.event_id,
        decision_timestamp=features.asof,
    )


def _audit_observation(*, branch: str, model: str, initial_id: int, final_id: int, final_decision: str, reason: str) -> StrategyObservation:
    observed_at = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
    evidence = build_observation_evidence({"branch": branch, "final": final_decision})
    key = make_observation_idempotency_key(
        strategy_family="CLIMAX_EXHAUSTION",
        strategy=branch,
        symbol="ONTUSDT",
        root_event_id="root-1",
        event_revision=1,
        evaluation_phase="PRE_DELIVERY_RECHECK",
        market_asof=observed_at,
        input_fingerprint=evidence.input_fingerprint,
        model_version=model,
        config_hash="a" * 64,
    )
    return StrategyObservation(
        observation_id=f"audit-{branch.lower()}-{final_decision.lower()}",
        idempotency_key=key,
        run_id="run-1",
        runtime_instance_id="runtime-1",
        runtime_started_at=observed_at - timedelta(minutes=1),
        code_version="test-code-version",
        strategy_family="CLIMAX_EXHAUSTION",
        strategy=branch,
        evaluation_phase="PRE_DELIVERY_RECHECK",
        symbol="ONTUSDT",
        event_id="ONTUSDT:15m:1:111",
        root_event_id="root-1",
        event_revision=1,
        attempt_id=None,
        evaluation_id=final_id,
        signal_id=None,
        observed_at=observed_at,
        exchange_time=None,
        market_asof=observed_at,
        live_decision=final_decision,
        shadow_decision="NOT_EVALUATED",
        score=82,
        blockers=[] if final_decision == "ACTIONABLE" else [reason],
        warnings=[],
        market_price=112.0,
        event_high=115.0,
        model_version=model,
        config_hash="a" * 64,
        input_fingerprint=evidence.input_fingerprint,
        input_snapshot=evidence.snapshot,
        initial_evaluation_id=initial_id,
        initial_decision="ACTIONABLE",
        final_decision=final_decision,
        final_reason=reason,
        finalized_at=observed_at,
    )


@pytest.mark.parametrize(
    ("strategy_type", "subtype", "model", "family", "root", "evaluation_ids"),
    [
        ("BASELINE_PULLBACK", "BASELINE_PULLBACK", "baseline-v1", "BASELINE_PULLBACK", None, False),
        ("CLIMAX_EXHAUSTION", "VOLUME_CLIMAX_UNWIND", "climax-v1", "CLIMAX_EXHAUSTION", "root-1", True),
        ("CLIMAX_EXHAUSTION", "LOW_VOLUME_EXTENSION_FAILURE", "climax-v1", "CLIMAX_EXHAUSTION", "root-1", True),
        ("TRAPPED_LONGS_REVERSAL", "TRAPPED_LONGS_REVERSAL", "trapped-longs-v1", "TRAPPED_LONGS_REVERSAL", "root-1", True),
    ],
)
def test_registry_decision_delivery_chain_is_atomic_idempotent_and_orphan_free(
    tmp_path,
    make_event_state,
    make_features,
    make_signal_provenance,
    strategy_type,
    subtype,
    model,
    family,
    root,
    evaluation_ids,
) -> None:
    database = Database(f"sqlite:///{tmp_path / f'{subtype}.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    features = make_features(market_asof=state.updated_at)
    bot = _bot(database)
    evaluation = _evaluation(
        state, features, strategy_type=strategy_type, subtype=subtype, model=model
    )
    zone = ShortZone(110.0, 113.8, "event_range")
    decision = bot._new_decision_from_registered_evaluation(
        state=state,
        features=features,
        zone=zone,
        signal_time=features.asof,
        evaluation=evaluation,
    )
    if strategy_type == "BASELINE_PULLBACK":
        provenance = make_signal_provenance()
    else:
        with database.session() as session:
            initial = ClimaxEvaluationModel(
                evaluation_time=features.asof,
                symbol=state.symbol,
                strategy=strategy_type,
                subtype_candidate=subtype,
                model_version=model,
                event_id=state.event_id,
            )
            admission = ClimaxEvaluationModel(
                evaluation_time=features.asof + timedelta(seconds=1),
                symbol=state.symbol,
                strategy=strategy_type,
                subtype_candidate=subtype,
                model_version=model,
                event_id=state.event_id,
            )
            session.add_all([initial, admission])
            session.flush()
            provenance = make_signal_provenance(
                strategy_family=family,
                strategy_branch=subtype,
                root_event_id=root,
                decision_evaluation_id=initial.id if evaluation_ids else None,
                admission_evaluation_id=admission.id if evaluation_ids else None,
            )
    first = repository.persist_final_signal_bundle(
        decision,
        state,
        delivery_payload=f"payload:{subtype}",
        provenance=provenance,
    )
    second = repository.persist_final_signal_bundle(
        decision,
        state,
        delivery_payload=f"payload:{subtype}",
        provenance=provenance,
    )

    assert first.id == second.id
    with database.session() as session:
        assert len(session.scalars(select(SignalModel)).all()) == 1
        assert len(session.scalars(select(SignalProvenanceModel)).all()) == 1
        outbox = session.scalars(select(TelegramDeliveryOutboxModel)).all()
        assert len(outbox) == 1
        assert outbox[0].status == "PENDING"
        assert outbox[0].payload == f"payload:{subtype}"
        assert outbox[0].entity_type == "SIGNAL"
        assert outbox[0].entity_id == first.id
        assert outbox[0].idempotency_key == f"telegram:signal:{first.id}"
        event = session.get(EventStateModel, state.symbol)
        assert event is not None
        assert event.signal_id == first.id
        assert event.state == EventStatus.SIGNAL_SENT.value
        assert session.execute(select(SignalProvenanceModel).where(SignalProvenanceModel.signal_id == first.id)).scalar_one()
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("pragma foreign_key_check").all() == []
        duplicate_queries = {
            "signals": "select symbol, event_id, strategy_subtype, model_version, count(*) from signals group by symbol, event_id, strategy_subtype, model_version having count(*) > 1",
            "provenance": "select signal_id, count(*) from signal_provenance group by signal_id having count(*) > 1",
            "outbox": "select idempotency_key, count(*) from telegram_delivery_outbox group by idempotency_key having count(*) > 1",
            "audit": "select idempotency_key, count(*) from strategy_observations group by idempotency_key having count(*) > 1",
        }
        assert {name: connection.exec_driver_sql(query).all() for name, query in duplicate_queries.items()} == {
            name: [] for name in duplicate_queries
        }
        orphan_queries = {
            "outbox": "select count(*) from telegram_delivery_outbox o left join signals s on s.id=o.entity_id where o.entity_type='SIGNAL' and s.id is null",
            "provenance": "select count(*) from signal_provenance p left join signals s on s.id=p.signal_id where s.id is null",
            "audit_signal": "select count(*) from strategy_observations a left join signals s on s.id=a.signal_id where a.signal_id is not null and s.id is null",
            "event_state": "select count(*) from event_states e left join signals s on s.id=e.signal_id where e.signal_id is not null and s.id is null",
        }
        assert {name: connection.exec_driver_sql(query).scalar_one() for name, query in orphan_queries.items()} == {
            name: 0 for name in orphan_queries
        }


@pytest.mark.parametrize(
    ("identifier", "deliverable"),
    [
        ("BASELINE_PULLBACK", True),
        ("CLIMAX_EXHAUSTION", False),
        ("VOLUME_CLIMAX_UNWIND", True),
        ("LOW_VOLUME_EXTENSION_FAILURE", True),
        ("TRAPPED_LONGS_REVERSAL", True),
    ],
)
def test_registry_generated_pass_and_block_matrix_is_fail_closed(
    tmp_path, make_event_state, make_features, make_frame, make_signal_decision,
    make_signal_provenance, identifier, deliverable
) -> None:
    """Exercise the canonical registry output before projecting delivery decisions."""
    database = Database(f"sqlite:///{tmp_path / f'matrix-{identifier}.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    now = datetime.now(timezone.utc)
    features = make_features(market_asof=now)
    bot = _bot(database)
    evaluations = bot._evaluate_registered_strategies(
        state=state,
        features=features,
        frame_1m=make_frame([110.0, 111.0]),
        short_zone=ShortZone(110.0, 113.8, "event_range"),
        decision_timestamp=now,
    )
    evaluation = next(item for item in evaluations if item.strategy_type == identifier)
    if identifier in {"VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"}:
        evaluation = replace(
            evaluation,
            strategy_type="CLIMAX_EXHAUSTION",
            strategy_subtype=identifier,
            actionable=True,
            vetoes=(),
        )
    elif identifier == "TRAPPED_LONGS_REVERSAL":
        evaluation = replace(
            evaluation,
            strategy_subtype=identifier,
            actionable=True,
            vetoes=(),
        )
    blocked = replace(evaluation, actionable=False, vetoes=("final_recheck_blocked",))
    assert blocked.actionable is False
    with database.session() as session:
        assert session.scalars(select(SignalModel)).all() == []
        assert session.scalars(select(TelegramDeliveryOutboxModel)).all() == []

    if deliverable:
        projected = bot._decision_from_registered_evaluation(
            make_signal_decision(), evaluation
        )
        if identifier == "BASELINE_PULLBACK":
            provenance = make_signal_provenance()
        else:
            with database.session() as session:
                first = ClimaxEvaluationModel(
                    evaluation_time=now, symbol=state.symbol,
                    strategy=identifier if identifier == "TRAPPED_LONGS_REVERSAL" else "CLIMAX_EXHAUSTION",
                    subtype_candidate=projected.strategy_subtype,
                    model_version=projected.model_version, event_id=state.event_id,
                )
                session.add(first)
                session.flush()
                provenance = make_signal_provenance(
                    strategy_family=("TRAPPED_LONGS_REVERSAL" if identifier == "TRAPPED_LONGS_REVERSAL" else "CLIMAX_EXHAUSTION"),
                    strategy_branch=projected.strategy_subtype,
                    root_event_id="root-1",
                    decision_evaluation_id=first.id,
                    admission_evaluation_id=first.id,
                )
        saved = repository.persist_final_signal_bundle(
            projected, state, delivery_payload="matrix-payload", provenance=provenance
        )
        assert saved.id > 0
        with database.session() as session:
            assert len(session.scalars(select(SignalModel)).all()) == 1
            assert len(session.scalars(select(SignalProvenanceModel)).all()) == 1
            assert len(session.scalars(select(TelegramDeliveryOutboxModel)).all()) == 1
    else:
        assert identifier == "CLIMAX_EXHAUSTION"


def test_registry_contains_every_registered_strategy_and_malformed_context_fails_closed(
    tmp_path, make_event_state, make_features, make_frame
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'registry.sqlite'}")
    database.create_all()
    scanner = _NoExchangeScanner()
    bot = _bot(database, scanner)
    state = make_event_state()
    now = datetime.now(timezone.utc)
    frame = make_frame([110.0, 111.0])
    valid = make_features(market_asof=now)
    evaluations = bot._evaluate_registered_strategies(
        state=state,
        features=valid,
        frame_1m=frame,
        short_zone=ShortZone(110.0, 113.8, "event_range"),
        decision_timestamp=now,
    )
    assert {item.strategy_type for item in evaluations} == set(REQUIRED_STRATEGY_TYPES)

    malformed = make_features(market_asof=None)
    assert bot._evaluate_registered_strategies(
        state=state,
        features=malformed,
        frame_1m=frame,
        short_zone=None,
        decision_timestamp=now,
    ) == ()
    with database.session() as session:
        assert session.scalars(select(SignalModel)).all() == []
        assert session.scalars(select(TelegramDeliveryOutboxModel)).all() == []
    assert scanner.client.calls == 0
    assert scanner.derivative_calls == 0


def test_final_recheck_block_is_durable_and_final_pass_links_exactly_once(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'final-recheck.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    when = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
    with database.session() as session:
        initial = ClimaxEvaluationModel(
            evaluation_time=when,
            symbol=state.symbol,
            strategy="CLIMAX_EXHAUSTION",
            subtype_candidate="LOW_VOLUME_EXTENSION_FAILURE",
            model_version="climax-v1",
            event_id=state.event_id,
        )
        final = ClimaxEvaluationModel(
            evaluation_time=when + timedelta(seconds=1),
            symbol=state.symbol,
            strategy="CLIMAX_EXHAUSTION",
            subtype_candidate="LOW_VOLUME_EXTENSION_FAILURE",
            model_version="climax-v1",
            event_id=state.event_id,
        )
        session.add_all([initial, final])
        session.flush()
        initial_id, final_id = initial.id, final.id

    blocked = _audit_observation(
        branch="LOW_VOLUME_EXTENSION_FAILURE",
        model="climax-v1",
        initial_id=initial_id,
        final_id=final_id,
        final_decision="BLOCKED_BY_RECHECK",
        reason="final_recheck_data_missing",
    )
    assert repository.record_strategy_observation(blocked).status is ObservationWriteStatus.INSERTED
    with database.session() as session:
        assert session.scalars(select(SignalModel)).all() == []
        row = session.scalars(select(StrategyObservationModel)).one()
        assert row.final_reason == "final_recheck_data_missing"
        assert row.signal_id is None

    passed = replace(
        blocked,
        observation_id="audit-low-volume-extension-failure-actionable",
        idempotency_key=blocked.idempotency_key + "-pass",
        final_decision="ACTIONABLE",
        final_reason="final_actionable",
    )
    assert repository.record_strategy_observation(passed).status is ObservationWriteStatus.INSERTED
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION",
        strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE",
        model_version="climax-v1",
        signal_type=SignalType.CONFIRM,
        strategy_metadata={"event_high": 115.0, "entry_distance_below_high_pct": 2.6},
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION",
        strategy_branch="LOW_VOLUME_EXTENSION_FAILURE",
        root_event_id="root-1",
        decision_evaluation_id=initial_id,
        admission_evaluation_id=final_id,
    )
    signal = repository.persist_final_signal_bundle(
        decision,
        state,
        delivery_payload="final payload",
        provenance=provenance,
        audit_root_event_id="root-1",
        audit_strategy="LOW_VOLUME_EXTENSION_FAILURE",
        lifecycle_state="FINAL_ACTIONABLE",
    )
    retry = repository.persist_final_signal_bundle(
        decision,
        state,
        delivery_payload="final payload",
        provenance=provenance,
        audit_root_event_id="root-1",
        audit_strategy="LOW_VOLUME_EXTENSION_FAILURE",
        lifecycle_state="FINAL_ACTIONABLE",
    )
    assert retry.id == signal.id
    with database.session() as session:
        assert len(session.scalars(select(SignalModel)).all()) == 1
        assert len(session.scalars(select(SignalProvenanceModel)).all()) == 1
        assert len(session.scalars(select(TelegramDeliveryOutboxModel)).all()) == 1
        linked = session.scalars(
            select(StrategyObservationModel).where(StrategyObservationModel.final_decision == "ACTIONABLE")
        ).one()
        assert linked.signal_id == signal.id
        assert session.scalars(select(StrategyObservationModel).where(StrategyObservationModel.final_decision == "BLOCKED_BY_RECHECK")).one().signal_id is None


def test_canonical_registry_evaluations_are_immutable_and_complete_before_delivery(
    tmp_path, make_event_state, make_features, make_frame, make_signal_decision
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'canonical.sqlite'}")
    database.create_all()
    state = BotRepository(database).upsert_event_state(make_event_state())
    features = make_features(asof=state.updated_at, market_asof=state.updated_at)
    bot = _bot(database)
    canonical = bot._evaluate_registered_strategies(
        state=state,
        features=features,
        frame_1m=make_frame([110.0, 111.0]),
        short_zone=ShortZone(110.0, 113.8, "event_range"),
        decision_timestamp=features.asof,
    )
    before = tuple(
        (
            item.strategy_type, item.strategy_subtype, item.model_version,
            item.actionable, tuple(item.vetoes), tuple(item.blockers),
            dict(item.strategy_metadata),
        )
        for item in canonical
    )
    assert tuple(item.strategy_type for item in canonical) == REQUIRED_STRATEGY_TYPES
    assert {item.strategy_subtype for item in canonical} >= {"BASELINE_PULLBACK"}
    for item in canonical:
        assert item is not None
        if item.actionable:
            projected = bot._decision_from_registered_evaluation(
                replace(make_signal_decision(), strategy_type=item.strategy_type),
                item,
            )
            assert projected.strategy_subtype == item.strategy_subtype
            assert projected.model_version == item.model_version
    after = tuple(
        (
            item.strategy_type, item.strategy_subtype, item.model_version,
            item.actionable, tuple(item.vetoes), tuple(item.blockers),
            dict(item.strategy_metadata),
        )
        for item in canonical
    )
    assert after == before


@pytest.mark.parametrize("identifier", REQUIRED_STRATEGY_TYPES)
def test_each_canonical_registry_identifier_reaches_delivery_without_projection_replacement(
    tmp_path, make_event_state, make_features, make_frame, make_signal_decision,
    make_signal_provenance, monkeypatch, identifier,
) -> None:
    """Persist the exact output of each real adapter, including the climax umbrella."""
    def actionable_legacy_input(_input, _config):
        decision = make_signal_decision(
            strategy_type="BASELINE_PULLBACK",
            strategy_subtype="BASELINE_PULLBACK",
            model_version="baseline-v1",
            actionable=True,
            strategy_metadata={"event_high": 115.0, "strategy_subtype": "BASELINE_PULLBACK"},
        )
        return SimpleNamespace(
            decision=decision, score=decision.score, grade=decision.grade,
            reject_reasons=[], blockers=[], risk_flags=[],
        )

    def actionable_climax(_state, features, _frame, _config, **_kwargs):
        def selected(subtype):
            return ClimaxEvaluation(
                subtype=subtype, score=82, grade="A",
                metadata={
                    "event_high": 115.0, "model_version": "climax-v1",
                    "strategy_subtype": subtype,
                }, veto_reasons=[], data_quality=[],
            )
        return ClimaxEvaluationBundle(
            selected=selected("LOW_VOLUME_EXTENSION_FAILURE"),
            branch_evaluations={
                "VOLUME_CLIMAX_UNWIND": selected("VOLUME_CLIMAX_UNWIND"),
                "LOW_VOLUME_EXTENSION_FAILURE": selected("LOW_VOLUME_EXTENSION_FAILURE"),
            },
        )

    def actionable_trapped(*_args, **_kwargs):
        return SimpleNamespace(
            subtype="TRAPPED_LONGS_REVERSAL", score=82, grade="A",
            actionable=True, metadata={"model_version": "trapped-longs-v1"},
            veto_reasons=[], data_quality=[],
        )

    monkeypatch.setattr("app.strategies.baseline.evaluate_strategy_input", actionable_legacy_input)
    monkeypatch.setattr("app.strategies.climax.evaluate_climax_bundle", actionable_climax)
    monkeypatch.setattr("app.strategies.trapped_longs.evaluate_trapped_longs_reversal", actionable_trapped)
    database = Database(f"sqlite:///{tmp_path / f'canonical-delivery-{identifier}.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    features = make_features(asof=state.updated_at, market_asof=state.updated_at)
    bot = _bot(database)
    evaluations = bot._evaluate_registered_strategies(
        state=state, features=features, frame_1m=make_frame([110.0, 111.0]),
        short_zone=ShortZone(110.0, 113.8, "event_range"),
        decision_timestamp=features.asof,
    )
    evaluation = next(item for item in evaluations if item.strategy_type == identifier)
    assert evaluation.actionable is True
    assert evaluation.strategy_type == identifier
    if identifier == "CLIMAX_EXHAUSTION":
        assert evaluation.strategy_subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    elif identifier in {"VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"}:
        assert evaluation.strategy_subtype == identifier

    before = (
        evaluation.strategy_type, evaluation.strategy_subtype, evaluation.model_version,
        evaluation.actionable, tuple(evaluation.vetoes), dict(evaluation.strategy_metadata),
    )
    projected = bot._decision_from_registered_evaluation(make_signal_decision(), evaluation)
    assert (
        projected.strategy_type, projected.strategy_subtype, projected.model_version,
        projected.actionable, tuple(projected.blockers), dict(projected.strategy_metadata),
    ) == (
        evaluation.strategy_type, evaluation.strategy_subtype, evaluation.model_version,
        evaluation.actionable, tuple(evaluation.blockers), dict(evaluation.strategy_metadata),
    )
    if identifier == "BASELINE_PULLBACK":
        provenance = make_signal_provenance()
    else:
        with database.session() as session:
            row = ClimaxEvaluationModel(
                evaluation_time=features.asof, symbol=state.symbol,
                strategy=("CLIMAX_EXHAUSTION" if identifier != "TRAPPED_LONGS_REVERSAL" else identifier),
                subtype_candidate=evaluation.strategy_subtype,
                model_version=evaluation.model_version, event_id=state.event_id,
            )
            session.add(row)
            session.flush()
            provenance = make_signal_provenance(
                strategy_family=("CLIMAX_EXHAUSTION" if identifier != "TRAPPED_LONGS_REVERSAL" else identifier),
                strategy_branch=evaluation.strategy_subtype,
                root_event_id="root-1", decision_evaluation_id=row.id,
                admission_evaluation_id=row.id,
            )
    saved = repository.persist_final_signal_bundle(
        projected, state, delivery_payload=f"canonical:{identifier}", provenance=provenance,
    )
    with database.session() as session:
        signal = session.scalars(select(SignalModel)).one()
        assert signal.strategy_type == evaluation.strategy_type
        assert signal.strategy_subtype == evaluation.strategy_subtype
        assert signal.model_version == evaluation.model_version
        assert session.scalars(select(TelegramDeliveryOutboxModel)).one().entity_id == signal.id
    assert before == (
        evaluation.strategy_type, evaluation.strategy_subtype, evaluation.model_version,
        evaluation.actionable, tuple(evaluation.vetoes), dict(evaluation.strategy_metadata),
    )


@pytest.mark.parametrize(
    ("snapshot_kind", "final_actionable", "expected_reason"),
    [
        ("missing", False, "final_recheck_data_missing"),
        ("malformed", False, "microstructure_break_missing"),
        ("empty-frame", False, "final_recheck_data_missing"),
        ("blocked", False, "microstructure_break_missing"),
        ("pass", True, "final_actionable"),
    ],
)
def test_runtime_final_recheck_delivery_seam_is_fail_closed_and_audited(
    tmp_path, make_event_state, make_features, make_frame, monkeypatch,
    snapshot_kind, final_actionable, expected_reason,
) -> None:
    async def run() -> tuple[int, int, int, int, int, list[tuple]]:
        database = Database(f"sqlite:///{tmp_path / f'recheck-{snapshot_kind}.sqlite'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        initial_features = make_features(asof=state.updated_at, market_asof=state.updated_at)
        fresh_features = make_features(
            asof=state.updated_at + timedelta(seconds=2),
            market_asof=state.updated_at + timedelta(seconds=2),
        )
        scanner = _NoExchangeScanner()
        notifier = _ExplodingNotifier() if not final_actionable else _RecordingNotifier()
        bot = _bot(database, scanner=scanner, notifier=notifier)
        canonical_calls = 0

        def canonical_bundle(_state, fresh, _frame, _config, **_kwargs):
            nonlocal canonical_calls
            canonical_calls += 1
            actionable = (fresh.asof == state.updated_at) or (final_actionable and fresh.liquidity_available)
            reason = [] if actionable else ["microstructure_break_missing"]
            selected = ClimaxEvaluation(
                subtype="LOW_VOLUME_EXTENSION_FAILURE", score=82, grade="A",
                metadata={
                    "event_high": state.event_high, "model_version": "climax-v1",
                    "strategy_subtype": "LOW_VOLUME_EXTENSION_FAILURE",
                    "entry_distance_below_high_pct": (state.event_high - fresh.price) / state.event_high * 100,
                }, veto_reasons=reason, data_quality=[],
            )
            return ClimaxEvaluationBundle(
                selected=selected, branch_evaluations={"LOW_VOLUME_EXTENSION_FAILURE": selected},
            )

        def divergent_legacy_bundle(_state, _fresh, _frame, _config, **_kwargs):
            selected = ClimaxEvaluation(
                subtype="LOW_VOLUME_EXTENSION_FAILURE", score=0, grade="C",
                metadata={"model_version": "climax-v1", "strategy_subtype": "LOW_VOLUME_EXTENSION_FAILURE"},
                veto_reasons=["legacy_evaluator_conflict"], data_quality=[],
            )
            return ClimaxEvaluationBundle(
                selected=selected, branch_evaluations={"LOW_VOLUME_EXTENSION_FAILURE": selected},
            )

        monkeypatch.setattr("app.strategies.climax.evaluate_climax_bundle", canonical_bundle)
        monkeypatch.setattr("app.main.evaluate_climax_bundle", divergent_legacy_bundle, raising=False)
        monkeypatch.setattr(
            "app.strategies.trapped_longs.evaluate_trapped_longs_reversal",
            lambda *_args, **_kwargs: SimpleNamespace(
                subtype="TRAPPED_LONGS_REVERSAL", score=0, grade="C", actionable=False,
                metadata={"model_version": "trapped-longs-v1"}, veto_reasons=[], data_quality=[],
            ),
        )
        initial_features = make_features(asof=state.updated_at, market_asof=state.updated_at)
        async def capture(_symbol: str):
            if snapshot_kind == "missing":
                return None
            frame = make_frame([110.0, 111.0]) if snapshot_kind != "empty-frame" else pd.DataFrame()
            liquidity = {} if snapshot_kind == "malformed" else {
                "available": True, "spread_pct": 0.05, "slippage_pct": 0.05,
                "orderbook_depth_usdt_1pct": 100000.0, "orderbook_depth_usdt_2pct": 200000.0,
            }
            return SimpleNamespace(
                symbol="ONTUSDT", frame_1m=SimpleNamespace(frame=frame),
                derivatives={}, liquidity=liquidity,
            )
        bot._market_data_provider.capture_decision_snapshot = capture
        await bot._evaluate_and_send_climax("ONTUSDT", make_frame([110.0, 111.0]), state, features=initial_features)
        with database.session() as session:
            audits = session.scalars(select(StrategyObservationModel).where(StrategyObservationModel.evaluation_phase == "PRE_DELIVERY_RECHECK")).all()
            return (
                len(session.scalars(select(SignalModel)).all()),
                len(session.scalars(select(TelegramDeliveryOutboxModel)).all()),
                notifier.calls,
                scanner.client.order_calls,
                canonical_calls,
                [(row.initial_evaluation_id, row.initial_decision, row.final_decision, row.final_reason, row.signal_id) for row in audits],
            )

    signal_count, outbox_count, notifier_calls, order_calls, canonical_calls, audits = asyncio.run(run())
    assert signal_count == (1 if final_actionable else 0)
    assert outbox_count == (1 if final_actionable else 0)
    assert notifier_calls == (1 if final_actionable else 0)
    assert order_calls == 0
    assert canonical_calls > 0
    assert len(audits) == 1
    initial_id, initial_decision, final_decision, reason, signal_id = audits[0]
    assert initial_id is not None
    assert initial_decision == "ACTIONABLE"
    assert final_decision == ("ACTIONABLE" if final_actionable else "BLOCKED_BY_RECHECK")
    assert reason == expected_reason
    assert (signal_id is not None) is final_actionable


def test_runtime_is_explicitly_manual_only_and_has_no_order_boundary_calls() -> None:
    config = require_manual_only(AppConfig(autoexecution="OFF"))
    assert config.autoexecution == "OFF"
    with pytest.raises(ManualOnlyConfigurationError, match="AUTOEXECUTION"):
        require_manual_only(AppConfig(autoexecution="ON"))

    boundary_client = _NoExchangeClient()
    for method in (boundary_client.create_order, boundary_client.place_order, boundary_client.cancel_order):
        with pytest.raises(AssertionError, match="manual-only runtime must not place orders"):
            asyncio.run(method("BTCUSDT", "Sell", 1))
    assert boundary_client.order_calls == 3

    runtime_client = _NoExchangeClient()

    async def run_manual_only_runtime() -> None:
        validated_config = require_manual_only(config)
        if validated_config.autoexecution != "OFF":
            await runtime_client.create_order("BTCUSDT", "Sell", 1)

    asyncio.run(run_manual_only_runtime())
    assert runtime_client.order_calls == 0


def test_restart_concurrent_retry_repairs_partial_outbox_without_duplicates(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'retry.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(strategy_type="BASELINE_PULLBACK", strategy_subtype="BASELINE_PULLBACK", model_version="baseline-v1")
    provenance = make_signal_provenance()
    first = repository.persist_final_signal_bundle(decision, state, delivery_payload="retry", provenance=provenance)
    with database.session() as session:
        session.query(TelegramDeliveryOutboxModel).delete()
    def retry():
        return BotRepository(database).persist_final_signal_bundle(
            decision, state, delivery_payload="retry", provenance=provenance
        ).id
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(lambda _index: retry(), range(2))) == [first.id, first.id]
    with database.session() as session:
        assert len(session.scalars(select(SignalModel)).all()) == 1
        assert len(session.scalars(select(SignalProvenanceModel)).all()) == 1
        assert len(session.scalars(select(TelegramDeliveryOutboxModel)).all()) == 1
