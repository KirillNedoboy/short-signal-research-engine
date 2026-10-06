from __future__ import annotations

from datetime import timedelta

from sqlalchemy import select
import pytest

from app.storage.models import EventStateModel, SignalModel, StrategyObservationModel
from app.storage.repository import BotRepository, EventStateConflictError
from app.storage.db import Database


def _repo(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'compat.db'}")
    database.create_all()
    return database, BotRepository(database)


def _audit(state, **overrides):
    values = dict(
        observation_id="audit-1", idempotency_key="audit-key", run_id="run-1",
        runtime_instance_id="runtime-1", runtime_started_at=state.updated_at,
        code_version="test", strategy_family="CLIMAX_EXHAUSTION",
        strategy="VOLUME_CLIMAX_UNWIND", evaluation_phase="PRE_DELIVERY_RECHECK",
        observed_at=state.updated_at, market_asof=state.updated_at,
        symbol=state.symbol, event_id=state.event_id, root_event_id="root-1",
        event_revision=1, evaluation_id=1, signal_id=None,
        exchange_time=None,
        live_decision="ACTIONABLE", shadow_decision="NOT_EVALUATED", score=1,
        blockers_json=[], warnings_json=[], market_price=1.0, event_high=2.0,
        model_version="climax-v1", config_hash="a" * 64, input_fingerprint="b" * 64,
        input_snapshot_json={}, initial_evaluation_id=1, initial_decision="ACTIONABLE",
        final_decision="ACTIONABLE", final_reason="final_actionable",
        finalized_at=state.updated_at,
    )
    values.update(overrides)
    return StrategyObservationModel(**values)


def test_final_actionable_requires_and_links_exact_recheck_audit(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with database.session() as session:
        session.add(_audit(state))
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND",
        model_version="climax-v1",
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION", strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1", decision_evaluation_id=1, admission_evaluation_id=1
    )
    record = repository.persist_final_signal_bundle(
        decision, state, delivery_payload="payload", provenance=provenance,
        lifecycle_state="FINAL_ACTIONABLE", audit_root_event_id="root-1",
        audit_strategy="VOLUME_CLIMAX_UNWIND",
    )
    with database.session() as session:
        audit = session.scalars(select(StrategyObservationModel)).one()
        event = session.get(EventStateModel, state.symbol)
        assert audit.signal_id == record.id
        assert event.lifecycle_state == "FINAL_ACTIONABLE"


def test_final_actionable_ignores_same_root_rows_for_other_symbol_or_evaluation(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with database.session() as session:
        session.add_all([
            _audit(state, observation_id="wrong-symbol", idempotency_key="wrong-symbol",
                   symbol="BTCUSDT", evaluation_id=1, observed_at=state.updated_at),
            _audit(state, observation_id="wrong-evaluation", idempotency_key="wrong-evaluation",
                   evaluation_id=2, observed_at=state.updated_at),
            _audit(state, observation_id="exact", idempotency_key="exact", evaluation_id=1,
                   observed_at=state.updated_at + timedelta(microseconds=1)),
        ])
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND",
        model_version="climax-v1",
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION", strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1", decision_evaluation_id=1, admission_evaluation_id=1,
    )
    record = repository.persist_final_signal_bundle(
        decision, state, delivery_payload="payload", provenance=provenance,
        lifecycle_state="FINAL_ACTIONABLE", audit_root_event_id="root-1",
        audit_strategy="VOLUME_CLIMAX_UNWIND",
    )
    with database.session() as session:
        linked = session.scalar(select(StrategyObservationModel).where(
            StrategyObservationModel.signal_id == record.id
        ))
        assert linked is not None
        assert linked.observation_id == "exact"


def test_final_actionable_audit_mismatch_rolls_back_before_signal_writes(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with database.session() as session:
        session.add(_audit(state, evaluation_id=2))
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND",
        model_version="climax-v1",
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION", strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1", decision_evaluation_id=1, admission_evaluation_id=1,
    )
    with pytest.raises(ValueError, match="final decision audit row not found"):
        repository.persist_final_signal_bundle(
            decision, state, delivery_payload="payload", provenance=provenance,
            lifecycle_state="FINAL_ACTIONABLE", audit_root_event_id="root-1",
            audit_strategy="VOLUME_CLIMAX_UNWIND",
        )
    with database.session() as session:
        assert session.scalar(select(SignalModel).where(SignalModel.symbol == decision.symbol)) is None
        assert session.get(EventStateModel, state.symbol).signal_id is None
        assert session.scalar(select(StrategyObservationModel).where(
            StrategyObservationModel.observation_id == "audit-1"
        )).signal_id is None


def test_event_rollover_cannot_retain_prior_signal_claim(tmp_path, make_event_state):
    database, repository = _repo(tmp_path)
    state = make_event_state(lifecycle_state="FINAL_ACTIONABLE", signal_id=42)
    claimed = repository.upsert_event_state(state)
    with pytest.raises(EventStateConflictError, match="event state conflict"):
        repository.upsert_event_state(make_event_state(
            event_id="stale-event", lifecycle_state=None, signal_id=None,
            updated_at=state.updated_at,
        ))
    persisted = repository.get_event_state(state.symbol)
    assert persisted.event_id == claimed.event_id
    assert persisted.signal_id == 42
    assert persisted.lifecycle_state == "FINAL_ACTIONABLE"


def test_compatibility_save_revalidates_event_owner_in_write_transaction(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance,
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    first = repository.save_signal(
        make_signal_decision(), state, delivery_payload="first",
        provenance=make_signal_provenance(),
    )
    compatibility_state = repository.get_event_state(state.symbol)
    original_persist = repository.persist_final_signal_bundle

    def mutate_owner_then_persist(*args, **kwargs):
        with database.session() as session:
            session.get(EventStateModel, state.symbol).signal_id = first.id + 100
        return original_persist(*args, **kwargs)

    repository.persist_final_signal_bundle = mutate_owner_then_persist
    with pytest.raises(EventStateConflictError, match="event state conflict"):
        repository.save_signal(
            make_signal_decision(
                strategy_type="BASELINE_PULLBACK", model_version="baseline-v2"
            ),
            compatibility_state,
            delivery_payload="second",
            provenance=make_signal_provenance(),
        )
    with database.session() as session:
        assert session.scalar(select(SignalModel).where(SignalModel.event_id == state.event_id).order_by(SignalModel.id.desc())).id == first.id


def test_competing_save_signal_rolls_back_before_creating_chain(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    first = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND"
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION", strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1", decision_evaluation_id=1, admission_evaluation_id=1
    )
    repository.save_signal(first, state, delivery_payload="one", provenance=provenance)
    second = make_signal_decision(
        event_id="other-event", strategy_type="CLIMAX_EXHAUSTION",
        strategy_subtype="VOLUME_CLIMAX_UNWIND"
    )
    with pytest.raises(EventStateConflictError):
        repository.save_signal(
            second, state, delivery_payload="two",
            provenance=make_signal_provenance(
                event_id="other-event", strategy_family="CLIMAX_EXHAUSTION",
                strategy_branch="VOLUME_CLIMAX_UNWIND"
            ),
        )
    with database.session() as session:
        assert session.scalar(select(SignalModel).where(SignalModel.event_id == "other-event")) is None


def test_final_actionable_rejects_competing_event_and_strategy_rows(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with database.session() as session:
        session.add_all([
            _audit(state, observation_id="wrong-event", idempotency_key="wrong-event", event_id="other-event"),
            _audit(state, observation_id="wrong-branch", idempotency_key="wrong-branch", strategy="LOW_VOLUME_EXTENSION_FAILURE"),
            _audit(state, observation_id="exact", idempotency_key="exact"),
        ])
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION", strategy_subtype="VOLUME_CLIMAX_UNWIND",
        model_version="climax-v1",
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION", strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1", decision_evaluation_id=1, admission_evaluation_id=1,
    )
    record = repository.persist_final_signal_bundle(
        decision, state, delivery_payload="payload", provenance=provenance,
        lifecycle_state="FINAL_ACTIONABLE", audit_root_event_id="root-1",
        audit_strategy="VOLUME_CLIMAX_UNWIND",
    )
    with database.session() as session:
        linked = session.scalar(select(StrategyObservationModel).where(
            StrategyObservationModel.signal_id == record.id
        ))
        assert linked is not None
        assert linked.observation_id == "exact"
