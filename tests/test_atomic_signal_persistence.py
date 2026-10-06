from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.domain import EventStatus
from app.storage.db import Database
from app.storage.models import (
    EventStateModel,
    SignalModel,
    SignalProvenanceModel,
    StrategyObservationModel,
    TelegramDeliveryOutboxModel,
)
from app.storage.repository import BotRepository, EventStateConflictError


def _repo(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'atomic-signal.db'}")
    database.create_all()
    return database, BotRepository(database)


def test_final_signal_bundle_commits_signal_chain_and_event_state_atomically(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())

    record = repository.persist_final_signal_bundle(
        make_signal_decision(),
        state,
        delivery_payload="final payload",
        provenance=make_signal_provenance(),
    )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        outbox = session.scalars(select(TelegramDeliveryOutboxModel)).one()
        event = session.get(EventStateModel, state.symbol)
        assert outbox.entity_id == record.id
        assert outbox.status == "PENDING"
        assert event.signal_id == record.id
        assert event.state == EventStatus.SIGNAL_SENT.value


def test_final_signal_bundle_rolls_back_on_attach_or_provenance_failure(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    bad_provenance = make_signal_provenance(event_id="different-event")

    with pytest.raises(ValueError, match="provenance event_id"):
        repository.persist_final_signal_bundle(
            make_signal_decision(),
            state,
            delivery_payload="final payload",
            provenance=bad_provenance,
        )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 0
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0
        event = session.get(EventStateModel, state.symbol)
        assert event.signal_id is None
        assert event.state != EventStatus.SIGNAL_SENT.value


def test_concurrent_final_signal_bundle_is_idempotent(tmp_path, make_event_state, make_signal_decision, make_signal_provenance):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision()
    provenance = make_signal_provenance()

    def persist():
        return repository.persist_final_signal_bundle(
            decision,
            state,
            delivery_payload="final payload",
            provenance=provenance,
        ).id

    with ThreadPoolExecutor(max_workers=2) as executor:
        ids = list(executor.map(lambda _: persist(), range(2)))

    assert ids[0] == ids[1]
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1
        event = session.get(EventStateModel, state.symbol)
        assert event.signal_id == ids[0]
        assert event.state == EventStatus.SIGNAL_SENT.value


def test_distinct_signal_identities_cannot_claim_one_event_state(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    first = make_signal_decision()
    second = make_signal_decision(event_id="ONTUSDT:15m:1:222", score=81)

    repository.persist_final_signal_bundle(
        first,
        state,
        delivery_payload="first payload",
        provenance=make_signal_provenance(),
    )
    with pytest.raises(EventStateConflictError, match="event state conflict"):
        repository.persist_final_signal_bundle(
            second,
            state,
            delivery_payload="second payload",
            provenance=make_signal_provenance(event_id=second.event_id),
        )

    with database.session() as session:
        event = session.get(EventStateModel, state.symbol)
        assert event.signal_id == 1
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 2



def test_provenance_conflict_fails_closed_on_retry(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    repository.persist_final_signal_bundle(
        make_signal_decision(), state, delivery_payload="final payload",
        provenance=make_signal_provenance(),
    )
    with pytest.raises(ValueError, match="conflicting provenance"):
        repository.persist_final_signal_bundle(
            make_signal_decision(), state, delivery_payload="final payload",
            provenance=make_signal_provenance(code_version="different-version"),
        )


def test_missing_delivery_payload_is_controlled_failure(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with pytest.raises(ValueError, match="delivery payload"):
        repository.persist_final_signal_bundle(
            make_signal_decision(), state, delivery_payload=None,
            provenance=make_signal_provenance(),
        )
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0


def test_save_signal_missing_delivery_payload_writes_nothing(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())

    with pytest.raises(ValueError, match="delivery payload"):
        repository.save_signal(
            make_signal_decision(), state, delivery_payload=None,
            provenance=make_signal_provenance(),
        )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 0
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0


def test_save_signal_none_model_version_adopts_historical_null_chain(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    historical = repository.save_signal(
        make_signal_decision(model_version="historical-v0"),
        state,
        delivery_payload="legacy payload",
        provenance=make_signal_provenance(),
    )
    original_decision = make_signal_decision(model_version=None)
    original_context = {"original_model_version": "historical-v0"}
    with database.session() as session:
        model = session.get(SignalModel, historical.id)
        model.model_version = None
        model.signal_identity = None
        model.context_json = original_context

    adopted = repository.save_signal(
        original_decision,
        state,
        delivery_payload="legacy payload",
        provenance=make_signal_provenance(),
    )
    retried = repository.save_signal(
        original_decision,
        state,
        delivery_payload="legacy payload",
        provenance=make_signal_provenance(),
    )

    assert adopted.id == historical.id == retried.id
    assert original_decision.model_version is None
    assert "original_model_version" not in original_decision.strategy_metadata
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1
        model = session.get(SignalModel, historical.id)
        assert model.model_version == "LEGACY_MODEL_VERSION"
        assert model.context_json == original_context
        event = session.get(EventStateModel, state.symbol)
        assert event is not None
        assert event.signal_id == historical.id


def test_save_signal_none_model_version_adopts_linked_nonbaseline_historical_null_chain(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    strategy = {
        "strategy_type": "CLIMAX_EXHAUSTION",
        "strategy_subtype": "VOLUME_CLIMAX_UNWIND",
    }
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION",
        strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1",
        decision_evaluation_id=1,
        admission_evaluation_id=1,
    )
    historical = repository.save_signal(
        make_signal_decision(**strategy, model_version="historical-v0"),
        state,
        delivery_payload="legacy payload",
        provenance=provenance,
    )
    with database.session() as session:
        model = session.get(SignalModel, historical.id)
        model.model_version = None
        model.signal_identity = None

    adopted = repository.save_signal(
        make_signal_decision(**strategy, model_version=None),
        state,
        delivery_payload="legacy payload",
        provenance=provenance,
    )

    assert adopted.id == historical.id
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1
        model = session.get(SignalModel, historical.id)
        event = session.get(EventStateModel, state.symbol)
        assert model.model_version == "LEGACY_MODEL_VERSION"
        assert event.signal_id == historical.id
        assert event.lifecycle_state == "OUTBOX_ENQUEUED"


def test_save_signal_none_model_version_fails_closed_on_ambiguous_null_chain(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    for model_version in ("historical-v0", "historical-v1"):
        repository.save_signal(
            make_signal_decision(model_version=model_version),
            state,
            delivery_payload=f"legacy payload {model_version}",
            provenance=make_signal_provenance(),
        )
    with database.session() as session:
        for model in session.scalars(select(SignalModel).order_by(SignalModel.id)).all():
            model.model_version = None
            model.signal_identity = None

    with pytest.raises(ValueError, match="ambiguous legacy signal"):
        repository.save_signal(
            make_signal_decision(model_version=None),
            state,
            delivery_payload="legacy payload historical-v0",
            provenance=make_signal_provenance(),
        )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 2
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 2
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 2


def test_final_actionable_requires_audit_identifiers_without_writes(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())

    with pytest.raises(ValueError, match="audit root event and strategy"):
        repository.persist_final_signal_bundle(
            make_signal_decision(), state, delivery_payload="final payload",
            provenance=make_signal_provenance(), lifecycle_state="FINAL_ACTIONABLE",
        )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 0
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0


def test_retry_repairs_missing_outbox_after_interruption(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    record = repository.persist_final_signal_bundle(
        make_signal_decision(), state, delivery_payload="final payload",
        provenance=make_signal_provenance(),
    )
    with database.session() as session:
        session.delete(session.scalars(select(TelegramDeliveryOutboxModel)).one())
    repaired = repository.persist_final_signal_bundle(
        make_signal_decision(), state, delivery_payload="final payload",
        provenance=make_signal_provenance(),
    )
    assert repaired.id == record.id
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1


def test_invalid_lifecycle_state_rolls_back_and_known_state_is_durable(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with pytest.raises(ValueError, match="lifecycle state"):
        repository.persist_final_signal_bundle(
            make_signal_decision(), state, delivery_payload="final payload",
            provenance=make_signal_provenance(), lifecycle_state="NOT_A_STATE",
        )
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
    repository.persist_final_signal_bundle(
        make_signal_decision(), state, delivery_payload="final payload",
        provenance=make_signal_provenance(), lifecycle_state="SENT",
    )
    with database.session() as session:
        event = session.get(EventStateModel, state.symbol)
        assert event.notes == "SENT"
        assert event.lifecycle_state == "SENT"
        assert event.state == EventStatus.SIGNAL_SENT.value


def test_existing_signal_conflicting_decision_fails_closed(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    repository.persist_final_signal_bundle(
        make_signal_decision(), state, delivery_payload="final payload",
        provenance=make_signal_provenance(),
    )
    with pytest.raises(ValueError, match="conflicting persisted signal"):
        repository.persist_final_signal_bundle(
            make_signal_decision(score=81), state, delivery_payload="final payload",
            provenance=make_signal_provenance(),
        )



def test_missing_final_audit_link_rolls_back_entire_bundle(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    with pytest.raises(ValueError, match="final decision audit row not found"):
        repository.persist_final_signal_bundle(
            make_signal_decision(), state, delivery_payload="final payload",
            provenance=make_signal_provenance(), audit_root_event_id="missing-root",
            audit_strategy="BASELINE_PULLBACK",
        )
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 0
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0
        event = session.get(EventStateModel, state.symbol)
        assert event.signal_id is None
        assert event.state != EventStatus.SIGNAL_SENT.value


@pytest.mark.parametrize("boundary", ["signal", "provenance", "outbox", "event", "audit"])
def test_final_signal_bundle_failure_at_each_write_boundary_is_atomic(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance, monkeypatch, boundary
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION" if boundary == "audit" else "BASELINE_PULLBACK",
        strategy_subtype="VOLUME_CLIMAX_UNWIND" if boundary == "audit" else None,
        model_version="climax-v1" if boundary == "audit" else "baseline-v2",
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION" if boundary == "audit" else "BASELINE_PULLBACK",
        strategy_branch="VOLUME_CLIMAX_UNWIND" if boundary == "audit" else "BASELINE_PULLBACK",
        root_event_id="root-1" if boundary == "audit" else None,
        decision_evaluation_id=1 if boundary == "audit" else None,
    )
    audit_kwargs = {}
    if boundary == "audit":
        from tests.test_compatibility_semantics import _audit
        with database.session() as session:
            session.add(_audit(state))
        audit_kwargs = {"lifecycle_state": "FINAL_ACTIONABLE", "audit_root_event_id": "root-1", "audit_strategy": "VOLUME_CLIMAX_UNWIND"}

    original_flush = Session.flush
    original_execute = Session.execute

    def flush(session, *args, **kwargs):
        new_types = {type(item) for item in session.new}
        dirty_audit = any(
            type(item) is StrategyObservationModel and item.signal_id is not None
            for item in session.dirty
        )
        result = original_flush(session, *args, **kwargs)
        if (
            (boundary == "signal" and SignalModel in new_types)
            or (boundary == "provenance" and SignalProvenanceModel in new_types)
            or (boundary == "outbox" and TelegramDeliveryOutboxModel in new_types)
            or (boundary == "audit" and dirty_audit)
        ):
            raise RuntimeError(f"injected failure after {boundary}")
        return result

    def execute(session, statement, *args, **kwargs):
        result = original_execute(session, statement, *args, **kwargs)
        if boundary == "event" and "event_states" in str(statement).lower() and str(statement).lstrip().upper().startswith("UPDATE"):
            raise RuntimeError("injected failure after event")
        return result

    monkeypatch.setattr(Session, "flush", flush)
    monkeypatch.setattr(Session, "execute", execute)
    with pytest.raises(RuntimeError, match="injected failure"):
        repository.persist_final_signal_bundle(
            decision, state, delivery_payload="final payload", provenance=provenance, **audit_kwargs
        )

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 0
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 0
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 0
        event = session.get(EventStateModel, state.symbol)
        assert event.signal_id is None
        assert event.state == EventStatus.PUMP_DETECTED.value
        if boundary == "audit":
            assert session.scalars(select(StrategyObservationModel)).one().signal_id is None


@pytest.mark.parametrize(
    "boundary",
    ["before_signal", "after_signal", "after_provenance", "after_outbox", "after_event", "after_audit", "before_commit"],
)
def test_final_signal_bundle_retries_after_restart_at_every_crash_boundary(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance, monkeypatch, boundary
):
    """A crashed writer can restart and repair one complete deterministic chain."""
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(
        strategy_type="CLIMAX_EXHAUSTION",
        strategy_subtype="VOLUME_CLIMAX_UNWIND",
        model_version="climax-v1",
    )
    provenance = make_signal_provenance(
        strategy_family="CLIMAX_EXHAUSTION",
        strategy_branch="VOLUME_CLIMAX_UNWIND",
        root_event_id="root-1",
        decision_evaluation_id=1,
    )
    from tests.test_compatibility_semantics import _audit

    with database.session() as session:
        session.add(_audit(state))
    audit_kwargs = {
        "lifecycle_state": "FINAL_ACTIONABLE",
        "audit_root_event_id": "root-1",
        "audit_strategy": "VOLUME_CLIMAX_UNWIND",
    }

    failpoint_hits = []
    _install_crash_boundary_failpoint(monkeypatch, boundary, failpoint_hits)
    with pytest.raises(RuntimeError, match=f"crash boundary: {boundary}"):
        repository.persist_final_signal_bundle(
            decision,
            state,
            delivery_payload="final payload",
            provenance=provenance,
            **audit_kwargs,
        )
    assert failpoint_hits == [boundary]

    # Simulate process death: discard both the old engine and repository.
    database.engine.dispose()
    restarted_database = Database(database.db_url)
    restarted_repository = BotRepository(restarted_database)
    restarted_state = restarted_repository.get_event_state(state.symbol)
    assert restarted_state is not None
    result = restarted_repository.persist_final_signal_bundle(
        decision,
        restarted_state,
        delivery_payload="final payload",
        provenance=provenance,
        **audit_kwargs,
    )

    with restarted_database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1
        event = session.get(EventStateModel, state.symbol)
        assert event is not None
        assert event.signal_id == result.id
        assert event.state == EventStatus.SIGNAL_SENT.value
        audits = session.scalars(select(StrategyObservationModel)).all()
        assert len(audits) == 1
        assert audits[0].signal_id == result.id


def _install_crash_boundary_failpoint(monkeypatch, boundary, hits):
    """Install a one-shot crash point without adding hooks to production code."""
    original_flush = Session.flush
    original_execute = Session.execute
    original_commit = Session.commit
    fired = False

    def fire():
        nonlocal fired
        if not fired:
            fired = True
            hits.append(boundary)
            raise RuntimeError(f"crash boundary: {boundary}")

    def flush(session, *args, **kwargs):
        new_types = {type(item) for item in session.new}
        dirty_audit = any(
            type(item) is StrategyObservationModel and item.signal_id is not None
            for item in session.dirty
        )
        if boundary == "before_signal" and SignalModel in new_types:
            fire()
        result = original_flush(session, *args, **kwargs)
        if (
            (boundary == "after_signal" and SignalModel in new_types)
            or (boundary == "after_provenance" and SignalProvenanceModel in new_types)
            or (boundary == "after_outbox" and TelegramDeliveryOutboxModel in new_types)
            or (boundary == "after_audit" and dirty_audit)
        ):
            fire()
        return result

    def execute(session, statement, *args, **kwargs):
        result = original_execute(session, statement, *args, **kwargs)
        if boundary == "after_event" and "event_states" in str(statement).lower() and str(statement).lstrip().upper().startswith("UPDATE"):
            fire()
        return result

    def commit(session, *args, **kwargs):
        if boundary == "before_commit":
            fire()
        return original_commit(session, *args, **kwargs)

    monkeypatch.setattr(Session, "flush", flush)
    monkeypatch.setattr(Session, "execute", execute)
    monkeypatch.setattr(Session, "commit", commit)


def test_save_signal_retry_returns_canonical_signal_and_one_complete_chain(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision()
    provenance = make_signal_provenance()
    first = repository.save_signal(decision, state, provenance=provenance, delivery_payload="payload")
    second = repository.save_signal(decision, state, provenance=provenance, delivery_payload="payload")
    assert second.id == first.id
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1
        assert session.scalar(select(func.count()).select_from(EventStateModel).where(EventStateModel.signal_id == first.id)) == 1


def test_save_signal_concurrent_identical_calls_share_one_chain(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision()
    provenance = make_signal_provenance()

    def save():
        return repository.save_signal(
            decision, state, provenance=provenance, delivery_payload="payload"
        ).id

    with ThreadPoolExecutor(max_workers=2) as executor:
        ids = list(executor.map(lambda _: save(), range(2)))

    assert ids[0] == ids[1]
    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1


def test_save_signal_omitted_payload_keeps_legacy_marker_and_none_remains_invalid(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision()
    provenance = make_signal_provenance()
    repository.save_signal(decision, state, provenance=provenance)
    with pytest.raises(ValueError, match="delivery payload"):
        repository.save_signal(decision, state, provenance=provenance, delivery_payload=None)
    with database.session() as session:
        assert session.scalars(select(TelegramDeliveryOutboxModel)).one().payload == "LEGACY_SIGNAL_PAYLOAD"
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1


def test_save_signal_none_model_version_uses_atomic_legacy_bundle(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
):
    database, repository = _repo(tmp_path)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(model_version=None)
    provenance = make_signal_provenance()

    first = repository.save_signal(decision, state, provenance=provenance)
    second = repository.save_signal(decision, state, provenance=provenance)

    assert first.id == second.id
    assert decision.model_version is None
    with database.session() as session:
        signal = session.get(SignalModel, first.id)
        assert signal is not None
        assert signal.model_version == "LEGACY_MODEL_VERSION"
        assert signal.context_json["model_version"] == "LEGACY_MODEL_VERSION"
        assert signal.context_json["original_model_version"] is None
        assert session.scalar(select(func.count()).select_from(SignalModel)) == 1
        assert session.scalar(select(func.count()).select_from(SignalProvenanceModel)) == 1
        assert session.scalar(select(func.count()).select_from(TelegramDeliveryOutboxModel)) == 1
        outbox = session.scalars(select(TelegramDeliveryOutboxModel)).one()
        assert outbox.payload == "LEGACY_SIGNAL_PAYLOAD"
        event = session.get(EventStateModel, state.symbol)
        assert event.lifecycle_state == "OUTBOX_ENQUEUED"
