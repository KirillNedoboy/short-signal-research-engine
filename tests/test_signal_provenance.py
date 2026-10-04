from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.domain import SignalProvenanceInput
from app.storage.db import Database
from app.storage.models import ClimaxEvaluationModel, SignalProvenanceModel, TelegramDeliveryOutboxModel
from app.storage.repository import BotRepository


def _baseline_provenance(*, when: datetime | None = None) -> SignalProvenanceInput:
    decision_at = when or datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
    return SignalProvenanceInput(
        strategy_family="BASELINE_PULLBACK",
        strategy_branch="BASELINE_PULLBACK",
        event_id="ONTUSDT:15m:1:111",
        root_event_id=None,
        decision_evaluation_id=None,
        admission_evaluation_id=None,
        code_version="test-code-version",
        config_hash="a" * 64,
        runtime_instance_id="runtime-1",
        runtime_started_at=decision_at - timedelta(minutes=5),
        decision_at=decision_at,
    )


def test_save_signal_persists_complete_baseline_provenance_atomically(tmp_path, make_event_state, make_signal_decision) -> None:
    database = Database(f"sqlite:///{tmp_path / 'provenance.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())

    signal = repository.save_signal(
        make_signal_decision(),
        state,
        telegram_sent=False,
        delivery_payload="immutable payload",
        provenance=_baseline_provenance(),
    )

    with database.session() as session:
        provenance = session.get(SignalProvenanceModel, signal.id)
        outbox = session.scalars(select(TelegramDeliveryOutboxModel)).one()

    assert provenance is not None
    assert provenance.signal_id == signal.id
    assert provenance.strategy_family == "BASELINE_PULLBACK"
    assert provenance.strategy_branch == "BASELINE_PULLBACK"
    assert provenance.event_id == "ONTUSDT:15m:1:111"
    assert provenance.root_event_id is None
    assert provenance.decision_evaluation_id is None
    assert provenance.admission_evaluation_id is None
    assert provenance.code_version == "test-code-version"
    assert provenance.config_hash == "a" * 64
    assert provenance.runtime_instance_id == "runtime-1"
    assert provenance.runtime_started_at == datetime(2026, 8, 4, 11, 55)
    assert provenance.decision_at == datetime(2026, 8, 4, 12, 0)
    assert provenance.signal_created_at is not None
    assert outbox.entity_type == "SIGNAL"
    assert outbox.entity_id == signal.id


def test_signal_serialization_uses_decision_time_event_high_snapshot(tmp_path, make_event_state, make_signal_decision) -> None:
    database = Database(f"sqlite:///{tmp_path / 'decision-snapshot.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state(event_high=115.0))
    provenance = replace(_baseline_provenance(), decision_entry_price=112.0, decision_event_high=114.0, decision_distance_from_high=((114.0 - 112.0) / 114.0) * 100)

    signal = repository.save_signal(make_signal_decision(), state, telegram_sent=False, provenance=provenance)

    with database.session() as session:
        stored = session.get(__import__("app.storage.models", fromlist=["SignalModel"]).SignalModel, signal.id)
    assert stored.event_high == 114.0


def test_signal_provenance_is_one_row_per_signal(tmp_path, make_event_state, make_signal_decision) -> None:
    database = Database(f"sqlite:///{tmp_path / 'provenance-unique.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    signal = repository.save_signal(
        make_signal_decision(),
        state,
        telegram_sent=False,
        provenance=_baseline_provenance(),
    )

    with database.engine.connect() as connection:
        assert connection.exec_driver_sql(
            "select count(*) from signal_provenance where signal_id = ?", (signal.id,)
        ).scalar_one() == 1
        assert connection.exec_driver_sql("pragma integrity_check").scalar_one() == "ok"
        assert connection.exec_driver_sql("pragma foreign_key_check").all() == []


@pytest.mark.parametrize("branch", ["VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"])
def test_climax_provenance_uses_exact_branch_and_immutable_evaluation_refs(
    tmp_path,
    make_event_state,
    make_signal_decision,
    branch: str,
) -> None:
    database = Database(f"sqlite:///{tmp_path / f'{branch}.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    when = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
    with database.session() as session:
        initial = ClimaxEvaluationModel(
            evaluation_time=when,
            symbol="ONTUSDT",
            strategy="CLIMAX_EXHAUSTION",
            subtype_candidate=branch,
            event_id=state.event_id,
        )
        final = ClimaxEvaluationModel(
            evaluation_time=when + timedelta(seconds=1),
            symbol="ONTUSDT",
            strategy="CLIMAX_EXHAUSTION",
            subtype_candidate=branch,
            event_id=state.event_id,
        )
        session.add_all([initial, final])
        session.flush()
        initial_id, final_id = initial.id, final.id

    signal = repository.save_signal(
        make_signal_decision(strategy_type="CLIMAX_EXHAUSTION", strategy_subtype=branch),
        state,
        telegram_sent=False,
        provenance=SignalProvenanceInput(
            strategy_family="CLIMAX_EXHAUSTION",
            strategy_branch=branch,
            event_id=state.event_id,
            root_event_id="root-1",
            decision_evaluation_id=initial_id,
            admission_evaluation_id=final_id if branch == "LOW_VOLUME_EXTENSION_FAILURE" else initial_id,
            code_version="test-code-version",
            config_hash="a" * 64,
            runtime_instance_id="runtime-1",
            runtime_started_at=when - timedelta(minutes=1),
            decision_at=when,
        ),
    )

    with database.session() as session:
        provenance = session.get(SignalProvenanceModel, signal.id)
    assert provenance is not None
    assert provenance.strategy_branch == branch
    assert provenance.decision_evaluation_id == initial_id
    assert provenance.admission_evaluation_id == (final_id if branch == "LOW_VOLUME_EXTENSION_FAILURE" else initial_id)


def test_second_or_competing_provenance_row_is_rejected(tmp_path, make_event_state, make_signal_decision) -> None:
    database = Database(f"sqlite:///{tmp_path / 'provenance-rejected.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    signal = repository.save_signal(
        make_signal_decision(), state, telegram_sent=False, provenance=_baseline_provenance()
    )

    with pytest.raises(IntegrityError):
        with database.session() as session:
            session.add(
                SignalProvenanceModel(
                    signal_id=signal.id,
                    strategy_family="BASELINE_PULLBACK",
                    strategy_branch="BASELINE_PULLBACK",
                    event_id="ONTUSDT:15m:1:111",
                    root_event_id=None,
                    decision_evaluation_id=None,
                    admission_evaluation_id=None,
                    code_version="other",
                    config_hash="b" * 64,
                    runtime_instance_id="runtime-2",
                    runtime_started_at=datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc),
                    decision_at=datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc),
                )
            )

    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("select count(*) from signal_provenance").scalar_one() == 1


def test_additive_migration_creates_empty_provenance_without_touching_legacy_rows(
    tmp_path,
    make_event_state,
    make_signal_decision,
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-without-provenance.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    repository.save_signal(
        make_signal_decision(), state, telegram_sent=False, delivery_payload="legacy payload", provenance=_baseline_provenance()
    )
    with database.engine.begin() as connection:
        before = {
            table: connection.exec_driver_sql(f"select count(*) from {table}").scalar_one()
            for table in ("signals", "strategy_observations", "telegram_delivery_outbox")
        }
        connection.exec_driver_sql("DROP TABLE signal_provenance")

    database.create_all()

    with database.engine.connect() as connection:
        after = {
            table: connection.exec_driver_sql(f"select count(*) from {table}").scalar_one()
            for table in ("signals", "strategy_observations", "telegram_delivery_outbox")
        }
        assert connection.exec_driver_sql("select count(*) from signal_provenance").scalar_one() == 0
        assert connection.exec_driver_sql("pragma integrity_check").scalar_one() == "ok"
        assert connection.exec_driver_sql("pragma foreign_key_check").all() == []
    assert after == before


def test_read_only_helper_accepts_pre_migration_database_without_provenance_table(
    tmp_path,
    make_event_state,
    make_signal_decision,
) -> None:
    database_path = tmp_path / "pre-migration.sqlite"
    database = Database(f"sqlite:///{database_path}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    repository.save_signal(
        make_signal_decision(), state, telegram_sent=False, provenance=_baseline_provenance()
    )
    with database.engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE signal_provenance")

    script = Path(__file__).parents[1] / "scripts" / "report_signal_provenance.py"
    result = subprocess.run(
        [sys.executable, str(script), str(database_path), "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert json.loads(result.stdout)[0]["strategy_branch"] == "LEGACY_UNKNOWN_BRANCH"


def test_read_only_helper_uses_uppercase_signal_outbox_and_preserves_legacy_unknown(
    tmp_path,
    make_event_state,
    make_signal_decision,
) -> None:
    database_path = tmp_path / "provenance-report.sqlite"
    database = Database(f"sqlite:///{database_path}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())

    signal_ids: list[int] = []
    for index in range(4):
        event_id = f"ONTUSDT:15m:1:{111 + index}"
        signal = repository.save_signal(
            make_signal_decision(event_id=event_id),
            state,
            telegram_sent=False,
            delivery_payload=f"payload-{index}",
            provenance=replace(_baseline_provenance(), event_id=event_id),
        )
        signal_ids.append(signal.id)

    claimed = repository.claim_due_deliveries(datetime.now(timezone.utc), limit=1, lease_seconds=30)
    repository.mark_delivery_sent(claimed[0]["id"])
    with database.session() as session:
        outboxes = session.scalars(select(TelegramDeliveryOutboxModel).order_by(TelegramDeliveryOutboxModel.entity_id)).all()
        outboxes[2].status = "RETRY"
        outboxes[3].status = "DEAD"
        session.delete(session.get(SignalProvenanceModel, signal_ids[3]))

    script = Path(__file__).parents[1] / "scripts" / "report_signal_provenance.py"
    result = subprocess.run(
        [sys.executable, str(script), str(database_path), "--format", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    rows = json.loads(result.stdout)

    assert len(rows) == 4
    assert [row["raw_outbox_status"] for row in rows] == ["SENT", "PENDING", "RETRY", "DEAD"]
    assert [row["delivery_state"] for row in rows] == ["SENT", "PENDING", "PENDING", "FAILED"]
    assert rows[3]["strategy_branch"] == "LEGACY_UNKNOWN_BRANCH"

    dry_run = subprocess.run(
        [sys.executable, str(script), str(database_path), "--legacy-dry-run"],
        check=True,
        capture_output=True,
        text=True,
    )
    summary = json.loads(dry_run.stdout)
    assert summary["signals_without_provenance"] == 1
    assert summary["outbox_status_counts"] == {"DEAD": 1, "PENDING": 1, "RETRY": 1, "SENT": 1}
