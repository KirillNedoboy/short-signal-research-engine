from __future__ import annotations

import json
from dataclasses import fields, replace
from datetime import datetime, timedelta, timezone

import numpy as np

from app.observability.strategy_observations import (
    ObservationWriteStatus,
    StrategyObservation,
    build_observation_evidence,
    make_observation_idempotency_key,
)
from app.storage.db import Database
from app.storage.repository import BotRepository


def _observation(
    *,
    runtime_instance_id: str = "runtime-1",
    strategy: str = "VOLUME_CLIMAX_UNWIND",
    code_version: str = "test-code-version",
) -> StrategyObservation:
    observed_at = datetime(2026, 7, 24, 12, 0, tzinfo=timezone.utc)
    evidence = build_observation_evidence({"ret_5m": 3.0, "reason": "oi_missing"})
    key = make_observation_idempotency_key(
        strategy_family="CLIMAX_EXHAUSTION",
        strategy=strategy,
        symbol="TESTUSDT",
        root_event_id="root-1",
        event_revision=1,
        evaluation_phase="INITIAL",
        market_asof=observed_at,
        input_fingerprint=evidence.input_fingerprint,
        model_version="climax-v1",
        config_hash="a" * 64,
    )
    return StrategyObservation(
        observation_id="observation-1",
        idempotency_key=key,
        run_id="run-1",
        runtime_instance_id=runtime_instance_id,
        runtime_started_at=observed_at - timedelta(minutes=5),
        code_version=code_version,
        strategy_family="CLIMAX_EXHAUSTION",
        strategy=strategy,
        evaluation_phase="INITIAL",
        symbol="TESTUSDT",
        root_event_id="root-1",
        event_revision=1,
        attempt_id=None,
        evaluation_id=None,
        signal_id=None,
        observed_at=observed_at,
        exchange_time=None,
        market_asof=observed_at,
        live_decision="BLOCKED",
        shadow_decision="NOT_EVALUATED",
        score=55,
        blockers=["oi_missing_for_volume_climax"],
        warnings=[],
        market_price=100.0,
        event_high=104.0,
        model_version="climax-v1",
        config_hash="a" * 64,
        input_fingerprint=evidence.input_fingerprint,
        input_snapshot=evidence.snapshot,
    )


def test_same_observation_is_duplicate_across_runtime_restarts(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'strategy-observations.sqlite'}")
    database.create_all()
    repository = BotRepository(database)

    first = repository.record_strategy_observation(_observation())
    second = repository.record_strategy_observation(
        replace(_observation(), observation_id="observation-2", run_id="run-2", runtime_instance_id="runtime-2")
    )

    assert first.status is ObservationWriteStatus.INSERTED
    assert second.status is ObservationWriteStatus.DUPLICATE
    with database.engine.connect() as connection:
        assert connection.exec_driver_sql("select count(*) from strategy_observations").scalar_one() == 1
        assert connection.exec_driver_sql("pragma integrity_check").scalar_one() == "ok"


def test_both_climax_branches_are_saved_for_one_root_event(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'strategy-branches.sqlite'}")
    database.create_all()
    repository = BotRepository(database)

    volume = repository.record_strategy_observation(_observation(strategy="VOLUME_CLIMAX_UNWIND"))
    low_volume = repository.record_strategy_observation(
        replace(_observation(strategy="LOW_VOLUME_EXTENSION_FAILURE"), observation_id="observation-2")
    )

    assert volume.status is ObservationWriteStatus.INSERTED
    assert low_volume.status is ObservationWriteStatus.INSERTED
    with database.engine.connect() as connection:
        rows = connection.exec_driver_sql(
            "select strategy from strategy_observations order by strategy"
        ).scalars().all()
    assert rows == ["LOW_VOLUME_EXTENSION_FAILURE", "VOLUME_CLIMAX_UNWIND"]


def test_nested_nonfinite_values_are_saved_as_null_with_exact_paths() -> None:
    evidence = build_observation_evidence(
        {
            "features": {
                "oi_change_pct": float("nan"),
                "levels": [float("inf"), {"floor": float("-inf")}],
                "finite": 1.25,
            }
        }
    )

    assert evidence.snapshot["features"] == {
        "finite": 1.25,
        "levels": [None, {"floor": None}],
        "oi_change_pct": None,
    }
    assert evidence.snapshot["evidence_warnings"] == [
        {"path": "features.levels[0]", "reason": "NON_FINITE_FLOAT", "original": "+Inf"},
        {"path": "features.levels[1].floor", "reason": "NON_FINITE_FLOAT", "original": "-Inf"},
        {"path": "features.oi_change_pct", "reason": "NON_FINITE_FLOAT", "original": "NaN"},
    ]
    assert evidence.warnings == evidence.snapshot["evidence_warnings"]


def test_numpy_nonfinite_values_are_saved_as_null() -> None:
    evidence = build_observation_evidence({"features": {"ratio": np.float32(np.nan)}})

    assert evidence.snapshot["features"]["ratio"] is None
    assert evidence.warnings[0]["path"] == "features.ratio"
    assert evidence.warnings[0]["original"] == "NaN"


def test_nonfinite_observation_is_persisted_with_warning_payload(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'sanitized-observation.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    evidence = build_observation_evidence({"features": {"oi_change_pct": float("nan")}})
    observation = replace(
        _observation(),
        input_fingerprint=evidence.input_fingerprint,
        input_snapshot=evidence.snapshot,
    )

    result = repository.record_strategy_observation(observation)

    assert result.status is ObservationWriteStatus.INSERTED
    with database.engine.connect() as connection:
        snapshot_json = connection.exec_driver_sql(
            "select input_snapshot_json from strategy_observations"
        ).scalar_one()
    snapshot = json.loads(snapshot_json)
    assert snapshot["features"]["oi_change_pct"] is None
    assert snapshot["evidence_warnings"][0]["path"] == "features.oi_change_pct"


def test_finite_snapshot_is_unchanged_and_fingerprint_is_deterministic() -> None:
    first = build_observation_evidence({"features": {"price": 1.25}, "finite": True})
    second = build_observation_evidence({"finite": True, "features": {"price": 1.25}})

    assert first.snapshot == {"features": {"price": 1.25}, "finite": True}
    assert first.warnings == []
    assert first.input_fingerprint == second.input_fingerprint
    assert first.snapshot_json == second.snapshot_json


def test_evidence_is_deterministic_and_bounded() -> None:
    evidence = build_observation_evidence({"when": datetime(2026, 7, 24, 15, tzinfo=timezone.utc), "payload": "x" * 40_000})

    assert evidence.snapshot["snapshot_truncated"] is True
    assert evidence.snapshot["full_input_fingerprint"] == evidence.input_fingerprint
    assert len(evidence.snapshot_json.encode("utf-8")) <= 32 * 1024
    assert evidence.warnings == []


def test_strategy_observation_outcome_can_be_updated_and_completed(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'strategy-observation-outcome.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    repository.record_strategy_observation(_observation())

    due = repository.list_strategy_observations_due_outcomes(limit=10)
    assert len(due) == 1
    assert due[0]["observation_id"] == "observation-1"

    repository.update_strategy_observation_outcome(
        "observation-1",
        {
            "data_status": "complete",
            "horizons": {"1m": {"price": 99.0}},
            "mfe_pct": 4.0,
            "mae_pct": 2.0,
            "time_to_mfe_minutes": 3.0,
            "time_to_mae_minutes": 1.0,
            "new_high_after_observation": False,
        },
        updated_at=datetime(2026, 7, 24, 12, 20, tzinfo=timezone.utc),
    )

    assert repository.list_strategy_observations_due_outcomes(limit=10) == []
    with database.engine.connect() as connection:
        row = connection.exec_driver_sql(
            "select outcome_status, outcome_mfe_pct, outcome_new_high_after_observation from strategy_observations"
        ).one()
    assert row == ("complete", 4.0, 0)


def test_incomplete_outcome_retry_does_not_starve_later_observations(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'strategy-observation-retry.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    first = _observation()
    second = replace(
        _observation(strategy="LOW_VOLUME_EXTENSION_FAILURE"),
        observation_id="observation-2",
        observed_at=first.observed_at + timedelta(seconds=1),
    )
    repository.record_strategy_observation(first)
    repository.record_strategy_observation(second)
    now = first.observed_at + timedelta(hours=1)

    repository.update_strategy_observation_outcome(
        first.observation_id,
        {"data_status": "incomplete", "data_reason": "AWAITING_15M_COVERAGE"},
        updated_at=now,
        next_attempt_at=now + timedelta(minutes=1),
    )

    due = repository.list_strategy_observations_due_outcomes(limit=1, now=now)
    assert [row["observation_id"] for row in due] == [second.observation_id]


def test_strategy_observation_contract_has_row_level_provenance() -> None:
    field_names = {field.name for field in fields(StrategyObservation)}

    assert {"runtime_instance_id", "code_version", "config_hash", "runtime_started_at"} <= field_names


def test_additive_strategy_observation_migration_preserves_existing_rows(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy-strategy-observation.sqlite'}")
    with database.engine.begin() as connection:
        connection.exec_driver_sql(
            "CREATE TABLE strategy_observations (observation_id TEXT PRIMARY KEY, outcome_status TEXT)"
        )
        connection.exec_driver_sql(
            "INSERT INTO strategy_observations (observation_id, outcome_status) VALUES ('legacy-1', 'incomplete')"
        )

    database.create_all()

    with database.engine.connect() as connection:
        columns = {row[1] for row in connection.exec_driver_sql("pragma table_info('strategy_observations')").all()}
        legacy_row = connection.exec_driver_sql(
            "select observation_id, outcome_status from strategy_observations"
        ).one()
    assert {"code_version", "runtime_started_at", "outcome_next_attempt_at", "outcome_attempt_count"} <= columns
    assert legacy_row == ("legacy-1", "incomplete")


def test_strategy_observation_schema_has_research_query_indexes(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'strategy-observation-indexes.sqlite'}")
    database.create_all()

    with database.engine.connect() as connection:
        index_names = {
            row[1]
            for row in connection.exec_driver_sql("pragma index_list('strategy_observations')").all()
        }

    assert "ix_strategy_observations_family_strategy_observed_at" in index_names
    with database.engine.connect() as connection:
        columns = {row[1] for row in connection.exec_driver_sql("pragma table_info('strategy_observations')").all()}
    assert {
        "outcome_status",
        "outcome_json",
        "outcome_mfe_pct",
        "outcome_mae_pct",
        "outcome_time_to_mfe_minutes",
        "outcome_time_to_mae_minutes",
        "outcome_new_high_after_observation",
        "outcome_updated_at",
        "code_version",
        "runtime_started_at",
        "outcome_next_attempt_at",
        "outcome_attempt_count",
    } <= columns
