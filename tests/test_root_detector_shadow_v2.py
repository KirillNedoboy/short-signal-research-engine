from datetime import datetime, timedelta, timezone

import pytest

from app.observability.root_detector_shadow_v2 import (
    EARLY_ROOT_CREATED,
    DESIGN_INCONCLUSIVE,
    CounterfactualContract,
    audit_current_detector_contract,
    evaluate_v2_observation,
    paired_root_metrics,
)
from app.storage.db import Database
from app.storage.models import RootDetectorShadowV2OutcomeModel, RootDetectorShadowV2RootModel
from app.storage.repository import BotRepository
from app.observability.strategy_observations import canonicalize_json_payload
from sqlalchemy import inspect


def _features(**overrides):
    value = {
        "pump_5m": 3.0,
        "pump_15m": 5.0,
        "pump_1h": 8.0,
        "pump_4h": 10.0,
        "volume_ratio": None,
        "volume_z": 1.0,
        "oi_5m": None,
        "oi_15m": None,
        "oi_1h": None,
        "distance_from_high": 1.0,
        "rejection": None,
        "liquidity": None,
        "squeeze_risk": None,
    }
    value.update(overrides)
    return value


def _contract():
    return CounterfactualContract(
        current_required=("v1_trigger", "live_stretch"),
        v2_required=("v1_trigger",),
        deferred=("live_stretch",),
        safety_rationale={"live_stretch": "not an execution-safety guard in this contract"},
        implementable=True,
    )


def test_contract_audit_exposes_exact_diff_and_blocks_ambiguous_revision():
    audit = audit_current_detector_contract()
    assert audit.predicates
    assert audit.counterfactual_diff
    assert any(item.name == "live_stretch" and item.classification == "STRUCTURAL_PUMP" for item in audit.predicates)
    assert audit.verdict != DESIGN_INCONCLUSIVE


def test_v2_creates_one_immutable_root_from_qualifying_v1_observation():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first = evaluate_v2_observation(
        episode_id="episode-1",
        symbol="AAAUSDT",
        episode_opened_at=now - timedelta(minutes=5),
        observed_at=now,
        reference_price=100.0,
        event_high=104.0,
        features=_features(),
        current_live_root_id=None,
        contract=_contract(),
    )
    assert first.state == EARLY_ROOT_CREATED
    assert first.root_id == "episode-1:FIRST_V2_EARLY_ROOT"
    assert first.reason


def test_v2_does_not_create_second_root_for_same_episode():
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    result = evaluate_v2_observation(
        episode_id="episode-1",
        symbol="AAAUSDT",
        episode_opened_at=now,
        observed_at=now + timedelta(minutes=15),
        reference_price=99.0,
        event_high=104.0,
        features=_features(),
        current_live_root_id="event-1",
        contract=_contract(),
        existing_root_id="episode-1:FIRST_V2_EARLY_ROOT",
    )
    assert result.root_id == "episode-1:FIRST_V2_EARLY_ROOT"
    assert result.state == EARLY_ROOT_CREATED


def test_v2_repository_is_idempotent_and_keeps_outcomes_separate(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'v2.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    payload = {
        "shadow_v2_root_id": "episode-1:FIRST_V2_EARLY_ROOT",
        "episode_id": "episode-1", "symbol": "AAAUSDT", "state": EARLY_ROOT_CREATED,
        "created_at": now, "episode_opened_at": now, "episode_age_seconds": 0.0,
        "reference_price": 100.0, "event_high_at_decision": 104.0, "distance_from_high": 3.8,
        "pump_5m": 3.0, "pump_15m": 5.0, "pump_1h": 8.0, "pump_4h": 10.0,
        "volume_ratio": None, "volume_z": 1.0, "oi_features_json": {}, "safety_features_json": {},
        "conditions_json": {"v1_trigger": "PASS"}, "downstream_features_json": {},
        "readiness_reason": "audited_v1_trigger", "live_root_present_at_creation": False,
        "code_version": "test", "runtime_instance_id": "runtime", "runtime_started_at": now,
        "method_version": "ROOT_DETECTOR_SHADOW_V2_CONTRACT_V1",
    }
    assert repo.record_root_detector_shadow_v2_root(payload) is True
    assert repo.record_root_detector_shadow_v2_root(payload) is False
    due = repo.list_root_detector_shadow_v2_outcomes_due(now=now + timedelta(minutes=15), limit=10)
    assert [row["shadow_v2_root_id"] for row in due] == [payload["shadow_v2_root_id"]]
    assert repo.record_root_detector_shadow_v2_outcome_attempt(
        payload["shadow_v2_root_id"], outcome={"outcome_status": "MATURE", "horizons": {}},
        next_due_at=None, computed_at=now + timedelta(minutes=15),
    ) is True
    with db.session() as session:
        assert session.query(RootDetectorShadowV2RootModel).count() == 1
        assert session.query(RootDetectorShadowV2OutcomeModel).one().outcome_status == "MATURE"


def test_paired_metrics_use_short_context_sign_and_na_zero_mfe():
    metrics = paired_root_metrics(
        v2_anchor_price=100.0, live_root_anchor_price=94.0,
        mfe_from_v2_to_live=4.0, mfe_24h=10.0,
    )
    assert metrics["price_move_before_live_root"] == 6.0
    assert metrics["fraction_of_total_24h_MFE_already_gone_before_live_root"] == 0.4
    assert paired_root_metrics(v2_anchor_price=100.0, live_root_anchor_price=100.0, mfe_from_v2_to_live=1.0, mfe_24h=0.0)["fraction_of_total_24h_MFE_already_gone_before_live_root"] == "N/A"


def test_database_can_boot_without_v2_tables_when_shadow_flag_is_off(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'off.sqlite'}")
    db.create_all(include_shadow_v2=False)
    assert "root_detector_shadow_v2_roots" not in inspect(db.engine).get_table_names()


def test_v2_outcome_persists_top_level_and_nested_datetimes_as_utc_iso(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'datetime.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=3)))
    payload = {
        "shadow_v2_root_id": "episode-datetime:FIRST_V2_EARLY_ROOT",
        "episode_id": "episode-datetime", "symbol": "AAAUSDT", "state": EARLY_ROOT_CREATED,
        "created_at": now, "episode_opened_at": now, "episode_age_seconds": 0.0,
        "reference_price": 100.0, "event_high_at_decision": 104.0, "distance_from_high": 3.8,
        "pump_5m": 3.0, "pump_15m": 5.0, "pump_1h": 8.0, "pump_4h": 10.0,
        "volume_ratio": None, "volume_z": 1.0, "oi_features_json": {}, "safety_features_json": {},
        "conditions_json": {"v1_trigger": "PASS"}, "downstream_features_json": {},
        "readiness_reason": "audited_v1_trigger", "live_root_present_at_creation": False,
        "code_version": "test", "runtime_instance_id": "runtime", "runtime_started_at": now,
        "method_version": "ROOT_DETECTOR_SHADOW_V2_CONTRACT_V1",
    }
    assert repo.record_root_detector_shadow_v2_root(payload) is True
    outcome = {
        "outcome_status": "PARTIAL",
        "coverage_end_at": now,
        "evidence": {"observed_at": now + timedelta(minutes=5)},
    }
    assert repo.record_root_detector_shadow_v2_outcome_attempt(
        payload["shadow_v2_root_id"], outcome=outcome,
        next_due_at=now + timedelta(minutes=30), computed_at=now,
    ) is True
    with db.session() as session:
        persisted = session.query(RootDetectorShadowV2OutcomeModel).one().outcome_json
    assert persisted["coverage_end_at"] == "2026-01-01T09:00:00Z"
    assert persisted["evidence"]["observed_at"] == "2026-01-01T09:05:00Z"


def test_v2_outcome_rejects_unsupported_json_values_explicitly(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'unsupported.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    with pytest.raises(TypeError, match="unsupported observation evidence value"):
        canonicalize_json_payload({"bad": object()})
