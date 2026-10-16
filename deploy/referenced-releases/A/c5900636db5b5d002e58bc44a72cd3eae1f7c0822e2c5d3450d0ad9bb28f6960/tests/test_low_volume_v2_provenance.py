from sqlalchemy import inspect
from datetime import UTC, datetime
from types import SimpleNamespace

from app.main import ShortSignalBot, _provenance_anomaly
from app.storage.db import Database


def test_v2_provenance_schema_preserves_identity_fields(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'provenance.db'}")
    db.create_all()
    columns = {column["name"] for column in inspect(db.engine).get_columns("signal_provenance")}
    assert {"root_event_id", "strategy_branch", "event_revision", "entry_trigger_time"} <= columns


def test_v1_provenance_remains_nullable_compatible(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'provenance-v1.db'}")
    db.create_all()
    info = {column["name"]: column for column in inspect(db.engine).get_columns("signal_provenance")}
    assert info["root_event_id"]["nullable"] is True
    assert info["event_revision"]["nullable"] is True


def test_v2_provenance_validation_uses_current_anchor_high() -> None:
    decision = SimpleNamespace(
        strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE_V2",
        market_price=95.0,
        strategy_metadata={
            "event_high": 110.0,
            "current_anchor_high": 100.0,
            "entry_distance_below_high_pct": 5.0,
        },
    )

    assert _provenance_anomaly(decision) is None


def test_v2_provenance_validation_flags_distance_from_wrong_high() -> None:
    decision = SimpleNamespace(
        strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE_V2",
        market_price=95.0,
        strategy_metadata={
            "event_high": 110.0,
            "current_anchor_high": 100.0,
            "entry_distance_below_high_pct": 13.636,
        },
    )

    assert _provenance_anomaly(decision) == "PROVENANCE_INVARIANT_FAILED"


def test_v2_provenance_builder_records_current_anchor_and_distance() -> None:
    bot = object.__new__(ShortSignalBot)
    bot._code_version = "test-code"
    bot._strategy_config_hash = "a" * 64
    bot._runtime_instance_id = "runtime"
    bot._runtime_started_at = datetime(2026, 10, 3, 10, tzinfo=UTC)
    decision = SimpleNamespace(
        strategy_type="CLIMAX_EXHAUSTION",
        strategy_subtype="LOW_VOLUME_EXTENSION_FAILURE_V2",
        event_id="event-1",
        signal_time=datetime(2026, 10, 3, 10, 5, tzinfo=UTC),
        market_price=95.0,
        strategy_metadata={
            "event_high": 110.0,
            "current_anchor_high": 100.0,
            "entry_distance_below_high_pct": 5.0,
            "event_revision": 2,
            "entry_trigger_time": "2026-10-03T10:04:00+00:00",
        },
    )

    provenance = bot._signal_provenance(
        decision,
        root_event_id="root-1",
        decision_evaluation_id=7,
    )

    assert provenance.decision_event_high == 100.0
    assert provenance.decision_distance_from_high == 5.0
