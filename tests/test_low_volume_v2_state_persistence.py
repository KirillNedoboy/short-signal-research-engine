from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import text

from app.observability.strategy_observations import (
    ObservationWriteStatus,
    StrategyObservation,
    build_observation_evidence,
    make_observation_idempotency_key,
)

from app.domain import EventState, EventStatus
from app.signals.low_volume_v2 import LowVolumeV2Observation, advance_low_volume_v2, new_episode
from app.storage.db import Database
from app.storage.repository import BotRepository


def _cfg():
    return SimpleNamespace(
        low_volume_v2_min_closed_candles_after_retest=2,
        low_volume_v2_max_episode_lifetime_minutes=45,
        low_volume_v2_new_high_tolerance_pct=0.30,
        low_volume_v2_require_breakout_close=True,
        low_volume_v2_require_lower_high_or_swing_break=True,
        low_volume_v2_block_oi_acceleration_pct=1.0,
        low_volume_v2_block_price_acceleration_resumed=True,
        low_volume_v2_max_entry_distance_below_high_pct=15.0,
    )


def _obs(at, high=100.0):
    return LowVolumeV2Observation(
        observed_at=at, current_high=high, close=95.0,
        breakout_reference=100.0, closed_candles=2,
        lower_high_or_swing_break=True,
    )


def test_v2_state_restores_across_repository_restart_without_v1_mutation(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'state.db'}"
    first = BotRepository(Database(db_url))
    first._db.create_all()
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("root", 2, 100.0, start)
    advance_low_volume_v2(state, _obs(start + timedelta(minutes=2), 101.0), _cfg())
    first.upsert_low_volume_v2_episode_state(state, symbol="BTCUSDT")

    second = BotRepository(Database(db_url))
    restored = second.get_low_volume_v2_episode_state("root", 2)
    assert restored["current_anchor_high"] == 101.0
    assert restored["reset_count"] == 1
    assert restored["v2_state"] == "NEW_HIGH_RESET"
    assert second.get_event_state("BTCUSDT") is None


def test_v2_emitted_state_suppresses_duplicate_after_restart(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'dedupe.db'}"
    repo = BotRepository(Database(db_url))
    repo._db.create_all()
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    state = new_episode("root", 1, 100.0, start)
    decision = advance_low_volume_v2(state, _obs(start + timedelta(minutes=2)), _cfg())
    assert decision is not None
    repo.upsert_low_volume_v2_episode_state(state, symbol="BTCUSDT")
    restored = repo.get_low_volume_v2_episode_state("root", 1)
    assert restored["emitted"] is True
    duplicate = new_episode("root", 1, 100.0, start)
    duplicate.emitted = restored["emitted"]
    duplicate.v2_state = restored["v2_state"]
    assert advance_low_volume_v2(duplicate, _obs(start + timedelta(minutes=3)), _cfg()) is None


def test_v2_persists_every_lifecycle_field_and_exact_episode_start(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'lifecycle.db'}"
    repo = BotRepository(Database(db_url))
    repo._db.create_all()
    start = datetime(2026, 10, 3, 9, 37, tzinfo=UTC)
    state = new_episode("root", 7, 100.0, start)
    state.last_retest_high = 99.25
    state.last_closed_candle_time = start + timedelta(minutes=4)
    state.invalidated_reason = "stale_data"
    state.v2_state = "INVALIDATED"
    repo.upsert_low_volume_v2_episode_state(state, symbol="BTCUSDT")

    restored = BotRepository(Database(db_url)).get_low_volume_v2_episode_state("root", 7)
    assert restored["first_event_high"] == 100.0
    assert restored["last_retest_high"] == 99.25
    assert restored["last_closed_candle_time"] == (start + timedelta(minutes=4)).replace(tzinfo=None)
    assert restored["episode_started_at"] == start.replace(tzinfo=None)
    assert restored["invalidated_reason"] == "stale_data"


def test_create_all_repairs_legacy_v2_columns_without_touching_v1_state(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'legacy.db'}")
    with db.engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE low_volume_v2_episode_states (
                root_event_id VARCHAR(128) NOT NULL,
                event_revision INTEGER NOT NULL,
                symbol VARCHAR(32) NOT NULL,
                current_anchor_high FLOAT NOT NULL,
                current_anchor_high_time DATETIME NOT NULL,
                reset_count INTEGER NOT NULL DEFAULT 0,
                entry_trigger_time DATETIME,
                v2_state VARCHAR(32) NOT NULL,
                emitted BOOLEAN NOT NULL DEFAULT 0,
                updated_at DATETIME NOT NULL,
                PRIMARY KEY (root_event_id, event_revision)
            )
        """))
        connection.execute(text("""
            INSERT INTO low_volume_v2_episode_states
            (root_event_id, event_revision, symbol, current_anchor_high,
             current_anchor_high_time, reset_count, v2_state, emitted, updated_at)
            VALUES ('legacy', 1, 'BTCUSDT', 101.0, '2026-10-03 10:00:00', 1,
                    'NEW_HIGH_RESET', 0, '2026-10-03 10:00:00')
        """))
    repo = BotRepository(db)
    repo._db.create_all()
    repo.upsert_event_state(EventState(symbol="ETHUSDT", event_id="v1", state=EventStatus.PUMP_DETECTED))
    BotRepository(Database(db.db_url))._db.create_all()

    restored = BotRepository(Database(db.db_url)).get_low_volume_v2_episode_state("legacy", 1)
    assert restored["current_anchor_high"] == 101.0
    assert restored["first_event_high"] is None
    assert restored["episode_started_at"] is None
    assert BotRepository(Database(db.db_url)).get_event_state("ETHUSDT").event_id == "v1"


def test_cleanup_removes_only_old_terminal_v2_state_and_preserves_history(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'cleanup.db'}")
    db.create_all()
    repo = BotRepository(db)
    now = datetime(2026, 10, 3, 12, tzinfo=UTC)

    def add(root, revision, state_name, updated_at, emitted=False):
        state = new_episode(root, revision, 100.0, updated_at)
        state.v2_state = state_name
        state.invalidated_reason = "test" if state_name == "INVALIDATED" else None
        state.emitted = emitted
        repo.upsert_low_volume_v2_episode_state(state, symbol="BTCUSDT")
        with db.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE low_volume_v2_episode_states SET updated_at = ? WHERE root_event_id = ?",
                (updated_at.replace(tzinfo=None), root),
            )

    add("stale-expired", 1, "EXPIRED", now - timedelta(days=2))
    add("stale-invalid", 1, "INVALIDATED", now - timedelta(days=2))
    add("stale-emitted", 1, "ACTIONABLE", now - timedelta(days=2), emitted=True)
    add("active", 1, "RETEST_CONFIRMATION", now - timedelta(days=2), emitted=True)
    repo.upsert_event_state(EventState(symbol="ETHUSDT", event_id="v1", state=EventStatus.PUMP_DETECTED))

    removed = repo.cleanup_low_volume_v2_episode_states(now=now, terminal_retention=timedelta(days=1))

    assert removed == 3
    retained = repo.get_low_volume_v2_episode_state("active", 1)
    assert retained["v2_state"] == "RETEST_CONFIRMATION"
    assert retained["emitted"] is True
    assert repo.get_event_state("ETHUSDT").event_id == "v1"


def test_v2_state_listing_is_bounded_and_keeps_newest_active_rows(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'bounded.db'}")
    db.create_all()
    repo = BotRepository(db)
    start = datetime(2026, 10, 3, 10, tzinfo=UTC)
    for index in range(300):
        state = new_episode(f"root-{index}", 1, 100.0, start + timedelta(minutes=index))
        repo.upsert_low_volume_v2_episode_state(state, symbol="BTCUSDT")

    rows = repo.list_low_volume_v2_episode_states(limit=256)

    assert len(rows) == 256
    assert rows[0]["root_event_id"] == "root-299"


def _branch_observation(strategy: str, observation_id: str, attempt_id: str | None = None):
    observed_at = datetime(2026, 10, 3, 12, tzinfo=UTC)
    evidence = build_observation_evidence({"reason": "test"})
    idempotency_key = make_observation_idempotency_key(
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
        observation_id=observation_id,
        idempotency_key=idempotency_key,
        run_id="run-1",
        runtime_instance_id="runtime-1",
        runtime_started_at=observed_at,
        code_version="test",
        strategy_family="CLIMAX_EXHAUSTION",
        strategy=strategy,
        evaluation_phase="INITIAL",
        symbol="TESTUSDT",
        root_event_id="root-1",
        event_revision=1,
        attempt_id=attempt_id,
        evaluation_id=None,
        signal_id=None,
        observed_at=observed_at,
        exchange_time=None,
        market_asof=observed_at,
        live_decision="BLOCKED",
        shadow_decision="NOT_EVALUATED",
        score=50,
        blockers=[],
        warnings=[],
        market_price=95.0,
        event_high=100.0,
        model_version="climax-v1",
        config_hash="a" * 64,
        input_fingerprint=evidence.input_fingerprint,
        input_snapshot=evidence.snapshot,
    )


def test_v2_climax_observation_is_scheduled_for_outcome_processing(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'v2-outcome.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    observation = _branch_observation("LOW_VOLUME_EXTENSION_FAILURE_V2", "observation-v2")

    assert repository.record_strategy_observation(observation).status is ObservationWriteStatus.INSERTED
    due = repository.list_strategy_observations_due_outcomes(limit=10, now=observation.observed_at)
    assert [row["observation_id"] for row in due] == ["observation-v2"]


def test_climax_root_report_includes_v2_branch(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'v2-report.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    observation = _branch_observation(
        "LOW_VOLUME_EXTENSION_FAILURE_V2",
        "observation-v2",
        "root-1:LOW_VOLUME_EXTENSION_FAILURE_V2:r1:a1",
    )

    assert repository.record_strategy_observation(observation).status is ObservationWriteStatus.INSERTED
    report = repository.list_climax_root_report("root-1")
    assert report["branches"]["LOW_VOLUME_EXTENSION_FAILURE_V2"][0]["attempt_id"] == (
        "root-1:LOW_VOLUME_EXTENSION_FAILURE_V2:r1:a1"
    )
