from datetime import datetime, timedelta, timezone

import pandas as pd

from app.observability.outcome_scheduler import (
    SchedulerStats,
    ShadowOutcomeScheduler,
    emit_scheduler_summary,
    next_outcome_due_at,
    quota_for_rate,
)
from app.storage.db import Database
from app.storage.models import RootDetectorShadowV2OutcomeModel, RootDetectorShadowV2RootModel
from app.storage.repository import BotRepository


UTC = timezone.utc


class FakeClient:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.calls: list[tuple[str, str, int, int, int]] = []

    async def fetch_klines(self, symbol, interval, limit, start_ms, end_ms):
        self.calls.append((symbol, interval, limit, start_ms, end_ms))
        return self.frame.to_dict("records")


class FakeRepository:
    def __init__(self, rows):
        self.rows = rows
        self.attempts = []

    def list_shadow_episode_outcomes_due(self, *, now, runtime_limit, legacy_limit):
        return {"runtime": self.rows[:runtime_limit], "legacy": self.rows[runtime_limit:runtime_limit + legacy_limit]}

    def record_root_detector_shadow_episode_outcome_attempt(self, episode_id, *, outcome, next_due_at, computed_at, error=None):
        self.attempts.append((episode_id, outcome, next_due_at, computed_at, error))
        return True


class FakeV2Repository(FakeRepository):
    def list_root_detector_shadow_v2_outcomes_due(self, *, now, limit):
        return [{
            "shadow_v2_root_id": "v2-1", "episode_id": "e-v2", "symbol": "AAAUSDT",
            "anchor_time": now - timedelta(minutes=30), "anchor_price": 100.0,
            "event_high": 105.0, "next_due_at": now - timedelta(minutes=1),
        }]

    def record_root_detector_shadow_v2_outcome_attempt(self, shadow_v2_root_id, *, outcome, next_due_at, computed_at, error=None):
        self.attempts.append((shadow_v2_root_id, outcome, next_due_at, computed_at, error))
        return True


class DisabledV2AccessRepository(FakeRepository):
    def list_root_detector_shadow_v2_outcomes_due(self, **kwargs):
        raise AssertionError("V2 repository accessed while disabled")


class MissingSchemaV2Repository(FakeRepository):
    def list_root_detector_shadow_v2_outcomes_due(self, **kwargs):
        raise RuntimeError("no such table: root_detector_shadow_v2_outcomes")


class AccountingV2Repository(FakeV2Repository):
    def __init__(self, outcomes):
        super().__init__([])
        self.outcomes = list(outcomes)

    def list_root_detector_shadow_v2_outcomes_due(self, *, now, limit):
        return [
            {
                "shadow_v2_root_id": f"v2-{index}", "episode_id": f"e-v2-{index}", "symbol": "AAAUSDT",
                "anchor_time": now - timedelta(minutes=30), "anchor_price": 100.0,
                "event_high": 105.0, "next_due_at": now - timedelta(minutes=1), "outcome_attempt_count": 0,
            }
            for index in range(len(self.outcomes))
        ]

    def record_root_detector_shadow_v2_outcome_attempt(self, shadow_v2_root_id, *, outcome, next_due_at, computed_at, error=None):
        self.attempts.append((shadow_v2_root_id, outcome, next_due_at, computed_at, error))
        if error is not None:
            return True
        result = self.outcomes.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _frame(start: datetime, count: int = 7) -> pd.DataFrame:
    rows = []
    for i in range(count):
        close_time = start + timedelta(minutes=5 * (i + 1))
        rows.append({
            "timestamp": close_time - timedelta(minutes=5),
            "close_time": close_time,
            "open": 100.0,
            "high": 102.0,
            "low": 98.0,
            "close": 99.0,
            "volume": 1.0,
        })
    return pd.DataFrame(rows)


def test_next_due_progresses_to_first_missing_horizon():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    outcome = {
        "outcome_status": "PARTIAL",
        "horizons": {
            "15m": {"price": 99.0},
            "30m": {"price": None},
            "1h": {"price": None},
        },
    }
    assert next_outcome_due_at(anchor, anchor + timedelta(hours=1), outcome) == anchor + timedelta(minutes=30)


def test_quota_uses_measured_rate_and_bounded_capacity_margin():
    assert quota_for_rate(30.0, scan_interval_sec=60, minimum=25, maximum=100) == 25
    assert quota_for_rate(3600.0, scan_interval_sec=60, minimum=25, maximum=100) == 100


def test_scheduler_groups_overlapping_episodes_into_one_request():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        {"episode_id": "e1", "symbol": "AAAUSDT", "anchor_time": anchor, "anchor_price": 100.0, "event_high": 105.0},
        {"episode_id": "e2", "symbol": "AAAUSDT", "anchor_time": anchor + timedelta(minutes=5), "anchor_price": 100.0, "event_high": 105.0},
    ]
    client = FakeClient(_frame(anchor))
    repository = FakeRepository(rows)
    scheduler = ShadowOutcomeScheduler(client=client, repository=repository, runtime_batch_size=10, legacy_batch_size=0, v2_enabled=False)

    import asyncio

    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))

    assert stats.episodes_processed == 2
    assert len(client.calls) == 1
    assert len(repository.attempts) == 2


def test_runtime_batch_has_priority_over_legacy_batch():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [{"episode_id": "runtime", "symbol": "AAAUSDT", "anchor_time": anchor, "anchor_price": 100.0, "event_high": 105.0}]
    repository = FakeRepository(rows)
    result = repository.list_shadow_episode_outcomes_due(now=anchor, runtime_limit=1, legacy_limit=10)
    assert [row["episode_id"] for row in result["runtime"]] == ["runtime"]


def test_scheduler_processes_v2_batch_through_same_grouped_fetch():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    repository = FakeV2Repository([])
    scheduler = ShadowOutcomeScheduler(client=FakeClient(_frame(anchor)), repository=repository, runtime_batch_size=10, legacy_batch_size=0, v2_enabled=True)
    import asyncio
    asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))
    assert repository.attempts and repository.attempts[0][0] == "v2-1"


def test_v2_disabled_never_accesses_v2_repository_and_keeps_v1_running():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [{"episode_id": "v1", "symbol": "AAAUSDT", "anchor_time": anchor, "anchor_price": 100.0, "event_high": 105.0}]
    repository = DisabledV2AccessRepository(rows)
    scheduler = ShadowOutcomeScheduler(client=FakeClient(_frame(anchor)), repository=repository, runtime_batch_size=10, legacy_batch_size=0, v2_enabled=False)
    import asyncio
    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))
    assert stats.runtime_due_seen == 1
    assert stats.runtime_processed == 1
    assert stats.v2_due_seen == 0
    assert stats.v2_processed == 0
    assert stats.v2_errors == 0
    assert stats.errors == 0


def test_v2_enabled_missing_schema_is_explicit_scheduler_error():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    repository = MissingSchemaV2Repository([])
    scheduler = ShadowOutcomeScheduler(client=FakeClient(_frame(anchor)), repository=repository, runtime_batch_size=10, legacy_batch_size=0, v2_enabled=True)
    import asyncio
    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))
    assert stats.v2_errors == 1
    assert stats.errors >= 1


def test_v2_scheduler_counts_only_successful_persistence():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    repository = AccountingV2Repository([True, False, True])
    scheduler = ShadowOutcomeScheduler(client=FakeClient(_frame(anchor)), repository=repository, runtime_batch_size=10, legacy_batch_size=0, v2_enabled=True)
    import asyncio
    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))
    assert stats.v2_due_seen == 3
    assert stats.v2_processed == 2
    assert stats.v2_errors == 1


def test_v2_scheduler_counts_all_persistence_failures_as_errors():
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    repository = AccountingV2Repository([RuntimeError("write failed")] * 3)
    scheduler = ShadowOutcomeScheduler(client=FakeClient(_frame(anchor)), repository=repository, runtime_batch_size=10, legacy_batch_size=0, v2_enabled=True)
    import asyncio
    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))
    assert stats.v2_due_seen == 3
    assert stats.v2_processed == 0
    assert stats.v2_errors == 3


def test_v2_scheduler_end_to_end_persists_partial_maturation(tmp_path):
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    db = Database(f"sqlite:///{tmp_path / 'e2e.sqlite'}")
    db.create_all()
    repository = BotRepository(db)
    root_id = "episode-e2e:FIRST_V2_EARLY_ROOT"
    assert repository.record_root_detector_shadow_v2_root({
        "shadow_v2_root_id": root_id, "episode_id": "episode-e2e", "symbol": "AAAUSDT",
        "state": "EARLY_ROOT_CREATED", "created_at": anchor, "episode_opened_at": anchor,
        "episode_age_seconds": 0.0, "reference_price": 100.0, "event_high_at_decision": 105.0,
        "distance_from_high": 4.7, "pump_5m": 3.0, "pump_15m": 5.0, "pump_1h": 8.0,
        "pump_4h": 10.0, "volume_ratio": None, "volume_z": None, "oi_features_json": {},
        "safety_features_json": {}, "conditions_json": {"v1_trigger": "PASS"},
        "downstream_features_json": {}, "readiness_reason": "audited_v1_trigger",
        "live_root_present_at_creation": False, "code_version": "test",
        "runtime_instance_id": "runtime", "runtime_started_at": anchor,
        "method_version": "ROOT_DETECTOR_SHADOW_V2_CONTRACT_V1",
    })
    scheduler = ShadowOutcomeScheduler(
        client=FakeClient(_frame(anchor, count=13)), repository=repository,
        runtime_batch_size=10, legacy_batch_size=0, v2_enabled=True,
    )
    import asyncio
    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))
    assert stats.v2_due_seen == 1
    assert stats.v2_processed == 1
    assert stats.v2_errors == 0
    with db.session() as session:
        outcome = session.query(RootDetectorShadowV2OutcomeModel).one()
        root = session.query(RootDetectorShadowV2RootModel).one()
        assert outcome.outcome_status == "PARTIAL"
        assert outcome.outcome_attempt_count == 1
        assert outcome.outcome_next_due_at is not None
        assert root.shadow_v2_root_id == root_id


def test_scheduler_summary_named_sentinel_mapping(caplog):
    caplog.set_level("INFO")
    stats = SchedulerStats(
        runtime_due_seen=101, runtime_processed=102,
        legacy_due_seen=103, legacy_processed=104,
        symbols_fetched=105, bybit_requests=106,
        v2_due_seen=107, v2_processed=108, v2_errors=109,
        remaining_due=110, oldest_overdue_seconds=111.0,
        retries=112, errors=113, cycle_duration_ms=114.0,
    )
    emit_scheduler_summary(stats, v2_enabled=True)
    record = next(item for item in caplog.records if item.name == "app.observability.outcome_scheduler")
    summary = record.scheduler_summary
    assert summary == {
        "runtime_due_seen": 101, "runtime_processed": 102,
        "legacy_due_seen": 103, "legacy_processed": 104,
        "symbols_fetched": 105, "bybit_requests": 106,
        "v2_enabled": True, "v2_due_seen": 107, "v2_processed": 108, "v2_errors": 109,
        "live_root_due_seen": 0, "live_root_processed": 0, "live_root_errors": 0,
        "episodes_deferred": 0, "episodes_per_request": (102 + 104) / 106,
        "remaining_due": 110, "oldest_overdue_seconds": 111.0,
        "retries": 112, "errors": 113, "cycle_duration_ms": 114.0,
    }


def test_scheduler_summary_off_contract_uses_actual_zero_counters(caplog):
    caplog.set_level("INFO")
    stats = SchedulerStats(v2_due_seen=0, v2_processed=0, v2_errors=0, bybit_requests=5)
    emit_scheduler_summary(stats, v2_enabled=False)
    record = next(item for item in caplog.records if item.name == "app.observability.outcome_scheduler")
    assert record.scheduler_summary["v2_enabled"] is False
    assert record.scheduler_summary["v2_due_seen"] == stats.v2_due_seen == 0
    assert record.scheduler_summary["v2_processed"] == stats.v2_processed == 0
    assert record.scheduler_summary["v2_errors"] == stats.v2_errors == 0
