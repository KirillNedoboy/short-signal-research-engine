from datetime import datetime, timedelta, timezone

import asyncio

from app.observability.outcome_scheduler import ShadowOutcomeScheduler
from app.storage.db import Database
from app.storage.models import CurrentRootOutcomeModel
from app.storage.repository import BotRepository

UTC = timezone.utc


class _Client:
    async def fetch_klines(self, symbol, interval, limit, start_ms, end_ms):
        start = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
        return [
            {
                "timestamp": start + timedelta(minutes=5 * index),
                "close_time": start + timedelta(minutes=5 * (index + 1)),
                "open": 100.0,
                "high": 102.0,
                "low": 98.0,
                "close": 99.0,
                "volume": 1.0,
            }
            for index in range(7)
        ]


def test_scheduler_matures_live_root_ledger_without_using_episode_anchor(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'telemetry.sqlite'}")
    db.create_all()
    repository = BotRepository(db)
    anchor = datetime(2026, 1, 1, tzinfo=UTC)
    repository.upsert_shadow_root_event(
        root_event_id="root-live-1",
        symbol="AAAUSDT",
        event_started_at=anchor - timedelta(minutes=30),
        event_base_price=100.0,
        peak_high=105.0,
        peak_high_time=anchor,
        initial_extension_pct=5.0,
        initial_extension_source="test",
        observed_at=anchor,
    )

    scheduler = ShadowOutcomeScheduler(
        client=_Client(), repository=repository,
        runtime_batch_size=10, legacy_batch_size=0, v2_enabled=False,
    )
    stats = asyncio.run(scheduler.run_cycle(now=anchor + timedelta(hours=1)))

    assert stats.live_root_due_seen == 1
    assert stats.live_root_processed == 1
    with db.session() as session:
        row = session.get(CurrentRootOutcomeModel, "root-live-1")
        assert row is not None
        assert row.anchor_type == "LIVE_ROOT"
        assert row.anchor_price == 100.0
        assert row.outcome_status == "DATA_GAP"
        assert row.outcome_next_due_at is None


def test_live_root_outcome_retryable_error_is_explicit(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'telemetry.sqlite'}")
    db.create_all()
    repository = BotRepository(db)
    anchor = datetime(2026, 1, 1, tzinfo=UTC)

    assert repository.record_live_root_outcome_attempt(
        root_event_id="root-live-2", symbol="AAAUSDT", anchor_time=anchor,
        anchor_price=100.0, outcome={"anchor_type": "LIVE_ROOT", "outcome_status": "RETRYABLE_ERROR"},
        error="provider timeout", next_due_at=anchor + timedelta(minutes=2), computed_at=anchor,
    )
    with db.session() as session:
        row = session.get(CurrentRootOutcomeModel, "root-live-2")
        assert row is not None
        assert row.outcome_status == "RETRYABLE_ERROR"
        assert row.outcome_last_error == "provider timeout"
        assert row.outcome_next_due_at is not None
