from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.infra.health import ServiceHealth
from app.main import ShortSignalBot
from app.storage.db import Database
from app.storage.repository import BotRepository


def test_process_alive_does_not_hide_stalled_scan():
    now = datetime(2026, 10, 5, tzinfo=timezone.utc)
    health = ServiceHealth()
    health.mark_process(now)
    health.mark_scan(now - timedelta(minutes=10), duration_ms=120)

    snapshot = health.snapshot(now=now, scan_stale_after_sec=60)

    assert snapshot["process_heartbeat"] == "HEALTHY"
    assert snapshot["scan_heartbeat"] == "STALE"
    assert snapshot["last_scan_duration_ms"] == 120.0


def test_healthy_scan_does_not_hide_stalled_outbox():
    now = datetime(2026, 10, 5, tzinfo=timezone.utc)
    health = ServiceHealth()
    health.mark_process(now)
    health.mark_scan(now, duration_ms=80)
    health.mark_outbox(now - timedelta(minutes=10))

    snapshot = health.snapshot(now=now, outbox_stale_after_sec=60)

    assert snapshot["scan_heartbeat"] == "HEALTHY"
    assert snapshot["outbox_health"] == "STALLED"


def test_fresh_outbox_observation_takes_precedence_over_old_progress():
    now = datetime(2026, 10, 5, tzinfo=timezone.utc)
    old_progress = now - timedelta(minutes=10)
    fresh_observation = now - timedelta(seconds=5)
    health = ServiceHealth(
        outbox_last_progress=old_progress,
        outbox_last_observed=fresh_observation,
    )

    snapshot = health.snapshot(now=now, outbox_stale_after_sec=60)

    assert snapshot["outbox_health"] == "HEALTHY"
    assert snapshot["outbox_last_observed"] == fresh_observation.isoformat()


def test_stale_outbox_progress_and_observation_remain_stalled():
    now = datetime(2026, 10, 5, tzinfo=timezone.utc)
    health = ServiceHealth(
        outbox_last_progress=now - timedelta(minutes=10),
        outbox_last_observed=now - timedelta(minutes=5),
    )

    snapshot = health.snapshot(now=now, outbox_stale_after_sec=60)

    assert snapshot["outbox_health"] == "STALLED"


def test_recovery_clears_only_relevant_degraded_component():
    now = datetime(2026, 10, 5, tzinfo=timezone.utc)
    health = ServiceHealth()
    health.mark_process(now)
    health.mark_scan(now - timedelta(minutes=10), duration_ms=120)
    health.mark_outbox(now - timedelta(minutes=10))
    health.mark_scan(now)

    snapshot = health.snapshot(now=now, scan_stale_after_sec=60, outbox_stale_after_sec=60)

    assert snapshot["scan_heartbeat"] == "HEALTHY"
    assert snapshot["outbox_health"] == "STALLED"


def test_health_projection_contains_no_private_identifiers():
    health = ServiceHealth()
    snapshot = health.snapshot(now=datetime.now(timezone.utc))
    assert "chat_id" not in snapshot
    assert "telegram" not in str(snapshot).lower()


def test_event_loop_lag_is_carried_into_the_health_projection():
    health = ServiceHealth(last_event_loop_lag_ms=12.5)

    assert health.snapshot()["last_event_loop_lag_ms"] == 12.5


def test_repository_health_projection_is_json_ready(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'health.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    checked_at = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)

    repository.update_runtime_health(
        checked_at=checked_at,
        process_last_seen=checked_at,
        last_event_loop_lag_ms=7.25,
    )

    projection = repository.get_runtime_health()
    assert projection["process_last_seen"] == checked_at.isoformat()
    assert projection["last_event_loop_lag_ms"] == 7.25


def test_empty_fast_monitor_poll_persists_completion_and_health(tmp_path, monkeypatch):
    database = Database(f"sqlite:///{tmp_path / 'fast-monitor.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    bot = ShortSignalBot.__new__(ShortSignalBot)
    bot._fast_monitor_running = True
    bot._active_climax_pool = {}
    bot._fast_monitor_poll_sequence = 0
    bot._repository = repository
    bot._health = ServiceHealth()
    bot._config = SimpleNamespace(climax_fast_poll_sec=0.1)
    bot._logger = SimpleNamespace(info=lambda *_args, **_kwargs: None)

    sleeps = 0

    async def fake_sleep(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps == 2:
            bot._fast_monitor_running = False

    monkeypatch.setattr("app.main.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("app.main.time.perf_counter", iter([10.0, 10.125, 20.0, 20.125]).__next__)

    import asyncio

    asyncio.run(bot._run_fast_monitor())

    projection = repository.get_runtime_health()
    assert projection["fast_monitor_last_complete"] is not None
    assert projection["last_event_loop_lag_ms"] == pytest.approx(25.0)


def test_idle_outbox_poll_marks_queue_checked_without_delivery(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'outbox-idle.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    bot = ShortSignalBot.__new__(ShortSignalBot)
    bot._repository = repository
    bot._health = ServiceHealth()
    bot._config = SimpleNamespace(send_watch_to_telegram=False, watch_max_per_cycle=0)
    bot._watch_sent_in_cycle = 0

    import asyncio

    assert asyncio.run(bot._drain_delivery_outbox(limit=5)) == 0

    projection = repository.get_runtime_health()
    assert projection["outbox_last_progress"] is None
    assert projection["outbox_last_observed"] is not None
    assert projection["outbox_health"] == "HEALTHY"


def test_failed_due_outbox_attempt_does_not_refresh_progress():
    class _Repository:
        def __init__(self):
            self.retries = []

        def claim_due_deliveries(self, *_args, **_kwargs):
            return [{"id": 7, "payload": "payload"}]

        def mark_delivery_retry(self, delivery_id, **kwargs):
            self.retries.append((delivery_id, kwargs))

        def update_runtime_health(self, **kwargs):
            self.persisted = kwargs

    async def _send_signal(_payload):
        return False

    repository = _Repository()
    bot = ShortSignalBot.__new__(ShortSignalBot)
    bot._repository = repository
    bot._health = ServiceHealth(outbox_last_progress=datetime(2026, 10, 5, tzinfo=timezone.utc))
    bot._config = SimpleNamespace(send_watch_to_telegram=False, watch_max_per_cycle=0)
    bot._watch_sent_in_cycle = 0
    bot._notifier = SimpleNamespace(send_signal=_send_signal)

    import asyncio

    assert asyncio.run(bot._drain_delivery_outbox(limit=5)) == 0
    assert bot._health.outbox_last_progress == datetime(2026, 10, 5, tzinfo=timezone.utc)
    assert bot._health.outbox_last_observed is None
    assert repository.persisted["outbox_health"] == "STALLED"
    assert repository.retries[0][0] == 7
