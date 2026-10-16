from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.domain import SignalType
from app.runtime.delivery_worker import DeliveryWorker
from app.storage.db import Database
from app.storage.models import TelegramDeliveryOutboxModel
from app.storage.repository import BotRepository


class _Notifier:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.messages: list[str] = []
        self.alerts: list[str] = []

    async def send_signal(self, message: str) -> bool:
        if self.fail:
            raise RuntimeError("transport down")
        self.messages.append(message)
        return True

    async def send_alert(self, message: str) -> bool:
        self.alerts.append(message)
        return True


class _Throttler:
    def should_send(self, _key: str) -> bool:
        return True


def test_due_retry_is_delivered_while_scan_is_blocked(
    tmp_path, make_event_state, make_signal_decision, make_signal_provenance
) -> None:
    async def scenario() -> tuple[list[str], str]:
        database = Database(f"sqlite:///{tmp_path / 'worker.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        signal = repository.save_signal(
            make_signal_decision(), state, telegram_sent=False, delivery_payload="queued", provenance=make_signal_provenance()
        )
        claimed = repository.claim_due_deliveries(datetime.now(timezone.utc), limit=1, lease_seconds=120)
        repository.mark_delivery_retry(claimed[0]["id"], error="old", next_attempt_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        worker = DeliveryWorker(repository, _Notifier(), _Throttler(), send_watch_to_telegram=False)
        blocked_scan = asyncio.Event()
        drain = asyncio.create_task(worker.drain_once())
        await asyncio.wait_for(drain, timeout=1)
        with database.session() as session:
            status = session.scalars(select(TelegramDeliveryOutboxModel.status)).one()
        assert not blocked_scan.is_set()
        return worker._notifier.messages, status

    messages, status = asyncio.run(scenario())
    assert messages == ["queued"]
    assert status == "SENT"


def test_disabled_watch_rows_remain_unclaimed(tmp_path, make_event_state, make_signal_decision) -> None:
    async def scenario() -> str:
        database = Database(f"sqlite:///{tmp_path / 'watch.db'}")
        database.create_all()
        repository = BotRepository(database)
        state = repository.upsert_event_state(make_event_state())
        decision = make_signal_decision(signal_type=SignalType.WATCH, actionable=False, decision_type="WATCH")
        repository.save_watch_candidate(decision, state, telegram_sent=False, delivery_payload="watch")
        worker = DeliveryWorker(repository, _Notifier(), _Throttler(), send_watch_to_telegram=False)
        assert await worker.drain_once() == 0
        with database.session() as session:
            return session.scalars(select(TelegramDeliveryOutboxModel.status)).one()

    assert asyncio.run(scenario()) == "PENDING"


def test_stop_cancels_worker_task() -> None:
    async def scenario() -> None:
        worker = DeliveryWorker(object(), _Notifier(), _Throttler(), send_watch_to_telegram=False)
        worker.start()
        assert worker._task is not None
        await worker.stop()
        assert worker._task is None

    asyncio.run(scenario())


def test_acknowledged_dead_row_stays_auditable_but_new_dead_row_alerts(monkeypatch) -> None:
    monkeypatch.setenv("DELIVERY_WATCHDOG_ACK_DEAD", "31:13")

    class _Repository:
        def audit_delivery_funnel(self, now):
            return [
                {"kind": "dead_delivery", "outbox_id": 31, "signal_id": 13},
                {"kind": "dead_delivery", "outbox_id": 32, "signal_id": 14},
            ]

    notifier = _Notifier()
    worker = DeliveryWorker(_Repository(), notifier, _Throttler(), send_watch_to_telegram=False)
    asyncio.run(worker._audit_delivery_funnel())
    assert notifier.alerts == ["Delivery funnel invariant violation: dead_delivery=1"]
    assert len(_Repository().audit_delivery_funnel(None)) == 2


def test_acknowledged_dead_row_does_not_mask_other_violations(monkeypatch) -> None:
    monkeypatch.setenv("DELIVERY_WATCHDOG_ACK_DEAD", "31:13")

    class _Repository:
        def audit_delivery_funnel(self, now):
            return [
                {"kind": "dead_delivery", "outbox_id": 31, "signal_id": 13},
                {"kind": "dead_delivery", "outbox_id": 31, "signal_id": 99},
                {"kind": "signal_without_provenance", "signal_id": 13},
            ]

    notifier = _Notifier()
    worker = DeliveryWorker(_Repository(), notifier, _Throttler(), send_watch_to_telegram=False)
    asyncio.run(worker._audit_delivery_funnel())
    assert notifier.alerts == [
        "Delivery funnel invariant violation: dead_delivery=1, signal_without_provenance=1"
    ]


def test_only_acknowledged_dead_row_sends_no_alert_after_restart(monkeypatch) -> None:
    monkeypatch.setenv("DELIVERY_WATCHDOG_ACK_DEAD", "31:13")

    class _Repository:
        def audit_delivery_funnel(self, now):
            return [{"kind": "dead_delivery", "outbox_id": 31, "signal_id": 13}]

    notifier = _Notifier()
    for _ in range(2):
        worker = DeliveryWorker(_Repository(), notifier, _Throttler(), send_watch_to_telegram=False)
        asyncio.run(worker._audit_delivery_funnel())
    assert notifier.alerts == []


def test_invalid_acknowledgement_fails_open(monkeypatch) -> None:
    monkeypatch.setenv("DELIVERY_WATCHDOG_ACK_DEAD", "31:not-a-signal")

    class _Repository:
        def audit_delivery_funnel(self, now):
            return [{"kind": "dead_delivery", "outbox_id": 31, "signal_id": 13}]

    notifier = _Notifier()
    worker = DeliveryWorker(_Repository(), notifier, _Throttler(), send_watch_to_telegram=False)
    asyncio.run(worker._audit_delivery_funnel())
    assert notifier.alerts == ["Delivery funnel invariant violation: dead_delivery=1"]
