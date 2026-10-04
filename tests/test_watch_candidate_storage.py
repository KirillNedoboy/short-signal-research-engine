from __future__ import annotations

from datetime import datetime, timedelta, timezone
from math import nan

from sqlalchemy import select

from app.domain import SignalType
from app.storage.db import Database
from app.storage.models import SignalModel, TelegramDeliveryOutboxModel, WatchCandidateModel
from app.storage.repository import BotRepository


def test_watch_candidate_with_all_metrics_is_stored_separately(tmp_path, make_event_state, make_signal_decision) -> None:
    database = Database(f"sqlite:///{tmp_path / 'watch-all.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(
        signal_type=SignalType.WATCH,
        actionable=False,
        decision_type="WATCH",
        blockers=["thin_orderbook"],
        data_quality_warnings=[],
    )

    record = repository.save_watch_candidate(decision, state, telegram_sent=False)

    assert record.symbol == "ONTUSDT"
    assert record.actionable is False
    assert record.dist_to_vwap_pct == 13.0
    with database.session() as session:
        assert session.scalars(select(SignalModel)).all() == []
        stored = session.scalars(select(WatchCandidateModel)).one()
        assert stored.signal_type == "Watch"
        assert stored.dist_to_vwap_pct == 13.0


def test_watch_candidate_missing_dist_to_vwap_pct_is_stored_without_integrity_error(tmp_path, make_event_state, make_signal_decision) -> None:
    database = Database(f"sqlite:///{tmp_path / 'watch-missing.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    decision = make_signal_decision(
        signal_type=SignalType.WATCH,
        actionable=False,
        decision_type="WATCH",
        features_snapshot={
            "pullback_from_high_pct": 3.5,
            "dist_to_vwap_pct": nan,
            "upper_wick_ratio": 0.25,
            "rejection_from_high_pct": 1.6,
            "vol_zscore_30m": 2.0,
            "dist_to_ema20_atr": 4.5,
            "rsi_15m": 78.5,
            "ret_1h": 15.0,
            "ret_4h": 35.0,
            "range_atr_ratio": 2.2,
            "oi_change_15m": None,
            "oi_change_1h": None,
            "funding_rate": None,
            "spread_pct": 0.2,
            "orderbook_depth_usdt_1pct": 20000.0,
            "signal_vwap": 99.0,
        },
        blockers=["thin_orderbook"],
        data_quality_warnings=["derivatives_missing"],
    )

    record = repository.save_watch_candidate(decision, state, telegram_sent=False)

    assert record.dist_to_vwap_pct is None
    with database.session() as session:
        assert session.scalars(select(SignalModel)).all() == []
        stored = session.scalars(select(WatchCandidateModel)).one()
        assert stored.dist_to_vwap_pct is None
        assert stored.actionable is False


def test_delivery_status_can_be_updated_after_durable_insert(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'delivery-status.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    signal = repository.save_signal(make_signal_decision(), state, telegram_sent=False, provenance=make_signal_provenance())
    watch_decision = make_signal_decision(
        signal_type=SignalType.WATCH,
        actionable=False,
        decision_type="WATCH",
        blockers=["thin_orderbook"],
    )
    watch = repository.save_watch_candidate(watch_decision, state, telegram_sent=False)

    repository.update_signal_telegram_status(signal.id, True)
    repository.update_watch_telegram_status(watch.id, True)

    with database.session() as session:
        assert session.get(SignalModel, signal.id).telegram_sent is True
        assert session.get(WatchCandidateModel, watch.id).telegram_sent is True



def test_actionable_signal_still_persists_dist_to_vwap_pct(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'signal.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())

    record = repository.save_signal(make_signal_decision(), state, telegram_sent=True, provenance=make_signal_provenance())

    assert record.signal_type == "Aggressive"
    assert record.dist_to_vwap_pct == 13.0
    with database.session() as session:
        stored = session.scalars(select(SignalModel)).one()
        assert stored.dist_to_vwap_pct == 13.0
        assert session.scalars(select(WatchCandidateModel)).all() == []


def test_signal_delivery_outbox_is_atomic_and_retryable(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'outbox.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())

    signal = repository.save_signal(
        make_signal_decision(),
        state,
        telegram_sent=False,
        delivery_payload="signal payload",
        provenance=make_signal_provenance(),
    )

    with database.session() as session:
        outbox = session.scalars(select(TelegramDeliveryOutboxModel)).one()
        assert outbox.entity_type == "SIGNAL"
        assert outbox.entity_id == signal.id
        assert outbox.payload == "signal payload"
        assert outbox.status == "PENDING"
        assert outbox.idempotency_key == f"telegram:signal:{signal.id}"

    now = datetime.now(timezone.utc)
    claimed = repository.claim_due_deliveries(now=now, limit=1, lease_seconds=30)
    assert len(claimed) == 1
    assert claimed[0]["payload"] == "signal payload"
    repository.mark_delivery_retry(
        claimed[0]["id"],
        error="telegram unavailable",
        next_attempt_at=now + timedelta(minutes=1),
    )

    with database.session() as session:
        outbox = session.scalars(select(TelegramDeliveryOutboxModel)).one()
        assert outbox.status == "RETRY"
        assert outbox.attempt_count == 1
        assert outbox.last_error == "telegram unavailable"


def test_delivery_lease_expiry_requeues_item(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'lease.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    repository.save_signal(make_signal_decision(), state, telegram_sent=False, delivery_payload="payload", provenance=make_signal_provenance())
    now = datetime.now(timezone.utc)

    first = repository.claim_due_deliveries(now, limit=1, lease_seconds=1)
    assert first[0]["attempt_count"] == 1
    second = repository.claim_due_deliveries(now + timedelta(seconds=2), limit=1, lease_seconds=30)
    assert second[0]["id"] == first[0]["id"]
    assert second[0]["attempt_count"] == 2


def test_delivery_lease_expiry_after_max_attempts_becomes_dead(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'lease-dead.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    repository.save_signal(make_signal_decision(), state, telegram_sent=False, delivery_payload="payload", provenance=make_signal_provenance())
    now = datetime.now(timezone.utc)

    claimed = None
    for attempt in range(5):
        claimed = repository.claim_due_deliveries(now + timedelta(seconds=attempt * 2), limit=1, lease_seconds=1)
        assert len(claimed) == 1
    assert repository.claim_due_deliveries(now + timedelta(seconds=12), limit=1, lease_seconds=1) == []
    with database.session() as session:
        assert session.scalars(select(TelegramDeliveryOutboxModel)).one().status == "DEAD"


def test_delivery_success_updates_source_and_outbox_atomically(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'outbox-success.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    signal = repository.save_signal(
        make_signal_decision(), state, telegram_sent=False, delivery_payload="payload", provenance=make_signal_provenance()
    )
    claimed = repository.claim_due_deliveries(datetime.now(timezone.utc), limit=1, lease_seconds=30)

    repository.mark_delivery_sent(claimed[0]["id"])

    with database.session() as session:
        assert session.get(SignalModel, signal.id).telegram_sent is True
        assert session.scalars(select(TelegramDeliveryOutboxModel)).one().status == "SENT"


def test_legacy_unsent_signals_are_not_auto_enqueued(tmp_path, make_event_state, make_signal_decision, make_signal_provenance) -> None:
    database = Database(f"sqlite:///{tmp_path / 'legacy.db'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    repository.save_signal(make_signal_decision(model_version=None), state, telegram_sent=False, provenance=make_signal_provenance())

    assert repository.count_legacy_unsent_signals() == 1
    assert repository.claim_due_deliveries(datetime.now(timezone.utc), limit=10, lease_seconds=30) == []
