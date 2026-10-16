from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.config import AppConfig
from app.domain import EventStatus, ShortZone, SignalType
from app.signals.climax import evaluate_climax
from app.signals.engine import SignalEngine
from app.storage.db import Database
from app.storage.models import SignalProvenanceModel, SignalModel, TelegramDeliveryOutboxModel
from app.storage.repository import BotRepository

from tests.contracts.helpers import climax_config, ready_low_volume_inputs


FIXED_TIME = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)


def _baseline_evaluation(make_event_state, make_features, **overrides):
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    features = make_features(asof=FIXED_TIME, **overrides)
    return SignalEngine(AppConfig()).analyze(
        state,
        features,
        ShortZone(low=110.5, high=113.8, mode="event_range"),
        FIXED_TIME,
    )


def test_grade_c_never_becomes_public_signal(make_event_state, make_features):
    state = make_event_state(state=EventStatus.PULLBACK_OBSERVED, zone_low=110.5, zone_high=113.8)
    features = make_features(
        asof=FIXED_TIME,
        ret_15m=7.0,
        ret_1h=9.0,
        ret_4h=21.0,
        dist_to_ema20_atr=1.8,
        upper_wick_ratio=0.16,
        rejection_from_high_pct=0.9,
        close_position_in_range=0.55,
        vol_zscore_30m=1.4,
        range_atr_ratio=1.3,
        pullback_from_high_pct=3.9,
        latest_failed_retest=False,
        funding_rate=-0.0009,
        oi_change_15m=1.2,
        oi_change_1h=0.6,
    )
    evaluation = SignalEngine(AppConfig(enable_watch_candidates=True, watch_min_score=50)).analyze(
        state,
        features,
        ShortZone(low=110.5, high=113.8, mode="event_range"),
        FIXED_TIME,
    )

    assert evaluation.grade == "C"
    assert evaluation.decision is not None
    assert evaluation.decision.signal_type is SignalType.WATCH
    assert evaluation.decision.actionable is False


def test_explicit_bad_liquidity_never_becomes_actionable(make_event_state, make_features):
    evaluation = _baseline_evaluation(
        make_event_state,
        make_features,
        orderbook_depth_usdt_1pct=1000.0,
        orderbook_depth_usdt_2pct=2000.0,
        spread_pct=0.5,
        slippage_pct=0.5,
    )

    assert evaluation.decision is None
    assert "spread_too_wide" in evaluation.reject_reasons


def test_dangerous_low_volume_squeeze_remains_blocked(make_features, make_event_state, make_frame):
    state, features, frame = ready_low_volume_inputs(
        make_features,
        make_event_state,
        make_frame,
        frame_prices=[100.0 + index for index in range(30)],
        ret_5m=7.0,
        latest_failed_retest=False,
        oi_change_pct=-5.0,
    )
    result = evaluate_climax(
        state,
        features,
        frame,
        climax_config(volume_climax_unwind_enabled=False),
    )

    assert result.actionable is False
    assert "active_short_squeeze" in result.veto_reasons


def test_same_frozen_strategy_input_is_deterministic(make_event_state, make_features):
    first = _baseline_evaluation(make_event_state, make_features)
    second = _baseline_evaluation(make_event_state, make_features)

    assert first.decision == second.decision
    assert first.score == second.score
    assert first.grade == second.grade
    assert first.reject_reasons == second.reject_reasons


def test_unknown_config_keys_are_currently_ignored():
    config = AppConfig.model_validate({"phase0_unknown_key": "ignored"})

    assert not hasattr(config, "phase0_unknown_key")


def test_signal_persistence_precedes_outbox_claim_and_keeps_provenance(
    tmp_path,
    make_event_state,
    make_signal_decision,
    make_signal_provenance,
):
    database = Database(f"sqlite:///{tmp_path / 'global-boundaries.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    signal = repository.save_signal(
        make_signal_decision(),
        state,
        telegram_sent=False,
        delivery_payload="frozen payload",
        provenance=make_signal_provenance(),
    )

    with database.session() as session:
        stored_signal = session.get(SignalModel, signal.id)
        stored_provenance = session.get(SignalProvenanceModel, signal.id)
        outbox = session.query(TelegramDeliveryOutboxModel).one()
    assert stored_signal is not None
    assert stored_provenance is not None
    assert stored_provenance.strategy_branch == "BASELINE_PULLBACK"
    assert outbox.status == "PENDING"
    assert outbox.idempotency_key == f"telegram:signal:{signal.id}"
    now = datetime.now(timezone.utc)
    claimed = repository.claim_due_deliveries(now, limit=1, lease_seconds=30)
    assert [item["id"] for item in claimed] == [outbox.id]
    assert repository.claim_due_deliveries(now, limit=1, lease_seconds=30) == []

    repository.mark_delivery_retry(
        outbox.id,
        error="transport unavailable",
        next_attempt_at=now + timedelta(minutes=1),
    )
    with database.session() as session:
        assert session.get(TelegramDeliveryOutboxModel, outbox.id).status == "RETRY"


def test_outbox_lease_expiry_allows_at_least_once_retry(tmp_path, make_event_state, make_signal_decision, make_signal_provenance):
    database = Database(f"sqlite:///{tmp_path / 'lease-retry.sqlite'}")
    database.create_all()
    repository = BotRepository(database)
    state = repository.upsert_event_state(make_event_state())
    repository.save_signal(
        make_signal_decision(),
        state,
        telegram_sent=False,
        delivery_payload="retry payload",
        provenance=make_signal_provenance(),
    )

    now = datetime.now(timezone.utc)
    first = repository.claim_due_deliveries(now, limit=1, lease_seconds=1)
    second = repository.claim_due_deliveries(now + timedelta(seconds=2), limit=1, lease_seconds=30)

    assert len(first) == 1
    assert second[0]["id"] == first[0]["id"]
    assert second[0]["attempt_count"] == 2
