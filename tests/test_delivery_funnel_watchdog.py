from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from app.observability.delivery_funnel import audit_delivery_funnel
from app.storage.db import Database
from app.storage.models import StrategyObservationModel
from app.storage.repository import BotRepository


def test_clean_delivery_funnel_has_no_violations() -> None:
    now = datetime.now(timezone.utc)

    result = audit_delivery_funnel(
        signals=[{"id": 1, "telegram_sent": True}],
        provenances=[{"signal_id": 1}],
        outbox=[
            {
                "id": 10,
                "entity_type": "SIGNAL",
                "entity_id": 1,
                "status": "SENT",
                "lease_until": None,
            }
        ],
        now=now,
    )

    assert result == []


def test_watchdog_finds_signal_breaks_and_expired_delivery_lease() -> None:
    now = datetime.now(timezone.utc)

    result = audit_delivery_funnel(
        signals=[
            {"id": 1, "telegram_sent": False},
            {"id": 2, "telegram_sent": False},
        ],
        provenances=[],
        outbox=[
            {
                "id": 10,
                "entity_type": "SIGNAL",
                "entity_id": 1,
                "status": "IN_FLIGHT",
                "lease_until": now - timedelta(seconds=1),
            },
            {
                "id": 11,
                "entity_type": "SIGNAL",
                "entity_id": 99,
                "status": "DEAD",
                "lease_until": None,
            },
        ],
        now=now,
    )

    assert [item["kind"] for item in result] == [
        "dead_delivery",
        "expired_in_flight",
        "outbox_without_signal",
        "signal_without_outbox",
        "signal_without_provenance",
        "signal_without_provenance",
    ]
    assert result[0]["outbox_id"] == 11


def test_watchdog_detects_sent_flag_mismatch() -> None:
    result = audit_delivery_funnel(
        signals=[{"id": 1, "telegram_sent": True}],
        provenances=[{"signal_id": 1}],
        outbox=[
            {
                "id": 10,
                "entity_type": "SIGNAL",
                "entity_id": 1,
                "status": "RETRY",
                "lease_until": None,
            }
        ],
        now=datetime.now(timezone.utc),
    )

    assert [item["kind"] for item in result] == ["sent_flag_mismatch"]
    assert result[0]["signal_id"] == 1


def test_watchdog_sanitizes_malformed_sent_flag_mismatch_outbox_id() -> None:
    result = audit_delivery_funnel(
        signals=[{"id": 1, "telegram_sent": True}],
        provenances=[{"signal_id": 1}],
        outbox=[
            {
                "id": "bad",
                "entity_type": "SIGNAL",
                "entity_id": 1,
                "status": "RETRY",
                "lease_until": None,
            }
        ],
        now=datetime.now(timezone.utc),
    )

    assert result == [{"kind": "malformed_input", "source": "outbox"}]


def test_watchdog_detects_duplicate_signal_identity() -> None:
    result = audit_delivery_funnel(
        signals=[
            {"id": 1, "signal_identity": "duplicate", "identity_required": True},
            {"id": 2, "signal_identity": "duplicate", "identity_required": True},
        ],
        provenances=[{"signal_id": 1}, {"signal_id": 2}],
        outbox=[
            {"id": 10, "entity_type": "SIGNAL", "entity_id": 1, "status": "SENT"},
            {"id": 11, "entity_type": "SIGNAL", "entity_id": 2, "status": "SENT"},
        ],
        now=datetime.now(timezone.utc),
    )

    assert [item["kind"] for item in result] == ["signal_identity_duplicate"]
    assert result[0]["signal_ids"] == [1, 2]


def test_watchdog_detects_missing_identity_on_required_signal() -> None:
    result = audit_delivery_funnel(
        signals=[{"id": 1, "signal_identity": None, "identity_required": True}],
        provenances=[{"signal_id": 1}],
        outbox=[{"id": 10, "entity_type": "SIGNAL", "entity_id": 1, "status": "SENT"}],
        now=datetime.now(timezone.utc),
    )

    assert result == [{"kind": "signal_identity_missing", "signal_id": 1}]


def test_watchdog_detects_signal_without_matching_event_state_link() -> None:
    result = audit_delivery_funnel(
        signals=[
            {
                "id": 1,
                "symbol": "BTCUSDT",
                "event_id": "event-1",
                "identity_required": False,
            }
        ],
        provenances=[{"signal_id": 1}],
        outbox=[{"id": 10, "entity_type": "SIGNAL", "entity_id": 1, "status": "SENT"}],
        event_states=[
            {"symbol": "BTCUSDT", "event_id": "event-1", "signal_id": None}
        ],
        now=datetime.now(timezone.utc),
    )

    assert result == [{"kind": "signal_without_event_link", "signal_id": 1}]


def test_watchdog_requires_exactly_one_terminal_final_outcome() -> None:
    result = audit_delivery_funnel(
        signals=[],
        provenances=[],
        outbox=[],
        final_audits=[
            {
                "observation_id": "initial-1",
                "evaluation_phase": "INITIAL_ACTIONABLE",
                "event_id": "event-1",
                "symbol": "BTCUSDT",
            },
            {
                "observation_id": "final-1",
                "evaluation_phase": "PRE_DELIVERY_RECHECK",
                "event_id": "event-1",
                "symbol": "BTCUSDT",
                "terminal": False,
            },
        ],
        now=datetime.now(timezone.utc),
    )

    assert result == [
        {
            "kind": "initial_actionable_without_final_outcome",
            "observation_id": "initial-1",
            "final_outcome_count": 0,
        }
    ]


def test_watchdog_clean_case_covers_all_ten_invariant_kinds() -> None:
    result = audit_delivery_funnel(
        signals=[
            {
                "id": 1,
                "symbol": "BTCUSDT",
                "event_id": "event-1",
                "signal_identity": "identity-1",
                "identity_required": True,
                "telegram_sent": True,
            }
        ],
        provenances=[{"signal_id": 1}],
        outbox=[
            {
                "id": 10,
                "entity_type": "SIGNAL",
                "entity_id": 1,
                "status": "SENT",
                "lease_until": None,
            }
        ],
        event_states=[
            {"symbol": "BTCUSDT", "event_id": "event-1", "signal_id": 1}
        ],
        now=datetime.now(timezone.utc),
    )

    assert result == []


def test_production_initial_actionable_requires_terminal_final_outcome() -> None:
    result = audit_delivery_funnel(
        signals=[], provenances=[], outbox=[],
        final_audits=[{
            "observation_id": "initial-1", "evaluation_id": 42,
            "evaluation_phase": "INITIAL", "live_decision": "ACTIONABLE",
        }], now=datetime.now(timezone.utc),
    )
    assert result == [{
        "kind": "initial_actionable_without_final_outcome",
        "observation_id": "initial-1", "final_outcome_count": 0,
    }]


def test_production_initial_actionable_with_one_final_is_clean() -> None:
    result = audit_delivery_funnel(
        signals=[], provenances=[], outbox=[],
        final_audits=[
            {"observation_id": "initial-1", "evaluation_id": 42,
             "evaluation_phase": "INITIAL", "live_decision": "ACTIONABLE"},
            {"observation_id": "final-1", "evaluation_id": 43,
             "initial_evaluation_id": 42, "evaluation_phase": "FINAL",
             "outcome_status": "COMPLETED"},
        ], now=datetime.now(timezone.utc),
    )
    assert result == []


def test_watchdog_finding_order_is_input_permutation_invariant() -> None:
    import itertools

    signals = [{"id": 2, "telegram_sent": False}, {"id": 1, "telegram_sent": False}]
    outbox = [{"id": 12, "entity_type": "SIGNAL", "entity_id": 9, "status": "DEAD"}]
    expected = audit_delivery_funnel(
        signals=signals, provenances=[], outbox=outbox,
        now=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    for signal_order, outbox_order in itertools.product(
        itertools.permutations(signals), itertools.permutations(outbox)
    ):
        assert audit_delivery_funnel(
            signals=signal_order, provenances=[], outbox=outbox_order,
            now=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ) == expected


def test_watchdog_sanitizes_malformed_rows() -> None:
    result = audit_delivery_funnel(
        signals=[{"id": "not-an-int"}], provenances=[{"signal_id": object()}],
        outbox=[{"id": 1, "entity_type": "SIGNAL", "entity_id": 1,
                 "status": "IN_FLIGHT", "lease_until": "naive-string"}],
        now=datetime.now(timezone.utc),
    )
    assert result == [
        {"kind": "malformed_input", "source": "outbox"},
        {"kind": "malformed_input", "source": "provenances"},
        {"kind": "malformed_input", "source": "signals"},
        {"kind": "outbox_without_signal", "outbox_id": 1, "signal_id": 1},
    ]


def test_repository_audit_reads_persisted_production_initial_without_writes(tmp_path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'watchdog.db'}")
    database.create_all()
    repository = BotRepository(database)
    observed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with database.session() as session:
        session.add(StrategyObservationModel(
            observation_id="initial-1", idempotency_key="initial-key", run_id="run-1",
            runtime_instance_id="runtime-1", strategy_family="CLIMAX_EXHAUSTION",
            strategy="VOLUME_CLIMAX_UNWIND", evaluation_phase="INITIAL",
            symbol="BTCUSDT", event_id="event-1", evaluation_id=42,
            observed_at=observed_at, live_decision="ACTIONABLE",
            shadow_decision="NOT_EVALUATED", model_version="test",
            config_hash="a" * 64, input_fingerprint="b" * 64,
        ))
    with database.session() as session:
        before = session.execute(text("select count(*) from strategy_observations")).scalar_one()
    result = repository.audit_delivery_funnel(now=observed_at)
    with database.session() as session:
        after = session.execute(text("select count(*) from strategy_observations")).scalar_one()
    assert before == after == 1
    assert result == [{
        "kind": "initial_actionable_without_final_outcome",
        "observation_id": "initial-1", "final_outcome_count": 0,
    }]
