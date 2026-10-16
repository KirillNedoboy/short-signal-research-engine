from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.observability.delivery_funnel import audit_delivery_funnel


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
        "signal_without_provenance",
        "signal_without_provenance",
        "signal_without_outbox",
        "outbox_without_signal",
        "expired_in_flight",
        "dead_delivery",
    ]
    assert result[-1]["outbox_id"] == 11


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
