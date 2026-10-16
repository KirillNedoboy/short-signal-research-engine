"""Read-only delivery funnel invariant checks."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable


def audit_delivery_funnel(
    *,
    signals: Iterable[dict[str, Any]],
    provenances: Iterable[dict[str, Any]],
    outbox: Iterable[dict[str, Any]],
    now: datetime,
) -> list[dict[str, Any]]:
    """Return durable delivery-chain violations without writing telemetry rows."""
    signal_rows = {int(row["id"]): row for row in signals}
    provenance_ids = {int(row["signal_id"]) for row in provenances}
    signal_outbox = {
        int(row["entity_id"]): row
        for row in outbox
        if row.get("entity_type") == "SIGNAL"
    }
    findings: list[dict[str, Any]] = []

    for signal_id in sorted(signal_rows):
        if signal_id not in provenance_ids:
            findings.append(
                {"kind": "signal_without_provenance", "signal_id": signal_id}
            )
        delivery = signal_outbox.get(signal_id)
        if delivery is None:
            findings.append(
                {"kind": "signal_without_outbox", "signal_id": signal_id}
            )
        elif signal_rows[signal_id].get("telegram_sent") and delivery.get("status") != "SENT":
            findings.append(
                {
                    "kind": "sent_flag_mismatch",
                    "signal_id": signal_id,
                    "outbox_id": int(delivery["id"]),
                }
            )

    signal_outbox_rows = [
        row for row in outbox if row.get("entity_type") == "SIGNAL"
    ]
    for row in signal_outbox_rows:
        entity_id = int(row["entity_id"])
        if entity_id not in signal_rows:
            findings.append(
                {
                    "kind": "outbox_without_signal",
                    "outbox_id": int(row["id"]),
                    "signal_id": entity_id,
                }
            )
    for row in signal_outbox_rows:
        lease_until = row.get("lease_until")
        if row.get("status") == "IN_FLIGHT" and lease_until is not None and lease_until <= now:
            findings.append(
                {
                    "kind": "expired_in_flight",
                    "outbox_id": int(row["id"]),
                    "signal_id": int(row["entity_id"]),
                }
            )
    for row in signal_outbox_rows:
        if row.get("status") == "DEAD":
            findings.append(
                {
                    "kind": "dead_delivery",
                    "outbox_id": int(row["id"]),
                    "signal_id": int(row["entity_id"]),
                }
            )

    return findings
