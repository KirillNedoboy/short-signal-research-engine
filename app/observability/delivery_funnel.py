"""Read-only delivery funnel invariant checks."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable


_TERMINAL_LIFECYCLE_STATES = {
    "BLOCKED_BY_RECHECK",
    "DEDUPLICATED",
    "PERSISTENCE_FAILED",
    "SENT",
    "FINAL_ACTIONABLE",
}


def _identity_required(row: dict[str, Any]) -> bool:
    """Return the explicit policy marker for identity coverage."""
    for key in (
        "identity_required",
        "requires_signal_identity",
        "is_new",
        "newly_written",
        "is_current",
        "current_signal",
        "current",
    ):
        if key in row:
            return bool(row[key])
    return False


def _is_initial_actionable(row: dict[str, Any]) -> bool:
    return (
        row.get("evaluation_phase") == "INITIAL_ACTIONABLE"
        or row.get("lifecycle_state") == "INITIAL_ACTIONABLE"
        or row.get("initial_decision") == "ACTIONABLE"
        or (
            row.get("evaluation_phase") == "INITIAL"
            and row.get("live_decision") == "ACTIONABLE"
            and row.get("evaluation_id") is not None
        )
    )


def _is_terminal_final(row: dict[str, Any]) -> bool:
    if "terminal" in row:
        return bool(row["terminal"])
    if row.get("lifecycle_state") in _TERMINAL_LIFECYCLE_STATES:
        return True
    if row.get("evaluation_phase") in {"FINAL", "FINAL_ACTIONABLE", "FINAL_OUTCOME"}:
        return True
    return bool(
        (row.get("final_decision") and row.get("final_reason"))
        or row.get("outcome_status")
        or row.get("finalized_at")
    )


def _same_audit_chain(initial: dict[str, Any], final: dict[str, Any]) -> bool:
    """Match final audit rows using the strongest available durable identity."""
    initial_evaluation_id = initial.get("evaluation_id")
    if initial_evaluation_id is not None:
        return final.get("initial_evaluation_id") == initial_evaluation_id
    keys = ("signal_id", "symbol", "event_id", "root_event_id", "strategy")
    present = [key for key in keys if initial.get(key) is not None]
    if initial.get("observation_id") is not None and final.get("initial_observation_id") is not None:
        return final.get("initial_observation_id") == initial.get("observation_id")
    return bool(present) and all(final.get(key) == initial.get(key) for key in present)


def _finding_sort_key(finding: dict[str, Any]) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Return a deterministic, sanitized ordering key for a finding."""
    return (
        str(finding.get("kind", "malformed_input")),
        tuple(
            sorted(
                (str(key), str(value))
                for key, value in finding.items()
                if key != "kind"
            )
        ),
    )


def audit_delivery_funnel(
    *,
    signals: Iterable[dict[str, Any]],
    provenances: Iterable[dict[str, Any]],
    outbox: Iterable[dict[str, Any]],
    now: datetime,
    event_states: Iterable[dict[str, Any]] | None = None,
    final_audits: Iterable[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Return durable delivery-chain violations without writing telemetry rows.

    ``event_states`` and ``final_audits`` are optional so older callers retain
    their original audit scope.  Their rows are observations only; this
    function never repairs or persists any of them.
    """
    signal_list = list(signals)
    findings: list[dict[str, Any]] = []
    signal_rows: dict[int, dict[str, Any]] = {}
    for row in signal_list:
        try:
            signal_id = int(row["id"])
        except (KeyError, TypeError, ValueError):
            findings.append({"kind": "malformed_input", "source": "signals"})
            continue
        signal_rows[signal_id] = row
    provenance_ids: set[int] = set()
    for row in provenances:
        try:
            provenance_ids.add(int(row["signal_id"]))
        except (KeyError, TypeError, ValueError):
            findings.append({"kind": "malformed_input", "source": "provenances"})
    outbox_list = list(outbox)
    signal_outbox: dict[int, dict[str, Any]] = {}
    for row in outbox_list:
        if row.get("entity_type") != "SIGNAL":
            continue
        try:
            signal_outbox[int(row["entity_id"])] = row
        except (KeyError, TypeError, ValueError):
            findings.append({"kind": "malformed_input", "source": "outbox"})

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
            try:
                outbox_id = int(delivery["id"])
            except (KeyError, TypeError, ValueError):
                continue
            findings.append(
                {
                    "kind": "sent_flag_mismatch",
                    "signal_id": signal_id,
                    "outbox_id": outbox_id,
                }
            )

    identities: dict[str, list[int]] = defaultdict(list)
    for signal_id in sorted(signal_rows):
        row = signal_rows[signal_id]
        identity = row.get("signal_identity")
        if identity is not None and str(identity) != "":
            identities[str(identity)].append(signal_id)
        if _identity_required(row) and (identity is None or str(identity) == ""):
            findings.append({"kind": "signal_identity_missing", "signal_id": signal_id})
    for identity in sorted(identities):
        signal_ids = identities[identity]
        if len(signal_ids) > 1:
            findings.append(
                {
                    "kind": "signal_identity_duplicate",
                    "signal_identity": identity,
                    "signal_ids": signal_ids,
                }
            )

    event_state_rows = list(event_states) if event_states is not None else None
    if event_state_rows is not None:
        for signal_id in sorted(signal_rows):
            signal = signal_rows[signal_id]
            if signal.get("event_id") is None or signal.get("symbol") is None:
                continue
            linked = any(
                row.get("symbol") == signal.get("symbol")
                and row.get("event_id") == signal.get("event_id")
                and row.get("signal_id") == signal_id
                for row in event_state_rows
            )
            if not linked:
                findings.append(
                    {"kind": "signal_without_event_link", "signal_id": signal_id}
                )
    else:
        for signal_id in sorted(signal_rows):
            signal = signal_rows[signal_id]
            if "event_state_signal_id" in signal and signal.get("event_state_signal_id") != signal_id:
                findings.append(
                    {"kind": "signal_without_event_link", "signal_id": signal_id}
                )

    signal_outbox_rows = sorted(
        (row for row in outbox_list if row.get("entity_type") == "SIGNAL"),
        key=lambda row: (str(row.get("id", "")), str(row.get("entity_id", ""))),
    )
    for row in signal_outbox_rows:
        try:
            entity_id = int(row["entity_id"])
            outbox_id = int(row["id"])
        except (KeyError, TypeError, ValueError):
            findings.append({"kind": "malformed_input", "source": "outbox"})
            continue
        if entity_id not in signal_rows:
            findings.append(
                {
                    "kind": "outbox_without_signal",
                    "outbox_id": outbox_id,
                    "signal_id": entity_id,
                }
            )
    for row in signal_outbox_rows:
        lease_until = row.get("lease_until")
        if row.get("status") != "IN_FLIGHT" or lease_until is None:
            continue
        try:
            expired = lease_until <= now
        except TypeError:
            findings.append({"kind": "malformed_input", "source": "outbox"})
            continue
        if expired:
            try:
                outbox_id = int(row["id"])
                signal_id = int(row["entity_id"])
            except (KeyError, TypeError, ValueError):
                findings.append({"kind": "malformed_input", "source": "outbox"})
                continue
            findings.append(
                {
                    "kind": "expired_in_flight",
                    "outbox_id": outbox_id,
                    "signal_id": signal_id,
                }
            )
    for row in signal_outbox_rows:
        if row.get("status") == "DEAD":
            try:
                outbox_id = int(row["id"])
                signal_id = int(row["entity_id"])
            except (KeyError, TypeError, ValueError):
                findings.append({"kind": "malformed_input", "source": "outbox"})
                continue
            findings.append(
                {
                    "kind": "dead_delivery",
                    "outbox_id": outbox_id,
                    "signal_id": signal_id,
                }
            )

    if final_audits is not None:
        audit_rows = list(final_audits)
        for initial in sorted(
            (row for row in audit_rows if _is_initial_actionable(row)),
            key=lambda row: str(row.get("observation_id", "")),
        ):
            terminal_count = sum(
                _is_terminal_final(final)
                for final in audit_rows
                if _same_audit_chain(initial, final)
            )
            if terminal_count != 1:
                finding = {
                    "kind": "initial_actionable_without_final_outcome",
                    "final_outcome_count": terminal_count,
                }
                if initial.get("observation_id") is not None:
                    finding["observation_id"] = initial["observation_id"]
                elif initial.get("signal_id") is not None:
                    finding["signal_id"] = initial["signal_id"]
                findings.append(finding)

    return sorted(findings, key=_finding_sort_key)
