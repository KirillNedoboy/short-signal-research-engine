from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


REQUIRED_COST_FIELDS = {
    "entry_bid",
    "entry_ask",
    "exit_bid",
    "exit_ask",
    "fee_bps",
    "slippage_bps",
    "funding_bps",
    "latency_ms",
}
REQUIRED_HORIZONS = {"15m", "30m", "1h", "4h"}


@dataclass(frozen=True, slots=True)
class ArchiveReadiness:
    status: str
    independent_roots: int
    outcome_rows: int
    missing_requirements: tuple[str, ...]

    @property
    def validated(self) -> bool:
        return self.status == "VALIDATED"


def inspect_replay_archive(bundle: str | Path) -> ArchiveReadiness:
    """Return a fail-closed readiness result for cost-aware chronological OOS."""
    root = Path(bundle)
    inputs = _rows(root / "inputs/strategy_inputs.parquet")
    outcomes = _rows(root / "outcomes/outcomes.parquet")
    decisions = _rows(root / "decisions/strategy_decisions.parquet")

    roots = {
        str(row.get("root_id") or row.get("root_event_id") or row.get("event_id"))
        for row in inputs
        if row.get("root_id") or row.get("root_event_id") or row.get("event_id")
    }
    missing: list[str] = []
    if not roots:
        missing.append("independent_root_ids")
    if not inputs:
        missing.append("strategy_inputs")
    if not decisions:
        missing.append("strategy_decisions")
    if not outcomes:
        missing.append("outcomes")

    observed_cost_fields: set[str] = set()
    horizons: set[str] = set()
    for row in inputs + outcomes:
        payload = _payload(row.get("payload_json"))
        observed_cost_fields.update(REQUIRED_COST_FIELDS.intersection(payload))
        horizon = row.get("horizon") or payload.get("horizon")
        if horizon:
            horizons.add(str(horizon))
    missing.extend(f"cost:{field}" for field in sorted(REQUIRED_COST_FIELDS - observed_cost_fields))
    missing.extend(f"horizon:{horizon}" for horizon in sorted(REQUIRED_HORIZONS - horizons))

    unique_missing = tuple(dict.fromkeys(missing))
    return ArchiveReadiness(
        status="VALIDATED" if not unique_missing else "NOT_VALIDATED",
        independent_roots=len(roots),
        outcome_rows=len(outcomes),
        missing_requirements=unique_missing,
    )


def _rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pq.read_table(path).to_pylist()


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}
