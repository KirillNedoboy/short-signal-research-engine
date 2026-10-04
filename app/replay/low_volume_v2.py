"""Read-only historical replay boundary for low-volume extension V2.

The production SQLite journal contains decision snapshots and persisted V1
outcomes, but not candle-level OHLC linked to each candidate.  This module
therefore never synthesizes a V2 fill or path: V2 fields stay UNKNOWN until
those inputs exist.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

STRATEGY = "LOW_VOLUME_EXTENSION_FAILURE"
CSV_FIELDS = [
    "root_event_id", "symbol", "anchor_time", "evaluation_phase", "source_observation_id",
    "research_holdout", "v1_signal_count", "v1_outcome_status", "v1_mfe_pct", "v1_mae_pct",
    "v1_entry_delay_minutes", "v1_new_high_before_entry", "eventual_favorable_movement",
    "v2_outcome_status", "v2_unknown_reason", "v2_entry_delay_minutes", "v2_mfe_pct",
    "v2_mae_after_final_entry_pct", "v2_new_high_before_entry", "v2_eventual_favorable_movement",
    "extension_below_threshold_policy_v1", "extension_below_threshold_policy_v2",
]


@dataclass(frozen=True)
class ReplayResult:
    rows: list[dict[str, Any]]
    signal_count: int
    retained_episode_count: int
    research_count: int
    holdout_count: int
    source_path: str
    coverage_start: str | None
    coverage_end: str | None
    candle_level_inputs_available: bool


def replay_sqlite(path: str | Path) -> ReplayResult:
    """Build a deterministic, read-only replay ledger from production SQLite."""
    db_path = Path(path)
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    try:
        rows = con.execute(
            """select * from strategy_observations
               where strategy = ? and root_event_id is not null
               order by observed_at, observation_id""", (STRATEGY,)
        ).fetchall()
        # A root is one analytical episode. Prefer the latest complete persisted
        # outcome; otherwise retain the latest row and call it incomplete/unknown.
        grouped: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            grouped.setdefault(str(row["root_event_id"]), []).append(row)
        selected: list[sqlite3.Row] = []
        for items in grouped.values():
            complete = [r for r in items if str(r["outcome_status"] or "").lower() == "complete"]
            selected.append((complete or items)[-1])
        selected.sort(key=lambda r: (str(r["observed_at"]), str(r["root_event_id"])))
        # Floor the boundary: small cohorts must not be rounded up into an
        # all-research split; the report states the achieved denominator.
        split_at = len(selected) * 70 // 100
        if len(selected) > 1:
            split_at = max(1, min(len(selected) - 1, split_at))
        signal_count = len({r["signal_id"] for r in rows if r["signal_id"] is not None})
        candle_inputs = _has_linked_candle_inputs(con)
        result_rows = [_row(r, i < split_at, candle_inputs) for i, r in enumerate(selected)]
        times = [r["observed_at"] for r in selected if r["observed_at"]]
        return ReplayResult(
            result_rows, signal_count, len(selected), min(split_at, len(selected)), max(0, len(selected) - split_at),
            str(db_path), min(times) if times else None, max(times) if times else None, candle_inputs,
        )
    finally:
        con.close()


def _has_linked_candle_inputs(con: sqlite3.Connection) -> bool:
    tables = {r[0] for r in con.execute("select name from sqlite_master where type='table'")}
    # No accepted candle table currently has the required root/availability
    # linkage. A generic market table is deliberately not treated as sufficient.
    return any(name in tables for name in ("replay_candles", "historical_replay_candles"))


def _row(r: sqlite3.Row, research: bool, candle_inputs: bool) -> dict[str, Any]:
    status = str(r["outcome_status"] or "UNKNOWN").upper()
    if status not in {"COMPLETE", "INCOMPLETE"}:
        status = "UNKNOWN"
    outcome = _json(r["outcome_json"])
    mfe = _number(r["outcome_mfe_pct"]) if status == "COMPLETE" else None
    mae = _number(r["outcome_mae_pct"]) if status == "COMPLETE" else None
    favorable = "YES" if mfe is not None and mfe > 0 else ("NO" if status == "COMPLETE" else "UNKNOWN")
    return {
        "root_event_id": r["root_event_id"], "symbol": r["symbol"], "anchor_time": r["observed_at"],
        "evaluation_phase": r["evaluation_phase"], "source_observation_id": r["observation_id"],
        "research_holdout": "RESEARCH" if research else "HOLDOUT", "v1_signal_count": 1 if r["signal_id"] is not None else 0,
        "v1_outcome_status": status, "v1_mfe_pct": mfe, "v1_mae_pct": mae,
        "v1_entry_delay_minutes": None, "v1_new_high_before_entry": "UNKNOWN",
        "eventual_favorable_movement": favorable,
        "v2_outcome_status": "UNKNOWN" if not candle_inputs else "INSUFFICIENT_DATA",
        "v2_unknown_reason": "INSUFFICIENT_CANDLE_LEVEL_INPUTS" if not candle_inputs else "UNIMPLEMENTED_LINKED_REPLAY",
        "v2_entry_delay_minutes": None, "v2_mfe_pct": None, "v2_mae_after_final_entry_pct": None,
        "v2_new_high_before_entry": "UNKNOWN", "v2_eventual_favorable_movement": "UNKNOWN",
        "extension_below_threshold_policy_v1": "UNKNOWN", "extension_below_threshold_policy_v2": "UNKNOWN",
    }


def _json(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError):
        return {}


def _number(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def write_artifacts(result: ReplayResult, output: str | Path) -> tuple[Path, Path]:
    csv_path = Path(output)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader(); writer.writerows(result.rows)
    digest = hashlib.sha256(csv_path.read_bytes()).hexdigest()
    md_path = csv_path.with_suffix(".md")
    md_path.write_text(_markdown(result, csv_path, digest), encoding="utf-8")
    return csv_path, md_path


def _markdown(result: ReplayResult, csv_path: Path, digest: str) -> str:
    unknown = sum(r["v2_outcome_status"] == "UNKNOWN" for r in result.rows)
    return f"""# Low-volume extension V2 historical counterfactual

- **Status:** `INSUFFICIENT_DATA_CONTINUE_OBSERVATION`
- **Canonical unit:** `(root_event_id, strategy)`; repeated observations are deduplicated.
- **Source:** read-only SQLite `{result.source_path}`
- **Coverage:** `{result.coverage_start}` – `{result.coverage_end}` (observation timestamps)
- **Signals:** {result.signal_count}; **retained episodes:** {result.retained_episode_count}
- **Chronological split:** research {result.research_count} / holdout {result.holdout_count} (70/30 target)
- **V2 outcomes:** {unknown} `UNKNOWN`; no V2 result is inferred.
- **CSV SHA-256:** `{digest}` ({csv_path})

## Outcome coverage

| Measure | V1 persisted journal | V2 counterfactual |
|---|---:|---:|
| Signal count | {result.signal_count} | UNKNOWN |
| Episode count | {result.retained_episode_count} | {result.retained_episode_count} candidates |
| Entry delay | UNKNOWN (no signal-linked entry) | UNKNOWN |
| MFE | persisted only when `COMPLETE` | UNKNOWN |
| MAE after final entry | UNKNOWN (entry absent) | UNKNOWN |
| New-high-before-entry | UNKNOWN | UNKNOWN |
| Eventual favorable movement | reported from persisted MFE only | UNKNOWN |
| `extension_below_threshold` policy comparison | UNKNOWN | UNKNOWN |

## Coverage boundary

The production SQLite journal contains decision snapshots and V1 outcome fields,
but no accepted candle-level OHLC input linked to each low-volume root and V2
entry anchor. Therefore the replay does **not** infer delayed entry, MFE, MAE,
new-high path, or policy effects. `INCOMPLETE` and missing outcomes remain
explicit; they are not counted as failures. Production configuration and
runtime state were not modified.
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="${DATA_DIR}/bot.sqlite")
    parser.add_argument("--strategy", default=STRATEGY)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if args.strategy != STRATEGY:
        parser.error(f"only {STRATEGY} is supported")
    result = replay_sqlite(args.db)
    csv_path, md_path = write_artifacts(result, args.output)
    print(json.dumps({"csv": str(csv_path), "markdown": str(md_path), "episodes": result.retained_episode_count,
                      "signals": result.signal_count, "v2_unknown": sum(r["v2_outcome_status"] == "UNKNOWN" for r in result.rows)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
