"""Read-only report for coverage and ROOT_DETECTOR_SHADOW_V1 telemetry."""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    connection = sqlite3.connect(args.database.resolve().as_uri() + "?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    def _has_table(name: str) -> bool:
        return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None
    def _has_column(table: str, column: str) -> bool:
        return any(row[1] == column for row in connection.execute(f"PRAGMA table_info({table})"))
    def count(table: str) -> int:
        return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    def distinct_count(predicate: str = "1=1") -> int:
        return int(connection.execute(
            f"SELECT COUNT(DISTINCT symbol) FROM market_coverage_ledger WHERE {predicate}"
        ).fetchone()[0])
    coverage = [dict(row) for row in connection.execute("SELECT scan_status, exclusion_reason, COUNT(*) AS n FROM market_coverage_ledger GROUP BY scan_status, exclusion_reason ORDER BY scan_status, exclusion_reason")]
    candidates = [dict(row) for row in connection.execute("SELECT candidate_id, symbol, first_seen_at, live_root_created, live_root_event_id, candidate_to_root_latency, peak_to_root_latency, outcome_status, outcome_mfe_pct, outcome_mae_pct, outcome_json FROM root_detector_shadow_candidates ORDER BY first_seen_at, id")]
    episode_count = count("root_detector_shadow_episodes") if _has_table("root_detector_shadow_episodes") else 0
    observation_count = count("root_detector_shadow_observations") if _has_table("root_detector_shadow_observations") else 0
    episode_outcomes = []
    if _has_table("root_detector_shadow_episode_outcomes"):
        episode_outcomes = [dict(row) for row in connection.execute("SELECT * FROM root_detector_shadow_episode_outcomes ORDER BY episode_id")]
        if _has_table("root_detector_shadow_episodes") and _has_table("root_detector_shadow_observations"):
            anchors = {
                row["episode_id"]: dict(row)
                for row in connection.execute(
                    """
                    SELECT e.episode_id, e.opened_at AS outcome_anchor_time,
                           o.price AS outcome_anchor_price
                    FROM root_detector_shadow_episodes e
                    LEFT JOIN root_detector_shadow_observations o
                      ON o.observation_id = (
                          SELECT o2.observation_id
                          FROM root_detector_shadow_observations o2
                          WHERE o2.episode_id = e.episode_id
                          ORDER BY o2.observed_at, o2.observation_id
                          LIMIT 1
                      )
                    """
                )
            }
            for row in episode_outcomes:
                anchor = anchors.get(row["episode_id"], {})
                row["outcome_anchor_type"] = "EPISODE_FIRST_SEEN"
                row["outcome_anchor_time"] = anchor.get("outcome_anchor_time")
                row["outcome_anchor_price"] = anchor.get("outcome_anchor_price")
    canonical_maturity = {status: sum(1 for row in episode_outcomes if row.get("outcome_status") == status) for status in {row.get("outcome_status") for row in episode_outcomes}}
    raw_maturity = {status: sum(1 for row in candidates if row["outcome_status"] == status) for status in {row["outcome_status"] for row in candidates}}
    horizon_summary = {}
    for label in ("15m", "30m", "1h", "4h", "12h", "24h"):
        values = [float(row[f"return_{label}"]) for row in episode_outcomes if row.get(f"return_{label}") is not None]
        horizon_summary[label] = {"count": len(values), "mean_short_return_pct": sum(values) / len(values) if values else None}
    latency_values = [float(row["candidate_to_root_latency"]) for row in candidates if row["candidate_to_root_latency"] is not None]
    root_link_reconciliation = {"PERSISTED_EPISODE_ROOT_LINK": 0, "LEGACY_DETERMINISTIC_ROOT_MAPPING": 0, "OTHER": 0}
    if _has_table("root_detector_shadow_episodes"):
        episodes = [row["episode_id"] for row in connection.execute("SELECT episode_id FROM root_detector_shadow_episodes")]
        persisted = {
            row["episode_id"]
            for row in connection.execute("SELECT DISTINCT episode_id FROM root_detector_shadow_episode_root_links")
        } if _has_table("root_detector_shadow_episode_root_links") else set()
        legacy_rooted = {
            row["episode_id"]
            for row in connection.execute(
                """
                SELECT DISTINCT m.episode_id
                FROM root_detector_shadow_legacy_mappings m
                JOIN root_detector_shadow_candidates c ON c.candidate_id = m.legacy_candidate_id
                WHERE c.live_root_created = 1
                """
            )
        } if _has_table("root_detector_shadow_legacy_mappings") else set()
        for episode_id in episodes:
            if episode_id in persisted:
                root_link_reconciliation["PERSISTED_EPISODE_ROOT_LINK"] += 1
            elif episode_id in legacy_rooted:
                root_link_reconciliation["LEGACY_DETERMINISTIC_ROOT_MAPPING"] += 1
            else:
                root_link_reconciliation["OTHER"] += 1
    due_buckets = {}
    if _has_table("root_detector_shadow_episode_outcomes"):
        for row in connection.execute(
            """
            SELECT outcome_status,
                   SUM(outcome_next_due_at IS NOT NULL AND outcome_next_due_at <= CURRENT_TIMESTAMP) AS due,
                   SUM(outcome_next_due_at IS NOT NULL AND outcome_next_due_at <= datetime('now','-1 hour')) AS overdue_1h,
                   SUM(outcome_next_due_at IS NOT NULL AND outcome_next_due_at <= datetime('now','-6 hours')) AS overdue_6h
            FROM root_detector_shadow_episode_outcomes
            GROUP BY outcome_status
            """
        ):
            due_buckets[str(row["outcome_status"])] = dict(row)
    print(json.dumps({
        "exchange_symbols": distinct_count(),
        "eligible": distinct_count("eligible = 1"),
        "scheduled": distinct_count("scheduled = 1"),
        "scanned": distinct_count("scanned = 1"),
        "exclusion_reasons": {str(row["exclusion_reason"]): row["n"] for row in coverage if row["exclusion_reason"]},
        "scheduled_scanned": coverage,
        "unexpected_result_present": int(connection.execute("SELECT COUNT(*) FROM market_coverage_ledger WHERE unexpected_result_present = 1").fetchone()[0]) if _has_column("market_coverage_ledger", "unexpected_result_present") else None,
        "shadow_candidates": candidates,
        "candidate_count": len(candidates),
        "episode_count": episode_count,
        "observation_count": observation_count,
        "episodes_without_roots": max(0, episode_count - len({row["live_root_event_id"] for row in candidates if row["live_root_event_id"]})),
        "episode_outcomes": episode_outcomes,
        "live_root_linked": sum(1 for row in candidates if row["live_root_created"]),
        "without_root": sum(1 for row in candidates if not row["live_root_created"]),
        "latency_seconds": {"count": len(latency_values), "min": min(latency_values) if latency_values else None, "max": max(latency_values) if latency_values else None},
        "outcome_maturity": canonical_maturity or raw_maturity,
        "raw_outcome_maturity": raw_maturity,
        "horizon_summary": horizon_summary,
        "outcome_anchor_contract": "EPISODE_FIRST_SEEN; signal outcomes use SIGNAL_ENTRY",
        "due_buckets": due_buckets,
        "root_link_reconciliation": root_link_reconciliation,
    }, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
