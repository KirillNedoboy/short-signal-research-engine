from __future__ import annotations

import sqlite3
from pathlib import Path

from app.replay.low_volume_v2 import replay_sqlite


def _db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        create table strategy_observations (
            observation_id text primary key, strategy text not null,
            evaluation_phase text not null, symbol text not null,
            root_event_id text, event_revision integer, signal_id integer,
            observed_at text not null, market_asof text, live_decision text,
            shadow_decision text, score integer, blockers_json text,
            warnings_json text, market_price real, event_high real,
            model_version text, config_hash text, input_fingerprint text,
            input_snapshot_json text, outcome_status text, outcome_json text,
            outcome_mfe_pct real, outcome_mae_pct real,
            outcome_time_to_mfe_minutes real, outcome_time_to_mae_minutes real,
            outcome_new_high_after_observation integer
        );
        """
    )
    rows = [
        ("a", "LOW_VOLUME_EXTENSION_FAILURE", "INITIAL", "AAA", "root-a", 1, None,
         "2026-01-01 00:00:00", "2026-01-01 00:00:00", "BLOCKED", "NOT_EVALUATED", 70,
         '["extension_below_threshold"]', '[]', 10, 11, "v1", "cfg", "fp",
         '{"evaluation":{"metadata":{"extension_gate_value":-1.0}}}', "complete",
         '{"mfe_pct":5,"mae_pct":3,"coverage_end":"2026-01-02T00:00:00Z"}', 5, 3, 2, 1, 1),
        ("b", "LOW_VOLUME_EXTENSION_FAILURE", "INITIAL", "AAA", "root-a", 1, None,
         "2026-01-01 00:01:00", "2026-01-01 00:01:00", "BLOCKED", "NOT_EVALUATED", 71,
         '[]', '[]', 10, 11, "v1", "cfg", "fp2", '{}', "complete",
         '{"mfe_pct":6,"mae_pct":4}', 6, 4, 3, 2, 0),
        ("c", "LOW_VOLUME_EXTENSION_FAILURE", "INITIAL", "BBB", "root-b", 1, None,
         "2026-01-02 00:00:00", "2026-01-02 00:00:00", "BLOCKED", "NOT_EVALUATED", 60,
         '["missing"]', '[]', 20, 22, "v1", "cfg", "fp3", '{}', "incomplete",
         '{"mfe_pct":9}', 9, None, None, None, None),
    ]
    con.executemany("insert into strategy_observations values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit(); con.close()


def test_replay_deduplicates_and_marks_v2_unknown_without_candles(tmp_path: Path):
    path = tmp_path / "replay.sqlite"
    _db(path)
    result = replay_sqlite(path)
    assert result.signal_count == 0
    assert result.retained_episode_count == 2
    assert result.research_count == 1 and result.holdout_count == 1
    row = result.rows[0]
    assert row["v1_outcome_status"] == "COMPLETE"
    assert row["v2_outcome_status"] == "UNKNOWN"
    assert row["v2_unknown_reason"] == "INSUFFICIENT_CANDLE_LEVEL_INPUTS"
    assert row["v1_mfe_pct"] == 6.0  # latest observation is canonical for the root
    assert row["v2_mfe_pct"] is None


def test_incomplete_is_not_reclassified_as_failure(tmp_path: Path):
    path = tmp_path / "replay.sqlite"
    _db(path)
    result = replay_sqlite(path)
    incomplete = next(row for row in result.rows if row["root_event_id"] == "root-b")
    assert incomplete["v1_outcome_status"] == "INCOMPLETE"
    assert incomplete["v1_mfe_pct"] is None
    assert incomplete["eventual_favorable_movement"] == "UNKNOWN"
