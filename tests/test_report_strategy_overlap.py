"""B6: cross-strategy overlap report against a synthetic read-only database."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "report_strategy_overlap",
    Path(__file__).resolve().parents[1] / "scripts" / "report_strategy_overlap.py",
)
report = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(report)

T0 = datetime(2026, 9, 23, 12, 0, tzinfo=timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _make_db(tmp_path: Path) -> Path:
    path = tmp_path / "overlap-test.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE signals (
            id INTEGER PRIMARY KEY,
            symbol TEXT NOT NULL,
            signal_time TEXT NOT NULL,
            event_id TEXT NOT NULL,
            strategy_type TEXT,
            strategy_subtype TEXT,
            grade TEXT,
            score INTEGER,
            market_price REAL
        );
        CREATE TABLE signal_provenance (
            signal_id INTEGER PRIMARY KEY,
            strategy_family TEXT,
            strategy_branch TEXT,
            root_event_id TEXT,
            decision_evaluation_id INTEGER,
            admission_evaluation_id INTEGER
        );
        CREATE TABLE climax_evaluations (
            id INTEGER PRIMARY KEY,
            root_event_id TEXT,
            evaluation_time TEXT,
            event_detected_at TEXT,
            observed_at TEXT
        );
        """
    )

    def signal(index, symbol, minutes, event_id, strategy):
        connection.execute(
            "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?)",
            (index, symbol, _iso(T0 + timedelta(minutes=minutes)), event_id, strategy, strategy, "A", 80, 100.0),
        )

    def provenance(index, root):
        connection.execute(
            "INSERT INTO signal_provenance VALUES (?,?,?,?,?,?)",
            (index, "FAMILY", None, root, index, index),
        )

    def evaluations(root, start_minutes, end_minutes):
        connection.executemany(
            "INSERT INTO climax_evaluations (root_event_id, evaluation_time, event_detected_at, observed_at) VALUES (?,?,?,?)",
            [
                (root, _iso(T0 + timedelta(minutes=start_minutes)), _iso(T0 + timedelta(minutes=start_minutes)), _iso(T0 + timedelta(minutes=start_minutes))),
                (root, _iso(T0 + timedelta(minutes=end_minutes)), None, _iso(T0 + timedelta(minutes=end_minutes))),
            ],
        )

    # (1) SAME_MARKET_EPISODE: shared root within the window.
    signal(1, "AAAUSDT", 0, "event-1", "VOLUME_CLIMAX_UNWIND")
    provenance(1, "root-A")
    signal(2, "AAAUSDT", 10, "event-2", "TRAPPED_LONGS_REVERSAL")
    provenance(2, "root-A")
    evaluations("root-A", 0, 20)

    # (2) SAME_MARKET_EPISODE: different roots, overlapping event windows.
    signal(3, "BBBUSDT", 0, "event-3", "VOLUME_CLIMAX_UNWIND")
    provenance(3, "root-B1")
    signal(4, "BBBUSDT", 15, "event-4", "LOW_VOLUME_EXTENSION_FAILURE")
    provenance(4, "root-B2")
    evaluations("root-B1", 0, 25)
    evaluations("root-B2", 20, 40)

    # (3) INDEPENDENT_SETUP: different roots, disjoint event windows, in window.
    signal(5, "CCUSDT", 0, "event-5", "VOLUME_CLIMAX_UNWIND")
    provenance(5, "root-C1")
    signal(6, "CCUSDT", 25, "event-6", "TRAPPED_LONGS_REVERSAL")
    provenance(6, "root-C2")
    evaluations("root-C1", 0, 5)
    evaluations("root-C2", 20, 30)

    # (4) UNKNOWN_OVERLAP: later signal has no provenance row at all.
    signal(7, "DDUSDT", 0, "event-7", "VOLUME_CLIMAX_UNWIND")
    provenance(7, "root-D1")
    evaluations("root-D1", 0, 5)
    signal(8, "DDUSDT", 5, "event-8", "TRAPPED_LONGS_REVERSAL")  # no provenance row
    signal(9, "EEUSDT", 0, "event-9", "VOLUME_CLIMAX_UNWIND")
    provenance(9, "root-E1")
    signal(10, "EEUSDT", 30, "event-10", "TRAPPED_LONGS_REVERSAL")
    provenance(10, "root-E2")
    evaluations("root-E1", 0, 5)
    evaluations("root-E2", 28, 40)
    signal(11, "FFUSDT", 0, "event-11", "VOLUME_CLIMAX_UNWIND")
    provenance(11, "root-F1")
    signal(12, "FFUSDT", 30 + 1 / 60, "event-12", "TRAPPED_LONGS_REVERSAL")
    provenance(12, "root-F2")
    evaluations("root-F1", 0, 5)
    evaluations("root-F2", 28, 40)

    # (6) Same strategy twice -> no cross-strategy pair; different symbol -> none.
    signal(13, "GGUSDT", 0, "event-13", "VOLUME_CLIMAX_UNWIND")
    provenance(13, "root-G1")
    signal(14, "GGUSDT", 5, "event-14", "VOLUME_CLIMAX_UNWIND")
    provenance(14, "root-G2")
    signal(15, "HHUSDT", 5, "event-15", "TRAPPED_LONGS_REVERSAL")
    provenance(15, "root-G1")

    connection.commit()
    connection.close()
    return path


def _report_dict(tmp_path: Path):
    connection = report._connect_read_only(_make_db(tmp_path))
    try:
        return report.build_report(connection, report.WINDOW_MINUTES_DEFAULT)
    finally:
        connection.close()


def test_classifications_and_boundaries(tmp_path) -> None:
    payload = _report_dict(tmp_path)
    by_pair = {(row["earlier"]["signal_id"], row["later"]["signal_id"]): row for row in payload["pairs"]}

    assert by_pair[(1, 2)]["classification"] == "SAME_MARKET_EPISODE"
    assert by_pair[(1, 2)]["reason"] == "shared_root_event_id"
    assert by_pair[(3, 4)]["classification"] == "SAME_MARKET_EPISODE"
    assert by_pair[(3, 4)]["reason"] == "overlapping_event_windows"
    assert by_pair[(5, 6)]["classification"] == "INDEPENDENT_SETUP"
    assert by_pair[(7, 8)]["classification"] == "UNKNOWN_OVERLAP"
    assert by_pair[(7, 8)]["reason"] == "provenance_or_root_missing"

    # Boundary: exactly the window length is inside; one second beyond is not.
    assert (9, 10) in by_pair
    assert by_pair[(9, 10)]["delta_minutes"] == 30.0
    assert (11, 12) not in by_pair
    # Same-strategy repeats and cross-symbol rows never pair.
    assert (13, 14) not in by_pair
    assert (7, 15) not in by_pair and (1, 15) not in by_pair

    counts = payload["counts_by_classification"]
    assert counts == {"SAME_MARKET_EPISODE": 2, "INDEPENDENT_SETUP": 2, "UNKNOWN_OVERLAP": 1}
    assert payload["cross_strategy_pairs_in_window"] == 5


def test_output_is_deterministic_and_cli_exits_zero(tmp_path, capsys) -> None:
    path = _make_db(tmp_path)
    code = report.main(["--db", str(path), "--examples", "1"])
    assert code == 0
    first = json.loads(capsys.readouterr().out)
    code = report.main(["--db", str(path), "--examples", "1"])
    assert code == 0
    second = json.loads(capsys.readouterr().out)
    assert first == second
    assert len(first["examples"]) == 1


def test_script_is_read_only(tmp_path) -> None:
    path = _make_db(tmp_path)
    before = path.read_bytes()
    assert report.main(["--db", str(path)]) == 0
    assert path.read_bytes() == before
    connection = sqlite3.connect(path)
    try:
        connection.execute("PRAGMA query_only = OFF;")
        rows = connection.execute("SELECT COUNT(*) FROM signals").fetchone()
        assert rows[0] == 15
    finally:
        connection.close()
