"""Cross-strategy overlap report for same-symbol signals (MEASUREMENT ONLY).

Two strategies can fire on the same symbol (or the same root event) within a
short window; nobody measures it.  This offline tool joins ``signals`` +
``signal_provenance`` (plus ``climax_evaluations`` for event windows) and
classifies every same-symbol cross-strategy pair whose signal times are within
a configurable window (default 30 minutes) as:

- ``SAME_MARKET_EPISODE``  the pair shares (or overlaps) a ``root_event_id``,
  or the later signal falls inside the earlier signal's event window;
- ``INDEPENDENT_SETUP``    distinct roots and no temporal overlap of event
  windows (or of the containment check);
- ``UNKNOWN_OVERLAP``      provenance or event-window data is insufficient to
  decide (e.g. legacy rows without a provenance record).

This script NEVER suppresses, deduplicates, mutates or re-delivers anything.
It performs no writes of any kind: the database is opened read-only
(``file:...?mode=ro`` + ``PRAGMA query_only=ON``).  Exit status is non-zero
only on real errors (missing file, unreadable schema).  Output is a
deterministic JSON document: rows are ordered by symbol, earlier decision
time, then signal ids.

Pair convention: a pair is (earlier signal, later signal) on the SAME symbol
with DIFFERENT strategy identities; the delta is ``later - earlier`` in
minutes; a delta equal to the configured window counts as INSIDE the window
(inclusive boundary).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

WINDOW_MINUTES_DEFAULT = 30

_PAIR_SQL = """
SELECT
    s.id               AS signal_id,
    s.symbol           AS symbol,
    s.signal_time      AS signal_time,
    s.event_id         AS event_id,
    s.strategy_type    AS strategy_type,
    s.strategy_subtype AS strategy_subtype,
    s.grade            AS grade,
    s.score            AS score,
    s.market_price     AS market_price,
    p.strategy_family  AS strategy_family,
    p.strategy_branch  AS strategy_branch,
    p.root_event_id    AS root_event_id,
    p.decision_evaluation_id AS decision_evaluation_id,
    p.admission_evaluation_id AS admission_evaluation_id
FROM signals AS s
LEFT JOIN signal_provenance AS p ON p.signal_id = s.id
ORDER BY s.symbol, s.signal_time, s.id
"""

_WINDOW_SQL = """
SELECT
    root_event_id AS root_event_id,
    MIN(COALESCE(event_detected_at, observed_at, evaluation_time)) AS window_start,
    MAX(COALESCE(observed_at, evaluation_time)) AS window_end
FROM climax_evaluations
WHERE root_event_id IS NOT NULL
GROUP BY root_event_id
"""


def _connect_read_only(database_path: Path) -> sqlite3.Connection:
    if not database_path.exists():
        raise SystemExit(f"error: database file not found: {database_path}")
    uri = database_path.resolve().as_uri() + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON;")
    return connection


def _has_table(connection: sqlite3.Connection, table_name: str) -> bool:
    return connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,)
    ).fetchone() is not None


def _utc(value: Any) -> datetime | None:
    """Normalize stored timestamps (ISO text or epoch seconds) to aware UTC."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _strategy_identity(row: sqlite3.Row) -> str:
    return str(
        row["strategy_branch"] or row["strategy_subtype"] or row["strategy_type"] or "UNKNOWN"
    )


def _load_signals(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for row in connection.execute(_PAIR_SQL):
        moment = _utc(row["signal_time"])
        if moment is None:
            continue
        signals.append(
            {
                "signal_id": row["signal_id"],
                "symbol": row["symbol"],
                "signal_time": moment,
                "event_id": row["event_id"],
                "strategy": _strategy_identity(row),
                "root_event_id": row["root_event_id"],
                "decision_evaluation_id": row["decision_evaluation_id"],
                "admission_evaluation_id": row["admission_evaluation_id"],
                "grade": row["grade"],
                "score": row["score"],
                "market_price": row["market_price"],
            }
        )
    return signals


def _load_event_windows(connection: sqlite3.Connection) -> dict[str, tuple[datetime, datetime]]:
    if not _has_table(connection, "climax_evaluations"):
        return {}
    windows: dict[str, tuple[datetime, datetime]] = {}
    for row in connection.execute(_WINDOW_SQL):
        root = row["root_event_id"]
        start = _utc(row["window_start"])
        end = _utc(row["window_end"])
        if root is None or start is None or end is None:
            continue
        windows[str(root)] = (start, max(end, start))
    return windows


def _overlaps(
    first: tuple[datetime, datetime] | None,
    second: tuple[datetime, datetime] | None,
) -> bool | None:
    if first is None or second is None:
        return None
    return first[0] <= second[1] and second[0] <= first[1]


def classify_pair(
    earlier: dict[str, Any],
    later: dict[str, Any],
    windows: dict[str, tuple[datetime, datetime]],
) -> tuple[str, str]:
    """Return (classification, reason) for one ordered cross-strategy pair."""
    e_root, l_root = earlier["root_event_id"], later["root_event_id"]
    if e_root is None or l_root is None:
        if earlier["event_id"] is not None and earlier["event_id"] == later["event_id"]:
            return "SAME_MARKET_EPISODE", "shared_event_id"
        return "UNKNOWN_OVERLAP", "provenance_or_root_missing"
    if str(e_root) == str(l_root):
        return "SAME_MARKET_EPISODE", "shared_root_event_id"
    if earlier["event_id"] == later["event_id"]:
        return "SAME_MARKET_EPISODE", "shared_event_id"
    e_win, l_win = windows.get(str(e_root)), windows.get(str(l_root))
    overlap = _overlaps(e_win, l_win)
    if overlap is True:
        return "SAME_MARKET_EPISODE", "overlapping_event_windows"
    if e_win is not None and later["signal_time"] <= e_win[1]:
        return "SAME_MARKET_EPISODE", "later_signal_inside_earlier_event_window"
    if e_win is not None and l_win is not None:
        return "INDEPENDENT_SETUP", "disjoint_event_windows"
    return "UNKNOWN_OVERLAP", "event_window_unavailable"


def build_pairs(
    signals: list[dict[str, Any]], window_minutes: int
) -> list[tuple[dict[str, Any], dict[str, Any], timedelta]]:
    """Enumerate ordered cross-strategy pairs inside the window deterministically."""
    limit = timedelta(minutes=window_minutes)
    pairs: list[tuple[dict[str, Any], dict[str, Any], timedelta]] = []
    by_symbol: dict[str, list[dict[str, Any]]] = {}
    for signal in sorted(signals, key=lambda item: (item["symbol"], item["signal_time"], item["signal_id"])):
        by_symbol.setdefault(signal["symbol"], []).append(signal)
    for _symbol, items in sorted(by_symbol.items()):
        for index, earlier in enumerate(items):
            for later in items[index + 1 :]:
                delta = later["signal_time"] - earlier["signal_time"]
                if delta > limit:
                    break
                if earlier["strategy"] == later["strategy"]:
                    continue
                pairs.append((earlier, later, delta))
    return pairs


def build_report(connection: sqlite3.Connection, window_minutes: int) -> dict[str, Any]:
    signals = _load_signals(connection)
    windows = _load_event_windows(connection)
    pair_rows: list[dict[str, Any]] = []
    counts: dict[str, int] = {"SAME_MARKET_EPISODE": 0, "INDEPENDENT_SETUP": 0, "UNKNOWN_OVERLAP": 0}
    per_strategy_pair: dict[str, dict[str, int]] = {}
    deltas: list[float] = []
    for earlier, later, delta in build_pairs(signals, window_minutes):
        classification, reason = classify_pair(earlier, later, windows)
        counts[classification] += 1
        key = f"{earlier['strategy']} -> {later['strategy']}"
        bucket = per_strategy_pair.setdefault(
            key, {"SAME_MARKET_EPISODE": 0, "INDEPENDENT_SETUP": 0, "UNKNOWN_OVERLAP": 0}
        )
        bucket[classification] += 1
        delta_minutes = round(delta.total_seconds() / 60, 6)
        deltas.append(delta_minutes)
        pair_rows.append(
            {
                "symbol": earlier["symbol"],
                "strategy_pair": key,
                "classification": classification,
                "reason": reason,
                "delta_minutes": delta_minutes,
                "earlier": {
                    "signal_id": earlier["signal_id"],
                    "strategy": earlier["strategy"],
                    "signal_time": earlier["signal_time"].isoformat().replace("+00:00", "Z"),
                    "root_event_id": earlier["root_event_id"],
                    "grade": earlier["grade"],
                    "score": earlier["score"],
                },
                "later": {
                    "signal_id": later["signal_id"],
                    "strategy": later["strategy"],
                    "signal_time": later["signal_time"].isoformat().replace("+00:00", "Z"),
                    "root_event_id": later["root_event_id"],
                    "grade": later["grade"],
                    "score": later["score"],
                },
            }
        )
    return {
        "schema": "strategy-overlap-v1",
        "window_minutes": window_minutes,
        "signals_with_strategy_identity": len(signals),
        "cross_strategy_pairs_in_window": len(pair_rows),
        "counts_by_classification": counts,
        "counts_by_strategy_pair": {key: per_strategy_pair[key] for key in sorted(per_strategy_pair)},
        "delta_minutes": {
            "min": min(deltas) if deltas else None,
            "median": statistics.median(deltas) if deltas else None,
            "max": max(deltas) if deltas else None,
        },
        "pairs": pair_rows,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", required=True, help="path to the SQLite database (opened read-only)")
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=WINDOW_MINUTES_DEFAULT,
        help=f"pair window in minutes (default {WINDOW_MINUTES_DEFAULT}, inclusive boundary)",
    )
    parser.add_argument("--symbol", default=None, help="optional symbol filter")
    parser.add_argument("--examples", type=int, default=10, help="max example pairs echoed (default 10)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.window_minutes <= 0:
        print("error: --window-minutes must be positive", file=sys.stderr)
        return 2
    connection = _connect_read_only(Path(args.db))
    try:
        if not _has_table(connection, "signals"):
            print("error: required table missing: signals", file=sys.stderr)
            return 2
        report = build_report(connection, args.window_minutes)
    finally:
        connection.close()
    if args.symbol:
        wanted = args.symbol.upper()
        report["pairs"] = [row for row in report["pairs"] if row["symbol"] == wanted]
        report["cross_strategy_pairs_in_window"] = len(report["pairs"])
    report["examples"] = report["pairs"][: max(0, args.examples)]
    print(json.dumps(report, sort_keys=True, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
