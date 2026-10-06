"""Read-only export of persisted signal data for offline analysis."""

from __future__ import annotations

import csv
import json
import os
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy.engine import make_url


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config


REPORTS_DIR = ROOT / "reports"
RAW_SIGNALS_CSV = REPORTS_DIR / "raw_signals.csv"
RAW_SIGNALS_JSONL = REPORTS_DIR / "raw_signals.jsonl"
DATA_INVENTORY_MD = REPORTS_DIR / "data_inventory.md"

SCAN_EXTENSIONS = {".sqlite", ".sqlite3", ".db", ".json", ".jsonl", ".csv", ".log"}
EXCLUDED_DIR_NAMES = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
}
CORE_CANONICAL_COLUMNS = [
    "signal_id",
    "timestamp_utc",
    "symbol",
    "side",
    "signal_type",
    "entry_reference_price",
    "market_price",
    "mark_price",
    "score",
    "grade",
    "event_id",
    "trigger_window",
    "short_zone_low",
    "short_zone_high",
    "telegram_payload",
    "telegram_sent",
    "telegram_status",
    "dedup_repeated_flag",
    "timeframe_metadata",
    "source_db_path",
    "source_signal_table",
    "source_outcome_table",
    "source_event_state_table",
    "price_5m",
    "price_15m",
    "price_30m",
    "price_1h",
    "price_4h",
    "price_12h",
    "price_24h",
    "ret_5m",
    "ret_15m",
    "ret_30m",
    "ret_1h",
    "ret_4h",
    "ret_12h",
    "ret_24h",
    "mfe_pct",
    "mae_pct",
    "best_price_until_24h",
    "worst_price_until_24h",
    "time_to_best_move_min",
    "time_to_worst_move_min",
]
ABSENT_FIELD_EXPLANATIONS = {
    "telegram_payload": "Telegram message text is not persisted anywhere in the current runtime.",
    "dedup_repeated_flag": "There is no persisted per-signal dedup/repeat flag in the current schema.",
    "mark_price": "Mark price exists in live market snapshots, but no local persisted source was found.",
    "price_5m": "No local post-signal candle store was found for 5m price reconstruction.",
    "price_30m": "No local post-signal candle store was found for 30m price reconstruction.",
    "price_12h": "No local post-signal candle store was found for 12h price reconstruction.",
    "price_24h": "No local post-signal candle store was found for 24h price reconstruction.",
    "best_price_until_24h": "Needs local post-signal candles through 24h, which were not found.",
    "worst_price_until_24h": "Needs local post-signal candles through 24h, which were not found.",
    "time_to_best_move_min": "Needs local post-signal candles through 24h, which were not found.",
    "time_to_worst_move_min": "Needs local post-signal candles through 24h, which were not found.",
}


@dataclass(slots=True)
class FileSource:
    """Metadata for a discovered file-like data source."""

    kind: str
    path: Path
    exists: bool
    size_bytes: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ExportContext:
    """Aggregated discovery and export context."""

    configured_db_url: str
    configured_sqlite_path: Path | None
    configured_sqlite_exists: bool
    sqlite_sources: list[FileSource]
    flat_file_sources: list[FileSource]
    rows: list[dict[str, Any]]
    matched_fields: list[str]
    export_rules: list[str]
    absent_fields: list[str]
    remote_origin: dict[str, Any]


def main() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    config = load_config(config_path=ROOT / "config.yaml", env_path=ROOT / ".env")
    configured_sqlite_path = _sqlite_path_from_url(config.db_url, ROOT)

    sqlite_sources = _discover_sqlite_sources(ROOT, configured_sqlite_path)
    flat_file_sources = _discover_flat_file_sources(ROOT)
    rows = _collect_rows(sqlite_sources)
    remote_origin = _load_remote_origin_manifest(ROOT / "data" / "export_source_manifest.json")

    context = ExportContext(
        configured_db_url=config.db_url,
        configured_sqlite_path=configured_sqlite_path,
        configured_sqlite_exists=bool(configured_sqlite_path and configured_sqlite_path.exists()),
        sqlite_sources=sqlite_sources,
        flat_file_sources=flat_file_sources,
        rows=rows,
        matched_fields=_matched_fields(rows),
        export_rules=_export_rules(sqlite_sources),
        absent_fields=_absent_fields(rows),
        remote_origin=remote_origin,
    )

    _write_jsonl(RAW_SIGNALS_JSONL, rows)
    _write_csv(RAW_SIGNALS_CSV, rows)
    DATA_INVENTORY_MD.write_text(_render_inventory(context), encoding="utf-8")

    summary = _build_summary(rows)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _sqlite_path_from_url(db_url: str, base_dir: Path) -> Path | None:
    url = make_url(db_url)
    if url.get_backend_name() != "sqlite":
        return None
    database = url.database
    if not database or database == ":memory:":
        return None
    path = Path(database).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def _discover_sqlite_sources(root: Path, configured_path: Path | None) -> list[FileSource]:
    candidates: dict[Path, FileSource] = {}
    if configured_path is not None:
        candidates[configured_path] = FileSource(
            kind="configured_sqlite_db",
            path=configured_path,
            exists=configured_path.exists(),
            size_bytes=configured_path.stat().st_size if configured_path.exists() else None,
            notes=["Configured by app config as the primary persisted signal store."],
        )

    for path in _iter_candidate_files(root):
        if path.suffix.lower() not in {".sqlite", ".sqlite3", ".db"}:
            continue
        if path not in candidates:
            candidates[path] = FileSource(
                kind="sqlite_db",
                path=path,
                exists=True,
                size_bytes=path.stat().st_size,
            )

    results: list[FileSource] = []
    for source in sorted(candidates.values(), key=lambda item: str(item.path)):
        if source.exists:
            source.details = _inspect_sqlite(source.path)
        else:
            source.notes.append("File does not currently exist in the workspace.")
        results.append(source)
    return results


def _discover_flat_file_sources(root: Path) -> list[FileSource]:
    sources: list[FileSource] = []
    for path in _iter_candidate_files(root):
        suffix = path.suffix.lower()
        if suffix in {".sqlite", ".sqlite3", ".db"}:
            continue
        kind = {
            ".json": "json_file",
            ".jsonl": "jsonl_file",
            ".csv": "csv_file",
            ".log": "log_file",
        }.get(suffix, "file")
        source = FileSource(
            kind=kind,
            path=path,
            exists=True,
            size_bytes=path.stat().st_size,
        )
        source.details = _inspect_flat_file(path)
        sources.append(source)
    return sorted(sources, key=lambda item: str(item.path))


def _iter_candidate_files(root: Path) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in EXCLUDED_DIR_NAMES for part in path.parts):
            continue
        if path.suffix.lower() not in SCAN_EXTENSIONS:
            continue
        if path.parent == REPORTS_DIR:
            continue
        yield path


def _inspect_sqlite(path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(path)
    try:
        table_rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        tables = [row[0] for row in table_rows]
        columns: dict[str, list[str]] = {}
        row_counts: dict[str, int] = {}
        for table in tables:
            quoted = _quote_identifier(table)
            columns[table] = [row[1] for row in connection.execute(f"PRAGMA table_info({quoted})").fetchall()]
            row_counts[table] = int(connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
        return {
            "tables": tables,
            "columns": columns,
            "row_counts": row_counts,
        }
    finally:
        connection.close()


def _inspect_flat_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                sample_keys = sorted(payload.keys())[:25]
                return {"sample_type": "object", "sample_keys": sample_keys}
            if isinstance(payload, list):
                sample_keys = sorted(payload[0].keys())[:25] if payload and isinstance(payload[0], dict) else []
                return {"sample_type": "array", "sample_len": len(payload), "sample_keys": sample_keys}
        if suffix == ".jsonl":
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    payload = json.loads(text)
                    if isinstance(payload, dict):
                        return {"sample_keys": sorted(payload.keys())[:25]}
                    return {"sample_type": type(payload).__name__}
            return {"sample_keys": []}
        if suffix == ".csv":
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                return {"fieldnames": reader.fieldnames or []}
        if suffix == ".log":
            with path.open("r", encoding="utf-8") as handle:
                first_line = handle.readline().strip()
            return {"first_line": first_line[:250] if first_line else ""}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {"inspection_error": "Could not read file as text or structured data."}
    return {}


def _collect_rows(sqlite_sources: list[FileSource]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in sqlite_sources:
        tables = set(source.details.get("tables", []))
        if not source.exists or "signals" not in tables:
            continue

        connection = sqlite3.connect(source.path)
        connection.row_factory = sqlite3.Row
        try:
            signal_rows = connection.execute("SELECT * FROM signals ORDER BY signal_time ASC, id ASC").fetchall()
            outcome_map = {}
            if "signal_outcomes" in tables:
                outcome_map = {
                    row["signal_id"]: dict(row)
                    for row in connection.execute("SELECT * FROM signal_outcomes").fetchall()
                }

            event_by_signal_id: dict[int, dict[str, Any]] = {}
            event_by_event_key: dict[tuple[str, str], dict[str, Any]] = {}
            if "event_states" in tables:
                for row in connection.execute("SELECT * FROM event_states").fetchall():
                    data = dict(row)
                    signal_id = data.get("signal_id")
                    if signal_id is not None:
                        event_by_signal_id[int(signal_id)] = data
                    event_key = (str(data.get("event_id") or ""), str(data.get("symbol") or ""))
                    if event_key != ("", ""):
                        event_by_event_key[event_key] = data

            for signal_row in signal_rows:
                signal = dict(signal_row)
                signal_id = int(signal["id"])
                outcome = outcome_map.get(signal_id)
                event_state = event_by_signal_id.get(signal_id) or event_by_event_key.get(
                    (str(signal.get("event_id") or ""), str(signal.get("symbol") or ""))
                )
                rows.append(_build_export_row(signal, outcome, event_state, source.path))
        finally:
            connection.close()

    rows.sort(key=lambda row: (row.get("timestamp_utc") or "", str(row.get("signal_id") or "")))
    return rows


def _build_export_row(
    signal: dict[str, Any],
    outcome: dict[str, Any] | None,
    event_state: dict[str, Any] | None,
    db_path: Path,
) -> dict[str, Any]:
    row: dict[str, Any] = {}
    context_json = _parse_json_object(signal.get("context_json"))
    event_snapshot = _parse_json_object(event_state.get("event_features_snapshot") if event_state else None)

    signal_time = _parse_datetime(signal.get("signal_time"))
    signal_time_iso = _to_iso_utc(signal_time)
    entry_price = _to_float(signal.get("market_price"))
    trigger_window = None
    if event_state:
        trigger_window = event_state.get("trigger_window")
    if not trigger_window:
        trigger_window = _trigger_window_from_event_id(signal.get("event_id"))

    source_db_path = os.environ.get("SIGNAL_EXPORT_ORIGIN_DB_PATH") or str(db_path)
    row.update(
        {
            "signal_id": signal.get("id"),
            "timestamp_utc": signal_time_iso,
            "symbol": signal.get("symbol"),
            "side": "short",
            "signal_type": signal.get("signal_type"),
            "entry_reference_price": entry_price,
            "market_price": entry_price,
            "mark_price": _first_non_null(
                context_json.get("mark_price"),
                event_snapshot.get("mark_price"),
            ),
            "score": signal.get("score"),
            "grade": signal.get("grade"),
            "event_id": signal.get("event_id"),
            "trigger_window": trigger_window,
            "short_zone_low": _to_float(signal.get("short_zone_low")),
            "short_zone_high": _to_float(signal.get("short_zone_high")),
            "telegram_payload": None,
            "telegram_sent": bool(signal.get("telegram_sent")) if signal.get("telegram_sent") is not None else None,
            "telegram_status": _telegram_status(signal.get("telegram_sent")),
            "dedup_repeated_flag": None,
            "timeframe_metadata": trigger_window,
            "source_db_path": source_db_path,
            "source_signal_table": "signals",
            "source_outcome_table": "signal_outcomes" if outcome is not None else None,
            "source_event_state_table": "event_states" if event_state is not None else None,
        }
    )

    if outcome is not None:
        row["price_15m"] = _to_float(outcome.get("price_after_15m"))
        row["price_1h"] = _to_float(outcome.get("price_after_1h"))
        row["price_4h"] = _to_float(outcome.get("price_after_4h"))
        row["mfe_pct"] = _to_float(outcome.get("mfe_pct"))
        row["mae_pct"] = _to_float(outcome.get("mae_pct"))
    else:
        row["price_15m"] = None
        row["price_1h"] = None
        row["price_4h"] = None
        row["mfe_pct"] = None
        row["mae_pct"] = None

    row["price_5m"] = None
    row["price_30m"] = None
    row["price_12h"] = None
    row["price_24h"] = None
    row["best_price_until_24h"] = None
    row["worst_price_until_24h"] = None
    row["time_to_best_move_min"] = None
    row["time_to_worst_move_min"] = None

    for horizon in ("5m", "15m", "30m", "1h", "4h", "12h", "24h"):
        price_value = row.get(f"price_{horizon}")
        row[f"ret_{horizon}"] = _short_return_pct(entry_price, price_value)

    row["context__json"] = _json_dumps(context_json)
    row["event_snapshot__json"] = _json_dumps(event_snapshot)

    for prefix, values in (
        ("signals__", signal),
        ("signal_outcomes__", outcome or {}),
        ("event_states__", event_state or {}),
    ):
        _merge_source_columns(row, prefix, values)

    _merge_context_columns(row, context_json)
    return row


def _merge_source_columns(row: dict[str, Any], prefix: str, values: dict[str, Any]) -> None:
    for key, value in values.items():
        column = f"{prefix}{_sanitize_key(key)}"
        if isinstance(value, (dict, list)):
            row[column] = _json_dumps(value)
        elif _looks_like_json_text(value):
            row[column] = _json_dumps(_parse_json_object(value))
        elif _is_datetime_key(key):
            row[column] = _to_iso_utc(_parse_datetime(value))
        else:
            row[column] = value


def _merge_context_columns(row: dict[str, Any], context_json: dict[str, Any]) -> None:
    reasons = context_json.get("reasons")
    risk_flags = context_json.get("risk_flags")
    core_filters = context_json.get("core_filters") or {}
    score_breakdown = context_json.get("score_breakdown") or {}

    row["reason_count"] = len(reasons) if isinstance(reasons, list) else None
    row["risk_flag_count"] = len(risk_flags) if isinstance(risk_flags, list) else None
    row["reasons_json"] = _json_dumps(reasons) if reasons is not None else None
    row["risk_flags_json"] = _json_dumps(risk_flags) if risk_flags is not None else None
    row["core_filters_json"] = _json_dumps(core_filters) if core_filters else None
    row["score_breakdown_json"] = _json_dumps(score_breakdown) if score_breakdown else None

    for key, value in sorted(context_json.items()):
        if key in {"reasons", "risk_flags", "core_filters", "score_breakdown"}:
            continue
        column = f"feature__{_sanitize_key(key)}"
        if isinstance(value, (str, int, float, bool)) or value is None:
            if _is_datetime_key(key):
                row[column] = _to_iso_utc(_parse_datetime(value))
            else:
                row[column] = value
        else:
            row[f"{column}_json"] = _json_dumps(value)

    for key, value in sorted(core_filters.items()):
        row[f"core_filter__{_sanitize_key(key)}"] = value

    for key, value in sorted(score_breakdown.items()):
        row[f"score_breakdown__{_sanitize_key(key)}"] = value


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            handle.write("\n")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = _ordered_columns(rows)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in columns})


def _ordered_columns(rows: list[dict[str, Any]]) -> list[str]:
    ordered = list(CORE_CANONICAL_COLUMNS)
    seen = set(ordered)
    for row in rows:
        for key in row:
            if key not in seen:
                ordered.append(key)
                seen.add(key)
    return ordered


def _csv_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return _json_dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def _build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    timestamps = [row["timestamp_utc"] for row in rows if row.get("timestamp_utc")]
    completeness = _column_completeness(rows)
    return {
        "signals_exported": len(rows),
        "period_start_utc": min(timestamps) if timestamps else None,
        "period_end_utc": max(timestamps) if timestamps else None,
        "well_populated_fields": [item["field"] for item in completeness if item["ratio"] >= 0.9][:20],
        "sparse_fields": [item["field"] for item in completeness if item["ratio"] <= 0.1][:20],
        "reports": {
            "csv": str(RAW_SIGNALS_CSV),
            "jsonl": str(RAW_SIGNALS_JSONL),
            "inventory": str(DATA_INVENTORY_MD),
        },
    }


def _column_completeness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    counts: Counter[str] = Counter()
    for row in rows:
        for key, value in row.items():
            if value is not None and value != "":
                counts[key] += 1
    completeness = []
    for key in _ordered_columns(rows):
        ratio = counts[key] / len(rows)
        completeness.append({"field": key, "count": counts[key], "ratio": round(ratio, 4)})
    return completeness


def _matched_fields(rows: list[dict[str, Any]]) -> list[str]:
    matched = ["signals.id -> signal_id"]
    if any(row.get("source_outcome_table") for row in rows):
        matched.append("signals.id -> signal_outcomes.signal_id")
    if any(row.get("source_event_state_table") for row in rows):
        matched.append("signals.id -> event_states.signal_id (fallback: event_id + symbol)")
    if rows:
        matched.append("signals.context_json -> feature__/core_filter__/score_breakdown__ columns")
    return matched


def _export_rules(sqlite_sources: list[FileSource]) -> list[str]:
    rules = [
        "Primary extraction source is SQLite table `signals` when a local DB file exists.",
        "Rows are left-joined to `signal_outcomes` on `signals.id = signal_outcomes.signal_id`.",
        "Rows are matched to `event_states` first by `signal_id`, then by `(event_id, symbol)` when available.",
        "Canonical `ret_*` columns use short-side return formula: ((entry_price - future_price) / entry_price) * 100.",
        "Canonical post-signal prices are only filled from local persisted sources; missing horizons remain null.",
        "Telegram payload text is not reconstructed because the exact sent message is not persisted.",
        "Raw source fields are preserved with prefixes: `signals__`, `signal_outcomes__`, `event_states__`, `feature__`.",
    ]
    if not any(source.exists and "signals" in set(source.details.get("tables", [])) for source in sqlite_sources):
        rules.append("No local SQLite source with a `signals` table was found in this workspace; export is empty.")
    return rules


def _absent_fields(rows: list[dict[str, Any]]) -> list[str]:
    absent = []
    for field_name in ABSENT_FIELD_EXPLANATIONS:
        if not any(row.get(field_name) not in {None, ""} for row in rows):
            absent.append(field_name)
    return sorted(absent)


def _render_inventory(context: ExportContext) -> str:
    lines: list[str] = []
    lines.append("# Data Inventory")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Export generated at: {datetime.now(UTC).isoformat()}")
    lines.append(f"- Signals exported: {len(context.rows)}")
    lines.append(f"- Configured DB URL: `{context.configured_db_url}`")
    if context.configured_sqlite_path is not None:
        lines.append(f"- Configured SQLite path: `{context.configured_sqlite_path}`")
        lines.append(f"- Configured SQLite exists: `{str(context.configured_sqlite_exists).lower()}`")
    else:
        lines.append("- Configured SQLite path: `null`")
    lines.append("")
    if context.remote_origin:
        lines.append("## Remote Origin")
        lines.append("")
        primary_remote_path = context.remote_origin.get("primary_remote_path")
        snapshot_local_path = context.remote_origin.get("snapshot_local_path")
        snapshot_taken_at = context.remote_origin.get("snapshot_taken_at_utc")
        if primary_remote_path:
            lines.append(f"- Primary remote DB path: `{primary_remote_path}`")
        if snapshot_local_path:
            lines.append(f"- Local snapshot path: `{snapshot_local_path}`")
        if snapshot_taken_at:
            lines.append(f"- Snapshot taken at UTC: `{snapshot_taken_at}`")
        for candidate in context.remote_origin.get("remote_db_candidates", []):
            lines.append(f"- Remote DB candidate: `{candidate}`")
        for service in context.remote_origin.get("systemd_units", []):
            lines.append(f"- Related service: `{service}`")
        lines.append("")

    lines.append("## Found Data Sources")
    lines.append("")

    if context.sqlite_sources:
        lines.append("### SQLite / DB")
        lines.append("")
        for source in context.sqlite_sources:
            lines.append(f"- Path: `{source.path}`")
            lines.append(f"- Kind: `{source.kind}`")
            lines.append(f"- Exists: `{str(source.exists).lower()}`")
            if source.size_bytes is not None:
                lines.append(f"- Size bytes: `{source.size_bytes}`")
            for note in source.notes:
                lines.append(f"- Note: {note}")
            tables = source.details.get("tables", [])
            if tables:
                lines.append(f"- Tables: `{', '.join(tables)}`")
            row_counts = source.details.get("row_counts", {})
            for table_name in tables:
                columns = source.details.get("columns", {}).get(table_name, [])
                count = row_counts.get(table_name)
                lines.append(f"- `{table_name}` row count: `{count}`")
                lines.append(f"- `{table_name}` columns: `{', '.join(columns)}`")
            lines.append("")
    else:
        lines.append("- No SQLite files were discovered.")
        lines.append("")

    if context.flat_file_sources:
        lines.append("### JSON / JSONL / CSV / LOG")
        lines.append("")
        for source in context.flat_file_sources:
            lines.append(f"- Path: `{source.path}`")
            lines.append(f"- Kind: `{source.kind}`")
            lines.append(f"- Size bytes: `{source.size_bytes}`")
            if source.details:
                lines.append(f"- Inspect: `{json.dumps(source.details, ensure_ascii=False, sort_keys=True)}`")
            lines.append("")
    else:
        lines.append("- No JSON/JSONL/CSV/LOG files were discovered inside the workspace.")
        lines.append("")

    lines.append("## Runtime / Non-File Sources")
    lines.append("")
    lines.append("- Runtime logs: application logging is configured with `logging.basicConfig(...)` and no file handler is configured in the repo.")
    lines.append("- Telegram send logs: no dedicated persisted send-log source was found; only `signals.telegram_sent` exists.")
    lines.append("- Market snapshots: live shortlist snapshots are transient in memory and no persisted local source was found.")
    lines.append("- Candles / OHLCV / OI / funding / mark price / last price: live inputs exist in runtime, but no local persisted raw store was found in the workspace.")
    lines.append("")

    lines.append("## Fields Available Per Exported Signal")
    lines.append("")
    if context.rows:
        columns = _ordered_columns(context.rows)
        for column in columns:
            lines.append(f"- `{column}`")
    else:
        for column in CORE_CANONICAL_COLUMNS:
            lines.append(f"- `{column}`")
        lines.append("- `signals__*`, `signal_outcomes__*`, `event_states__*`, `feature__*` are added dynamically when source data exists.")
    lines.append("")

    lines.append("## Matched Fields Between Sources")
    lines.append("")
    for item in context.matched_fields:
        lines.append(f"- {item}")
    lines.append("")

    lines.append("## Missing / Null Fields")
    lines.append("")
    for field_name in context.absent_fields:
        lines.append(f"- `{field_name}`: {ABSENT_FIELD_EXPLANATIONS[field_name]}")
    lines.append("")

    lines.append("## Final Export Rules")
    lines.append("")
    for rule in context.export_rules:
        lines.append(f"- {rule}")
    lines.append("")

    return "\n".join(lines)


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _load_remote_origin_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in {None, ""}:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _parse_datetime(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _to_iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _to_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _short_return_pct(entry_price: float | None, future_price: float | None) -> float | None:
    if entry_price in {None, 0} or future_price is None:
        return None
    return round(((entry_price - future_price) / entry_price) * 100, 8)


def _telegram_status(value: Any) -> str | None:
    if value is None:
        return None
    return "sent" if bool(value) else "not_sent"


def _sanitize_key(value: Any) -> str:
    text = str(value).strip().lower()
    sanitized = []
    last_was_sep = False
    for char in text:
        if char.isalnum():
            sanitized.append(char)
            last_was_sep = False
            continue
        if not last_was_sep:
            sanitized.append("_")
            last_was_sep = True
    result = "".join(sanitized).strip("_")
    return result or "field"


def _is_datetime_key(key: Any) -> bool:
    text = str(key).lower()
    return text.endswith("_at") or text.endswith("_time") or text == "timestamp" or text == "asof"


def _looks_like_json_text(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return stripped.startswith("{") and stripped.endswith("}")


def _trigger_window_from_event_id(event_id: Any) -> str | None:
    if not isinstance(event_id, str):
        return None
    parts = event_id.split(":")
    if len(parts) >= 2 and parts[1]:
        return parts[1]
    return None


def _first_non_null(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


if __name__ == "__main__":
    main()
