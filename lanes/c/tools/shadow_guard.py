"""Fail-closed supervisor for a bounded Lane C shadow process.

The command shape is intentionally small and reviewable::

    python lanes/c/tools/shadow_guard.py --root /var/lib/lane-c-shadow -- \
        /opt/lane-c-release/.venv/bin/python /opt/lane-c-release/run_live.py

The supervisor owns all writable shadow paths, starts the child without a
shell, removes Telegram and database credentials from the inherited
environment, closes delivery before startup, and stops only the child process
group. The child integration contract is JSON Lines at ``SHADOW_EVENT_FILE``:

* ``{"type":"capability","raw_capture_disable":true}`` acknowledges the
  storage control protocol;
* ``{"type":"raw_capture_disabled"}`` acknowledges a control request;
* ``{"type":"ws_429"}`` or ``{"type":"rest_429"}`` is a machine-readable
  readiness stop event.

When the raw-storage threshold is reached, the guard writes
``{"action":"disable_raw_capture"}`` to ``SHADOW_RAW_CONTROL_FILE``. If the
child has not acknowledged that API, the guard stops fail-closed and never
deletes arbitrary data. Baseline/production read-only arguments provide a
bounded p95 gate probe; the production database is never assigned to the
child's ``DB_URL``.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 2
DEFAULT_MAX_RUNTIME_SEC = 24 * 60 * 60
DEFAULT_MAX_STORAGE_BYTES = 1 * 1024 * 1024 * 1024
DEFAULT_MIN_FREE_BYTES = 10 * 1024 * 1024 * 1024
KNOWN_PRODUCTION_PARTS = frozenset(
    {
        "short-telegram-bot-lite",
        "short-telegram-bot-lite-admin",
        "short-bot-lane-c",
    }
)
SENSITIVE_EXACT = frozenset(
    {
        "BOT_TOKEN",
        "TELEGRAM_TOKEN",
        "SIGNAL_CHAT_ID",
        "ALERTS_CHAT_ID",
        "TELEGRAM_CHAT_ID",
        "DB_URL",
        "DATABASE_URL",
        "SQLALCHEMY_DATABASE_URI",
    }
)
SENSITIVE_PARTS = ("TELEGRAM", "BOT_TOKEN", "CHAT_ID", "DATABASE_URL", "DB_URL")
STOP_EVENT_TYPES = frozenset({"ws_429", "rest_429", "http_429", "readiness_429", "new_429"})


class ShadowGuardError(RuntimeError):
    """Raised before startup when isolation cannot be guaranteed."""


def _path_parts(path: Path) -> set[str]:
    return {part.casefold() for part in path.resolve().parts}


def _looks_like_production(path: Path) -> bool:
    parts = _path_parts(path)
    return bool(parts & KNOWN_PRODUCTION_PARTS)


def _reject_symlink_chain(path: Path) -> None:
    current = path
    while True:
        if current.exists() and current.is_symlink():
            raise ShadowGuardError(f"shadow path or parent is a symlink: {path}")
        if current.parent == current:
            return
        current = current.parent


def _safe_shadow_root(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    _reject_symlink_chain(path)
    if _looks_like_production(path):
        raise ShadowGuardError("shadow root resolves into a production lane path")
    path.mkdir(parents=True, exist_ok=True)
    path = path.resolve()
    _reject_symlink_chain(path)
    if _looks_like_production(path):
        raise ShadowGuardError("shadow root resolves into a production lane path")
    try:
        path.chmod(0o700)
    except OSError as exc:
        raise ShadowGuardError("shadow root permissions cannot be restricted") from exc
    return path


def _safe_existing_path(value: str | Path, *, allow_production_readonly: bool = False) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    _reject_symlink_chain(path)
    try:
        path = path.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ShadowGuardError(f"path does not exist: {path}") from exc
    if not path.is_file():
        raise ShadowGuardError(f"path is not a regular file: {path}")
    if _looks_like_production(path) and not allow_production_readonly:
        raise ShadowGuardError("production path requires explicit read-only probe mode")
    return path


@dataclass(frozen=True, slots=True)
class ShadowLayout:
    root: Path
    db: Path
    runtime: Path
    locks: Path
    checkpoints: Path
    comparisons: Path
    raw: Path
    logs: Path
    event_file: Path
    raw_control: Path
    report: Path
    delivery_gate: Path

    @classmethod
    def create(cls, root: Path) -> "ShadowLayout":
        directories = {
            name: root / name
            for name in ("db", "runtime", "locks", "checkpoints", "comparisons", "raw", "logs")
        }
        for directory in directories.values():
            _reject_symlink_chain(directory)
            if directory.exists() and not directory.is_dir():
                raise ShadowGuardError(f"shadow path is not a directory: {directory}")
            directory.mkdir(parents=True, exist_ok=True)
            try:
                directory.chmod(0o700)
            except OSError as exc:
                raise ShadowGuardError(f"shadow directory permissions cannot be restricted: {directory}") from exc
        files = {
            "db": directories["db"] / "shadow.sqlite",
            "event_file": directories["runtime"] / "shadow-events.jsonl",
            "raw_control": directories["runtime"] / "raw-capture.control.json",
            "report": root / "shadow-guard-report.json",
            "delivery_gate": directories["runtime"] / "delivery-gate.json",
        }
        for path in files.values():
            _reject_symlink_chain(path)
            if path.exists() and path.is_dir():
                raise ShadowGuardError(f"shadow path is not a file: {path}")
        return cls(
            root=root,
            db=files["db"],
            runtime=directories["runtime"],
            locks=directories["locks"],
            checkpoints=directories["checkpoints"],
            comparisons=directories["comparisons"],
            raw=directories["raw"],
            logs=directories["logs"],
            event_file=files["event_file"],
            raw_control=files["raw_control"],
            report=files["report"],
            delivery_gate=files["delivery_gate"],
        )


def _credential_key(key: str) -> bool:
    upper = key.upper()
    return upper in SENSITIVE_EXACT or any(part in upper for part in SENSITIVE_PARTS)


def sanitized_environment(layout: ShadowLayout, inherited: dict[str, str] | None = None) -> dict[str, str]:
    """Return a child environment with no credentials or production DB URL."""

    source = dict(inherited if inherited is not None else os.environ)
    environment = {key: value for key, value in source.items() if not _credential_key(key)}
    db_uri = layout.db.resolve().as_uri()
    environment.update(
        {
            "AUTOEXECUTION": "OFF",
            "SHADOW_MODE": "1",
            "SHADOW_DELIVERY_GATE": "CLOSED",
            "DELIVERY_GATE": "CLOSED",
            "PYTHONNOUSERSITE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "SHADOW_ROOT": str(layout.root),
            # The application must use these paths while SHADOW_MODE=1.  It
            # must never fall back to the release directory's `.env`, which
            # can belong to the production lane.
            "SHADOW_CONFIG_PATH": str(layout.root / "config.yaml"),
            "SHADOW_ENV_PATH": str(layout.root / "shadow.env"),
            "SHADOW_ENV_FILE": str(layout.root / "shadow.env"),
            "SHADOW_DB_PATH": str(layout.db),
            "SHADOW_DB_URI": db_uri,
            "DB_URL": f"sqlite:///{layout.db.as_posix()}",
            "SHADOW_RUNTIME_ROOT": str(layout.runtime),
            "SHADOW_LOCK_PATH": str(layout.locks / "shadow.lock"),
            "SHADOW_CHECKPOINT_ROOT": str(layout.checkpoints),
            "SHADOW_COMPARISON_ROOT": str(layout.comparisons),
            "SHADOW_RAW_ROOT": str(layout.raw),
            "SHADOW_EVENT_FILE": str(layout.event_file),
            "SHADOW_RAW_CONTROL_FILE": str(layout.raw_control),
            "SHADOW_DELIVERY_GATE_FILE": str(layout.delivery_gate),
            "XDG_CACHE_HOME": str(layout.runtime / "cache"),
            "TMPDIR": str(layout.runtime / "tmp"),
        }
    )
    for directory in (layout.runtime / "cache", layout.runtime / "tmp"):
        directory.mkdir(parents=True, exist_ok=True)
    return environment


def _write_json(path: Path, value: dict[str, Any]) -> None:
    encoded = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, path)


def _storage_bytes(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        if path.is_symlink() or not path.is_file():
            continue
        try:
            total += path.stat().st_size
        except OSError:
            continue
    return total


def _p95(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[int(0.95 * (len(values) - 1))]


def _cohort(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _duration_values(values: Any) -> list[float]:
    if not isinstance(values, list):
        return []
    result: list[float] = []
    for item in values:
        try:
            value = float(item)
        except (TypeError, ValueError):
            continue
        if value >= 0:
            result.append(value)
    return result


def _cohort_bounds(value: Any) -> tuple[int | None, int | None]:
    """Parse a scheduled-symbol range without silently inventing a bound."""

    if isinstance(value, (list, tuple)) and len(value) == 2:
        low, high = _cohort(value[0]), _cohort(value[1])
        if low is not None and high is not None:
            return min(low, high), max(low, high)
    return None, None


def _metric_rows(
    rows: list[tuple[Any, Any]],
    *,
    cohort_min: int | None,
    cohort_max: int | None,
) -> dict[str, Any]:
    durations: list[float] = []
    cohorts: list[int] = []
    for duration, scheduled in rows:
        try:
            duration_value = float(duration)
        except (TypeError, ValueError):
            continue
        if duration_value < 0:
            continue
        cohort = _cohort(scheduled)
        if cohort_min is not None and (cohort is None or cohort < cohort_min):
            continue
        if cohort_max is not None and (cohort is None or cohort > cohort_max):
            continue
        durations.append(duration_value)
        if cohort is not None:
            cohorts.append(cohort)
    return {
        "p95_ms": _p95(durations),
        "samples": len(durations),
        "cohorts": cohorts,
        "cohort_min": min(cohorts) if cohorts else None,
        "cohort_max": max(cohorts) if cohorts else None,
    }


def _utc_timestamp(value: Any) -> datetime | None:
    """Parse a database timestamp into an aware UTC value.

    The C repository stores SQLAlchemy ``DateTime`` values as ISO-like text,
    but older SQLite copies can omit the timezone or use a trailing ``Z``.
    Treat a timezone-less value as UTC because the probe window is UTC.
    """

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _sqlite_utc_parameter(value: datetime) -> str:
    """Return a SQLite datetime() compatible UTC parameter."""

    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _metrics_from_json(
    path: Path,
    *,
    cohort_min: int | None,
    cohort_max: int | None,
) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ShadowGuardError("baseline is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ShadowGuardError("baseline JSON must be an object")

    raw_rows = payload.get("records")
    rows: list[tuple[Any, Any]] = []
    if isinstance(raw_rows, list):
        for row in raw_rows:
            if isinstance(row, dict):
                rows.append((row.get("duration_ms"), row.get("scheduled_symbols")))
    durations = payload.get("durations_ms")
    scheduled = payload.get("scheduled_symbols")
    if not rows and isinstance(durations, list):
        scheduled_values = scheduled if isinstance(scheduled, list) else []
        rows = [
            (duration, scheduled_values[index] if index < len(scheduled_values) else scheduled)
            for index, duration in enumerate(durations)
        ]
    if rows:
        metrics = _metric_rows(rows, cohort_min=cohort_min, cohort_max=cohort_max)
    else:
        value = payload.get("p95_ms", payload.get("scan_p95_ms"))
        if value is None:
            raise ShadowGuardError("baseline JSON has no p95_ms, records, or durations_ms")
        try:
            parsed_value = float(value)
        except (TypeError, ValueError) as exc:
            raise ShadowGuardError("baseline JSON p95_ms is not numeric") from exc
        if parsed_value < 0:
            raise ShadowGuardError("baseline JSON p95_ms must be non-negative")
        cohort = _cohort(scheduled)
        if cohort_min is not None and (cohort is None or cohort < cohort_min):
            cohort = None
        if cohort_max is not None and (cohort is None or cohort > cohort_max):
            cohort = None
        metrics = {
            "p95_ms": parsed_value,
            "samples": 1,
            "cohorts": [cohort] if cohort is not None else [],
            "cohort_min": cohort,
            "cohort_max": cohort,
        }
    return metrics


def _metrics_from_sqlite(
    path: Path,
    *,
    cohort_min: int | None,
    cohort_max: int | None,
    raise_errors: bool,
    interval_start: datetime | None = None,
    interval_end: datetime | None = None,
) -> dict[str, Any] | None:
    if (interval_start is None) != (interval_end is None):
        raise ShadowGuardError("p95 interval requires both start and end timestamps")
    if interval_start is not None and interval_end is not None:
        interval_start = interval_start.astimezone(timezone.utc)
        interval_end = interval_end.astimezone(timezone.utc)
        if interval_end <= interval_start:
            raise ShadowGuardError("p95 interval end must be after start")
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5.0)
        connection.execute("PRAGMA query_only = ON")
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(market_scan_cycles)").fetchall()
        }
        if "duration_ms" not in columns or "scheduled_symbols" not in columns:
            raise sqlite3.OperationalError("market_scan_cycles lacks duration/cohort columns")
        time_column = (
            "cycle_completed_at"
            if "cycle_completed_at" in columns
            else "cycle_started_at"
            if "cycle_started_at" in columns
            else None
        )
        if interval_start is not None and interval_end is not None and time_column is None:
            raise sqlite3.OperationalError(
                "market_scan_cycles lacks cycle_completed_at/cycle_started_at for bounded probe"
            )
        selected_time = f", {time_column}" if time_column else ""
        query = (
            "SELECT duration_ms, scheduled_symbols"
            f"{selected_time} FROM market_scan_cycles "
            "WHERE duration_ms IS NOT NULL"
        )
        parameters: tuple[str, ...] = ()
        if interval_start is not None and interval_end is not None:
            # Filter on the recorded cycle timestamp.  The query execution
            # time is deliberately not part of this window.
            query += (
                f" AND datetime({time_column}) >= datetime(?)"
                f" AND datetime({time_column}) < datetime(?)"
            )
            parameters = (
                _sqlite_utc_parameter(interval_start),
                _sqlite_utc_parameter(interval_end),
            )
        if time_column:
            query += f" ORDER BY datetime({time_column}) DESC"
        query += " LIMIT 10000"
        rows = connection.execute(query, parameters).fetchall()
    except (OSError, sqlite3.Error) as exc:
        if raise_errors:
            raise ShadowGuardError("baseline SQLite has no readable scan durations") from exc
        return None
    finally:
        try:
            connection.close()
        except (UnboundLocalError, AttributeError):
            pass
    metric_rows = [(row[0], row[1]) for row in rows]
    metrics = _metric_rows(metric_rows, cohort_min=cohort_min, cohort_max=cohort_max)
    metrics.update(
        {
            "timestamp_column": time_column,
            "interval_start_utc": _iso_utc(interval_start),
            "interval_end_utc": _iso_utc(interval_end),
            "sample_time_min_utc": None,
            "sample_time_max_utc": None,
            "sample_limit": 10000,
        }
    )
    if time_column:
        sample_times: list[datetime] = []
        for row in rows:
            one = _metric_rows([(row[0], row[1])], cohort_min=cohort_min, cohort_max=cohort_max)
            if one["samples"] != 1:
                continue
            timestamp = _utc_timestamp(row[2])
            if timestamp is not None:
                sample_times.append(timestamp)
        if sample_times:
            metrics["sample_time_min_utc"] = _iso_utc(min(sample_times))
            metrics["sample_time_max_utc"] = _iso_utc(max(sample_times))
    return metrics


def _metrics_from_path(
    path: Path,
    *,
    cohort_min: int | None = None,
    cohort_max: int | None = None,
    raise_errors: bool = True,
    interval_start: datetime | None = None,
    interval_end: datetime | None = None,
) -> dict[str, Any] | None:
    if interval_start is not None or interval_end is not None:
        if path.suffix.casefold() in {".json", ".jsonl"}:
            raise ShadowGuardError("bounded p95 intervals require a SQLite production probe")
    if path.suffix.casefold() in {".json", ".jsonl"}:
        return _metrics_from_json(path, cohort_min=cohort_min, cohort_max=cohort_max)
    return _metrics_from_sqlite(
        path,
        cohort_min=cohort_min,
        cohort_max=cohort_max,
        raise_errors=raise_errors,
        interval_start=interval_start,
        interval_end=interval_end,
    )


def _baseline_p95(path: Path) -> tuple[float | None, int | None]:
    """Backward-compatible single-probe helper."""

    metrics = _metrics_from_path(path)
    cohorts = metrics["cohorts"] if metrics else []
    return (
        metrics["p95_ms"] if metrics else None,
        max(cohorts) if cohorts else None,
    )


def _production_p95(path: Path) -> tuple[float | None, int | None]:
    """Backward-compatible single-probe helper."""

    metrics = _metrics_from_path(path, raise_errors=False)
    cohorts = metrics["cohorts"] if metrics else []
    return (
        metrics["p95_ms"] if metrics else None,
        max(cohorts) if cohorts else None,
    )


def _load_p95_probes(path: Path) -> list[dict[str, Any]]:
    """Load repeatable named A/B probes with explicit cohort ranges."""

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ShadowGuardError("p95 probe config is not valid JSON") from exc
    raw_probes = payload.get("probes") if isinstance(payload, dict) else payload
    if not isinstance(raw_probes, list) or not raw_probes:
        raise ShadowGuardError("p95 probe config requires a non-empty probes list")
    probes: list[dict[str, Any]] = []
    names: set[str] = set()
    for raw in raw_probes:
        if not isinstance(raw, dict):
            raise ShadowGuardError("each p95 probe must be an object")
        name = str(raw.get("name", "")).strip()
        if not name or len(name) > 64 or name in names:
            raise ShadowGuardError("p95 probe names must be unique and non-empty")
        names.add(name)
        baseline_value = raw.get("baseline")
        production_value = raw.get("production_readonly_db", raw.get("production"))
        if not baseline_value or not production_value:
            raise ShadowGuardError(f"p95 probe {name} requires baseline and production_readonly_db")
        range_value = raw.get("scheduled_symbols_range")
        cohort_min = _cohort(raw.get("cohort_min", raw.get("scheduled_symbols_min")))
        cohort_max = _cohort(raw.get("cohort_max", raw.get("scheduled_symbols_max")))
        if range_value is not None:
            range_min, range_max = _cohort_bounds(range_value)
            if range_min is None or range_max is None:
                raise ShadowGuardError(f"p95 probe {name} has an invalid scheduled_symbols_range")
            cohort_min = range_min if cohort_min is None else cohort_min
            cohort_max = range_max if cohort_max is None else cohort_max
        if cohort_min is None or cohort_max is None or cohort_min <= 0 or cohort_max < cohort_min:
            raise ShadowGuardError(
                f"p95 probe {name} requires a positive scheduled-symbol cohort range"
            )
        baseline = _safe_existing_path(baseline_value)
        production = _safe_existing_path(production_value, allow_production_readonly=True)
        baseline_metrics = _metrics_from_path(
            baseline,
            cohort_min=cohort_min,
            cohort_max=cohort_max,
        )
        if baseline_metrics is None or baseline_metrics.get("p95_ms") is None:
            raise ShadowGuardError(f"p95 probe {name} baseline has no comparable duration data")
        probes.append(
            {
                "name": name,
                "baseline": baseline,
                "production_readonly_db": production,
                "cohort_min": cohort_min,
                "cohort_max": cohort_max,
                "baseline_metrics": baseline_metrics,
            }
        )
    return probes


def _legacy_p95_probe(args: argparse.Namespace) -> list[dict[str, Any]]:
    """Adapt the original one-probe flags without changing their contract."""

    baseline_value = getattr(args, "baseline", None)
    production_value = getattr(args, "production_readonly_db", None)
    if baseline_value is None and production_value is None:
        return []
    if baseline_value is None or production_value is None:
        raise ShadowGuardError("--baseline and --production-readonly-db must be provided together")
    baseline = _safe_existing_path(baseline_value)
    production = _safe_existing_path(production_value, allow_production_readonly=True)
    explicit_cohort = getattr(args, "baseline_cohort", None)
    metrics = _metrics_from_path(baseline)
    recorded = max(metrics.get("cohorts") or [], default=None)
    cohort = explicit_cohort if explicit_cohort is not None else recorded
    if cohort is None or cohort <= 0:
        raise ShadowGuardError(
            "legacy p95 probe needs --baseline-cohort or a scheduled_symbols value"
        )
    baseline_metrics = _metrics_from_path(
        baseline,
        cohort_min=cohort,
        cohort_max=cohort,
    )
    if baseline_metrics is None or baseline_metrics.get("p95_ms") is None:
        raise ShadowGuardError("baseline has no duration data for the requested cohort")
    return [
        {
            "name": "legacy",
            "baseline": baseline,
            "production_readonly_db": production,
            "cohort_min": cohort,
            "cohort_max": cohort,
            "baseline_metrics": baseline_metrics,
        }
    ]


def _command_has_production_path(command: list[str]) -> bool:
    for argument in command:
        if not isinstance(argument, str) or ("/" not in argument and "\\" not in argument):
            continue
        candidate = Path(argument)
        try:
            if _looks_like_production(candidate):
                return True
        except (OSError, RuntimeError):
            continue
    return False


def _safe_event(raw: str) -> dict[str, Any] | None:
    try:
        event = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(event, dict):
        return None
    event_type = event.get("type", event.get("event"))
    if not isinstance(event_type, str):
        return None
    # Keep only bounded control metadata; never copy payload or credentials to
    # the machine-readable supervisor report.
    return {
        "type": event_type[:80],
        "at": str(event.get("at", datetime.now(timezone.utc).isoformat()))[:80],
        "count": int(event["count"]) if isinstance(event.get("count"), (int, float)) else None,
        "raw_capture_disable": bool(event.get("raw_capture_disable")) if "raw_capture_disable" in event else None,
    }


def _read_events(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], offset
    try:
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(offset)
            lines = handle.readlines()
            offset = handle.tell()
    except (OSError, UnicodeDecodeError):
        return [], offset
    events = []
    for line in lines:
        event = _safe_event(line.strip())
        if event is not None:
            events.append(event)
    return events, offset


def _write_control(layout: ShadowLayout) -> None:
    _write_json(layout.raw_control, {"action": "disable_raw_capture", "at": datetime.now(timezone.utc).isoformat()})


def _terminate_child(process: subprocess.Popen[Any], grace_sec: float) -> tuple[int | None, str]:
    if process.poll() is not None:
        return process.returncode, "already_exited"
    if os.name == "nt":
        process.terminate()
    else:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        return process.wait(timeout=max(0.1, grace_sec)), "terminated"
    except subprocess.TimeoutExpired:
        if os.name == "nt":
            process.kill()
        else:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            return process.wait(timeout=max(0.1, grace_sec)), "killed_after_grace"
        except subprocess.TimeoutExpired:
            return process.poll(), "cleanup_timeout"


def supervise(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    root = _safe_shadow_root(args.root)
    layout = ShadowLayout.create(root)
    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise ShadowGuardError("a child command is required after --")
    if _command_has_production_path(command):
        raise ShadowGuardError("child command contains a known production path")
    probe_config = getattr(args, "p95_probes", None)
    if probe_config is not None and (
        getattr(args, "baseline", None) is not None
        or getattr(args, "production_readonly_db", None) is not None
    ):
        raise ShadowGuardError("use --p95-probes or legacy single-probe flags, not both")
    probes = (
        _load_p95_probes(_safe_existing_path(probe_config))
        if probe_config is not None
        else _legacy_p95_probe(args)
    )
    if len(probes) > 1 and not {"A", "B"}.issubset({probe["name"] for probe in probes}):
        # Named probes are intentionally explicit in autonomous mode.  More
        # than two probes is supported, but A/B must remain visible in the
        # report so an operator cannot mistake a partial gate for coverage.
        raise ShadowGuardError("multiple p95 probes must include named A and B")
    baseline = probes[0]["baseline"] if len(probes) == 1 else None
    production_readonly_db = probes[0]["production_readonly_db"] if len(probes) == 1 else None
    baseline_metrics = probes[0]["baseline_metrics"] if len(probes) == 1 else None
    baseline_p95 = baseline_metrics.get("p95_ms") if baseline_metrics else None
    baseline_cohort = (
        probes[0]["cohort_max"]
        if len(probes) == 1
        else None
    )
    raw_storage_threshold_bytes = getattr(args, "raw_storage_threshold_bytes", None)
    if args.max_runtime_sec <= 0 or args.max_runtime_sec > DEFAULT_MAX_RUNTIME_SEC:
        raise ShadowGuardError("max runtime must be in (0, 24h]")
    if args.max_storage_bytes <= 0 or args.min_free_bytes < 0:
        raise ShadowGuardError("storage limits must be non-negative")
    if raw_storage_threshold_bytes is not None and (
        raw_storage_threshold_bytes <= 0
        or raw_storage_threshold_bytes > args.max_storage_bytes
    ):
        raise ShadowGuardError(
            "raw storage threshold must be in (0, max storage bytes]"
        )
    if args.poll_sec <= 0 or args.shutdown_grace_sec <= 0:
        raise ShadowGuardError("poll and shutdown grace must be positive")

    # The gate marker is written before Popen; even a child that exits during
    # import cannot observe an open delivery gate.
    _write_json(layout.delivery_gate, {"state": "closed", "reason": "shadow_guard_startup"})
    environment = sanitized_environment(layout)
    if any(_credential_key(key) and key.upper() != "DB_URL" for key in environment):
        raise ShadowGuardError("credential key survived environment sanitization")
    process_creation = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    stdout_path = layout.logs / "child.stdout.log"
    stderr_path = layout.logs / "child.stderr.log"
    started_at_utc = datetime.now(timezone.utc)
    started = time.monotonic()
    event_offset = 0
    events: list[dict[str, Any]] = []
    reason = "child_exit"
    raw_disable_supported = False
    raw_disable_requested = False
    raw_disable_ack = False
    raw_threshold_reached = False
    capability_wait_started: float | None = None
    probe_states = {
        probe["name"]: {"probe": probe, "intervals": [], "breaches": 0}
        for probe in probes
    }
    ab_intervals: list[dict[str, Any]] = []
    control_requested_at: float | None = None
    child_returncode: int | None = None
    cleanup_mode = "not_started"
    runtime_reached_with_child_running = False
    with stdout_path.open("ab") as stdout, stderr_path.open("ab") as stderr:
        process = subprocess.Popen(
            command,
            cwd=str(root),
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            start_new_session=(os.name != "nt"),
            creationflags=process_creation,
        )
        try:
            while True:
                child_returncode = process.poll()
                new_events, event_offset = _read_events(layout.event_file, event_offset)
                if new_events:
                    events.extend(new_events[-64:])
                    for event in new_events:
                        event_type = event["type"].casefold()
                        if event_type == "capability" and event.get("raw_capture_disable"):
                            raw_disable_supported = True
                        if event_type == "raw_capture_disabled":
                            raw_disable_ack = True
                        if event_type in STOP_EVENT_TYPES:
                            reason = f"stop_event:{event_type}"
                if reason.startswith("stop_event:"):
                    break
                if child_returncode is not None:
                    reason = "child_exit"
                    break
                elapsed = time.monotonic() - started
                if elapsed >= args.max_runtime_sec:
                    # The bounded 24-hour observation is complete only when
                    # the child was still alive at the deadline.  Preserve
                    # that fact before cleanup changes the return code.
                    runtime_reached_with_child_running = child_returncode is None
                    reason = "max_runtime_exceeded"
                    break
                try:
                    usage = _storage_bytes(root)
                    free = shutil.disk_usage(root).free
                except OSError:
                    usage = args.max_storage_bytes + 1
                    free = 0
                if free < args.min_free_bytes:
                    reason = "free_disk_below_reserve"
                    break
                raw_usage = _storage_bytes(layout.raw)
                raw_threshold_reached = bool(
                    raw_storage_threshold_bytes is not None
                    and raw_usage >= raw_storage_threshold_bytes
                )
                total_storage_limit_reached = usage >= args.max_storage_bytes
                if total_storage_limit_reached:
                    # The total-root budget is always a hard stop. A raw
                    # watermark may continue below this limit after an ACK,
                    # but an ACK can never extend the total budget.
                    reason = "storage_limit_exceeded"
                    break
                if raw_threshold_reached:
                    if not raw_disable_supported:
                        # A pre-existing raw directory can already be at the
                        # watermark before the child has emitted capability.
                        # Give the child one bounded capability window so a
                        # startup race cannot turn a supported control API
                        # into a false fail-closed result.
                        if capability_wait_started is None:
                            capability_wait_started = time.monotonic()
                        elif (
                            time.monotonic() - capability_wait_started
                            >= max(args.raw_disable_grace_sec, args.poll_sec)
                        ):
                            reason = "storage_threshold_raw_disable_api_unknown"
                            break
                    else:
                        capability_wait_started = None
                        if not raw_disable_requested:
                            raw_disable_requested = True
                            control_requested_at = time.monotonic()
                            _write_control(layout)
                        elif (
                            not raw_disable_ack
                            and control_requested_at is not None
                            and time.monotonic() - control_requested_at
                            >= args.raw_disable_grace_sec
                        ):
                            reason = "storage_threshold_raw_disable_not_acknowledged"
                            break
                if probes:
                    interval_age = time.monotonic() - started
                    if interval_age >= args.ab_interval_sec * (len(ab_intervals) + 1):
                        interval_number = len(ab_intervals) + 1
                        interval_start = started_at_utc + timedelta(
                            seconds=args.ab_interval_sec * (interval_number - 1)
                        )
                        interval_end = started_at_utc + timedelta(
                            seconds=args.ab_interval_sec * interval_number
                        )
                        interval_snapshot: dict[str, Any] = {
                            "interval": interval_number,
                            "interval_start_utc": _iso_utc(interval_start),
                            "interval_end_utc": _iso_utc(interval_end),
                            "probes": {},
                        }
                        for name, state in probe_states.items():
                            probe = state["probe"]
                            current = _metrics_from_path(
                                probe["production_readonly_db"],
                                cohort_min=probe["cohort_min"],
                                cohort_max=probe["cohort_max"],
                                raise_errors=False,
                                interval_start=interval_start,
                                interval_end=interval_end,
                            ) or {}
                            current_p95 = current.get("p95_ms")
                            cohorts = current.get("cohorts") or []
                            comparable = bool(
                                current_p95 is not None
                                and current.get("samples", 0) > 0
                                and cohorts
                                and current.get("sample_time_min_utc")
                                and current.get("sample_time_max_utc")
                            )
                            baseline_value = probe["baseline_metrics"].get("p95_ms")
                            breach = bool(
                                comparable
                                and baseline_value is not None
                                and current_p95 > float(baseline_value) * 1.2
                            )
                            state["breaches"] = state["breaches"] + 1 if breach else 0
                            interval = {
                                "interval_start_utc": _iso_utc(interval_start),
                                "interval_end_utc": _iso_utc(interval_end),
                                "p95_ms": current_p95,
                                "baseline_p95_ms": baseline_value,
                                "samples": current.get("samples", 0),
                                "timestamp_column": current.get("timestamp_column"),
                                "sample_time_min_utc": current.get("sample_time_min_utc"),
                                "sample_time_max_utc": current.get("sample_time_max_utc"),
                                "sample_limit": current.get("sample_limit", 10000),
                                "scheduled_cohort_min": min(cohorts) if cohorts else None,
                                "scheduled_cohort_max": max(cohorts) if cohorts else None,
                                "comparable": comparable,
                                "insufficient_recent_data": not comparable,
                                "breach_over_20pct": breach,
                            }
                            state["intervals"].append(interval)
                            interval_snapshot["probes"][name] = interval
                        ab_intervals.append(interval_snapshot)
                        insufficient_name = next(
                            (
                                name
                                for name, state in probe_states.items()
                                if state["intervals"][-1].get("insufficient_recent_data")
                            ),
                            None,
                        )
                        if insufficient_name is not None:
                            reason = f"ab_p95_insufficient_recent_data:{insufficient_name}"
                            break
                        breached_name = next(
                            (
                                name
                                for name, state in probe_states.items()
                                if state["breaches"] >= 2
                            ),
                            None,
                        )
                        if breached_name is not None:
                            reason = f"ab_p95_over_20pct_two_intervals:{breached_name}"
                            break
                time.sleep(args.poll_sec)
        finally:
            child_returncode, cleanup_mode = _terminate_child(process, args.shutdown_grace_sec)
    probe_reports: dict[str, dict[str, Any]] = {}
    for name, state in probe_states.items():
        probe = state["probe"]
        intervals = state["intervals"]
        probe_reports[name] = {
            "baseline": str(probe["baseline"]),
            "baseline_p95_ms": probe["baseline_metrics"].get("p95_ms"),
            "baseline_samples": probe["baseline_metrics"].get("samples", 0),
            "production_readonly_db": str(probe["production_readonly_db"]),
            "cohort_min": probe["cohort_min"],
            "cohort_max": probe["cohort_max"],
            "intervals": intervals,
            "breaches": state["breaches"],
            "passed": bool(
                len(intervals) >= 2
                and all(item.get("comparable") and not item.get("breach_over_20pct") for item in intervals[-2:])
            ),
        }
    ab_enabled = bool(probes)
    ab_passed = not ab_enabled or all(item["passed"] for item in probe_reports.values())
    expected_runtime_completion = bool(
        reason == "max_runtime_exceeded" and runtime_reached_with_child_running
    )
    observation_complete = bool(expected_runtime_completion and ab_passed)
    report = {
        "schema_version": SCHEMA_VERSION,
        "status": "completed" if observation_complete else "stopped",
        "reason": reason,
        "started_at_utc": _iso_utc(started_at_utc),
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "root": str(root),
        "child": {"pid": process.pid, "returncode": child_returncode, "cleanup": cleanup_mode, "command": command},
        "delivery_gate": "closed",
        "credentials_removed": True,
        "completion": {
            "expected_runtime_reached": expected_runtime_completion,
            "ab_p95_passed": ab_passed,
            "observation_complete": observation_complete,
        },
        "limits": {
            "max_runtime_sec": args.max_runtime_sec,
            "max_storage_bytes": args.max_storage_bytes,
            "raw_storage_threshold_bytes": raw_storage_threshold_bytes,
            "min_free_bytes": args.min_free_bytes,
        },
        "storage": {
            "bytes": _storage_bytes(root),
            "raw_bytes": _storage_bytes(layout.raw),
            "raw_storage_threshold_bytes": raw_storage_threshold_bytes,
            "raw_threshold_reached": raw_threshold_reached,
            "raw_disable_supported": raw_disable_supported,
            "raw_disable_requested": raw_disable_requested,
            "raw_disable_ack": raw_disable_ack,
        },
        "events": events[-64:],
        "ab_p95_gate": {
            "enabled": ab_enabled,
            "baseline": str(baseline) if baseline else None,
            "baseline_p95_ms": baseline_p95,
            "baseline_cohort": baseline_cohort,
            "production_readonly_db": str(production_readonly_db) if production_readonly_db else None,
            "intervals": ab_intervals,
            "breaches": sum(int(state["breaches"]) for state in probe_states.values()),
            "probes": probe_reports,
            "missing_data": [
                name
                for name, item in probe_reports.items()
                if any(not interval.get("comparable") for interval in item["intervals"])
            ],
            "passed": ab_passed,
        },
    }
    _write_json(layout.report, report)
    if args.report is not None:
        report_path = Path(args.report).expanduser().resolve()
        if _looks_like_production(report_path):
            raise ShadowGuardError("refusing a report path in a production lane")
        _write_json(report_path, report)
    return report, 0 if observation_complete else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path, help="dedicated writable shadow root")
    parser.add_argument("--max-runtime-sec", type=float, default=float(DEFAULT_MAX_RUNTIME_SEC))
    parser.add_argument("--max-storage-bytes", type=int, default=DEFAULT_MAX_STORAGE_BYTES)
    parser.add_argument(
        "--raw-storage-threshold-bytes",
        type=int,
        default=None,
        help="optional raw-directory watermark that requests disable_raw_capture",
    )
    parser.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE_BYTES)
    parser.add_argument("--poll-sec", type=float, default=5.0)
    parser.add_argument("--shutdown-grace-sec", type=float, default=10.0)
    parser.add_argument("--raw-disable-grace-sec", type=float, default=5.0)
    parser.add_argument("--baseline", type=Path, help="baseline JSON/SQLite p95 metrics path")
    parser.add_argument("--production-readonly-db", type=Path, help="explicit read-only production metrics DB")
    parser.add_argument("--baseline-cohort", type=int, help="minimum scheduled-symbol cohort for comparison")
    parser.add_argument(
        "--p95-probes",
        type=Path,
        help="JSON with named A/B baseline and read-only DB probes plus cohort ranges",
    )
    parser.add_argument("--ab-interval-sec", type=float, default=1800.0, help="p95 comparison interval; production uses 1800s")
    parser.add_argument("--report", type=Path, help="optional additional safe report path")
    parser.add_argument("command", nargs=argparse.REMAINDER, help="child command after --")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report, status = supervise(args)
    except ShadowGuardError as exc:
        report = {"schema_version": SCHEMA_VERSION, "status": "blocked", "reason": str(exc), "finished_at_utc": datetime.now(timezone.utc).isoformat()}
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
