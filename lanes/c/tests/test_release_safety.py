"""Focused tests for release, compatibility, and shadow safety tooling."""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


LANE_ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_release_archive_is_deterministic_and_verifiable(tmp_path: Path) -> None:
    builder = _load("lane_c_builder", LANE_ROOT / "scripts" / "build_release_manifest.py")
    first = builder.build_release(tmp_path / "one.tar.gz")
    second = builder.build_release(tmp_path / "two.tar.gz")

    assert first["archive_sha256"] == second["archive_sha256"]
    verified = builder.verify_archive(tmp_path / "one.tar.gz")
    assert verified["source_pin"] == first["source_pin"]
    assert verified["files"] > verified["source_files"]

    import tarfile

    with tarfile.open(tmp_path / "one.tar.gz", "r:gz") as archive:
        names = [member.name for member in archive.getmembers()]
    assert "run_live.py" in names
    assert "requirements.lock" in names
    assert "deploy/lane-c-shadow-root-guard.service.example" in names
    assert all(not name.startswith(prefix) for name in names for prefix in ("data/", "runtime/", "vendor/", ".env"))

    extracted = tmp_path / "extracted"
    with tarfile.open(tmp_path / "one.tar.gz", "r:gz") as archive:
        archive.extractall(extracted)
    child_environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(extracted / "src"),
    }
    verified = subprocess.run(
        [sys.executable, "scripts/build_release_manifest.py", "--verify"],
        cwd=extracted,
        env=child_environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert verified.returncode == 0, verified.stderr
    help_result = subprocess.run(
        [sys.executable, "tools/shadow_guard.py", "--help"],
        cwd=extracted,
        env=child_environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert help_result.returncode == 0, help_result.stderr
    imported = subprocess.run(
        [
            sys.executable,
            "-c",
            "import app, app.main, app.runtime.legacy_engine, app.storage.identity; "
            "print(app.__file__); print(app.main.__file__); "
            "print(app.runtime.legacy_engine.__file__); print(app.storage.identity.__file__)",
        ],
        cwd=extracted,
        env=child_environment,
        capture_output=True,
        text=True,
        check=False,
    )
    if imported.returncode != 0 and "WinError 10106" in imported.stderr:
        pytest.skip("Windows asyncio socket support is unavailable in this environment")
    assert imported.returncode == 0, imported.stderr
    assert str(extracted) in imported.stdout
    assert str(LANE_ROOT.parent.parent) not in imported.stdout
    try:
        help_result = subprocess.run(
            [sys.executable, "run_live.py", "--help"],
            cwd=extracted,
            env=child_environment,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError("run_live.py --help did not terminate offline") from exc
    if help_result.returncode != 0 and "WinError 10106" in help_result.stderr:
        pytest.skip("Windows asyncio socket support is unavailable in this environment")
    assert help_result.returncode == 0, help_result.stderr
    assert "help" in (help_result.stdout + help_result.stderr).casefold()


def test_compat_report_preserves_safe_identity_and_skips_strategy_rows(tmp_path: Path) -> None:
    compat = _load("lane_c_compat", LANE_ROOT / "scripts" / "compat_validate.py")
    database = tmp_path / "reference.sqlite"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            PRAGMA user_version=1;
            CREATE TABLE event_states (
                symbol TEXT PRIMARY KEY, event_id TEXT NOT NULL, state TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE signals (
                id INTEGER PRIMARY KEY, symbol TEXT, event_id TEXT,
                strategy_type TEXT, strategy_subtype TEXT, model_version TEXT,
                signal_identity TEXT
            );
            CREATE TABLE telegram_delivery_outbox (
                id INTEGER PRIMARY KEY, entity_id INTEGER, entity_type TEXT,
                channel TEXT, idempotency_key TEXT, status TEXT,
                attempt_count INTEGER, sent_at TEXT, next_attempt_at TEXT,
                last_attempt_at TEXT, lease_until TEXT, created_at TEXT
            );
            CREATE TABLE strategy_observations (
                observation_id INTEGER PRIMARY KEY, input_snapshot_json TEXT
            );
            INSERT INTO event_states VALUES ('BTCUSDT', 'event-1', 'ACTIVE', '2026-10-17');
            INSERT INTO telegram_delivery_outbox VALUES
                (1, 7, 'signal', 'telegram', 'outbox-1', 'PENDING', 0,
                 NULL, NULL, NULL, NULL, '2026-10-17');
            INSERT INTO strategy_observations VALUES (1, 'secret payload must not be read');
            """
        )
    report = compat.inspect(database)
    assert report["db"]["read_only"] is True
    assert report["event_states"]["rows"] == 1
    assert report["telegram_delivery_outbox"]["rows"] == 1
    assert report["strategy_observations"] == {
        "present": True,
        "inspected": False,
        "rows": None,
        "reason": "intentionally not scanned; may be a large historical table",
    }
    assert "secret payload" not in json.dumps(report)


def test_shadow_guard_sanitizes_environment_and_times_out_child(tmp_path: Path) -> None:
    guard = _load("lane_c_guard", LANE_ROOT / "tools" / "shadow_guard.py")
    layout = guard.ShadowLayout.create(tmp_path / "shadow-env")
    environment = guard.sanitized_environment(
        layout,
        {
            "PATH": "safe",
            "TELEGRAM_TOKEN": "secret",
            "SIGNAL_CHAT_ID": "123",
            "DB_URL": "sqlite:///production.sqlite",
            "PRODUCTION_DB_URL": "sqlite:///production.sqlite",
        },
    )
    assert "TELEGRAM_TOKEN" not in environment
    assert "SIGNAL_CHAT_ID" not in environment
    assert "PRODUCTION_DB_URL" not in environment
    assert environment["AUTOEXECUTION"] == "OFF"
    assert layout.db.as_posix() in environment["DB_URL"]
    assert environment["SHADOW_ENV_PATH"].startswith(str(layout.root))

    script = LANE_ROOT / "tools" / "shadow_guard.py"
    root = tmp_path / "shadow-process"
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--root",
            str(root),
            "--max-runtime-sec",
            "0.15",
            "--poll-sec",
            "0.02",
            "--min-free-bytes",
            "0",
            "--shutdown-grace-sec",
            "0.1",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(2)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0
    report = json.loads(completed.stdout)
    assert report["reason"] == "max_runtime_exceeded"
    assert report["status"] == "completed"
    assert report["completion"]["expected_runtime_reached"] is True
    assert report["completion"]["observation_complete"] is True
    assert report["delivery_gate"] == "closed"
    assert (root / "shadow-guard-report.json").is_file()


def test_shadow_guard_does_not_complete_early_clean_child_exit(tmp_path: Path) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(LANE_ROOT / "tools" / "shadow_guard.py"),
            "--root",
            str(tmp_path / "shadow-early-exit"),
            "--max-runtime-sec",
            "5",
            "--poll-sec",
            "0.02",
            "--min-free-bytes",
            "0",
            "--shutdown-grace-sec",
            "0.1",
            "--",
            sys.executable,
            "-c",
            "raise SystemExit(0)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 1
    report = json.loads(completed.stdout)
    assert report["reason"] == "child_exit"
    assert report["status"] == "stopped"
    assert report["completion"]["expected_runtime_reached"] is False
    assert report["completion"]["observation_complete"] is False


def test_shadow_guard_requests_raw_disable_at_watermark_without_total_stop(
    tmp_path: Path,
) -> None:
    root = tmp_path / "shadow-raw-watermark"
    raw = root / "raw"
    raw.mkdir(parents=True)
    threshold = 1024
    (raw / "bounded-input.bin").write_bytes(b"x" * threshold)
    helper = tmp_path / "ack_raw_disable.py"
    helper.write_text(
        "import json, os, time\n"
        "from pathlib import Path\n"
        "event = Path(os.environ['SHADOW_EVENT_FILE'])\n"
        "event.write_text(json.dumps({'type': 'capability', 'raw_capture_disable': True}) + '\\n', encoding='utf-8')\n"
        "time.sleep(0.05)\n"
        "control = Path(os.environ['SHADOW_RAW_CONTROL_FILE'])\n"
        "deadline = time.monotonic() + 1.0\n"
        "while time.monotonic() < deadline and not control.exists(): time.sleep(0.01)\n"
        "if control.exists():\n"
        "    with event.open('a', encoding='utf-8') as handle: handle.write(json.dumps({'type': 'raw_capture_disabled'}) + '\\n')\n"
        "time.sleep(1.0)\n",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(LANE_ROOT / "tools" / "shadow_guard.py"),
            "--root",
            str(root),
            "--max-runtime-sec",
            "0.35",
            "--poll-sec",
            "0.01",
            "--min-free-bytes",
            "0",
            "--max-storage-bytes",
            "1048576",
            "--raw-storage-threshold-bytes",
            str(threshold),
            "--raw-disable-grace-sec",
            "0.1",
            "--shutdown-grace-sec",
            "0.1",
            "--",
            sys.executable,
            str(helper),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    report = json.loads(completed.stdout)
    assert report["status"] == "completed"
    assert report["reason"] == "max_runtime_exceeded"
    assert report["storage"]["raw_storage_threshold_bytes"] == threshold
    assert report["storage"]["raw_bytes"] >= threshold
    assert report["storage"]["raw_disable_requested"] is True
    assert report["storage"]["raw_disable_ack"] is True
    assert report["storage"]["bytes"] < 1048576


def test_shadow_guard_refuses_known_production_root() -> None:
    guard = _load("lane_c_guard_paths", LANE_ROOT / "tools" / "shadow_guard.py")
    try:
        guard._safe_shadow_root(Path("/opt/short-bot-lane-c"))
    except guard.ShadowGuardError:
        return
    raise AssertionError("known production root was accepted")


def test_shadow_guard_stops_on_machine_readable_429_event(tmp_path: Path) -> None:
    helper = tmp_path / "emit_event.py"
    helper.write_text(
        "import json, os, time\n"
        "from pathlib import Path\n"
        "Path(os.environ['SHADOW_EVENT_FILE']).write_text("
        "json.dumps({'type': 'rest_429', 'count': 1, 'payload': 'secret'}) + '\\n', "
        "encoding='utf-8')\n"
        "time.sleep(2)\n",
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(LANE_ROOT / "tools" / "shadow_guard.py"),
            "--root",
            str(tmp_path / "shadow-event"),
            "--max-runtime-sec",
            "5",
            "--poll-sec",
            "0.02",
            "--min-free-bytes",
            "0",
            "--shutdown-grace-sec",
            "0.1",
            "--",
            sys.executable,
            str(helper),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 1
    report = json.loads(completed.stdout)
    assert report["reason"] == "stop_event:rest_429"
    assert len(report["events"]) == 1
    assert report["events"][0]["type"] == "rest_429"
    assert report["events"][0]["count"] == 1
    assert report["events"][0]["raw_capture_disable"] is None
    assert "secret" not in completed.stdout


def test_shadow_guard_loads_named_ab_probes_and_filters_real_cycle_schema(tmp_path: Path) -> None:
    guard = _load("lane_c_guard_probes", LANE_ROOT / "tools" / "shadow_guard.py")
    baseline_a = tmp_path / "baseline-a.json"
    baseline_b = tmp_path / "baseline-b.json"
    baseline_a.write_text(
        json.dumps(
            {
                "records": [
                    {"duration_ms": 1000, "scheduled_symbols": 102},
                    {"duration_ms": 1200, "scheduled_symbols": 150},
                    {"duration_ms": 9000, "scheduled_symbols": 18},
                ]
            }
        ),
        encoding="utf-8",
    )
    baseline_b.write_text(
        json.dumps(
            {
                "records": [
                    {"duration_ms": 1500, "scheduled_symbols": 0},
                    {"duration_ms": 1700, "scheduled_symbols": 1},
                    {"duration_ms": 9000, "scheduled_symbols": 121},
                ]
            }
        ),
        encoding="utf-8",
    )
    production = tmp_path / "bot.sqlite"
    with sqlite3.connect(production) as connection:
        connection.executescript(
            """
            CREATE TABLE market_scan_cycles (
                cycle_id TEXT PRIMARY KEY,
                cycle_started_at TEXT,
                cycle_completed_at TEXT,
                duration_ms REAL,
                scheduled_symbols INTEGER,
                status TEXT,
                failed_symbols INTEGER,
                skipped_symbols INTEGER
            );
            INSERT INTO market_scan_cycles VALUES
                ('a', '2026-10-17T00:00:00Z', '2026-10-17T00:01:00Z', 1100, 121, 'ok', 0, 0),
                ('b', '2026-10-17T00:02:00Z', '2026-10-17T00:03:00Z', 1300, 149, 'ok', 0, 0),
                ('c', '2026-10-17T00:04:00Z', '2026-10-17T00:05:00Z', 9900, 0, 'ok', 0, 0);
            """
        )
    config = tmp_path / "p95-probes.json"
    config.write_text(
        json.dumps(
            {
                "probes": [
                    {
                        "name": "A",
                        "baseline": str(baseline_a),
                        "production_readonly_db": str(production),
                        "cohort_min": 100,
                        "cohort_max": 150,
                    },
                    {
                        "name": "B",
                        "baseline": str(baseline_b),
                        "production_readonly_db": str(production),
                        "cohort_min": 0,
                        "cohort_max": 1,
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    probes = guard._load_p95_probes(config)
    assert [probe["name"] for probe in probes] == ["A", "B"]
    a_metrics = guard._metrics_from_path(
        production, cohort_min=100, cohort_max=150
    )
    b_metrics = guard._metrics_from_path(
        production, cohort_min=0, cohort_max=1
    )
    assert a_metrics["samples"] == 2
    assert b_metrics["samples"] == 1
    assert a_metrics["cohort_min"] == 121
    assert a_metrics["cohort_max"] == 149
    assert b_metrics["cohort_min"] == 0
    assert b_metrics["cohort_max"] == 0

    for cohort_min, cohort_max in ((-1, 1), (0, 0)):
        invalid = json.loads(config.read_text(encoding="utf-8"))
        invalid["probes"][1]["cohort_min"] = cohort_min
        invalid["probes"][1]["cohort_max"] = cohort_max
        invalid_path = tmp_path / f"p95-probes-invalid-{cohort_min}-{cohort_max}.json"
        invalid_path.write_text(json.dumps(invalid), encoding="utf-8")
        with pytest.raises(guard.ShadowGuardError):
            guard._load_p95_probes(invalid_path)


def test_shadow_guard_p95_uses_recorded_cycle_window_and_reports_sample_range(tmp_path: Path) -> None:
    guard = _load("lane_c_guard_interval", LANE_ROOT / "tools" / "shadow_guard.py")
    production = tmp_path / "bot.sqlite"
    with sqlite3.connect(production) as connection:
        connection.executescript(
            """
            CREATE TABLE market_scan_cycles (
                cycle_id TEXT PRIMARY KEY,
                cycle_started_at TEXT,
                cycle_completed_at TEXT,
                duration_ms REAL,
                scheduled_symbols INTEGER
            );
            """
        )
        rows = []
        # Historical fast rows must not dilute the later slow interval.
        for index in range(100):
            rows.append(
                (
                    f"old-{index}",
                    f"2026-10-16T23:{index // 60:02d}:{index % 60:02d}Z",
                    f"2026-10-16T23:{index // 60:02d}:{index % 60:02d}Z",
                    1000,
                    121,
                )
            )
        rows.extend(
            [
                ("good-1", "2026-10-17T00:05:00Z", "2026-10-17T00:06:00Z", 1100, 121),
                ("good-2", "2026-10-17T00:25:00Z", "2026-10-17T00:26:00Z", 1200, 121),
                ("slow-1", "2026-10-17T00:35:00Z", "2026-10-17T00:36:00Z", 9000, 121),
                ("slow-2", "2026-10-17T00:55:00Z", "2026-10-17T00:56:00Z", 9100, 121),
            ]
        )
        connection.executemany("INSERT INTO market_scan_cycles VALUES (?, ?, ?, ?, ?)", rows)

    start = datetime(2026, 10, 17, tzinfo=timezone.utc)
    first = guard._metrics_from_path(
        production,
        cohort_min=100,
        cohort_max=150,
        interval_start=start,
        interval_end=start.replace(minute=30),
    )
    second = guard._metrics_from_path(
        production,
        cohort_min=100,
        cohort_max=150,
        interval_start=start.replace(minute=30),
        interval_end=start.replace(hour=1),
    )

    assert first["samples"] == 2
    assert first["p95_ms"] == 1100
    assert first["sample_time_min_utc"] == "2026-10-17T00:06:00Z"
    assert first["sample_time_max_utc"] == "2026-10-17T00:26:00Z"
    assert second["samples"] == 2
    assert second["p95_ms"] == 9000
    assert second["sample_time_min_utc"] == "2026-10-17T00:36:00Z"
    assert second["sample_time_max_utc"] == "2026-10-17T00:56:00Z"


def test_shadow_guard_fails_closed_when_a_probe_interval_has_no_recent_rows(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    baseline.write_text(
        json.dumps({"records": [{"duration_ms": 1000, "scheduled_symbols": 121}]}),
        encoding="utf-8",
    )
    production = tmp_path / "bot.sqlite"
    with sqlite3.connect(production) as connection:
        connection.execute(
            "CREATE TABLE market_scan_cycles ("
            "cycle_id TEXT PRIMARY KEY, cycle_started_at TEXT, cycle_completed_at TEXT, "
            "duration_ms REAL, scheduled_symbols INTEGER)"
        )
    config = tmp_path / "p95-probes.json"
    config.write_text(
        json.dumps(
            {
                "probes": [
                    {
                        "name": "A",
                        "baseline": str(baseline),
                        "production_readonly_db": str(production),
                        "cohort_min": 100,
                        "cohort_max": 150,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(LANE_ROOT / "tools" / "shadow_guard.py"),
            "--root",
            str(tmp_path / "shadow-no-recent"),
            "--p95-probes",
            str(config),
            "--ab-interval-sec",
            "0.02",
            "--poll-sec",
            "0.005",
            "--min-free-bytes",
            "0",
            "--shutdown-grace-sec",
            "0.1",
            "--",
            sys.executable,
            "-c",
            "import time; time.sleep(2)",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 1
    report = json.loads(completed.stdout)
    assert report["reason"] == "ab_p95_insufficient_recent_data:A"
    interval = report["ab_p95_gate"]["probes"]["A"]["intervals"][0]
    assert interval["insufficient_recent_data"] is True
    assert interval["samples"] == 0
    assert report["ab_p95_gate"]["passed"] is False
