from __future__ import annotations

import ast
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from app.infra.disk_capacity import (
    DiskHealthState,
    InsufficientDiskCapacity,
    StorageIncidentTracker,
    derive_reserve_bytes,
    guarded_copy,
    guarded_sqlite_backup,
    is_sqlite_full,
    require_capacity,
)


def snapshot(
    *, free_bytes: int, free_inodes: int = 100, db_bytes: int = 1_000
) -> object:
    from app.infra.disk_capacity import DiskCapacitySnapshot

    return DiskCapacitySnapshot(
        total_bytes=10_000,
        used_bytes=10_000 - free_bytes,
        free_bytes=free_bytes,
        free_percent=free_bytes / 100,
        total_inodes=1_000,
        free_inodes=free_inodes,
        free_inode_percent=free_inodes / 10,
        db_bytes=db_bytes,
        wal_bytes=100,
    )


def test_reserve_uses_db_rollback_margin_and_configured_log_margin() -> None:
    assert (
        derive_reserve_bytes(
            snapshot(free_bytes=8_000, db_bytes=3_000),
            log_wal_margin_bytes=1_500,
            minimum_reserve_bytes=4_000,
        )
        == 4_500
    )


def test_preflight_rejects_before_copy_when_artifact_plus_overhead_exceeds_free_space() -> (
    None
):
    with pytest.raises(InsufficientDiskCapacity) as caught:
        require_capacity(
            snapshot(free_bytes=5_000),
            artifact_estimate_bytes=3_000,
            temporary_overhead_bytes=1_000,
            reserve_bytes=2_000,
        )
    error = caught.value
    assert error.available_bytes == 5_000
    assert error.required_bytes == 6_000
    assert error.artifact_estimate_bytes == 3_000
    assert error.reserve_bytes == 2_000
    assert "available=5000" in str(error)


def test_sqlite_full_detects_direct_and_wrapped_operational_errors() -> None:
    direct = sqlite3.OperationalError("database or disk is full")
    wrapped = RuntimeError("outer")
    wrapped.__cause__ = direct
    assert is_sqlite_full(direct)
    assert is_sqlite_full(wrapped)
    assert not is_sqlite_full(RuntimeError("database is locked"))


def test_incident_tracker_deduplicates_and_emits_recovery_after_write() -> None:
    tracker = StorageIncidentTracker(cooldown_seconds=60)
    first = tracker.record_failure(at=100.0)
    assert first.event == "DB_STORAGE_FULL_ENTERED"
    assert tracker.record_failure(at=110.0) is None
    assert tracker.record_failure(at=170.0).event == "DB_STORAGE_FULL_STILL_ACTIVE"
    recovered = tracker.record_success(at=180.0, free_bytes=9_000, free_percent=90.0)
    assert recovered.event == "DB_STORAGE_RECOVERED"
    assert recovered.duration_seconds == 80.0
    assert tracker.record_success(at=181.0, free_bytes=9_000, free_percent=90.0) is None


def test_health_state_is_exposed_without_sqlite_telemetry() -> None:
    tracker = StorageIncidentTracker(cooldown_seconds=60)
    assert (
        tracker.health_state(free_bytes=10_000, reserve_bytes=4_000, free_inodes=10)
        is DiskHealthState.HEALTHY
    )
    assert (
        tracker.health_state(free_bytes=3_999, reserve_bytes=4_000, free_inodes=10)
        is DiskHealthState.LOW_SPACE
    )
    assert (
        tracker.health_state(free_bytes=0, reserve_bytes=4_000, free_inodes=0)
        is DiskHealthState.CRITICAL
    )


def test_guarded_copy_does_not_create_destination_when_preflight_fails(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source.sqlite"
    target = tmp_path / "nested" / "target.sqlite"
    source.write_bytes(b"source")
    monkeypatch.setattr(
        "app.infra.disk_capacity.read_capacity",
        lambda _path: snapshot(free_bytes=5_000, db_bytes=1_000),
    )
    with pytest.raises(InsufficientDiskCapacity):
        guarded_copy(
            source, target, reserve_bytes=2_000, temporary_overhead_bytes=3_000
        )
    assert not target.exists()
    assert not target.parent.exists()


def test_guarded_copy_atomically_publishes_after_preflight(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source.sqlite"
    target = tmp_path / "nested" / "target.sqlite"
    source.write_bytes(b"source")
    monkeypatch.setattr(
        "app.infra.disk_capacity.read_capacity",
        lambda _path: snapshot(free_bytes=10_000, db_bytes=1_000),
    )
    guarded_copy(source, target, reserve_bytes=2_000)
    assert target.read_bytes() == b"source"
    assert not list(target.parent.glob("*.partial"))


def test_guarded_sqlite_backup_accounts_for_rehearsal_amplification(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "source.sqlite"
    target = tmp_path / "rehearsal" / "target.sqlite"
    with sqlite3.connect(source) as connection:
        connection.execute("create table sample (value text)")
        connection.execute("insert into sample values ('ok')")
    estimate = source.stat().st_size
    monkeypatch.setattr(
        "app.infra.disk_capacity.read_capacity",
        lambda _path: snapshot(free_bytes=estimate + 2_000, db_bytes=estimate),
    )
    with pytest.raises(InsufficientDiskCapacity) as caught:
        guarded_sqlite_backup(
            source,
            target,
            reserve_bytes=1_000,
            temporary_overhead_bytes=1_001,
        )
    assert caught.value.required_bytes == estimate + 2_001
    assert not target.exists()
    assert not target.parent.exists()


def test_large_copy_call_sites_are_centralized() -> None:
    root = Path(__file__).parents[1]
    offenders: list[str] = []
    for path in (
        *root.joinpath("app").rglob("*.py"),
        *root.joinpath("scripts").rglob("*.py"),
        *root.joinpath("research").rglob("*.py"),
    ):
        if path.name == "disk_capacity.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in {"copyfile", "copy2", "backup"}
            for node in ast.walk(tree)
        ):
            offenders.append(str(path.relative_to(root)))
    assert offenders == []


def test_migrate_db_direct_script_bootstraps_import_path(tmp_path: Path) -> None:
    script = Path(__file__).parents[1] / "scripts" / "migrate_db.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "migration plan" in result.stdout.lower()
