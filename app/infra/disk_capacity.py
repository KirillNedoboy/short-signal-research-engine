"""Filesystem capacity and SQLite storage-failure primitives."""

from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class DiskHealthState(StrEnum):
    HEALTHY = "HEALTHY"
    LOW_SPACE = "LOW_SPACE"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True)
class DiskCapacitySnapshot:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    free_percent: float
    total_inodes: int
    free_inodes: int
    free_inode_percent: float
    db_bytes: int
    wal_bytes: int


@dataclass(frozen=True)
class StorageIncidentEvent:
    event: str
    occurred_at: float
    failure_started_at: float | None = None
    duration_seconds: float | None = None
    free_bytes: int | None = None
    free_percent: float | None = None


class InsufficientDiskCapacity(RuntimeError):
    """Raised before a large artifact destination is created."""

    def __init__(
        self,
        *,
        available_bytes: int,
        required_bytes: int,
        artifact_estimate_bytes: int,
        reserve_bytes: int,
    ) -> None:
        self.available_bytes = available_bytes
        self.required_bytes = required_bytes
        self.artifact_estimate_bytes = artifact_estimate_bytes
        self.reserve_bytes = reserve_bytes
        super().__init__(
            "insufficient disk capacity: "
            f"available={available_bytes} required={required_bytes} "
            f"artifact_estimate={artifact_estimate_bytes} reserve={reserve_bytes}"
        )


def read_capacity(database_path: str | Path) -> DiskCapacitySnapshot:
    database = Path(database_path).expanduser().resolve()
    if not hasattr(os, "statvfs"):
        raise OSError("filesystem capacity inspection is unavailable on this platform")
    usage = os.statvfs(database.parent)
    total_bytes = usage.f_blocks * usage.f_frsize
    free_bytes = usage.f_bavail * usage.f_frsize
    used_bytes = max(0, (usage.f_blocks - usage.f_bfree) * usage.f_frsize)
    total_inodes = usage.f_files
    free_inodes = usage.f_favail
    db_bytes = database.stat().st_size
    wal_path = Path(f"{database}-wal")
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0
    return DiskCapacitySnapshot(
        total_bytes=total_bytes,
        used_bytes=used_bytes,
        free_bytes=free_bytes,
        free_percent=(free_bytes / total_bytes * 100) if total_bytes else 0.0,
        total_inodes=total_inodes,
        free_inodes=free_inodes,
        free_inode_percent=(free_inodes / total_inodes * 100) if total_inodes else 0.0,
        db_bytes=db_bytes,
        wal_bytes=wal_bytes,
    )


def derive_reserve_bytes(
    snapshot: DiskCapacitySnapshot,
    *,
    log_wal_margin_bytes: int,
    minimum_reserve_bytes: int,
) -> int:
    if log_wal_margin_bytes < 0 or minimum_reserve_bytes < 0:
        raise ValueError("capacity reserve components must be non-negative")
    return max(minimum_reserve_bytes, snapshot.db_bytes + log_wal_margin_bytes)


def require_capacity(
    snapshot: DiskCapacitySnapshot,
    *,
    artifact_estimate_bytes: int,
    temporary_overhead_bytes: int = 0,
    reserve_bytes: int,
) -> int:
    if artifact_estimate_bytes < 0 or temporary_overhead_bytes < 0 or reserve_bytes < 0:
        raise ValueError("capacity estimates must be non-negative")
    required_bytes = artifact_estimate_bytes + temporary_overhead_bytes + reserve_bytes
    if snapshot.free_bytes < required_bytes or snapshot.free_inodes < 1:
        raise InsufficientDiskCapacity(
            available_bytes=snapshot.free_bytes,
            required_bytes=required_bytes,
            artifact_estimate_bytes=artifact_estimate_bytes,
            reserve_bytes=reserve_bytes,
        )
    return required_bytes


def guarded_copy(
    source: str | Path,
    target: str | Path,
    *,
    reserve_bytes: int,
    temporary_overhead_bytes: int = 0,
    snapshot: DiskCapacitySnapshot | None = None,
) -> None:
    """Copy through a sibling partial file, only after a capacity preflight."""

    source_path = Path(source).expanduser().resolve()
    target_path = Path(target).expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if target_path.exists():
        raise FileExistsError(target_path)
    estimate = source_path.stat().st_size
    current = snapshot or read_capacity(source_path)
    require_capacity(
        current,
        artifact_estimate_bytes=estimate,
        temporary_overhead_bytes=temporary_overhead_bytes,
        reserve_bytes=reserve_bytes,
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    partial = target_path.with_name(target_path.name + ".partial")
    try:
        shutil.copy2(source_path, partial)
        os.replace(partial, target_path)
    finally:
        if partial.exists():
            partial.unlink()


def guarded_sqlite_backup(
    source: str | Path,
    target: str | Path,
    *,
    reserve_bytes: int,
    temporary_overhead_bytes: int = 0,
) -> None:
    """Create an SQLite online-backup artifact only after a capacity preflight."""

    source_path = Path(source).expanduser().resolve()
    target_path = Path(target).expanduser().resolve()
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if target_path.exists():
        raise FileExistsError(target_path)
    current = read_capacity(source_path)
    require_capacity(
        current,
        artifact_estimate_bytes=source_path.stat().st_size,
        temporary_overhead_bytes=temporary_overhead_bytes,
        reserve_bytes=reserve_bytes,
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    partial = target_path.with_name(target_path.name + ".partial")
    source_connection = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    target_connection = sqlite3.connect(partial)
    completed = False
    try:
        source_connection.execute("PRAGMA query_only=ON")
        source_connection.backup(target_connection)
        target_connection.commit()
        completed = True
    finally:
        target_connection.close()
        source_connection.close()
        if completed and partial.exists() and not target_path.exists():
            os.replace(partial, target_path)
        elif partial.exists():
            partial.unlink()


def is_sqlite_full(error: BaseException) -> bool:
    """Classify direct and nested SQLite FULL failures without matching locks."""

    seen: set[int] = set()
    current: BaseException | None = error
    for _ in range(24):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        code = getattr(current, "sqlite_errorcode", None)
        if code == getattr(sqlite3, "SQLITE_FULL", 13):
            return True
        name = str(getattr(current, "sqlite_errorname", ""))
        if name == "SQLITE_FULL":
            return True
        message = str(current).lower().replace("_", " ")
        if (
            "database or disk is full" in message
            or "database is full" in message
            or "disk is full" in message
        ):
            return True
        current = (
            getattr(current, "__cause__", None)
            or getattr(current, "__context__", None)
            or getattr(current, "orig", None)
        )
    return False


class StorageIncidentTracker:
    """In-memory dedupe for storage failures and capacity state transitions."""

    def __init__(self, *, cooldown_seconds: float) -> None:
        if cooldown_seconds <= 0:
            raise ValueError("cooldown_seconds must be positive")
        self.cooldown_seconds = float(cooldown_seconds)
        self._failure_started_at: float | None = None
        self._last_event_at: float | None = None

    def record_failure(self, *, at: float) -> StorageIncidentEvent | None:
        if self._failure_started_at is None:
            self._failure_started_at = at
            self._last_event_at = at
            return StorageIncidentEvent("DB_STORAGE_FULL_ENTERED", at, at)
        if (
            self._last_event_at is not None
            and at - self._last_event_at >= self.cooldown_seconds
        ):
            self._last_event_at = at
            return StorageIncidentEvent(
                "DB_STORAGE_FULL_STILL_ACTIVE",
                at,
                self._failure_started_at,
                at - self._failure_started_at,
            )
        return None

    def record_success(
        self, *, at: float, free_bytes: int, free_percent: float
    ) -> StorageIncidentEvent | None:
        if self._failure_started_at is None:
            return None
        started = self._failure_started_at
        self._failure_started_at = None
        self._last_event_at = at
        return StorageIncidentEvent(
            "DB_STORAGE_RECOVERED", at, started, at - started, free_bytes, free_percent
        )

    @staticmethod
    def health_state(
        *, free_bytes: int, reserve_bytes: int, free_inodes: int
    ) -> DiskHealthState:
        if free_bytes <= 0 or free_inodes <= 0:
            return DiskHealthState.CRITICAL
        if free_bytes < reserve_bytes:
            return DiskHealthState.LOW_SPACE
        return DiskHealthState.HEALTHY
