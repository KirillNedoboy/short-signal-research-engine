"""Bounded research-telemetry spool primitives.

This module is intentionally independent from the live strategy and SQLite
repository.  It provides the safe storage contract used by a later runtime
router: deterministic, bounded records; a disk guard that precedes the spool
quota; and an explicit fail-safe terminal-row lifecycle.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from app.infra.disk_capacity import DiskCapacitySnapshot, DiskHealthState


MAX_RECORD_BYTES = 64 * 1024
MAX_SPOOL_BYTES = 1 * 1024 * 1024 * 1024
MIN_SHADOW_HEADROOM_BYTES = 1 * 1024 * 1024 * 1024
SEGMENT_TARGET_BYTES = {
    "market_coverage_ledger": 16 * 1024 * 1024,
    "market_scan_symbol_results": 8 * 1024 * 1024,
    "climax_monitor_events": 4 * 1024 * 1024,
    "reject_stats": 1 * 1024 * 1024,
    "root_detector_shadow_legacy_mappings": 1 * 1024 * 1024,
}


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        .encode("utf-8")
    )


def canonical_sha256(value: Any) -> str:
    """Return SHA-256 over compact canonical JSON."""

    return hashlib.sha256(_json_bytes(value)).hexdigest()


_FORBIDDEN_PAYLOAD_KEY_PARTS = (
    "raw",
    "frame",
    "ticker_payload",
    "candles",
    "klines",
    "secret",
    "credential",
    "password",
    "private_key",
    "database",
    "sqlite",
    "ws_message",
)


def _validate_safe_payload(value: Any, *, path: str = "payload") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower()
            if any(part in normalized for part in _FORBIDDEN_PAYLOAD_KEY_PARTS):
                raise ValueError(f"forbidden raw or secret field in {path}.{key}")
            _validate_safe_payload(child, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _validate_safe_payload(child, path=f"{path}[{index}]")


@dataclass(frozen=True, slots=True)
class SpoolRecord:
    record_id: str
    dataset: str
    schema_version: int
    code_sha: str
    occurred_at_utc: str
    payload: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "record_id": self.record_id,
            "dataset": self.dataset,
            "schema_version": self.schema_version,
            "code_sha": self.code_sha,
            "occurred_at_utc": self.occurred_at_utc,
            "payload": dict(self.payload),
        }

    def encoded(self, *, max_bytes: int = MAX_RECORD_BYTES) -> bytes:
        data = _json_bytes(self.as_dict()) + b"\n"
        if len(data) > max_bytes:
            raise ValueError(f"spool record exceeds {max_bytes} bytes")
        return data


def make_spool_record(
    *,
    dataset: str,
    payload: Mapping[str, Any],
    code_sha: str,
    occurred_at_utc: datetime,
    schema_version: int = 1,
    max_bytes: int = MAX_RECORD_BYTES,
) -> SpoolRecord:
    """Create an allowlisted, deterministic record without raw market frames."""

    if not dataset or not code_sha:
        raise ValueError("dataset and code_sha are required")
    _validate_safe_payload(payload)
    occurred = occurred_at_utc.astimezone(timezone.utc).isoformat()
    body = {
        "dataset": dataset,
        "schema_version": schema_version,
        "code_sha": code_sha,
        "occurred_at_utc": occurred,
        "payload": dict(payload),
    }
    record = SpoolRecord(
        record_id=canonical_sha256(body),
        dataset=dataset,
        schema_version=schema_version,
        code_sha=code_sha,
        occurred_at_utc=occurred,
        payload=dict(payload),
    )
    record.encoded(max_bytes=max_bytes)
    return record


def shadow_headroom_bytes(
    *,
    release_bytes: int,
    sqlite_growth_60m_bytes: int,
    spool_growth_60m_bytes: int,
    wal_log_growth_60m_bytes: int,
) -> int:
    """Calculate the conservative SPOOL_SHADOW headroom requirement."""

    values = (
        release_bytes,
        sqlite_growth_60m_bytes,
        spool_growth_60m_bytes,
        wal_log_growth_60m_bytes,
    )
    if any(value < 0 for value in values):
        raise ValueError("headroom inputs must be non-negative")
    return max(
        MIN_SHADOW_HEADROOM_BYTES,
        release_bytes
        + 4
        * (
            sqlite_growth_60m_bytes
            + spool_growth_60m_bytes
            + wal_log_growth_60m_bytes
        ),
    )


def require_spool_capture_capacity(
    snapshot: DiskCapacitySnapshot,
    *,
    reserve_bytes: int,
    spool_bytes: int,
    spool_budget_bytes: int,
    record_bytes: int,
    shadow: bool = False,
    shadow_headroom_bytes: int = MIN_SHADOW_HEADROOM_BYTES,
) -> None:
    """Raise before capture when P0 disk health or spool limits are unsafe.

    The P0 reserve/health guard is evaluated before the configured spool quota;
    a low-space disk therefore pauses capture even when the spool is mostly
    empty.
    """

    if min(reserve_bytes, spool_bytes, spool_budget_bytes, record_bytes) < 0:
        raise ValueError("capacity values must be non-negative")
    if record_bytes > MAX_RECORD_BYTES:
        raise ValueError("record exceeds hard record ceiling")
    health = DiskHealthState.HEALTHY
    if snapshot.free_bytes <= 0 or snapshot.free_inodes <= 0:
        health = DiskHealthState.CRITICAL
    elif snapshot.free_bytes < reserve_bytes:
        health = DiskHealthState.LOW_SPACE
    if health is not DiskHealthState.HEALTHY:
        raise RuntimeError(f"research spool capture paused: disk={health.value}")
    if shadow and snapshot.free_bytes < reserve_bytes + shadow_headroom_bytes:
        raise RuntimeError("research spool shadow paused: insufficient healthy headroom")
    if spool_bytes + record_bytes > spool_budget_bytes:
        raise RuntimeError("research spool budget exhausted")


class ResearchSpool:
    """Small bounded append-only JSONL spool with atomic segment sealing."""

    def __init__(
        self,
        root: str | Path,
        *,
        max_bytes: int = MAX_SPOOL_BYTES,
        record_max_bytes: int = MAX_RECORD_BYTES,
    ) -> None:
        if max_bytes <= 0 or record_max_bytes <= 0:
            raise ValueError("spool limits must be positive")
        self.root = Path(root)
        self.max_bytes = max_bytes
        self.record_max_bytes = record_max_bytes
        self.root.mkdir(parents=True, exist_ok=True)
        for name in ("open", "sealed", "failed"):
            (self.root / name).mkdir(parents=True, exist_ok=True)

    def _size(self) -> int:
        return sum(
            path.stat().st_size
            for path in self.root.rglob("*.jsonl")
            if path.is_file()
        )

    def append(
        self,
        record: SpoolRecord,
        *,
        capacity: DiskCapacitySnapshot,
        reserve_bytes: int,
        shadow: bool = False,
        shadow_headroom_bytes: int = MIN_SHADOW_HEADROOM_BYTES,
    ) -> Path:
        data = record.encoded(max_bytes=self.record_max_bytes)
        require_spool_capture_capacity(
            capacity,
            reserve_bytes=reserve_bytes,
            spool_bytes=self._size(),
            spool_budget_bytes=self.max_bytes,
            record_bytes=len(data),
            shadow=shadow,
            shadow_headroom_bytes=shadow_headroom_bytes,
        )
        stem = f"{record.dataset}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
        target = self.root / "open" / f"{stem}.jsonl"
        target_limit = SEGMENT_TARGET_BYTES.get(record.dataset, 4 * 1024 * 1024)
        if target.exists() and target.stat().st_size + len(data) > target_limit:
            self.seal(target)
            target = self.root / "open" / f"{stem}-{uuid.uuid4().hex[:8]}.jsonl"
        with target.open("ab") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        return target

    def quarantine_failed(self, path: str | Path) -> Path:
        """Move a corrupt segment out of the active queues."""

        source = Path(path)
        if source.parent not in {self.root / "open", self.root / "sealed"}:
            raise ValueError("only open or sealed segments can be quarantined")
        target = self.root / "failed" / source.name
        os.replace(source, target)
        return target

    def seal(self, path: str | Path) -> Path:
        """Atomically move an open segment to the sealed queue."""

        source = Path(path)
        if source.parent != self.root / "open":
            raise ValueError("only open spool segments can be sealed")
        target = self.root / "sealed" / source.name
        os.replace(source, target)
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
        return target

    def recover_open(self) -> tuple[Path, ...]:
        """Return durable open segments for restart recovery without rewriting them."""

        return tuple(sorted(path for path in (self.root / "open").glob("*.jsonl") if path.is_file()))


class DRecordLifecycle:
    """Fail-safe state machine for delayed terminal rows.

    SQLite removal is eligible only after a sealed segment has a COMPLETE
    manifest and its identity digest has been verified.
    """

    def __init__(self) -> None:
        self.state = "LIVE"
        self.record_id: str | None = None
        self.segment_id: str | None = None
        self.manifest_id: str | None = None

    def mark_terminal(self, record_id: str) -> None:
        if self.state != "LIVE":
            raise RuntimeError(f"invalid terminal transition from {self.state}")
        self.record_id = record_id
        self.state = "TERMINAL"

    def mark_spooled(self, segment_id: str) -> None:
        if self.state != "TERMINAL":
            raise RuntimeError("row must be terminal before spooling")
        self.segment_id = segment_id
        self.state = "SPOOLED"

    def mark_sealed(self) -> None:
        if self.state != "SPOOLED":
            raise RuntimeError("segment must contain the terminal spool record")
        self.state = "SEALED"

    def mark_complete(self, manifest_id: str) -> None:
        if self.state != "SEALED":
            raise RuntimeError("segment must be SEALED before COMPLETE")
        self.manifest_id = manifest_id
        self.state = "COMPLETE"

    def verify_identity(self, *, verified: bool) -> None:
        if self.state != "COMPLETE":
            raise RuntimeError("manifest must be COMPLETE before identity verification")
        if not verified:
            raise RuntimeError("archive identity verification failed")
        self.state = "IDENTITY_VERIFIED"

    @property
    def sqlite_removal_eligible(self) -> bool:
        return self.state == "IDENTITY_VERIFIED"


def build_complete_manifest(
    *,
    dataset: str,
    segment_id: str,
    input_hashes: list[str],
    output_hashes: list[str],
    input_count: int,
    output_count: int,
    identity_digest: str,
    schema_fingerprint: str,
    code_sha: str,
    started_at_utc: str,
    ended_at_utc: str,
) -> dict[str, Any]:
    """Build the immutable COMPLETE manifest payload used by the compactor."""

    if input_count < 0 or output_count < 0:
        raise ValueError("manifest counts must be non-negative")
    manifest = {
        "status": "COMPLETE",
        "dataset": dataset,
        "segment_id": segment_id,
        "input_hashes": list(input_hashes),
        "output_hashes": list(output_hashes),
        "input_count": input_count,
        "output_count": output_count,
        "identity_digest": identity_digest,
        "schema_fingerprint": schema_fingerprint,
        "code_sha": code_sha,
        "started_at_utc": started_at_utc,
        "ended_at_utc": ended_at_utc,
    }
    manifest["manifest_id"] = canonical_sha256(manifest)
    return manifest


def atomic_write_json(path: str | Path, payload: Mapping[str, Any]) -> Path:
    """Write a deterministic JSON artifact using same-directory replace."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    data = _json_bytes(payload) + b"\n"
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        try:
            directory_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return target
