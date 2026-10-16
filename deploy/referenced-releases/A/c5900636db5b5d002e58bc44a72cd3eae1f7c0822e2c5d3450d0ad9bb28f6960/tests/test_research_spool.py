from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.infra.disk_capacity import DiskCapacitySnapshot
from app.research.spool import (
    DRecordLifecycle,
    ResearchSpool,
    atomic_write_json,
    build_complete_manifest,
    make_spool_record,
    require_spool_capture_capacity,
    shadow_headroom_bytes,
)


def _capacity(*, free_bytes: int = 10_000, free_inodes: int = 100) -> DiskCapacitySnapshot:
    return DiskCapacitySnapshot(
        total_bytes=100_000,
        used_bytes=90_000,
        free_bytes=free_bytes,
        free_percent=free_bytes / 1000,
        total_inodes=1000,
        free_inodes=free_inodes,
        free_inode_percent=free_inodes / 10,
        db_bytes=1_000,
        wal_bytes=100,
    )


def test_record_is_deterministic_and_bounded() -> None:
    when = datetime(2026, 9, 6, tzinfo=timezone.utc)
    first = make_spool_record(
        dataset="market_coverage_ledger",
        payload={"symbol": "BTCUSDT", "status": "EXCLUDED"},
        code_sha="abc",
        occurred_at_utc=when,
    )
    second = make_spool_record(
        dataset="market_coverage_ledger",
        payload={"status": "EXCLUDED", "symbol": "BTCUSDT"},
        code_sha="abc",
        occurred_at_utc=when,
    )
    assert first.record_id == second.record_id
    assert len(first.encoded()) <= 64 * 1024
    with pytest.raises(ValueError):
        make_spool_record(
            dataset="x",
            payload={"raw": "x" * 70_000},
            code_sha="abc",
            occurred_at_utc=when,
        )
    with pytest.raises(ValueError, match="forbidden"):
        make_spool_record(
            dataset="market_coverage_ledger",
            payload={"raw_ticker_payload": {"last": 1}},
            code_sha="abc",
            occurred_at_utc=when,
        )


def test_p0_disk_guard_precedes_spool_budget() -> None:
    with pytest.raises(RuntimeError, match="disk=LOW_SPACE"):
        require_spool_capture_capacity(
            _capacity(free_bytes=999),
            reserve_bytes=1_000,
            spool_bytes=0,
            spool_budget_bytes=1_000_000,
            record_bytes=10,
        )


def test_shadow_requires_real_health_headroom() -> None:
    assert shadow_headroom_bytes(
        release_bytes=1,
        sqlite_growth_60m_bytes=1,
        spool_growth_60m_bytes=1,
        wal_log_growth_60m_bytes=1,
    ) == 1 * 1024 * 1024 * 1024
    with pytest.raises(RuntimeError, match="headroom"):
        require_spool_capture_capacity(
            _capacity(free_bytes=2_000),
            reserve_bytes=1_000,
            spool_bytes=0,
            spool_budget_bytes=1_000_000,
            record_bytes=10,
            shadow=True,
            shadow_headroom_bytes=2_000,
        )


def test_spool_append_and_atomic_json(tmp_path) -> None:
    spool = ResearchSpool(tmp_path / "spool")
    record = make_spool_record(
        dataset="market_coverage_ledger",
        payload={"symbol": "ETHUSDT"},
        code_sha="abc",
        occurred_at_utc=datetime.now(timezone.utc),
    )
    segment = spool.append(record, capacity=_capacity(), reserve_bytes=1_000)
    assert segment.exists()
    target = atomic_write_json(tmp_path / "report.json", {"status": "COMPLETE"})
    assert target.read_text(encoding="utf-8").endswith("\n")
    assert list(tmp_path.glob(".*report.json.*")) == []


def test_delayed_row_removal_is_fail_safe() -> None:
    lifecycle = DRecordLifecycle()
    lifecycle.mark_terminal("row-1")
    lifecycle.mark_spooled("segment-1")
    lifecycle.mark_sealed()
    lifecycle.mark_complete("manifest-1")
    assert not lifecycle.sqlite_removal_eligible
    with pytest.raises(RuntimeError):
        lifecycle.verify_identity(verified=False)
    assert not lifecycle.sqlite_removal_eligible
    lifecycle.verify_identity(verified=True)
    assert lifecycle.sqlite_removal_eligible


def test_complete_manifest_is_explicitly_complete() -> None:
    manifest = build_complete_manifest(
        dataset="strategy_observations",
        segment_id="segment-1",
        input_hashes=["in"],
        output_hashes=["out"],
        input_count=1,
        output_count=1,
        identity_digest="identity",
        schema_fingerprint="schema",
        code_sha="abc",
        started_at_utc="2026-09-06T00:00:00Z",
        ended_at_utc="2026-09-06T00:01:00Z",
    )
    assert manifest["status"] == "COMPLETE"
    assert manifest["manifest_id"]
