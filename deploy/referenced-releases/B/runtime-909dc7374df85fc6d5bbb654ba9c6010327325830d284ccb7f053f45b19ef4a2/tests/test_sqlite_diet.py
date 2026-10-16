from __future__ import annotations

import json
import ast
from datetime import datetime, timezone
from pathlib import Path

from app.config import ResearchTelemetryStorageMode
from app.infra.disk_capacity import DiskCapacitySnapshot
from app.research.spool import ResearchSpool
from app.research.storage_health import ResearchStorageHealthPublisher
from app.research.storage_router import (
    RESEARCH_DATASET_CLASS,
    ResearchStorageRouter,
)
from app.research.spool_compactor import compact_sealed_segment


def _capacity(free_bytes: int = 3_000_000_000) -> DiskCapacitySnapshot:
    return DiskCapacitySnapshot(
        total_bytes=4_000_000_000,
        used_bytes=1_000_000_000,
        free_bytes=free_bytes,
        free_percent=free_bytes / 200_000,
        total_inodes=1000,
        free_inodes=500,
        free_inode_percent=50.0,
        db_bytes=1000,
        wal_bytes=100,
    )


def _router(tmp_path: Path, mode: ResearchTelemetryStorageMode) -> ResearchStorageRouter:
    return ResearchStorageRouter(
        mode=mode,
        spool=ResearchSpool(tmp_path / "spool", max_bytes=1024 * 1024),
        capacity_provider=lambda: _capacity(),
        reserve_bytes=1000,
        code_sha="test-sha",
    )


def test_sqlite_only_never_writes_spool(tmp_path: Path) -> None:
    router = _router(tmp_path, ResearchTelemetryStorageMode.SQLITE_ONLY)
    result = router.write(
        "market_coverage_ledger",
        {"symbol": "BTCUSDT", "status": "EXCLUDED"},
        identity="row-1",
        observed_at_utc=datetime.now(timezone.utc),
    )
    assert result.sqlite_required is True
    assert result.spooled is False
    assert not list((tmp_path / "spool").rglob("*.jsonl"))


def test_spool_shadow_dual_writes_and_canonical_stops_sqlite(tmp_path: Path) -> None:
    shadow = _router(tmp_path / "shadow", ResearchTelemetryStorageMode.SPOOL_SHADOW)
    shadow_result = shadow.write(
        "market_coverage_ledger", {"symbol": "BTCUSDT"}, identity="row-1", observed_at_utc=datetime.now(timezone.utc)
    )
    assert shadow_result.sqlite_required is True
    assert shadow_result.spooled is True

    canonical = _router(tmp_path / "canonical", ResearchTelemetryStorageMode.SPOOL_CANONICAL)
    canonical_result = canonical.write(
        "market_coverage_ledger", {"symbol": "BTCUSDT"}, identity="row-1", observed_at_utc=datetime.now(timezone.utc)
    )
    assert canonical_result.sqlite_required is False
    assert canonical_result.spooled is True


def test_class_a_and_e_always_require_sqlite(tmp_path: Path) -> None:
    router = _router(tmp_path, ResearchTelemetryStorageMode.SPOOL_CANONICAL)
    for dataset in ("signals", "climax_evaluations"):
        result = router.write(dataset, {"id": "x"}, identity="x", observed_at_utc=datetime.now(timezone.utc))
        assert result.sqlite_required is True
        assert result.spooled is False


def test_class_d_stays_sqlite_until_terminal(tmp_path: Path) -> None:
    router = _router(tmp_path, ResearchTelemetryStorageMode.SPOOL_CANONICAL)
    result = router.write(
        "strategy_observations",
        {"observation_id": "obs-1"},
        identity="obs-1",
        observed_at_utc=datetime.now(timezone.utc),
    )
    assert result.sqlite_required is True
    assert result.spooled is False


def test_terminal_row_is_not_removal_eligible_before_identity_verification(tmp_path: Path) -> None:
    router = _router(tmp_path, ResearchTelemetryStorageMode.SPOOL_CANONICAL)
    token = router.write_terminal(
        "strategy_observations",
        {"observation_id": "obs-1", "decision": "BLOCKED"},
        identity="obs-1",
        observed_at_utc=datetime.now(timezone.utc),
    )
    assert token is not None
    assert router.removal_eligibility(token) is False
    router.mark_sealed(token, "segment-1")
    router.mark_complete(token, "manifest-1")
    assert router.removal_eligibility(token) is False
    router.verify_identity(token, verified=True)
    assert router.removal_eligibility(token) is True


def test_low_disk_pauses_canonical_capture_without_sqlite_fallback(tmp_path: Path) -> None:
    router = ResearchStorageRouter(
        mode=ResearchTelemetryStorageMode.SPOOL_CANONICAL,
        spool=ResearchSpool(tmp_path / "spool"),
        capacity_provider=lambda: _capacity(free_bytes=500),
        reserve_bytes=1000,
        code_sha="test-sha",
    )
    result = router.write("market_coverage_ledger", {"symbol": "BTCUSDT"}, identity="x", observed_at_utc=datetime.now(timezone.utc))
    assert result.sqlite_required is False
    assert result.paused is True
    assert router.health_snapshot()["paused_total"] == 1


def test_open_segments_seal_atomically(tmp_path: Path) -> None:
    router = _router(tmp_path, ResearchTelemetryStorageMode.SPOOL_CANONICAL)
    router.write("market_coverage_ledger", {"symbol": "BTCUSDT"}, identity="x", observed_at_utc=datetime.now(timezone.utc))
    sealed = router.seal_open_segments()
    assert len(sealed) == 1
    assert list((tmp_path / "spool" / "open").glob("*.jsonl")) == []
    assert list((tmp_path / "spool" / "sealed").glob("*.jsonl"))


def test_canonical_cycle_keeps_operational_cycle_in_sqlite(tmp_path: Path) -> None:
    class Repository:
        def __init__(self) -> None:
            self.operational_called = False
            self.composite_called = False

        def record_market_scan_cycle_operational(self, **kwargs):
            self.operational_called = True
            return {"cycle_id": "sqlite-cycle"}

        def record_market_scan_cycle(self, **kwargs):
            self.composite_called = True
            raise AssertionError("Class C composite writer must not run")

    repository = Repository()
    router = _router(tmp_path, ResearchTelemetryStorageMode.SPOOL_CANONICAL)
    proxy = router.wrap_repository(repository)
    result = proxy.record_market_scan_cycle(
        cycle_started_at=datetime.now(timezone.utc),
        cycle_completed_at=datetime.now(timezone.utc),
        exchange_symbols=["BTCUSDT"],
        eligible_symbols=["BTCUSDT"],
        excluded=[],
        scheduled_symbols=["BTCUSDT"],
        symbol_results=[{"symbol": "BTCUSDT", "terminal_status": "SCANNED_OK"}],
    )
    assert result == {"cycle_id": "sqlite-cycle"}
    assert repository.operational_called is True
    assert repository.composite_called is False


def test_health_publisher_is_atomic_and_bounded(tmp_path: Path) -> None:
    target = tmp_path / "runtime" / "research-storage-health.json"
    publisher = ResearchStorageHealthPublisher(target)
    report = {"schema_version": 1, "storage_mode": "SQLITE_ONLY", "records": 0}
    assert publisher.publish(report) == target
    assert json.loads(target.read_text(encoding="utf-8"))["storage_mode"] == "SQLITE_ONLY"
    assert not list(target.parent.glob(".*.tmp"))


def test_write_map_has_every_production_table_once() -> None:
    map_path = Path(__file__).parents[1] / "app" / "research" / "sqlite_write_map_v1.json"
    payload = json.loads(map_path.read_text(encoding="utf-8"))
    names = [row["table"] for row in payload["tables"]]
    assert len(names) == len(set(names))
    expected = {
        "__db_heartbeat", "event_states", "signals", "signal_provenance", "signal_outcomes",
        "telegram_delivery_outbox", "watch_candidates", "climax_root_events", "climax_entry_attempts",
        "runtime_heartbeats", "runtime_heartbeat_history", "climax_evaluations", "volume_climax_observations",
        "strategy_observations", "climax_entry_attempt_events", "climax_monitor_events", "market_coverage_ledger",
        "market_scan_cycles", "market_scan_rotations", "market_scan_symbol_results", "reject_stats",
        "root_detector_shadow_candidates", "root_detector_shadow_episodes", "root_detector_shadow_observations",
        "root_detector_shadow_episode_outcomes", "root_detector_shadow_episode_root_links",
        "root_detector_shadow_legacy_mappings", "root_detector_shadow_v2_roots", "root_detector_shadow_v2_outcomes",
        "current_root_outcomes", "current_root_predicate_snapshots",
    }
    assert set(names) == expected
    assert set(RESEARCH_DATASET_CLASS) >= expected


def test_live_runtime_storage_modules_do_not_import_admin_engines() -> None:
    for relative in ("app/main.py", "app/research/storage_router.py", "app/research/storage_health.py"):
        tree = ast.parse((Path(__file__).parents[1] / relative).read_text(encoding="utf-8"))
        imports = [node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        imports.extend(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)
        assert not any(name.startswith(("pyarrow", "duckdb")) for name in imports)


def test_compactor_rerun_after_verified_delete_is_noop(tmp_path: Path) -> None:
    segment = tmp_path / "spool" / "sealed" / "coverage.jsonl"
    manifest_dir = tmp_path / "archive" / "manifests"
    manifest_dir.mkdir(parents=True)
    manifest = {
        "status": "COMPLETE",
        "input": {"path": str(segment)},
    }
    (manifest_dir / "compaction-existing.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    result = compact_sealed_segment(segment, tmp_path / "archive", code_sha="test")
    assert result["status"] == "NOOP_ALREADY_COMPACTED"
