from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from app.research.archive import (
    ArchiveError,
    archive_database,
    simulate_cleanup_on_copy,
    verify_manifest,
)


pyarrow = pytest.importorskip("pyarrow")


def _make_snapshot(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE signals (id INTEGER PRIMARY KEY);
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY,
            observed_at DATETIME NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            score INTEGER,
            ratio FLOAT,
            accepted BOOLEAN NOT NULL,
            payload JSON,
            nullable_value FLOAT
        );
        INSERT INTO strategy_observations VALUES
            ('obs-1', '2026-08-01 00:00:00.123456', 'AAAUSDT', 70, 0.125, 1, '{"b":2,"a":1}', NULL),
            ('obs-2', '2026-08-02 00:00:00.123456', 'AAAUSDT', 80, 999999.125, 0, '[1,null,"x"]', 3.5),
            ('obs-3', '2026-08-03 00:00:00.123456', 'BBBUSDT', NULL, NULL, 1, NULL, NULL),
            ('obs-boundary', '2026-08-04 00:00:00.000000', 'BBBUSDT', 99, 1.0, 1, 'boundary', 4.0),
            ('obs-after', '2026-08-05 00:00:00.000000', 'CCCUSDT', 100, 2.0, 1, 'after', 5.0);
        """
    )
    connection.commit()
    connection.close()


def test_archive_selection_and_fidelity(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    output = tmp_path / "archive"
    _make_snapshot(source)
    before_hash = hashlib.sha256(source.read_bytes()).hexdigest()

    result = archive_database(
        source,
        output,
        "2026-08-04T00:00:00Z",
        tables=["strategy_observations"],
        batch_size=2,
    )

    table = result.manifest["tables"][0]
    assert result.manifest["verification_status"] == "COMPLETE"
    assert table["source_row_count"] == 5
    assert table["selected_row_count"] == 3
    assert table["archive_row_count"] == 3
    assert table["min_timestamp"] == "2026-08-01T00:00:00.123456+00:00"
    assert table["max_timestamp"] == "2026-08-03T00:00:00.123456+00:00"
    assert table["null_counts"]["nullable_value"] == 2
    assert table["numeric_aggregates"]["score"]["sum"] == 150
    assert len(table["archive_files"]) == 2
    assert all("year=2026/month=08" in item["path"] for item in table["archive_files"])
    assert source.read_bytes()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before_hash

    parquet_rows = []
    for file_info in table["archive_files"]:
        parquet_rows.extend(pyarrow.parquet.read_table(output / file_info["path"]).to_pylist())
    parquet_rows.sort(key=lambda row: row["observation_id"])
    assert [row["observation_id"] for row in parquet_rows] == ["obs-1", "obs-2", "obs-3"]
    assert parquet_rows[0]["payload"] == '{"b":2,"a":1}'
    assert parquet_rows[0]["nullable_value"] is None
    assert parquet_rows[1]["ratio"] == 999999.125
    assert parquet_rows[1]["accepted"] is False
    assert parquet_rows[0]["observed_at"].isoformat() == "2026-08-01T00:00:00.123456+00:00"

    verification = verify_manifest(result.manifest_path, source)
    assert verification["verification_status"] == "PASS"
    assert verification["errors"] == []


def test_archive_is_idempotent_for_same_snapshot_window(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    output = tmp_path / "archive"
    _make_snapshot(source)

    first = archive_database(source, output, "2026-08-04T00:00:00Z", tables=["strategy_observations"])
    first_files = sorted(path.relative_to(output).as_posix() for path in output.rglob("*.parquet"))
    second = archive_database(source, output, "2026-08-04T00:00:00Z", tables=["strategy_observations"])
    second_files = sorted(path.relative_to(output).as_posix() for path in output.rglob("*.parquet"))

    assert second.manifest_path == first.manifest_path
    assert second_files == first_files
    assert len([path for path in (output / "manifests").glob("archive-run-*.json") if "incomplete" not in path.name]) == 1


def test_dry_run_is_read_only_and_does_not_create_archive(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    output = tmp_path / "archive"
    _make_snapshot(source)
    before_hash = hashlib.sha256(source.read_bytes()).hexdigest()

    result = archive_database(source, output, "2026-08-04T00:00:00Z", tables=["strategy_observations"], dry_run=True)

    assert result.manifest_path is None
    assert result.manifest["verification_status"] == "DRY_RUN"
    assert result.manifest["tables"][0]["selected_row_count"] == 3
    assert not output.exists()
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before_hash

def test_cli_supports_dry_run_and_verify_only(tmp_path: Path, capsys) -> None:
    from scripts.archive_research import main

    source = tmp_path / "snapshot.sqlite"
    output = tmp_path / "archive"
    _make_snapshot(source)

    assert main([
        "--db",
        str(source),
        "--output",
        str(output),
        "--before",
        "2026-08-04T00:00:00Z",
        "--table",
        "strategy_observations",
        "--dry-run",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["verification_status"] == "DRY_RUN"
    assert not output.exists()

    assert main([
        "--db",
        str(source),
        "--output",
        str(output),
        "--before",
        "2026-08-04T00:00:00Z",
        "--table",
        "strategy_observations",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--db",
        str(source),
        "--output",
        str(output),
        "--verify-only",
    ]) == 0
    assert json.loads(capsys.readouterr().out)["verification_status"] == "PASS"


def test_non_research_table_is_refused(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    _make_snapshot(source)

    with pytest.raises(ArchiveError, match="non-research"):
        archive_database(source, tmp_path / "archive", "2026-08-04T00:00:00Z", tables=["signals"])


def test_incomplete_manifest_is_not_publishable_and_rerun_recovers(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    output = tmp_path / "archive"
    _make_snapshot(source)
    manifests = output / "manifests"
    manifests.mkdir(parents=True)
    incomplete = manifests / "archive-run-crashed.incomplete-test.json"
    incomplete.write_text(json.dumps({"verification_status": "INCOMPLETE"}), encoding="utf-8")
    stale_staging = output / ".staging" / "crashed-at-write"
    stale_staging.mkdir(parents=True)
    (stale_staging / "partial.parquet").write_bytes(b"not complete")

    assert verify_manifest(incomplete)["verification_status"] == "INCOMPLETE"
    result = archive_database(source, output, "2026-08-04T00:00:00Z", tables=["strategy_observations"])

    assert result.manifest["verification_status"] == "COMPLETE"
    assert verify_manifest(result.manifest_path, source)["verification_status"] == "PASS"


def test_cleanup_simulation_only_mutates_disposable_copy(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    copy = tmp_path / "copy.sqlite"
    _make_snapshot(source)
    shutil.copy2(source, copy)
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()

    result = simulate_cleanup_on_copy(copy, "strategy_observations", "2026-08-04T00:00:00Z")

    assert result["rows_before"] == 5
    assert result["rows_eligible"] == 3
    assert result["rows_remaining"] == 2
    assert result["vacuum_performed"] is False
    assert hashlib.sha256(source.read_bytes()).hexdigest() == source_hash
    assert hashlib.sha256(copy.read_bytes()).hexdigest() != source_hash
