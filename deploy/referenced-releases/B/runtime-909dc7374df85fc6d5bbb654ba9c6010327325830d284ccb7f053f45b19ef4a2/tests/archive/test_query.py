from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.research.archive import archive_database
from app.research.query import count_by_symbol, query_parquet

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")


def _make_snapshot(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE strategy_observations (
            observation_id TEXT PRIMARY KEY,
            observed_at DATETIME NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            strategy VARCHAR(64) NOT NULL,
            score INTEGER,
            nullable_value FLOAT
        );
        INSERT INTO strategy_observations VALUES
            ('obs-1', '2026-08-01 00:00:00.000000', 'AAAUSDT', 'LOW_VOLUME_EXTENSION_FAILURE', 70, NULL),
            ('obs-2', '2026-08-02 00:00:00.000000', 'AAAUSDT', 'VOLUME_CLIMAX_UNWIND', 80, 3.5),
            ('obs-3', '2026-08-03 00:00:00.000000', 'BBBUSDT', 'LOW_VOLUME_EXTENSION_FAILURE', 65, 1.5),
            ('obs-4', '2026-08-04 00:00:00.000000', 'CCCUSDT', 'VOLUME_CLIMAX_UNWIND', 90, 2.0);
        """
    )
    connection.commit()
    connection.close()


def test_duckdb_reads_partitioned_archive_and_matches_sqlite_queries(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.sqlite"
    output = tmp_path / "archive"
    _make_snapshot(source)
    result = archive_database(source, output, "2026-08-04T00:00:00Z", tables=["strategy_observations"])
    glob = (output / "dataset=strategy_observations" / "**" / "*.parquet").as_posix()

    with sqlite3.connect(source) as connection:
        sqlite_counts = connection.execute(
            "SELECT symbol, COUNT(*) FROM strategy_observations WHERE observed_at < ? GROUP BY symbol ORDER BY symbol",
            ("2026-08-04 00:00:00.000000",),
        ).fetchall()
        sqlite_strategy = connection.execute(
            "SELECT strategy, COUNT(*) FROM strategy_observations WHERE observed_at < ? GROUP BY strategy ORDER BY strategy",
            ("2026-08-04 00:00:00.000000",),
        ).fetchall()
        sqlite_range = connection.execute(
            "SELECT COUNT(*) FROM strategy_observations WHERE observed_at >= ? AND observed_at < ?",
            ("2026-08-02 00:00:00.000000", "2026-08-04 00:00:00.000000"),
        ).fetchone()[0]

    assert count_by_symbol(output, "strategy_observations") == sqlite_counts
    assert query_parquet(
        glob,
        "SELECT strategy, COUNT(*) FROM read_parquet(?) GROUP BY strategy ORDER BY strategy",
        [glob],
    ) == sqlite_strategy
    assert query_parquet(
        glob,
        "SELECT COUNT(*) FROM read_parquet(?) WHERE observed_at >= ? AND observed_at < ?",
        [glob, "2026-08-02 00:00:00+00", "2026-08-04 00:00:00+00"],
    )[0][0] == sqlite_range
    assert result.manifest["verification_status"] == "COMPLETE"
