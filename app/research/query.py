"""Read-only DuckDB helpers for published research Parquet datasets."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence


def query_parquet(parquet_glob: str | Path, query: str, parameters: Sequence[Any] | None = None) -> list[tuple[Any, ...]]:
    """Run an offline read-only SQL query over Parquet files.

    The caller supplies the SQL so this remains a research/admin boundary; the
    live bot never imports this module.
    """
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb is required for archive queries; install requirements-research.txt") from exc
    connection = duckdb.connect(database=":memory:")
    try:
        result = connection.execute(query, list(parameters or [])).fetchall()
        return [tuple(row) for row in result]
    finally:
        connection.close()


def count_by_symbol(dataset_root: str | Path, table: str) -> list[tuple[Any, ...]]:
    root = Path(dataset_root).expanduser().resolve()
    glob = (root / f"dataset={table}" / "**" / "*.parquet").as_posix()
    return query_parquet(
        glob,
        "SELECT symbol, COUNT(*) FROM read_parquet(?) GROUP BY symbol ORDER BY symbol",
        [glob],
    )
