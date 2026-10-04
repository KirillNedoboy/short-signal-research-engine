"""Admin-only spool to Parquet compaction.

This module is intentionally not imported by the live runtime.  It requires
optional research dependencies only when a compaction is requested.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.research.spool import atomic_write_json, canonical_sha256


class SpoolCompactionError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_segment(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict) or not value.get("record_id"):
                raise SpoolCompactionError(f"invalid spool record in {path}")
            rows.append(value)
    return rows


def _partition(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return f"year={parsed.year:04d}/month={parsed.month:02d}"


def compact_sealed_segment(
    segment: str | Path,
    archive_root: str | Path,
    *,
    code_sha: str,
) -> dict[str, Any]:
    """Compact one sealed NDJSON segment and delete it only after verification."""

    segment_path = Path(segment)
    root = Path(archive_root)
    if not segment_path.exists():
        for candidate in sorted((root / "manifests").glob("compaction-*.json")):
            try:
                existing = json.loads(candidate.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if (
                existing.get("status") == "COMPLETE"
                and str(existing.get("input", {}).get("path")) == str(segment_path)
            ):
                return {"status": "NOOP_ALREADY_COMPACTED", "manifest": existing}
        raise FileNotFoundError(segment_path)
    if segment_path.parent.name != "sealed":
        raise SpoolCompactionError("only sealed segments may be compacted")
    rows = _load_segment(segment_path)
    if not rows:
        raise SpoolCompactionError("cannot compact an empty segment")
    source_hash = _sha256_file(segment_path)
    segment_id = source_hash[:24]
    manifest_path = root / "manifests" / f"compaction-{segment_id}.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing.get("status") == "COMPLETE":
            return {"status": "NOOP_ALREADY_COMPACTED", "manifest": existing}

    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SpoolCompactionError("pyarrow is required for admin compaction") from exc

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        dataset = str(row["dataset"])
        grouped[(dataset, _partition(str(row["occurred_at_utc"])))].append(row)

    outputs: list[dict[str, Any]] = []
    for (dataset, partition), grouped_rows in sorted(grouped.items()):
        target = root / dataset / partition / f"batch-{segment_id}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            raise SpoolCompactionError(f"refusing to overwrite existing output: {target}")
        table = pa.table(
            {
                "record_id": [str(row["record_id"]) for row in grouped_rows],
                "dataset": [str(row["dataset"]) for row in grouped_rows],
                "schema_version": [int(row["schema_version"]) for row in grouped_rows],
                "code_sha": [str(row["code_sha"]) for row in grouped_rows],
                "occurred_at_utc": [str(row["occurred_at_utc"]) for row in grouped_rows],
                "payload_json": [json.dumps(row["payload"], sort_keys=True, separators=(",", ":")) for row in grouped_rows],
            }
        )
        pq.write_table(table, target, compression="zstd")
        outputs.append(
            {
                "path": str(target),
                "sha256": _sha256_file(target),
                "row_count": len(grouped_rows),
                "identity_digest": canonical_sha256([row["record_id"] for row in grouped_rows]),
            }
        )

    manifest = {
        "status": "COMPLETE",
        "manifest_version": 1,
        "segment_id": segment_id,
        "dataset": sorted({str(row["dataset"]) for row in rows}),
        "input": {"path": str(segment_path), "sha256": source_hash, "row_count": len(rows)},
        "outputs": outputs,
        "output_row_count": sum(int(item["row_count"]) for item in outputs),
        "code_sha": code_sha,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    manifest["manifest_id"] = canonical_sha256(manifest)
    atomic_write_json(manifest_path, manifest)

    if manifest["output_row_count"] != len(rows):
        raise SpoolCompactionError("compaction row count verification failed")
    os.unlink(segment_path)
    return {"status": "COMPACTED", "manifest": manifest}
