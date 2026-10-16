"""Offline research archive command; never deletes source or archive rows."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from app.config import AppConfig
from app.infra.disk_capacity import (
    derive_reserve_bytes,
    read_capacity,
    require_capacity,
)
from app.research.archive import (
    ArchiveError,
    archive_database,
    disk_budget_report,
    inventory_database,
    verify_manifest,
)
from app.research.retention import evaluate_database, load_policy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive research SQLite tables to immutable Parquet"
    )
    parser.add_argument(
        "--db", required=True, help="SQLite snapshot/copy; opened mode=ro"
    )
    parser.add_argument("--output", required=True, help="archive root directory")
    parser.add_argument("--before", help="strict UTC cutoff, e.g. 2026-08-24T00:00:00Z")
    parser.add_argument(
        "--table", action="append", dest="tables", help="research table; repeatable"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="report selection without writing output"
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="verify an existing complete manifest",
    )
    parser.add_argument("--manifest", help="manifest path for --verify-only")
    parser.add_argument(
        "--inventory",
        action="store_true",
        help="report every source table without writing",
    )
    parser.add_argument(
        "--disk-budget", action="store_true", help="report DB/WAL/archive disk budget"
    )
    parser.add_argument(
        "--retention-dry-run",
        action="store_true",
        help="evaluate lifecycle retention without writing output",
    )
    parser.add_argument(
        "--policy",
        type=Path,
        default=Path(__file__).resolve().parents[1]
        / "app"
        / "research"
        / "retention_policy_v1.json",
        help="versioned retention policy JSON",
    )
    parser.add_argument(
        "--as-of", help="UTC observation boundary for retention evaluation"
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=30,
        help="maximum representative retention samples",
    )
    return parser


def _latest_manifest(output: Path) -> Path:
    manifests = sorted(
        (
            path
            for path in (output / "manifests").glob("archive-run-*.json")
            if "incomplete" not in path.name
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not manifests:
        raise ArchiveError(f"no complete manifest found under {output / 'manifests'}")
    return manifests[0]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.retention_dry_run:
            incompatible = {
                "--before": args.before,
                "--table": args.tables,
                "--dry-run": args.dry_run,
                "--verify-only": args.verify_only,
                "--manifest": args.manifest,
                "--inventory": args.inventory,
                "--disk-budget": args.disk_budget,
            }
            selected = [name for name, value in incompatible.items() if value]
            if selected:
                raise ArchiveError(
                    "--retention-dry-run cannot be combined with " + ", ".join(selected)
                )
            if args.sample_limit < 0:
                raise ArchiveError("--sample-limit must be non-negative")
            result = evaluate_database(
                args.db,
                load_policy(args.policy),
                as_of_utc=args.as_of,
                sample_limit=args.sample_limit,
            )
            print(json.dumps(asdict(result), sort_keys=True, indent=2, default=str))
            return 0 if result.source_unchanged and result.query_only == 1 else 2
        if args.inventory:
            print(
                json.dumps(
                    inventory_database(args.db), sort_keys=True, indent=2, default=str
                )
            )
            return 0
        if args.disk_budget:
            print(
                json.dumps(
                    disk_budget_report(args.db, args.output), sort_keys=True, indent=2
                )
            )
            return 0
        if args.verify_only:
            manifest = (
                Path(args.manifest)
                if args.manifest
                else _latest_manifest(Path(args.output))
            )
            result = verify_manifest(manifest, args.db)
            print(json.dumps(result, sort_keys=True, indent=2, default=str))
            return 0 if result["verification_status"] == "PASS" else 2
        if not args.before:
            raise ArchiveError(
                "--before is required unless --verify-only or --disk-budget is used"
            )
        capacity = read_capacity(args.db)
        config = AppConfig()
        reserve = derive_reserve_bytes(
            capacity,
            log_wal_margin_bytes=config.disk_log_wal_margin_bytes,
            minimum_reserve_bytes=config.disk_min_safety_reserve_bytes,
        )
        require_capacity(
            capacity, artifact_estimate_bytes=capacity.db_bytes, reserve_bytes=reserve
        )
        result = archive_database(
            args.db,
            args.output,
            args.before,
            tables=args.tables,
            dry_run=args.dry_run,
        )
        print(json.dumps(result.manifest, sort_keys=True, indent=2, default=str))
        return 0
    except (ArchiveError, FileNotFoundError, ValueError) as exc:
        print(f"archive error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
