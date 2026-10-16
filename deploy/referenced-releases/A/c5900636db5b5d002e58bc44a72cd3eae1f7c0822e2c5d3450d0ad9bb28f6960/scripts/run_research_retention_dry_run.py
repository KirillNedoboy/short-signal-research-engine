"""Disabled-by-default, read-only retention dry-run evidence job.

The job evaluates the existing versioned retention policy against the live
database opened strictly read-only and publishes projected row/byte counts
for research-classified tables (e.g. ``market_scan_symbol_results``).  It
never deletes: production deletion requires separate explicit authorization
and the underlying engine only ever proposes KEEP/ARCHIVE/DELETE decisions
for tables the policy classifies as research; signal, provenance, root-event,
attempt, outcome, outbox, and current event-state rows are protected by the
policy itself.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.config import AppConfig, load_config
from app.research.retention import evaluate_database, load_policy

LOGGER = logging.getLogger(__name__)

POLICY_PATH = (
    Path(__file__).resolve().parents[1]
    / "app"
    / "research"
    / "retention_policy_v1.json"
)


def project_counts(db_path: str, policy_path: Path = POLICY_PATH) -> dict[str, object]:
    """One read-only evaluation; returns bounded projected-row evidence."""
    result = evaluate_database(db_path, load_policy(policy_path))
    tables = [
        {
            "table": report["table_name"],
            "category": report["category"],
            "total": report["total"],
            "delete_eligible": report["delete_eligible"],
            "archive_only": report["archive_only"],
            "keep": report["keep"],
        }
        for report in result.tables
        if report.get("delete_eligible") or report.get("archive_only")
    ]
    return {
        "schema_version": 1,
        "mode": "DRY_RUN_READ_ONLY",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "policy_version": result.policy_version,
        "source_unchanged": result.source_unchanged,
        "projected": {
            "delete_eligible_rows": sum(
                int(table["delete_eligible"]) for table in tables
            ),
            "archive_only_rows": sum(int(table["archive_only"]) for table in tables),
        },
        "tables": tables,
        "protected_identity_counts": result.protected_identity_counts,
    }


class RetentionDryRunJob:
    """Periodic scheduled dry-run reporting inside the runtime process."""

    def __init__(
        self,
        *,
        db_path: str,
        report_path: str,
        interval_sec: float,
        logger: logging.Logger | None = None,
    ) -> None:
        self._db_path = db_path
        self._report_path = report_path
        self._interval_sec = max(60.0, float(interval_sec))
        self._logger = logger or LOGGER
        self._task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._run(), name="retention-dry-run")

    async def stop(self) -> None:
        self._stopping = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        while not self._stopping:
            try:
                report = await asyncio.to_thread(project_counts, self._db_path)
                Path(self._report_path).parent.mkdir(parents=True, exist_ok=True)
                tmp = Path(self._report_path + ".tmp")
                tmp.write_text(
                    json.dumps(report, sort_keys=True, separators=(",", ":")),
                    encoding="utf-8",
                )
                tmp.replace(self._report_path)
                self._logger.info(
                    "Retention dry-run projected | delete_eligible_rows=%s "
                    "archive_only_rows=%s unchanged=%s",
                    report["projected"]["delete_eligible_rows"],
                    report["projected"]["archive_only_rows"],
                    report["source_unchanged"],
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - evidence job must never gate runtime
                self._logger.exception("retention dry-run evaluation failed")
            await asyncio.sleep(self._interval_sec)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--db", help="override db path; default from config")
    parser.add_argument("--output", help="report path; default from config")
    parser.add_argument(
        "--once",
        action="store_true",
        help="run a single dry-run evaluation and exit (rollout mode)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config: AppConfig = load_config(config_path=args.config)
    if not config.research_retention_dry_run_enabled and not args.once:
        print("research_retention_dry_run_enabled=false; refusing to run (use --once)")
        return 2
    db_path = args.db or config.db_url.removeprefix("sqlite:///")
    report_path = args.output or config.research_retention_report_path
    if args.once:
        report = project_counts(db_path)
        Path(report_path).parent.mkdir(parents=True, exist_ok=True)
        Path(report_path).write_text(
            json.dumps(report, sort_keys=True, indent=2), encoding="utf-8"
        )
        print(json.dumps(report["projected"], sort_keys=True))
        return 0
    job = RetentionDryRunJob(
        db_path=db_path,
        report_path=report_path,
        interval_sec=config.research_retention_interval_sec,
    )

    async def run() -> None:
        job.start()
        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            await job.stop()

    asyncio.run(run())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
