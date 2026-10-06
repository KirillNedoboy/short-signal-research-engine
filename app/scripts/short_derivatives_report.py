from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import load_config
from app.scripts.reporting_cli import add_report_window_args, resolve_since
from app.storage.db import Database
from app.storage.repository import BotRepository

DERIVATIVES_STATUS_KEYS = ["OK", "MISSING", "API_ERROR", "RATE_LIMITED", "UNSUPPORTED_SYMBOL"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize derivatives diagnostics for the short bot.")
    add_report_window_args(parser)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    since = resolve_since(args)

    config = load_config(Path("config.yaml"))
    database = Database(config.db_url)
    database.create_all()
    repository = BotRepository(database)
    summary = repository.reject_reason_summary(hours=24, since=since)
    by_status = {key: int(summary.get("by_derivatives_status", {}).get(key, 0)) for key in DERIVATIVES_STATUS_KEYS}
    report = {
        "since": summary.get("since"),
        "rows_in_window": summary.get("rows_in_window", 0),
        "checked_candidates": summary.get("checked_candidates", 0),
        "by_derivatives_status": by_status,
        "derivatives_reason_counts": summary.get("derivatives_reason_counts", {}),
        "data_quality_counts": summary.get("data_quality_counts", {}),
        "funding_negative_count": summary.get("funding_negative_count", 0),
        "oi_rising_count": summary.get("oi_rising_count", 0),
        "symbols_missing_derivatives": summary.get("symbols_missing_derivatives", {}),
        "symbols_high_squeeze_risk": summary.get("symbols_high_squeeze_risk", {}),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
