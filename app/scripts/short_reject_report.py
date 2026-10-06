from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.config import load_config
from app.scripts.reporting_cli import add_report_window_args, resolve_since
from app.storage.db import Database
from app.storage.repository import BotRepository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Summarize reject stats for the short bot.")
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
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
