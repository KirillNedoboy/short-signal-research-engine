"""Read-only report for durable delivery and legacy unsent signal state."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from urllib.parse import quote


def report(db_path: Path) -> dict[str, object]:
    uri = f"file:{quote(str(db_path.resolve()))}?mode=ro"
    with sqlite3.connect(uri, uri=True) as connection:
        tables = {
            row[0]
            for row in connection.execute("select name from sqlite_master where type='table'")
        }
        signal_count = connection.execute("select count(*) from signals").fetchone()[0]
        unsent_count = connection.execute("select count(*) from signals where telegram_sent = 0").fetchone()[0]
        legacy_unsent = connection.execute(
            "select count(*) from signals where telegram_sent = 0 "
            "and strategy_subtype is null and model_version is null"
        ).fetchone()[0]
        duplicate_groups = connection.execute(
            "select count(*) from ("
            "select symbol, event_id, strategy_subtype, model_version, count(*) as n "
            "from signals group by symbol, event_id, strategy_subtype, model_version having n > 1"
            ")"
        ).fetchone()[0]
        outbox = {}
        if "telegram_delivery_outbox" in tables:
            for status, count in connection.execute(
                "select status, count(*) from telegram_delivery_outbox group by status order by status"
            ):
                outbox[str(status)] = int(count)
        return {
            "db": str(db_path),
            "read_only": True,
            "signals": int(signal_count),
            "unsent_signals": int(unsent_count),
            "legacy_unsent_signals": int(legacy_unsent),
            "duplicate_signal_identity_groups": int(duplicate_groups),
            "delivery_outbox": outbox,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("data/bot.sqlite"))
    args = parser.parse_args()
    print(json.dumps(report(args.db), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
