"""Standalone read-only gate-attribution aggregation (LANE-B analytics).

Fills ``gate_stats`` for one or several UTC days from
``climax_evaluations.veto_reasons_json`` joined to the root outcomes
(``current_root_outcomes.return_4h``).

Design constraints:
- The source tables are only ever read (pure SELECTs).
- The only writes are an upsert into ``gate_stats``, the job's own table.
- No runtime wiring: this script is invoked by an operator (e.g. cron or
  a lane-B runbook), never by the bot process.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config  # noqa: E402
from app.storage.models import GateStatsModel  # noqa: E402


def _database_url(args: argparse.Namespace) -> str:
    if args.db_url:
        return args.db_url
    if args.database:
        return f"sqlite:///{args.database}"
    config = load_config(config_path=args.config, env_path=args.env)
    return config.db_url


def _expand_evaluations(engine: sa.Engine, *, since: datetime) -> list[dict[str, object]]:
    """Read evaluations (and their root outcome return_4h) since ``since``.

    Returns one entry per evaluation with a non-nullable strategy:
    {"period", "branch", "veto_codes", "return_4h"}.
    """
    from app.storage.models import ClimaxEvaluationModel, CurrentRootOutcomeModel

    stmt = (
        select(
            ClimaxEvaluationModel.evaluation_time,
            ClimaxEvaluationModel.strategy,
            ClimaxEvaluationModel.veto_reasons_json,
            CurrentRootOutcomeModel.return_4h,
        )
        .outerjoin(
            CurrentRootOutcomeModel,
            CurrentRootOutcomeModel.root_event_id
            == ClimaxEvaluationModel.root_event_id,
        )
        .where(ClimaxEvaluationModel.evaluation_time >= since)
    )
    rows: list[dict[str, object]] = []
    with Session(engine) as session:
        for row in session.execute(stmt):
            branch = row.strategy
            if not branch:
                continue
            vetoes = row.veto_reasons_json or []
            if not isinstance(vetoes, list):
                continue
            rows.append(
                {
                    "period": row.evaluation_time.strftime("%Y-%m-%d"),
                    "branch": branch,
                    "veto_codes": [str(code) for code in vetoes],
                    "return_4h": row.return_4h,
                }
            )
    return rows


def _aggregate(rows: list[dict[str, object]]) -> dict[tuple[str, str, str], dict[str, object]]:
    """Group by (period, branch, veto_code) into gate_stats rows.

    n_evals: evaluations whose veto list contains the code.
    n_single_blocker: evaluations whose veto list is exactly [code].
    mean_ret4h: average return_4h of the evaluations in the group
    (each evaluation counted once via distinct evaluation ids so that the
    mean is not skewed by multi-veto rows).
    """
    groups: dict[tuple[str, str, str], set[int]] = defaultdict(set)
    single: dict[tuple[str, str, str], set[int]] = defaultdict(set)
    returns: dict[tuple[str, str, str], dict[int, float | None]] = defaultdict(dict)
    for ordinal, row in enumerate(rows):
        period = row["period"]
        branch = row["branch"]
        veto_codes = row["veto_codes"]
        if not veto_codes:
            continue
        for code in veto_codes:
            key = (period, branch, code)
            groups[key].add(ordinal)
            returns[key][ordinal] = row["return_4h"]
        if len(veto_codes) == 1:
            key = (period, branch, veto_codes[0])
            single[key].add(ordinal)

    aggregated: dict[tuple[str, str, str], dict[str, object]] = {}
    for key in groups:
        group_eval_ids = groups[key]
        values = [v for v in returns[key].values() if v is not None]
        aggregated[key] = {
            "n_evals": len(group_eval_ids),
            "n_single_blocker": len(single[key]),
            "mean_ret4h": round(sum(values) / len(values), 6) if values else None,
        }
    return aggregated


def _upsert(engine: sa.Engine, rows: list[dict[str, object]]) -> int:
    if not rows:
        return 0
    stmt = sqlite_insert(GateStatsModel).values(
        [
            {
                "period": period,
                "branch": branch,
                "veto_code": veto_code,
                "n_evals": stats["n_evals"],
                "n_single_blocker": stats["n_single_blocker"],
                "mean_ret4h": stats["mean_ret4h"],
            }
            for (period, branch, veto_code), stats in rows.items()
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            GateStatsModel.period,
            GateStatsModel.branch,
            GateStatsModel.veto_code,
        ],
        set_={
            "n_evals": stmt.excluded.n_evals,
            "n_single_blocker": stmt.excluded.n_single_blocker,
            "mean_ret4h": stmt.excluded.mean_ret4h,
        },
    )
    with engine.begin() as connection:
        connection.execute(stmt)
    return len(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate daily gate-attribution stats into gate_stats"
    )
    parser.add_argument(
        "--database",
        type=Path,
        help="SQLite database path (alternative to --db-url / --config)",
    )
    parser.add_argument(
        "--db-url",
        help="SQLAlchemy database URL (takes precedence over --database/--config)",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=ROOT / "config.yaml",
        help="Runtime config used to resolve db_url (default: config.yaml)",
    )
    parser.add_argument(
        "--env",
        type=Path,
        default=ROOT / ".env",
        help="Environment file used with --config",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Aggregate evaluations from the trailing N UTC days (default: 1)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the aggregated rows without writing",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.days < 1:
        print("--days must be >= 1", file=sys.stderr)
        return 2
    engine = sa.create_engine(_database_url(args))
    since = datetime.now(UTC) - timedelta(days=args.days)
    rows = _expand_evaluations(engine, since=since)
    aggregated = _aggregate(rows)
    if args.dry_run:
        for (period, branch, veto_code), stats in sorted(aggregated.items()):
            print(f"{period}\t{branch}\t{veto_code}\t{stats}")
        return 0
    written = _upsert(engine, aggregated)
    print(f"gate_stats upserted rows={written} evaluations_scanned={len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())