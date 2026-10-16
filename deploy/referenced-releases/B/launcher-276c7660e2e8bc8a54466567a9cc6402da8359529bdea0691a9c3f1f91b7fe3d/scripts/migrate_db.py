"""Explicit SQLite schema bootstrap and migration command."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from sqlalchemy import create_engine

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config
from app.infra.disk_capacity import (
    derive_reserve_bytes,
    guarded_copy,
    read_capacity,
)
from app.storage.migrations import (
    MigrationError,
    migrate_database,
    validate_schema,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap or validate the bot SQLite schema (admin-only)"
    )
    parser.add_argument(
        "database",
        nargs="?",
        type=Path,
        help="SQLite database path; defaults to db_url from --config",
    )
    parser.add_argument(
        "--db",
        "--database",
        dest="database_option",
        type=Path,
        help="SQLite database path (alternative to the positional path)",
    )
    parser.add_argument(
        "--db-url",
        help="SQLAlchemy database URL (takes precedence over --config)",
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--plan", action="store_true", help="Print a read-only migration plan (default)"
    )
    mode.add_argument(
        "--verify",
        "--check",
        "--validate",
        action="store_true",
        help="Validate without changing the database",
    )
    mode.add_argument(
        "--apply", action="store_true", help="Apply an explicit migration or bootstrap"
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Allow --apply to create an empty schema",
    )
    parser.add_argument(
        "--backup",
        type=Path,
        help="Required destination backup for --apply on an existing database",
    )
    v2 = parser.add_mutually_exclusive_group()
    v2.add_argument(
        "--include-shadow-v2",
        dest="include_shadow_v2",
        action="store_true",
        help="Require the optional root-detector shadow V2 table pair",
    )
    v2.add_argument(
        "--no-shadow-v2",
        dest="include_shadow_v2",
        action="store_false",
        help="Exclude optional root-detector shadow V2 tables from the contract (default)",
    )
    parser.set_defaults(include_shadow_v2=False)
    return parser


def _database_url(args: argparse.Namespace) -> str:
    paths = [path for path in (args.database, args.database_option) if path is not None]
    if len(paths) > 1:
        raise MigrationError("database path may be provided only once")
    if paths and args.db_url:
        raise MigrationError("database path cannot be combined with --db-url")
    if paths:
        return f"sqlite:///{paths[0]}"
    if args.db_url:
        return args.db_url
    from app.config import load_config

    config = load_config(config_path=args.config, env_path=args.env)
    return config.db_url


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.bootstrap and not args.apply:
            raise MigrationError("--bootstrap requires --apply")
        if args.backup is not None and not args.apply:
            raise MigrationError("--backup requires --apply")
        db_url = _database_url(args)
        include_shadow_v2 = args.include_shadow_v2
        if args.apply:
            from app.storage.db import Database

            database = Database(db_url)
            engine = database.engine
        else:
            if not db_url.startswith("sqlite:///"):
                raise MigrationError("read-only plan/verify supports SQLite only")
            source = Path(db_url.removeprefix("sqlite:///"))
            engine = create_engine(
                f"sqlite:///file:{source.as_posix()}?mode=ro&immutable=1&uri=true",
            )
        if args.verify:
            result = validate_schema(
                engine,
                include_shadow_v2=include_shadow_v2,
            )
            print(json.dumps(result.as_dict(), sort_keys=True, indent=2))
            return 0 if result.valid else 2

        current = validate_schema(engine, include_shadow_v2=include_shadow_v2)
        if not args.apply:
            print(
                json.dumps(
                    {
                        "mode": "plan",
                        "version": current.version,
                        "valid": current.valid,
                        "action": "NO_MIGRATIONS_NEEDED"
                        if current.valid
                        else "EXPLICIT_APPLY_REQUIRED",
                        "errors": list(current.errors),
                    },
                    sort_keys=True,
                    indent=2,
                )
            )
            return 0 if current.valid else 2

        if args.backup is not None and Path(db_url.removeprefix("sqlite:///")).exists():
            source = Path(db_url.removeprefix("sqlite:///"))
            snapshot = read_capacity(source)
            reserve = derive_reserve_bytes(
                snapshot,
                log_wal_margin_bytes=load_config(
                    config_path=args.config, env_path=args.env
                ).disk_log_wal_margin_bytes,
                minimum_reserve_bytes=load_config(
                    config_path=args.config, env_path=args.env
                ).disk_min_safety_reserve_bytes,
            )
            guarded_copy(source, args.backup, reserve_bytes=reserve, snapshot=snapshot)
        elif not args.bootstrap:
            raise MigrationError(
                "--backup is required for --apply on an existing database"
            )

        migration = migrate_database(
            engine,
            include_shadow_v2=include_shadow_v2,
        )
        print(
            json.dumps(
                {
                    "version": migration.version,
                    "previous_version": migration.previous_version,
                    "bootstrapped": migration.bootstrapped,
                    "adopted_legacy": migration.adopted_legacy,
                    "changed": migration.changed,
                    "validation": migration.validation.as_dict(),
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    except (MigrationError, OSError, ValueError) as exc:
        print(f"migration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
