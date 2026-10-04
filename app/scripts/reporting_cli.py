from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone


def add_report_window_args(parser: argparse.ArgumentParser) -> None:
    window_group = parser.add_mutually_exclusive_group()
    window_group.add_argument(
        "--since",
        type=_parse_since_arg,
        help="Include rows at or after this ISO timestamp, e.g. 2026-06-20T04:26:33Z",
    )
    window_group.add_argument(
        "--since-minutes",
        type=_positive_int,
        help="Include rows from the last N minutes.",
    )


def resolve_since(args: argparse.Namespace, *, now: datetime | None = None) -> datetime | None:
    if getattr(args, "since", None) is not None:
        return args.since
    since_minutes = getattr(args, "since_minutes", None)
    if since_minutes is None:
        return None
    current_time = now or datetime.now(timezone.utc)
    return current_time - timedelta(minutes=since_minutes)


def _parse_since_arg(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid --since timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("--since-minutes must be > 0")
    return parsed
