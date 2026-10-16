"""Alert-only external check of the runtime heartbeat.

Runs under a systemd timer as an independent process.  It opens the bot's
SQLite database strictly read-only, evaluates heartbeat freshness, and sends
Telegram alerts with its own cooldown state file.  It never signals,
restarts, or mutates the bot or its database.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config  # noqa: E402
from app.infra.atomic_json import atomic_write_json  # noqa: E402
from app.notifications.telegram import TelegramNotifier  # noqa: E402

DB_BUSY_TIMEOUT_MS = 5_000
FULL_SCAN_MIN_AGE_SEC = 600
# Stable states: None = no alert outstanding; otherwise alert time + kind.
_STATE_ALERTED = "alerted"
STATE_SCHEMA_VERSION = 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--env", default=".env")
    parser.add_argument("--max-age-sec", type=float, default=120.0)
    parser.add_argument("--alert-cooldown-sec", type=float, default=900.0)
    parser.add_argument(
        "--state-file",
        default="/run/short-telegram-bot-lite-heartbeat-check.json",
    )
    return parser


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_stored_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _read_heartbeat(config_path: str, env_path: str) -> tuple[str, datetime | None]:
    """Return (mode, checked_at) from the freshest authoritative heartbeat."""

    config = load_config(config_path=config_path, env_path=env_path)
    if not config.db_url.startswith("sqlite:///"):
        raise RuntimeError("heartbeat checker supports sqlite:/// databases only")
    db_path = config.db_url.removeprefix("sqlite:///")
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
    try:
        connection.execute("PRAGMA busy_timeout = %d" % DB_BUSY_TIMEOUT_MS)
        if config.climax_fast_monitor_enabled:
            row = connection.execute(
                "SELECT checked_at, runtime_instance_id FROM runtime_heartbeats"
                " WHERE id = 1"
            ).fetchone()
            mode = "fast-monitor"
        else:
            row = connection.execute(
                "SELECT checked_at FROM __db_heartbeat WHERE id = 1"
            ).fetchone()
            mode = "db-heartbeat"
    finally:
        connection.close()
    if row is None:
        return mode, None
    return mode, _parse_stored_timestamp(row[0])


def _load_state(state_path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _max_age_for(config_path: str, env_path: str, max_age_sec: float) -> float:
    config = load_config(config_path=config_path, env_path=env_path)
    if config.climax_fast_monitor_enabled:
        return max_age_sec
    # A disabled fast monitor leaves only the DB heartbeat, which tolerates a
    # known long full scan; keep detection on at the widened floor.
    return max(max_age_sec, FULL_SCAN_MIN_AGE_SEC)


def _default_notifier_factory(config):
    return TelegramNotifier(
        token=config.telegram_token,
        signal_chat_id=config.signal_chat_id,
        alerts_chat_id=config.alerts_chat_id,
    )


async def _send(config, message: str, notifier_factory=_default_notifier_factory) -> bool:
    notifier = notifier_factory(config)
    try:
        await notifier.start()
        return await notifier.send_alert(message)
    finally:
        await notifier.close()


async def check(args: argparse.Namespace, *, notifier_factory=_default_notifier_factory) -> int:
    logger = logging.getLogger("heartbeat-checker")
    state_path = Path(args.state_file)
    config = load_config(config_path=args.config, env_path=args.env)
    now = _utcnow()
    state = _load_state(state_path)
    was_alerted = bool(state and state.get("status") == _STATE_ALERTED)

    problem: str | None = None
    detail = ""
    try:
        mode, checked_at = _read_heartbeat(args.config, args.env)
        allowed_age = _max_age_for(args.config, args.env, args.max_age_sec)
        if checked_at is None:
            problem = "missing"
            detail = f"no heartbeat row ({mode})"
        else:
            age_sec = (now - checked_at).total_seconds()
            if age_sec > allowed_age:
                problem = "stale"
                detail = f"age_sec={age_sec:.0f} limit={allowed_age:.0f} ({mode})"
    except Exception as exc:  # noqa: BLE001 - checker must report, never crash
        problem = "unavailable"
        detail = f"{type(exc).__name__}: {exc}"

    if problem is None:
        logger.info("Heartbeat healthy | checked_at=%s", checked_at.isoformat() if checked_at else "n/a")
        if was_alerted:
            message = "Runtime heartbeat recovered."
            try:
                delivered = await _send(config, message, notifier_factory)
            except Exception as exc:  # noqa: BLE001 - recovery alert retries next tick
                logger.error("recovery alert failed | error=%s", type(exc).__name__)
                return 0
            if delivered:
                state_path.unlink(missing_ok=True)
            else:
                logger.error("recovery alert not delivered; state retained")
        return 0

    logger.error("Runtime heartbeat problem | kind=%s %s", problem, detail)
    in_cooldown = (
        was_alerted
        and isinstance(state.get("alerted_at"), str)
        and (
            now - (_parse_stored_timestamp(state["alerted_at"]) or now)
        ).total_seconds()
        < args.alert_cooldown_sec
    )
    if in_cooldown:
        logger.info("Alert suppressed within cooldown | kind=%s", problem)
        return 2
    message = f"Runtime heartbeat {problem}: {detail}"
    try:
        delivered = await _send(config, message, notifier_factory)
    except Exception as exc:  # noqa: BLE001 - transport failure leaves no cooldown
        logger.error("alert delivery failed | error=%s", type(exc).__name__)
        return 2
    if not delivered:
        logger.error("alert not delivered (channel unconfigured or rejected)")
        return 2
    atomic_write_json(
        state_path,
        {
            "schema_version": STATE_SCHEMA_VERSION,
            "status": _STATE_ALERTED,
            "kind": problem,
            "alerted_at": now.isoformat(),
        },
    )
    return 2


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    return asyncio.run(check(args))


if __name__ == "__main__":
    raise SystemExit(main())
