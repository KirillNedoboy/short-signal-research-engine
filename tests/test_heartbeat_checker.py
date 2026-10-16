from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "check_runtime_heartbeat",
    Path(__file__).resolve().parents[1] / "scripts" / "check_runtime_heartbeat.py",
)
checker = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(checker)


class _Notifier:
    def __init__(self, *, fail: bool = False, rejects: bool = False) -> None:
        self.fail = fail
        self.rejects = rejects
        self.sent: list[str] = []
        self.built = 0

    async def start(self) -> None:
        self.built += 1

    async def send_alert(self, message: str) -> bool:
        if self.fail:
            raise RuntimeError("telegram transport down")
        if self.rejects:
            return False
        self.sent.append(message)
        return True

    async def close(self) -> None:
        return None


def _make_env(
    tmp_path: Path,
    *,
    minutes_ago: float | None,
    monitor_enabled: bool = True,
    name: str = "checker",
) -> tuple[str, str]:
    """Create an isolated config/env/db triple matching deployed table shapes."""

    db = tmp_path / f"{name}.sqlite"
    config = tmp_path / f"{name}.yaml"
    env = tmp_path / f"{name}.env"
    connection = sqlite3.connect(db)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS runtime_heartbeats (id INTEGER PRIMARY KEY,"
            " checked_at TEXT, runtime_instance_id TEXT)"
        )
        if minutes_ago is not None:
            checked_at = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
            connection.execute(
                "INSERT OR REPLACE INTO runtime_heartbeats VALUES (1, ?, 'inst-1')",
                (checked_at,),
            )
        connection.commit()
    finally:
        connection.close()
    config.write_text(
        "db_url: sqlite:///%s\nclimax_fast_monitor_enabled: %s\nscan_interval_sec: 60\n"
        % (db.as_posix(), "true" if monitor_enabled else "false"),
        encoding="utf-8",
    )
    env.write_text("", encoding="utf-8")
    return str(config), str(env)


def _args(tmp_path: Path, config: str, env: str, name: str = "checker", **overrides) -> argparse.Namespace:
    base = dict(
        config=config,
        env=env,
        max_age_sec=120.0,
        alert_cooldown_sec=900.0,
        state_file=str(tmp_path / f"{name}-state.json"),
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def _run(
    tmp_path: Path,
    notifier: _Notifier,
    *,
    name: str = "checker",
    minutes_ago: float | None = 10,
    monitor_enabled: bool = True,
    **arg_overrides,
) -> int:
    config, env = _make_env(
        tmp_path, minutes_ago=minutes_ago, monitor_enabled=monitor_enabled, name=name
    )

    async def scenario() -> int:
        return await checker.check(
            _args(tmp_path, config, env, name=name, **arg_overrides),
            notifier_factory=lambda _config: notifier,
        )

    return asyncio.run(scenario())


def test_fresh_heartbeat_exits_zero_without_alert(tmp_path) -> None:
    notifier = _Notifier()
    assert _run(tmp_path, notifier, minutes_ago=0.5) == 0
    assert notifier.sent == []
    assert notifier.built == 0  # no alert channel constructed when healthy
    assert not (tmp_path / "checker-state.json").exists()


def test_stale_heartbeat_exits_two_and_alerts(tmp_path) -> None:
    notifier = _Notifier()
    assert _run(tmp_path, notifier) == 2
    assert len(notifier.sent) == 1
    assert "stale" in notifier.sent[0]
    state = json.loads((tmp_path / "checker-state.json").read_text(encoding="utf-8"))
    assert state["status"] == "alerted"
    assert state["kind"] == "stale"


def test_missing_heartbeat_row_alerts(tmp_path) -> None:
    notifier = _Notifier()
    assert _run(tmp_path, notifier, minutes_ago=None) == 2
    assert "missing" in notifier.sent[0]


def test_unreadable_database_alerts(tmp_path) -> None:
    config = tmp_path / "missing-config.yaml"
    env = tmp_path / "missing.env"
    config.write_text(
        "db_url: sqlite:///%s\nclimax_fast_monitor_enabled: true\n"
        % (tmp_path / "no-such-db.sqlite").as_posix(),
        encoding="utf-8",
    )
    env.write_text("", encoding="utf-8")
    notifier = _Notifier()

    async def run() -> int:
        return await checker.check(
            _args(tmp_path, str(config), str(env), name="missing-config"),
            notifier_factory=lambda _c: notifier,
        )

    assert asyncio.run(run()) == 2
    assert "unavailable" in notifier.sent[0]


def test_duplicate_stale_checks_suppress_within_cooldown(tmp_path) -> None:
    notifier = _Notifier()
    assert _run(tmp_path, notifier, minutes_ago=10) == 2
    assert _run(tmp_path, notifier, minutes_ago=11) == 2
    assert len(notifier.sent) == 1


def test_failed_alert_delivery_does_not_start_cooldown(tmp_path) -> None:
    failing = _Notifier(fail=True)
    assert _run(tmp_path, failing) == 2
    assert not (tmp_path / "checker-state.json").exists()
    good = _Notifier()
    assert _run(tmp_path, good) == 2
    assert len(good.sent) == 1


def test_rejected_alert_delivery_keeps_state_absent(tmp_path) -> None:
    rejects = _Notifier(rejects=True)
    assert _run(tmp_path, rejects) == 2
    assert not (tmp_path / "checker-state.json").exists()


def test_recovery_notifies_once_and_clears_state(tmp_path) -> None:
    stale = _Notifier()
    assert _run(tmp_path, stale, minutes_ago=10) == 2
    recovered = _Notifier()
    assert _run(tmp_path, recovered, minutes_ago=0.1) == 0
    assert recovered.sent == ["Runtime heartbeat recovered."]
    assert not (tmp_path / "checker-state.json").exists()
    again = _Notifier()
    assert _run(tmp_path, again, minutes_ago=0.1) == 0
    assert again.sent == []


def test_recovery_alert_failure_keeps_state_for_retry(tmp_path) -> None:
    stale = _Notifier()
    assert _run(tmp_path, stale, minutes_ago=10) == 2
    failing = _Notifier(fail=True)
    assert _run(tmp_path, failing, minutes_ago=0.1) == 0
    assert (tmp_path / "checker-state.json").exists()


def test_disabled_monitor_uses_db_heartbeat_widened_floor(tmp_path) -> None:
    db = tmp_path / "slow.sqlite"
    config = tmp_path / "slow.yaml"
    env = tmp_path / "slow.env"
    connection = sqlite3.connect(db)
    try:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS __db_heartbeat (id INTEGER PRIMARY KEY, checked_at TEXT)"
        )
        connection.commit()
    finally:
        connection.close()
    config.write_text(
        "db_url: sqlite:///%s\nclimax_fast_monitor_enabled: false\n" % db.as_posix(),
        encoding="utf-8",
    )
    env.write_text("", encoding="utf-8")

    def _set_age(seconds: float) -> None:
        connection = sqlite3.connect(db)
        try:
            connection.execute(
                "INSERT OR REPLACE INTO __db_heartbeat VALUES (1, ?)",
                ((datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat(),),
            )
            connection.commit()
        finally:
            connection.close()

    async def healthy_check() -> int:
        return await checker.check(
            _args(tmp_path, str(config), str(env), name="slow"),
            notifier_factory=lambda _c: _Notifier(),
        )

    # 300 s: past max_age_sec but inside the 600 s full-scan floor -> healthy.
    _set_age(300)
    assert asyncio.run(healthy_check()) == 0

    # 700 s: beyond the floor -> stale alert despite the small max-age.
    _set_age(700)
    notifier = _Notifier()

    async def stale_check() -> int:
        return await checker.check(
            _args(tmp_path, str(config), str(env), name="slow"),
            notifier_factory=lambda _c: notifier,
        )

    assert asyncio.run(stale_check()) == 2
    assert "stale" in notifier.sent[0]


def test_checker_never_writes_bot_database(tmp_path) -> None:
    config, env = _make_env(tmp_path, minutes_ago=10)
    db = tmp_path / "checker.sqlite"
    before = db.read_bytes()

    async def run() -> None:
        await checker.check(
            _args(tmp_path, config, env),
            notifier_factory=lambda _c: _Notifier(),
        )

    asyncio.run(run())
    assert db.read_bytes() == before


def test_checker_source_has_no_control_apis() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "scripts" / "check_runtime_heartbeat.py"
    ).read_text(encoding="utf-8")
    # Executable control APIs are absent by construction; prose may mention them.
    code = "\n".join(
        line
        for line in source.splitlines()
        if not line.lstrip().startswith("#")
    )
    for forbidden in ("systemctl", "subprocess", "os.kill", "SIGKILL", "SIGTERM", "import signal"):
        assert forbidden not in code
