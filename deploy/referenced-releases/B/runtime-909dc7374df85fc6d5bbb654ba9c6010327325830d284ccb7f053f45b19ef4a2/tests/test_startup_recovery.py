from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import event

from app.config import AppConfig
from app.main import ShortSignalBot
from app.storage.db import Database
from app.storage.repository import BotRepository


class _Client:
    async def fetch_klines(self, *_args, **_kwargs):
        return [["ok"]]


class _Scanner:
    def __init__(self):
        self.client = _Client()


class _Notifier:
    def __init__(self):
        self.started = False
        self.closed = False

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True

    async def send_alert(self, _message):
        return True


def test_startup_reaches_ready_only_after_schema_restore_and_market_probe(tmp_path):
    async def run():
        database = Database(f"sqlite:///{tmp_path / 'startup.sqlite'}")
        database.create_all()
        notifier = _Notifier()
        bot = ShortSignalBot(
            config=AppConfig(),
            repository=BotRepository(database),
            scanner=_Scanner(),
            notifier=notifier,
        )
        bot._ready = False
        with pytest.raises(RuntimeError, match="not READY"):
            await bot.run_cycle()
        statements = []
        def capture(_conn, _cursor, statement, _parameters, _context, _executemany):
            statements.append(statement.strip().upper())
        event.listen(database.engine, "before_cursor_execute", capture)
        try:
            await bot.startup()
        finally:
            event.remove(database.engine, "before_cursor_execute", capture)
        assert bot._lifecycle == "READY"
        assert bot._ready is True
        assert notifier.started is True
        assert not any(statement.startswith(("CREATE", "ALTER", "DROP")) for statement in statements)
        await bot.shutdown()
        assert notifier.closed is True

    asyncio.run(run())
