from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import select

from app.config import AppConfig
from app.main import ShortSignalBot
from app.market.scanner import MarketScanner
from app.market.coverage import normalize_scan_failure
from app.storage.db import Database
from app.storage.models import MarketScanSymbolResultModel
from app.storage.repository import BotRepository


class _KlineClient:
    async def fetch_klines(self, *_args, **_kwargs):
        return [["bad"]]


class _CycleScanner:
    def __init__(self, frame, failures=None):
        self.client = object()
        self.frame = frame
        self.failures = failures or {}
        self.last_universe_telemetry = type(
            "Telemetry",
            (),
            {
                "exchange_symbols": ("AAAUSDT",),
                "eligible_symbols": ("AAAUSDT",),
                "excluded": (),
                "observed_at": datetime.now(timezone.utc),
            },
        )()

    @property
    def last_scan_failures(self):
        return dict(self.failures)

    async def fetch_market_snapshots(self):
        return []

    def shortlist(self, _snapshots):
        return [type("Snapshot", (), {"symbol": "AAAUSDT"})()]

    async def fetch_symbol_frames(self, symbols):
        return {symbol: self.frame for symbol in symbols if self.frame is not None}

    async def fetch_optional_derivatives(self, _symbol):
        return {}


def _result(tmp_path, scanner, *, capture=None):
    database = Database(f"sqlite:///{tmp_path / 'task31.db'}")
    database.create_all()
    bot = ShortSignalBot(
        config=AppConfig(shortlist_size=1),
        repository=BotRepository(database),
        scanner=scanner,
    )
    if capture is not None:
        bot._market_data_provider.capture_decision_snapshot = capture
    asyncio.run(bot.run_cycle())
    with database.session() as session:
        return session.scalars(select(MarketScanSymbolResultModel)).all()


def test_all_scan_failure_codes_remain_stable():
    codes = (
        "DEADLINE_EXCEEDED",
        "STALE_MARKET_DATA",
        "MARKET_DATA_INCOMPLETE",
        "MARKET_DATA_DISCONTINUITY",
        "EMPTY_RESPONSE",
        "PROVIDER_ERROR",
    )

    assert [normalize_scan_failure(code)["reason_code"] for code in codes] == list(codes)


def test_scanner_malformed_nonempty_klines_is_sanitized_failure():
    async def _run():
        scanner = MarketScanner(client=_KlineClient(), config=AppConfig())
        frames = await scanner.fetch_symbol_frames(["AAAUSDT"])
        return frames, scanner.last_scan_failures

    frames, failures = asyncio.run(_run())

    assert frames == {}
    assert failures["AAAUSDT"] == {
        "terminal_status": "SCAN_FAILED",
        "reason_code": "PROVIDER_ERROR",
        "details": {"exception_type": "ValueError"},
    }


def test_cycle_propagates_scanner_failure_to_canonical_result(tmp_path):
    frame = pd.DataFrame({"close": [1.0]})
    rows = _result(
        tmp_path,
        _CycleScanner(
            frame,
            {"AAAUSDT": {"terminal_status": "SCAN_FAILED", "reason_code": "STALE_MARKET_DATA"}},
        ),
    )

    assert [(row.terminal_status, row.reason_code) for row in rows] == [
        ("SCAN_FAILED", "STALE_MARKET_DATA")
    ]


def test_cycle_records_terminal_failure_when_decision_snapshot_is_none(tmp_path):
    frame = pd.DataFrame({"close": [1.0]})

    async def capture(*_args, **_kwargs):
        return None

    rows = _result(tmp_path, _CycleScanner(frame), capture=capture)

    assert [(row.terminal_status, row.reason_code) for row in rows] == [
        ("SCAN_FAILED", "MARKET_DATA_INCOMPLETE")
    ]
