from __future__ import annotations

import asyncio
import gzip
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from app.config import AppConfig
from app.main import ShortSignalBot
from app.market_data.continuity import GapTracker
from app.market_data.models import ConnectionEpoch, MarketCandle, MarketTickerSnapshot
from app.market_data.provider import CanonicalMarketDataProvider
from app.runtime.shutdown_checkpoint import ShutdownCheckpointBarrier


def test_real_provider_barrier_persists_generation_and_pid(tmp_path: Path) -> None:
    async def run() -> None:
        now = datetime.now(UTC).replace(second=0, microsecond=0)
        config = AppConfig(
            canonical_market_data_provider_mode="HUB_PREFERRED",
            market_frame_store_mode="PREFERRED",
            deep_scan_kline_limit=3,
            market_frame_snapshot_path=str(tmp_path / "frames.json.gz"),
        )
        provider = CanonicalMarketDataProvider(
            scanner=SimpleNamespace(client=object()),
            config=config,
            continuity=GapTracker(),
            clock=lambda: now,
        )
        store = provider.frame_store
        assert store is not None
        store.update_symbols(("TESTUSDT",))
        epoch = ConnectionEpoch(0, 1)
        for minutes_ago in (3, 2, 1):
            opened = now - timedelta(minutes=minutes_ago)
            store.observe_closed_candle(
                MarketCandle(
                    symbol="TESTUSDT",
                    open_time=opened,
                    close_time=opened + timedelta(minutes=1),
                    interval="1",
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    volume=Decimal("1000"),
                    turnover=Decimal("100000"),
                    confirmed=True,
                    exchange_timestamp=opened,
                    received_at=opened,
                    connection_epoch=epoch,
                )
            )
        store.observe_ticker(
            MarketTickerSnapshot(
                symbol="TESTUSDT",
                connection_epoch=epoch,
                last_price=Decimal("100"),
                mark_price=Decimal("100"),
                index_price=Decimal("100"),
                funding_rate=Decimal("0.0001"),
                open_interest=Decimal("1500"),
                volume_24h=Decimal("100000"),
                turnover_24h=Decimal("10000000"),
                received_at=now,
            )
        )
        frame = store.peek_snapshot("TESTUSDT", now)
        assert frame is not None and frame.actionable

        bot = object.__new__(ShortSignalBot)
        bot._market_data_provider = provider
        barrier: ShutdownCheckpointBarrier
        sent: list[tuple[int, object]] = []

        def send_signal(pid: int, signum: object) -> None:
            sent.append((pid, signum))
            barrier.notify_signal()

        barrier = ShutdownCheckpointBarrier(
            checkpoint=bot._checkpoint_for_shutdown,
            pid_provider=lambda: 4321,
            signal_sender=send_signal,
        )
        ack = await barrier.request(timeout_sec=1.0)

        assert sent and sent[0][0] == 4321
        assert ack.generation == 1
        assert ack.pid == 4321
        assert ack.symbols == 1
        assert store.checkpoint_generation == 1
        with gzip.open(config.market_frame_snapshot_path, "rt", encoding="utf-8") as stream:
            payload = json.load(stream)
        assert payload["generation"] == 1
        assert payload["metadata"] == {"pid": 4321, "reason": "SIGUSR1_SHUTDOWN"}
        assert list(payload["symbols"]) == ["TESTUSDT"]

    asyncio.run(run())
