"""Service orchestration for the collection-only research process."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
import logging
import time
from typing import Any

from .config import CollectorConfig, load_config
from .db import ResearchDB
from .feeds import FeedManager, now_ms
from .signals import ReadOnlyLaneReader


LOG = logging.getLogger(__name__)


class Collector:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.run_started_ms = now_ms()
        storage = config.storage
        self.db = ResearchDB(
            storage.db_path,
            max_bytes=storage.max_bytes,
            reserve_free_bytes=storage.reserve_free_bytes,
            raw_retention_sec=storage.raw_retention_sec,
            raw_max_rows=storage.raw_max_rows,
        )
        self.db.prepare_run(self.run_started_ms)
        self.readers = [
            ReadOnlyLaneReader(
                source,
                started_ms=self.run_started_ms,
                freshness_sec=config.candidate_freshness_sec,
            )
            for source in config.lanes
        ]
        self.feed = FeedManager(
            self.db,
            config.bybit,
            config.binance,
            reconnect_initial=config.reconnect_initial_sec,
            reconnect_max=config.reconnect_max_sec,
        )
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[Any]] = []

    async def run(self) -> None:
        await self.feed.start()
        self.tasks = [
            asyncio.create_task(self._signal_loop(), name="signal-linker"),
            asyncio.create_task(self._feature_loop(), name="feature-outcomes"),
            asyncio.create_task(self._retention_loop(), name="retention"),
            asyncio.create_task(self._report_loop(), name="reports"),
        ]
        try:
            await self.stop_event.wait()
        finally:
            for task in self.tasks:
                task.cancel()
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
            await self.feed.stop()
            self.db.close()

    def request_stop(self) -> None:
        self.stop_event.set()

    async def _signal_loop(self) -> None:
        while not self.stop_event.is_set():
            for reader in self.readers:
                try:
                    signals = await asyncio.to_thread(reader.poll)
                    for signal in signals:
                        await self._handle_signal(signal)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOG.exception("lane polling failed for %s", reader.source.name)
            await self._promote_pending()
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.signal_poll_sec)
            except asyncio.TimeoutError:
                pass

    async def _handle_signal(self, signal: Any) -> None:
        now = now_ms()
        event_end = signal.delivery_time_ms + self.config.outcome_horizon_sec[-1] + self.config.outcome_grace_sec * 1000
        event_id, created = self.db.link_signal(
            lane=signal.lane,
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            signal_time_ms=signal.signal_time_ms,
            delivery_time_ms=signal.delivery_time_ms,
            delivery_status=signal.delivery_status,
            strategy=signal.strategy,
            model_version=signal.model_version,
            config_version=signal.config_version,
            source_event_id=signal.event_id,
            signal_price=signal.signal_price,
            decision=signal.decision,
            merge_window_sec=self.config.merge_window_sec,
            now_ms=now,
            observation_end_ms=event_end,
        )
        if not created:
            return
        if event_end < now:
            self.db.mark_capacity_rejected(event_id, now)
            return
        active_rows = self.db.active_events(now)
        active_symbols = {str(row["symbol"]) for row in active_rows}
        if signal.symbol not in active_symbols and len(active_symbols) >= self.config.max_active_symbols:
            self.db.mark_capacity_rejected(event_id, now)
            LOG.info("observation capacity reached; event %s remains linked", event_id)
            return
        state = await self.feed.prepare_symbol(signal.symbol)
        price = signal.signal_price
        status = "SIGNAL_PRICE" if price is not None else "UNKNOWN"
        if price is None:
            current, _provider, current_status = state.current_price(now)
            if current is not None:
                price, status = current, "FEED_PRICE_AT_COLLECTION"
        profile = state.snapshot(now).market_profile
        self.db.activate_event(event_id, entry_price=price, entry_status=status, market_profile=profile, now_ms=now)
        self.feed.activate(signal.symbol)
        LOG.info("linked delivered %s signal %s to event %s (%s)", signal.lane, signal.signal_id, event_id, profile)

    async def _promote_pending(self) -> None:
        now = now_ms()
        active_symbols = {str(row["symbol"]) for row in self.db.active_events(now)}
        available = self.config.max_active_symbols - len(active_symbols)
        if available <= 0:
            return
        for row in self.db.pending_events(now):
            symbol = str(row["symbol"])
            if symbol not in active_symbols and available <= 0:
                break
            if symbol in active_symbols:
                self.db.activate_event(str(row["event_id"]), entry_price=row["entry_price"], entry_status=str(row["entry_price_status"]), market_profile=str(row["market_profile"]), now_ms=now)
                continue
            state = self.feed.states.get(symbol)
            if not state:
                state = await self.feed.prepare_symbol(symbol)
            price, _provider, status = state.current_price(now)
            self.db.activate_event(str(row["event_id"]), entry_price=price or row["entry_price"], entry_status=status if price else str(row["entry_price_status"]), market_profile=state.snapshot(now).market_profile, now_ms=now)
            self.feed.activate(symbol)
            active_symbols.add(symbol)
            available -= 1

    async def _feature_loop(self) -> None:
        while not self.stop_event.is_set():
            stamp = now_ms()
            active_rows = self.db.active_events(stamp)
            active_symbols = {str(row["symbol"]) for row in active_rows}
            for symbol in tuple(self.feed.active_symbols()):
                if symbol not in active_symbols:
                    self.feed.deactivate(symbol)
            for row in active_rows:
                event_id, symbol = str(row["event_id"]), str(row["symbol"])
                state = self.feed.states.get(symbol)
                if not state:
                    continue
                snapshot = state.snapshot(stamp)
                self.db.add_feature(event_id, symbol, stamp, snapshot.market_profile, snapshot.features)
                price, provider, status = state.current_price(stamp)
                reason = None if status == "KNOWN" else "perpetual_feed_unknown"
                self.db.add_price_sample(event_id, symbol, stamp, price, status, provider, reason)
                if price is not None and row["entry_price"] is None:
                    self.db.set_entry_price(event_id, price, "FEED_PRICE_AT_COLLECTION", stamp)
                self.db.record_crossings(event_id, stamp, price, status)
                self.db.record_outcome_samples(event_id, stamp, self.config.outcome_horizon_sec, self.config.outcome_grace_sec)
                self.db.health("bybit", "linear", symbol, stamp, snapshot.features.get("perp_feed", "UNKNOWN"), None, {"market_profile": snapshot.market_profile})
            await self._promote_pending()
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.feature_interval_sec)
            except asyncio.TimeoutError:
                pass

    async def _retention_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                # All writes use the event-loop-owned connection. Keeping the
                # prune on the same loop avoids concurrent SQLite transactions
                # against the single research connection.
                result = self.db.prune(now_ms())
                if not result["raw_enabled"]:
                    LOG.error("raw stream writes paused by storage guard: %s", result)
            except asyncio.CancelledError:
                raise
            except Exception:
                LOG.exception("retention pass failed")
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.storage.prune_interval_sec)
            except asyncio.TimeoutError:
                pass

    async def _report_loop(self) -> None:
        while not self.stop_event.is_set():
            stamp = now_ms()
            first = self.db.conn.execute("SELECT MIN(first_delivery_ms) FROM market_events").fetchone()[0]
            if first:
                if stamp >= int(first) + 7 * 24 * 60 * 60 * 1000:
                    self.db.report("7d", int(first) + 7 * 24 * 60 * 60 * 1000, stamp)
                if stamp >= int(first) + 28 * 24 * 60 * 60 * 1000:
                    self.db.report("28d", int(first) + 28 * 24 * 60 * 60 * 1000, stamp)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=self.config.report_interval_sec)
            except asyncio.TimeoutError:
                pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collection-only microstructure research service")
    parser.add_argument("--config", default="config.toml", help="TOML configuration path")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = load_config(args.config)
    collector = Collector(config)
    try:
        asyncio.run(collector.run())
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

