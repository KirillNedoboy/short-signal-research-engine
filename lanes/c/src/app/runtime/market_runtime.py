"""Market-data composition for C.

The WebSocket hub and canonical bounded store are the preferred source when
the configured mode enables them.  REST is used for startup/backfill or only
for symbols whose canonical frame is not ready; no broad REST frame fetch is
performed for symbols already covered by the store.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any, Iterable

import pandas as pd

from app.config import AppConfig, CanonicalMarketDataProviderMode
from app.infra.request_scheduler import RequestPriority
from app.market_data.continuity import GapTracker
from app.market_data.hub import MarketDataHub
from app.market_data.provider import CanonicalMarketDataProvider
from app.market_data.store import CanonicalMarketStore
from app.runtime.derivatives import DerivativeRefreshCache, classify_payload

LOGGER = logging.getLogger(__name__)


class LaneCMarketRuntime:
    def __init__(
        self,
        *,
        config: AppConfig,
        provider: CanonicalMarketDataProvider,
        scanner: Any,
    ) -> None:
        self.config = config
        self.provider = provider
        self.scanner = scanner
        self.continuity: GapTracker | None = None
        self.hub: MarketDataHub | None = None
        self.store: CanonicalMarketStore | None = None
        self._warm_task: asyncio.Task[None] | None = None
        self._warm_attempted_at: dict[str, float] = {}
        self._warm_cursor = 0
        self.derivatives_cache: DerivativeRefreshCache | None = (
            DerivativeRefreshCache(scanner, config)
            if config.derivatives_enabled
            else None
        )
        if self.mode is not CanonicalMarketDataProviderMode.REST_ONLY:
            self.continuity = getattr(provider, "continuity", None) or GapTracker()
            self.hub = getattr(provider, "hub", None)
            if self.hub is None:
                self.hub = MarketDataHub(observers=(self.continuity,))
            elif hasattr(self.hub, "add_observer"):
                self.hub.add_observer(self.continuity)
            self.store = CanonicalMarketStore(
                ring_size=max(30, int(config.deep_scan_kline_limit)),
                stale_after_sec=max(90, int(config.scan_interval_sec * 2)),
                continuity=self.continuity,
                hub=self.hub,
            )
            self.hub.add_observer(self.store)
            bind_hub = getattr(self.provider, "set_hub", None)
            if callable(bind_hub):
                bind_hub(self.hub)
            self.provider.set_continuity(self.continuity)

    @property
    def mode(self) -> CanonicalMarketDataProviderMode:
        return CanonicalMarketDataProviderMode(self.config.canonical_market_data_provider_mode)

    async def start(self) -> None:
        await self.provider.start()
        # Let a newly-created Hub enter its empty-universe wait before the
        # first scan publishes the eligible symbols.  Without this handoff,
        # an update issued in the same event-loop turn leaves the wake event
        # set and is interpreted as a second planned rotation immediately
        # after the first connection opens.
        if self.hub is not None:
            await asyncio.sleep(0)
        if self.derivatives_cache is not None:
            await self.derivatives_cache.start()
        if self.store is not None:
            snapshot_path = getattr(self.config, "market_frame_snapshot_path", None)
            if snapshot_path:
                try:
                    result = self.store.restore(
                        snapshot_path,
                        max_age_sec=int(getattr(self.config, "market_frame_snapshot_max_age_sec", 900)),
                    )
                    if result.get("corrupt") or result.get("rejected"):
                        LOGGER.warning("C frame-store restore was partial | result=%s", result)
                except Exception as exc:  # noqa: BLE001 - fresh WS/REST can recover
                    LOGGER.warning("C frame-store restore failed | error=%s", type(exc).__name__)

    async def stop(self) -> None:
        if self._warm_task is not None:
            self._warm_task.cancel()
            await asyncio.gather(self._warm_task, return_exceptions=True)
            self._warm_task = None
        if self.store is not None:
            snapshot_path = getattr(self.config, "market_frame_snapshot_path", None)
            if snapshot_path:
                try:
                    await asyncio.to_thread(
                        self.store.checkpoint,
                        snapshot_path,
                        metadata={"provider_mode": self.mode.value},
                    )
                except Exception as exc:  # noqa: BLE001 - checkpoint never blocks shutdown
                    LOGGER.warning("C frame-store checkpoint failed | error=%s", type(exc).__name__)
        if self.derivatives_cache is not None:
            await self.derivatives_cache.stop()
        await self.provider.stop()

    async def scan(self) -> tuple[list[Any], list[str]]:
        snapshot = await self.provider.build_scan_snapshot()
        snapshots = list(snapshot.snapshots)
        if self.store is not None:
            telemetry = self.provider.last_universe_telemetry
            if telemetry is not None:
                eligible = getattr(telemetry, "eligible_symbols", ())
                self.store.update_symbols(eligible)
                self.provider.update_universe(list(eligible))
        shortlist = self.provider.shortlist(snapshots)
        symbols = [item.symbol for item in shortlist]
        if self.derivatives_cache is not None:
            # Historical C derivatives are needed for admission, not for every
            # liquid ticker.  Keep refresh work bounded to the hot shortlist;
            # active symbols are added by the frame path when required.
            self.derivatives_cache.update_symbols(symbols)
            self.derivatives_cache.kick()
        if self.store is not None:
            # Progress over the selected shortlist in bounded batches.  The WS
            # subscription remains on the full eligible universe, while REST
            # warmup never spends its 20-request budget on cold tail symbols.
            self._schedule_warmup(symbols)
        return shortlist, symbols

    def _schedule_warmup(self, symbols: Any) -> None:
        """Queue bounded startup/recovery seed work after WS universe admission."""

        if self.store is None or self._warm_task is not None and not self._warm_task.done():
            return
        now = time.monotonic()
        normalized = tuple(dict.fromkeys(str(symbol).upper() for symbol in symbols))
        if not normalized:
            return
        captured = self.store.snapshot_time()
        cursor = self._warm_cursor % len(normalized)
        rotated = normalized[cursor:] + normalized[:cursor]
        targets = [
            symbol
            for symbol in rotated
            if not self.store.is_complete(symbol, captured)
            and now - self._warm_attempted_at.get(symbol, 0.0) >= 60.0
        ]
        if not targets:
            return
        max_requests = int(getattr(self.config, "market_frame_warmup_max_requests", 20))
        if max_requests <= 0:
            return
        client = getattr(self.provider, "client", None)
        if client is None:
            LOGGER.warning("C frame-store warmup skipped; provider has no REST client")
            return
        targets = targets[: max(1, max_requests // 2)]
        last_target_index = max(rotated.index(symbol) for symbol in targets)
        self._warm_cursor = (cursor + last_target_index + 1) % len(normalized)
        for symbol in targets:
            self._warm_attempted_at[symbol] = now

        async def _warm() -> None:
            try:
                result = await self.store.warm_symbols(
                    client,
                    targets,
                    priority=RequestPriority.RECOVERY,
                    timeout_sec=float(self.config.request_timeout_sec),
                    max_requests=max_requests,
                )
                # A full ring can still contain an interior gap.  Spend only
                # the remaining bounded recovery budget on those explicit
                # intervals; never fan out a second broad REST fetch.
                remaining = max(0, max_requests - int(result.get("requests", 0)))
                for symbol in targets:
                    if remaining <= 0:
                        break
                    if self.store.next_missing_interval(symbol) is None:
                        continue
                    await self.store.backfill_missing(
                        client,
                        symbol,
                        priority=RequestPriority.RECOVERY,
                    )
                    remaining -= 1
                LOGGER.info("C frame-store warmup | result=%s", result)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - readiness remains fail-closed
                LOGGER.warning("C frame-store warmup failed | error=%s", type(exc).__name__)

        self._warm_task = asyncio.create_task(_warm(), name="lane-c-frame-store-warmup")

    async def frames(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        normalized = [item.upper() for item in symbols]
        if self.derivatives_cache is not None:
            # The composition root's ordered symbols include active events
            # ahead of the shortlist.  Keep the historical cache on exactly
            # this bounded admission set instead of refreshing the exchange
            # universe selected during the ticker scan.
            self.derivatives_cache.update_symbols(normalized)
            self.derivatives_cache.kick()
        frames: dict[str, pd.DataFrame] = {}
        missing: list[str] = []
        now = datetime.now(UTC)
        if self.store is not None:
            for symbol in normalized:
                frame = self.store.snapshot(symbol, now)
                if frame is not None and frame.actionable:
                    frames[symbol] = frame.frame_1m.copy(deep=True)
                else:
                    missing.append(symbol)
        else:
            missing = normalized
        if missing:
            if self.store is None:
                # REST_ONLY is the explicit compatibility mode.  HUB_PREFERRED
                # never turns a missing canonical frame into a broad REST
                # prefetch; the bounded warmup task seeds/repairs the store.
                frames.update(await self.provider.fetch_symbol_frames(missing))
            else:
                self._schedule_warmup(missing)
        return frames

    def derivatives(self, symbol: str) -> dict[str, Any] | None:
        """Return WS/store or background-cache derivatives without awaiting."""

        if self.derivatives_cache is not None:
            # C strategy semantics require historical OI/funding observations;
            # WS ticker derivatives remain a separate market-data signal and
            # must never silently replace the settled historical cache.
            return self.derivatives_cache.get(symbol)
        if self.store is None:
            return None
        payload = self.store.derivatives_payload(symbol)
        return classify_payload(payload, now=datetime.now(UTC)) if payload is not None else {}

    def readiness(self, symbols: Iterable[str] | None = None) -> tuple[bool, str]:
        """Return fail-closed market warmup/readiness for shadow supervision.

        A process can be started while the hub is still connecting or the
        bounded REST seed is incomplete.  Only decision-ready store snapshots
        from the current acknowledged WebSocket epoch satisfy this gate.
        """

        if self.store is None or self.hub is None:
            return False, "market_store_unavailable"
        expected = tuple(
            dict.fromkeys(
                str(symbol).upper()
                for symbol in (symbols if symbols is not None else self.store.symbols())
            )
        )
        if not expected:
            return False, "market_warmup_empty"
        now = self.store.snapshot_time()
        complete = sum(1 for symbol in expected if self.store.is_complete(symbol, now))
        if complete != len(expected):
            return False, f"market_warmup_{complete}_of_{len(expected)}"
        health = self.hub.get_health()
        state = str(getattr(getattr(health, "state", None), "name", ""))
        if state != "HEALTHY":
            return False, "market_ws_not_healthy"
        return True, "market_coverage_valid"

    def seed_derivatives(
        self,
        payloads: dict[str, dict[str, Any]],
        *,
        observed_at: datetime | None = None,
    ) -> None:
        """Seed deterministic validation fixtures; live code uses refreshers."""

        if self.derivatives_cache is None:
            return
        for symbol, payload in payloads.items():
            self.derivatives_cache.seed(symbol, payload, observed_at=observed_at)


__all__ = ["LaneCMarketRuntime"]

