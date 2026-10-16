"""Canonical decision-ready market history maintained from Hub observations.

``CanonicalMarketStore`` is a bounded observer of ``MarketDataHub``: it keeps
rolling closed-candle rings, ticker snapshots, and minute-sampled open
interest so live evaluation never needs recurring broad REST calls.  It owns
no transport, no strategy, and no persistence beyond the atomic gzip
checkpoint.
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import tempfile
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import pandas as pd

from app.market.candles import klines_to_frame

from .continuity import GapState, GapTracker
from .models import (
    BYBIT_PUBLIC_WS_SOURCE,
    REST_BACKFILL_SOURCE,
    ConnectionEpoch,
    MarketCandle,
    MarketTickerSnapshot,
    TICKER_DECIMAL_FIELDS,
)
from .observers import ConnectionOpened

LOGGER = logging.getLogger(__name__)


_MINUTE = timedelta(minutes=1)
_TICKER_FRESHNESS = timedelta(seconds=45)


class ContinuityStatus(StrEnum):
    HEALTHY = "HEALTHY"
    WARMING = "WARMING"
    GAP = "GAP"
    RECONNECTING = "RECONNECTING"


# Stale reasons are a closed set consumed by provider/telemetry code.
STALE_WARMING = "WARMING"
STALE_INCOMPLETE_RING = "INCOMPLETE_RING"
STALE_CANDLE_STALE = "CANDLE_STALE"
STALE_CONTINUITY = "CONTINUITY_UNHEALTHY"
STALE_EPOCH_INVALIDATED = "EPOCH_INVALIDATED"
STALE_NO_TICKER = "NO_TICKER"
STALE_OI_HISTORY = "OI_HISTORY_INCOMPLETE"
STALE_FUTURE_CANDLE = "FUTURE_CANDLE"
STALE_TICKER_STALE = "TICKER_STALE"


@dataclass(frozen=True, slots=True)
class DecisionMarketFrame:
    """One immutable decision-ready view of a symbol at ``captured_at_utc``."""

    symbol: str
    frame_1m: pd.DataFrame
    ticker: MarketTickerSnapshot | None
    oi_change_15m: float | None
    oi_change_1h: float | None
    funding_rate: float | None
    captured_at_utc: datetime
    last_closed_at_utc: datetime
    connection_epoch: ConnectionEpoch | None
    continuity_status: ContinuityStatus
    source: str
    stale_reason: str | None

    @property
    def actionable(self) -> bool:
        return self.stale_reason is None


@dataclass(slots=True)
class _SymbolState:
    candles: dict[datetime, MarketCandle] = field(default_factory=dict)
    ticker: MarketTickerSnapshot | None = None
    # Minute-sampled (minute_start, open_interest), ~130 minutes retained so a
    # full hour-back sample always exists once the feed has been live.
    oi_samples: deque[tuple[datetime, Decimal]] = field(
        default_factory=lambda: deque(maxlen=130)
    )
    last_oi_sample_minute: datetime | None = None
    # 15m-spaced history rows seeded by REST warmup (newest-first shape used
    # by FeatureBuilder), superseded by live minute samples once accumulated.
    oi_seed_rows: list[dict[str, Any]] = field(default_factory=list)
    funding_rate: Decimal | None = None
    funding_observed_at: datetime | None = None
    oi_observed_at: datetime | None = None
    continuity_reestablished: bool = True
    last_closed_epoch: ConnectionEpoch | None = None


class CanonicalMarketStore:
    """Bounded rolling history for every eligible symbol, fed by the Hub."""

    def __init__(
        self,
        *,
        ring_size: int,
        stale_after_sec: int = 90,
        continuity: GapTracker | None = None,
        clock: Callable[[], datetime] | None = None,
        hub: Any | None = None,
    ) -> None:
        if ring_size < 2:
            raise ValueError("ring_size must be at least 2")
        self._ring_size = ring_size
        self._stale_after = timedelta(seconds=stale_after_sec)
        self._continuity = continuity
        self._clock = clock or (lambda: datetime.now(UTC))
        self._hub = hub
        self._states: dict[str, _SymbolState] = {}
        self._counters: dict[str, int] = {
            "candle_duplicates_replaced": 0,
            "candle_gap_fill": 0,
            "candle_out_of_order_rejected": 0,
            "candle_out_of_window_rejected": 0,
            "candle_future_rejected": 0,
            "epoch_invalidations": 0,
            "frames_actionable": 0,
            "frames_stale": 0,
        }

    @property
    def ring_size(self) -> int:
        return self._ring_size

    @property
    def counters(self) -> Mapping[str, int]:
        return dict(self._counters)

    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted(self._states))

    def update_symbols(self, symbols: Iterable[str]) -> None:
        """Keep state only for the currently subscribed universe."""
        allowed = {symbol.upper() for symbol in symbols}
        for symbol in allowed:
            self._states.setdefault(symbol, _SymbolState())
        for symbol in list(self._states):
            if symbol not in allowed:
                self._states.pop(symbol, None)

    def ensure_symbols(self, symbols: Iterable[str]) -> None:
        for symbol in symbols:
            self._states.setdefault(symbol.upper(), _SymbolState())

    # ----------------------------------------------------- observer protocol

    def observe_connection(self, event: ConnectionOpened) -> None:
        """A new kline socket generation invalidates freshness until the
        current epoch re-establishes continuity for the symbol."""
        for topic in event.topics:
            if not topic.startswith("kline.1."):
                continue
            symbol = topic.removeprefix("kline.1.").upper()
            state = self._states.get(symbol)
            if state is not None and state.continuity_reestablished:
                state.continuity_reestablished = False
                self._counters["epoch_invalidations"] += 1

    def observe_ticker(self, snapshot: MarketTickerSnapshot) -> None:
        state = self._states.get(snapshot.symbol)
        if state is None:
            return
        state.ticker = snapshot
        if snapshot.funding_rate is not None:
            state.funding_rate = snapshot.funding_rate
            state.funding_observed_at = snapshot.received_at
        if snapshot.open_interest is None or snapshot.received_at is None:
            return
        state.oi_observed_at = snapshot.received_at
        minute = snapshot.received_at.replace(second=0, microsecond=0)
        if state.last_oi_sample_minute == minute:
            # One sample per closed minute: the newest value within the minute
            # wins so the sample matches the confirmed state at rollover.
            if state.oi_samples and state.oi_samples[-1][0] == minute:
                state.oi_samples[-1] = (minute, snapshot.open_interest)
            return
        state.oi_samples.append((minute, snapshot.open_interest))
        state.last_oi_sample_minute = minute

    def observe_rest_ticker(
        self,
        symbol: str,
        data: Mapping[str, Any],
        *,
        server_time: datetime | None,
        captured: datetime,
    ) -> bool:
        """Accept only a fresh, complete REST ticker without replacing newer WS state."""
        symbol = symbol.upper()
        state = self._states.get(symbol)
        if (
            state is None
            or data.get("symbol") != symbol
            or server_time is None
            or server_time.utcoffset() is None
        ):
            return False
        server_time = server_time.astimezone(UTC)
        age = captured.astimezone(UTC) - server_time
        if age > _TICKER_FRESHNESS or age < -timedelta(seconds=5):
            return False
        if (
            state.ticker is not None
            and state.ticker.received_at is not None
            and state.ticker.received_at >= server_time
        ):
            return False
        values: dict[str, Decimal] = {}
        for external, internal in TICKER_DECIMAL_FIELDS.items():
            value = data.get(external)
            if value is None or isinstance(value, bool):
                continue
            try:
                decimal = Decimal(str(value))
            except (InvalidOperation, ValueError):
                continue
            if decimal.is_finite():
                values[internal] = decimal
        required = (
            "last_price", "mark_price", "index_price", "open_interest",
            "funding_rate", "volume_24h", "turnover_24h",
        )
        if any(field not in values for field in required):
            return False
        if (
            any(values[field] <= 0 for field in ("last_price", "mark_price", "index_price"))
            or any(values[field] < 0 for field in ("open_interest", "volume_24h", "turnover_24h"))
        ):
            return False
        self.observe_ticker(MarketTickerSnapshot(
            symbol=symbol, received_at=server_time, source=REST_BACKFILL_SOURCE, **values
        ))
        return True

    def observe_closed_candle(self, candle: MarketCandle) -> None:
        state = self._states.get(candle.symbol)
        if state is None:
            return
        self._apply_closed_candle(state, candle)

    def observe_forming_candle(self, candle: MarketCandle) -> None:
        """Forming candles live on the Hub; nothing to retain here."""
        return None

    # ------------------------------------------------------------- evaluation

    def snapshot(self, symbol: str, decision_at: datetime) -> DecisionMarketFrame | None:
        """Return a frame for ``symbol``, or ``None`` when nothing is retained.

        A returned frame is actionable only when the ring is complete, the
        most recent confirmed candle is fresh, a ticker snapshot exists, and
        continuity is healthy; otherwise ``stale_reason`` names the gate.
        """
        frame = self._build_frame(symbol, decision_at)
        if frame is not None:
            if frame.actionable:
                self._counters["frames_actionable"] += 1
            else:
                self._counters["frames_stale"] += 1
        return frame

    def peek_snapshot(
        self, symbol: str, decision_at: datetime
    ) -> DecisionMarketFrame | None:
        return self._build_frame(symbol, decision_at)

    def _build_frame(
        self, symbol: str, captured: datetime
    ) -> DecisionMarketFrame | None:
        state = self._states.get(symbol.upper())
        if state is None:
            return None
        ring = self._ordered_ring(state)
        stale_reason: str | None = None
        if not ring:
            stale_reason = STALE_WARMING
        elif ring[-1].close_time > captured + timedelta(seconds=5):
            stale_reason = STALE_FUTURE_CANDLE
        elif len(ring) < self._ring_size:
            # Completion is the plan's contract: a full ring of retained
            # closed slots.  Sparse historical minutes supplied by the
            # exchange itself stay legal (the legacy REST frames never saw
            # them either); every recent interval gap is decided by the
            # shared GapTracker continuity gate below.
            stale_reason = STALE_INCOMPLETE_RING
        elif captured - ring[-1].close_time > self._stale_after:
            stale_reason = STALE_CANDLE_STALE
        elif (
            self._continuity is not None
            and self._continuity.get_symbol_state(symbol) is GapState.GAP_DETECTED
            and self._continuity.has_gap_since(symbol, ring[0].open_time)
        ):
            stale_reason = STALE_CONTINUITY
        elif not state.continuity_reestablished:
            stale_reason = STALE_EPOCH_INVALIDATED
        elif state.ticker is None:
            stale_reason = STALE_NO_TICKER
        elif (
            state.ticker.received_at is not None
            and captured - state.ticker.received_at > _TICKER_FRESHNESS
        ):
            stale_reason = STALE_TICKER_STALE
        oi_15m, oi_1h = self._oi_changes(state, captured)
        if (
            stale_reason is None
            and oi_15m is None
            and oi_1h is None
            and not state.oi_samples
            and not state.oi_seed_rows
        ):
            stale_reason = STALE_OI_HISTORY
        frame = klines_to_frame(
            [
                [
                    str(int(candle.open_time.timestamp() * 1000)),
                    str(candle.open),
                    str(candle.high),
                    str(candle.low),
                    str(candle.close),
                    str(candle.volume),
                    str(candle.turnover),
                ]
                for candle in ring
            ]
        )
        forming = self._current_forming(symbol, state)
        if forming is not None:
            frame = pd.concat(
                [
                    frame,
                    klines_to_frame(
                        [
                            [
                                str(int(forming.open_time.timestamp() * 1000)),
                                str(forming.open),
                                str(forming.high),
                                str(forming.low),
                                str(forming.close),
                                str(forming.volume),
                                str(forming.turnover),
                            ]
                        ]
                    ),
                ]
            )
        sources = {candle.source for candle in ring}
        source = (
            "EMPTY"
            if not sources
            else BYBIT_PUBLIC_WS_SOURCE
            if sources == {BYBIT_PUBLIC_WS_SOURCE}
            else "MIXED"
        )
        return DecisionMarketFrame(
            symbol=symbol.upper(),
            frame_1m=frame,
            ticker=state.ticker,
            oi_change_15m=oi_15m,
            oi_change_1h=oi_1h,
            funding_rate=(
                float(state.funding_rate) if state.funding_rate is not None else None
            ),
            captured_at_utc=captured,
            last_closed_at_utc=ring[-1].close_time if ring else captured,
            connection_epoch=(
                ring[-1].connection_epoch if ring else state.last_closed_epoch
            ),
            continuity_status=self._continuity_status(stale_reason),
            source=source,
            stale_reason=stale_reason,
        )

    def derivatives_payload(self, symbol: str) -> dict[str, Any] | None:
        """FeatureBuilder-shaped derivatives mapping for one symbol."""
        state = self._states.get(symbol.upper())
        if state is None:
            return None
        captured = self._now()
        oi_rows = self._oi_history_rows(state, captured)
        funding_rows = (
            [{"fundingRate": str(state.funding_rate)}]
            if state.funding_rate is not None
            else []
        )
        status = "OK"
        reasons: list[str] = []
        warnings: list[str] = []
        if not oi_rows:
            status = "MISSING"
            reasons.append("open_interest_missing")
            warnings.append("oi_missing")
        if not funding_rows:
            warnings.append("derivatives_missing")
            if status == "OK":
                status = "MISSING"
                reasons.append("funding_missing")
        return {
            "open_interest": oi_rows,
            "funding": funding_rows,
            "derivatives_status": status,
            "derivatives_reasons": reasons,
            "data_quality_warnings": warnings,
            "oi_observed_at": state.oi_observed_at.isoformat() if state.oi_observed_at else None,
            "funding_observed_at": state.funding_observed_at.isoformat() if state.funding_observed_at else None,
            "symbol": symbol.upper(),
        }

    def is_complete(self, symbol: str, decision_at: datetime | None = None) -> bool:
        frame = self.peek_snapshot(symbol, decision_at or self._now())
        return frame is not None and frame.actionable

    def completeness_counts(self) -> dict[str, int]:
        now = self._now()
        complete = sum(
            1
            for symbol in self._states
            if self.is_complete(symbol, now)
        )
        return {"symbols": len(self._states), "complete": complete}

    # ------------------------------------------------------------------ warm

    async def warm_symbols(
        self,
        client: Any,
        symbols: Iterable[str],
        *,
        priority: Any,
        interval: str = "15min",
    ) -> dict[str, int]:
        """One bounded REST 1m-candle + OI-history request per symbol.

        Existing data-quality semantics are preserved: a symbol whose warmup
        request fails simply keeps its ``WARMING`` continuity state.
        """
        self.ensure_symbols(symbols)
        seeded = 0
        failed = 0
        skipped = 0
        for symbol in sorted({item.upper() for item in symbols}):
            state = self._states[symbol]
            if len(state.candles) >= self._ring_size:
                # A full ring (checkpoint restore or earlier warm) needs no
                # REST re-seed; warmup is idempotent by design.
                skipped += 1
                continue
            try:
                rows = await client.fetch_klines(
                    symbol, "1", limit=self._ring_size, priority=priority
                )
            except Exception as exc:  # noqa: BLE001 - warmup failure stays WARMING
                failed += 1
                LOGGER.warning(
                    "frame store candle warmup failed | symbol=%s error=%s",
                    symbol,
                    type(exc).__name__,
                )
                continue
            self._seed_ring(state, symbol, rows)
            try:
                oi_rows = await client.fetch_open_interest(
                    symbol, interval, limit=5, priority=priority
                )
                state.oi_seed_rows = list(oi_rows)
            except Exception as exc:  # noqa: BLE001 - OI comes from the ticker stream
                LOGGER.warning(
                    "frame store OI warmup failed | symbol=%s error=%s",
                    symbol,
                    type(exc).__name__,
                )
            seeded += 1
        return {"seeded": seeded, "failed": failed, "skipped": skipped}

    async def backfill_missing(
        self,
        client: Any,
        symbol: str,
        *,
        priority: Any,
        deadline_monotonic: float | None = None,
    ) -> bool:
        """REST-fill only missing closed intervals for one symbol.

        Returns ``True`` when the ring is complete afterwards.  A failed or
        expired request leaves the symbol stale; the caller surfaces the
        explicit status instead of silently serving old data.
        """
        state = self._states.setdefault(symbol.upper(), _SymbolState())
        captured = self._now()
        ring = self._ordered_ring(state)
        try:
            if not ring:
                rows = await client.fetch_klines(
                    symbol.upper(),
                    "1",
                    limit=self._ring_size,
                    priority=priority,
                    deadline_monotonic=deadline_monotonic,
                )
                self._seed_ring(state, symbol.upper(), rows)
                return len(self._ordered_ring(state)) >= self._ring_size
            oldest = ring[0].open_time
            latest_closed_start = captured.replace(second=0, microsecond=0) - _MINUTE
            missing = [
                oldest + _MINUTE * step
                for step in range(1, self._ring_size + 1)
                if (oldest + _MINUTE * step) <= latest_closed_start
                and (oldest + _MINUTE * step) not in state.candles
            ]
            rows = await client.fetch_klines(
                symbol.upper(),
                "1",
                limit=self._ring_size,
                start_ms=int(oldest.timestamp() * 1000),
                end_ms=int(latest_closed_start.timestamp() * 1000),
                priority=priority,
                deadline_monotonic=deadline_monotonic,
            )
            for entry in rows or []:
                candle = self._candle_from_rest_row(symbol.upper(), entry)
                if candle is None or candle.open_time > latest_closed_start:
                    continue
                if candle.open_time not in state.candles:
                    state.candles[candle.open_time] = candle
                    if candle.open_time in missing:
                        self._counters["candle_gap_fill"] += 1
                self._observe_restored(candle)
            self._evict(state)
            return len(self._ordered_ring(state)) >= self._ring_size
        except Exception as exc:  # noqa: BLE001 - fallback miss keeps stale status
            LOGGER.warning(
                "frame store REST fallback failed | symbol=%s error=%s",
                symbol,
                type(exc).__name__,
            )
            return False

    # ------------------------------------------------------------ checkpoint

    def checkpoint(self, path: str | Path, *, symbols: Iterable[str] | None = None) -> int:
        """Atomically write a gzip JSON snapshot of complete closed-candle/OI rings.

        Returns the number of symbols whose complete rings were stored; a
        symbol with an incomplete ring is excluded rather than checkpointed
        partially.
        """
        target = Path(path)
        payload: dict[str, Any] = {
            "schema_version": 1,
            "ring_size": self._ring_size,
            "symbols": {},
        }
        wanted = (
            {symbol.upper() for symbol in symbols}
            if symbols is not None
            else set(self._states)
        )
        written = 0
        for symbol in sorted(wanted):
            state = self._states.get(symbol)
            if state is None:
                continue
            ring = self._ordered_ring(state)
            if len(ring) < self._ring_size:
                continue
            payload["symbols"][symbol] = {
                "written_at": self._now().isoformat(),
                "candles": [
                    [
                        int(candle.open_time.timestamp() * 1000),
                        str(candle.open),
                        str(candle.high),
                        str(candle.low),
                        str(candle.close),
                        str(candle.volume),
                        str(candle.turnover),
                    ]
                    for candle in ring
                ],
                "oi_samples": [
                    [int(minute.timestamp() * 1000), str(value)]
                    for minute, value in state.oi_samples
                ],
            }
            written += 1
        if written == 0:
            return 0
        target.parent.mkdir(parents=True, exist_ok=True)
        handle = tempfile.NamedTemporaryFile(
            dir=target.parent, prefix=target.name + ".", suffix=".tmp", delete=False
        )
        try:
            with gzip.GzipFile(fileobj=handle, mode="wb") as stream:
                stream.write(json.dumps(payload).encode("utf-8"))
            handle.close()
            os.replace(handle.name, target)
        except BaseException:
            handle.close()
            try:
                os.unlink(handle.name)
            except OSError:
                pass
            raise
        return written

    def restore(
        self,
        path: str | Path,
        *,
        max_age_sec: int,
        symbols: Iterable[str] | None = None,
    ) -> dict[str, int]:
        """Load a snapshot at most ``max_age_sec`` old.

        Restored rings are non-actionable until the current WebSocket epoch
        re-establishes continuity; the caller then REST-backfills only the
        missing interval.
        """
        target = Path(path)
        result = {"restored": 0, "rejected": 0}
        if not target.exists():
            return result
        try:
            with gzip.GzipFile(target, mode="rb") as stream:
                payload = json.loads(stream.read().decode("utf-8"))
        except (OSError, ValueError) as exc:
            LOGGER.warning(
                "frame store snapshot unreadable | error=%s", type(exc).__name__
            )
            result["rejected"] += 1
            return result
        if not isinstance(payload, dict) or payload.get("schema_version") != 1:
            result["rejected"] += 1
            return result
        wanted = (
            {symbol.upper() for symbol in symbols}
            if symbols is not None
            else None
        )
        symbols_payload = payload.get("symbols")
        if not isinstance(symbols_payload, dict):
            return result
        now = self._now()
        for raw_symbol, blob in symbols_payload.items():
            symbol = str(raw_symbol).upper()
            if wanted is not None and symbol not in wanted:
                continue
            if not isinstance(blob, dict) or not isinstance(blob.get("candles"), list):
                continue
            try:
                written_at = datetime.fromisoformat(str(blob.get("written_at")))
            except (TypeError, ValueError):
                continue
            if written_at.tzinfo is None:
                written_at = written_at.replace(tzinfo=UTC)
            if (now - written_at.astimezone(UTC)).total_seconds() > max_age_sec:
                result["rejected"] += 1
                continue
            state = self._states.setdefault(symbol, _SymbolState())
            if state.candles:
                continue  # live data wins over a checkpoint
            for row in blob["candles"]:
                candle = self._candle_from_rest_row(symbol, row)
                if candle is not None:
                    state.candles[candle.open_time] = candle
            samples = blob.get("oi_samples")
            if isinstance(samples, list):
                for sample in samples:
                    try:
                        state.oi_samples.append(
                            (
                                datetime.fromtimestamp(
                                    int(sample[0]) / 1000, tz=UTC
                                ),
                                Decimal(str(sample[1])),
                            )
                        )
                    except (TypeError, ValueError, IndexError):
                        continue
            # Restored history never starts actionable.
            state.continuity_reestablished = False
            self._evict(state)
            result["restored"] += 1
        return result

    def next_missing_interval(
        self, symbol: str, decision_at: datetime | None = None
    ) -> datetime | None:
        """Return the oldest missing closed-minute slot inside the ring."""
        state = self._states.get(symbol.upper())
        if state is None:
            return None
        ring = self._ordered_ring(state)
        if not ring:
            return None
        now = decision_at or self._now()
        latest_closed_start = now.replace(second=0, microsecond=0) - _MINUTE
        expected = ring[0].open_time
        while expected <= latest_closed_start:
            if expected not in state.candles:
                return expected
            expected += _MINUTE
        return None

    # --------------------------------------------------------------- internals

    def _apply_closed_candle(self, state: _SymbolState, candle: MarketCandle) -> None:
        if candle.close_time > self._now() + timedelta(seconds=5):
            self._counters["candle_future_rejected"] += 1
            return
        # One-minute candle slots are minute-aligned; keying by the normalized
        # slot makes REST/WS timestamp jitter idempotent instead of duplicating
        # a minute into two near-identical ring entries.
        candle = replace(
            candle,
            open_time=candle.open_time.replace(second=0, microsecond=0),
        )
        if state.candles.get(candle.open_time) is not None:
            # Idempotent replacement of the same open-time slot.
            state.candles[candle.open_time] = candle
            self._counters["candle_duplicates_replaced"] += 1
        else:
            ring = self._ordered_ring(state)
            newest = ring[-1] if ring else None
            out_of_order = newest is not None and candle.open_time < newest.open_time
            if out_of_order and not self._fills_known_gap(state, candle):
                self._counters["candle_out_of_order_rejected"] += 1
                return
            if (
                newest is not None
                and len(ring) >= self._ring_size
                and candle.open_time
                < newest.open_time - _MINUTE * (self._ring_size - 1)
            ):
                self._counters["candle_out_of_window_rejected"] += 1
                return
            state.candles[candle.open_time] = candle
            if out_of_order:
                self._counters["candle_gap_fill"] += 1
        state.last_closed_epoch = candle.connection_epoch
        if not state.continuity_reestablished and candle.source == (
            BYBIT_PUBLIC_WS_SOURCE
        ):
            is_current = (
                getattr(self._hub, "is_current_connection_epoch", None)
                if self._hub is not None
                else None
            )
            if is_current is None or is_current(candle.connection_epoch):
                state.continuity_reestablished = True
        self._evict(state)

    def _observe_restored(self, candle: MarketCandle) -> None:
        """Forward a REST candle that repairs a known continuity gap.

        The GapTracker learns about REST-repaired intervals through the same
        observer contract it uses for WebSocket candles, so a repaired gap
        clears instead of staying ``GAP_DETECTED`` forever.  Seeding (warmup
        or a cold backfill) deliberately does NOT forward: replaying a full
        REST history through the tracker's bounded sliding window recomputes
        one synthetic gap per candle and stalls the loop.
        """
        if self._continuity is None:
            return
        if (
            self._continuity.get_symbol_state(candle.symbol) is not GapState.GAP_DETECTED
            or not self._continuity.has_gap_at(candle.symbol, candle.open_time)
        ):
            return
        try:
            self._continuity.observe_closed_candle(candle)
        except Exception:  # noqa: BLE001 - tracker evidence cannot corrupt the ring
            LOGGER.warning("continuity forwarding failed", exc_info=True)

    def _fills_known_gap(self, state: _SymbolState, candle: MarketCandle) -> bool:
        if self._continuity is None:
            return True
        return self._continuity.get_symbol_state(candle.symbol) is (
            GapState.GAP_DETECTED
        ) or state.continuity_reestablished is False

    def _evict(self, state: _SymbolState) -> None:
        while len(state.candles) > self._ring_size:
            state.candles.pop(min(state.candles))

    def _ordered_ring(self, state: _SymbolState) -> list[MarketCandle]:
        if not state.candles:
            return []
        return [state.candles[key] for key in sorted(state.candles)]

    def _current_forming(
        self, symbol: str, state: _SymbolState
    ) -> MarketCandle | None:
        reader = getattr(self._hub, "get_forming_candle", None) if self._hub else None
        if reader is None:
            return None
        candle = reader(symbol)
        if candle is None or candle.confirmed:
            return None
        return candle

    def _oi_changes(
        self, state: _SymbolState, captured: datetime
    ) -> tuple[float | None, float | None]:
        minute = captured.replace(second=0, microsecond=0) - _MINUTE
        latest = self._sample_at(state, minute)
        if latest is None:
            if len(state.oi_seed_rows) >= 5:
                return self._seed_change(state.oi_seed_rows, 1), self._seed_change(
                    state.oi_seed_rows, 4
                )
            if len(state.oi_seed_rows) >= 2:
                return self._seed_change(state.oi_seed_rows, 1), None
            return None, None
        change_15m = self._pct(latest, self._sample_at(state, minute - 15 * _MINUTE))
        change_1h = self._pct(latest, self._sample_at(state, minute - 60 * _MINUTE))
        if change_15m is None and len(state.oi_seed_rows) >= 2:
            change_15m = self._seed_change(state.oi_seed_rows, 1)
        if change_1h is None and len(state.oi_seed_rows) >= 5:
            change_1h = self._seed_change(state.oi_seed_rows, 4)
        return change_15m, change_1h

    @staticmethod
    def _sample_at(state: _SymbolState, minute: datetime) -> Decimal | None:
        for sample_minute, value in reversed(state.oi_samples):
            if sample_minute <= minute:
                if minute - sample_minute <= timedelta(minutes=2):
                    return value
                return None
        return None

    @staticmethod
    def _pct(latest: Decimal, prior: Decimal | None) -> float | None:
        if prior is None or prior == 0:
            return None
        return round((float(latest) / float(prior) - 1.0) * 100.0, 6)

    @staticmethod
    def _seed_change(rows: list[dict[str, Any]], count: int) -> float | None:
        if len(rows) <= count:
            return None
        try:
            latest = float(
                rows[0].get("openInterest") or rows[0].get("openInterestValue") or 0.0
            )
            prior = float(
                rows[count].get("openInterest")
                or rows[count].get("openInterestValue")
                or 0.0
            )
        except (AttributeError, TypeError, ValueError):
            return None
        if prior == 0:
            return None
        return round((latest / prior - 1.0) * 100.0, 6)

    def _oi_history_rows(
        self, state: _SymbolState, captured: datetime
    ) -> list[dict[str, Any]]:
        """Newest-first 15-minute grid rows, FeatureBuilder-compatible."""
        minute = captured.replace(second=0, microsecond=0) - _MINUTE
        rows: list[dict[str, Any]] = []
        for step in range(5):
            sample_minute = minute - 15 * step * _MINUTE
            sample = self._sample_at(state, sample_minute)
            if sample is None:
                break
            rows.append(
                {
                    "openInterest": str(sample),
                    "timestamp": str(int(sample_minute.timestamp() * 1000)),
                }
            )
        if rows:
            return rows
        return list(state.oi_seed_rows)

    def _seed_ring(self, state: _SymbolState, symbol: str, rows: Any) -> int:
        # Bybit returns newest-first klines; rows at or after the current
        # minute boundary are the forming minute and stay out of the ring.
        current_minute = self._now().replace(second=0, microsecond=0)
        accepted = 0
        for entry in reversed(list(rows or [])):
            candle = self._candle_from_rest_row(symbol, entry)
            if candle is None or candle.open_time >= current_minute:
                continue
            seeded = replace(candle, source=REST_BACKFILL_SOURCE)
            if state.candles.get(seeded.open_time) is None:
                state.candles[seeded.open_time] = seeded
            accepted += 1
        self._evict(state)
        return accepted

    def _candle_from_rest_row(self, symbol: str, entry: Any) -> MarketCandle | None:
        try:
            start_ms = int(entry[0])
            open_time = datetime.fromtimestamp(start_ms / 1000, tz=UTC)
            values = [Decimal(str(item)) for item in entry[1:7]]
        except (TypeError, ValueError, IndexError):
            return None
        if len(values) < 6:
            return None
        return MarketCandle(
            symbol=symbol.upper(),
            open_time=open_time.replace(second=0, microsecond=0),
            close_time=open_time.replace(second=0, microsecond=0) + _MINUTE,
            interval="1",
            open=values[0],
            high=values[1],
            low=values[2],
            close=values[3],
            volume=values[4],
            turnover=values[5],
            confirmed=True,
            exchange_timestamp=None,
            received_at=self._now(),
            connection_epoch=None,
            source=REST_BACKFILL_SOURCE,
        )

    @staticmethod
    def _continuity_status(stale_reason: str | None) -> ContinuityStatus:
        if stale_reason in {STALE_WARMING, STALE_INCOMPLETE_RING}:
            return ContinuityStatus.WARMING
        if stale_reason == STALE_CONTINUITY:
            return ContinuityStatus.GAP
        if stale_reason == STALE_EPOCH_INVALIDATED:
            return ContinuityStatus.RECONNECTING
        return ContinuityStatus.HEALTHY

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("store clock must return a timezone-aware timestamp")
        return value.astimezone(UTC)
