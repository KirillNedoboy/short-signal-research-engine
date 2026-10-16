"""Bounded closed-1m continuity tracking for the WS shadow."""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from itertools import pairwise

from .models import (
    BYBIT_PUBLIC_WS_SOURCE,
    REST_BACKFILL_SOURCE,
    ConnectionEpoch,
    MarketCandle,
    MarketTickerSnapshot,
)
from .observers import ConnectionOpened

LOGGER = logging.getLogger(__name__)


class GapState(StrEnum):
    UNINITIALIZED = "UNINITIALIZED"
    TRACKING = "TRACKING"
    GAP_DETECTED = "GAP_DETECTED"
    BACKFILLING = "BACKFILLING"
    CONTIGUOUS = "CONTIGUOUS"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class GapEvent:
    symbol: str
    expected_start: datetime
    observed_next_start: datetime
    missing_count: int
    detected_at: datetime
    connection_epoch: ConnectionEpoch | None
    reason: str = "MISSING_CLOSED_1M_INTERVAL"

    @property
    def identity(self) -> tuple[str, datetime, datetime]:
        return self.symbol, self.expected_start, self.observed_next_start


@dataclass(frozen=True, slots=True)
class ParityConflict:
    symbol: str
    open_time: datetime
    rest_candle: MarketCandle
    ws_candle: MarketCandle
    field_differences: tuple[str, ...]
    detected_at: datetime
    connection_epoch: ConnectionEpoch | None


@dataclass(frozen=True, slots=True)
class GapHealth:
    connections_observed: int = 0
    gaps_detected: int = 0
    gaps_backfilled: int = 0
    backfill_failures: int = 0
    parity_conflicts: int = 0
    source_confirmations: int = 0
    duplicates: int = 0
    late_candles: int = 0
    active_gaps: int = 0


_CANDLE_FIELDS = ("open", "high", "low", "close", "volume", "turnover")


class GapTracker:
    """Track only confirmed one-minute candle continuity in bounded memory."""

    def __init__(
        self, *, max_candles_per_symbol: int = 17, max_evidence_events: int = 100
    ) -> None:
        if max_candles_per_symbol < 2 or max_evidence_events < 1:
            raise ValueError("gap tracker bounds must be positive")
        self._max_candles = max_candles_per_symbol
        self._candles: dict[str, dict[datetime, MarketCandle]] = {}
        self._states: dict[str, GapState] = {}
        self._active: dict[str, dict[tuple[str, datetime, datetime], GapEvent]] = {}
        self._events: deque[GapEvent] = deque(maxlen=max_evidence_events)
        self._conflicts: deque[ParityConflict] = deque(maxlen=max_evidence_events)
        self._health = GapHealth()

    def ensure_decision_window(self, candles: int) -> None:
        """Cover the store ring before observing candles; never resize lost history."""
        if candles <= self._max_candles:
            return
        if self._candles:
            raise ValueError("cannot enlarge continuity window after observing candles")
        self._max_candles = candles

    @property
    def active_gaps(self) -> tuple[GapEvent, ...]:
        return tuple(sorted((gap for gaps in self._active.values() for gap in gaps.values()), key=lambda gap: gap.identity))

    @property
    def events(self) -> tuple[GapEvent, ...]:
        return tuple(self._events)

    @property
    def conflicts(self) -> tuple[ParityConflict, ...]:
        return tuple(self._conflicts)

    def get_health(self) -> GapHealth:
        return replace(self._health, active_gaps=sum(len(gaps) for gaps in self._active.values()))

    def get_symbol_state(self, symbol: str) -> GapState:
        return self._states.get(symbol.upper(), GapState.UNINITIALIZED)

    def has_gap_since(self, symbol: str, oldest_open_time: datetime) -> bool:
        """Whether an unresolved gap overlaps the retained decision candles."""
        return any(
            gap.observed_next_start > oldest_open_time
            for gap in self._active.get(symbol.upper(), {}).values()
        )

    def has_gap_at(self, symbol: str, open_time: datetime) -> bool:
        return any(
            gap.expected_start <= open_time < gap.observed_next_start
            for gap in self._active.get(symbol.upper(), {}).values()
        )

    def get_candles(self, symbol: str) -> tuple[MarketCandle, ...]:
        candles = self._candles.get(symbol.upper(), {})
        return tuple(candles[key] for key in sorted(candles))

    def observe_connection(self, event: ConnectionOpened) -> None:
        self._health = replace(
            self._health,
            connections_observed=self._health.connections_observed + 1,
        )

    def observe_ticker(self, snapshot: MarketTickerSnapshot) -> None:
        return None

    def observe_closed_candle(self, candle: MarketCandle) -> None:
        if not candle.confirmed or candle.interval != "1":
            return
        symbol = candle.symbol.upper()
        candles = self._candles.setdefault(symbol, {})
        existing = candles.get(candle.open_time)
        if existing is not None:
            self._handle_duplicate(existing, candle)
            return
        if candles and candle.open_time < max(candles):
            self._health = replace(
                self._health, late_candles=self._health.late_candles + 1
            )
        candles[candle.open_time] = candle
        while len(candles) > self._max_candles:
            candles.pop(min(candles))
        repaired = self._recompute_symbol(symbol, detected_at=candle.received_at)
        if candle.source == REST_BACKFILL_SOURCE and repaired:
            self._health = replace(
                self._health, gaps_backfilled=self._health.gaps_backfilled + 1,
            )

    def mark_backfilling(self, gap: GapEvent) -> None:
        if gap.identity in self._active.get(gap.symbol, {}):
            self._states[gap.symbol] = GapState.BACKFILLING

    def mark_backfill_failure(self, gap: GapEvent) -> None:
        if gap.identity in self._active.get(gap.symbol, {}):
            self._states[gap.symbol] = GapState.DEGRADED
        self._health = replace(
            self._health, backfill_failures=self._health.backfill_failures + 1
        )

    def _handle_duplicate(
        self, existing: MarketCandle, incoming: MarketCandle
    ) -> None:
        differences = tuple(
            field
            for field in _CANDLE_FIELDS
            if getattr(existing, field) != getattr(incoming, field)
        )
        sources = {existing.source, incoming.source}
        if sources == {BYBIT_PUBLIC_WS_SOURCE, REST_BACKFILL_SOURCE}:
            if differences:
                rest = existing if existing.source == REST_BACKFILL_SOURCE else incoming
                ws = incoming if incoming.source == BYBIT_PUBLIC_WS_SOURCE else existing
                self._conflicts.append(
                    ParityConflict(
                        symbol=incoming.symbol,
                        open_time=incoming.open_time,
                        rest_candle=rest,
                        ws_candle=ws,
                        field_differences=differences,
                        detected_at=incoming.received_at,
                        connection_epoch=incoming.connection_epoch,
                    )
                )
                self._health = replace(
                    self._health,
                    parity_conflicts=self._health.parity_conflicts + 1,
                )
                LOGGER.warning(
                    "market data parity conflict symbol=%s open_time=%s epoch=%s fields=%s",
                    incoming.symbol,
                    incoming.open_time.isoformat(),
                    incoming.connection_epoch,
                    ",".join(differences),
                )
            else:
                self._health = replace(
                    self._health,
                    source_confirmations=self._health.source_confirmations + 1,
                )
            return
        self._health = replace(
            self._health, duplicates=self._health.duplicates + 1
        )

    def _recompute_symbol(self, symbol: str, *, detected_at: datetime) -> bool:
        candles = self._candles[symbol]
        times = sorted(candles)
        active = self._active.setdefault(symbol, {})
        prior_for_symbol = dict(active)
        active.clear()
        # A gap at or before the earliest retained candle cannot overlap the
        # decision window. Retiring it is not a REST repair.
        retained = {
            identity: gap for identity, gap in prior_for_symbol.items()
            if gap.observed_next_start > times[0]
        }
        repaired = False
        for identity, gap in retained.items():
            # Check only the missing span that can still overlap the retained
            # decision window. An older unfilled prefix is not a veto once it
            # has left that window; nor does its retirement count as a repair.
            missing_start = max(gap.expected_start, times[0])
            relevant_count = int(
                (gap.observed_next_start - missing_start) / timedelta(minutes=1)
            )
            if relevant_count > len(candles) or any(
                missing_start + timedelta(minutes=index) not in candles
                for index in range(relevant_count)
            ):
                active[identity] = gap
            elif gap.expected_start >= times[0]:
                repaired = True
        if len(times) == 1:
            self._states[symbol] = GapState.TRACKING
            return repaired
        detected = False
        for previous, current in pairwise(times):
            distance = int((current - previous) / timedelta(minutes=1))
            if distance <= 1:
                continue
            gap = GapEvent(
                symbol=symbol,
                expected_start=previous + timedelta(minutes=1),
                observed_next_start=current,
                missing_count=distance - 1,
                detected_at=detected_at,
                connection_epoch=candles[current].connection_epoch,
            )
            if any(_contains(existing, gap) for existing in retained.values()):
                continue
            active[gap.identity] = gap
            detected = True
            self._events.append(gap)
            self._health = replace(
                self._health, gaps_detected=self._health.gaps_detected + 1
            )
            LOGGER.warning(
                "closed candle gap detected symbol=%s expected=%s observed=%s missing=%s epoch=%s",
                gap.symbol,
                gap.expected_start.isoformat(),
                gap.observed_next_start.isoformat(),
                gap.missing_count,
                gap.connection_epoch,
            )
        self._states[symbol] = (
            GapState.GAP_DETECTED
            if detected or any(identity in active for identity in retained)
            else GapState.CONTIGUOUS
        )
        return repaired


def _contains(existing: GapEvent, candidate: GapEvent) -> bool:
    return (
        existing.symbol == candidate.symbol
        and existing.expected_start <= candidate.expected_start
        and candidate.observed_next_start <= existing.observed_next_start
    )
