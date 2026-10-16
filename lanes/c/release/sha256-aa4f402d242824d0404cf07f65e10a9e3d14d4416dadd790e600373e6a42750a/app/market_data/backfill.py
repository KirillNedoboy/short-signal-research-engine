"""Probe-only bounded REST repair for closed one-minute shadow gaps."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol

from app.market.bybit_client import RestMarketResponse

from .continuity import GapEvent, GapTracker
from .models import REST_BACKFILL_SOURCE, ConnectionEpoch, MarketCandle


class BackfillStatus(StrEnum):
    SUCCESS = "SUCCESS"
    RANGE_EXCEEDED = "BACKFILL_RANGE_EXCEEDED"
    REST_MISSING = "REST_MISSING"
    REST_ERROR = "REST_ERROR"
    FUTURE_LEAKAGE = "FUTURE_LEAKAGE"
    DATA_GAP = "DATA_GAP"


@dataclass(frozen=True, slots=True)
class BackfillResult:
    gap: GapEvent
    status: BackfillStatus
    attempts: int
    request_started_at: datetime | None
    response_received_at: datetime | None
    candles: tuple[MarketCandle, ...] = ()
    error_type: str | None = None


class KlineMetadataClient(Protocol):
    async def fetch_klines_with_metadata(
        self, symbol: str, interval: str, **kwargs: object
    ) -> RestMarketResponse[list[list[str]]]: ...


class ClosedCandleBackfiller:
    def __init__(
        self,
        *,
        client: KlineMetadataClient,
        tracker: GapTracker,
        max_candles: int = 15,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if max_candles < 1:
            raise ValueError("max_candles must be positive")
        self._client = client
        self._tracker = tracker
        self._max_candles = max_candles
        self._sleeper = sleeper
        self._clock = clock or (lambda: datetime.now(UTC))

    async def repair(
        self, gap: GapEvent, *, decision_time: datetime
    ) -> BackfillResult:
        decision_at = _utc(decision_time)
        if gap.missing_count > self._max_candles:
            self._tracker.mark_backfill_failure(gap)
            return BackfillResult(gap, BackfillStatus.RANGE_EXCEEDED, 0, None, None)
        expected = tuple(
            gap.expected_start + timedelta(minutes=index)
            for index in range(gap.missing_count)
        )
        start_ms = int(expected[0].timestamp() * 1000)
        end_ms = int(gap.observed_next_start.timestamp() * 1000) - 1
        self._tracker.mark_backfilling(gap)
        last_started: datetime | None = None
        last_received: datetime | None = None
        for attempt, delay in enumerate((0.0, 1.0, 2.0), start=1):
            if delay:
                await self._sleeper(delay)
            last_started = _utc(self._clock())
            try:
                response = await self._client.fetch_klines_with_metadata(
                    gap.symbol,
                    "1",
                    limit=gap.missing_count,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - evidence records only exception type
                last_received = _utc(self._clock())
                self._tracker.mark_backfill_failure(gap)
                return BackfillResult(
                    gap,
                    BackfillStatus.REST_ERROR,
                    attempt,
                    last_started,
                    last_received,
                    error_type=type(exc).__name__,
                )
            last_received = _utc(self._clock())
            parsed = _parse_rows(
                response.data,
                symbol=gap.symbol,
                expected=expected,
                received_at=last_received,
                server_time=response.server_time,
                connection_epoch=gap.connection_epoch,
            )
            if parsed is None:
                self._tracker.mark_backfill_failure(gap)
                return BackfillResult(
                    gap,
                    BackfillStatus.DATA_GAP,
                    attempt,
                    last_started,
                    last_received,
                )
            if len(parsed) != len(expected):
                continue
            if any(candle.close_time > decision_at for candle in parsed):
                self._tracker.mark_backfill_failure(gap)
                return BackfillResult(
                    gap,
                    BackfillStatus.FUTURE_LEAKAGE,
                    attempt,
                    last_started,
                    last_received,
                )
            for candle in parsed:
                self._tracker.observe_closed_candle(candle)
            return BackfillResult(
                gap,
                BackfillStatus.SUCCESS,
                attempt,
                last_started,
                last_received,
                parsed,
            )
        self._tracker.mark_backfill_failure(gap)
        return BackfillResult(
            gap,
            BackfillStatus.REST_MISSING,
            3,
            last_started,
            last_received,
        )


def _parse_rows(
    rows: list[list[str]],
    *,
    symbol: str,
    expected: tuple[datetime, ...],
    received_at: datetime,
    server_time: datetime | None,
    connection_epoch: ConnectionEpoch | None,
) -> tuple[MarketCandle, ...] | None:
    expected_by_ms = {int(value.timestamp() * 1000): value for value in expected}
    found: dict[int, MarketCandle] = {}
    for row in rows:
        if len(row) < 7:
            return None
        try:
            start_ms = int(str(row[0]))
            values = tuple(Decimal(str(value)) for value in row[1:7])
        except (InvalidOperation, TypeError, ValueError):
            return None
        if start_ms not in expected_by_ms:
            return None
        if start_ms in found or any(not value.is_finite() for value in values):
            return None
        opened = expected_by_ms[start_ms]
        found[start_ms] = MarketCandle(
            symbol=symbol.upper(),
            open_time=opened,
            close_time=opened + timedelta(milliseconds=59_999),
            interval="1",
            open=values[0],
            high=values[1],
            low=values[2],
            close=values[3],
            volume=values[4],
            turnover=values[5],
            confirmed=True,
            exchange_timestamp=_utc(server_time) if server_time else None,
            received_at=received_at,
            connection_epoch=connection_epoch,  # type: ignore[arg-type]
            source=REST_BACKFILL_SOURCE,
        )
    return tuple(found[key] for key in sorted(found))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("backfill timestamps must be timezone-aware")
    return value.astimezone(UTC)
