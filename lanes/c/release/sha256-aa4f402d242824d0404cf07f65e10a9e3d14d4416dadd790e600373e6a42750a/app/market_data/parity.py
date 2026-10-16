"""Bounded temporal ticker and exact closed-kline parity primitives."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum

from .models import MarketCandle, MarketTickerSnapshot
from .observers import ConnectionOpened


class TickerMatchClassification(StrEnum):
    EXACT_OBSERVED_IN_WINDOW = "EXACT_OBSERVED_IN_WINDOW"
    EXACT_NEAREST_BEFORE = "EXACT_NEAREST_BEFORE"
    EXACT_NEAREST_AFTER = "EXACT_NEAREST_AFTER"
    TEMPORAL_VALUE_DIFFERENCE = "TEMPORAL_VALUE_DIFFERENCE"
    WS_FIELD_UNOBSERVED = "WS_FIELD_UNOBSERVED"
    REST_FIELD_UNOBSERVED = "REST_FIELD_UNOBSERVED"
    WS_STALE = "WS_STALE"
    NOT_COMPARABLE = "NOT_COMPARABLE"


class KlineClassification(StrEnum):
    EXACT_MATCH = "EXACT_MATCH"
    REST_NOT_YET_VISIBLE = "REST_NOT_YET_VISIBLE"
    REST_MISSING = "REST_MISSING"
    WS_MISSING = "WS_MISSING"
    FIELD_MISMATCH = "FIELD_MISMATCH"
    DATA_GAP = "DATA_GAP"


@dataclass(frozen=True, slots=True)
class UniverseCoverage:
    rest_symbols: tuple[str, ...]
    ws_symbols: tuple[str, ...]
    intersection: tuple[str, ...]
    rest_only: tuple[str, ...]
    ws_only: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TickerFieldResult:
    symbol: str
    field: str
    classification: TickerMatchClassification
    rest_value: object | None
    ws_value: object | None


@dataclass(frozen=True, slots=True)
class TickerProbeResult:
    request_started_at: datetime
    request_started_monotonic: float
    response_received_at: datetime
    response_received_monotonic: float
    server_time: datetime | None
    comparison_anchor: datetime
    anchor_source: str
    fields: tuple[TickerFieldResult, ...]
    coverage: UniverseCoverage
    overflowed: bool
    high_water_states: int

    @property
    def exact_observed(self) -> int:
        exact = {
            TickerMatchClassification.EXACT_OBSERVED_IN_WINDOW,
            TickerMatchClassification.EXACT_NEAREST_BEFORE,
            TickerMatchClassification.EXACT_NEAREST_AFTER,
        }
        return sum(item.classification in exact for item in self.fields)


@dataclass(frozen=True, slots=True)
class KlineParityResult:
    symbol: str
    open_time: datetime | None
    classification: KlineClassification
    field_differences: tuple[str, ...] = ()
    rest_values: tuple[Decimal, ...] | None = None


_FIELDS = {
    "lastPrice": "last_price",
    "markPrice": "mark_price",
    "indexPrice": "index_price",
    "bid1Price": "bid1_price",
    "bid1Size": "bid1_size",
    "ask1Price": "ask1_price",
    "ask1Size": "ask1_size",
    "openInterest": "open_interest",
    "openInterestValue": "open_interest_value",
    "fundingRate": "funding_rate",
    "nextFundingTime": "next_funding_time",
}


class ParityMonitor:
    def __init__(
        self,
        *,
        ws_symbols: tuple[str, ...],
        max_states_per_symbol: int = 64,
        max_total_states: int = 20_000,
        stale_after_seconds: float = 45,
    ) -> None:
        if max_states_per_symbol < 1 or max_total_states < 1:
            raise ValueError("parity buffer bounds must be positive")
        self._ws_symbols = tuple(sorted({symbol.upper() for symbol in ws_symbols}))
        self._per_symbol_cap = max_states_per_symbol
        self._total_cap = max_total_states
        self._stale_after = timedelta(seconds=stale_after_seconds)
        self._current: dict[str, MarketTickerSnapshot] = {}
        self._active = False
        self._carry: dict[str, MarketTickerSnapshot] = {}
        self._states: dict[str, list[MarketTickerSnapshot]] = {}
        self._request_started_at: datetime | None = None
        self._request_started_monotonic = 0.0
        self._overflowed = False
        self._high_water = 0
        self._max_high_water = 0

    @property
    def max_high_water_states(self) -> int:
        return self._max_high_water

    def observe_connection(self, event: ConnectionOpened) -> None:
        affected = {
            topic.removeprefix("tickers.")
            for topic in event.topics
            if topic.startswith("tickers.")
        }
        for symbol in affected:
            self._current.pop(symbol, None)

    def observe_closed_candle(self, candle: MarketCandle) -> None:
        return None

    def observe_ticker(self, snapshot: MarketTickerSnapshot) -> None:
        symbol = snapshot.symbol.upper()
        self._current[symbol] = snapshot
        if not self._active:
            return
        states = self._states.setdefault(symbol, [])
        total = sum(len(items) for items in self._states.values())
        if len(states) >= self._per_symbol_cap or total >= self._total_cap:
            self._overflowed = True
            return
        states.append(snapshot)
        self._high_water = max(self._high_water, total + 1)
        self._max_high_water = max(self._max_high_water, self._high_water)

    def begin_ticker_window(
        self, *, request_started_at: datetime, request_started_monotonic: float
    ) -> None:
        if self._active:
            raise RuntimeError("ticker parity window is already active")
        self._active = True
        self._carry = dict(self._current)
        self._states = {}
        self._request_started_at = _utc(request_started_at)
        self._request_started_monotonic = request_started_monotonic
        self._overflowed = False
        self._high_water = 0

    def abort_ticker_window(self) -> None:
        """Release all probe-only state after a failed REST request."""

        self._active = False
        self._carry = {}
        self._states = {}
        self._request_started_at = None
        self._request_started_monotonic = 0.0
        self._overflowed = False
        self._high_water = 0

    def finish_ticker_window(
        self,
        *,
        rest_rows: list[dict],
        server_time: datetime | None,
        response_received_at: datetime,
        response_received_monotonic: float,
    ) -> TickerProbeResult:
        if not self._active or self._request_started_at is None:
            raise RuntimeError("ticker parity window is not active")
        response_at = _utc(response_received_at)
        server_at = _utc(server_time) if server_time is not None else None
        anchor = server_at or self._request_started_at + (
            response_at - self._request_started_at
        ) / 2
        rest_by_symbol = {
            str(row.get("symbol", "")).upper(): row
            for row in rest_rows
            if row.get("symbol")
        }
        rest_symbols = tuple(sorted(rest_by_symbol))
        intersection = tuple(sorted(set(rest_symbols) & set(self._ws_symbols)))
        coverage = UniverseCoverage(
            rest_symbols=rest_symbols,
            ws_symbols=self._ws_symbols,
            intersection=intersection,
            rest_only=tuple(sorted(set(rest_symbols) - set(self._ws_symbols))),
            ws_only=tuple(sorted(set(self._ws_symbols) - set(rest_symbols))),
        )
        fields: list[TickerFieldResult] = []
        for symbol in intersection:
            row = rest_by_symbol[symbol]
            candidates = ([] if symbol not in self._carry else [self._carry[symbol]]) + self._states.get(symbol, [])
            for external, internal in _FIELDS.items():
                if self._overflowed:
                    classification = TickerMatchClassification.NOT_COMPARABLE
                    rest_value = None
                    ws_value = None
                else:
                    rest_value = _ticker_value(external, row.get(external))
                    ws_values = [
                        (candidate, getattr(candidate, internal))
                        for candidate in candidates
                        if getattr(candidate, internal) is not None
                    ]
                    ws_value = ws_values[-1][1] if ws_values else None
                    classification = self._classify(
                        external=external,
                        row=row,
                        rest_value=rest_value,
                        ws_values=ws_values,
                        in_window=self._states.get(symbol, []),
                        anchor=anchor,
                        response_at=response_at,
                    )
                fields.append(
                    TickerFieldResult(
                        symbol, external, classification, rest_value, ws_value
                    )
                )
        result = TickerProbeResult(
            request_started_at=self._request_started_at,
            request_started_monotonic=self._request_started_monotonic,
            response_received_at=response_at,
            response_received_monotonic=response_received_monotonic,
            server_time=server_at,
            comparison_anchor=anchor,
            anchor_source="REST_SERVER_TIME" if server_at else "REQUEST_RESPONSE_MIDPOINT",
            fields=tuple(fields),
            coverage=coverage,
            overflowed=self._overflowed,
            high_water_states=self._high_water,
        )
        self._active = False
        self._carry = {}
        self._states = {}
        return result

    def _classify(
        self,
        *,
        external: str,
        row: dict,
        rest_value: object | None,
        ws_values: list[tuple[MarketTickerSnapshot, object]],
        in_window: list[MarketTickerSnapshot],
        anchor: datetime,
        response_at: datetime,
    ) -> TickerMatchClassification:
        if external not in row or row.get(external) in (None, ""):
            return TickerMatchClassification.REST_FIELD_UNOBSERVED
        if rest_value is None:
            return TickerMatchClassification.NOT_COMPARABLE
        if not ws_values:
            return TickerMatchClassification.WS_FIELD_UNOBSERVED
        newest = ws_values[-1][0]
        if not in_window and newest.received_at and response_at - newest.received_at > self._stale_after:
            return TickerMatchClassification.WS_STALE
        exact = [(snapshot, value) for snapshot, value in ws_values if value == rest_value]
        if not exact:
            return TickerMatchClassification.TEMPORAL_VALUE_DIFFERENCE
        in_window_ids = {id(snapshot) for snapshot in in_window}
        exact_in_window = [item for item in exact if id(item[0]) in in_window_ids]
        if exact_in_window:
            nearest = min(
                exact_in_window,
                key=lambda item: abs((item[0].exchange_timestamp or anchor) - anchor),
            )[0]
            if nearest.exchange_timestamp is None or nearest.exchange_timestamp == anchor:
                return TickerMatchClassification.EXACT_OBSERVED_IN_WINDOW
            if nearest.exchange_timestamp < anchor:
                return TickerMatchClassification.EXACT_NEAREST_BEFORE
            return TickerMatchClassification.EXACT_NEAREST_AFTER
        return TickerMatchClassification.EXACT_NEAREST_BEFORE


def compare_closed_kline(
    ws_candle: MarketCandle | None,
    rest_rows: list[list[str]],
    *,
    final_attempt: bool = False,
) -> KlineParityResult:
    if ws_candle is None:
        return KlineParityResult("", None, KlineClassification.WS_MISSING)
    if not ws_candle.confirmed:
        raise ValueError("closed-kline parity requires a confirmed WS candle")
    target_ms = int(ws_candle.open_time.timestamp() * 1000)
    matches = [row for row in rest_rows if row and str(row[0]) == str(target_ms)]
    if not matches:
        classification = (
            KlineClassification.REST_MISSING
            if final_attempt
            else KlineClassification.REST_NOT_YET_VISIBLE
        )
        return KlineParityResult(ws_candle.symbol, ws_candle.open_time, classification)
    if len(matches) != 1 or len(matches[0]) < 7:
        return KlineParityResult(ws_candle.symbol, ws_candle.open_time, KlineClassification.DATA_GAP)
    try:
        values = tuple(Decimal(str(value)) for value in matches[0][1:7])
    except (InvalidOperation, ValueError):
        return KlineParityResult(ws_candle.symbol, ws_candle.open_time, KlineClassification.DATA_GAP)
    ws_values = tuple(getattr(ws_candle, name) for name in ("open", "high", "low", "close", "volume", "turnover"))
    differences = tuple(
        name
        for name, ws_value, rest_value in zip(
            ("open", "high", "low", "close", "volume", "turnover"),
            ws_values,
            values,
            strict=True,
        )
        if ws_value != rest_value
    )
    return KlineParityResult(
        ws_candle.symbol,
        ws_candle.open_time,
        KlineClassification.FIELD_MISMATCH if differences else KlineClassification.EXACT_MATCH,
        differences,
        values,
    )


def _ticker_value(field: str, value: object) -> Decimal | datetime | None:
    if value in (None, ""):
        return None
    try:
        if field == "nextFundingTime":
            return datetime.fromtimestamp(int(str(value)) / 1000, tz=UTC)
        decimal = Decimal(str(value))
        return decimal if decimal.is_finite() else None
    except (InvalidOperation, TypeError, ValueError, OSError, OverflowError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("parity timestamps must be timezone-aware")
    return value.astimezone(UTC)
