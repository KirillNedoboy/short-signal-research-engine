"""Normalized immutable market-data models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

BYBIT_PUBLIC_WS_SOURCE = "BYBIT_PUBLIC_WS"
REST_BACKFILL_SOURCE = "REST_BACKFILL"


@dataclass(frozen=True, slots=True)
class ConnectionEpoch:
    """Identity of one physical socket lifecycle within a runtime."""

    shard_id: int
    generation: int


class HealthStatus(StrEnum):
    CONNECTING = "CONNECTING"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    RECONNECTING = "RECONNECTING"
    DISCONNECTED = "DISCONNECTED"
    STOPPED = "STOPPED"


@dataclass(frozen=True, slots=True)
class MarketTickerSnapshot:
    symbol: str
    connection_epoch: ConnectionEpoch | None = None
    last_price: Decimal | None = None
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    funding_rate: Decimal | None = None
    open_interest: Decimal | None = None
    volume_24h: Decimal | None = None
    turnover_24h: Decimal | None = None
    price_24h_pcnt: Decimal | None = None
    high_price_24h: Decimal | None = None
    low_price_24h: Decimal | None = None
    prev_price_24h: Decimal | None = None
    bid1_price: Decimal | None = None
    bid1_size: Decimal | None = None
    ask1_price: Decimal | None = None
    ask1_size: Decimal | None = None
    open_interest_value: Decimal | None = None
    next_funding_time: datetime | None = None
    cs: int | None = None
    exchange_timestamp: datetime | None = None
    received_at: datetime | None = None
    source: str = BYBIT_PUBLIC_WS_SOURCE


@dataclass(frozen=True, slots=True)
class MarketCandle:
    symbol: str
    open_time: datetime
    close_time: datetime
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    turnover: Decimal
    confirmed: bool
    exchange_timestamp: datetime | None
    received_at: datetime
    connection_epoch: ConnectionEpoch | None = None
    source: str = BYBIT_PUBLIC_WS_SOURCE


@dataclass(frozen=True, slots=True)
class MarketDataHealth:
    state: HealthStatus
    updated_at: datetime
    last_message_at: datetime | None = None
    last_ticker_at: datetime | None = None
    last_closed_candle_at: datetime | None = None
    parse_errors: int = 0
    anomalies: int = 0
    reconnects: int = 0
    disconnects: int = 0
    active_connections: int = 0
    messages_received: int = 0
    ticker_updates: int = 0
    kline_updates: int = 0
    acknowledgements: int = 0
    pong_messages: int = 0
    observer_errors: int = 0
    stale_ticker_symbols: frozenset[str] = frozenset()
    stale_closed_candle_symbols: frozenset[str] = frozenset()
    stale_symbols: int = 0


@dataclass(frozen=True, slots=True)
class SubscriptionState:
    expected_topics: frozenset[str]
    acknowledged_topics: frozenset[str]
    failed_topics: frozenset[str]
    pending_topics: frozenset[str]
