"""Small value types shared by source readers, feeds, and persistence."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True, slots=True)
class DeliveredSignal:
    lane: str
    signal_id: str
    symbol: str
    signal_time_ms: int
    delivery_time_ms: int
    delivery_status: str
    strategy: str
    model_version: str | None
    config_version: str
    event_id: str | None
    decision: dict[str, Any]
    signal_price: float | None


@dataclass(frozen=True, slots=True)
class Trade:
    provider: str
    market: str
    symbol: str
    trade_id: str | None
    side: str
    price: Decimal
    quantity: Decimal
    exchange_ts_ms: int
    received_ts_ms: int

    @property
    def notional(self) -> Decimal:
        return self.price * self.quantity


@dataclass(frozen=True, slots=True)
class BookUpdate:
    provider: str
    market: str
    symbol: str
    update_type: str
    update_id: int
    cross_seq: int | None
    exchange_ts_ms: int
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    first_update_id: int | None = None
    previous_update_id: int | None = None


@dataclass(frozen=True, slots=True)
class FeatureSnapshot:
    symbol: str
    observed_ts_ms: int
    market_profile: str
    features: dict[str, Any]

