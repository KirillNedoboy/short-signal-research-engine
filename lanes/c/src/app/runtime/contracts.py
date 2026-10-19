"""Runtime contracts shared by Lane C's fast path and background workers.

The contract deliberately contains no exchange or persistence implementation.
Market data is immutable at the evaluation boundary; missing and stale values
remain explicit so a worker cannot turn a feed outage into a zero-valued signal.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite
from types import MappingProxyType
from typing import Any, Mapping


class SnapshotQuality(StrEnum):
    COMPLETE = "COMPLETE"
    PARTIAL = "PARTIAL"
    UNKNOWN = "UNKNOWN"


class SnapshotStatus(StrEnum):
    READY = "READY"
    WARMING = "WARMING"
    STALE = "STALE"
    DISCONNECTED = "DISCONNECTED"
    INVALID = "INVALID"


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({str(k): _freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_freeze(item) for item in value)
    return value


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """One as-of market view consumed by C feature/strategy adapters."""

    symbol: str
    asof: datetime
    received_at: datetime
    quality: SnapshotQuality
    status: SnapshotStatus
    last_price: float | None = None
    candles: tuple[Mapping[str, Any], ...] = ()
    orderbook: Mapping[str, Any] | None = None
    derivatives: Mapping[str, Any] | None = None
    source: str = "BYBIT_WS"
    missing_fields: tuple[str, ...] = ()
    continuity_epoch: str | None = None

    def __post_init__(self) -> None:
        if not self.symbol or self.symbol != self.symbol.upper():
            raise ValueError("symbol must be a non-empty uppercase identifier")
        if self.asof.tzinfo is None or self.received_at.tzinfo is None:
            raise ValueError("market timestamps must be timezone-aware")
        if self.asof > self.received_at:
            raise ValueError("asof must not be newer than received_at")
        object.__setattr__(self, "candles", tuple(_freeze(item) for item in self.candles))
        object.__setattr__(self, "orderbook", _freeze(self.orderbook))
        object.__setattr__(self, "derivatives", _freeze(self.derivatives))
        object.__setattr__(self, "missing_fields", tuple(sorted(set(self.missing_fields))))

    def with_status(
        self,
        status: SnapshotStatus,
        *,
        quality: SnapshotQuality | None = None,
        missing_fields: tuple[str, ...] | None = None,
    ) -> "MarketSnapshot":
        missing = self.missing_fields if missing_fields is None else missing_fields
        if status is not SnapshotStatus.READY and not missing:
            missing = ("market_data",)
        return replace(
            self,
            status=status,
            quality=quality or self.quality,
            missing_fields=missing,
        )

    def actionable(self, *, max_age: timedelta) -> bool:
        """Return whether this snapshot may reach an actionable strategy."""
        if self.status is not SnapshotStatus.READY:
            return False
        if self.quality is not SnapshotQuality.COMPLETE:
            return False
        if self.missing_fields:
            return False
        if self.received_at - self.asof > max_age:
            return False
        return self.last_price is not None and isfinite(float(self.last_price)) and self.last_price > 0


__all__ = ["MarketSnapshot", "SnapshotQuality", "SnapshotStatus"]

