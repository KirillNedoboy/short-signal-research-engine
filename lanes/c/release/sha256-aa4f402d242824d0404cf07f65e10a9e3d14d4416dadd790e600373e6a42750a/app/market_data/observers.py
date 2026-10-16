"""Synchronous observer contracts for normalized market-data evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from .models import ConnectionEpoch, MarketCandle, MarketTickerSnapshot


@dataclass(frozen=True, slots=True)
class ConnectionOpened:
    connection_epoch: ConnectionEpoch
    topics: tuple[str, ...]
    connected_at: datetime


class MarketDataObserver(Protocol):
    def observe_connection(self, event: ConnectionOpened) -> None: ...

    def observe_ticker(self, snapshot: MarketTickerSnapshot) -> None: ...

    def observe_closed_candle(self, candle: MarketCandle) -> None: ...
