"""Protocol-neutral orderbook state with an explicit UNKNOWN state."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable

from .models import BookUpdate


@dataclass(slots=True)
class BookState:
    depth: int = 50
    stale_after_ms: int = 5_000
    state: str = "UNINITIALIZED"
    last_update_id: int | None = None
    last_cross_seq: int | None = None
    last_received_ms: int | None = None
    last_reason: str | None = None
    bids: dict[Decimal, Decimal] = field(default_factory=dict)
    asks: dict[Decimal, Decimal] = field(default_factory=dict)
    depth_history: list[tuple[int, Decimal, Decimal]] = field(default_factory=list)

    def reset(self, reason: str = "reset") -> None:
        self.state = "INVALID"
        self.last_reason = reason
        self.last_update_id = None
        self.last_cross_seq = None

    def apply(self, update: BookUpdate, now_ms: int) -> bool:
        if update.update_type == "snapshot":
            self.bids = {p: q for p, q in update.bids if q > 0}
            self.asks = {p: q for p, q in update.asks if q > 0}
            self._trim()
            self.last_update_id = update.update_id
            self.last_cross_seq = update.cross_seq
            self.last_received_ms = now_ms
            self.state = "READY"
            self.last_reason = None
            self._remember_depth(now_ms)
            return True

        # Bybit cross-sequence values are monotonic correlation values, not a
        # promised +1 stream.  Binance supplies a previous-final-id check.
        if self.state not in {"READY", "CONTIGUOUS"} or self.last_update_id is None:
            self.reset("delta-before-snapshot")
            return False
        if update.previous_update_id is not None and update.previous_update_id != self.last_update_id:
            self.reset("delta-gap")
            return False
        if update.update_id <= self.last_update_id:
            self.reset("update-id-regression")
            return False
        if self.last_cross_seq is not None and update.cross_seq is not None and update.cross_seq <= self.last_cross_seq:
            self.reset("cross-seq-regression")
            return False
        for price, size in update.bids:
            self._apply_level(self.bids, price, size)
        for price, size in update.asks:
            self._apply_level(self.asks, price, size)
        self._trim()
        self.last_update_id = update.update_id
        self.last_cross_seq = update.cross_seq
        self.last_received_ms = now_ms
        self.state = "CONTIGUOUS"
        self.last_reason = None
        self._remember_depth(now_ms)
        return True

    def is_fresh(self, now_ms: int) -> bool:
        return self.state in {"READY", "CONTIGUOUS"} and self.last_received_ms is not None and now_ms - self.last_received_ms <= self.stale_after_ms

    def top(self, side: str) -> tuple[Decimal, Decimal] | None:
        if side == "bid" and self.bids:
            price = max(self.bids)
            return price, self.bids[price]
        if side == "ask" and self.asks:
            price = min(self.asks)
            return price, self.asks[price]
        return None

    def mid(self) -> Decimal | None:
        bid, ask = self.top("bid"), self.top("ask")
        if not bid or not ask:
            return None
        return (bid[0] + ask[0]) / Decimal(2)

    def depth_notional(self, side: str, levels: int = 10) -> Decimal | None:
        book = self.bids if side == "bid" else self.asks
        if not book:
            return None
        values = sorted(book.items(), reverse=side == "bid")[:levels]
        return sum((price * qty for price, qty in values), Decimal(0))

    def replenishment(self, side: str, levels: int = 10) -> float | None:
        if len(self.depth_history) < 3:
            return None
        current = self.depth_notional(side, levels)
        if current is None:
            return None
        index = 1 if side == "bid" else 2
        history = [float(row[index]) for row in self.depth_history]
        minimum = min(history)
        if minimum <= 0 or current < Decimal(str(minimum)):
            return None
        dip = (minimum - history[0]) / history[0] if history[0] else 0.0
        recovery = (float(current) - minimum) / minimum if minimum else 0.0
        return recovery if dip <= -0.05 or recovery >= 0.05 else 0.0

    def _remember_depth(self, now_ms: int) -> None:
        bid = self.depth_notional("bid", 10) or Decimal(0)
        ask = self.depth_notional("ask", 10) or Decimal(0)
        self.depth_history.append((now_ms, bid, ask))
        cutoff = now_ms - 120_000
        self.depth_history[:] = [row for row in self.depth_history[-240:] if row[0] >= cutoff]

    def _trim(self) -> None:
        if len(self.bids) > self.depth:
            self.bids = dict(sorted(self.bids.items(), reverse=True)[: self.depth])
        if len(self.asks) > self.depth:
            self.asks = dict(sorted(self.asks.items())[: self.depth])

    @staticmethod
    def _apply_level(book: dict[Decimal, Decimal], price: Decimal, size: Decimal) -> None:
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size

