"""Pure, non-actionable LANE-B early-drop warning evaluator."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any


class EarlyDropWarningStatus(StrEnum):
    WATCHING = "WATCHING"
    WEAKENING = "WEAKENING"
    WARNING = "WARNING"
    CONFIRMED_DROP = "CONFIRMED_DROP"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    DATA_DEGRADED = "DATA_DEGRADED"


@dataclass(frozen=True, slots=True)
class EarlyDropSnapshot:
    root_event_id: str
    event_revision: int
    symbol: str
    root_started_at: datetime | None
    observed_at: datetime | None
    market_asof: datetime | None
    latest_candle_at: datetime | None
    impulse_pct: float | None
    drawdown_pct: float | None
    event_high: float | None
    price: float | None
    previous_close: float | None
    current_close: float | None
    spread_pct: float | None
    slippage_pct: float | None
    depth_1pct_usdt: float | None
    depth_2pct_usdt: float | None


@dataclass(frozen=True, slots=True)
class EarlyDropWarningEvaluation:
    status: EarlyDropWarningStatus
    root_event_id: str
    event_revision: int
    symbol: str
    warning: bool
    actionable: bool
    reason: str
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def dedupe_key(self) -> str:
        return f"EARLY_DROP_WARNING:{self.root_event_id}:r{self.event_revision}"


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def evaluate_early_drop_warning(
    snapshot: EarlyDropSnapshot,
    *,
    min_impulse_pct: float = 8.0,
    min_drawdown_pct: float = 2.0,
    max_candle_age: timedelta = timedelta(minutes=2),
    max_root_age: timedelta = timedelta(minutes=90),
    max_spread_pct: float = 1.0,
    max_slippage_pct: float = 1.5,
    min_depth_1pct_usdt: float = 5_000.0,
    min_depth_2pct_usdt: float = 10_000.0,
) -> EarlyDropWarningEvaluation:
    """Evaluate a complete snapshot; missing/bad data never becomes a warning."""
    base = dict(
        root_event_id=snapshot.root_event_id,
        event_revision=snapshot.event_revision,
        symbol=snapshot.symbol,
        actionable=False,
    )
    required = (
        snapshot.root_event_id,
        snapshot.symbol,
        snapshot.root_started_at,
        snapshot.observed_at,
        snapshot.market_asof,
        snapshot.latest_candle_at,
        snapshot.impulse_pct,
        snapshot.drawdown_pct,
        snapshot.event_high,
        snapshot.price,
        snapshot.previous_close,
        snapshot.current_close,
        snapshot.spread_pct,
        snapshot.slippage_pct,
        snapshot.depth_1pct_usdt,
        snapshot.depth_2pct_usdt,
    )
    numeric = required[6:]
    if not isinstance(snapshot.event_revision, int) or snapshot.event_revision < 1 or not all(required[:6]) or not all(_finite(v) for v in numeric):
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.DATA_DEGRADED, warning=False, reason="incomplete_or_non_finite_data")
    observed = snapshot.observed_at
    asof = snapshot.market_asof
    if observed.tzinfo is None or asof.tzinfo is None or snapshot.latest_candle_at.tzinfo is None or snapshot.root_started_at.tzinfo is None:
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.DATA_DEGRADED, warning=False, reason="timezone_missing")
    if observed < asof or observed - snapshot.latest_candle_at > max_candle_age or observed - snapshot.root_started_at > max_root_age:
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.DATA_DEGRADED, warning=False, reason="stale_market_or_expired_root")
    if snapshot.event_high <= 0 or snapshot.price <= 0 or snapshot.current_close <= 0 or snapshot.previous_close <= 0:
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.DATA_DEGRADED, warning=False, reason="invalid_price")
    if snapshot.spread_pct > max_spread_pct or snapshot.slippage_pct > max_slippage_pct or snapshot.depth_1pct_usdt < min_depth_1pct_usdt or snapshot.depth_2pct_usdt < min_depth_2pct_usdt:
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.DATA_DEGRADED, warning=False, reason="incomplete_or_unsafe_orderbook")
    metadata = {
        "impulse_pct": float(snapshot.impulse_pct),
        "drawdown_pct": float(snapshot.drawdown_pct),
        "event_high": float(snapshot.event_high),
        "price": float(snapshot.price),
        "spread_pct": float(snapshot.spread_pct),
        "slippage_pct": float(snapshot.slippage_pct),
        "depth_1pct_usdt": float(snapshot.depth_1pct_usdt),
        "depth_2pct_usdt": float(snapshot.depth_2pct_usdt),
        "non_actionable": True,
    }
    if snapshot.impulse_pct < min_impulse_pct:
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.WATCHING, warning=False, reason="impulse_not_strong", metadata=metadata)
    if snapshot.drawdown_pct < min_drawdown_pct or snapshot.current_close >= snapshot.previous_close:
        return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.WEAKENING, warning=False, reason="drop_not_confirmed", metadata=metadata)
    return EarlyDropWarningEvaluation(**base, status=EarlyDropWarningStatus.WARNING, warning=True, reason="strong_impulse_followed_by_early_drop", metadata=metadata)


def format_early_drop_warning(evaluation: EarlyDropWarningEvaluation) -> str:
    """Format a clearly separate operator observation, never a short signal."""
    m = evaluation.metadata
    return (
        "⚠️ EARLY DROP WARNING | LANE-B\n"
        "НЕ ЯВЛЯЕТСЯ ГОТОВЫМ SHORT-СИГНАЛОМ\n"
        "Только наблюдение: вход и autoexecution запрещены.\n"
        f"Монета: {evaluation.symbol}\n"
        f"Root: {evaluation.root_event_id}, revision={evaluation.event_revision}\n"
        f"Статус: {evaluation.status.value}\n"
        f"Импульс: {m.get('impulse_pct', 'n/a')}% | Снижение: {m.get('drawdown_pct', 'n/a')}%\n"
        f"Причина: {evaluation.reason}\n"
    )
