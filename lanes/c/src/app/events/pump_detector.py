"""Pump event detection."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta

import pandas as pd

from app.config import AppConfig
from app.domain import EventState, EventStatus, SymbolFeatures
from app.market.candles import closed_1m_rows


class PumpDetector:
    """Detect and materialize post-pump candidate events."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def qualifies(self, features: SymbolFeatures) -> tuple[bool, str | None]:
        """Return whether the current symbol looks like a fresh pump event."""

        trigger_window = None
        if features.ret_15m >= self._config.event_ret_15m_min:
            trigger_window = "15m"
        elif features.ret_1h >= self._config.event_ret_1h_min:
            trigger_window = "1h"
        elif features.ret_4h >= self._config.event_ret_4h_min:
            trigger_window = "4h"

        if trigger_window is None:
            return False, None

        has_stretch = any(
            [
                features.dist_to_vwap_pct >= self._config.event_dist_to_vwap_min,
                features.dist_to_ema20_atr >= self._config.event_dist_to_ema20_atr_min,
                features.vol_zscore_30m >= self._config.vol_zscore_min,
                features.range_atr_ratio >= self._config.range_atr_bonus_level,
            ]
        )
        return has_stretch, trigger_window

    def build_event(
        self,
        symbol: str,
        frame_1m: pd.DataFrame,
        features: SymbolFeatures,
        now: datetime,
    ) -> EventState | None:
        """Create a new event state from current candles when a pump qualifies."""

        qualifies, trigger_window = self.qualifies(features)
        if not qualifies or trigger_window is None:
            return None

        minutes = {"15m": 15, "1h": 60, "4h": 240}[trigger_window]
        closed = closed_1m_rows(frame_1m, features.asof)
        window = closed.tail(minutes if len(closed) >= minutes else len(closed))
        if window.empty:
            return None
        base_row = window.loc[window["low"].idxmin()]
        high_row = window.loc[window["high"].idxmax()]
        base_price = float(base_row["low"])
        event_high = float(high_row["high"])
        event_range_pct = ((event_high / base_price) - 1) * 100 if base_price > 0 else 0.0
        event_high_time = _to_datetime(high_row["timestamp"])
        event_start_time = _to_datetime(base_row["timestamp"])
        event_id = f"{symbol}:{trigger_window}:{int(event_high_time.timestamp())}:{int(event_high * 1_000_000)}"

        return EventState(
            symbol=symbol,
            event_id=event_id,
            state=EventStatus.PUMP_DETECTED,
            event_start_time=event_start_time,
            event_high=event_high,
            event_high_time=event_high_time,
            event_base_price=base_price,
            event_range_pct=event_range_pct,
            event_features_snapshot={
                "trigger_window": trigger_window,
                "detected_at": now.isoformat(),
                **asdict(features),
            },
            trigger_window=trigger_window,
            expires_at=now + timedelta(minutes=self._config.signal_expiry_minutes),
            updated_at=now,
        )


def _to_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value
    return pd.Timestamp(value, tz="UTC").to_pydatetime()
