"""Short-zone builders."""

from __future__ import annotations

from app.config import AppConfig
from app.domain import EventState, ShortZone, SymbolFeatures


class ShortZoneBuilder:
    """Build the working short zone from event state and live ATR."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def build(self, state: EventState, features: SymbolFeatures) -> ShortZone | None:
        """Return the configured short zone for an active event."""

        if state.event_base_price is None or state.event_high is None:
            return None

        if self._config.short_zone_mode == "atr_from_high":
            zone_low = state.event_high - (self._config.short_zone_atr_low_mult * features.atr_14)
            zone_high = state.event_high - (self._config.short_zone_atr_high_mult * features.atr_14)
        else:
            event_range = state.event_high - state.event_base_price
            zone_low = state.event_base_price + (self._config.short_zone_range_low_pct * event_range)
            zone_high = state.event_base_price + (self._config.short_zone_range_high_pct * event_range)

        return ShortZone(low=min(zone_low, zone_high), high=max(zone_low, zone_high), mode=self._config.short_zone_mode)

    @staticmethod
    def contains(zone: ShortZone, price: float) -> bool:
        """Return True when the price sits inside the zone."""

        return zone.low <= price <= zone.high
