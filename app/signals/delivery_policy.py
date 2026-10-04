"""Delivery policy for actionable live signal strategies."""

from __future__ import annotations

from app.config import AppConfig
from app.domain import SignalDecision, SignalType


def live_delivery_enabled(decision: SignalDecision, config: AppConfig) -> bool:
    """Return whether a decision may enter the live signal delivery path."""

    if decision.signal_type == SignalType.WATCH:
        return config.send_watch_to_telegram
    if not decision.actionable:
        return False
    if decision.strategy_type == "BASELINE_PULLBACK":
        return config.baseline_live_delivery_enabled
    if decision.strategy_type == "TRAPPED_LONGS_REVERSAL":
        return getattr(config, "trapped_longs_live_delivery_enabled", False)
    if decision.strategy_type != "CLIMAX_EXHAUSTION":
        return False
    if decision.strategy_subtype == "VOLUME_CLIMAX_UNWIND":
        return config.volume_climax_live_delivery_enabled
    if decision.strategy_subtype == "LOW_VOLUME_EXTENSION_FAILURE":
        return config.low_volume_live_delivery_enabled
    return False
