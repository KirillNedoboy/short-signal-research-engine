"""The sole native asyncio WebSocket transport for Bybit public linear data."""

from __future__ import annotations

import os
from typing import Any

BYBIT_PUBLIC_LINEAR_ENDPOINT = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_MAX_QUEUE = 128


def _record_shadow_rate_limit(error: object) -> None:
    """Record a WS rate-limit before the Hub reconnect loop absorbs it."""

    if os.environ.get("SHADOW_MODE", "").strip().casefold() not in {
        "1", "true", "yes", "on"
    }:
        return
    try:
        from app.runtime.shadow import get_shadow_control

        get_shadow_control().record_rate_limit(error, transport="ws")
    except Exception:
        # Keep the transport exception for the Hub's bounded reconnect policy;
        # direct control recording is best effort at this outer boundary.
        return


async def connect_public_linear(
    *,
    url: str = BYBIT_PUBLIC_LINEAR_ENDPOINT,
    max_queue: int = DEFAULT_MAX_QUEUE,
    ping_interval: float | None = None,
    ping_timeout: float | None = None,
    **kwargs: Any,
) -> Any:
    """Open one public connection with library auto-ping disabled.

    Importing lazily keeps deterministic unit tests independent of the optional
    runtime dependency while production uses the current ``websockets`` API.
    """
    from websockets.asyncio.client import connect

    try:
        return await connect(
            url,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            max_queue=max_queue,
            **kwargs,
        )
    except Exception as exc:
        code = getattr(exc, "status_code", None)
        if code in (10006, 429) or "10006" in str(exc) or "429" in str(exc):
            _record_shadow_rate_limit(exc)
        raise
