"""The sole native asyncio WebSocket transport for Bybit public linear data."""

from __future__ import annotations

from typing import Any

BYBIT_PUBLIC_LINEAR_ENDPOINT = "wss://stream.bybit.com/v5/public/linear"
DEFAULT_MAX_QUEUE = 128


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

    return await connect(
        url,
        ping_interval=ping_interval,
        ping_timeout=ping_timeout,
        max_queue=max_queue,
        **kwargs,
    )
