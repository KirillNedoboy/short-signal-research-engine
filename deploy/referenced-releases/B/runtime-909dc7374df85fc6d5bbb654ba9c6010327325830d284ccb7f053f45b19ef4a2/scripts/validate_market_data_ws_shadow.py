"""Finite, read-only Ubuntu validation for the public WS shadow."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import AppConfig, load_config
from app.infra.request_scheduler import RequestScheduler
from app.market.bybit_client import BybitClient
from app.market.scanner import MarketScanner
from app.market_data.hub import MarketDataHub
from app.market_data.models import MarketDataHealth, SubscriptionState

try:
    import resource
except ImportError:  # pragma: no cover - Windows validation uses task-manager metrics.
    resource = None  # type: ignore[assignment]


def build_report(
    *,
    symbols: tuple[str, ...],
    health: MarketDataHealth,
    subscriptions: SubscriptionState,
    duration_sec: int,
    cpu_seconds: float,
    rss_kib: int,
) -> dict[str, Any]:
    return {
        "duration_sec": duration_sec,
        "state": health.state.value,
        "symbols": len(symbols),
        "expected_topics": len(subscriptions.expected_topics),
        "active_topics": len(subscriptions.acknowledged_topics),
        "failed_topics": len(subscriptions.failed_topics),
        "pending_topics": len(subscriptions.pending_topics),
        "messages": getattr(health, "messages_received", 0),
        "parse_errors": health.parse_errors,
        "disconnects": getattr(health, "disconnects", 0),
        "reconnects": health.reconnects,
        "stale_symbols": getattr(health, "stale_symbols", 0),
        "cpu_seconds": round(cpu_seconds, 3),
        "rss_kib": rss_kib,
    }


async def observe(duration_sec: int) -> dict[str, Any]:
    config: AppConfig = load_config()
    scheduler = RequestScheduler(
        max_concurrency=config.max_request_concurrency,
        min_delay_ms=config.request_min_delay_ms,
        jitter_min_ms=config.request_jitter_min_ms,
        jitter_max_ms=config.request_jitter_max_ms,
    )
    client = BybitClient(scheduler=scheduler, timeout=config.request_timeout_sec)
    scanner = MarketScanner(client=client, config=config)
    await scanner.fetch_market_snapshots()
    telemetry = scanner.last_universe_telemetry
    symbols = tuple(telemetry.eligible_symbols) if telemetry is not None else ()

    hub = MarketDataHub(symbols)
    cpu_start = time.process_time()
    wall_deadline = time.monotonic() + duration_sec
    await hub.start()
    try:
        while time.monotonic() < wall_deadline:
            await asyncio.sleep(min(1.0, max(0.0, wall_deadline - time.monotonic())))
    finally:
        await hub.stop()
    rss_kib = (
        int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        if resource is not None
        else 0
    )
    return build_report(
        symbols=symbols,
        health=hub.get_health(),
        subscriptions=hub.get_subscription_state(),
        duration_sec=duration_sec,
        cpu_seconds=time.process_time() - cpu_start,
        rss_kib=rss_kib,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-sec", type=int, default=120)
    args = parser.parse_args()
    if args.duration_sec <= 0:
        parser.error("--duration-sec must be positive")
    print(json.dumps(asyncio.run(observe(args.duration_sec)), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
