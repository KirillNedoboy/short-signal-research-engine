"""Read-only event-driven shadow probe against Bybit public market data.

Exact CLI::

    python scripts/run_event_driven_shadow.py --duration-sec 600 \
        --target-symbols 100 --output artifacts/event-driven-shadow.json

The probe never instantiates the production repository, notifier, strategies,
or delivery.  It measures what the event-driven runtime needs from the feed:
subscription acknowledgements, frame completeness, REST fallback volume,
closed-candle parity, gaps, stale frames, queue latency, CPU/RSS, and
disconnect/reconnect recovery.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import resource
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import AppConfig  # noqa: E402
from app.infra.request_scheduler import RequestPriority, request_class  # noqa: E402
from app.market.bybit_client import BybitClient  # noqa: E402
from app.market_data.continuity import GapTracker  # noqa: E402
from app.market_data.hub import MarketDataHub  # noqa: E402
from app.market_data.planning import plan_topics  # noqa: E402
from app.market_data.store import CanonicalMarketStore  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-sec", type=int, default=600)
    parser.add_argument("--target-symbols", type=int, default=100)
    parser.add_argument("--output", required=True)
    parser.add_argument("--min-turnover", type=float, default=5_000_000)
    return parser


def _rss_mib() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Linux reports KiB; macOS bytes.
    return usage / 1024.0 if sys.platform.startswith("linux") else usage / (1024**2)


def _cpu_percent(process_cpu_ms: float, wall_ms: float) -> float:
    return round(100.0 * process_cpu_ms / wall_ms, 2) if wall_ms else 0.0


def _stale_reason_counts(store) -> dict[str, int]:  # noqa: ANN001
    counts: dict[str, int] = {}
    now = datetime.now(UTC)
    for symbol in store.symbols():
        frame = store.peek_snapshot(symbol, now)
        reason = "ACTIONABLE" if frame is not None and frame.actionable else (
            frame.stale_reason if frame is not None else "NO_FRAME"
        )
        counts[reason] = counts.get(reason, 0) + 1
    return counts


async def probe(
    *, duration_sec: int, target_symbols: int, min_turnover: float
) -> dict[str, Any]:
    request_class.set(RequestPriority.RESEARCH)
    config = AppConfig()  # defaults only; no production config or credentials
    scheduler_limit = config.max_request_concurrency
    started = time.monotonic()
    start_cpu_ms = (
        resource.getrusage(resource.RUSAGE_SELF).ru_utime + resource.getrusage(resource.RUSAGE_SELF).ru_stime
    ) * 1000.0

    from app.infra.request_scheduler import RequestScheduler

    scheduler = RequestScheduler(max_concurrency=scheduler_limit)
    client = BybitClient(scheduler=scheduler, timeout=config.request_timeout_sec)
    continuity = GapTracker()
    hub = MarketDataHub(observers=(continuity,))
    store = CanonicalMarketStore(
        ring_size=config.deep_scan_kline_limit,
        stale_after_sec=90,
        continuity=continuity,
        hub=hub,
    )
    # The store observes through the same hub; attach it too.
    hub.add_observer(store)

    print("fetching ticker universe (REST, P1)...", flush=True)
    tickers = await client.fetch_tickers()
    # Production shortlist semantics: rank eligible perps by 24h turnover and
    # take the top-N, so thin symbols that legitimately stop streaming
    # one-minute candles cannot masquerade as store defects.
    ranked = sorted(
        (
            (float(ticker.get("turnover24h") or 0), str(ticker["symbol"]))
            for ticker in tickers
            if float(ticker.get("turnover24h") or 0) >= min_turnover
            and str(ticker.get("symbol", "")).endswith("USDT")
        ),
        reverse=True,
    )
    symbols = tuple(sorted({symbol for _, symbol in ranked[:target_symbols]}))
    if not symbols:
        raise RuntimeError("no eligible symbols found; aborting probe")
    print(f"target symbols: {len(symbols)}", flush=True)

    fallback_events: list[float] = []
    original_fetch_klines = client.fetch_klines

    async def counted_fetch_klines(*args, **kwargs):  # noqa: ANN002, ANN003
        # Decision-time REST fallback carries a window range (start_ms);
        # plain bounded fetches are probe-internal parity samples.
        if kwargs.get("start_ms") is not None:
            fallback_events.append(time.monotonic())
        return await original_fetch_klines(*args, **kwargs)

    client.fetch_klines = counted_fetch_klines  # type: ignore[method-assign]
    warm_started = time.monotonic()
    warm = await store.warm_symbols(client, symbols, priority=RequestPriority.RESEARCH)
    warm_seconds = time.monotonic() - warm_started
    # Warmup seeding is bounded initial load, not a decision-time fallback;
    # only count fallbacks from the observation phase onward.
    fallback_events.clear()
    client.fetch_klines = original_fetch_klines  # type: ignore[method-assign]

    hub.update_universe(symbols)
    await hub.start()
    subscription_deadline = time.monotonic() + 90
    acked: set[str] = set()
    while time.monotonic() < subscription_deadline:
        state = hub.get_subscription_state()
        acked = set(state.acknowledged_topics)
        if len(acked) >= len(state.expected_topics):
            break
        await asyncio.sleep(1)

    client.fetch_klines = counted_fetch_klines  # type: ignore[method-assign]
    parity_mismatches: list[dict[str, Any]] = []
    future_leaks: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    end_at = started + duration_sec
    reconnects = 0
    prev_reconnects = hub.get_health().reconnects
    try:
        while time.monotonic() < end_at:
            now = datetime.now(UTC)
            health = hub.get_health()
            reconnects += max(0, health.reconnects - prev_reconnects)
            prev_reconnects = health.reconnects
            counts = store.completeness_counts()
            state = hub.get_subscription_state()
            queue_probe = time.monotonic()
            await asyncio.sleep(0)  # one loop turn measures scheduler latency
            queue_probe_ms = (time.monotonic() - queue_probe) * 1000.0
            samples.append(
                {
                    "t": round(time.monotonic() - started, 1),
                    "hub_state": str(health.state),
                    "acked": len(state.acknowledged_topics),
                    "expected": len(state.expected_topics),
                    "complete": counts["complete"],
                    "symbols": counts["symbols"],
                    "queue_probe_ms": round(queue_probe_ms, 3),
                }
            )
            # Parity: compare the newest closed candle against REST (sampled).
            if counts["complete"] and int(time.monotonic() - started) % 30 == 5:
                symbol = store.symbols()[0]
                candle = hub.get_last_closed_candle(symbol)
                if candle is not None:
                    try:
                        rows = await original_fetch_klines(symbol, "1", limit=2)
                        for row in rows or []:
                            rest_open = int(row[0])
                            if rest_open == int(candle.open_time.timestamp() * 1000):
                                if any(
                                    str(value) != str(field)
                                    for value, field in zip(
                                        row[1:5],
                                        (candle.open, candle.high, candle.low, candle.close),
                                        strict=True,
                                    )
                                ):
                                    parity_mismatches.append(
                                        {
                                            "symbol": symbol,
                                            "open_time": candle.open_time.isoformat(),
                                        }
                                    )
                    except Exception as exc:  # noqa: BLE001
                        print(f"parity sample skipped: {type(exc).__name__}", flush=True)
            # Future-candle leakage check across the whole store.
            for symbol in store.symbols():
                frame = store.peek_snapshot(symbol, now)
                if frame is None:
                    continue
                timestamps = list(frame.frame_1m["timestamp"]) if not frame.frame_1m.empty else []
                if any(pd_ts > now for pd_ts in timestamps):
                    future_leaks.append({"symbol": symbol, "at": now.isoformat()})
            await asyncio.sleep(5)
    finally:
        await hub.stop()

    end_cpu_ms = (
        resource.getrusage(resource.RUSAGE_SELF).ru_utime + resource.getrusage(resource.RUSAGE_SELF).ru_stime
    ) * 1000.0
    wall_ms = (time.monotonic() - started) * 1000.0
    queue_values = sorted(item["queue_probe_ms"] for item in samples)
    p95_queue = queue_values[int(0.95 * (len(queue_values) - 1))] if queue_values else 0.0
    final_counts = store.completeness_counts()
    return {
        "schema_version": 1,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "duration_sec": duration_sec,
        "target_symbols": len(symbols),
        "warmup": {"seconds": round(warm_seconds, 1), **warm},
        "subscriptions": {
            "expected_topics": len(hub.get_subscription_state().expected_topics),
            "acknowledged_topics": len(acked),
            "planner_connections": len(plan_topics(symbols).connections),
        },
        "frames": {
            "complete_final": final_counts["complete"],
            "symbols": final_counts["symbols"],
            "stale_reason_final": _stale_reason_counts(store),
            "store_counters": dict(store.counters),
            "hub_parse_errors": hub.get_health().parse_errors,
            "continuity_active_gaps": continuity.get_health().active_gaps,
        },
        "rest_fallback_count": len(fallback_events),
        "parity_mismatches": parity_mismatches,
        "future_candle_leaks": future_leaks,
        "queue_latency_ms_p95": round(p95_queue, 3),
        "cpu_percent_mean": _cpu_percent(end_cpu_ms - start_cpu_ms, wall_ms),
        "rss_mib_peak": round(_rss_mib(), 1),
        "disconnects": reconnects,
        "hub_final_state": str(hub.get_health().state),
        "samples": samples[-200:],
        "no_production_access": {
            "repository_instantiated": False,
            "notifier_instantiated": False,
            "delivery_started": False,
            "sqlite_opened": False,
        },
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.duration_sec < 60 or args.target_symbols < 1:
        print("error: --duration-sec must be >= 60 and --target-symbols >= 1", file=sys.stderr)
        return 2
    result = asyncio.run(
        probe(
            duration_sec=args.duration_sec,
            target_symbols=args.target_symbols,
            min_turnover=args.min_turnover,
        )
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    print(
        f"complete: {result['frames']['complete_final']}/{result['target_symbols']} symbols,"
        f" fallbacks={result['rest_fallback_count']},"
        f" parity_mismatches={len(result['parity_mismatches'])},"
        f" writes={output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
