"""Finite DB-free Bybit REST/WS parity and reconnect/backfill proof."""



from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import AppConfig, load_config
from app.infra.disk_capacity import (
    derive_reserve_bytes,
    read_capacity,
    require_capacity,
)
from app.infra.request_scheduler import RequestScheduler
from app.infra.runtime_metadata import resolve_code_version
from app.market.bybit_client import BybitClient, BybitResponseError
from app.market.scanner import MarketScanner
from app.market_data.backfill import BackfillStatus, ClosedCandleBackfiller
from app.market_data.bybit_ws import connect_public_linear
from app.market_data.continuity import GapTracker
from app.market_data.hub import MarketDataHub
from app.market_data.models import MarketCandle, MarketTickerSnapshot
from app.market_data.observers import ConnectionOpened
from app.market_data.parity import (
    KlineClassification,
    ParityMonitor,
    compare_closed_kline,
)

try:
    import resource
except ImportError:  # pragma: no cover - Ubuntu is the acceptance host.
    resource = None  # type: ignore[assignment]


ARTIFACT_ESTIMATE_BYTES = 10 * 1024 * 1024


def select_kline_sample(snapshots: Any) -> tuple[str, ...]:
    ranked = sorted(
        snapshots,
        key=lambda item: (-float(item.turnover_24h), item.symbol.upper()),
    )
    if len(ranked) <= 9:
        return tuple(item.symbol.upper() for item in ranked)
    middle = len(ranked) // 2 - 1
    indexes = (0, 1, 2, middle, middle + 1, middle + 2, len(ranked) - 3, len(ranked) - 2, len(ranked) - 1)
    return tuple(ranked[index].symbol.upper() for index in indexes)


def build_report(
    *,
    ticker_probes: int,
    ticker_field_counts: dict[str, int],
    kline_counts: dict[str, int],
    kline_sample: tuple[str, ...],
    kline_per_symbol: dict[str, int],
    reconnect: dict[str, bool],
    backfill: dict[str, Any],
    resources: dict[str, Any],
    universe: dict[str, Any],
    duration_seconds: float,
    rest_rate_limit_errors: int = 0,
    **extra: Any,
) -> dict[str, Any]:
    exact_kline = kline_counts.get(KlineClassification.EXACT_MATCH.value, 0)
    mismatch = kline_counts.get(KlineClassification.FIELD_MISMATCH.value, 0)
    kline_ready = bool(kline_sample) and all(
        kline_per_symbol.get(symbol, 0) >= 3 for symbol in kline_sample
    )
    reconnect_ready = all(reconnect.values())
    backfill_ready = (
        backfill.get("status") == BackfillStatus.SUCCESS.value
        and backfill.get("future_leakage_violations", 0) == 0
    )
    passed = (
        ticker_probes >= 20
        and exact_kline >= len(kline_sample) * 3
        and mismatch == 0
        and kline_ready
        and reconnect_ready
        and backfill_ready
    )
    if passed:
        classification = "PARITY_PASS_BACKFILL_PROBE_ONLY"
    elif mismatch:
        classification = "CLOSED_KLINE_PARITY_MISMATCH"
    elif rest_rate_limit_errors and ticker_probes < 20:
        classification = "REST_RATE_LIMIT_SAFETY_BLOCKED"
    elif not reconnect_ready or not backfill_ready:
        classification = "RECONNECT_BACKFILL_FAILED"
    elif not kline_ready:
        classification = "GAP_DETECTION_NOT_PROVEN"
    else:
        classification = "TICKER_PARITY_UNEXPLAINED_MISMATCH"
    return {
        "phase6": "PASS" if passed else "BLOCKED",
        "classification": classification,
        "rest_remains_canonical": True,
        "ws_remains_shadow": True,
        "production_backfill_enabled": False,
        "ticker_probes": ticker_probes,
        "ticker_field_counts": ticker_field_counts,
        "kline_counts": kline_counts,
        "kline_sample": list(kline_sample),
        "kline_per_symbol": kline_per_symbol,
        "reconnect": reconnect,
        "backfill": backfill,
        "resources": resources,
        "universe": universe,
        "duration_seconds": round(duration_seconds, 3),
        "rate_limit_impact": {
            "classification": (
                "BLOCKING"
                if classification == "REST_RATE_LIMIT_SAFETY_BLOCKED"
                else "ACCEPTABLE" if rest_rate_limit_errors else "NONE"
            ),
            "errors": rest_rate_limit_errors,
        },
        **extra,
    }


def write_evidence(
    report: dict[str, Any],
    *,
    output_dir: Path,
    database_path: Path,
    log_wal_margin_bytes: int,
    minimum_reserve_bytes: int,
) -> Path:
    snapshot = read_capacity(database_path)
    reserve = derive_reserve_bytes(
        snapshot,
        log_wal_margin_bytes=log_wal_margin_bytes,
        minimum_reserve_bytes=minimum_reserve_bytes,
    )
    require_capacity(
        snapshot,
        artifact_estimate_bytes=ARTIFACT_ESTIMATE_BYTES,
        reserve_bytes=reserve,
    )
    report["disk_health"] = {
        "state": "HEALTHY",
        "free_bytes": getattr(snapshot, "free_bytes", None),
        "free_percent": getattr(snapshot, "free_percent", None),
        "free_inodes": getattr(snapshot, "free_inodes", None),
        "reserve_bytes": reserve,
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = output_dir / f"architecture-phase6-rest-ws-parity-{stamp}.json"
    partial = path.with_suffix(path.suffix + ".partial")
    resources = report.setdefault("resources", {})
    artifact_bytes = -1
    for _ in range(4):
        resources["artifact_bytes"] = max(0, artifact_bytes)
        payload = json.dumps(report, default=_json_default, sort_keys=True, indent=2)
        markdown_payload = _markdown_report(report)
        next_size = len((payload + "\n").encode("utf-8")) + len(markdown_payload.encode("utf-8"))
        if next_size == artifact_bytes:
            break
        artifact_bytes = next_size
    resources["artifact_bytes"] = artifact_bytes
    payload = json.dumps(report, default=_json_default, sort_keys=True, indent=2)
    markdown_payload = _markdown_report(report)
    if len(payload.encode("utf-8")) + len(markdown_payload.encode("utf-8")) > ARTIFACT_ESTIMATE_BYTES:
        raise ValueError("Phase 6 evidence exceeds the bounded artifact contract")
    partial.write_text(payload + "\n", encoding="utf-8")
    os.replace(partial, path)
    markdown = path.with_suffix(".md")
    markdown_partial = markdown.with_suffix(".md.partial")
    markdown_partial.write_text(markdown_payload, encoding="utf-8")
    os.replace(markdown_partial, markdown)
    return path


class RecordingConnector:
    def __init__(self) -> None:
        self.connections: list[Any] = []

    async def __call__(self, **kwargs: Any) -> Any:
        connection = await connect_public_linear(**kwargs)
        self.connections.append(connection)
        return connection

    async def close_probe_socket(self) -> bool:
        for connection in reversed(self.connections):
            if not getattr(connection, "closed", False):
                await connection.close()
                return True
        return False


class ProbeCollector:
    def __init__(self, sample: tuple[str, ...]) -> None:
        self.sample = set(sample)
        self.connections: list[ConnectionOpened] = []
        self.closed_queue: asyncio.Queue[MarketCandle] = asyncio.Queue(maxsize=256)
        self.recent: dict[str, deque[MarketCandle]] = defaultdict(lambda: deque(maxlen=3))
        self.dropped_closed = 0

    def observe_connection(self, event: ConnectionOpened) -> None:
        self.connections.append(event)

    def observe_ticker(self, snapshot: MarketTickerSnapshot) -> None:
        return None

    def observe_closed_candle(self, candle: MarketCandle) -> None:
        if candle.symbol not in self.sample:
            return
        self.recent[candle.symbol].append(candle)
        try:
            self.closed_queue.put_nowait(candle)
        except asyncio.QueueFull:
            self.dropped_closed += 1


async def observe(config: AppConfig, *, min_duration: int, max_duration: int) -> dict[str, Any]:
    scheduler = RequestScheduler(
        max_concurrency=config.max_request_concurrency,
        min_delay_ms=config.request_min_delay_ms,
        jitter_min_ms=config.request_jitter_min_ms,
        jitter_max_ms=config.request_jitter_max_ms,
    )
    client = BybitClient(scheduler=scheduler, timeout=config.request_timeout_sec)
    scanner = MarketScanner(client=client, config=config)
    snapshots = await scanner.fetch_market_snapshots()
    telemetry = scanner.last_universe_telemetry
    symbols = tuple(telemetry.eligible_symbols) if telemetry else tuple(item.symbol for item in snapshots)
    sample = select_kline_sample(snapshots)
    parity = ParityMonitor(ws_symbols=symbols)
    gaps = GapTracker()
    collector = ProbeCollector(sample)
    connector = RecordingConnector()
    hub = MarketDataHub(symbols, connector=connector, observers=(parity, gaps, collector))
    ticker_counts: Counter[str] = Counter()
    kline_counts: Counter[str] = Counter()
    kline_per_symbol: Counter[str] = Counter()
    compared_candles: set[tuple[str, datetime]] = set()
    ticker_probes = 0
    http_latencies: list[float] = []
    ws_ages: list[float] = []
    last_coverage: dict[str, Any] = {}
    ticker_windows: list[dict[str, Any]] = []
    kline_evidence: list[dict[str, Any]] = []
    rest_error_counts: Counter[str] = Counter()
    rest_rate_limit_errors = 0
    reconnect_requested = False
    reconnect_baseline = 0
    initial_epochs: dict[int, int] = {}
    synthetic: dict[str, Any] = {"status": "NOT_RUN", "future_leakage_violations": 0}
    cpu_started = time.process_time()
    wall_started = time.monotonic()
    next_ticker = wall_started
    await hub.start()
    try:
        while time.monotonic() - wall_started < max_duration:
            now_mono = time.monotonic()
            if now_mono >= next_ticker:
                started_at = datetime.now(UTC)
                started_mono = time.monotonic()
                parity.begin_ticker_window(
                    request_started_at=started_at,
                    request_started_monotonic=started_mono,
                )
                try:
                    response = await client.fetch_tickers_with_metadata()
                except asyncio.CancelledError:
                    parity.abort_ticker_window()
                    raise
                except Exception as exc:  # noqa: BLE001 - bounded evidence, no payload
                    parity.abort_ticker_window()
                    rest_error_counts[type(exc).__name__] += 1
                    if isinstance(exc, BybitResponseError) and exc.ret_code == 10006:
                        rest_rate_limit_errors += 1
                else:
                    received_mono = time.monotonic()
                    received_at = datetime.now(UTC)
                    result = parity.finish_ticker_window(
                        rest_rows=response.data,
                        server_time=response.server_time,
                        response_received_at=received_at,
                        response_received_monotonic=received_mono,
                    )
                    ticker_probes += 1
                    http_latencies.append(received_mono - started_mono)
                    classifications = Counter(item.classification.value for item in result.fields)
                    ticker_counts.update(classifications)
                    last_coverage = _jsonable(asdict(result.coverage))
                    ticker_windows.append({
                        "request_started_at": result.request_started_at,
                        "request_started_monotonic": result.request_started_monotonic,
                        "response_received_at": result.response_received_at,
                        "response_received_monotonic": result.response_received_monotonic,
                        "rest_server_time": result.server_time,
                        "comparison_anchor": result.comparison_anchor,
                        "anchor_source": result.anchor_source,
                        "http_latency_seconds": received_mono - started_mono,
                        "classifications": dict(classifications),
                        "buffer_high_water": result.high_water_states,
                        "overflowed": result.overflowed,
                    })
                    for symbol in result.coverage.intersection:
                        snapshot = hub.get_snapshot(symbol)
                        if snapshot and snapshot.received_at:
                            ws_ages.append(max(0.0, (received_at - snapshot.received_at).total_seconds()))
                next_ticker += 30.0
            while not collector.closed_queue.empty():
                candle = collector.closed_queue.get_nowait()
                identity = (candle.symbol, candle.open_time)
                if identity in compared_candles:
                    continue
                compared_candles.add(identity)
                result, evidence = await _compare_with_retry(client, candle)
                kline_counts[result.classification.value] += 1
                if len(kline_evidence) < 500:
                    kline_evidence.append(evidence)
                if result.classification is KlineClassification.EXACT_MATCH:
                    kline_per_symbol[candle.symbol] += 1
            subscriptions = hub.get_subscription_state()
            if (
                not reconnect_requested
                and ticker_probes >= 1
                and subscriptions.expected_topics
                and subscriptions.expected_topics == subscriptions.acknowledged_topics
                and collector.connections
            ):
                initial_epochs = {
                    event.connection_epoch.shard_id: event.connection_epoch.generation
                    for event in collector.connections
                }
                reconnect_baseline = hub.get_health().reconnects
                reconnect_requested = await connector.close_probe_socket()
            if synthetic["status"] == "NOT_RUN":
                sequence = _consecutive_three(collector.recent)
                if sequence:
                    synthetic = await _prove_synthetic_backfill(client, sequence)
            elapsed = time.monotonic() - wall_started
            reconnect = _reconnect_status(
                hub, collector, initial_epochs, reconnect_baseline, reconnect_requested
            )
            ready = (
                elapsed >= min_duration
                and ticker_probes >= 20
                and sample
                and all(kline_per_symbol[symbol] >= 3 for symbol in sample)
                and all(reconnect.values())
                and synthetic.get("status") == BackfillStatus.SUCCESS.value
            )
            if ready:
                break
            await asyncio.sleep(0.1)
    finally:
        await hub.stop()
    elapsed = time.monotonic() - wall_started
    reconnect = _reconnect_status(
        hub, collector, initial_epochs, reconnect_baseline, reconnect_requested
    )
    rss = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) if resource else 0
    resources = {
        "cpu_seconds": round(time.process_time() - cpu_started, 3),
        "peak_rss_kib": rss,
        "artifact_bytes": 0,
        "parity_buffer_high_water": parity.max_high_water_states,
        "closed_queue_drops": collector.dropped_closed,
    }
    excluded = dict(telemetry.excluded) if telemetry else {}
    if last_coverage:
        last_coverage["rest_only_explanations"] = {
            symbol: excluded.get(symbol, "RUNTIME_ELIGIBILITY_OR_LISTING_TIMING")
            for symbol in last_coverage.get("rest_only", [])
        }
    connection_epochs = [
        {
            "shard_id": event.connection_epoch.shard_id,
            "generation": event.connection_epoch.generation,
            "topics": len(event.topics),
            "connected_at": event.connected_at,
        }
        for event in collector.connections[:100]
    ]
    return build_report(
        ticker_probes=ticker_probes,
        ticker_field_counts=dict(ticker_counts),
        kline_counts=dict(kline_counts),
        kline_sample=sample,
        kline_per_symbol=dict(kline_per_symbol),
        reconnect=reconnect,
        backfill=synthetic,
        resources=resources,
        universe=last_coverage,
        duration_seconds=elapsed,
        rest_rate_limit_errors=rest_rate_limit_errors,
        http_latency_seconds=_distribution(http_latencies),
        ws_age_seconds=_distribution(ws_ages),
        ticker_windows=ticker_windows[:60],
        kline_evidence=kline_evidence,
        rest_error_counts=dict(rest_error_counts),
        connection_epochs=connection_epochs,
        source={
            "base_sha": "ab835ca64f61a905f7c7d4b7b44b809d9391f2ca",
            "required_runtime_ancestor": "d7d54fa8af070241711d5072cffbf8208f482b5d",
            "phase6_sha": resolve_code_version(),
            "main_source_exception": True,
        },
        gap_health=_jsonable(asdict(gaps.get_health())),
        raw_ws_persistence=False,
        production_db_used=False,
    )


async def _compare_with_retry(client: BybitClient, candle: MarketCandle):
    start_ms = int(candle.open_time.timestamp() * 1000)
    evidence: dict[str, Any] = {
        "symbol": candle.symbol,
        "ws_confirm_received_at": candle.received_at,
        "ws_exchange_timestamp": candle.exchange_timestamp,
        "candle_start": candle.open_time,
        "candle_end": candle.close_time,
        "connection_epoch": candle.connection_epoch,
        "attempts": [],
    }
    for attempt, delay in enumerate((0.0, 1.0, 2.0)):
        if delay:
            await asyncio.sleep(delay)
        request_at = datetime.now(UTC)
        request_mono = time.monotonic()
        try:
            response = await client.fetch_klines_with_metadata(
                candle.symbol, "1", limit=1, start_ms=start_ms, end_ms=int(candle.close_time.timestamp() * 1000)
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - evidence excludes response payload
            received_at = datetime.now(UTC)
            evidence["attempts"].append({
                "request_at": request_at,
                "response_at": received_at,
                "latency_seconds": time.monotonic() - request_mono,
                "error_type": type(exc).__name__,
            })
            result = compare_closed_kline(candle, [], final_attempt=True)
            evidence["classification"] = KlineClassification.DATA_GAP.value
            return result.__class__(
                result.symbol, result.open_time, KlineClassification.DATA_GAP
            ), evidence
        received_at = datetime.now(UTC)
        evidence["attempts"].append({
            "request_at": request_at,
            "response_at": received_at,
            "rest_server_time": response.server_time,
            "latency_seconds": time.monotonic() - request_mono,
        })
        result = compare_closed_kline(candle, response.data, final_attempt=attempt == 2)
        if result.classification is not KlineClassification.REST_NOT_YET_VISIBLE:
            evidence["classification"] = result.classification.value
            evidence["field_differences"] = result.field_differences
            return result, evidence
    evidence["classification"] = result.classification.value
    return result, evidence


def _consecutive_three(recent: dict[str, deque[MarketCandle]]) -> tuple[MarketCandle, MarketCandle, MarketCandle] | None:
    for symbol in sorted(recent):
        values = tuple(recent[symbol])
        if len(values) == 3 and values[1].open_time - values[0].open_time == timedelta(minutes=1) and values[2].open_time - values[1].open_time == timedelta(minutes=1):
            return values
    return None


async def _prove_synthetic_backfill(client: BybitClient, sequence: tuple[MarketCandle, MarketCandle, MarketCandle]) -> dict[str, Any]:
    tracker = GapTracker()
    tracker.observe_closed_candle(sequence[0])
    tracker.observe_closed_candle(sequence[2])
    gap = tracker.active_gaps[0]
    result = await ClosedCandleBackfiller(client=client, tracker=tracker).repair(
        gap, decision_time=datetime.now(UTC)
    )
    if result.status is BackfillStatus.SUCCESS:
        tracker.observe_closed_candle(sequence[1])
    return {
        "status": result.status.value,
        "attempts": result.attempts,
        "candles": len(result.candles),
        "conflicts": tracker.get_health().parity_conflicts,
        "source_confirmations": tracker.get_health().source_confirmations,
        "future_leakage_violations": int(result.status is BackfillStatus.FUTURE_LEAKAGE),
        "requested_start": gap.expected_start,
        "requested_end": gap.observed_next_start - timedelta(milliseconds=1),
        "request_started_at": result.request_started_at,
        "response_received_at": result.response_received_at,
        "retrieved_starts": [candle.open_time for candle in result.candles],
    }


def _reconnect_status(hub: MarketDataHub, collector: ProbeCollector, initial: dict[int, int], baseline: int, requested: bool) -> dict[str, bool]:
    newer = [
        event for event in collector.connections
        if event.connection_epoch.generation > initial.get(event.connection_epoch.shard_id, 0)
    ]
    affected = {
        topic.removeprefix("tickers.")
        for event in newer for topic in event.topics if topic.startswith("tickers.")
    }
    ticker_ready = bool(affected) and all(
        (snapshot := hub.get_snapshot(symbol)) is not None
        and any(snapshot.connection_epoch == event.connection_epoch and f"tickers.{symbol}" in event.topics for event in newer)
        for symbol in affected
    )
    subscriptions = hub.get_subscription_state()
    return {
        "reconnect": requested and hub.get_health().reconnects > baseline,
        "resubscribe": bool(subscriptions.expected_topics) and subscriptions.expected_topics == subscriptions.acknowledged_topics,
        "new_epoch": bool(newer),
        "ticker_reinitialized": ticker_ready,
    }


def _distribution(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {"count": 0}
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": round(ordered[0], 6),
        "median": round(ordered[len(ordered) // 2], 6),
        "p95": round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 6),
        "max": round(ordered[-1], 6),
    }


def _database_path(db_url: str) -> Path:
    prefix = "sqlite:///"
    if not db_url.startswith(prefix):
        raise ValueError("Phase 6 capacity preflight requires a SQLite filesystem path")
    path = Path(db_url.removeprefix(prefix))
    return path if path.is_absolute() else (ROOT / path).resolve()


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, Decimal, Enum)):
        return value.isoformat() if isinstance(value, datetime) else str(value.value if isinstance(value, Enum) else value)
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(type(value).__name__)


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=_json_default))


def _markdown_report(report: dict[str, Any]) -> str:
    return (
        "# Architecture Phase 6 REST/WS Parity Evidence\n\n"
        f"## Result\n\n- Phase 6: `{report.get('phase6')}`\n"
        f"- Classification: `{report.get('classification')}`\n"
        "- REST remains canonical: `YES`\n- WS remains shadow: `YES`\n"
        "- Production automatic backfill: `OFF`\n\n"
        "## Source\n\n```json\n"
        + json.dumps(report.get("source", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n"
        "## Ticker parity\n\n```json\n"
        + json.dumps(report.get("ticker_field_counts", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Closed kline parity\n\n```json\n"
        + json.dumps(report.get("kline_counts", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Gap detection\n\n```json\n"
        + json.dumps(report.get("gap_health", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Reconnect\n\n```json\n"
        + json.dumps(report.get("reconnect", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Backfill\n\n```json\n"
        + json.dumps(report.get("backfill", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Rate-limit impact\n\n```json\n"
        + json.dumps(report.get("rate_limit_impact", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Resource usage\n\n```json\n"
        + json.dumps(report.get("resources", {}), default=_json_default, sort_keys=True, indent=2)
        + "\n```\n\n## Disk health\n\nCapacity preflight passed before publication.\n\n"
        "## Strategy isolation\n\n- Differences: `0` required before PASS.\n\n"
        "## Production\n\n- Pending controlled deployment evidence.\n"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Finite REST/WS parity, gap, reconnect, and backfill proof")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--env", default=".env")
    parser.add_argument("--min-duration-sec", type=int, default=600)
    parser.add_argument("--max-duration-sec", type=int, default=1800)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "docs" / "evidence")
    args = parser.parse_args()
    if args.min_duration_sec < 600 or args.max_duration_sec > 1800 or args.max_duration_sec < args.min_duration_sec:
        parser.error("duration contract is 600 <= min <= max <= 1800 seconds")
    config = load_config(config_path=args.config, env_path=args.env)
    report = asyncio.run(observe(config, min_duration=args.min_duration_sec, max_duration=args.max_duration_sec))
    evidence = write_evidence(
        report,
        output_dir=args.output_dir,
        database_path=_database_path(config.db_url),
        log_wal_margin_bytes=config.disk_log_wal_margin_bytes,
        minimum_reserve_bytes=config.disk_min_safety_reserve_bytes,
    )
    print(json.dumps({"evidence": str(evidence), "phase6": report["phase6"], "classification": report["classification"]}, sort_keys=True))
    return 0 if report["phase6"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
