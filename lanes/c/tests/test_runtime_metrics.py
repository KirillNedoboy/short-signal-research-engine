from __future__ import annotations

import json

from app.runtime.metrics import (
    GAUGE_NAMES,
    MAX_RECENT_LATENCIES,
    PHASE_NAMES,
    REQUEST_ENDPOINT_NAMES,
    RuntimeMetrics,
)


def test_phase_request_snapshot_is_safe_and_keeps_unknown_as_none() -> None:
    metrics = RuntimeMetrics(window_id="window-1", max_recent_latencies=3)

    with metrics.phase("universe"):
        pass
    with metrics.phase("malformed/secret?token=do-not-copy"):
        pass
    metrics.request(
        "https://bybit.example/private?api_key=secret&symbol=BTCUSDT",
        elapsed_ms="bad",
        limiter_wait_ms="bad",
        status={"secret": "value"},
    )
    metrics.request("market.frames", elapsed_ms=12.5, limiter_wait_ms=None, status=200)
    metrics.set_gauge("data_age_ms", None)
    metrics.set_gauge("BTCUSDT", 42)

    snapshot = metrics.snapshot()
    encoded = metrics.json_snapshot()
    assert json.loads(encoded) == snapshot
    assert set(snapshot["phases"]) == set(PHASE_NAMES)
    assert snapshot["phases"]["universe"]["count"] == 1
    assert snapshot["phases"]["unknown"]["count"] == 1
    assert snapshot["requests"]["unknown"]["count"] == 1
    assert snapshot["requests"]["unknown"]["elapsed_ms"]["total"] is None
    assert snapshot["requests"]["unknown"]["limiter_wait_ms"]["total"] is None
    assert snapshot["requests"]["market.frames"]["statuses"]["ok"] == 1
    assert snapshot["gauges"]["data_age_ms"] is None
    assert "secret" not in encoded
    assert "BTCUSDT" not in encoded
    assert set(snapshot["requests"]) == set(REQUEST_ENDPOINT_NAMES)
    assert set(snapshot["gauges"]) == set(GAUGE_NAMES)


def test_recent_latency_and_endpoint_cardinality_are_bounded() -> None:
    metrics = RuntimeMetrics(window_id="bounded", max_recent_latencies=3)
    assert (
        RuntimeMetrics(max_recent_latencies=MAX_RECENT_LATENCIES * 10).max_recent_latencies
        == MAX_RECENT_LATENCIES
    )
    for value in range(20):
        metrics.request("market.frames", elapsed_ms=value + 1, status=200)
    for value in range(20):
        metrics.request(f"symbol-{value}", elapsed_ms=100, status=500)

    snapshot = metrics.snapshot()
    frames = snapshot["requests"]["market.frames"]
    assert frames["count"] == 20
    assert frames["recent_latency_ms"] == [18.0, 19.0, 20.0]
    assert frames["elapsed_ms"]["p95"] == 20.0
    assert snapshot["requests"]["unknown"]["count"] == 20
    assert len(frames["recent_latency_ms"]) <= MAX_RECENT_LATENCIES
    assert len(snapshot["requests"]) == len(REQUEST_ENDPOINT_NAMES)
