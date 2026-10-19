"""Small, bounded, side-effect-free metrics for one Lane C scan window.

``RuntimeMetrics`` is an in-process measurement boundary.  It does not know
about a provider, database, research queue, or telemetry service.  The caller
records phase/request observations and later hands :meth:`snapshot` to its
existing research path.  All labels are fixed low-cardinality allowlists;
malformed or unknown labels are folded into ``"unknown"`` so URLs, symbols,
credentials, and request parameters cannot become metric keys.
"""

from __future__ import annotations

import json
import math
import re
import time
from collections import Counter, deque
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any


SCHEMA_VERSION = 1
UNKNOWN_LABEL = "unknown"
MAX_RECENT_LATENCIES = 256

# Public labels are deliberately finite.  A caller may use any of these names
# without creating a new time-series key.
PHASE_NAMES = (
    "universe",
    "frames",
    "liquidity",
    "features_strategy",
    "state_signal_outbox",
    "research_enqueue",
    UNKNOWN_LABEL,
)
REQUEST_ENDPOINT_NAMES = (
    "market.scan",
    "market.frames",
    "market.liquidity",
    "market.derivatives",
    "market.ticker",
    "market.kline",
    "market.orderbook",
    "market.open_interest",
    "market.funding",
    "research.outcomes",
    UNKNOWN_LABEL,
)
GAUGE_NAMES = (
    "data_age_ms",
    "loop_lag_ms",
    "cached_worker_backlog",
)

PHASE_ALLOWLIST = frozenset(PHASE_NAMES)
REQUEST_ENDPOINT_ALLOWLIST = frozenset(REQUEST_ENDPOINT_NAMES)
GAUGE_ALLOWLIST = frozenset(GAUGE_NAMES)
STATUS_NAMES = ("ok", "rate_limited", "error", UNKNOWN_LABEL)
STATUS_ALLOWLIST = frozenset(STATUS_NAMES)

_SAFE_WINDOW_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
_ENDPOINT_ALIASES = {
    "scan": "market.scan",
    "build_scan_snapshot": "market.scan",
    "frames": "market.frames",
    "fetch_symbol_frames": "market.frames",
    "liquidity": "market.liquidity",
    "derivatives": "market.derivatives",
    "ticker": "market.ticker",
    "get_tickers": "market.ticker",
    "kline": "market.kline",
    "get_kline": "market.kline",
    "orderbook": "market.orderbook",
    "get_orderbook": "market.orderbook",
    "open_interest": "market.open_interest",
    "get_open_interest": "market.open_interest",
    "funding": "market.funding",
    "get_funding_rate": "market.funding",
    "outcomes": "research.outcomes",
}
_GAUGE_ALIASES = {
    "market_data_age_ms": "data_age_ms",
    "event_loop_lag_ms": "loop_lag_ms",
    "worker_backlog": "cached_worker_backlog",
    "research_backlog": "cached_worker_backlog",
}


def _finite_nonnegative(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    return number


def _safe_window_id(value: Any) -> str:
    text = str(value or "").strip()
    return text if _SAFE_WINDOW_ID.fullmatch(text) else UNKNOWN_LABEL


def _phase_name(value: Any) -> str:
    text = str(value or "").strip().casefold()
    return text if text in PHASE_ALLOWLIST else UNKNOWN_LABEL


def _endpoint_name(value: Any) -> str:
    text = str(value or "").strip().casefold()
    if text in REQUEST_ENDPOINT_ALLOWLIST:
        return text
    return _ENDPOINT_ALIASES.get(text, UNKNOWN_LABEL)


def _status_name(value: Any) -> str:
    if isinstance(value, bool):
        return UNKNOWN_LABEL
    if isinstance(value, int):
        if 200 <= value < 400:
            return "ok"
        if value == 429:
            return "rate_limited"
        return "error"
    if isinstance(value, str):
        text = value.strip().casefold()
        if text in {"ok", "success", "completed", "2xx", "200"}:
            return "ok"
        if "429" in text or "rate_limit" in text or "ratelimited" in text:
            return "rate_limited"
        if text in {"error", "failed", "failure", "5xx", "4xx"}:
            return "error"
    return UNKNOWN_LABEL


def _p95(values: deque[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * 0.95) - 1)
    return float(ordered[index])


@dataclass(slots=True)
class _PhaseAggregate:
    count: int = 0
    total_ms: float = 0.0
    valid_count: int = 0
    recent: deque[float] = field(default_factory=deque)

    def add(self, elapsed_ms: float | None, *, limit: int) -> None:
        self.count += 1
        if elapsed_ms is None:
            return
        self.total_ms += elapsed_ms
        self.valid_count += 1
        self.recent.append(elapsed_ms)
        while len(self.recent) > limit:
            self.recent.popleft()

    def snapshot(self) -> dict[str, Any]:
        return {
            "count": self.count,
            "total_ms": self.total_ms if self.valid_count else None,
            "p95_ms": _p95(self.recent),
            "recent_latency_ms": list(self.recent),
        }


@dataclass(slots=True)
class _RequestAggregate:
    count: int = 0
    elapsed_total_ms: float = 0.0
    elapsed_valid_count: int = 0
    wait_total_ms: float = 0.0
    wait_valid_count: int = 0
    recent: deque[float] = field(default_factory=deque)
    wait_recent: deque[float] = field(default_factory=deque)
    statuses: Counter[str] = field(default_factory=Counter)

    def add(
        self,
        elapsed_ms: float | None,
        limiter_wait_ms: float | None,
        status: str,
        *,
        limit: int,
    ) -> None:
        self.count += 1
        self.statuses[status] += 1
        if elapsed_ms is not None:
            self.elapsed_total_ms += elapsed_ms
            self.elapsed_valid_count += 1
            self.recent.append(elapsed_ms)
            while len(self.recent) > limit:
                self.recent.popleft()
        if limiter_wait_ms is not None:
            self.wait_total_ms += limiter_wait_ms
            self.wait_valid_count += 1
            self.wait_recent.append(limiter_wait_ms)
            while len(self.wait_recent) > limit:
                self.wait_recent.popleft()

    def snapshot(self) -> dict[str, Any]:
        elapsed = {
            "count": self.elapsed_valid_count,
            "total": self.elapsed_total_ms if self.elapsed_valid_count else None,
            "p95": _p95(self.recent),
        }
        wait = {
            "count": self.wait_valid_count,
            "total": self.wait_total_ms if self.wait_valid_count else None,
            "p95": _p95(self.wait_recent),
        }
        # Keep the names explicit and stable.  The nested values make it clear
        # that missing measurements are UNKNOWN rather than zero.
        return {
            "count": self.count,
            "elapsed_ms": elapsed,
            "limiter_wait_ms": wait,
            "elapsed_total_ms": elapsed["total"],
            "elapsed_p95_ms": elapsed["p95"],
            "limiter_wait_total_ms": wait["total"],
            "limiter_wait_p95_ms": wait["p95"],
            "statuses": {name: self.statuses.get(name, 0) for name in STATUS_NAMES},
            "recent_latency_ms": list(self.recent),
        }


class RuntimeMetrics:
    """Bounded per-window phase, request, and readiness metrics."""

    def __init__(
        self,
        *,
        window_id: Any = None,
        max_recent_latencies: int = MAX_RECENT_LATENCIES,
    ) -> None:
        if int(max_recent_latencies) < 1:
            raise ValueError("max_recent_latencies must be positive")
        self.max_recent_latencies = min(MAX_RECENT_LATENCIES, int(max_recent_latencies))
        self.window_id = _safe_window_id(window_id)
        self._phases = {
            name: _PhaseAggregate(recent=deque()) for name in PHASE_NAMES
        }
        self._requests = {
            name: _RequestAggregate(recent=deque(), wait_recent=deque())
            for name in REQUEST_ENDPOINT_NAMES
        }
        self._gauges: dict[str, float | int | None] = {
            name: None for name in GAUGE_NAMES
        }

    def begin_window(self, window_id: Any = None) -> None:
        """Reset bounded observations for the next window."""

        self.window_id = _safe_window_id(window_id)
        self._phases = {
            name: _PhaseAggregate(recent=deque()) for name in PHASE_NAMES
        }
        self._requests = {
            name: _RequestAggregate(recent=deque())
            for name in REQUEST_ENDPOINT_NAMES
        }
        self._gauges = {name: None for name in GAUGE_NAMES}

    reset = begin_window

    @contextmanager
    def window(self, window_id: Any = None) -> Iterator["RuntimeMetrics"]:
        self.begin_window(window_id)
        yield self

    @contextmanager
    def phase(self, name: Any) -> Iterator[None]:
        """Measure one bounded phase in milliseconds, including failures."""

        label = _phase_name(name)
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = _finite_nonnegative((time.perf_counter() - started) * 1000.0)
            self._phases[label].add(elapsed_ms, limit=self.max_recent_latencies)

    def request(
        self,
        endpoint_name: Any,
        elapsed_ms: Any,
        limiter_wait_ms: Any = None,
        status: Any = None,
    ) -> str:
        """Record one request without retaining URL, params, or symbol data."""

        label = _endpoint_name(endpoint_name)
        self._requests[label].add(
            _finite_nonnegative(elapsed_ms),
            _finite_nonnegative(limiter_wait_ms),
            _status_name(status),
            limit=self.max_recent_latencies,
        )
        return label

    def set_gauge(self, name: Any, value: Any) -> bool:
        """Set one explicitly allowlisted gauge; return False for unknown names."""

        label = str(name or "").strip().casefold()
        label = _GAUGE_ALIASES.get(label, label)
        if label not in GAUGE_ALLOWLIST:
            return False
        if label == "cached_worker_backlog":
            numeric = _finite_nonnegative(value)
            self._gauges[label] = int(numeric) if numeric is not None else None
        else:
            self._gauges[label] = _finite_nonnegative(value)
        return True

    gauge = set_gauge

    def snapshot(self) -> dict[str, Any]:
        """Return a bounded JSON-compatible snapshot for the current window."""

        return {
            "schema_version": SCHEMA_VERSION,
            "window_id": self.window_id,
            "phases": {name: metric.snapshot() for name, metric in self._phases.items()},
            "requests": {
                name: metric.snapshot() for name, metric in self._requests.items()
            },
            "gauges": dict(self._gauges),
        }

    safe_snapshot = snapshot

    def json_snapshot(self) -> str:
        return json.dumps(
            self.snapshot(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    serialize = json_snapshot


# Compatibility names for callers that prefer the window-oriented term.
WindowMetrics = RuntimeMetrics
MetricsCollector = RuntimeMetrics


__all__ = [
    "GAUGE_ALLOWLIST",
    "GAUGE_NAMES",
    "MAX_RECENT_LATENCIES",
    "MetricsCollector",
    "PHASE_ALLOWLIST",
    "PHASE_NAMES",
    "REQUEST_ENDPOINT_ALLOWLIST",
    "REQUEST_ENDPOINT_NAMES",
    "RuntimeMetrics",
    "STATUS_NAMES",
    "UNKNOWN_LABEL",
    "WindowMetrics",
]
