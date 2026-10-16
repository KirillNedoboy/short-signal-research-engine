"""Task-local cycle component timing and event-loop lag evidence.

Component spans are inclusive and intentionally overlap; ``cycle_total_ms`` is
the only authoritative wall-clock figure.  ``timing_semantics`` pins this
contract so operators never sum the component metrics.
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Iterator, Mapping

from app.infra.atomic_json import atomic_write_json

TIMING_SEMANTICS = "component_inclusive_v1"
EVIDENCE_SCHEMA_VERSION = 1
MAX_EVIDENCE_BYTES = 64 * 1024

COMPONENTS = ("market_fetch", "strategy", "storage", "outcomes", "research")
CURRENT_METRIC_KEYS = ("cycle_total_ms", "cycle_overrun_ms", "event_loop_lag_ms")
METRIC_KEYS = tuple(f"{name}_ms" for name in COMPONENTS) + CURRENT_METRIC_KEYS


_current_timing: ContextVar["CycleTiming | None"] = ContextVar(
    "current_cycle_timing", default=None
)


def attach_cycle_timing(timing: "CycleTiming | None") -> Token:
    """Bind (or detach with ``None``) the cycle timing for the current task."""

    return _current_timing.set(timing)


def detach_cycle_timing(token: Token) -> None:
    _current_timing.reset(token)


def current_cycle_timing() -> "CycleTiming | None":
    return _current_timing.get()


def _accumulate(component: str, started_ns: int) -> None:
    timing = _current_timing.get()
    if timing is None:
        return
    timing.components_ns[component] += time.perf_counter_ns() - started_ns


@asynccontextmanager
async def measure(component: str) -> AsyncIterator[None]:
    """Accumulate one async component span when this task carries live timing."""

    if component not in COMPONENTS:
        raise KeyError(f"unknown timing component: {component}")
    started_ns = time.perf_counter_ns()
    try:
        yield
    finally:
        _accumulate(component, started_ns)


@contextmanager
def measure_sync(component: str) -> Iterator[None]:
    """Accumulate one synchronous component span for the live cycle only."""

    if component not in COMPONENTS:
        raise KeyError(f"unknown timing component: {component}")
    started_ns = time.perf_counter_ns()
    try:
        yield
    finally:
        _accumulate(component, started_ns)


def storage_scope() -> "contextmanager[None]":
    """Attribute a synchronous Database.session() scope to the live cycle."""

    return measure_sync("storage")


@dataclass(slots=True)
class CycleTiming:
    """One cycle's timing accumulator; finalized exactly once."""

    cycle_sequence: int
    started_at: datetime
    started_ns: int
    components_ns: dict[str, int] = field(
        default_factory=lambda: {name: 0 for name in COMPONENTS}
    )
    status: str = "RUNNING"
    completed_at: datetime | None = None
    completed_ns: int = 0
    cycle_overrun_ms: float = 0.0
    skipped_ticks: int = 0
    event_loop_lag_ms: float = 0.0

    @classmethod
    def begin(cls, *, cycle_sequence: int) -> "CycleTiming":
        return cls(
            cycle_sequence=cycle_sequence,
            started_at=datetime.now(timezone.utc),
            started_ns=time.perf_counter_ns(),
        )

    def finish(
        self,
        *,
        status: str,
        cycle_overrun_ms: float = 0.0,
        skipped_ticks: int = 0,
        event_loop_lag_ms: float = 0.0,
    ) -> None:
        if self.completed_ns:
            return
        self.completed_ns = time.perf_counter_ns()
        self.completed_at = datetime.now(timezone.utc)
        self.status = status
        self.cycle_overrun_ms = cycle_overrun_ms
        self.skipped_ticks = skipped_ticks
        self.event_loop_lag_ms = event_loop_lag_ms

    def metrics_ms(self) -> dict[str, float]:
        completed_ns = self.completed_ns or time.perf_counter_ns()
        metrics = {
            f"{name}_ms": self.components_ns[name] / 1_000_000 for name in COMPONENTS
        }
        metrics["cycle_total_ms"] = (completed_ns - self.started_ns) / 1_000_000
        metrics["cycle_overrun_ms"] = self.cycle_overrun_ms
        metrics["event_loop_lag_ms"] = self.event_loop_lag_ms
        return metrics


class EventLoopLagMonitor:
    """One-second probe reporting the worst scheduling lag in a cycle window."""

    def __init__(self, *, probe_interval_sec: float = 1.0) -> None:
        self._interval = probe_interval_sec
        self._max_lag_ms = 0.0
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="event-loop-lag-monitor")

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_at = loop.time() + self._interval
        while True:
            await asyncio.sleep(max(0.0, next_at - loop.time()))
            now = loop.time()
            self._max_lag_ms = max(self._max_lag_ms, (now - next_at) * 1000)
            next_at += self._interval

    def begin_window(self) -> None:
        self._max_lag_ms = 0.0

    def latest_lag_ms(self) -> float:
        return self._max_lag_ms


def build_evidence_document(
    timing: CycleTiming,
    *,
    runtime_instance_id: str,
    code_version: str,
) -> dict[str, Any]:
    """Assemble the bounded overwrite-in-place timing evidence payload."""

    metrics = timing.metrics_ms()
    if any(
        not (value == value and 0.0 <= value < float("inf"))
        for value in metrics.values()
    ):
        raise ValueError("timing metrics must be finite and non-negative")
    return {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "timing_semantics": TIMING_SEMANTICS,
        "runtime_instance_id": runtime_instance_id,
        "code_version": code_version,
        "cycle_sequence": timing.cycle_sequence,
        "cycle_started_at": timing.started_at.isoformat(),
        "cycle_completed_at": (
            timing.completed_at or datetime.now(timezone.utc)
        ).isoformat(),
        "status": timing.status,
        "skipped_ticks": timing.skipped_ticks,
        "metrics_ms": metrics,
    }


def publish_evidence(path: str | Path, document: Mapping[str, Any]) -> Path:
    """Atomically overwrite the single bounded evidence document."""

    encoded = json.dumps(
        document, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    if len(encoded) > MAX_EVIDENCE_BYTES:
        raise ValueError("timing evidence exceeds the 64 KiB budget")
    return atomic_write_json(path, document)
