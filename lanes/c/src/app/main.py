"""Lane C event-driven composition root.

This module intentionally does not expose or call the historical sequential
``run_cycle``.  It schedules bounded P0/P1 tasks, serializes each symbol, and
hands durable outbox IDs to a separate wake worker.  The copied C strategy
pipeline remains the owner of formulas and state transitions.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from copy import deepcopy
from pathlib import Path
from typing import Any

from app.config import AppConfig
from app.events.state_store import EventStateStore
from app.infra.request_scheduler import RequestScheduler
from app.infra.runtime_metadata import resolve_code_version
from app.market.bybit_client import BybitClient
from app.market.scanner import MarketScanner
from app.market_data.provider import CanonicalMarketDataProvider
from app.notifications.telegram import TelegramNotifier
from app.notifications.throttling import ErrorThrottler
from app.observability.outcome_scheduler import ShadowOutcomeScheduler
from app.outcomes.tracker import OutcomeTracker
from app.runtime.coordinator import PerSymbolCoordinator
from app.runtime.delivery import DurableDeliveryWorker
from app.runtime.fast_monitor import BoundedFastMonitor, CandidatePool, SnapshotVersion
from app.runtime.market_runtime import LaneCMarketRuntime
from app.runtime.metrics import RuntimeMetrics
from app.runtime.outcome_worker import OutcomeWorker
from app.runtime.outcomes import OutcomeScheduler
from app.runtime.persistence import SignalPersistence
from app.runtime.pipeline import LaneCStrategyPipeline
from app.runtime.research_queue import ResearchQueue
from app.replay.config import strategy_config_fingerprint as _c_strategy_config_fingerprint
from app.runtime.scan import ScanPriority, ScanTask, ScanWindowCoordinator, TerminalStatus
from app.runtime.scheduling import next_scan_deadline
from app.runtime.shadow import get_shadow_control
from app.runtime.shadow_comparison import ShadowCaptureRecorder
from app.signals.delivery_policy import live_delivery_enabled
from app.storage.db import Database
from app.storage.repository import BotRepository

LOGGER = logging.getLogger(__name__)

# A minute boundary is opened after a short settling period so the newly
# closed candle and its WS/store snapshot have time to arrive.  The deadline
# values remain owned by ScanWindowCoordinator; this constant only controls
# the anchored run-forever clock.
SCAN_WINDOW_GRACE_SEC = 30.0
SUCCESSFUL_EVALUATION_STATUSES = frozenset(
    {TerminalStatus.SCANNED_OK.value, TerminalStatus.NO_SETUP.value}
)


def strategy_config_fingerprint(config: AppConfig) -> str:
    """Use the established C allowlist so runtime flags do not alter identity."""

    return _c_strategy_config_fingerprint(config)


def successful_evaluation_count(statuses: Any) -> int:
    """Count evaluations that reached a valid C decision boundary.

    ``STALE_DATA``, missing data, rate limits, deadline misses, and scanner
    errors are terminal accounting outcomes, but they are not evaluations.
    ``NO_SETUP`` is successful because the strategy evaluated the input and
    found no actionable setup.
    """

    return sum(
        1
        for status in statuses
        if str(getattr(status, "value", status)) in SUCCESSFUL_EVALUATION_STATUSES
    )


@dataclass(frozen=True, slots=True)
class WindowReport:
    window_id: str
    scheduled: int
    statuses: dict[str, str]
    duration_ms: float
    p0_deadline_misses: int = 0
    coverage: float = 0.0
    p0_durations_ms: tuple[float, ...] = ()
    metrics: dict[str, Any] | None = None


class LaneCCompositionRoot:
    """Own C runtime resources and the event-driven scan lifecycle."""

    def __init__(
        self,
        config: AppConfig,
        *,
        repository: BotRepository | None = None,
        provider: CanonicalMarketDataProvider | None = None,
        scanner: MarketScanner | None = None,
        runtime_instance_id: str | None = None,
    ) -> None:
        self.config = config
        self.started_at = datetime.now(UTC)
        self.runtime_instance_id = runtime_instance_id or uuid.uuid4().hex
        self.config_hash = strategy_config_fingerprint(config)
        self.code_version = resolve_code_version()
        self.shadow_mode = os.environ.get("SHADOW_MODE", "").strip().casefold() in {
            "1", "true", "yes", "on"
        }
        self.shadow_control = get_shadow_control() if self.shadow_mode else None
        if self.shadow_control is not None:
            # run_live.py emits this before composition when available.  Keep
            # the composition root self-contained for direct controlled tests,
            # while avoiding a duplicate event in the launcher path.
            event_path = getattr(getattr(self.shadow_control, "paths", None), "event_path", None)
            try:
                has_events = bool(event_path and event_path.exists() and event_path.stat().st_size)
            except OSError:
                has_events = False
            if not has_events:
                self.shadow_control.emit_capability()
            self.shadow_capture = ShadowCaptureRecorder(
                self.shadow_control.paths.root,
                control=self.shadow_control,
            )
        else:
            self.shadow_capture = None
        if repository is None:
            database = Database(config.db_url)
            repository = BotRepository(database)
        self.repository = repository
        self.state_store = EventStateStore(repository)
        if provider is None:
            scheduler = RequestScheduler(
                max_concurrency=config.max_request_concurrency,
                min_delay_ms=config.request_min_delay_ms,
                jitter_min_ms=config.request_jitter_min_ms,
                jitter_max_ms=config.request_jitter_max_ms,
            )
            client = BybitClient(scheduler=scheduler, timeout=config.request_timeout_sec)
            scanner = scanner or MarketScanner(client=client, config=config)
            provider = CanonicalMarketDataProvider(scanner=scanner, config=config)
        self.provider = provider
        self.scanner = scanner or getattr(provider, "_scanner", None)
        if self.scanner is None:
            raise ValueError("scanner is required when provider is injected")
        self.market = LaneCMarketRuntime(config=config, provider=provider, scanner=self.scanner)
        self.pipeline = LaneCStrategyPipeline(config, config_fingerprint=self.config_hash)
        self.mutations = PerSymbolCoordinator()
        self.windows = ScanWindowCoordinator(p0_deadline_sec=15.0, p1_deadline_sec=30.0)
        self.research = ResearchQueue(maxsize=512, batch_size=25)
        self.notifier = (
            None
            if self.shadow_mode
            else TelegramNotifier(
                config.telegram_token,
                config.signal_chat_id,
                config.alerts_chat_id,
            )
        )
        self.delivery = DurableDeliveryWorker(
            repository,
            self.notifier,
            ErrorThrottler(config.error_alert_ttl_sec),
            send_watch_to_telegram=config.send_watch_to_telegram,
            send_early_drop_warning_to_telegram=False,
            shadow_mode=self.shadow_mode,
            enabled=(
                not self.shadow_mode
                and bool(config.telegram_token and config.signal_chat_id)
            ),
        )
        self.outcomes: OutcomeWorker | OutcomeScheduler = self._build_outcome_worker(
            getattr(provider, "client", None)
        )
        self.outcome_worker_mode = (
            "real" if isinstance(self.outcomes, OutcomeWorker) else "test_compatibility_seam"
        )
        self.persistence = SignalPersistence(
            repository,
            config=config,
            code_version=self.code_version,
            config_hash=self.config_hash,
            runtime_instance_id=self.runtime_instance_id,
            runtime_started_at=self.started_at,
        )
        self._fast_event_queue: deque[dict[str, Any]] = deque(maxlen=256)
        self._fast_heartbeat_queue: deque[dict[str, Any]] = deque(maxlen=256)
        self._fast_capture_before: dict[tuple[str, str, int], Any] = {}
        self._fast_capture_order: deque[tuple[str, str, int]] = deque()
        self._loop_lag_samples: deque[float] = deque(maxlen=256)
        self._loop_lag_task: asyncio.Task[None] | None = None
        self._fast_monitor_task: asyncio.Task[None] | None = None
        self.fast_pool = CandidatePool(
            ttl_minutes=float(config.climax_candidate_ttl_minutes),
            max_active_symbols=int(config.climax_max_active_symbols),
            now_fn=lambda: datetime.now(UTC),
            event_sink=self._queue_fast_event,
            root_event_tracking_enabled=bool(
                getattr(config, "climax_root_event_tracking_enabled", False)
            ),
            root_event_preparer=self._prepare_fast_root_event,
        )
        self.fast_monitor = BoundedFastMonitor(
            self.fast_pool,
            frames_callback=self._fast_frames,
            evaluate_callback=self._evaluate_fast_candidate,
            on_result=self._handle_fast_result,
            mutation_lock=self.mutations.hold,
            state_version_callback=self._fast_state_version,
            event_sink=self._queue_fast_event,
            heartbeat_callback=self._queue_fast_heartbeat,
            poll_interval_sec=float(config.climax_fast_poll_sec),
            max_active_symbols=int(config.climax_max_active_symbols),
            clock=lambda: datetime.now(UTC),
        )
        self._ready = False
        self._stopping = False
        self._window_running = False
        self._fast_monitor_poll_sequence = 0
        self.metrics = RuntimeMetrics()

    def _poll_shadow_control(self) -> None:
        control = self.shadow_control
        if control is None:
            return
        try:
            action = control.poll_control()
        except Exception:
            LOGGER.exception("Lane C shadow control poll failed")
            self._stopping = True
            return
        if action:
            LOGGER.info("Lane C shadow control action applied | action=%s", action)
        if getattr(control, "safety_failed", False):
            self._stopping = True

    def _set_shadow_readiness(self, symbols: tuple[str, ...] = ()) -> None:
        control = self.shadow_control
        if control is None:
            return
        try:
            ready, reason = self.market.readiness(symbols)
            if ready != control.readiness:
                control.set_readiness(ready, reason=reason)
        except Exception:
            LOGGER.exception("Lane C shadow readiness check failed")
            if control.readiness:
                control.set_readiness(False, reason="market_readiness_check_failed")

    def _record_shadow_capture(
        self,
        *,
        before_state: Any,
        decision_market: Any,
        result: Any,
        task: ScanTask | None = None,
        final_recheck_required: bool = False,
        fast_monitor: bool = False,
        poll_sequence: int | None = None,
        symbol: str | None = None,
    ) -> None:
        recorder = self.shadow_capture
        if recorder is None:
            return
        final_recheck = getattr(result, "final_recheck", None)
        final_recheck_captured = (
            bool(getattr(result, "final_recheck_captured", False))
            if final_recheck_required
            else True
        )
        context = {
            "decision_at": getattr(decision_market, "captured_at_utc", None),
            "fast_monitor": bool(fast_monitor),
            "poll_sequence": poll_sequence,
            "strategy_config": self.config,
            "strategy_config_fingerprint": self.config_hash,
            # The final provider snapshot is owned inside finalize_admission;
            # do not claim it was captured unless a future explicit callback
            # supplies that immutable snapshot.
            "final_recheck_required": final_recheck_required,
            "final_recheck_captured": final_recheck_captured,
            "final_recheck": deepcopy(final_recheck) if final_recheck is not None else None,
        }
        # StrategyEvaluation metadata is intentionally immutable and may hold
        # mappingproxy values.  Convert the bounded result projection before
        # handing it to the recorder, whose replay serializer deep-copies its
        # input.  The fresh recheck remains solely in the explicit context.
        capture_result = self._shadow_result_payload(result)
        try:
            recorder.record(
                deepcopy(before_state),
                deepcopy(decision_market),
                deepcopy(capture_result),
                context,
            )
        except Exception:
            # Capture is noncritical telemetry; it must not alter the C
            # evaluation, persistence, or delivery result.
            LOGGER.exception(
                "Lane C shadow capture failed | symbol=%s",
                symbol or getattr(task, "symbol", "unknown"),
            )

    @staticmethod
    def _shadow_result_payload(result: Any) -> Any:
        """Return a deepcopy-safe pre-persistence result for shadow replay."""

        if result is None or isinstance(result, dict):
            return deepcopy(result)
        evaluations = []
        for evaluation in getattr(result, "evaluations", ()) or ():
            canonical = getattr(evaluation, "canonical_payload", None)
            evaluations.append(canonical() if callable(canonical) else deepcopy(evaluation))
        return {
            "features": deepcopy(getattr(result, "features", None)),
            "state": deepcopy(getattr(result, "state", None)),
            "decision": deepcopy(getattr(result, "decision", None)),
            "evaluations": evaluations,
            "blocked_reason": getattr(result, "blocked_reason", None),
        }

    def _build_outcome_worker(self, client: Any | None) -> OutcomeWorker | OutcomeScheduler:
        """Create the bounded real worker when the injected runtime is complete.

        Small in-memory composition tests intentionally inject a provider/repository
        without a Bybit client or outcome tables; those use the compatibility
        semaphore and never start a background network worker.
        """

        if client is None:
            # CanonicalMarketDataProvider exposes ``client``; this fallback is
            # for older provider wrappers that keep the same client on their
            # private scanner.  A missing client is a deliberate in-memory
            # test seam, never a live-runtime default.
            scanner = getattr(self.provider, "_scanner", None)
            client = getattr(scanner, "client", None)
        if client is None or not hasattr(self.repository, "list_shadow_episode_outcomes_due"):
            return OutcomeScheduler(max_concurrency=1)
        tracker = OutcomeTracker(client, self.repository)
        shadow_scheduler = ShadowOutcomeScheduler(
            client=client,
            repository=self.repository,
            runtime_batch_size=self.config.shadow_outcome_runtime_batch_size,
            legacy_batch_size=self.config.shadow_outcome_legacy_batch_size,
            data_gap_retry_minutes=self.config.shadow_outcome_data_gap_retry_minutes,
            outcome_code_version=self.code_version,
            scan_interval_sec=self.config.scan_interval_sec,
            runtime_min_batch=self.config.shadow_outcome_runtime_min_batch,
            runtime_max_batch=self.config.shadow_outcome_runtime_max_batch,
            capacity_margin=self.config.shadow_outcome_capacity_margin,
            v2_enabled=False,
        )
        return OutcomeWorker(
            shadow_scheduler=shadow_scheduler,
            outcome_tracker=tracker,
            signal_batch_size=self.config.shadow_outcome_runtime_batch_size,
            observation_batch_size=self.config.shadow_outcome_legacy_batch_size,
            idle_poll_sec=max(1.0, float(self.config.scan_interval_sec)),
            fetcher=client,
        )

    async def start(self) -> None:
        self._poll_shadow_control()
        if self.shadow_control is not None:
            self.shadow_control.set_readiness(False, reason="process_started")
        await self.market.start()
        self.research.start()
        delivery_started = self.delivery.start()
        if inspect.isawaitable(delivery_started):
            await delivery_started
        if isinstance(self.outcomes, OutcomeWorker):
            await self.outcomes.start()
        self._loop_lag_task = asyncio.create_task(
            self._sample_loop_lag(),
            name="lane-c-loop-lag-sampler",
        )
        if self.config.climax_fast_monitor_enabled:
            self._fast_monitor_task = self.fast_monitor.start()
        self._ready = True
        self._poll_shadow_control()

    async def stop(self) -> None:
        self._stopping = True
        self._ready = False
        self._window_running = False
        self._poll_shadow_control()
        await self.windows.stop()
        if self._fast_monitor_task is not None:
            await self.fast_monitor.stop()
            self._fast_monitor_task = None
        if self._loop_lag_task is not None:
            self._loop_lag_task.cancel()
            await asyncio.gather(self._loop_lag_task, return_exceptions=True)
            self._loop_lag_task = None
        if isinstance(self.outcomes, OutcomeWorker):
            await self.outcomes.stop()
        await self.research.stop()
        await self.delivery.stop()
        if self.notifier is not None:
            await self.notifier.close()
        await self.market.stop()
        if self.shadow_control is not None:
            try:
                self.shadow_control.shutdown(reason="shutdown")
            except Exception:
                LOGGER.exception("Lane C shadow control shutdown failed")

    async def run_window(self, *, window_id: str | None = None) -> WindowReport:
        if not self._ready:
            raise RuntimeError("Lane C runtime is not ready")
        self._poll_shadow_control()
        started = time.perf_counter()
        current = datetime.now(UTC)
        window_id = window_id or current.strftime("%Y%m%dT%H%M%S")
        self._window_running = True
        self.metrics.begin_window(window_id)
        scheduler = self._request_scheduler()
        scheduler_before = self._scheduler_stats(scheduler)
        with self.metrics.phase("universe"):
            shortlist, symbols = await self.market.scan()
        self._poll_shadow_control()
        active = tuple(sorted(self.state_store.load_active(now=current)))
        active_set = set(active)
        ordered = tuple(dict.fromkeys((*active, *symbols)))
        window_scheduled_at = time.perf_counter()
        count = self.windows.open(window_id, current, active=active, shortlist=ordered)
        with self.metrics.phase("frames"):
            frames = await self.market.frames(list(ordered))
        self.metrics.set_gauge(
            "data_age_ms",
            self._frame_data_age_ms(frames, datetime.now(UTC)),
        )
        self._set_shadow_readiness(ordered)
        self._poll_shadow_control()
        statuses: dict[str, str] = {}
        symbol_durations_ms: dict[str, float] = {}
        p0_durations_ms: list[float] = []
        remaining = count
        lock = asyncio.Lock()

        async def consumer() -> None:
            nonlocal remaining
            while True:
                async with lock:
                    if remaining <= 0:
                        return
                    remaining -= 1
                task = await self.windows.acquire()
                if task is None:
                    return
                task_started = time.perf_counter()
                status = await self._evaluate_task(task, frames)
                elapsed_ms = (time.perf_counter() - task_started) * 1000.0
                # The coordinator deadline is measured from window admission,
                # so a task that starts before the deadline but completes after
                # it cannot be reported as a successful evaluation.
                if (
                    time.monotonic() >= task.deadline
                    and status.value in SUCCESSFUL_EVALUATION_STATUSES
                ):
                    status = TerminalStatus.DEADLINE_EXCEEDED
                symbol_durations_ms[task.symbol] = elapsed_ms
                if task.priority is ScanPriority.P0:
                    # Include queue wait: this is the P0 scheduled-window
                    # latency used for the deadline SLO, not service time only.
                    p0_durations_ms.append(
                        (time.perf_counter() - window_scheduled_at) * 1000.0
                    )
                statuses[task.symbol] = status.value
                self.windows.complete(task, status)

        await asyncio.gather(*(consumer() for _ in range(2)))
        self._set_shadow_readiness(ordered)
        with self.metrics.phase("research_enqueue"):
            self._submit_scan_research(
                cycle_started_at=current,
                cycle_completed_at=datetime.now(UTC),
                symbols=ordered,
                statuses=statuses,
                durations_ms=symbol_durations_ms,
            )
        self._record_scheduler_metrics(scheduler_before, self._scheduler_stats(scheduler))
        try:
            self.metrics.set_gauge("cached_worker_backlog", self.research.stats().get("depth"))
        except Exception:
            # Metrics are observational and must not affect window completion.
            LOGGER.debug("Lane C metrics backlog snapshot unavailable", exc_info=True)
        self.metrics.set_gauge("loop_lag_ms", self._loop_lag_value())
        metrics_snapshot = self.metrics.snapshot()
        metrics_snapshot["provenance"] = {
            "code_version": self.code_version,
            "strategy_config_fingerprint": self.config_hash,
        }
        report = WindowReport(
            window_id=window_id,
            scheduled=count,
            statuses=statuses,
            duration_ms=(time.perf_counter() - started) * 1000.0,
            p0_deadline_misses=sum(
                status == TerminalStatus.DEADLINE_EXCEEDED.value
                for symbol, status in statuses.items()
                if symbol in active_set
            ),
            coverage=(successful_evaluation_count(statuses.values()) / count if count else 1.0),
            p0_durations_ms=tuple(p0_durations_ms),
            metrics=metrics_snapshot,
        )
        self._window_running = False
        return report

    def _request_scheduler(self) -> Any | None:
        """Return the shared request scheduler when the provider exposes it."""

        client = getattr(self.provider, "client", None)
        return getattr(client, "_scheduler", None) or getattr(client, "scheduler", None)

    @staticmethod
    def _scheduler_stats(scheduler: Any | None) -> dict[str, Any]:
        if scheduler is None:
            return {}
        snapshot = getattr(scheduler, "stats_snapshot", None)
        if not callable(snapshot):
            return {}
        try:
            result = snapshot()
        except Exception:
            LOGGER.debug("Lane C request scheduler metrics unavailable", exc_info=True)
            return {}
        return result if isinstance(result, dict) else {}

    def _record_scheduler_metrics(
        self,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> None:
        """Expose available admission waits without inventing service latency."""

        if not before and not after:
            return
        endpoint_aliases = {
            "fetch_instruments": "market.scan",
            "fetch_tickers": "market.ticker",
            "fetch_symbol_ticker": "market.ticker",
            "fetch_klines": "market.kline",
            "fetch_open_interest": "market.open_interest",
            "fetch_funding": "market.funding",
            "fetch_orderbook": "market.orderbook",
        }
        for key, current in after.items():
            if not isinstance(current, dict) or key in {"metrics", "provider_request_queue_depth", "in_flight"}:
                continue
            previous = before.get(key, {})
            if not isinstance(previous, dict):
                previous = {}
            queued = max(0, int(current.get("queued", 0)) - int(previous.get("queued", 0)))
            if queued <= 0:
                continue
            operation = str(key).split(":", 1)[0]
            endpoint = endpoint_aliases.get(operation, operation)
            wait_delta = max(
                0.0,
                float(current.get("queue_wait_ms_total", 0.0))
                - float(previous.get("queue_wait_ms_total", 0.0)),
            )
            errors = max(0, int(current.get("errors", 0)) - int(previous.get("errors", 0)))
            self.metrics.request(
                endpoint,
                elapsed_ms=None,
                limiter_wait_ms=wait_delta / queued if wait_delta else None,
                status="error" if errors else "ok",
            )

    @staticmethod
    def _frame_data_age_ms(frames: Any, now: datetime) -> float | None:
        """Return the worst known candle age without manufacturing timestamps."""

        ages: list[float] = []
        values = frames.values() if isinstance(frames, dict) else ()
        for holder in values:
            frame = getattr(holder, "frame", holder)
            columns = getattr(frame, "columns", ())
            try:
                if "timestamp" in columns:
                    latest = frame["timestamp"].max()
                else:
                    latest = frame.index.max()
                converter = getattr(latest, "to_pydatetime", None)
                if callable(converter):
                    latest = converter()
                if not isinstance(latest, datetime):
                    continue
                if latest.tzinfo is None or latest.utcoffset() is None:
                    latest = latest.replace(tzinfo=UTC)
                age = (now - latest.astimezone(UTC)).total_seconds() * 1000.0
                if age >= 0:
                    ages.append(age)
            except (AttributeError, KeyError, TypeError, ValueError):
                continue
        return max(ages) if ages else None

    @staticmethod
    def _prepare_fast_root_event(state: Any, observed_at: datetime) -> int:
        """Return the existing C root revision without creating a new event."""

        snapshot = getattr(state, "event_features_snapshot", {})
        value = snapshot.get("root_event_revision") if isinstance(snapshot, dict) else None
        try:
            return int(value or 1)
        except (TypeError, ValueError):
            return 1

    def _queue_fast_event(self, payload: Any) -> None:
        """Keep fast lifecycle telemetry bounded and enqueue its DB write."""

        item = dict(payload) if isinstance(payload, dict) else {"details": payload}
        self._fast_event_queue.append(item)
        recorder = getattr(self.repository, "record_climax_monitor_event", None)
        symbol = item.get("symbol")
        event_id = item.get("event_id")
        if not callable(recorder) or not symbol or not event_id:
            return
        created_at = _as_utc_datetime(item.get("created_at")) or datetime.now(UTC)
        known = {
            "created_at",
            "symbol",
            "event_id",
            "event_high_time",
            "action",
            "reason",
            "pool_size",
            "poll_sequence",
            "root_event_id",
            "event_revision",
            "pool_added_at",
            "observed_at",
            "market_asof",
            "event_age_sec",
            "pool_age_sec",
        }
        details = {key: value for key, value in item.items() if key not in known}
        revision = item.get("event_revision")
        try:
            revision = int(revision) if revision is not None else None
        except (TypeError, ValueError):
            revision = None
        kwargs = {
            "created_at": created_at,
            "symbol": str(symbol),
            "event_id": str(event_id),
            "event_high_time": _as_utc_datetime(item.get("event_high_time")),
            "action": str(item.get("action") or "fast_monitor"),
            "reason": item.get("reason"),
            "pool_size": int(item.get("pool_size") or len(self.fast_pool)),
            "poll_sequence": item.get("poll_sequence"),
            "worker_id": self.runtime_instance_id,
            "details": details,
            "runtime_instance_id": self.runtime_instance_id,
            "root_event_id": str(item.get("root_event_id") or event_id),
            "event_revision": revision,
            "observed_at": _as_utc_datetime(item.get("observed_at")) or created_at,
            "market_asof": _as_utc_datetime(item.get("market_asof")),
            "pool_added_at": _as_utc_datetime(item.get("pool_added_at")),
            "event_age_sec": item.get("event_age_sec"),
            "pool_age_sec": item.get("pool_age_sec"),
        }
        self.research.submit(
            "climax_monitor_event",
            lambda recorder=recorder, kwargs=kwargs: recorder(**kwargs),
        )

    def _queue_fast_heartbeat(self, payload: Any) -> None:
        """Queue bounded heartbeat writes without blocking the fast poll."""

        item = dict(payload) if isinstance(payload, dict) else {}
        self._fast_heartbeat_queue.append(item)
        updater = getattr(self.repository, "update_fast_monitor_heartbeat", None)
        if not callable(updater):
            return
        checked_at = (
            _as_utc_datetime(item.get("completed_at"))
            or _as_utc_datetime(item.get("checked_at"))
            or datetime.now(UTC)
        )
        try:
            sequence = int(item.get("poll_sequence") or self._fast_monitor_poll_sequence)
            pool_size = int(item.get("pool_size") or len(self.fast_pool))
        except (TypeError, ValueError):
            sequence = self._fast_monitor_poll_sequence
            pool_size = len(self.fast_pool)
        action = str(item.get("action") or "")
        kwargs = {
            "checked_at": checked_at,
            "pool_size": pool_size,
            "poll_sequence": sequence,
            "last_poll_at": checked_at if action == "poll_start" else None,
            "last_complete_at": checked_at if action == "poll_complete" else None,
            "last_error": item.get("error_code"),
            "runtime_instance_id": self.runtime_instance_id,
            "model_version": self.code_version,
            "config_fingerprint": self.config_hash,
            "full_scan_running": bool(getattr(self, "_window_running", False)),
            "fast_monitor_running": True,
            "event_loop_lag_ms": self._loop_lag_value(),
            "poll_duration_ms": item.get("poll_duration_ms"),
        }
        self.research.submit(
            "fast_monitor_heartbeat",
            lambda updater=updater, kwargs=kwargs: updater(**kwargs),
        )

    async def _sample_loop_lag(self) -> None:
        """Measure scheduler delay with a bounded real event-loop sampler."""

        loop = asyncio.get_running_loop()
        interval = 0.5
        expected = loop.time() + interval
        while not self._stopping:
            await asyncio.sleep(max(0.0, expected - loop.time()))
            observed = loop.time()
            self._loop_lag_samples.append(max(0.0, (observed - expected) * 1000.0))
            expected += interval
            if expected <= observed:
                expected = observed + interval

    def _loop_lag_value(self) -> float | None:
        if not self._loop_lag_samples:
            return None
        return max(self._loop_lag_samples)

    async def _fast_frames(self, symbols: tuple[str, ...]) -> dict[str, Any]:
        """Capture bounded candidate snapshots from the WS/store frame path."""

        if not symbols:
            return {}
        raw_frames = await self.market.frames(list(symbols))

        async def capture(symbol: str) -> tuple[str, Any | None]:
            frame = raw_frames.get(symbol)
            if frame is None:
                frame = raw_frames.get(symbol.upper())
            if frame is None or getattr(frame, "empty", True):
                return symbol, None
            derivatives = self.market.derivatives(symbol)
            if (
                _derivative_terminal_status(
                    derivatives,
                    required=self.config.derivatives_enabled,
                )
                is not None
            ):
                return symbol, None
            try:
                snapshot = await self.provider.capture_decision_snapshot(
                    symbol,
                    frame_1m=frame,
                    include_liquidity=True,
                    derivatives=derivatives,
                )
            except Exception:
                LOGGER.debug("fast monitor snapshot failed | symbol=%s", symbol, exc_info=True)
                return symbol, None
            return symbol, snapshot

        values = await asyncio.gather(*(capture(symbol) for symbol in symbols))
        return {symbol: snapshot for symbol, snapshot in values if snapshot is not None}

    async def _fast_state_version(self, symbol: str) -> SnapshotVersion:
        state = self.state_store.load_active_symbol(symbol, now=datetime.now(UTC))
        if state is None:
            return SnapshotVersion()
        return SnapshotVersion(
            asof=_as_utc_datetime(state.updated_at or state.event_high_time),
            generation=self._state_generation(state),
        )

    @staticmethod
    def _state_generation(state: Any) -> Any:
        snapshot = getattr(state, "event_features_snapshot", {})
        if not isinstance(snapshot, dict):
            snapshot = {}
        for key in (
            "generation",
            "root_event_revision",
            "volume_climax_event_revision",
            "event_revision",
        ):
            value = snapshot.get(key)
            if value is not None:
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return value
        return None

    def _store_fast_before(self, key: tuple[str, str, int], state: Any) -> None:
        self._fast_capture_before[key] = deepcopy(state)
        self._fast_capture_order.append(key)
        while len(self._fast_capture_order) > 256:
            old_key = self._fast_capture_order.popleft()
            self._fast_capture_before.pop(old_key, None)

    def _pop_fast_before(self, key: tuple[str, str, int]) -> Any:
        return self._fast_capture_before.pop(key, None)

    async def _evaluate_fast_candidate(
        self,
        candidate: Any,
        snapshot: Any,
        poll_sequence: int,
    ) -> Any:
        self._fast_monitor_poll_sequence = int(poll_sequence)
        state = self.state_store.load_active_symbol(
            candidate.symbol,
            now=getattr(snapshot, "captured_at_utc", None) or datetime.now(UTC),
        )
        if state is None or state.event_id != candidate.event_id:
            self.fast_pool.remove(
                candidate.symbol,
                candidate.event_id,
                reason="state_replaced",
            )
            return None
        if state.signal_id is not None:
            self.fast_pool.remove(
                candidate.symbol,
                candidate.event_id,
                reason="signal_created",
            )
            return None
        key = (candidate.symbol, candidate.event_id, int(poll_sequence))
        self._store_fast_before(key, state)
        try:
            return self.pipeline.evaluate_climax_only(
                candidate.symbol,
                snapshot.frame_1m.frame,
                deepcopy(state),
                derivatives=snapshot.derivatives,
                liquidity=snapshot.liquidity,
                decision_at=snapshot.captured_at_utc,
                poll_sequence=poll_sequence,
            )
        except Exception:
            self._pop_fast_before(key)
            raise

    async def _handle_fast_result(
        self,
        candidate: Any,
        snapshot: Any,
        result: Any,
        poll_sequence: int,
    ) -> None:
        self._fast_monitor_poll_sequence = int(poll_sequence)
        key = (candidate.symbol, candidate.event_id, int(poll_sequence))
        before_state = self._pop_fast_before(key)
        if result is None:
            return
        self._record_shadow_capture(
            before_state=before_state,
            decision_market=snapshot,
            result=result,
            final_recheck_required=False,
            fast_monitor=True,
            poll_sequence=poll_sequence,
            symbol=candidate.symbol,
        )
        state = getattr(result, "state", None)
        if state is None or getattr(state, "event_id", None) != candidate.event_id:
            return
        decision = getattr(result, "decision", None)
        if decision is not None and decision.actionable:
            if live_delivery_enabled(decision, self.config) and state.signal_id is None:
                record = self.persistence.save(decision, state)
                if record is not None:
                    self.fast_pool.remove(
                        candidate.symbol,
                        candidate.event_id,
                        reason="signal_created",
                        now=getattr(snapshot, "captured_at_utc", None),
                    )
                    self.delivery.wake()
        self._queue_fast_event(
            {
                "created_at": getattr(snapshot, "captured_at_utc", None),
                "symbol": candidate.symbol,
                "event_id": candidate.event_id,
                "event_high_time": getattr(candidate, "event_high_time", None),
                "action": "candidate_evaluated",
                "reason": getattr(result, "blocked_reason", None),
                "pool_size": len(self.fast_pool),
                "poll_sequence": poll_sequence,
                "details": {
                    "actionable": bool(decision is not None and decision.actionable),
                    "strategy_subtype": getattr(decision, "strategy_subtype", None),
                    "score": getattr(decision, "score", None),
                },
            }
        )

    def _update_fast_candidate(
        self,
        *,
        before_state: Any,
        result: Any,
        observed_at: datetime,
    ) -> None:
        if not (
            self.config.climax_fast_monitor_enabled
            and self.config.climax_short_enabled
        ):
            return
        state = getattr(result, "state", None)
        if state is None or not bool(getattr(state, "is_active", False)):
            if before_state is not None:
                self.fast_pool.remove(
                    before_state.symbol,
                    before_state.event_id,
                    reason="state_terminal",
                    now=observed_at,
                )
            return
        if state.signal_id is not None:
            self.fast_pool.remove(
                state.symbol,
                state.event_id,
                reason="signal_created",
                now=observed_at,
            )
            return
        self.fast_pool.track(
            state,
            now=observed_at,
            generation=self._state_generation(state),
        )

    def _remove_fast_candidate(self, state: Any, *, reason: str, now: datetime) -> None:
        if state is None:
            return
        self.fast_pool.remove(
            state.symbol,
            state.event_id,
            reason=reason,
            now=now,
        )

    def _submit_scan_research(
        self,
        *,
        cycle_started_at: datetime,
        cycle_completed_at: datetime,
        symbols: tuple[str, ...],
        statuses: dict[str, str],
        durations_ms: dict[str, float],
    ) -> None:
        """Queue bounded coverage telemetry after the scan has accounted tasks."""

        recorder = getattr(self.repository, "record_market_scan_cycle", None)
        if not callable(recorder) or not symbols:
            return
        symbol_results = [
            {
                "symbol": symbol,
                "terminal_status": statuses.get(symbol, "UNKNOWN"),
                "reason_code": statuses.get(symbol, "UNKNOWN"),
                "duration_ms": durations_ms.get(symbol),
                "details": {},
            }
            for symbol in symbols
        ]
        kwargs = {
            "cycle_started_at": cycle_started_at,
            "cycle_completed_at": cycle_completed_at,
            "exchange_symbols": list(symbols),
            "eligible_symbols": list(symbols),
            "excluded": [],
            "scheduled_symbols": list(symbols),
            "symbol_results": symbol_results,
            "candidate_symbols": len(symbols),
            "evaluated_symbols": successful_evaluation_count(statuses.values()),
            "last_error": None,
        }
        self.research.submit("market_scan_cycle", lambda: recorder(**kwargs))

    async def _evaluate_task(self, task: ScanTask, frames: dict[str, Any]) -> TerminalStatus:
        if time.monotonic() >= task.deadline:
            return TerminalStatus.DEADLINE_EXCEEDED
        frame = frames.get(task.symbol)
        if frame is None or frame.empty:
            return TerminalStatus.INCOMPLETE_DATA
        async with self.mutations.hold(task.symbol):
            try:
                state = self.state_store.load_active_symbol(task.symbol, now=datetime.now(UTC))
                before_state = deepcopy(state)
                store_derivatives = self.market.derivatives(task.symbol)
                derivative_status = _derivative_terminal_status(
                    store_derivatives,
                    required=self.config.derivatives_enabled,
                )
                if derivative_status is not None:
                    return derivative_status
                # Phase 1 is deliberately book-free.  C builds the real
                # features/lifecycle envelope first; an orderbook is needed
                # only for an existing active event or a newly detected root.
                # This keeps a normal 100-symbol no-event window off the
                # expensive REST orderbook path while preserving the full
                # strategy evaluation for candidates.
                with self.metrics.phase("features_strategy"):
                    decision_market = await self.provider.capture_decision_snapshot(
                        task.symbol,
                        frame_1m=frame,
                        include_liquidity=False,
                        derivatives=store_derivatives,
                    )
                if decision_market is None:
                    return TerminalStatus.STALE_DATA
                if _snapshot_older_than_state(decision_market, state):
                    return TerminalStatus.STALE_DATA

                def evaluate_with_market(market: Any) -> Any:
                    return self.pipeline.evaluate(
                        task.symbol,
                        market.frame_1m.frame,
                        state,
                        derivatives=market.derivatives,
                        liquidity=market.liquidity,
                        decision_at=market.captured_at_utc,
                        fast_monitor=False,
                        poll_sequence=None,
                    )

                with self.metrics.phase("features_strategy"):
                    result = evaluate_with_market(decision_market)
                state_value = str(getattr(getattr(state, "state", None), "value", ""))
                active_event = state is not None and state_value not in {"IDLE", "EXPIRED"}
                needs_liquidity = active_event or result.state is not None
                if needs_liquidity:
                    if time.monotonic() >= task.deadline:
                        return TerminalStatus.DEADLINE_EXCEEDED
                    with self.metrics.phase("liquidity"):
                        decision_market = await self.provider.capture_decision_snapshot(
                            task.symbol,
                            frame_1m=frame,
                            include_liquidity=True,
                            derivatives=store_derivatives,
                        )
                    if decision_market is None:
                        return TerminalStatus.STALE_DATA
                    if _snapshot_older_than_state(decision_market, state):
                        return TerminalStatus.STALE_DATA
                    with self.metrics.phase("features_strategy"):
                        result = evaluate_with_market(decision_market)

                final_recheck_required = (
                    getattr(getattr(result, "decision", None), "strategy_subtype", None)
                    == "LOW_VOLUME_EXTENSION_FAILURE"
                )
                with self.metrics.phase("features_strategy"):
                    result = await self.pipeline.finalize_admission(
                        result,
                        provider=self.provider,
                        symbol=task.symbol,
                        frame_1m=decision_market.frame_1m.frame,
                        derivatives=decision_market.derivatives,
                        decision_at=decision_market.captured_at_utc,
                        fast_monitor=False,
                        poll_sequence=None,
                    )
                self._update_fast_candidate(
                    before_state=before_state,
                    result=result,
                    observed_at=decision_market.captured_at_utc,
                )
                self._record_shadow_capture(
                    before_state=before_state,
                    decision_market=decision_market,
                    result=result,
                    task=task,
                    final_recheck_required=final_recheck_required,
                    fast_monitor=False,
                    poll_sequence=None,
                )
                with self.metrics.phase("state_signal_outbox"):
                    if result.state is not None:
                        self.state_store.save(result.state)
                    if result.blocked_reason in {
                        "LOW_VOLUME_FINAL_RECHECK_DATA_MISSING",
                        "LOW_VOLUME_FINAL_RECHECK_LIQUIDITY_MISSING",
                    }:
                        return TerminalStatus.INCOMPLETE_DATA
                    if result.decision is not None and result.decision.actionable and result.state is not None:
                        record = self.persistence.save(result.decision, result.state)
                        if record is not None:
                            self._remove_fast_candidate(
                                result.state,
                                reason="signal_created",
                                now=decision_market.captured_at_utc,
                            )
                            # Persistence links the signal and outbox atomically.
                            # The terminal lifecycle marker belongs to the
                            # eventual delivery owner; setting it here would
                            # claim delivery before a consumer can send or retry.
                            self.delivery.wake()
                return TerminalStatus.SCANNED_OK if result.features is not None else TerminalStatus.NO_SETUP
            except (TimeoutError, asyncio.TimeoutError):
                return TerminalStatus.RATE_LIMITED
            except Exception:
                LOGGER.exception("Lane C symbol task failed | symbol=%s", task.symbol)
                return TerminalStatus.SCAN_ERROR

    async def run_forever(self) -> None:
        await self.start()
        loop = asyncio.get_running_loop()
        interval_sec = max(1.0, float(self.config.scan_interval_sec))
        now_utc = datetime.now(UTC)
        next_boundary = now_utc.replace(second=0, microsecond=0) + timedelta(
            seconds=interval_sec
        )
        # Align the first window to the next interval boundary, while keeping
        # at least the configured settling grace.  Subsequent windows use an
        # anchored monotonic deadline so a slow window skips missed ticks
        # instead of launching a catch-up storm.
        scheduled_deadline = loop.time() + max(
            SCAN_WINDOW_GRACE_SEC,
            (next_boundary - now_utc).total_seconds(),
        )
        try:
            while not self._stopping:
                await asyncio.sleep(max(0.0, scheduled_deadline - loop.time()))
                if self._stopping:
                    break
                await self.run_window()
                finished_at = loop.time()
                schedule = next_scan_deadline(
                    scheduled_deadline=scheduled_deadline,
                    finished_at=finished_at,
                    interval_sec=interval_sec,
                )
                if schedule.skipped_ticks:
                    LOGGER.warning(
                        "Lane C scan overrun; missed ticks skipped | "
                        "overrun_ms=%.1f skipped_ticks=%s",
                        schedule.cycle_overrun_ms,
                        schedule.skipped_ticks,
                    )
                scheduled_deadline = schedule.next_deadline
        finally:
            await self.stop()


__all__ = [
    "LaneCCompositionRoot",
    "SCAN_WINDOW_GRACE_SEC",
    "SUCCESSFUL_EVALUATION_STATUSES",
    "WindowReport",
    "strategy_config_fingerprint",
    "successful_evaluation_count",
]


def _as_utc_datetime(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _snapshot_older_than_state(snapshot: Any, state: Any) -> bool:
    """Reject a captured market snapshot older than the state it would write."""

    if state is None:
        return False
    captured = _as_utc_datetime(
        getattr(snapshot, "captured_at_utc", None)
        or getattr(snapshot, "max_input_availability_time_utc", None)
    )
    state_asof = _as_utc_datetime(
        getattr(state, "updated_at", None)
        or getattr(state, "event_high_time", None)
    )
    return captured is not None and state_asof is not None and captured < state_asof


def _derivative_terminal_status(
    derivatives: dict[str, Any] | None,
    *,
    required: bool,
) -> TerminalStatus | None:
    """Keep derivative cache misses explicit before provider/strategy work."""

    if not required:
        return None
    if not derivatives:
        return TerminalStatus.INCOMPLETE_DATA
    status = str(derivatives.get("derivatives_status") or "UNKNOWN").upper()
    if status == "OK" and derivatives.get("open_interest") and derivatives.get("funding"):
        return None
    if status == "STALE":
        return TerminalStatus.STALE_DATA
    if status == "RATE_LIMITED":
        return TerminalStatus.RATE_LIMITED
    return TerminalStatus.INCOMPLETE_DATA

