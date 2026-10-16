"""Application runtime for the short signal bot."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Self, cast

import pandas as pd

from app.early_drop_warning import (
    EarlyDropSnapshot,
    evaluate_early_drop_warning,
    format_early_drop_warning,
)
from app.baseline.lifecycle import BaselineLifecycle
from app.config import AppConfig, load_config
from app.promotions import validate_promotion_registry
from app.domain import (
    EventState,
    EventStatus,
    MarketSnapshot,
    SignalDecision,
    SignalProvenanceInput,
    SignalType,
    SymbolFeatures,
)
from app.events.pullback_tracker import PullbackTracker
from app.events.pump_detector import PumpDetector
from app.events.short_zone import ShortZoneBuilder
from app.events.state_store import EventStateStore
from app.features.builder import FeatureBuilder
from app.infra.disk_capacity import (
    DiskCapacitySnapshot,
    DiskHealthState,
    StorageIncidentTracker,
    derive_reserve_bytes,
    is_sqlite_full,
    read_capacity,
)
from app.infra.health import ServiceHealth
from app.infra.request_scheduler import (
    RequestDeadlineExceeded,
    RequestPriority,
    RequestScheduler,
)
from app.infra.runtime_metadata import resolve_code_version
from app.infra.runtime_timing import (
    METRIC_KEYS,
    CycleTiming,
    EventLoopLagMonitor,
    attach_cycle_timing,
    build_evidence_document,
    detach_cycle_timing,
    measure,
    measure_sync,
    publish_evidence,
)
from app.logger import configure_logging
from app.market.bybit_client import BybitClient
from app.market.coverage import select_rotation_batch
from app.market.scanner import MarketScanner
from app.market_data.provider import CanonicalMarketDataProvider
from app.market_data.provider import ComparisonClassification
from app.market_data.store import (
    STALE_CANDLE_STALE,
    STALE_EPOCH_INVALIDATED,
    STALE_NO_TICKER,
    STALE_OI_HISTORY,
    STALE_TICKER_STALE,
    STALE_WAITING_FOR_WS,
)
from app.market_data.validation import MarketDataQuality, MarketDataValidator
from app.market_data.evidence import ProviderEvidencePublisher
from app.research.spool import ResearchSpool
from app.research.storage_health import ResearchStorageHealthPublisher
from app.research.storage_router import ResearchStorageRouter
from app.notifications.telegram import TelegramNotifier
from app.notifications.throttling import ErrorThrottler
from app.observability.outcome_scheduler import ShadowOutcomeScheduler
from app.observability.causal_pump_v2 import (
    CausalPumpV2Comparator,
    CausalPumpV2Config,
    CausalPumpV2Detector,
    CausalPumpV2Tracker,
    CausalV2RootDecision,
)
from app.observability.root_detector_shadow import (
    ShadowCandidateInput,
    should_create_shadow_candidate,
)
from app.observability.root_detector_shadow_v2 import (
    default_counterfactual_contract,
    evaluate_v2_observation,
)
from app.observability.root_predicate_snapshot import (
    build_current_root_predicate_snapshot,
)
from app.observability.strategy_observations import (
    ObservationWriteResult,
    ObservationWriteStatus,
    StrategyObservation,
    build_observation_evidence,
    make_observation_idempotency_key,
)
from app.outcomes.tracker import OutcomeTracker
from app.replay.config import strategy_config_fingerprint
from app.replay.canonical import sha256_canonical
from app.replay.market import CandleObservationKind, NormalizedMarketObservation
from app.runtime.delivery_worker import DeliveryWorker
from app.runtime.outcome_worker import OutcomeWorker
from app.runtime.research_queue import ResearchQueue
from app.runtime.scheduling import next_scan_deadline
from app.runtime.shutdown_checkpoint import ShutdownCheckpointBarrier
from app.runtime.scan_coordinator import (
    ScanCoordinator,
    ScanPriority,
    ScanTask,
    ScanTerminalStatus,
)
from app.runtime.symbol_mutation import SymbolMutationCoordinator
from app.runtime.warmup import WarmupCoverage
from app.signals.climax import (
    ClimaxEvaluation,
    ClimaxEvaluationBundle,
    advance_volume_climax_lifecycle,
    climax_branch_attempt_id,
    climax_branch_candidate_id,
    evaluate_climax_bundle,
    evaluate_climax_shadow,
    volume_climax_attempt_id,
)
from app.signals.delivery_policy import live_delivery_enabled
from app.signals.trapped_longs_v2 import (
    FrozenBreakout,
    OiObservation,
    TRAPPED_LONGS_REVERSAL_V2,
    TRAPPED_LONGS_V2_MODEL_VERSION,
    evaluate_trapped_longs_reversal_v2,
)
from app.signals.trapped_longs import (
    TRAPPED_LONGS_MODEL_VERSION,
    TRAPPED_LONGS_REVERSAL,
    evaluate_trapped_longs_reversal,
    advance_trapped_longs_lifecycle,
    trapped_longs_attempt_id,
    trapped_longs_root_event_id,
)
from app.signals.engine import SignalEngine
from app.signals.formatter import format_signal_message
from app.storage.db import Database
from app.storage.repository import BotRepository



FRAME_STORE_WARMUP_ATTEMPT_TIMEOUT_SEC = 90.0
FRAME_STORE_WARMUP_RETRY_BACKOFF_SEC = 1.0
_FRAME_STORE_WS_WAITING_REASONS = frozenset({
    STALE_CANDLE_STALE,
    STALE_EPOCH_INVALIDATED,
    STALE_NO_TICKER,
    STALE_OI_HISTORY,
    STALE_TICKER_STALE,
    STALE_WAITING_FOR_WS,
})

def _attach_market_data_observer(hub: object, observer: object) -> None:
    """Attach continuity to injected Hub implementations without widening their API."""
    add_observer = getattr(hub, "add_observer", None)
    if callable(add_observer):
        add_observer(observer)
        return
    observers = getattr(hub, "_observers", None)
    if observers is None or observer in observers:
        return
    try:
        hub._observers = (*observers, observer)  # type: ignore[attr-defined]
    except AttributeError:
        # Test doubles without an observer seam remain usable as a lifecycle seam;
        # real MarketDataHub instances expose the mutable observer hook below.
        return


def _actionable_canonical_symbols(
    store: object,
    symbols: tuple[str, ...] | list[str],
    decision_at: datetime,
) -> tuple[str, ...]:
    """Return only symbols with an existing actionable canonical frame."""
    peek = getattr(store, "peek_snapshot", None)
    if not callable(peek):
        return ()
    ready: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = str(raw_symbol).upper()
        if symbol in seen:
            continue
        seen.add(symbol)
        frame = peek(symbol, decision_at)
        if frame is not None and frame.actionable:
            ready.append(symbol)
    return tuple(ready)


def _provenance_anomaly(decision: SignalDecision) -> str | None:
    """Validate the decision-time snapshot without vetoing live delivery."""
    if decision.strategy_subtype not in {
        "VOLUME_CLIMAX_UNWIND",
        "LOW_VOLUME_EXTENSION_FAILURE",
        "TRAPPED_LONGS_REVERSAL",
    }:
        return None
    high = decision.strategy_metadata.get("event_high")
    if (
        high is None
        or decision.market_price <= 0
        or float(high) < decision.market_price
    ):
        return "PROVENANCE_INVARIANT_FAILED"
    computed = (float(high) - decision.market_price) / float(high) * 100.0
    stored = decision.strategy_metadata.get("entry_distance_below_high_pct")
    if stored is not None and abs(float(stored) - computed) > 0.05:
        return "PROVENANCE_INVARIANT_FAILED"
    return None


def _public_config_fingerprint(config: AppConfig) -> str:
    """Hash only non-secret, behavior-relevant config values."""
    values = config.model_dump(mode="json")
    excluded = (
        "token",
        "secret",
        "password",
        "api_key",
        "apikey",
        "private_key",
        "credential",
        "webhook",
    )
    public = {
        key: value
        for key, value in values.items()
        if not any(part in key.lower() for part in excluded)
        and key not in {"db_url", "signal_chat_id", "alerts_chat_id"}
    }
    encoded = json.dumps(
        public, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _strategy_config_fingerprint(config: AppConfig) -> str:
    """Hash strategy-affecting configuration without operational settings."""
    return strategy_config_fingerprint(config)


def _parse_optional_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _decision_delta(
    live: ClimaxEvaluation, shadow: ClimaxEvaluation | None
) -> str | None:
    if shadow is None:
        return None
    if live.actionable and shadow.actionable:
        return "BOTH_ACTIONABLE"
    if not live.actionable and not shadow.actionable:
        return "BOTH_REJECTED"
    if not live.actionable and shadow.actionable:
        return "LIVE_REJECTED_SHADOW_ACTIONABLE"
    return "LIVE_ACTIONABLE_SHADOW_REJECTED"


def _fingerprint_value(value: object) -> object:
    """Reduce runtime input values to deterministic canonical-JSON primitives."""

    if isinstance(value, dict):
        return {str(key): _fingerprint_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _fingerprint_value(item())
        except (TypeError, ValueError):
            pass
    return str(value)


def _market_snapshot_fingerprint(snapshot: object) -> str:
    frame = getattr(getattr(snapshot, "frame_1m"), "frame")
    payload = {
        "symbol": getattr(snapshot, "symbol", None),
        "frame": frame.to_json(orient="split", date_format="iso"),
        "derivatives": _fingerprint_value(getattr(snapshot, "derivatives", {})),
        "liquidity": _fingerprint_value(getattr(snapshot, "liquidity", None)),
    }
    return sha256_canonical(payload)


def _decision_fingerprint(decision: SignalDecision | None) -> str:
    if decision is None:
        return sha256_canonical({"decision": None})
    return sha256_canonical(
        {
            "symbol": decision.symbol,
            "event_id": decision.event_id,
            "signal_type": decision.signal_type,
            "grade": decision.grade,
            "score": decision.score,
            "decision_type": decision.decision_type,
            "actionable": decision.actionable,
            "lifecycle_state": decision.lifecycle_state,
            "strategy_type": decision.strategy_type,
            "strategy_subtype": decision.strategy_subtype,
            "blockers": decision.blockers,
            "reasons": decision.reasons,
            "risk_flags": decision.risk_flags,
            "strategy_metadata": _fingerprint_value(decision.strategy_metadata),
        }
    )


def _causal_pump_v2_config(config: AppConfig) -> CausalPumpV2Config:
    """Build V2 thresholds from the existing event and pullback contract."""
    return CausalPumpV2Config(
        pump_min_ret_5m=config.event_ret_15m_min,
        pump_min_ret_15m=config.event_ret_15m_min,
        pump_min_ret_1h=config.event_ret_1h_min,
        pump_min_ret_4h=config.event_ret_4h_min,
        drawup_lookback_candles=config.causal_pump_v2_lookback_candles,
        pullback_min_pct=config.pullback_min_pct,
        pullback_max_pct=config.pullback_max_pct,
        zone_fraction_of_range_low=config.short_zone_range_low_pct,
        zone_fraction_of_range_high=config.short_zone_range_high_pct,
        expiry_minutes=config.causal_pump_v2_expiry_minutes,
    )


@dataclass(slots=True)
class _ClimaxCandidate:
    symbol: str
    event_id: str
    event_high_time: datetime | None
    candidate_added_at: datetime
    pool_added_at: datetime


class ShortSignalBot:
    """Single-process runtime that scans, scores, and sends signals."""

    def __init__(
        self,
        config: AppConfig,
        repository: BotRepository | None = None,
        scanner: MarketScanner | None = None,
        notifier: TelegramNotifier | None = None,
        market_data_hub: object | None = None,
        market_data_provider: CanonicalMarketDataProvider | None = None,
        provider_evidence_publisher: ProviderEvidencePublisher | None = None,
    ) -> None:
        self._config = config
        self._runtime_started_at = datetime.now(timezone.utc)
        self._runtime_instance_id = uuid.uuid4().hex
        self._code_version = resolve_code_version()
        self._config_fingerprint = _public_config_fingerprint(config)
        self._strategy_config_hash = _strategy_config_fingerprint(config)
        self._logger = logging.getLogger(self.__class__.__name__)
        self._watch_enqueued_in_cycle = 0
        self._cycle_sequence = 0
        self._lag_monitor: EventLoopLagMonitor | None = None
        self._active_climax_pool: dict[tuple[str, str], _ClimaxCandidate] = {}
        self._fast_monitor_cursor = 0
        self._fast_monitor_poll_sequence = 0
        self._fast_monitor_task: asyncio.Task[None] | None = None
        self._fast_monitor_running = False
        self._shadow_rotation_id = "unknown"
        self._symbol_mutations = SymbolMutationCoordinator(self._logger)
        self._lifecycle = "STARTING"
        self._ready = False
        self._warmup_coverage = WarmupCoverage()
        self._shutdown_checkpoint_barrier = ShutdownCheckpointBarrier(
            checkpoint=self._checkpoint_for_shutdown
        )
        self._last_shutdown_checkpoint_ack = None
        self._startup_cleanup_required = False
        self._market_data_gap_tracker: object | None = None
        self._request_scheduler: RequestScheduler | None = None
        self._market_frame_store: object | None = None
        self._scan_coordinator: ScanCoordinator | None = None
        self._scan_consumers: list[asyncio.Task[None]] = []
        self._outcome_worker: OutcomeWorker | None = None
        self._research_queue: ResearchQueue | None = None
        self._universe_refreshed_at_monotonic = 0.0
        self._event_universe_snapshots: list[MarketSnapshot] = []
        self._event_eligible_symbols: tuple[str, ...] = ()
        self._window_records: dict[str, dict[str, Any]] = {}
        self._window_decision_counts: dict[str, int] = {}
        self._scan_busy_ns_total = 0
        self._last_window_busy_ratio = 0.0
        self._frame_store_last_checkpoint_monotonic = 0.0
        self._frame_store_warm_pending: set[str] = set()
        self._frame_store_warm_waiting_ws: set[str] = set()
        self._frame_store_warm_retry_tasks: dict[str, asyncio.Task[None]] = {}
        provider_mode_enabled = str(config.canonical_market_data_provider_mode) != "REST_ONLY"
        if provider_mode_enabled:
            from app.market_data.continuity import GapTracker

            self._market_data_gap_tracker = GapTracker()
            if market_data_provider is not None:
                provider_continuity = getattr(market_data_provider, "continuity", None)
                if provider_continuity is not None:
                    self._market_data_gap_tracker = provider_continuity
            if market_data_hub is not None:
                _attach_market_data_observer(
                    market_data_hub, self._market_data_gap_tracker
                )
            elif market_data_provider is None:
                from app.market_data.hub import MarketDataHub

                market_data_hub = MarketDataHub(
                    observers=(self._market_data_gap_tracker,)
                )

        if repository is None:
            database = Database(config.db_url)
            repository = BotRepository(database)
        else:
            # Dependency-injected unit harnesses provide an already prepared repository;
            # the live constructor always reaches READY through startup().
            self._ready = True
            self._lifecycle = "READY" if self._ready else "STARTING"
        self._research_storage_router: ResearchStorageRouter | None = None
        self._research_storage_health_publisher: ResearchStorageHealthPublisher | None = (
            ResearchStorageHealthPublisher(config.research_storage_health_path)
        )
        if config.research_telemetry_storage_mode.value != "SQLITE_ONLY":
            try:
                capacity = read_capacity(Path(config.db_url.removeprefix("sqlite:///")))
            except (FileNotFoundError, OSError):
                capacity = DiskCapacitySnapshot(
                    total_bytes=0,
                    used_bytes=0,
                    free_bytes=0,
                    free_percent=0.0,
                    total_inodes=0,
                    free_inodes=0,
                    free_inode_percent=0.0,
                    db_bytes=0,
                    wal_bytes=0,
                )
            reserve = derive_reserve_bytes(
                capacity,
                log_wal_margin_bytes=config.disk_log_wal_margin_bytes,
                minimum_reserve_bytes=config.disk_min_safety_reserve_bytes,
            )
            router = ResearchStorageRouter(
                mode=config.research_telemetry_storage_mode,
                spool=ResearchSpool(
                    config.research_spool_root,
                    max_bytes=config.research_spool_max_bytes,
                    record_max_bytes=config.research_spool_record_max_bytes,
                ),
                capacity_provider=lambda: self._capacity_snapshot() or capacity,
                reserve_bytes=reserve,
                shadow_headroom_bytes=config.research_spool_shadow_headroom_bytes,
                code_sha=self._code_version,
            )
            self._research_storage_router = router
            repository = router.wrap_repository(repository)
        self._repository = repository
        set_runtime_metadata = getattr(self._repository, "set_runtime_metadata", None)
        if set_runtime_metadata is not None:
            set_runtime_metadata(
                runtime_instance_id=self._runtime_instance_id,
                config_fingerprint=self._config_fingerprint,
                model_version="climax-v1",
            )

        if market_data_provider is None:
            if scanner is None:
                scheduler = RequestScheduler(
                    max_concurrency=config.max_request_concurrency,
                    min_delay_ms=config.request_min_delay_ms,
                    jitter_min_ms=config.request_jitter_min_ms,
                    jitter_max_ms=config.request_jitter_max_ms,
                )
                self._request_scheduler = scheduler
                client = BybitClient(
                    scheduler=scheduler,
                    timeout=config.request_timeout_sec,
                )
                scanner = MarketScanner(client=client, config=config)
            self._market_data_provider = CanonicalMarketDataProvider(
                scanner=scanner,
                hub=market_data_hub,
                continuity=self._market_data_gap_tracker,
                config=config,
            )
        else:
            self._market_data_provider = market_data_provider
            provider_continuity = getattr(market_data_provider, "continuity", None)
            if provider_continuity is not None:
                self._market_data_gap_tracker = provider_continuity
            elif self._market_data_gap_tracker is None and provider_mode_enabled:
                from app.market_data.continuity import GapTracker

                self._market_data_gap_tracker = GapTracker()
            if self._market_data_gap_tracker is not None:
                setter = getattr(market_data_provider, "set_continuity", None)
                if setter is not None:
                    setter(self._market_data_gap_tracker)
            provider_hub = getattr(market_data_provider, "hub", None)
            market_data_hub = provider_hub or market_data_hub
            if market_data_hub is not None and self._market_data_gap_tracker is not None:
                _attach_market_data_observer(
                    market_data_hub, self._market_data_gap_tracker
                )
        self._market_data_hub = market_data_hub
        self._provider_evidence_publisher = (
            provider_evidence_publisher or ProviderEvidencePublisher()
        )
        # Existing runtime consumers retain the scanner compatibility surface;
        # source selection has exactly one owner.
        self._scanner = self._market_data_provider
        self._client = self._market_data_provider
        self._market_frame_store = getattr(
            self._market_data_provider, "frame_store", None
        )
        self._shadow_outcome_scheduler = ShadowOutcomeScheduler(
            client=self._client,
            repository=self._repository,
            runtime_batch_size=config.shadow_outcome_runtime_batch_size,
            legacy_batch_size=config.shadow_outcome_legacy_batch_size,
            data_gap_retry_minutes=config.shadow_outcome_data_gap_retry_minutes,
            outcome_code_version=self._code_version,
            scan_interval_sec=config.scan_interval_sec,
            runtime_min_batch=config.shadow_outcome_runtime_min_batch,
            runtime_max_batch=config.shadow_outcome_runtime_max_batch,
            capacity_margin=config.shadow_outcome_capacity_margin,
            v2_enabled=config.root_detector_shadow_v2_enabled,
        )

        self._notifier = notifier or TelegramNotifier(
            token=config.telegram_token,
            signal_chat_id=config.signal_chat_id,
            alerts_chat_id=config.alerts_chat_id,
        )
        self._state_store = EventStateStore(repository)
        self._feature_builder = FeatureBuilder()
        # Keep the established seams injectable for runtime characterization tests;
        # the shared lifecycle owns these exact production helpers.
        self._pump_detector = PumpDetector(config)
        self._pullback_tracker = PullbackTracker(config)
        self._zone_builder = ShortZoneBuilder(config)
        self._baseline_lifecycle = BaselineLifecycle(
            config,
            self._feature_builder,
            pump_detector=self._pump_detector,
            pullback_tracker=self._pullback_tracker,
            zone_builder=self._zone_builder,
        )
        self._signal_engine = SignalEngine(config)
        self._outcome_tracker = OutcomeTracker(self._market_data_provider, repository)
        self._error_throttler = ErrorThrottler(config.error_alert_ttl_sec)
        self._delivery_worker = DeliveryWorker(
            self._repository,
            self._notifier,
            self._error_throttler,
            send_watch_to_telegram=config.send_watch_to_telegram,
            send_early_drop_warning_to_telegram=config.early_drop_warning_send_to_telegram,
        )
        self._storage_incident = StorageIncidentTracker(
            cooldown_seconds=config.disk_alert_cooldown_sec
        )
        self._disk_health_state: DiskHealthState | None = None
        self._health = ServiceHealth()
        self._storage_health_last_ok: str | None = None
        if not config.derivatives_enabled:
            self._logger.warning(
                "Derivatives confirmation unavailable; derivatives_enabled=false."
            )

    async def _start_market_data_shadow(self) -> None:
        """Start the optional WS shadow without making it a readiness dependency."""
        provider = getattr(self, "_market_data_provider", None)
        if provider is not None:
            await provider.start()
            return
        hub = getattr(self, "_market_data_hub", None)
        if hub is not None:
            result = hub.start()
            if inspect.isawaitable(result):
                await result

    async def _stop_market_data_shadow(self) -> None:
        """Stop the optional WS shadow before closing runtime resources."""
        provider = getattr(self, "_market_data_provider", None)
        if provider is not None:
            await provider.stop()
            return
        hub = getattr(self, "_market_data_hub", None)
        if hub is not None:
            result = hub.stop()
            if inspect.isawaitable(result):
                await result

    def _install_shutdown_checkpoint_barrier(self) -> None:
        if getattr(self._market_data_provider, "frame_store", None) is None:
            return
        try:
            self._shutdown_checkpoint_barrier.install()
        except (NotImplementedError, RuntimeError, ValueError) as exc:
            self._logger.info(
                "SIGUSR1 checkpoint barrier unavailable | error=%s",
                type(exc).__name__,
            )

    async def _checkpoint_for_shutdown(self, *, generation: int, pid: int) -> int | None:
        provider = self._market_data_provider
        store = getattr(provider, "frame_store", None)
        checkpoint = getattr(provider, "checkpoint_frame_store", None)
        if store is None or checkpoint is None:
            return None
        written = await checkpoint(
            generation=generation,
            metadata={"pid": int(pid), "reason": "SIGUSR1_SHUTDOWN"},
        )
        if written is None:
            return None
        if int(getattr(store, "checkpoint_generation", 0)) != int(generation):
            raise RuntimeError("checkpoint generation acknowledgement mismatch")
        return int(written)

    async def _request_shutdown_checkpoint(self) -> None:
        if not self._shutdown_checkpoint_barrier.installed:
            return
        try:
            ack = await self._shutdown_checkpoint_barrier.request(timeout_sec=10.0)
        except (asyncio.TimeoutError, RuntimeError, OSError) as exc:
            self._logger.warning(
                "SIGUSR1 shutdown checkpoint failed | error=%s",
                type(exc).__name__,
            )
            return
        self._last_shutdown_checkpoint_ack = ack
        self._logger.info(
            "SIGUSR1 shutdown checkpoint acknowledged | generation=%s pid=%s symbols=%s",
            ack.generation,
            ack.pid,
            ack.symbols,
        )

    async def _frame_store_cycle_maintenance(self) -> None:
        """Checkpoint complete rings and surface SHADOW parity evidence.

        Runs on every legacy cycle when a frame store exists, so canary-1
        acceptance (ten completed scans at full store parity) is observable
        without the event-driven window loop.
        """
        provider = self._market_data_provider
        store = getattr(provider, "frame_store", None)
        if store is None:
            return
        loop = asyncio.get_running_loop()
        checkpoint = getattr(provider, "checkpoint_frame_store", None)
        if (
            checkpoint is not None
            and loop.time() - self._frame_store_last_checkpoint_monotonic
            >= self._config.market_frame_checkpoint_sec
        ):
            self._frame_store_last_checkpoint_monotonic = loop.time()
            written = await checkpoint()
            if written is not None:
                self._logger.info("Frame store checkpoint | symbols=%s", written)
        if str(self._config.market_frame_store_mode) != "SHADOW":
            return
        counters = provider.evidence_snapshot().get("counters", {})
        completeness = store.completeness_counts()
        self._logger.info(
            "Frame store shadow parity | actionable=%s/%s match=%s mismatch=%s"
            " missing=%s warmup=%s",
            completeness["complete"],
            completeness["symbols"],
            counters.get("frame_store_parity_match", 0),
            counters.get("frame_store_parity_mismatch", 0),
            counters.get("frame_store_parity_missing", 0),
            counters.get("frame_store_shadow_warmup", 0),
        )

    def _publish_provider_evidence(self) -> None:
        """Best-effort publication boundary for one completed full scan."""

        provider = getattr(self, "_market_data_provider", None)
        publisher = getattr(self, "_provider_evidence_publisher", None)
        report_builder = getattr(provider, "evidence_report", None)
        if publisher is None or not callable(report_builder):
            return
        try:
            report = report_builder(
                code_sha=self._code_version,
                strategy_fingerprint=self._strategy_config_hash,
                generated_at_utc=datetime.now(timezone.utc),
            )
            publisher.publish(report)
        except Exception as exc:  # noqa: BLE001 - evidence never gates runtime
            self._logger.warning(
                "provider evidence publication failed in runtime: %s",
                type(exc).__name__,
            )

    def _publish_research_storage_health(self) -> None:
        router = self._research_storage_router
        publisher = self._research_storage_health_publisher
        if publisher is None:
            return
        report = (
            router.health_snapshot()
            if router is not None
            else {
                "schema_version": 1,
                "storage_mode": self._config.research_telemetry_storage_mode.value,
                "spool_bytes": 0,
                "append_total": 0,
                "class_c_sqlite_writes": 0,
                "class_c_spool_writes": 0,
                "paused_total": 0,
                "dropped_total": 0,
                "errors_total": 0,
            }
        )
        report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
        try:
            publisher.publish(report)
        except Exception as exc:  # noqa: BLE001 - health is non-fatal
            self._logger.warning(
                "research storage health publication failed in runtime: %s",
                type(exc).__name__,
            )

    def _record_provider_evaluation_evidence(
        self,
        symbol: str,
        market_snapshot: object,
        decision: SignalDecision | None,
    ) -> None:
        """Record compact, side-effect-free comparison data for one evaluation."""

        recorder = getattr(self._market_data_provider, "record_evaluation_evidence", None)
        if not callable(recorder):
            return
        canonical_input = _market_snapshot_fingerprint(market_snapshot)
        canonical_decision = _decision_fingerprint(decision)
        candidate_source = getattr(
            self._market_data_provider, "candidate_source_for", lambda _symbol: None
        )(symbol)
        classification = ComparisonClassification.EXACT
        classification_getter = getattr(
            self._market_data_provider,
            "candidate_comparison_classification_for",
            None,
        )
        if candidate_source is not None and callable(classification_getter):
            classification = classification_getter(symbol)
        ticker_group = getattr(market_snapshot, "ticker_group", None)
        provenance = getattr(ticker_group, "provenance", None)
        canonical_source = getattr(provenance, "source", "REST")
        fallback_reason = getattr(provenance, "fallback_reason", None)
        if str(getattr(canonical_source, "value", canonical_source)) != "REST_FALLBACK":
            fallback_reason = None
        recorder(
            symbol=symbol,
            evaluation_time_utc=datetime.now(timezone.utc),
            canonical_source=canonical_source,
            candidate_source=candidate_source,
            classification=classification,
            canonical_input_fingerprint=canonical_input,
            candidate_input_fingerprint=canonical_input if candidate_source else None,
            canonical_decision_fingerprint=canonical_decision,
            candidate_decision_fingerprint=canonical_decision if candidate_source else None,
            fallback_reason=fallback_reason,
        )

    def _publish_market_data_universe(self, telemetry: object | None) -> None:
        """Publish REST-derived eligible symbols to the shadow control plane."""
        if telemetry is None:
            return
        symbols = getattr(telemetry, "eligible_symbols", ())
        try:
            provider = getattr(self, "_market_data_provider", None)
            if provider is not None:
                provider.update_universe(tuple(symbols))
            else:
                hub = getattr(self, "_market_data_hub", None)
                if hub is not None:
                    hub.update_universe(tuple(symbols))
        except Exception:
            getattr(self, "_logger", logging.getLogger(self.__class__.__name__)).exception(
                "MarketDataHub universe update failed; REST remains canonical"
            )

    @classmethod
    def from_files(
        cls,
        config_path: str | Path = "config.yaml",
        env_path: str | Path = ".env",
    ) -> ShortSignalBot:
        configure_logging()
        config = load_config(config_path=config_path, env_path=env_path)
        return cls(config=config)

    async def __aenter__(self) -> Self:
        try:
            await self.startup()
        except BaseException:
            await self.shutdown()
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.shutdown()

    def warmup_status(self) -> dict[str, int | str]:
        """Return bounded partial-ready coverage for diagnostics and startup."""
        return self._warmup_coverage.snapshot()

    async def startup(self) -> None:
        self._lifecycle = "STARTING"
        self._ready = False
        # Code-enforced promotion registry: a live-delivery flag without a
        # VALIDATED evidence entry in promotion_registry.json is a fatal
        # misconfiguration, checked before any storage work.
        validate_promotion_registry(self._config)
        validate_schema = getattr(self._repository, "validate_storage_schema", None)
        if validate_schema is not None:
            validate_schema(
                include_shadow_v2=self._config.root_detector_shadow_v2_enabled
            )
        storage_healthy = await self._ensure_storage_healthy("startup")
        if not storage_healthy:
            raise RuntimeError("startup storage validation failed")
        self._lifecycle = "RECOVERING"
        self._state_store.load_active(now=datetime.now(timezone.utc))
        reconcile_leases = getattr(self._repository, "reconcile_delivery_leases", None)
        if reconcile_leases is not None:
            reconcile_leases(datetime.now(timezone.utc))
        reconcile = getattr(self._repository, "reconcile_shadow_lifecycle", None)
        if reconcile is not None:
            reconciliation = reconcile(
                observed_at=datetime.now(timezone.utc),
                runtime_instance_id=self._runtime_instance_id,
                model_version="climax-v1-shadow",
            )
            if reconciliation.get("reconciliation_failed"):
                raise RuntimeError("shadow lifecycle reconciliation failed")
            elif any(reconciliation.values()):
                self._logger.warning(
                    "Shadow lifecycle reconciliation | %s", reconciliation
                )
        await self._minimum_market_readiness()
        await self._notifier.start()
        self._lag_monitor = EventLoopLagMonitor()
        await self._lag_monitor.start()
        self._delivery_worker.start()
        await self._start_market_data_shadow()
        self._install_shutdown_checkpoint_barrier()
        if (
            self._config.climax_short_enabled
            and self._config.climax_fast_monitor_enabled
        ):
            self._fast_monitor_running = True
            self._fast_monitor_task = asyncio.create_task(
                self._run_fast_monitor(), name="climax-fast-monitor"
            )
            self._logger.info(
                "Climax strategies registered | fast_monitor=enabled poll=%ss max_symbols=%s",
                self._config.climax_fast_poll_sec,
                self._config.climax_max_active_symbols,
            )
        if self._config.background_outcomes_enabled:
            await self._start_outcome_worker()
        if self._config.event_driven_scan_enabled:
            await self._prepare_event_driven_runtime()
        self._lifecycle = (
            "WARMING_UP"
            if self._config.event_driven_scan_enabled
            and self._warmup_coverage.snapshot()["state"] == "WARMING_UP"
            else "READY"
        )
        self._ready = True
        self._logger.info("Runtime %s | warmup=%s", self._lifecycle, self.warmup_status())

    async def _minimum_market_readiness(self) -> None:
        """Probe a bounded REST response before exposing scan/evaluation work."""
        fetch = getattr(self._client, "fetch_klines", None)
        if fetch is None:
            return
        try:
            frame = await asyncio.wait_for(fetch("BTCUSDT", "5", limit=2), timeout=90)
        except Exception as exc:
            raise RuntimeError("minimum market-data readiness failed") from exc
        if (
            frame is None
            or (hasattr(frame, "empty") and frame.empty)
            or (not hasattr(frame, "empty") and not frame)
        ):
            raise RuntimeError("minimum market-data readiness returned no data")

    async def flush_deliveries(self, *, timeout_sec: float = 30.0) -> int:
        """Drain a bounded outbox batch for one-shot callers."""
        return await asyncio.wait_for(
            self._delivery_worker.drain_until_idle(max_batches=10), timeout=timeout_sec
        )

    async def shutdown(self) -> None:
        self._lifecycle = "STOPPING"
        self._ready = False
        await self._request_shutdown_checkpoint()
        self._fast_monitor_running = False
        if self._outcome_worker is not None:
            await self._outcome_worker.stop()
            self._outcome_worker = None
        await self._stop_event_driven_runtime()
        await self._stop_market_data_shadow()
        if self._fast_monitor_task:
            self._fast_monitor_task.cancel()
            await asyncio.gather(self._fast_monitor_task, return_exceptions=True)
            self._fast_monitor_task = None
        await self._delivery_worker.stop()
        if self._lag_monitor is not None:
            await self._lag_monitor.stop()
            self._lag_monitor = None
        await self._notifier.close()
        self._shutdown_checkpoint_barrier.uninstall()

    async def run_cycle(
        self, *, timing: CycleTiming | None = None
    ) -> list[SignalDecision]:
        if not self._ready:
            raise RuntimeError("runtime is not READY")
        self._health.on_cycle_start()
        cycle_started_at = datetime.now(timezone.utc)
        self._shadow_rotation_id = "unknown"
        self._watch_enqueued_in_cycle = 0
        owned_timing = timing is None
        if owned_timing:
            self._cycle_sequence += 1
            timing = CycleTiming.begin(cycle_sequence=self._cycle_sequence)
        if self._lag_monitor is not None:
            self._lag_monitor.begin_window()
        timing_token = attach_cycle_timing(timing)
        cycle_status = "FAILED"
        decisions: list[SignalDecision] = []
        symbol_results: list[dict[str, object]] = []
        try:
            if not await self._ensure_storage_healthy("cycle"):
                return []
            if not self._config.background_outcomes_enabled:
                async with measure("outcomes"):
                    await self._shadow_outcome_scheduler.run_cycle(now=cycle_started_at)
            active_selection_at = datetime.now(timezone.utc)
            active_states = self._state_store.load_active(now=active_selection_at)
            async with measure("market_fetch"):
                scan_snapshot = await self._market_data_provider.build_scan_snapshot()
            snapshots = list(scan_snapshot.snapshots)
            self._publish_market_data_universe(
                getattr(self._scanner, "last_universe_telemetry", None)
            )
            prepare_rotation = getattr(
                self._repository, "prepare_market_scan_rotation", None
            )
            telemetry = getattr(self._scanner, "last_universe_telemetry", None)
            rotation_exchange_symbols = (
                list(telemetry.exchange_symbols) if telemetry is not None else []
            )
            rotation_eligible_symbols = (
                list(telemetry.eligible_symbols) if telemetry is not None else []
            )
            if prepare_rotation is not None:
                if telemetry is not None:
                    self._shadow_rotation_id = (
                        prepare_rotation(
                            rotation_started_at=cycle_started_at,
                            exchange_symbols=list(telemetry.exchange_symbols),
                            eligible_symbols=list(telemetry.eligible_symbols),
                        )
                        or "unknown"
                    )
            rotation_universe_reader = getattr(
                self._repository, "rotation_universe", None
            )
            if (
                self._shadow_rotation_id != "unknown"
                and callable(rotation_universe_reader)
            ):
                universe_reader = cast(
                    Callable[[str], tuple[list[str], list[str]] | None],
                    rotation_universe_reader,
                )
                frozen_universe = universe_reader(self._shadow_rotation_id)
                if frozen_universe is not None:
                    rotation_exchange_symbols, rotation_eligible_symbols = (
                        frozen_universe
                    )
            shortlist = self._scanner.shortlist(snapshots)
            preferred_symbols = [snapshot.symbol for snapshot in shortlist]
            symbols = preferred_symbols
            rotation_symbols = getattr(
                self._repository, "rotation_scheduled_symbols", None
            )
            if (
                self._shadow_rotation_id != "unknown"
                and telemetry is not None
                and callable(rotation_symbols)
            ):
                scheduled_reader = cast(
                    Callable[[str], set[str] | None], rotation_symbols
                )
                already_scheduled = scheduled_reader(self._shadow_rotation_id)
                if already_scheduled is not None:
                    symbols = select_rotation_batch(
                        eligible_symbols=rotation_eligible_symbols,
                        already_scheduled=already_scheduled,
                        preferred_symbols=preferred_symbols,
                        batch_size=self._config.shortlist_size,
                    )
            seen_symbols = set(symbols)
            for active_symbol in active_states:
                if active_symbol in seen_symbols:
                    continue
                symbols.append(active_symbol)
                seen_symbols.add(active_symbol)
            async with measure("market_fetch"):
                frozen_frames = (
                    await self._market_data_provider.prefetch_historical_frames(symbols)
                )
            frames = {
                symbol: frozen.frame.copy(deep=True)
                for symbol, frozen in frozen_frames.items()
            }

            for symbol in symbols:
                frame = frames.get(symbol)
                if frame is None or frame.empty:
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "terminal_status": "SCAN_FAILED",
                            "reason_code": "MARKET_DATA_INCOMPLETE",
                        }
                    )
                    continue
                async with measure("market_fetch"):
                    decision_market = (
                        await self._market_data_provider.capture_decision_snapshot(
                            symbol,
                            frame_1m=frame,
                            include_liquidity=False,
                        )
                    )
                if decision_market is None:
                    continue
                frame = decision_market.frame_1m.frame
                try:
                    async with measure("strategy"), self._symbol_mutations.mutation(symbol):
                        load_active_symbol = getattr(
                            self._state_store, "load_active_symbol", None
                        )
                        state = (
                            load_active_symbol(symbol, now=active_selection_at)
                            if load_active_symbol is not None
                            else self._state_store.load(symbol)
                        )
                        if (
                            symbol not in active_states
                            or state is not None
                            and (
                                state.state in {EventStatus.IDLE, EventStatus.EXPIRED}
                                or (
                                    state.expires_at is not None
                                    and state.expires_at <= active_selection_at
                                )
                            )
                        ):
                            state = None
                        decision, updated_state = await self._process_symbol(
                            symbol, frame, state, market_snapshot=decision_market
                        )
                        if updated_state is not None:
                            self._state_store.save(updated_state)
                    with measure_sync("research"):
                        self._record_provider_evaluation_evidence(
                            symbol, decision_market, decision
                        )
                except Exception as exc:  # noqa: BLE001 - isolate one symbol from the cycle
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "terminal_status": "SCAN_FAILED",
                            "reason_code": "SCAN_EXCEPTION",
                            "details": {"error_code": type(exc).__name__},
                        }
                    )
                    await self._handle_error(f"symbol:{symbol}", exc)
                    continue
                symbol_results.append(
                    {
                        "symbol": symbol,
                        "terminal_status": "SCANNED_OK",
                        "reason_code": "SCANNED_OK",
                    }
                )
                if decision is not None:
                    decisions.append(decision)
                    if decision.actionable:
                        self._health.on_signal()

            if self._config.background_outcomes_enabled:
                updated_outcomes = 0
            else:
                async with measure("outcomes"):
                    updated_outcomes = await self.update_outcomes()
            universe = getattr(self._scanner, "last_universe_telemetry", None)
            record_coverage = getattr(
                self._repository, "record_market_scan_cycle", None
            )
            if universe is not None and record_coverage is not None:
                with measure_sync("research"):
                    coverage_result = record_coverage(
                    cycle_started_at=cycle_started_at,
                    cycle_completed_at=datetime.now(timezone.utc),
                    exchange_symbols=rotation_exchange_symbols,
                    eligible_symbols=rotation_eligible_symbols,
                    excluded=[
                        (symbol, reason)
                        for symbol, reason in universe.excluded
                        if str(symbol).upper()
                        in {str(item).upper() for item in rotation_exchange_symbols}
                    ],
                    scheduled_symbols=list(symbols),
                    symbol_results=symbol_results,
                    candidate_symbols=len(decisions),
                    evaluated_symbols=len(symbol_results),
                )
                if coverage_result is None:
                    await self._handle_error(
                        "market-coverage-telemetry",
                        RuntimeError("coverage ledger write returned no result"),
                    )
            self._logger.info(
                "Cycle complete | shortlist=%s symbols=%s signals=%s outcomes=%s",
                len(shortlist),
                len(symbols),
                len([d for d in decisions if d.actionable]),
                updated_outcomes,
            )
            async with measure("research"):
                self._publish_research_storage_health()
                self._publish_provider_evidence()
                await self._frame_store_cycle_maintenance()
            cycle_status = "COMPLETED"
            return decisions
        except Exception as exc:  # noqa: BLE001 - cycle boundary must preserve runtime loop
            await self._handle_error("cycle", exc)
            return []
        finally:
            self._health.on_cycle_finish()
            timing.finish(
                status=cycle_status,
                event_loop_lag_ms=(
                    self._lag_monitor.latest_lag_ms()
                    if self._lag_monitor is not None
                    else 0.0
                ),
            )
            detach_cycle_timing(timing_token)
            if owned_timing:
                self._finalize_cycle_timing(timing)

    def _finalize_cycle_timing(
        self,
        timing: CycleTiming,
        *,
        cycle_overrun_ms: float = 0.0,
        skipped_ticks: int = 0,
    ) -> None:
        """Emit one stable timing line and overwrite the bounded evidence doc."""
        if not timing.completed_ns:
            timing.finish(status="FAILED")
        timing.cycle_overrun_ms = max(0.0, float(cycle_overrun_ms))
        timing.skipped_ticks = max(0, int(skipped_ticks))
        metrics = timing.metrics_ms()
        self._logger.info(
            "Runtime cycle timing | cycle_sequence=%s status=%s %s",
            timing.cycle_sequence,
            timing.status,
            " ".join(f"{key}={metrics[key]:.3f}" for key in METRIC_KEYS),
        )
        try:
            publish_evidence(
                self._config.runtime_timing_evidence_path,
                build_evidence_document(
                    timing,
                    runtime_instance_id=self._runtime_instance_id,
                    code_version=self._code_version,
                ),
            )
        except Exception as exc:  # noqa: BLE001 - evidence never gates signal behavior
            if self._error_throttler.should_send("runtime_timing_evidence"):
                self._logger.warning(
                    "Runtime timing evidence publication failed | error=%s",
                    type(exc).__name__,
                )

    async def run_forever(self) -> None:
        if getattr(self._config, "event_driven_scan_enabled", False):
            await self.run_event_driven_forever()
            return
        loop = asyncio.get_running_loop()
        interval_sec = float(self._config.scan_interval_sec)
        scheduled_deadline = loop.time()
        cycle_sequence = 0
        while True:
            cycle_sequence += 1
            timing = CycleTiming.begin(cycle_sequence=cycle_sequence)
            await self.run_cycle(timing=timing)
            finished_at = loop.time()
            schedule = next_scan_deadline(
                scheduled_deadline=scheduled_deadline,
                finished_at=finished_at,
                interval_sec=interval_sec,
            )
            if schedule.cycle_overrun_ms > 0:
                self._logger.warning(
                    "Cycle overrun | cycle_sequence=%s cycle_overrun_ms=%.3f "
                    "skipped_ticks=%s interval_ms=%.3f",
                    cycle_sequence,
                    schedule.cycle_overrun_ms,
                    schedule.skipped_ticks,
                    interval_sec * 1000,
                )
            scheduled_deadline = schedule.next_deadline
            self._finalize_cycle_timing(
                timing,
                cycle_overrun_ms=schedule.cycle_overrun_ms,
                skipped_ticks=schedule.skipped_ticks,
            )
            await asyncio.sleep(max(0.0, scheduled_deadline - loop.time()))

    def _latest_event_loop_lag_ms(self) -> float | None:
        if self._lag_monitor is None:
            return None
        return self._lag_monitor.latest_lag_ms()

    # ------------------------------------------------- event-driven live scan

    async def _start_outcome_worker(self) -> None:
        if self._outcome_worker is not None:
            return
        self._outcome_worker = OutcomeWorker(
            shadow_scheduler=self._shadow_outcome_scheduler,
            outcome_tracker=self._outcome_tracker,
            signal_batch_size=self._config.outcome_worker_signal_batch_size,
            observation_batch_size=self._config.outcome_worker_observation_batch_size,
            idle_poll_sec=self._config.outcome_worker_idle_poll_sec,
        )
        await self._outcome_worker.start()
        self._logger.info(
            "Background outcome worker started | signal_batch=%s observation_batch=%s",
            self._config.outcome_worker_signal_batch_size,
            self._config.outcome_worker_observation_batch_size,
        )

    async def _prepare_event_driven_runtime(self) -> None:
        """Build the coordinator, warm the frame store, and seed P0 state."""
        if self._scan_coordinator is not None:
            return
        from app.market_data.planning import plan_topics

        provider = self._market_data_provider
        store = getattr(provider, "frame_store", None)
        hub = getattr(provider, "hub", None)
        if store is None or hub is None:
            raise RuntimeError(
                "event_driven_scan_enabled requires a frame store and WebSocket Hub"
            )
        # Restore technical seed data before the first universe publish.  The
        # subsequent WS subscription then supplies the current epoch without
        # attaching observers a second time.
        restore = getattr(provider, "restore_frame_store", None)
        if restore is not None:
            restored = restore()
            if restored:
                self._logger.info("Frame store restore | %s", restored)
        telemetry = await self._refresh_event_universe(startup=True)
        # Fail readiness instead of silently dropping symbols when the topic
        # planner cannot cover the eligible universe.
        plan_topics(
            telemetry.eligible_symbols,
            max_connections=getattr(hub, "_max_connections", 4),
        )
        now = datetime.now(timezone.utc)
        active = tuple(self._state_store.load_active(now=now))
        if getattr(store, "warm_symbols", None) is not None:
            warm_targets = sorted(
                {str(symbol).upper() for symbol in telemetry.eligible_symbols}
                | {str(symbol).upper() for symbol in active}
            )
            ready_targets = [
                symbol for symbol in warm_targets if store.is_complete(symbol, now)
            ]
            self._warmup_coverage.begin(warm_targets, ready=ready_targets)
            if self._warmup_coverage.snapshot()["state"] == "WARMING_UP":
                self._lifecycle = "WARMING_UP"
            # Warmup is bounded REST work (2 requests/scheduler slot per
            # symbol). Runtime readiness exposes partial counts while every
            # decision path remains gated on actionable frames.
            self._schedule_frame_store_warmup(warm_targets)
        self._research_queue = ResearchQueue(
            maxsize=2000,
            batch_size=50,
            on_error=lambda exc, name: self._logger.warning(
                "research queue write failed | name=%s error=%s", name, type(exc).__name__
            ),
        )
        self._research_queue.start()
        self._scan_coordinator = ScanCoordinator(
            p0_deadline_sec=self._config.p0_scan_deadline_sec,
            p1_deadline_sec=self._config.p1_scan_deadline_sec,
        )
        # Seed the P0 pool from persisted active states before READY returns.
        if self._config.climax_short_enabled:
            for symbol in active:
                state = self._state_store.load(symbol)
                if state is not None:
                    self._track_climax_candidate(state, now)
        for index in range(self._config.scan_evaluation_consumers):
            self._scan_consumers.append(
                asyncio.create_task(
                    self._scan_consumer(), name=f"scan-evaluation-consumer-{index}"
                )
            )
        self._logger.info(
            "Event-driven scan runtime ready | consumers=%s store_symbols=%s",
            self._config.scan_evaluation_consumers,
            len(store.symbols()),
        )

    def _warmup_frame(self, store: object, symbol: str) -> object | None:
        peek = getattr(store, "peek_snapshot", None)
        if not callable(peek):
            return None
        try:
            return peek(symbol, datetime.now(timezone.utc))
        except Exception:  # noqa: BLE001 - readiness is best effort
            return None

    @staticmethod
    def _warmup_result_has_rest_failure(result: object) -> bool:
        if not isinstance(result, dict):
            return True
        return any(
            bool(result.get(key))
            for key in (
                "error",
                "failed",
                "candle_failed",
                "candle_timed_out",
                "oi_failed",
                "oi_timed_out",
            )
        )

    @staticmethod
    def _warmup_frame_waits_for_ws(frame: object | None) -> bool:
        return (
            frame is not None
            and not bool(getattr(frame, "actionable", False))
            and getattr(frame, "stale_reason", None) in _FRAME_STORE_WS_WAITING_REASONS
        )

    def _reconcile_frame_store_warmup(self) -> None:
        """Promote symbols when WS makes a seeded frame actionable."""
        provider = self._market_data_provider
        store = getattr(provider, "frame_store", None)
        if store is None:
            return
        for symbol in self._warmup_coverage.target_symbols:
            frame = self._warmup_frame(store, symbol)
            if frame is not None and bool(getattr(frame, "actionable", False)):
                self._warmup_coverage.mark_ready(symbol)
                self._frame_store_warm_waiting_ws.discard(symbol)
            elif symbol in self._frame_store_warm_waiting_ws:
                self._warmup_coverage.mark_waiting(symbol)
        if (
            self._ready
            and self._lifecycle == "WARMING_UP"
            and self._warmup_coverage.snapshot()["state"] == "READY"
        ):
            self._lifecycle = "READY"

    def _schedule_frame_store_retry(self, symbol: str) -> None:
        if (
            symbol in self._frame_store_warm_waiting_ws
            or symbol in self._frame_store_warm_retry_tasks
        ):
            return
        loop = asyncio.get_running_loop()

        async def _retry() -> None:
            try:
                await asyncio.sleep(FRAME_STORE_WARMUP_RETRY_BACKOFF_SEC)
            except asyncio.CancelledError:
                return
            finally:
                self._frame_store_warm_retry_tasks.pop(symbol, None)
            if symbol in self._frame_store_warm_waiting_ws:
                return
            self._frame_store_warm_pending.add(symbol)
            self._ensure_frame_store_warm_task()

        self._frame_store_warm_retry_tasks[symbol] = loop.create_task(
            _retry(), name=f"frame-store-warmup-retry-{symbol}"
        )

    def _ensure_frame_store_warm_task(self) -> None:
        task = getattr(self, "_frame_store_warm_task", None)
        if task is not None and not task.done():
            return
        loop = asyncio.get_running_loop()
        provider = self._market_data_provider
        store = getattr(provider, "frame_store", None)
        if store is None:
            return

        async def _drain() -> None:
            while self._frame_store_warm_pending:
                symbol = min(self._frame_store_warm_pending)
                self._frame_store_warm_pending.discard(symbol)
                try:
                    warmup_priority = (
                        RequestPriority.RECOVERY
                        if hasattr(RequestPriority, "RECOVERY")
                        else RequestPriority.P1
                    )
                    result = await store.warm_symbols(
                        provider.client,
                        [symbol],
                        priority=warmup_priority,
                        timeout_sec=FRAME_STORE_WARMUP_ATTEMPT_TIMEOUT_SEC,
                    )
                except Exception as exc:  # noqa: BLE001 - warmup is best effort
                    result = {"error": type(exc).__name__}
                    self._logger.warning(
                        "frame store warmup failed | symbol=%s error=%s",
                        symbol,
                        type(exc).__name__,
                    )
                frame = self._warmup_frame(store, symbol)
                if frame is not None and bool(getattr(frame, "actionable", False)):
                    self._warmup_coverage.mark_ready(symbol)
                    self._frame_store_warm_waiting_ws.discard(symbol)
                elif self._warmup_result_has_rest_failure(result):
                    self._warmup_coverage.mark_failed(symbol)
                    self._frame_store_warm_waiting_ws.discard(symbol)
                    self._schedule_frame_store_retry(symbol)
                elif self._warmup_frame_waits_for_ws(frame):
                    self._warmup_coverage.mark_waiting(symbol)
                    self._frame_store_warm_waiting_ws.add(symbol)
                else:
                    self._warmup_coverage.mark_failed(symbol)
                    self._frame_store_warm_waiting_ws.discard(symbol)
                    self._schedule_frame_store_retry(symbol)
                self._reconcile_frame_store_warmup()
                self._logger.info(
                    "Frame store warmup | symbol=%s result=%s coverage=%s",
                    symbol,
                    result,
                    self.warmup_status(),
                )

        self._frame_store_warm_task = loop.create_task(
            _drain(), name="frame-store-warmup"
        )

    def _schedule_frame_store_warmup(self, symbols: list[str]) -> None:
        """Queue bounded, deduplicated per-symbol warmup."""
        provider = self._market_data_provider
        store = getattr(provider, "frame_store", None)
        if store is None:
            return
        self._warmup_coverage.add_targets(symbols)
        for raw_symbol in symbols:
            symbol = str(raw_symbol).upper()
            if (
                symbol in self._frame_store_warm_waiting_ws
                or symbol in self._frame_store_warm_retry_tasks
            ):
                continue
            frame = self._warmup_frame(store, symbol)
            if frame is not None and bool(getattr(frame, "actionable", False)):
                self._warmup_coverage.mark_ready(symbol)
                continue
            try:
                complete = bool(store.is_complete(symbol))
            except Exception:  # noqa: BLE001 - readiness is best effort
                complete = False
            if not complete or not self._warmup_frame_waits_for_ws(frame):
                self._frame_store_warm_pending.add(symbol)
            else:
                self._warmup_coverage.mark_waiting(symbol)
                self._frame_store_warm_waiting_ws.add(symbol)
        self._ensure_frame_store_warm_task()

    async def _stop_event_driven_runtime(self) -> None:
        warm_task = getattr(self, "_frame_store_warm_task", None)
        if warm_task is not None:
            warm_task.cancel()
            self._frame_store_warm_task = None
        retry_tasks = tuple(self._frame_store_warm_retry_tasks.values())
        for retry_task in retry_tasks:
            retry_task.cancel()
        if retry_tasks:
            await asyncio.gather(*retry_tasks, return_exceptions=True)
        self._frame_store_warm_retry_tasks.clear()
        self._frame_store_warm_pending.clear()
        self._frame_store_warm_waiting_ws.clear()
        if self._research_queue is not None:
            await self._research_queue.stop()
            self._research_queue = None
        if self._scan_coordinator is not None:
            await self._scan_coordinator.stop()
        consumers, self._scan_consumers = self._scan_consumers, []
        for consumer in consumers:
            consumer.cancel()
        if consumers:
            await asyncio.gather(*consumers, return_exceptions=True)
        self._scan_coordinator = None

    async def _refresh_event_universe(self, *, startup: bool = False) -> Any:
        """Refresh instruments + eligible universe (P1) and publish to the Hub."""
        provider = self._market_data_provider
        snapshots = await provider.fetch_market_snapshots()
        telemetry = getattr(provider, "last_universe_telemetry", None)
        if telemetry is None:
            raise RuntimeError("universe refresh produced no telemetry")
        self._universe_refreshed_at_monotonic = time.monotonic()
        self._event_universe_snapshots = list(snapshots)
        self._event_eligible_symbols = tuple(
            str(symbol).upper() for symbol in telemetry.eligible_symbols
        )
        # The Hub/store subscription is eligible ∪ persisted active symbols:
        # active events must keep streaming even after leaving the universe.
        active = self._state_store.load_active(now=datetime.now(timezone.utc))
        subscribed = sorted(
            {str(symbol).upper() for symbol in telemetry.eligible_symbols}
            | {str(symbol).upper() for symbol in active}
        )
        publish = getattr(provider, "update_universe", None)
        if publish is not None:
            publish(tuple(subscribed))
        if startup:
            self._logger.info(
                "Event-driven universe ready | exchange=%s eligible=%s",
                len(telemetry.exchange_symbols),
                len(telemetry.eligible_symbols),
            )
        return telemetry

    def _hub_market_snapshots(self) -> list[MarketSnapshot]:
        """Current-minute shortlist input from hub ticker state, not REST."""
        hub = getattr(self._market_data_provider, "hub", None)
        if hub is None:
            return []
        by_symbol = {
            snapshot.symbol: snapshot
            for snapshot in self._event_universe_snapshots
        }
        snapshots: list[MarketSnapshot] = []
        for symbol in self._event_eligible_symbols:
            hub_snapshot = hub.get_snapshot(symbol)
            if hub_snapshot is not None and all(
                getattr(hub_snapshot, name) is not None
                for name in (
                    "last_price",
                    "price_24h_pcnt",
                    "turnover_24h",
                    "volume_24h",
                    "mark_price",
                    "open_interest",
                )
            ):
                snapshots.append(
                    MarketSnapshot(
                        symbol=symbol,
                        last_price=float(hub_snapshot.last_price),
                        price_24h_pct=float(hub_snapshot.price_24h_pcnt) * 100,
                        turnover_24h=float(hub_snapshot.turnover_24h),
                        volume_24h=float(hub_snapshot.volume_24h),
                        mark_price=float(hub_snapshot.mark_price),
                        open_interest=float(hub_snapshot.open_interest),
                        timestamp=hub_snapshot.received_at,
                    )
                )
                continue
            rest = by_symbol.get(symbol)
            if rest is not None:
                snapshots.append(rest)
        return snapshots

    async def run_event_driven_forever(self) -> None:
        """Minute-window event-driven loop; one process, one state owner."""
        await self._prepare_event_driven_runtime()
        coordinator = self._scan_coordinator
        assert coordinator is not None
        interval_sec = float(self._config.scan_interval_sec)
        grace_sec = float(self._config.scan_window_grace_sec)
        loop = asyncio.get_running_loop()
        window_sequence = 0
        next_checkpoint_monotonic = (
            loop.time() + float(self._config.market_frame_checkpoint_sec)
        )
        while True:
            now_utc = datetime.now(timezone.utc)
            boundary = (
                now_utc.replace(second=0, microsecond=0)
                + timedelta(seconds=interval_sec)
            )
            open_at = loop.time() + max(
                grace_sec, (boundary - now_utc).total_seconds()
            )
            await asyncio.sleep(max(0.0, open_at - loop.time()))
            window_sequence += 1
            window_id = f"{boundary.strftime('%Y%m%dT%H%M%SZ')}-{window_sequence}"
            try:
                if (
                    loop.time() - self._universe_refreshed_at_monotonic >= 900.0
                ):
                    await self._refresh_event_universe()
                active_states = self._state_store.load_active(now=boundary)
                active_symbols = tuple(
                    sorted({str(symbol).upper() for symbol in active_states})
                )
                # Re-publish eligible + active every window: new events start
                # streaming immediately, without waiting for the 900-second
                # instrument refresh.  The Hub coalesces unchanged universes.
                subscribed = sorted(
                    set(self._event_eligible_symbols) | set(active_symbols)
                )
                publish = getattr(self._market_data_provider, "update_universe", None)
                if publish is not None:
                    publish(tuple(subscribed))
                store = getattr(self._market_data_provider, "frame_store", None)
                if store is not None:
                    new_symbols = sorted(
                        set(subscribed) - set(store.symbols())
                    )
                    if new_symbols:
                        self._schedule_frame_store_warmup(new_symbols)
                self._reconcile_frame_store_warmup()
                shortlist = self._market_data_provider.shortlist(
                    self._hub_market_snapshots()
                )
                shortlist_symbols = (
                    _actionable_canonical_symbols(
                        store,
                        tuple(snapshot.symbol for snapshot in shortlist),
                        boundary,
                    )
                    if store is not None
                    else ()
                )
                active_scan_symbols = (
                    _actionable_canonical_symbols(store, active_symbols, boundary)
                    if store is not None
                    else ()
                )
                scheduled = coordinator.open_window(
                    window_id,
                    boundary,
                    active_symbols=active_scan_symbols,
                    shortlist_symbols=shortlist_symbols,
                )
                self._window_records[window_id] = {"opened_at": boundary}
                window_started_monotonic = loop.time()
                self._logger.info(
                    "Event window opened | window=%s scheduled=%s p0=%s shortlist=%s",
                    window_id,
                    scheduled,
                    len(active_scan_symbols),
                    len(shortlist_symbols),
                )
                try:
                    await asyncio.wait_for(
                        coordinator.wait_window_complete(window_id),
                        timeout=self._config.p1_scan_deadline_sec
                        + self._config.scan_window_grace_sec
                        + 60.0,
                    )
                except asyncio.TimeoutError:
                    # The watchdog only finalizes expired tasks and reports
                    # missing terminal accounting; it never launches a scan.
                    coordinator.expire_pending()
                    missing = coordinator.missing_tasks(window_id)
                    if missing:
                        self._logger.warning(
                            "Coverage watchdog | window=%s missing_terminals=%s",
                            window_id,
                            len(missing),
                        )
                try:
                    await asyncio.wait_for(
                        coordinator.wait_window_complete(window_id),
                        timeout=interval_sec * 2.0,
                    )
                except asyncio.TimeoutError:
                    # Second wait is bounded so a never-completing window
                    # (e.g. empty scheduling that escapes the coordinator
                    # guard) cannot stall the scan loop indefinitely.
                    self._logger.error(
                        "Stuck scan window | window=%s missing_terminals=%s"
                        " bounded wait expired after %.1fs; continuing loop",
                        window_id,
                        len(coordinator.missing_tasks(window_id)),
                        interval_sec * 2.0,
                    )
                busy_ns = self._scan_busy_ns_total
                self._scan_busy_ns_total = 0
                elapsed_ms = max(1.0, (loop.time() - window_started_monotonic) * 1000.0)
                consumers = max(1, self._config.scan_evaluation_consumers)
                self._last_window_busy_ratio = round(
                    min(1.0, (busy_ns / 1_000_000) / (elapsed_ms * consumers)), 4
                )
                await self._finalize_event_window(window_id)
                self._publish_window_evidence(coordinator, window_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - window boundary must survive
                await self._handle_error(f"event-window:{window_id}", exc)
            self._coverage_watchdog_check(coordinator)
            if loop.time() >= next_checkpoint_monotonic:
                next_checkpoint_monotonic = (
                    loop.time() + float(self._config.market_frame_checkpoint_sec)
                )
                checkpoint = getattr(
                    self._market_data_provider, "checkpoint_frame_store", None
                )
                if checkpoint is not None:
                    written = await checkpoint()
                    if written is not None:
                        self._logger.info(
                            "Frame store checkpoint | symbols=%s", written
                        )

    def _publish_window_evidence(
        self, coordinator: ScanCoordinator, window_id: str
    ) -> None:
        """Bounded overwrite-in-place window evidence; never gates trading."""
        store = getattr(self._market_data_provider, "frame_store", None)
        document: dict[str, Any] = {
            "schema_version": 2,
            "timing_semantics": "window_event_driven_v1",
            "runtime_instance_id": self._runtime_instance_id,
            "code_version": self._code_version,
            "window_id": window_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "coordinator": coordinator.evidence_document(),
            "frame_store": (
                dict(store.counters) if store is not None else {}
            ),
            "provider_counters": {
                key: value
                for key, value in self._market_data_provider.evidence_snapshot()[
                    "counters"
                ].items()
                if key.startswith("frame_store")
            },
            "outcome_worker": (
                self._outcome_worker.stats()
                if self._outcome_worker is not None
                else None
            ),
            "request_scheduler": (
                self._request_scheduler.stats_snapshot()
                if self._request_scheduler is not None
                else {}
            ),
            "research_queue": (
                self._research_queue.stats()
                if self._research_queue is not None
                else {}
            ),
            "delivery": self._delivery_worker.stats(),
            "workers": {
                "evaluation_consumers": self._config.scan_evaluation_consumers,
                "busy_ratio_last_window": self._last_window_busy_ratio,
            },
            "diagnostics": {
                "event_loop_lag_ms": self._latest_event_loop_lag_ms() or 0.0,
                "warmup": self.warmup_status(),
                "pipeline_counters": {
                    "frame_store": dict(store.counters) if store is not None else {},
                    "coordinator_terminal": coordinator.status_counts(),
                    "coordinator_outstanding": coordinator.outstanding_count(),
                    "coordinator_p0_deadline_misses": coordinator.p0_deadline_misses,
                },
            },
        }
        try:
            publish_evidence(
                self._config.runtime_timing_evidence_path, document
            )
        except Exception as exc:  # noqa: BLE001 - evidence never gates runtime
            if self._error_throttler.should_send("window_evidence"):
                self._logger.warning(
                    "Window evidence publication failed | error=%s",
                    type(exc).__name__,
                )

    def _coverage_watchdog_check(self, coordinator: ScanCoordinator) -> None:
        """Report missing terminal tasks; never launch a second scan."""
        for window_id in coordinator.recent_windows():
            missing = coordinator.missing_tasks(window_id)
            if missing:
                self._logger.warning(
                    "Coverage watchdog | window=%s missing_terminals=%s symbols=%s",
                    window_id,
                    len(missing),
                    ",".join(missing[:5]),
                )

    async def _scan_consumer(self) -> None:
        coordinator = self._scan_coordinator
        assert coordinator is not None
        while True:
            task = await coordinator.acquire()
            if task is None:
                return
            busy_started = time.perf_counter_ns()
            try:
                status = await self._execute_scan_task(task)
            except asyncio.CancelledError:
                # Mid-flight cancellation still owes terminal accounting.
                coordinator.record_terminal(task, ScanTerminalStatus.SCAN_ERROR)
                raise
            except Exception as exc:  # noqa: BLE001 - one task cannot kill consumers
                self._logger.exception(
                    "Scan task crash | window=%s symbol=%s error=%s",
                    task.window_id,
                    task.symbol,
                    type(exc).__name__,
                )
                status = ScanTerminalStatus.SCAN_ERROR
            self._scan_busy_ns_total += time.perf_counter_ns() - busy_started
            coordinator.record_terminal(task, status)

    async def _execute_scan_task(self, task: ScanTask) -> ScanTerminalStatus:
        provider = self._market_data_provider
        store = provider.frame_store
        coordinator = self._scan_coordinator
        assert store is not None and coordinator is not None
        priority = (
            RequestPriority.P0
            if task.priority is ScanPriority.P0
            else RequestPriority.P1
        )
        spans: dict[str, float] = {}
        stage_started = time.perf_counter()
        async with self._symbol_mutations.mutation(task.symbol):
            frame = await provider.decision_market_frame(
                task.symbol,
                priority=priority,
                deadline_monotonic=task.deadline_monotonic,
            )
            if frame is None or not frame.actionable:
                stale_reason = frame.stale_reason if frame else "NO_FRAME"
                self._logger.debug(
                    "Scan task data gate | window=%s symbol=%s reason=%s",
                    task.window_id,
                    task.symbol,
                    stale_reason,
                )
                if stale_reason in {
                    "WARMING",
                    "INCOMPLETE_RING",
                    "OI_HISTORY_INCOMPLETE",
                    "NO_FRAME",
                }:
                    return ScanTerminalStatus.INCOMPLETE_DATA
                return ScanTerminalStatus.STALE_DATA
            derivatives = store.derivatives_payload(task.symbol) or {}
            if (
                self._config.derivatives_enabled
                and derivatives.get("derivatives_status") == "MISSING"
            ):
                # Preserve existing data-quality blocking semantics: missing
                # enrichment is INCOMPLETE_DATA, never a reinterpretation as
                # NO_SETUP.
                return ScanTerminalStatus.INCOMPLETE_DATA
            decision_market = await provider.capture_decision_snapshot(
                task.symbol,
                frame_1m=frame.frame_1m,
                include_liquidity=False,
                derivatives=derivatives,
            )
            if decision_market is None:
                return ScanTerminalStatus.INCOMPLETE_DATA
            validation_mode = self._config.market_data_validation_mode
            if validation_mode.value != "OFF":
                try:
                    validation = MarketDataValidator(
                        derivatives_enabled=self._config.derivatives_enabled,
                        now=decision_market.captured_at_utc,
                    ).validate(
                        task.symbol,
                        decision_market.frame_1m.frame,
                        captured_at=decision_market.captured_at_utc,
                        oi_observed_at=_observed_at(derivatives.get("oi_observed_at")),
                        funding_observed_at=_observed_at(derivatives.get("funding_observed_at")),
                        required_sources=("oi", "funding"),
                    )
                except Exception as exc:  # noqa: BLE001 - shadow must be observational
                    provider.record_decision_evidence("market_validation_error", error=type(exc).__name__)
                    if validation_mode.value == "ENFORCED":
                        return ScanTerminalStatus.INCOMPLETE_DATA
                    self._logger.warning("market-data validation shadow failed | error=%s", type(exc).__name__)
                else:
                    provider.record_decision_evidence(
                        "market_validation", quality=validation.overall.value,
                    )
                    if validation_mode.value == "SHADOW" and validation.overall is not MarketDataQuality.ACTIONABLE:
                        self._logger.warning("market-data validation shadow invalid | symbol=%s quality=%s", task.symbol, validation.overall.value)
                    elif validation_mode.value == "ENFORCED" and validation.overall is not MarketDataQuality.ACTIONABLE:
                        if validation.overall is MarketDataQuality.STALE:
                            return ScanTerminalStatus.STALE_DATA
                        return ScanTerminalStatus.INCOMPLETE_DATA
            spans["feature_ms"] = (time.perf_counter() - stage_started) * 1000.0
            stage_started = time.perf_counter()
            active_selection_at = datetime.now(timezone.utc)
            load_active_symbol = getattr(
                self._state_store, "load_active_symbol", None
            )
            state = (
                load_active_symbol(task.symbol, now=active_selection_at)
                if load_active_symbol is not None
                else self._state_store.load(task.symbol)
            )
            if state is not None and (
                state.state in {EventStatus.IDLE, EventStatus.EXPIRED}
                or (
                    state.expires_at is not None
                    and state.expires_at <= active_selection_at
                )
            ):
                state = None
            task_timing = CycleTiming.begin(cycle_sequence=0)
            timing_token = attach_cycle_timing(task_timing)
            try:
                decision, updated_state = await self._process_symbol(
                    task.symbol,
                    decision_market.frame_1m.frame,
                    state,
                    market_snapshot=decision_market,
                )
            except RequestDeadlineExceeded:
                detach_cycle_timing(timing_token)
                return ScanTerminalStatus.DEADLINE_EXCEEDED
            except Exception as exc:  # noqa: BLE001 - classify per-symbol failures
                detach_cycle_timing(timing_token)
                message = str(exc).lower()
                if (
                    "rate limit" in message
                    or "10006" in message
                    or "too many visits" in message
                ):
                    return ScanTerminalStatus.RATE_LIMITED
                raise
            storage_ms = task_timing.components_ns["storage"] / 1_000_000
            detach_cycle_timing(timing_token)
            process_ms = (time.perf_counter() - stage_started) * 1000.0
            spans["strategy_ms"] = max(0.0, process_ms - storage_ms)
            spans["db_write_ms"] = storage_ms
            spans["outbox_enqueue_ms"] = storage_ms if decision is not None else 0.0
            stage_started = time.perf_counter()
            if updated_state is not None:
                self._state_store.save(updated_state)
            spans["db_write_ms"] += (time.perf_counter() - stage_started) * 1000.0
            self._record_provider_evaluation_evidence(
                task.symbol, decision_market, decision
            )
            coordinator.record_task_spans(task, spans)
            if decision is not None and decision.actionable:
                self._health.on_signal()
                self._window_decision_counts[task.window_id] = (
                    self._window_decision_counts.get(task.window_id, 0) + 1
                )
            return (
                ScanTerminalStatus.SCANNED_OK
                if decision is not None
                else ScanTerminalStatus.NO_SETUP
            )

    # Coverage vocabulary conversion: the coordinator keeps its own seven
    # terminal statuses; the persisted coverage tables retain the established
    # SCANNED_OK/SCAN_FAILED reason-code vocabulary.
    _COVERAGE_STATUS_MAP = {
        ScanTerminalStatus.SCANNED_OK: ("SCANNED_OK", "SCANNED_OK"),
        ScanTerminalStatus.NO_SETUP: ("SCANNED_OK", "NO_SETUP"),
        ScanTerminalStatus.INCOMPLETE_DATA: ("SCAN_FAILED", "MARKET_DATA_INCOMPLETE"),
        ScanTerminalStatus.STALE_DATA: ("SCAN_FAILED", "STALE_MARKET_DATA"),
        ScanTerminalStatus.RATE_LIMITED: ("SCAN_FAILED", "RATE_LIMITED"),
        ScanTerminalStatus.DEADLINE_EXCEEDED: ("SCAN_FAILED", "DEADLINE_EXCEEDED"),
        ScanTerminalStatus.SCAN_ERROR: ("SCAN_FAILED", "SCAN_EXCEPTION"),
    }

    async def _finalize_event_window(self, window_id: str) -> None:
        """Persist one rotation/cycle coverage record per completed window."""
        record = self._window_records.pop(window_id, None)
        decisions = self._window_decision_counts.pop(window_id, 0)
        coordinator = self._scan_coordinator
        if record is None or coordinator is None:
            return
        telemetry = getattr(
            self._market_data_provider, "last_universe_telemetry", None
        )
        if telemetry is None:
            return
        started = record["opened_at"]
        terminal = coordinator.terminal_statuses(window_id)
        symbol_results = [
            {
                "symbol": symbol,
                "terminal_status": self._COVERAGE_STATUS_MAP[status][0],
                "reason_code": self._COVERAGE_STATUS_MAP[status][1],
            }
            for symbol, status in sorted(terminal.items())
        ]
        prepare_rotation = getattr(
            self._repository, "prepare_market_scan_rotation", None
        )
        if prepare_rotation is not None:
            self._shadow_rotation_id = (
                prepare_rotation(
                    rotation_started_at=started,
                    exchange_symbols=list(telemetry.exchange_symbols),
                    eligible_symbols=list(telemetry.eligible_symbols),
                )
                or "unknown"
            )
        record_coverage = getattr(self._repository, "record_market_scan_cycle", None)
        if record_coverage is None:
            return

        def _write_coverage() -> Any:
            return record_coverage(
                cycle_started_at=started,
                cycle_completed_at=datetime.now(timezone.utc),
                exchange_symbols=list(telemetry.exchange_symbols),
                eligible_symbols=list(telemetry.eligible_symbols),
                excluded=list(telemetry.excluded),
                scheduled_symbols=[row["symbol"] for row in symbol_results],
                symbol_results=symbol_results,
                candidate_symbols=decisions,
                evaluated_symbols=len(symbol_results),
            )

        if self._research_queue is not None:
            # Coverage is research-class: it drains through the bounded queue
            # outside P0/P1 evaluation and is dropped (counted) under pressure,
            # never applied as backpressure onto the trading path.
            loop = asyncio.get_running_loop()
            done = loop.create_future()

            def _queued_write() -> Any:
                result = _write_coverage()
                if not done.done():
                    loop.call_soon_threadsafe(done.set_result, result)
                return result

            if self._research_queue.submit(f"coverage:{window_id}", _queued_write):
                try:
                    coverage_result = await done
                except asyncio.CancelledError:
                    raise
            else:
                coverage_result = {"dropped": True}
                self._logger.warning(
                    "coverage payload dropped under queue pressure | window=%s",
                    window_id,
                )
        else:
            with measure_sync("research"):
                coverage_result = _write_coverage()
        if coverage_result is None:
            await self._handle_error(
                "market-coverage-telemetry",
                RuntimeError("coverage ledger write returned no result"),
            )

    async def _run_fast_monitor(self) -> None:
        """Bounded, fair active-candidate monitor; never scans the full universe."""
        while self._fast_monitor_running:
            try:
                await asyncio.sleep(self._config.climax_fast_poll_sec)
                poll_started = datetime.now(timezone.utc)
                self._fast_monitor_poll_sequence += 1
                poll_sequence = self._fast_monitor_poll_sequence
                keys = list(self._active_climax_pool)
                self._repository.update_fast_monitor_heartbeat(
                    checked_at=poll_started,
                    pool_size=len(keys),
                    poll_sequence=poll_sequence,
                    last_poll_at=poll_started,
                    last_error="",
                    event_loop_lag_ms=self._latest_event_loop_lag_ms(),
                )
                if not keys:
                    completed = datetime.now(timezone.utc)
                    self._repository.update_fast_monitor_heartbeat(
                        checked_at=completed,
                        pool_size=0,
                        poll_sequence=poll_sequence,
                        last_complete_at=completed,
                        last_error="",
                        event_loop_lag_ms=self._latest_event_loop_lag_ms(),
                    )
                    self._repository.record_climax_monitor_event(
                        created_at=completed,
                        symbol="__FAST_MONITOR__",
                        event_id=f"poll:{poll_sequence}",
                        event_high_time=None,
                        action="poll_complete",
                        reason=None,
                        pool_size=0,
                        poll_sequence=poll_sequence,
                        worker_id="climax-fast-monitor",
                        details={"selected_count": 0, "terminal": True},
                    )
                    self._logger.info(
                        "Climax fast-monitor poll_complete | seq=%s pool=0",
                        poll_sequence,
                    )
                    continue
                selected = self._select_fast_monitor_keys(keys)
                self._logger.info(
                    "Climax fast-monitor poll_start | seq=%s pool=%s selected=%s",
                    poll_sequence,
                    len(keys),
                    len(selected),
                )
                for symbol, event_id in selected:
                    candidate = self._active_climax_pool.get((symbol, event_id))
                    if candidate is None:
                        continue
                    self._repository.record_climax_monitor_event(
                        created_at=poll_started,
                        symbol=symbol,
                        event_id=event_id,
                        event_high_time=candidate.event_high_time,
                        action="poll_start",
                        reason=None,
                        pool_size=len(self._active_climax_pool),
                        poll_sequence=poll_sequence,
                        worker_id="climax-fast-monitor",
                    )
                # Live active-event frames use the reserved P0 lane (and the
                # frame store once PREFERRED mode is active).
                frozen_frames = await self._market_data_provider.prefetch_historical_frames(
                    [symbol for symbol, _ in selected],
                    priority=RequestPriority.P0,
                )
                frames = {
                    symbol: frozen.frame.copy(deep=True)
                    for symbol, frozen in frozen_frames.items()
                }
                for symbol, event_id in selected:
                    frame = frames.get(symbol)
                    candidate = self._active_climax_pool.get((symbol, event_id))
                    if candidate is None:
                        continue
                    if not self._fast_monitor_running or frame is None or frame.empty:
                        self._repository.record_climax_monitor_event(
                            created_at=datetime.now(timezone.utc),
                            symbol=symbol,
                            event_id=event_id,
                            event_high_time=candidate.event_high_time,
                            action="poll_skip",
                            reason="empty_frame_or_shutdown",
                            pool_size=len(self._active_climax_pool),
                            poll_sequence=poll_sequence,
                            worker_id="climax-fast-monitor",
                        )
                        continue
                    decision_market = await self._market_data_provider.capture_decision_snapshot(
                        symbol, frame_1m=frame, include_liquidity=False
                    )
                    if decision_market is None:
                        continue
                    async with self._symbol_mutations.mutation(symbol):
                        candidate = self._active_climax_pool.get((symbol, event_id))
                        state = self._state_store.load(symbol)
                        if candidate is None:
                            continue
                        if state is None or state.event_id != event_id:
                            self._active_climax_pool.pop((symbol, event_id), None)
                            self._repository.record_climax_monitor_event(
                                created_at=datetime.now(timezone.utc),
                                symbol=symbol,
                                event_id=event_id,
                                event_high_time=candidate.event_high_time,
                                action="candidate_removed",
                                reason="event_state_missing_or_replaced",
                                pool_size=len(self._active_climax_pool),
                                poll_sequence=poll_sequence,
                                worker_id="climax-fast-monitor",
                            )
                            continue
                        await self._evaluate_and_send_climax(
                            symbol,
                            decision_market.frame_1m.frame,
                            state,
                            market_snapshot=decision_market,
                            fast_monitor=True,
                            poll_sequence=poll_sequence,
                        )
                now = datetime.now(timezone.utc)
                for key, candidate in list(self._active_climax_pool.items()):
                    async with self._symbol_mutations.mutation(candidate.symbol):
                        current = self._active_climax_pool.get(key)
                        if (
                            current is None
                            or now - current.candidate_added_at
                            < timedelta(
                                minutes=self._config.climax_candidate_ttl_minutes
                            )
                        ):
                            continue
                        self._active_climax_pool.pop(key, None)
                        self._repository.record_climax_monitor_event(
                            created_at=now,
                            symbol=current.symbol,
                            event_id=current.event_id,
                            event_high_time=current.event_high_time,
                            action="candidate_expired",
                            reason="ttl",
                            pool_size=len(self._active_climax_pool),
                            poll_sequence=poll_sequence,
                            worker_id="climax-fast-monitor",
                        )
                completed = datetime.now(timezone.utc)
                self._repository.update_fast_monitor_heartbeat(
                    checked_at=completed,
                    pool_size=len(self._active_climax_pool),
                    poll_sequence=poll_sequence,
                    last_complete_at=completed,
                    last_error="",
                    event_loop_lag_ms=self._latest_event_loop_lag_ms(),
                )
                self._repository.record_climax_monitor_event(
                    created_at=completed,
                    symbol="__FAST_MONITOR__",
                    event_id=f"poll:{poll_sequence}",
                    event_high_time=None,
                    action="poll_complete",
                    reason=None,
                    pool_size=len(self._active_climax_pool),
                    poll_sequence=poll_sequence,
                    worker_id="climax-fast-monitor",
                    details={"selected_count": len(selected), "terminal": True},
                )
                self._logger.info(
                    "Climax fast-monitor poll_complete | seq=%s pool=%s",
                    poll_sequence,
                    len(self._active_climax_pool),
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                now = datetime.now(timezone.utc)
                self._repository.update_fast_monitor_heartbeat(
                    checked_at=now,
                    pool_size=len(self._active_climax_pool),
                    poll_sequence=self._fast_monitor_poll_sequence,
                    last_error=str(exc),
                    event_loop_lag_ms=self._latest_event_loop_lag_ms(),
                )
                self._repository.record_climax_monitor_event(
                    created_at=now,
                    symbol="__FAST_MONITOR__",
                    event_id=f"poll:{self._fast_monitor_poll_sequence}",
                    event_high_time=None,
                    action="poll_skip",
                    reason="poll_exception",
                    pool_size=len(self._active_climax_pool),
                    poll_sequence=self._fast_monitor_poll_sequence,
                    worker_id="climax-fast-monitor",
                    details={"error_code": type(exc).__name__, "terminal": True},
                )
                self._logger.exception("Climax fast-monitor error")

    def _select_fast_monitor_keys(
        self, keys: list[tuple[str, str]]
    ) -> list[tuple[str, str]]:
        """Return the next bounded slice and advance the fairness cursor."""
        start = self._fast_monitor_cursor % len(keys)
        ordered = keys[start:] + keys[:start]
        selected = ordered[: self._config.climax_max_active_symbols]
        self._fast_monitor_cursor = (start + len(selected)) % len(keys)
        return selected

    def _prepare_shadow_root_event(
        self, state: EventState, observed_at: datetime
    ) -> int:
        """Freeze initial pump evidence and persist the shadow root identity."""
        if not getattr(self._config, "climax_root_event_tracking_enabled", False):
            return int(
                (state.event_features_snapshot or {}).get("root_event_revision") or 1
            )
        snapshot = state.event_features_snapshot
        frozen = snapshot.get("initial_extension_pct")
        source = snapshot.get("initial_extension_source")
        if frozen is None:
            base = state.event_base_price
            high = state.event_high
            if base and high and base > 0 and high >= base:
                frozen = (high - base) / base * 100.0
                source = "event_base_to_peak"
            elif snapshot.get("initial_ret_5m") is not None:
                frozen = snapshot.get("initial_ret_5m")
                source = "event_snapshot_ret_5m"
            if frozen is not None:
                snapshot["initial_extension_pct"] = float(frozen)
                snapshot["initial_extension_source"] = source or "event_snapshot"
        revision, persisted_frozen = self._repository.upsert_shadow_root_event(
            root_event_id=state.event_id,
            symbol=state.symbol,
            event_started_at=state.event_start_time,
            event_base_price=state.event_base_price,
            peak_high=state.event_high,
            peak_high_time=state.event_high_time,
            initial_extension_pct=float(frozen) if frozen is not None else None,
            initial_extension_source=source,
            observed_at=observed_at,
        )
        snapshot["root_event_id"] = state.event_id
        snapshot["root_event_revision"] = revision
        if (
            snapshot.get("initial_extension_pct") is None
            and persisted_frozen is not None
        ):
            snapshot["initial_extension_pct"] = float(persisted_frozen)
        return revision

    def _track_climax_candidate(self, state: EventState, now: datetime) -> None:
        """Track one event identity once; do not refresh TTL on every full scan."""
        if not state.event_id:
            return
        event_revision = self._prepare_shadow_root_event(state, now)
        key = (state.symbol, state.event_id)
        for old_key in list(self._active_climax_pool):
            if old_key[0] == state.symbol and old_key != key:
                old = self._active_climax_pool.pop(old_key)
                if self._config.climax_root_event_tracking_enabled:
                    self._repository.close_open_shadow_attempts_for_root(
                        root_event_id=old.event_id,
                        new_state="ROOT_REPLACED",
                        reason="root_replaced_by_new_event",
                        observed_at=now,
                        runtime_instance_id=self._runtime_instance_id,
                        model_version="climax-v1",
                    )
                self._repository.record_climax_monitor_event(
                    created_at=now,
                    symbol=old.symbol,
                    event_id=old.event_id,
                    event_high_time=old.event_high_time,
                    action="candidate_removed",
                    reason="replaced_by_new_event",
                    pool_size=len(self._active_climax_pool),
                    poll_sequence=self._fast_monitor_poll_sequence,
                    worker_id="full-scan",
                )
        if key in self._active_climax_pool:
            return
        added_at = state.event_high_time or now
        if now - added_at >= timedelta(
            minutes=self._config.climax_candidate_ttl_minutes
        ):
            self._repository.record_climax_monitor_event(
                created_at=now,
                symbol=state.symbol,
                event_id=state.event_id,
                event_high_time=state.event_high_time,
                action="candidate_rejected",
                reason="ttl_expired_before_pool_add",
                pool_size=len(self._active_climax_pool),
                poll_sequence=self._fast_monitor_poll_sequence,
                worker_id="full-scan",
            )
            return
        self._active_climax_pool[key] = _ClimaxCandidate(
            symbol=state.symbol,
            event_id=state.event_id,
            event_high_time=state.event_high_time,
            candidate_added_at=added_at,
            pool_added_at=now,
        )

        self._repository.record_climax_monitor_event(
            created_at=now,
            symbol=state.symbol,
            event_id=state.event_id,
            event_high_time=state.event_high_time,
            action="pool_add",
            reason="new_event_id",
            pool_size=len(self._active_climax_pool),
            poll_sequence=self._fast_monitor_poll_sequence,
            worker_id="full-scan",
            root_event_id=state.event_id,
            event_revision=event_revision,
            observed_at=now,
            pool_added_at=now,
        )

    def _remove_climax_candidate(
        self, symbol: str, event_id: str, *, reason: str
    ) -> None:
        candidate = self._active_climax_pool.pop((symbol, event_id), None)
        if candidate is not None:
            self._repository.record_climax_monitor_event(
                created_at=datetime.now(timezone.utc),
                symbol=symbol,
                event_id=event_id,
                event_high_time=candidate.event_high_time,
                action="candidate_removed",
                reason=reason,
                pool_size=len(self._active_climax_pool),
                poll_sequence=self._fast_monitor_poll_sequence,
                worker_id="runtime",
            )

    async def update_outcomes(
        self,
        now: datetime | None = None,
        *,
        signal_limit: int = 25,
        observation_limit: int = 25,
    ) -> int:
        return await self._outcome_tracker.update_due_outcomes(
            now=now,
            signal_limit=signal_limit,
            observation_limit=observation_limit,
        )

    async def _evaluate_trapped_longs_v2_shadow(
        self,
        state: EventState,
        features: SymbolFeatures,
        frame_1m: pd.DataFrame,
        derivatives: dict[str, Any] | None = None,
    ) -> None:
        """Evaluate V2 in the scan loop and persist research-only evidence.

        This method intentionally has no delivery return value and never calls
        signal, watch-candidate, outbox, or notifier code.
        """
        if not getattr(self._config, "trapped_longs_reversal_v2_enabled", False):
            return
        breakout_at = state.event_high_time or state.event_start_time or features.asof
        snapshot = dict(state.event_features_snapshot or {})
        raw_oi = (derivatives or {}).get("open_interest") or snapshot.get("oi_observations", ())
        oi_items: list[OiObservation] = []
        for item in raw_oi:
            if not isinstance(item, dict):
                continue
            observed = item.get("observed_at", item.get("timestamp"))
            value = item.get("value", item.get("openInterest"))
            try:
                if isinstance(observed, (int, float)) or str(observed).isdigit():
                    observed_at = pd.to_datetime(int(observed), unit="ms", utc=True).to_pydatetime()
                else:
                    observed_at = pd.to_datetime(observed, utc=True).to_pydatetime()
                oi_items.append(OiObservation(observed_at, float(value) if value is not None else None))
            except (TypeError, ValueError, OverflowError):
                continue
        oi = tuple(oi_items)
        breakout = FrozenBreakout(
            identity=state.event_id or f"{features.symbol}:{breakout_at.isoformat()}",
            occurred_at=breakout_at,
            reference=snapshot.get("breakout_reference") or state.event_high,
            revision=int(snapshot.get("event_revision") or 1),
            symbol=features.symbol,
        )
        result = evaluate_trapped_longs_reversal_v2(frame_1m, breakout, oi, features.asof)
        evidence = build_observation_evidence(
            {
                "result": {
                    "identity": result.identity,
                    "revision": result.revision,
                    "actionable": result.actionable,
                    "blockers": result.blockers,
                    "explanation": {
                        key: {"measurement": value.measurement, "passed": value.passed}
                        for key, value in result.explanation.items()
                    },
                },
                "features": asdict(features),
            }
        )
        observation_id = f"{result.identity}:{features.asof.isoformat()}"
        key = make_observation_idempotency_key(
            strategy_family="TRAPPED_LONGS",
            strategy=TRAPPED_LONGS_REVERSAL_V2,
            symbol=features.symbol,
            root_event_id=result.identity,
            event_revision=result.revision,
            evaluation_phase="SHADOW_EVALUATION",
            market_asof=features.asof,
            input_fingerprint=evidence.input_fingerprint,
            model_version=TRAPPED_LONGS_V2_MODEL_VERSION,
            config_hash=self._strategy_config_hash,
        )
        if result.actionable:
            candidate_id = f"{result.identity}:r{result.revision}"
            attempt_id = f"{candidate_id}:a1"
            try:
                self._repository.upsert_strategy_candidate(
                    candidate_id=candidate_id,
                    root_event_id=result.identity,
                    event_revision=result.revision,
                    strategy=TRAPPED_LONGS_REVERSAL_V2,
                    symbol=features.symbol,
                    state="ACTIONABLE_SHADOW",
                    observed_at=datetime.now(timezone.utc),
                    attempt_id=attempt_id,
                    evidence={
                        "identity": result.identity,
                        "blockers": list(result.blockers),
                        "explanation": {
                            key: {"measurement": value.measurement, "passed": value.passed}
                            for key, value in result.explanation.items()
                        },
                    },
                )
            except Exception:
                self._logger.exception(
                    "V2 candidate ledger write failed symbol=%s root_event_id=%s",
                    features.symbol,
                    result.identity,
                )
            self._repository.record_strategy_observation(
                StrategyObservation(
                    observation_id=observation_id,
                    idempotency_key=key,
                    run_id=self._runtime_instance_id,
                    runtime_instance_id=self._runtime_instance_id,
                    runtime_started_at=self._runtime_started_at,
                    code_version=self._code_version,
                    strategy_family="TRAPPED_LONGS",
                    strategy=TRAPPED_LONGS_REVERSAL_V2,
                    evaluation_phase="SHADOW_EVALUATION",
                    symbol=features.symbol,
                    root_event_id=result.identity,
                    event_revision=result.revision,
                    attempt_id=(
                        f"{result.identity}:r{result.revision}:a1"
                        if result.actionable
                        else None
                    ),
                    evaluation_id=None,
                    signal_id=None,
                    observed_at=datetime.now(timezone.utc),
                    exchange_time=None,
                    market_asof=features.asof,
                    live_decision="UNCHANGED",
                    shadow_decision="ACTIONABLE" if result.actionable else "BLOCKED",
                    score=0,
                    blockers=list(result.blockers),
                    warnings=[item["path"] for item in evidence.warnings],
                    market_price=features.price,
                    event_high=state.event_high,
                    model_version=TRAPPED_LONGS_V2_MODEL_VERSION,
                    config_hash=self._strategy_config_hash,
                    input_fingerprint=evidence.input_fingerprint,
                    input_snapshot=evidence.snapshot,
                )
            )

    def evaluate_trapped_longs_reversal(
        self, state: EventState, features: SymbolFeatures, frame_1m: pd.DataFrame, **kwargs: Any
    ):
        """Evaluate the independent contour without old-bundle selection."""
        if not getattr(self._config, "trapped_longs_reversal_enabled", False):
            return None
        return evaluate_trapped_longs_reversal(state, features, frame_1m, self._config, **kwargs)

    async def _evaluate_and_send_trapped_longs(
        self, state: EventState, features: SymbolFeatures, frame_1m: pd.DataFrame
    ) -> SignalDecision | None:
        """Run the independent trapped-longs contour without old bundle selection."""
        if not getattr(self._config, "trapped_longs_reversal_enabled", False):
            return None
        if not state.event_id:
            return None
        snapshot = dict(state.event_features_snapshot or {})
        if "breakout_reference" not in snapshot and state.event_high:
            snapshot["breakout_reference"] = float(state.event_high) * 0.995
            state.event_features_snapshot = snapshot
            self._state_store.save(state)
        root_id = trapped_longs_root_event_id(features.symbol, state.event_id)
        attempt_id = trapped_longs_attempt_id(features.symbol, state.event_id)
        evaluation_time = datetime.now(timezone.utc)
        root_created_at = state.event_start_time or state.event_high_time or features.asof
        breakout_at = state.event_high_time or root_created_at
        proposed_expiry = breakout_at + timedelta(
            minutes=int(getattr(self._config, "trapped_longs_max_lifetime_minutes", 15))
        )
        stored_attempt = self._repository.get_shadow_entry_attempt(attempt_id=attempt_id)
        attempt_created_at = (
            stored_attempt["attempt_created_at"]
            if stored_attempt and stored_attempt["attempt_created_at"] is not None
            else evaluation_time
        )
        confirmation_expires_at = (
            stored_attempt["confirmation_expires_at"]
            if stored_attempt and stored_attempt["confirmation_expires_at"] is not None
            else proposed_expiry
        )
        evaluation = self.evaluate_trapped_longs_reversal(
            state,
            features,
            frame_1m,
            attempt_created_at=attempt_created_at,
            confirmation_expires_at=confirmation_expires_at,
            decision_time=evaluation_time,
        )
        expiry_blocked = bool(
            set(evaluation.veto_reasons)
            & {"CONFIRMATION_WINDOW_EXPIRED", "BORN_EXPIRED_ATTEMPT"}
        )
        lifecycle = advance_trapped_longs_lifecycle(
            root_created_at=root_created_at,
            breakout_at=breakout_at,
            observed_at=features.asof,
            event_revision=1,
            breakout_confirmed="breakout_not_confirmed" not in evaluation.veto_reasons,
            oi_confirmed="oi_missing" not in evaluation.veto_reasons and "oi_below_threshold" not in evaluation.veto_reasons,
            closed_candles=int(evaluation.metadata.get("closed_structural_candles") or 0),
            close_below_reference=bool(evaluation.metadata.get("close_below_breakout_reference")),
            failed_retest=bool(evaluation.metadata.get("failed_retest_confirmed")),
            no_new_high=bool(evaluation.metadata.get("no_new_high")),
            liquidity_ok=not any(reason in evaluation.veto_reasons for reason in {"liquidity_unavailable", "liquidity_incomplete", "liquidity_block"}),
            rejection_ok="rejection_below_threshold" not in evaluation.veto_reasons,
            max_lifetime_minutes=int(getattr(self._config, "trapped_longs_max_lifetime_minutes", 15)),
        )
        self._repository.upsert_shadow_entry_attempt(
            attempt_id=attempt_id,
            root_event_id=root_id,
            observed_at=evaluation_time,
            local_retest_high=features.last_high,
            breakdown_level=float(evaluation.metadata.get("breakout_reference") or 0.0),
            attempt_state=(
                "EXPIRED"
                if expiry_blocked
                else "ACTIONABLE" if lifecycle.state == "ADMITTED" else lifecycle.state
            ),
            attempt_trigger="trapped_longs_reversal",
            confirmation_expires_at=proposed_expiry,
            event_revision=lifecycle.event_revision,
            runtime_instance_id=self._runtime_instance_id,
            model_version=TRAPPED_LONGS_MODEL_VERSION,
            max_attempts_per_root_event=1,
        )
        if lifecycle.state == "ADMITTED" and evaluation.actionable:
            self._repository.transition_shadow_entry_attempt(
                attempt_id=attempt_id,
                root_event_id=root_id,
                event_revision=lifecycle.event_revision,
                evaluation_id=None,
                new_state="ACTIONABLE",
                reason="trapped_longs_conditions_met",
                observed_at=evaluation_time,
                market_asof=features.asof,
                runtime_instance_id=self._runtime_instance_id,
                model_version=TRAPPED_LONGS_MODEL_VERSION,
            )
        evaluation_id = self._repository.record_climax_evaluation(
            evaluation_time=evaluation_time,
            symbol=features.symbol,
            strategy=TRAPPED_LONGS_REVERSAL,
            subtype_candidate=evaluation.subtype or TRAPPED_LONGS_REVERSAL,
            model_version=TRAPPED_LONGS_MODEL_VERSION,
            event_id=root_id,
            event_high=evaluation.metadata.get("event_high"),
            event_high_time=state.event_high_time,
            event_detected_at=state.event_start_time,
            candidate_added_at=None,
            candidate_age_sec=None,
            fast_monitor=False,
            poll_sequence=None,
            frame_asof=features.asof,
            candles_asof=features.asof,
            oi_asof=features.asof if features.oi_change_15m is not None else None,
            orderbook_asof=features.asof if features.liquidity_available else None,
            score=evaluation.score,
            grade=evaluation.grade,
            actionable=evaluation.actionable,
            admission_passed=evaluation.actionable,
            veto_reasons=evaluation.veto_reasons,
            passed_conditions=[k for k, v in evaluation.metadata.items() if isinstance(v, bool) and v],
            data_quality=evaluation.data_quality,
            liquidity={
                "available": features.liquidity_available,
                "spread_pct": features.spread_pct,
                "slippage_pct": features.slippage_pct,
                "depth_1pct_usdt": features.orderbook_depth_usdt_1pct,
                "depth_2pct_usdt": features.orderbook_depth_usdt_2pct,
            },
            oi={"change_15m_pct": features.oi_change_15m, "status": features.derivatives_status},
            features={**asdict(features), "trapped_longs": evaluation.metadata},
            lifecycle_state=(
                "EXPIRED" if expiry_blocked else "ADMITTED" if evaluation.actionable else "RETEST_IN_PROGRESS"
            ),
            telegram_eligible=evaluation.actionable,
            runtime_instance_id=self._runtime_instance_id,
            root_event_id=root_id,
            event_revision=1,
            attempt_id=attempt_id,
            observed_at=evaluation_time,
            market_asof=features.asof,
            evaluation_completed_at=evaluation_time,
            live_decision="ACTIONABLE" if evaluation.actionable else "BLOCKED",
            live_veto_reasons=evaluation.veto_reasons,
        )
        if not evaluation.actionable or evaluation_id is None:
            return None
        if self._repository.has_signal_for_event(
            features.symbol, root_id, TRAPPED_LONGS_REVERSAL, TRAPPED_LONGS_MODEL_VERSION
        ):
            return None
        reference = float(evaluation.metadata["breakout_reference"])
        decision = SignalDecision(
            symbol=features.symbol,
            event_id=root_id,
            signal_type=SignalType.CONFIRM,
            grade=evaluation.grade,
            score=evaluation.score,
            market_price=features.price,
            short_zone_low=reference * 0.99,
            short_zone_high=reference * 1.01,
            signal_time=features.asof,
            reasons=["False breakout", "OI build-up", "Failed retest below breakout level"],
            risk_flags=[],
            features_snapshot=asdict(features),
            score_breakdown={"trapped_longs_v1": float(evaluation.score)},
            strategy_type=TRAPPED_LONGS_REVERSAL,
            strategy_subtype=TRAPPED_LONGS_REVERSAL,
            model_version=TRAPPED_LONGS_MODEL_VERSION,
            lifecycle_state="TRAPPED_LONGS_ADMITTED",
            strategy_metadata=evaluation.metadata,
        )
        if not live_delivery_enabled(decision, self._config):
            return None
        payload = format_signal_message(decision, self._config.timezone)
        self._repository.save_signal(
            decision,
            state,
            telegram_sent=False,
            delivery_payload=payload,
            provenance=self._signal_provenance(
                decision,
                root_event_id=root_id,
                decision_evaluation_id=evaluation_id,
                admission_evaluation_id=evaluation_id,
            ),
        )
        self._delivery_worker.wake()
        return decision

    async def _evaluate_and_send_climax(
        self,
        symbol: str,
        frame_1m: pd.DataFrame,
        state: EventState,
        *,
        features: SymbolFeatures | None = None,
        fast_monitor: bool = False,
        poll_sequence: int | None = None,
        market_asof: datetime | None = None,
        market_snapshot: object | None = None,
    ) -> SignalDecision | None:
        """Evaluate, dedupe, persist, and deliver one climax signal."""
        if not self._config.climax_short_enabled or state.signal_id is not None:
            return None
        if features is None:
            market_asof = market_asof or datetime.now(timezone.utc)
            derivatives = (
                dict(getattr(market_snapshot, "derivatives"))
                if market_snapshot is not None
                else await self._scanner.fetch_optional_derivatives(symbol)
            )
            captured_liquidity = (
                getattr(market_snapshot, "liquidity", None)
                if market_snapshot is not None
                else None
            )
            if captured_liquidity is not None:
                liquidity = dict(captured_liquidity)
            elif market_snapshot is not None:
                enriched = await self._enrich_decision_market_snapshot(
                    symbol, market_snapshot, float(frame_1m["close"].iloc[-1])
                )
                liquidity = dict(enriched.liquidity or {})
            else:
                liquidity = await self._fetch_optional_liquidity(
                    symbol, float(frame_1m["close"].iloc[-1])
                )
            features = self._feature_builder.build(
                symbol,
                frame_1m,
                state=state,
                derivatives=derivatives,
                liquidity=liquidity,
                market_asof=market_asof,
            )
        bundle = evaluate_climax_bundle(
            state, features, frame_1m, self._config, strict_closed_candles=True
        )
        evaluation: ClimaxEvaluation = bundle.selected
        shadow_evaluation: ClimaxEvaluation | None = None
        if (
            self._config.low_volume_frozen_initial_extension_enabled
            and self._config.low_volume_frozen_initial_extension_shadow_only
        ):
            shadow_evaluation = evaluate_climax_shadow(
                state, features, frame_1m, self._config
            )
        evaluated_at = datetime.now(timezone.utc)
        candidate = self._active_climax_pool.get((symbol, state.event_id))
        candidate_added_at = candidate.candidate_added_at if candidate else None
        candidate_age_sec = (
            (evaluated_at - candidate_added_at).total_seconds()
            if candidate_added_at
            else None
        )
        root_event_id = str(
            (state.event_features_snapshot or {}).get("root_event_id") or state.event_id
        )
        event_revision = int(
            (state.event_features_snapshot or {}).get("root_event_revision") or 1
        )
        lifecycle_shadow = None
        lifecycle_attempt_id: str | None = None
        shadow_attempt_id: str | None = None
        lifecycle_metadata = (
            evaluation.metadata.get("volume_climax_metadata") or evaluation.metadata
        )
        is_volume_climax_candidate = bool(
            evaluation.metadata.get("volume_climax_candidate")
        ) or (
            evaluation.subtype == "VOLUME_CLIMAX_UNWIND"
            or evaluation.metadata.get("strategy_subtype") == "VOLUME_CLIMAX_UNWIND"
        )
        if (
            self._config.volume_climax_lifecycle_shadow_enabled
            and is_volume_climax_candidate
        ):
            snapshot = dict(state.event_features_snapshot or {})
            current_high = float(lifecycle_metadata.get("event_high") or features.price)
            prior_high = float(
                snapshot.get("volume_climax_latest_high") or current_high
            )
            root_created_at = (
                state.event_start_time or state.event_high_time or features.asof
            )
            latest_high_at = (
                _parse_optional_datetime(snapshot.get("volume_climax_latest_high_at"))
                or state.event_high_time
                or root_created_at
            )
            confirmation_started_at = (
                _parse_optional_datetime(
                    snapshot.get("volume_climax_confirmation_started_at")
                )
                or latest_high_at
            )
            last_observed_at = (
                _parse_optional_datetime(snapshot.get("volume_climax_last_observed_at"))
                or root_created_at
            )
            prior_revision = int(snapshot.get("volume_climax_event_revision") or 1)
            lifecycle_shadow = advance_volume_climax_lifecycle(
                root_created_at=root_created_at,
                latest_high=prior_high,
                latest_high_at=latest_high_at,
                confirmation_started_at=confirmation_started_at,
                last_observed_at=last_observed_at,
                event_revision=prior_revision,
                current_high=current_high,
                observed_at=features.asof,
                closed_candles_after_high=int(
                    lifecycle_metadata.get("closed_candles_after_high") or 0
                ),
                min_closed_candles_after_high=self._config.volume_climax_min_closed_candles_after_high,
                max_lifetime_minutes=self._config.volume_climax_max_lifetime_minutes,
                confirmation_window_minutes=self._config.volume_climax_confirmation_window_minutes,
                price_acceleration_resumed=features.ret_5m > 0,
                active_short_squeeze=(
                    features.oi_change_pct is not None
                    and features.oi_change_pct < 0
                    and features.ret_5m > 0
                ),
                oi_continuation=features.oi_change_pct is None
                or features.oi_change_pct >= 0,
                rejection_ok=float(lifecycle_metadata.get("rejection_pct") or 0.0)
                >= self._config.volume_climax_min_rejection_pct,
                liquidity_ok=features.liquidity_available
                and not bool(lifecycle_metadata.get("liquidity_warning")),
                entry_distance_ok=float(
                    lifecycle_metadata.get("entry_distance_below_high_pct") or 999.0
                )
                <= self._config.volume_climax_max_entry_distance_below_high_pct,
            )
            db_revision, _ = self._repository.upsert_shadow_root_event(
                root_event_id=root_event_id,
                symbol=symbol,
                event_started_at=root_created_at,
                event_base_price=state.event_base_price,
                peak_high=current_high,
                peak_high_time=features.asof,
                initial_extension_pct=features.ret_15m,
                initial_extension_source="volume_climax",
                observed_at=features.asof,
            )
            lifecycle_shadow.event_revision = db_revision
            event_revision = db_revision
            lifecycle_attempt_id = volume_climax_attempt_id(root_event_id, db_revision)
            shadow_attempt_id = lifecycle_attempt_id
            snapshot.update(
                {
                    "volume_climax_latest_high": lifecycle_shadow.latest_high,
                    "volume_climax_latest_high_at": lifecycle_shadow.latest_high_at.isoformat(),
                    "volume_climax_confirmation_started_at": lifecycle_shadow.confirmation_started_at.isoformat(),
                    "volume_climax_last_observed_at": lifecycle_shadow.last_observed_at.isoformat(),
                    "volume_climax_event_revision": lifecycle_shadow.event_revision,
                    "volume_climax_lifecycle_state": lifecycle_shadow.state,
                    "volume_climax_lifecycle_vetoes": lifecycle_shadow.veto_reasons,
                }
            )
            state.event_features_snapshot = snapshot
            self._state_store.save(state)
            attempt_persisted = False
            if not lifecycle_shadow.expired:
                attempt_persisted = self._repository.upsert_shadow_entry_attempt(
                    attempt_id=lifecycle_attempt_id,
                    root_event_id=root_event_id,
                    observed_at=features.asof,
                    local_retest_high=lifecycle_shadow.latest_high,
                    breakdown_level=float(
                        lifecycle_metadata.get("breakout_reference")
                        or lifecycle_shadow.latest_high * 0.995
                    ),
                    attempt_state=(
                        "SHADOW_ACTIONABLE"
                        if lifecycle_shadow.state == "FALLBACK_READY"
                        else "RETEST_IN_PROGRESS"
                    ),
                    attempt_trigger="volume_climax_lifecycle",
                    confirmation_expires_at=lifecycle_shadow.confirmation_started_at
                    + timedelta(
                        minutes=self._config.volume_climax_confirmation_window_minutes
                    ),
                    event_revision=lifecycle_shadow.event_revision,
                    runtime_instance_id=self._runtime_instance_id,
                    model_version="climax-lifecycle-v1-shadow",
                    max_attempts_per_root_event=self._config.climax_max_attempts_per_root_event,
                )
            if not attempt_persisted:
                lifecycle_attempt_id = None
                shadow_attempt_id = None
            if (
                not lifecycle_shadow.expired
                and lifecycle_shadow.state == "FALLBACK_READY"
                and attempt_persisted
            ):
                self._repository.transition_shadow_entry_attempt(
                    attempt_id=lifecycle_attempt_id,
                    root_event_id=root_event_id,
                    event_revision=lifecycle_shadow.event_revision,
                    evaluation_id=None,
                    new_state="SHADOW_ACTIONABLE",
                    reason="fallback_conditions_met",
                    observed_at=features.asof,
                    market_asof=features.asof,
                    runtime_instance_id=self._runtime_instance_id,
                    model_version="climax-lifecycle-v1-shadow",
                    details={"veto_reasons": lifecycle_shadow.veto_reasons},
                )
        if self._config.climax_root_event_tracking_enabled and lifecycle_shadow is None:
            if (
                shadow_evaluation is not None
                and shadow_evaluation.metadata.get("post_high_retest_high") is not None
            ):
                shadow_data = shadow_evaluation.metadata
                shadow_attempt_id = f"{root_event_id}:r{event_revision}:a1"
                if shadow_evaluation.actionable or shadow_data.get(
                    "failed_retest_confirmed"
                ):
                    attempt_state = "BREAKDOWN_PENDING"
                else:
                    attempt_state = "RETEST_IN_PROGRESS"
                attempt_persisted = self._repository.upsert_shadow_entry_attempt(
                    attempt_id=shadow_attempt_id,
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    observed_at=evaluated_at,
                    local_retest_high=shadow_data.get("post_high_retest_high"),
                    breakdown_level=shadow_data.get("breakout_reference"),
                    attempt_state=attempt_state,
                    confirmation_expires_at=evaluated_at
                    + timedelta(minutes=self._config.climax_entry_attempt_ttl_minutes),
                    runtime_instance_id=self._runtime_instance_id,
                    model_version=str(shadow_data.get("model_version", "climax-v1")),
                    max_attempts_per_root_event=self._config.climax_max_attempts_per_root_event,
                )
                if not attempt_persisted:
                    shadow_attempt_id = None
            else:
                shadow_attempt_id = self._repository.get_open_shadow_attempt_id(
                    root_event_id=root_event_id
                )
                if shadow_attempt_id:
                    self._repository.record_attempt_reused_after_restart(
                        attempt_id=shadow_attempt_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        observed_at=evaluated_at,
                        runtime_instance_id=self._runtime_instance_id,
                        model_version="climax-v1",
                    )
        passed_conditions = [
            key
            for key, value in evaluation.metadata.items()
            if isinstance(value, bool) and value
        ]
        lifecycle_shadow_decision = (
            lifecycle_shadow.state if lifecycle_shadow is not None else None
        )
        lifecycle_shadow_vetoes = (
            lifecycle_shadow.veto_reasons if lifecycle_shadow is not None else None
        )
        lifecycle_decision_delta = (
            "LIVE_REJECTED_SHADOW_FALLBACK_READY"
            if lifecycle_shadow is not None
            and not evaluation.actionable
            and lifecycle_shadow.state == "FALLBACK_READY"
            else None
        )
        evaluation_features = asdict(features)
        evaluation_features["climax_evaluation"] = {
            "selected_subtype": evaluation.metadata.get("strategy_subtype"),
            "selected_score": evaluation.score,
            "selected_grade": evaluation.grade,
            "selected_veto_reasons": list(evaluation.veto_reasons),
            "volume_climax_observed": bool(
                evaluation.metadata.get("volume_climax_observed")
            ),
            "volume_climax_candidate": bool(
                evaluation.metadata.get("volume_climax_candidate")
            ),
            "volume_climax_metadata": evaluation.metadata.get("volume_climax_metadata"),
        }
        evaluation_id = self._repository.record_climax_evaluation(
            evaluation_time=evaluated_at,
            symbol=symbol,
            strategy="CLIMAX_EXHAUSTION",
            subtype_candidate=evaluation.metadata.get("strategy_subtype"),
            model_version=str(evaluation.metadata.get("model_version", "climax-v1")),
            event_id=state.event_id,
            event_high=evaluation.metadata.get("event_high"),
            event_high_time=state.event_high_time,
            event_detected_at=state.event_start_time,
            candidate_added_at=candidate_added_at,
            candidate_age_sec=candidate_age_sec,
            fast_monitor=fast_monitor,
            poll_sequence=poll_sequence,
            frame_asof=features.asof,
            candles_asof=features.asof,
            oi_asof=features.asof if features.oi_change_pct is not None else None,
            orderbook_asof=features.asof if features.liquidity_available else None,
            score=evaluation.score,
            grade=evaluation.grade,
            actionable=evaluation.actionable,
            admission_passed=not evaluation.veto_reasons,
            veto_reasons=evaluation.veto_reasons,
            passed_conditions=passed_conditions,
            data_quality=evaluation.data_quality,
            liquidity={
                "available": features.liquidity_available,
                "spread_pct": features.spread_pct,
                "slippage_pct": features.slippage_pct,
                "depth_1pct_usdt": features.orderbook_depth_usdt_1pct,
                "depth_2pct_usdt": features.orderbook_depth_usdt_2pct,
            },
            oi={
                "status": features.derivatives_status,
                "change_pct": features.oi_change_pct,
                "reasons": features.derivatives_reasons,
            },
            features=evaluation_features,
            lifecycle_state="ACTIONABLE" if evaluation.actionable else "REJECTED",
            telegram_eligible=evaluation.actionable,
            runtime_instance_id=self._runtime_instance_id,
            root_event_id=root_event_id,
            event_revision=event_revision,
            attempt_id=shadow_attempt_id,
            observed_at=evaluated_at,
            market_asof=features.asof,
            pool_added_at=candidate.pool_added_at if candidate else None,
            event_age_sec=(
                (evaluated_at - state.event_high_time).total_seconds()
                if state.event_high_time
                else None
            ),
            pool_age_sec=(
                (evaluated_at - candidate.pool_added_at).total_seconds()
                if candidate
                else None
            ),
            evaluation_completed_at=evaluated_at,
            live_decision="ACTIONABLE" if evaluation.actionable else "REJECTED",
            live_veto_reasons=evaluation.veto_reasons,
            shadow_decision=lifecycle_shadow_decision
            or (
                (
                    "ACTIONABLE"
                    if shadow_evaluation and shadow_evaluation.actionable
                    else "REJECTED"
                )
                if shadow_evaluation
                else None
            ),
            shadow_veto_reasons=lifecycle_shadow_vetoes
            if lifecycle_shadow is not None
            else (shadow_evaluation.veto_reasons if shadow_evaluation else None),
            decision_delta=lifecycle_decision_delta
            or _decision_delta(evaluation, shadow_evaluation),
            shadow_hypothetical_entry_price=(
                features.price
                if shadow_evaluation and shadow_evaluation.actionable
                else None
            ),
            shadow_hypothetical_grade=shadow_evaluation.grade
            if shadow_evaluation
            else None,
            shadow_hypothetical_score=shadow_evaluation.score
            if shadow_evaluation
            else None,
            shadow_removed_vetoes=(
                sorted(
                    set(evaluation.veto_reasons) - set(shadow_evaluation.veto_reasons)
                )
                if shadow_evaluation
                else None
            ),
        )
        self._repository.record_current_root_predicate_snapshot(
            root_event_id=root_event_id,
            episode_id=None,
            evaluation_id=evaluation_id,
            observed_at=evaluated_at,
            snapshot=build_current_root_predicate_snapshot(
                metadata=evaluation.metadata,
                veto_reasons=list(evaluation.veto_reasons),
                passed_conditions=passed_conditions,
                feature_values={
                    "liquidity_available": features.liquidity_available,
                    "spread_pct": features.spread_pct,
                    "slippage_pct": features.slippage_pct,
                    "oi_change_pct": features.oi_change_pct,
                    "derivatives_status": features.derivatives_status,
                },
            ),
        )
        observation_results = self._record_strategy_observations(
            bundle,
            evaluation_phase="INITIAL",
            state=state,
            features=features,
            root_event_id=root_event_id,
            event_revision=event_revision,
            attempt_id=shadow_attempt_id,
            evaluation_id=evaluation_id,
            lifecycle_shadow=lifecycle_shadow,
            observed_at=evaluated_at,
        )
        await self._report_strategy_observation_failures(
            observation_results, root_event_id=root_event_id
        )
        if evaluation.metadata.get("volume_climax_observed"):
            volume_metadata = dict(
                evaluation.metadata.get("volume_climax_metadata") or {}
            )
            volume_candidate = bool(evaluation.metadata.get("volume_climax_candidate"))
            volume_actionable = bool(
                evaluation.metadata.get("volume_climax_actionable")
            )
            volume_stage = (
                "ACTIONABLE"
                if volume_actionable
                else "CANDIDATE"
                if volume_candidate
                else "OBSERVED"
            )
            volume_vetoes = (
                list(evaluation.veto_reasons) if not volume_candidate else []
            )
            self._repository.record_volume_climax_observation(
                observed_at=evaluated_at,
                market_asof=features.asof,
                symbol=symbol,
                event_id=state.event_id,
                root_event_id=root_event_id,
                event_revision=event_revision,
                runtime_instance_id=self._runtime_instance_id,
                model_version=str(volume_metadata.get("model_version", "climax-v1")),
                subtype=str(
                    volume_metadata.get("strategy_subtype", "VOLUME_CLIMAX_UNWIND")
                ),
                stage=volume_stage,
                score=int(
                    evaluation.metadata.get("volume_climax_score", evaluation.score)
                ),
                grade=str(
                    evaluation.metadata.get("volume_climax_grade", evaluation.grade)
                ),
                veto_reasons=list(
                    evaluation.metadata.get("volume_climax_veto_reasons", volume_vetoes)
                ),
                data_quality=list(evaluation.data_quality),
                metadata=volume_metadata,
                source_evaluation_id=evaluation_id,
                attempt_id=climax_branch_attempt_id(
                    root_event_id, "VOLUME_CLIMAX_UNWIND", event_revision
                ),
            )
        if self._config.climax_root_event_tracking_enabled and lifecycle_shadow is None:
            lifecycle_model_version = str(
                (
                    shadow_evaluation.metadata
                    if shadow_evaluation
                    else evaluation.metadata
                ).get("model_version", "climax-v1")
            )
            if shadow_attempt_id is None:
                self._repository.record_attempt_correlation_missing(
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    attempt_id=None,
                    evaluation_id=evaluation_id,
                    observed_at=evaluated_at,
                    market_asof=features.asof,
                    runtime_instance_id=self._runtime_instance_id,
                    model_version=lifecycle_model_version,
                    details={"event_id": state.event_id, "fast_monitor": fast_monitor},
                )
            elif shadow_evaluation is not None and shadow_evaluation.actionable:
                self._repository.transition_shadow_entry_attempt(
                    attempt_id=shadow_attempt_id,
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    evaluation_id=evaluation_id,
                    new_state="SHADOW_ACTIONABLE",
                    reason="shadow_actionable",
                    observed_at=evaluated_at,
                    market_asof=features.asof,
                    runtime_instance_id=self._runtime_instance_id,
                    model_version=lifecycle_model_version,
                )
            else:
                self._repository.expire_shadow_attempt_if_due(
                    attempt_id=shadow_attempt_id,
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    evaluation_id=evaluation_id,
                    observed_at=evaluated_at,
                    market_asof=features.asof,
                    runtime_instance_id=self._runtime_instance_id,
                    model_version=lifecycle_model_version,
                )
        if not evaluation.actionable:
            return None
        subtype = evaluation.subtype or ""
        model_version = str(evaluation.metadata.get("model_version", "climax-v1"))
        if self._repository.has_signal_for_event(
            symbol, state.event_id, subtype, model_version
        ):
            self._remove_climax_candidate(
                symbol, state.event_id, reason="duplicate_signal"
            )
            return None
        high = float(evaluation.metadata.get("event_high") or features.price)
        decision = SignalDecision(
            symbol=symbol,
            event_id=state.event_id,
            signal_type=SignalType.CONFIRM,
            grade=evaluation.grade,
            score=evaluation.score,
            market_price=features.price,
            short_zone_low=high * 0.97,
            short_zone_high=high,
            signal_time=features.asof,
            reasons=[
                "Climax exhaustion confirmed",
                "Failed continuation below event high",
            ],
            risk_flags=["Повышенный риск ликвидности"]
            if evaluation.metadata.get("liquidity_warning")
            else [],
            features_snapshot=asdict(features),
            score_breakdown={"climax_v1": float(evaluation.score)},
            decision_type="SIGNAL",
            actionable=True,
            lifecycle_state="CLIMAX_SIGNAL_SENT",
            strategy_type="CLIMAX_EXHAUSTION",
            strategy_subtype=subtype,
            model_version=model_version,
            strategy_metadata={
                **evaluation.metadata,
                "veto_reasons": evaluation.veto_reasons,
                "fast_monitor": fast_monitor,
            },
        )
        admission_evaluation_id = evaluation_id
        # VCU pre-delivery re-check parity with LVEF (lane-B experiment).
        # Consistency-veto safety audit (2026-09-15, live DB read-only): among
        # 4360 climax_evaluations rows with subtype_candidate=VOLUME_CLIMAX_UNWIND
        # in the last 7 days, 4360 carry entry_distance_below_high_pct and 0 lack
        # it, so a missing-key row cannot realistically trigger this veto; the
        # hard veto is kept. A malformed row would be suppressed WITH a logged
        # warning (never silently dropped).
        recheck_subtypes = {"LOW_VOLUME_EXTENSION_FAILURE"}
        if getattr(self._config, "climax_recheck_extended", True):
            recheck_subtypes.add("VOLUME_CLIMAX_UNWIND")
        if decision.strategy_subtype in recheck_subtypes:
            event_high = float(decision.strategy_metadata.get("event_high") or 0.0)
            stored_distance = float(
                decision.strategy_metadata.get("entry_distance_below_high_pct") or 0.0
            )
            computed_distance = (
                ((event_high - decision.market_price) / event_high * 100)
                if event_high
                else 999.0
            )
            if abs(computed_distance - stored_distance) > 0.15:
                self._logger.warning(
                    "Climax delivery veto: inconsistent_event_high_metadata symbol=%s",
                    symbol,
                )
                return None
            if getattr(self._scanner, "supports_symbol_frames", hasattr(self._scanner, "fetch_symbol_frames")):
                decision_market = (
                    await self._market_data_provider.capture_decision_snapshot(symbol)
                )
                if decision_market is not None and not decision_market.frame_1m.frame.empty:
                    fresh_frame = decision_market.frame_1m.frame.copy(deep=True)
                    fresh_derivatives = dict(decision_market.derivatives)
                    fresh_liquidity = dict(decision_market.liquidity or {})
                    recheck_market_asof = datetime.now(timezone.utc)
                    fresh_features = self._feature_builder.build(
                        symbol,
                        fresh_frame,
                        state=state,
                        derivatives=fresh_derivatives,
                        liquidity=fresh_liquidity,
                        market_asof=recheck_market_asof,
                    )
                    fresh_bundle = evaluate_climax_bundle(
                        state,
                        fresh_features,
                        fresh_frame,
                        self._config,
                        strict_closed_candles=True,
                    )
                    fresh_eval = fresh_bundle.selected
                    fresh_evaluation_id = self._repository.record_climax_evaluation(
                        evaluation_time=datetime.now(timezone.utc),
                        symbol=symbol,
                        strategy="CLIMAX_EXHAUSTION",
                        subtype_candidate=fresh_eval.metadata.get("strategy_subtype"),
                        model_version=str(
                            fresh_eval.metadata.get("model_version", "climax-v1")
                        ),
                        event_id=state.event_id,
                        event_high=fresh_eval.metadata.get("event_high"),
                        event_high_time=state.event_high_time,
                        event_detected_at=state.event_start_time,
                        candidate_added_at=candidate_added_at,
                        candidate_age_sec=candidate_age_sec,
                        fast_monitor=fast_monitor,
                        poll_sequence=poll_sequence,
                        frame_asof=fresh_features.asof,
                        candles_asof=fresh_features.asof,
                        oi_asof=fresh_features.asof
                        if fresh_features.oi_change_pct is not None
                        else None,
                        orderbook_asof=fresh_features.asof
                        if fresh_features.liquidity_available
                        else None,
                        score=fresh_eval.score,
                        grade=fresh_eval.grade,
                        actionable=fresh_eval.actionable,
                        admission_passed=not fresh_eval.veto_reasons,
                        veto_reasons=fresh_eval.veto_reasons,
                        passed_conditions=[],
                        data_quality=fresh_eval.data_quality,
                        liquidity={
                            "available": fresh_features.liquidity_available,
                            "spread_pct": fresh_features.spread_pct,
                            "slippage_pct": fresh_features.slippage_pct,
                            "depth_1pct_usdt": fresh_features.orderbook_depth_usdt_1pct,
                            "depth_2pct_usdt": fresh_features.orderbook_depth_usdt_2pct,
                        },
                        oi={
                            "status": fresh_features.derivatives_status,
                            "change_pct": fresh_features.oi_change_pct,
                        },
                        features=asdict(fresh_features),
                        lifecycle_state="DELIVERY_RECHECK",
                        telegram_eligible=fresh_eval.actionable,
                        runtime_instance_id=self._runtime_instance_id,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        attempt_id=shadow_attempt_id,
                        observed_at=fresh_features.asof,
                        market_asof=fresh_features.asof,
                    )
                    observation_results = self._record_strategy_observations(
                        fresh_bundle,
                        evaluation_phase="PRE_DELIVERY_RECHECK",
                        state=state,
                        features=fresh_features,
                        root_event_id=root_event_id,
                        event_revision=event_revision,
                        attempt_id=shadow_attempt_id,
                        evaluation_id=fresh_evaluation_id,
                        lifecycle_shadow=lifecycle_shadow,
                        observed_at=datetime.now(timezone.utc),
                    )
                    await self._report_strategy_observation_failures(
                        observation_results, root_event_id=root_event_id
                    )
                    if (
                        not fresh_eval.actionable
                        or fresh_eval.subtype != decision.strategy_subtype
                    ):
                        self._logger.info(
                            "Climax delivery veto: fresh_admission_failed symbol=%s reasons=%s",
                            symbol,
                            fresh_eval.veto_reasons,
                        )
                        return None
                    admission_evaluation_id = fresh_evaluation_id
        if not live_delivery_enabled(decision, self._config):
            self._logger.warning(
                "live_delivery_disabled strategy_type=%s strategy_subtype=%s",
                decision.strategy_type,
                decision.strategy_subtype,
            )
            return None
        # Symbol-level rate limiting before a signal row exists (lane-B):
        # per-symbol cooldown window and rolling 24 h cap. Suppression is
        # logged with a distinct reason; never silently dropped.
        cooldown_now = datetime.now(timezone.utc)
        cooldown_minutes = getattr(self._config, "climax_event_cooldown_minutes", 90)
        if cooldown_minutes > 0 and self._repository.has_recent_signal_for_symbol(
            symbol, since=cooldown_now - timedelta(minutes=cooldown_minutes)
        ):
            self._logger.info(
                "Climax delivery veto: climax_event_cooldown symbol=%s window_minutes=%s",
                symbol,
                cooldown_minutes,
            )
            return None
        max_signals_24h = getattr(self._config, "max_signals_per_symbol_24h", 3)
        emitted_24h = self._repository.count_recent_signals_for_symbol(
            symbol, since=cooldown_now - timedelta(hours=24)
        )
        if max_signals_24h > 0 and emitted_24h >= max_signals_24h:
            self._logger.info(
                "Climax delivery veto: max_signals_per_symbol_24h symbol=%s emitted=%s cap=%s",
                symbol,
                emitted_24h,
                max_signals_24h,
            )
            return None
        payload = format_signal_message(decision, self._config.timezone)
        record = self._repository.save_signal(
            decision,
            state,
            telegram_sent=False,
            delivery_payload=payload,
            provenance=self._signal_provenance(
                decision,
                root_event_id=root_event_id,
                decision_evaluation_id=evaluation_id,
                admission_evaluation_id=admission_evaluation_id,
            ),
        )
        if shadow_attempt_id is not None:
            # Gate attribution: correlate the persisted signal with the entry
            # attempt that produced it (idempotent repository write).
            self._repository.link_attempt_signal(
                attempt_id=shadow_attempt_id, signal_id=record.id
            )

        self._delivery_worker.wake()
        state = self._baseline_lifecycle.mark_signal_sent(
            state, signal_id=record.id, when=features.asof
        )
        self._state_store.save(state)
        self._remove_climax_candidate(symbol, state.event_id, reason="signal_created")
        return decision

    def _record_strategy_observations(
        self,
        bundle: ClimaxEvaluationBundle,
        *,
        evaluation_phase: str,
        state: EventState,
        features: SymbolFeatures,
        root_event_id: str,
        event_revision: int,
        attempt_id: str | None,
        evaluation_id: int | None,
        lifecycle_shadow: object | None,
        observed_at: datetime,
    ) -> list[ObservationWriteResult]:
        """Persist every enabled climax branch without affecting live evaluation."""

        record_observation = getattr(
            self._repository, "record_strategy_observation", None
        )
        if record_observation is None:
            return []
        run_id = uuid.uuid4().hex
        results: list[ObservationWriteResult] = []
        for strategy, branch_evaluation in bundle.branch_evaluations.items():
            try:
                branch_candidate_id = climax_branch_candidate_id(
                    root_event_id, strategy, event_revision
                )
                branch_attempt_id = climax_branch_attempt_id(
                    root_event_id, strategy, event_revision
                )
                snapshot = {
                    "event": {
                        "event_id": state.event_id,
                        "root_event_id": root_event_id,
                        "event_revision": event_revision,
                        "candidate_id": branch_candidate_id,
                        "attempt_id": branch_attempt_id,
                        "event_high": state.event_high,
                    },
                    "evaluation": {
                        "strategy": strategy,
                        "evaluation_phase": evaluation_phase,
                        "score": branch_evaluation.score,
                        "grade": branch_evaluation.grade,
                        "metadata": branch_evaluation.metadata,
                        "blockers": branch_evaluation.veto_reasons,
                        "warnings": branch_evaluation.data_quality,
                    },
                    "features": asdict(features),
                }
                evidence = build_observation_evidence(snapshot)
                if evidence.warnings:
                    self._logger.warning(
                        "strategy observation evidence sanitized strategy=%s symbol=%s field_paths=%s non_finite_count=%s",
                        strategy,
                        features.symbol,
                        [warning["path"] for warning in evidence.warnings],
                        len(evidence.warnings),
                    )
                model_version = str(
                    branch_evaluation.metadata.get("model_version", "climax-v1")
                )
                idempotency_key = make_observation_idempotency_key(
                    strategy_family="CLIMAX_EXHAUSTION",
                    strategy=strategy,
                    symbol=features.symbol,
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    evaluation_phase=evaluation_phase,
                    market_asof=features.asof,
                    input_fingerprint=evidence.input_fingerprint,
                    model_version=model_version,
                    config_hash=self._strategy_config_hash,
                )
                observation = StrategyObservation(
                    observation_id=uuid.uuid4().hex,
                    idempotency_key=idempotency_key,
                    run_id=run_id,
                    runtime_instance_id=self._runtime_instance_id,
                    runtime_started_at=self._runtime_started_at,
                    code_version=self._code_version,
                    strategy_family="CLIMAX_EXHAUSTION",
                    strategy=strategy,
                    evaluation_phase=evaluation_phase,
                    symbol=features.symbol,
                    root_event_id=root_event_id,
                    event_revision=event_revision,
                    attempt_id=branch_attempt_id,
                    evaluation_id=evaluation_id
                    if branch_evaluation is bundle.selected
                    else None,
                    signal_id=None,
                    observed_at=observed_at,
                    exchange_time=None,
                    market_asof=features.asof,
                    live_decision=self._observation_live_decision(
                        branch_evaluation, evaluation_phase
                    ),
                    shadow_decision=self._observation_shadow_decision(
                        strategy, lifecycle_shadow
                    ),
                    score=branch_evaluation.score,
                    blockers=list(branch_evaluation.veto_reasons),
                    warnings=list(branch_evaluation.data_quality),
                    market_price=features.price,
                    event_high=branch_evaluation.metadata.get("event_high")
                    or state.event_high,
                    model_version=model_version,
                    config_hash=self._strategy_config_hash,
                    input_fingerprint=evidence.input_fingerprint,
                    input_snapshot=evidence.snapshot,
                )
                results.append(record_observation(observation))
            except Exception:
                self._logger.exception(
                    "strategy observation preparation failed strategy=%s symbol=%s root_event_id=%s",
                    strategy,
                    features.symbol,
                    root_event_id,
                )
                results.append(ObservationWriteResult(ObservationWriteStatus.FAILED))
        return results

    def _signal_provenance(
        self,
        decision: SignalDecision,
        *,
        root_event_id: str | None = None,
        decision_evaluation_id: int | None = None,
        admission_evaluation_id: int | None = None,
    ) -> SignalProvenanceInput:
        """Build immutable evidence for a signal without changing admission semantics."""

        if decision.strategy_type == "BASELINE_PULLBACK":
            return SignalProvenanceInput(
                strategy_family="BASELINE_PULLBACK",
                strategy_branch="BASELINE_PULLBACK",
                event_id=decision.event_id,
                root_event_id=None,
                decision_evaluation_id=None,
                admission_evaluation_id=None,
                code_version=self._code_version,
                config_hash=self._strategy_config_hash,
                runtime_instance_id=self._runtime_instance_id,
                runtime_started_at=self._runtime_started_at,
                decision_at=decision.signal_time,
            )
        if decision.strategy_type == "TRAPPED_LONGS_REVERSAL":
            if root_event_id is None or decision_evaluation_id is None:
                raise ValueError("trapped longs signal has incomplete provenance")
            return SignalProvenanceInput(
                strategy_family="TRAPPED_LONGS_REVERSAL",
                strategy_branch="TRAPPED_LONGS_REVERSAL",
                event_id=decision.event_id,
                root_event_id=root_event_id,
                decision_evaluation_id=decision_evaluation_id,
                admission_evaluation_id=admission_evaluation_id,
                code_version=self._code_version,
                config_hash=self._strategy_config_hash,
                runtime_instance_id=self._runtime_instance_id,
                runtime_started_at=self._runtime_started_at,
                decision_at=decision.signal_time,
                decision_entry_price=decision.market_price,
                decision_event_high=float(decision.strategy_metadata.get("event_high") or 0.0),
                provenance_anomaly=_provenance_anomaly(decision),
            )
        if (
            decision.strategy_type != "CLIMAX_EXHAUSTION"
            or decision.strategy_subtype
            not in {"VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"}
            or root_event_id is None
            or decision_evaluation_id is None
        ):
            raise ValueError(
                "actionable signal has incomplete deterministic provenance"
            )
        return SignalProvenanceInput(
            strategy_family="CLIMAX_EXHAUSTION",
            strategy_branch=decision.strategy_subtype,
            event_id=decision.event_id,
            root_event_id=root_event_id,
            decision_evaluation_id=decision_evaluation_id,
            admission_evaluation_id=admission_evaluation_id,
            code_version=self._code_version,
            config_hash=self._strategy_config_hash,
            runtime_instance_id=self._runtime_instance_id,
            runtime_started_at=self._runtime_started_at,
            decision_at=decision.signal_time,
            decision_entry_price=decision.market_price,
            decision_event_high=float(decision.strategy_metadata.get("event_high"))
            if decision.strategy_metadata.get("event_high") is not None
            else None,
            decision_distance_from_high=float(
                decision.strategy_metadata.get("entry_distance_below_high_pct")
            )
            if decision.strategy_metadata.get("entry_distance_below_high_pct")
            is not None
            else None,
            provenance_anomaly=_provenance_anomaly(decision),
        )

    @staticmethod
    def _observation_live_decision(
        evaluation: ClimaxEvaluation, evaluation_phase: str
    ) -> str:
        if evaluation_phase == "EVENT_EXPIRED":
            return "EXPIRED"
        if evaluation.actionable:
            return "ACTIONABLE"
        if evaluation.veto_reasons:
            return "BLOCKED"
        return "CANDIDATE"

    @staticmethod
    def _observation_shadow_decision(
        strategy: str, lifecycle_shadow: object | None
    ) -> str:
        if strategy != "VOLUME_CLIMAX_UNWIND" or lifecycle_shadow is None:
            return "NOT_EVALUATED"
        state = getattr(lifecycle_shadow, "state", None)
        if state in {"WATCHING", "FALLBACK_READY", "EXPIRED"}:
            return state
        return "REJECTED"

    async def _report_strategy_observation_failures(
        self,
        results: list[ObservationWriteResult],
        *,
        root_event_id: str | None = None,
    ) -> None:
        failures = [
            result
            for result in results
            if result.status is ObservationWriteStatus.FAILED
        ]
        if not failures:
            return
        self._health.on_strategy_observation_write_failure(len(failures))
        if any(result.failure_classification == "SQLITE_FULL" for result in failures):
            await self._emit_storage_incident(
                self._storage_incident.record_failure(at=time.time())
            )
            return
        alert_key = (
            f"strategy_observation_ledger_write_failed:{root_event_id}"
            if root_event_id
            else "strategy_observation_ledger_write_failed"
        )
        if not self._error_throttler.should_send(alert_key):
            return
        try:
            await self._notifier.send_alert(
                "Strategy observation ledger write failed; scanner delivery continues."
            )
        except Exception:
            self._logger.exception(
                "strategy observation ledger failure alert could not be sent"
            )


    def _run_causal_pump_v2_shadow(self, symbol: str, frame_1m: pd.DataFrame, features: Any, state: EventState | None, now: datetime) -> None:
        if not self._config.causal_pump_v2_shadow_enabled or not isinstance(frame_1m, pd.DataFrame):
            return
        config = _causal_pump_v2_config(self._config)
        current = self._repository.get_active_causal_pump_v2_root(symbol, now=now)
        if current is None:
            decision = CausalPumpV2Detector(config).detect(symbol, frame_1m, asdict(features), now)
            if decision is None or decision.root_id is None:
                return
            created_at = now
        else:
            decision = CausalPumpV2Tracker(config).advance(CausalV2RootDecision(**{key: current[key] for key in CausalV2RootDecision.__dataclass_fields__ if key in current}), frame_1m, asdict(features), now)
            created_at = current["created_at"]
        expires_at = (decision.event_high_time + timedelta(minutes=config.expiry_minutes) if decision.event_high_time else now + timedelta(minutes=config.expiry_minutes))
        changed = self._repository.upsert_causal_pump_v2_root({**asdict(decision), "symbol": symbol, "created_at": created_at, "updated_at": now, "expires_at": expires_at, "trigger_window": decision.condition_snapshot.get("trigger_window")})
        if not changed:
            return
        v1 = state
        comparison = CausalPumpV2Comparator().compare(symbol, now, v1.event_id if v1 else None, v1.state.value if v1 else None, v1.event_high_time if v1 else None, v1.event_high if v1 else None, v1.trigger_window if v1 else None, decision.root_id, decision.state, decision.event_low_time, decision.event_low_price, decision.event_high_time, decision.event_high_price, decision.condition_snapshot.get("trigger_window"), features.price)
        self._repository.record_causal_pump_v2_comparison(asdict(comparison))

    def _evaluate_early_drop_warning(
        self, state: EventState, features: SymbolFeatures, frame_1m: pd.DataFrame
    ) -> None:
        """Persist an optional LANE-B observation without entering signal flow."""
        if not self._config.early_drop_warning_enabled:
            return
        if not isinstance(frame_1m, pd.DataFrame) or len(frame_1m) < 2:
            return
        try:
            latest_at = frame_1m.index[-1].to_pydatetime()
            previous_close = float(frame_1m["close"].iloc[-2])
            current_close = float(frame_1m["close"].iloc[-1])
        except (KeyError, IndexError, TypeError, ValueError):
            return
        root_id = str((state.event_features_snapshot or {}).get("root_event_id") or state.event_id)
        revision = int((state.event_features_snapshot or {}).get("root_event_revision") or 1)
        evaluation = evaluate_early_drop_warning(
            EarlyDropSnapshot(
                root_event_id=root_id,
                event_revision=revision,
                symbol=features.symbol,
                root_started_at=state.event_start_time,
                observed_at=features.asof,
                market_asof=features.market_asof or features.asof,
                latest_candle_at=latest_at,
                impulse_pct=state.event_range_pct or features.event_range_pct,
                drawdown_pct=features.pullback_from_high_pct,
                event_high=state.event_high,
                price=features.price,
                previous_close=previous_close,
                current_close=current_close,
                spread_pct=features.spread_pct,
                slippage_pct=features.slippage_pct,
                depth_1pct_usdt=features.orderbook_depth_usdt_1pct,
                depth_2pct_usdt=features.orderbook_depth_usdt_2pct,
            ),
            max_candle_age=timedelta(seconds=self._config.early_drop_warning_max_candle_age_sec),
        )
        if not evaluation.warning:
            self._repository.save_early_drop_warning(evaluation, None)
            return
        if self._repository.has_recent_early_drop_warning(
            evaluation.symbol,
            now=features.asof,
            cooldown=timedelta(minutes=self._config.early_drop_warning_cooldown_minutes),
        ):
            return
        payload = (
            format_early_drop_warning(evaluation)
            if self._config.early_drop_warning_send_to_telegram
            else None
        )
        write = self._repository.save_early_drop_warning(evaluation, payload)
        if write.created and payload is not None:
            self._delivery_worker.wake()

    async def _process_symbol(
        self,
        symbol: str,
        frame_1m: pd.DataFrame,
        state: EventState | None,
        *,
        market_snapshot: object | None = None,
    ) -> tuple[SignalDecision | None, EventState | None]:
        market_asof = datetime.now(timezone.utc)
        derivatives = (
            dict(getattr(market_snapshot, "derivatives"))
            if market_snapshot is not None
            else await self._scanner.fetch_optional_derivatives(symbol)
        )
        captured_liquidity = (
            getattr(market_snapshot, "liquidity", None)
            if market_snapshot is not None
            else None
        )
        snapshot_liquidity = (
            dict(captured_liquidity)
            if captured_liquidity is not None
            else None
        )
        evaluation_market_snapshot = market_snapshot
        features = self._feature_builder.build(
            symbol,
            frame_1m,
            state=state,
            derivatives=derivatives,
            market_asof=market_asof,
        )
        now = features.asof
        self._run_causal_pump_v2_shadow(symbol, frame_1m, features, state, now)
        shadow_candidate_id = self._observe_root_detector_shadow(features, state, now)
        if shadow_candidate_id and state is not None and state.event_id:
            self._repository.link_root_detector_shadow_candidate(
                shadow_candidate_id,
                root_event_id=state.event_id,
                root_created_at=state.event_start_time or now,
                peak_time=state.event_high_time,
            )
        if state is None or state.state in {EventStatus.IDLE, EventStatus.EXPIRED}:
            if isinstance(frame_1m, pd.DataFrame):
                observation = NormalizedMarketObservation(
                    symbol, now, CandleObservationKind.PARTIAL_ASOF_CANDLE, frame_1m
                )
                new_state = self._baseline_lifecycle.detect_event(
                    observation, features=features
                ).state
            else:
                # Preserve the injected-feature seam used by legacy runtime
                # characterization tests; production frames are always strict
                # normalized observations.
                new_state = self._pump_detector.build_event(
                    symbol, frame_1m, features, now
                )
            if new_state is None:
                return None, new_state
            if shadow_candidate_id:
                self._repository.link_root_detector_shadow_candidate(
                    shadow_candidate_id,
                    root_event_id=new_state.event_id,
                    root_created_at=now,
                    peak_time=new_state.event_high_time,
                )
                if getattr(self._config, "root_detector_shadow_v2_enabled", False):
                    self._repository.link_root_detector_shadow_v2_live_root(
                        episode_id=shadow_candidate_id,
                        root_event_id=new_state.event_id,
                        live_root_created_at=now,
                    )
            if self._config.climax_short_enabled:
                self._track_climax_candidate(new_state, now)
                if market_snapshot is not None and snapshot_liquidity is None:
                    evaluation_market_snapshot = await self._enrich_decision_market_snapshot(
                        symbol, market_snapshot, features.price
                    )
                    liquidity = dict(
                        getattr(evaluation_market_snapshot, "liquidity", None) or {}
                    )
                else:
                    liquidity = await self._resolved_liquidity(
                        symbol, features.price, market_snapshot, snapshot_liquidity
                    )
                features = self._feature_builder.build(
                    symbol,
                    frame_1m,
                    state=new_state,
                    derivatives=derivatives,
                    liquidity=liquidity,
                    market_asof=market_asof,
                )
                self._evaluate_early_drop_warning(new_state, features, frame_1m)
                climax_decision = await self._evaluate_and_send_climax(
                    symbol,
                    frame_1m,
                    new_state,
                    features=features,
                    market_snapshot=evaluation_market_snapshot,
                )
                if climax_decision is not None:
                    return climax_decision, new_state
            early_watch = await self._maybe_emit_early_pump_watch(
                new_state, features, now
            )
            return early_watch, new_state

        if self._config.climax_short_enabled:
            self._track_climax_candidate(state, now)
            if market_snapshot is not None and snapshot_liquidity is None:
                evaluation_market_snapshot = await self._enrich_decision_market_snapshot(
                    symbol, market_snapshot, features.price
                )
                liquidity = dict(
                    getattr(evaluation_market_snapshot, "liquidity", None) or {}
                )
            else:
                liquidity = await self._resolved_liquidity(
                    symbol, features.price, market_snapshot, snapshot_liquidity
                )
            features = self._feature_builder.build(
                symbol,
                frame_1m,
                state=state,
                derivatives=derivatives,
                liquidity=liquidity,
                market_asof=market_asof,
            )
            self._evaluate_early_drop_warning(state, features, frame_1m)
            climax_decision = await self._evaluate_and_send_climax(
                symbol,
                frame_1m,
                state,
                features=features,
                market_snapshot=evaluation_market_snapshot,
            )
            if climax_decision is not None:
                return climax_decision, state

        # Preserve the live-runtime compatibility hook and its established
        # ordering.  Historical replay has no runtime cancellation hook; the
        # shared lifecycle owns only the source-derived BASELINE transitions.
        if self._should_cancel_after_high_break(state, features, now):
            state.state = EventStatus.EXPIRED
            state.expires_at = now
            state.updated_at = now
            return None, state

        if isinstance(frame_1m, pd.DataFrame):
            observation = NormalizedMarketObservation(
                symbol, now, CandleObservationKind.PARTIAL_ASOF_CANDLE, frame_1m
            )
            lifecycle = self._baseline_lifecycle.advance_active_event(
                observation,
                state,
                derivatives=derivatives,
                features=features,
            )
        else:
            # Characterization/legacy adapters may inject a pre-built feature
            # snapshot with a sentinel frame.  Keep the same shared lifecycle
            # implementation while skipping strict market-frame validation.
            lifecycle = self._baseline_lifecycle.advance_active_event_from_features(
                state,
                features,
                now,
                derivatives=derivatives,
            )
        state = lifecycle.state
        if state is None or state.state == EventStatus.EXPIRED:
            return None, state
        await self._evaluate_trapped_longs_v2_shadow(
            state, features, frame_1m, derivatives=derivatives
        )
        if state.state == EventStatus.PUMP_DETECTED:
            return await self._maybe_emit_early_pump_watch(state, features, now), state

        zone = lifecycle.short_zone
        if zone is None:
            return None, state

        state.zone_low = zone.low
        state.zone_high = zone.high
        if market_snapshot is not None and snapshot_liquidity is None:
            evaluation_market_snapshot = await self._enrich_decision_market_snapshot(
                symbol, market_snapshot, features.price
            )
            liquidity = dict(
                getattr(evaluation_market_snapshot, "liquidity", None) or {}
            )
        else:
            liquidity = await self._resolved_liquidity(
                symbol, features.price, market_snapshot, snapshot_liquidity
            )
        features = self._feature_builder.build(
            symbol,
            frame_1m,
            state=state,
            derivatives=derivatives,
            liquidity=liquidity,
            market_asof=market_asof,
        )

        if (
            zone.low <= features.price <= zone.high
            and state.state == EventStatus.PULLBACK_OBSERVED
        ):
            state.state = EventStatus.SHORT_ZONE_ACTIVE

        await self._evaluate_and_send_trapped_longs(state, features, frame_1m)
        evaluation = self._signal_engine.analyze(state, features, zone, now)
        self._repository.record_reject_stat(
            symbol=symbol,
            timeframe=state.trigger_window or "15m",
            decision_type=evaluation.decision.decision_type
            if evaluation.decision
            else "REJECT",
            score=evaluation.score,
            reasons=evaluation.reject_reasons,
            blockers=evaluation.blockers,
            risk_flags=evaluation.risk_flags,
            close_to_watch=evaluation.close_to_watch,
            squeeze_risk_level=evaluation.squeeze_risk_level,
            derivatives_status=features.derivatives_status,
            derivatives_reasons=features.derivatives_reasons,
            data_quality_warnings=evaluation.data_quality_warnings,
            logged_at=now,
        )
        decision = evaluation.decision
        if decision is None or state.signal_id is not None:
            return None, state

        if decision.signal_type == SignalType.WATCH:
            watch_type = _watch_delivery_type(decision)
            if _watch_already_emitted(state, watch_type=watch_type):
                return None, state
            delivery_enabled = (
                self._config.send_watch_to_telegram
                and self._watch_enqueued_in_cycle < self._config.watch_max_per_cycle
            )
            payload = (
                format_signal_message(decision, self._config.timezone)
                if delivery_enabled
                else None
            )
            self._repository.save_watch_candidate(
                decision, state, telegram_sent=False, delivery_payload=payload
            )
            if delivery_enabled:
                self._watch_enqueued_in_cycle += 1
                self._delivery_worker.wake()
            _mark_watch_emitted(state, watch_type=watch_type)
            return decision, state

        if not live_delivery_enabled(decision, self._config):
            self._logger.warning(
                "live_delivery_disabled strategy_type=%s strategy_subtype=%s",
                decision.strategy_type,
                decision.strategy_subtype,
            )
            return None, state
        payload = format_signal_message(decision, self._config.timezone)
        record = self._repository.save_signal(
            decision,
            state,
            telegram_sent=False,
            delivery_payload=payload,
            provenance=self._signal_provenance(decision),
        )
        self._delivery_worker.wake()
        state = self._baseline_lifecycle.mark_signal_sent(
            state, signal_id=record.id, when=now
        )
        return decision, state

    def _observe_root_detector_shadow(
        self, features: SymbolFeatures, state: EventState | None, now: datetime
    ) -> str | None:
        """Observe early acceleration only; this function cannot affect live decisions."""
        high = float(
            state.event_high
            if state and state.event_high
            else max(features.last_high, features.price)
        )
        high_time = (
            state.event_high_time
            if state and state.event_high_time
            else (features.last_high_time or now)
        )
        candidate = ShadowCandidateInput(
            symbol=features.symbol,
            first_seen_at=now,
            rotation_id=self._shadow_rotation_id,
            price=features.price,
            event_high=high,
            high_time=high_time,
            pump_5m=features.ret_5m,
            pump_15m=features.ret_15m,
            pump_1h=features.ret_1h,
            pump_4h=features.ret_4h,
            volume_ratio=None,
            volume_z=features.vol_zscore_30m,
            oi_5m=features.oi_change_pct,
            oi_15m=features.oi_change_15m,
            oi_1h=features.oi_change_1h,
            distance_from_high=features.distance_to_event_high_pct,
            conditions={
                "pump_5m": "PASS" if features.ret_5m >= 2 else "FAIL",
                "pump_15m": "PASS" if features.ret_15m >= 4 else "FAIL",
                "pump_1h": "PASS" if features.ret_1h >= 7 else "FAIL",
                "pump_4h": "PASS" if features.ret_4h >= 10 else "FAIL",
                "volume_z": "PASS" if features.vol_zscore_30m > 0 else "MISSING",
            },
        )
        if not should_create_shadow_candidate(candidate):
            return None
        candidate_payload = {
            "candidate_id": "pending",
            "symbol": candidate.symbol,
            "first_seen_at": candidate.first_seen_at,
            "rotation_id": candidate.rotation_id,
            "price": candidate.price,
            "event_high": candidate.event_high,
            "high_time": candidate.high_time,
            "pump_5m": candidate.pump_5m,
            "pump_15m": candidate.pump_15m,
            "pump_1h": candidate.pump_1h,
            "pump_4h": candidate.pump_4h,
            "volume_ratio": candidate.volume_ratio,
            "volume_z": candidate.volume_z,
            "oi_5m": candidate.oi_5m,
            "oi_15m": candidate.oi_15m,
            "oi_1h": candidate.oi_1h,
            "distance_from_high": candidate.distance_from_high,
            "conditions_json": candidate.conditions or {},
            "live_root_created": False,
            "code_version": self._code_version,
            "config_hash": self._config_fingerprint,
            "runtime_instance_id": self._runtime_instance_id,
            "runtime_started_at": self._runtime_started_at,
            "observations_json": [],
            "outcome_json": {},
        }
        candidate_id = self._repository.record_root_detector_shadow_episode_observation(
            candidate_payload
        )
        if not candidate_id:
            return None
        candidate_payload["candidate_id"] = candidate_id
        if getattr(self._config, "root_detector_shadow_v2_enabled", False):
            v2 = evaluate_v2_observation(
                episode_id=candidate_id,
                symbol=candidate.symbol,
                episode_opened_at=candidate.first_seen_at,
                observed_at=now,
                reference_price=candidate.price,
                event_high=candidate.event_high,
                features={
                    "pump_5m": candidate.pump_5m,
                    "pump_15m": candidate.pump_15m,
                    "pump_1h": candidate.pump_1h,
                    "pump_4h": candidate.pump_4h,
                    "volume_ratio": candidate.volume_ratio,
                    "volume_z": candidate.volume_z,
                    "oi_5m": candidate.oi_5m,
                    "oi_15m": candidate.oi_15m,
                    "oi_1h": candidate.oi_1h,
                    "distance_from_high": candidate.distance_from_high,
                },
                current_live_root_id=state.event_id if state is not None else None,
                contract=default_counterfactual_contract(),
            )
            if v2.root_id:
                self._repository.record_root_detector_shadow_v2_root(
                    {
                        "shadow_v2_root_id": v2.root_id,
                        "episode_id": candidate_id,
                        "symbol": candidate.symbol,
                        "state": v2.state,
                        "created_at": now,
                        "episode_opened_at": candidate.first_seen_at,
                        "episode_age_seconds": max(
                            0.0, (now - candidate.first_seen_at).total_seconds()
                        ),
                        "reference_price": candidate.price,
                        "event_high_at_decision": candidate.event_high,
                        "distance_from_high": candidate.distance_from_high,
                        "pump_5m": candidate.pump_5m,
                        "pump_15m": candidate.pump_15m,
                        "pump_1h": candidate.pump_1h,
                        "pump_4h": candidate.pump_4h,
                        "volume_ratio": candidate.volume_ratio,
                        "volume_z": candidate.volume_z,
                        "oi_features_json": {
                            "oi_5m": candidate.oi_5m,
                            "oi_15m": candidate.oi_15m,
                            "oi_1h": candidate.oi_1h,
                        },
                        "safety_features_json": {},
                        "conditions_json": v2.condition_snapshot,
                        "downstream_features_json": {},
                        "readiness_reason": v2.reason,
                        "live_root_present_at_creation": state is not None
                        and state.event_id is not None,
                        "code_version": self._code_version,
                        "runtime_instance_id": self._runtime_instance_id,
                        "runtime_started_at": self._runtime_started_at,
                        "method_version": v2.method_version,
                    }
                )
        self._repository.record_root_detector_shadow_candidate(candidate_payload)
        self._repository.append_root_detector_shadow_observation(
            candidate_id,
            {
                "observed_at": now,
                "price": candidate.price,
                "event_high": candidate.event_high,
                "conditions": candidate.conditions or {},
                "code_version": self._code_version,
            },
        )
        return candidate_id

    async def _maybe_emit_early_pump_watch(
        self,
        state: EventState,
        features: SymbolFeatures,
        now: datetime,
    ) -> SignalDecision | None:
        if not self._config.enable_watch_candidates:
            return None
        if state.state != EventStatus.PUMP_DETECTED:
            return None
        if _early_watch_already_emitted(state, watch_type="EARLY_PUMP_WATCH"):
            return None

        score = _early_watch_score(features, self._config)
        if score < self._config.watch_min_score:
            return None

        blockers = [
            "early_pump_not_mature",
            "no_pullback_observed",
            "no_short_zone_active",
        ]
        reject_reasons = [*blockers, "not_actionable"]
        risk_flags: list[str] = [
            "Observed before pullback maturity.",
            "Not actionable until pullback and short-zone confirmation.",
        ]
        if (
            features.vol_zscore_30m < self._config.vol_zscore_min
            and features.vol_zscore_30m >= _early_watch_volume_threshold(self._config)
        ):
            risk_flags.append(
                "Volume z-score is near the actionable threshold but still below it."
            )

        decision = SignalDecision(
            symbol=features.symbol,
            event_id=state.event_id,
            signal_type=SignalType.WATCH,
            grade=_grade_from_score(score),
            score=score,
            market_price=features.price,
            short_zone_low=state.zone_low or features.price,
            short_zone_high=state.zone_high or features.price,
            signal_time=now,
            reasons=[
                f"Dist to VWAP: +{features.dist_to_vwap_pct:.1f}%",
                f"Dist to EMA20 ATR: {features.dist_to_ema20_atr:.2f}",
                f"Volume z-score 30m: {features.vol_zscore_30m:.2f}",
                f"Range/ATR ratio: {features.range_atr_ratio:.2f}",
                f"Early pump trigger window: {state.trigger_window or '15m'}",
            ],
            risk_flags=risk_flags,
            features_snapshot={**asdict(features), "signal_vwap": features.vwap},
            score_breakdown={
                "momentum_confirms": _early_watch_momentum_confirms(
                    features, self._config
                ),
                "stretch_confirms": _early_watch_stretch_confirms(
                    features, self._config
                ),
                "mode": "early_pump_watch",
            },
            decision_type="EARLY_PUMP_WATCH",
            actionable=False,
            lifecycle_state=state.state.value,
            blockers=blockers,
            squeeze_risk_score=0,
            squeeze_risk_level="LOW",
            squeeze_risk_reasons=[],
            squeeze_guard_action="WATCH_ONLY",
            data_quality_warnings=[],
        )
        _mark_early_watch_emitted(state, watch_type="EARLY_PUMP_WATCH")
        self._repository.record_reject_stat(
            symbol=features.symbol,
            timeframe=state.trigger_window or "15m",
            decision_type=decision.decision_type,
            score=decision.score,
            reasons=reject_reasons,
            blockers=blockers,
            risk_flags=risk_flags,
            close_to_watch=True,
            squeeze_risk_level="LOW",
            derivatives_status=features.derivatives_status,
            derivatives_reasons=features.derivatives_reasons,
            data_quality_warnings=[],
            logged_at=now,
        )
        delivery_enabled = (
            self._config.send_watch_to_telegram
            and self._watch_enqueued_in_cycle < self._config.watch_max_per_cycle
        )
        payload = (
            format_signal_message(decision, self._config.timezone)
            if delivery_enabled
            else None
        )
        self._repository.save_watch_candidate(
            decision, state, telegram_sent=False, delivery_payload=payload
        )
        if delivery_enabled:
            self._watch_enqueued_in_cycle += 1
            self._delivery_worker.wake()
        return decision

    def _finalize_persisted_signal(
        self,
        state: EventState,
        signal_id: int,
        when: datetime,
        *,
        remove_candidate: bool = False,
    ) -> None:
        """Commit the state marker after a durable signal before propagating cancellation."""
        try:
            state = self._baseline_lifecycle.mark_signal_sent(
                state, signal_id=signal_id, when=when
            )
            self._state_store.save(state)
            if remove_candidate:
                self._remove_climax_candidate(
                    state.symbol, state.event_id, reason="signal_created"
                )
        except Exception:
            self._logger.exception(
                "Persisted signal state finalization failed | symbol=%s signal_id=%s",
                state.symbol,
                signal_id,
            )

    async def _resolved_liquidity(
        self,
        symbol: str,
        price: float,
        market_snapshot: object | None,
        captured_liquidity: dict[str, object] | None,
    ) -> dict[str, object]:
        if captured_liquidity is not None:
            return captured_liquidity
        if market_snapshot is None:
            return await self._fetch_optional_liquidity(symbol, price)
        enriched = await self._enrich_decision_market_snapshot(
            symbol, market_snapshot, price
        )
        return dict(enriched.liquidity or {})

    async def _enrich_decision_market_snapshot(
        self, symbol: str, base_snapshot: object, price: float
    ) -> object:
        """Add REST orderbook context only after the existing candidate gates."""
        return await self._market_data_provider.capture_decision_snapshot(
            symbol,
            frame_1m=getattr(base_snapshot, "frame_1m").frame,
            price=price,
            derivatives=getattr(base_snapshot, "derivatives"),
            include_liquidity=True,
        )

    async def _fetch_optional_liquidity(
        self, symbol: str, price: float
    ) -> dict[str, object]:
        fetcher = getattr(self._scanner, "fetch_optional_liquidity", None)
        if fetcher is None:
            return {}
        return await fetcher(symbol, price)

    def _should_cancel_after_high_break(
        self, state: EventState, features: SymbolFeatures, now: datetime
    ) -> bool:
        """Retain the compatibility hook; confirmed highs reset before baseline evaluation."""

        return False

    async def _handle_error(self, key: str, exc: Exception) -> None:
        self._health.on_error()
        self._logger.exception("Runtime error in %s: %s", key, exc)
        if is_sqlite_full(exc):
            await self._emit_storage_incident(
                self._storage_incident.record_failure(at=time.time())
            )
            return
        if self._error_throttler.should_send(key):
            await self._notifier.send_alert(f"Short signal bot error in {key}: {exc!r}")

    def _capacity_snapshot(self) -> DiskCapacitySnapshot | None:
        if not self._repository.db_url.startswith("sqlite:///"):
            return None
        database = Path(self._repository.db_url.removeprefix("sqlite:///"))
        try:
            return read_capacity(database)
        except (FileNotFoundError, OSError) as exc:
            self._logger.warning(
                "Disk capacity snapshot unavailable: %s", type(exc).__name__
            )
            return None

    async def _emit_storage_incident(self, event: object | None) -> None:
        if event is None:
            return
        name = getattr(event, "event", "")
        if name == "DB_STORAGE_FULL_ENTERED":
            message = "DB_STORAGE_FULL_ENTERED: SQLite writes failed because database or disk is full."
        elif name == "DB_STORAGE_FULL_STILL_ACTIVE":
            message = (
                "DB_STORAGE_FULL_STILL_ACTIVE: SQLite storage failure remains active."
            )
        elif name == "DB_STORAGE_RECOVERED":
            message = (
                "DB_STORAGE_RECOVERED: SQLite writes resumed; "
                f"failure_start={getattr(event, 'failure_started_at', None)} "
                f"duration_seconds={getattr(event, 'duration_seconds', None)} "
                f"free_bytes={getattr(event, 'free_bytes', None)} "
                f"free_percent={getattr(event, 'free_percent', None)}"
            )
        else:
            return
        try:
            await self._notifier.send_alert(message)
        except Exception:
            self._logger.exception(
                "Storage incident alert could not be sent | event=%s", name
            )

    async def _emit_disk_health_transition(
        self,
        snapshot: DiskCapacitySnapshot,
        *,
        forced_state: DiskHealthState | None = None,
    ) -> None:
        reserve = derive_reserve_bytes(
            snapshot,
            log_wal_margin_bytes=self._config.disk_log_wal_margin_bytes,
            minimum_reserve_bytes=self._config.disk_min_safety_reserve_bytes,
        )
        state = forced_state or self._storage_incident.health_state(
            free_bytes=snapshot.free_bytes,
            reserve_bytes=reserve,
            free_inodes=snapshot.free_inodes,
        )
        if state == self._disk_health_state:
            return
        self._disk_health_state = state
        self._logger.warning(
            "Disk health transition | state=%s free_bytes=%s free_percent=%.2f "
            "free_inodes=%s db_bytes=%s wal_bytes=%s reserve_bytes=%s",
            state,
            snapshot.free_bytes,
            snapshot.free_percent,
            snapshot.free_inodes,
            snapshot.db_bytes,
            snapshot.wal_bytes,
            reserve,
        )
        if state is DiskHealthState.LOW_SPACE:
            message = f"DISK_CAPACITY_LOW: free_bytes={snapshot.free_bytes} free_percent={snapshot.free_percent:.2f} reserve_bytes={reserve}"
        elif state is DiskHealthState.CRITICAL:
            message = f"DISK_CAPACITY_CRITICAL: free_bytes={snapshot.free_bytes} free_percent={snapshot.free_percent:.2f} reserve_bytes={reserve}"
        else:
            return
        try:
            await self._notifier.send_alert(message)
        except Exception:
            self._logger.exception(
                "Disk capacity alert could not be sent | state=%s", state
            )

    async def _ensure_storage_healthy(self, context: str) -> bool:
        capacity = self._capacity_snapshot()
        if capacity is not None:
            await self._emit_disk_health_transition(capacity)
        try:
            health = self._repository.check_storage_health()
        except Exception as exc:  # noqa: BLE001 - classify storage failures at the boundary
            if is_sqlite_full(exc):
                if capacity is not None:
                    await self._emit_disk_health_transition(
                        capacity, forced_state=DiskHealthState.CRITICAL
                    )
                await self._emit_storage_incident(
                    self._storage_incident.record_failure(at=time.time())
                )
            else:
                await self._handle_error(f"db-heartbeat:{context}", exc)
            return False

        if capacity is not None:
            await self._emit_storage_incident(
                self._storage_incident.record_success(
                    at=time.time(),
                    free_bytes=capacity.free_bytes,
                    free_percent=capacity.free_percent,
                )
            )

        checked_at = str(health.get("checked_at"))
        if checked_at != self._storage_health_last_ok:
            journal_mode = health.get("journal_mode")
            busy_timeout = health.get("busy_timeout")
            self._logger.info(
                "DB heartbeat OK | context=%s db_url=%s journal_mode=%s busy_timeout=%sms checked_at=%s",
                context,
                self._repository.db_url,
                journal_mode,
                busy_timeout,
                checked_at,
            )
            self._storage_health_last_ok = checked_at
        return True


def _early_watch_volume_threshold(config: AppConfig) -> float:
    return min(config.vol_zscore_min * 0.85, 0.70)


def _early_watch_stretch_confirms(features: SymbolFeatures, config: AppConfig) -> int:
    return sum(
        [
            features.dist_to_vwap_pct >= config.event_dist_to_vwap_min,
            features.dist_to_ema20_atr >= config.event_dist_to_ema20_atr_min,
            features.vol_zscore_30m >= _early_watch_volume_threshold(config),
            features.range_atr_ratio >= config.range_atr_bonus_level,
        ]
    )


def _early_watch_momentum_confirms(features: SymbolFeatures, config: AppConfig) -> int:
    return sum(
        [
            features.ret_15m >= config.event_ret_15m_min,
            features.ret_1h >= config.event_ret_1h_min,
            features.ret_4h >= config.event_ret_4h_min,
        ]
    )


def _early_watch_score(features: SymbolFeatures, config: AppConfig) -> int:
    stretch_confirms = _early_watch_stretch_confirms(features, config)
    momentum_confirms = _early_watch_momentum_confirms(features, config)
    return min(100, 45 + (stretch_confirms * 4) + (momentum_confirms * 3))


def _early_watch_already_emitted(state: EventState, watch_type: str) -> bool:
    return _watch_already_emitted(state, watch_type)


def _mark_early_watch_emitted(state: EventState, watch_type: str) -> None:
    _mark_watch_emitted(state, watch_type)


def _watch_already_emitted(state: EventState, watch_type: str) -> bool:
    snapshot = state.event_features_snapshot or {}
    emitted = snapshot.get("emitted_watch_types") or {}
    return emitted.get(watch_type) == state.event_id


def _mark_watch_emitted(state: EventState, watch_type: str) -> None:
    snapshot = dict(state.event_features_snapshot or {})
    emitted = dict(snapshot.get("emitted_watch_types") or {})
    emitted[watch_type] = state.event_id
    snapshot["emitted_watch_types"] = emitted
    state.event_features_snapshot = snapshot


def _watch_delivery_type(decision: SignalDecision) -> str:
    if decision.decision_type == "EARLY_PUMP_WATCH":
        return "EARLY_PUMP_WATCH"
    if decision.grade == "B":
        return "WATCH_B_BLOCKED"
    return "WATCH_C_STRONG_MOVE"


def _grade_from_score(score: int) -> str:
    if score >= 80:
        return "A"
    if score >= 65:
        return "B"
    return "C"


def _observed_at(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return pd.Timestamp(value).to_pydatetime()
    except (TypeError, ValueError):
        return None
