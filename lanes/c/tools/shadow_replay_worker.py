"""Replay one captured Lane C input in one isolated app package process.

The parent CLI starts this worker once for the frozen release and once for the
candidate.  The worker receives only a JSON capture record; it never imports the
running service, opens a database, contacts a provider, or sends Telegram.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import importlib
import inspect
import json
import logging
import sys
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if dataclasses.is_dataclass(value):
        return {
            item.name: _jsonable(getattr(value, item.name))
            for item in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(item) for item in value]
    item = getattr(value, "item", None)
    if callable(item):
        try:
            return _jsonable(item())
        except (TypeError, ValueError):
            return None
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else None
    return value


def _datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _frame(payload: Mapping[str, Any]) -> Any:
    import pandas as pd

    frame_payload = payload.get("frame_1m") or payload.get("frame") or {}
    if isinstance(frame_payload, Mapping) and "rows" in frame_payload:
        rows = list(frame_payload.get("rows") or [])
        frame = pd.DataFrame(rows, columns=frame_payload.get("columns") or None)
        for name in (
            "timestamp",
            "open_time_utc",
            "close_time_utc",
            "availability_time_utc",
            "available_at_utc",
        ):
            if name in frame.columns:
                converted = pd.to_datetime(frame[name], utc=True, errors="coerce")
                if not converted.isna().all():
                    frame[name] = converted
        if "timestamp" not in frame.columns:
            for candidate in ("open_time_utc", "close_time_utc"):
                if candidate in frame.columns:
                    frame["timestamp"] = frame[candidate]
                    break
        index = frame_payload.get("index")
        if isinstance(index, list) and len(index) == len(frame) and index:
            converted = pd.to_datetime(index, utc=True, errors="coerce")
            if not converted.isna().all():
                frame.index = converted
                frame.index.name = frame_payload.get("index_name") or "timestamp"
        if "timestamp" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
            # FeatureBuilder uses the timestamp column; the index is also part
            # of the raw frame contract for resampling and closed-candle gates.
            if not isinstance(frame.index, pd.DatetimeIndex):
                frame = frame.set_index("timestamp", drop=False)
        return frame
    if isinstance(frame_payload, list):
        frame = pd.DataFrame(frame_payload)
        if "timestamp" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
            frame = frame.set_index("timestamp", drop=False)
        return frame
    raise ValueError("capture is missing serialized frame_1m rows")


def _state(payload: Any) -> Any:
    if payload is None:
        return None
    from app.domain import EventState, EventStatus

    values = dict(payload)
    if "state" in values:
        values["state"] = EventStatus(str(values["state"]))
    datetime_fields = (
        "event_start_time",
        "event_high_time",
        "pullback_detected_at",
        "signal_sent_at",
        "expires_at",
        "updated_at",
    )
    for name in datetime_fields:
        if name in values:
            values[name] = _datetime(values[name])
    fields = {item.name for item in dataclasses.fields(EventState)}
    return EventState(**{name: values[name] for name in fields if name in values})


def _config(record: Mapping[str, Any]) -> tuple[Any, str]:
    from app.config import AppConfig

    strategy = record.get("strategy_config") or {}
    payload = dict(strategy.get("payload") or {})
    context = record.get("context") or {}
    payload.update(dict(context.get("evaluation_flags") or {}))
    # A replay must not inherit runtime secrets or paths from this process.
    payload.pop("autoexecution", None)
    config = AppConfig.model_validate(payload)
    fingerprint = str(strategy.get("fingerprint") or "")
    if not fingerprint:
        try:
            from app.replay.config import strategy_config_fingerprint

            fingerprint = strategy_config_fingerprint(config)
        except Exception:
            fingerprint = "captured-config-unknown"
    return config, fingerprint


def _decision_time(record: Mapping[str, Any], market: Mapping[str, Any], frame: Any) -> datetime:
    context = record.get("context") or {}
    for name in ("decision_at_utc", "decision_at"):
        stamp = _datetime(context.get(name))
        if stamp is not None:
            return stamp
    stamp = _datetime(market.get("captured_at_utc"))
    if stamp is not None:
        return stamp
    if "timestamp" in frame.columns:
        return _datetime(frame["timestamp"].iloc[-1]) + timedelta(minutes=1)  # type: ignore[return-value]
    return datetime.now(timezone.utc)


def _fast_monitor(record: Mapping[str, Any]) -> tuple[bool, int | None]:
    value = record.get("fast_monitor") or {}
    return bool(value.get("enabled", False)), value.get("poll_sequence")


def _result_payload(result: Any) -> dict[str, Any]:
    decision = result.decision
    return {
        "features": _jsonable(result.features),
        "state": _jsonable(result.state),
        "decision": _jsonable(decision),
        "evaluations": [_jsonable(item) for item in result.evaluations],
        "blocked_reason": result.blocked_reason,
        "actionable": bool(decision is not None and decision.actionable),
        "identity": (
            {
                name: getattr(decision, name)
                for name in (
                    "symbol",
                    "event_id",
                    "strategy_type",
                    "strategy_subtype",
                    "model_version",
                )
            }
            if decision is not None
            else None
        ),
    }


class _CapturedProvider:
    """Provider-shaped adapter for an explicitly captured fresh recheck."""

    def __init__(
        self,
        market: Mapping[str, Any],
        *,
        symbol: str,
        supports_symbol_frames: bool = False,
    ) -> None:
        frame = _frame(market)
        captured = _datetime(market.get("captured_at_utc")) or datetime.now(timezone.utc)
        availability = _datetime(market.get("max_input_availability_time_utc")) or captured
        frozen = SimpleNamespace(
            frame=frame,
            source=(market.get("frame_1m") or {}).get("source", "CAPTURED"),
            captured_at_utc=captured,
            max_input_availability_time_utc=availability,
        )
        self.snapshot = SimpleNamespace(
            symbol=symbol,
            frame_1m=frozen,
            derivatives=deepcopy(market.get("derivatives") or {}),
            liquidity=deepcopy(market.get("liquidity")),
            captured_at_utc=captured,
            max_input_availability_time_utc=availability,
        )
        self.supports_symbol_frames = bool(supports_symbol_frames)
        self.capture_calls = 0
        self.network_calls = 0

    async def capture_decision_snapshot(self, symbol: str, **_: Any) -> Any:
        self.capture_calls += 1
        return self.snapshot

    async def fetch_optional_derivatives(self, *_: Any, **__: Any) -> Any:
        self.network_calls += 1
        raise RuntimeError("reference replay attempted uncaptured provider derivatives I/O")

    async def fetch_optional_liquidity(self, *_: Any, **__: Any) -> Any:
        self.network_calls += 1
        raise RuntimeError("reference replay attempted uncaptured provider liquidity I/O")


def _candidate_run(record: Mapping[str, Any]) -> dict[str, Any]:
    from app.runtime.pipeline import LaneCStrategyPipeline

    market = record.get("market") or {}
    symbol = str(record.get("symbol") or market.get("symbol") or "")
    if not symbol:
        raise ValueError("capture is missing symbol")
    frame = _frame(market)
    state = _state(record.get("before_state"))
    config, fingerprint = _config(record)
    decision_at = _decision_time(record, market, frame)
    fast_monitor, poll_sequence = _fast_monitor(record)
    pipeline = LaneCStrategyPipeline(config, config_fingerprint=fingerprint)
    if fast_monitor:
        if state is None:
            raise _ReferenceUnknown("fast_monitor_before_state_missing")
        # The frozen fast loop calls only _evaluate_and_send_climax.  A
        # fast capture must therefore use the candidate's dedicated pure
        # climax seam; passing fast_monitor=True to the full evaluate path
        # would still advance baseline/trapped lifecycle state and make a
        # false same-input comparison look like a parity result.
        evaluate_climax_only = getattr(pipeline, "evaluate_climax_only", None)
        if not callable(evaluate_climax_only):
            raise _ReferenceUnknown("candidate_climax_only_api_missing")
        kwargs: dict[str, Any] = {
            "derivatives": market.get("derivatives") or {},
            "liquidity": market.get("liquidity"),
            "decision_at": decision_at,
            "poll_sequence": poll_sequence,
        }
        try:
            parameters = inspect.signature(evaluate_climax_only).parameters
        except (TypeError, ValueError):
            parameters = {}
        if "fast_monitor" in parameters or any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters.values()
        ):
            kwargs["fast_monitor"] = True
        result = evaluate_climax_only(symbol, frame, state, **kwargs)
        if inspect.isawaitable(result):
            result = asyncio.run(result)
    else:
        result = pipeline.evaluate(
            symbol,
            frame,
            state,
            derivatives=market.get("derivatives") or {},
            liquidity=market.get("liquidity"),
            decision_at=decision_at,
            fast_monitor=False,
            poll_sequence=poll_sequence,
        )
    if record.get("final_recheck_required") and record.get("final_recheck_captured"):
        final_market = record.get("final_recheck")
        if not isinstance(final_market, Mapping):
            raise ValueError("final recheck is marked captured but has no market snapshot")
        provider = _CapturedProvider(final_market, symbol=symbol)
        result = asyncio.run(
            pipeline.finalize_admission(
                result,
                provider=provider,
                symbol=symbol,
                frame_1m=frame,
                derivatives=market.get("derivatives") or {},
                decision_at=decision_at,
                fast_monitor=fast_monitor,
                poll_sequence=poll_sequence,
            )
        )
    payload = _result_payload(result)
    payload["comparison_boundary"] = "pre_persistence"
    payload["reference_scope"] = (
        "candidate.runtime.pipeline.LaneCStrategyPipeline.evaluate_climax_only"
        if fast_monitor
        else "candidate.runtime.pipeline.LaneCStrategyPipeline.evaluate"
    )
    return payload



class _ReferenceUnknown(RuntimeError):
    """Replay input lacks a captured fact required by frozen C admission."""


class _ReferenceNotifier:
    """No-network notifier used only at the persistence boundary."""

    def __init__(self) -> None:
        self.send_calls = 0
        self.alert_calls = 0

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def send_signal(self, _payload: str) -> bool:
        self.send_calls += 1
        return False

    async def send_alert(self, _payload: str) -> bool:
        self.alert_calls += 1
        return False


class _ReferenceRepository:
    """In-memory I/O seam; all feature, lifecycle, and admission code stays real."""

    db_url = "sqlite:///:memory:"

    def __init__(self, seeded_state: Any) -> None:
        self._state = deepcopy(seeded_state)
        self.pre_persistence_state: Any | None = None
        self.pre_persistence_decision: Any | None = None
        self.delivery_retry_calls = 0
        self._next_id = 1

    def set_runtime_metadata(self, **_: Any) -> None:
        return None

    def list_active_event_states(self, *, now: Any = None) -> list[Any]:
        if self._state is None:
            return []
        return [deepcopy(self._state)]

    def get_event_state(self, symbol: str) -> Any:
        if self._state is None or getattr(self._state, "symbol", None) != symbol:
            return None
        return deepcopy(self._state)

    def upsert_event_state(self, state: Any) -> Any:
        self._state = deepcopy(state)
        return state

    def expire_symbol(self, _symbol: str) -> Any:
        return None

    def get_shadow_entry_attempt(self, *, attempt_id: str) -> Any:
        return None

    def record_root_detector_shadow_episode_observation(self, _payload: Mapping[str, Any]) -> None:
        return None

    def record_root_detector_shadow_v2_root(self, **_: Any) -> None:
        return None

    def record_root_detector_shadow_candidate(self, _payload: Mapping[str, Any]) -> None:
        return None

    def append_root_detector_shadow_observation(self, *_: Any, **__: Any) -> None:
        return None

    def link_root_detector_shadow_candidate(self, *_: Any, **__: Any) -> None:
        return None

    def link_root_detector_shadow_v2_live_root(self, *_: Any, **__: Any) -> None:
        return None

    def record_reject_stat(self, **_: Any) -> None:
        return None

    def record_climax_evaluation(self, **_: Any) -> int:
        return 1

    def record_current_root_predicate_snapshot(self, **_: Any) -> None:
        return None

    def upsert_shadow_root_event(self, **_: Any) -> tuple[int, None]:
        # The replay has no historical shadow-root ledger.  Keep the real
        # climax method on its I/O seam without inventing an owner or attempt.
        return 1, None

    def upsert_shadow_entry_attempt(self, **_: Any) -> bool:
        return False

    def transition_shadow_entry_attempt(self, **_: Any) -> None:
        return None

    def get_open_shadow_attempt_id(self, **_: Any) -> None:
        return None

    def record_attempt_reused_after_restart(self, **_: Any) -> None:
        return None

    def record_volume_climax_observation(self, **_: Any) -> None:
        return None

    def record_attempt_correlation_missing(self, **_: Any) -> None:
        return None

    def persist_final_signal_bundle(self, decision: Any, state: Any, **_: Any) -> Any:
        self.pre_persistence_decision = deepcopy(decision)
        self.pre_persistence_state = deepcopy(state)
        record_id = self._next_id
        self._next_id += 1
        return SimpleNamespace(id=record_id)

    def save_signal(self, decision: Any, state: Any, **kwargs: Any) -> Any:
        return self.persist_final_signal_bundle(decision, state, **kwargs)

    def save_watch_candidate(self, decision: Any, state: Any, **_: Any) -> Any:
        self.pre_persistence_decision = deepcopy(decision)
        self.pre_persistence_state = deepcopy(state)
        record_id = self._next_id
        self._next_id += 1
        return SimpleNamespace(id=record_id)

    def claim_due_deliveries(
        self,
        _now: Any,
        *,
        limit: int,
        lease_seconds: int,
        entity_type: str,
        entity_id: int | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        return [{"id": int(entity_id or 1), "payload": "shadow-replay"}]

    def mark_delivery_retry(self, *_: Any, **__: Any) -> None:
        self.delivery_retry_calls += 1

    def mark_delivery_sent(self, *_: Any, **__: Any) -> None:
        return None

    def update_signal_telegram_status(self, *_: Any, **__: Any) -> None:
        return None

    def update_watch_telegram_status(self, *_: Any, **__: Any) -> None:
        return None

    def has_signal_for_event(self, *_: Any, **__: Any) -> bool:
        return False


def _build_reference_bot(
    *,
    main_module: Any,
    config: Any,
    fingerprint: str,
    repository: _ReferenceRepository,
    provider: _CapturedProvider,
    notifier: _ReferenceNotifier,
    now: datetime,
) -> tuple[Any, list[Any], list[tuple[Any, ...]], dict[str, int]]:
    """Build ShortSignalBot with real C dependencies and I/O-only seams."""

    ShortSignalBot = getattr(main_module, "ShortSignalBot")
    from app.baseline.lifecycle import BaselineLifecycle
    from app.events.pump_detector import PumpDetector
    from app.events.pullback_tracker import PullbackTracker
    from app.events.short_zone import ShortZoneBuilder
    from app.events.state_store import EventStateStore
    from app.features.builder import FeatureBuilder
    from app.runtime.symbol_mutation import SymbolMutationCoordinator
    from app.signals.engine import SignalEngine

    bot = ShortSignalBot.__new__(ShortSignalBot)
    logger = logging.getLogger("lane-c-shadow-reference")
    bot._config = config
    bot._runtime_started_at = now
    bot._runtime_instance_id = "shadow-replay-reference"
    bot._code_version = "frozen-reference"
    bot._config_fingerprint = fingerprint
    bot._strategy_config_hash = fingerprint
    bot._logger = logger
    bot._watch_sent_in_cycle = 0
    bot._active_climax_pool = {}
    bot._fast_monitor_cursor = 0
    bot._fast_monitor_poll_sequence = 0
    bot._fast_monitor_task = None
    bot._fast_monitor_running = False
    bot._shadow_rotation_id = "shadow-replay"
    bot._symbol_mutations = SymbolMutationCoordinator(logger)
    bot._lifecycle = "READY"
    bot._ready = True
    bot._startup_cleanup_required = False
    bot._market_data_gap_tracker = None
    bot._market_data_checkpoint = None
    bot._research_storage_router = None
    bot._research_storage_health_publisher = None
    bot._repository = repository
    bot._market_data_provider = provider
    bot._market_data_hub = None
    bot._provider_evidence_publisher = None
    bot._scanner = provider
    bot._client = provider
    bot._shadow_outcome_scheduler = None
    bot._notifier = notifier
    feature_builder = FeatureBuilder()
    bot._state_store = EventStateStore(repository)
    bot._feature_builder = feature_builder
    bot._pump_detector = PumpDetector(config)
    bot._pullback_tracker = PullbackTracker(config)
    bot._zone_builder = ShortZoneBuilder(config)
    bot._baseline_lifecycle = BaselineLifecycle(
        config,
        feature_builder,
        pump_detector=bot._pump_detector,
        pullback_tracker=bot._pullback_tracker,
        zone_builder=bot._zone_builder,
    )
    bot._signal_engine = SignalEngine(config)
    bot._strategy_registry = None
    bot._outcome_tracker = None
    bot._error_throttler = None
    bot._storage_incident = None
    bot._disk_health_state = None
    bot._health = SimpleNamespace()

    feature_calls: list[Any] = []
    real_build = feature_builder.build

    def build_spy(*args: Any, **kwargs: Any) -> Any:
        features = real_build(*args, **kwargs)
        feature_calls.append(deepcopy(features))
        return features

    feature_builder.build = build_spy
    evaluation_calls: list[tuple[Any, ...]] = []
    real_evaluate = bot._evaluate_registered_strategies

    def evaluate_spy(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
        evaluations = real_evaluate(*args, **kwargs)
        # Strategy metadata can contain immutable mappingproxy values.  Keep
        # the returned tuple itself and let the bounded JSON projection below
        # normalize it; forcing deepcopy here would make a valid real registry
        # replay fail before the comparison seam.
        evaluation_calls.append(tuple(evaluations))
        return evaluations

    bot._evaluate_registered_strategies = evaluate_spy
    method_calls = {
        "climax": 0,
        "process_symbol": 0,
        "baseline": 0,
        "trapped": 0,
    }

    # These wrappers call the frozen methods.  They exist only to prove which
    # dispatch path ran; no evaluator, feature builder, registry, or lifecycle
    # implementation is replaced by the replay harness.
    real_climax = bot._evaluate_and_send_climax

    async def climax_spy(*args: Any, **kwargs: Any) -> Any:
        method_calls["climax"] += 1
        return await real_climax(*args, **kwargs)

    bot._evaluate_and_send_climax = climax_spy
    real_process_symbol = bot._process_symbol

    async def process_symbol_spy(*args: Any, **kwargs: Any) -> Any:
        method_calls["process_symbol"] += 1
        return await real_process_symbol(*args, **kwargs)

    bot._process_symbol = process_symbol_spy
    trapped_method = getattr(bot, "_evaluate_and_send_trapped_longs", None)
    if callable(trapped_method):
        async def trapped_spy(*args: Any, **kwargs: Any) -> Any:
            method_calls["trapped"] += 1
            return await trapped_method(*args, **kwargs)

        bot._evaluate_and_send_trapped_longs = trapped_spy

    # BaselineLifecycle is the only lifecycle used by _process_symbol.  Keep
    # the original methods and count calls so a fast replay can assert that
    # no root detection or active-event advancement was entered.
    for method_name in ("detect_event", "advance_active_event"):
        lifecycle_method = getattr(bot._baseline_lifecycle, method_name, None)
        if not callable(lifecycle_method):
            continue

        def lifecycle_spy(
            *args: Any,
            _method=lifecycle_method,
            **kwargs: Any,
        ) -> Any:
            method_calls["baseline"] += 1
            return _method(*args, **kwargs)

        setattr(bot._baseline_lifecycle, method_name, lifecycle_spy)
    return bot, feature_calls, evaluation_calls, method_calls


def _old_run(record: Mapping[str, Any]) -> dict[str, Any]:
    """Run the frozen app.main process_symbol harness at the pre-persistence seam."""

    market = record.get("market") or {}
    symbol = str(record.get("symbol") or market.get("symbol") or "")
    if not symbol:
        raise ValueError("capture is missing symbol")
    frame = _frame(market)
    before_state = _state(record.get("before_state"))
    process_state = deepcopy(before_state)
    config, fingerprint = _config(record)
    fast_monitor, poll_sequence = _fast_monitor(record)
    if fast_monitor and process_state is None:
        raise _ReferenceUnknown("fast_monitor_before_state_missing")
    final_required = bool(record.get("final_recheck_required"))
    final_captured = bool(record.get("final_recheck_captured"))
    if final_required and not final_captured:
        raise _ReferenceUnknown("final_fresh_recheck_not_captured")
    if bool(getattr(config, "trapped_longs_reversal_enabled", False)):
        raise _ReferenceUnknown(
            "missing_historical_repository_context:trapped_longs_owner_prior_id"
        )

    decision_at = _decision_time(record, market, frame)
    initial_provider = _CapturedProvider(market, symbol=symbol)
    final_market = record.get("final_recheck")
    provider_market = (
        final_market
        if final_captured and isinstance(final_market, Mapping)
        else market
    )
    provider = _CapturedProvider(
        provider_market,
        symbol=symbol,
        supports_symbol_frames=final_required and final_captured,
    )
    repository = _ReferenceRepository(process_state)
    notifier = _ReferenceNotifier()
    main_module = importlib.import_module("app.main")
    real_datetime = main_module.datetime

    class _FrozenDateTime(real_datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Any:
            if tz is None:
                return decision_at.replace(tzinfo=None)
            return decision_at.astimezone(tz)

    main_module.datetime = _FrozenDateTime
    try:
        bot, feature_calls, evaluation_calls, method_calls = _build_reference_bot(
            main_module=main_module,
            config=config,
            fingerprint=fingerprint,
            repository=repository,
            provider=provider,
            notifier=notifier,
            now=decision_at,
        )

        async def invoke() -> tuple[Any, Any]:
            if fast_monitor:
                decision = await bot._evaluate_and_send_climax(
                    symbol,
                    frame.copy(deep=True),
                    process_state,
                    features=None,
                    market_snapshot=initial_provider.snapshot,
                    market_asof=decision_at,
                    fast_monitor=True,
                    poll_sequence=poll_sequence,
                )
                return decision, deepcopy(process_state)
            return await bot._process_symbol(
                symbol,
                frame.copy(deep=True),
                process_state,
                market_snapshot=initial_provider.snapshot,
            )

        decision, returned_state = asyncio.run(invoke())
    finally:
        main_module.datetime = real_datetime

    if not feature_calls:
        raise _ReferenceUnknown("reference_harness_did_not_execute_feature_builder")
    if bool(getattr(config, "climax_short_enabled", False)) and (
        repository.pre_persistence_decision is not None
    ):
        raise _ReferenceUnknown(
            "missing_historical_repository_context:existing_signal_owner"
        )

    boundary_decision = (
        repository.pre_persistence_decision
        if repository.pre_persistence_decision is not None
        else decision
    )
    boundary_state = (
        repository.pre_persistence_state
        if repository.pre_persistence_state is not None
        else returned_state
    )
    evaluations = evaluation_calls[-1] if evaluation_calls else ()
    if fast_monitor:
        # _evaluate_and_send_climax asks the frozen registry for all adapter
        # observations, then admits only the first CLIMAX_EXHAUSTION entry.
        # The candidate fast seam returns that selected climax admission
        # evaluation, so expose the same semantic result projection instead
        # of treating baseline/trapped observations as fast-path outputs.
        selected_climax = next(
            (
                item
                for item in evaluations
                if getattr(item, "strategy_type", None) == "CLIMAX_EXHAUSTION"
            ),
            None,
        )
        evaluations = (selected_climax,) if selected_climax is not None else ()
    identity = (
        {
            name: getattr(boundary_decision, name)
            for name in (
                "symbol",
                "event_id",
                "strategy_type",
                "strategy_subtype",
                "model_version",
            )
        }
        if boundary_decision is not None
        else None
    )
    reference_scope = (
        "frozen.app.main.ShortSignalBot._evaluate_and_send_climax"
        if fast_monitor
        else "frozen.app.main.ShortSignalBot._process_symbol"
    )
    return {
        "features": _jsonable(feature_calls[-1]),
        "state": _jsonable(boundary_state),
        "decision": _jsonable(boundary_decision),
        "evaluations": [_jsonable(item) for item in evaluations],
        "blocked_reason": None,
        "actionable": bool(
            boundary_decision is not None and boundary_decision.actionable
        ),
        "identity": identity,
        "comparison_boundary": "pre_persistence",
        "reference_scope": reference_scope,
        "post_persistence_state": _jsonable(returned_state),
        "harness": {
            "actual_process_symbol": bool(method_calls["process_symbol"]),
            "process_symbol_calls": method_calls["process_symbol"],
            "actual_climax_method": bool(method_calls["climax"]),
            "climax_method_calls": method_calls["climax"],
            "baseline_calls": method_calls["baseline"],
            "trapped_calls": method_calls["trapped"],
            "feature_builder_calls": len(feature_calls),
            "registry_evaluation_calls": len(evaluation_calls),
            "provider_snapshot_calls": provider.capture_calls,
            "provider_network_calls": provider.network_calls,
            "notifier_send_calls": notifier.send_calls,
            "notifier_alert_calls": notifier.alert_calls,
            "persistence_boundary_captured": (
                repository.pre_persistence_decision is not None
            ),
        },
    }

def run(record: Mapping[str, Any], implementation: str) -> dict[str, Any]:
    if implementation == "candidate":
        return _candidate_run(record)
    if implementation == "old":
        return _old_run(record)
    raise ValueError(f"unknown implementation: {implementation}")


def _batch_error(exc: BaseException) -> str:
    # Keep worker diagnostics bounded and free of full record/path payloads.
    detail = str(exc).replace("\r", " ").replace("\n", " ")
    return f"{type(exc).__name__}:{detail[-1200:]}"


def _run_stdin_jsonl(implementation: str) -> int:
    """Run a bounded JSONL stream in this one isolated package process."""

    for sequence, line in enumerate(sys.stdin):
        if not line.strip():
            continue
        index = sequence
        try:
            envelope = json.loads(line)
            if isinstance(envelope, Mapping) and "record" in envelope:
                index = int(envelope.get("index", sequence))
                record = envelope["record"]
            else:
                record = envelope
            if not isinstance(record, Mapping):
                raise ValueError("batch record must be an object")
            result = run(record, implementation)
            output = {"index": index, "ok": True, "result": result}
        except Exception as exc:  # noqa: BLE001 - classify one input as UNKNOWN in parent
            output = {"index": index, "ok": False, "error": _batch_error(exc)}
        sys.stdout.write(json.dumps(output, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")
        sys.stdout.flush()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True, type=Path)
    parser.add_argument("--record", type=Path)
    parser.add_argument("--implementation", required=True, choices=("old", "candidate"))
    parser.add_argument("--stdin-jsonl", action="store_true")
    args = parser.parse_args()
    source_root = args.source_root.expanduser().resolve()
    if source_root.name == "app" and source_root.is_dir():
        source_root = source_root.parent
    sys.path.insert(0, str(source_root))
    if args.stdin_jsonl:
        return _run_stdin_jsonl(args.implementation)
    if args.record is None:
        parser.error("--record is required unless --stdin-jsonl is set")
    record = json.loads(args.record.read_text(encoding="utf-8"))
    output = run(record, args.implementation)
    print(json.dumps(output, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

