"""Canonical runtime market-data facade.

REST remains the source for historical candles and supplemental inputs.  The public
hub may replace only a complete, acknowledged, fresh ticker group in preferred mode.
"""

from __future__ import annotations

import inspect
from copy import deepcopy
from collections import Counter, deque
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Callable, Mapping

import pandas as pd

from app.config import AppConfig, CanonicalMarketDataProviderMode
from app.domain import MarketSnapshot

from .continuity import GapState
from .evidence import normalize_provider_evidence
from .models import HealthStatus, MarketTickerSnapshot
from .parity import KlineClassification, compare_closed_kline
from .planning import normalize_symbols

_HUB_TICKER_FRESHNESS = timedelta(seconds=45)
_MAX_DETAILS = 2048


class MarketDataSource(StrEnum):
    """Stable source labels used by provider provenance."""

    HUB = "HUB"
    REST = "REST"
    REST_FALLBACK = "REST_FALLBACK"


class FallbackReason(StrEnum):
    """Stable, bounded reasons for refusing a Hub canonical group."""

    DISABLED = "DISABLED"
    DISCONNECTED = "DISCONNECTED"
    RECONNECTING = "RECONNECTING"
    UNINITIALIZED_GENERATION = "UNINITIALIZED_GENERATION"
    STALE = "STALE"
    MISSING_FIELD = "MISSING_FIELD"
    MISSING_CONFIRMED_CANDLE = "MISSING_CONFIRMED_CANDLE"
    GAP = "GAP"
    FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
    SOURCE_CONFLICT = "SOURCE_CONFLICT"


class ComparisonClassification(StrEnum):
    """Closed set for bounded provider evidence comparisons."""

    EXACT = "EXACT"
    EXPECTED_TEMPORAL_DIFFERENCE = "EXPECTED_TEMPORAL_DIFFERENCE"
    WS_UNINITIALIZED = "WS_UNINITIALIZED"
    WS_STALE = "WS_STALE"
    WS_REQUIRED_FIELD_MISSING = "WS_REQUIRED_FIELD_MISSING"
    REST_UNAVAILABLE = "REST_UNAVAILABLE"
    SOURCE_SEMANTIC_MISMATCH = "SOURCE_SEMANTIC_MISMATCH"
    DECISION_DIVERGENCE = "DECISION_DIVERGENCE"


@dataclass(frozen=True, slots=True)
class SourceProvenance:
    source: str
    reason: str
    captured_at_utc: datetime
    max_input_availability_time_utc: datetime
    fallback_reason: str | None = None

    def __post_init__(self) -> None:
        _validate_availability(self.captured_at_utc, self.max_input_availability_time_utc)


@dataclass(frozen=True, slots=True, init=False)
class FrozenCandleFrame:
    symbol: str
    source: str
    captured_at_utc: datetime
    max_input_availability_time_utc: datetime
    _frame: pd.DataFrame

    def __init__(self, symbol: str, frame: pd.DataFrame, source: str, captured_at_utc: datetime, max_input_availability_time_utc: datetime) -> None:
        _validate_availability(captured_at_utc, max_input_availability_time_utc)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "captured_at_utc", _utc(captured_at_utc))
        object.__setattr__(self, "max_input_availability_time_utc", _utc(max_input_availability_time_utc))
        object.__setattr__(self, "_frame", frame.copy(deep=True))

    @property
    def frame(self) -> pd.DataFrame:
        return self._frame.copy(deep=True)


@dataclass(frozen=True, slots=True, init=False)
class ResolvedTickerGroup:
    _snapshot: MarketSnapshot
    provenance: SourceProvenance

    def __init__(self, snapshot: MarketSnapshot, provenance: SourceProvenance) -> None:
        object.__setattr__(self, "_snapshot", replace(snapshot))
        object.__setattr__(self, "provenance", provenance)

    @property
    def snapshot(self) -> MarketSnapshot:
        return replace(self._snapshot)


@dataclass(frozen=True, slots=True, init=False)
class CanonicalScanSnapshot:
    _snapshots: tuple[MarketSnapshot, ...]
    _resolved_tickers: Mapping[str, ResolvedTickerGroup]
    captured_at_utc: datetime
    max_input_availability_time_utc: datetime

    def __init__(self, snapshots: tuple[MarketSnapshot, ...], resolved_tickers: Mapping[str, ResolvedTickerGroup], captured_at_utc: datetime, max_input_availability_time_utc: datetime) -> None:
        _validate_availability(captured_at_utc, max_input_availability_time_utc)
        object.__setattr__(self, "_snapshots", tuple(replace(item) for item in snapshots))
        object.__setattr__(self, "_resolved_tickers", dict(resolved_tickers))
        object.__setattr__(self, "captured_at_utc", _utc(captured_at_utc))
        object.__setattr__(self, "max_input_availability_time_utc", _utc(max_input_availability_time_utc))

    @property
    def snapshots(self) -> tuple[MarketSnapshot, ...]:
        return tuple(replace(item) for item in self._snapshots)

    @property
    def resolved_tickers(self) -> Mapping[str, ResolvedTickerGroup]:
        return {symbol: ResolvedTickerGroup(group.snapshot, group.provenance) for symbol, group in self._resolved_tickers.items()}


@dataclass(frozen=True, slots=True, init=False)
class DecisionMarketSnapshot:
    symbol: str
    frame_1m: FrozenCandleFrame
    _derivatives: Mapping[str, Any]
    _liquidity: Mapping[str, Any] | None
    _ticker_group: ResolvedTickerGroup | None
    captured_at_utc: datetime
    max_input_availability_time_utc: datetime

    def __init__(self, symbol: str, frame_1m: FrozenCandleFrame, derivatives: Mapping[str, Any], liquidity: Mapping[str, Any] | None, captured_at_utc: datetime, max_input_availability_time_utc: datetime, ticker_group: ResolvedTickerGroup | None = None) -> None:
        _validate_availability(captured_at_utc, max_input_availability_time_utc)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "frame_1m", FrozenCandleFrame(symbol, frame_1m.frame, frame_1m.source, frame_1m.captured_at_utc, frame_1m.max_input_availability_time_utc))
        object.__setattr__(self, "_derivatives", deepcopy(dict(derivatives)))
        object.__setattr__(self, "_liquidity", deepcopy(dict(liquidity)) if liquidity is not None else None)
        object.__setattr__(self, "_ticker_group", ResolvedTickerGroup(ticker_group.snapshot, ticker_group.provenance) if ticker_group is not None else None)
        object.__setattr__(self, "captured_at_utc", _utc(captured_at_utc))
        object.__setattr__(self, "max_input_availability_time_utc", _utc(max_input_availability_time_utc))

    @property
    def derivatives(self) -> Mapping[str, Any]:
        return deepcopy(dict(self._derivatives))

    @property
    def liquidity(self) -> Mapping[str, Any] | None:
        return deepcopy(dict(self._liquidity)) if self._liquidity is not None else None

    @property
    def ticker_group(self) -> ResolvedTickerGroup | None:
        if self._ticker_group is None:
            return None
        return ResolvedTickerGroup(self._ticker_group.snapshot, self._ticker_group.provenance)


@dataclass(slots=True)
class _Evidence:
    counters: Counter[str] = field(default_factory=Counter)
    details: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=_MAX_DETAILS))

    def record(self, event: str, **detail: Any) -> None:
        self.counters[event] += 1
        if detail:
            self.details.append({"event": event, **detail})


class CanonicalMarketDataProvider:
    """Own scanner/hub lifecycle and prevent source selection leaking into strategy code."""

    def __init__(
        self,
        *,
        scanner: Any,
        config: AppConfig,
        hub: Any | None = None,
        continuity: Any | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._scanner = scanner
        self._config = config
        self._hub = hub
        self._continuity = continuity
        self._clock = clock or (lambda: datetime.now(UTC))
        self._evidence = _Evidence()
        self._last_scan_snapshot: CanonicalScanSnapshot | None = None
        self._last_universe: tuple[str, ...] | None = None
        self._candidate_sources: dict[str, str | None] = {}
        self._candidate_source_differences: dict[str, bool] = {}

    @property
    def client(self) -> Any:
        return self._scanner.client

    @property
    def hub(self) -> Any | None:
        return self._hub

    @property
    def continuity(self) -> Any | None:
        return self._continuity

    def set_continuity(self, continuity: Any | None) -> None:
        """Attach the runtime continuity observer for an injected Hub seam."""
        self._continuity = continuity

    @property
    def last_universe_telemetry(self) -> Any:
        return getattr(self._scanner, "last_universe_telemetry", None)

    @property
    def supports_symbol_frames(self) -> bool:
        """Retain the old optional scanner capability for injected test seams."""
        return hasattr(self._scanner, "fetch_symbol_frames")

    @property
    def mode(self) -> CanonicalMarketDataProviderMode:
        return CanonicalMarketDataProviderMode(
            self._config.canonical_market_data_provider_mode
        )

    async def start(self) -> None:
        if self._hub is None or self.mode is CanonicalMarketDataProviderMode.REST_ONLY:
            return
        await _await_if_needed(self._hub.start())

    async def stop(self) -> None:
        if self._hub is None:
            return
        await _await_if_needed(self._hub.stop())

    def update_universe(self, symbols: tuple[str, ...] | list[str]) -> None:
        if self._hub is None or self.mode is CanonicalMarketDataProviderMode.REST_ONLY:
            return
        normalized = normalize_symbols(symbols)
        if self._last_universe == normalized:
            return
        self._last_universe = normalized
        self._hub.update_universe(normalized)

    async def build_scan_snapshot(self) -> CanonicalScanSnapshot:
        rest = await self._scanner.fetch_market_snapshots()
        captured = _utc(self._clock())
        groups: dict[str, ResolvedTickerGroup] = {}
        resolved: list[MarketSnapshot] = []
        for snapshot in rest:
            group = self._resolve_ticker(snapshot, captured)
            groups[snapshot.symbol] = group
            resolved.append(group.snapshot)
        result = CanonicalScanSnapshot(
            snapshots=tuple(resolved), resolved_tickers=groups,
            captured_at_utc=captured, max_input_availability_time_utc=captured,
        )
        self._last_scan_snapshot = result
        return result

    async def fetch_market_snapshots(self) -> list[MarketSnapshot]:
        snapshot = await self.build_scan_snapshot()
        telemetry = self.last_universe_telemetry
        if telemetry is not None:
            self.update_universe(list(getattr(telemetry, "eligible_symbols", ())))
        return list(snapshot.snapshots)

    def shortlist(self, snapshots: list[MarketSnapshot]) -> list[MarketSnapshot]:
        return self._scanner.shortlist(snapshots)

    async def prefetch_historical_frames(self, symbols: list[str]) -> dict[str, FrozenCandleFrame]:
        frames = await self._scanner.fetch_symbol_frames(symbols)
        captured = _utc(self._clock())
        output: dict[str, FrozenCandleFrame] = {}
        for symbol, frame in frames.items():
            output[symbol] = FrozenCandleFrame(
                symbol=symbol, frame=frame, source="REST", captured_at_utc=captured,
                max_input_availability_time_utc=captured,
            )
        return output

    async def fetch_symbol_frames(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        frames = await self.prefetch_historical_frames(symbols)
        return {symbol: frozen.frame.copy(deep=True) for symbol, frozen in frames.items()}

    async def capture_decision_snapshot(
        self,
        symbol: str,
        *,
        frame_1m: pd.DataFrame | None = None,
        price: float | None = None,
        include_liquidity: bool = True,
        derivatives: Mapping[str, Any] | None = None,
    ) -> DecisionMarketSnapshot | None:
        if frame_1m is None:
            frames = await self.prefetch_historical_frames([symbol])
            frozen = frames.get(symbol)
            if frozen is None:
                return None
        else:
            frame_available = _utc(self._clock())
            frozen = FrozenCandleFrame(symbol, frame_1m, "REST", frame_available, frame_available)
        effective_price = price if price is not None else float(frozen.frame["close"].iloc[-1])
        resolved_derivatives = (
            deepcopy(dict(derivatives))
            if derivatives is not None
            else await self.fetch_optional_derivatives(symbol)
        )
        liquidity = (
            await self.fetch_optional_liquidity(symbol, effective_price)
            if include_liquidity
            else None
        )
        captured = _utc(self._clock())
        if self.mode is not CanonicalMarketDataProviderMode.REST_ONLY:
            self._record_closed_1m_shadow(symbol, captured, rest_frame=frozen.frame)
        return DecisionMarketSnapshot(
            symbol=symbol, frame_1m=frozen, derivatives=resolved_derivatives, liquidity=liquidity,
            captured_at_utc=captured,
            max_input_availability_time_utc=max(captured, frozen.max_input_availability_time_utc),
            ticker_group=(
                self._last_scan_snapshot.resolved_tickers.get(symbol)
                if self._last_scan_snapshot is not None
                else None
            ),
        )

    async def fetch_rest_candles(self, symbol: str, interval: str, **kwargs: Any) -> Any:
        return await self.client.fetch_klines(symbol, interval, **kwargs)

    async def fetch_klines(self, symbol: str, interval: str, **kwargs: Any) -> Any:
        """Compatibility client seam; historical reads are always REST."""
        return await self.fetch_rest_candles(symbol, interval, **kwargs)

    async def fetch_optional_derivatives(self, symbol: str) -> dict[str, Any]:
        return await self._scanner.fetch_optional_derivatives(symbol)

    async def fetch_optional_liquidity(self, symbol: str, price: float) -> dict[str, Any]:
        fetcher = getattr(self._scanner, "fetch_optional_liquidity", None)
        if fetcher is None:
            return {}
        return await fetcher(symbol, price)

    def record_decision_evidence(self, event: str, **detail: Any) -> None:
        self._evidence.record(event, **detail)

    def evidence_snapshot(self) -> dict[str, Any]:
        return {"counters": dict(self._evidence.counters), "details": list(self._evidence.details)}

    def evidence_report(
        self,
        *,
        code_sha: str,
        strategy_fingerprint: str,
        generated_at_utc: datetime,
    ) -> dict[str, Any]:
        """Return the bounded, publication-ready provider report."""

        return normalize_provider_evidence(
            self.evidence_snapshot(),
            code_sha=code_sha,
            strategy_fingerprint=strategy_fingerprint,
            provider_mode=self.mode,
            generated_at_utc=generated_at_utc,
        )

    def record_evaluation_evidence(
        self,
        *,
        symbol: str,
        evaluation_time_utc: datetime,
        canonical_source: str,
        candidate_source: str | None,
        classification: ComparisonClassification,
        canonical_input_fingerprint: str,
        candidate_input_fingerprint: str | None,
        canonical_decision_fingerprint: str,
        candidate_decision_fingerprint: str | None,
        fallback_reason: str | None = None,
        ages_ms: Mapping[str, int] | None = None,
    ) -> None:
        """Record one side-effect-free evaluation comparison.

        Only compact provenance and fingerprints enter the bounded detail ring;
        frames, ticker payloads, and strategy/database objects never do.
        """

        classification_value = str(getattr(classification, "value", classification))
        canonical_value = str(getattr(canonical_source, "value", canonical_source))
        candidate_value = (
            str(getattr(candidate_source, "value", candidate_source))
            if candidate_source is not None
            else None
        )
        counters = self._evidence.counters
        counters["evaluations_total"] += 1
        if canonical_value in {MarketDataSource.REST.value, MarketDataSource.REST_FALLBACK.value}:
            counters["canonical_rest_groups"] += 1
        elif canonical_value == MarketDataSource.HUB.value:
            counters["canonical_hub_groups"] += 1
        if candidate_value == MarketDataSource.HUB.value:
            counters["hub_candidates_total"] += 1
            counters["source_comparisons"] += 1
            if (
                candidate_input_fingerprint != canonical_input_fingerprint
                or classification_value == ComparisonClassification.SOURCE_SEMANTIC_MISMATCH.value
            ):
                counters["source_comparison_divergences"] += 1
        if candidate_decision_fingerprint is not None:
            counters["decision_comparisons"] += 1
            if (
                candidate_decision_fingerprint != canonical_decision_fingerprint
                or classification_value == ComparisonClassification.DECISION_DIVERGENCE.value
            ):
                counters["decision_divergences"] += 1
                counters["last_divergence_at"] = _timestamp(evaluation_time_utc)
        classification_counter = {
            ComparisonClassification.EXACT.value: "hub_exact",
            ComparisonClassification.EXPECTED_TEMPORAL_DIFFERENCE.value: "hub_expected_temporal_difference",
            ComparisonClassification.WS_UNINITIALIZED.value: "hub_uninitialized",
            ComparisonClassification.WS_STALE.value: "hub_stale",
            ComparisonClassification.WS_REQUIRED_FIELD_MISSING.value: "hub_required_field_missing",
            ComparisonClassification.SOURCE_SEMANTIC_MISMATCH.value: "hub_semantic_mismatch",
        }.get(classification_value)
        # Classification counters describe Hub comparisons only.  Normal
        # REST_ONLY runtime records have no candidate source and therefore do
        # not increment these counters; an explicit candidate passed by a
        # characterization test remains observable regardless of configured
        # mode.
        if classification_counter and candidate_value == MarketDataSource.HUB.value:
            counters[classification_counter] += 1
        if fallback_reason is not None:
            counters["rest_fallback_total"] += 1
            reasons = counters.setdefault("fallback_by_reason", {})
            reasons[str(getattr(fallback_reason, "value", fallback_reason))] = (
                int(reasons.get(str(getattr(fallback_reason, "value", fallback_reason)), 0)) + 1
            )
        detail: dict[str, Any] = {
            "event": "evaluation",
            "symbol": symbol,
            "evaluation_time": _timestamp(evaluation_time_utc),
            "group": "ticker",
            "canonical_source": canonical_value,
            "candidate_source": candidate_value,
            "classification": classification_value,
            "fallback_reason": str(getattr(fallback_reason, "value", fallback_reason))
            if fallback_reason is not None
            else None,
            "canonical_input_fingerprint": canonical_input_fingerprint,
            "candidate_input_fingerprint": candidate_input_fingerprint,
            "canonical_decision_fingerprint": canonical_decision_fingerprint,
            "candidate_decision_fingerprint": candidate_decision_fingerprint,
            "ages_ms": dict(ages_ms) if ages_ms is not None else None,
        }
        self._evidence.details.append(
            {key: value for key, value in detail.items() if value is not None}
        )

    def candidate_source_for(self, symbol: str) -> str | None:
        """Return the current Hub candidate provenance for an evaluated symbol."""

        return self._candidate_sources.get(symbol)

    def candidate_comparison_classification_for(
        self, symbol: str
    ) -> ComparisonClassification:
        """Return the source-only comparison result for one ticker candidate."""

        if self._candidate_source_differences.get(symbol, False):
            return ComparisonClassification.SOURCE_SEMANTIC_MISMATCH
        return ComparisonClassification.EXACT

    def _resolve_ticker(self, rest: MarketSnapshot, captured: datetime) -> ResolvedTickerGroup:
        if self.mode is CanonicalMarketDataProviderMode.REST_ONLY:
            self._candidate_sources[rest.symbol] = None
            self._candidate_source_differences[rest.symbol] = False
            return self._rest_group(
                rest, captured, "REST_ONLY", fallback_reason=FallbackReason.DISABLED
            )
        if self.mode is CanonicalMarketDataProviderMode.HUB_SHADOW:
            self._shadow_compare(rest, captured)
            return self._rest_group(rest, captured, "HUB_SHADOW")
        hub_snapshot, reason = self._eligible_hub_snapshot(rest.symbol, captured)
        if hub_snapshot is None:
            self._candidate_sources[rest.symbol] = None
            self._candidate_source_differences[rest.symbol] = False
            self._evidence.record("rest_fallback", symbol=rest.symbol, reason=reason)
            return self._rest_group(
                rest,
                captured,
                "REST_FALLBACK",
                fallback_reason=_fallback_reason(reason),
            )
        resolved = replace(
            rest, last_price=float(hub_snapshot.last_price),
            price_24h_pct=float(hub_snapshot.price_24h_pcnt) * 100,
            turnover_24h=float(hub_snapshot.turnover_24h), volume_24h=float(hub_snapshot.volume_24h),
            mark_price=float(hub_snapshot.mark_price), open_interest=float(hub_snapshot.open_interest),
            timestamp=hub_snapshot.received_at,
        )
        self._candidate_sources[rest.symbol] = MarketDataSource.HUB.value
        self._candidate_source_differences[rest.symbol] = False
        provenance = SourceProvenance(
            MarketDataSource.HUB,
            "HUB_PREFERRED",
            captured,
            hub_snapshot.received_at,
        )
        self._evidence.record("hub_selected", symbol=rest.symbol)
        return ResolvedTickerGroup(resolved, provenance)

    def _rest_group(
        self,
        snapshot: MarketSnapshot,
        captured: datetime,
        reason: str,
        *,
        fallback_reason: str | None = None,
    ) -> ResolvedTickerGroup:
        source = (
            MarketDataSource.REST_FALLBACK
            if reason == "REST_FALLBACK"
            else MarketDataSource.REST
        )
        provenance = SourceProvenance(
            source, reason, captured, captured, fallback_reason=fallback_reason
        )
        return ResolvedTickerGroup(snapshot, provenance)

    def _shadow_compare(self, rest: MarketSnapshot, captured: datetime) -> None:
        hub, reason = self._eligible_hub_snapshot(rest.symbol, captured)
        if hub is None:
            self._candidate_sources[rest.symbol] = None
            self._candidate_source_differences[rest.symbol] = False
            self._evidence.record("hub_shadow_unavailable", symbol=rest.symbol, reason=reason)
        else:
            self._candidate_sources[rest.symbol] = MarketDataSource.HUB.value
            differences = _ticker_differences(rest, hub)
            self._candidate_source_differences[rest.symbol] = bool(differences)
            event = "hub_shadow_match" if not differences else "hub_shadow_mismatch"
            self._evidence.record(
                event,
                symbol=rest.symbol,
                fields=tuple(differences),
                classification=(
                    ComparisonClassification.EXACT
                    if not differences
                    else ComparisonClassification.SOURCE_SEMANTIC_MISMATCH
                ),
            )
        self._record_closed_1m_shadow(rest.symbol, captured)

    def _eligible_hub_snapshot(self, symbol: str, captured: datetime) -> tuple[MarketTickerSnapshot | None, str]:
        if self._hub is None:
            return None, "HUB_UNAVAILABLE"
        health = self._hub.get_health()
        if health.state is not HealthStatus.HEALTHY:
            return None, "HUB_NOT_HEALTHY"
        state = self._hub.get_subscription_state()
        topic = f"tickers.{symbol.upper()}"
        if topic not in state.acknowledged_topics:
            return None, "HUB_UNACKNOWLEDGED"
        value = self._hub.get_snapshot(symbol)
        if value is None:
            return None, "HUB_MISSING"
        if value.connection_epoch is None:
            return None, "HUB_UNINITIALIZED"
        is_current = getattr(self._hub, "is_current_ticker_snapshot", None)
        if is_current is not None and not is_current(value):
            return None, "HUB_OLD_GENERATION"
        if self._continuity is None:
            return None, "HUB_CONTINUITY_UNAVAILABLE"
        continuity = self._continuity.get_symbol_state(symbol)
        if continuity is GapState.UNINITIALIZED:
            return None, "HUB_UNINITIALIZED"
        if continuity is GapState.GAP_DETECTED:
            return None, "HUB_GAP_DETECTED"
        if any(getattr(value, name) is None for name in (
            "last_price", "price_24h_pcnt", "turnover_24h", "volume_24h", "mark_price", "open_interest",
        )):
            return None, "HUB_INCOMPLETE"
        received = value.received_at
        exchange = value.exchange_timestamp
        if received is None or received > captured or (exchange is not None and exchange > captured):
            return None, "HUB_FUTURE"
        if captured - received > _HUB_TICKER_FRESHNESS:
            return None, "HUB_STALE"
        return value, "HUB_PREFERRED"

    def _record_closed_1m_shadow(
        self,
        symbol: str,
        captured: datetime,
        *,
        rest_frame: pd.DataFrame | None = None,
    ) -> None:
        if self._hub is None:
            return
        candle = self._hub.get_last_closed_candle(symbol)
        if candle is None:
            self._evidence.record(
                "hub_closed_1m_shadow_unavailable",
                symbol=symbol,
                reason="CLOSED_1M_MISSING",
                classification=ComparisonClassification.WS_UNINITIALIZED,
            )
            return
        if not candle.confirmed or candle.interval != "1":
            self._evidence.record(
                "hub_closed_1m_shadow_unavailable",
                symbol=symbol,
                reason="CLOSED_1M_UNCONFIRMED",
                classification=ComparisonClassification.WS_REQUIRED_FIELD_MISSING,
            )
            return
        if candle.connection_epoch is None:
            self._evidence.record(
                "hub_closed_1m_shadow_unavailable",
                symbol=symbol,
                reason="CLOSED_1M_UNINITIALIZED",
                classification=ComparisonClassification.WS_UNINITIALIZED,
            )
            return
        is_current = getattr(self._hub, "is_current_connection_epoch", None)
        if is_current is not None and not is_current(candle.connection_epoch):
            self._evidence.record(
                "hub_closed_1m_shadow_unavailable",
                symbol=symbol,
                reason="CLOSED_1M_OLD_GENERATION",
                classification=ComparisonClassification.WS_UNINITIALIZED,
            )
            return
        if candle.received_at > captured:
            self._evidence.record(
                "hub_closed_1m_shadow_unavailable",
                symbol=symbol,
                reason="CLOSED_1M_FUTURE",
                classification=ComparisonClassification.WS_STALE,
            )
            return
        if self._continuity is None or self._continuity.get_symbol_state(symbol) in {GapState.UNINITIALIZED, GapState.GAP_DETECTED}:
            self._evidence.record(
                "hub_closed_1m_shadow_unavailable",
                symbol=symbol,
                reason="CLOSED_1M_GAP_OR_UNINITIALIZED",
                classification=ComparisonClassification.WS_UNINITIALIZED,
            )
            return
        if rest_frame is None:
            self._evidence.record(
                "hub_closed_1m_shadow",
                symbol=symbol,
                open_time=candle.open_time,
                close_time=candle.close_time,
                epoch=candle.connection_epoch,
                classification=ComparisonClassification.EXACT,
            )
            return
        parity = compare_closed_kline(candle, _frame_to_rest_rows(rest_frame))
        classification = _comparison_classification(parity.classification)
        self._evidence.record(
            "hub_closed_1m_shadow_parity",
            symbol=symbol,
            open_time=candle.open_time,
            close_time=candle.close_time,
            epoch=candle.connection_epoch,
            classification=classification,
            kline_classification=parity.classification.value,
            fields=parity.field_differences,
        )


async def _await_if_needed(value: Any) -> None:
    if inspect.isawaitable(value):
        await value


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _validate_availability(captured: datetime, available: datetime) -> None:
    if _utc(available) > _utc(captured):
        raise ValueError("max input availability time must not be after captured_at_utc")


def _ticker_differences(rest: MarketSnapshot, hub: MarketTickerSnapshot) -> list[str]:
    expected = {
        "last_price": rest.last_price,
        "price_24h_pct": rest.price_24h_pct,
        "turnover_24h": rest.turnover_24h,
        "volume_24h": rest.volume_24h,
        "mark_price": rest.mark_price,
        "open_interest": rest.open_interest,
    }
    actual = {
        "last_price": float(hub.last_price),
        "price_24h_pct": float(hub.price_24h_pcnt) * 100,
        "turnover_24h": float(hub.turnover_24h),
        "volume_24h": float(hub.volume_24h),
        "mark_price": float(hub.mark_price),
        "open_interest": float(hub.open_interest),
    }
    return [name for name, value in expected.items() if value != actual[name]]


def _fallback_reason(reason: str) -> str:
    if reason in {"HUB_NOT_HEALTHY", "HUB_DISCONNECTED"}:
        return FallbackReason.DISCONNECTED
    if reason == "HUB_RECONNECTING":
        return FallbackReason.RECONNECTING
    if reason in {"HUB_UNINITIALIZED", "HUB_OLD_GENERATION", "HUB_CONTINUITY_UNAVAILABLE"}:
        return FallbackReason.UNINITIALIZED_GENERATION
    if reason == "HUB_STALE":
        return FallbackReason.STALE
    if reason == "HUB_INCOMPLETE":
        return FallbackReason.MISSING_FIELD
    if reason == "HUB_FUTURE":
        return FallbackReason.FUTURE_TIMESTAMP
    if reason == "HUB_GAP_DETECTED":
        return FallbackReason.GAP
    if reason == "HUB_UNAVAILABLE":
        return FallbackReason.DISABLED
    return FallbackReason.SOURCE_CONFLICT


def _comparison_classification(classification: KlineClassification) -> str:
    return {
        KlineClassification.EXACT_MATCH: ComparisonClassification.EXACT,
        KlineClassification.REST_NOT_YET_VISIBLE: ComparisonClassification.EXPECTED_TEMPORAL_DIFFERENCE,
        KlineClassification.REST_MISSING: ComparisonClassification.REST_UNAVAILABLE,
        KlineClassification.WS_MISSING: ComparisonClassification.WS_UNINITIALIZED,
        KlineClassification.FIELD_MISMATCH: ComparisonClassification.SOURCE_SEMANTIC_MISMATCH,
        KlineClassification.DATA_GAP: ComparisonClassification.REST_UNAVAILABLE,
    }[classification]


def _frame_to_rest_rows(frame: pd.DataFrame) -> list[list[str]]:
    """Materialize only the parity row shape from a defensive REST frame copy."""
    if frame.empty:
        return []
    rows: list[list[str]] = []
    for _, row in frame.iterrows():
        start_ms = row.get("start_ms")
        if pd.isna(start_ms):
            timestamp = row.get("timestamp", row.name)
            if pd.isna(timestamp):
                continue
            start_ms = int(pd.Timestamp(timestamp).timestamp() * 1000)
        values = [row.get(name) for name in ("open", "high", "low", "close", "volume", "turnover")]
        if any(pd.isna(value) for value in values):
            continue
        rows.append([str(int(start_ms)), *(str(value) for value in values)])
    return rows
