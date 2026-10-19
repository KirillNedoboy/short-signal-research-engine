"""Deterministic benchmark of the candidate composition-root hot path.

The provider and repository are in-memory seams, but every window enters the
candidate ``LaneCCompositionRoot.run_window`` path: shortlist, frame admission,
per-symbol coordination, feature/lifecycle pipeline and terminal status.  It
is a synthetic throughput check for 120 windows x 100 symbols, never a
production 24-hour claim.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import yaml

SRC = Path(__file__).parents[1] / "src"
sys.path.insert(0, str(SRC))

from app.config import AppConfig, CanonicalMarketDataProviderMode  # noqa: E402
from app.domain import EventState, EventStatus  # noqa: E402
from app.main import (  # noqa: E402
    LaneCCompositionRoot,
    SUCCESSFUL_EVALUATION_STATUSES,
    strategy_config_fingerprint,
)


def _frame(end: datetime, rows: int = 300) -> pd.DataFrame:
    timestamps = [end - timedelta(minutes=rows - index) for index in range(rows)]
    close = [100.0 + index * 0.01 for index in range(rows)]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": close,
            "high": [value + 0.2 for value in close],
            "low": [value - 0.2 for value in close],
            "close": close,
            "volume": [1000.0 + index for index in range(rows)],
            "turnover": [100000.0 + index for index in range(rows)],
        }
    ).set_index("timestamp", drop=False)


class _Repository:
    def __init__(self, states: dict[str, EventState]) -> None:
        self._states = states
        self.save_signal_calls = 0

    def list_active_event_states(self, *, now: datetime) -> list[Any]:
        return [state for state in self._states.values() if state.is_active]

    def get_event_state(self, symbol: str) -> EventState | None:
        return self._states.get(symbol.upper())

    def upsert_event_state(self, state: Any) -> Any:
        self._states[state.symbol.upper()] = state
        return state

    def save_signal(self, *_args: Any, **_kwargs: Any) -> Any:
        self.save_signal_calls += 1
        return SimpleNamespace(id=1)


class _Scanner:
    async def fetch_optional_derivatives(self, _symbol: str) -> dict[str, Any]:
        return _derivatives()


class _Provider:
    def __init__(self, symbols: tuple[str, ...], frame: pd.DataFrame, captured_at: datetime) -> None:
        self._symbols = symbols
        self._frame = frame
        self._captured_at = captured_at
        self._scanner = _Scanner()
        self._derivatives = _derivatives(captured_at)
        self.last_universe_telemetry = None

    def refresh_window(self, captured_at: datetime) -> None:
        """Keep synthetic frame timestamps coherent with the window as-of."""

        self._captured_at = captured_at
        self._frame = _frame(captured_at - timedelta(minutes=1))
        self._derivatives = _derivatives(captured_at)

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def build_scan_snapshot(self) -> Any:
        return SimpleNamespace(
            snapshots=tuple(SimpleNamespace(symbol=symbol) for symbol in self._symbols)
        )

    def shortlist(self, snapshots: list[Any]) -> list[Any]:
        return snapshots

    def update_universe(self, symbols: list[str]) -> None:
        return None

    async def fetch_symbol_frames(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        return {symbol: self._frame for symbol in symbols}

    async def capture_decision_snapshot(
        self,
        symbol: str,
        *,
        frame_1m: pd.DataFrame,
        include_liquidity: bool,
        derivatives: dict[str, Any] | None = None,
    ) -> Any:
        frame = self._frame if frame_1m is None else frame_1m
        return SimpleNamespace(
            frame_1m=SimpleNamespace(frame=frame),
            derivatives=derivatives or self._derivatives,
            liquidity={
                "spread_pct": 0.05,
                "slippage_pct": 0.05,
                "orderbook_depth_usdt_1pct": 100_000.0,
                "orderbook_depth_usdt_2pct": 200_000.0,
            },
            captured_at_utc=self._captured_at,
        )


def _derivatives(observed_at: datetime | None = None) -> dict[str, Any]:
    """Return complete derivative inputs with an explicit window as-of."""

    observed = observed_at.isoformat() if observed_at is not None else None
    return {
        "open_interest": [
            {"openInterest": "110", "timestamp": observed},
            {"openInterest": "105", "timestamp": observed},
            {"openInterest": "100", "timestamp": observed},
            {"openInterest": "100", "timestamp": observed},
            {"openInterest": "100", "timestamp": observed},
        ],
        "funding": [{"fundingRate": "0.001", "timestamp": observed}],
        "derivatives_status": "OK",
        "derivatives_reasons": [],
        "data_quality_warnings": [],
        "oi_observed_at": observed,
        "funding_observed_at": observed,
    }


def _states(symbols: tuple[str, ...], now: datetime) -> dict[str, EventState]:
    started = now - timedelta(minutes=20)
    return {
        symbol: EventState(
            symbol=symbol,
            event_id=f"benchmark-event-{index}",
            state=EventStatus.SHORT_ZONE_ACTIVE,
            event_start_time=started,
            event_high=130.0,
            event_high_time=started + timedelta(minutes=5),
            event_base_price=100.0,
            event_range_pct=30.0,
            event_features_snapshot={"breakout_reference": 129.0},
            trigger_window="15m",
            pullback_detected_at=now - timedelta(minutes=5),
            pullback_depth_pct=2.0,
            pullback_low_price=127.0,
            zone_low=127.0,
            zone_high=130.0,
            updated_at=now,
        )
        for index, symbol in enumerate(symbols)
    }


async def run(*, windows: int = 120, symbols_count: int = 100) -> dict[str, object]:
    if windows < 1 or symbols_count < 1:
        raise ValueError("windows and symbols must be positive")
    symbols = tuple(f"COIN{index:03d}USDT" for index in range(symbols_count))
    captured_at = datetime.now(UTC).replace(second=0, microsecond=0)
    frame = _frame(captured_at - timedelta(minutes=1))
    provider = _Provider(symbols, frame, captured_at)
    repository = _Repository(_states(symbols, captured_at))
    # Start from the checked-in effective C example so strategy thresholds do
    # not silently fall back to AppConfig defaults.  REST_ONLY is selected only
    # for this in-memory provider seam; production config remains HUB_PREFERRED.
    config_payload = yaml.safe_load(
        (SRC.parent / "config.example.yaml").read_text(encoding="utf-8")
    ) or {}
    config_payload.pop("AUTOEXECUTION", None)
    config_payload.update(
        {
            "autoexecution": "OFF",
            "db_url": "sqlite:///./data/benchmark.sqlite",
            "shortlist_size": symbols_count,
            "canonical_market_data_provider_mode": CanonicalMarketDataProviderMode.REST_ONLY,
        }
    )
    config = AppConfig.model_validate(config_payload)
    runtime = LaneCCompositionRoot(
        config,
        repository=repository,
        provider=provider,
    )
    durations: list[float] = []
    p0_durations: list[float] = []
    coverage: list[float] = []
    status_counts: Counter[str] = Counter()
    await runtime.start()
    try:
        for window_number in range(windows):
            captured_at = datetime.now(UTC).replace(second=0, microsecond=0)
            provider.refresh_window(captured_at)
            # Model the bounded background refresher at the same as-of as the
            # frame.  The scan reads the cache synchronously and never invents
            # freshness in the hot path.
            runtime.market.seed_derivatives(
                {symbol: _derivatives(captured_at) for symbol in symbols},
                observed_at=captured_at,
            )
            started = time.perf_counter()
            report = await runtime.run_window(window_id=f"synthetic-{window_number:03d}")
            durations.append((time.perf_counter() - started) * 1000.0)
            p0_durations.extend(report.p0_durations_ms)
            coverage.append(report.coverage)
            status_counts.update(report.statuses.values())
    finally:
        await runtime.stop()

    ordered = sorted(durations)
    p95 = ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]
    ordered_p0 = sorted(p0_durations)
    p0_p95 = (
        ordered_p0[min(len(ordered_p0) - 1, round(0.95 * (len(ordered_p0) - 1)))]
        if ordered_p0
        else None
    )
    total_statuses = sum(status_counts.values())
    status_fractions = {
        status: round(count / total_statuses, 6)
        for status, count in sorted(status_counts.items())
    }
    successful_evaluations = sum(
        count
        for status, count in status_counts.items()
        if status in SUCCESSFUL_EVALUATION_STATUSES
    )
    failure_status_counts = {
        status: count
        for status, count in sorted(status_counts.items())
        if status not in SUCCESSFUL_EVALUATION_STATUSES
    }
    coverage_min = min(coverage)
    window_p95_pass = p95 <= 20_000
    coverage_pass = coverage_min >= 0.99
    p0_p95_pass = p0_p95 is not None and p0_p95 <= 15_000
    acceptance_pass = window_p95_pass and coverage_pass and p0_p95_pass
    return {
        "synthetic": True,
        "hot_path": "LaneCCompositionRoot.run_window",
        "pipeline": "full C registry/lifecycle/admission with per-window refreshed cached valid OI+funding",
        "strategy_config_fingerprint": strategy_config_fingerprint(config),
        "canonical_market_data_provider_mode": config.canonical_market_data_provider_mode.value,
        "outcome_worker_mode": runtime.outcome_worker_mode,
        "effective_strategy": {
            "enable_squeeze_guard": config.enable_squeeze_guard,
            "squeeze_guard_mode": config.squeeze_guard_mode,
            "derivatives_enabled": config.derivatives_enabled,
            "climax_short_enabled": config.climax_short_enabled,
            "volume_climax_unwind_enabled": config.volume_climax_unwind_enabled,
            "low_volume_extension_enabled": config.low_volume_extension_enabled,
            "trapped_longs_reversal_enabled": config.trapped_longs_reversal_enabled,
        },
        "windows": windows,
        "symbols_per_window": symbols_count,
        "window_p95_ms": round(p95, 3),
        "window_median_ms": round(statistics.median(durations), 3),
        "p0_p95_ms": round(p0_p95, 3) if p0_p95 is not None else None,
        "p0_samples": len(p0_durations),
        "coverage_min": round(coverage_min, 6),
        "status_counts": dict(status_counts),
        "status_fractions": status_fractions,
        "successful_evaluations": successful_evaluations,
        "failure_status_counts": failure_status_counts,
        "acceptance_pass": acceptance_pass,
        "acceptance": {
            "pass": acceptance_pass,
            "window_p95_pass": window_p95_pass,
            "p0_p95_pass": p0_p95_pass,
            "coverage_pass": coverage_pass,
        },
        "p0_deadline_misses": status_counts.get("DEADLINE_EXCEEDED", 0),
        "p0_target_ms": 15_000,
        "p0_p95_pass": p0_p95_pass,
        "registry": {
            "entries_per_evaluation": 5,
            "families": [
                "BASELINE_PULLBACK",
                "CLIMAX_EXHAUSTION",
                "VOLUME_CLIMAX_UNWIND",
                "LOW_VOLUME_EXTENSION_FAILURE",
                "TRAPPED_LONGS_REVERSAL",
            ],
        },
        "persistence": {
            "path": "SignalPersistence.save -> repository.save_signal",
            "repository": "in_memory_benchmark_seam",
            "save_signal_calls": repository.save_signal_calls,
            "production_db_writes": False,
        },
        "market_io": {
            "provider": "in_memory_snapshot_seam",
            "derivatives": "bounded_cache_seed_before_each_window_with_observed_at",
            "freshness": "frame_and_derivatives_asof_regenerated_before_each_window",
            "network_requests": False,
        },
        "targets": {"window_p95_ms": 20_000, "coverage_min": 0.99},
        "production_24h_claim": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--windows", type=int, default=120)
    parser.add_argument("--symbols", type=int, default=100)
    args = parser.parse_args()
    result = asyncio.run(run(windows=args.windows, symbols_count=args.symbols))
    print(json.dumps(result, sort_keys=True))
    return 0 if result["acceptance_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
