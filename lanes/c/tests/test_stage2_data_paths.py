from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.config import AppConfig
from app.main import _derivative_terminal_status
from app.runtime.derivatives import DerivativeRefreshCache
from app.runtime.pipeline import LaneCStrategyPipeline


class _DerivativeScanner:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    async def fetch_optional_derivatives(
        self, symbol: str, *, force_refresh: bool = False
    ) -> dict[str, object]:
        self.calls.append((symbol, force_refresh))
        return {
            "symbol": symbol,
            "open_interest": [
                {"openInterest": "110"},
                {"openInterest": "100"},
            ],
            "funding": [{"fundingRate": "0.001"}],
            "derivatives_status": "OK",
            "derivatives_reasons": [],
            "data_quality_warnings": [],
        }


@pytest.mark.asyncio
async def test_derivative_cache_refreshes_outside_hot_path_and_expires_unknown() -> None:
    now = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    scanner = _DerivativeScanner()
    cache = DerivativeRefreshCache(
        scanner,
        AppConfig(autoexecution="OFF", derivatives_enabled=True),
        clock=lambda: now,
    )
    await cache.refresh_now(["SOLUSDT"])
    fresh = cache.get("SOLUSDT")
    assert fresh["derivatives_status"] == "OK"
    assert fresh["open_interest"]
    assert scanner.calls == [("SOLUSDT", True)]

    now += timedelta(seconds=181)
    stale = cache.get("SOLUSDT")
    assert stale["derivatives_status"] == "STALE"
    assert stale["open_interest"] == []
    assert stale["scan_failure"] == {"code": "DERIVATIVES_STALE"}


@pytest.mark.asyncio
async def test_derivative_response_ttl_ignores_old_measurement_rows_and_preserves_on_failure() -> None:
    class _Clock:
        value = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

        def __call__(self) -> datetime:
            return self.value

    class _HistoricalScanner:
        fail = False

        async def fetch_optional_derivatives(
            self, symbol: str, *, force_refresh: bool = False
        ) -> dict[str, object]:
            if self.fail:
                raise RuntimeError("temporary provider outage")
            return {
                "symbol": symbol,
                "open_interest": [
                    {
                        "openInterest": "110",
                        "timestamp": (clock.value - timedelta(minutes=4)).isoformat(),
                    },
                    {
                        "openInterest": "100",
                        "timestamp": (clock.value - timedelta(minutes=19)).isoformat(),
                    },
                ],
                "funding": [
                    {
                        "fundingRate": "0.001",
                        "timestamp": (clock.value - timedelta(hours=8)).isoformat(),
                    }
                ],
                "derivatives_status": "OK",
                "derivatives_reasons": [],
                "data_quality_warnings": [],
            }

    clock = _Clock()
    scanner = _HistoricalScanner()
    cache = DerivativeRefreshCache(
        scanner,
        AppConfig(autoexecution="OFF", derivatives_enabled=True),
        clock=clock,
    )
    await cache.refresh_now(["SOLUSDT"])
    fresh = cache.get("SOLUSDT")
    assert fresh["derivatives_status"] == "OK"
    assert fresh["open_interest"] and fresh["funding"]
    assert 3 * 60 < fresh["oi_measurement_age_sec"] < 5 * 60

    clock.value += timedelta(seconds=120)
    scanner.fail = True
    failed_refresh = await cache.refresh_now(["SOLUSDT"])
    assert failed_refresh == {"requested": 1, "refreshed": 0, "failed": 1}
    still_usable = cache.get("SOLUSDT")
    assert still_usable["derivatives_status"] == "OK"
    assert still_usable["open_interest"]

    clock.value += timedelta(seconds=61)
    expired = cache.get("SOLUSDT")
    assert expired["derivatives_status"] == "STALE"
    assert expired["open_interest"] == []


def test_derivative_gate_has_explicit_terminal_statuses() -> None:
    valid = {
        "derivatives_status": "OK",
        "open_interest": [{"openInterest": "1"}],
        "funding": [{"fundingRate": "0.001"}],
    }
    assert _derivative_terminal_status(valid, required=True) is None
    assert _derivative_terminal_status(
        {"derivatives_status": "STALE"}, required=True
    ).value == "STALE_DATA"
    assert _derivative_terminal_status(
        {"derivatives_status": "UNKNOWN"}, required=True
    ).value == "INCOMPLETE_DATA"


def _low_volume_case() -> dict[str, object]:
    path = Path(__file__).parent / "fixtures" / "strategy_cases.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return next(item for item in payload["cases"] if item["name"] == "low_volume_actionable")


@pytest.mark.asyncio
async def test_low_volume_final_recheck_replays_c_veto_semantics() -> None:
    root = Path(__file__).parents[1]
    sys.path.insert(0, str(root / "tools"))
    from strategy_parity_worker import _features, _frame, _state  # type: ignore[import-not-found]

    case = _low_volume_case()
    config = AppConfig.model_validate(case["config"])
    decision_at = datetime.fromisoformat(case["decision_at"])
    frame = _frame(case["frame_rows"], case.get("volume_tail"))
    state = _state(case)
    features = _features(case, frame, state, config, decision_at)
    pipeline = LaneCStrategyPipeline(
        config, config_fingerprint=case["strategy_config_fingerprint"]
    )
    from app.domain import ShortZone

    result = pipeline.evaluate_envelope(
        state=state,
        features=features,
        frame_1m=frame,
        short_zone=ShortZone(**case["short_zone"]),
        decision_at=decision_at,
    )
    assert result.decision is not None
    assert result.decision.strategy_subtype == "LOW_VOLUME_EXTENSION_FAILURE"

    # The parity envelope above deliberately overrides the raw feature price
    # and OI delta.  A final recheck must consume a real provider snapshot, so
    # give it a raw derivative history that produces the same C-side
    # short-covering classification without relying on fixture overrides.
    fresh_derivatives = dict(case["derivatives"])
    fresh_derivatives["open_interest"] = [
        {"openInterest": "99.5"},
        {"openInterest": "100"},
        {"openInterest": "100"},
        {"openInterest": "100"},
        {"openInterest": "100"},
    ]

    class _Provider:
        def __init__(self, liquidity: dict[str, float]) -> None:
            self.calls = 0
            self.liquidity = liquidity

        async def capture_decision_snapshot(self, symbol: str, **kwargs: object):
            self.calls += 1
            assert kwargs["frame_1m"] is None
            return SimpleNamespace(
                frame_1m=SimpleNamespace(frame=frame),
                derivatives=fresh_derivatives,
                liquidity=self.liquidity,
                captured_at_utc=decision_at,
            )

    provider = _Provider(case["liquidity"])
    finalized = await pipeline.finalize_admission(
        result,
        provider=provider,
        symbol=case["symbol"],
        frame_1m=frame,
        derivatives=case["derivatives"],
        decision_at=decision_at,
    )
    assert finalized.decision is not None
    assert finalized.decision.strategy_subtype == "LOW_VOLUME_EXTENSION_FAILURE"
    assert provider.calls == 1

    blocked = await pipeline.finalize_admission(
        result,
        provider=_Provider({}),
        symbol=case["symbol"],
        frame_1m=frame,
        derivatives=case["derivatives"],
        decision_at=decision_at,
    )
    assert blocked.decision is None
    assert blocked.blocked_reason == "LOW_VOLUME_FINAL_RECHECK_LIQUIDITY_MISSING"
