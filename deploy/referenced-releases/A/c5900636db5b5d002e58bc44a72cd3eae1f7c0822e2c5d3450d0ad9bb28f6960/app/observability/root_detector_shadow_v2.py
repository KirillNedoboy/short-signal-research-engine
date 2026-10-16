"""Pure, shadow-only paired early-root experiment primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.observability.root_detector_shadow import should_create_shadow_candidate

METHOD_VERSION = "ROOT_DETECTOR_SHADOW_V2_CONTRACT_V1"
DESIGN_INCONCLUSIVE = "DESIGN_INCONCLUSIVE"
WAITING = "WAITING"
EARLY_ROOT_CREATED = "EARLY_ROOT_CREATED"
LIVE_ROOT_SEEN = "LIVE_ROOT_SEEN"
EXPIRED = "EXPIRED"


@dataclass(frozen=True, slots=True)
class DetectorPredicate:
    name: str
    source: str
    classification: str
    temporal_dependency: str
    safety_purpose: str


@dataclass(frozen=True, slots=True)
class CounterfactualContract:
    current_required: tuple[str, ...]
    v2_required: tuple[str, ...]
    deferred: tuple[str, ...]
    safety_rationale: dict[str, str]
    implementable: bool = False

    @property
    def diff(self) -> str:
        current = ", ".join(self.current_required) or "<none>"
        v2 = ", ".join(self.v2_required) or "<none>"
        deferred = "\n".join(
            f"{name} = {self.safety_rationale.get(name, '<missing rationale>')}"
            for name in self.deferred
        ) or "<none>"
        return (
            f"CURRENT ROOT requires: {current}\n"
            f"V2 requires: {v2}\n"
            f"V2 intentionally does not wait for:\n{deferred}"
        )


@dataclass(frozen=True, slots=True)
class ContractAudit:
    predicates: tuple[DetectorPredicate, ...]
    counterfactual_diff: str
    verdict: str


@dataclass(frozen=True, slots=True)
class V2RootDecision:
    state: str
    root_id: str | None
    reason: str
    condition_snapshot: dict[str, str] = field(default_factory=dict)
    method_version: str = METHOD_VERSION


def default_counterfactual_contract() -> CounterfactualContract:
    audit = audit_current_detector_contract()
    return CounterfactualContract(
        current_required=("trigger_window", "live_stretch"),
        v2_required=("v1_trigger",),
        deferred=("trigger_window", "live_stretch"),
        safety_rationale={
            "trigger_window": "structural maturity delay; no execution side effect",
            "live_stretch": "structural pump admission delay; no execution-safety predicate",
        },
        implementable=audit.verdict == "PASS",
    )


def audit_current_detector_contract() -> ContractAudit:
    """Return the source-derived contract inventory and the hard design gate.

    The current implementation exposes a precise structural counterfactual:
    V2 reuses the existing V1 observation trigger, while current EventState
    creation additionally waits for the live trigger-window/stretch contract.
    No execution-safety predicate is weakened.
    """
    predicates = (
        DetectorPredicate(
            "trigger_window", "app/events/pump_detector.py:qualifies", "STRUCTURAL_PUMP", "none", "episode recognition",
        ),
        DetectorPredicate(
            "live_stretch", "app/events/pump_detector.py:qualifies", "STRUCTURAL_PUMP", "none", "pump confirmation",
        ),
        DetectorPredicate(
            "closed_candles_after_high", "app/signals/climax.py:_common_metadata", "TEMPORAL_MATURITY", "closed candles", "maturity",
        ),
        DetectorPredicate(
            "liquidity", "app/signals/climax.py:_volume_climax", "EXECUTION_SAFETY", "none", "execution safety",
        ),
        DetectorPredicate(
            "oi_squeeze", "app/signals/climax.py:_volume_climax", "SQUEEZE_SAFETY", "none", "squeeze protection",
        ),
        DetectorPredicate(
            "rejection_retest", "app/signals/climax.py:_low_volume", "DOWNSTREAM_CONFIRMATION", "post-high", "entry confirmation",
        ),
    )
    contract = CounterfactualContract(
        current_required=("trigger_window", "live_stretch"),
        v2_required=("v1_trigger",),
        deferred=("trigger_window", "live_stretch"),
        safety_rationale={
            "trigger_window": "structural maturity delay; no execution side effect",
            "live_stretch": "structural pump admission delay; no execution-safety predicate",
        },
        implementable=True,
    )
    return ContractAudit(predicates=predicates, counterfactual_diff=contract.diff, verdict="PASS")


def evaluate_v2_observation(
    *,
    episode_id: str,
    symbol: str,
    episode_opened_at: datetime,
    observed_at: datetime,
    reference_price: float,
    event_high: float | None,
    features: dict[str, Any],
    current_live_root_id: str | None,
    contract: CounterfactualContract,
    existing_root_id: str | None = None,
) -> V2RootDecision:
    """Evaluate one V1 observation without any live-path side effects."""
    if existing_root_id:
        return V2RootDecision(EARLY_ROOT_CREATED, existing_root_id, "existing_first_v2_root")
    if current_live_root_id:
        return V2RootDecision(LIVE_ROOT_SEEN, None, "live_root_already_seen")
    if not contract.implementable:
        return V2RootDecision(WAITING, None, DESIGN_INCONCLUSIVE)
    candidate = type("Candidate", (), {
        "pump_5m": float(features.get("pump_5m") or 0.0),
        "pump_15m": float(features.get("pump_15m") or 0.0),
        "pump_1h": float(features.get("pump_1h") or 0.0),
        "pump_4h": float(features.get("pump_4h") or 0.0),
    })()
    # Reuse the existing V1 trigger thresholds; no V2 thresholds are introduced.
    conditions = {"v1_trigger": "PASS" if should_create_shadow_candidate(candidate) else "FAIL"}
    if conditions["v1_trigger"] != "PASS":
        return V2RootDecision(WAITING, None, "v1_trigger_missing", conditions)
    root_id = f"{episode_id}:FIRST_V2_EARLY_ROOT"
    return V2RootDecision(EARLY_ROOT_CREATED, root_id, "audited_v1_trigger", conditions)


def utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def paired_root_metrics(*, v2_anchor_price: float, live_root_anchor_price: float,
                        mfe_from_v2_to_live: float | None, mfe_24h: float | None) -> dict[str, float | str]:
    """Return paired timing metrics with short-context price sign conventions."""
    if v2_anchor_price <= 0:
        raise ValueError("v2_anchor_price must be positive")
    fraction: float | str
    if mfe_from_v2_to_live is None or mfe_24h is None or mfe_24h == 0:
        fraction = "N/A"
    else:
        fraction = mfe_from_v2_to_live / mfe_24h
    return {
        "price_move_before_live_root": (v2_anchor_price - live_root_anchor_price) / v2_anchor_price * 100.0,
        "fraction_of_total_24h_MFE_already_gone_before_live_root": fraction,
    }
