"""Cost-aware research aggregation over independently identified outcomes."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.outcomes.contract_v2 import OutcomeContractV2Result


@dataclass(frozen=True, slots=True)
class CostAwareSummary:
    independent_roots: int
    mature_roots: int
    data_gap_roots: int
    unknown_roots: int
    wins: int
    losses: int
    mean_net_return_pct: float | None
    max_drawdown_pct: float | None

    @property
    def net_return_pct(self) -> float | None:
        """Backward-readable alias for the mean mature net return."""
        return self.mean_net_return_pct


def summarize_cost_aware_results(
    results: Iterable[OutcomeContractV2Result],
) -> CostAwareSummary:
    """Deduplicate by independent root and summarize only mature net outcomes."""
    by_root: dict[str, OutcomeContractV2Result] = {}
    for result in results:
        root = result.event_id or f"{result.symbol}:{result.decision_time.isoformat()}"
        existing = by_root.get(root)
        if existing is None or result.evaluated_at > existing.evaluated_at:
            by_root[root] = result

    mature = [
        result
        for result in by_root.values()
        if result.evaluation_status == "MATURE" and result.net_return_pct is not None
    ]
    data_gaps = sum(
        result.evaluation_status == "DATA_GAP" or result.first_passage.value == "DATA_GAP"
        for result in by_root.values()
    )
    unknown = len(by_root) - len(mature) - data_gaps
    returns = [float(result.net_return_pct) for result in mature]
    equity = 0.0
    peak = 0.0
    drawdown = 0.0
    for value in returns:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)

    return CostAwareSummary(
        independent_roots=len(by_root),
        mature_roots=len(mature),
        data_gap_roots=data_gaps,
        unknown_roots=unknown,
        wins=sum(value > 0 for value in returns),
        losses=sum(value < 0 for value in returns),
        mean_net_return_pct=round(sum(returns) / len(returns), 6) if returns else None,
        max_drawdown_pct=drawdown if returns else None,
    )
