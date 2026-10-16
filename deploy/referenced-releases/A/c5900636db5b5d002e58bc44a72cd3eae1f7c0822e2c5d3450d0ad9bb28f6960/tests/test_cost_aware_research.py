from __future__ import annotations

from datetime import datetime, timezone

from app.outcomes.contract_v2 import (
    ContractV2Resolution,
    EntryFillStatus,
    OutcomeContractV2Result,
)
from app.research.cost_aware import summarize_cost_aware_results


def _result(root: str, net: float | None, status: str = "MATURE") -> OutcomeContractV2Result:
    return OutcomeContractV2Result(
        signal_id=None,
        symbol="TESTUSDT",
        event_id=root,
        decision_time=datetime(2026, 9, 1, tzinfo=timezone.utc),
        entry_fill_time=datetime(2026, 9, 1, 0, 1, tzinfo=timezone.utc),
        entry_fill_price=100.0,
        entry_fill_status=EntryFillStatus.FILLED,
        tp_price=97.0,
        sl_price=103.0,
        tp_hit_time=None,
        sl_hit_time=None,
        first_passage=ContractV2Resolution.TP_FIRST if (net or 0) > 0 else ContractV2Resolution.SL_FIRST,
        resolution_detail="test",
        mfe_pct=4.0,
        mae_pct=2.0,
        return_at_expiry_pct=net,
        total_cost_pct=0.2,
        net_return_pct=net,
        evaluated_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
        evaluation_status=status,
    )


def test_cost_aware_summary_deduplicates_roots_and_preserves_unknowns() -> None:
    summary = summarize_cost_aware_results([
        _result("root-1", 1.8),
        _result("root-1", 1.8),
        _result("root-2", -1.2),
        _result("root-3", None, status="DATA_GAP"),
    ])

    assert summary.independent_roots == 3
    assert summary.mature_roots == 2
    assert summary.data_gap_roots == 1
    assert summary.wins == 1
    assert summary.losses == 1
    assert summary.net_return_pct == 0.3
    assert summary.max_drawdown_pct == 1.2
