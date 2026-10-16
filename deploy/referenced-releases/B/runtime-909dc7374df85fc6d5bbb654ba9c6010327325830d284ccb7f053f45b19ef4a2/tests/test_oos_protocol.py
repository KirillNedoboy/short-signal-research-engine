from datetime import UTC, datetime, timedelta

import pytest

from app.research.oos_protocol import assess_dataset, chronological_split


def row(root, at, *, symbol="AAAUSDT", outcome="MATURE", quality="VALID"):
    t = datetime.fromisoformat(at).replace(tzinfo=UTC)
    return {
        "root_id": root,
        "attempt_id": f"attempt:{root}",
        "decision_time": t,
        "outcome_end_time": t + timedelta(minutes=15),
        "symbol": symbol,
        "market_regime": "normal",
        "outcome_status": outcome,
        "data_quality_status": quality,
    }


def test_dataset_assessment_uses_unique_roots_and_reports_unknowns():
    rows = [row("r1", "2026-01-01T00:00:00"), row("r1", "2026-01-01T00:01:00"), row("r2", "2026-01-01T01:00:00", outcome="UNKNOWN", quality="DATA_GAP")]
    result = assess_dataset(rows)
    assert result.unique_roots == 2
    assert result.rows == 3
    assert result.unknown_or_gap == 1
    assert result.status == "PRELIMINARY"


def test_chronological_split_applies_purge_embargo_and_symbol_holdout():
    rows = [
        row("train", "2026-01-01T00:00:00", symbol="AAAUSDT"),
        row("near-train", "2026-01-01T00:59:00", symbol="AAAUSDT"),
        row("validation", "2026-01-01T02:00:00", symbol="BBBUSD"),
        row("holdout", "2026-01-01T04:00:00", symbol="CCCUSD"),
    ]
    split = chronological_split(
        rows,
        train_end=datetime(2026, 1, 1, 1, tzinfo=UTC),
        validation_end=datetime(2026, 1, 1, 3, tzinfo=UTC),
        max_horizon=timedelta(minutes=15),
        embargo=timedelta(minutes=15),
        holdout_symbols={"CCCUSD"},
    )
    assert [r["root_id"] for r in split.train] == ["train"]
    assert [r["root_id"] for r in split.validation] == ["validation"]
    assert [r["root_id"] for r in split.holdout] == ["holdout"]
    assert split.status == "VALID"


def test_oos_status_never_completes_without_frozen_holdout():
    result = assess_dataset([row("r1", "2026-01-01T00:00:00")])
    assert result.status == "PRELIMINARY"
    assert result.promotion_eligible is False
