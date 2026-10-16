from pathlib import Path

from app.research.cost_aware_replay import inspect_replay_archive


BUNDLE = Path("/opt/short-telegram-bot-lite-admin/artifacts/strategy-correctness-20260910/candidate-empty-replay")


def test_empty_replay_archive_is_fail_closed_not_validated() -> None:
    report = inspect_replay_archive(BUNDLE)

    assert report.status == "NOT_VALIDATED"
    assert report.validated is False
    assert report.independent_roots == 0
    assert report.outcome_rows == 0
    assert "independent_root_ids" in report.missing_requirements
    assert "cost:entry_bid" in report.missing_requirements
    assert "horizon:4h" in report.missing_requirements
