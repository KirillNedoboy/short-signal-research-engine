from __future__ import annotations

from pathlib import Path

import pandas as pd

from app.outcomes.evaluator import evaluate_hypothetical_short
from research.climax_replay import load_fixture


FIXTURE_PATH = Path("research/fixtures/akeusdt_2026-07-15.json")


def test_hypothetical_short_outcomes_include_required_shadow_horizons() -> None:
    timestamps = pd.date_range("2026-07-15T08:00:00Z", periods=20, freq="min")
    frame = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [100.0] * 20,
            "high": [101.0 + i * 0.1 for i in range(20)],
            "low": [99.0 - i * 0.1 for i in range(20)],
            "close": [100.0 - i * 0.2 for i in range(20)],
        }
    )
    outcome = evaluate_hypothetical_short(
        entry_time=timestamps[0].to_pydatetime(),
        entry_price=100.0,
        frame_1m=frame,
    )

    assert set(outcome["horizons"]) == {"1m", "3m", "5m", "15m"}
    assert outcome["mfe_pct"] > 0
    assert outcome["mae_pct"] >= 0
    assert outcome["new_high_after_entry"] is True


def test_ake_fixture_is_complete_and_marks_unavailable_microstructure() -> None:
    fixture = load_fixture(FIXTURE_PATH)

    assert fixture["symbol"] == "AKEUSDT"
    assert len(fixture["candles_1m"]) == 301
    assert fixture["missing_data"]["trades"]["value"] is None
    assert fixture["missing_data"]["trades"]["missing_reason"]
    assert fixture["missing_data"]["orderbook"]["value"] is None


def test_models_are_deterministic_and_taker_model_is_explicitly_incomplete() -> None:
    from research.climax_replay import run_replay

    first = run_replay(load_fixture(FIXTURE_PATH))
    second = run_replay(load_fixture(FIXTURE_PATH))

    assert first == second
    assert set(first["models"]) == {"M1", "M2", "M3", "M4"}
    assert first["models"]["M1"]["confirmation_time_utc"] == "2026-07-15T08:05:00Z"
    assert first["models"]["M2"]["confirmation_time_utc"] == "2026-07-15T08:02:00Z"
    assert first["models"]["M3"]["status"] == "INSUFFICIENT_DATA"
    assert first["models"]["M3"]["missing_reason"] == "historical_trades_not_available"
    assert first["models"]["M4"]["confirmation_time_utc"] == "2026-07-15T08:03:00Z"
    assert first["first_confirmed_model"] == "M2"


def test_report_keeps_blogger_claims_separate_from_market_and_server_evidence() -> None:
    from research.climax_replay import run_replay

    report = run_replay(load_fixture(FIXTURE_PATH))

    assert report["evidence"]["blogger"]["source"] == "user_screenshot_derived"
    assert report["baseline"]["eligible"] is True
    assert report["baseline"]["deep_scan"] is True
    assert report["missing_data"]["orderbook"]["missing_reason"]


def test_oi_observations_pair_adjacent_snapshots_without_losing_last_sample() -> None:
    from research.climax_replay import run_replay

    report = run_replay(load_fixture(FIXTURE_PATH))

    assert len(report["oi_price_observations"]["5m"]) == 60
    assert report["oi_price_observations"]["5m"][0]["from_utc"] == "2026-07-15T07:30:00Z"


def test_confirmed_model_has_paper_outcomes_and_first_hit_ordering() -> None:
    from research.climax_replay import run_replay

    report = run_replay(load_fixture(FIXTURE_PATH))
    confirmed = [model for model in report["models"].values() if model["status"] == "CONFIRMED"]

    assert confirmed
    for model in confirmed:
        assert model["paper_entry"]["time_utc"]
        assert set(model["outcomes"]) == {"5m", "15m", "30m", "1h", "4h"}
        assert model["outcomes"]["4h"]["mfe_pct"] >= 0
        assert model["outcomes"]["4h"]["mae_pct"] >= 0
        assert model["first_hit"]["favorable_5_vs_adverse_3"] in {
            "FAVORABLE_5_FIRST",
            "ADVERSE_3_FIRST",
            "NEITHER",
        }
        assert model["first_hit"]["favorable_10_vs_adverse_5"] in {
            "FAVORABLE_10_FIRST",
            "ADVERSE_5_FIRST",
            "NEITHER",
        }


def test_report_writer_outputs_json_and_markdown_without_touching_runtime(tmp_path: Path) -> None:
    from research.climax_replay import run_replay
    from research.run_akeusdt_replay import write_reports

    paths = write_reports(run_replay(load_fixture(FIXTURE_PATH)), tmp_path)

    assert paths["json"].is_file()
    assert paths["markdown"].is_file()
    assert 'user_screenshot_derived' in paths["markdown"].read_text(encoding="utf-8")
    assert 'historical_trades_not_available' in paths["json"].read_text(encoding="utf-8")
