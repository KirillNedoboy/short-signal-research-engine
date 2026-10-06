from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.market.coverage import (
    FAILURE_REASON_CODES,
    build_coverage_rows,
    coverage_percent,
    normalize_scan_failure,
    universe_fingerprint,
    validate_scan_accounting,
)
from app.storage.db import Database
from app.storage.models import MarketCoverageLedgerModel, MarketScanCycleModel, MarketScanRotationModel, MarketScanSymbolResultModel
from app.storage.repository import BotRepository


def test_universe_fingerprint_is_order_independent_and_unique():
    assert universe_fingerprint(["bUSDT", "AUSDT", "AUSDT"]) == universe_fingerprint(["ausdt", "BUSDT"])
    assert len(universe_fingerprint(["AUSDT"])) == 64


def _record(repo, eligible, batch, results):
    now = datetime.now(timezone.utc)
    return repo.record_market_scan_cycle(
        cycle_started_at=now - timedelta(seconds=1),
        cycle_completed_at=now,
        exchange_symbols=eligible + ["EXCLUDEDUSDT"],
        eligible_symbols=eligible,
        excluded=[("EXCLUDEDUSDT", "LIQUIDITY_FILTER")],
        scheduled_symbols=batch,
        symbol_results=results,
    )


def test_batch_100_of_500_is_not_full_rotation(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'coverage.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    eligible = [f"S{i}USDT" for i in range(500)]
    result = _record(repo, eligible, eligible[:100], [{"symbol": s, "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"} for s in eligible[:100]])
    assert result["status"] == "OPEN"
    assert result["eligible_coverage_pct"] == 20.0
    assert result["scheduled_unique_symbols"] == 100


def test_five_batches_complete_rotation_and_failed_is_counted_once(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'coverage.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    eligible = [f"S{i}USDT" for i in range(500)]
    last = None
    for index in range(5):
        batch = eligible[index * 100:(index + 1) * 100]
        status = "SCAN_FAILED" if index == 4 else "SCANNED_OK"
        last = _record(repo, eligible, batch, [{"symbol": s, "terminal_status": status, "reason_code": status} for s in batch])
    assert last["status"] == "COMPLETED"
    assert last["eligible_coverage_pct"] == 100.0
    assert last["failed_unique_symbols"] == 100
    assert last["scheduled_unique_symbols"] == 500
    with db.session() as session:
        assert session.query(MarketScanSymbolResultModel).count() == 501
        rotation = session.query(MarketScanRotationModel).one()
        assert rotation.eligible_coverage_pct <= 100.0


def test_rotation_scheduled_symbols_are_available_for_next_batch(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'rotation-state.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    eligible = ["AUSDT", "BUSDT", "CUSDT"]
    result = _record(
        repo,
        eligible,
        ["AUSDT", "BUSDT"],
        [{"symbol": s, "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"} for s in ["AUSDT", "BUSDT"]],
    )

    assert repo.rotation_scheduled_symbols(result["rotation_id"]) == {
        "AUSDT",
        "BUSDT",
        "EXCLUDEDUSDT",
    }


def test_rotation_state_read_failure_is_not_treated_as_empty_rotation(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'rotation-state-error.sqlite'}")
    db.create_all()
    repo = BotRepository(db)

    class BrokenDatabase:
        def session(self):
            raise RuntimeError("rotation state unavailable")

    repo._db = BrokenDatabase()

    assert repo.rotation_scheduled_symbols("rotation-1") is None


def test_rotation_universe_remains_frozen_when_current_universe_drifts(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'rotation-universe.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    result = _record(
        repo,
        ["AUSDT", "BUSDT", "CUSDT"],
        ["AUSDT"],
        [{"symbol": "AUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"}],
    )

    assert repo.rotation_universe(result["rotation_id"]) == (
        ["AUSDT", "BUSDT", "CUSDT", "EXCLUDEDUSDT"],
        ["AUSDT", "BUSDT", "CUSDT"],
    )


def test_open_rotation_is_reused_when_current_universe_fingerprint_drifts(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'rotation-drift.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    first = repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT", "BUSDT"],
        eligible_symbols=["AUSDT"],
    )
    second = repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc) + timedelta(seconds=1),
        exchange_symbols=["AUSDT", "BUSDT", "CUSDT"],
        eligible_symbols=["AUSDT", "BUSDT"],
    )

    assert second == first


def test_coverage_percent_is_bounded():
    assert coverage_percent(600, 500) == 100.0
    assert coverage_percent(0, 0) is None


def test_coverage_ledger_is_append_only_per_rotation_and_symbol(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'coverage-ledger.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    result = _record(repo, ["AUSDT"], ["AUSDT"], [{"symbol": "AUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"}])
    assert result["rotation_id"]
    with db.session() as session:
        rows = session.query(MarketCoverageLedgerModel).all()
        assert len(rows) == 2
        assert {row.symbol for row in rows} == {"AUSDT", "EXCLUDEDUSDT"}
        assert session.query(MarketCoverageLedgerModel).filter_by(symbol="EXCLUDEDUSDT").one().exclusion_reason == "LIQUIDITY_FILTER"


def test_excluded_result_remains_canonical_excluded_and_is_flagged():
    rows = build_coverage_rows(
        rotation_id="r1", observed_at=datetime.now(timezone.utc), exchange_symbols=["BADUSDT"],
        eligible_symbols=[], excluded=[("BADUSDT", "LIQUIDITY_FILTER")], scheduled_symbols=[],
        symbol_results=[{"symbol": "BADUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"}],
    )
    assert rows[0]["scan_status"] == "EXCLUDED"
    assert rows[0]["scanned"] is False
    assert rows[0]["unexpected_result_present"] is True


def test_scan_failure_reason_codes_are_stable_and_diagnostics_are_sanitized():
    assert FAILURE_REASON_CODES == {
        "DEADLINE_EXCEEDED",
        "STALE_MARKET_DATA",
        "MARKET_DATA_INCOMPLETE",
        "MARKET_DATA_DISCONTINUITY",
        "EMPTY_RESPONSE",
        "PROVIDER_ERROR",
    }

    result = normalize_scan_failure(
        "request timed out",
        exception=ValueError("secret-token=do-not-persist"),
    )

    assert result["terminal_status"] == "SCAN_FAILED"
    assert result["reason_code"] == "DEADLINE_EXCEEDED"
    assert result["details"] == {"exception_type": "ValueError"}


def test_scan_failed_never_becomes_no_setup_and_missing_reason_is_normalized():
    rows = build_coverage_rows(
        rotation_id="r1",
        observed_at=datetime.now(timezone.utc),
        exchange_symbols=["AAAUSDT", "BBBUSDT"],
        eligible_symbols=["AAAUSDT", "BBBUSDT"],
        excluded=[],
        scheduled_symbols=["AAAUSDT", "BBBUSDT"],
        symbol_results=[
            {"symbol": "AAAUSDT", "terminal_status": "SCAN_FAILED", "reason_code": "NO_SETUP"},
            {"symbol": "BBBUSDT", "terminal_status": "SCAN_FAILED"},
        ],
    )

    assert [row["scan_status"] for row in rows] == ["SCAN_FAILED", "SCAN_FAILED"]
    assert [row["evidence_json"]["reason_code"] for row in rows] == [
        "MARKET_DATA_INCOMPLETE",
        "MARKET_DATA_INCOMPLETE",
    ]


def test_invalid_terminal_status_is_persisted_as_safe_failure_with_evidence():
    rows = build_coverage_rows(
        rotation_id="r1",
        observed_at=datetime.now(timezone.utc),
        exchange_symbols=["AAAUSDT"],
        eligible_symbols=["AAAUSDT"],
        excluded=[],
        scheduled_symbols=["AAAUSDT"],
        symbol_results=[{"symbol": "AAAUSDT", "terminal_status": "BOGUS"}],
    )

    assert rows[0]["scan_status"] == "SCAN_FAILED"
    assert rows[0]["scan_status"] in {"SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
    assert rows[0]["evidence_json"]["observed_terminal_status"] == "BOGUS"


@pytest.mark.parametrize("terminal_status", [{"secret": "token-abc"}, ["token-abc"], None])
def test_unhashable_or_null_terminal_status_is_a_mismatch(terminal_status):
    result = validate_scan_accounting(
        scheduled_symbols=["AAAUSDT"],
        symbol_results=[{"symbol": "AAAUSDT", "terminal_status": terminal_status}],
    )

    assert result["valid"] is False
    assert result["mismatch"]["invalid_terminal_statuses"]


def test_coverage_evidence_redacts_token_like_and_structured_values():
    rows = build_coverage_rows(
        rotation_id="r1", observed_at=datetime.now(timezone.utc),
        exchange_symbols=["AAAUSDT"], eligible_symbols=["AAAUSDT"], excluded=[],
        scheduled_symbols=["AAAUSDT"],
        symbol_results=[{
            "symbol": "AAAUSDT",
            "terminal_status": {"token": "sk-live-secret"},
            "reason_code": ["private", "token-abc"],
        }],
    )

    evidence = rows[0]["evidence_json"]
    assert evidence["reason_code"] != ["private", "token-abc"]
    assert evidence["observed_terminal_status"] != {"token": "sk-live-secret"}
    assert "sk-live-secret" not in str(evidence)
    assert "token-abc" not in str(evidence)


def test_scan_accounting_rejects_missing_symbol_even_without_schedule():
    result = validate_scan_accounting(
        scheduled_symbols=[], symbol_results=[{"terminal_status": "SCANNED_OK"}]
    )

    assert result["valid"] is False
    assert result["mismatch"]["missing_symbol_rows"] == [0]


def test_scan_accounting_rejects_malformed_result_row():
    result = validate_scan_accounting(scheduled_symbols=[], symbol_results=[None])

    assert result["valid"] is False
    assert result["mismatch"]["malformed_result_rows"] == [0]


@pytest.mark.parametrize(
    ("results", "expected_detail"),
    [
        ([{"symbol": "AUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"}], "missing_symbols"),
        ([
            {"symbol": "AUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"},
            {"symbol": "AUSDT", "terminal_status": "SCAN_FAILED", "reason_code": "PROVIDER_ERROR"},
        ], "duplicate_result_symbols"),
        ([
            {"symbol": "AUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"},
            {"symbol": "CUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"},
        ], "unexpected_result_symbols"),
    ],
)
def test_scan_accounting_mismatch_is_persisted_as_cycle_failure(tmp_path, results, expected_detail):
    db = Database(f"sqlite:///{tmp_path / f'{expected_detail}.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)

    result = _record(repo, ["AUSDT", "BUSDT", "CUSDT"], ["AUSDT", "BUSDT"], results)

    assert result["status"] == "FAILED"
    assert result["cycle_status"] == "FAILED"
    assert result["accounting_error"] is True
    assert expected_detail in result["accounting_mismatch"]
    with db.session() as session:
        cycle = session.query(MarketScanCycleModel).one()
        assert cycle.status == "FAILED"
        assert cycle.details_json["accounting_mismatch"][expected_detail]
        assert cycle.details_json["unfinished_symbols"]
        assert session.query(MarketScanSymbolResultModel).filter(
            MarketScanSymbolResultModel.symbol.in_(["AUSDT", "BUSDT"])
        ).count() == 0


def test_scan_accounting_does_not_report_full_coverage_for_partial_batch(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'partial-batch.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)

    result = _record(
        repo,
        ["AUSDT", "BUSDT"],
        ["AUSDT", "BUSDT"],
        [{"symbol": "AUSDT", "terminal_status": "SCANNED_OK", "reason_code": "SCANNED_OK"}],
    )

    assert result["status"] == "FAILED"
    assert result["eligible_coverage_pct"] == 0.0
    assert result["scheduled_unique_symbols"] == 0
