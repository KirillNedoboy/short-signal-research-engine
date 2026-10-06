from __future__ import annotations

import json
from datetime import datetime, timezone

from app.config import ResearchTelemetryStorageMode
from app.infra.disk_capacity import DiskCapacitySnapshot
from app.market.coverage import build_coverage_rows, validate_scan_accounting
from app.research.spool import ResearchSpool
from app.research.storage_router import ResearchStorageRouter
from app.storage.db import Database
from app.storage.models import (
    MarketCoverageLedgerModel,
    MarketScanCycleModel,
    MarketScanSymbolResultModel,
)
from app.storage.repository import BotRepository


def _capacity() -> DiskCapacitySnapshot:
    return DiskCapacitySnapshot(
        total_bytes=4_000_000_000, used_bytes=1_000_000_000,
        free_bytes=3_000_000_000, free_percent=75.0,
        total_inodes=1000, free_inodes=500, free_inode_percent=50.0,
        db_bytes=1000, wal_bytes=100,
    )


def _router(tmp_path):
    return ResearchStorageRouter(
        mode=ResearchTelemetryStorageMode.SPOOL_CANONICAL,
        spool=ResearchSpool(tmp_path / "spool"),
        capacity_provider=_capacity,
        reserve_bytes=1000,
        code_sha="test-sha",
    )


def test_coverage_rows_use_terminal_status_for_missing_scheduled_and_unscheduled():
    rows = build_coverage_rows(
        rotation_id="r1", observed_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT", "BUSDT", "CUSDT"],
        eligible_symbols=["AUSDT", "BUSDT", "CUSDT"], excluded=[],
        scheduled_symbols=["AUSDT", "BUSDT"],
        symbol_results=[{"symbol": "AUSDT", "terminal_status": "SCANNED_OK"}],
    )
    assert {row["scan_status"] for row in rows} == {"SCANNED_OK", "SCAN_FAILED", "SCAN_SKIPPED"}
    assert rows[1]["evidence_json"]["reason_code"] == "MARKET_DATA_INCOMPLETE"


def test_spool_canonical_rejects_scheduled_outside_eligible_and_persists_failure(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'outside.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    proxy = _router(tmp_path).wrap_repository(repo)
    now = datetime.now(timezone.utc)
    result = proxy.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        scheduled_symbols=["AUSDT", "OUTSIDEUSDT"],
        symbol_results=[
            {"symbol": "AUSDT", "terminal_status": "SCANNED_OK"},
            {"symbol": "OUTSIDEUSDT", "terminal_status": "SCANNED_OK"},
        ],
    )
    assert result["cycle_status"] == "FAILED"
    assert result["accounting_mismatch"]["scheduled_outside_eligible"] == ["OUTSIDEUSDT"]
    with db.session() as session:
        cycle = session.query(MarketScanCycleModel).one()
        assert cycle.status == "FAILED"
        assert cycle.details_json["accounting_mismatch"]["scheduled_outside_eligible"] == ["OUTSIDEUSDT"]


def test_blank_scheduled_symbol_is_not_filtered_before_validation():
    result = validate_scan_accounting(scheduled_symbols=[""], symbol_results=[])
    assert result["valid"] is False
    assert result["mismatch"]["blank_scheduled_symbols"] == [0]


def test_canonical_proxy_preserves_non_dict_row_for_operational_persistence(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'malformed.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    proxy = _router(tmp_path).wrap_repository(repo)
    now = datetime.now(timezone.utc)
    result = proxy.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        scheduled_symbols=["AUSDT"], symbol_results=[None],
    )
    assert result["cycle_status"] == "FAILED"
    assert result["accounting_mismatch"]["malformed_result_rows"] == [0]
    with db.session() as session:
        cycle = session.query(MarketScanCycleModel).one()
        assert cycle.status == "FAILED"
        assert cycle.details_json["accounting_mismatch"]["malformed_result_rows"] == [0]


def test_primary_record_preserves_blank_schedule_for_accounting_failure(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'blank.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )

    now = datetime.now(timezone.utc)
    result = repo.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"], excluded=[],
        scheduled_symbols=[""], symbol_results=[],
    )

    assert result["cycle_status"] == "FAILED"
    assert result["accounting_mismatch"]["blank_scheduled_symbols"] == [0]


def test_spool_canonical_keeps_sanitized_symbol_status_and_evidence(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'spool-evidence.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    proxy = _router(tmp_path).wrap_repository(repo)
    now = datetime.now(timezone.utc)
    proxy.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        scheduled_symbols=["AUSDT"], symbol_results=[{
            "symbol": "AUSDT", "terminal_status": {"token": "sk-live-secret"},
            "reason_code": ["private", "token-abc"],
        }],
    )

    records = []
    for path in (tmp_path / "spool").rglob("*.jsonl"):
        records.extend(json.loads(line) for line in path.read_text().splitlines())
    symbol_records = [r["payload"] for r in records if r.get("dataset") == "market_scan_symbol_results"]
    assert symbol_records
    payload = symbol_records[0]
    assert payload["terminal_status"] != {"token": "sk-live-secret"}
    assert payload.get("reason_code") != ["private", "token-abc"]
    assert "sk-live-secret" not in json.dumps(payload)
    assert "token-abc" not in json.dumps(payload)


def test_primary_persistence_sanitizes_symbol_and_coverage_fields(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'primary-sanitize.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT", "BADUSDT"], eligible_symbols=["AUSDT"],
    )
    now = datetime.now(timezone.utc)
    repo.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT", "BADUSDT"], eligible_symbols=["AUSDT"],
        excluded=[("BADUSDT", {"secret": "token-abc"})],
        scheduled_symbols=["AUSDT"],
        symbol_results=[{
            "symbol": "AUSDT", "terminal_status": "SCANNED_OK",
            "reason_code": "token-abc", "details": ["private", "token-abc"],
            "extra": "must-not-persist",
        }],
        last_error={"secret": "token-abc"},
    )
    with db.session() as session:
        result = session.query(MarketScanSymbolResultModel).filter_by(symbol="AUSDT").one()
        ledger = session.query(MarketCoverageLedgerModel).filter_by(symbol="BADUSDT").one()
        cycle = session.query(MarketScanCycleModel).one()
        persisted = json.dumps({
            "result": result.__dict__, "ledger": ledger.__dict__, "cycle": cycle.__dict__,
        }, default=str)
        assert "token-abc" not in persisted
        assert result.reason_code != "token-abc"
        assert result.details_json != ["private", "token-abc"]
        assert ledger.exclusion_reason != {"secret": "token-abc"}
        assert cycle.last_error != {"secret": "token-abc"}


def test_spool_canonical_sanitizes_raw_cycle_fields_and_whitelists_result(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'spool-cycle-sanitize.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    proxy = _router(tmp_path).wrap_repository(repo)
    now = datetime.now(timezone.utc)
    proxy.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        excluded=[["AUSDT", {"secret": "token-abc"}]],
        scheduled_symbols=["AUSDT"], symbol_results=[{
            "symbol": "AUSDT", "terminal_status": "SCANNED_OK",
            "reason_code": "token-abc", "details_json": {"secret": "token-abc"},
            "extra": "must-not-persist",
        }], last_error=["token-abc"],
    )
    records = []
    for path in (tmp_path / "spool").rglob("*.jsonl"):
        records.extend(json.loads(line) for line in path.read_text().splitlines())
    payloads = [r["payload"] for r in records]
    encoded = json.dumps(payloads)
    assert "token-abc" not in encoded
    cycle = next(p for r, p in zip(records, payloads) if r["dataset"] == "market_scan_cycles")
    coverage = next(p for r, p in zip(records, payloads) if r["dataset"] == "market_coverage_ledger")
    result = next(p for r, p in zip(records, payloads) if r["dataset"] == "market_scan_symbol_results")
    assert cycle["last_error"] != ["token-abc"]
    assert coverage["excluded"][0][1] != {"secret": "token-abc"}
    assert "extra" not in result


def test_spool_canonical_propagates_raising_exclusion_diagnostic_to_operational_cycle(tmp_path):
    class RaisingExclusions:
        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError("exclusion source failed")

    db = Database(f"sqlite:///{tmp_path / 'raising-exclusions.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    proxy = _router(tmp_path).wrap_repository(repo)
    now = datetime.now(timezone.utc)

    result = proxy.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        excluded=RaisingExclusions(), scheduled_symbols=["AUSDT"],
        symbol_results=[{"symbol": "AUSDT", "terminal_status": "SCANNED_OK"}],
    )

    assert result["cycle_status"] == "FAILED"
    assert result["accounting_mismatch"] == {
        "exclusion_sanitization_mismatch": ["excluded_iterator_iteration_failed"]
    }
    with db.session() as session:
        cycle = session.query(MarketScanCycleModel).one()
        assert cycle.status == "FAILED"
        assert cycle.details_json["accounting_mismatch"] == result["accounting_mismatch"]

    records = []
    for path in (tmp_path / "spool").rglob("*.jsonl"):
        records.extend(json.loads(line) for line in path.read_text().splitlines())
    cycle_payload = next(
        record["payload"] for record in records if record["dataset"] == "market_scan_cycles"
    )
    assert cycle_payload["exclusion_sanitization_mismatch"] == [
        "excluded_iterator_iteration_failed"
    ]


def test_primary_record_sanitizes_token_like_reason_details_and_extra_without_dropping_cycle(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'primary-boundary.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    now = datetime.now(timezone.utc)
    result = repo.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        excluded=[("AUSDT", ["secret-token=abc"])],
        scheduled_symbols=["AUSDT"],
        symbol_results=[{
            "symbol": "AUSDT", "terminal_status": "SCANNED_OK",
            "reason_code": ["secret-token=abc"],
            "details": {"token": "secret-token=abc"}, "extra": {"secret": "x"},
        }],
        last_error={"token": "secret-token=abc"},
    )
    assert result is not None
    with db.session() as session:
        row = session.query(MarketScanSymbolResultModel).one()
        ledger = session.query(MarketCoverageLedgerModel).one()
        cycle = session.query(MarketScanCycleModel).one()
        assert isinstance(row.reason_code, str)
        assert isinstance(row.details_json, dict)
        persisted = json.dumps({"row": row.__dict__, "ledger": ledger.__dict__, "cycle": cycle.__dict__}, default=str)
        assert "secret-token=abc" not in persisted
        assert "extra" not in row.details_json


def test_spool_symbol_result_has_only_contract_fields(tmp_path):
    from app.market.coverage import sanitize_symbol_result

    assert set(sanitize_symbol_result({
        "symbol": "AUSDT", "terminal_status": "SCANNED_OK",
        "reason_code": "SCANNED_OK", "duration_ms": 1,
        "details": {"ok": "value"}, "extra": "omit",
    })) == {"symbol", "terminal_status", "reason_code", "duration_ms", "details"}


def test_cycle_counters_are_bounded_in_primary_and_spool_with_mismatch_evidence(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'counter-boundary.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
    )
    now = datetime.now(timezone.utc)
    proxy = _router(tmp_path).wrap_repository(repo)
    proxy.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["AUSDT"], eligible_symbols=["AUSDT"],
        scheduled_symbols=["AUSDT"], symbol_results=[{"symbol": "AUSDT", "terminal_status": "SCANNED_OK"}],
        candidate_symbols=10**30, evaluated_symbols={"bad": []},
    )
    with db.session() as session:
        cycle = session.query(MarketScanCycleModel).one()
        assert (cycle.candidate_symbols, cycle.evaluated_symbols) == (0, 0)
        assert set(cycle.details_json["accounting_mismatch"]["invalid_cycle_counters"]) == {
            "candidate_symbols", "evaluated_symbols"
        }
    payloads = [json.loads(line)["payload"] for path in (tmp_path / "spool").rglob("*.jsonl") for line in path.read_text().splitlines()]
    cycle_payload = next(p for p in payloads if "candidate_symbols" in p)
    assert (cycle_payload["candidate_symbols"], cycle_payload["evaluated_symbols"]) == (0, 0)
    assert set(cycle_payload["counter_sanitization_mismatch"]) == {"candidate_symbols", "evaluated_symbols"}


def test_primary_materializes_one_shot_exclusions_for_results_and_ledger(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'one-shot-exclusions.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["BADUSDT"], eligible_symbols=[],
    )
    now = datetime.now(timezone.utc)
    repo.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["BADUSDT"], eligible_symbols=[],
        excluded=((symbol, "LIQUIDITY_FILTER") for symbol in ["BADUSDT"]),
        scheduled_symbols=[], symbol_results=[],
    )
    with db.session() as session:
        assert session.query(MarketScanSymbolResultModel).one().reason_code == "LIQUIDITY_FILTER"
        assert session.query(MarketCoverageLedgerModel).one().exclusion_reason == "LIQUIDITY_FILTER"


def test_raising_exclusion_iterator_fails_closed_and_keeps_cycle(tmp_path):
    class RaisingIterator:
        def __iter__(self):
            raise RuntimeError("iterator exploded")

    db = Database(f"sqlite:///{tmp_path / 'raising-exclusions.sqlite'}")
    db.create_all()
    repo = BotRepository(db)
    repo.set_runtime_metadata(runtime_instance_id="runtime-1", config_fingerprint="c" * 64)
    repo.prepare_market_scan_rotation(
        rotation_started_at=datetime.now(timezone.utc),
        exchange_symbols=["BADUSDT"], eligible_symbols=[],
    )
    now = datetime.now(timezone.utc)
    result = repo.record_market_scan_cycle(
        cycle_started_at=now, cycle_completed_at=now,
        exchange_symbols=["BADUSDT"], eligible_symbols=[],
        excluded=RaisingIterator(), scheduled_symbols=[], symbol_results=[],
    )
    assert result is not None
    assert result["cycle_status"] == "FAILED"
    with db.session() as session:
        cycle = session.query(MarketScanCycleModel).one()
        assert cycle.details_json["accounting_mismatch"]["exclusion_sanitization_failed"] == [
            "excluded_iterator_conversion_failed"
        ]
