from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("pyarrow")
import pyarrow as pa
import pyarrow.parquet as pq

from app.config import AppConfig
from app.domain import EventStatus, ShortZone
from app.replay.contracts import BaselineStrategyInput, strategy_input_to_dict
from app.research.replay import build_bundle, replay_bundle, verify_bundle


def test_bundle_is_complete_and_tamper_evident(tmp_path, make_event_state, make_features) -> None:
    item = BaselineStrategyInput(
        make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE),
        make_features(),
        ShortZone(110.0, 114.0, "event_range"),
        datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
    )
    spec = {
        "schema_version": 1,
        "dataset_cutoff_utc": "2026-04-13T12:00:00Z",
        "candles": [{
            "exchange": "BYBIT", "market_type": "LINEAR", "settle_coin": "USDT", "symbol": "ONTUSDT",
            "interval": "1m", "open": 112.0, "high": 113.0, "low": 111.0, "close": 112.0, "volume": 1.0,
            "open_time_utc": "2026-04-13T11:59:00Z", "close_time_utc": "2026-04-13T12:00:00Z",
            "available_time_utc": "2026-04-13T12:00:00Z", "source": "fixture", "stable_row_id": "candle-1",
        }],
        "inputs": [strategy_input_to_dict(item)],
    }

    path = build_bundle(spec, AppConfig(), tmp_path, run_id="fixture")

    assert (path / "COMPLETE").is_file()
    assert verify_bundle(path).valid
    hashes = __import__("json").loads((path / "hashes.json").read_text(encoding="utf-8"))
    manifest = __import__("json").loads((path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["lifecycle_status"] == "COMPLETE"
    assert manifest["status"] == "COMPLETE"
    assert manifest["canonical_status"] == "CANONICAL"
    assert manifest["sanitized_config_path"] == "config/config_sanitized.yaml"
    assert manifest["dataset_epoch"] is None
    assert len(manifest["strategy_contract_version"]) == 64
    contract = __import__("json").loads((path / "config/strategy_contract.json").read_text(encoding="utf-8"))
    assert "LOW_VOLUME_EXTENSION_FAILURE_V2" in contract["strategies"]
    assert hashes["artifacts"]["market_data/candles_1m.parquet"]["row_count"] == 1
    assert hashes["artifacts"]["market_data/candles_1m.parquet"]["logical_sha256"]
    decision_row = __import__("pyarrow.parquet", fromlist=["read_table"]).read_table(path / "decisions/strategy_decisions.parquet").to_pylist()[0]
    decision_payload = __import__("json").loads(decision_row["payload_json"])
    assert len(decision_payload["input_fingerprint"]) == 64
    assert len(decision_payload["decision_fingerprint"]) == 64
    assert replay_bundle(path).matches_recorded_decisions
    (path / "report.md").write_text("tampered", encoding="utf-8")
    assert not verify_bundle(path).valid


@pytest.mark.parametrize("artifact", ["asof_candles", "baseline_transitions"])
def test_optional_artifacts_reject_future_cutoff_rows(tmp_path, artifact):
    row = (
        {"exchange": "BYBIT", "market_type": "LINEAR", "settle_coin": "USDT", "symbol": "ONTUSDT", "interval": "1m", "open_time_utc": "2026-04-13T12:00:00Z", "close_time_utc": "2026-04-13T12:01:00Z", "availability_time_utc": "2026-04-13T12:01:01Z", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0, "stable_row_id": "future"}
        if artifact == "asof_candles"
        else {"availability_time_utc": "2026-04-13T12:01:01Z", "sequence": 0, "symbol": "ONTUSDT", "event_type": "STATE_TRANSITION", "stable_event_id": "future"}
    )
    spec = {"schema_version": 1, "dataset_cutoff_utc": "2026-04-13T12:00:00Z", "candles": [], artifact: [row]}
    with pytest.raises(ValueError, match="after dataset cutoff"):
        build_bundle(spec, AppConfig(), tmp_path, run_id=f"future-{artifact}", allow_dirty=True)


def test_optional_artifact_tamper_is_rejected_by_verifier(tmp_path):
    candle = {"exchange": "BYBIT", "market_type": "LINEAR", "settle_coin": "USDT", "symbol": "ONTUSDT", "interval": "1m", "open_time_utc": "2026-04-13T11:59:00Z", "close_time_utc": "2026-04-13T12:00:00Z", "availability_time_utc": "2026-04-13T12:00:00Z", "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0, "stable_row_id": "ok"}
    spec = {"schema_version": 1, "dataset_cutoff_utc": "2026-04-13T12:00:00Z", "candles": [], "asof_candles": [candle], "baseline_transitions": [{"availability_time_utc": "2026-04-13T12:00:00Z", "sequence": 0, "symbol": "ONTUSDT", "event_type": "STATE_TRANSITION", "stable_event_id": "ok"}]}
    path = build_bundle(spec, AppConfig(), tmp_path, run_id="optional-tamper", allow_dirty=True)
    table = pq.read_table(path / "market_data/asof_candles_1m.parquet").to_pydict()
    table["availability_time_utc"][0] = "2026-04-13T12:00:01Z"
    pq.write_table(pa.Table.from_pydict(table), path / "market_data/asof_candles_1m.parquet")
    report = verify_bundle(path)
    assert not report.valid
    assert "hash-mismatch:market_data/asof_candles_1m.parquet" in report.errors
    assert "asof-market-row-after-cutoff" in report.errors


def test_replay_validation_binds_decisions_by_input_id(tmp_path, make_event_state, make_features):
    from app.research.replay.bundle import _validate_replay_rows
    import json
    import pyarrow.parquet as pq
    import pyarrow as pa

    decision = datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc)
    items = [
        BaselineStrategyInput(
            make_event_state(event_id=f"event-{index}", state=EventStatus.SHORT_ZONE_ACTIVE),
            make_features(symbol=f"COIN{index}USDT", asof=decision),
            ShortZone(110.0, 114.0, "event_range"),
            decision,
        )
        for index in (1, 2)
    ]
    spec = {
        "schema_version": 1,
        "dataset_cutoff_utc": "2026-04-13T12:00:00Z",
        "candles": [],
        "inputs": [strategy_input_to_dict(item) for item in items],
    }
    path = build_bundle(spec, AppConfig(), tmp_path, run_id="binding", allow_dirty=True)
    decision_path = path / "decisions/strategy_decisions.parquet"
    table = pq.read_table(decision_path)
    rows = table.to_pylist()
    payloads = [row["payload_json"] for row in rows]
    for row, payload in zip(rows, payloads[::-1], strict=True):
        row["payload_json"] = payload
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), decision_path)
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    errors = []
    _validate_replay_rows(path, manifest, errors)
    assert any(error.startswith("decision-identity-mismatch:") for error in errors)


def test_dirty_source_refuses_canonical_bundle(monkeypatch, tmp_path):
    from app.research.replay import bundle as bundle_module

    monkeypatch.setattr(bundle_module, "_git_dirty", lambda: True)
    with pytest.raises(RuntimeError, match="dirty source"):
        build_bundle({"schema_version": 1, "dataset_cutoff_utc": "2026-04-13T12:00:00Z"}, AppConfig(), tmp_path, run_id="dirty")


def test_phase4_bundle_seals_asof_transition_and_evaluation_alias(tmp_path, make_event_state, make_features):
    decision = datetime(2026, 8, 22, 4, 22, 57, tzinfo=timezone.utc)
    item = BaselineStrategyInput(make_event_state(state=EventStatus.SHORT_ZONE_ACTIVE), make_features(asof=decision), ShortZone(110, 114, "event_range"), decision)
    payload = strategy_input_to_dict(item)
    payload["max_input_availability_time_utc"] = "2026-08-22T04:22:56Z"
    candle = {"exchange": "BYBIT", "market_type": "LINEAR", "settle_coin": "USDT", "symbol": "TRUMPUSDT", "interval": "1m", "open_time_utc": "2026-08-22T04:22:00Z", "close_time_utc": "2026-08-22T04:23:00Z", "availability_time_utc": "2026-08-22T04:22:56Z", "open": 2.7, "high": 2.71, "low": 2.69, "close": 2.71, "volume": 3.0, "stable_row_id": "trump:0422"}
    spec = {"schema_version": 1, "dataset_cutoff_utc": "2026-08-22T04:23:00Z", "candles": [candle], "asof_candles": [{**candle, "stable_row_id": "trump:0422:partial"}], "inputs": [payload], "evaluation_records": True, "baseline_transitions": [{"availability_time_utc": "2026-08-22T04:22:57Z", "sequence": 0, "symbol": "TRUMPUSDT", "event_type": "STATE_TRANSITION", "stable_event_id": "event-1"}]}
    path = build_bundle(spec, AppConfig(), tmp_path, run_id="phase4-alias", allow_dirty=True)
    assert verify_bundle(path).valid
    manifest = __import__("json").loads((path / "manifest.json").read_text())
    assert "market_data/asof_candles_1m.parquet" in manifest["artifacts"]
    assert "state/baseline_transitions.parquet" in manifest["artifacts"]
    assert "inputs/evaluation_records.parquet" in manifest["artifacts"]
