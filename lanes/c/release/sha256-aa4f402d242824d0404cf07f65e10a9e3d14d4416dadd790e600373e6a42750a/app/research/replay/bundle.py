"""Research-side immutable ReplayBundle implementation using PyArrow only."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from app.config import AppConfig
from app.replay.canonical import canonical_json_bytes, canonicalize, sha256_canonical, utc_iso_z
from app.replay.config import strategy_config_fingerprint, strategy_config_payload
from app.replay.contracts import ClimaxStrategyInput, StrategyInput, strategy_input_from_dict
from app.replay.evaluator import evaluate_strategy_input


BUNDLE_VERSION = 1
PAYLOAD_FILES = (
    "config/config_sanitized.yaml", "config/strategy_contract.json", "market_data/candles_1m.parquet",
    "inputs/strategy_inputs.parquet", "decisions/strategy_decisions.parquet", "outcomes/outcomes.parquet",
    "evidence/provenance.parquet", "report.json", "report.md",
)
REQUIRED_FILES = ("manifest.json", "hashes.json", "COMPLETE", *PAYLOAD_FILES)


def _payload_files(root: Path | None = None, manifest: Mapping[str, Any] | None = None) -> tuple[str, ...]:
    """Return required payloads plus any explicitly requested optional state files."""

    if manifest is not None:
        listed = manifest.get("artifacts")
        if isinstance(listed, list) and all(isinstance(item, str) for item in listed):
            return tuple(listed)
    optional: list[str] = []
    if root is not None:
        for relative in (
            "state/initial_state.json", "state/state_transitions.parquet",
            "state/baseline_transitions.parquet", "market_data/asof_candles_1m.parquet",
            "inputs/evaluation_records.parquet",
        ):
            if (root / relative).is_file():
                optional.append(relative)
    return (*PAYLOAD_FILES, *optional)


@dataclass(frozen=True, slots=True)
class VerificationReport:
    valid: bool
    errors: tuple[str, ...]
    payload_hashes: dict[str, str]


@dataclass(frozen=True, slots=True)
class ReplayResult:
    verified: VerificationReport
    decisions: tuple[dict[str, Any], ...]
    matches_recorded_decisions: bool


@dataclass(frozen=True, slots=True)
class Report:
    data: dict[str, Any]
    markdown: str


def build_bundle(spec: Mapping[str, Any] | str | Path, config: AppConfig | Mapping[str, Any], output_root: str | Path, run_id: str | None = None, allow_dirty: bool = False) -> Path:
    """Build, verify, seal and atomically publish a new ReplayBundle directory."""

    source_spec = _load_spec(spec)
    config_obj = config if isinstance(config, AppConfig) else AppConfig.model_validate(config)
    run = run_id or str(source_spec.get("run_id") or _utc_now_run_id(source_spec, config_obj))
    if not run or Path(run).name != run:
        raise ValueError("run_id must be one plain directory name")
    root = Path(output_root)
    final = root / run
    if final.exists():
        raise FileExistsError(f"replay bundle already exists: {final}")
    _validate_spec(source_spec)
    dirty = _git_dirty()
    if dirty and not allow_dirty:
        raise RuntimeError("refusing canonical replay bundle from a dirty source; pass allow_dirty to record NON_CANONICAL")

    root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{run}.staging-", dir=root))
    try:
        _write_payload(stage, source_spec, config_obj, run_id=run, dirty=dirty)
        _seal(stage, source_spec, config_obj, run_id=run, dirty=dirty)
        verification = verify_bundle(stage)
        if not verification.valid:
            raise RuntimeError(f"staged bundle verification failed: {verification.errors}")
        if final.exists():
            raise FileExistsError(f"replay bundle already exists: {final}")
        os.replace(stage, final)
        return final
    except Exception:
        # Preserve failed staging evidence for diagnosis; it never gets COMPLETE.
        if stage.exists():
            _write_json(stage / "FAILED", {"status": "FAILED", "lifecycle_status": "FAILED"})
        raise


def verify_bundle(path: str | Path) -> VerificationReport:
    """Validate the complete marker, manifest seal and physical payload hashes."""

    root = Path(path)
    errors: list[str] = []
    for name in REQUIRED_FILES:
        if not (root / name).is_file():
            errors.append(f"missing:{name}")
    if errors:
        return VerificationReport(False, tuple(errors), {})
    try:
        hashes = _read_json(root / "hashes.json")
        manifest = _read_json(root / "manifest.json")
        complete = _read_json(root / "COMPLETE")
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        return VerificationReport(False, (f"invalid-seal:{exc}",), {})
    payload_hashes = hashes.get("payload_sha256")
    if not isinstance(payload_hashes, dict):
        errors.append("invalid:hashes.payload_sha256")
        payload_hashes = {}
    manifest_artifacts = manifest.get("artifacts") if isinstance(manifest, dict) else None
    payload_files = tuple(manifest_artifacts) if isinstance(manifest_artifacts, list) and all(isinstance(item, str) for item in manifest_artifacts) else PAYLOAD_FILES
    for relative in payload_files:
        expected = payload_hashes.get(relative)
        actual = _sha256_file(root / relative)
        if expected != actual:
            errors.append(f"hash-mismatch:{relative}")
    if hashes.get("manifest_sha256") != _sha256_file(root / "manifest.json"):
        errors.append("hash-mismatch:manifest.json")
    if complete.get("manifest_sha256") != _sha256_file(root / "manifest.json"):
        errors.append("complete-mismatch:manifest.json")
    if complete.get("hashes_sha256") != _sha256_file(root / "hashes.json"):
        errors.append("complete-mismatch:hashes.json")
    if manifest.get("payload_sha256") != payload_hashes:
        errors.append("manifest-hash-list-mismatch")
    artifacts = hashes.get("artifacts")
    if not isinstance(artifacts, dict) or set(artifacts) != set(payload_files) or not set(PAYLOAD_FILES).issubset(set(payload_files)):
        errors.append("invalid:artifact-metadata")
    else:
        if manifest.get("artifact_hashes") != artifacts:
            errors.append("manifest-artifact-metadata-mismatch")
        for relative, metadata in artifacts.items():
            if metadata.get("physical_sha256") != payload_hashes.get(relative):
                errors.append(f"artifact-physical-mismatch:{relative}")
            if not metadata.get("logical_sha256") or "row_count" not in metadata or "schema_sha256" not in metadata:
                errors.append(f"artifact-logical-metadata-missing:{relative}")
                continue
            actual_metadata = _artifact_metadata(root / relative)
            for field in ("logical_sha256", "schema_sha256", "row_count"):
                if metadata.get(field) != actual_metadata[field]:
                    errors.append(f"artifact-logical-mismatch:{relative}:{field}")
        if isinstance(artifacts, dict):
            actual_logical_dataset = sha256_canonical({relative: metadata.get("logical_sha256") for relative, metadata in artifacts.items()})
            if manifest.get("logical_dataset_sha256") != actual_logical_dataset:
                errors.append("logical-dataset-digest-mismatch")
    required_manifest = {"bundle_version", "run_id", "created_at_utc", "code_sha", "replay_code_sha", "source_live_code_sha", "status", "lifecycle_status", "canonical_status", "dataset_cutoff_utc", "dataset_epoch", "config_hash", "strategy_config_sha256", "sanitized_config_path", "strategy_contract_version", "universe", "sources", "source", "config", "data", "schema_versions", "row_counts", "semantic_identity", "logical_dataset_sha256", "artifacts", "artifact_hashes"}
    if required_manifest - set(manifest):
        errors.append("invalid:manifest-required-fields")
    if manifest.get("bundle_version") != BUNDLE_VERSION or complete.get("bundle_version") != BUNDLE_VERSION:
        errors.append("invalid:bundle-version")
    try:
        _parse_utc(manifest.get("created_at_utc"))
    except (TypeError, ValueError):
        errors.append("invalid:manifest-created-at")
    if manifest.get("status") != "COMPLETE" or manifest.get("lifecycle_status") != "COMPLETE" or complete.get("status") != "COMPLETE" or complete.get("lifecycle_status") != "COMPLETE":
        errors.append("invalid:lifecycle-status")
    if complete.get("canonical_status") != manifest.get("canonical_status") or complete.get("dataset_cutoff_utc") != manifest.get("dataset_cutoff_utc"):
        errors.append("complete-metadata-mismatch")
    if complete.get("strategy_config_sha256") != manifest.get("strategy_config_sha256"):
        errors.append("complete-config-fingerprint-mismatch")
    if manifest.get("config_hash") != manifest.get("strategy_config_sha256"):
        errors.append("manifest-config-fingerprint-mismatch")
    try:
        _parse_utc(manifest.get("dataset_cutoff_utc"))
        if not isinstance(manifest.get("source_live_code_sha"), (str, type(None))):
            errors.append("invalid:source-live-code-sha")
        if not isinstance(manifest.get("row_counts"), dict) or not isinstance(manifest.get("schema_versions"), dict):
            errors.append("invalid:manifest-artifact-indexes")
        config = AppConfig.model_validate(yaml.safe_load((root / "config/config_sanitized.yaml").read_text(encoding="utf-8")))
        if strategy_config_fingerprint(config) != manifest.get("config_hash"):
            errors.append("config-fingerprint-mismatch")
        contract = _read_json(root / "config/strategy_contract.json")
        contract_body = {key: value for key, value in contract.items() if key != "strategy_contract_version"}
        if contract.get("strategy_contract_version") != sha256_canonical(contract_body):
            errors.append("strategy-contract-version-mismatch")
        if contract.get("strategy_config_sha256") != manifest.get("config_hash") or contract.get("strategy_contract_version") != manifest.get("strategy_contract_version"):
            errors.append("strategy-contract-mismatch")
        if set(contract.get("strategies", [])) != {"BASELINE_PULLBACK", "VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"}:
            errors.append("strategy-contract-strategy-set-mismatch")
        if contract.get("grade_c_public") is not False or contract.get("autoexecution") != "OFF" or contract.get("root_detector_shadow_v2") != "RESEARCH_ONLY_NOT_STRATEGY":
            errors.append("strategy-contract-invariants-mismatch")
        _validate_replay_rows(root, manifest, errors)
    except (OSError, TypeError, ValueError, yaml.YAMLError) as exc:
        errors.append(f"invalid:config-contract:{exc}")
    return VerificationReport(not errors, tuple(errors), dict(payload_hashes))


def replay_bundle(path: str | Path) -> ReplayResult:
    """Replay sealed inputs locally; this never opens a repository or network client."""

    root = Path(path)
    verified = verify_bundle(root)
    if not verified.valid:
        return ReplayResult(verified, (), False)
    config = AppConfig.model_validate(yaml.safe_load((root / "config/config_sanitized.yaml").read_text(encoding="utf-8")))
    candles = pq.read_table(root / "market_data/candles_1m.parquet").to_pandas()
    input_rows = pq.read_table(root / "inputs/strategy_inputs.parquet").to_pylist()
    manifest = _read_json(root / "manifest.json")
    decisions = [_decision_record(json.loads(row["payload_json"]), candles, config, run_id=str(manifest["run_id"]), strategy_contract_version=str(manifest["strategy_contract_version"]), code_sha=str(manifest.get("replay_code_sha") or manifest.get("code_sha") or "UNKNOWN")) for row in input_rows]
    expected = pq.read_table(root / "decisions/strategy_decisions.parquet").to_pylist()
    expected_payloads = [json.loads(row["payload_json"]) for row in expected]
    return ReplayResult(verified, tuple(decisions), decisions == expected_payloads)


def generate_report(path: str | Path) -> Report:
    """Read the sealed report; no bundle artifact is modified during reporting."""

    root = Path(path)
    return Report(_read_json(root / "report.json"), (root / "report.md").read_text(encoding="utf-8"))


def _write_payload(root: Path, spec: Mapping[str, Any], config: AppConfig, *, run_id: str, dirty: bool) -> None:
    for relative in PAYLOAD_FILES:
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
    if "initial_state" in spec or "state" in spec or spec.get("state_transitions") or spec.get("baseline_transitions"):
        (root / "state").mkdir(parents=True, exist_ok=True)
    strategy_payload = strategy_config_payload(config)
    (root / "config/config_sanitized.yaml").write_text(yaml.safe_dump(strategy_payload, sort_keys=True, allow_unicode=True), encoding="utf-8")
    contract_body = {
        "bundle_version": BUNDLE_VERSION,
        "strategies": ["BASELINE_PULLBACK", "VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"],
        "grade_c_public": False,
        "autoexecution": "OFF",
        "root_detector_shadow_v2": "RESEARCH_ONLY_NOT_STRATEGY",
        "strategy_config_sha256": strategy_config_fingerprint(config),
        "canonical_json": "UTF-8 sorted keys compact separators; timestamps UTC Z; finite floats .15g",
    }
    contract = {**contract_body, "strategy_contract_version": sha256_canonical(contract_body)}
    _write_json(root / "config/strategy_contract.json", contract)
    candles = [_normalize_candle(row) for row in spec.get("candles", [])]
    _write_parquet(root / "market_data/candles_1m.parquet", candles)
    if "asof_candles" in spec:
        _write_parquet(root / "market_data/asof_candles_1m.parquet", [_normalize_candle(row) for row in spec.get("asof_candles", [])])
    inputs = sorted(
        (canonicalize(row) for row in spec.get("inputs", [])),
        key=lambda row: (
            str(row.get("decision_time_utc", "")),
            str(row.get("features", {}).get("symbol", "")),
            str(row.get("strategy", "")),
            sha256_canonical(row),
        ),
    )
    rows = []
    decision_rows = []
    candle_frame = pd.DataFrame(candles)
    for index, payload in enumerate(inputs):
        input_id = f"input-{sha256_canonical(payload)[:24]}"
        rows.append({"input_id": input_id, "strategy": payload["strategy"], "symbol": payload["features"]["symbol"], "decision_time_utc": payload["decision_time_utc"], "payload_json": canonical_json_bytes(payload).decode("utf-8")})
        decision_rows.append({"input_id": input_id, "payload_json": canonical_json_bytes(_decision_record(payload, candle_frame, config, run_id=run_id, strategy_contract_version=contract["strategy_contract_version"])).decode("utf-8")})
    _write_parquet(root / "inputs/strategy_inputs.parquet", rows)
    if "evaluation_records" in spec:
        _write_parquet(root / "inputs/evaluation_records.parquet", rows)
    _write_parquet(root / "decisions/strategy_decisions.parquet", decision_rows)
    _write_outcomes(root / "outcomes/outcomes.parquet", list(spec.get("outcomes", [])))
    if "initial_state" in spec or "state" in spec:
        _write_json(root / "state/initial_state.json", spec.get("initial_state", spec.get("state")))
    if spec.get("state_transitions"):
        transitions = sorted((canonicalize(row) for row in spec["state_transitions"]), key=_event_order_key)
        _write_state_transitions(root / "state/state_transitions.parquet", transitions)
    if spec.get("baseline_transitions"):
        transitions = sorted((canonicalize(row) for row in spec["baseline_transitions"]), key=_event_order_key)
        _write_state_transitions(root / "state/baseline_transitions.parquet", transitions)
    provenance = {
        "dataset_cutoff_utc": spec["dataset_cutoff_utc"], "source": spec.get("source", "spec"), "dirty": dirty,
        "source_status_sha256": _git_status_hash() if dirty else None,
        "source_diff_sha256": _git_diff_hash() if dirty else None,
    }
    _write_provenance(root / "evidence/provenance.parquet", [{**provenance, **row} for row in spec.get("provenance", [{}])])
    report = {"bundle_version": BUNDLE_VERSION, "dataset_cutoff_utc": spec["dataset_cutoff_utc"], "provenance": provenance, "counts": {"candles": len(candles), "inputs": len(inputs), "decisions": len(decision_rows), "outcomes": len(spec.get("outcomes", []))}, "strategies": _strategy_counts(decision_rows), "actionable_decisions": _actionable_count(decision_rows), "grades": _grade_counts(decision_rows), "verification": "COMPLETE", "determinism": "ordered canonical inputs and physical/logical hashes", "strategy_quality_verdict": None}
    _write_json(root / "report.json", report)
    (root / "report.md").write_text(_report_markdown(report), encoding="utf-8")


def _seal(root: Path, spec: Mapping[str, Any], config: AppConfig, *, run_id: str, dirty: bool) -> None:
    payload_files = _payload_files(root)
    artifact_hashes = {relative: _artifact_metadata(root / relative) for relative in payload_files}
    payload_hashes = {relative: metadata["physical_sha256"] for relative, metadata in artifact_hashes.items()}
    canonical_status = "NON_CANONICAL" if dirty else "CANONICAL"
    fingerprint = strategy_config_fingerprint(config)
    code_sha = _git_code_sha()
    row_counts = {relative: metadata["row_count"] for relative, metadata in artifact_hashes.items()}
    schema_versions = {relative: 1 for relative in payload_files}
    semantic_identity = {
        "input_logical_sha256": artifact_hashes["inputs/strategy_inputs.parquet"]["logical_sha256"],
        "decision_logical_sha256": artifact_hashes["decisions/strategy_decisions.parquet"]["logical_sha256"],
        "report_logical_sha256": artifact_hashes["report.json"]["logical_sha256"],
    }
    logical_dataset_sha256 = sha256_canonical({relative: metadata["logical_sha256"] for relative, metadata in artifact_hashes.items()})
    manifest = {
        "bundle_version": BUNDLE_VERSION, "run_id": run_id,
        "created_at_utc": str(spec.get("created_at_utc") or _utc_now_iso()), "code_sha": code_sha, "replay_code_sha": code_sha, "source_live_code_sha": spec.get("source_live_code_sha"),
        "status": "COMPLETE", "lifecycle_status": "COMPLETE", "canonical_status": canonical_status, "dirty": dirty,
        "dataset_cutoff_utc": spec["dataset_cutoff_utc"], "dataset_epoch": spec.get("dataset_epoch"),
        "config_hash": fingerprint, "strategy_config_sha256": fingerprint,
        "sanitized_config_path": "config/config_sanitized.yaml", "strategy_contract_version": _read_json(root / "config/strategy_contract.json")["strategy_contract_version"],
        "source": spec.get("source", "spec"), "sources": canonicalize(spec.get("sources", [spec.get("source", "spec")])), "source_status_sha256": _git_status_hash() if dirty else None, "source_diff_sha256": _git_diff_hash() if dirty else None,
        "config": {"sanitized_path": "config/config_sanitized.yaml", "strategy_config_sha256": fingerprint},
        "data": {"market_data_path": "market_data/candles_1m.parquet", "dataset_cutoff_utc": spec["dataset_cutoff_utc"], "dataset_epoch": spec.get("dataset_epoch")},
        "universe": sorted({str(row["symbol"]) for row in spec.get("candles", [])}),
        "schema_versions": schema_versions, "row_counts": row_counts, "semantic_identity": semantic_identity, "logical_dataset_sha256": logical_dataset_sha256,
        "artifacts": list(payload_files), "artifact_hashes": artifact_hashes, "payload_sha256": payload_hashes,
    }
    _write_json(root / "manifest.json", manifest)
    hashes = {"algorithm": "sha256", "payload_sha256": payload_hashes, "artifacts": artifact_hashes, "manifest_sha256": _sha256_file(root / "manifest.json")}
    _write_json(root / "hashes.json", hashes)
    _write_json(root / "COMPLETE", {"bundle_version": BUNDLE_VERSION, "status": "COMPLETE", "lifecycle_status": "COMPLETE", "canonical_status": canonical_status, "dataset_cutoff_utc": spec["dataset_cutoff_utc"], "strategy_config_sha256": fingerprint, "manifest_sha256": _sha256_file(root / "manifest.json"), "hashes_sha256": _sha256_file(root / "hashes.json")})


def _validate_spec(spec: Mapping[str, Any]) -> None:
    if int(spec.get("schema_version", BUNDLE_VERSION)) != BUNDLE_VERSION:
        raise ValueError("unsupported ReplayBundle spec schema_version")
    cutoff = _parse_utc(spec.get("dataset_cutoff_utc"))
    seen_candles: set[str] = set()
    for row in spec.get("candles", []):
        if (row.get("exchange"), row.get("market_type"), row.get("settle_coin")) != ("BYBIT", "LINEAR", "USDT"):
            raise ValueError("ReplayBundle market data must be Bybit Linear USDT perpetual")
        if row.get("interval") != "1m":
            raise ValueError("ReplayBundle v1 accepts only 1m candles")
        identity = str(row.get("stable_row_id", ""))
        if not identity or identity in seen_candles:
            raise ValueError("candles require unique stable_row_id")
        seen_candles.add(identity)
        availability = row.get("availability_time_utc", row.get("available_at_utc", row.get("available_time_utc")))
        if _parse_utc(availability) > cutoff:
            raise ValueError("market row available after dataset cutoff")
    ordering: set[tuple[str, int, str, str, str]] = set()
    for row in [*spec.get("provenance", []), *spec.get("state_transitions", [])]:
        key = _event_order_key(row)
        if key in ordering:
            raise ValueError("duplicate full event ordering key")
        ordering.add(key)
        if row.get("availability_time_utc", row.get("available_at_utc")) is not None and _parse_utc(row.get("availability_time_utc", row.get("available_at_utc"))) > cutoff:
            raise ValueError("event available after dataset cutoff")
    for payload in spec.get("inputs", []):
        decision_time = _parse_utc(payload.get("decision_time_utc"))
        if decision_time > cutoff:
            raise ValueError("input decision time after dataset cutoff")
        features_asof = _parse_utc(payload["features"]["asof"])
        if features_asof > decision_time:
            raise ValueError("input features are not available at decision time")
        max_availability = payload.get("max_input_availability_time_utc")
        if max_availability is not None and _parse_utc(max_availability) > decision_time:
            raise ValueError("input availability is after decision time")


def _evaluate_record(payload: Mapping[str, Any], candles: pd.DataFrame, config: AppConfig) -> dict[str, Any]:
    item: StrategyInput = strategy_input_from_dict(payload)
    if isinstance(item, ClimaxStrategyInput):
        availability_column = "availability_time_utc" if "availability_time_utc" in candles.columns else "available_at_utc"
        frame = candles[(candles["symbol"] == item.features.symbol) & (pd.to_datetime(candles[availability_column], utc=True) <= pd.Timestamp(item.decision_time_utc))].copy()
        references = item.frame_ref
        if isinstance(references, str):
            references = [] if references in {"", "all", "*"} else [references]
        if references:
            frame = frame[frame["stable_row_id"].isin([str(value) for value in references])].copy()
        item = ClimaxStrategyInput(item.state, item.features, frame, item.decision_time_utc, item.strategy)
    return _result_payload(evaluate_strategy_input(item, config))


def _decision_record(
    payload: Mapping[str, Any],
    candles: pd.DataFrame,
    config: AppConfig,
    *,
    run_id: str,
    strategy_contract_version: str | None = None,
    code_sha: str | None = None,
) -> dict[str, Any]:
    """Wrap the evaluator result with stable replay provenance and fingerprints."""

    result = _evaluate_record(payload, candles, config)
    strategy = str(payload["strategy"])
    symbol = str(payload["features"]["symbol"])
    decision_time = str(payload["decision_time_utc"])
    contract_version = strategy_contract_version or sha256_canonical({
        "bundle_version": BUNDLE_VERSION,
        "strategies": ["BASELINE_PULLBACK", "VOLUME_CLIMAX_UNWIND", "LOW_VOLUME_EXTENSION_FAILURE"],
        "grade_c_public": False,
        "autoexecution": "OFF",
        "root_detector_shadow_v2": "RESEARCH_ONLY_NOT_STRATEGY",
        "strategy_config_sha256": strategy_config_fingerprint(config),
        "canonical_json": "UTF-8 sorted keys compact separators; timestamps UTC Z; finite floats .15g",
    })
    if strategy == "BASELINE_PULLBACK":
        actionable = bool((result.get("decision") or {}).get("actionable")) if result.get("decision") else False
        score = result.get("score")
        grade = result.get("grade")
        blockers = list(result.get("blockers") or [])
        warnings = list(result.get("data_quality_warnings") or [])
        selected_for_live_delivery = actionable
    else:
        selected = result.get("selected") or {}
        actionable = bool(selected.get("actionable"))
        score = selected.get("score")
        grade = selected.get("grade")
        blockers = list(selected.get("veto_reasons") or [])
        warnings = list(selected.get("data_quality") or [])
        selected_for_live_delivery = actionable and str(selected.get("subtype")) == strategy
    return {
        "run_id": run_id,
        "strategy": strategy,
        "symbol": symbol,
        "decision_time_utc": decision_time,
        "actionable": actionable,
        "selected_for_live_delivery": selected_for_live_delivery,
        "score": score,
        "grade": grade,
        "blockers": blockers,
        "warnings": warnings,
        "input_fingerprint": sha256_canonical(payload),
        "decision_fingerprint": sha256_canonical(result),
        "code_sha": code_sha or _git_code_sha(),
        "config_hash": strategy_config_fingerprint(config),
        "strategy_contract_version": contract_version,
        "max_input_availability_time_utc": payload.get("max_input_availability_time_utc"),
        "event_id": payload.get("event_id", payload.get("state", {}).get("event_id")),
        "context_identity": payload.get("context_identity"),
        "observability_flags": list(payload.get("observability_flags", [])),
        "result": result,
    }


def _validate_replay_rows(root: Path, manifest: Mapping[str, Any], errors: list[str]) -> None:
    """Validate cutoff, fingerprints, identities and UTC for input/decision rows."""

    try:
        inputs = pq.read_table(root / "inputs/strategy_inputs.parquet").to_pylist()
        decisions = pq.read_table(root / "decisions/strategy_decisions.parquet").to_pylist()
        candles = pq.read_table(root / "market_data/candles_1m.parquet").to_pandas()
        cutoff = _parse_utc(manifest["dataset_cutoff_utc"])
        if len(inputs) != len(decisions):
            errors.append("row-count-mismatch:inputs-decisions")
        seen: set[str] = set()
        for input_row, decision_row in zip(inputs, decisions, strict=False):
            input_id = str(input_row.get("input_id"))
            if input_id in seen:
                errors.append(f"duplicate-input-id:{input_id}")
            seen.add(input_id)
            payload = json.loads(input_row["payload_json"])
            decision_time = _parse_utc(payload["decision_time_utc"])
            if decision_time > cutoff:
                errors.append(f"input-after-cutoff:{input_id}")
            if _parse_utc(payload["features"]["asof"]) > decision_time:
                errors.append(f"feature-after-decision:{input_id}")
            max_availability = payload.get("max_input_availability_time_utc")
            if max_availability is not None and _parse_utc(max_availability) > decision_time:
                errors.append(f"input-availability-after-decision:{input_id}")
            decision = json.loads(decision_row["payload_json"])
            if decision.get("strategy") != payload.get("strategy") or decision.get("symbol") != payload.get("features", {}).get("symbol"):
                errors.append(f"decision-identity-mismatch:{input_id}")
            if decision.get("config_hash") != manifest.get("config_hash") or decision.get("strategy_contract_version") != manifest.get("strategy_contract_version"):
                errors.append(f"decision-provenance-mismatch:{input_id}")
            if decision.get("input_fingerprint") != sha256_canonical(payload):
                errors.append(f"input-fingerprint-mismatch:{input_id}")
            result = decision.get("result")
            if decision.get("decision_fingerprint") != sha256_canonical(result):
                errors.append(f"decision-fingerprint-mismatch:{input_id}")
        availability_column = "availability_time_utc" if "availability_time_utc" in candles.columns else "available_at_utc"
        if not candles.empty and (pd.to_datetime(candles[availability_column], utc=True) > pd.Timestamp(cutoff)).any():
            errors.append("market-row-after-cutoff")
    except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        errors.append(f"invalid:replay-rows:{exc}")


def _result_payload(value: Any) -> dict[str, Any]:
    if is_dataclass(value):
        value = asdict(value)
    return canonicalize(value)


def _event_order_key(row: Mapping[str, Any]) -> tuple[str, int, str, str, str]:
    return (
        str(row.get("availability_time_utc", row.get("available_at_utc", ""))),
        int(row.get("sequence", 0)),
        str(row.get("symbol", "")),
        str(row.get("event_type", "")),
        str(row.get("stable_event_id", "")),
    )


def _write_outcomes(path: Path, outcomes: list[Mapping[str, Any]]) -> None:
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        payload = canonicalize(outcome)
        rows.append({
            "decision_identity": str(outcome.get("decision_identity", outcome.get("decision_id", ""))),
            "horizon": str(outcome.get("horizon", "")),
            "status": str(outcome.get("status", "")),
            "mfe": outcome.get("mfe", outcome.get("MFE")),
            "mae": outcome.get("mae", outcome.get("MAE")),
            "new_high": outcome.get("new_high", outcome.get("new_high_after_observation")),
            "mature_at_utc": outcome.get("mature_at_utc", outcome.get("mature_at")),
            "payload_json": canonical_json_bytes(payload).decode("utf-8"),
        })
    _write_parquet(path, rows)


def _write_provenance(path: Path, rows: list[Mapping[str, Any]]) -> None:
    normalized = []
    for row in sorted((canonicalize(item) for item in rows), key=_event_order_key):
        normalized.append({
            "availability_time_utc": row.get("availability_time_utc", row.get("available_at_utc")),
            "sequence": int(row.get("sequence", 0)),
            "symbol": str(row.get("symbol", "")),
            "event_type": str(row.get("event_type", "")),
            "stable_event_id": str(row.get("stable_event_id", "")),
            "source": str(row.get("source", "spec")),
            "dirty": bool(row.get("dirty", False)),
            "source_status_sha256": row.get("source_status_sha256"),
            "source_diff_sha256": row.get("source_diff_sha256"),
            "payload_json": canonical_json_bytes(row).decode("utf-8"),
        })
    _write_parquet(path, normalized)


def _write_state_transitions(path: Path, rows: list[Mapping[str, Any]]) -> None:
    normalized = [{
        "availability_time_utc": row.get("availability_time_utc", row.get("available_at_utc")),
        "sequence": int(row.get("sequence", 0)),
        "symbol": str(row.get("symbol", "")),
        "event_type": str(row.get("event_type", "STATE_TRANSITION")),
        "stable_event_id": str(row.get("stable_event_id", "")),
        "payload_json": canonical_json_bytes(row).decode("utf-8"),
    } for row in rows]
    _write_parquet(path, normalized)


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    if path.name in {"candles_1m.parquet", "asof_candles_1m.parquet"}:
        schema = pa.schema([(name, pa.string()) for name in ("exchange", "market_type", "settle_coin", "symbol", "interval", "open_time_utc", "close_time_utc", "availability_time_utc", "available_at_utc", "source", "stable_row_id")] + [(name, pa.float64()) for name in ("open", "high", "low", "close", "volume")])
        table = pa.Table.from_pylist(rows, schema=schema)
    elif path.name == "outcomes.parquet":
        schema = pa.schema([("decision_identity", pa.string()), ("horizon", pa.string()), ("status", pa.string()), ("mfe", pa.float64()), ("mae", pa.float64()), ("new_high", pa.bool_()), ("mature_at_utc", pa.string()), ("payload_json", pa.string())])
        table = pa.Table.from_pylist(rows, schema=schema)
    elif path.name == "provenance.parquet":
        schema = pa.schema([("availability_time_utc", pa.string()), ("sequence", pa.int64()), ("symbol", pa.string()), ("event_type", pa.string()), ("stable_event_id", pa.string()), ("source", pa.string()), ("dirty", pa.bool_()), ("source_status_sha256", pa.string()), ("source_diff_sha256", pa.string()), ("payload_json", pa.string())])
        table = pa.Table.from_pylist(rows, schema=schema)
    elif path.name in {"state_transitions.parquet", "baseline_transitions.parquet"}:
        schema = pa.schema([("availability_time_utc", pa.string()), ("sequence", pa.int64()), ("symbol", pa.string()), ("event_type", pa.string()), ("stable_event_id", pa.string()), ("payload_json", pa.string())])
        table = pa.Table.from_pylist(rows, schema=schema)
    elif path.name in {"strategy_inputs.parquet", "evaluation_records.parquet"}:
        schema = pa.schema([("input_id", pa.string()), ("strategy", pa.string()), ("symbol", pa.string()), ("decision_time_utc", pa.string()), ("payload_json", pa.string())])
        table = pa.Table.from_pylist(rows, schema=schema)
    elif path.name == "strategy_decisions.parquet":
        schema = pa.schema([("input_id", pa.string()), ("payload_json", pa.string())])
        table = pa.Table.from_pylist(rows, schema=schema)
    else:
        table = pa.Table.from_pylist(rows, schema=pa.schema([("payload_json", pa.string())]))
    pq.write_table(table, path, compression="zstd", use_dictionary=False, write_statistics=True, version="2.6")


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(canonical_json_bytes(value))


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact_metadata(path: Path) -> dict[str, Any]:
    if path.suffix == ".parquet":
        table = pq.read_table(path)
        rows = canonicalize(table.to_pylist())
        schema = str(table.schema.remove_metadata())
        return {"physical_sha256": _sha256_file(path), "logical_sha256": sha256_canonical(rows), "schema_sha256": sha256_canonical(schema), "row_count": table.num_rows}
    raw = path.read_bytes()
    logical = sha256_canonical(json.loads(raw.decode("utf-8"))) if path.suffix == ".json" else hashlib.sha256(raw).hexdigest()
    return {"physical_sha256": hashlib.sha256(raw).hexdigest(), "logical_sha256": logical, "schema_sha256": sha256_canonical("utf8"), "row_count": 1}


def _load_spec(spec: Mapping[str, Any] | str | Path) -> Mapping[str, Any]:
    if isinstance(spec, Mapping):
        return spec
    path = Path(spec)
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _parse_utc(value: Any) -> datetime:
    if not isinstance(value, str):
        raise TypeError("UTC timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("UTC timestamp must have a timezone")
    return parsed.astimezone(timezone.utc)


def _grade_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        payload = json.loads(row["payload_json"])
        grade = str(payload.get("grade", payload.get("result", {}).get("grade", payload.get("result", {}).get("selected", {}).get("grade", "C"))))
        result[grade] = result.get(grade, 0) + 1
    return result


def _strategy_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        strategy = str(json.loads(row["payload_json"]).get("strategy", "UNKNOWN"))
        result[strategy] = result.get(strategy, 0) + 1
    return result


def _actionable_count(rows: list[dict[str, Any]]) -> int:
    return sum(bool(json.loads(row["payload_json"]).get("actionable")) for row in rows)


def _report_markdown(report: Mapping[str, Any]) -> str:
    return "# ReplayBundle report\n\n" + "\n".join(f"- {key}: {value}" for key, value in report["counts"].items()) + "\n\nNo strategy quality verdict is included.\n"


def _git_dirty() -> bool:
    return bool(subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=False).stdout.strip())


def _git_status_hash() -> str:
    status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=False).stdout
    return sha256_canonical(status)


def _utc_now_run_id(spec: Mapping[str, Any], config: AppConfig) -> str:
    semantic_hash = sha256_canonical({"spec": spec, "strategy_config": strategy_config_payload(config)})[:12]
    return f"replaybundle-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{semantic_hash}"


def _utc_now_iso() -> str:
    return utc_iso_z(datetime.now(timezone.utc))


def _git_code_sha() -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    return result.stdout.strip() or "UNKNOWN"


def _git_diff_hash() -> str:
    result = subprocess.run(["git", "diff", "HEAD", "--binary"], capture_output=True, check=False)
    return hashlib.sha256(result.stdout).hexdigest()


def _normalize_candle(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    if "availability_time_utc" not in normalized and "available_time_utc" in normalized:
        normalized["availability_time_utc"] = normalized.pop("available_time_utc")
    if "availability_time_utc" not in normalized and "available_at_utc" in normalized:
        normalized["availability_time_utc"] = normalized["available_at_utc"]
    normalized["available_at_utc"] = normalized.get("availability_time_utc")
    return canonicalize(normalized)
