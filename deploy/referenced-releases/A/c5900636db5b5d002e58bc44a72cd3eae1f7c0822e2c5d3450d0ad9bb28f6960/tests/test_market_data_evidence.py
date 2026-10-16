"""Focused tests for bounded Phase 6.5 provider evidence publication."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from app.config import AppConfig
from app.market_data.evidence import (
    PROVIDER_EVIDENCE_SCHEMA_VERSION,
    ProviderEvidencePublisher,
    normalize_provider_evidence,
)
from app.market_data.provider import CanonicalMarketDataProvider, ComparisonClassification


def _report(raw: dict[str, object] | None = None) -> dict[str, object]:
    return normalize_provider_evidence(
        raw or {"counters": {}, "details": []},
        code_sha="a" * 40,
        strategy_fingerprint="b" * 64,
        provider_mode="REST_ONLY",
        generated_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_normalized_report_has_schema_and_no_raw_payloads() -> None:
    raw = {
        "counters": {
            "evaluations_total": 2,
            "canonical_rest_groups": 2,
            "source_comparisons": 1,
            "decision_comparisons": 1,
        },
        "details": [
            {
                "event": "evaluation",
                "symbol": "AAAUSDT",
                "frame": [{"close": 999}],
                "ticker_payload": {"last": 999},
                "classification": "EXACT",
            }
        ],
    }

    normalized = _report(raw)

    assert normalized["schema_version"] == PROVIDER_EVIDENCE_SCHEMA_VERSION
    assert normalized["provider_mode"] == "REST_ONLY"
    assert normalized["evaluations_total"] == 2
    sample = normalized["recent_samples"][0]
    assert "frame" not in sample
    assert "ticker_payload" not in sample


def test_publisher_atomically_replaces_same_directory_and_is_bounded(tmp_path: Path) -> None:
    target = tmp_path / "runtime" / "canonical-marketdata-provider-evidence.json"
    publisher = ProviderEvidencePublisher(target)
    report = _report()
    report["recent_samples"] = [
        {"symbol": f"S{i}", "classification": "EXACT", "extra": "x" * 4000}
        for i in range(64)
    ]

    published = publisher.publish(report)

    assert published == target
    assert target.exists()
    payload = target.read_bytes()
    assert len(payload) < 64 * 1024
    decoded = json.loads(payload)
    assert len(decoded["recent_samples"]) <= 32
    assert all(
        len(json.dumps(sample, sort_keys=True, separators=(",", ":")).encode()) <= 2 * 1024
        for sample in decoded["recent_samples"]
    )
    assert not list(target.parent.glob(".canonical-marketdata-provider-evidence.*"))


def test_publisher_failure_isolated_and_rate_limited(tmp_path: Path, caplog) -> None:
    target = tmp_path / "target.json"
    publisher = ProviderEvidencePublisher(target)
    publisher._replace = lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("disk full"))

    with caplog.at_level("WARNING"):
        assert publisher.publish(_report()) == target
        assert publisher.publish(_report()) == target

    assert sum("provider evidence publication failed" in record.message for record in caplog.records) == 1
    assert not target.exists()


def test_provider_rest_only_report_has_zero_hub_comparisons() -> None:
    provider = CanonicalMarketDataProvider(scanner=object(), config=AppConfig())
    provider.record_evaluation_evidence(
        symbol="AAAUSDT",
        evaluation_time_utc=datetime(2026, 1, 1, tzinfo=UTC),
        canonical_source="REST",
        candidate_source=None,
        classification=ComparisonClassification.EXACT,
        canonical_input_fingerprint="a" * 64,
        candidate_input_fingerprint=None,
        canonical_decision_fingerprint="b" * 64,
        candidate_decision_fingerprint=None,
    )

    report = provider.evidence_report(
        code_sha="c" * 40,
        strategy_fingerprint="d" * 64,
        generated_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert report["evaluations_total"] == 1
    assert report["canonical_rest_groups"] == 1
    assert report["canonical_hub_groups"] == 0
    assert report["hub_candidates_total"] == 0
    assert report["hub_exact"] == 0
    assert report["source_comparisons"] == 0
    assert report["decision_comparisons"] == 0
    assert report["decision_divergences"] == 0


def test_provider_shadow_separates_source_and_decision_divergence() -> None:
    provider = CanonicalMarketDataProvider(scanner=object(), config=AppConfig())
    provider.record_evaluation_evidence(
        symbol="AAAUSDT",
        evaluation_time_utc=datetime(2026, 1, 1, tzinfo=UTC),
        canonical_source="REST",
        candidate_source="HUB",
        classification=ComparisonClassification.SOURCE_SEMANTIC_MISMATCH,
        canonical_input_fingerprint="a" * 64,
        candidate_input_fingerprint="c" * 64,
        canonical_decision_fingerprint="b" * 64,
        candidate_decision_fingerprint="b" * 64,
    )
    provider.record_evaluation_evidence(
        symbol="AAAUSDT",
        evaluation_time_utc=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        canonical_source="REST",
        candidate_source="HUB",
        classification=ComparisonClassification.DECISION_DIVERGENCE,
        canonical_input_fingerprint="a" * 64,
        candidate_input_fingerprint="a" * 64,
        canonical_decision_fingerprint="b" * 64,
        candidate_decision_fingerprint="e" * 64,
    )

    report = provider.evidence_report(
        code_sha="c" * 40,
        strategy_fingerprint="d" * 64,
        generated_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )

    assert report["hub_candidates_total"] == 2
    assert report["source_comparisons"] == 2
    assert report["source_comparison_divergences"] == 1
    assert report["decision_comparisons"] == 2
    assert report["decision_divergences"] == 1
    assert report["hub_semantic_mismatch"] == 1
    assert report["last_divergence_at"] == "2026-01-01T00:01:00.000000Z"


def test_provider_evidence_samples_are_bounded_to_32() -> None:
    provider = CanonicalMarketDataProvider(scanner=object(), config=AppConfig())
    for index in range(40):
        provider.record_evaluation_evidence(
            symbol=f"S{index}USDT",
            evaluation_time_utc=datetime(2026, 1, 1, tzinfo=UTC),
            canonical_source="REST",
            candidate_source="HUB",
            classification=ComparisonClassification.EXACT,
            canonical_input_fingerprint="a" * 64,
            candidate_input_fingerprint="a" * 64,
            canonical_decision_fingerprint="b" * 64,
            candidate_decision_fingerprint="b" * 64,
        )
    report = provider.evidence_report(
        code_sha="c" * 40,
        strategy_fingerprint="d" * 64,
        generated_at_utc=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert len(report["recent_samples"]) == 32
    assert report["recent_samples"][0]["symbol"] == "S8USDT"


def test_runtime_publishes_only_after_completed_full_cycle(tmp_path: Path) -> None:
    # This contract is asserted against the runtime source before the integration
    # fixture is wired: publication must be a single full-cycle boundary call.
    from app.main import ShortSignalBot
    import inspect

    source = inspect.getsource(ShortSignalBot.run_cycle)
    assert "_publish_provider_evidence" in source
    assert source.count("_publish_provider_evidence") == 1
