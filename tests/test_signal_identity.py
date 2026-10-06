from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

from app.storage.identity import signal_identity


BASE_INPUTS = {
    "symbol": " ontusdt ",
    "event_id": " ONTUSDT:15m:1:111 ",
    "strategy_type": " baseline_pullback ",
    "strategy_subtype": " baseline_pullback ",
    "model_version": " baseline-v1 ",
}


def test_same_canonical_inputs_produce_same_identity() -> None:
    assert signal_identity(**BASE_INPUTS) == signal_identity(**BASE_INPUTS)


def test_identity_normalizes_case_composition_and_edge_unicode_whitespace() -> None:
    equivalent = {
        "symbol": "\u2003ONTUSDT\u00a0",
        "event_id": "\u2009ONTUSDT:15m:1:111\u2009",
        "strategy_type": "BASELINE_PULLBACK",
        "strategy_subtype": "baseline_pullback",
        "model_version": "baseline-v1",
    }
    composed = dict(BASE_INPUTS)
    composed["model_version"] = "caf\u00e9-v1"
    decomposed = dict(BASE_INPUTS)
    decomposed["model_version"] = "cafe\u0301-v1"
    assert signal_identity(**equivalent) == signal_identity(**BASE_INPUTS)
    assert signal_identity(**decomposed) == signal_identity(**composed)


@pytest.mark.parametrize(
    "compatibility_value,ordinary_value",
    [
        ("micro-µ", "micro-μ"),
        ("space\u00a0inside", "space inside"),
        ("space\u2003inside", "space inside"),
        ("roman-Ⅳ", "roman-iv"),
    ],
)
def test_identity_preserves_security_sensitive_compatibility_distinctions(
    compatibility_value: str, ordinary_value: str
) -> None:
    compatibility = dict(BASE_INPUTS, model_version=compatibility_value)
    ordinary = dict(BASE_INPUTS, model_version=ordinary_value)
    assert signal_identity(**compatibility) != signal_identity(**ordinary)


def test_identity_rejects_control_and_format_characters() -> None:
    for field, character in (
        ("symbol", "\x00"),
        ("event_id", "\x7f"),
        ("strategy_type", "\u200b"),
        ("strategy_subtype", "\u202e"),
        ("model_version", "\u2060"),
    ):
        invalid = dict(BASE_INPUTS)
        invalid[field] = f"valid{character}value"
        with pytest.raises(ValueError):
            signal_identity(**invalid)


@pytest.mark.parametrize("field", BASE_INPUTS)
@pytest.mark.parametrize("value", [1, object(), ["not", "a", "string"]])
def test_identity_rejects_wrong_type_components(field: str, value: object) -> None:
    invalid: dict[str, object] = dict(BASE_INPUTS)
    invalid[field] = value
    with pytest.raises(ValueError):
        signal_identity(**cast(Any, invalid))


def test_identity_is_lowercase_sha256_hex() -> None:
    identity = signal_identity(**BASE_INPUTS)
    assert len(identity) == 64
    assert identity == identity.lower()
    assert all(character in "0123456789abcdef" for character in identity)


def test_identity_uses_versioned_domain_separator() -> None:
    canonical = {
        "event_id": "ontusdt:15m:1:111",
        "model_version": "baseline-v1",
        "strategy_subtype": "baseline_pullback",
        "strategy_type": "baseline_pullback",
        "symbol": "ontusdt",
    }
    canonical_json = json.dumps(
        canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    expected = hashlib.sha256(
        f"signal_identity:v1\0{canonical_json}".encode("utf-8")
    ).hexdigest()
    assert signal_identity(**BASE_INPUTS) == expected


def test_changed_identity_component_changes_identity() -> None:
    original = signal_identity(**BASE_INPUTS)
    for field in BASE_INPUTS:
        changed = dict(BASE_INPUTS)
        changed[field] = f"{changed[field]}-changed"
        assert signal_identity(**changed) != original, field


def test_baseline_uses_explicit_normalized_subtype_when_omitted() -> None:
    omitted = dict(BASE_INPUTS)
    omitted.pop("strategy_subtype")
    explicit = dict(BASE_INPUTS)
    explicit["strategy_subtype"] = "BASELINE_PULLBACK"
    assert signal_identity(**omitted) == signal_identity(**explicit)


def test_restart_reproduces_identity() -> None:
    source_root = Path(__file__).resolve().parents[1]
    script = """
import json
from app.storage.identity import signal_identity
print(signal_identity(**json.loads(__import__('sys').stdin.read())))
"""
    env = {**os.environ, "PYTHONPATH": str(source_root)}
    outputs = []
    for _ in range(2):
        result = subprocess.run(
            [sys.executable, "-c", script],
            input=json.dumps(BASE_INPUTS),
            text=True,
            capture_output=True,
            check=True,
            cwd=source_root,
            env=env,
        )
        outputs.append(result.stdout.strip())
    assert outputs[0] == outputs[1]
    assert outputs[0] == signal_identity(**BASE_INPUTS)


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", None),
        ("symbol", "   "),
        ("event_id", None),
        ("event_id", "   "),
        ("strategy_type", None),
        ("model_version", None),
        ("model_version", "   "),
    ],
)
def test_missing_or_malformed_required_input_is_rejected(field: str, value: object) -> None:
    inputs = dict(BASE_INPUTS)
    inputs[field] = value
    with pytest.raises(ValueError):
        signal_identity(**inputs)


def test_non_baseline_missing_subtype_is_rejected() -> None:
    inputs = dict(BASE_INPUTS)
    inputs["strategy_type"] = "CLIMAX_EXHAUSTION"
    inputs.pop("strategy_subtype")
    with pytest.raises(ValueError):
        signal_identity(**inputs)


def test_baseline_null_subtype_is_normalized_to_explicit_value() -> None:
    omitted = dict(BASE_INPUTS)
    omitted.pop("strategy_subtype")
    assert signal_identity(**{**BASE_INPUTS, "strategy_subtype": None}) == signal_identity(
        **omitted
    )
