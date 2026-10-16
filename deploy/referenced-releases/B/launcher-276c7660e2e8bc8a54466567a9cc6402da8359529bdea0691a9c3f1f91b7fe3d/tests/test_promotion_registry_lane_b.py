"""Lane B promotion-registry tests (ported mechanism, Lane-B-owned evidence).

1) The SHIPPED config and the SHIPPED promotion_registry.json must agree, so the
   release boots: every '*_live_delivery_enabled: true' has a VALIDATED entry.
2) Boot is refused when a delivery flag is true but the registry lacks a
   VALIDATED/evidence-bearing entry (mechanism behaviour, synthetic fixtures).
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from app.config import AppConfig
from app.promotions import validate_promotion_registry

RELEASE_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = RELEASE_ROOT / 'config.yaml'
REGISTRY_PATH = RELEASE_ROOT / 'promotion_registry.json'

ENABLED_FLAGS = (
    'baseline_live_delivery_enabled',
    'volume_climax_live_delivery_enabled',
    'low_volume_live_delivery_enabled',
)


def test_shipped_config_matches_shipped_registry() -> None:
    config = AppConfig(**yaml.safe_load(CONFIG_PATH.read_text(encoding='utf-8')))
    for flag in ENABLED_FLAGS:
        assert getattr(config, flag) is True, f'{flag} must remain true (unchanged delivery contour)'
    assert config.trapped_longs_live_delivery_enabled is False
    validate_promotion_registry(config, REGISTRY_PATH)  # must not raise


def test_registry_entries_carry_lane_b_evidence() -> None:
    payload = json.loads(REGISTRY_PATH.read_text(encoding='utf-8'))
    assert set(payload['strategies']) == {
        'BASELINE_PULLBACK',
        'VOLUME_CLIMAX_UNWIND',
        'LOW_VOLUME_EXTENSION_FAILURE',
    }
    for entry in payload['strategies'].values():
        assert entry['status'] == 'VALIDATED'
        assert 'measured on Lane B' in entry['evidence']
        assert entry['recorded_at'] and entry['source']


def test_boot_refused_when_enabled_flag_has_no_entry(tmp_path: Path) -> None:
    reg = tmp_path / 'promotion_registry.json'
    reg.write_text(json.dumps({'strategies': {}}), encoding='utf-8')
    config = SimpleNamespace(**{flag: flag == ENABLED_FLAGS[0] for flag in ENABLED_FLAGS},
                             trapped_longs_live_delivery_enabled=False)
    with pytest.raises(RuntimeError, match='promotion registry check failed'):
        validate_promotion_registry(config, reg)


def test_boot_refused_when_entry_not_validated_or_evidence_less(tmp_path: Path) -> None:
    for bad in (
        {'status': 'PENDING', 'evidence': 'x', 'recorded_at': '2026-09-23', 'source': 's'},
        {'status': 'VALIDATED', 'evidence': '', 'recorded_at': '2026-09-23', 'source': 's'},
        {'status': 'VALIDATED', 'evidence': 'x', 'recorded_at': '', 'source': 's'},
    ):
        reg = tmp_path / 'promotion_registry.json'
        reg.write_text(json.dumps({'strategies': {'BASELINE_PULLBACK': bad}}), encoding='utf-8')
        config = SimpleNamespace(**{flag: flag == ENABLED_FLAGS[0] for flag in ENABLED_FLAGS},
                                 trapped_longs_live_delivery_enabled=False)
        with pytest.raises(RuntimeError, match='promotion registry check failed'):
            validate_promotion_registry(config, reg)
