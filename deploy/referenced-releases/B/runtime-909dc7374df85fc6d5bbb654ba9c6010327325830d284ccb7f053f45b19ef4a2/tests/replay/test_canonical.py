from __future__ import annotations

from datetime import datetime, timezone

from app.replay.canonical import canonical_json_bytes, sha256_canonical


def test_canonical_json_is_sorted_compact_utc_and_stable() -> None:
    value = {
        "z": ["kept", 1.2345678901234567],
        "when": datetime(2026, 4, 13, 12, 0, tzinfo=timezone.utc),
        "a": {"truth": True, "none": None, "integer": 1},
    }

    assert canonical_json_bytes(value) == (
            b'{"a":{"integer":1,"none":null,"truth":true},"when":"2026-04-13T12:00:00.000000Z","z":["kept",1.23456789012346]}'
    )
    assert sha256_canonical(value) == sha256_canonical(dict(reversed(list(value.items()))))
