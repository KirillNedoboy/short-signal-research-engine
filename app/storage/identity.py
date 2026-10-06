"""Deterministic identity for persisted signals.

The identity is derived only from immutable logical signal inputs.  It must not
include timestamps, process/runtime identifiers, or delivery state.  Version 1
hashes the UTF-8 preimage ``signal_identity:v1\\0<canonical JSON>`` so the
identity namespace is explicit and future formats cannot share this preimage.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from typing import Final


_BASELINE_STRATEGY: Final = "BASELINE_PULLBACK"
_IDENTITY_DOMAIN: Final = "signal_identity:v1"
_INVALID_UNICODE_CATEGORIES: Final = frozenset({"Cc", "Cf", "Cs"})
# ``str.casefold`` maps MICRO SIGN to GREEK SMALL LETTER MU.  Preserve that
# compatibility distinction while retaining case folding for ordinary letters.
_CASEFOLD_PRESERVED: Final = {"\u00b5": "\u00b5"}


def _normalize_required(value: str | None, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a non-empty string")
    normalized = unicodedata.normalize("NFC", value).strip()
    normalized = "".join(_CASEFOLD_PRESERVED.get(char, char.casefold()) for char in normalized)
    if not normalized or any(
        unicodedata.category(char) in _INVALID_UNICODE_CATEGORIES
        for char in normalized
    ):
        raise ValueError(f"{field_name} must be a non-empty printable string")
    return normalized


def signal_identity(
    *,
    symbol: str | None,
    event_id: str | None,
    strategy_type: str | None,
    strategy_subtype: str | None = None,
    model_version: str | None,
) -> str:
    """Return a restart-stable identity for one logical signal.

    Baseline signals have no separate subtype in legacy callers; their subtype
    is made explicit as ``BASELINE_PULLBACK`` before canonicalization.  Every
    other strategy must provide a non-empty subtype so the identity never
    contains a nullable component.
    """

    normalized_strategy_type = _normalize_required(
        strategy_type, field_name="strategy_type"
    )
    normalized_subtype = (
        _normalize_required(strategy_subtype, field_name="strategy_subtype")
        if strategy_subtype is not None
        else _BASELINE_STRATEGY.casefold()
        if normalized_strategy_type == _BASELINE_STRATEGY.casefold()
        else None
    )
    if normalized_subtype is None:
        raise ValueError(
            "strategy_subtype is required for non-baseline strategies"
        )

    canonical = {
        "symbol": _normalize_required(symbol, field_name="symbol"),
        "event_id": _normalize_required(event_id, field_name="event_id"),
        "strategy_type": normalized_strategy_type,
        "strategy_subtype": normalized_subtype,
        "model_version": _normalize_required(model_version, field_name="model_version"),
    }
    canonical_json = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    # The domain and NUL separator are part of the v1 preimage format.  Keep
    # this explicit so future hashes cannot collide with another payload that
    # happens to use the same canonical JSON representation.
    preimage = f"{_IDENTITY_DOMAIN}\0{canonical_json}".encode("utf-8")
    return hashlib.sha256(preimage).hexdigest()
