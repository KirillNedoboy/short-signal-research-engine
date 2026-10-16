"""Deterministic, side-effect-free registry for strategy adapters."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TypeAlias

from .adapters import StrategyAdapter, normalize_strategy_evaluation, validate_strategy_identifier
from .contracts import (
    BASELINE_PULLBACK,
    StrategyContext,
    StrategyEvaluation,
    StrategyEvaluationError,
)

CLIMAX_EXHAUSTION = "CLIMAX_EXHAUSTION"
VOLUME_CLIMAX_UNWIND = "VOLUME_CLIMAX_UNWIND"
LOW_VOLUME_EXTENSION_FAILURE = "LOW_VOLUME_EXTENSION_FAILURE"
TRAPPED_LONGS_REVERSAL = "TRAPPED_LONGS_REVERSAL"

REQUIRED_STRATEGY_TYPES = (
    BASELINE_PULLBACK,
    CLIMAX_EXHAUSTION,
    VOLUME_CLIMAX_UNWIND,
    LOW_VOLUME_EXTENSION_FAILURE,
    TRAPPED_LONGS_REVERSAL,
)
_SUPPORTED_STRATEGY_TYPES = frozenset(REQUIRED_STRATEGY_TYPES)

RegisteredAdapter: TypeAlias = tuple[str, StrategyAdapter]


class StrategyRegistry:
    """Explicit registry whose lookup and evaluation order never depends on imports."""

    def __init__(self, adapters: Mapping[str, object] | Iterable[object]) -> None:
        if isinstance(adapters, Mapping):
            entries = list(adapters.items())
        else:
            entries = []
            for item in adapters:
                if isinstance(item, tuple) and len(item) == 2:
                    entries.append((item[0], item[1]))
                else:
                    entries.append((getattr(item, "strategy_type", None), item))
        registered: dict[str, StrategyAdapter] = {}
        for raw_identifier, adapter in entries:
            identifier = self._validate_identifier(raw_identifier)
            if identifier in registered:
                raise StrategyEvaluationError(f"duplicate strategy identifier: {identifier}")
            self._validate_adapter(adapter, identifier)
            registered[identifier] = adapter
        if set(registered) != _SUPPORTED_STRATEGY_TYPES:
            missing = tuple(identifier for identifier in REQUIRED_STRATEGY_TYPES if identifier not in registered)
            raise StrategyEvaluationError(
                f"complete strategy set required; missing: {', '.join(missing)}"
            )
        self._adapters = registered

    @property
    def identifiers(self) -> tuple[str, ...]:
        """Return registered identifiers in the canonical order."""
        return tuple(identifier for identifier in REQUIRED_STRATEGY_TYPES if identifier in self._adapters)

    def __iter__(self):
        return iter(self.identifiers)

    def __len__(self) -> int:
        return len(self._adapters)

    def get(self, strategy_type: object) -> StrategyAdapter:
        """Look up an adapter, rejecting malformed and unknown identifiers."""
        self._validate_identifier(strategy_type)
        try:
            return self._adapters[strategy_type]
        except KeyError as exc:
            raise StrategyEvaluationError(f"unknown strategy identifier: {strategy_type}") from exc

    def evaluate_all(self, context: StrategyContext) -> tuple[StrategyEvaluation, ...]:
        """Evaluate every registered adapter in canonical order and validate outputs."""
        if not isinstance(context, StrategyContext):
            raise StrategyEvaluationError("registry context must be a StrategyContext")
        evaluations: list[StrategyEvaluation] = []
        for identifier in self.identifiers:
            adapter = self._adapters[identifier]
            try:
                raw = adapter.evaluate(context)
            except StrategyEvaluationError:
                raise
            except Exception as exc:
                raise StrategyEvaluationError(
                    f"strategy adapter failed for {identifier}"
                ) from exc
            evaluations.append(normalize_strategy_evaluation(adapter, context, raw))
        return tuple(evaluations)

    @staticmethod
    def _validate_identifier(identifier: object) -> str:
        try:
            normalized = validate_strategy_identifier(identifier)
        except StrategyEvaluationError:
            raise
        if normalized not in _SUPPORTED_STRATEGY_TYPES:
            raise StrategyEvaluationError(f"unknown strategy identifier: {normalized}")
        return normalized

    @staticmethod
    def _validate_adapter(adapter: object, identifier: str) -> None:
        if not callable(getattr(adapter, "evaluate", None)):
            raise StrategyEvaluationError(f"adapter for {identifier} must expose a callable evaluate")
        adapter_type = getattr(adapter, "strategy_type", None)
        if not isinstance(adapter_type, str):
            raise StrategyEvaluationError(f"adapter for {identifier} has an invalid strategy_type")
        try:
            validated = validate_strategy_identifier(adapter_type)
        except StrategyEvaluationError as exc:
            raise StrategyEvaluationError(f"adapter for {identifier} has an invalid strategy_type") from exc
        if validated != identifier:
            raise StrategyEvaluationError(
                f"adapter strategy_type {validated} does not match identifier {identifier}"
            )
        if not isinstance(adapter, StrategyAdapter):
            raise StrategyEvaluationError(f"adapter for {identifier} violates StrategyAdapter contract")


__all__ = [
    "CLIMAX_EXHAUSTION",
    "LOW_VOLUME_EXTENSION_FAILURE",
    "REQUIRED_STRATEGY_TYPES",
    "StrategyRegistry",
    "TRAPPED_LONGS_REVERSAL",
    "VOLUME_CLIMAX_UNWIND",
]
