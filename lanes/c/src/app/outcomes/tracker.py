"""Repository-driven outcome updater."""

from __future__ import annotations

import logging
import inspect
from datetime import datetime, timedelta, timezone
from typing import Any

from app.market.bybit_client import BybitClient
from app.market.candles import klines_to_frame, normalize_utc
from app.outcomes.evaluator import OutcomeEvaluator
from app.outcomes.strategy_observations import (
    empty_strategy_observation_outcome,
    evaluate_strategy_observation,
    observation_coverage_ready_at,
)
from app.storage.repository import BotRepository
from app.runtime.blocking import run_blocking


logger = logging.getLogger(__name__)

STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE = 25
STRATEGY_OBSERVATION_OUTCOME_MAX_BATCHES = 4


class OutcomeTracker:
    """Update saved signals and climax observations with paper outcomes."""

    def __init__(self, client: BybitClient, repository: BotRepository) -> None:
        self._client = client
        self._repository = repository
        self._evaluator = OutcomeEvaluator()

    async def update_due_outcomes(
        self,
        now: datetime | None = None,
        *,
        signal_limit: int = 200,
        observation_limit: int = STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE,
        max_observation_batches: int = STRATEGY_OBSERVATION_OUTCOME_MAX_BATCHES,
    ) -> int:
        """Refresh saved signal and strategy-observation outcomes."""

        if signal_limit < 1 or observation_limit < 1 or max_observation_batches < 1:
            raise ValueError("outcome limits must be positive")
        now = now or datetime.now(timezone.utc)
        updated = await self._update_signal_outcomes(now, limit=signal_limit)
        updated += await self._update_strategy_observation_outcomes(
            now,
            limit=observation_limit,
            max_batches=max_observation_batches,
        )
        return updated

    async def _update_signal_outcomes(self, now: datetime, *, limit: int = 200) -> int:
        pending = await _repository_call_with_optional_kwargs(
            self._repository.list_signals_missing_outcomes,
            now=now,
            limit=limit,
        )
        updated = 0
        for signal in pending:
            start_ms = int((signal.signal_time - timedelta(minutes=5)).timestamp() * 1000)
            end_ms = int(min(now, signal.signal_time + timedelta(hours=4, minutes=5)).timestamp() * 1000)
            raw = await self._client.fetch_klines(
                signal.symbol,
                "1",
                limit=500,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            frame = klines_to_frame(raw)
            outcome = self._evaluator.evaluate(signal, frame, now=now)
            if outcome is None:
                continue
            await _repository_call(self._repository.upsert_signal_outcome, outcome)
            updated += 1
        return updated

    async def _update_strategy_observation_outcomes(
        self,
        now: datetime,
        *,
        limit: int = STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE,
        max_batches: int = STRATEGY_OBSERVATION_OUTCOME_MAX_BATCHES,
    ) -> int:
        list_due = getattr(self._repository, "list_strategy_observations_due_outcomes", None)
        update_outcome = getattr(self._repository, "update_strategy_observation_outcome", None)
        if list_due is None or update_outcome is None:
            return 0

        updated = 0
        attempted_observation_ids: set[str] = set()
        for _ in range(max_batches):
            pending = await _repository_call_with_optional_kwargs(
                list_due,
                limit=limit,
                now=now,
                exclude_observation_ids=attempted_observation_ids,
            )
            if not pending:
                break
            attempted_observation_ids.update(str(row["observation_id"]) for row in pending)
            for observation in pending:
                outcome = await self._evaluate_strategy_observation_outcome(observation, now)
                next_attempt_at = _next_outcome_attempt_at(
                    outcome,
                    now=now,
                    attempt_count=int(observation.get("outcome_attempt_count") or 0),
                )
                try:
                    saved = await _repository_call(
                        update_outcome,
                        observation["observation_id"],
                        outcome,
                        updated_at=now,
                        next_attempt_at=next_attempt_at,
                    )
                    if saved:
                        updated += 1
                except Exception:
                    logger.exception(
                        "strategy observation outcome persistence failed observation_id=%s symbol=%s",
                        observation.get("observation_id"),
                        observation.get("symbol"),
                    )
        return updated

    async def _evaluate_strategy_observation_outcome(
        self,
        observation: dict[str, object],
        now: datetime,
    ) -> dict[str, object]:
        market_price = observation.get("market_price")
        if market_price is None:
            return empty_strategy_observation_outcome(
                data_status="unknown",
                data_reason="MISSING_MARKET_PRICE",
            )
        if float(market_price) <= 0:
            return empty_strategy_observation_outcome(
                data_status="unknown",
                data_reason="INVALID_MARKET_PRICE",
            )

        observed_at = normalize_utc(observation["observed_at"])
        start_ms = int(observed_at.timestamp() * 1000)
        end_ms = int(min(now, observation_coverage_ready_at(observed_at)).timestamp() * 1000)
        try:
            raw = await self._client.fetch_klines(
                str(observation["symbol"]),
                "1",
                limit=100,
                start_ms=start_ms,
                end_ms=end_ms,
            )
            frame = klines_to_frame(raw)
            return evaluate_strategy_observation(
                observed_at=observed_at,
                entry_price=float(market_price),
                event_high=observation.get("event_high"),
                frame_1m=frame,
                now=now,
            )
        except Exception:
            logger.exception(
                "strategy observation outcome fetch failed observation_id=%s symbol=%s",
                observation.get("observation_id"),
                observation.get("symbol"),
            )
            return empty_strategy_observation_outcome(
                data_status="unknown",
                data_reason="MARKET_DATA_FETCH_FAILED",
                retryable=True,
            )


def _next_outcome_attempt_at(
    outcome: dict[str, object],
    *,
    now: datetime,
    attempt_count: int,
) -> datetime | None:
    if not outcome.get("retryable"):
        return None
    status = str(outcome.get("data_status") or "unknown")
    if status == "incomplete":
        return now + timedelta(minutes=1)
    delay_minutes = min(60, 2 ** min(attempt_count, 6))
    return now + timedelta(minutes=delay_minutes)


async def _repository_call(function: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Run synchronous repository sessions away from the shared event loop."""

    if inspect.iscoroutinefunction(function):
        return await function(*args, **kwargs)
    result = await run_blocking(function, *args, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


async def _repository_call_with_optional_kwargs(
    function: Any, /, *args: Any, **kwargs: Any
) -> Any:
    """Call old repository doubles while keeping bounded C limits explicit."""

    try:
        return await _repository_call(function, *args, **kwargs)
    except TypeError as exc:
        # Historical repository/test doubles may not have the newer optional
        # ``limit`` keyword.  Retry only when signature inspection proves the
        # keyword is unsupported; do not hide a TypeError raised by the body.
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError):
            raise
        accepts_kwargs = any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in signature.parameters.values()
        )
        unsupported = [
            key for key in kwargs
            if key not in signature.parameters and not accepts_kwargs
        ]
        if not unsupported:
            raise
        retry_kwargs = {key: value for key, value in kwargs.items() if key not in unsupported}
        return await _repository_call(function, *args, **retry_kwargs)
