"""Repository-driven outcome updater."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.market.bybit_client import BybitClient
from app.market.candles import klines_to_frame, normalize_utc
from app.outcomes.evaluator import OutcomeEvaluator
from app.outcomes.strategy_observations import (
    empty_strategy_observation_outcome,
    evaluate_strategy_observation,
    observation_coverage_ready_at,
)
from app.storage.repository import BotRepository


logger = logging.getLogger(__name__)

STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE = 25
STRATEGY_OBSERVATION_OUTCOME_MAX_BATCHES = 4


class OutcomeTracker:
    """Update saved signals and climax observations with paper outcomes."""

    def __init__(self, client: BybitClient, repository: BotRepository) -> None:
        self._client = client
        self._repository = repository
        self._evaluator = OutcomeEvaluator()

    async def update_due_outcomes(self, now: datetime | None = None) -> int:
        """Refresh saved signal and strategy-observation outcomes."""

        now = now or datetime.now(timezone.utc)
        updated = await self._update_signal_outcomes(now)
        updated += await self._update_strategy_observation_outcomes(now)
        return updated

    async def _update_signal_outcomes(self, now: datetime) -> int:
        pending = self._repository.list_signals_missing_outcomes(now=now)
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
            self._repository.upsert_signal_outcome(outcome)
            updated += 1
        return updated

    async def _update_strategy_observation_outcomes(self, now: datetime) -> int:
        list_due = getattr(self._repository, "list_strategy_observations_due_outcomes", None)
        update_outcome = getattr(self._repository, "update_strategy_observation_outcome", None)
        if list_due is None or update_outcome is None:
            return 0

        updated = 0
        attempted_observation_ids: set[str] = set()
        for _ in range(STRATEGY_OBSERVATION_OUTCOME_MAX_BATCHES):
            pending = list_due(
                limit=STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE,
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
                    if update_outcome(
                        observation["observation_id"],
                        outcome,
                        updated_at=now,
                        next_attempt_at=next_attempt_at,
                    ):
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
