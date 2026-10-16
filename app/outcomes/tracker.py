"""Repository-driven outcome updater."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from app.infra.provider_errors import OutcomeBudgetUnavailable, ProviderRateLimited
from app.infra.request_scheduler import RequestPriority
from app.market.bybit_client import BybitClient
from app.market.candles import klines_to_frame, normalize_utc
from app.outcomes.evaluator import OutcomeEvaluator
from app.outcomes.fetch import OutcomeCandleFetcher
from app.runtime.blocking import run_blocking
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

    def __init__(self, client: BybitClient, repository: BotRepository, *, fetcher: OutcomeCandleFetcher | None = None) -> None:
        self._client = client
        self._repository = repository
        self._fetcher = fetcher or OutcomeCandleFetcher(client, repository)
        self._evaluator = OutcomeEvaluator()
        self._observations_first = False

    async def update_due_outcomes(
        self,
        now: datetime | None = None,
        *,
        signal_limit: int = 25,
        observation_limit: int = 25,
    ) -> int:
        """Bounded outcome work; both cohorts share one physical request budget."""
        now = now or datetime.now(timezone.utc)
        self._observations_first = not self._observations_first
        with self._fetcher.cycle():
            if self._observations_first:
                updated = await self._update_strategy_observation_outcomes(now, observation_limit=observation_limit)
                return updated + await self._update_signal_outcomes(now, signal_limit=signal_limit)
            updated = await self._update_signal_outcomes(now, signal_limit=signal_limit)
            return updated + await self._update_strategy_observation_outcomes(now, observation_limit=observation_limit)

    async def _update_signal_outcomes(
        self, now: datetime, *, signal_limit: int = 25
    ) -> int:
        pending = (
            await run_blocking(
                self._repository.list_signals_missing_outcomes,
                now=now,
                limit=signal_limit,
            )
        )[:signal_limit]
        updated = 0
        for signal in pending:
            start_ms = int((signal.signal_time - timedelta(minutes=5)).timestamp() * 1000)
            end_ms = int(min(now, signal.signal_time + timedelta(hours=4, minutes=5)).timestamp() * 1000)
            try:
                raw = await self._fetcher.fetch_klines(
                    signal.symbol, "1", limit=500, start_ms=start_ms, end_ms=end_ms,
                )
            except OutcomeBudgetUnavailable:
                break
            except ProviderRateLimited as exc:
                await run_blocking(
                    self._fetcher.defer,
                    f"signal:{signal.id}", exc, symbol=signal.symbol, interval="1"
                )
                continue
            frame = klines_to_frame(raw)
            outcome = self._evaluator.evaluate(signal, frame, now=now)
            if outcome is None:
                continue
            await run_blocking(
                self._persist_signal_outcome,
                outcome,
                f"signal:{signal.id}",
            )
            updated += 1
        return updated

    async def _update_strategy_observation_outcomes(
        self, now: datetime, *, observation_limit: int = STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE
    ) -> int:
        list_due = getattr(self._repository, "list_strategy_observations_due_outcomes", None)
        update_outcome = getattr(self._repository, "update_strategy_observation_outcome", None)
        if list_due is None or update_outcome is None:
            return 0

        updated = 0
        attempted_observation_ids: set[str] = set()
        max_rows = max(1, int(observation_limit))
        for _ in range(min(STRATEGY_OBSERVATION_OUTCOME_MAX_BATCHES, max_rows // STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE + 1)):
            pending = (
                await run_blocking(
                    list_due,
                    limit=min(STRATEGY_OBSERVATION_OUTCOME_BATCH_SIZE, max_rows - updated),
                    now=now,
                    exclude_observation_ids=attempted_observation_ids,
                )
            ) or []
            if not pending:
                break
            attempted_observation_ids.update(str(row["observation_id"]) for row in pending)
            for observation in pending:
                try:
                    outcome = await self._evaluate_strategy_observation_outcome(observation, now)
                except OutcomeBudgetUnavailable:
                    return updated
                next_attempt_at = (
                    outcome.pop("_next_attempt_at", None)
                    if outcome.get("data_status") == "deferred"
                    else _next_outcome_attempt_at(
                        outcome, now=now,
                        attempt_count=int(observation.get("outcome_attempt_count") or 0),
                    )
                )
                try:
                    if await run_blocking(
                        self._persist_strategy_observation_outcome,
                        update_outcome,
                        observation,
                        outcome,
                        now,
                        next_attempt_at,
                    ):
                        updated += 1

                except Exception:
                    logger.exception(
                        "strategy observation outcome persistence failed observation_id=%s symbol=%s",
                        observation.get("observation_id"),
                        observation.get("symbol"),
                    )
        return updated

    def _persist_signal_outcome(self, outcome: object, task_key: str) -> None:
        """Persist and recover a signal in one executor turn."""
        self._repository.upsert_signal_outcome(outcome)
        self._fetcher.recovered(task_key)

    def _persist_strategy_observation_outcome(
        self,
        update_outcome,
        observation: dict[str, object],
        outcome: dict[str, object],
        now: datetime,
        next_attempt_at: datetime | None,
    ) -> bool:
        """Keep persistence, retry recovery, and candidate transition ordered."""
        if not update_outcome(
            observation["observation_id"],
            outcome,
            updated_at=now,
            next_attempt_at=next_attempt_at,
        ):
            return False
        if outcome.get("data_status") in {"complete", "incomplete"}:
            self._fetcher.recovered(f"observation:{observation['observation_id']}")
        candidate_id = observation.get("candidate_id")
        if candidate_id:
            status = str(outcome.get("data_status") or "unknown")
            terminal_state = (
                "COMPLETE"
                if status == "complete"
                else "DATA_GAP"
                if status == "unknown" and not outcome.get("retryable")
                else None
            )
            transition = getattr(self._repository, "transition_strategy_candidate", None)
            if terminal_state and transition is not None:
                transition(
                    candidate_id=str(candidate_id),
                    state=terminal_state,
                    observed_at=now,
                    reason=f"OUTCOME_{terminal_state}",
                )
        return True

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
            raw = await self._fetcher.fetch_klines(
                str(observation["symbol"]),
                "1",
                limit=100,
                start_ms=start_ms,
                end_ms=end_ms,
                priority=RequestPriority.OUTCOME_REPLAY,
            )
            frame = klines_to_frame(raw)
            return evaluate_strategy_observation(
                observed_at=observed_at,
                entry_price=float(market_price),
                event_high=observation.get("event_high"),
                frame_1m=frame,
                now=now,
            )
        except OutcomeBudgetUnavailable:
            raise
        except ProviderRateLimited as exc:
            data, retry_at, _ = await run_blocking(
                self._fetcher.defer,
                f"observation:{observation['observation_id']}", exc,
                symbol=str(observation["symbol"]), interval="1",
            )
            return {**data, "_next_attempt_at": retry_at}
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
