from datetime import datetime, timedelta, timezone

import asyncio

from app.outcomes.tracker import OutcomeTracker


UTC = timezone.utc


class _Client:
    def __init__(self, raw_klines):
        self.raw_klines = raw_klines
        self.calls = []

    async def fetch_klines(self, symbol, interval, *, limit, start_ms, end_ms):
        self.calls.append((symbol, interval, start_ms, end_ms))
        return self.raw_klines


class _WindowedClient(_Client):
    async def fetch_klines(self, symbol, interval, *, limit, start_ms, end_ms):
        self.calls.append((symbol, interval, start_ms, end_ms))
        return [row for row in self.raw_klines if start_ms <= int(row[0]) <= end_ms]


class _Repository:
    def __init__(self, rows):
        self.rows = rows
        self.updated = []

    def list_signals_missing_outcomes(self, *, now):
        return []

    def list_strategy_observations_due_outcomes(self, *, limit, now=None, exclude_observation_ids=()):
        excluded = set(exclude_observation_ids)
        return [row for row in self.rows if row["observation_id"] not in excluded][:limit]

    def update_strategy_observation_outcome(self, observation_id, outcome, *, updated_at, next_attempt_at=None):
        self.updated.append((observation_id, outcome, updated_at))
        self.rows = [row for row in self.rows if row["observation_id"] != observation_id]
        return True


def test_outcome_tracker_updates_strategy_observations_without_signal_side_effects(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0] * 16, start=start)
    raw = [
        [str(int(row["start_ms"])), str(row["open"]), str(row["high"]), str(row["low"]), str(row["close"]), str(row["volume"]), str(row["turnover"])]
        for _, row in frame.iterrows()
    ]
    client = _Client(raw)
    repository = _Repository(
        [
            {
                "observation_id": "obs-1",
                "symbol": "TESTUSDT",
                "observed_at": start,
                "market_price": 100.0,
                "event_high": 110.0,
            }
        ]
    )

    updated = asyncio.run(OutcomeTracker(client, repository).update_due_outcomes(now=start.replace(hour=13)))

    assert updated == 1
    assert len(client.calls) == 1
    assert repository.updated[0][0] == "obs-1"
    assert repository.updated[0][1]["data_status"] == "complete"


def test_outcome_tracker_fetches_the_closed_candle_after_fractional_15m_target(make_frame) -> None:
    candle_start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    observed_at = candle_start + timedelta(seconds=30)
    frame = make_frame([100.0] * 17, start=candle_start)
    raw = [
        [str(int(row["start_ms"])), str(row["open"]), str(row["high"]), str(row["low"]), str(row["close"]), str(row["volume"]), str(row["turnover"])]
        for _, row in frame.iterrows()
    ]
    client = _WindowedClient(raw)
    repository = _Repository(
        [
            {
                "observation_id": "obs-fractional",
                "symbol": "TESTUSDT",
                "observed_at": observed_at,
                "market_price": 100.0,
                "event_high": 110.0,
            }
        ]
    )

    asyncio.run(OutcomeTracker(client, repository).update_due_outcomes(now=candle_start + timedelta(minutes=17)))

    assert repository.updated[0][1]["data_status"] == "complete"
    assert repository.updated[0][1]["horizons"]["15m"]["price"] == 100.0


def test_outcome_tracker_processes_multiple_bounded_batches(make_frame) -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    frame = make_frame([100.0] * 16, start=start)
    raw = [
        [str(int(row["start_ms"])), str(row["open"]), str(row["high"]), str(row["low"]), str(row["close"]), str(row["volume"]), str(row["turnover"])]
        for _, row in frame.iterrows()
    ]
    repository = _Repository(
        [
            {
                "observation_id": f"obs-{index}",
                "symbol": "TESTUSDT",
                "observed_at": start,
                "market_price": 100.0,
                "event_high": 110.0,
            }
            for index in range(30)
        ]
    )

    updated = asyncio.run(OutcomeTracker(_Client(raw), repository).update_due_outcomes(now=start + timedelta(hours=1)))

    assert updated == 30
    assert repository.rows == []


def test_outcome_tracker_records_exact_unknown_reason_for_missing_market_price() -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
    repository = _Repository(
        [
            {
                "observation_id": "obs-no-price",
                "symbol": "TESTUSDT",
                "observed_at": start,
                "market_price": None,
                "event_high": 110.0,
            }
        ]
    )

    asyncio.run(OutcomeTracker(_Client([]), repository).update_due_outcomes(now=start + timedelta(hours=1)))

    outcome = repository.updated[0][1]
    assert outcome["data_status"] == "unknown"
    assert outcome["data_reason"] == "MISSING_MARKET_PRICE"


def test_outcome_tracker_does_not_revisit_failed_rows_in_the_same_cycle() -> None:
    start = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)

    class _FailingUpdatesRepository(_Repository):
        def update_strategy_observation_outcome(
            self, observation_id, outcome, *, updated_at, next_attempt_at=None
        ):
            self.updated.append((observation_id, outcome, updated_at))
            return False

    repository = _FailingUpdatesRepository(
        [
            {
                "observation_id": f"obs-{index}",
                "symbol": "TESTUSDT",
                "observed_at": start,
                "market_price": None,
                "event_high": 110.0,
            }
            for index in range(30)
        ]
    )

    updated = asyncio.run(
        OutcomeTracker(_Client([]), repository).update_due_outcomes(now=start + timedelta(hours=1))
    )

    assert updated == 0
    assert len(repository.updated) == 30
    assert len({observation_id for observation_id, *_ in repository.updated}) == 30
