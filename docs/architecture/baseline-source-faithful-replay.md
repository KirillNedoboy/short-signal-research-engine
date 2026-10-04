# Phase 4 BASELINE source-faithful replay

## Implementation map

| Layer | Production source | Replay ownership |
|---|---|---|
| Observation and availability | `app/market/bybit_client.py`, `app/market/candles.py`, Bybit V5 kline/trade archives | `app/replay/market.py`; UTC-normalized rows, closed vs partial-as-of kind, no future availability |
| Feature construction | `app/features/builder.py` | Shared `FeatureBuilder`; replay supplies a 300-row sliding frame (300-minute pre-roll plus safety margin) |
| Event creation | `app/events/pump_detector.py` | `BaselineLifecycle.detect_event` |
| Pullback/reset/expiry | `app/events/pullback_tracker.py` | `BaselineLifecycle.advance_active_event`; no copied thresholds |
| Short zone | `app/events/short_zone.py` | Shared `ShortZoneBuilder` in lifecycle |
| Evaluation | `app/replay/evaluator.py` → `SignalEngine.analyze` | `BaselineReplayDriver` calls only `evaluate_strategy_input` |
| Scan trigger | `run_cycle` and read-only scan-cycle/scheduled-symbol evidence | `ReplayOpportunity.trigger=REPLAY_EVALUATION_TRIGGER`; cycle start is the safe fallback when a per-symbol second is unavailable |
| Side effects | repository, Telegram, outbox, scheduler | absent from replay driver; only in-memory state and transition ledger |

The climax fast monitor is intentionally outside this BASELINE pipeline.

## State contract

| State | Entry | Reads/writes and expiry |
|---|---|---|
| `IDLE` / absent | no active event | reads no event; a qualifying `PumpDetector` observation creates a new identity |
| `PUMP_DETECTED` | pump return/stretch qualifies | event high/base, trigger window and `expires_at` are written; pullback tracker may advance or expire |
| `PULLBACK_OBSERVED` | tracker observes a valid pullback | pullback timestamp/depth/low are written; a confirmed new high resets the event |
| `SHORT_ZONE_ACTIVE` | price enters `ShortZoneBuilder` zone | zone bounds are written; evaluator is eligible |
| `SIGNAL_SENT` | evaluator is actionable and replay marks the signal | synthetic replay signal id is recorded; terminal |
| `EXPIRED` | timeout, kill price, or signal-sent timeout | terminal; no further evaluation |

Transitions are immutable records containing symbol, event identity, transition
time, from/to states, trigger, source market time, reason and stable sequence.
At a dataset cutoff an otherwise active event is marked
`ACTIVE_AT_DATASET_CUTOFF`; an unavailable source interval is represented as
`DATA_GAP_CENSORED`, never silently filled.

For every event identity the replay ledger satisfies the terminality equation:
`terminal transition ∈ {EXPIRED, SIGNAL_SENT, ACTIVE_AT_DATASET_CUTOFF,
DATA_GAP_CENSORED}` exactly once; an event cannot disappear without one of
these classifications.

## Evaluation artifact

`BaselineStrategyInput` remains the Phase 3 EvaluationRecord contract.  Phase 4
adds artifact metadata around each input: UTC decision time, maximum input
availability, event/context identity, canonical input fingerprint and
observability flags.  `max_input_availability_time_utc` must never exceed the
decision time.  Historical orderbook evidence is nullable; absent evidence is
`FULL_ADMISSION_HISTORICALLY_UNOBSERVABLE`, not a false liquidity result.

## Replay gates and mismatch taxonomy

The staged workflow is `fetch-controls → normalize-controls → replay-controls →
verify-controls → fetch-broad → replay-broad → build-bundle`.  Broad stages
require a sealed `CONTROL_REPLAY_PASS` report for both TRUMP controls.  A
mismatch is reported at the first divergent layer in this order:

`RAW_DATA → CLOCK → STATE_TRANSITION → FEATURE → EVALUATION_RECORD →
EVALUATOR → HISTORICAL_UNOBSERVABLE`.

Expected/actual values, first divergence time, field/state and production
function are retained in the control report.  No coarse replay result is used
as a baseline.

## Bundle additions

Phase 4 bundles remain ReplayBundle v1 compatible and may include
`market_data/asof_candles_1m.parquet`,
`state/baseline_transitions.parquet`, and
`inputs/evaluation_records.parquet` (an alias of the existing strategy-input
artifact).  The manifest and hashes seal every artifact, including normalized
source provenance and row counts.  Raw archives are not committed to Git.
