# Anima Review: `VOLUME_CLIMAX_UNWIND`

- **Run time:** 2026-07-22T15:47:48Z
- **Repository:** `/opt/short-telegram-bot-lite`
- **Commit reviewed:** `a4160e4c099a02118e1c27bc7a2619bc255e235a`
- **Mode:** read-only multi-interpretation review
- **Scope:** shadow lifecycle before live admission
- **Verdict:** `REVISE`

## Protocol state

This cycle used the Anima-style incubator loop:

1. define proof and scope;
2. run one short, verifiable review cycle;
3. obtain independent interpretations;
4. separate facts, hypotheses, and blockers;
5. produce one synthesis artifact;
6. do not promote before validation gates pass.

No live signal, order, service restart, or external mutation was performed.

## Baseline evidence

- Working tree was clean at review start.
- `HEAD` was `a4160e4c099a02118e1c27bc7a2619bc255e235a`.
- Full local suite: `118 passed in 4.00s`.
- `compileall`: passed.
- `git diff --check`: passed.
- Local SQLite, shadow CSVs, and the existing AKEUSDT replay fixture were available.
- Current implementation is shadow-only; live Telegram admission is not replaced by the lifecycle.

## Interpretation A — trader

### Confirmed strengths

- `root_created_at` remains immutable when a new high appears.
- New high increments `event_revision`, updates `latest_high`, and restarts only the confirmation window.
- Fallback requires two closed-candle observations, a closed window, no new high, no renewed acceleration, no active squeeze, acceptable OI, rejection, liquidity, and entry-distance gates.
- The clean lifecycle function handles the intended ERA-style sequence in a deterministic smoke test.

### Trader risks

- A fast reversal during the first minute or before two closed candles remains `CLIMAX_WATCHING`; this is the chosen protection against premature entry, but it is a measurable latency trade-off.
- The current production evaluator uses the latest frame and does not prove that the last 1m candle is closed.
- `latest_failed_retest` can suppress both acceleration and squeeze vetoes even while price/OI still indicate continuation.
- The lifecycle is not currently a full independent shadow lifecycle: rejected live candidates do not enter it.

### Trader patch candidates

1. Make rejected-but-eligible volume-climax candidates observable in shadow without changing live admission.
2. Separate `rejection_confirmed`, `price_acceleration_resumed`, and `active_short_squeeze`.
3. Add a partial-candle regression fixture.
4. Tune `max_entry_distance` only after cost-adjusted replay, not from the 3–5% hypothesis alone.

## Interpretation B — systems

### Confirmed strengths

- EventState snapshot stores lifecycle timestamps, revision, state, and vetoes.
- SQLite stores root event, attempts, attempt events, and climax evaluations.
- Restart/reuse of an existing attempt is covered at the storage level.
- Root lifetime is anchored to the original event timestamp.

### Systems risks

1. `climax_root_events` stores only the latest peak and revision counter; previous peak values are overwritten. There is no append-only root revision table.
2. Lifecycle attempts can collide with the existing shadow attempt namespace (`root:rN:a1`) and overwrite state semantics from another shadow family.
3. Root row, EventState snapshot, and attempt are written in separate database sessions; a crash between writes can leave revision state inconsistent.
4. `EXPIRED` is stored in the lifecycle snapshot but does not necessarily make the enclosing EventState terminal.
5. Persistence errors can fall back to revision `1`, which is unsafe for monotonic lifecycle state.
6. The configured `climax_max_attempts_per_root_event` limit is not enforced in the reviewed lifecycle path.

### Systems patch candidates

1. Use a namespaced attempt ID, for example `volume_climax:{root}:r{revision}:a1`.
2. Add append-only `climax_root_event_revisions` with peak, timestamp, revision, and observation metadata.
3. Add monotonic reconciliation between SQLite root state and EventState snapshot on restart.
4. Make persistence failures fail closed for shadow state instead of silently returning revision `1`.
5. Add integration tests for restart, attempt collision, partial persistence, and expiry cleanup.

## Interpretation C — research

### Confirmed strengths

- A research replay harness exists and its tests pass.
- ERAUSDT is present in local SQLite/CSV data, including an historical `SQUEEZE_BEFORE_TP` outcome.
- The existing AKEUSDT fixture demonstrates that different interpretations can produce different confirmation times and outcomes.

### Research blockers

- Existing replay does not execute the production lifecycle end-to-end.
- Replay metrics are raw price movement; they do not include taker/maker fees, funding cash flow, slippage, spread crossing, depth impact, partial fills, latency, or leverage/liquidation.
- No temporal out-of-sample split exists.
- Repeated evaluations of one root event are not independent observations.
- The production scanner does not explicitly exclude the current unclosed 1m candle; `timestamp <= asof` is insufficient proof of closure.
- ERAUSDT raw candles/OI/orderbook snapshots are not stored immutably per evaluation, so historical reconstruction is incomplete.

### Minimum research gate

1. Add an explicit closed-candle boundary.
2. Replay the actual production lifecycle, not only the separate M1–M4 research models.
3. Add execution accounting: fee, funding, spread, slippage, depth, and latency.
4. Use one row per independent root/attempt and report denominator plus missing/unknown counts.
5. Validate on a time-separated holdout and multiple symbols.
6. Add an ERAUSDT fixture with the intermediate high sequence and fast-reversal/squeeze branches.

## Cross-interpretation synthesis

All three interpretations agree on the core direction:

> Early climax should be a candidate, not an immediate short entry.

They also agree that the current commit is not ready for promotion because the production integration does not yet provide reliable evidence for every eligible candidate and because execution/replay validity is incomplete.

## Decision

```text
REVISE — do not promote to live admission
```

### Required next patch batch

1. Run lifecycle shadow for eligible volume-climax candidates before live-admission filtering, while keeping live signals unchanged.
2. Replace hardcoded `3` and `2` with configuration parameters everywhere.
3. Namespace volume-climax attempts separately from other shadow families.
4. Correct lifecycle-specific telemetry, including `decision_delta`.
5. Add explicit closed-candle filtering and partial-candle tests.
6. Add append-only revision history or an equivalent durable audit trail.
7. Enforce per-root attempt limits and fail closed on persistence inconsistency.
8. Build a cost/latency-aware, time-split replay before considering live activation.

## Validation-gate status

| Gate | Status | Evidence |
|---|---:|---|
| Repeatability | `pending` | Current lifecycle unit tests pass, but independent market-event cohort is insufficient |
| Operational value | `candidate` | Addresses the ERA-style early-entry failure, not yet proven OOS |
| Specificity | `pass` | Lifecycle and patch candidates are concrete |
| Reversibility | `pass` | Shadow-only and live admission unchanged |
| Constitution fit | `pass` | Evidence-first, fail-closed, no secrets or live execution |
| No raw-log leakage | `pass` | Report contains summarized evidence only |

## Next Anima step

The next cycle should be one narrow implementation/review step:

> Fix shadow admission ordering, configuration wiring, and attempt namespace; then rerun the full suite plus lifecycle integration tests.

Do not activate live admission in that cycle.
