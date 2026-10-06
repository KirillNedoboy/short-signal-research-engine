# Strategy adapter registry architecture

Status: canonical architecture for the strategy registry at commit `c78c8a4`.
This document describes the code and test boundary; it does not authorize a
production deployment or a change to strategy behavior.

## Canonical registry identifiers

The explicit registry contains exactly these five identifiers, in deterministic
order:

1. `BASELINE_PULLBACK`
2. `CLIMAX_EXHAUSTION`
3. `VOLUME_CLIMAX_UNWIND`
4. `LOW_VOLUME_EXTENSION_FAILURE`
5. `TRAPPED_LONGS_REVERSAL`

`CLIMAX_EXHAUSTION` is the umbrella evaluation for the climax bundle. Its
selected subtype is one of the registered climax branches when a branch is
selected; `VOLUME_CLIMAX_UNWIND` and `LOW_VOLUME_EXTENSION_FAILURE` also have
explicit branch adapters. `TRAPPED_LONGS_REVERSAL` is the canonical registry
name for the trapped-longs reversal evaluator; legacy evaluator metadata does
not change that registry identity.

The registry is explicit, not import-scanned. Construction rejects an unknown
identifier, duplicate registration, an adapter whose `strategy_type` does not
match its key, or an incomplete five-identifier set. Lookup and evaluation
order are stable so parity records can compare the same sequence every time.

## Evaluation contract

`StrategyContext` is an immutable, canonical input containing:

- symbol and event identity;
- frozen `EventState`, `SymbolFeatures`, and optional `ShortZone`;
- timezone-aware decision timestamp and ordered market-data timestamps;
- strategy configuration fingerprint;
- normalized input-quality state.

Each adapter implements the side-effect-free `StrategyAdapter` protocol:

```text
evaluate(context: StrategyContext) -> StrategyEvaluation
```

`StrategyEvaluation` is immutable and contains strategy identity, subtype, model
version, actionability, score, grade, reasons/blockers/vetoes, risk flags,
bounded metadata, and the context identity. Baseline subtype normalization and
the legacy model-version policy are applied at this boundary. Invalid or
blocked input cannot remain actionable.

The adapter layer is a translation boundary, not a second strategy engine:

- `BaselineAdapter` delegates to the existing baseline evaluator;
- `ClimaxAdapter` delegates to `evaluate_climax_bundle`;
- the two climax branch adapters select the corresponding bundle branch;
- `TrappedLongsAdapter` delegates to
  `evaluate_trapped_longs_reversal`.

Adapters do not own lifecycle transitions, score formulas, thresholds, final
rechecks, persistence, Telegram delivery, exchange clients, order methods, or
execution state. They do not import repositories, notifications, or exchange
/order APIs. Existing evaluators remain the source of strategy behavior.

## Runtime call flow

```text
validated market snapshot and lifecycle context
  -> StrategyContext
  -> StrategyRegistry.evaluate_all(context)
  -> canonical StrategyEvaluation values
  -> existing strategy selection semantics
  -> SignalDecision projection
  -> final delivery recheck
  -> atomic signal/provenance/outbox/EventState/audit persistence
  -> Telegram outbox delivery
```

The projection to `SignalDecision` copies the selected canonical identity and
model fields; it does not replace the evaluation with a legacy branch result.
The final recheck remains authoritative immediately before persistence. A
recheck block is durable and creates no signal or outbox row. A passing result
uses the existing idempotent persistence bundle and existing delivery retry
semantics.

`SignalDecision` is a decision and delivery-persistence input, not an exchange
order. `AUTOEXECUTION=OFF` remains the manual-only boundary. No private
exchange/order API, position, leverage, risk, or execution path is part of the
registry architecture.

## Parity boundary

Parity means an empty behavior diff for every canonical identifier and branch:
strategy identity/subtype, model version, score, grade, actionability, reasons,
blockers/vetoes, risk flags, metadata, lifecycle-relevant values, and the
strategy configuration fingerprint. The registry adds translation and
selection structure; it does not retune formulas, thresholds, gates, candle
semantics, or delivery policy.

The parity evidence for this task is recorded in
[`docs/plans/evidence/manual-only-p0/task-strategy-adapter-registry-20261006.md`](../plans/evidence/manual-only-p0/task-strategy-adapter-registry-20261006.md).
It contains only disposable test evidence and sanitized identifiers; it does
not contain credentials, private destinations, production paths, configured
DB/runtime artifacts, or historical return promises.

## Operational boundary

This documentation records code and disposable-test verification only.
`AUTOEXECUTION=OFF`; no private exchange/order APIs were used; no production
deployment or service restart was performed. Production configuration,
databases, release paths, credentials, and Telegram destinations remain
outside this repository and outside the parity claim.
