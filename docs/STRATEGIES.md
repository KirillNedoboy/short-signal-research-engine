# Strategies and admission

This document distinguishes baseline-verified code from production-reported overlays. Source paths refer to the public baseline unless stated otherwise.

## Common contract

Every actionable branch requires trusted market data, a valid lifecycle/event context, strategy hard gates, liquidity/squeeze acceptance, score/grade policy, persistence, and delivery policy. A feature or score alone never creates a signal.

## BASELINE_PULLBACK

Pump detection selects the first eligible 15m/1h/4h horizon and requires a stretch condition. The event tracks base/high/range/expiry. A mature pullback must reach the configured interval, preserve the required VWAP/range-floor behavior, and enter the short zone. Public baseline gates include event state, age, zone membership, VWAP distance, rejection geometry, volume z-score, pullback bounds, score, grade, liquidity, squeeze, and breakout-risk vetoes. See `app/signals/engine.py`, `app/signals/filters.py`, `app/events/`, and `MATHEMATICS.md`.

## VOLUME_CLIMAX_UNWIND (V1 live branch)

Requires usable OI, volume climax evidence, positive short-horizon returns, rejection, bounded entry distance, complete liquidity data, and the configured climax score/grade. This is distinct from the V2 lifecycle telemetry described below.

## VOLUME_CLIMAX_LIFECYCLE_SHADOW_V2

`CLIMAX_WATCHING` and `FALLBACK_READY` are confirmation/research states. A new high creates a revision and restarts confirmation. Missing closed candles, active acceleration, squeeze, OI continuation, weak rejection, bad liquidity, or excessive entry distance hold the candidate. These states do not automatically change V1 live delivery.

## LOW_VOLUME_EXTENSION_FAILURE

Requires a confirmed event, sufficient closed candles, no new high within tolerance, extension, declining volume ratio and efficiency, close below breakout reference, structural failure/retest, no accelerating OI/squeeze, acceptable rejection/distance/liquidity, and score/grade. Any configured veto blocks delivery.

## TRAPPED_LONGS_REVERSAL

The baseline implementation evaluates breakout, at least two closed candles, close below breakout reference, failed retest, OI increase, rejection, no new high, liquidity and expiry. In the public baseline it is disabled for live Telegram delivery and remains an experimental/shadow contour. Do not use its score as evidence of a production signal.

## Scoring and grade

Baseline score uses bounded buckets and risk penalties; baseline engine grade is A at 80+, B at 65+, otherwise C. Climax/trapped-longs flag scores use their own `10 + 15×flags` cap and A/B thresholds. Public delivery policy may require grade B or better and can veto a candidate independently.

## Non-actionable paths

- `WATCH`: candidate withheld by admission or veto; no manual short instruction.
- `EARLY_DROP_WARNING`: separate multi-factor warning; does not enter short admission.
- shadow evaluation: telemetry for research; not a Telegram signal.
- `NO_SETUP`: only after `SCANNED_OK`.

## Release overlay

The current operator-verified topology runs Lane A `bf47d2b1` and Lane B `dfcdb9df` simultaneously in isolated contours. Lane A is the primary production lane. Lane B contains the split climax evaluators and keeps `TRAPPED_LONGS_REVERSAL` live delivery disabled in its effective configuration. Thresholds must be read from the pinned effective release configuration, not copied from this baseline narrative.
