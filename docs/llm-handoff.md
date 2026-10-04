# LLM handoff guide

Use the current architecture, pipeline, and data-model documents below as the source of truth for runtime behavior. Historical research reports are evidence, not current architecture documentation.

## Mission

This is a read-only Telegram signal bot for Bybit USDT perpetual contracts. It scans markets, detects post-pump short setups, scores signals, sends alerts, and stores observations/outcomes. It does not place orders automatically.

Live V1 strategies are `BASELINE_PULLBACK`, `VOLUME_CLIMAX_UNWIND`, and `LOW_VOLUME_EXTENSION_FAILURE`; their delivery gates default to `true`. `VOLUME_CLIMAX_LIFECYCLE_SHADOW_V2` is research-only. WATCH Telegram delivery defaults to `false`.

Telegram uses the persistent `telegram_delivery_outbox` with at-least-once delivery. A retry can duplicate a message after an ambiguous network acknowledgement.

`strategy_observations` is a separate append-only research ledger. It records every enabled `CLIMAX_EXHAUSTION` evaluator branch for `INITIAL` and actual delivery-recheck phases, including rejected and low-score cases. It is best-effort telemetry: a write failure must not change live selection, signals, outbox delivery, or event state. `BASELINE_PULLBACK` is outside this first instrumentation scope.

## Read first

1. `README.md` — purpose, quick start, and boundaries.
2. `docs/current_bot_architecture.md` — module/data-flow map.
3. `docs/current_bot_signal_pipeline.md` — event-to-signal lifecycle.
4. `docs/current_bot_data_model.md` — SQLite entities and persistence.
5. `docs/current_bot_score_tier_map.md` — score/tier semantics.
6. `config.example.yaml` and `.env.example` — complete safe configuration surface.
7. `tests/` — executable behavior contract.

## Main code paths

- `app/market/` — Bybit client, candles, coverage, scanner, shortlist.
- `app/events/` — pump detection, short-zone state, pullback tracking.
- `app/signals/` — features, scoring, filters, formatting, risk flags.
- `app/outcomes/` — signal outcome tracking/evaluation.
- `app/storage/` — SQLite models, repository, and database access.
- `app/notifications/` — Telegram delivery and throttling.
- `scripts/` — live, one-shot, reporting, and export entrypoints.

## Expected workflow for changes

1. Inspect the relevant module and its tests.
2. Add or update a focused test first for behavior changes.
3. Run `.venv/bin/pytest -q`.
4. Update docs/config references.
5. Review the diff for secrets and accidental runtime files.
6. Keep order execution out of scope unless explicitly redesigned.

## Questions an LLM should ask before changing behavior

- Is this changing market selection, event detection, signal eligibility, scoring, or notification behavior?
- Does the change alter the meaning of an existing persisted state or outcome?
- Is the behavior safe when Bybit data is delayed, incomplete, rate-limited, or contradictory?
- Is the change covered by a regression test?
- Does any new configuration need a safe default and documentation?
