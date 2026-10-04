# Data Model and Evidence

## Storage

The default store is SQLite with WAL and a busy timeout. The repository layer owns transactions and DTO-oriented reads/writes. Production databases are never committed to Git.

Core entities include:

- `event_states` — persistent event and pullback lifecycle;
- `signals` — actionable persisted signal records;
- `signal_outcomes` — post-signal MFE/MAE and classification;
- `strategy_observations` — append-only strategy evaluation evidence, including blocked/low-score branches;
- `telegram_delivery_outbox` — immutable delivery intent, lease/retry state and send status;
- scan/reject/coverage telemetry tables — operational denominators and diagnostics.

## Transactional delivery

For an actionable live signal, persistence follows:

```text
signal row + immutable outbox intent → Telegram attempt → SENT/RETRY/DEAD
```

The outbox is at-least-once. A lost acknowledgement after Telegram accepted a message can produce a duplicate retry. It is not exactly-once delivery.

Legacy unsent rows are not silently auto-enqueued. Delivery status must be read from the outbox and source signal together.

## Observation evidence

Shadow observations are diagnostic and must include strategy, symbol, root/revision, phase, market timestamp, input fingerprint, model/config identity and bounded canonical evidence. Repeated evaluations are not independent setups; use `root_event_id` as the primary independence key.

Unknown, incomplete and unavailable market evidence remain explicit states. They must not be converted to zero or pass.

## Outcome semantics

Outcome calculations distinguish short-side MFE and MAE. Required horizons depend on the evaluation contract; the Strategy-4 cohort uses 15m, 1h and 4h horizons where coverage exists. Missing future candles yield incomplete/unknown results.

Persisted outcomes are research metrics, not executable fills. They do not automatically model fees, funding cash flow, latency, leverage, partial fills or liquidation.

## Data hygiene

Never store in Git:

```text
.env
config.yaml with real values
*.sqlite
*.sqlite-wal
*.sqlite-shm
API keys/tokens/passwords/private keys
raw Telegram exports or credential-bearing logs
```

Use `config.example.yaml` and `.env.example` as safe templates.
