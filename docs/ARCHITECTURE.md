# Architecture

## System purpose

Short Telegram Bot is a deterministic market-monitoring service for Bybit USDT perpetuals. It converts current market snapshots and closed-candle history into event state, strategy evaluations, persisted signal intent, Telegram delivery, and later outcome measurements.

It has three separate concerns:

1. **Market observation** — fetch and normalize exchange data.
2. **Decisioning** — detect events and evaluate strategy contracts.
3. **Evidence and delivery** — persist decisions/outcomes and deliver only explicitly enabled live strategies.

No module places an exchange order.

## Runtime topology

`ShortSignalBot` in `app/main.py` composes:

- validated YAML/environment configuration;
- SQLite/SQLAlchemy database and heartbeat;
- Bybit client, request scheduler and rate limiter;
- market scanner and shortlist builder;
- event/pullback/short-zone state machines;
- feature builder;
- baseline and independent climax strategy branches;
- repository and durable Telegram outbox;
- outcome tracker and shadow-observation scheduler.

The deployed service is normally a single systemd process. The process is asynchronous, but persistence is the source of truth for event state, signals, observations, delivery intent and outcomes.

## Data flow

1. Load active event state and due outbox work.
2. Fetch Bybit tickers/instruments and filter the USDT-perpetual universe.
3. Rank the shortlist; retain active-event symbols even if they leave the current shortlist.
4. Fetch ascending 1m OHLCV frames.
5. Exclude forming candles from closed-candle structure calculations.
6. Fetch optional OI/funding/orderbook data without converting missing evidence into a pass.
7. Build `SymbolFeatures`.
8. Advance pump, pullback, high-reset and short-zone state.
9. Evaluate baseline and independent climax branches.
10. Persist event/evaluation/observation state.
11. Persist actionable signal plus immutable Telegram outbox payload transactionally.
12. Send due outbox items with bounded leases/retries.
13. Refresh due signal and research outcomes.
14. Record health and scan telemetry.

## Component map

| Layer | Modules | Responsibility |
|---|---|---|
| Config | `app/config.py` | YAML, `.env` overrides, validation and fingerprints |
| Market | `app/market/*` | Bybit REST, instruments, candles, derivatives, orderbook |
| Features | `app/features/*` | ATR, VWAP, EMA, RSI, volume and candle structure |
| Events | `app/events/*` | Pump, pullback, short-zone and lifecycle state |
| Signals | `app/signals/*` | Filters, scoring, risk flags, strategy branches and formatting |
| Storage | `app/storage/*` | SQLite bootstrap, ORM models and repository transactions |
| Delivery | `app/notifications/*` | Telegram transport and alert throttling |
| Outcomes | `app/outcomes/*` | Signal and shadow outcome evaluation |
| Research | `research/*`, `scripts/*` | Replay, reports, exports and read-only analysis |

## Boundary rules

- Strategy-specific gates belong in the strategy branch; shared thresholds must not be changed to fix one strategy.
- Shadow decisions never route through live Telegram delivery.
- A WATCH candidate is not an actionable signal.
- Missing OI/liquidity/candle evidence is explicit unknown or veto context, never an implicit pass.
- SQLite schema changes require a migration and regression suite; observation code must not create ad-hoc schemas.
- Delivery occurs after persistence, never before.
- Outcome metrics are evidence, not execution fills or profitability proof.

## Failure containment

The runtime uses bounded external requests, retry/backoff, request pacing, DB heartbeat checks, outbox leases, delivery retries, operational alert throttling and disk-capacity guards. A failed shadow-observation write must not turn a WATCH/shadow row into a live signal.

## Deployment model

The repository contains application code and examples. Production service files, real config, databases and runtime reports are deployment artifacts. Use the example systemd unit under `deploy/` and keep production paths/operator credentials outside Git.
