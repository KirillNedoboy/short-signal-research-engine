# Short Telegram Bot

> Documentation status: versioned public baseline with explicitly separated production and historical release overlays.

## Source of truth

Start with [`docs/SOURCE_OF_TRUTH.md`](docs/SOURCE_OF_TRUTH.md). The public checkout is baseline commit `ce77d744`; the operator-reported production release is Lane A `bf47d2b1` from 2026-09-14, and Lane B `dfcdb9df` from 2026-09-17 is historical/experimental. The latter two release artifacts are not Git objects in this shallow public clone.

## Documentation map

- [Mathematical specification](docs/MATHEMATICS.md)
- [Strategies and admission](docs/STRATEGIES.md)
- [Signal pipeline](docs/SIGNAL_PIPELINE.md)
- [Market-data contract](docs/MARKET_DATA.md)
- [Signal and delivery contract](docs/DELIVERY.md)
- [Data model](docs/DATA_MODEL.md)
- [Release matrix](docs/RELEASES.md)
- [Configuration](docs/CONFIGURATION.md)
- [Testing and reproducibility](docs/TESTING.md)
- [Sanitized examples](docs/examples/README.md)

The repository is a signal and research system. It does not place live orders, enable copy-trading, or provide autoexecution. Ordinary short notifications are manual-entry only. The current operator-verified topology runs Lane A and Lane B simultaneously in isolated release/config/database contours. `EARLY_DROP_WARNING` and `WATCH` are non-actionable and never enter ordinary short admission.

A Bybit USDT-perpetual market-monitoring bot that detects short-side reversal and exhaustion setups, persists decisions in SQLite, and delivers human-readable Telegram alerts. **It is a signal and research system, not an order-execution engine.**

## Status

- Runtime: asynchronous single-process poller
- Market data: Bybit REST, closed-candle aware feature pipeline
- Storage: SQLite with WAL and durable Telegram outbox
- Execution: no order placement; `AUTOEXECUTION=OFF`
- Live V1 strategies: `BASELINE_PULLBACK`, `VOLUME_CLIMAX_UNWIND`, `LOW_VOLUME_EXTENSION_FAILURE`
- `TRAPPED_LONGS_REVERSAL`: evaluation/shadow contour; live Telegram delivery remains disabled
- WATCH candidates: non-actionable by default

## Architecture

```text
Bybit REST
   │
   ├─ tickers/instruments ─> universe filter + shortlist
   ├─ 1m klines ────────────> candle normalization + features
   ├─ OI/funding (optional) ─┘
   └─ orderbook (optional) ──> liquidity features
                                      │
                              event/state machines
                                      │
                           strategy evaluation branches
                                      │
                 ┌────────────────────┴────────────────────┐
                 │                                         │
           live admission                         shadow observations
                 │                                         │
          signals + outbox                         outcomes/research
                 │
          Telegram notifier
```

The composition root is `app/main.py:ShortSignalBot`. Detailed component boundaries are in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Strategies

| Strategy | Role | Default delivery |
|---|---|---|
| `BASELINE_PULLBACK` | Mature post-pump pullback inside the configured short zone | Live |
| `VOLUME_CLIMAX_UNWIND` | High-volume exhaustion with unwind/rejection evidence | Live |
| `LOW_VOLUME_EXTENSION_FAILURE` | Weak extension, low volume efficiency and failed high | Live |
| `TRAPPED_LONGS_REVERSAL` | Experimental trapped-long reversal hypothesis | Shadow only |

The authoritative strategy contracts and invariants are in [`docs/STRATEGIES.md`](docs/STRATEGIES.md). Do not infer profitability from score or grade alone.

## Quick start

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
cp config.example.yaml config.yaml
.venv/bin/python scripts/run_once.py
```

Run the service loop only after reviewing configuration and delivery policy:

```bash
.venv/bin/python scripts/run_live.py
```

## Quality gates

```bash
.venv/bin/pytest -q
.venv/bin/python -m compileall -q app scripts tests
.venv/bin/ruff check app scripts tests
```

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — components, runtime flow and boundaries
- [`docs/STRATEGIES.md`](docs/STRATEGIES.md) — strategy contracts and state machines
- [`docs/DATA_MODEL.md`](docs/DATA_MODEL.md) — persistence, outbox and outcome semantics
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md) — deployment, health checks and incident runbook
- [`docs/SHADOW_VALIDATION.md`](docs/SHADOW_VALIDATION.md) — forward cohort and promotion rules
- [`docs/current_bot_signal_pipeline.md`](docs/current_bot_signal_pipeline.md) — detailed signal pipeline
- [`docs/current_bot_score_tier_map.md`](docs/current_bot_score_tier_map.md) — score and grade map
- [`SECURITY.md`](SECURITY.md) — secret handling and operational security
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — development workflow

## Configuration and data hygiene

- `config.example.yaml` is the safe tracked template.
- Real `config.yaml`, `.env`, databases, WAL/SHM files, logs and generated reports stay outside Git.
- Never commit Telegram tokens, API keys, private keys, passwords, chat exports or connection strings.
- The repository deliberately contains no production SQLite database.

## License

Add the project license before public distribution. This repository is intended for private operational development unless explicitly sanitized for publication.
