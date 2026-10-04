# Public Short-Signal Research Bot

This repository contains a sanitized, research-oriented implementation of a short-side market-monitoring and manual-signal pipeline. It consumes supplied or public market data, evaluates deterministic strategy contracts, persists local observations, and can render human-readable Telegram messages.

## Repository scope

- Feature construction and strategy evaluators under `app/features/` and `app/signals/`.
- Event and lifecycle state under `app/events/` and `app/baseline/`.
- Replay contracts and deterministic evaluators under `app/replay/`.
- Market-data normalization and data-quality handling under `app/market/`, `app/market_data/`.
- Regression and contract tests under `tests/`.
- Review-oriented documentation in `docs/` and `docs/trader-review/`.

## Signal lifecycle

1. Market snapshots and closed candles are acquired or loaded from a fixture.
2. Features, coverage, freshness, and liquidity evidence are built.
3. Event state advances through the configured lifecycle.
4. Strategy gates, vetoes, and scoring produce an outcome such as actionable, WATCH, or data-quality rejection.
5. The decision and delivery intent can be persisted locally.
6. A manual-readable Telegram message may be rendered when explicitly configured.

This export does not include live destinations or credentials.

## V1 and V2

V1 is the baseline source-faithful signal contract. V2 research adds stricter lifecycle/admission and evidence concepts while remaining review/replay material. See `docs/trader-review/v1-v2-comparison.md` and `docs/trader-review/strategy-matrix.md`.

## Manual-only boundary

**Autoexecution: OFF.** There is no order-placement interface, wallet integration, exchange credential, or automatic execution path in this public export. Telegram output is informational/manual review only.

## Local setup

Use Python 3.12+, create a virtual environment, and install `requirements.txt`:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -m compileall -q app tests
PYTHONPATH=. pytest -q
```

Network access is not required for the unit and contract tests; tests use fakes and fixtures where applicable. Runtime integrations require separately supplied credentials and endpoints and are intentionally outside this export's operational scope.

## Replay and limitations

Replay consumes local fixtures or explicit bundles; it does not read production databases or private paths. Results are bounded by fixture coverage, timestamps, data availability, and the selected contract. A replay result is not a profitability claim or a forecast. See `docs/architecture/replaybundle.md`, `docs/TESTING.md`, and `docs/trader-review/evidence-and-results.md`.

## Evidence taxonomy

- **Verified:** behavior directly exercised by source tests or deterministic replay in this repository.
- **Historical:** supplied research context with stated time window and denominator; not current production proof.
- **Experimental:** candidate or shadow behavior requiring further replay/live-cohort validation.
- **Unknown:** coverage or timing not established by public fixtures.

## Security boundary

The export intentionally omits `.env` files, real Telegram IDs/tokens, exchange credentials, private keys, databases/WAL/SHM, logs, caches, backups, production hostnames/PIDs, private telemetry, and absolute operator paths. Copy `.env.example` and `config.example.yaml` only as schemas; keep actual values outside version control.

## Contribution

Read `CONTRIBUTING.md`, `SECURITY.md`, `docs/SOURCE_OF_TRUTH.md`, and `docs/trader-review/README.md` before changing strategy behavior. Add or update deterministic tests with behavior changes. Keep manual-only semantics and explicit evidence boundaries intact.
