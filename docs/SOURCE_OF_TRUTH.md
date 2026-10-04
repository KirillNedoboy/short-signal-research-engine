# Source of truth

## Purpose

This document separates the public checkout, optional external deployments, and research artifacts. Only the public checkout is reproducible from this repository.

## Evidence labels

- **Baseline-verified:** behavior exercised by the source files and tests in this checkout.
- **Historical:** retained research context with a stated time window and denominator.
- **Experimental:** shadow or replay behavior that requires additional validation.
- **Unknown:** coverage or timing not established by public fixtures.
- **External/not verified:** deployment state, effective production configuration, and private runtime evidence outside this repository.

## Public baseline

The public `main` branch is the source of truth for the code, formulas, contracts, examples, fixtures, and tests committed here. The repository intentionally does not claim that this checkout is identical to any private deployment.

## Canonical sources by topic

| Topic | Canonical public source | Evidence boundary |
|---|---|---|
| Architecture | `docs/ARCHITECTURE.md` | Baseline-verified from checked-in code |
| Signal pipeline | `docs/SIGNAL_PIPELINE.md` | Baseline-verified; delivery remains manual-only |
| Feature formulas | `docs/MATHEMATICS.md`, `app/features/` | Baseline-verified; thresholds may be configuration-dependent |
| Market data | `docs/MARKET_DATA.md`, `app/market/`, `app/market_data/` | Bybit integration contract and local tests |
| Strategy gates | `docs/STRATEGIES.md`, `app/signals/`, `app/events/` | Baseline contracts; effective external overlays are not published |
| Persistence and delivery | `docs/DATA_MODEL.md`, `docs/DELIVERY.md`, `app/storage/`, `app/notifications/` | Local persistence and manual delivery semantics |
| Configuration | `docs/CONFIGURATION.md`, `config.example.yaml` | Example schema only; no operator configuration |
| Replay | `docs/architecture/replaybundle.md`, `app/replay/`, `research/` | Fixture/bundle coverage only |
| Trader review | `docs/trader-review/` | Review package; claims link back to code or labeled evidence |
| Testing | `docs/TESTING.md`, `tests/` | Reproducible from a clean checkout |

## External deployments

Private deployments may use different releases, thresholds, universes, credentials, databases, service topology, or delivery toggles. Those values are intentionally not recorded here. Before applying the public code operationally, pin the exact checkout, review the effective configuration, validate data coverage, and run the complete test suite.

No production database, Telegram routing identifier, token, private URL, credential, host path, PID, or private runtime report is part of this repository.

## Non-negotiable interpretation rules

```text
NO_SETUP is valid only after SCANNED_OK.
EARLY_DROP_WARNING != SHORT_SIGNAL.
WATCH != ACTIONABLE_SHORT_SIGNAL.
AUTOEXECUTION=OFF.
```
