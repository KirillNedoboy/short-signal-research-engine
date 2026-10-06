# Source of truth

## Purpose

This document prevents the public GitHub checkout, a server release, and research artifacts from being presented as the same system.

## Evidence labels

- **Baseline-verified:** directly verified in the public checkout at `ce77d744`.
- **Production-reported:** supplied from the operator's production release inventory; the release object is not present in this shallow public clone.
- **Historical/experimental:** retained for comparison; not active production.
- **Not verified:** a claim for which this repository has no reproducible artifact.

## Release map

| Layer | Identifier | Date | Status | Evidence boundary |
|---|---|---:|---|---|
| Public GitHub baseline | `ce77d744` | 2026-09-08 | Baseline-verified | Source files and tests in this repository |
| Lane A production | `bf47d2b1d2b0b159da780dc337adefe3dcd2b6e71c13c2506721c4c816be92ea` | 2026-09-14 | Production-reported | External operator release artifact; not a Git object in this clone |
| Lane B | `dfcdb9df3e6c6a3667fa914e3ea0189f7a6f8e35db43ac530ba8db51b69f7553` | 2026-09-17 | Active secondary lane / experimental | External operator release artifact; not a Git object here; isolated config/database |

The short identifiers are documentation aliases for the release identifiers above, not claims that those objects are reachable from `origin/main`.

## Canonical sources by topic

| Topic | Baseline source | Production status | Notes |
|---|---|---|---|
| Feature formulas | `app/features/` | Verify against the production artifact before applying operationally | This document describes baseline implementation exactly. |
| Strategy gates | `app/signals/`, `app/events/` | Production overlay may differ | See `STRATEGIES.md`. |
| Baseline score/grade | `app/signals/scoring.py`, `app/signals/engine.py` | Release-specific | Do not infer production settings from `config.example.yaml`. |
| Market-data semantics | `app/market_data/`, `app/features/builder.py` | Release-specific | See `MARKET_DATA.md`. |
| Persistence and delivery | `app/storage/`, `app/runtime/`, `app/notifications/` | Production additive warning backport reported | See `DATA_MODEL.md` and `DELIVERY.md`. |
| Deployment | `deploy/`, `docs/deployment.md` | Private operator runbook | Paths and credentials are intentionally abstracted here. |
| Research/shadow | `research/`, `docs/SHADOW_VALIDATION.md` | Non-production | Never treat shadow observations as live signals. |

## Current production topology

As of the latest operator verification, both isolated lanes are active simultaneously:

- Lane A `bf47d2b1` — primary production lane;
- Lane B `dfcdb9df` — secondary experimental lane.

Lane A currently uses:

- `shortlist_size: 132`;
- `min_24h_volume: 2,000,000`;
- `AUTOEXECUTION=OFF`;
- manual entry only;
- `EARLY_DROP_WARNING` as a separate non-actionable stream.

The 132-symbol / `$2M` configuration was activated after a bounded rollout. Initial post-change windows completed in approximately 15.9–19.7 seconds with all 132 symbols terminally processed and no `DEADLINE_EXCEEDED`; one fast-monitor frame-prefetch timestamp error occurred during warmup and subsequent polls recovered.

Lane B uses its own release/config/database contour, a 50-symbol / `$5M` shortlist mode, and the split climax evaluators. Its `TRAPPED_LONGS_REVERSAL` evaluator is enabled, while `trapped_longs_live_delivery_enabled` remains false in the effective B configuration.

The two lanes must not share a database, limiter, routing configuration, or release working directory.

The later `111`-symbol / `$3M` candidate is not current production. Its rollout was stopped after incomplete-data and cycle-capacity observations.

## Non-negotiable interpretation rules

```text
NO_SETUP is valid only after SCANNED_OK.
EARLY_DROP_WARNING != SHORT_SIGNAL.
WATCH != ACTIONABLE_SHORT_SIGNAL.
AUTOEXECUTION=OFF.
```

No production database, Telegram routing identifier, token, private URL, or credential is part of this repository.
