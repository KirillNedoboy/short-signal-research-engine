# Release matrix

## Verified and reported releases

| Release | Date | Lane | Status | Universe/config | Delivery | Result |
|---|---:|---|---|---|---|---|
| `ce77d744` | 2026-09-08 | Public baseline | Verified in this clone | Baseline defaults; compare `app/config.py` and `config.example.yaml` | Baseline live branches; shadow branches marked separately | Public source of truth for checked-in code |
| `bf47d2b1...` | 2026-09-14 | Lane A | Active primary lane | 132 symbols; `$2M` minimum 24h volume after bounded rollout | Manual short signals plus additive separate warning stream | Current active configuration; initial windows 15.9–19.7s |
| `dfcdb9df...` | 2026-09-17 | Lane B | Active secondary lane | 50 symbols; `$5M`; stricter limiter/resource policy | Split climax evaluators; trapped-longs delivery disabled | Active alongside Lane A; observed deadline pressure remains |
| candidate | 2026-09-20 | A/B candidate | Rejected/rolled back | 111 symbols; `$3M` minimum volume | Not activated as production | Cycle duration and incomplete-data capacity gate failed |

Full release identifiers are recorded in `SOURCE_OF_TRUTH.md`. The two production identifiers are external artifacts and are not Git objects in this public shallow clone.

## Current production claims

The current operator-verified state has Lane A and Lane B active simultaneously as isolated lanes. Lane A is currently 132 symbols / `$2M`; Lane B remains 50 symbols / `$5M`. Ordinary short signals remain manual-entry notifications; no order-placement path is part of the contract. `EARLY_DROP_WARNING` is a separate non-actionable observation and does not enter ordinary short admission.

## Why the 111-symbol candidate was not promoted

The candidate produced scan windows around 32.1–36.4 seconds and incomplete-data observations against the operational cycle target. The decision was to retain the stable 100-symbol / `$5M` mode rather than claim that the candidate was production-ready.

## Version comparison

- **Baseline:** public code and tests; formulas and state logic are documented from checked-in source.
- **Lane A production:** operator-reported release with the stable universe and additive warning backport.
- **Lane B:** separate active release tree with stricter request/resource settings and experimental branches; it is not an alias for Lane A and uses its own database/configuration.
- **Candidate:** tested but not promoted; never document it as active runtime.

## Rollback contract

A rollback is a release-selection operation, not a source rewrite. Before switching a service, verify the release identifier, config provenance, database compatibility, migration status, service state, heartbeat, data connection, scan completion, and delivery outbox. For a dual-lane topology, switch or stop only the named lane unless the operator explicitly requests a topology change. Do not delete or rewrite production SQLite. See `DEPLOYMENT.md`.
