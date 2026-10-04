# Release matrix

## Public release boundary

The public repository is a sanitized research baseline with independent Git history. Its code, tests, formulas, examples, and replay contracts are the only release artifacts verified by this repository.

| Layer | Status | What can be reproduced |
|---|---|---|
| Public baseline | Verified in a clean checkout | Source tests, deterministic fixtures, replay contracts, and documentation |
| External deployment | Not verified here | Requires a separately pinned checkout, effective configuration, credentials, database, and runtime evidence |
| Historical research | Labeled per artifact | Only the stated cohort, timestamps, and coverage boundary |

## Public boundary

This public mirror does not publish production lane names, release identifiers, host details, database paths, PIDs, private configuration, Telegram routing, or deployment timings. External deployment claims must not be inferred from this repository.

## Manual execution contract

All public signal output is informational and manual-readable. There is no order-placement interface in this export.

```text
Autoexecution: OFF
```

## Release review checklist

Before using a checkout outside local research, verify:

1. exact commit and dependency versions;
2. effective configuration and secret injection outside Git;
3. market-data source, freshness, continuity, and universe boundaries;
4. database schema and migration compatibility;
5. complete tests and deterministic replay coverage;
6. delivery toggles and outbox behavior;
7. runtime health and rollback artifact.

A rollback is an external release-selection operation. It is not performed by rewriting this public repository's history.
