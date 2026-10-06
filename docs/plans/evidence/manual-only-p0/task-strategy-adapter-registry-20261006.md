# Task 9 — strategy adapter registry parity evidence

- **Task:** 9 — capture strategy parity and update documentation
- **Evidence date:** 2026-10-06
- **Evidence commit:** `c78c8a4521c6d51285433eeb0a3a38da04d350e0` (`c78c8a4`)
- **Scope:** documentation and disposable-test evidence only
- **AUTOEXECUTION:** `OFF`; manual entry only
- **Private exchange/order APIs:** none used or introduced
- **Production deployment/restart:** none

## Canonical identifier set

The registry is required to contain exactly five identifiers, in this order:

```text
BASELINE_PULLBACK
CLIMAX_EXHAUSTION
VOLUME_CLIMAX_UNWIND
LOW_VOLUME_EXTENSION_FAILURE
TRAPPED_LONGS_REVERSAL
```

`CLIMAX_EXHAUSTION` is the umbrella evaluator for the climax bundle; the two
climax branches retain their own canonical identifiers. `TRAPPED_LONGS_REVERSAL`
is the registry identity for the trapped-longs reversal adapter.

## Parity claim and boundary

The adapter architecture is a pure translation boundary over the existing
evaluators. It does not alter formulas, thresholds, score/grade rules, event
lifecycle, closed-candle semantics, final delivery recheck, persistence,
Telegram routing, or manual-only execution policy.

The parity claim is limited to disposable local tests at the evidence commit.
The required strategy-behavior diff is empty for the registered set: no
strategy identity/subtype, model version, score, grade, actionability,
reason/blocker/veto, risk flag, metadata, lifecycle-relevant value, or strategy
configuration fingerprint was changed by the adapter integration tests.

Adapters do not call repositories, Telegram clients, exchange clients, order
methods, or execution components. `SignalDecision` remains a decision and
persistence input; it does not place an exchange order. No private exchange API,
credential, private destination, production path, configured database, runtime
artifact, or historical return promise is included in this record.

## Verification results at `c78c8a4`

Exact recorded results:

| Check | Result |
|---|---|
| Task 8 focused delivery/invariant suite | **24 passed** |
| Full test suite | **825 passed** |
| `python3 -m compileall -q app scripts tests` | **passed** |
| `git diff --check` | **passed** |
| `ruff check app tests` | **unavailable** — Ruff is not installed in the environment |

The Task 8 and full-suite counts are the results recorded for HEAD
`c78c8a4`; this documentation-only task does not reinterpret them as runtime or
production evidence. Markdown link-consistency tooling was not installed; the
repository-relative links added here were checked structurally and point to
tracked documentation paths.

## Explicit non-claims

- No production deployment or service restart occurred.
- No configured or production database was opened or changed.
- No live Telegram destination was contacted.
- No credentials or private operational paths were inspected or recorded.
- No private exchange/order API was used.
- No order, position, leverage, risk, or autoexecution behavior was added.
- Parity evidence does not claim profitability, execution returns, or future
  performance.
