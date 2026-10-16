# Anima Research Review Cycle 8: unified liquidity policy

- **Run time:** 2026-07-22T17:06:31Z
- **Repository:** `/opt/short-telegram-bot-lite`
- **Reviewed baseline:** `a4160e4` plus the current uncommitted unified-liquidity/lifecycle patch
- **Review mode:** read-only; local source, tests, systemd, journal, and SQLite evidence only
- **Live admission:** unchanged by this review
- **Service restart:** not performed
- **Trading/auto-execution claim:** none
- **Decision:** `REVISE` — `PROMOTE: NO`

## Scope and evidence boundary

Cycle 8 reviews the current working-tree patch as a unified liquidity policy and compares it with Cycles 1–7. No files in the application or configuration were changed by this review. The current service is PID `128948`, started at `2026-07-22 08:09:34 UTC`; the changed application files have later modification times. Consequently, the running SQLite/journal sample cannot be attributed to the reviewed patch. Runtime observations are operational evidence only, not trading evidence.

## Fresh local verification

- `.venv/bin/pytest -q`: **124 passed in 4.13s**.
- `.venv/bin/python -m compileall -q app tests research`: exit `0`.
- `git diff --check`: exit `0`.
- Working tree: modified `app/main.py`, `app/signals/climax.py`, `app/signals/engine.py`, `app/storage/repository.py`, and related tests; no application/config changes were made during Cycle 8.
- systemd: `ActiveState=active`, `SubState=running`, `MainPID=128948`, `NRestarts=0`, `ExecMainStatus=0`.
- Journal from the current service start: `2325` lines, `Traceback=0`, `Exception=0`, `sendMessage=0`, `Cycle complete=133`.
- SQLite: `integrity_check=ok`, `journal_mode=wal`, `signals=93`, `climax_evaluations=8403`, `climax_entry_attempts=278`.
- Runtime DB: `volume_climax:%` attempts `0`; `LIVE_REJECTED_SHADOW_FALLBACK_READY` evaluations `0`.

The green suite and healthy process establish code/test and operational facts only. They do not establish runtime execution of the current patch, liquidity selectivity, or OOS performance.

## Unified liquidity policy: grounded assessment

### What is now consistently fail-closed

1. **Missing liquidity is an explicit block in the general signal engine.** `app/signals/engine.py:_liquidity_block_level()` now returns `block` when `features.liquidity_available` is false. The added regression tests assert that missing liquidity produces no actionable signal.
2. **Missing liquidity is an explicit veto for both climax branches.** `app/signals/climax.py` adds `liquidity_not_confirmed` in `_volume_climax()` and `_low_volume()` when liquidity is unavailable.
3. **Lifecycle fallback is independently hard-gated.** `app/main.py` passes `liquidity_ok=features.liquidity_available and not liquidity_warning`; the lifecycle therefore cannot reach `FALLBACK_READY` on missing or warning liquidity. `test_missing_liquidity_blocks_fallback` covers the missing-data case.
4. **The patch preserves the shadow-only boundary.** The lifecycle uses the isolated `volume_climax:{root}:r{revision}:a1` namespace and a lifecycle-specific delta; live admission and Telegram delivery were not enabled.
5. **Persistence is stricter on the reviewed root path.** The repository no longer fabricates revision `1` after the root-event DB exception; terminal attempt close metadata is populated on creation.

### What is unified, and what is not

The patch unifies the most important safety invariant: **unknown/missing liquidity must not become an actionable live signal or a lifecycle fallback**. It does not yet prove one uniform liquidity policy across every lane:

- The general engine distinguishes `ok`, `watch`, and `block` for available but degraded order books; a single moderate failure can produce WATCH rather than a hard block.
- Climax uses separate, stricter `climax_*` spread/slippage/depth thresholds.
- Low-volume retains `low_volume_high_liquidity_risk_mode: warn` in `config.yaml`; its evaluator can downgrade to grade B for a warning, while the lifecycle separately blocks on `liquidity_warning`.
- `risk_flags.py` and `squeeze_guard.py` record missing liquidity/data quality, but are not themselves the admission authority.
- There is no post-restart evidence showing that these paths agree on live inputs, reason codes, or terminal outcomes.

Therefore the correct finding is **unified missing-liquidity fail-closed behavior at code/test level, but not yet a fully evidenced single threshold/decision policy across all lanes**. This is a reason to verify and document the policy, not to relax any gate.

## Grounded comparison with Cycles 1–7

| Area | Cycles 1–2 | Cycles 3–4 | Cycles 5–6 | Cycle 7 | Cycle 8 finding |
|---|---|---|---|---|---|
| Config/namespace wiring | blockers | fixed | retained | green in tests; runtime unverified | retained; no runtime attribution |
| Rejected volume candidate | blocker | metadata workaround | not independent | no durable pre-admission ledger | still open; selected-evaluation metadata is insufficient for coverage |
| Independent acceleration/squeeze vetoes | coupled | wiring fixed | retained | runtime unverified | retained in code; runtime unverified |
| Missing-liquidity admission | insufficiently evidenced | open | strictness requested | hard-blocked in code/tests | **hard-blocked consistently for missing data in code/tests; runtime and cross-lane policy unverified** |
| Degraded-liquidity thresholds | mixed/unclear | open | open | open | **still lane-specific (`general`, `climax`, `low_volume`) and requires policy reconciliation/documentation** |
| Persistence/atomicity | fail-open/partial | root improved | atomicity open | atomicity/reconciliation open | unchanged; no transaction/reconciliation proof |
| Closed-candle proof | blocker | blocker | blocker | blocker | blocker remains: `timestamp <= features.asof` is not exchange-close proof |
| Runtime attribution | unverified | unverified | unverified | unverified; service predates patch | **still unverified; no restart** |
| Cost-aware OOS | absent | absent | absent | absent | absent |
| Overall | `REVISE` | `REVISE` | `REVISE` | `REVISE` | **`REVISE`** |

Cycle 8 improves the local evidence count from Cycle 7's 123 tests to 124 passing tests and confirms the unified missing-liquidity regression coverage. It does not remove any promotion blocker because the service was not restarted and no OOS cohort exists.

## Remaining requirements before a shadow restart

A restart must remain a separately approved, controlled, **shadow-only** operation. Before it:

1. Freeze and record the exact patch/config hashes, service PID, DB integrity/WAL state, signal count, Telegram sender counters, and live-admission configuration.
2. Keep live admission, Telegram delivery, WATCH emission, Grade-C delivery, scheduler changes, and auto-execution disabled; no V3B/V3C or shadow signal may be sent externally.
3. Add or verify an immutable pre-selection candidate record for every eligible volume-climax root, including selected, rejected, missing-liquidity, warning-liquidity, and unknown/incomplete cases. Current `volume_climax_candidate` metadata attached to the selected evaluation does not prove this coverage.
4. Define a single documented reason-code contract for missing, warning, blocked, and accepted liquidity across general, climax, low-volume, and lifecycle paths; add cross-path tests for the same fixtures.
5. Add exchange close-time filtering and a partial-candle regression fixture. `timestamp <= features.asof` alone is insufficient.
6. Prove persistence invariants: atomic or reconciled root/EventState/attempt/evaluation writes, uniform fail-closed behavior, enforced per-root attempt limits, expiry removal from the active pool, and append-only revision/peak history.
7. Define restart success criteria before execution: namespaced runtime rows, candidate-to-attempt/evaluation correlation, expected lifecycle transitions and terminal reasons, no correlation gaps, no new live/Telegram side effects, and no unexplained signal-count/admission drift.
8. After restart, observe a bounded validation window long enough to capture the intended lifecycle path; read back SQLite and journal artifacts without treating absence of a candidate as proof of correctness.

## Remaining requirements for OOS

OOS is not ready to run as a promotion study until the replay uses the actual production lifecycle and records:

1. A time-separated holdout, frozen before inspection/tuning, with multiple symbols and independent root/attempt denominators rather than repeated evaluation rows.
2. Immutable/provenanced candle, OI, and order-book inputs per evaluation, with strict closed-candle semantics.
3. Eligible, excluded, unknown, incomplete, and no-fill counts; rejected candidates must remain counterfactual and must not enter the valid-signal denominator.
4. Fixed forecast horizons and lifecycle-consistent favorable-first/adverse-first ordering, including MAE/MFE only as descriptive measures.
5. Fees, funding, spread crossing, slippage, depth, partial fills, latency, conservative sellability, and leverage/liquidation assumptions.
6. A comparison against the unchanged baseline, with predeclared pass/fail thresholds and sensitivity to costs, latency, liquidity warnings, and missing-data policy.
7. No promotion based on unit-test count, raw MFE/MAE, terminal marks, runtime health, or shadow deltas alone. No claim of hit-rate, expectancy, PnL, or drawdown improvement is currently supported.

## Decision

```text
REVISE
PROMOTE: NO
SHADOW RESTART: NOT YET — gated controlled validation only
LIVE ADMISSION: UNCHANGED / OFF FOR THIS PATCH
AUTO-EXECUTION: OFF
TELEGRAM SIDE EFFECTS: NONE OBSERVED IN REVIEW WINDOW
NO CLAIM OF TRADING IMPROVEMENT
```

The unified policy is a meaningful code/test safety improvement: missing liquidity is no longer silently treated as acceptable in the reviewed actionable paths. It is not yet a runtime-validated or fully cross-lane threshold policy, and it has no cost-aware time-split OOS evidence. Next step is to close the candidate-coverage, closed-candle, persistence, and policy-contract gaps, then obtain explicit approval for one shadow-only restart with zero sender side effects. Do not relax liquidity gates or promote live admission from Cycle 8.
