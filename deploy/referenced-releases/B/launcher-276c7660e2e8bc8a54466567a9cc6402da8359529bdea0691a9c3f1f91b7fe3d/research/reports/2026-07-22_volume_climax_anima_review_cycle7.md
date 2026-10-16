# Anima Research Review Cycle 7: `VOLUME_CLIMAX_UNWIND`

- **Run time:** 2026-07-22T16:55Z
- **Repository:** `/opt/short-telegram-bot-lite`
- **Reviewed baseline:** `a4160e4` plus the current uncommitted live-liquidity fail-closed patch
- **Review mode:** read-only; code, tests, systemd, and SQLite inspection
- **Live admission:** unchanged
- **Service restart:** not performed
- **Auto-execution/trading claim:** none
- **Decision:** `REVISE` — `PROMOTE: NO`

## Evidence boundary

The working tree is uncommitted in `app/main.py`, `app/signals/climax.py`, `app/storage/repository.py`, and climax tests. The systemd service is PID `128948`, started at `2026-07-22 08:09:34 UTC`; the changed application files have mtimes between `16:07:26Z` and `16:52:54Z`. The service therefore predates this patch. Current SQLite/journal rows are not attributable to the patch. No restart, configuration change, threshold relaxation, Telegram delivery, or execution was performed.

## Fresh verification

- `.venv/bin/pytest -q`: **123 passed in 4.30s**.
- `.venv/bin/python -m compileall -q app tests research`: exit `0`.
- `git diff --check`: exit `0`.
- systemd: `ActiveState=active`, `SubState=running`, `MainPID=128948`, `NRestarts=0`, `ExecMainStatus=0`.
- SQLite `/opt/short-telegram-bot-lite/data/bot.sqlite`: `integrity_check=ok`, `journal_mode=wal`.
- Current totals: `climax_evaluations=8397`, `climax_root_events=214`, `climax_entry_attempts=278`, `climax_entry_attempt_events=1114`, `signals=93`.
- Runtime DB: `volume_climax:%` attempts `0`; `LIVE_REJECTED_SHADOW_FALLBACK_READY` evaluations `0`; decision deltas: `UNCHANGED_REJECTED=7867`, `LIVE_REJECTED_SHADOW_ACTIONABLE=38`, `UNCHANGED_ACTIONABLE=1`, `NULL=491`.

## What Cycle 7 confirms after the live-liquidity fail-closed patch

1. **Code-level safety direction is correct:** the live climax evaluator now appends `liquidity_not_confirmed` when `liquidity_available=False`; the lifecycle fallback requires `features.liquidity_available` and no liquidity warning.
2. **Regression coverage exists:** tests explicitly verify that missing liquidity blocks a live volume candidate and blocks lifecycle fallback. The full suite is green at 123 tests.
3. **Existing improvements remain present:** volume metadata survives strategy selection, the lifecycle attempt namespace is `volume_climax:{root}:r{revision}:a1`, acceleration/squeeze vetoes are independent of `latest_failed_retest`, and candle/window parameters are configuration-driven.
4. **Persistence root path remains stricter:** the reviewed root-event DB exception path raises instead of fabricating revision `1`; terminal attempt close metadata is populated on creation.
5. **No live-liquidity claim is made:** the running process predates the patch, so the DB contains no attributable evidence that the new hard gate executed in runtime.

## Comparison with Cycles 1–6

| Area | Cycle 1–2 | Cycle 3–4 | Cycle 5–6 | Cycle 7 |
|---|---|---|---|---|
| Config/namespace wiring | blockers | code fixed | retained | tests/code remain green; runtime unverified |
| Rejected volume candidate | blocker | metadata workaround | not independent | still no durable independent pre-admission ledger |
| Independent acceleration/squeeze vetoes | coupled | wiring fixed | retained | retained; runtime unverified |
| Liquidity admission | insufficiently evidenced | open | strictness requested | missing liquidity hard-blocked in code/tests; runtime/OOS unverified |
| Persistence/atomicity | fail-open/partial | root improved | atomicity open | atomicity/reconciliation and uniform fail-closed behavior open |
| Closed-candle proof | blocker | blocker | blocker | blocker remains; no exchange close-time/partial-candle proof |
| Runtime attribution | unverified | unverified | unverified | unverified; service predates patch |
| Cost-aware OOS replay | absent | absent | absent | absent |
| Overall | `REVISE` | `REVISE` | `REVISE` | **`REVISE`** |

## Remaining promotion blockers

- No controlled shadow-only restart has exercised the patch; namespaced candidate/attempt correlation and lifecycle transitions remain unproven at runtime.
- No immutable pre-selection candidate record covers every eligible root, including rejected and unknown-liquidity cases. Selected-evaluation metadata cannot establish full candidate coverage.
- `timestamp <= features.asof` is not proof that the latest 1m candle is exchange-closed; no partial-candle regression fixture is present.
- No demonstrated transaction/reconciliation boundary across root, EventState, attempt, and evaluation writes; attempt-limit enforcement, expiry cleanup, and append-only peak/revision history remain open.
- No multi-symbol, time-separated, cost/latency-aware OOS replay. Required accounting remains fees, funding, spread crossing, slippage, depth/partial fills, latency, conservative sellability, fixed horizons, and independent-root denominators with unknown/incomplete counts.
- The green unit suite, runtime health, raw MFE/MAE, terminal marks, or shadow deltas must not be interpreted as trading evidence.

## Decision

```text
REVISE
PROMOTE: NO
LIVE RESTART: NO (this review)
AUTO-EXECUTION: OFF
NO CLAIM OF TRADING IMPROVEMENT
```

The live-liquidity fail-closed change is a validated code/test improvement, not a validated runtime or research result. Keep the lifecycle shadow-only. Next controlled step: separately approve a shadow-only restart, verify namespaced runtime coverage and zero sender side effects, then complete the independent-root, cost-aware, time-separated OOS replay. Do not relax the liquidity gate or promote live admission from this review.
