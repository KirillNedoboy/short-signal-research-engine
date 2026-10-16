# Anima Research Review Cycle 6: `VOLUME_CLIMAX_UNWIND`

- **Run time:** 2026-07-22T16:49:34Z
- **Repository:** `/opt/short-telegram-bot-lite`
- **Reviewed baseline:** `a4160e4` plus the current uncommitted strict candidate/liquidity patch
- **Review mode:** read-only; source, tests, systemd, and SQLite inspection
- **Live admission:** unchanged
- **Service restart:** not performed
- **Auto-execution/trading claim:** none
- **Decision:** `REVISE` — `PROMOTE: NO`

## Evidence boundary

The current working tree is uncommitted in `app/main.py`, `app/signals/climax.py`, `app/storage/repository.py`, and the two climax test files. The service is running as PID `128948`, started at `2026-07-22 08:09:34 UTC`. The changed application files were modified after that start (`app/main.py` 16:46:24Z; `app/signals/climax.py` 16:46:09Z; `app/storage/repository.py` 16:07:26Z). Therefore the runtime DB cannot be attributed to the current patch. No restart, config change, threshold relaxation, Telegram delivery, or execution was performed.

## Fresh verification

- `.venv/bin/pytest -q`: **122 passed in 4.28s**.
- `.venv/bin/python -m compileall -q app tests research`: exit `0`.
- `git diff --check`: exit `0`.
- systemd: `ActiveState=active`, `SubState=running`, `MainPID=128948`, `NRestarts=0`, `ExecMainStatus=0`.
- Journal since service start: 2,263 lines; `Traceback=0`, `Exception=0`, `poll_error=0`, `poll_start=928`, `poll_complete=1069`, `Cycle complete=129`.
- Read-only SQLite `/opt/short-telegram-bot-lite/data/bot.sqlite`: `integrity_check=ok`, `journal_mode=wal`.
- Current totals: `climax_evaluations=8,392`, `climax_root_events=214`, `climax_entry_attempts=278`, `climax_entry_attempt_events=1,114`, `signals=93`.
- Runtime DB has **0** `volume_climax:%` attempts and **0** `LIVE_REJECTED_SHADOW_FALLBACK_READY` evaluations. Decision deltas: `UNCHANGED_REJECTED=7,862`, `LIVE_REJECTED_SHADOW_ACTIONABLE=38`, `UNCHANGED_ACTIONABLE=1`, `NULL=491`.
- Attempt states: `BREAKDOWN_PENDING=128`, `EXPIRED=50`, `RETEST_IN_PROGRESS=28`, `ROOT_REPLACED=63`, `SHADOW_ACTIONABLE=9`.

The service is healthy as an operational process, but the absence of namespaced rows is not evidence that the new branch works or fails: the process predates the patch.

## Strict candidate/liquidity patch assessment

### Improvements supported by code/tests

1. Volume-climax metadata is retained through final strategy selection when the volume candidate reaches the configured score floor.
2. The lifecycle can use the retained metadata and has a separate `volume_climax:{root}:r{revision}:a1` attempt namespace.
3. Acceleration and squeeze inputs are no longer disabled by `latest_failed_retest`.
4. Minimum closed-candle count and confirmation window are configuration-driven at the lifecycle boundary.
5. Missing liquidity is now a hard lifecycle veto: `liquidity_ok=features.liquidity_available and not liquidity_warning`; the regression test confirms `liquidity_not_confirmed` blocks fallback.
6. Root-event DB exceptions are no longer converted to a fabricated revision `1`; terminal attempt close metadata is populated on creation.

### Strict candidate limitation still present

The patch is not yet a genuine independent pre-admission candidate ledger. `volume_climax_candidate` is set to `not volume_candidate.veto_reasons`; it is attached to whichever evaluation wins the final selection. The added test explicitly proves the case where the volume candidate is observed but `volume_climax_candidate=False`. If a rejected-but-eligible volume candidate is not the selected evaluation, the main path still has no durable independent candidate row to establish coverage. This is an auditability gap, not a reason to weaken the liquidity gate.

Other lifecycle acceptance gaps remain: no proof of atomic root/EventState/attempt/evaluation writes or startup reconciliation; attempt-limit enforcement and complete expiry/pool cleanup are unproven; root peak history is mutable rather than append-only; and `timestamp <= features.asof` still does not establish that the last 1m candle is exchange-closed. The current tests do not provide a partial-candle fixture or restart smoke.

## Grounded comparison with Cycles 1–5

| Area | Cycles 1–2 | Cycles 3–4 | Cycle 5 | Cycle 6 |
|---|---|---|---|---|
| Config/namespace wiring | blockers | code fixed | code retained | **tests pass; runtime unverified** |
| Rejected volume candidate | blocker | partial metadata workaround | still not independent | **still not independent pre-admission ledger** |
| Independent acceleration/squeeze vetoes | coupled | wiring fixed | retained | **retained in code; runtime unverified** |
| Liquidity admission | insufficiently evidenced | open | strictness requested | **missing liquidity hard-blocked; no OOS proof of selectivity/cost** |
| Persistence | fail-open/partial | root improved, atomicity open | open | **atomicity/reconciliation and uniform fail-closed behavior open** |
| Closed-candle proof | blocker | blocker | blocker | **blocker remains** |
| Runtime effect | unverified | unverified | unverified, no restart | **unverified; service predates patch** |
| Cost-aware OOS | absent | absent | absent | **absent** |
| Verdict | `REVISE` | `REVISE` | `REVISE` | **`REVISE`** |

The evidence quality improved at the unit-test and code-review level: 121 → 122 passing tests, explicit liquidity veto coverage, and preserved namespace/config wiring. It did not improve at the attribution level because the service was not restarted; the SQLite sample remains legacy/non-namespaced for this patch. Operational telemetry is healthy, but it is not trading evidence.

## Promotion gate

**PROMOTE: NO.** The patch must remain shadow-only. No hit-rate, MAE/MFE, expectancy, PnL, or drawdown improvement is established.

Before a promotion decision, require:

1. a controlled shadow-only restart and post-restart runtime proof of `volume_climax:%` candidate/attempt correlation, lifecycle transitions, terminal reasons, and zero sender side effects;
2. an immutable pre-selection candidate/evaluation record for every eligible volume-climax root, including rejected and unknown liquidity states;
3. strict exchange candle-close filtering with partial-candle regression coverage;
4. persistence atomicity/reconciliation, per-root attempt limits, expiry cleanup, and append-only revision history;
5. a multi-symbol, time-separated OOS replay of the actual lifecycle with fixed horizons, independent-root denominators, unknown/incomplete counts, fees, funding, spread, slippage, depth/partial fills, latency, and conservative sellability assumptions.

Do not promote or relax thresholds based on 122 green unit tests, raw MFE, current/terminal marks, or the absence of runtime failures.

```text
REVISE
PROMOTE: NO
LIVE RESTART: NO (this review)
AUTO-EXECUTION: OFF
NO CLAIM OF TRADING IMPROVEMENT
```
