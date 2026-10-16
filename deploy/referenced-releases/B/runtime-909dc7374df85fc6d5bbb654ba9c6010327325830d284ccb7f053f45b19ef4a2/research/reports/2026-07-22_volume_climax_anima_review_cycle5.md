# Anima Research Review Cycle 5: `VOLUME_CLIMAX_UNWIND`

- **Run time:** 2026-07-22T16:45Z
- **Repository:** `/opt/short-telegram-bot-lite`
- **Reviewed baseline:** `a4160e4` plus the current uncommitted telemetry/lifecycle patch
- **Review mode:** read-only; code, tests, and local SQLite/journal evidence only
- **Live admission:** unchanged
- **Service restart:** **not performed**
- **Trading claim:** none; this review does not establish trading improvement
- **Decision:** `REVISE` — `PROMOTE: NO`

## Scope and evidence boundary

The current working tree contains an uncommitted patch in `app/main.py`, `app/signals/climax.py`, `app/storage/repository.py`, `tests/test_climax_engine.py`, and `tests/test_climax_lifecycle.py`. The running service is PID `128948`, started at `2026-07-22 08:09:34 UTC`; it was not restarted for this review. Therefore local tests describe the current patch, while SQLite/journal observations describe the already-running deployed process and cannot be attributed to the current patch.

No thresholds, live admission, Telegram delivery, auto-execution, or production configuration was changed.

## Fresh verification

- `.venv/bin/pytest -q`: **121 passed in 4.26s**.
- `.venv/bin/python -m compileall -q app tests research`: exit `0`.
- `git diff --check`: exit `0`.
- systemd: `ActiveState=active`, `SubState=running`, `MainPID=128948`, `NRestarts=0`, `ExecMainStatus=0`.
- SQLite `/opt/short-telegram-bot-lite/data/bot.sqlite`: `integrity_check=ok`, `journal_mode=wal`.
- Current DB totals: `climax_evaluations=8369`, `climax_root_events=214`, `climax_entry_attempts=278`, `climax_entry_attempt_events=1112`, `signals=93`.
- Latest heartbeat: runtime ID `b9f1506b...`, model `climax-v1`, `fast_monitor_last_error=NULL`.
- Journal since the service start: no `Traceback`, `Exception`, rate-limit, or poll-error markers; recurring literal `timeout` matches were not treated as errors because they occur in unrelated timeout/status text. There were 127 logged full-cycle completions and 913 `poll_start` / 1054 `poll_complete` matches in the collected interval.

## Runtime telemetry currently observed (not attributed to current patch)

Since the current service start, the DB contains 2,445 evaluation rows, 90 attempt rows touched, and 647 lifecycle-event rows. Their decision deltas are `UNCHANGED_REJECTED=2,435` and `LIVE_REJECTED_SHADOW_ACTIONABLE=10`; lifecycle state remains `REJECTED` for these evaluation rows. The 90 attempts use the legacy/non-`volume_climax:` namespace; there are **0** `volume_climax:%` attempts in the DB and **0** `LIVE_REJECTED_SHADOW_FALLBACK_READY` rows.

This is useful evidence that the older telemetry contour is persisting evaluations, attempts, transitions, and some shadow deltas. It is **not** evidence that the current uncommitted metadata/pre-admission patch is running, nor evidence of improved trade outcomes.

## Grounded comparison with Cycles 1–4

| Area | Cycle 1 | Cycle 2 | Cycle 3 | Cycle 4 | Cycle 5 finding |
|---|---|---|---|---|---|
| Config wiring | blocker | fixed | fixed | code/tests verified | remains verified in current tests |
| Attempt namespace | collision risk | fixed in code | fixed in code | verified in code | current code retains `volume_climax:{root}:r{revision}:a1`; runtime has not exercised it |
| Independent veto inputs | coupled | coupled | wiring fixed | retained | current code keeps acceleration/squeeze independent; runtime attribution pending |
| Rejected volume candidate | blocker | blocker | partial metadata handoff | improved, runtime unverified | still not proven as a true independent pre-admission cohort; no current-runtime IDs |
| Root persistence | fail-open fallback | blocker | root path fail-closed | retained | root path remains improved; all storage operations/atomicity still not proven |
| Expiry / attempt limit | open | open | partly addressed | open | no evidence that current patch enforces per-root limits or complete pool cleanup |
| Closed-candle boundary | blocker | blocker | blocker | blocker | still blocker: `timestamp <= features.asof` does not prove last 1m candle is closed; no partial-candle fixture |
| Revision history | absent | absent | absent | absent | still not append-only; latest peak/revision remains mutable |
| Runtime effect | unverified | unverified | unverified | unverified | still unverified for current patch because no restart |
| Cost-aware OOS | absent | absent | absent | absent | still absent |
| Overall | REVISE | REVISE | REVISE | REVISE | **REVISE** |

## What improved

1. Current code preserves volume-climax metadata through final strategy selection and allows lifecycle admission based on that retained candidate marker.
2. The lifecycle uses the isolated `volume_climax:` attempt-ID namespace.
3. Acceleration and squeeze veto inputs are no longer coupled to `latest_failed_retest` in the lifecycle call.
4. Closed-candle count and confirmation window are configuration-driven at the lifecycle boundary.
5. The local regression suite covers the metadata handoff, configurable candle threshold, and namespace, with 121 tests passing.
6. Independently of the current patch, the running telemetry contour now provides a larger persisted sample than Cycle 4: 2,445 post-start evaluations, 90 attempts, and 647 lifecycle events. This improves observability, not trading evidence.

## Remaining blockers for promotion

### Runtime and coverage

- No restart means no verified runtime observation of the current patch.
- No `volume_climax:%` attempts and no lifecycle `LIVE_REJECTED_SHADOW_FALLBACK_READY` rows are present in the running DB.
- Existing post-start rows are legacy/non-namespaced telemetry and cannot prove coverage of the new candidate path.
- Repeated evaluation rows are not independent opportunities; promotion needs root/attempt-level denominators and explicit unknown/incomplete counts.

### Lifecycle integrity

- A dedicated immutable pre-selection `volume_climax_candidate` record is still preferable to metadata attached to the selected evaluation; it is needed to prove candidate coverage even when another strategy wins selection.
- Persistence is not shown to be atomic across root, EventState, attempt, and evaluation writes; no startup reconciliation evidence was produced.
- All lifecycle storage paths are not proven uniformly fail-closed.
- `climax_max_attempts_per_root_event`, expiry cleanup from the active pool, and append-only root revision history remain open acceptance items.
- The latest 1m candle closure is not established by `timestamp <= features.asof`; an exchange close-time rule and partial-candle regression are required.

### Cost-aware OOS research

Before any promote discussion, build a replay that:

1. uses one row per independent root/attempt, with fixed horizons and explicit missing/unknown windows;
2. replays the actual lifecycle and preserves favorable-first/adverse-first ordering, MFE and MAE;
3. applies fees, funding, spread crossing, slippage, depth/partial-fill limits, latency, and conservative sellability assumptions;
4. freezes raw candles/OI/order-book inputs per evaluation or records their provenance immutably;
5. uses a time-separated out-of-sample holdout and multiple symbols, with no threshold tuning on the holdout;
6. reports eligible/excluded/unknown denominators and sensitivity to costs/latency; rejected rows remain labeled counterfactual, never valid signals;
7. compares the candidate against the unchanged baseline without claiming PnL or trading improvement from MFE alone.

## Decision

```text
REVISE
PROMOTE: NO
LIVE RESTART: NO (this review)
AUTO-EXECUTION: OFF
NO CLAIM OF TRADING IMPROVEMENT
```

The telemetry patch improves code-level auditability and the already-running contour supplies more persisted operational evidence, but the current patch's runtime coverage is unverified and cost-aware, time-split OOS evidence is absent. Keep the lifecycle shadow-only. Next controlled step: restart only under a separately approved shadow-only validation procedure, verify namespaced candidate/attempt correlation and terminal transitions, then run the cost-aware multi-symbol OOS replay. Do not relax thresholds or promote live admission based on the green unit suite, raw MFE, or the observed shadow deltas.
