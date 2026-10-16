# Anima architecture review — attempt admission hardening

Date: 2026-07-23
Repository: `short-telegram-bot-lite`
Scope: shadow-only architecture and evidence pipeline. Live admission, Telegram delivery, and auto-execution were not intentionally changed.

## Changes applied

- Added database-side attempt admission cap via `max_attempts_per_root_event`.
- SQLite admission uses `BEGIN IMMEDIATE` so count-plus-insert is serialized for concurrent workers.
- Rejected admissions append `attempt_limit_reached` telemetry and do not create an attempt row.
- Runtime handles failed shadow persistence without treating the attempt as created.
- Shadow root-event persistence is fail-open: telemetry failure is logged and returns a fallback revision instead of aborting the live evaluation path.
- Added regression coverage for the cap and concurrent SQLite admission.

## Validation

- `.venv/bin/python -m pytest -q`: `135 passed`
- `python3 -m compileall -q app tests research`: pass
- `git diff --check`: pass
- Fresh SQLite schema bootstrap: pass
- SQLite `PRAGMA integrity_check`: `ok`
- No restart, live activation, order placement, or Telegram delivery was performed.

## Anima verdict

`REVISE / PROMOTE: NO`

### Confirmed positive boundaries

- Shadow lifecycle has no direct notifier call.
- Existing live sender paths remain in place and were not intentionally broadened.
- Unknown and partial liquidity remain fail-closed.
- Observation ledger is additive and separate from live `signals`.
- Test success proves code-level invariants only; it does not prove profitability.

### Remaining blockers

1. Root → attempt → evaluation persistence is still split across transactions.
2. Reverse event-level correlation can contain `evaluation_id=NULL` for pre-evaluation lifecycle events.
3. Exchange close-time semantics are still inferred from candle intervals in the strict shadow path.
4. Expiry/restart reconciliation and active-pool cleanup require additional runtime evidence.
5. The observation ledger is not yet attributable to a fresh runtime after this local package because no restart was performed.
6. No cost-aware, time-separated, multi-symbol OOS validation exists.

## Promotion rule

`PROMOTE: NO` until concurrency, crash/restart reconciliation, exchange-close semantics, complete pre-selection denominators, and cost/latency-aware multi-symbol OOS replay are demonstrated. Live admission and auto-execution remain unchanged/off.
