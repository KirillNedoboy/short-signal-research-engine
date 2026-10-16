# Anima Review Cycle 4: `VOLUME_CLIMAX_UNWIND`

- **Repository:** `/opt/short-telegram-bot-lite`
- **Review mode:** read-only research review after independent pre-admission patch
- **Live admission:** unchanged
- **Service restart:** no
- **Auto-execution:** off
- **Verdict:** `REVISE — code-level improvement, runtime/trading evidence unverified`

## Fresh verification

- Full local test suite: **`121 passed in 4.45s`** (`.venv/bin/pytest -q`).
- `compileall`: pass.
- `git diff --check`: pass.
- Working tree contains an uncommitted patch in `app/main.py`, `app/signals/climax.py`, `app/storage/repository.py`, and related tests.
- The running systemd process is PID `128948`, started **2026-07-22 08:09:34 UTC**. It was not restarted for this review, so its runtime behavior cannot be attributed to the current working-tree patch.
- Read-only SQLite inspection of `/opt/short-telegram-bot-lite/data/bot.sqlite` found **0** `volume_climax:%` attempts and **0** `LIVE_REJECTED_SHADOW_FALLBACK_READY` evaluations. This is absence of observed runtime evidence, not proof that the code path cannot work.

## What improved at code level since Cycles 1–3

1. **Selection metadata is now retained.** `evaluate_climax()` computes the volume-climax evaluation before final selection and attaches `volume_climax_candidate` plus `volume_climax_metadata` to the selected evaluation when the volume candidate clears the score threshold. This addresses the Cycle-3 failure where a stronger low-volume evaluation could erase the volume subtype from the selected metadata.
2. **Lifecycle admission consumes the retained metadata.** `app/main.py` can enter the volume lifecycle when the selected evaluation is not itself `VOLUME_CLIMAX_UNWIND`, provided the retained candidate metadata marks the volume candidate. The live evaluator/Telegram admission branch remains unchanged.
3. **Independent veto inputs remain separated in the lifecycle call.** `price_acceleration_resumed` and `active_short_squeeze` are no longer disabled by `latest_failed_retest`.
4. **Closed-candle threshold and confirmation window are configuration-driven.** The lifecycle receives `min_closed_candles_after_high` and uses the configured confirmation window in metadata construction.
5. **Shadow attempt namespace remains isolated:** `volume_climax:{root}:r{revision}:a1`.
6. **Root-event persistence is fail-closed** on the reviewed exception path; terminal/expired attempt rows receive close metadata on creation.
7. **Regression coverage increased to 121 passing tests**, including selected-evaluation metadata preservation, configurable candle threshold, and namespace behavior.

## What is still not proven or still incomplete

### Runtime evidence

- No service restart was performed; the live PID predates the current patch. Therefore there is no verified production observation of the new metadata-admission branch, namespaced attempts, or lifecycle-specific decision delta.
- The current SQLite readback contains no `volume_climax:%` attempts and no `LIVE_REJECTED_SHADOW_FALLBACK_READY` rows. Runtime activation and candidate coverage remain unverified.
- The test suite proves deterministic code behavior, not that live rejected-but-eligible candidates are actually being captured under current market conditions.

### Trading/research evidence

- No claim is supported about improved hit rate, MAE/MFE, expectancy, PnL, or drawdown.
- Replay remains incomplete for fees, funding, spread crossing, slippage, order-book depth impact, latency, partial fills, and leverage/liquidation effects.
- No time-separated out-of-sample cohort, multi-symbol validation, or independent-root denominator has been demonstrated.
- The implementation still derives `latest_closed` using `timestamp <= features.asof`; this is not sufficient proof that the latest 1m candle is fully closed. No partial-candle regression fixture was added in this patch.
- The metadata handoff is an improvement but not yet a fully independent pre-admission candidate object: it is attached after candidate evaluations are constructed and after the final selection path is known. A dedicated pre-selection candidate record would make coverage and accounting unambiguous.
- Remaining lifecycle integrity gaps from Cycle 3 still require evidence or fixes: all storage operations are not uniformly fail-closed, no transaction/reconciliation boundary exists, attempt limits are not enforced, expired candidates may remain in the active pool, and root revision history is not append-only.

## Cycle comparison

| Area | Cycle 1 | Cycle 2 | Cycle 3 | Cycle 4 |
|---|---|---|---|---|
| Config wiring | blocker | fixed | fixed | **verified in code/tests** |
| Attempt namespace | collision risk | fixed | fixed | **verified in code/tests** |
| Independent veto inputs | coupled | coupled | wiring fixed | **code-level improvement retained** |
| Rejected volume candidate | blocker | blocker | partial metadata workaround | **better metadata handoff; true pre-admission/runtime coverage unverified** |
| Root persistence fallback | blocker | blocker | fixed | **still fixed on reviewed path** |
| All persistence fail-closed | blocker | blocker | open | **open** |
| Expiry cleanup / attempt limit | blocker | blocker | open | **open** |
| Closed-candle boundary | blocker | blocker | blocker | **blocker; no partial-candle proof** |
| Atomicity/reconciliation | absent | absent | absent | **absent** |
| Cost-aware OOS replay | absent | absent | absent | **absent** |
| Runtime effect | unverified | unverified | unverified | **unverified; no restart and zero new runtime IDs** |
| Overall verdict | `REVISE` | `REVISE` | `REVISE` | `REVISE` |

## Decision

The patch is a real **code-level improvement** over Cycles 1–3: it preserves volume-climax candidate metadata across strategy selection, wires that metadata into the shadow lifecycle, keeps the live admission lane unchanged, and is backed by 121 passing tests. It does **not** establish runtime activation or trading validity. The correct decision remains:

```text
PROMOTE: NO
LIVE RESTART: NO
AUTO-EXECUTION: OFF
VERDICT: REVISE
```

Next controlled step: add a genuine pre-selection candidate record, strict exchange candle-close filtering with partial-candle tests, and read-only runtime validation after a controlled shadow-only restart. Only then run cost/latency-aware, time-split, multi-symbol replay; do not infer trading edge from the green unit suite.
