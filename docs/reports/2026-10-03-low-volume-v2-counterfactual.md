# Low-volume extension V2 historical counterfactual

- **Status:** `INSUFFICIENT_DATA_CONTINUE_OBSERVATION`
- **Canonical unit:** `(root_event_id, strategy)`; repeated observations are deduplicated.
- **Source:** read-only SQLite `<APP_ROOT>/data/bot.sqlite`
- **Coverage:** `2026-08-31 21:01:44.602559` – `2026-10-03 13:01:58.282441` (observation timestamps)
- **Signals:** 0; **retained episodes:** 1382
- **Chronological split:** research 967 / holdout 415 (70/30 target)
- **V2 outcomes:** 1382 `UNKNOWN`; no V2 result is inferred.
- **CSV SHA-256:** `fec47d79a7fc174a1fe28fbba29426ad1f4f651fd5462145467f98ed192244ed` (docs/reports/2026-10-03-low-volume-v2-counterfactual.csv)

## Outcome coverage

| Measure | V1 persisted journal | V2 counterfactual |
|---|---:|---:|
| Signal count | 0 | UNKNOWN |
| Episode count | 1382 | 1382 candidates |
| Entry delay | UNKNOWN (no signal-linked entry) | UNKNOWN |
| MFE | persisted only when `COMPLETE` | UNKNOWN |
| MAE after final entry | UNKNOWN (entry absent) | UNKNOWN |
| New-high-before-entry | UNKNOWN | UNKNOWN |
| Eventual favorable movement | reported from persisted MFE only | UNKNOWN |
| `extension_below_threshold` policy comparison | UNKNOWN | UNKNOWN |

## Coverage boundary

The production SQLite journal contains decision snapshots and V1 outcome fields,
but no accepted candle-level OHLC input linked to each low-volume root and V2
entry anchor. Therefore the replay does **not** infer delayed entry, MFE, MAE,
new-high path, or policy effects. `INCOMPLETE` and missing outcomes remain
explicit; they are not counted as failures. Production configuration and
runtime state were not modified.
