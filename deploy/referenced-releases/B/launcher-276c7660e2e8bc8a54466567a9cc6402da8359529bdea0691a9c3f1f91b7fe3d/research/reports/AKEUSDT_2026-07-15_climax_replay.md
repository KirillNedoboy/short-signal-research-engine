# AKEUSDT — CLIMAX_SHORT_RESEARCH_V1 forensic replay

## Scope and evidence

- Replay window: `2026-07-15T07:30:00Z`–`2026-07-15T12:30:00Z`.
- Market data: Bybit public REST historical snapshot, captured into the deterministic fixture.
- Blogger data: `user_screenshot_derived`. It is kept separate from server and exchange facts.
- Blogger timestamp: `2026-07-15T08:08:00Z` / `2026-07-15 11:08 MSK`; published short zone: `around 0.0006340+`.
- This is offline research only. It does not read or write the baseline SQLite database and is not imported by the runtime.

## Blogger versus baseline versus research models

| Side | Time UTC | State / decision | Paper entry | Delay vs 11:08 MSK |
| --- | --- | --- | --- | --- |
| Blogger | 2026-07-15T08:08:00Z | screenshot-derived short idea | around 0.0006340+ | 0m |
| Baseline | 2026-07-15T08:03:00Z | EARLY_PUMP_WATCH, score 70; not actionable | — | -5m |
| Baseline | 2026-07-15T08:07:00Z | REJECT, score 0 | — | -1m |
| Baseline | 2026-07-15T08:10:00Z | REJECT, score 0 | — | +2m |
| M1 | 2026-07-15T08:05:00Z | CONFIRMED | 0.0006119 | -3m |
| M2 | 2026-07-15T08:02:00Z | CONFIRMED | 0.0006851 | -6m |
| M3 | — | INSUFFICIENT_DATA: historical_trades_not_available | — | — |
| M4 | 2026-07-15T08:03:00Z | CONFIRMED | 0.0006824 | -5m |

## Baseline finding

AKEUSDT was eligible and deep-scanned. `pump_detected` is established by the 08:03 EARLY_PUMP_WATCH. The lifecycle at 08:08 UTC is `PUMP_DETECTED` with `inferred` certainty: The 08:03 EARLY_PUMP_WATCH requires PUMP_DETECTED; the 08:07/08:10 score-0 REJECT rows are consistent with a state that never reached PULLBACK_OBSERVED. Historical event-state versions were not persisted.

The baseline only admits a signal after a pullback in the configured 2.4–8.0% band and activation of the short zone. The first large reversal fell outside that narrow maturity path, so the engine retained no actionable state and emitted no SIGNAL. Journal data also shows roughly three-minute cycle stretches when Bybit rate limits hit, which made this fast reversal harder to observe at a useful point.

## Climax evidence from candles

| Feature | Value |
| --- | --- |
| event_new_high | True |
| event_high_time_utc | 2026-07-15T08:01:00Z |
| event_high | 0.0007688 |
| price_velocity_5m_pct | 31.734052 |
| acceleration_rollover | True |
| upper_wick_pct | 4.869731 |
| rejection_from_high_pct | 4.869731 |
| failed_continuation | True |
| time_near_high_minutes | 1 |
| volume | 5182427300.0 |
| volume_ratio_to_prior_30m | 7.950398 |
| volume_zscore_to_prior_30m | 8.733588 |

## OI, funding, and premium

OI observations use matched public snapshot timestamps. They describe association, not intrabar causality.

### OI 5m

| From | To | Price change | OI change |
| --- | --- | ---: | ---: |
| 2026-07-15T07:55:00Z | 2026-07-15T08:00:00Z | +4.4560% | +2.9886% |
| 2026-07-15T08:00:00Z | 2026-07-15T08:05:00Z | +4.8312% | -5.7703% |
| 2026-07-15T08:05:00Z | 2026-07-15T08:10:00Z | +2.5168% | -3.2340% |
| 2026-07-15T08:10:00Z | 2026-07-15T08:15:00Z | -12.8487% | +0.3172% |
| 2026-07-15T08:15:00Z | 2026-07-15T08:20:00Z | +13.0419% | +0.5792% |
| 2026-07-15T08:20:00Z | 2026-07-15T08:25:00Z | -0.2913% | +3.1821% |
| 2026-07-15T08:25:00Z | 2026-07-15T08:30:00Z | -0.0325% | +0.2596% |
| 2026-07-15T08:30:00Z | 2026-07-15T08:35:00Z | +5.9091% | -0.6530% |
| 2026-07-15T08:35:00Z | 2026-07-15T08:40:00Z | -9.3041% | +2.2577% |
| 2026-07-15T08:40:00Z | 2026-07-15T08:45:00Z | -0.3211% | +2.3306% |
| 2026-07-15T08:45:00Z | 2026-07-15T08:50:00Z | +1.4751% | +3.0725% |
| 2026-07-15T08:50:00Z | 2026-07-15T08:55:00Z | +5.6642% | -1.8009% |

### OI 15m

| From | To | Price change | OI change |
| --- | --- | ---: | ---: |
| 2026-07-15T07:45:00Z | 2026-07-15T08:00:00Z | +17.8240% | +0.5669% |
| 2026-07-15T08:00:00Z | 2026-07-15T08:15:00Z | -6.3389% | -8.5284% |
| 2026-07-15T08:15:00Z | 2026-07-15T08:30:00Z | +12.6761% | +4.0492% |
| 2026-07-15T08:30:00Z | 2026-07-15T08:45:00Z | -4.2532% | +3.9577% |

### OI 30m

| From | To | Price change | OI change |
| --- | --- | ---: | ---: |
| 2026-07-15T07:30:00Z | 2026-07-15T08:00:00Z | +16.0207% | +10.7865% |
| 2026-07-15T08:00:00Z | 2026-07-15T08:30:00Z | +5.5337% | -4.8245% |

Funding nearest the blogger time: `0.00024091` at `2026-07-15T08:00:00Z`.
Premium-index close nearest the blogger time: `0.00054187` at `2026-07-15T08:08:00Z`.

## Research-model outcomes

### M1

Criteria: price_up_while_oi_down, post_high_oi_rollover, short_covering_exhaustion.

| Horizon | Close | Short return | MFE | MAE |
| --- | ---: | ---: | ---: | ---: |
| 5m | 0.0006273 | -2.4550% | 3.7470% | 9.7565% |
| 15m | 0.0006180 | -0.9871% | 19.3951% | 9.7565% |
| 30m | 0.0006524 | -6.2078% | 19.3951% | 13.0577% |
| 1h | 0.0006405 | -4.4653% | 19.3951% | 13.0577% |
| 4h | 0.0005285 | +15.7805% | 21.2403% | 13.0577% |

First hit, favorable 5% vs adverse 3%: `ADVERSE_3_FIRST`.
First hit, favorable 10% vs adverse 5%: `ADVERSE_5_FIRST`.

### M2

Criteria: new_high_event, volume_climax, upper_wick_rejection, acceleration_rollover, failed_continuation.

| Horizon | Close | Short return | MFE | MAE |
| --- | ---: | ---: | ---: | ---: |
| 5m | 0.0006476 | +5.7906% | 16.1580% | 1.6494% |
| 15m | 0.0005956 | +15.0269% | 33.6780% | 1.6494% |
| 30m | 0.0006248 | +9.6511% | 33.6780% | 1.6494% |
| 1h | 0.0006434 | +6.4812% | 33.6780% | 1.6494% |
| 4h | 0.0005297 | +29.3374% | 35.7440% | 1.6494% |

First hit, favorable 5% vs adverse 3%: `FAVORABLE_5_FIRST`.
First hit, favorable 10% vs adverse 5%: `FAVORABLE_10_FIRST`.

### M3

No confirmation: `historical_trades_not_available`.

### M4

Criteria: new_high_event, volume_climax, upper_wick_rejection, time_near_high, two_candle_acceleration_rollover, failed_continuation.

| Horizon | Close | Short return | MFE | MAE |
| --- | ---: | ---: | ---: | ---: |
| 5m | 0.0006381 | +6.9425% | 15.7002% | 0.7913% |
| 15m | 0.0006107 | +11.7406% | 33.1512% | 0.7913% |
| 30m | 0.0006751 | +1.0813% | 33.1512% | 1.3775% |
| 1h | 0.0006371 | +7.1103% | 33.1512% | 1.3775% |
| 4h | 0.0005298 | +28.8033% | 35.2090% | 1.3775% |

First hit, favorable 5% vs adverse 3%: `FAVORABLE_5_FIRST`.
First hit, favorable 10% vs adverse 5%: `FAVORABLE_10_FIRST`.

## Explicitly unavailable historical data

- `trades`: `NULL`; `historical_trades_not_available`.
- `orderbook`: `NULL`; `historical_orderbook_not_available`.
- `taker_buy_sell_pressure`: `NULL`; `historical_trades_not_available`.
- `approximate_cvd`: `NULL`; `historical_trades_not_available`.

## What to capture live

- Persist every 1m candle used by the scanner with request/as-of timestamp.
- Persist OI snapshots at 1m or the finest available cadence with source timestamp.
- Persist funding, premium-index, mark-price, and index-price snapshots.
- Persist raw public trades with side, price, size, and exchange timestamp to derive CVD.
- Persist orderbook top levels, spread, and depth at each scan and at event transitions.
- Persist shortlist eligibility, fetch failures, state transitions, and rejection reasons per cycle.
