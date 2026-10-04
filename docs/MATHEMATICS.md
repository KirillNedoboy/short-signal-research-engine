# Mathematical specification

This specification is canonical for the public baseline `ce77d744`. Runtime production values must be checked against the release artifact named in `SOURCE_OF_TRUTH.md`.

## Notation and data contract

For candle index `t`, `O_t`, `H_t`, `L_t`, `C_t`, and `V_t` are open, high, low, close, and volume. A feature is usable only when its lookback is available and its source timestamp passes freshness/continuity checks. Missing, stale, rate-limited, or discontinuous inputs remain missing; they are not silently converted into a positive signal.

## Features

### Percentage return

```text
return_n(t) = (C_t / C_{t-n} - 1) × 100
```

Implementation: `app/features/returns.py`. A zero denominator returns `0.0` in the baseline helper; callers still need data-quality validation.

### VWAP

```text
typical_t = (H_t + L_t + C_t) / 3
VWAP_t = Σ(typical_i × V_i) / ΣV_i
```

Zero volumes are treated as missing in `app/features/vwap.py`.

### True range and ATR

```text
TR_t = max(H_t-L_t, |H_t-C_{t-1}|, |L_t-C_{t-1}|)
ATR_t = EWMA(TR, alpha=1/p, min_periods=p, adjust=False)
```

The baseline uses `p=14`; implementation: `app/features/atr.py`.

### EMA

```text
EMA_t = EWM(C_t, adjust=False)
```

The runtime feature builder uses EMA20 on the 15-minute candle series. Implementation: `app/features/ema.py`, `app/features/builder.py`.

### RSI

```text
d_t = C_t-C_{t-1}
gain_t = max(d_t,0)
loss_t = max(-d_t,0)
avg_gain = WilderEWM(gain)
avg_loss = WilderEWM(loss)
RSI = 100 - 100/(1 + avg_gain/avg_loss)
```

Implementation: `app/features/rsi.py`; baseline period is 14.

### Volume z-score

```text
z_t = (V_t - mean_window(V)) / std_window(V, ddof=0)
```

`min_periods=max(2, window//2)`. Zero standard deviation becomes missing. Implementation: `app/features/volume.py`.

### Candle geometry

```text
range_t = max(H_t-L_t, 1e-9)
upper_wick = max(H_t-max(O_t,C_t),0)/range_t
lower_wick = max(min(O_t,C_t)-L_t,0)/range_t
body = |C_t-O_t|/range_t
rejection_pct = (H_t-C_t)/H_t × 100
close_position = (C_t-L_t)/range_t
```

Implementation: `app/features/candle_stats.py`.

### Derived features

The baseline builder computes returns for 5m/15m/1h/4h by positional lookbacks `[-6]`, `[-16]`, `[-61]`, and `[-241]`; distances to VWAP and EMA20, pullback from event high, and range-to-ATR are defined in `app/features/builder.py`:

```text
dist_to_vwap_pct = return(price, VWAP)
dist_to_ema20_atr = (price-EMA20)/ATR
pullback_from_high_pct = (event_high-price)/event_high × 100
range_atr_ratio = (latest_high-latest_low)/ATR
```

### Open interest

For the derivative rows used by the baseline:

```text
OI_change_pct = (latest_OI / prior_OI - 1) × 100
```

The 15-minute comparison uses rows `[0]` and `[1]`; the 1-hour comparison uses rows `[0]` and `[4]`. Implementation: `app/features/builder.py`.

## Event and zone mathematics

### Pump event

The first satisfied horizon is selected in order 15m, 1h, 4h:

```text
ret_15m >= 6%  OR  ret_1h >= 8%  OR  ret_4h >= 20%
```

At least one stretch condition is also required:

```text
dist_to_vwap_pct >= 6%
or dist_to_ema20_atr >= 2
or vol_zscore_30m >= 0.8
or range_atr_ratio >= 1.3
```

Implementation: `app/events/pump_detector.py`.

```text
event_range_pct = (event_high/event_base_price - 1) × 100
```

### Short zones

For event-range zones:

```text
zone_low  = base + 0.70 × (high-base)
zone_high = base + 0.92 × (high-base)
```

For ATR zones, the bounds are `event_high - 0.3×ATR` and `event_high - 1.5×ATR`, sorted before use. Implementation: `app/events/short_zone.py`.

## Scores and grades

The baseline score sums bounded bucket contributions. `_scale(x,a,b,m)` is linear from 0 to `m` and clipped to that interval. `_ideal_band(x,ideal,width,m)` reaches `m` at the ideal and decreases linearly with distance.

Buckets include stretch, exhaustion, volume, event quality, pullback maturity, zone quality, and derivative bonuses. The raw total is normalized as:

```text
score = int(round(clip(raw_total - penalty_points, 0, 100)))
```

Baseline risk penalties are capped at 40. Exact component definitions are in `app/signals/scoring.py` and `app/signals/risk_flags.py`; config thresholds are release-specific.

Baseline grade mapping in the score engine is:

```text
A: score >= 80
B: score >= 65
C: otherwise
```

Climax and trapped-longs evaluators use their own bounded flag score:

```text
strategy_score = min(100, 10 + 15 × number_of_true_flags)
A >= 85; B >= 70; C otherwise
```

These grade maps must not be conflated with public-admission thresholds.

## Liquidity and squeeze

Liquidity gates use spread, estimated slippage, and depth at 1% and 2%; the live branch rejects when configured thresholds are breached or data is incomplete. Exact thresholds belong to the release configuration and are not inferred from a generic formula.

Squeeze score is an independent veto/penalty assessment. It adds bounded points for funding/OI continuation, shallow pullback, weak rejection, proximity to high, breakout risk, spread, slippage, and thin depth. Levels are `LOW < 12`, `MEDIUM >= 12`, `HIGH >= 24`, `EXTREME >= 34`; the engine applies configured penalties for `SCORE_PENALTY` action.

## Missing-data rule

No feature formula authorizes a signal by itself. A candidate must pass market-data quality, lifecycle, hard-gate, liquidity, squeeze, score, grade, and delivery checks. If a required input is missing, the relevant branch is rejected or downgraded to a non-actionable observation.
