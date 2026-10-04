# Current Trading Contract

Status: Phase 0 characterization baseline. This document describes the behavior at code SHA `261646f7e7653957438dee54b38a23104f35c4ea`; it is not a proposal to change the strategy.

## Runtime version

- **Code SHA:** `261646f7e7653957438dee54b38a23104f35c4ea`.
- **Config contract:** `AppConfig` in `app/config.py` is Pydantic with `ConfigDict(extra="ignore", validate_assignment=True, hide_input_in_errors=True)`. Unknown mapping keys are silently ignored. `config.yaml` is merged with selected `.env` overrides; environment values win. The strategy fingerprint is the SHA-256 of canonical JSON containing `climax_`, `volume_climax_`, `low_volume_`, derivatives, liquidity, and squeeze settings. The public runtime fingerprint excludes secrets, database URL, and chat IDs.
- **Dataset epoch:** no dataset epoch is currently a runtime field. For replay or comparison, the epoch must be supplied externally as the exact closed-candle/derivatives/liquidity snapshot interval and source snapshot identity. `features.asof` is part of the frozen input; wall-clock time is not a strategy input.
- **Config example:** `config.yaml` contains `climax_block_price_oi_accelerating_together`, which is not an `AppConfig` field and is therefore silently ignored under `extra="ignore"`.

## Strategies

Exactly three trading strategies exist. `ROOT_DETECTOR_SHADOW_V2` is documented separately as research telemetry, not a trading strategy.

### BASELINE_PULLBACK

**INPUT**

`SymbolFeatures` derived from closed recent 1m candles, optional derivatives and order-book liquidity; `EventState`; a `ShortZone`; fixed `signal_time`; and `AppConfig`. Decision-time features include returns, VWAP/EMA stretch, candle shape, volume z-score, ATR/range, event/pullback distance, retest, derivatives, and liquidity.

**STATE**

Evaluation is checked only when `EventState.state` is `PULLBACK_OBSERVED` or `SHORT_ZONE_ACTIVE`, the event is not expired/stale, and price is inside the short zone. `PumpDetector` creates `PUMP_DETECTED`; `PullbackTracker` advances to `PULLBACK_OBSERVED`; the runtime assigns `SHORT_ZONE_ACTIVE`; signal persistence marks `SIGNAL_SENT`.

**HARD GATES**

- Event expiry and maximum signal age.
- Price inside the configured short zone.
- Core filters: `dist_to_vwap_pct >= dist_to_vwap_min`; rejection by upper wick OR rejection-from-high; `vol_zscore_30m >= vol_zscore_min`; pullback inside `[pullback_min_pct, pullback_max_pct]`.
- Score at least 50, public grade allowed, no liquidity `block`, no squeeze block, no breakout/continuation risk, and no forced-watch result.
- Missing liquidity is a hard block. Explicit severe liquidity or at least two moderate failures is a hard block; one moderate failure is `watch`.

**WARNINGS**

Weak-but-near rejection/volume may be represented as WATCH when watch candidates are enabled. Squeeze levels in the current production config use `warn_only`; `block_extreme` remains the explicit blocking mode. Risk flags include shallow pullback, near-high price, weak rejection, thin VWAP buffer, continuation, retest, liquidity, and recent breakout conditions.

**SCORE**

`score_setup` sums stretch, exhaustion, volume, event quality, pullback maturity, zone quality, and derivatives bonus, subtracts risk penalties, rounds, and clamps to 0–100. Squeeze `SCORE_PENALTY` subtracts 0/6/10/16 for LOW/MEDIUM/HIGH/EXTREME.

**GRADE**

A at score >= 80; B at score >= 65; otherwise C. Grade C is not public with the current `min_public_signal_grade: B` and `send_grade_c_to_telegram: false`; `grade_c_mode: watch_only` can create a non-actionable WATCH candidate when enabled.

**ADMISSION**

Actionable output is a `SignalDecision` with `decision_type="SIGNAL"`, `actionable=True`, and `SignalType.AGGRESSIVE` when score >= 75 with strong rejection and no breakout risk; otherwise `SignalType.CONFIRM`. Non-admitted near candidates can be `WATCH` with `actionable=False` only when configured.

**DEDUPE**

Runtime first suppresses an event with `state.signal_id`; persisted signal identity is unique on `(symbol, event_id, strategy_subtype, model_version)`. Baseline uses `strategy_subtype=NULL` and `model_version="baseline-v1"` in the persisted signal shape.

**OUTPUT**

`SignalDecision` contains the exact score, grade, type, reasons, risk flags, feature snapshot, score breakdown, lifecycle state, and squeeze fields. Actionable baseline output is persisted with provenance and a `SIGNAL` Telegram outbox row before delivery is attempted.

### VOLUME_CLIMAX_UNWIND

**INPUT**

`EventState`, `SymbolFeatures`, the 1m OHLCV frame, and climax config. `_common_metadata` uses the current closed-candle semantics, computes event high, equal five-candle volume windows, volume ratio, post-high closes/retests, breakout reference, entry distance, OI, rejection, and liquidity metadata.

**STATE**

The live branch is evaluated for a tracked event; the optional bounded lifecycle shadow separately uses `CLIMAX_WATCHING`, `FALLBACK_READY`, or `EXPIRED`. A new high increments the shadow event revision and resets its confirmation window; root lifetime remains anchored to root creation.

**HARD GATES**

- OI must be present and not in missing/rate-limited/API-error status.
- Reject price and OI accelerating together (`oi_change_pct >= 0` while `ret_5m > 0`).
- Require extreme volume: volume ratio >= 3.0 OR volume z-score >= 2.5.
- Require `ret_5m >= 3.0`, `ret_15m >= 12.0`, rejection >= 2.0, and entry distance <= 20%.
- Missing or explicitly bad climax liquidity is a veto.
- Actionable subtype requires no veto and score >= `climax_min_signal_score` (70).

**WARNINGS**

Liquidity that is below general thresholds but above climax hard limits is represented as `liquidity_warning` and grade B. The lifecycle shadow records confirmation-window, closed-candle, acceleration, squeeze, OI-continuation, rejection, liquidity, and entry-distance vetoes without replacing live V1 admission.

**SCORE**

Five boolean gates are scored at 15 points each plus 10, capped at 100: ret5, extreme volume, OI below the configured maximum, rejection, and failed retest. The source formula is `_score([ ... five flags ... ])`.

**GRADE**

A at score >= 85; B at score >= 70; otherwise C. `ClimaxEvaluation.actionable` accepts only a non-null subtype with no vetoes and grade A/B. Grade C cannot become a public climax signal.

**ADMISSION**

`VOLUME_CLIMAX_UNWIND` is selected only when all vetoes pass and score reaches 70. The runtime applies the live delivery policy and, if enabled, persists the decision; no order execution exists.

**DEDUPE**

`has_signal_for_event(symbol, event_id, strategy_subtype, model_version)` and the database unique identity `(symbol, event_id, strategy_subtype, model_version)` protect repeated full-scan/fast-monitor evaluations. The model version is `climax-v1`.

**OUTPUT**

A `ClimaxEvaluation` and, on admission, a `SignalDecision` with `strategy_type="CLIMAX_EXHAUSTION"`, subtype, branch metadata, evaluation references, provenance, and a `SIGNAL` outbox row.

### LOW_VOLUME_EXTENSION_FAILURE

**INPUT**

`EventState`, `SymbolFeatures`, the 1m OHLCV frame, and low-volume config. Inputs include equal volume windows, closed-candle count, event high/retest, extension, ratio/efficiency, post-high closes, OI, rejection, entry distance, and liquidity.

**STATE**

The branch requires an established event and the current event confirmation window. `initial_extension_pct` may be frozen for the research shadow variant; live evaluation uses current `ret_5m` by default. Independent event IDs are independent evaluation/signal identities.

**HARD GATES**

- Established event, comparable equal volume windows, at least two required closed candles, and age no more than 15 minutes.
- No new high beyond 0.30% tolerance before delivery.
- Second-leg ratio and efficiency each remain <= 0.70.
- With current config, require a close below breakout reference, lower high AND failed retest, and microstructure break.
- OI acceleration >= 1.0%, active short squeeze, resumed price acceleration without failed retest, rejection < 2.0, excessive entry distance (>15%), missing liquidity, or bad liquidity are vetoes.
- **Current source semantics:** `micro_break == close_below_breakout`. `microstructure_break_confirmed` is assigned from that close condition. This is intentionally frozen and is not corrected in Phase 0.

**WARNINGS**

`extension_below_threshold` is warning/data quality only and does not veto. Missing OI sets `oi_confirmation_state="unavailable"` and grade B. Negative OI records `possible_short_covering` and downgrades A to B. Warning liquidity caps score to the configured climax minimum and grade B when `low_volume_high_liquidity_risk_mode="warn"`; the checked-in config currently uses `warn`.

**SCORE**

Seven boolean components (event, low ratio, low efficiency, close below breakout, lower-high+failed-retest, micro break, and OI absent/decreasing) are scored at 15 points each plus 10, capped at 100. Liquidity-warning mode caps the score at 70.

**GRADE**

A at score >= 85; B at score >= 70; otherwise C. Missing/negative OI can force B. Actionable admission still requires no veto and score >= 70.

**ADMISSION**

`LOW_VOLUME_EXTENSION_FAILURE` is admitted only when all hard gates pass and score reaches 70; `extension_below_threshold` alone does not block. Live delivery policy and provenance are applied before Telegram delivery.

**DEDUPE**

The same `(symbol, event_id, strategy_subtype, model_version)` identity is used for repository dedupe and the database unique constraint. A new event ID can produce an independent evaluation/signal path.

**OUTPUT**

`ClimaxEvaluation` metadata records gate inputs, warnings, OI state, liquidity state, and `microstructure_break_confirmed` derived from the close condition; admitted output is a `SignalDecision` with `LOW_VOLUME_EXTENSION_FAILURE` subtype and persisted provenance/outbox.

## Shadow components

### ROOT_DETECTOR_SHADOW_V2

**RESEARCH / SHADOW ONLY — NOT A TRADING STRATEGY.**

Method version: `ROOT_DETECTOR_SHADOW_V2_CONTRACT_V1`. It reuses the broad V1 observation trigger, records an early-root observation and paired metrics, and does not alter EventState creation, strategy selection, scores, admission, Telegram, outbox, or execution. Liquidity, OI/squeeze safety, downstream rejection/retest, and temporal maturity remain downstream predicates.

## Global safety invariants

- **AUTOEXECUTION OFF:** no exchange order placement path exists; signal evaluation only creates decisions and persisted delivery work.
- **WATCH OFF for Telegram:** `send_watch_to_telegram` is false in the checked-in production config; WATCH decisions remain non-actionable.
- **Grade C non-public:** current public minimum is B; C is not an actionable/public signal.
- **Explicit bad liquidity remains protected:** missing or hard-bad liquidity vetoes all three admission paths.
- **Dangerous squeeze remains protected:** low-volume active-short-squeeze is a hard veto; baseline's current `warn_only` squeeze mode intentionally records extreme risk as a warning unless `block_extreme` is configured.
- **Determinism:** fixed strategy inputs, config, and code contract produce the same decision; `features.asof` is frozen and evaluators do not use wall-clock time.
- **Outbox ordering:** signal/provenance/outbox persistence precedes Telegram claim; delivery is at-least-once with retry and DEAD after five attempts.

## Architectural references (non-dependencies)

The following repositories were used only as high-level references for executable invariants, explicit lifecycle/recovery boundaries, deterministic fixtures, strategy versioning, and exchange-boundary separation. No dependency or source-code copy was added:

- [NautilusTrader](https://github.com/nautechsystems/nautilus_trader)
- [Freqtrade](https://github.com/freqtrade/freqtrade)
- [Bybit pybit](https://github.com/bybit-exchange/pybit)
