# Signal pipeline

```text
instrument discovery
  → shortlist
  → candle/derivative ingestion
  → freshness + continuity validation
  → feature builder
  → pump/event detector
  → pullback/state lifecycle
  → strategy evaluator
  → hard gates
  → liquidity + squeeze vetoes
  → score + grade
  → admission policy
  → database + provenance
  → transactional outbox
  → Telegram worker
  → delivery/outcome telemetry
```

## Stage contract

1. **Universe:** select eligible Bybit USDT perpetuals and apply configured exclusions/turnover rules.
2. **Market data:** load closed candles and optional derivatives/liquidity data; preserve timestamps and quality state.
3. **Features:** calculate returns, VWAP, EMA, ATR, RSI, volume statistics, candle geometry, OI and derived distances.
4. **Event:** detect a pump using the first eligible horizon plus a stretch condition; record base, high, range and expiry.
5. **Lifecycle:** observe pullback, VWAP/range-floor behavior, short-zone entry and expiry.
6. **Strategy:** evaluate baseline and live climax branches separately from shadow/research branches.
7. **Gates:** apply hard conditions, freshness, liquidity, squeeze, breakout-risk, score and grade policies.
8. **Persistence:** store the signal, reason codes, feature snapshot, release provenance and decision timestamp.
9. **Delivery:** enqueue a separate outbox item and process it with bounded retry/idempotency semantics.
10. **Outcome:** record delivery and optional virtual outcome telemetry; outcome is not execution/fill evidence.

## State semantics

The baseline event lifecycle is:

```text
IDLE → PUMP_DETECTED → PULLBACK_OBSERVED → SHORT_ZONE_ACTIVE
                                      ├→ SIGNAL_SENT
                                      └→ EXPIRED
```

Climax lifecycle and trapped-longs confirmation are separate evaluation/shadow state machines. They must not be presented as ordinary baseline event states unless the release explicitly promotes them.

## Failure distinction

- `SCANNED_OK + NO_SETUP`: valid negative strategy result.
- `INCOMPLETE_DATA`, `STALE_DATA`, timeout, or gap: data-quality failure; do not call it `NO_SETUP`.
- `WATCH`: evaluated candidate withheld from public actionable delivery.
- `EARLY_DROP_WARNING`: independent non-actionable warning path.
