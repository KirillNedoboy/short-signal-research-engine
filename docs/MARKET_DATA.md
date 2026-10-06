# Market-data contract

## Sources and scope

The bot consumes Bybit linear USDT-perpetual market data. The public baseline separates instrument/universe discovery, candles, open interest, funding, and order-book liquidity. Market-data acquisition is not an order-execution interface.

## Data-quality states

A scan is successful only after required symbols and required candle windows pass freshness and continuity checks. `NO_SETUP` is meaningful only after `SCANNED_OK`; timeout, stale, incomplete, rate-limited, or discontinuous data produces a data-quality outcome instead.

```text
REQUESTED → FETCHING → VALIDATED → SCANNED_OK
                         ├→ STALE_DATA
                         ├→ INCOMPLETE_DATA
                         ├→ RATE_LIMITED
                         └→ API_ERROR
```

The exact runtime enum names are release-specific; the semantic boundary is stable: data failure is not strategy rejection.

## Candle rules

- Use closed candles for confirmation and strategy decisions.
- Keep timestamps in UTC internally.
- Preserve source `as_of` timestamps.
- Reject a feature window when its required lookback is absent.
- Detect gaps and continuity breaks before computing a state transition.
- A reconnect or REST backfill must restore continuity before the symbol returns to `SCANNED_OK`.

## Freshness

Every required input has a maximum acceptable age. The production warning backport uses its own candle-age limit and cooldown; it does not change ordinary short admission. Freshness is checked against the decision timestamp, not merely the local receive timestamp.

## Optional derivatives and liquidity

OI, funding, spread, slippage, and depth are independent inputs. A strategy that requires one of them cannot treat missing data as confirmation. Climax and trapped-longs branches fail closed when required OI/liquidity data is unavailable. Baseline branches apply the configured liquidity and squeeze policy.

## Recovery behavior

WebSocket reconnects, REST recovery, warmup, and continuity reconciliation are operational mechanisms, not strategy signals. Recovery must be bounded and must not turn a backlog into an unbounded request storm. A symbol remains non-actionable until its data quality is restored.

## Universe boundary

The baseline shortlist uses USDT perpetuals, configured volume and exclusion rules, then combines daily movers and scan-to-scan velocity candidates. Production release settings are documented separately because the active reported mode is 100 symbols / `$5M`, while the rejected candidate was 111 / `$3M`.
