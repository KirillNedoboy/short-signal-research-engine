# Strategy Matrix

| Strategy/stream | Purpose | Hard gates | Soft evidence | Delivery status |
|---|---|---|---|---|
| Baseline short reversal | Evaluate post-expansion reversal after lifecycle confirmation | Data freshness, event state, short-zone and risk vetoes | Feature score, liquidity and context | Manual signal contract; test-backed |
| Volume climax observation | Record exhaustion/climax context for later outcome analysis | Valid candle/volume evidence | Relative volume and follow-through | Observation/research only |
| Trapped-longs reversal | Study stricter reversal confirmation and open-interest context | Required sequence and evidence completeness | Supporting market structure | Experimental/shadow; not live admission |
| Low-volume extension | Study reduced-liquidity edge cases | Explicit low-volume contract and data-quality gates | Relative volume context | Replay/regression research |
| WATCH/data quality | Separate incomplete evidence from a negative setup | Coverage/freshness/availability checks | Diagnostics | Informational; never an order |

A hard-gate failure is not equivalent to a profitable opposite-side signal. Missing evidence remains UNKNOWN or a data-quality outcome.
