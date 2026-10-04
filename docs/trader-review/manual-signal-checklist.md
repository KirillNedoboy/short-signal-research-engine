# Manual Signal Checklist

Before acting on any received message, independently check:

- [ ] Symbol, market type, timestamp, and candle-close status are present.
- [ ] The signal is distinguishable from WATCH, early-drop warning, and data-quality outcomes.
- [ ] Entry/reference, invalidation, and risk context are explicit; do not infer missing values.
- [ ] Market data is fresh enough for the stated strategy contract.
- [ ] Liquidity, spread/order-book evidence, and coverage are known or clearly marked UNKNOWN.
- [ ] The lifecycle and strategy version are identified.
- [ ] The message is treated as informational/manual only.
- [ ] No order is placed automatically by this repository.
- [ ] Independent position sizing, venue rules, and risk controls are completed before any human decision.

A missing field is a reason to pause and classify the signal, not to fill it with an assumption.
