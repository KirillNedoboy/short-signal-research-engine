# Configuration reference

## Precedence

Use the release's checked-in defaults first, then an operator-owned configuration file, then environment-variable overrides where supported. Never commit the operator configuration, database, token, chat identifier, or private endpoint.

## Important groups

- **Market:** category, quote/settle coin, contract type, status, exclusions, shortlist size, minimum 24h volume.
- **Features:** lookback periods, EMA/ATR/RSI windows, volume windows, freshness limits.
- **Events:** pump horizons, stretch thresholds, short-zone method and bounds, pullback limits, event expiry.
- **Admission:** minimum score, grade policy, rejection/confirmation gates, squeeze and liquidity thresholds.
- **Delivery:** live branch toggles, WATCH behavior, outbox retry/lease policy, Telegram enablement.
- **Research:** shadow lifecycle, replay, cohort and outcome telemetry. Research toggles never imply live delivery.
- **Secrets:** Telegram token, chat identifiers, credentials and private URLs. These stay outside Git.

## Known baseline ambiguity

`app/config.py` and `config.example.yaml` contain different example/default values for several thresholds, including pullback minimum, short-zone lower bound, VWAP distance, and volume z-score. This is intentional documentation debt, not a value to guess around. For a reproducible deployment, pin one release and record the effective configuration after validation.

## External deployment boundary

This repository contains example configuration only. External deployments may use different universes, thresholds, rate limits, databases, and delivery toggles. Review and record the effective configuration separately; do not infer it from a historical report or from an example file.

The public contract remains manual-only:

```text
Autoexecution: OFF
```
