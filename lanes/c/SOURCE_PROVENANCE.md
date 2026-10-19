# Lane C source provenance

`src/app` is a self-contained package. The historical C package under
`release/sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a`
is read-only reference material and is not imported by `run_live.py`.

## C semantic set

The following C modules are copied into `src/app` and remain the authority for
strategy behavior: `config.py`, `domain.py`, `baseline/`, `events/`,
`features/`, `replay/`, `signals/`, `strategies/`, `storage/`, `market/`,
`outcomes/`, and the existing observability/research serializers. Their
thresholds, feature formulas, state transitions, model versions, and signal
identity are not rewritten by the runtime layer.

## Selected runtime set

The runtime-specific modules added or selected for this candidate are:

- `runtime/contracts.py`, `coordinator.py`, `scan.py`, `pipeline.py`,
  `legacy_engine.py`, `persistence.py`, `market_runtime.py`, `delivery.py`,
  `outcomes.py`, and
  `research.py`;
- A runtime infrastructure copied into C: `runtime/scan_coordinator.py`,
  `runtime/symbol_mutation.py`, `runtime/scheduling.py`, `runtime/warmup.py`,
  `runtime/shutdown_checkpoint.py`, `runtime/blocking.py`, and the bounded
  market-data modules `market_data/hub.py`, `store.py`, `continuity.py`,
  `checkpoint.py`, `repair_queue.py`, `validation.py`, `backfill.py`,
  `parity.py`, and `models.py`.

The A `main.py`, A domain, A repository, and A migrations are not used. C
`main.py` is a new composition root; the historical C sequential loop is not
on its call path. The scan composition does not construct a Telegram client;
delivery remains a separate worker boundary after atomic persistence.

The candidate engine evaluates the full five-entry C registry and preserves
the historical priority and identity rules, including the derived
`trapped_longs:<symbol>:<event>` signal event ID. `tools/strategy_parity.py`
replays actionable fixtures through the pinned C source and candidate in
separate processes; it is a correctness check, not a production-duration
claim.

The exact source file set is recorded in `SHA256SUMS`; `SOURCE_PIN` is the
aggregate SHA-256 checked by the launcher before configuration or service
startup. Regenerate both only as part of a reviewed source change:

```text
python lanes/c/scripts/build_release_manifest.py
python lanes/c/scripts/build_release_manifest.py --verify
```

## Safe operational baseline

The sanitized baseline captured on 2026-10-16 has strategy fingerprint
`4b3a37c8f4ffc67351d6d052d083e93d9ae2efef7701407371cf768817e0a586`.
`config.example.yaml` contains the complete allowlisted C strategy payload and
loads to that same fingerprint. It keeps effective derivatives and all five
C registry strategies enabled, with the original freshness, liquidity, OI,
funding, score, and delivery thresholds.

The example runtime selects `HUB_PREFERRED` with the bounded canonical
WebSocket/store path and REST warmup/backfill. Provider, store, scheduling,
research, and snapshot controls are runtime settings and are excluded from
the strategy fingerprint. Credentials remain absent.
