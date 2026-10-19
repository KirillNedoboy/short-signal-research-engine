# Lane C event-driven runtime

This directory contains the candidate C runtime. `run_live.py` pins imports to
the self-contained `src/` tree. The historical `release/sha256-aa4...` tree
is retained as an immutable reference and is not on the production import
path of this candidate.

## Runtime boundary

- C `FeatureBuilder`, lifecycle, `SignalEngine`, strategy adapters, identity,
  state store, and repository are copied into `src/app` from the pinned C
  source set.
- The runtime uses the selected A infrastructure modules for normalized
  WebSocket data, continuity, bounded canonical store, repair and P0/P1 scan
  scheduling. When the configured provider is `HUB_PREFERRED` or
  `HUB_SHADOW`, the store is preferred and REST is limited to startup/backfill
  for symbols without a ready frame.
- Each symbol has one read/modify/write lock. Every scheduled task records one
  terminal status. Missing, stale, disconnected, or malformed data is a
  blocked evaluation and is never treated as zero.
- Persistence and outbox enqueue happen before a delivery wake. This runtime
  has no Telegram notifier and does not send over the network from the scan
  consumer. Outcome and research work are bounded background components.
- `runtime/legacy_engine.py` preserves the C admission order: climax branches,
  baseline lifecycle, trapped-longs, then baseline fallback. It evaluates all
  five registry entries, applies final delivery/provenance gates, and returns a
  deferred decision; it does not persist, retry, or send.

The existing C strategy thresholds, formulas, model versions, and signal
identity are kept in the copied C modules. The runtime layer does not enable
additional A trading logic automatically.

## Checks and benchmark

Run the scoped checks from the export root:

```text
python -m pytest lanes/c/tests/test_runtime_contracts.py lanes/c/tests/test_runtime_failure_paths.py lanes/c/tests/test_pipeline_failure_paths.py -q
python lanes/c/tools/strategy_parity.py
python -m compileall -q lanes/c/src/app lanes/c/tests lanes/c/tools
python lanes/c/tools/benchmark_runtime.py --windows 120 --symbols 100
```

The benchmark loads the complete strategy payload from `config.example.yaml`
and reports its fingerprint. Its P0 p95 starts at window admission and includes
queue wait; terminal failures are listed separately from successful
`SCANNED_OK`/`NO_SETUP` evaluations. It exits nonzero when the window p95,
P0 p95, or successful coverage target fails. The run is deterministic,
in-memory scheduler input and is explicitly synthetic; it is not a production
24-hour measurement.

## Shadow gate

Before a server run, use [SHADOW_GATE.md](SHADOW_GATE.md). Shadow must have a
separate database, state, lock, runtime path, and disabled delivery owner. Do
not point it at a production Lane C database for writes. The candidate has no
Telegram sender, but the surrounding service unit must still use a closed
delivery gate and a separate process identity.

## Release and rollback

`lanes/c/scripts/build_release_manifest.py` generates a deterministic SHA-256
manifest for `src/` while excluding caches. `SOURCE_PIN` is the launcher
source pin. See [ROLLBACK.md](ROLLBACK.md) for the cutover sequence; deployment
is a separate reviewed operation and is not performed by this export.
