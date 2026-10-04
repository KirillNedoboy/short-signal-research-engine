# ReplayBundle v1

ReplayBundle is an offline, research-side evidence artifact. It does not change
market scanning, strategy admission, persistence, delivery, or deployment.

`build_bundle` creates a never-before-existing run directory in a sibling staging
directory. It writes Parquet market/input/decision/outcome/provenance artifacts,
sanitized decision configuration, reports, and canonical JSON manifests; verifies
their physical SHA-256 hashes; then atomically publishes `COMPLETE`.

Only `BASELINE_PULLBACK`, `VOLUME_CLIMAX_UNWIND`, and
`LOW_VOLUME_EXTENSION_FAILURE` are strategies. Replay only deserializes immutable
inputs and dispatches to `SignalEngine.analyze` or `evaluate_climax_bundle`.

Canonical JSON is UTF-8 with sorted keys and compact separators. UTC timestamps
end in `Z`, lists retain their order, maps sort by key, and finite floats are
normalized using `.15g`. The allowlisted strategy config excludes credentials,
chat IDs, database URLs, endpoints, schedules, and delivery controls. Runtime
strategy fingerprinting uses the same allowlist.

V1 only accepts Bybit Linear USDT perpetual 1m OHLCV. Rows have stable identities,
availability timestamps, and must not exceed the dataset cutoff. Inputs cannot use
features newer than their decision time. A bundle produced from a dirty source is
refused by default; `--allow-dirty` records `NON_CANONICAL` and only a status hash,
never secret values.

Use `python -m scripts.replay_bundle build|verify|replay|report`. PyArrow is a
research-only dependency from `requirements-research.txt`; live startup imports no
`app.research.replay` module.
