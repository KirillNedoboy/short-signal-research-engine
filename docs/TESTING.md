# Testing and reproducibility

## Baseline gates

Run from the repository root in a clean virtual environment:

```bash
python -m compileall -q app tests research
pytest -q
```

Run the configured Ruff gate from `.github/workflows/ci.yml`. The README must not claim a broader lint gate than CI actually enforces.

## Documentation gates

Before publication:

- validate Markdown links and code fences;
- scan tracked files for tokens, private URLs, real chat IDs, connection strings, database snapshots and private filesystem paths;
- verify every formula has a source file and units/lookback/missing-data behavior;
- verify every release claim has an evidence label;
- ensure `NO_SETUP`, `WATCH`, `EARLY_DROP_WARNING`, and `AUTOEXECUTION=OFF` remain distinct;
- run tests after documentation-only changes to detect accidental source edits;
- inspect `git diff --check` and `git status`.

## Release verification

For an external production artifact, record the exact release identifier, effective config hash or manifest, migration status, service state, heartbeat, data connection, scan completion, and outbox status. A successful process restart alone is not proof of a healthy signal pipeline.

## Reproducibility boundary

The public baseline can reproduce source-level tests. It cannot reproduce private production runtime state, production databases, Telegram delivery, or external release artifacts without operator-owned infrastructure. Such claims remain explicitly labelled `Production-reported`.
