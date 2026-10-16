# Export provenance

This repository is a sanitized source export of the currently selected project. The root `app/`, `scripts/`, `tests/`, and `research/` trees come from the live Lane A release recorded in `EXPORT_MANIFEST.json`. Existing documentation and license files remain tracked unless the export manifest says otherwise.

## Included source sets

- Lane A live source: release `80201402a0877d79d9fb838f5a8bf791c5e45bd4f4fdc9262e56ef934a946871`, projected to the repository root.
- Lane A configured next-start source: release `c5900636db5b5d002e58bc44a72cd3eae1f7c0822e2c5d3450d0ad9bb28f6960` under `deploy/referenced-releases/A/`.
- Lane B launcher source: `276c7660e2e8bc8a54466567a9cc6402da8359529bdea0691a9c3f1f91b7fe3d`; Lane B runtime/config source: `909dc7374df85fc6d5bbb654ba9c6010327325830d284ccb7f053f45b19ef4a2`, each under a distinct directory.
- Lane C launcher and source layout: `lanes/c/run_live.py` and `lanes/c/release/sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a/`.
- Research collector: deployed `research_collector` source; the deployed and local source hashes differed, so the deployed source is the selected copy.
- Operator tooling: the allowlisted autoheal, ops-alert, release-drift, research-retention, and runtime-heartbeat scripts under `admin/`.

The Lane B launcher is a source snapshot without its own runtime config file. Its safe config example is derived from the tracked example; the effective runtime config example is derived from the `909dc...` release because that is the configured working directory.

## Sanitization

Only source and non-secret examples are included. Real environment files, configs, databases, WAL/SHM files, logs, runtime state, virtual environments, caches, and C vendor files are excluded. Lane config examples replace credential and identifier fields with null placeholders and replace host-specific paths with placeholders. Redaction categories are: tokens, secrets, passwords, credentials, private keys, API keys, chat/channel/user identifiers, webhooks, authorization/cookie fields, database URLs, and private filesystem paths.

Strategy thresholds and formulas are copied from the selected source trees without tuning. The export does not enable delivery changes or autoexecution.

## Verifying the export

`EXPORT_MANIFEST.json` is the allowlist and `EXPORT_SHA256SUMS` contains the file hashes. Use the generic verification command in `deploy/install.md` before publishing or installing. The generated systemd files are templates; replace `@...@` placeholders with local paths and review read/write boundaries before enabling them.
