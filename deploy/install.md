# Generic installation notes

These steps are intentionally host-neutral. Run them only after reviewing the source, substituting local paths, creating the service account, and provisioning credentials outside the repository.

1. Create a dedicated installation root and a separate writable collector data directory.
2. Create the Python environment from the pinned dependency files. Do not copy a virtual environment or the C `vendor/` tree into the repository.
3. Copy a sanitized config example to a host-only config file and fill credentials through a protected environment file. Keep lane database paths read-only for the collector.
4. Replace every `@PLACEHOLDER@` in the relevant unit template, review `ReadOnlyPaths` and `ReadWritePaths`, then install the unit and timer files with normal systemd tooling.
5. Run `python scripts/verify_export.py` and the focused checks described in the repository README before enabling a unit.

The live bot services are outside this export workflow. Do not change their databases, configs, signals, delivery policy, or service state as part of installing the research collector.
