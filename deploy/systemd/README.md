# Generic systemd templates

Every file in this directory is a template. Replace the `@...@` values before installation:

- `@COLLECTOR_ROOT@`, `@COLLECTOR_CONFIG@`, `@COLLECTOR_DATA@`
- `@LANE_A_DB@`, `@LANE_B_DB@`, `@LANE_C_DB@`
- `@ADMIN_ROOT@`, `@REPO_ROOT@`, `@ENV_FILE@`

The collector template uses a root service account because the live Lane A SQLite file is root-owned. Keep source databases read-only and grant write access only to the collector data directory. The ops templates are oneshot jobs; timers are optional and must be reviewed for the host's operational policy. No template contains a token, chat ID, or secret value.
