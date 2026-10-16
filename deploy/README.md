# Deployment and release references

This directory contains sanitized source snapshots, safe configuration examples, operator tool copies, and systemd templates. It is a review artifact. It does not contain production configuration or data.

- `config-examples/` contains one safe example per lane/source set.
- `referenced-releases/` preserves the requested A next-start and B launcher/runtime source sets.
- `systemd/` contains generic service and timer templates with `@PLACEHOLDER@` values.
- `install.md` describes substitution and verification steps without embedding credentials.

The research collector remains collection-only. It reads the lane SQLite databases, writes its own database, and does not send Telegram messages or write to any lane database.
