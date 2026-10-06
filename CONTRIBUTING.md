# Contributing

## Development setup

```bash
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env
```

Use placeholder credentials for tests. The test suite does not need live Telegram or Bybit credentials.

## Quality gates

Run before opening a change:

```bash
.venv/bin/pytest -q
```

Keep changes focused, add regression tests for behavior changes, and update the relevant documentation/configuration reference when a parameter or signal rule changes.

## Runtime boundary

This bot produces Telegram signals and persists observations. It does not place exchange orders automatically. Do not add live order execution without a separate design, explicit controls, and tests.
