# Security policy

## Never commit secrets

Keep credentials and runtime state outside Git:

- copy `.env.example` to `.env` on the server;
- keep Telegram bot tokens, chat IDs, API keys, and private endpoints in `.env` or a secret manager;
- do not commit SQLite databases, logs, local exports, or virtual environments;
- rotate a credential immediately if it was ever committed or pasted into a public issue, chat, or pull request.

The repository intentionally contains configuration *schemas and safe defaults*, not live credentials.

## Reporting a vulnerability

Open a private GitHub security advisory when possible. Do not publish a token, exploit details, or personally identifying data in a public issue.
