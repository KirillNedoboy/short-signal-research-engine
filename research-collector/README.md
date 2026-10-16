# Research microstructure collector

This is a separate, collection-only process for the live A/B/C lanes. It
does not import lane strategy code, send Telegram messages, or write to the
lane SQLite databases. Existing signals and delivery rules stay unchanged.

The process reads delivered signal rows and only the `status`, `channel`, and
`sent_at` fields for the matching `telegram_delivery_outbox` rows. It never
reads outbox payloads or error text. Source SQLite connections use
`mode=ro` plus `PRAGMA query_only=ON`.

For each delivered candidate it records the lane, strategy, model version,
SHA-256 of the configured non-secret `config.yaml`, sanitized decision fields,
signal/delivery timestamps, and a shared event link. Signals from different
lanes for the same symbol within the merge window are attached to one event.
At every process start a source watermark is established. Only deliveries
after that watermark and inside `candidate_freshness_sec` (15 minutes by
default) can create a collecting event; old source rows are advanced past and
never consume the ten-symbol capacity.
If an existing research database contains events but has no collector run
watermark, startup stops with a clear error; use a new `storage.db_path` so a
historical smoke database cannot become the production cohort.

After a candidate is accepted, the service observes at most ten symbols at a
time:

- Bybit linear perpetual trades, L50 orderbook, ticker, OI, funding, and
  liquidation messages;
- Bybit spot when the public ticker probe says that pair exists;
- Binance spot trades and depth when the pair is available.

Bybit `seq` is treated as a monotonic correlation field. It is not required to
increase by one. A book update regression, Binance `pu` gap, stale stream, or
transport reconnect makes the affected book UNKNOWN until a fresh snapshot is
accepted. Trade and book freshness are independent. Bybit liquidation `S` is
stored as position side and `p` as bankruptcy price.

Metrics are persisted as typed `{status, value, reason}` objects. The service
never turns missing feed data into zero. If both spot probes say that a pair is
not available, snapshots are labeled `PERP_ONLY`; if a known spot feed is
temporarily broken, the profile remains `SPOT_UNKNOWN` and spot metrics are
UNKNOWN.

Outcomes use the actual outbox delivery timestamp. Price samples support 15m,
1h, and 4h observations, first passage of +10% and +20% against the short,
and the order in which those levels were crossed. A descriptive 7-day report
and a first 28-day report are stored in the research database. Reports do not
change live signals.

## Local dry start

Use Python 3.11+ on the collection host. Copy `config.example.toml` to
`config.toml`, replace only the three read-only database paths and the
non-secret config paths, then install the one runtime dependency:

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
mkdir -p data
.venv/bin/python -m research_collector --config config.toml --log-level INFO
```

The first start creates only `data/research-collector.sqlite3`. It does not
create directories or files below the A/B/C source roots.

## systemd installation

Copy this directory to `/opt/short-bot-research-collector`, create the venv,
copy and edit `config.example.toml` as `config.toml`, then install the unit:

```sh
install -d -m 0750 /opt/short-bot-research-collector/data
install -m 0644 research-collector.service /etc/systemd/system/short-bot-research-collector.service
systemctl daemon-reload
systemctl enable --now short-bot-research-collector.service
```

The unit intentionally runs as root because the active Lane A database is
root-readable only. `ProtectSystem=strict` and a narrow `ReadWritePaths` keep
the collector's writable area separate. Do not grant it write access to a
lane database. Review `ReadOnlyPaths` if the active lane roots differ from the
example.

## Storage policy

The research database has a hard 5 GiB target and a 10 GiB free-space reserve.
Raw trades, orderbook frames, and liquidations are deleted by age first and
then by oldest row count. WAL checkpointing and incremental vacuum run after
pruning. If pressure remains, high-cadence feature/price timelines are
compacted oldest-first. Signal snapshots, outcomes, health, and reports remain
protected. If pressure still remains, raw writes pause while those protected
records continue.

The collector is intentionally descriptive. It does not make, rank, block, or
alter a signal.
