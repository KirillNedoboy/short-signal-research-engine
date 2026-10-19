# Lane C shadow gate

The candidate may be evaluated in shadow only after a verified release archive
and scoped parity/failure checks are recorded. A synthetic benchmark does not
count as a production 24-hour observation.

## Supervisor interface

Run the bounded supervisor with a dedicated writable root and an exact child
command after `--`:

```text
python lanes/c/tools/shadow_guard.py --root /var/lib/short-bot-lane-c-shadow -- \
  /opt/short-bot-lane-c-shadow-release/.venv/bin/python \
  /opt/short-bot-lane-c-shadow-release/run_live.py
```

### Mode-0600 production A probe

When the A database is mode `0600`, use the root-guard template
`deploy/lane-c-shadow-root-guard.service.example`. The service runs only the
supervisor as root so its read-only probe can open A. Its child command is
dropped before exec with this exact wrapper:

```text
/usr/bin/setpriv --reuid=short-bot-c-shadow --regid=short-bot-c-shadow \
  --clear-groups --no-new-privs -- \
  /opt/short-bot-lane-c-shadow-release/.venv/bin/python \
  /opt/short-bot-lane-c-shadow-release/run_live.py
```

The unit's `ExecStartPre` creates and explicitly `chown`s the isolated root,
`db/`, `runtime/`, `runtime/cache/`, `runtime/tmp/`, `locks/`,
`checkpoints/`, `comparisons/`, `raw/`, and `logs/` to
`short-bot-c-shadow`, all with mode `0700`. This is required because the
supervisor also enforces `0700` on those directories. Root-created gate and
raw-control JSON files use the unit's `UMask=0022`, so the dropped child can
read them. The A/B probe config is the isolated file
`/var/lib/short-bot-lane-c-shadow/p95-probes.json`; the unit never loads a
production `.env` or `EnvironmentFile`. Production paths are explicit
read-only mounts in the unit and their ACLs/modes are left unchanged.

Provision the isolated read-write root before starting the unit. This must be
done outside the systemd namespace: `ProtectSystem=strict` is applied before
`ExecStartPre`, so `ExecStartPre` cannot create a missing `/var/lib` root on a
fresh host.

```sh
shadow_root=/var/lib/short-bot-lane-c-shadow
shadow_user=short-bot-c-shadow
sudo install -d -m 0700 "$shadow_root"
sudo install -d -m 0700 \
  "$shadow_root/db" "$shadow_root/runtime" "$shadow_root/locks" \
  "$shadow_root/checkpoints" "$shadow_root/comparisons" \
  "$shadow_root/raw" "$shadow_root/logs" \
  "$shadow_root/runtime/cache" "$shadow_root/runtime/tmp"
sudo chown "$shadow_user:$shadow_user" \
  "$shadow_root" "$shadow_root/db" "$shadow_root/runtime" \
  "$shadow_root/locks" "$shadow_root/checkpoints" \
  "$shadow_root/comparisons" "$shadow_root/raw" "$shadow_root/logs" \
  "$shadow_root/runtime/cache" "$shadow_root/runtime/tmp"
sudo chmod 0700 \
  "$shadow_root" "$shadow_root/db" "$shadow_root/runtime" \
  "$shadow_root/locks" "$shadow_root/checkpoints" \
  "$shadow_root/comparisons" "$shadow_root/raw" "$shadow_root/logs" \
  "$shadow_root/runtime/cache" "$shadow_root/runtime/tmp"
sudo test -r "$shadow_root/p95-probes.json"
sudo systemctl daemon-reload
sudo systemctl start short-bot-c-shadow.service
```

The release path must be the verified extracted archive; its `.venv` may be a
verified symlink to the pinned runtime environment. Keep the validation root
separate from production and traversable only as provisioned by the operator.

The older unprivileged command above remains suitable only when the selected
read-only probe files are already readable by that service account.

The supervisor creates `db/`, `runtime/`, `locks/`, `checkpoints/`,
`comparisons/`, `raw/`, and `logs/` below that root. It sets `DB_URL` to the
shadow database, `AUTOEXECUTION=OFF`, `SHADOW_DELIVERY_GATE=CLOSED`, and
removes inherited Telegram/database credentials before `Popen`. It also sets
`SHADOW_CONFIG_PATH=<ROOT>/config.yaml` and
`SHADOW_ENV_PATH=<ROOT>/shadow.env`; while `SHADOW_MODE=1`, the application
must use those paths, refuse the release directory's `.env` or any production
`.env`, and fail before opening a DB connection when the selected config or DB
path is outside `ROOT`. The supervisor never loads an `EnvironmentFile` or a
`.env` from a production release.

The command must not contain a known production lane path. The root and every
existing parent must be free of symlinks. Production data may be supplied only
to the optional read-only p95 probes; it is never assigned to the child
database URL and is opened with SQLite `mode=ro`. The guard process needs read
permission for those copies. Keep existing production mode/ACLs unchanged;
the root-guard template is the practical mode-0600 arrangement: root reads
the probes while `setpriv` drops the child to `short-bot-c-shadow` before the
candidate starts.

The child writes bounded JSON Lines to `SHADOW_EVENT_FILE`:

```json
{"type":"capability","raw_capture_disable":true}
{"type":"readiness","ready":true}
{"type":"raw_capture_disabled"}
{"type":"rest_429","count":1}
```

The capability event is required before a storage threshold can be handled.
When `--raw-storage-threshold-bytes` is configured, the supervisor measures
the exact `raw/` directory usage and writes
`{"action":"disable_raw_capture"}` to `SHADOW_RAW_CONTROL_FILE` as soon as
that watermark is reached. The shipped service templates configure a 256 MiB
raw watermark and a 90-second acknowledgement grace, which covers the
bounded control polling cadence. The 1 GiB `--max-storage-bytes` limit remains a hard total-root
limit; after an acknowledged raw-disable request the supervisor continues
while total usage is below that limit, so the candidate can finish bounded
metadata or comparison work. If the child does not acknowledge the request,
or if total root usage reaches 1 GiB, the supervisor stops fail-closed with a
machine-readable reason and does not delete arbitrary raw or comparison data.
The report records total bytes, raw bytes, the configured watermark, and
request/acknowledgement state. WS/REST 429 event types stop the child
immediately. The report is `shadow-guard-report.json` under the shadow root
and is also printed as JSON.

### Application control API

The launcher validates the root and creates one process-local control object
from `src/app/runtime/shadow.py` before configuration or provider startup:

```python
from app.runtime.shadow import ShadowPaths, get_shadow_control

paths = ShadowPaths.from_environment()
control = get_shadow_control(paths)
```

`run_live.py` emits the capability event during bootstrap. The composition
root must call `control.set_readiness(True, reason="started")` only after its
market/runtime startup is complete, call `control.poll_control()` at each
bounded scan boundary, and call `control.shutdown(reason="shutdown")` on every
exit path. Shutdown emits `{"type":"readiness","ready":false}`. A control
write failure raises `ShadowSafetyError`; the runtime must stop admission and
exit rather than continue without the safety stream.

Every REST and WebSocket provider boundary should pass its exception or error
envelope to `control.record_rate_limit(error, transport="rest"|"ws")`.
`ProviderRateLimited`, HTTP 429, and Bybit 10006 produce `rest_429` or
`ws_429`, which the supervisor treats as an immediate stop. The optional
`install_rate_limit_handler(control)` helper bridges provider log records and
sets `control.safety_failed` if the event cannot be persisted.

Raw and future comparison inputs may use
`control.write_bounded_artifact(relative_path, payload, comparison=...)`.
The writer refuses paths outside `raw/` or `comparisons/`, enforces byte
budgets, and stops raw capture after the supervisor action is acknowledged.
Writing an artifact never reports parity or `comparison PASS`; a real
reference comparison must produce that result separately.

Defaults are a 2 GiB service memory limit, 1 GiB shadow storage, 10 GiB free
disk reserve, and a 24-hour maximum runtime. The guard terminates only its
child process group and performs bounded cleanup. The systemd template adds
read-only protection for A/B/C production roots; `MemoryMax=2G` is enforced by
systemd. A report is `completed` only when the child was still running at the
maximum-runtime deadline and every enabled gate passed; the expected cleanup
termination is recorded in the child report. Any early child exit, including
exit code 0, remains `stopped` and returns a failure status.

## Offline same-input reference replay

After a bounded capture has been collected, replay it offline with the frozen
reference package and the candidate package in separate worker processes. Pass
both package roots explicitly; `--old-root` must point to an immutable extracted
release `app/` directory and `--candidate-root` to the candidate `src/` tree:

```text
python lanes/c/tools/replay_shadow.py \
  --capture-root /var/lib/short-bot-lane-c-shadow \
  --old-root /opt/short-bot-lane-c-shadow-release/release/sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a/app \
  --candidate-root /opt/short-bot-lane-c-shadow-release/src \
  --max-records 4096 \
  --max-input-bytes 268435456 \
  --max-seconds 300 \
  --output /var/lib/short-bot-lane-c-shadow/comparisons/shadow-replay-report.json
```

The replay defaults are `4096` records, `268435456` input bytes (256 MiB),
and `300` seconds. The capture defaults are one sampled record per event,
`1073741824` raw bytes (1 GiB), `262144` bytes per record (256 KiB), and
`67108864` aggregate bytes (64 MiB). Sampling, dropped or oversize records,
raw-control stops, byte-budget exhaustion, malformed or truncated input,
worker errors, and an uncaptured required final fresh recheck are counted as
`UNKNOWN`; they never become `PASS` through omission. The aggregate remains
available after raw capture is stopped.

The report's `reference_scope` identifies the normal old side as the frozen
`app.main.ShortSignalBot._process_symbol` harness and the normal candidate side
as `LaneCStrategyPipeline.evaluate`. Fast-monitor captures use the actual old
`app.main.ShortSignalBot._evaluate_and_send_climax` method and require the
candidate `LaneCStrategyPipeline.evaluate_climax_only` seam; a missing fast
seam is `UNKNOWN`, never a full-pipeline fallback. Both sides are compared at
the `pre_persistence` boundary. The harness deep-copies the old decision and
state at the repository persistence call; post-persistence state is diagnostic
only.
For admission branches that require uncaptured historical repository ownership
or attempt context, the old worker emits an error and the comparison is
`UNKNOWN` rather than inventing that context. For each otherwise complete
input, the report compares the actual feature output, state, decision,
evaluator results, identity, actionable flag, and blocked reason.
The fast no-action contour keeps the frozen method's unset `blocked_reason`
(`null`); the candidate replay must preserve that value. Diagnostic markers for
missing input or an unavailable fast seam are reported as `UNKNOWN`, never
rewritten into a semantic result.
Scheduled-universe and historical Telegram delivery timing comparisons are
`UNSUPPORTED`; this report cannot establish a historical production 24-hour
observation. The replay reads only captured input files and does not open a
production database, contact providers, invoke the live service, or send
Telegram messages.

## Read-only A/B p95 probe

To observe both production cohorts, provide a repeatable named probe config:

```text
python lanes/c/tools/shadow_guard.py --root /var/lib/short-bot-lane-c-shadow \
  --p95-probes /var/lib/short-bot-lane-c-shadow/p95-probes.json -- \
  /opt/short-bot-lane-c-shadow-release/.venv/bin/python \
  /opt/short-bot-lane-c-shadow-release/run_live.py
```

The config is JSON and must include explicit ranges, so a changing scheduled
universe cannot silently become an equality check:

```json
{
  "probes": [
    {
      "name": "A",
      "baseline": "/var/lib/short-bot-lane-c-shadow/baseline-a.json",
      "production_readonly_db": "/opt/short-telegram-bot-lite/data/bot.sqlite",
      "cohort_min": 100,
      "cohort_max": 150
    },
    {
      "name": "B",
      "baseline": "/var/lib/short-bot-lane-c-shadow/baseline-b.json",
      "production_readonly_db": "/opt/short-telegram-bot-lite-admin/lane-b/data/bot.sqlite",
      "cohort_min": 18,
      "cohort_max": 22
    }
  ]
}
```

The baseline JSON may contain `p95_ms` with `scheduled_symbols`,
`durations_ms` with matching `scheduled_symbols`, or `records` containing
`duration_ms` and `scheduled_symbols`. SQLite probes read only
`market_scan_cycles.duration_ms`, `scheduled_symbols`, and the ordering column
`cycle_completed_at` (falling back to `cycle_started_at`). Rows outside the
configured range are excluded from the p95. Each interval is anchored to the
supervisor's UTC start time and queries the recorded cycle timestamp inside a
fixed `[interval_start_utc, interval_end_utc)` window; SQL query execution
time is never used as the window. The report records that window, the
timestamp column, sample count, and comparable sample timestamp range. The
query is bounded to 10,000 rows. A missing timestamp column or insufficient
recent rows stops the child with
`ab_p95_insufficient_recent_data:<probe>` and leaves the gate failed. Two
consecutive 30-minute intervals above 120% of baseline stop the child for the
named probe. Missing p95, missing scheduled cohort, and insufficient
comparable intervals never pass by default. The legacy `--baseline` plus
`--production-readonly-db` flags remain as a single exact-cohort compatibility
mode; use `--p95-probes` for A/B operation.

## Required checks

```text
python lanes/c/scripts/build_release_manifest.py --verify
python lanes/c/scripts/build_release_manifest.py --verify-archive /tmp/lane-c.tar.gz
python lanes/c/scripts/compat_validate.py --db /path/to/private-copy.sqlite --report /tmp/c-compat.json
```

Stop the shadow and close its gate if source pin verification, identity
parity, state durability, outbox isolation, task coverage, a new 429 event,
free disk reserve, or either p95 interval fails.
