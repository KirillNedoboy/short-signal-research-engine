# Lane C runtime: validation record

Status: implementation is complete for the approved design. Scoped Linux
candidate verification and release provenance are recorded. Overall validation
remains IN PROGRESS because the isolated 24-hour shadow is NOT_STARTED at this
report snapshot. Production Lane C has not been switched, and no production
cutover has been performed.

## Pinned references

- Repository base: `5bbc0155d7bab3a5ef88351ce2e74f607f90a221`.
- Running A runtime: `80201402a0877d79d9fb838f5a8bf791c5e45bd4f4fdc9262e56ef934a946871`.
- C semantic reference: `sha256-aa4f402d242824d0404cf07f65e10a9e3d14d4416dadd790e600373e6a42750a`.
- Candidate source pin: `85fc8fc00d8f5ec1291839b0e2a701e33289718d06726865c86200f043c90f04` (the normalized `lanes/c/SOURCE_PIN` aggregate).
- Candidate release archive: `7dd001008bbff38bf122a172806078ff7e973095a07252d984f9427c75df6bc7` (`157` files, `139` source files).
- Validation bundle archive: `d3c9b30d09cc2515d8b4396a1b49bc1cd8fd6c41ba1bd2bd91dea73def2b9a42`.
- Effective C strategy fingerprint: `4b3a37c8f4ffc67351d6d052d083e93d9ae2efef7701407371cf768817e0a586`.

## Completed preparation

| Check | Date | Observed result |
|---|---|---|
| Actual C database compatibility | 2026-10-17 (historical check) | The six C storage modules (`__init__`, `db`, `identity`, `migrations`, `models`, `repository`) were identical; the private-copy schema/version and preserved signal/outbox evidence remain historical records |
| Additive migration bridge on private copy | 2026-10-17 | No migration required; no removed tables or columns |
| Candidate source pin and sanitized export | 2026-10-19 | `SOURCE_PIN` is normalized; root allowlist/checksums are generated from staged Git index blob bytes, retain `LICENSE`, and exclude secrets, databases, and the historical C vendor tree |
| Clean Linux dependency installation | 2026-10-19 | PASS in the frozen candidate validation bundle; `139` source files verified at source pin `85fc8fc0...`; no production DB writes |
| A/B read-only probe under `ProtectSystem=strict` | 2026-10-19 | PASS in the protected scoped validation; final guard ran with `MemoryMax=2G`, `CPUWeight=10`, `PrivateNetwork=yes`, and `NoNewPrivileges=yes` |
| Unprivileged dependency imports under service protection | 2026-10-19 | PASS in the scoped Linux validation; `51 passed, 11 warnings in 19.80s` |
| Final raw-capture watermark guard | 2026-10-19 | `1 passed, 9 deselected` for `shadow_guard_requests_raw_disable_at_watermark_without_total_stop`; unit `lane-c-cguard-final-raw-85fc.service` |
| Linux valid-data benchmark | 2026-10-19 | `120` windows × `100` symbols; `12000 SCANNED_OK`, coverage `1.0`, window median `4536.226 ms`, p95 `4871.118 ms`, P0 p95 `11998.862 ms` over `100` P0 samples, `0` P0 deadline misses; synthetic seams disclosed below |
| Candidate release archive | 2026-10-19 | SHA-256 `7dd001008bbff38bf122a172806078ff7e973095a07252d984f9427c75df6bc7`; remote verification `157` files / `139` source files |
| Production process check | 2026-10-19 | A/B/C/collector running with unchanged PIDs; no shadow service running |

The historical database compatibility check used a consistent private SQLite
copy. It did not scan the large `strategy_observations` table. The temporary
database copy was deleted after the check; aggregate reports were retained
outside Git. Production databases were not migrated or modified. The
2026-10-19 production process observation also reports unchanged services; it
does not establish candidate readiness.

## Candidate handoff

- The candidate source/import boundary is implemented under `lanes/c/src/app`;
  the frozen C release remains an explicit offline reference package.
- The candidate is pinned to `85fc8fc00d8f5ec1291839b0e2a701e33289718d06726865c86200f043c90f04`;
  `EXPORT_MANIFEST.json` and `EXPORT_SHA256SUMS` describe the staged sanitized
  export. The original launcher remains attributed to repository base
  `5bbc0155d7bab3a5ef88351ce2e74f607f90a221`, while the frozen C release is
  retained as the semantic reference.
- Bounded raw input/state capture, strict same-input replay, and fast-monitor
  climax-only dispatch are implemented. Missing input, missing final recheck,
  unavailable fast seams, and replay errors remain `UNKNOWN`.
- Focused local checks cover the frozen normal process path and the fast
  no-action path. The final scoped Linux run recorded `51 passed, 11 warnings
  in 19.80s`; the final raw-capture guard recorded `1 passed, 9 deselected`.
- The Linux benchmark used the same candidate source pin and recorded `12000
  SCANNED_OK` evaluations across `120` windows × `100` symbols. It used
  `REST_ONLY` with an `in_memory_snapshot_seam` (WebSocket throughput was not
  tested), `test_compatibility_seam` for the outcome worker, fresh cached
  derivatives in each window, and an in-memory repository with
  `save_signal_calls=0`; it makes no production DB-throughput claim. The
  effective strategy fingerprint is `4b3a37c8...`, with all five C registry
  entries enabled. The pre-watermark benchmark used the same C source pin; the
  guard was verified separately.
- These checks are release/protection verification evidence. They do not
  represent real-market or completed 24-hour shadow evidence.
- The shadow guard treats `256 MiB` as the configured raw-capture watermark
  with a `90s` acknowledgement grace; the `1 GiB` value is the hard total
  shadow-root limit. The watermark can be crossed during that bounded grace
  while raw capture is being disabled, so these values are not a claim of a
  strict per-recorder `256 MiB` cap.
- The production services and production databases remain unchanged. No
  candidate process was started by this handoff; the actual shadow start/end
  timestamps will be appended after root start approval.

## Execution status (2026-10-19)

- Source pin, release archive, scoped Linux validation, and the final raw-capture
  watermark guard are recorded above.
- The actual isolated shadow is NOT_STARTED. Overall validation remains
  IN PROGRESS; no 24-hour elapsed-time or parity result is claimed.
- Production A/B/C/collector processes and production databases remain
  unchanged.

## Rejected development measurement

The first Windows 120-window, 100-symbol benchmark produced 853 `SCANNED_OK`
and 11,147 `STALE_DATA` statuses. Its reported coverage of 1.0 incorrectly
counted stale tasks as successful evaluations. This run is rejected and is
not performance acceptance evidence. The duplicate Windows run was stopped.

## Remaining verification gates

- Preserve the recorded Linux coverage, latency, input timestamp lineage, seam
  disclosures, source pin, and archive hash in the final handoff.
- Verify launcher isolation and structured supervisor events, including rate
  limits and raw capture control, during the actual shadow.
- Collect bounded real input/state capture and run exact offline comparison with
  frozen C; missing inputs and final rechecks remain `UNKNOWN`.
- Complete the isolated 24-hour shadow report before user review of production
  cutover. The shadow has not started at this snapshot.

Final synthetic measurements must disclose in-memory provider/repository seams.
They cannot substitute for the real-market observation. Missing comparison
inputs must remain unknown. Production cutover is a separate step after the
user has reviewed the completed report.
