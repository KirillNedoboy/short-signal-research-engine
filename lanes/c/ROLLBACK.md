# Lane C rollback

Rollback is a separate reviewed production action. Keep the previous C
launcher, unit, config, database backup, and pending outbox before cutover.

1. Close the candidate delivery gate and stop the candidate service.
2. Confirm that the old C owner is stopped before starting the previous
   launcher. There must be exactly one delivery owner.
3. Restore the previous immutable source path and runtime config. Do not
   blindly restore a database backup over newer events or outbox rows.
4. Start the previous owner with its original lock and verify heartbeat,
   state reads, outbox ownership, and one short scan window.
5. Preserve the candidate database and logs for investigation. Reconcile any
   candidate outbox rows before reopening delivery so a message is not sent
   twice.

Immediate rollback conditions are identity or parity mismatch, damaged state,
lost/purged outbox, two delivery owners, unbounded backlog, or three
consecutive incomplete scan windows. Deployment commands are intentionally
left to the operator who owns the live service paths.
