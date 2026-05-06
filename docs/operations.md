# Operations: runtime environment variables

Pipeline-orchestrator reads a small number of env vars at process start
to gate experimental code paths during planned rollouts. All flags
default to off; existing deployments inherit prior behavior automatically.

## Recovery source-switch (PR-266 series)

Two env vars control which queue source `recover_state` uses on daemon
startup. Defaults are conservative: both `0`, behavior byte-identical
to pre-PR-266.

| Variable                          | Default | Meaning                                           |
| --------------------------------- | ------- | ------------------------------------------------- |
| `PIPELINE_RECOVERY_FROM_HEADERS`  | `0`     | When `1`, `recover_state` parses `tasks/PR-*.md`. |
| `PIPELINE_RECOVERY_AUDIT`         | `0`     | When `1`, both paths run; one applies, diffs log. |

The two flags resolve to four operational modes:

| audit | headers | mode                       | applies state | dry-run path | event log signal             |
| ----- | ------- | -------------------------- | ------------- | ------------ | ---------------------------- |
| 0     | 0       | `LEGACY_ONLY`              | legacy        | none         | none                         |
| 1     | 0       | `AUDIT_LEGACY_APPLIES`     | legacy        | headers      | `[AUDIT] recover_state ...`  |
| 0     | 1       | `HEADERS_ONLY`             | headers       | none         | none                         |
| 1     | 1       | `AUDIT_HEADERS_APPLIES`    | headers       | legacy       | `[AUDIT] recover_state ...`  |

The daemon logs the resolved mode at INFO level once per startup. Look
for `recover_state mode: <MODE>` near the top of `docker compose logs daemon`
to confirm the env was read as expected.

### Audit-diff schema

When the applied path and the dry-run path produce different
projections, the daemon emits a structured event:

```json
{
  "audit": "recover_state",
  "mode": "AUDIT_LEGACY_APPLIES",
  "diff": {
    "pipeline_state": {"legacy": "IDLE", "new": "WATCH"},
    "current_task_pr_id": {"legacy": null, "new": "PR-005"},
    "current_pr_number": {"legacy": null, "new": 142},
    "current_queue_length": {"legacy": 8, "new": 8},
    "current_queue_status_drift": [
      {"pr_id": "PR-005", "legacy_status": "TODO", "new_status": "DONE"}
    ]
  }
}
```

Only fields that differ appear in `diff`. Parity is silent so audit logs
stay grep-friendly.

### Production rollout sequence

1. **Audit window.** Set `PIPELINE_RECOVERY_AUDIT=1` (mode
   `AUDIT_LEGACY_APPLIES`) in `docker-compose.yml`, restart the daemon,
   and run for 24-48 hours across all production repos. Every recovery
   (daemon restart + cycle-trigger inside long-running daemon) emits
   either zero `[AUDIT]` events on parity or structured diffs on
   divergence. Operator reviews each diff and confirms the new path is
   the intended outcome (e.g. correctly identifies a merged task that
   the legacy convention scan missed).
2. **Flip to headers.** When the audit window has shown zero
   unexpected divergences, set `PIPELINE_RECOVERY_FROM_HEADERS=1` and
   `PIPELINE_RECOVERY_AUDIT=0` (mode `HEADERS_ONLY`). The legacy
   `_parse_base_queue` path is not executed.
3. **Optional paranoia cycle.** Briefly run `audit=1 headers=1` (mode
   `AUDIT_HEADERS_APPLIES`) for one or two restarts to confirm clean
   operation with the new path applying.
4. **Cleanup.** PR-266c retires the legacy path and removes both flags
   from the runtime once the headers-only mode has shipped and stayed
   green.

The flags are restart-only — `docker compose restart daemon` is
sufficient to flip modes.
