# Operations: runtime environment variables

Pipeline-orchestrator reads runtime connection settings such as
`REDIS_URL` and auth-directory paths from the environment. Recovery no
longer has rollout flags: daemon startup always reconstructs queue state
from structured `tasks/PR-*.md` headers.

## Read-only MCP diagnostics

The localhost-scoped Orchestrator MCP exposes `get_orchestrator_status` for a
compact overview of configured repositories. Pass a validated `owner__repo`
slug for structured pipeline, inhibitor, cancellation, pending Retry-command,
and run-record metadata. Results use explicit field allowlists and fixed status
codes: task text, error messages, exception text, arbitrary payload fields, and
other free-form producer content are not returned.

Redis values and indexes are read with fixed size and count bounds. Missing,
malformed, oversized, and unavailable sources are reported independently.
These reads do not refresh TTLs, prune stale index members, or mutate daemon
state. Snapshot age describes only the persisted `RepoState` observation; even
a fresh snapshot is not evidence that a coder process is alive or progressing.

Runtime diagnostics are available on the primary MCP service, whose published
port remains bound to localhost. The same service container runs two
streamable-HTTP listeners: the host port maps to the opted-in diagnostics
listener on container port 5174, while the optional `cloudflared` profile keeps
its existing `mcp:5173` target and reaches a restricted listener started with
`MCP_RUNTIME_DIAGNOSTICS=0`. Existing non-diagnostic MCP tools remain available
through the tunnel, but runtime status is not registered there.

Runtime diagnostics are opt-in at server startup. The MCP service entrypoint
sets `MCP_RUNTIME_DIAGNOSTICS=1` only for its localhost-published listener; an
unset value defaults to disabled for direct and custom deployments. Operators
starting `python -m src.mcp` outside Compose must explicitly set the variable
and retain an equivalent loopback-only or authenticated access boundary.

Raw CLI, CI, event, artifact, daemon-stdout, and live-stream retrieval is not
exposed. Persistent capture and a separately reviewed safe log-export contract
remain follow-up work.

## Task format migration

Task files are migrating from legacy status headers to explicit YAML
frontmatter. The offline operator script is dry-run by default:

```bash
python3 scripts/migrate_task_format.py --repo /data/repos/AlexBomber12__pipeline-orchestrator
```

Review the per-file status output, then apply the migration per managed
repository:

```bash
python3 scripts/migrate_task_format.py --repo /data/repos/AlexBomber12__pipeline-orchestrator --apply
python3 scripts/migrate_task_format.py --repo /data/repos/AlexBomber12__pipeline-orchestrator --verify
```

Repeat the same dry-run, apply, and verify sequence for
`megaraid-dashboard` and `sms-gateway-v2`. Apply mode writes backups
under `artifacts/task-format-backups/<timestamp>/` and prints the exact
backup path. Do not ship the legacy parser removal until `--verify`
passes on every repository.

## Recovery from ERROR

When a task ships its terminal failure state to ERROR (frontmatter
status:ERROR), the operator has two recovery affordances:

1. **Retry button** — clears the cancellation_cause record + frontmatter
   status, daemon re-dispatches the spec from the top on the next IDLE
   cycle. Retry counter capped by `DaemonConfig.retry_button_cap`
   (default 3, configurable via `daemon.retry_button_cap` in `config.yml`)
   in Redis (resets on file content change). Deployments that override
   the cap will enforce that configured value, not the default.

2. **Re-upload spec with changed content** — file content hash differs
   from stored hash → daemon treats as fresh task, cancellation_cause
   cleared, retry counter reset.

Both affordances are mutually exclusive: Retry is for unchanged content
("try again, environment may have transient issue"); re-upload is for
changed content ("operator iterated on the spec itself").

## WorkInhibitor rollback

The WorkInhibitor cutover is complete: `use_unified_inhibitor_check`
defaults to `true`, so repositories use the unified
`src.inhibitor.is_work_inhibited` path unless they opt out.

If a regression affects one repository, keep the daemon-wide default on
and add a per-repo override to that repository's entry in `config.yml`:

```yaml
feature_flags:
  use_unified_inhibitor_check: false
```

Do not put this rollback override in `user_state.yml`; the current
runtime config loader does not read that file. Reload the daemon config
through the normal inotify path, or restart the daemon container. Verify
the rollback by checking the dashboard event log for legacy throttle
decisions on the affected repository.

## Required Check Adoption

`RepoConfig.required_checks` lets a repository declare the exact GitHub
check context names required before merge. Names are exact-match strings;
for example, the tracked pipeline-orchestrator example declares `unit`
and `integration`.

Repositories without `required_checks` keep the existing weaker fallback:
the daemon observes the contexts GitHub reports for the current PR instead
of inventing required contexts from unrelated repositories. Preserved
server configs and production overlays must opt in explicitly during the
final production update; this repository change only updates committed
examples and validation.
