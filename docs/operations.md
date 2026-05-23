# Operations: runtime environment variables

Pipeline-orchestrator reads runtime connection settings such as
`REDIS_URL` and auth-directory paths from the environment. Recovery no
longer has rollout flags: daemon startup always reconstructs queue state
from structured `tasks/PR-*.md` headers.

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
