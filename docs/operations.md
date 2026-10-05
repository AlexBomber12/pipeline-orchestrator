# Operations: runtime environment variables

Pipeline-orchestrator reads runtime connection settings such as
`REDIS_URL` and auth-directory paths from the environment. Recovery no
longer has rollout flags: daemon startup always reconstructs queue state
from structured `tasks/PR-*.md` headers.

## Read-only MCP diagnostics

The localhost-scoped Orchestrator MCP exposes three incident-inspection tools:

- `get_orchestrator_status` returns a compact configured-repository overview;
  pass a validated `owner__repo` slug for queue, inhibitor, Retry-command,
  recent-event, and run-record detail. Snapshot age describes only the last
  persisted `RepoState` write and is never presented as proof that a coder
  process is alive.
- `list_orchestrator_logs` discovers retained CLI snapshots, Redis repository
  event history, disk event partitions, and the current checkout's
  `artifacts/ci.log`. Every source reports retention, timestamps, mutability,
  and whether task/run/SHA identity was actually recorded. Static sources use
  integer offsets; the returned opaque Redis-history cursor preserves bounded
  `SCAN` continuation and fetches no more history values than the page limit.
- `read_orchestrator_log` reads a discovered source with a bounded continuation
  cursor or a tail page. Redis cursors address redacted characters; filesystem
  cursors normally address source bytes. A record longer than `max_chars` uses
  the returned opaque continuation cursor so no unreturned suffix is skipped;
  each filesystem page window remains at most 256 KiB. A filesystem read may
  inspect up to 1 MiB of older context to determine whether its window begins
  inside a multiline private-key block; if that bounded scan cannot establish
  the state, the page is omitted fail-closed. It also inspects at most 1 MiB of
  preceding context so credential keys and plain, quoted, or block scalar values
  split across lines or page boundaries remain redacted, including TOML
  triple-quoted values and shell backslash continuations, with the same
  fail-closed behavior when context is indeterminate. Diagnostic text is
  credential-redacted before it is returned; standalone and level/timestamp-
  prefixed JSON records are redacted structurally, and mixed lines exceeding
  the bounded embedded-JSON candidate limit are omitted. Producer-added `[truncated]`
  markers are preserved; because their removed prefix may contain a sensitive
  opener, the retained Redis CLI tail is omitted fail-closed. Redis event
  history is fetched through a
  read-only bounded script and reported as oversized, without materializing its
  records in the MCP process, when the retained list exceeds 256 KiB. Disk
  partition discovery streams at most 200 directory candidates per request and
  reports when that bound may leave additional partitions undiscovered; an
  exact validated `events:disk/YYYY-MM-DD` source ID remains directly readable.

The web/daemon producers and MCP reader all resolve disk events from
`PO_EVENTS_DIR` (default `/data/events`). Compose maps
`PO_EVENTS_HOST_DIR` (default `./data/events`) to that container path: when
selecting a custom event directory, set both values to the corresponding host
and container locations. The producers' existing `/data` mounts must make the
selected directory writable, while MCP receives only the explicit read-only
event-directory mount. For example, use
`PO_EVENTS_HOST_DIR=./data/audit-events` with
`PO_EVENTS_DIR=/data/audit-events`.

These tools only accept configured repository slugs and fixed source IDs; they
cannot read arbitrary paths or Redis keys. Missing, malformed, expired, stale,
and unavailable data is reported explicitly. Legacy CLI snapshots and mutable
CI artifacts are not attributed to a task, run, or SHA because their producers
do not record that association. Daemon stdout and live CLI streams are not
persistently retained yet, so the tools report both as known logging gaps.

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
