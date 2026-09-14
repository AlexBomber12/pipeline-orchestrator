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

Use the **Retry button** for an unchanged specification after a retryable failure. The
request is durable; the daemon applies it when existing Pause, Stop, budget,
provider and dependency controls permit. Retry preserves the existing PR and
implementation. Its per-task allowance is controlled by
`daemon.retry_button_cap` (default 3).

**Approve** permits the selected guardrail deviation and continues the existing
PR with its work intact. CI and review gates still apply. Approval remains bound
to the current finding and HEAD; extending it to the entire PR is a separate
follow-up.

**Reject** is final for the current attempt. The confirmation explains that the
daemon will stop attempt-owned execution and close that exact PR without merging.
An HTTP acknowledgement means the request was accepted. The guardrail panel shows
stopping, closure awaiting confirmation, deferred verification, and final
rejection separately. A timeout is not closure confirmation. The same durable
operation is reconciled after restart; retrying closure does not retry coding.
If the PR already merged, its task is completed and cannot be reused.
If its PR is not yet tracked, unresolved PR creation or unknown legacy execution
remain pending for exact PR reconciliation. Final rejection without a PR can
release immediately only for a durable never-dispatched receipt with no recorded
or unresolved PR creation. A dispatched attempt with no PR number and no pending
creation flag uses a bounded confirmation window instead: the first empty PR
discovery records absence, and a later empty discovery at least 60 seconds later
finalizes rejection. Legacy attempts without that evidence remain deferred.

After final rejection, manually rewrite or remove unfinished task specifications
and update their dependencies. An unfinished task may retain its filename, task
ID and branch. Its specification must actually change: changing only `status`
or `blocked_reason`, or re-uploading identical content, cannot bypass rejection.
An identical rejected specification has no ordinary Retry action. Tasks that
were queued but never started can also be rewritten with the same IDs.

Re-upload specs through the existing task upload control, or commit them to the
configured base. An upload acknowledgement means the files are staged. The daemon
applies the same admission checks to staged uploads and Git synchronization before
making a task runnable; dashboard requests do not perform completion verification.
If a staged task was changed or explicitly deleted before consumption, or the
batch is permanently inadmissible, the daemon discards that batch and records the
reason in repository history. Submit corrected files again. Temporary evidence
outages and rejection closure still in progress retain the batch for reconciliation.
Completion is established using Git/GitHub merge evidence and verified completion
records, including implementation through another PR. Checks retain the previous
accepted file identity before replacement; unavailable evidence defers admission.
A ZIP does not delete omitted tasks. Deletion is an explicit Git change, and
missing dependencies remain blockers until the operator fixes them.

Before manually replacing an unfinished task set, **pause repository processing
and confirm process quiescence**. Resolve pending rejection/admission operations
first. A legacy record without exact ownership, an unknown process, uncommitted
checkout files, or an unexpected branch update requires operator reconciliation.
The daemon reports the missing evidence and holds the attempt.

Once admission succeeds, scheduling is automatic, subject to existing Pause,
Stop, dependencies and inhibitors; no additional task Retry click is required.
A reused branch is removed only when its exact old HEAD belongs to the confirmed
closed, unmerged attempt. The new coder creates its branch from the current
configured base and creates a **new PR number**, with its own CI, review and
approval evidence. The rejected PR stays closed. Closing it does not erase its
historical commits. There is no same-PR specification revision or automatic
salvage, dependency rewriting, or sprint replanning.

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
