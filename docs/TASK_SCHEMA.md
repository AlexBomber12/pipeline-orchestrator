# Task file schema (`tasks/PR-*.md`)

Every PR has a corresponding task file under `tasks/`. The header at the
top of the file is parsed by `src/queue_parser.py:parse_task_header` and
must contain the following fields:

```
# PR-XXX: Short title

Branch: <branch-name>
- Type: <type>
- Complexity: low | medium | high
- Depends on: none | PR-001, PR-002, ...
- Priority: 1-5
- Coder: claude | codex | any
```

## Frontmatter status field

Task files may include YAML frontmatter before the task header:

```yaml
---
status: TODO
---
```

The canonical status values are uppercase by convention:

- `TODO`: default work-queue state. Absence of the frontmatter field is
  treated as TODO.
- `DONE`: terminal success state. The daemon writes this after a PR is
  merged.
- `ERROR`: terminal failure state. The daemon writes this after a final
  failure or escalation that skips the task.

The parser is case-insensitive for backward compatibility, but emitters
must write the canonical uppercase values. Operator-uploaded specs should
include `status: TODO` for explicit intent. The daemon owns terminal
status writes; operators never edit the status field directly.

## Frontmatter blocked_reason field

`blocked_reason` is a daemon-owned companion field for `status: ERROR`:

```yaml
---
status: ERROR
blocked_reason: guardrail
---
```

It carries exactly one canonical `SuppressionReason` enum value, such as
`crash`, `guardrail`, `review_timeout`, or `infra_failure`. Operators do
not set it by hand. The field is the durable coarse failure layer stored
in git so the system remains fail-safe if Redis loses the richer
suppression detail. Rich detail such as excerpts, counters, and
`approved_once` stays in Redis and must not be copied into task
frontmatter.

The daemon writes `status` and `blocked_reason` together when parking a
task in ERROR. ERROR writers must provide the canonical coarse reason
that matches the terminal failure source so an ERROR task never lacks or
misstates its durable reason. When a task returns to `status: TODO`
through Retry or re-upload, the daemon removes `blocked_reason` to avoid
carrying stale suppression state on recovered work.

## Cancellation Availability

The current daemon policy always skip-and-records individual terminal
failures. Operator availability only controls repo-level attention:
AVAILABLE mode auto-pauses when the ERROR-rate threshold is reached, and
AWAY mode never auto-pauses for rate alone. The authoritative policy
notes live in `docs/roadmap.md` under "Sprint 15b Phase 1 finalized
decisions (2026-05-07)" and the historical "Cancellation policy" section.

## Cancellation cause classification

Terminal failures record a `CancellationCause` with `category="ERROR"` and
a `payload.subsource` field carrying forensic detail:

| Subsource | Trigger |
| --- | --- |
| `crash` | Daemon process died mid-operation |
| `coder_escalate` | Coder stdout contained explicit ESCALATE: marker |
| `guardrail` | Tier 1/2 guardrail violation in coder stdout or PR diff |
| `review_timeout` | WATCH cycle exceeded review_timeout_min without new Codex feedback |
| `fix_idle_timeout` | FIX cycle exceeded fix_idle_timeout_sec without push |
| `fix_iteration_cap` | FIX cycle exceeded fix_iteration_cap iterations |
| `no_push_deadlock` | Coder claimed fix without git push |
| `infra_failure` | Repeated INFRA failures past grace period |

Historical records pre-dating the 2026-05-XX migration may carry
`payload.legacy_category` with the original vocabulary value (ESCALATE,
TIMEOUT, INFRA, NO_PUSH_DEADLOCK, CRASH). Dashboard renders these with
a "Legacy" badge for continuity.

All terminal failures route to the single `ERROR` category. The operator
recovers an ERROR task either via the dashboard Retry button (for
unchanged content; retry counter capped by `DaemonConfig.retry_button_cap`,
default 3 and configurable in `config.yml`) or by re-uploading a spec
whose content has changed. See `docs/operations.md` for the full recovery
flow.

## Type field

Canonical values:

- `architecture`
- `bugfix`
- `config`
- `docs`
- `feature`
- `refactor`
- `ux`

### Type field synonyms

The parser also accepts a small number of common synonyms. They are
normalized to the canonical value at parse time but are otherwise
equivalent. Synonyms are accepted but discouraged in new task files;
scripts and AI assistants generating task files should output canonical
values directly.

| Input synonym | Canonical value |
| ------------- | ---------------- |
| `bug`         | `bugfix`         |
| `fix`         | `bugfix`         |
| `chore`       | `refactor`       |
| `feat`        | `feature`        |
| `task`        | `feature`        |

The synonym map is one-way and immutable: once a synonym is shipped, its
key must never be reused for a different canonical value, since doing so
would silently change the meaning of historical task files.

## Validation

Validation runs at upload time (dashboard) and on the daemon's IDLE
selector. Unknown values for any field cause the task to be rejected.
For batch uploads, validation collects errors across all files and
returns one aggregated report instead of stopping at the first failure.

## MCP tool for status-aware spec preparation

LLM clients preparing a spec-upload zip should call the MCP tool
`get_repo_task_status(repo_slug)` before zip assembly. It returns a
`task_id -> status` map (uppercase `TODO` / `DONE` / `ERROR`) for every
`PR-*.md` in the target repo's `tasks/` directory, sourced from each
file's frontmatter. Filter out (or preserve the existing status of)
already-`DONE` specs so a regenerated zip does not regress merged work
back to `status: TODO`. The server-side upload guard from PR-337 is the
backstop; this tool is the source-side prevention layer.
