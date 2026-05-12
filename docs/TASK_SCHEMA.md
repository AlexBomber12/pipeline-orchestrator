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

For one release cycle, the parser still accepts the legacy tokens
`queued`, `in_progress`, `in_review`, `merged`, `blocked`, and
`canceled`. These values are deprecated and will be removed by PR-280.

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

All terminal failures route to the single `ERROR` category; the operator
Retry button is the only recovery affordance.

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
