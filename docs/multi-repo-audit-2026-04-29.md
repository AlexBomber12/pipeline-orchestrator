# Multi-Repo State Isolation Audit (2026-04-29)

## Why this exists

Pipeline-orchestrator runs in production today with a single managed repo
(`AlexBomber12__pipeline-orchestrator`). The upcoming megaraid-dashboard
onboarding test will be the first time a second repo is managed
concurrently. This document is the closing record for "is multi-repo
safe" — re-read it before onboarding any new repo.

The audit covers seven concerns from `tasks/PR-193.md`. For each,
the entry below states a status (`PASSED` / `FIXED` / `FOLLOW-UP-NEEDED`)
and the evidence (file:line references) that supports it.

Status legend:
- `PASSED` — code is already isolated; no change needed.
- `FIXED` — a trivial bug was found and fixed inline in PR-193.
- `FOLLOW-UP-NEEDED` — non-trivial work; tracked as a separate task.

## Summary

| # | Concern | Status |
|---|---------|--------|
| 1 | Redis key naming | PASSED |
| 2 | Lock granularity / sequential `run_cycle` | FOLLOW-UP-NEEDED → PR-207 |
| 3 | GraphQL quota distribution | PASSED (intentional global) |
| 4 | Slug collision | PASSED |
| 5 | UI dashboard with multiple repo cards | PASSED |
| 6 | tasks/ directory isolation | PASSED |
| 7 | Event log isolation | PASSED |

No code fixes were needed inline. One non-trivial concern (sequential
runner scheduling in the daemon main loop) is tracked in `tasks/PR-207.md`.

## 1. Redis key naming — PASSED

Every per-repo Redis key includes the repo slug (`self.name`, computed
once per `PipelineRunner` from `repo_slug_from_url`). Global keys are
intentionally shared because they back installation-wide state (single
GitHub PAT, single budget pool).

Per-repo keys (slug-prefixed):
- `pipeline:{self.name}` — pipeline state
  (`src/daemon/runner.py:819`).
- `control:{self.name}:config_dirty` — config-reload flag
  (`src/daemon/runner.py:380`).
- `control:{self.name}:stop` — user stop signal
  (`src/daemon/runner.py:744`).
- `upload:{self.name}:pending` — pending upload manifest
  (`src/daemon/runner.py:1209`, `src/daemon/repo_ops.py:320`).
- `github_rate_limit_burns:{repo_name}` — per-repo cycle burn list
  (`src/daemon/github_rate_limit.py:181`).
- `repo-events:{repo_name}` and `repo-events-history:{repo_name}` —
  event channel and history list (`src/events/publisher.py:16-21`).
- `orchestrator:wake:{repo_name}` — daemon wake channel
  (`src/events/wake.py:17-21`).
- `metrics:repo:{repo_scope}:{task_prefix}` — per-repo metrics index;
  `repo_scope` falls back to the literal string `global` when callers
  omit `repo_name` (`src/metrics.py:87-91`).

Global keys (intentional):
- `github_rate_limit_budget` — installation-wide GitHub quota snapshot
  (`src/daemon/github_rate_limit.py:23`). Correct: the GitHub App has
  one quota pool per installation, so a single key is the right model.
- `github_rate_limit_refresh_lock` — non-blocking probe lock
  (`src/daemon/github_rate_limit.py:29`). Correct: prevents N runners
  from racing to refresh the same shared snapshot.
- `metrics:run:{run_id}` — record by globally unique run UUID
  (`src/metrics.py:85`). Safe by construction.

## 2. Lock granularity / sequential `run_cycle` — FOLLOW-UP-NEEDED

The daemon main loop iterates runners sequentially:

```python
for key, runner in list(runners.items()):
    ...
    await runner.run_cycle()
```

(`src/daemon/main.py:582-604`). There is no shared mutex, but the
`await` is serial, so while runner A's cycle is in progress, runner B
cannot advance. This matters because `run_cycle` for a runner in
CODING awaits the coder subprocess via `await cli_task`
(`src/daemon/handlers/coding.py:122-137`), which can take 30+ minutes
during a PLANNED PR. During that window:

- Runner B's WATCH polling stalls.
- Runner B's MERGE state cannot progress.
- Runner B's IDLE state cannot pick up the next task.
- Wake events targeted at runner B are buffered until the loop returns.

In a single-repo deployment this never bites. In a two-repo deployment
it would be visible the first time both repos try to make progress at
the same time.

Other potentially-shared synchronization points were checked and are
fine in isolation:
- The upload `asyncio.Lock` is per-repo
  (`src/web/app.py:2206,2246-2249`).
- `try_claim_refresh_lock` is non-blocking; runners that lose fall back
  to the cached snapshot (`src/daemon/github_rate_limit.py:138-155`).

The fix (parallelize per-runner cycles via `asyncio.create_task` while
keeping per-runner serialization) is non-trivial because it changes the
scheduling model and must preserve the existing
`asyncio.CancelledError` contract in the CODING handler. Tracked in
`tasks/PR-207.md`.

## 3. GraphQL quota distribution — PASSED (intentional global)

Quota is tracked in a single global Redis snapshot
(`src/daemon/github_rate_limit.py:23`, key
`github_rate_limit_budget`). This is correct because the GitHub App
has one quota pool per installation, so a per-repo budget would be a
fiction. Per-repo *consumption* is tracked separately in the burn list
`github_rate_limit_burns:{repo_name}` for dashboard observability
(`src/daemon/github_rate_limit.py:181`).

When the budget enters the critical zone, all runners are throttled
together via the existing slowdown path. No multi-repo-specific change
is required.

## 4. Slug collision — PASSED

`repo_slug_from_url` (`src/utils.py:6-19`) is the canonical computer
of `owner__repo` slugs. Behavior at the edge cases requested in the
audit:

| Input URL                                  | Slug                  |
|--------------------------------------------|-----------------------|
| `https://github.com/owner/repo.git`        | `owner__repo`         |
| `https://github.com/owner/repo`            | `owner__repo`         |
| `https://github.com/owner/repo/`           | `owner__repo`         |
| `git@github.com:owner/repo.git`            | `owner__repo`         |
| `https://github.com/org/project`           | `org__project`        |
| `https://github.com/org/project-v2`        | `org__project-v2`     |
| `https://github.com/owner-a/api`           | `owner-a__api`        |

Two distinct `owner/repo` pairs cannot produce the same slug. The
function joins owner and repo with a literal `__` separator. GitHub
org names allow only alphanumerics and hyphens (no underscores), so
the prefix before the `__` separator is unambiguous; even if a repo
name contains underscores (e.g. `foo/bar__baz` → `foo__bar__baz`),
no other valid `owner/repo` pair could rearrange to the same slug
because the alternative parse `foo__bar/baz` would require an org
name with underscores, which GitHub forbids. Existing regression tests in `tests/test_utils.py` (lines
12-17, 28-29, 36-37, 40-41) cover the SSH form, the bare-name form,
and the explicit owner-a vs owner-b inequality.

## 5. UI dashboard with multiple repo cards — PASSED

The dashboard repo grid uses Tailwind's responsive grid:

```html
<div class="grid gap-4 md:grid-cols-2">
    {% for repo in repos %} ... {% endfor %}
</div>
```

(`src/web/templates/components/repo_cards.html:48`). The loop is
unconditional, every repo gets an isolated card keyed by `repo.name`
(lines 56-95), and the SSE manager in `src/web/templates/index.html`
opens up to 8 concurrent streams (line 19, `MAX_STREAMS = 8`) iterating
over `[data-repo-card][data-repo]` selectors (line 25). No
single-card assumption is encoded anywhere.

## 6. tasks/ directory isolation — PASSED

Every code path that reads or writes the per-repo `tasks/` directory
joins it onto `self.repo_path` (which is itself slug-derived in
`src/daemon/runner.py:174`). Verified call sites:

- `src/daemon/repo_ops.py:274` — `Path(self.repo_path) / "tasks" /
  "QUEUE.md"`.
- `src/daemon/repo_ops.py:361` — `Path(self.repo_path) / "tasks"`.
- `src/daemon/repo_ops.py:404` — upload destination via
  `_uploaded_repo_path`.
- `src/daemon/repo_ops.py:470` — queue path for canceled-row cleanup.
- `src/daemon/handlers/idle.py:91` — queue parser entry point.
- `src/web/app.py:1431` — web reader uses
  `Path(REPOS_DIR) / name / "tasks" / "QUEUE.md"` where `name`
  is the slug from the URL parameter and `REPOS_DIR = "/data/repos"`
  (line 60).

The helper `_uploaded_repo_path` (`src/daemon/repo_ops.py:34-38`)
returns repo-relative paths only, never absolute, so it cannot escape
`self.repo_path`.

## 7. Event log isolation — PASSED

Events are published to a per-repo channel and stored in a per-repo
history list:

- Channel: `repo-events:{repo_name}`
  (`src/events/publisher.py:16`).
- History list: `repo-events-history:{repo_name}`
  (`src/events/publisher.py:21`).

`publish_repo_event` (`src/events/publisher.py:48-67`) takes
`repo_name` as a required argument and threads it through both
`lpush`/`ltrim` for the history and `publish` for the live stream.
Runner-side callers always pass `self.name`
(`src/daemon/runner.py:588-591`). No path bypasses the slug.

## Follow-ups

- `tasks/PR-207.md` — parallelize per-repo `run_cycle` so a CODING
  subprocess on one repo does not block polling on another.

## Re-validation cadence

This audit is a point-in-time snapshot. Re-run it (and update or
supersede this document) whenever any of the following change:

- A new Redis key is introduced.
- The daemon main loop or runner scheduling model is modified.
- A new per-repo state file is added under `tasks/`.
- A new SSE channel or pub/sub key is introduced.
