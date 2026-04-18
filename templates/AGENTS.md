# AGENTS

These rules apply to every PR and every task in this repo.

This file has three layered sections:

1. **ORCHESTRATOR CORE** — managed by Pipeline Orchestrator. Contains the
   work-mode trigger phrases, runbooks, queue protocol, Codex Review gate,
   artifact requirements, daemon mode, branch naming, and queue stability
   rules. Manual edits will be preserved but may be overwritten on
   orchestrator upgrades.
2. **PROJECT CONFIG** — customize per repo. Tech stack, repo invariants,
   allowed MCP servers, secrets handling, CI specifics, test requirements.
3. **LESSONS LEARNED** — evolve over time as you discover recurring
   patterns.

## === ORCHESTRATOR CORE (do not edit) ===

This section is managed by Pipeline Orchestrator. Manual edits will be
preserved but may be overwritten on orchestrator upgrades.

Quick rules
- Always choose a work mode using an exact trigger phrase from the Work Modes section.
- PLANNED PRs must follow `tasks/QUEUE.md` and the corresponding `tasks/PR-*.md` file exactly.
- Never commit secrets. Runtime secrets belong in `/data/secrets` (mounted) or injected via env vars.
- Always run the local gate `scripts/ci.sh` until it exits with code 0.
- Always generate review artifacts: `artifacts/ci.log`, `artifacts/pr.patch`, `artifacts/structure.txt`. These are for review only and must not appear in commits (.gitignore handles this).
- Codex Review is "green" only when the Codex bot reacts with thumbs up (`+1`) on the PR body or the review anchor comment (see Codex Review gate). Do not use screenshots.
- Do not drift from the plan and do not add "nice-to-have" work.

### Work Modes
Exact trigger phrases:
- `PLANNED PR`
- `MICRO PR: <one sentence description>`
- `FIX REVIEW`

Meaning:
- `PLANNED PR`: the default mode. Work strictly from `tasks/QUEUE.md`.
- `MICRO PR: ...`: a tiny change. Do not touch `tasks/QUEUE.md` and do not create `tasks/PR-*.md`.
- `FIX REVIEW`: fix feedback on an existing PR branch.

### Daemon Mode

When triggered by the pipeline orchestrator daemon (non-interactive):
- Do not ask for confirmation or clarification.
- If something is unclear, commit what you have and note the ambiguity in the PR description.
- Log all decisions to stdout for the daemon to capture.
- NEVER use `git add -f` or explicitly stage files matched by .gitignore.
- Artifacts (ci.log, pr.patch, structure.txt) are generated for Codex review but must not appear in commits. The .gitignore already excludes them.
- NEVER commit .patch files to the repository under any circumstances.

### Codex Review gate (GitHub PR)

This repo treats Codex Review as a single pass/fail signal.

Pass signal
- thumbs up (`+1`) reaction from the Codex bot on either:
  - the PR body (description), OR
  - the review anchor comment
- The daemon compares the reaction timestamp against the latest push timestamp. A +1 that predates the most recent push is stale and does not count as a pass.

In-progress signal
- eyes (`eyes`) reaction from the Codex bot.

Feedback
- If there is no valid (non-stale) thumbs up, assume the PR is not approved yet.
- Codex may post a comment with findings (often labeled `P1` or `P2`). Fix all `P1` and `P2` by default to obtain thumbs up.

Definitions
- Codex bot: any GitHub user or app whose login contains `codex` (case-insensitive), for example `chatgpt-codex-conn`.
- Review anchor comment: the most recent PR conversation comment authored by the PR author that matches at least one:
  - contains `@codex review`
  If none exist, use the first PR author comment in the conversation.

In `PLANNED PR` and `MICRO PR` flows, the coder does not post
`@codex review`. Generate artifacts, commit, push, and create the PR; the
daemon posts a clean `@codex review` after PR creation and after every
fix push. Codex Automatic Reviews should be configured for PR creation
only (not every push) to avoid duplicate reviews.

Fix loop (used in `FIX REVIEW` mode)
1. Fetch PR comments, reviews, and reactions via GitHub CLI (`gh`). No screenshots.
2. Check for Codex thumbs up on both the PR body and the review anchor comment.
3. If a non-stale thumbs up exists (reaction created after the latest push), stop. The PR is green.
4. Otherwise, collect the latest Codex feedback after the most recent push:
   - review comments
   - top-level PR comments
5. If no new feedback exists after the latest push, stop. Do not fix already-resolved issues.
6. Extract actionable items and fix them. Treat `P1` as mandatory; treat `P2` as mandatory unless the user explicitly waives them.
7. Run `scripts/ci.sh` until exit code 0 and generate required review artifacts.
8. Commit and push to the same PR branch.
9. Poll the PR for up to 15 minutes:
   - if a non-stale thumbs up appears, stop
   - if a new Codex feedback comment appears after the push, repeat the loop

### Required review artifacts
Single entrypoint: `scripts/make-review-artifacts.sh`.

If the script does not exist yet, manual fallback:
- Save CI output to `artifacts/ci.log`
- Save a patch to `artifacts/pr.patch` (diff from `origin/main` to HEAD)
- Save project structure to `artifacts/structure.txt` (`find . -type f | grep -v __pycache__ | grep -v .git/ | grep -v node_modules | sort`)

Artifacts are generated for Codex review but excluded from commits by .gitignore. Do not use `git add -f` to override this.

### Branch naming
- PLANNED: use `Branch:` from the active `tasks/PR-*.md` as the source of truth.
- If `Branch:` is missing, use `pr-<sanitized-pr-id>`:
  - lowercase
  - replace `.` with `-`
  - allow only `[a-z0-9-]`
- MICRO: `micro-YYYYMMDD-<short-slug>`

### PLANNED PR runbook (queue-driven)

#### Rules
- Preflight: `git status --porcelain` must be empty. If not, stop and list dirty files.
- Task selection:
  - if any item is `DOING`, take the earliest `DOING`
  - otherwise take the earliest `TODO` whose dependencies are all `DONE`
  - `PR_ID` must match `tasks/QUEUE.md` exactly
  - `TASK_FILE` must come from the `Tasks file:` line, do not guess
- Read `TASK_FILE` fully before coding.
- Create the branch from `origin/main`.
- Implement only the scope defined in `TASK_FILE`. No extra refactors, upgrades, or bundled features.
- During the PR, do not edit `tasks/PR-*.md` unless the user explicitly requests it.
- CI and artifacts are mandatory.
- Queue update rules:
  - when starting: set the current PR `- Status: DOING`
  - when CI is green and before push: set `- Status: DONE`
  - only change the `- Status:` line for the current PR

#### Checklist
- [ ] Preflight clean
- [ ] Selected PR from `tasks/QUEUE.md`; recorded `PR_ID` and `TASK_FILE`
- [ ] Read `TASK_FILE`
- [ ] `git fetch origin main` and created branch from `origin/main`
- [ ] Implemented only `TASK_FILE` scope
- [ ] Ran `scripts/ci.sh` to exit 0
- [ ] Generated `artifacts/ci.log`, `artifacts/pr.patch`, `artifacts/structure.txt` (not committed, excluded by .gitignore)
- [ ] Updated `tasks/QUEUE.md` for current PR: `DOING` -> `DONE`
- [ ] Commit message: `<PR_ID>: <short summary>`
- [ ] Pushed branch
- [ ] Created PR via GitHub CLI (`gh`) or provided manual PR steps
- [ ] Final report prepared (see below)

#### Final report (PR description or final message)
- PR_ID
- TASK_FILE
- Branch
- What changed (1-5 bullets)
- How verified (exact command)
- Artifacts: `artifacts/ci.log`, `artifacts/pr.patch`, `artifacts/structure.txt`
- Manual test steps (if applicable)

### MICRO PR runbook

#### Eligibility (all must be true)
- <= 3 files changed
- <= 100 lines changed (excluding lockfile noise)
- no DB migrations/schema changes
- no dependency upgrades
- no auth/permissions changes
- no large refactors or sweeping formatting

If any condition fails, MICRO is not allowed. Use PLANNED PR.

#### Rules
- Do not create `tasks/PR-*.md`
- Do not edit `tasks/QUEUE.md`

#### Checklist
- [ ] Preflight clean
- [ ] `git fetch origin main`
- [ ] Branch `micro-YYYYMMDD-<short-slug>` from `origin/main`
- [ ] Only the requested change
- [ ] Ran `scripts/ci.sh` to exit 0
- [ ] Generated review artifacts (not committed, excluded by .gitignore)
- [ ] Commit: `MICRO: <short summary>`
- [ ] Pushed branch and opened PR

### REVIEW FIX runbook (existing PR branch)
- Do not select a new task from `tasks/QUEUE.md`
- Do not create a new branch
- Stay on the existing PR branch
- Do not edit `tasks/QUEUE.md` or `tasks/PR-*.md`
- Fix only the review comments
- Use the Codex Review gate above and stop only when a non-stale Codex thumbs up is present
- Run `scripts/ci.sh` to exit 0
- Generate review artifacts (not committed, excluded by .gitignore)
- Commit and push to the same PR branch

### Queue stability rules (PLANNED PR only)
- `tasks/` is the source of truth.
- Do not rewrite tasks retroactively during a PR.
- If the user updates `tasks/` while you are working, stop and ask for explicit direction: continue as-is, incorporate changes, or revert.

## === PROJECT CONFIG (customize per repo) ===

Fill in the placeholders below for this specific project. The orchestrator
does not manage this section — edit it freely.

### Tech stack
- Language: Python 3.12  # TODO: update for your project
- Framework: # TODO
- Database: # TODO

### Repo invariants
# TODO: define your project's non-negotiable contracts (e.g. "/data is
# the single runtime state root", "Dashboard is read-only", service
# boundaries, etc.)

### MCP servers
Allowed MCP servers (by name):
- github (optional)
# TODO: add project-specific MCP servers

Rules
- Use GitHub CLI (`gh`) or GitHub MCP to read PR conversation, reviews, and reactions. This is the source of truth for the Codex Review gate.
- If an MCP server is not listed above, do not use it.

### Test requirements
- Tests required for every PR: true  # set to false for prototypes
- Minimum test coverage: none  # or specify percentage

### CI specifics
- Single entrypoint: `scripts/ci.sh`
# TODO: describe what your CI checks (lint, type, unit, integration)

If `scripts/ci.sh` does not exist yet, use the fallback:
- `python -m ruff check .`
- `python -m pytest -q` (if tests exist)

Do not claim "green" unless the exit code is 0.

## === LESSONS LEARNED (evolve over time) ===

Add rules here as you discover recurring patterns. Examples:
- "Always add type hints to new Python code"
- "Wrap async network calls in `try/except` and log the exception"
- "Codex frequently flags missing docstrings on public functions — add them proactively"

<!-- TODO: populate as the project matures -->
