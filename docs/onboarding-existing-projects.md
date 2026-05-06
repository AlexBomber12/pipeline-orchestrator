# Onboarding existing projects

This guide walks an operator through bringing an existing repository under
pipeline-orchestrator without losing any hand-crafted content already in
its `AGENTS.md`. The flow is dry-run-first: you always see the full
unified diff before anything is written.

## What "daemon-managed sections" means

Pipeline-orchestrator owns a fixed set of sections inside `AGENTS.md`.
Each section is wrapped in HTML comment markers so it stays invisible
when the markdown is rendered, while remaining easy to grep, diff, and
re-emit:

```
<!-- pipeline-orchestrator: managed BEGIN <section_name> -->
...section body...
<!-- pipeline-orchestrator: managed END <section_name> -->
```

The currently managed sections are:

- `quick_rules`
- `work_modes`
- `daemon_mode`
- `ci_gates`
- `codex_review_gate`
- `escalate_protocol`
- `branch_naming`
- `auto_pr_runbook`
- `planned_pr_runbook`
- `micro_pr_runbook`
- `review_fix_runbook`
- `queue_stability_rules`

## Work-mode trigger phrases

Pipeline-orchestrator drives coders with four exact trigger phrases. The
daemon-managed `work_modes` section in every onboarded `AGENTS.md`
documents the same set, so these are the contract a managed repo's
coder is expected to follow:

- `AUTO PR` — daemon-only. The pipeline-orchestrator daemon prepends
  this trigger to the prompt along with explicit `Task: PR-XXX` and
  `File: tasks/PR-XXX.md` headers and the full task body inline. The
  coder works strictly from the inline body and does not consult
  `tasks/QUEUE.md` for task selection.
- `PLANNED PR` — manual VS Code workflow for queue-driven task
  discovery. The coder reads the active entry in `tasks/QUEUE.md` to
  identify the task file, then works from that file.
- `MICRO PR: <one sentence description>` — manual VS Code workflow for
  small ad-hoc changes that do not warrant a `tasks/PR-*.md` file.
- `FIX FEEDBACK` — manual VS Code workflow for applying fixes to an
  existing PR branch in response to CI failures or review feedback.

`AUTO PR` is the daemon's invocation mode; the other three are
operator-invoked from an editor. New repos onboarded after the AUTO PR
rollout receive the same four-trigger model from the very first
scaffold pass.

Anything outside those marker blocks is treated as user-owned and is
preserved byte-for-byte across reconciliation. Anything inside a marker
block is overwritten with the canonical content from this repo's own
`AGENTS.md`, which is the source of truth.

## Step 1: dry-run

Call the preview endpoint with the repo slug (`owner__repo` form, the
same one the dashboard uses):

```bash
curl -s -X POST http://localhost:8000/onboarding/preview \
    -F repo_name=my-org__my-app | jq -r .diff
```

The response is JSON:

```json
{
    "applied": false,
    "diff": "--- a/...AGENTS.md\n+++ b/...AGENTS.md\n@@ ...",
    "proposed_content": "<full file as it would be written>"
}
```

A blank `diff` means the file is already aligned and nothing would
change. Anything else is a unified diff you can review the same way you
review a PR.

## Step 2: apply

Once the diff looks right, call the apply endpoint:

```bash
curl -s -X POST http://localhost:8000/onboarding/apply \
    -F repo_name=my-org__my-app | jq -r .diff
```

`applied` flips to `true` in the response and the file is written. The
returned `diff` is identical to what dry-run produced, so you have a
durable record of exactly what changed.

## How to opt out of a managed section

Delete the entire marker block (including the BEGIN and END comments)
from the target repo's `AGENTS.md`, then re-run reconciliation. The
section will be **re-appended** at the bottom of the file because the
daemon treats a missing managed section as one that needs to be added.

To opt out durably, you have two options:

1. Delete the section from this repo's `AGENTS.md` so it is no longer
   in the canonical template. All onboarded repos lose it on the next
   reconciliation. This is the right move when the section is no longer
   policy.
2. Leave the section in the template and accept that it will be
   re-added every reconciliation cycle. Fork the daemon if you need a
   per-repo opt-out.

Long term, per-repo opt-outs are out of scope: the whole point of
managed sections is that policy stays in lockstep across every onboarded
repo.

## Path-traversal sandbox

The endpoints accept only repo slugs that match `owner__repo` and that
are listed in `config.yml`. The resolved path must remain inside
`/data/repos`, and `/data/repos/<slug>` must already be an existing
git checkout (its `.git` entry is present). A request that fails any
of those checks returns HTTP 422 without touching the filesystem. The
`.git` precondition matters because creating AGENTS.md under a slug
that has not been cloned yet would leave a non-repo directory that the
daemon later tries to `git fetch`, parking the repo in an error state
until an operator removes the directory by hand.

## Malformed managed markers

If the target repo's existing `AGENTS.md` contains malformed managed
markers — an unmatched `BEGIN`, a mismatched `END`, nested regions, or
a duplicate section name — both endpoints return HTTP 422 with a JSON
body of the form:

```json
{ "error": "Malformed managed markers in AGENTS.md: <reason>" }
```

The file is left untouched. Fix the markers in the target file by hand
(typically by deleting the broken block — reconciliation will re-append
the canonical region on the next run) and retry.

## Example walkthrough — `my-app`

Suppose `my-org/my-app` already has an `AGENTS.md` like:

```markdown
# AGENTS

## Mission
Ship the thing.

## Security policy
- Never log PII.
```

After preview, the diff shows the daemon-managed sections being
appended verbatim:

```
--- a/data/repos/my-org__my-app/AGENTS.md
+++ b/data/repos/my-org__my-app/AGENTS.md
@@ -3,2 +3,4 @@
 ## Mission
 Ship the thing.
+<!-- pipeline-orchestrator: managed BEGIN work_modes -->
+...
```

The `Mission` and `Security policy` sections are untouched. After
apply, re-running preview produces an empty diff: reconciliation is
idempotent.
