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

- `work_modes`
- `daemon_mode`
- `ci_gates`
- `codex_review_gate`
- `escalate_protocol`
- `branch_naming`
- `planned_pr_runbook`
- `micro_pr_runbook`
- `review_fix_runbook`
- `queue_stability_rules`

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
`/data/repos`. A request that fails any of those checks returns HTTP
422 without touching the filesystem.

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
