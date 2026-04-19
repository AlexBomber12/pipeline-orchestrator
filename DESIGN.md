# DESIGN

## 1. Overview

Pipeline Orchestrator is a daemon-driven system for turning repository intent into merged pull requests. Today the implemented scope is the Coder stage and its supporting control loop: pick eligible work, run a coder, open or update a PR, watch CI and review state, fix feedback, and merge when the gate is green. The longer-term product shape is a multi-agent pipeline with Planner, Coder, Reviewer, and possibly additional stages, all sharing a common cost and state model.

## 2. North-star metric

The north-star metric is cost per merged PR, summed across every stage that contributed to the merged outcome. Two cost views matter:

- Marginal cost: the incremental cost of choosing an agent for the next run. Within a subscription quota this is effectively zero, so routing decisions should treat in-quota work differently from metered work.
- Amortized cost: monthly subscription price divided by merged PRs in the same month. This is the reporting view because it answers whether the system is actually becoming cheaper or more expensive as usage scales.

Routing should optimize marginal cost subject to success and latency constraints. Reporting should show both marginal and amortized cost so operators can distinguish "free inside the current quota" from "cheap in the aggregate." Pricing details belong in `docs/CODER_PRICING.md`, which does not exist yet.

## 3. Pipeline vision

The target product is a multi-agent pipeline:

- Planner: turns user intent into structured `tasks/PR-*.md` files.
- Coder: implements a selected task, commits, pushes, opens or updates a PR.
- Reviewer: reviews the PR and decides whether the work is ready to merge.
- Merger: performs the final landing path, either by GitHub auto-merge or a local conflict-resolution flow.

Current implementation is narrower. Only the Coder role has concrete in-process agents today, exposed via the `CoderPlugin` protocol and selected by the daemon. Review is externalized to Codex Review on GitHub, and merge remains a mix of GitHub merge actions plus an internal pre-merge sync step. Planner and Reviewer are documented here so future work extends a known architecture instead of inventing one ad hoc.

## 4. Architectural invariants (do not violate)

1. `CoderPlugin` is the stable integration seam for coder implementations. Adding a new coder should not require branching changes in selector logic, runner lifecycle, daemon handlers, or dashboard rendering. Introduced in PR-073 and integrated end-to-end in PR-077.
2. Fallback is a selector policy, not a hand-written chain. The daemon computes eligible coders and ranks them; it should never hard-code "try A, then B, then C" paths per handler. Introduced in PR-091.
3. Exploration is built into selection from day one. The daemon-level `exploration_epsilon` exists so runner-up coders continue collecting outcome data; pure greedy routing would starve alternatives and lock in stale assumptions. Introduced in PR-091.
4. Metrics capture is mandatory for every execution path that spends agent effort. CODING, FIX, and MERGE runs must emit a `RunRecord`, because routing quality cannot improve without observed cost and outcome data. Introduced in PR-080, PR-081, and PR-082.
5. `RunRecord.stage` is a required architectural direction even though the schema extension has not landed yet. The next metrics expansion should default current runs to `"coder"` and preserve bundle-level aggregation by `(task_id, repo_name)` across future Planner and Reviewer records. Decision adopted on 2026-04-19 for planned implementation in PR-093.
6. `CoderPlugin` should generalize into a broader `AgentPlugin` shape when a second in-process role arrives. That future extraction must be additive and must not break existing coder plugins. Direction established in this document on 2026-04-19.
7. Subscription and metered costs are different signals and must stay separate. Routing uses marginal cost, while reporting and operator dashboards should show both marginal and amortized views. Decision formalized on 2026-04-19.
8. Repo state is isolated per repository. Runtime state lives in Redis and must remain namespaced by repo so one repository cannot leak review, rate-limit, or queue status into another. Existing runtime state uses `pipeline:{name}` for the published dashboard snapshot and repo-scoped metrics indexes under `metrics:repo:{repo_scope}:...`.
9. CLI retry policy belongs in shared transient handling, not at individual call sites. When new transient failure patterns are discovered they should be added to the shared retry mechanism rather than patched into one-off command wrappers. Introduced in PR-054.
10. Task files are the source of truth for planned work. `tasks/QUEUE.md` exists for visibility and orchestration, but after PR-088 its canonical content is generated from structured task headers rather than maintained as an independent planning artifact. Introduced in PR-084 through PR-088.
11. Auto-queue selection must respect explicit dependency ordering plus task priority. Eligible work is computed from structured headers, and ordering is priority first, then PR number. Introduced in PR-086 and PR-087.
12. The dashboard is read-only and must not become an agent surface. It renders state from Redis and configuration, and should not make AI calls or own orchestration decisions. This is a repo-level product invariant reflected in the current FastAPI app and deployment model.

## 5. Self-healing invariants

Self-healing is useful only if it stays inside a narrow trust boundary. The existing `diagnose_error` path in `src/daemon/handlers/error.py` is already a self-heal mechanism, so these rules apply to present code and all future automation.

### SH-1. Bounded retries with escalation to human

Every self-heal path must have a hard attempt ceiling and a terminal failure mode. Existing examples include `_error_diagnose_count > 3` and the soft-skip cap when Claude is already rate-limited. If repeated attempts do not restore forward progress, the runner must remain in `ERROR` or transition to another explicit terminal state instead of retrying silently forever. Rationale: unbounded retries waste quota, hide real faults, and make operator intervention arrive too late.

### SH-2. Reversibility

Any automatic mutation must be reversible with normal git or state operations, or must happen on a disposable branch instead of a protected base branch. The current `diagnose_error` auto-fix path commits on the active PR branch and can be reverted with normal git history operations. Queue synchronization uses remediation branches when direct base-branch publication fails. Rationale: if the daemon is allowed to act without per-step approval, it must also leave behind a clean recovery path.

### SH-3. Observability of all self-heal actions

Every self-heal action must emit enough log context for an operator to reconstruct what happened: the failure being addressed, the action taken, any branch or commit involved, and whether the follow-up push or publication succeeded. The existing handlers already log decisions such as skipped diagnosis, branch mismatch, auto-fix push failure, and queue-sync fallback. Rationale: silent healing is operational drift; if a machine can change the system, the operator needs an audit trail.

### SH-4. Self-heal must not silently rewrite analytical truth

Metrics and historical records must not be auto-corrected in ways that erase evidence of daemon intervention. When future self-heals touch analytical data, they need an explicit marker such as `self_heal_applied` or an equivalent event trail so model-quality analysis does not confuse agent performance with daemon cleanup. Rationale: routing trusts metrics, so hidden correction poisons the very signal the selector is supposed to learn from.

### SH-5. Scope whitelist

Self-heal is allowed to touch orchestrator infrastructure, not arbitrary product code. Safe scope includes queue files, orchestrator config, daemon-owned runtime state, and the active PR branch when the daemon is recovering its own failed coding or fix attempt. Unsafe scope includes secrets, unrelated repositories, manual operator data, and broad git history rewriting. `diagnose_error` is acceptable only because it stays on the active work branch, logs the action, and is bounded by the earlier principles. Rationale: the daemon may manage software work, but self-healing must not become permissionless control over the user's whole environment.

Any future self-heal feature should state explicitly how it satisfies SH-1 through SH-5. If it cannot do that, it is not a self-heal feature and should require human approval instead.

## 6. State machine

The persistent daemon state is:

```text
PREFLIGHT -> IDLE -> CODING -> WATCH -> FIX -> WATCH
                              \-> MERGE -> IDLE
WATCH -> HUNG
WATCH -> IDLE          (PR closed or merged externally)
ERROR -> IDLE|ERROR    (after diagnosis or operator action)
PAUSED -> prior state  (after rate-limit window expires)
```

The implemented enum also includes `PREFLIGHT`, which is the startup and degraded-state placeholder used before a healthy Redis-backed repo state is available. `CODING`, `FIX`, and `MERGE` are transient working states resolved within a single cycle; `IDLE`, `WATCH`, `HUNG`, `ERROR`, and `PAUSED` are the long-lived persisted states. On restart, the daemon reconstructs enough context from Redis, git, and GitHub to continue from the last published state rather than replaying the entire history.

## 7. Coder plugin architecture

`src/coder_registry.py` defines the `CoderPlugin` protocol. Each coder supplies:

- `name`
- `display_name`
- `models`
- `run_planned_pr(...)`
- `fix_review(...)`
- `check_auth()`
- `create_usage_provider(...)`
- `rate_limit_patterns()`

Concrete implementations live in `src/coders/claude.py` and `src/coders/codex.py`, and registration happens through `build_coder_registry()`. The runner depends on the registry, not concrete coder modules, which is why new coders should arrive as new plugin modules plus factory registration, not as new branches in handler code.

## 8. Selector and fallback policy

`src/daemon/selector.py` is intentionally small and pure. It uses:

- `eligible_coders(ctx)` to remove coders blocked by rate limits, auth failures, repo-level disablement, or unsupported runtime status.
- `rank_coders(eligible, ctx)` to apply greedy priority ordering or epsilon-driven exploration.
- `select_coder(ctx)` to return the top-ranked eligible plugin or `None`.

The preferred coder still matters: repo pinning overrides daemon default, and a pinned coder suppresses exploration. Everything else is policy data, not branching. That design matters because the next coder should change configuration and priors, not the shape of the daemon.

## 9. Metrics capture and cost

`RunRecord` currently stores the basics required for per-run analysis: run identity, task identity, profile, task metadata, timing, token counts, exit reason, fix iterations, operator intervention, and repo name. Records are stored in Redis through `MetricsStore`, keyed per run with a small repo-scoped recency index.

The architectural direction is broader than the current schema:

- merged-PR analysis should aggregate all records for the same `(task_id, repo_name)` bundle;
- future planner and reviewer records should join the same bundle;
- pricing conversion should eventually use a maintained pricing table rather than ad hoc assumptions.

In other words, the existing schema is the first layer, not the final one. The document treats cost-per-merged-PR as the long-term reporting unit even though the current storage only captures coder-stage runs.

## 10. Auto-queue and task files

Structured task headers are parsed in `src/queue_parser.py`. Dependency reasoning lives in `src/dag.py`, git-derived status inference in `src/task_status.py`, and IDLE-state dispatch in `src/daemon/handlers/idle.py`. The result is a queue system where:

- task files define branch, priority, complexity, and dependencies;
- `QUEUE.md` is generated from the structured task set and current derived statuses;
- the daemon selects the next eligible task by dependency satisfaction, then priority, then PR number.

This keeps planning legible to humans while still making the daemon deterministic.

## 11. Rate limit system

Rate limiting is handled at three layers:

1. Proactive checks via usage providers before invoking a coder.
2. In-flight observation during CLI execution.
3. Reactive detection from CLI stderr and known rate-limit patterns.

The state model supports both legacy whole-daemon limits and coder-specific limits via `rate_limited_coders` and `rate_limited_coder_until`. The selector respects those fields directly, which means rate limiting is not a side effect bolted onto execution; it is part of eligibility. Auto-recovery happens when the recorded window expires and the coder becomes eligible again.

## 12. Dashboard

The dashboard is a FastAPI app with Jinja2 templates and HTMX polling. It is operationally important but intentionally narrow:

- it is read-only with respect to pipeline execution;
- it renders repo snapshots from Redis, falling back to synthetic `PREFLIGHT` or `IDLE` states when Redis is unavailable;
- it exposes configuration editing and task upload flows, but it does not act as an AI runtime.

Per-repo state is published under `pipeline:{name}`, and the dashboard uses that key as the canonical runtime snapshot for UI rendering.

## 13. Deployment

Deployment is defined in `docker-compose.yml` with three services:

- `web`: FastAPI dashboard
- `daemon`: pipeline state machine
- `redis`: runtime state bridge

The project root contains `config.yml`. Repository checkouts live under `/data/repos/{owner}__{repo}`. GitHub auth uses `/data/auth/gh`, Claude auth uses `/data/auth/claude`, and Codex currently rides a mounted Codex home volume at `/data/auth/.codex`. The web service mounts the repo read-write for local editing flows, while the daemon mounts the app code read-only and writes runtime state through `/data` and Redis.

## 14. Future agents (placeholder)

### Planner

Planner should turn free-form user intent into one or more valid `tasks/PR-*.md` files. A useful starting interface is:

```python
class PlannerPlugin(Protocol):
    @property
    def name(self) -> str: ...
    async def plan(
        self,
        repo_path: str,
        intent: str,
        context: dict[str, Any],
    ) -> list[str]: ...
    def check_auth(self) -> dict[str, str]: ...
    def create_usage_provider(self, **kwargs: Any) -> UsageProvider | None: ...
```

The output should be structured task files with populated headers, not just prose. Planner metrics should eventually be stored as the same run bundle with `stage="planner"`.

### Reviewer

Reviewer should accept a PR diff plus recent context and return structured review output rather than free-form commentary alone. A starting interface is:

```python
class ReviewerPlugin(Protocol):
    @property
    def name(self) -> str: ...
    async def review(
        self,
        repo_path: str,
        pr_number: int,
        diff_text: str,
        context: dict[str, Any],
    ) -> ReviewResult: ...
    def check_auth(self) -> dict[str, str]: ...
    def create_usage_provider(self, **kwargs: Any) -> UsageProvider | None: ...
```

`ReviewResult` should at minimum contain a verdict plus a list of actionable comments with enough structure to map back to file and line when possible. Reviewer metrics should eventually use `stage="reviewer"` so review cost and review quality are first-class.

### AgentPlugin direction

When Planner or Reviewer becomes an in-process role, the common pieces shared with `CoderPlugin` should be extracted into a broader `AgentPlugin` base shape. That refactor should preserve backward compatibility for existing coder modules.

## 15. Decision log

- 2026-04-18: `CoderPlugin` protocol introduced and registry-based coder integration completed (PR-073, PR-077). Rationale: adding coders should not require rewriting the runner or handlers.
- 2026-04-18: Run metrics storage introduced (PR-080). Rationale: routing and reporting need durable per-run data, not just logs.
- 2026-04-18: Coding-handler metrics capture added (PR-081). Rationale: the main cost-producing path must emit structured data.
- 2026-04-18: Fix and merge metrics capture added (PR-082). Rationale: partial capture would bias cost and quality analysis.
- 2026-04-18: Auto-queue integration landed in `handle_idle` (PR-087). Rationale: task selection should be derived from structured task files instead of manual queue interpretation.
- 2026-04-19: `QUEUE.md` auto-generation formalized from task headers (PR-088). Rationale: task files remain authoritative while the queue stays visible and synchronized.
- 2026-04-19: Fallback moved to ranked policy with eligibility and exploration (PR-091). Rationale: linear fallback chains do not scale beyond a tiny coder set.
- 2026-04-19: Bandit-style exploration accepted as a permanent selector feature (PR-091). Rationale: pure greedy selection starves alternatives and blocks learning.
- 2026-04-19: Pipeline vision documented as multi-agent, not coder-only. Rationale: the current implementation is coder-centric, but the product direction is end-to-end intent-to-merged-PR automation.
- 2026-04-19: Cost per merged PR adopted as the north-star metric. Rationale: it aligns speed, success rate, and spending better than raw token or latency metrics.
- 2026-04-19: Subscription and metered cost views split conceptually. Rationale: in-quota routing and monthly reporting are different optimization problems.
- 2026-04-19: Future `RunRecord.stage` support accepted before implementation (planned PR-093). Rationale: planner and reviewer records should fit the model without a later conceptual rewrite.
- 2026-04-19: Self-healing invariants formalized in response to the existing `diagnose_error` automation path. Rationale: automated recovery needs explicit limits, observability, and scope control.

## 16. Links to supporting docs

- [`docs/CODER_PRIORS.md`](docs/CODER_PRIORS.md): current per-coder priority priors and benchmark basis.
- `docs/CODER_PRICING.md`: planned future pricing table and amortization notes. Not present yet.
- `docs/MODEL_PROFILING.md`: optional future profiling notes. Not present yet.
- `ARCHITECTURE_HYPOTHESES.md`: optional external critique document if the repo adopts one later. Not present today.

`DESIGN.md` is the canonical current-state architecture and decision document. Supporting docs should provide inputs, measurements, or challenge material, not replace this file.

## 17. How to evolve this document

Append new irreversible architecture decisions to section 15 instead of silently overwriting old rationale. Update section 4 when a new invariant becomes mandatory, and update section 5 whenever a self-healing mechanism is added or its trust boundary changes. If an invariant is intentionally broken, document that change explicitly and explain why the new rule is better than the old one. The cost of a stale architecture document is future rework; the cost of a living document is a few disciplined edits per architectural PR.
