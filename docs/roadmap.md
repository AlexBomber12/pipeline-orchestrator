# Pipeline Orchestrator Roadmap

Живой документ. Обновляется после каждой merge'нутой волны и после каждой chat-session.

Последнее обновление: 2026-04-29 (full roadmap rewrite на основе Implementation Audit. Прежние Sprint F1-F4 нумерации reconciled с реальным состоянием кода. Numbering для нового бэклога продолжает существующую sequence от PR-180).

Предыдущие: 2026-04-28 (sigkill recovery test multi-race resolved via PR-228/PR-232/PR-234/PR-236; production daemon deployed on fresh main; GraphQL quota burn analyzed; onboarding test subjects identified), 2026-04-27 (OBS-AA test pollution v1 misdiagnosis + v2 docker-exec fix; OBS-Y premature merge; Multi-tier agent direction; OBS-Z Codex EYES race), 2026-04-26 (Sprint F1.0 + PR-156/157 + PR-158/159 merged; Variant D direction; Development model & Layer 2 substrate observations), 2026-04-24 (after code audit zip __27__).

---

## Текущий статус

- **Production deployed 2026-04-29** with 130+ commits since 2026-04-24. Daemon stable, sigkill races resolved, all UI polish from PR-176/177/178/179 nightly run merged.
- **Implementation Audit completed 2026-04-29.** Full fact-check of every Sprint F1-F4 deliverable against actual `src/` code. Audit document preserved at `docs/audit-2026-04-29.md` (см. также секцию "Implementation Audit summary" ниже).
- **Реальный backlog: 20-22 PR ближайшие 2 недели** (после audit) plus sprint-scale work на месяц.
- **GraphQL quota observed as binding constraint** (OBS-AC). Diet plan documented; leverages 2-4 + GitHub App migration не shipped — это первый critical batch.

---

## Implementation Audit summary (2026-04-29)

Detailed PR-by-PR audit см. `docs/audit-2026-04-29.md`. Сводка по статусам:

### Sprint F1.0 — Playwright e2e infrastructure (DONE)
PR-153, PR-154, PR-155 все merged. tests/e2e/ существует с 12 файлами, scripts/test-e2e.sh + docker-compose.test.yml на месте.

### Sprint F1.1 — Coder pin (DONE)
PR-156 (FINDING-2 fix) shipped. `task_coder_pin` routes pinned-but-unavailable to empty list → HUNG. OBS-16 ENV-TOKEN verification остаётся как manual operational task.

### Sprint F1.2 — QUEUE.md presentation layer (PARTIAL)
- `_generate_queue_md` в `idle.py` работает (in-memory generation done)
- **NOT DONE:** QUEUE.md ещё в git (не в .gitignore). Closes OBS-2 drift только частично.
- **NOT DONE:** PR-158 diagnose_error bypass для infra errors (OBS-4 still open).

### Sprint F1.3 — Reliability correctness (PARTIAL)
- **DONE:** PR-160 — `BoundedRecoveryPolicy[T]` framework в `recovery_policy.py`, used in `fix.py`.
- **NOT DONE:** PR-159 asymmetric push verification.

### Sprint F1.4 — UX immediate batch (DONE)
PR-161 (remove STALLED), PR-162 (spinners), PR-163 (event dedup display), PR-164 (fuzzy dedup в log_event), PR-165 (light-theme dropdown), PR-166 (HTMX 400 whitelist) — все 6 shipped. Plus PR-176/177/178/179 nightly run added Tasks viewer + HTMX 400 (duplicate of PR-166) + Pulse fixes.

### Sprint F2.1 — Sprint 10 SoT direct instructions (NOT STARTED)
PR-167/168/169 не начаты. AGENTS.md "Use the active entry" sections всё ещё актуальны.

### Sprint F2.2 — PAUSED state model removal (NOT STARTED)
PR-170/171/172/173 не начаты. PAUSED enum value, 15 usages в handlers, awaiting_start field — ничего не сделано. Самый высокого риска refactor.

### Sprint F3.1 — Settings comprehensive (PARTIAL)
- **DONE:** PR-175 review_timeout_min UI.
- **PARTIAL:** PR-174 dropdown существует но "Auto-Select" label + remove-Inherit не сделано.
- **NOT DONE:** PR-176, PR-177 AGENTS.md commits.

### Sprint F3.2 — Selector + measurement (Thompson Sampling) (NOT STARTED)
PR-178/179/180/181 не начаты. epsilon-greedy всё ещё default.

### Sprint F3.3 — UI polish + tasks viewer (PARTIAL)
- **DONE:** Pulse animation fix, pulse badge, task content viewer (через ad-hoc PR-176/178/179 нумерацию в task files).
- **NOT DONE:** PR-184 event text clarity, PR-185 immediate upload pickup via Redis pub/sub.

### Sprint F4.1 — Failure modes comprehensive (NOT STARTED)
PR-187/188/189/190/191 — все 5 не начаты. Sprint целиком deferred.

### Sprint F4.2 — Testing infra + cleanup (PARTIAL)
- **DONE:** PR-193 upload locks (`_upload_locks` dict в `app.py`).
- **NOT DONE:** PR-192 nightly e2e schedule, PR-194 STALLED documentation, PR-195 MERGE dead value cleanup (PR-195 → renumbered PR-198 in current backlog; verification 2026-04-30 rejected the cleanup, see Polish batch PR-198 below).

### Sigkill multi-race fixes (2026-04-28 session)
PR-228 (Coder ESCALATE), PR-232 (test fixture isolation /stop), PR-234 (REST quota), PR-236 (shim explicit lease + entrypoint mutex) все merged. Test_sigkill_recovery deterministically green в CI.

### Active OBS items
- OBS-2 (QUEUE.md regen drift): **PARTIAL FIX** — in-memory работает, remove-from-git нет.
- OBS-4 (diagnose_error infra bypass): **OPEN**.
- OBS-5 (gh credential helper exit 128): **OPEN** — не было instrumentation работ.
- OBS-Y (daemon merges before APPROVED): **status unclear** — нужна отдельная проверка.
- OBS-Z (Codex EYES race window): **OPEN** — pre-push state check (PR-181 candidate) не реализован.
- OBS-AA (test pollution Redis state survival): **PRESUMABLY DONE** через PR-230 task-fixture-redis-cleanup.
- OBS-AB (sigkill multi-race): **DONE** (см. выше).
- OBS-AC (GraphQL quota burn): **OPEN** — diet plan documented в section ниже, leverages 2-4 + GitHub App не shipped.

### Memory items still actionable
- push_count desync — UI metric `len(commits)` или local +=1, не reconciled с GitHub Commits tab.
- AGENTS.md prohibit draft PRs — handler-side `gh pr ready` есть в `merge.py`, но AGENTS.md text не обновлён.

### Production lessons (from 2026-04-28 session, recorded for future reference)
- **Production config.yml gap:** ~15 daemon overrides существуют только как local file on production host, never committed. `git reset --hard` revert'ит их к upstream defaults. Production behavior не reproducible from git alone. Action: либо commit `config.production.yml`, либо move to env vars, либо deploy step с config diff verification.
- **Deploy checkout vs daemon `/data/repos/.../tasks/` distinction:** daemon работает с собственным clone в docker volume. Deploy-time `~/pipeline-orchestrator/tasks/` может содержать другой набор файлов. Don't conflate the two when investigating queue discrepancies.
- **N>=3 verification reruns rule:** для race condition fixes один зелёный CI run не валидация. Тест мог проходить на lucky timing до фикса. Require 3+ green reruns на same commit перед merge.
- **Single-step on stateful operations:** rebase, merge, deploy не должны быть в `&&` chains. Each command output must be reviewed перед next.
- **Read file before writing patch:** during long debug sessions, мой cached snapshot drift'ит от user actual state. Always re-read user current file перед generation patches.

---

## Архитектурные решения (принятые ранее, актуальные)

### State model

- **PAUSED state будет удалён.** Заменяется на 2 orthogonal flag на RepoState: `awaiting_start: bool` и `rate_limited_until: datetime | None`.
- 7 work states: IDLE, CODING, WATCH, FIX, MERGE, ERROR, HUNG.
- Stop click = kill subprocess + awaiting_start=True + state=IDLE (dirty tree warning).
- Pause click = awaiting_start=True, current PR цикл завершается естественно, потом state=IDLE.
- Play click = awaiting_start=False, daemon picks next из IDLE.
- Rate limit = rate_limited_until overlay, state не меняется, UI показывает LIMIT chip поверх основного state chip.
- Разное IDLE различается по `awaiting_start`: True = стоп, ждём Play; False = нормальное ожидание следующей task.

### Coder selection UX

- Daemon default coder: [Auto-Select / Claude / Codex] в Settings.
- Per-repo coder override в Settings table: [Auto-Select / Claude / Codex].
- Repo detail page — read-only display "Coder: Codex CLI", клик ведёт в Settings.
- "Auto-Select" label везде вместо "Any (bandit picks per-PR)".
- Убрать Inherit option.
- Hot reload + confirmation checkmark после save.

### Event log display

- Dedup repeated events: single timestamp (last_seen), Lucide `rotate-ccw` icon + count справа в row.
- Tooltip с first_seen через native `title` attribute.
- Без `(xN)` inline в message.
- Fuzzy dedup на уровне log_event для counter-like patterns (`\b\d+/\d+m\b`).

### STALLED indicator

- Удалить целиком. Причина: прыгающие кнопки при DOM swap, false positives, отсутствие ценности.

### AGENTS.md scope

- **Downgraded to Tier 2.** Изначально был Wave 1 Tier 1, но Wave 2 прошёл без этих fixes. Оставляем как nice-to-have в Wave 6.

### Product positioning (clarified 2026-04-21)

Pipeline-orchestrator это **measurement + routing layer для agentic coding tools**, не для LLM.

Key insight: агенты (Claude Code, Codex CLI, Aider, Cline, goose, OpenHands) это harness'ы над LLM с разным поведением — exploration aggressiveness, tool use quality, context management strategy, long-task stamina. Разница в поведении между Claude Code и Codex CLI на одинаковой сложности PR больше чем разница между моделями под ними.

**Core value:**
- User не успевает следить за agent+model landscape (новые releases еженедельно)
- Pipeline-orchestrator автоматически measures (cost, speed, merge success, review iterations) per agent+model+task-type combination
- Thompson Sampling / bandit selector выбирает optimal option per конкретную задачу
- Новые коммерческие agents добавляются через CLI plugin
- Новые open models используются через local agent wrappers (Aider / goose / OpenHands) без изменения core

**Moat thesis:** measurement data per (agent, model, repo, task type) combination накапливается. Новому игроку придётся выполнить много прогонов прежде чем воспроизвести routing quality. Чем больше deployments, тем сильнее moat. Это совпадает с north star "cost per merged PR" — routing quality = ниже cost.

**Positioning evolution:** раньше формулировал как "cross-vendor routing for Claude + Codex". Теперь точнее — "agentic coding routing с поддержкой local inference". Добавляет privacy/offline/cost reduction dimensions.

### Phase-resource separation (observed 2026-04-29)

Empirical observation from production daemon: GitHub API quota и Claude API quota burn в **non-overlapping phases**.

```
CODING/FIX:  Claude API up    GitHub API flat   (coder subprocess eating Claude tokens, daemon only watches stdout)
WATCH:       Claude API flat  GitHub API up     (daemon polls CI status, review state, comments; no Claude calls)
IDLE:        Claude API flat  GitHub API low    (baseline polling without active PR)
MERGE:       Claude API flat  GitHub API short  (short burst for merge checks)
```

This is **clean separation of resources by phase**. Implications:

- **Adaptive polling cannot use one global multiplier.** Each phase has different polling cost characteristics; tuning one without considering the other is wrong layer.
- **WATCH is the dominant GitHub burn**, not IDLE. Optimization priority should reflect this.
- **CODING/FIX is dominant Claude burn**, but Claude quota is per-account и harder to control architecturally (can only choose model, not skip calls).
- **OBS-AC GraphQL diet leverages**: PR-180 (REST replacement) targets the WATCH-dominant burn; PR-184 (IDLE adaptive) targets baseline. Both are valid because they hit different phases.
- **Future PR-202 (WATCH adaptive)** specifically targets the dominant burn phase, with phase-aware polling logic that differs from generic exponential backoff (slow-start instead of fast-start; rationale в task spec).

This model also informs **multi-repo capacity planning**: two repos in CODING/FIX simultaneously do not double GitHub burn (Claude doubles instead). Two repos in WATCH simultaneously DO double GitHub burn — this is the multi-repo risk OBS-AC anticipated.

### Outcome data version-drift (decided 2026-04-29)

Architectural decision recorded for analytics and future self-learning capabilities.

**Lessons learned from past PRs are valid only for the same coder × model × version combination.**

Cross-version aggregation is **unsafe** because:

- Each model version has different error distribution (Claude Sonnet 4.6 → 4.7 → 5 produces different bugs)
- Newer training data shifts the population of issues
- Version-specific fixes to upstream coders eliminate certain classes of failures
- Tool-version drift (CLI extensions like `@anthropic-ai/claude-code` and `@openai/codex`) changes interaction patterns

Therefore:

1. **Outcome logs (PR-204) record coder/model/version explicitly** as required schema fields.
2. **Analytics queries default-filter to current version.** Mixed-version queries are explicit opt-in with caveat warning.
3. **Lessons learned recommendations** must be scoped by version triple. A pattern observed under `claude-opus-4-7` does not auto-apply to `claude-opus-5`.
4. **Selector training (Thompson Sampling, etc)** must reset or heavily-discount data when underlying coder version changes. Stale posterior distributions on outdated versions are worse than no posterior at all.

**Practical implication**: dataset accumulation has natural decay. After a major version change, much of the historical dataset becomes advisory rather than authoritative. This is a fundamental limitation of ML-based recommendations for AI coding tools, not a bug to fix.

**Storage decision (related)**: PR-204 uses JSONL append-only files in `/data/analytics/<year>-<month>.jsonl`. SQLite migration deferred until any of: (a) cross-month queries become slow (>10s), (b) need for indexed columns on million-row tables, (c) multi-process concurrent writes that flock cannot handle. None apply at current scale (~250 PR/year, single daemon process).


### Defense-in-depth confirms architectural approach (observed 2026-04-29 evening)

A real instance validated that bounded-everything design choices are paying off, even when the immediate logic underneath has a bug.

**The case:** PR-181 (Remove tasks/QUEUE.md from git tracking) merged earlier in the day. `_mark_queue_done` had been written before PR-181 and assumed `git add tasks/QUEUE.md` would always succeed. After PR-181 made QUEUE.md gitignored, the `git add` started returning exit 1 silently inside an exception path, leaving `pending_queue_sync_branch` set to a non-existent branch (`queue-done-pr-N`). Every subsequent IDLE cycle polled this fictional PR via `gh pr view`. Result: livelock.

**The defense that worked:** `_escalate_queue_sync_if_expired` with `_QUEUE_SYNC_MAX_WAIT_SEC = 3600`. After 1 hour of polling, daemon escalated to ERROR with explicit message `queue-sync PR ... unresolved after 3647s (max 3600s)`, cleared the stuck field, and recovered. No human intervention needed for unblocking. Operator (and assistant) noticed via dashboard, but only because of the ERROR badge — daemon would have eventually recovered alone.

**Why this matters:** nobody anticipated PR-181 × `_mark_queue_done` interaction. Defense-in-depth covered the unknown failure mode anyway. This is the value of bounded-everything design.

**Principles that fired here:**

1. **Bounded everything.** Every wait has a timeout. Every retry has a cap. Every poll has an escape hatch. Same principle as `BoundedRecoveryPolicy` framework from PR-160 — different surface, same idea.

2. **State machine with explicit ERROR.** Daemon has no "stuck forever" state. Any impasse → ERROR with `error_message`. Operator can see and intervene; code can self-recover from ERROR to IDLE on next tick.

3. **Self-recovery without operator.** Restart, timeout, escalate — three mechanisms that close unknown failure modes. PR-181/queue-sync interaction was unknown ahead of time; recovery worked anyway, costing only 1 hour latency and ~60 wasted GraphQL calls.

4. **Honest logging.** `queue-sync PR queue-done-pr-182 view failed: gh pr view ... no pull requests found for branch "queue-done-pr-182"` — exact cause in log, not generic "something went wrong". Operator reconstructed the full sequence through `grep` retroactively.

**Implication for future design decisions:** when adding any new wait/retry/poll loop, the timeout/cap/escape-hatch must be designed in from day one, not added later. Adding bounds retroactively to a long-running loop usually requires understanding all paths into and out of it — easier to bake in upfront.

**This is industrial control system orthodoxy** ported to autonomous software. Operator's industrial B2B background (chiller/HVAC equipment) directly informed the daemon's defense-in-depth posture. AI coding tools as a category are still rediscovering principles that industrial automation solidified decades ago. This is the same calibration-as-engineering thesis recorded in 2026-04-17 strategy session ("Industrial engineering solved good-enough 30 years ago. AI coding is repeating the mistake."), surfaced here as direct evidence in production code.


### Testing policy для managed repos (added 2026-04-24 Day 5)

Любой repo onboarded в pipeline-orchestrator должен иметь test pyramid:

- **Fast tier** (`scripts/ci.sh`, on every PR by coder): unit tests + linters + type checks. Target duration <5 минут.
- **Integration tier** (`scripts/ci.sh`, on every PR by coder): e2e tests spinning up target application via docker compose (separate stack от orchestrator), running Playwright/API suite, teardown. Target duration <15 минут.
- **Coder policy:** PR не opens пока `scripts/ci.sh` не exits 0. Покрывает оба tier'а. Coder retries up to `fix_iteration_count`.

**Pipeline-orchestrator repo — special case.** Self-testing через `scripts/ci.sh` self-destructive (daemon under test = daemon invoking coder). Для этого repo только e2e suite runs:
- локально через `tests-manual/auto/dayN/run.sh` (manual trigger)
- nightly cron на homelab (future PR-193)

Это policy document, не code. Living в `docs/ci-template.md` (TODO create), referenced из CLAUDE.md / AGENTS.md чтобы coder знал expected standard для нового managed repo.

---

---

---

## Implementation Plan (post-audit, 2026-04-29)

**Принцип:** finish before extend. **Pre-multi-repo readiness FIRST**, потом большие refactor (Sprint 10 SoT, PAUSED removal, Thompson Sampling).

Numbering продолжает существующую sequence от PR-180 (последний DONE в `tasks/` = PR-179). Старый legacy Implementation Plan numbering ("PR-167-PR-195" из original plan) **больше не используется** — task files = source of truth, не roadmap-side reservation.

### Critical batch — Pre-multi-repo readiness (PR-180..PR-185, ~5-7 days)

**Цель:** убрать GraphQL quota constraints + production stability gaps before подключать второй репозиторий.

- **PR-180** REST replacement for `gh pr list --json statusCheckRollup` (OBS-AC Leverage 2). ~50 LOC. Switches WATCH/MERGE polling to REST `check-runs` + `status` endpoints (REST core quota 5000/hr is dramatically underused).
- **PR-181** Finish PR-157 — remove tasks/QUEUE.md from git (add to .gitignore + cleanup commit). Closes OBS-2 drift fully.
- **PR-182** PR-158 diagnose_error bypass для infra/git/network errors. Routes git-class failures bypass diagnose loop, surfaces directly to operator alert. Closes OBS-4.
- **PR-183** Redis pub/sub upload-trigger (was Sprint F3.3 PR-185). Web publishes `repo_upload_completed:{name}` после upload commit. Daemon main loop wakes via `asyncio.wait` on combined sleep+subscriber. Earliest wins. Scope expand на: Play button, coder swap, settings change.
- **PR-184** Adaptive IDLE polling (depends PR-183). После 3 consecutive IDLE без work — slow до 300s. Wake immediately on pub/sub event OR state transition. Trade-off acceptable: daemon overnight worker, не interactive.
- **PR-185** Daemon-side GraphQL points consumed per cycle observability. Surfaces в dashboard как companion metric к existing GitHub API budget. Helps verify diet effectiveness over time.

**Exit criteria:** GraphQL burn measurably reduced (target: <500 points/hour per active repo at IDLE), responsiveness preserved (upload -> daemon wake <2s).

### Important batch — Stability fixes (PR-186..PR-191, ~5-7 days)

- **PR-186** Recovery skip crashed-task-retry (Sprint F4.1 PR-187 equivalent). Если task fails в CODING/PREFLIGHT, recovery должен skip и pick next без retry на той же задаче.
- **PR-187** Coder exit=0 diagnostic handler (Sprint F4.1 PR-188 equivalent). Discriminate: branch missing vs exists no PR vs no branch. Route appropriately, не treat все как HUNG.
- **PR-188** Codex bot error comment detection (Sprint F4.1 PR-189). Watch handler polls comments от `chatgpt-codex-connector[bot]`, on `Something went wrong` -> immediate @codex review re-trigger.
- **PR-189** OBS-Z fix: Codex EYES race window. Pre-push state check (если Codex already reacted EYES on PR body — skip duplicate `@codex review`). Plus EYES-specific stale threshold (`stale_review_threshold_eyes_min: 5`) — separate от general 10-min threshold.
- **PR-190** Asymmetric push verification в fix.py normal path (Sprint F1.3 PR-159).
- **PR-191** ETag conditional requests across `github_client.py` (OBS-AC Leverage 3). `If-None-Match` returns 304 for unchanged, не counted against rate limit. Most polling cycles return identical data.

**Exit criteria:** все active failure modes either auto-recover or surface clearly. CHANGES_REQUESTED stale dead-end + EYES race resolved.

### Multi-repo readiness batch (PR-192..PR-194, ~3-5 days)

- **PR-192** PR-220 reconciliation logic for existing AGENTS.md (см. также Onboarding existing project section ниже). Section-marker append pattern via marked blocks. User content authoritative.
- **PR-193** Multi-repo state isolation audit + fixes. Verify per-repo Redis keys не collision, one repo CODING не блокирует другого WATCH polling, slug collision handling. Fix anything found.
- **PR-194** Production config tracking — `config.production.yml` overlay file, gitignored, читается daemon поверх `config.yml`. Closes config drift gap from 2026-04-28 sigkill session lesson 5.

**Exit criteria:** megaraid-dashboard onboarded в read-only/observe mode без ручного редактирования AGENTS.md. Multi-repo dashboard работает корректно. Production config reproducible from git + override file.

### Polish batch (PR-195..PR-204, ~2-3 days)

- **PR-195** push_count desync fix. UI metric совпадает с GitHub Commits tab — single source of truth.
- **PR-196** AGENTS.md prohibit draft PRs (text update + PR-220 reconciliation example).
- **PR-197** Document WATCH STALLED substate (Sprint F4.2 PR-194). Confirm intentional vs bug, document semantics в architecture docs.
- **PR-198** PipelineState.MERGE dead value cleanup (Sprint F4.2 PR-195). **REJECTED — value is reachable.** Verification (2026-04-30): no production code path assigns `state.state = PipelineState.MERGE`, but comparison sites exist in `_TRANSIENT_STATES` (`src/daemon/runner.py:89`), the pause-substate set (`src/daemon/runner.py:1293`), `_DEFERRED_RUNNER_CONFIG_STATES` (`src/daemon/main.py:66`), two state sets in `src/web/app.py`, and four jinja templates. These defensive references guard `RepoState.model_validate_json` against stale Redis payloads written by older daemon binaries that may carry `state == "MERGE"`. Removing the enum value would break that backward-compatibility path. Architectural truth (MERGE has no live transition; merges happen inline inside `handle_watch`) is already documented in `docs/architecture-state-machine.md`.
- **PR-199** Event text clarity pass — audit log_event calls, normalize messages, remove ambiguity (Sprint F3.3 PR-184).

**Exit criteria:** UI polished, documentation caught up, dead code removed.

### Polish batch addendum (added 2026-04-29 evening)

Observed during 2026-04-29 task-upload session:

- **PR-200** Task header validation — synonyms map + multi-error report. Map common synonyms to canonical enum values (`bug` → `bugfix`, `fix` → `bugfix`, `chore` → `refactor`, `feat` → `feature`, `task` → `feature`). Continue validation past first error so user sees all problems in single upload attempt rather than fix-one-at-a-time. Update `docs/TASK_SCHEMA.md` with synonym mappings as alternative input form. Type: refactor. Complexity: low. Priority: 3. Reasoning: AI assistants and humans both regularly use looser vocabulary than the strict enum; rejecting on first hit is annoying for batch uploads.

- **PR-201** Dashboard control row visual consistency. Current state (observed 2026-04-29): repo card top-right has Pause/Stop as flat icon buttons (no border, hover-only highlight) while Upload tasks button has solid border + padding + text label. Mixed visual language. Decision: align all controls in row to one style. Recommendation: keep flat-icon style for control actions (Pause/Stop), but Upload tasks is fundamentally a different action (CRUD on queue, not state control) — surface as small text button with consistent border-less hover style, OR move Upload tasks to a different region (dropdown menu, secondary toolbar). Either way, design pass needed before "feels polished" criteria is met. Type: ux. Complexity: low. Priority: 3.

- **PR-202** WATCH adaptive polling — slow-start, fast-tail. Empirical observation 2026-04-29: WATCH state polls GitHub heavily but Codex Review + CI typically take 2-7 min and 5-15 min respectively to respond. Polling every 30s for the first 5 min is wasted quota on a scheduled wait. Logic: WATCH entry → slow 300s for first 5 min; after 5 min without event → fast 30-60s; on event detected → reset to slow start. Inverted from standard exponential backoff (which is fast-start, slow-tail). Combined with PR-184 IDLE adaptive: IDLE worker pattern (fast → slow on inactivity) and WATCH response-anticipation pattern (slow → fast on expected wait window passage) cover both dominant burn phases per Phase-resource separation observation. Type: feature. Complexity: low. Priority: 2. Depends on PR-180 (REST replacement) so the WATCH polling is already on REST core quota.

- **PR-203** Compact resource limits row with tooltips. Replaces current single GitHub API budget bar with 4 chips: GH REST, GH GraphQL, Claude 5h, Claude weekly. Color zones by remaining percentage (green > 50%, amber 20-50%, red < 20%). Hover tooltip shows absolute values and reset time. No click action in this PR — history modal deferred (see Deferred section). Type: ux. Complexity: low. Reasoning: current visualization shows only GitHub API budget; system actually depends on 4 distinct quotas. Operator awareness gap.

- **PR-204** Structured per-PR outcome logging for future analytics. Append-only JSONL at `/data/analytics/<year>-<month>.jsonl` with one record per merged PR. Schema captures coder/model/version explicitly to support future analytics that respect outcome-data version drift (see Architectural decisions section). No analysis layer, no telemetry, no upload — pure persistence with the right schema for future use. SQLite migration trigger documented as future work. Type: feature. Complexity: low. Reasoning: foundational gap for any future selector training or lessons-learned capability; storage cost is negligible (~125 KB/year), schema choice now prevents painful retroactive backfill later.

These add to the existing PR-195..PR-199 polish batch. Total polish batch now PR-195..PR-204.

### Deferred (sprint-scale, не в ближайшем 2-week плане)

- **Sprint F2.1 SoT direct instructions:** PR-167/168/169 — feature flag guarded refactor of how daemon talks to coder. Большая работа, deferred until critical/important/multi-repo batches shipped.
- **Sprint F2.2 PAUSED removal:** PR-170/171/172/173. Высокого риска refactor state model. Prerequisite: F1 stable, no active sprints conflicting. Deferred indefinitely until awaiting_start clearly needed.
- **Sprint F3.2 Thompson Sampling:** PR-178/179/180/181. epsilon-greedy works adequately. Defer until measurement data justifies (need ~50+ merged PRs across both coders before posterior actually informs).
- **GitHub App migration (OBS-AC Leverage 6):** sprint-scale rewrite of daemon auth. Defer until either GraphQL diet (PR-180/PR-191) proves insufficient OR third-party adoption becomes relevant.
- **Manifest flow for third-party adoption (PR-241 candidate):** depends on App migration. Defer until first external user.
- **Nightly e2e self-testing for pipeline-orchestrator (Sprint F4.2 PR-192):** depends on stable production. Defer until critical/important batches shipped.
- **OBS-5 gh credential helper instrumentation (PR-191 candidate):** intermittent, low-impact. Investigation PR, not immediate fix. Defer until other items closed.

---

- **Resource limit history charts (future PR candidate):** modal-on-click history graphs (4-hour rolling) for each of the 4 quotas surfaced in PR-203. Pending decision on storage backend — in-memory loses on daemon restart, Redis depends on RDB snapshot persistence, SQLite adds new dependency, PostgreSQL is overkill. Defer until storage decision is made or until operator concretely needs trend visibility (currently visual chip + reset time gives enough situational awareness for solo operator workflow).

## Vision (beyond Round 4, возможно отдельный продукт)

### Release Qualification Agent (Tester role)

Human-in-the-loop release gate, не pipeline stage. Это **пятый actor** в pipeline: Planner — Coder — Reviewer — Merger — **Tester**.

**Trigger:** определяется Planner'ом по критериям milestone (например "после 7 sprint'ов MVP готов → release candidate"). Не запускается per-PR.

**Scope работы Tester'а:**
- Анализ coverage report, идентификация uncovered paths
- Тестирование слепых пятен (skip то что покрыто unit tests, focus на untested)
- Security probing (auth bypass, injection, privilege escalation, secret exposure)
- Integration reality check (orchestrator кодит запуск full stack — может быть любой stack, не только web+daemon+redis)
- UI exploration (реальные клики, state transitions, error paths)
- Architecture compliance (drift от Planner'овского замысла, неплановые dependencies, нарушенные invariants)
- User journey simulation (realistic flows от регистрации до advanced scenarios)

**Human involvement:** не automated. Tester планирует и руководит LLM, человек наблюдает, направляет, скидывает свои наблюдения и findings в session, вместе достигают release decision.

**Output:** structured findings report с severity ratings, reproducible steps, artifacts. Human gate: release / block / defer.

**Implementation estimate:** это major product phase. Нужно release management (branches, tags, changelog), Tester runbook methodology (STRIDE для security, persona walkthroughs для UX), stack-agnostic integration test environment (orchestrator генерирует test setup для любого target stack), Playwright/browser automation, live collaboration UI (human ↔ Tester bidirectional), findings artifact storage.

**Может выделиться в отдельный продукт.** Обоснование: Tester не требует самого pipeline-orchestrator'а для работы — он может работать с любым готовым кодом. Это standalone "Release Qualification Agent" / "Pre-Release AI QA". Separate positioning, separate pricing, separate moat.

Пока — Vision. Без PR'ов, без Round'а, без конкретики. Returnить когда Round 3 + Round 4 закрыты и baseline стабилен.

---

---

## Development model & substrate observations (added 2026-04-26)

### Trunk-based reality vs PR-driven formalism

This project is, in practice, **trunk-based development** with PR-formalism as a mechanical convention, not a collaboration protocol. The recognized gap between intent and reality matters because some industry-standard backlog items become lower priority once we acknowledge the actual model.

**Reality:**
- Single architect (Aleksei). 100% control over main.
- No external contributors. No PR boundary serves as a "stranger gate" — everything is one team writing one repo.
- All architectural decisions happen in **post-sprint zip-dump strategic review chats** where the entire codebase is loaded into context and reviewed holistically. This IS continuous integration thinking, just out-of-band relative to GitHub.
- Memory entries + roadmap.md preserve continuity across PR boundaries.
- PR-formalism serves three concrete purposes only: (1) Codex review automation gate, (2) GitHub branch protection audit trail, (3) atomic merge units for `gh pr merge`.

**Implication:** "PR-driven" downsides don't fully apply here. We have:
- 3 review levels (architect post-sprint, Codex per-PR, integration tests) — vs. 1 in typical PR-driven shops.
- No "different teams own different services" coordination overhead.
- No "external PR onboarding documentation" burden.

**But also:** single-architect bus factor = 1. No autonomous architectural decisions allowed (per Aleksei). Pipeline is execution layer for human decisions, not autonomous architect.

### AI context limit on large PRs (empirical)

Verified on PR-153 (5 concerns in one task file → coder finished only Python infra, got stuck for 7+ commits on remaining edge cases). Hypothesis: AI **effective context window** << **advertised context window**. Quality of attention degrades after ~30-50K active tokens of working state, even on models advertising 200K. Long-tail dependencies (file A line 100 connected to file Z line 5000) are lost. Drift to local optimum becomes the failure mode — coder fixes locally, breaks elsewhere, fixes elsewhere, breaks back.

**Conclusion:** Spike / vertical-slice PRs do NOT work with current AI. Small focused PRs (one concern, one logical area, ≤150 LoC) are the optimal unit. Post-sprint architectural review by human is the integration mechanism.

This conclusion is **stable** until either (a) AI context handling improves materially, or (b) we deploy a substrate that compresses context (see Layer 2 substrate below).

### Backlog reweighting

Given trunk-based reality + AI context limits, the following adjustments apply:

- **HIGHER priority:** features that empower the single architect to direct, observe, and intervene. Stop button per repo (memory #4), task content viewer (memory #4 wishlist + roadmap PR-186), immediate upload pickup via Redis pub/sub (PR-185), per-repo coder pin in task header (FINDING-2 / PR-156 — DONE).
- **LOWER priority:** features intended for theoretical multi-user / multi-team setups. AGENTS.md bounded reading scope (PR-168/177), per-repo `review_timeout_min` UI, multi-tenancy considerations.
- **Neutral:** features that improve baseline reliability regardless of team size. State model refactor (Wave 5), bounded-retry unification (PR-184), Thompson Sampling (Sprint 11).

---

---

## Multi-tier agent hierarchy (architectural direction, added 2026-04-26)

Direction crystallized in conversation 2026-04-26 evening. Aleksei's framing: "звать human раньше — а он должен звать другого агента, который имеет [memory access, full architecture nav, time/tokens for cross-file reasoning, escape capability]."

This is a refinement of the **Tester role** Vision item (line 698) — broader and more specific. Not just review-time second opinion, but **always-available diagnostic agent** that coder can escalate to mid-cycle.

### Three-tier model

```
Tier 1: Coder agent (claude / codex CLI in PR working dir)
   ↓ ESCALATE protocol (PR-166)
Tier 2: Architect / Diagnostic agent (NEW)
   ↓ ESCALATE when needs strategic decision
Tier 3: Human (Aleksei)
```

### Tier 1 (current, mostly built)
- Cwd: single PR working dir
- Context: task file + AGENTS.md + (after PR-167) CI logs + review feedback
- Mandate: implement task per spec
- Time budget: 5-30 min per FIX cycle
- Cost: low (CLI quota)
- ESCALATE trigger: PR-166 protocol, no-push counter (PR-164), explicit ESCALATE marker in stdout

### Tier 2 (new direction, future PR series)
- Read access: full repo + memory entries + roadmap.md + past PR history + past chats
- Mandate: diagnose cross-component issues, classify infra-vs-product, decide split-vs-fix-in-place, propose architectural direction
- Time budget: 5-15 min per investigation
- Cost: medium (Sonnet/Opus with large context window + Graphify navigation)
- ESCALATE trigger: ambiguous architectural decision, novel pattern requiring strategic taste, business/priority decision required

### Tier 3 (untouchable)
- Mandate: architectural taste, approval, strategic priority
- Receives notifications from Tier 2 with diagnosis + recommendation
- Decides split-vs-merge, prioritization, halt-vs-continue

### What's needed to build Tier 2

**Code:**
- ESCALATED state in daemon state machine (foundation in PR-166)
- Tier 2 invocation logic in daemon — spawn architect agent with rich prompt
- Architect agent prompt template — what slice of memory + roadmap + diff is included
- Architect agent output protocol — recommendation format (diagnosis + action + escalation flag)

**Infrastructure:**
- Graphify (or equivalent) for navigating large codebase under context budget
- Search-past-chats access for Tier 2 (similar to `conversation_search` tool available in human-AI sessions)
- Cost budget enforcement for Tier 2 invocations — only when Tier 1 stuck, not for every issue
- Notification system to human when Tier 2 escalates above

**Mental model discipline:**
- Tier 2 can be wrong → it RECOMMENDS, does not DECIDE
- Tier 2 has cost → use only when Tier 1 stuck, not as routine helper
- Coder stays in PR scope; Architect crosses scope; Human spans projects
- Each tier's mandate and limits are explicit and respected

### Realistic walkthrough (the case from 2026-04-26 session)

```
14:30 Daemon picks PR-160 (test pollution fix). Tier 1 coder works.
14:45 Initial implementation, push, CI runs.
14:50 CI fails on test_stop_during_coding_then_resume.
14:51 Tier 1 enters FIX cycle, reads CI log via PR-167 enrichment.
14:55 Tier 1 attempts fix. Push. CI fails again (same test, same error).
15:00 Tier 1 attempts again. CI fails (same).
15:05 Tier 1 attempts again. CI fails (same). PR-164 no-push counter reaches 3.
15:08 PR-166 ESCALATE protocol triggers. Tier 1 posts ESCALATE marker:
      "ESCALATE: CI fails consistently on test X; my fixes don't address root cause."
15:09 Daemon transitions PR to ESCALATED state, spawns Tier 2 architect.

Tier 2 receives:
- PR url + diff + commit messages
- All CI logs from failed runs
- Read access to full repo (via Graphify nav)
- Access to memory entries + roadmap
- Search-past-chats capability

Tier 2 reasoning (5-10 min):
- "test_stop_during_coding fails with timed out for IDLE state"
- "Examining conftest.py: reset_testbed only closes PRs, doesn't wipe tasks/"
- "Memory: similar pattern discussed yesterday in PR-159 self-FIX"
- "Roadmap: Variant D direction long-term, but this is shorter horizon"
- "Diagnosis: test pollution between test runs, C-1 pattern"
- "Recommendation: separate PR fixing reset_testbed fixture; OUT OF SCOPE for current PR-160"
- "Don't force coder to fix it; ESCALATE to human for follow-up PR approval"

15:15 Tier 2 posts comment on ESCALATED PR with diagnosis + recommendation. Notifies human.
15:20 Human accepts recommendation. New PR-160a opened with fixture fix. Original PR-160 on hold.
15:30 Tier 1 coder works on PR-160a (smaller, focused). Merges OK.
15:40 Daemon resumes PR-160 (original), now has fixture fix in main, retries successfully.
```

### Priority and timing

**Sprint group:** beyond current Sprint F-foundation series. Realistic order:
1. F1 series (PR-163, 164, 165, 166) — foundation reliability + ESCALATE protocol (Tier 1 escalation capability)
2. Stage 2 quick wins (PR-167, 168, 169) — production-ready basics
3. Stage 3 UX (Tools 4, 5, 6) — single-architect empowerment
4. Variant D series — kill QUEUE.md, direct injection
5. Sprint 11 — Thompson Sampling, cost-aware reward
6. **Tier 2 architect agent series (3-5 PRs)** — diagnostic agent foundation
7. Round 5+ — Graphify integration (likely co-developed with Tier 2)

Estimate: Tier 2 minimal viable becomes possible after Sprint 11 + Graphify spike, roughly 2-3 months from foundation closure. Could come earlier if found higher priority than Sprint 11.

### Boundary conditions

**Tier 2 must NOT:**
- Auto-merge PRs (architect recommends, doesn't decide)
- Modify production code directly (architect proposes diffs, coder applies)
- Spawn additional agents (no recursive Tier 3 self-replication)
- Operate without rate limit / cost budget

**Tier 2 SHOULD:**
- Always escalate to human when budget exhausted
- Post recommendations as markdown comments (human-readable)
- Reference specific lines / files / past PR numbers (auditable reasoning)
- Time-out gracefully if reasoning exceeds budget

This direction explicitly informed by **AI context limit observation** (line 745) and **trunk-based reality** (line 727). Tier 2 is the natural next step in single-architect productivity multiplication: not replacement, **leverage**.

---

### Layer 2 substrate: Graphify investigation

**Item to investigate:** [Graphify](https://graphify.net/) — open-source AI coding assistant skill that builds a navigable knowledge graph of a repo (AST via Tree-sitter + semantic via LLM + Leiden clustering). MIT-licensed, runs locally, no telemetry. Target compression: ~70× token reduction (1.7K vs 123K on a 52-file mixed corpus per their published benchmarks).

**Why relevant for pipeline-orchestrator:** Graphify directly addresses the AI context limit pain we documented. Coder process (Claude Code / Codex) consults `GRAPH_REPORT.md` before raw file Glob/Grep via PreToolUse hook (Claude Code) or AGENTS.md instruction (Codex/others). For our project specifically — pipeline-orchestrator has ~580K of source + ~1.3M of tests + accumulated docs/roadmap/memory state. It does not fit in any AI context window. Currently we work around this by zip-dumping the entire repo to a strategic chat for architecture work, but that is human-in-the-loop. Graphify could help the **autonomous coder** navigate the repo without re-grepping every cycle.

**Investigation scope:**
- Run `pip install graphifyy && graphify` on pipeline-orchestrator main clone, generate `graphify-out/`. Inspect `graph.html`, `GRAPH_REPORT.md`, `graph.json`. Evaluate quality of god-nodes detection and surprising-connections.
- Test with one e2e cycle: install `graphify claude install` (writes CLAUDE.md hook + PreToolUse), trigger one PLANNED PR via daemon, observe whether coder's Glob/Grep calls are reduced. Compare token cost vs control PR without Graphify.
- Evaluate fit as **Layer 2 substrate** for pipeline-orchestrator: does coder produce better PRs, faster, with less rework?
- Risk assessment: Graphify Pass 3 (semantic extraction) sends file contents to AI provider. For pipeline-orchestrator (public repo, MIT) this is fine. For other managed repos with proprietary code — concern. May restrict to code-only mode.
- Privacy/cost: Pass 3 burns API tokens of the coder's own quota. Not free — needs measurement.

**Decision criteria:** investigate, then decide whether to add as standard recommendation in AGENTS.md ("for any managed repo, run /graphify on first sync") or as automatic step in `ensure_repo_cloned` (controversial — adds dependency, runtime cost). Default position: recommend manual usage by Aleksei, do not auto-install in daemon flow until proven beneficial across multiple repo types.

**Priority:** Round 5+ (after Variant D + Sprint 10 + Sprint 11 stabilize). Substrate question, not blocker for any current PR. Possible earlier promotion if coder context-loss incidents become frequent post-Variant D.

---

---

---

## Active investigations (added 2026-04-27)

### OBS-Y: Daemon merges PR before formal APPROVED state

**Observed:** PR #222 (PR-164 FIX no-push deadlock circuit breaker) merged automatically at 00:46:34 on 2026-04-27. Post-mortem analysis revealed:

- Head commit `174ea408` pushed at 00:39:57
- All 4 Codex formal reviews: `state=COMMENTED`, none `APPROVED`
- +1 reaction on PR body: created at 00:50:20 — **4 minutes after** the merge
- Production config `allow_merge_without_review` is unset (defaults to false)
- Therefore daemon should NOT have merged

**Two independent issues sandwiched together:**

1. **Claude in FIX cycle hallucinated state.** Claude's STDOUT at 00:46:24:
   > "Codex +1 at 2026-04-27T00:45:30Z (post round-3 push at ~00:42Z → non-stale)"
   
   No such +1 reaction existed at 00:45:30 (it was created at 00:50:20). Claude either fabricated the timestamp or misread an earlier signal. This is a **coder reasoning bug** — coder confidently asserted false fact about PR state.

2. **Daemon also merged.** Even if coder's verdict is ignored, daemon's own `_compute_review_status` decided APPROVED was true. Without an actual +1 reaction or formal APPROVED review at that moment, the only mechanism would be:
   - Stale cache from earlier cycle, OR
   - Misinterpretation of body anchor parsing path (`src/github_client.py:503+`), OR
   - Bug in threshold comparison (line 462-477)

**Impact:** the PR happened to be a good merge (claude's fix was correct, Codex eventually +1'd). But the merge path was incorrect. If a real bad fix had slipped through this gap, prod main would be broken.

**Action plan:**

1. **Add debug logging to `_compute_review_status`** in `src/github_client.py`. Each branch (line 444-488) should log: `head_sha`, `latest_review_sha`, `latest_review_time`, `reaction_time`, `head_commit_time`, `threshold`, decision (`body_approved=True/False`). Plus log cache hits explicitly. MICRO PR (~30 LoC).

2. **Add coder reasoning verification**. Coder's claims about PR state in STDOUT should be cross-checked by daemon before being acted on. If coder says "PR green" but daemon's own `_compute_review_status` returns NOT APPROVED — daemon should NOT trust coder's verdict.

3. **Once 2-3 more observations of premature merge collected with debug logs** — root cause becomes diagnosable. Targeted fix follows.

**Priority:** medium. Not blocking deployment, but represents trust-erosion risk in autonomous merging. Fix in batch after Stage 3 UX polish merges.

**Backlog items:**

- **PR-180 (next foundation batch):** Debug logging in `_compute_review_status`. MICRO PR.
- **PR-181 (after observations):** Coder verdict verification — daemon does not trust coder's "PR is green" claim without independent confirmation.
- **PR-182 (after debug logs collected):** Targeted fix once root cause identified.

### Onboarding existing project (gap, added 2026-04-27)

**Observed:** Pipeline-orchestrator currently assumes the managed repo is greenfield with full conventions in place: AGENTS.md, scripts/ci.sh, tasks/ directory, .gitignore for artifacts, GHA workflow with unit+integration jobs, Codex Connector enabled, optional branch protection. New project starting from zero — works fine.

For an EXISTING project (e.g., LAN_Transcriber, AWA-App, or any of Aleksei's other repos), none of these conventions exist. Daemon clones the repo successfully but the first PLANNED PR breaks because:
- AGENTS.md missing → coder doesn't know Work Modes / FIX FEEDBACK trigger
- scripts/ci.sh missing → no local gate
- tasks/ directory missing → coder doesn't know task file format
- Existing CI workflow may conflict with pipeline conventions
- Existing branch protection may require human reviewers (Codex can only Comment)
- AGENTS.md / CLAUDE.md if already present → merge conflict with template
- Non-Python stack → ci.sh template needs adaptation
- Long history → coder context overflow risk

**Chicken-and-egg:** the bootstrap convention itself requires conventions to follow. Solution direction: special bootstrap task whose body INCLUDES the AGENTS.md template inline, so coder can read it from the task body and then create the file in the repo.

**Action plan (separate PR series, sized ~3-5 PRs):**

1. **Onboarding runbook** (MICRO PR): `docs/onboarding-existing-project.md` — manual steps, edge cases, troubleshooting per common stack (Python, JS, Go, Rust).

2. **Bootstrap task template** (config PR): `templates/PR-bootstrap.md.template` and supporting `templates/AGENTS.md.template` + `templates/ci.sh.template` (per-language variants). User copies and customizes for each new repo.

3. **Stack detection helper** (medium feature): coder (or future Tier 2 architect) detects language/framework from repo files (pyproject.toml, package.json, go.mod, Cargo.toml) and adapts ci.sh template accordingly during bootstrap.

4. **Bootstrap merge bypass** (small feature): config flag or per-repo setting `bootstrap_pr_count: 1` allows the first N PRs of a new repo to merge without full review/CI gate. Necessary because the gate itself doesn't exist yet on first PR.

5. **Existing convention reconciliation** (medium feature): if repo already has AGENTS.md / CLAUDE.md / ci.sh, bootstrap PR merges with existing rather than overwrites. Risk-managed via 3-way diff and human review of the merge.

6. **Onboarding wizard in UI** (large feature, optional): Settings page step-by-step "Add new repo" that walks user through clone, bootstrap task, first PR review, gate enable. Replaces manual runbook over time.

**Testing path:** onboard one of Aleksei's existing public repos (LAN_Transcriber or AWA-App) as the first real-world test. Document what breaks, fix iteratively.

**Priority:** high once Stage 1-3 stable. This unblocks "use orchestrator for multiple projects" — a strategic capability for sustained productivity. Currently the orchestrator only manages itself, which is impressive but limited.

**Backlog items:**

- **PR-183:** Onboarding runbook + templates (Python, JS, Go variants).
- **PR-184:** Stack detection helper (pyproject.toml etc → adapt ci.sh).
- **PR-185:** Bootstrap merge bypass config flag.
- **PR-186:** Existing AGENTS.md/CLAUDE.md reconciliation — section markers approach (see detail below).
- **PR-187+:** Onboarding wizard UI (later, optional).
- **PR-220:** Implementation of the section-markers reconciliation logic in scaffolder.py (per spec below).

#### Test subjects identified (added 2026-04-28)

Two real existing projects in the AlexBomber12 account are valid candidates for testing the onboarding reconciliation logic (PR-220) before we touch any user-facing flow:

**megaraid-dashboard** (`github.com/AlexBomber12/megaraid-dashboard`):
- Python project, ~27 src files, ~27 test files, Alembic migrations present
- AGENTS.md: 67 lines, sections: Mission, Workflow Rules, Code Style, Testing, Architecture Rules, Security Model, Hardware Target, Out Of Scope, Communication
- CLAUDE.md present, README.md present, ci.yml in `.github/workflows/`
- Status: incomplete project (per operator), good non-greenfield baseline

**sms-gateway-v2** (`github.com/AlexBomber12/sms-gateway-v2`):
- Python project, ~24 src files, ~51 test files (notably higher test coverage), Dockerfile + deploy/
- AGENTS.md: 61 lines, sections: Mission, Workflow, Code Style, Testing, Architecture, Security, Hardware Target
- CLAUDE.md present, README.md present, ci.yml in `.github/workflows/`
- Status: incomplete project (per operator), good non-greenfield baseline

**Reconciliation observations (preliminary):**

Both projects use a section structure that **substantially differs** from pipeline-orchestrator's AGENTS.md (which has Work Modes, Daemon Mode, CI gates, Codex Review gate, ESCALATE protocol, etc.). Test subjects use a "guidelines for AI coder" structure (Mission/Workflow/Code Style/Testing/Architecture/Security/Hardware Target). Pipeline-orchestrator's AGENTS.md is "operating manual for the daemon."

This is a real test of PR-220 reconciliation: the user's existing AGENTS.md is **not wrong** — it is a different document genre serving a different purpose. The orchestrator must either:

1. **Append daemon-required sections** (Work Modes, CI gates, etc.) without disturbing the user's content. Section-marker reconciliation per the existing PR-220 spec. Project's AGENTS.md keeps its identity, daemon adds what it needs in marked regions.
2. **Refuse to onboard** if reconciliation cannot be done safely — surface to user, let them decide whether to integrate.
3. **Migrate user's content** into the daemon's section structure — risky, destructive, not recommended.

**Recommendation:** option 1 (section-marker append). PR-220's reconciliation logic should treat the user's existing AGENTS.md as authoritative for its sections and add only the daemon's required sections in clearly-marked regions (e.g., `<!-- pipeline-orchestrator: managed -->` blocks).

**Hardware Target section:** both subjects have a "Hardware Target" section that pipeline-orchestrator's AGENTS.md does not have. This is project-specific (megaraid is a hardware monitoring dashboard, sms-gateway runs on specific hardware). Onboarding logic must treat such project-specific sections as user-owned and never touch them.

**Test plan (when ready):**

1. Clone both subjects locally.
2. Run pipeline-orchestrator onboarding on each, dry-run mode (do not write files yet).
3. Inspect proposed reconciliation diff for each.
4. Verify: user content preserved, daemon sections added in marked regions, no destructive overwrites.
5. Apply reconciliation, run daemon against the onboarded project, verify CODING/WATCH/MERGE flow works without test-only assumptions leaking through.

**Multi-repo aspect:** running daemon against both projects simultaneously will validate the multi-repo path that has so far been exercised only with a single repo (pipeline-orchestrator itself). Concrete things to verify:
- Per-repo state isolation (one repo's CODING does not block another repo's WATCH polling)
- GraphQL quota distribution across repos (related to OBS-AC diet — burn doubles with second active repo)
- `tasks/` directory isolation per repo
- Slug collision handling if two repos have similar names
- UI dashboard handles 2+ repo cards correctly

**Defer until:** PR-220 reconciliation logic exists and OBS-AC diet leverages 2-3 are shipped (otherwise multi-repo will hit GraphQL quota limit immediately).

#### Reconciliation strategy for existing AGENTS.md / CLAUDE.md (clarified 2026-04-27)

**Problem:** scaffolder.py:260 currently SKIPS copying template if `AGENTS.md` or `CLAUDE.md` already exists. This is correct defensive behavior — we never overwrite user content. But it creates a real gap: existing repo's AGENTS.md describes the user's project conventions (code style, testing rules, framework specifics) but does NOT describe orchestrator conventions (Work Modes, FIX FEEDBACK trigger, @codex review protocol, artifacts). Coder reads existing AGENTS.md, follows project conventions correctly, but does NOT execute orchestrator protocol. Daemon waits for actions coder doesn't perform → silent breakage.

**Four directions evaluated:**

**A. Section markers in AGENTS.md.** Orchestrator template wrapped in HTML comments:
```
<!-- BEGIN: orchestrator-managed (do not edit between these markers; auto-updated by pipeline-orchestrator) -->
## Work Modes
... orchestrator conventions here ...
<!-- END: orchestrator-managed -->
```
On scaffold: append marked section to existing file (or replace between existing markers for future updates). User content outside markers untouched. The HTML-comment markers themselves are visible in the rendered markdown explaining provenance — user is NOT surprised.

**B. Separate ORCHESTRATOR.md file.** Don't touch AGENTS.md at all. Coder gets two files in prompt context. Lower reliability — coder may ignore second file.

**C. Inject conventions via task body.** Every task file includes inline conventions reminder. No repo modification. Verbose, expensive in tokens, fatigue.

**D. Manual AI-assisted merge with user oversight.** Bootstrap PR proposes diff (append orchestrator section); requires human review and merge. No silent modification of user's repo.

**Selected approach:** **D + A combined.**

- **First-time bootstrap (D):** scaffolder detects existing AGENTS.md, generates bootstrap PR with proposed diff appending the marked orchestrator section. PR has `auto_merge: false` enforced — Aleksei reviews, accepts (or modifies), merges manually. This is a **deliberate decision moment** — onboarding orchestrator into an existing project deserves explicit human review of the convention merge.

- **Visible markers (A):** the appended section uses HTML comments visible to user as commentary. Quote: `<!-- BEGIN: orchestrator-managed (do not edit between these markers; auto-updated by pipeline-orchestrator) -->`. User is not surprised when they next open AGENTS.md — they see what is managed and why.

- **Future automatic updates (A):** subsequent pipeline-orchestrator version bumps that update the orchestrator-managed section can do so automatically by replacing content between markers. User content outside markers stays untouched.

- **Conflict resolution rules:** if user removes the BEGIN/END markers (intentionally or accidentally), scaffolder treats the file as not-managed and creates a fresh bootstrap PR rather than silently appending again. If user moves markers, scaffolder respects the new boundaries (only updates content between markers). If markers are malformed (BEGIN without END or vice versa) — scaffolder logs warning and leaves file untouched, opens fresh bootstrap PR.

**PR-186 revised spec:**

`src/daemon/scaffolder.py`:
- Detect existing AGENTS.md / CLAUDE.md.
- If file does not exist → copy template as today (current behavior preserved).
- If file exists with valid orchestrator markers → replace content between markers with current template's marked section (silent update path).
- If file exists without markers → do NOT modify. Instead, set a flag in repo state indicating "bootstrap reconciliation PR pending". Daemon's IDLE handler picks this up next cycle and creates a bootstrap task file with the proposed merge as a normal PR (`auto_merge: false` flag in the task header).
- Bootstrap reconciliation PR has type `config`, priority 1, depends_on: none, body = explanation + diff preview. Coder for bootstrap is `claude` (better at multi-file merge reasoning than codex).

`templates/AGENTS.md`: rewrap orchestrator-specific sections in BEGIN/END markers. Non-orchestrator-specific content (if any) outside markers.

`tests/test_scaffolder_reconciliation.py` (new):
- Existing AGENTS.md without markers + scaffold → no file modification + bootstrap flag set.
- Existing AGENTS.md with markers + scaffold + template change → content between markers updated.
- Existing AGENTS.md with markers, user adds text outside markers → scaffold preserves user text.
- Existing AGENTS.md with malformed markers (BEGIN, no END) → no modification + warning logged.
- Existing AGENTS.md with markers removed by user → treated as not-managed (no silent re-append).

`docs/onboarding-existing-project.md` (PR-183 dependency): documents the markers convention, what to expect on first bootstrap, how to remove orchestrator management cleanly if user later wants to (just delete the marked section + markers).

**Renumber:** PR-186 in this spec is **distinct** from the older "PR-186 Task content viewer" (which was originally a different number; the current number for that is PR-198 → PR-186 in the renumber table at line 358). To avoid confusion, this onboarding reconciliation PR is **PR-220** in the new sequence. Updated backlog item list above.

### OBS-Z: Codex EYES race window (observed 2026-04-27)

**Observed:** PR #227 (PR-170 Remove STALLED indicator) sat in WATCH state for 21 minutes with `review=EYES, ci=SUCCESS`. Codex had emoji-react'd with eyes on PR body indicating "I'm reviewing" but never delivered actual review. HUNG handler triggered at 21-min timeout, posted `@codex review` fallback, Codex responded with +1 within 1 minute, PR merged normally.

**Root cause:** dual-trigger race on the Codex Connector side.

- Codex auto-triggers on PR creation and push (default Connector behavior, cannot be disabled)
- Daemon ALSO posts `@codex review` after push (defensive — covers cases where Codex auto-trigger silently fails)
- When both triggers fire within a small window, Codex sometimes posts EYES (acknowledgment) but the actual review work hangs internally — never produces +1 or CHANGES_REQUESTED
- Only HUNG timeout (20 min default) recovers via re-trigger

**Trade-off observed in dual-trigger design:**

- DEFENSIVE side: Codex auto-trigger sometimes silently fails (their rate limit, transient error). Daemon's `@codex review` post is the only recovery path. Without it — silent permanent stuck.
- COST side: when both triggers race, ~5-15% of PRs hit EYES-stuck state. 21+ minute recovery via HUNG.

**Solution direction (PR-181 candidate, post-Stage-3 batch):**

Combination of two approaches:

1. **Pre-push state check (avoid creating race):** before daemon posts `@codex review`, check current PR state via `gh pr view --json reactions,reviews`. If Codex already reacted with EYES on PR body (auto-trigger fired first) → SKIP posting duplicate. Log: "Codex auto-trigger detected, skipping duplicate @codex review post."

2. **Differentiated stale threshold (faster recovery from existing race):** new config field `stale_review_threshold_eyes_min: 5` (separate from general `stale_review_threshold_min: 10`). When review state is EYES and last activity older than 5 minutes — re-trigger as stale. EYES is recognized stuck pattern; treat with shorter timeout than CHANGES_REQUESTED (which represents legitimate active review work).

3. **Analytics counter:** count "EYES race events recovered via pre-push check" vs "EYES race events recovered via 5-min stale retrigger" vs "EYES race events escalated to HUNG". Informs whether fix is effective; visible in dashboard or API endpoint.

**Files to touch (PR-181 spec sketch):**

- `src/daemon/handlers/coding.py` and `fix.py`: before `_post_codex_review` calls, add pre-push state check (~10 lines).
- `src/daemon/handlers/watch.py`: add EYES branch to `_maybe_retrigger_stale_review` with shorter threshold (~15 lines).
- `config.yml`, `config.test.yml`: add `stale_review_threshold_eyes_min` field (default 5).
- `src/web/`: optional UI counter for EYES race telemetry (~30 lines).
- Tests: 5-7 new unit tests covering pre-push skip, EYES retrigger, threshold comparison.

**Total size:** small-medium, ~50-80 LoC product + tests.

**Priority:** medium. Quality-of-life improvement, not blocker. Loses ~21 minutes per stuck PR; current cycle is ~5-10 PRs/day so estimated cost is ~1-2 hours/day of waste. Worth fixing in post-Stage-3 batch alongside PR-180 (debug logging) and PR-181 (this).

**Relationship to PR-166 (coder ESCALATE protocol):** PR-166 handles coder-side stuck. This (PR-181) handles Codex-side stuck. Two different agents, both can stall; both deserve targeted recovery mechanisms.

**Backlog item:**

- **PR-181:** Codex EYES race resolution (pre-push state check + differentiated stale threshold + analytics counter).

### OBS-AA: Test pollution via daemon Redis state survival (root cause located 2026-04-27, fix in flight)

**Observed:** `tests/e2e/test_stop_and_resume.py::test_stop_during_coding_then_resume_picks_next_task` failing consistently in CI integration job after recent merges. Initial hypothesis (architectural defect in `_select_next_task` DOING path) was incorrect. Investigation via captured `stack-logs.txt` from a real failed CI run revealed the actual sequence:

```
08:48:39  POST /stop                                         ← test sends stop
08:48:39  Picked task PR-1777279712: e2e-sigkill-recovery   ← previous test's task!
08:48:42  Recovered: DOING task PR-1777279712, no PR -> 
            re-running CODING                                ← daemon recovery
08:49:28  Picked task PR-1777279712 (again)
08:49:29  User stop requested; terminating current coder
08:49:34  CODING aborted: user stop requested                ← stops the WRONG task
08:49:35  PAUSED -> IDLE
08:49:42  Picked task PR-1777279767: e2e-stop-resume-slow   ← test's task A picked AFTER stop
```

**Root cause:** the per-test `reset_testbed` fixture in `tests/e2e/conftest.py` only closes GitHub PRs, deletes branches, wipes `tasks/` directory. It does NOT clear the daemon's persistent Redis state (`pipeline:{slug}` containing `current_task`, `current_pr`; `control:{slug}:*` containing stop/pause/dirty flags). Previous test's `current_task = PR-X, status=DOING` survives into next test, daemon's recovery path picks it up at first poll cycle of next test, the stop/resume logic operates on the leftover task instead of the test's intended task.

**This is INFRASTRUCTURE pollution, not an ARCHITECTURE defect.** Daemon's `_select_next_task`, `_user_stopped_task_pr_ids`, DOING/TODO derivation are all correct given the input state. The input state is wrong because the fixture didn't reset it.

**Earlier misdiagnosis (recorded for posterity):** an initial investigation attempted to locate the bug in `_select_next_task` (DOING tasks bypass stopped set). Log evidence disproved this — the stopped task in the failing run was `PR-1777279712` (previous test's leftover), not the test's task A. The DOING path is technically correct in this scenario; daemon stopped what the state machine said was current. The real defect is upstream — the state machine was carrying a task from the previous test.

**MICRO PR v1 attempt (failed 2026-04-27):** First fix attempt used `redis.Redis.from_url()` to clear keys directly from host pytest process. Codex implemented `_default_test_redis_url()` that ran `docker inspect` to get container IP and connect from host. This **failed** because `redis-test` container in `docker-compose.test.yml` has NO host port mapping (only internal network exposure on 6379/tcp). Host pytest tried to connect to `172.22.0.2:6379` (internal docker IP), hit `TimeoutError`. CI runs showed `Redis is unavailable: Error -3 connecting to redis-test:6379` even from inside daemon container during test execution — the failed connection attempts plus CI workflow's `docker compose down -v` cleanup explained the SIGTERM events seen in stack-logs.txt. Branch `micro-20260427-redis-testbed-reset` deleted before merge.

**MICRO PR v2 (in flight 2026-04-27, expected to land):** Rewrites `clear_testbed_redis_state(slug: str) -> int` to use `subprocess.run` with `docker compose -f docker-compose.test.yml exec -T redis-test redis-cli ...` for both KEYS enumeration of `control:{slug}:*` pattern and DEL of all collected keys (plus `pipeline:{slug}` and `upload:{slug}:pending`). No host port mapping added. No python `redis` package dependency. Subprocess approach uses container network namespace that already works correctly (the same approach is already used by CI workflow line 91 to verify `which claude` inside daemon container). Validated locally: `docker compose exec -T redis-test redis-cli ping` returns PONG, SET/GET/DEL/KEYS all work via this path. Branch `micro-20260427-redis-testbed-reset-v2`.

**Lesson recorded:** when a test failure pattern looks like a state-machine bug but the symptom is reproducible only after specific test ordering, suspect test fixture state pollution before suspecting architectural defects in the daemon. The capture-and-read-actual-logs approach was decisive here; without `stack-logs.txt` the architectural-fix hypothesis would have shipped and not solved the actual problem.

**Second lesson:** when designing test infrastructure helpers that need to reach docker-internal services, prefer `docker compose exec -T <container> <cmd>` over python clients connecting to discovered container IPs. The subprocess approach uses the container's own network namespace and works identically from CI runner host and developer DESKTOP. The python-from-host approach requires host port mappings or `docker inspect` IP discovery, both of which add infrastructure complexity and fail in subtle ways.

---

### OBS-AB: Sigkill recovery test multi-race root cause (resolved 2026-04-28)

**Observed:** `tests/e2e/test_sigkill_recovery.py::test_sigkill_during_coding_recovers_correctly` had been intermittent across many sprints. Failure modes varied across runs: `(stale info)` push rejection, `Base branch was modified` mid-merge, timeout reaching `IDLE`, `claude] CLI failed`. The non-determinism made every diagnostic attempt feel like guesswork.

**Root cause: three independent races layered on the same test.**

1. **Shim push lease bug (deterministic when triggered).** `git checkout -B "${branch}" origin/main` in `tests/e2e/lib/coder_shim.sh::git_setup_branch` sets local upstream to `refs/heads/main`. A later `git push --force-with-lease` (no arg) reads upstream config to determine the lease check, comparing remote `pr-...` ref against local `main` HEAD — mismatch, reject. Triggered when daemon recovery's preserve-push had already created `refs/remotes/origin/<branch>` for the same branch.
2. **Stale tracking ref on shim re-invocation (deterministic when triggered).** When shim is re-invoked for the same branch (recovery → CODING retry), local `refs/remotes/origin/<branch>` is stale because `git fetch origin` (without explicit refspec) does not reliably refresh non-default-fetched refs. Lease check uses stale local cache against actual remote tip. Reject.
3. **Test fixture isolation race (intermittent).** Daemon mid-merge of one test's PR while next test's `reset_testbed` is concurrently wiping `main`. `gh pr merge` fails with "Base branch was modified". Cascades into all subsequent tests because daemon enters ERROR state.

**Fixes shipped:**
- PR-236 introduces `safe_push_branch` helper with explicit lease against fresh `refs/remotes/origin/<branch>` (force-fetched before lease computation). Resolves races 1 and 2.
- PR-232 adds `/stop daemon → wait PAUSED → cleanup → resume` pattern in `reset_testbed_full`. Resolves race 3.
- PR-236 also serializes shared `git config --global` setup in `scripts/entrypoint.sh` via `mkdir`-based mutex, eliminating a baseline `could not lock config file` race when daemon-test and web-test containers start concurrently sharing `HOME=/data/auth`.

**Diagnostic approach that worked:** added `DBG_SHIM` and `DBG_RECOVERY` instrumentation to dump `git for-each-ref` snapshots, `ls-remote` ground truth, local tracking config, and the exact lease value passed to `--force-with-lease`. After 4 CI integration reruns the failure-mode distribution was clear: 2× race-1 deterministic + 1× race-3 + 1× lucky pass. Verification that the fix held: 4 sequential CI reruns on the same fixed commit, all green, before merge.

**Lesson recorded:** when a flaky test's failure cause is contested across multiple debugging cycles, stop hypothesizing and add ref-state instrumentation to the suspect code paths. The trace will resolve the question deterministically. Coverage gates can be satisfied with `# pragma: no cover` on the debug exception handlers — debug instrumentation is non-production code by design. Cost of instrumentation: 2 commits + 5 lines. Time saved: hours.

**Second lesson:** for race condition fixes, one green CI run is not validation. The test was passing on lucky timing some fraction of runs before the fix existed. Require N≥3 green reruns on the same commit before merge.

**Third lesson:** flaky test investigations should run in **both** environments. CI runner timing differs from local Docker (CI runners are slower; sigkill landed before shim made any commit, race never triggered). Local alone may hide CI-only races; CI alone may hide local-only races. Once a hypothesis forms, validate in both contexts before declaring confidence.

**Fourth lesson (operator side):** during high-stakes debugging sessions, single-step execution mode is required. Composite scripts that chain `&&` past stateful failure points (rebase, merge, deploy) caused real production damage in this session when a `git rebase` failed but the script continued through `docker compose down/up`, baking conflict markers into the running config. Each command's output must be reviewed before the next is issued.

**Fifth lesson (production config gap):** during the 2026-04-28 deploy, `git reset --hard` on the production checkout reverted `config.yml` to upstream defaults that did not match the running daemon's actual configuration. The production `config.yml` had ~15 daemon overrides (review_timeout_min=20, planned_pr_timeout_sec=2400, rate_limit session/weekly split, statusline_hook, etc.) that **existed only as a local file on the production host**, never committed to the repo. Production behavior was therefore not reproducible from git alone. Action item: decide on a configuration discipline — either commit a `config.production.yml` referenced explicitly at deploy, or move all environment-specific values to environment variables, or add a deploy step that diffs the running config against expected production values.

### OBS-AC: GraphQL quota burn — diet plan and GitHub App migration (added 2026-04-28)

**Observed:** during the 2026-04-28 debug session, GraphQL quota (5000/hour on the personal token) exhausted twice within ~2 hours of intensive work. This blocked CI re-runs at peak debugging and forced ~13 minute wait windows for reset. The personal token is shared across daemon polling, Codex Connector reviewer activity, IDE GitHub Pull Requests extension passive polling, and CI re-run triggers from `gh` CLI.

**Verified GraphQL consumers:**
- **Daemon polling cycle:** `gh pr list --json statusCheckRollup` (heavy GraphQL, called per WATCH/MERGE poll). Approximate burn: 100-300 points per cycle depending on PR count and check rollup depth. At `poll_interval_sec: 60` and 1 active repo, this dominates daemon-side burn.
- **Codex Connector reviewer:** posts P1/P2 review comments on every PR push. Each review post is GraphQL-heavy (PR context fetch, comment thread inspection, file diff via GraphQL).
- **IDE GitHub Pull Requests extension (VS Code):** polls GraphQL for PR list refresh ~once per minute when window is open, regardless of user activity. **Identified as a major silent consumer; removed during 2026-04-28 session.**
- **CI workflow `gh` usage:** workflow files use `gh api` and `gh pr list` for status checks and rollup queries. Each CI run costs additional GraphQL.
- **Daemon `_get_codex_review_signals`:** GraphQL call per WATCH cycle to detect Codex review state transitions.

**Suspected additional consumers (not yet verified, action items below):**
- Browser tabs open on github.com PR pages (each PR view does background GraphQL refresh).
- Other IDE extensions that integrate with GitHub (Octotree, Copilot PR features, GitHub Actions extension).
- `gh cli` background credential refresh.
- GitHub Desktop or any other local app polling.

**Diet plan (GraphQL leverage list, ordered by ease × payoff):**

1. **Leverage 0 (free, immediate):** raise `poll_interval_sec` from 60 → 180 in production for low-activity periods. Trade-off: 3× longer detection latency for state transitions. Acceptable since daemon is overnight worker, not interactive. Already deferred in earlier OBS-Y discussion; reaffirmed here.
2. **Leverage 1.5 (PR-234, merged 2026-04-28):** drop `refresh=True` from IDLE merged_prs fetch. Cache absorbs cycles. ~10-15% GraphQL reduction during IDLE periods. Already shipped.
3. **Leverage 2 (small, ~50 lines) — proposed PR-237:** replace `gh pr list --json statusCheckRollup` with REST `GET /repos/{owner}/{repo}/commits/{sha}/check-runs` + `GET /repos/{owner}/{repo}/commits/{sha}/status`. REST is core quota (5000/hr) which has been consistently underused (4900+ remaining at the time of GraphQL exhaustion). Eliminates the dominant GraphQL consumer in WATCH/MERGE polling paths.
4. **Leverage 3 (medium, ~150 lines) — proposed PR-238:** add ETag conditional requests to all GitHub REST calls. `If-None-Match` returns 304 when nothing changed and **does not count against rate limit**. Most polling cycles return identical data; this is essentially free for the common case.
5. **Leverage 4 (medium) — proposed PR-239:** adaptive polling per state. IDLE without PR → 300s. CODING/FIX → 60s. WATCH/MERGE → 30s. Cuts IDLE burn dramatically for the common case where daemon is between tasks.
6. **Sprint-scale — proposed PR-240:** **migrate from personal access token to GitHub App authentication.** Each App installation gets its own 5000/hr quota independent of personal token. Solves the shared-quota problem at the root. Allows daemon, Codex Connector (already an App), and CI to operate on independent budgets.

**GitHub App migration plan (architectural):**

Three paths considered:

**Path X — Centralized API server:** single Anthropic-hosted (or self-hosted) App receives all installations, daemon authenticates via this server. Rejected: GDPR concerns, infrastructure burden, single point of failure, does not match self-hosted posture.

**Path Y — Manifest Flow (selected):** each user creates own App via predefined GitHub App manifest. Daemon ships with a manifest URL the user clicks to provision their own App. Each user's App gets its own 5000/hr quota. App's private key stored locally on user's server. This matches the project's self-hosted positioning and has zero centralized infrastructure. Proposed PR-241 covers manifest flow + onboarding doc + automated key handling. Defer until first external user.

**Path Z — BYO PAT (current state):** user provides personal access token. Acceptable for solo use today; not scalable to third-party adoption.

**Status (2026-04-28):**
- New GitHub App `alexbomber-pipeline-orchestrator` created in account settings (App ID generated; private key not yet downloaded; permissions correct: Contents R+W, Issues R+W, Metadata R, Pull requests R+W; Repository scope "Only on this account" sufficient for personal use).
- Existing App `pipeline-orchestrator-testbed-ci` (App ID 3502150) kept for CI testbed only.
- Pending: generate private key, store at `/etc/pipeline-orchestrator/private-key.pem` (chmod 600) on AI-Server, install App on personal repos (knowledge-vault, LAN_Transcriber, AWA-App, pipeline-orchestrator), add `*.pem` to `.gitignore`.
- Pending (sprint-scale): refactor daemon auth from `gh auth login` PAT path to App-installation-token path. Estimated 1-2 days work for daemon code; existing `gh` CLI calls work transparently with App tokens once env is configured.

**Action items (Round 4 candidates):**

- **PR-237 (proposed):** Leverage 2 — REST `check-runs`/`status` replacement for `statusCheckRollup`. Highest payoff for least code.
- **PR-238 (proposed):** Leverage 3 — ETag conditional requests across `github_client.py`. Medium effort, high payoff.
- **PR-239 (proposed):** Leverage 4 — adaptive polling per state. Configuration-only with state-aware multiplier in `runner.py` poll loop.
- **PR-240 (proposed):** GitHub App auth refactor for daemon. Sprint-scale; do after the smaller leverages prove insufficient OR when third-party adoption becomes relevant.
- **PR-241 (proposed):** Manifest flow for third-party adoption — predefined App manifest URL, onboarding doc, automated key handling. Defer until first external user.

**Lesson recorded:** GraphQL quota is the binding constraint for an autonomous daemon that uses GitHub heavily, especially when the operator simultaneously runs IDE extensions and Codex Connector against the same token. Visibility into who-burns-what is essential; the IDE extension's contribution was invisible until removal made the difference observable. Recommend periodic `gh api rate_limit` checks during heavy debug sessions and instrumenting daemon to log GraphQL points consumed per cycle.

**Lesson recorded (operator hygiene):** during long debug sessions, disable passive GitHub-polling extensions in the IDE (VS Code GitHub Pull Requests, GitHub Desktop, browser PR tabs left open). Each contributes silently and compounds at peak debugging when CI re-runs are most needed.


---

### OBS-AD: PR-180 self-healing convergence pattern (recorded 2026-04-29, autonomous merge confirmed)

**Observed:** PR-180 (REST replacement for `gh pr list --json statusCheckRollup`) was merged to main at 14:25 UTC after autonomous convergence. Timeline:

- 10:31 — first CI run, integration job FAILED (timeout waiting for IDLE state, last seen WATCH).
- 10:50, 11:07, 11:39, 11:58, 12:30, 12:52, 13:13 — seven additional FAILED runs, same symptom.
- Between 13:13 and 14:09 daemon committed 4 fix commits via FIX iterations:
  1. `5f1ced0` PR-180: trust combined commit-status state, ignore stale history
  2. `22c82a1` PR-180: map fetch failure to PENDING, not FAILURE, to avoid FIX storm
  3. `0b23c45` PR-180: treat partial REST fetch failures with empty signal as PENDING
  4. `e6c42c4` PR-180: trust empty surviving signal on partial REST fetch failure
- 14:09 — first GREEN CI run.
- 14:17 — second GREEN CI run (daemon verification rerun before merge).
- 14:25 — daemon auto-merged the PR.

**Each fix commit addressed a real edge case** in the new REST mapping function — not a cosmetic patch. Specifically the four edge cases the PR-180 task spec mentioned generically as "edge cases (empty rollup, mixed statuses, в WATCH STALLED case)" but did NOT enumerate explicitly:

1. **Stale check-run history interpretation:** REST `/check-runs` returns history including expired/stale runs. Naive mapping interpreted stale FAILURE entries as current state. Fix: trust the combined commit-status state, ignore historical entries when current status is available.
2. **Partial REST fetch failure on FIX storm:** when one of two REST endpoints (check-runs or status) failed transiently, naive mapping returned FAILURE. This triggered FIX iterations which made more REST calls which had higher failure rate. Self-amplifying loop. Fix: map partial failure to PENDING, not FAILURE.
3. **Empty signal on combined-fetch partial failure:** edge case where one endpoint returns empty (no checks) and the other fails. Naive mapping treated empty + failure as FAILURE. Fix: empty + failure = PENDING.
4. **Trust empty surviving signal:** if check-runs returned empty AND status returned partial-failure, the surviving "no checks" signal should be trusted (likely a fresh PR with no CI yet) rather than escalated as failure.

**Validates:**

The autonomous loop converges on real bugs, not just cosmetic patches. Daemon used FIX iteration as designed: each cycle interpreted CI failure → asked Claude to fix → committed → waited for next CI signal. Convergence on a 4-step edge-case staircase is exactly the use case the system was built for.

**Cost paid:**

- ~3.9 hours wall-clock (10:31 → 14:25).
- 8 integration job runs (each ~10 minutes) = ~80 minutes CI runner time.
- 4 Claude FIX iterations on Claude Pro quota.
- ~30 minutes operator time (interrupted twice to triage what was happening).

**Lessons recorded:**

1. **Initial assessment was wrong.** The operator and assistant initially read the 8 fails + 2 greens as "flaky test" pattern from OBS-AB sigkill multi-race, where lucky timing determined results. The truth was different — every fail was deterministic on the same root cause (REST mapping edge cases) and the greens came after real fixes. Confirming via `git log --since/--until` on the failure window resolved the question definitively. **Future debugging should always check git log on the affected branch before declaring "flaky" — fix commits in the failure window mean the failures were deterministic.**

2. **Edge cases in task spec must be enumerated explicitly with test fixtures** if we want to short-circuit discovery cost. The PR-180 spec mentioned "edge cases (empty rollup, mixed statuses, в WATCH STALLED case)" generically but did not provide concrete fixtures. Daemon discovered them through trial, costing 4 FIX iterations. For future REST/API replacement PRs, the task spec should include explicit fixture data for each edge case identified during the original API analysis. This converts a 4-cycle discovery into a 1-cycle implementation.

3. **Autonomous merge worked correctly.** Daemon waited for 2 consecutive green CI runs (14:09 + 14:17) before merging at 14:25. This matches the N≥2 verification policy the operator and assistant established after OBS-AB sigkill resolution. The merge gate is functioning as designed.

4. **GraphQL/CI quota cost of FIX iterations is real.** Each FIX cycle re-runs the full CI integration job (10 minutes) and consumes Claude tokens. PR-180's own purpose (reduce GraphQL burn) was paid for during convergence. Net win still positive — PR-180 in steady state saves much more than it cost during convergence — but worth recording the meta-cost so we know the price of underspecified task files.

5. **The "quick merge despite flaky CI" advice would have been wrong here.** Assistant initially recommended "merge anyway, it is flaky" based on incomplete evidence. If operator had merged at run #9 (14:09 green), the merge would have succeeded but bypassed the verification rerun — which would have been okay in this specific case but would be wrong policy. Operator did the right thing waiting for daemon's second green confirmation.

**Action items:**

- For PR-191a/PR-191b (ETag — also REST/API surface) and PR-202 (WATCH adaptive polling — depends on PR-180): ensure task specs include explicit edge case fixtures derived from PR-180 lessons (stale history, partial fetch failure, empty signal interpretation).
- No production fix needed; PR-180 is merged in correct final form.


### OBS-AE: Coder opens PR for wrong task (observed 2026-04-29 evening)

**Observed:** daemon picked task PR-182 (diagnose_error infra bypass) at 18:42. Coder ran, but the resulting PR was opened as **PR-183 (Redis pub/sub upload trigger)** on branch `pr-183-redis-pubsub-upload-trigger` as GitHub PR #248. Daemon correctly classified this as failure (diagnose_error: FIX → IDLE twice) and re-picked PR-182 at 19:05, on the second attempt coder opened the correct PR #249 on `pr-182-diagnose-error-infra-bypass` branch.

**Root cause hypothesis:** coder has freedom to interpret which task to work on rather than receiving the exact task file path as a non-negotiable instruction. Several possible failure modes:

1. Coder reads `QUEUE.md` itself and picks the next task by its own logic, not respecting daemon's selection.
2. Coder receives task file path but ignores it under certain conditions (multiple TODO entries near top of QUEUE confuse it).
3. Coder pattern-matches task content and decides to do "what looks easier" first.

This is the same class of problem as Sprint F2.1 SoT (Source of Truth direct instructions) which is currently NOT STARTED. The current path lets coder participate in task selection; the fix is to make daemon authoritative and coder mechanical.

**Side effect:** PR #248 became an orphan — open on GitHub, but daemon does not track it in state, no task file points to it, no FIX iterations happen on it. Codex did one review on it (COMMENTED) and nothing else moves forward. Manual operator action required to close or reassign.

**Related observation:** at the same time as this happened, `tasks/PR-183.md` is NOT present on production server (`cat: tasks/PR-183.md: No such file or directory`). The task file may have been lost during a previous upload (transient git error during zip extraction, partial commit). This means even when coder eventually picks PR-183 by its own logic, it does not have a task spec to work from. **Two failures stacked:** lost task file AND coder pick-without-instruction freedom.

**Action items proposed:**

- **PR-205 (proposed):** Mandatory task_file injection into coder prompt. Daemon constructs the coder invocation with explicit `--task-file=<path>` argument. AGENTS.md adds a hard rule: "Coder MUST work only on the task at the given path. If the path is missing or unreadable, ESCALATE — never pick another task." This is a Sprint F2.1 building block — minimal version that closes the immediate hole without requiring the full SoT refactor.
- **PR-206 (proposed):** Upload integrity verification — after upload commit, daemon verifies all listed task files in QUEUE.md exist on disk. If any missing, log error and surface to operator before daemon picks any next task. Closes the lost-task-file failure mode.

**Lessons recorded:**

1. **Coder freedom to interpret task selection is a critical bug surface.** Even though one bad outcome happened in 250+ PRs (low frequency), the consequence is significant — orphan PR, wasted Codex review cycle, operator confusion when investigating. Defense in depth needed: both task spec injection (PR-205 above) and post-upload verification (PR-206 above).
2. **Symptom looked like daemon abandoned a task mid-flight, root cause was different.** Initial hypothesis was that daemon manually serialized only one PR at a time and lost track of the second. Reading logs carefully showed the actual sequence: coder created wrong PR, daemon recovered correctly, picked task again, second attempt succeeded. **Lesson: read the logs around the suspected event window before forming hypothesis from current state alone.**
3. **Lost task file went undetected for hours.** Operator only noticed because they were debugging a different issue. Add proactive integrity check at upload time so this surfaces immediately, not via second-order observation.


## Deferred / Round 4

- **Sprint 10 direct task injection.** Daemon передаёт pr_id + task_file path + task body в prompt coder'у напрямую. Снижен в приоритете: Wave 2 прошёл без AGENTS.md fixes вообще, значит текущая indirection работает приемлемо. Вернуть в Tier 1 только если incident повторится несколько раз.
- **Heartbeats как status widget не event.** Отдельное поле на state + новый UI widget. Medium-high complexity.
- **Thompson Sampling selector.** Из sprint 11 плана. Beta posteriors (Codex Beta(81,19), Claude Beta(76,24)), cost-aware reward. Blocker для real agent+model routing quality — как только Wave 4-5 закончатся, это первый приоритет Round 4.
  **Must include `force_coder` override (Day 4 finding FINDING-2)** для fault testing и manual pinning. Без этого невозможно test Codex-unavailable / Claude-unavailable scenarios — selector всегда routes на working coder.

---

## Round 5+ exploration

- **Local agent integration (Aider + Ollama/vLLM based).** Третий coder plugin который wrap'ит Aider запущенный против local model endpoint. Allows:
  - Cost reduction (free inference на owned hardware типа DGX Spark)
  - Privacy (code не leaves infrastructure)
  - Capability addition (some local models strong на specific tasks — Minimax на long context, DeepSeek на Python, Qwen Coder на refactoring)
  - Offline development
  
  Implementation path: OssAiderPlugin conforming к CoderPlugin protocol. Config: `aider_endpoint_url`, `aider_model_name`, `aider_binary_path`. Behavior: spawns `aider --model {model} --openai-api-base {url} --yes-always --message "PLANNED PR"` как subprocess. Usage provider returns $0 cost (local inference). Rate limit patterns основаны на Ollama/vLLM error messages.
  
  **Prerequisite:** benchmark Qwen2.5-Coder-32B-Instruct Q5_K_M на DGX Spark с Aider standalone прежде чем integration. Критерии: single PLANNED PR completion в <10 минут на typical pipeline-orchestrator task, edit application accuracy >80%, tests pass на first try ≥70%. Если Qwen не прошёл — fallback на DeepSeek-Coder-V2-Lite-16B. Если и DeepSeek не прошёл — защитить что local coding недостижимо на текущем hardware tier'е, revisit через 6 месяцев когда появятся better models или hardware.
  
  **Dependency:** Thompson Sampling (Round 4) — без adaptive selector local agent будет picked когда не надо, ломая throughput. С селектором он станет "bottom tier fallback для low-priority tasks" automatically.

- **Additional agent plugins beyond Aider.** goose (Block), OpenHands (CLI mode), Cline (if CLI wrapper становится доступен). Каждый plugin минимальный (CoderPlugin protocol), routing accumulates data per agent+model, система сама learns который agent+model pair best per task type.

- **Task-type taxonomy.** Router currently routes по repo. Более granular — по task_type (bugfix / feature / refactor / ux / config / docs / architecture) поскольку agent+model strengths различаются по типу работы. Требует: task_type как routing dimension в Thompson Sampling posteriors, per-agent per-task-type Beta distribution.

---

---

## Известные риски и осторожности

### Sprint F2.2 (PAUSED state model refactor) — самый высокий риск
4 sequential PR (PR-170/171/172/173). Migration script для existing Redis states требуется. Backward compatibility window для production restart. Не делать одновременно с другими Sprint. Минимум один 1-day отдельный sprint с only this work.

### PR-180 (REST replacement) — средний риск
`statusCheckRollup` GraphQL response shape отличается от REST `check-runs` + `status`. Mapping function требует тщательного тестирования edge cases (empty rollup, mixed statuses, в WATCH STALLED case). Нужны unit tests + integration test перебор.

### PR-183/PR-184 (pub/sub + adaptive polling) — средний риск
Race conditions: upload event arrives while daemon mid-cycle; multiple uploads quick succession (dedup); upload during rate limit pause; wake during stop cancellation (graceful). Тесты должны cover все четыре scenario.

### PR-192 (existing AGENTS.md reconciliation) — низкий риск
Section-marker append pattern conservative. Возможен edge case: user manually deletes daemon-managed section, daemon re-adds на next cycle, looks like nagging. Mitigation: log warning, не silent re-add.

### Multi-repo onboarding (overall)
GraphQL quota distribution across repos критична. Без PR-180 + PR-191 second repo может exhaust budget within hours. Defer onboarding until critical batch shipped.

---

## Process notes (как работать с этим документом)

### Как обновлять roadmap

После каждой merged волны или significant chat-session:
1. Update "Последнее обновление" header.
2. Move just-shipped PRs из "Implementation Plan" в "Implementation Audit summary" с DONE status.
3. Add new observations к "Active investigations" если есть.
4. If sprint shifted in priority — move между "Critical / Important / Multi-repo / Polish / Deferred" batches.
5. Don not introduce new PR numbering без проверки on conflicts с tasks/PR-XXX.md actual files.

### Numbering discipline

- **PR-001..PR-179:** completed work. Frozen numbering.
- **PR-180..PR-199:** active backlog batches от 2026-04-29 audit (Critical / Important / Multi-repo / Polish).
- **PR-200..PR-204:** task-validation synonyms + dashboard UI consistency + WATCH adaptive polling + compact resource limits row + outcome logging (added 2026-04-29 evening).
- **PR-204+:** future work — sprint-scale items deferred (GitHub App migration, Thompson Sampling, PAUSED removal, manifest flow, resource limit history charts pending storage decision, JSONL → SQLite analytics migration when scale demands).

Verify free numbers перед creating new task files: `ls tasks/PR-XXX.md` + `grep PR-XXX docs/roadmap.md`.

### Daemon vs deploy-time tasks/

Daemon работает из `/data/repos/<slug>/tasks/` (docker volume). Deploy-time `~/pipeline-orchestrator/tasks/` может отличаться. Ground truth для queue computation = daemon volume. Не conflate the two when investigating queue discrepancies.

---

## Открытые вопросы ждущие решения

### Next session priorities (after 2026-04-29 audit)

1. **Start Critical batch (PR-180..PR-185).** Я генерирую 6 task files в одном блоке когда скажешь.
2. **OBS-16 ENV-TOKEN verification.** Manual run `docker compose up -d --force-recreate daemon`, check `GITHUB_TOKEN` visibility. Не PR, operational task.
3. **OBS-Y status verification.** "Daemon merges PR before formal APPROVED state" — нужно current behavior проверить и закрыть либо как DONE либо как actionable PR.

### Still open architectural decisions

- **GitHub App migration timing.** Wait для GraphQL diet effectiveness data (after PR-180/PR-191) или start App migration параллельно? Risk: App migration sprint-scale, может block other work.
- **Production config discipline.** PR-194 outline `config.production.yml` overlay, но возможны другие подходы (env vars only, secrets in vault). Decision needed before PR-194 task file generated.
- **Multi-tier agent (Tier 2).** Roadmap section ниже describes architectural direction. Когда start? After all critical+important+multi-repo PRs ship? Or earlier as separate exploration sprint?

### Resolved decisions (2026-04-28..29)

- **Sigkill multi-race resolution path** -> 4 PRs merged (228, 232, 234, 236). Done.
- **Roadmap rewrite** -> executed 2026-04-29 на основе audit. This document.
- **PR numbering for new backlog** -> continues from PR-180 (next free после PR-179 DONE). Legacy roadmap reservations dropped — task files are source of truth, not roadmap-side numbering.
