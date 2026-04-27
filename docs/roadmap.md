# Pipeline Orchestrator Roadmap

Живой документ. Обновляется после каждой merge'нутой волны и после каждой chat-session.

Последнее обновление: 2026-04-27 (after Stage 1 foundation merging in progress; Multi-tier agent direction added; OBS-Y premature merge investigation logged).

Предыдущие: 2026-04-26 (after Sprint F1.0 + PR-156/157 + PR-158/159 merged; Variant D direction confirmed; Development model & Layer 2 substrate observations added), 2026-04-24 (after code audit zip __27__, corrections applied), Day 5 closure, Day 4 auto, 2026-04-21 after PR-145..PR-150.

---

## Текущий статус

- **Testing-week CLOSED** (Days 1-5 complete). Day 5 auto-run выполнен, week summary ниже.
- **Code audit completed 2026-04-24** — verified каждый запланированный PR против actual code. Corrections applied inline. Key findings below.
- **Real product bugs found by testing-week:** 1 confirmed (FINDING-2 coder pin → PR-194). Hypothesized OBS-13 retracted — pack bug (OBS-14), не product.
- **Production validations earned:** PR-190a counter-based retry (natural experiment Day 4 + EXT-01 Day 5), DATA-01 SIGKILL queue integrity, DATA-02 upload during Redis down, EXT-04 Redis down + recover end-to-end.
- **Positive behaviors confirmed:** OBS-7 dirty tree auto-recovery, OBS-11 UI graceful degrade on Redis down, OBS-12 WATCH STALLED state.
- **Testing packs** (`tests-manual/auto/day4/`, `day5/`) существуют локально как uncommitted в working tree. Не в main. Decision: не commit'ить, подход C — write production e2e layer from scratch в Sprint F1.0, pack использовать как reference.

### Code audit corrections (2026-04-24)

Применены при аудите zip `__27__.zip`:

- **PR-162 REMOVED.** Auto-injection sequential deps НЕ существует в `src/dag.py`. DAG корректно использует `header.depends_on` напрямую. PR-146 имеет `Depends on: none` correct. Мое memory про "PR-146 получил auto-depends" было ошибочным.
- **PR-159 + PR-160 + PR-181 CONSOLIDATED.** После PR-181 Option B recovery не может читать через `git show origin/main:tasks/QUEUE.md` (путь удалён). Эти три PR должны быть one coordinated refactor или tight 3-PR sequence — иначе recovery ломается.
- **Wave 5 scope REDUCED.** `rate_limited_until` уже существует в `src/models.py:103`. PR-163 нужно добавить только `awaiting_start` (rate_limited_until готов). Migration proof easier.
- **Memory items VERIFIED done в коде:** upload zip support, QUEUE.md не required в upload, Play/Pause/Stop per repo, post-pre-merge-sync @codex re-review, Codex draft PR ready conversion. Nothing to do. Memory update needed.
- **New task added:** PR-198 Task content viewer в repo detail page (из memory wishlist).

### Active TODO в tasks/

- **PR-151** Play/Pause/Stop buttons size responsive (Wave 2 tail, TODO)
- **PR-152** Remove nonfunctional Start badge (Wave 2 tail, TODO)

PR-153 первый свободный номер для Round 3 Wave 3.

- **Next session priorities** (см. конец документа): OBS-16 ENV-TOKEN verification, PR-194 task file первым, затем Sprint F1.0 e2e infrastructure.

---

## Новое с Day 4 (2026-04-24)

### Day 5 corrections to Day 4 findings

**OBS-13 RETRACTED.** Prior claim "event history has window/dedup evicting lifecycle events after 3 min" falsified: Day 5 EVENT-LOG-01 long 3min PASS, 48 events persisted в UI including Pause/Stop/Resume. OBS-13 был caused by test reading wrong data source. Not a product issue.

**FINDING-1 reclassified as pack bug (OBS-14).** Root cause: test reads `state.history` (state transitions like IDLE→CODING), не `/api/events/{slug}` endpoint (user-action events). Events есть в UI, invisible для test. Fix — pack-only (PR-196). Product side correct.

**OBS-15 (new pack bug).** Cross-test contamination: `reset_testbed_clean` closes open PRs но не waits daemon state → IDLE. Next test picks up mid-work state. Example chain: DATA-01 leaves PR-301 → EXT-04 enters ERROR вместо IDLE recovery → EXT-01 no_pr times out. Fix — pack-only (PR-195).

**OBS-16 (open, needs verification).** ENV-TOKEN-01 SKIPPED на Day 5 despite user confirming `.env` updated. Hypothesis: `docker compose up -d daemon` без `--force-recreate` не reloads `.env`. User verification steps:
```
docker compose up -d --force-recreate daemon
docker compose exec -T daemon printenv GITHUB_TOKEN | head -c 20
```
Если token visible → re-run ENV-TOKEN-01 manually для OBS-5 verification. Если не visible → separate compose investigation нужен.

### Real product findings (after Day 5 clarifications)

**FINDING-1: RESOLVED as pack bug OBS-14 (see Day 5 corrections above).** Product emits events correctly, test was reading wrong source.

**FINDING-2: Coder pin silently ignored (Medium severity, CONFIRMED Day 5).**
Task file с `coder: codex` → silently routed to Claude when codex unavailable. User intent ignored.

Fix (PR-194, see Wave 4): if `task_coder_pin in ("codex", "claude")` и coder unavailable → HUNG с message, не fall back на any. Day 5 EXT-02 already covers this (FAIL) — fixing product code сделает PASS.

Это подтверждает необходимость `force_coder` override в Thompson Sampling selector — Day 4 signal был right.

### Production validations (behaviors earned empirically)

- **PR-190a (bounded coder retry counter)** validated in production natural experiment: in-memory counter incremented 1→2 → HUNG with "Task PR-160 blocked: coder failed to create PR 2 times in a row. Manual intervention required." (OBS-1)
- **DATA-01 SIGKILL preserves queue integrity**: daemon killed mid-work, on restart → auto-recovery → picked different next task без stuck'а (OBS-7 related)
- **DATA-02 upload during Redis down**: manifest persistence layer doesn't require Redis для write path. Post-recovery task появился в queue correctly.
- **EXT-04 Redis down + recover end-to-end**: detected ~1 cycle, UI shows PREFLIGHT+banner, daemon paused, recovery → normal idle polling.

### New positive behaviors (undocumented, worth noting)

**OBS-7: Auto-recovery from dirty working tree (NEW pattern).**
Daemon detects dirty tree в preflight, logs 3 ERROR cycles, затем `"Dirty tree persisted 3 cycles, auto-resetting to recover"` → auto-reset → IDLE.

Это bounded retry pattern аналогичный PR-190a (counter-based), applied на different failure mode. **Implication:** roadmap items assuming "dirty tree requires manual fix" may be obsolete. Нужно pass through Round 3 Wave 4 задачи и check если обе pattern'ы можно unify.

**OBS-11: UI graceful degrade when Redis unavailable.**
Banner "Redis unavailable — state unknown" + state badge → PREFLIGHT. Page не crashes, rest of UI navigable. Web layer имеет defensive state-reading.

**OBS-12: WATCH STALLED substate visible in UI.**
`WATCH STALLED` compound state с full PR panel (CI=success, Review=👀). Undocumented в roadmap до сих пор.

Action: документировать STALLED semantics в architecture docs. Confirm intentional или bug. Приоритет low — behavior polezno, но нужна ясность.

### Production issues (real problems found)

**OBS-2: QUEUE.md regen coupled только к merge handler.**
`git rm tasks/PR-160.md` + commit + push to main не триггерит QUEUE.md regeneration. Direct file manipulation на main bypasses source-of-truth sync. Daemon keeps picking up "ghost" tasks.

Два варианта fix:
- A: QUEUE.md regen trigger на любой push где tasks/ changed (polling или webhook)
- B: Remove QUEUE.md committed file, regenerate purely in-memory из tasks/ listing каждый cycle

Я предлагаю **B как long-term решение** — устраняет весь class drift'а. А как hotfix если B слишком invasive.

New candidate PR записан в Wave 6 ниже.

**OBS-4: diagnose_error CLI fails на git infra errors.**
После `"ensure_repo_cloned failed: git fetch origin main failed after 3 attempts"` daemon invoke'ает Claude CLI 3 раза для diagnose — все exit 1 → max_attempts → staying ERROR.

Root cause hypothesis: diagnose_error prompt не designed для network-class failures. CLI не может produce actionable diagnosis.

Fix: route git-class/infra errors straight to operator alert bypassing diagnose_error. Или extend prompt на recognize/pass-through infra errors.

**OBS-5: Intermittent git fetch exit 128 via gh credential helper.**
~3-4 раза в час в idle. Environment verified (gh v2.90.0, hosts.yml proper, auth token valid, network OK, manual git fetch always succeeds). Hypotheses: concurrent gh spawn contention на hosts.yml, secondary rate limit на credential helper, timeout race.

Action: instrument credential helper path (strace или wrapper logging). Possibly add stable `$GITHUB_TOKEN` fallback в .env as secondary auth. Candidate PR записан в Wave 6.

### Pack infrastructure issues (non-product, testing pack)

OBS-6, OBS-8, OBS-9, OBS-10 — все про test pack (regex compose v2, screenshot embedding, shim user permissions, React hydration race). Day 5 pack improvements responsibility, не product roadmap.

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

## Implementation Plan (phased)

**Принцип:** finish before extend. Finished product сначала, развитие потом. "Работающий продукт который нравится" перед любыми new features (local agents, Tester, etc).

### Sprint F1.0 — Playwright e2e infrastructure (BLOCKER, делать первым)

**Нельзя писать последующие PR без integration test gate.** Все PR после F1.0 должны pass e2e перед merge.

- **PR-153** Integration test infrastructure setup (pytest-playwright, tests/e2e/, docker-compose.test.yml, conftest fixtures, scripts/ci.sh обновить на two-tier)
- **PR-154** Core e2e tests covering validated behaviors (upload flow, stop/resume, Redis down, SIGKILL preservation)
- **PR-155** Local dev workflow + docs (scripts/test-e2e.sh wrapper, docs/local-e2e.md)

**AGENTS.md update** (одним из PR F1.0): `scripts/ci.sh` must exit 0 before PR opened — covers both fast tier и integration tier. Without this сoder может open PR который fails integration.

**Exit criteria F1.0:** e2e suite running локально в <15 минут, CI включает integration tier, first trivial test passes.

### Sprint F1.1 — Immediate unblockers

- **PR-156** Coder pin respected (FINDING-2). Critical: user expects `coder: codex` works.
- Wait for Wave 2 tail (PR-151, PR-152 merge auto через daemon)
- OBS-16 verify (manual, не PR — run force-recreate daemon, check GITHUB_TOKEN)

### Sprint F1.2 — QUEUE.md как presentation layer (consolidated)

**Single coordinated refactor.** PR-159/160/181 объединены потому что они tightly coupled:

- **PR-157** QUEUE.md presentation layer + recovery rewrite + DOING current_task. Combines:
  - Remove tasks/QUEUE.md from git tracking (add to .gitignore)
  - Change recovery to list tasks/ directly (не git show QUEUE.md)
  - derive_task_status accepts current_task_pr_id — DOING даже без open PR
  - Generate QUEUE.md purely in memory per IDLE cycle для UI rendering
  - Migration path: existing committed QUEUE.md deprecated, removed in single commit
- **PR-158** diagnose_error bypass для infra errors (OBS-4 — git fetch, credential, network не через CLI diagnose)

**Risk:** PR-157 large scope. Разбить на 2-3 под-PR через feature flag если task file слишком large после drafting.

### Sprint F1.3 — Reliability correctness

- **PR-159** Asymmetric push verification в fix.py normal path (OBS, symmetry with stop-cancel path)
- **PR-160** Unify bounded-retry recovery patterns (OBS-7 + PR-190a common policy refactor)

### Sprint F1.4 — UX immediate batch

Все Wave 3 UX одним batch'ом в одной session'e task files:

- **PR-161** Remove STALLED indicator entirely
- **PR-162** Spinners на всех HTMX actions >100ms
- **PR-163** Event log single-row dedup display (Lucide rotate-ccw + count)
- **PR-164** Fuzzy dedup в log_event (regex numeric counter parts)
- **PR-165** Light-theme dropdown fix (color-scheme dark)
- **PR-166** HTMX 400 whitelist

**Phase 1 exit criteria:** все active bugs ежедневной работы устранены, daemon deterministic, UI не раздражает, e2e gate guards все changes.

### Sprint F2.1 — Sprint 10 SoT refactor

Daemon передаёт coder'у direct instruction с pr_id + task_file body. AGENTS.md документирует только manual modes.

Разбито на 3 sequential PR с feature flag:

- **PR-167** Sprint 10a: add direct instruction code path параллельно existing, `PO_DIRECT_INSTRUCTIONS` feature flag default false
- **PR-168** Sprint 10b: switch daemon на new path по default, legacy через flag
- **PR-169** Sprint 10c: remove legacy path, update AGENTS.md (remove "Use the active entry" rules, keep manual sections)

### Sprint F2.2 — State model refactor (Wave 5)

**Prerequisite check перед началом:** Sprint 10 полностью merged, Sprint F1 stable. Нельзя делать одновременно.

Scope reduced после code audit: `rate_limited_until` уже существует в models.py.

- **PR-170** Add awaiting_start field to RepoState. rate_limited_until уже есть.
- **PR-171** Rewrite rate_limit.py + handle_paused на новые flags
- **PR-172** Remove PAUSED из handler error paths (27 usages → replace on `awaiting_start=True; state=IDLE`)
- **PR-173** Remove PAUSED из enum + UI rewrite + migration script для existing Redis states

**Phase 2 exit criteria:** coder gets direct instructions, state model clean, no PAUSED overloading.

### Sprint F3.1 — Settings comprehensive

- **PR-174** Per-repo coder в Settings table + remove dropdown из repo detail (read-only display, "Auto-Select" label, remove Inherit)
- **PR-175** Configurable review_timeout_min per-repo UI
- **PR-176** AGENTS.md explicit failure signalling (manual commit, quick win)
- **PR-177** AGENTS.md bounded reading scope (manual commit)

### Sprint F3.2 — Selector + measurement (Sprint 11)

**Prerequisite:** PR-156 merged (coder pin works).

- **PR-178** Sprint 11a: Selector Protocol abstraction, existing ε-greedy wrapped
- **PR-179** Sprint 11b: Thompson Sampling implementation, Beta posteriors (Codex Beta(81,19), Claude Beta(76,24))
- **PR-180** Sprint 11c: cost-aware reward function, tokens_cost tracking в RunRecord
- **PR-181** Sprint 11d: force_coder override для fault testing + manual pinning

### Sprint F3.3 — UI polish + tasks viewer

- **PR-182** Pulse animation jump fix (CSS transition вместо keyframes)
- **PR-183** Pulse badge вместо pulse dot
- **PR-184** Event text clarity pass (audit log_event calls)
- **PR-185** Immediate upload pickup via Redis pub/sub (30-45s → 1-2s latency)
- **PR-186** Task content viewer в repo detail page (из memory wishlist)

**Phase 3 exit criteria:** product feature-complete. Settings comprehensive. Selector learning. UI feels responsive.

### Sprint F4.1 — Failure modes comprehensive

- **PR-187** Recovery skip crashed-task-retry
- **PR-188** Coder exit=0 diagnostic handler (branch missing vs exists no PR vs no branch)
- **PR-189** Codex bot error comment detection (immediate @codex re-trigger)
- **PR-190** Surface external service errors в UI
- **PR-191** gh credential helper instrumentation (OBS-5 investigation)

### Sprint F4.2 — Testing infrastructure + cleanup

- **PR-192** Nightly e2e runner для pipeline-orchestrator self-testing
- **PR-193** Lock upload during QUEUE.md regeneration (может стать obsolete после PR-157, но keep as safety)
- **PR-194** Document WATCH STALLED substate
- **PR-195** Clean up PipelineState.MERGE dead value

**Phase 4 exit criteria:** все edge cases либо auto-recover, либо surface clearly. Self-testing running. Documentation caught up. Dead code removed.

---

## Нумерация: актуальное сопоставление

Старые планы в roadmap ссылаются на PR-153..PR-196. После code audit numбers сдвинулись. **Authoritative mapping (используй эту таблицу):**

| Старый | Новый | Что |
|--------|-------|-----|
| PR-194 | **PR-156** | Coder pin respected |
| PR-153 | **PR-161** | Remove STALLED |
| PR-154 | **PR-162** | Spinners |
| PR-155 | **PR-163** | Event log dedup display |
| PR-156 | **PR-164** | Fuzzy dedup log_event |
| PR-157 | **PR-165** | Light-theme dropdown |
| PR-158 | **PR-166** | HTMX 400 whitelist |
| PR-159 | merged in **PR-157** | DOING current_task |
| PR-160 | merged in **PR-157** | Recovery source of truth |
| PR-161 | **PR-159** | Asymmetric push verification |
| PR-162 | **REMOVED** | (auto-deps injection — не существует) |
| PR-163 | **PR-170** | awaiting_start field |
| PR-164 | **PR-171** | rewrite rate_limit |
| PR-165 | **PR-172** | Remove PAUSED из handlers |
| PR-166 | **PR-173** | Remove PAUSED из enum |
| PR-167 | **PR-174** | Per-repo coder Settings |
| PR-168 | **PR-177** | AGENTS.md bounded reading |
| PR-169 | **PR-176** | AGENTS.md failure signalling |
| PR-170 | **PR-182** | Pulse animation fix |
| PR-171 | **PR-183** | Pulse badge |
| PR-172 | **PR-184** | Event text clarity |
| PR-173 | **PR-187** | Recovery skip crashed-retry |
| PR-174 | **PR-188** | exit=0 diagnostic |
| PR-175 | **PR-189** | Codex bot error detection |
| PR-176 | **PR-190** | External service errors UI |
| PR-177 | **PR-175** | review_timeout_min UI |
| PR-178 | **PR-193** | Lock upload during regen |
| PR-179 | **PR-195** | Cleanup MERGE dead value |
| PR-180 | **PR-185** | Upload pickup pub/sub |
| PR-181 | merged in **PR-157** | QUEUE.md presentation |
| PR-182 | **PR-158** | diagnose_error bypass infra |
| PR-183 | **PR-191** | gh credential helper |
| PR-184 | **PR-160** | Unify bounded-retry |
| PR-185 | **PR-194** | Document STALLED |
| PR-186 | CANCELLED | (FINDING-1 was pack bug) |
| PR-193 | **PR-192** | Nightly e2e runner |
| PR-194 | **PR-156** | Coder pin (see above) |
| PR-195 | скипнут | (pack-only, не product) |
| PR-196 | скипнут | (pack-only, не product) |
| PR-198 | **PR-186** | Tasks content viewer |

Sprint 10 (SoT refactor) — **PR-167, PR-168, PR-169**.
Sprint 11 (Thompson Sampling) — **PR-178, PR-179, PR-180, PR-181**.

---

## Backlog Round 3 (DEPRECATED — заменён Implementation Plan выше)

Ниже original roadmap content оставлен для reference до следующего update. Authoritative source — Implementation Plan выше.

---

Нумерация PR сквозная. PR-151 и PR-152 заняты (TODO в Wave 2). Round 3 начинается с PR-153.

### Wave 2 остаток (Round 2, через daemon)

- **PR-151** Play/Pause/Stop buttons responsive subtle. TODO.
- **PR-152** Remove nonfunctional Start badge. TODO.

### Wave 3 (Round 3 Tier 1 UX — low risk, можно параллельно)

**PR-153. Remove STALLED indicator entirely**
Файлы: `src/web/templates/base.html` (JS + CSS), `components/repo_summary.html`, `components/repo_cards.html`.
Удалить: syncStalledBadges JS (base.html:496-520), CSS `.stalled` правила (base.html:357-365), hint spans (repo_summary:32, repo_cards:58).
Сохранить: data-* attributes (harmless без JS).
Размер: ~30 строк delete.
Tier 1. Low risk.

**PR-154. Spinners на все HTMX actions >100ms**
Файлы: HTMX templates где есть actions — `_controls.html`, upload form, settings form, coder selector.
Default `hx-indicator` + shared spinner partial.
Размер: medium (несколько templates).
Tier 1. Low risk.

**PR-155. Event log single-row dedup display**
Файлы: `src/web/templates/components/event_list.html`, `components/activity_feed.html`.
Single timestamp (last_seen), убрать inline `(xN)`, добавить в конец row span с Lucide rotate-ccw SVG + count если count>1, `title` attribute с first_seen.
Зависит от PR-150 alignment (done).
Размер: low (2 templates).
Tier 1. Low risk.

**PR-156. Fuzzy dedup в log_event для numeric counter parts**
Файлы: `src/daemon/runner.py:770-793`.
Regex pattern list `\b\d+/\d+m\b`, normalize перед compare, update event до latest text, increment count, update last_seen_at.
Tests в `tests/test_runner.py`.
Размер: ~10 строк + tests.
Tier 1. Low risk.

**PR-157. Light-theme dropdown fix**
Файлы: CSS в `base.html` или shared styles.
Добавить `color-scheme: dark` на select elements.
Размер: 1-2 строки.
Tier 1. Low risk.

**PR-158. HTMX 400 whitelist**
Файлы: `src/web/templates/base.html:16`.
Добавить status===400 в whitelist HTMX response handlers. Validation errors были silent в UI.
Размер: 1 строка.
Tier 1. Low risk. Был отложен из Round 2 — включаем сюда.

### Wave 4 (Round 3 Tier 1 reliability — medium risk, sequential)

**Priority additions from Day 4-5 observations** (promoted из higher-numbered Waves для execution order):
- **PR-181** (QUEUE.md as presentation layer, OBS-2 ghost tasks) — fundamental reliability fix
- **PR-194** (coder pin respected, FINDING-2 Day 5 confirmed) — user-facing contract violation, Tier 1

Обе должны быть done early в Wave 4 перед другими reliability items, или даже ahead of Sprint 10 SoT refactor.

**PR-159. QUEUE.md DOING reflects current_task**
Файлы: `src/task_status.py` (derive_task_status), `src/daemon/handlers/idle.py` (_write_generated_queue_md caller).
Pass `current_task_pr_id` в derive_task_status. Если header.pr_id == current_task_pr_id и не DONE → return DOING даже без open PR.
Single writer principle сохраняется — меняется только helper.
Tests: crash in CODING recovery scenarios.
Размер: ~30 строк + tests.
Tier 1. Medium risk. Depends on baseline stable.

**PR-160. Recovery source of truth from open PRs**
Файлы: `src/daemon/recovery.py`.
Первичный signal: `gh pr list --state open`. Match по branch name prefix с task files. Matched → WATCH. Иначе → IDLE + pick next TODO через get_next_task.
Tests: многочисленные edge cases recovery (crash в CODING, crash в FIX, user_paused during crash, open PR unrelated to current_task).
Размер: medium.
Tier 1. Medium risk. Depends on PR-159.

**PR-161. Asymmetric push verification в fix.py normal path**
Файлы: `src/daemon/handlers/fix.py:454`.
Добавить `remote_branch_contains_head()` check перед `record_fix_push()` в normal completion path.
Если local HEAD changed но remote не contains → state=ERROR с clear message, не incrementing push_count.
Wrap fetch в `retry_transient` чтобы избежать false positive на transient network.
Симметричный с stop-cancel path (fix.py:405-415).
Размер: ~20 строк + tests.
Tier 1. Medium risk.

**PR-162. QUEUE auto-regen respects explicit `Depends on: none`**
Файлы: `src/dag.py`.
Если task header имеет `Depends on: none` — не добавлять sequential dependencies автоматически.
Tests: task file с none не получает auto-deps.
Размер: low.
Tier 1. Medium risk.

### Wave 5 (Round 3 Tier 1 state model — high risk, разбит на 4 sequential)

Делать после Wave 4 когда baseline stable.

**PR-163. Add awaiting_start + rate_limited_until fields to RepoState**
Файлы: `src/models.py`.
Добавить 2 field, PAUSED пока живёт как transient dispatcher state. Migration в recovery: если state=PAUSED и user_paused=True → awaiting_start=True. Если state=PAUSED и rate_limited_until != None → оставить как есть.
Tier 1. High risk. 1 of 4.

**PR-164. Rewrite rate_limit.py + handle_paused на новые flags**
Файлы: `src/daemon/rate_limit.py`, `src/daemon/handlers/idle.py:642` (handle_paused).
Rate limit путь использует rate_limited_until flag. handle_paused остаётся как fallback для legacy PAUSED state в existing Redis.
Tier 1. High risk. 2 of 4.

**PR-165. Remove PAUSED из handler error paths**
Файлы: `src/daemon/handlers/fix.py`, `coding.py`, `merge.py`, `error.py`.
Заменить 15+ `state = PipelineState.PAUSED` на `awaiting_start = True; state = IDLE`.
Tier 1. High risk. 3 of 4.

**PR-166. Remove PAUSED из enum, UI rewrite, migration script**
Файлы: `src/models.py` (enum), `src/web/app.py`, templates где PAUSED badge показан.
Migration script: existing Redis states c state=PAUSED переносим на IDLE+flags.
UI: Play button visible if awaiting_start OR (IDLE AND queue_has_work).
Tier 1. High risk. 4 of 4.

### Wave 6 (Round 3 Tier 2 — по приоритету когда baseline стабилен)

**PR-167. Per-repo coder в Settings table + remove from repo detail**
Settings получает table всех repos с coder dropdown. Repo detail — read-only. Rename "Any" → "Auto-Select". Remove Inherit. Hot reload + confirmation.
Tier 2. Low-medium risk.

**PR-168. AGENTS.md bounded reading scope для PLANNED PR**
В `## PLANNED PR runbook` добавить блок `### Files to read`.
Whitelist: QUEUE.md, tasks/PR-*.md активной задачи, файлы в "Files to touch".
Blacklist: README.md, CLAUDE.md, other tasks/PR-*.md, unrelated src/.
Размер: ~20 строк.
Downgraded to Tier 2 — Wave 2 прошёл без этого.
Low risk. Manual commit или через daemon.

**PR-169. AGENTS.md explicit failure signalling**
В `## Daemon Mode` добавить блок `### Exit behavior`.
Exit 0 только если PR создан + @codex review posted. Иначе exit non-zero + stderr `UNABLE: <reason>`.
Размер: ~15 строк.
Downgraded to Tier 2.
Low risk.

**PR-170. Pulse animation jump fix**
CSS transition вместо keyframes reset при HTMX swap.
Tier 2. Low risk.

**PR-171. Pulse badge вместо pulse dot**
Badge заменяет точку в state indicator.
Tier 2. Low risk.

**PR-172. Event text clarity pass**
Переписать confusing messages (audit всех log_event). Осторожно — влияет на fuzzy dedup.
Tier 2. Low-medium risk.

**PR-173. Recovery skip crashed-task-retry**
Если recovery находит branch on remote без open PR — mark blocked, skip на next TODO.
Tier 2. Medium risk.

**PR-174. Coder exit=0 без push diagnostic handler**
coding.py: перед ERROR check `git ls-remote` + `git rev-list origin/{branch}..HEAD`. Различать "branch missing" vs "branch exists PR missing" vs "no branch at all".
Tier 2. Low-medium risk.

**PR-175. Codex bot error comment detection**
Watch handler polling comments от `chatgpt-codex-connector[bot]`. Pattern match `Something went wrong` → immediate re-trigger @codex review.
Tier 2. Low risk.

**PR-176. Surface external service errors in UI**
Event log entries для external service failures.
Tier 2. Low risk.

**PR-177. Configurable review_timeout_min per-repo UI**
UI control в Settings table рядом с coder selector.
Tier 2. Low risk.

**PR-178. Lock upload during QUEUE.md regeneration**
Upload endpoint returns 503 Retry-After=10 если `queue_regen_in_progress` Redis key set.
Tier 2. Low risk.

**PR-179. Clean up PipelineState.MERGE dead value**
handle_merge никогда не set'ит PipelineState.MERGE. Решить: либо восстановить usage в начале handle_merge, либо удалить из enum и _TRANSIENT_STATES.
Tier 3 cleanup.

**PR-180. Immediate upload pickup via Redis pub/sub**
Файлы: `src/daemon/main.py` (loop wake-up), `src/web/app.py:upload_tasks` (publish event), `src/events/__init__.py` (add channel type если нужен).
Web publishes `repo_upload_completed:{name}` через Redis после успешного git commit. Daemon's main loop использует `asyncio.wait` на combined `[sleep_tick_task, redis_subscriber_task]`. Earliest wins.
При wake: `last_run[key] = 0` for affected repo, continue to run_cycle immediately.
Scope расширить на другие user actions которые сейчас ждут next tick: Play button click, coder swap, settings change affecting active repo.
Tests: concurrent upload + ongoing cycle (race safety), multiple uploads в quick succession (dedup), upload during rate limit (respect rate_limited_until), wake during stop cancellation (graceful).
Размер: medium.
Latency improvement: upload → CODING pickup с 30-45s (half of poll_interval_sec default 60) до 1-2s.
Tier 2. Medium risk — async coordination между pub/sub and sleep loop требует careful testing. Рекомендованная wave: 6.

**PR-181. QUEUE.md as presentation layer (OBS-2)**
Файлы: `src/daemon/handlers/idle.py`, возможно `src/daemon/main.py`, `src/task_status.py`, all readers of `tasks/QUEUE.md`.

Два подхода:
- **A (minimal):** trigger regen в idle handler если `tasks/` changed since last QUEUE.md write. Detection: `git log --name-only` между `HEAD` и last-known-QUEUE-sha. Simple, всё ещё polling-based.
- **B (radical):** Remove QUEUE.md committed file entirely. Generate purely in-memory каждый cycle из `tasks/PR-*.md` listing + derive_task_status для каждого. QUEUE.md перестаёт быть source of truth — становится presentation layer only.

**Recommended: B.** Устраняет entire class drift'а. QUEUE.md commit spam в main history исчезает. Direct file manipulation на main автоматически consistent.

**Risk B:** значительное изменение семантики. Все места reading `tasks/QUEUE.md` (coder через AGENTS.md, UI dashboard, recovery) должны либо читать через daemon API, либо получать generated view. Migration: Deprecate committed QUEUE.md, emit warning если found. Remove после одного release cycle.

Tests: arbitrary push с added/removed task files → next cycle показывает corrected queue. Coder picks correct task. Recovery работает.

Размер: A — medium. B — large.

Tier 1 для Wave 4 (reliability). Ranks выше многих текущих Wave 4 items — ghost tasks это реальная production issue validated natural experiment (OBS-1, OBS-2).

**PR-182. diagnose_error bypass для infra errors (OBS-4)**
Files: `src/daemon/handlers/error.py`, возможно новый `src/daemon/error_classifier.py`.

Classify errors перед diagnose_error invocation:
- **Infra** (git fetch, network, credential, disk) → direct operator alert, не invoke CLI diagnose
- **Coder-caused** (exit 0 no PR, dirty tree, broken commit) → current diagnose_error path
- **External service** (GitHub 5xx, Codex bot error) → retry with backoff

Infra patterns list: `git fetch.*failed`, `ensure_repo_cloned`, `connection refused`, `disk full`.

Tests: synthetic infra error → no CLI invocation, state=ERROR, operator alert. Synthetic coder error → diagnose path как раньше.

Размер: medium.
Tier 2. Wave 6.

**PR-183. gh credential helper instrumentation (OBS-5)**
Investigation PR, не immediate fix.

Цель: reproduce intermittent git fetch exit 128 в controlled environment.

Approaches:
- Wrap `gh auth` invocations в logging script (timestamp, concurrent context, exit code, env snapshot)
- Log `$GITHUB_TOKEN` presence (не value) per invocation
- Capture `hosts.yml` mtime и size per invocation
- Correlate с daemon cycle timing

Fallback recommendation сразу применить: добавить `$GITHUB_TOKEN` env var в .env как secondary auth. Credential helper primary, token fallback — reduces dependency на helper stability.

Размер: low-medium для instrumentation. Real fix size unknown до collection.
Tier 2. Wave 6 или early Round 4.

**PR-184. Unify bounded-retry recovery patterns (OBS-7)**
Refactor PR extracting common framework.

Два validated bounded-retry patterns:
- PR-190a: coder exit 0 no PR → counter → HUNG after N attempts
- Dirty tree auto-recovery (OBS-7): preflight ERROR 3 cycles → auto-reset → IDLE

Хороший сигнал pattern reusability. Extract:
```
class BoundedRecoveryPolicy:
    max_attempts: int
    counter_key: str
    on_threshold: Callable  # HUNG или auto-reset или escalate
```

Apply existing patterns через policy. Future failure modes добавляются declaratively.

Prerequisite: verify обе pattern exist в current code (grep "auto-resetting to recover", check PR-190a counter).

Tests: existing PR-190a test + new dirty-tree recovery test, оба через common policy API.

Размер: medium (refactor, не new feature).
Tier 2. Wave 6.

**PR-185. Document WATCH STALLED substate (OBS-12)**
Files: `docs/states.md` (create), `AGENTS.md` state table.

STALLED — compound substate of WATCH. Trigger suspected: no progress for N minutes.

Action:
1. Grep code на `STALLED` — confirm existing implementation
2. Document trigger conditions, thresholds, transitions
3. Если emergent not intentional — decide keep/remove/rename

Размер: low.
Tier 3. Wave 6 or ad-hoc.

**PR-186. CANCELLED.** Был based on FINDING-1 (EVENT-LOG-01 label mismatch). Day 5 evidence показал FINDING-1 это pack bug (OBS-14), не product issue. Product event emission работает корректно. Canonical labels refactor не нужен. Если когда-то ещё понадобится для API consumers — revisit позже.

**PR-193. Nightly e2e runner для pipeline-orchestrator self-testing (Day 5)**
Files: new `scripts/nightly-e2e.sh`, cron entry, notification setup.

Scope:
- Wrapper running most recent `tests-manual/auto/dayN` pack
- Cron entry на homelab (Server-AI with RTX 3090 suggested — **confirm placement with user перед execution**)
- Report retention: `/home/alexey/nightly-reports/day5-YYYYMMDD.html`, rotate после 7 дней
- Notification on failure (telegram bot или email)
- Testbed cleanup: close open PRs >3 дня, delete merged branches

**Rationale:** pipeline-orchestrator repo не может run свой e2e suite в `scripts/ci.sh` (self-destructive). Nightly runner — accepted mechanism для self-testing.

Размер: medium.
Tier 2. Wave 7 (после Sprint 10 + Sprint 11). Medium risk.

**PR-194. Coder pin respected (FINDING-2, Day 5 confirmed)**
Files: `src/daemon/selector.py` или где coder pin resolved.

Problem: task file с `coder: codex` silently routed на Claude когда codex broken/unavailable. User intent ignored.

Fix: если `task_coder_pin in ("codex", "claude")` и coder unavailable → HUNG с clear message, **не** fall back на any.

Tests: Day 5 EXT-02 already covers (currently FAIL) — fixing product сделает PASS.

Размер: low-medium.
**Tier 1. Wave 4** — user expects coder pin работает. File task file ahead of Sprint 10 start.

**PR-195. Pack infrastructure: reset_testbed_clean effectiveness (OBS-15)**
Files: `tests-manual/auto/dayN/conftest.py`.

Problem: `reset_testbed_clean` closes open PRs но не cleans state reliably. Cross-test contamination evidence:
- DATA-01 leaves PR-301 in testbed
- EXT-04 next test sees PR-301, enters ERROR instead of IDLE recovery
- EXT-01 no_pr times out waiting for IDLE because of leftover state

Fix options:
- Wait for IDLE state transition перед proceeding после cleanup (synchronous confirmation)
- Force stop + clear Redis key + force start в cleanup (не just close PRs)
- Add cleanup verification assertion

**Pack-only change**, не product. Tier 2. Low risk.

**PR-196. Pack: EVENT-LOG-01 reads correct event source (OBS-14)**
Files: `tests-manual/auto/day5/tests/test_event_recheck_01.py`.

Problem: test reads `state.history` (state transitions), correct source is `/api/events/{slug}` или similar API endpoint. Events present в UI but invisible to test.

Fix: switch test на correct API endpoint.

Outcome: Day 4 и Day 5 EVENT-LOG FAILs resolve. OBS-13 (event eviction hypothesis) officially retracted — evidence показывает events persist 3+ минут в UI.

**Pack-only change**, не product. Tier 3. Low risk.

---

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

---

## Deferred / Round 4

- **Sprint 10 direct task injection.** Daemon передаёт pr_id + task_file path + task body в prompt coder'у напрямую. Снижен в приоритете: Wave 2 прошёл без AGENTS.md fixes вообще, значит текущая indirection работает приемлемо. Вернуть в Tier 1 только если incident повторится несколько раз.
- **Heartbeats как status widget не event.** Отдельное поле на state + новый UI widget. Medium-high complexity.
- **Thompson Sampling selector.** Из sprint 11 плана. Beta posteriors (Codex Beta(81,19), Claude Beta(76,24)), cost-aware reward. Blocker для real agent+model routing quality — как только Wave 4-5 закончатся, это первый приоритет Round 4.
  **Must include `force_coder` override (Day 4 finding FINDING-2)** для fault testing и manual pinning. Без этого невозможно test Codex-unavailable / Claude-unavailable scenarios — selector всегда routes на working coder.

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

## Известные риски и осторожности

### Wave 5 (state model refactor) — самый высокий риск

- PAUSED overloaded с 3 смыслами: rate-limited, user-paused, stop-cancel cleanup
- 27 прямых usages `PipelineState.PAUSED` в 7 файлах
- 22 точки где `error_message` is preserved/cleared на state transitions
- handle_paused (idle.py:642-700) содержит 60+ строк сложной логики coder swap при частичном rate limit
- Уже был один legacy migration в runner.py:899 ("Legacy ERROR + rate_limited_until -> PAUSED"). Новая миграция — вторая.
- Migration Redis state keys должна быть idempotent
- UI tests на user_paused visible state могут сломаться

### Wave 4 (reliability fixes) — средний риск

- PR-159 (DOING sync): race между _mark_queue_done + auto regen + new DOING writer. Решено single writer principle — меняется только derive_task_status helper, не добавляется новый writer.
- PR-160 (recovery source of truth): нужны тщательные tests на recovery scenarios. `_preserve_crashed_run_commits` должна правильно вызываться чтобы не терять commits.
- PR-161 (push verification): false positive ERROR на transient fetch failure. Решено retry_transient wrap.

### Wave 3 (UX) — низкий риск

- Все локальные изменения, нет cross-cutting concerns. Revert простой.

---

## Process notes (как работать с этим документом)

1. После каждой merge'нутой Wave обновлять статус задач на "merged" и следующая Wave готова.
2. Новые observations по мере их появления — добавлять в соответствующий Tier под "New observations" секцию (создать при надобности).
3. При открытии нового chat: скидывать этот документ + свежий zip репо + краткое описание "где мы сейчас".
4. Task files писать партиями по Wave (например Wave 3 — 6 task files в одной session).
5. Architectural decisions (раздел выше) — append-only. Если решение меняется, старое помечать "superseded by …", новое добавлять как отдельный пункт.
6. **Testing-week reports (multi-day observation runs):** по итогам каждого дня — update roadmap с FINDING-*, OBS-* inline (уже делается для Day 4). Day summary включает evidence path, pass/fail breakdown, new observations, production validations, action items.

---

## Testing-week closure summary (Days 1-5, concluded 2026-04-24)

Duration: 5 days (4 manual + 1 auto).
Observations collected: 60+.

**Real product bugs confirmed:**
- FINDING-2 / EXT-02: coder pin silently ignored → PR-194

**Hypothesized product bugs retracted/inconclusive:**
- OBS-13 event eviction: RETRACTED (pack bug OBS-14)
- OBS-5 git fetch 128: inconclusive, ENV-TOKEN SKIPPED — re-verify via OBS-16 steps

**Production validations earned:**
- PR-190a (natural experiment Day 4 + EXT-01 Day 5)
- DATA-01 daemon SIGKILL queue integrity
- DATA-02 upload during Redis down
- EXT-04 Redis down + recover end-to-end

**Positive behaviors confirmed (protect from regression):**
- OBS-7 dirty tree auto-recovery
- OBS-11 UI graceful degrade on Redis down
- OBS-12 WATCH STALLED state (awaits PR-153 STALLED removal)

**Pack improvements queued для next pack:**
- OBS-14 correct event source (PR-196)
- OBS-15 effective cleanup (PR-195)

**Findings queued для separate attention:**
- OBS-2 QUEUE.md regen coupled к merge (PR-181)
- OBS-4 diagnose_error CLI fails on infra (PR-182)

---

## Открытые вопросы ждущие решения

### Next session priorities (post-audit, 2026-04-24)

1. **OBS-16 verify.** Run `docker compose up -d --force-recreate daemon`, check `GITHUB_TOKEN`, если visible → re-run ENV-TOKEN-01 для OBS-5 verification.
2. **Start Sprint F1.0 e2e infrastructure.** PR-153 (infrastructure), PR-154 (core tests), PR-155 (docs). BLOCKER для всего остального.
3. **Write PR-156 task file** (coder pin respected, FINDING-2) сразу после F1.0 merged. Needs EXT-02 spec for reference — user to provide.
4. **Then Sprint F1.2 (PR-157 QUEUE.md presentation layer)** — large consolidated refactor. Breakdown в 2-3 под-PR если scope слишком large после drafting.
5. **Defer Sprint 10** (PR-167..169) and Sprint 11 (PR-178..181) до конца Phase 1.

### Still open architectural decisions

- **PR-157 внутренний breakdown:** если task file reaches >200 lines, split на подPR: (a) remove QUEUE.md from git tracking + migration, (b) recovery rewrite, (c) derive_task_status accepts current_task_pr_id. Decision making в draft phase.
- **Migration Redis state keys для Wave 5:** как обрабатывать users которые в момент rollout находятся в state=PAUSED. Детали будут в PR-173 task file. Обычный users = только ты, так что migration trivial.
- **Testing packs storage:** оставить локально uncommitted (подход C) или commit в archive branch? Recommended C — packs служили разовой цели, production e2e пишется с нуля.

### Resolved decisions

- **PR-157 Option A vs B** → **B** (radical, QUEUE.md removed from git). Выбрано пользователем 2026-04-24.
- **Sprint 10 breakdown** → **3 sub-PR с feature flag**. Выбрано пользователем 2026-04-24.
- **E2e testing** → add immediately в Sprint F1.0 как blocker. Decided 2026-04-24.
- **PR-156 task file first** → yes, после OBS-16 verify. Decided 2026-04-24.
