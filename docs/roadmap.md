# Pipeline Orchestrator Roadmap

Живой документ. Обновляется после каждой merge'нутой волны и после каждой chat-session.

Последнее обновление: 2026-04-21, конец сессии Round 3 planning.

---

## Текущий статус

- **Round 2 Tier 1+Tier 2:** PR-144 merged, PR-145 stuck в CODING loop (повторные retry), PR-146..PR-152 TODO, PR-155 TODO (HTMX 400).
- **Round 3:** 24 observations собраны, приоритизированы, оценены по риску регрессий. Task files ещё НЕ написаны. Нумерация начнётся с PR-153 (сквозная), с учётом что PR-155 уже занят.
- **Blocker сейчас:** PR-145 CODING retry. Codex часто exit 0 без push. Hotfix через AGENTS.md (Wave 1) должен помочь.

---

## Архитектурные решения (принятые в этой session)

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

- Ограничить reading scope coder'а: whitelist (QUEUE.md, task file, files в "Files to touch"), blacklist (README, CLAUDE.md, unrelated source, other tasks).
- Explicit failure signalling: exit non-zero + `UNABLE: <reason>` в stderr если PR не создан.

---

## Backlog Round 3

Нумерация PR через сквозную (PR-153 и далее). PR-155 уже занят для HTMX 400 из Round 2.

### Wave 1 (manual commits в main, не через daemon)

Приоритет максимальный. Делает coder надёжнее для всех последующих task'ов.

**PR-153. AGENTS.md bounded reading scope для PLANNED PR**
Файл: `AGENTS.md`. В секцию `## PLANNED PR runbook` добавить блок `### Files to read`.
Whitelist: QUEUE.md, tasks/PR-*.md активной задачи, файлы в "Files to touch".
Blacklist: README.md, CLAUDE.md, other tasks/PR-*.md, unrelated src/.
Размер: ~20 строк.
Tier 1. Low risk. Manual commit.

**PR-154. AGENTS.md explicit failure signalling**
Файл: `AGENTS.md`. В секцию `## Daemon Mode` добавить блок `### Exit behavior`.
Exit 0 только если PR создан + @codex review posted. Иначе exit non-zero + stderr `UNABLE: <reason>`.
Размер: ~15 строк.
Tier 1. Low risk. Manual commit.

### Wave 2 (завершить Round 2 через daemon)

Task files уже написаны, только execute через daemon после Wave 1 merge.

- PR-145 FIX iteration cap (stuck сейчас, retry после Wave 1)
- PR-146 Pause/Stop event log
- PR-147 Stale CHANGES_REQUESTED retrigger
- PR-148 Separate Commits metric
- PR-149 Coder-neutral logs
- PR-150 State badges consistent width
- PR-151 Buttons responsive subtle
- PR-152 Remove dead Start badge
- PR-155 HTMX 400 whitelist

### Wave 3 (Round 3 Tier 1 UX — low risk, можно параллельно)

**PR-156. Remove STALLED indicator entirely**
Файлы: `src/web/templates/base.html` (JS + CSS), `components/repo_summary.html`, `components/repo_cards.html`.
Удалить: syncStalledBadges JS (base.html:496-520), CSS `.stalled` правила (base.html:357-365), hint spans (repo_summary:32, repo_cards:58).
Сохранить: data-* attributes (harmless без JS).
Размер: ~30 строк delete.
Tier 1. Low risk.

**PR-157. Spinners на все HTMX actions >100ms**
Файлы: HTMX templates где есть actions — `_controls.html`, upload form, settings form, coder selector.
Default `hx-indicator` + shared spinner partial.
Размер: medium (несколько templates).
Tier 1. Low risk.

**PR-158. Event log single-row dedup display**
Файлы: `src/web/templates/components/event_list.html`, `components/activity_feed.html`.
Single timestamp (last_seen), убрать inline `(xN)`, добавить в конец row span с Lucide rotate-ccw SVG + count если count>1, `title` attribute с first_seen.
Зависит от PR-150 alignment (done в Wave 2).
Размер: low (2 templates).
Tier 1. Low risk.

**PR-159. Fuzzy dedup в log_event для numeric counter parts**
Файлы: `src/daemon/runner.py:770-793`.
Regex pattern list `\b\d+/\d+m\b`, normalize перед compare, update event до latest text, increment count, update last_seen_at.
Tests в `tests/test_runner.py`.
Размер: ~10 строк + tests.
Tier 1. Low risk.

**PR-160. Light-theme dropdown fix**
Файлы: CSS в `base.html` или shared styles.
Добавить `color-scheme: dark` на select elements.
Размер: 1-2 строки.
Tier 1. Low risk.

### Wave 4 (Round 3 Tier 1 reliability — medium risk, sequential)

**PR-161. QUEUE.md DOING reflects current_task** (was PR-R3-10 revised)
Файлы: `src/task_status.py` (derive_task_status), `src/daemon/handlers/idle.py` (_write_generated_queue_md caller).
Pass `current_task_pr_id` в derive_task_status. Если header.pr_id == current_task_pr_id и не DONE → return DOING даже без open PR.
Single writer principle сохраняется — меняется только helper.
Tests: crash in CODING recovery scenarios.
Размер: ~30 строк + tests.
Tier 1. Medium risk. Depends on baseline stable.

**PR-162. Recovery source of truth from open PRs**
Файлы: `src/daemon/recovery.py`.
Первичный signal: `gh pr list --state open`. Match по branch name prefix с task files. Matched → WATCH. Иначе → IDLE + pick next TODO через get_next_task.
Tests: многочисленные edge cases recovery (crash в CODING, crash в FIX, user_paused during crash, open PR unrelated to current_task).
Размер: medium.
Tier 1. Medium risk. Depends on PR-161.

**PR-163. Asymmetric push verification в fix.py normal path**
Файлы: `src/daemon/handlers/fix.py:454`.
Добавить `remote_branch_contains_head()` check перед `record_fix_push()` в normal completion path.
Если local HEAD changed но remote не contains → state=ERROR с clear message, не incrementing push_count.
Wrap fetch в `retry_transient` чтобы избежать false positive на transient network.
Симметричный с stop-cancel path (fix.py:405-415).
Размер: ~20 строк + tests.
Tier 1. Medium risk (false positive ERROR на unstable network).

**PR-164. QUEUE auto-regen respects explicit `Depends on: none`**
Файлы: `src/dag.py`.
Если task header имеет `Depends on: none` — не добавлять sequential dependencies автоматически.
Tests: task file с none не получает auto-deps.
Размер: low.
Tier 1. Medium risk (tasks могут запускаться раньше если плохо указаны deps, но это user's responsibility).

### Wave 5 (Round 3 Tier 1 state model — high risk, разбит на 4 sequential)

Делать после Wave 4 когда baseline stable.

**PR-165. Add awaiting_start + rate_limited_until fields to RepoState**
Файлы: `src/models.py`.
Добавить 2 field, PAUSED пока живёт как transient dispatcher state. Migration в recovery: если state=PAUSED и user_paused=True → awaiting_start=True. Если state=PAUSED и rate_limited_until != None → оставить как есть.
Tier 1. High risk. 4 of 4 sub-PR sequence.

**PR-166. Rewrite rate_limit.py + handle_paused на новые flags**
Файлы: `src/daemon/rate_limit.py`, `src/daemon/handlers/idle.py:642` (handle_paused).
Rate limit путь использует rate_limited_until flag. handle_paused остаётся как fallback для legacy PAUSED state в existing Redis.
Tier 1. High risk.

**PR-167. Remove PAUSED из handler error paths**
Файлы: `src/daemon/handlers/fix.py`, `coding.py`, `merge.py`, `error.py`.
Заменить 15+ `state = PipelineState.PAUSED` на `awaiting_start = True; state = IDLE`.
Tier 1. High risk.

**PR-168. Remove PAUSED из enum, UI rewrite, migration script**
Файлы: `src/models.py` (enum), `src/web/app.py`, templates где PAUSED badge показан.
Migration script: existing Redis states c state=PAUSED переносим на IDLE+flags.
UI: Play button visible if awaiting_start OR (IDLE AND queue_has_work).
Tier 1. High risk.

### Wave 6 (Round 3 Tier 2 — по приоритету когда baseline стабилен)

**PR-169. Per-repo coder в Settings table + remove from repo detail**
Settings получает table всех repos с coder dropdown. Repo detail — read-only. Rename "Any" → "Auto-Select". Remove Inherit. Hot reload + confirmation.
Tier 2. Low-medium risk.

**PR-170. Pulse animation jump fix**
CSS transition вместо keyframes reset при HTMX swap.
Tier 2. Low.

**PR-171. Pulse badge вместо pulse dot**
Badge заменяет точку в state indicator.
Tier 2. Low.

**PR-172. Event text clarity pass**
Переписать confusing messages (audit всех log_event). Осторожно — влияет на fuzzy dedup.
Tier 2. Low-medium.

**PR-173. Recovery skip crashed-task-retry**
Если recovery находит branch on remote без open PR — mark blocked, skip на next TODO.
Tier 2. Medium.

**PR-174. Coder exit=0 без push diagnostic handler**
coding.py: перед ERROR check `git ls-remote` + `git rev-list origin/{branch}..HEAD`. Различать "branch missing" vs "branch exists PR missing" vs "no branch at all".
Tier 2. Low-medium.

**PR-175. Codex bot error comment detection**
Watch handler polling comments от `chatgpt-codex-connector[bot]`. Pattern match `Something went wrong` → immediate re-trigger @codex review.
Tier 2. Low.

**PR-176. Surface external service errors in UI**
Event log entries для external service failures.
Tier 2. Low.

**PR-177. Configurable review_timeout_min per-repo UI**
UI control в Settings table рядом с coder selector.
Tier 2. Low.

**PR-178. Lock upload during QUEUE.md regeneration**
Upload endpoint returns 503 Retry-After=10 если `queue_regen_in_progress` Redis key set.
Tier 2. Low.

**PR-179. Clean up PipelineState.MERGE dead value**
handle_merge никогда не set'ит PipelineState.MERGE. Решить: либо восстановить usage в начале handle_merge, либо удалить из enum и _TRANSIENT_STATES.
Tier 3 cleanup.

---

## Deferred / Round 4

- **Sprint 10 direct task injection.** Daemon передаёт pr_id + task_file path + task body в prompt coder'у напрямую. После Wave 1 AGENTS.md fixes эта задача снижена в приоритете. Вернуть в Tier 1 только если после Wave 1 coder всё ещё ненадёжно работает.
- **Heartbeats как status widget не event.** Отдельное поле на state + новый UI widget. Medium-high complexity.
- **Thompson Sampling selector.** Из sprint 11 плана. Beta posteriors (Codex Beta(81,19), Claude Beta(76,24)), cost-aware reward.

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

- PR-161 (DOING sync): race между _mark_queue_done + auto regen + new DOING writer. Решено single writer principle — меняется только derive_task_status helper, не добавляется новый writer.
- PR-162 (recovery source of truth): нужны тщательные tests на recovery scenarios. `_preserve_crashed_run_commits` должна правильно вызываться чтобы не терять commits.
- PR-163 (push verification): false positive ERROR на transient fetch failure. Решено retry_transient wrap.

### Wave 1 (AGENTS.md) — низкий риск

- Изменения в runbook могут по-разному интерпретироваться разными coder'ами (Claude vs Codex). Proof в pudding — execution Wave 2 покажет.
- Revert простой (revert commit в main).

---

## Process notes (как работать с этим документом)

1. После каждой merge'нутой Wave обновлять статус задач на "merged" и следующая Wave готова.
2. Новые observations по мере их появления — добавлять в соответствующий Tier под "New observations" секцию (создать при надобности).
3. При открытии нового chat: скидывать этот документ + свежий zip репо + краткое описание "где мы сейчас".
4. Task files писать партиями по Wave (например Wave 3 — 5 task files в одной session).
5. Architectural decisions (раздел выше) — append-only. Если решение меняется, старое помечать "superseded by …", новое добавлять как отдельный пункт.

---

## Открытые вопросы ждущие решения

- **Custom loop icon vs Lucide rotate-ccw** для event log dedup indicator: финализировано — Lucide rotate-ccw как есть.
- **Sprint 10 direct task injection timing:** решим после Wave 1 merge — смотрим на стабильность coder'а.
- **Migration Redis state keys для Wave 5:** как обрабатывать users которые в момент rollout находятся в state=PAUSED. Детали будут в PR-168 task file.
