# Pipeline Orchestrator Roadmap

Живой документ. Обновляется после каждой merge'нутой волны и после каждой chat-session.

Последнее обновление: 2026-05-02 (major cleanup pass: deleted Active investigations + broken Work Modes blocks 528 lines, replaced with compact Lessons learned appendix; collapsed Implementation Audit summary to 5 lines; rewrote Risks section for current Sprint 13/14; rewrote Open questions reflecting today's strategic confirmations; demoted Multi-tier agent to Vision E; sprint nomenclature unified Sprint 12-17. Strategic confirmations today: License Apache 2.0 ASAP actionable (current LICENSE is MIT, not AGPL as memory implied), Vision A Sprint 17+, SQLite migration before Thompson, PR-FUTURE-7 eliminate QUEUE.md confirmed, PR-FUTURE-4 tier scaffolder after Sprint 13 OBS-AX, Telegram bot Vision D Sprint 17+. OBS-BI Path A locked. OBS-BG codex non-determinism explicitly NOT added (operator deliberate choice; defense-in-depth via existing pre-merge sync re-trigger). Sprint 14 expanded with OBS-BK + OBS-BL + OBS-BM totaling 9-11 PRs ~27h. Earlier session work: OBS-BE expanded scope, Cancellation policy v1, v1.1 refinements, Vision C companion app, Vision D conversational morning triage 4-stage gating, throughput baseline corrected to 25-30 PR/day. File reduced from 2790 to ~2200 lines).

Предыдущие: 2026-05-01 (PR-180..PR-207 shipped, все 28 PR merged. Multi-repo isolation audit complete, parallel run_cycle in main loop deployed. Foundation Sprint 36 PR specs generated for PR-208..PR-236 batch, internal architecture cleanup. Architectural future work section added for post-Foundation: AGENTS template scope, per-repo config, onboarding wizard, CI script generator MICRO PR. Onboarding of megaraid-dashboard и sms-gateway-v2 actively in progress; reconciled AGENTS.md files prepared, scripts/ci.sh manual creation required pre-onboarding due to scaffolder stub trap), 2026-04-29 (full roadmap rewrite на основе Implementation Audit), 2026-04-28 (sigkill recovery test multi-race resolved via PR-228/PR-232/PR-234/PR-236; production daemon deployed on fresh main; GraphQL quota burn analyzed; onboarding test subjects identified), 2026-04-27 (OBS-AA test pollution v1 misdiagnosis + v2 docker-exec fix; OBS-Y premature merge; Multi-tier agent direction; OBS-Z Codex EYES race), 2026-04-26 (Sprint F1.0 + PR-156/157 + PR-158/159 merged; Variant D direction; Development model & Layer 2 substrate observations), 2026-04-24 (after code audit zip __27__).

---

## Sprint nomenclature (unified 2026-05-02)

Continuous sprint numbering aligned with operator's mental model. Replaces ad-hoc "Wave X" / "Phase X" / "Round X" labels used earlier in this document. Earlier labels remain inline for cross-reference but new planning uses sprint numbers.

| Sprint | Content | Status | Estimate |
|---|---|---|---|
| Sprint 12 | Foundation Sprint (PR-208..PR-236) | In progress | 36 PRs, ~25 daemon-hours, ~1 daemon-day at 25-30 PR/day |
| Sprint 13 | OBS-AX scaffolder fix + OBS-AY UI freeze fix + License Apache 2.0 switch (was Wave 1 + Wave 2 + new license task added 2026-05-02) | Queued | 4-5 PRs, ~8 daemon-hours |
| Sprint 14 | Recovery + Cancellation policy expanded (was Wave 5; reordered earlier than Wave 3-4 due to throughput cost demonstrated 2026-05-02; expanded with OBS-BK + OBS-BL + OBS-BM additions) | Queued | 9-11 PRs, ~27 daemon-hours |
| Sprint 15 | UX polish + vocabulary + DONE-row metrics Path A (was Wave 3 + Wave 4, plus OBS-BH + OBS-BI Path A locked + OBS-BJ from 2026-05-02) | Queued | 7-9 PRs, ~17-19 daemon-hours |
| Sprint 16 | Multi-testbed harness + multi-repo tests (was Wave 6 + Wave 7) | Queued | 7+ PRs, ~15 daemon-hours |
| Sprint 17+ | Vision A multi-vendor routing first slice | Pending strategic decision | TBD |

Earlier "Wave X" references inside OBS items remain for backward compatibility; mapping is `Wave 1+2 = Sprint 13`, `Wave 5 = Sprint 14`, `Wave 3+4 = Sprint 15`, `Wave 6+7 = Sprint 16`. New OBS items added 2026-05-02 use sprint terminology directly.

---

## Текущий статус

- **Production deployed and stable** since 2026-04-29 with 28 additional PRs (PR-180..PR-207) shipped between 2026-04-29 and 2026-05-01. Daemon main loop now parallelizes per-repo run_cycle (PR-207) — multi-repo readiness unblocked.
- **GraphQL diet shipped** (PR-180 REST replacement, PR-184 adaptive IDLE polling, PR-185 GraphQL points observability, PR-191a/b ETag conditional requests, PR-202 WATCH adaptive polling). 36 ETag occurrences and 23 REST check-runs occurrences in `src/github_client.py`. OBS-AC binding constraint resolved.
- **AGENTS.md reconciliation framework shipped** (PR-192a markers, PR-192b apply, PR-192c dry-run + onboarding endpoints `/onboarding/preview` and `/onboarding/apply`).
- **Multi-repo isolation audit complete** (PR-193, `docs/multi-repo-audit-2026-04-29.md`). PR-207 parallelized main loop based on audit finding — sequential run_cycle was the remaining blocker.
- **Foundation Sprint 36 PR specs generated** (2026-05-01). PR-208..PR-236 batch covering god-class decomposition, atomic primitive in idle.py, regression test suite, observed-bug fixes (OBS-AR event log spam). Tasks ready in `/mnt/user-data/outputs/foundation-tasks/`. Estimated 2-3 days at daemon's 15-20 PR/day throughput.
- **Onboarding of megaraid-dashboard and sms-gateway-v2 in progress** (2026-05-01). Reconciled AGENTS.md files prepared; pre-onboarding manual scripts/ci.sh creation required to avoid scaffolder stub trap (scaffolder creates exit-0 stub if no ci.sh exists; no semantic conflict resolution yet — PR-FUTURE-3 territory).
- **Architectural future work documented** for post-Foundation period: PR-FUTURE-1 (AGENTS template scope cleanup), PR-FUTURE-2 (per-repo config inheritance), PR-FUTURE-3 (onboarding wizard with semantic conflict resolution), PR-FUTURE-4 (AI-driven scaffold replacing template-driven), PR-FUTURE-5 (read-only/observe mode for trial onboarding), PR-FUTURE-6 (UI-driven auth flow for GH/Claude/Codex), PR-FUTURE-7 (eliminate tasks/QUEUE.md entirely — in-memory queue model).

---

## Implementation Audit summary (2026-04-29) — collapsed 2026-05-02

Pre-2026-04-29 sprint legacy summary (Sprint F1.0..F4.2 detailed status) removed during 2026-05-02 cleanup. All items either shipped via 2026-04-29 Implementation Plan (PR-180..PR-207, see section below) or deferred indefinitely (PR-167..PR-173 SoT/PAUSED removal, Thompson Sampling). The Active OBS items subsection below is the only part still needed for current state tracking.

### Active OBS items

- OBS-2 (QUEUE.md regen drift): **CLOSED** — PR-181 removed QUEUE.md from git tracking, in-memory regeneration is now the only path.
- OBS-4 (diagnose_error infra bypass): **CLOSED** — PR-182 routes git/network errors past diagnose loop.
- OBS-5 (gh credential helper exit 128): **STILL OPEN** — intermittent, no instrumentation work done. Defer.
- OBS-Y (daemon merges before APPROVED): **status unclear** — no incidents observed since deploy. Monitor.
- OBS-Z (Codex EYES race window): **CLOSED** — PR-189 added pre-push state check + EYES-specific stale threshold.
- OBS-AA (test pollution Redis state survival): **CLOSED** — PR-230 task-fixture-redis-cleanup shipped.
- OBS-AB (sigkill multi-race): **CLOSED** — sigkill recovery test deterministic on test_sigkill_recovery via PR-228/232/234/236.
- OBS-AC (GraphQL quota burn): **CLOSED** — diet shipped via PR-180 (REST replacement), PR-184 (IDLE adaptive), PR-191a/b (ETag), PR-202 (WATCH adaptive). Daemon now operates well within quota limits.
- OBS-AD (PR-180 self-healing convergence): **DOCUMENTED** — observation captured, no fix needed.
- OBS-AE (coder opens PR for wrong task): **MITIGATED** — PR-205 (control wake) and PR-206 (settings wake) reduce window. Defense-in-depth via PR-200 (task header validation).
- OBS-AR (event log spam — 304 Not Modified loop): **OPEN** — bug in `_etag_get` returns None when status=304 AND cached=None, causing alternating "INFRA No tasks available" + "INFRA IDLE: merged PR check failed: gh: HTTP 304" every poll cycle. Reproduced by operator screenshot 76 events alternating these two types. Fix scoped as PR-236 in Foundation Sprint Batch E.
- OBS-AS (UI inconsistencies during onboarding): **OPEN, polish-tier** — two issues observed 2026-05-01: (1) "initializing" state shows both pulsing dot AND solid badge, inconsistent with other active states; (2) upload-result toast notifications dismiss too quickly (~2-3s) before operator can read failure messages. Fix scoped as small Polish PR (~15 LOC, ux, low complexity). Not a blocker for active onboarding.
- OBS-AT (successful multi-repo onboarding validation): **CLOSED, positive observation** — sequential onboarding of megaraid-dashboard + sms-gateway-v2 alongside existing pipeline-orchestrator validated 5 production surfaces in single session: AGENTS.md reconciliation, scaffolder idempotency, multi-repo coordination (PR-207), GraphQL diet headroom at 3 active repos, shared auth volumes. Foundation Sprint can now run safely on multi-repo daemon. Recorded for future reference.
- OBS-AU (Uploading spinner appears on all repo cards during single repo upload): **OPEN, medium severity** — observed 2026-05-01 evening across multiple sessions. Spinner does NOT appear during normal poll cycles (verified by operator: spinners disappear when no active upload). **Real pattern:** when operator uploads tasks to one repo, "Uploading..." spinner appears on ALL 3 repo cards simultaneously for the duration of the backend upload processing (validation, git stage, commit — several seconds). HTMX scoping leak: form's `hx-indicator="#upload-indicator-{name}"` should target only the matching card's spinner, but spinner shows on all cards. Sometimes spinner sticks past upload completion (browser warning "Form submission canceled because the form is not connected") — this is a secondary race-condition bug on top of the scoping leak. Two distinct problems in one observation: (a) scoping leak shows spinner on irrelevant cards during upload; (b) form-disconnect race occasionally leaves spinner stuck. **Diagnostic needed:** capture DevTools Network tab DURING an active upload (not after — spinners clear on complete). Look for: single vs multiple XHR requests, response timings, any `htmx:` events fired. **Priority:** near-term polish PR. ~15-25 LOC fix once root cause confirmed.
- OBS-AV (partial task upload + missing vocabulary synonyms): **OPEN, medium severity** — observed 2026-05-01 evening. Operator uploaded task batch with `Complexity: small` (6 files) and `Type: infra` (1 file). Validation banner correctly flagged 6 errors, but **valid PR-001 file made it to disk and was picked by daemon** while dependent tasks (PR-002..PR-008) were rejected. Three sub-bugs: (1) missing TYPE_SYNONYMS entry for `infra` (likely should map to `config`); (2) COMPLEXITY_SYNONYMS doesn't exist at all (`small/medium/large` should map to `low/medium/high`); (3) upload validation is per-file, not all-or-nothing — partial acceptance breaks dependency chains and traps daemon on orphan tasks. Fix: add missing synonyms maps + atomic upload validation (all task files validate before any file commits to disk). ~3 PRs, ~5 daemon-hours.
- OBS-AW (missing per-repo HUNG recovery control): **OPEN, medium severity** — observed 2026-05-01 evening. Repo stuck in HUNG state. Existing UI controls: Pause/Resume/Stop only manipulate `user_paused` flag, do not transition state machine out of HUNG. Resume on a HUNG repo is no-op. Operator forced to either: (a) restart entire daemon container (heavy hammer affecting all repos), (b) manually clear `tasks/PR-*.md` to force IDLE transition on next cycle, (c) wait for `handle_hung` to detect external PR merge/close. None of these surface in UI. Need explicit "Reset to IDLE" or "Recover from HUNG" button on repo card that: (1) clears local HUNG flag, (2) closes any orphan branch the coder may have created locally, (3) marks the trapped task as CANCELED in derived queue, (4) transitions state to IDLE. Fix: per-repo recovery control with confirmation dialog. ~2-3 PRs, ~4 daemon-hours. **High user-frustration impact even though not a daily occurrence — operator cannot recover from HUNG without shell access or destructive workarounds.**
- OBS-AX (scaffolder must replace CLAUDE.md, not preserve it): **OPEN, high severity, immediate priority** — verified 2026-05-01 evening through hypothesis test on megaraid + sms-gateway. External repos with user-authored Claude-specific notes in CLAUDE.md (storcli/D-Bus hints, "prefer existing modules", etc.) cause coder to HUNG with "PLANNED PR alone isn't enough context" because CLAUDE.md becomes system prompt and competes with AGENTS.md redirect. Replacing CLAUDE.md with single line `Read and follow AGENTS.md in this repository.` immediately unblocks coder. **Current scaffolder leaves existing CLAUDE.md untouched** — should overwrite with minimal redirect instead, optionally migrating user-authored notes into a section in AGENTS.md user portion. Fix scoped as 1 PR, ~2 daemon-hours: update `scaffolder.py` to overwrite CLAUDE.md unconditionally with minimal content; document rationale in template. **Without this fix, every external repo with non-trivial CLAUDE.md will HUNG on first task pick.** Should ship before any non-author user attempts onboarding.
- OBS-AY (UI freezes when navigating between repo views — setInterval leak + slow /api/states): **OPEN, HIGH severity, ROOT CAUSE CONFIRMED 2026-05-01** — initial hypothesis was SSE connection leak; verification via DevTools Network tab disproved that (SSE connections work fine, only 3 active). **Real root cause is two stacked bugs:** (A) `checkAlerts()` JS function in `base.html` runs `setInterval(checkAlerts, 10000)` on EVERY page; navigation does NOT cleanup the interval, so every page switch multiplies the polling rate of `/api/states`. After 5-6 navigations, fetch fires every ~2 seconds. (B) `/api/states` endpoint is slow — observed 14-25 second response times under load (Network tab confirmed). Browser HTTP/1.1 6-connection-per-domain limit gets saturated by stuck `/api/states` calls; new requests (including htmx polls and the next /api/states attempt) block waiting for a free slot, manifesting as full UI freeze. **Backend code-level finding (src/web/app.py:542 `get_all_repo_states`):** function does synchronous `load_config(config_path)` file I/O inside async function (blocks event loop), then `for repo in cfg.repositories: await _get_repo_state_safe(...)` — sequential awaits instead of `asyncio.gather()`. With multiple concurrent `/api/states` calls accumulating from leaked setInterval, sync file I/O on each + sequential per-repo Redis trips compound into multi-second waits per request. **Fix A (frontend, ~3 LOC):** add `window.addEventListener('beforeunload', () => clearInterval(intervalId))` to base.html. **Fix B (backend):** (i) cache config or load it async via `asyncio.to_thread`; (ii) parallelize per-repo Redis reads via `asyncio.gather`; (iii) short-TTL cache on /api/states response (5s acceptable for alert checks); (iv) expose only `has_alerts: bool` instead of full states JSON for the alert-check use case. Both fixes needed — Fix A stops the bleeding immediately, Fix B addresses the root performance issue. **Diagnostic evidence:** Network tab Image showing 3 sequential /api/states requests at 24.90s, 14.99s, 3.98s response times; pending fetch entries stacked behind them; /api/states triggered from `(index):1255` which is `checkAlerts`. ~2-3 PRs total, ~5 daemon-hours. **Impact: critical for multi-repo UX. Will get worse as repo count grows** — at 5 repos this could be 60s+ freeze, at 10 repos completely unusable. Without fix, every operator with 2+ repos navigating actively will experience UI freezes that scale super-linearly with repo count.
- OBS-AZ (repo card header layout inconsistent — upload icon wraps to new row on long repo names): **OPEN, low-medium severity** — observed 2026-05-01 evening. Repo cards have a flex-wrap container holding state badge + Pause + Stop + Upload icon. For repos with long names like `AlexBomber12__pipeline-orchestrator` (truncated as `AlexBomber12__pipeline-orchestra…`), the long name+badge consume the row's width budget, forcing the upload icon to wrap onto a new line **below** the Pause/Stop pair. Repos with shorter names (`AlexBomber12__sms-gateway-v2`, `AlexBomber12__megaraid-dashboard`) fit all 4 elements inline. The behaviour is technically responsive (flex-wrap doing its job) but visually inconsistent across cards in the same dashboard view. Looks like a broken layout, not a deliberate stacking. Fix options: (a) put controls in a fixed-position right edge of card with overflow hidden, name+badge truncate harder; (b) drop name truncation point earlier so all variants fit one row; (c) use grid layout with explicit column widths; (d) hide upload icon behind a kebab menu when card width < threshold. Likely fix: option (a) — controls always at right edge regardless of name length, name truncates to fit. ~1 PR, ~2 daemon-hours. **Group with OBS-AS UI inconsistencies into single Polish PR for Wave 3.**
- OBS-BA (Pause/Stop/Upload buttons positioned mid-card, not anchored to right edge): **OPEN, low severity** — observed 2026-05-01 evening, related to OBS-AZ same root cause (flex-wrap layout). Controls cluster appears in the middle horizontal space between repo name and card edge, rather than firmly anchored to right edge. Same fix as OBS-AZ option (a) addresses this: explicit right-edge positioning via `justify-end` on outer container with hard width allocation, or grid layout with right-aligned column. Combine with OBS-AZ into single Polish PR. **Same wave, same fix scope, no separate cost.**
- OBS-BC (daemon escalates on infra-failure CI runs without classifying failure type): **OPEN, medium severity** — observed 2026-05-02 morning on pipeline-orchestrator PR-219b. CI failed because GitHub Actions runner could not resolve `azure.archive.ubuntu.com` to fetch apt packages for Playwright chromium font dependencies. This is an **infra-failure** (external network glitch on Microsoft Azure mirror or GitHub runner side), **not a code-failure** in the PR. Coder correctly diagnosed root cause in FIX FEEDBACK output: "Confirmed — the failure is in `playwright install --with-deps chromium` because the GitHub runner could not resolve azure.archive.ubuntu.com to fetch apt packages." Despite this, daemon escalated PR for manual review instead of retrying CI. **Same class of bug as OBS-BB and OBS-AX:** daemon accepts signal at face value without post-condition validation. Here it sees "CI failed" and escalates, without distinguishing flaky-infra-failure from real-code-failure. **This is the auto-classification failure category from strategy-conversation-summary.md (раздел 6, "Failures — flaky vs real regression vs env vs coder logic"), not yet implemented in code.** Fix approach: daemon classifies CI failure by parsing CI log; if classification is infra-related (network resolution, apt fetch, transient timeout), retry CI run before escalating; only escalate on classified real-failures (test assertion, lint error, type error, coder logic regression). Bandit could optionally learn classification from labeled history, but heuristic rules cover the common cases. ~2-3 PRs, ~5 daemon-hours. **Wave 5 alongside OBS-AW + OBS-BB recovery work.** Strategic significance: this is the missing piece that distinguishes "noise" from "signal" in churn metric — without it, infra-flakes inflate FIX iterations and pollute cost-per-merged-PR data, making bandit posteriors less reliable.
- OBS-BD (gh label create fails when label already exists, daemon does not handle gracefully): **OPEN, low severity** — observed 2026-05-02 morning. Daemon attempted `gh label create escalated --color B60205 --description "Daemon escalated, manual review required"` during PR-219b escalation. Failed exit 1 with `label with name "escalated" already exists; use --force to update its color and description`. Daemon logged "skipped" and continued (functional behavior preserved — escalation worked sans label), but this is brittle. **Fix options:** (a) check `gh label list` before create, only create if absent; (b) catch "already exists" error specifically and treat as success; (c) use `gh label create --force` always, idempotent. Likely fix: option (b) — exception catch on the specific stderr pattern. ~1 PR, ~1h. **Wave 3 polish, low priority — does not block work.**
- OBS-BE (daemon classifies coder ESCALATE output as CRASH, marks task CANCELED): **OPEN, medium-high severity** — observed 2026-05-02 morning on pipeline-orchestrator PR-231. Coder (Claude) made a **correct, deliberate escalation**: read task spec, identified that production-critical config values (incl. `usage_api_beta_header` which could break Anthropic auth) were not provided in any source (task file, config.production.yml, /data/secrets/, git history, prompt), noted internal contradiction in spec (fallback says "open draft PR" but AGENTS.md PR-196 forbids draft PRs), and exited with structured `ESCALATE:` message in stdout summarizing the reasoning. **This is exactly the escalation behavior we want.** However, daemon treated this as CRASH: event log entry "Task PR-231 crashed, marking CANCELED. Manually re-upload to retry." The escalation reasoning was **lost** — operator sees no indication of what coder needed, has to read raw stdout in some other surface to understand. **Two distinct fixes needed:** (1) Daemon parses coder stdout for `ESCALATE:` prefix and treats it as deliberate signal, transitions task to ESCALATED state (new state) or BLOCKED with reasoning preserved in event log; (2) UI surfaces escalation reasoning prominently in task card so operator can act on it without digging into raw logs. **Same class of bug as OBS-BC and OBS-BB:** daemon accepts signal at face value without parsing intent. Here the signal IS intent (deliberate, structured ESCALATE), but daemon's classifier doesn't recognize it as distinct from crash. ~2-3 PRs, ~5 daemon-hours. **Wave 5 alongside OBS-AW + OBS-BB + OBS-BC recovery work.** Strategic significance: ESCALATE is the **correct alternative** to coder freedom-bug (OBS-AE, OBS-AX, OBS-BB) — instead of guessing and pushing wrong code, coder explicitly hands control back to operator. This pattern should be **rewarded by daemon**, not classified as failure.

  **Scope expanded 2026-05-02 (after operator session on CRASH preservation):** the underlying issue is generalization of finding 1 above. Daemon must preserve cause-of-CANCELED for **every** path into CANCELED, not only structured ESCALATE. The current generic event log line "Crashed during a prior run; re-upload the task file to retry" loses information regardless of which path was taken. Four source categories with distinct payloads:

  1. **CRASH** (uncaught exception or non-zero exit without ESCALATE marker): payload is `exit_code` plus last N lines of stderr/stdout (suggested N=20).
  2. **ESCALATE** (deliberate ESCALATE marker on last non-empty stdout line): payload is the one-line reason text.
  3. **TIMEOUT** (per-cycle, per-PR, per-FIX, planned-PR overall budget): payload is which limit hit, duration elapsed, active phase at timeout (CODING / FIX / WATCH).
  4. **INFRA** (subsystem fault before coder even ran, or during recovery): payload is subsystem identifier (gh, network, redis, github_app_token) plus error class.

  **Storage model:** Redis key `cancellation:{repo_name}:{task_id}` containing `{category, reason_text, timestamp, pr_number_if_open, event_log_range_start_id, event_log_range_end_id}`. Naming aligns with existing per-repo scope-noun keys (`pipeline:{repo_name}`, `control:{repo_name}:*`, `upload:{repo_name}:pending`) per `docs/multi-repo-audit-2026-04-29.md`. NOT in task file: task files are TODO/DONE markers per Variant D direction, runtime state lives in Redis. **TTL = `_TTL_SECONDS` from `src/metrics.py:9`** (currently 90 days), reusing the existing long-term-metrics horizon constant rather than introducing a new magic number. If forensics window proves insufficient or excessive in practice, both metrics records and cancellation records adjust together via the single constant.

  **UI surface:** click on the CANCELED badge expands an inline panel below the badge with category icon, reason text, timestamp, optional PR link, and a button "Open in event log" that scrolls or filters the live event log to the captured range. **NOT a tooltip:** by analogy with the toast-dismiss problem documented in OBS-AS (auto-dismiss too fast for operator to read), tooltip pattern would inherit the same invisibility on mouseout. Inline expand keeps content visible until operator clicks elsewhere.

  **Operator preference recorded:** CANCELED is an acceptable terminal state when cause survives, allowing later forensics. The bug is loss of cause, not the CANCELED outcome itself. Re-upload of the same task file remains the recovery flow; this fix only adds a "what happened" surface around it.

  **Revised estimate:** 3-4 PRs (one extra PR added for the multi-source category model and detection paths across the four entry points, plus the inline-expand UI), ~7 daemon-hours total. **Wave 5 sequencing unchanged.**

  **Behaviour aspects** (when daemon goes ESCALATED vs CANCELED, operator availability signal, dependency-aware blocking, dashboard surfacing): covered separately in **Cancellation policy** section below. OBS-BE handles the storage/preservation problem; Cancellation policy handles the routing/policy problem. Both ship together in Wave 5.
- OBS-BF (task generator produces internally-contradictory specs that violate established AGENTS.md rules): **OPEN, medium severity, root cause finding** — observed 2026-05-02 morning on PR-231 spec, surfaced by coder's ESCALATE reasoning. Task spec for PR-231 included a fallback instruction "Open a draft PR with TODO markers… wait for operator to fill in the values" but AGENTS.md and PR-196 establish hard rule "PRs must be created in ready state, not draft." Coder correctly flagged the contradiction. **Root cause:** task generator (whoever/whatever produced PR-231 spec — likely chat session like this one) did not have full context of AGENTS.md rules, so emitted instructions that conflict with established conventions. **This is a structural problem that grows with project age:** as the repo accumulates conventions, a generator without full rule context produces increasing rate of conflicting specs. Coder catches it via ESCALATE if smart enough; otherwise produces non-conforming PRs. **Fix approach:** (a) task generator must read AGENTS.md before producing spec; (b) automated linter on task files that checks for known anti-patterns ("open draft PR", "use --force", "skip CI", etc.); (c) longer-term, Sprint F2.1 SoT (Source of Truth direct instructions) where daemon validates task spec against AGENTS.md rules before accepting upload. ~2 PRs short-term (linter + generator-context-check), full Sprint F2.1 long-term. **Wave 4 vocabulary alongside OBS-AV** (both about task spec validation). Strategic: this is the **author's own task generation reliability problem**, distinct from coder reliability — surfaces only when sufficiently smart coder catches it via ESCALATE. With weaker coders, contradictory specs would silently produce non-conforming PRs (OBS-AX class).

- OBS-BH (event log badge text duplicates badge label): **OPEN, low severity, observed 2026-05-02 by operator** - every event log line in the dashboard has the form `[BADGE_NAME] message text` (e.g. `[INFRA] Posted @codex review on PR #298`, `[FIX] [claude] entering FIX`, `[CODING] Opened PR #298 → WATCH`). The badge is also rendered as a coloured pill on the same row. The bracketed prefix in the text is therefore redundant and visually noisy. Fix: strip leading `[STATE_NAME]` token from the text rendering when the corresponding badge is present (template-side change, no event-log storage migration needed). Keep `[STATE_NAME]` in the underlying log payload for grep / debugging in raw mode, render-time stripping only. ~1 PR, ~1-2 daemon-hours. **Sprint 15 polish bucket.**

- OBS-BI (per-PR metrics surface in DONE list rows): **OPEN, low-medium severity, observed 2026-05-02 by operator** - DONE list currently shows only PR ID + title + branch. Per-PR metrics are collected (RunRecord schema in `src/metrics.py` has `duration_ms`, `fix_iterations`, `tokens_in`, `tokens_out`, `task_type`, `complexity`, `exit_reason`, `files_touched_count`, `diff_lines_added/deleted`) and surfaced as a separate "Recent PRs" panel (`src/web/templates/components/pr_metrics.html`, endpoint `/partials/repo/{name}/metrics`, polls every 60s, last 20 records: Task / Coder / Model / Duration / FIX Iterations / Exit Reason). However, this panel is **not visible from the DONE list view** that the operator uses for historical browsing - it lives in a different repo-detail surface that does not naturally surface for completed PRs.

  **Operator decision recorded 2026-05-02:** scope locked to **Path A** - surface what is already collected, no new fields, no dollar conversion, no cross-stage aggregation. Concretely: integrate columns from existing `pr_metrics.html` (Coder, Model, Duration, FIX Iterations, Exit Reason) inline into DONE list row rendering. Backend reuses existing `_recent_repo_metrics_payload` lookup, joining by `task_id` in DONE list serializer. Optionally deprecate or repurpose the standalone `pr_metrics.html` panel once DONE-row integration covers the use case (decision deferred - keep both panels initially as dual surface for transition period).

  **Out of scope for this OBS:**
  - Dollar-cost conversion of `tokens_in`/`tokens_out`. Separate decision; ties to plugin cost model architectural work in Vision A territory. Subscription users see token counts (marginal cost zero); API users see token counts (real billing). Both groups served by raw token display until cost model formalized.
  - Cross-stage cost aggregation (planner/reviewer/qa stages). Currently only `coder` stage emits RunRecords (per `src/metrics.py:32` comment); aggregation premature.
  - Subscription utilization fraction display. Vision A territory.

  **Estimate:** ~1 PR, ~3 daemon-hours. **Sprint 15 polish bucket alongside OBS-BH and OBS-BJ.** Possibly bundle all three into single PR if scope stays compact.

- OBS-BJ (DONE list reverse chronological sort, default newest first): **OPEN, low severity, observed 2026-05-02 by operator** - DONE list currently sorts oldest-first (PR-067, 068, 069, ...). At 175+ DONE PRs accumulated, finding recent merges requires scrolling to the bottom. Fix: reverse default sort to newest-first, optionally add a sort toggle in the panel header. Storage: in-memory list reversal at render time (no schema migration). Alternative: simple sortable column header for `merged_at` timestamp if present. ~1 PR, ~1-2 daemon-hours. **Sprint 15 polish bucket. Bundle with OBS-BH and OBS-BI in same sprint, possibly same PR if scope stays small.**

- OBS-BK (WATCH→FIX trigger logic short-circuits when CI is PENDING): **OPEN, medium-high severity, observed 2026-05-02 morning on PR-227c** - confirmed by code inspection at `src/daemon/handlers/watch.py:180-189`. The elif chain evaluates `ci == FAILURE → handle_fix()` first, then `ci == PENDING → pass`, then `review == CHANGES_REQUESTED → handle_fix()`. Python short-circuits on first match: if CI is PENDING (CI hasn't completed yet, e.g. queue-stuck or slow-runner), the `pass` branch wins and the review-driven FIX trigger is never reached, even when codex has explicitly posted CHANGES_REQUESTED. Result: PR sits in WATCH waiting for CI to converge while codex feedback remains unaddressed. In production case 2026-05-02, combined with OBS-BL circuit-breaker absence, this caused a 4-hour WATCH↔HUNG loop on PR-227c.

  **Fix:** swap the elif ordering so `review == CHANGES_REQUESTED → handle_fix()` runs **independent of CI state** (as long as new feedback is detected via existing `_has_new_codex_feedback_since_last_push()` check). CI PENDING should not block FIX-on-review - review feedback addressing is orthogonal to CI completion, and the next push from FIX will trigger a new CI run anyway.

  **Care needed:** existing logic intentionally does not trigger FIX when CI is PENDING for a freshly-opened PR (CI hasn't even started). The fix must distinguish "fresh PR, CI not started" from "CHANGES_REQUESTED present on already-running CI". Existing `_has_new_codex_feedback_since_last_push()` already implements this distinction correctly (returns NEW only if codex has posted comments after last push); the bug is purely in elif ordering masking it. ~1 PR, ~3 daemon-hours including regression test. **Sprint 14.**

- OBS-BL (WATCH↔HUNG escalation loop without circuit breaker): **OPEN, medium-high severity, observed 2026-05-02 morning on PR-227c** - when WATCH cycle hits 20-minute timeout and review is stale, daemon transitions to HUNG, posts `@codex review` re-trigger via ESCALATE path, transitions back to WATCH, resets the 20-minute timer. No circuit breaker on N-th attempt. Production observation 2026-05-02: PR-227c cycled through this loop 4+ times over 4 hours before operator manually intervened (cancelled blocking GHA run). Each cycle posts another `@codex review` mention (small Anthropic API cost per cycle, plus codex API/UI noise), and produces no progress because the underlying blocker (CI concurrency lock - see OBS-BM) was external and unaffected by re-triggers.

  **Fix:** introduce circuit breaker on stale-review re-trigger attempts. After N consecutive HUNG→ESCALATE→WATCH→HUNG cycles for the same PR (suggested N=3, configurable), daemon transitions PR to ESCALATED state (per Cancellation policy) instead of looping. Circuit breaker resets when fresh push or fresh review activity observed. **Distinct from OBS-BB FIX-no-push deadlock** (which is about coder claiming fix without pushing); this is about stale-review WATCH retrigger loop. Same architectural pattern (bounded retry with operator escalation as exit), different trigger.

  **Estimate:** ~1-2 PRs, ~3 daemon-hours. **Sprint 14, alongside OBS-BB.**

- OBS-BM (long CI PENDING duration without classification - concurrency-lock-stuck vs slow-runner vs infra-down): **OPEN, medium severity, observed 2026-05-02 morning on PR-227c** - extension of OBS-BC scope. OBS-BC covers CI **failure** classification (infra-fail vs real-fail). OBS-BM covers CI **stuck-pending** classification: when CI has been PENDING for an extended duration (e.g. >15 minutes for repos where typical run completes in <5 min), daemon should distinguish:
  1. **Concurrency-lock-stuck:** GitHub Actions workflow concurrency:group serializes integration job behind another run. If the blocking run is itself stuck (abandoned PR, infra issue), this PR will never converge. Detected by: GHA API query for `waiting_on` job, check if the blocking run is in-progress more than 2x typical duration.
  2. **Slow-runner:** runner picked up the job but is processing slowly. Detected by: job is in_progress but no log activity for >5 min (heartbeat absent).
  3. **Queued-no-runner:** GHA queue backlogged, no runner picked the job. Detected by: job is queued (not in_progress), waited > expected.
  4. **Workflow file error:** workflow definition itself broken. Detected by: GHA returned error event on workflow start.

  **Production case 2026-05-02:** PR-227c integration job stuck PENDING 4 hours due to concurrency-lock-stuck on prior main-branch push run that was itself hung (case 1). Daemon held in WATCH polling, never escalated, never alerted operator. Operator only noticed after 4 hours via dashboard observation.

  **Fix:** expand WATCH cycle CI status check to include duration analysis. If `ci.status == PENDING AND duration > threshold`, daemon classifies stuck cause via GHA API queries, surfaces classified state in event log (`[WATCH] PR #N CI stuck PENDING (concurrency-locked on run #M, blocking run hung 3h+)`), and after operator-configured duration threshold transitions PR to ESCALATED with classification preserved per OBS-BE storage. Also: optional auto-recovery path for concurrency-locked case (`gh run cancel <blocking_run_id>` if blocking run idle > N hours, requires operator-permission flag).

  **Estimate:** ~2-3 PRs, ~5 daemon-hours. **Sprint 14, alongside OBS-BC.** Strategic significance: completes the daemon's CI awareness model - failure classification (OBS-BC) plus stuck-pending classification (OBS-BM) gives daemon a robust view of CI health regardless of which bad-state CI is in.

### Memory items still actionable

- push_count desync: **CLOSED** - PR-195 reconciled UI metric with GitHub Commits tab via observed_head_shas tracking.
- AGENTS.md prohibit draft PRs: **CLOSED** - PR-196 updated AGENTS.md text + handler-side enforcement via `gh pr ready`.
- All known memory items from 2026-04-28 session are now closed or deferred to long-term backlog.

### Production lessons (from 2026-04-28 session, recorded for future reference)
- **Production config.yml gap:** ~15 daemon overrides существуют только как local file on production host, never committed. `git reset --hard` revert'ит их к upstream defaults. Production behavior не reproducible from git alone. Action: либо commit `config.production.yml`, либо move to env vars, либо deploy step с config diff verification.
- **Deploy checkout vs daemon `/data/repos/.../tasks/` distinction:** daemon работает с собственным clone в docker volume. Deploy-time `~/pipeline-orchestrator/tasks/` может содержать другой набор файлов. Don't conflate the two when investigating queue discrepancies.
- **N>=3 verification reruns rule:** для race condition fixes один зелёный CI run не валидация. Тест мог проходить на lucky timing до фикса. Require 3+ green reruns на same commit перед merge.
- **Single-step on stateful operations:** rebase, merge, deploy не должны быть в `&&` chains. Each command output must be reviewed перед next.
- **Read file before writing patch:** during long debug sessions, мой cached snapshot drift'ит от user actual state. Always re-read user current file перед generation patches.

---

## Cancellation policy (Wave 5 architectural decision, 2026-05-02)

OBS-BE expanded scope captures cause-of-CANCELED preservation (the storage and surfacing question). This section captures the orthogonal behaviour question: when daemon hits ESCALATE/CRASH/TIMEOUT/INFRA, should it halt the queue (state=ESCALATED) or record the cause and continue with next pickable task (state=CANCELED). Decision depends on operator availability and queue dependency structure. Operator preference established 2026-05-02: CANCELED with cause preserved is acceptable terminal state, not failure mode.

### Behaviour matrix

Trigger × operator availability → daemon action.

| Trigger | Operator: Active | Operator: Off |
|---|---|---|
| ESCALATE marker (deliberate) | state=ESCALATED, halt task | state=CANCELED with cause, continue with next pickable |
| CRASH (uncaught exception, non-zero exit) | state=ESCALATED, halt task | state=CANCELED, continue |
| TIMEOUT (per-cycle, per-PR, per-FIX, planned-PR budget) | state=ESCALATED | state=CANCELED, continue |
| INFRA (gh/network/redis/auth subsystem fault) | retry-aware per OBS-BC future fix; on final failure ESCALATED | retry-aware; on final failure CANCELED, continue |

Justification for off-hours CANCELED + continue across all four categories: in worst case (queue is fully blocked because failed task has many dependents) daemon goes IDLE same as ESCALATED would; in best case (independent branches in queue) daemon makes progress instead of stalling the entire night. CANCELED-with-continue is strictly no worse than ESCALATED-and-halt in any scenario, given cause is preserved per OBS-BE expanded.

### Operator availability signal

**Layered design:**

Layer 1 (always present): manual override. 3-state switch in dashboard top bar: `active` / `auto` / `off`. Click flips immediately. Persisted in Redis under `presence:override:global` (single global presence per orchestrator instance, not per-repo).

Layer 2 (when override=auto): pluggable `SignalSource` Protocol. v1 ships with two implementations:
- `HeartbeatSource`: introspection of UI requests. Daemon already routes XHR/SSE through `web/app.py`. If any operator-facing request received within last `presence_heartbeat_window_min` (default 30 min), considered active.
- `ActiveHoursSource`: optional config field `daemon.operator_active_hours: "HH:MM-HH:MM Timezone"` (e.g. `"09:00-22:00 Europe/Rome"`). If unset, source returns "no opinion". If set, returns active when current time within window.

**Composition rule:** if override = `active` or `off`, that wins (heartbeat and active hours ignored). If override = `auto`, OR-merge: active if ANY source says active. Conservative default; false positive on active is safer than false negative.

**Failure-safe fallback:** if Redis unavailable to read override or heartbeat history, default to `active`. Pre-policy behaviour. Safer to halt on unknown than to silently CANCEL.

Layer 3 (vision, NOT v1): additional `SignalSource` implementations such as `CompanionAppSource` (Vision C, see below), `CalendarSource` (Google/Outlook OAuth busy/free), `WebhookPresenceSource` (generic external automation input). Protocol designed to allow these; no implementations in v1. Reference for design hook stability.

### Human Availability indicator (non-negotiable UI requirement)

Status visibility is the operator preference that drives this entire section. Must be visually prominent at all times.

**Placement:** persistent chip in dashboard top bar, visible across all views and all repo cards.

**Visual states:**
- Green chip "Active": override=active OR (override=auto AND any signal source says active).
- Red chip "Off": override=off OR (override=auto AND all signal sources say off).
- Yellow optional intermediate "Idle (auto)" if want to distinguish "auto-mode currently sleeping" from "explicit off". v1 may collapse Yellow into Red for simplicity; revisit if operator wants distinction.

**Interaction:**
- Click on chip: inline 3-state override switch opens (Active / Auto / Off). Click selection, switch persists.
- Hover or expand: reasoning text. Examples: "Active because heartbeat at 14:23 from /api/states", "Off because no UI activity since 02:18 AND outside active_hours 09:00-22:00 Europe/Rome", "Active because manual override".

**Invariant:** what operator sees on the chip is what daemon used for the most recent ESCALATED/CANCELED decision. No surface drift between displayed state and behaviour state. If this invariant is hard to maintain due to async update lag, daemon decisions log the chip state at decision time so post-hoc forensics can reconcile.

**Refinement idea (recorded 2026-05-02, not v1 commitment):** operator proposed a richer 4-state visual scheme that maps onto the same 2-bucket behaviour:

| Visual | Meaning | Source signal | Daemon behaviour |
|---|---|---|---|
| Green | Recently active | heartbeat within window OR override=active | ESCALATE on trigger |
| Yellow | In active hours but not currently interacting | active_hours window matches AND heartbeat stale | ESCALATE on trigger |
| Red with white stripe | Do-not-disturb (manual) | override=off (deliberate flip, e.g. focus mode) | SKIP on trigger |
| Cross / X | Outside active hours, no manual override | active_hours window does not match AND heartbeat stale AND override=auto | SKIP on trigger |

Green and Yellow oscillate based on heartbeat; transitions are smooth and frequent during the day. Red and Cross are stable until operator intervention or schedule boundary. The behavioural distinction is binary (ESCALATE vs SKIP), but the visual gives operator informative context about WHY daemon is in current mode without clicking through. Switching between manual states (Red) and automatic states (Cross/Green/Yellow) happens by clicking the chip; switching between Green and Yellow happens automatically as heartbeat ages.

**Why recorded as refinement:** v1 design with 3 visual states (Green/Yellow-intermediate/Red) is mechanically simpler and ships first. Operator's 4-state scheme adds the Red-vs-Cross distinction (manual-do-not-disturb vs auto-off-hours) which improves operator awareness but is incremental UX polish rather than core behaviour change. Promote to v1 if implementation cost stays similar; otherwise ship as v1.1 refinement after baseline ships.

**Secondary insight from 4-state distinction (added 2026-05-02):** Red and Cross differ not only visually but in expected wait time before operator returns. Red is a deliberate flip (focus mode, meeting, brief pause); operator likely returns within 30 to 90 minutes. Cross is schedule-driven off-hours (night, weekend); operator returns at the next active_hours boundary, likely 6 to 12 hours later. CANCELED accumulation differs by an order of magnitude between the two. Implication for v1.1 dashboard surfacing: secondary sort criterion can use the off-state-source to differentiate "short pause backlog" (small, urgent triage) from "overnight backlog" (large, needs grouped review panel rather than raw list). Operator returning from Red sees one or two CANCELED cards inline; operator returning from Cross sees a digest summary first, raw list on demand.

### Dependency-aware blocked_set computation

When daemon decides CANCELED + continue:

1. Failed task = T.
2. `blocked_set = {T} ∪ {tasks where Depends on transitively reaches T}` computed from current queue task headers.
3. `pickable_set = current_queue \ blocked_set`.
4. If `pickable_set` empty: daemon enters IDLE until either operator triages (override→active reveals dashboard with CANCELED + dependents) OR queue gains new tasks via upload. Same outcome as ESCALATED in this scenario; no regression.
5. If `pickable_set` non-empty: daemon proceeds with next entry per existing selector logic. Independent branches keep moving.

Note: dependency graph already exists for queue ordering (Depends on field is parsed in idle.py for selector). This computation reuses the same parse, no new data model required.

### Dashboard surfacing on operator return

When operator wakes (or override flips active) and reviews dashboard:

Sort order for tasks needing triage (CANCELED + ESCALATED filtered view):
1. ESCALATED (any) at top: "immediate, halt'd queue".
2. CANCELED descending by `dependents_count`: most-blocking first ("5 PRs blocked").
3. CANCELED ascending by timestamp within same dependents_count (older first).

Each CANCELED card shows: category icon (CRASH/ESCALATE/TIMEOUT/INFRA per OBS-BE), reason text (preserved per OBS-BE storage), `dependents_count` badge if non-zero, "Open in event log" button to jump to captured range.

### Welcome-back digest (v1.1 refinement, recorded 2026-05-02)

Beyond the sorted triage list, operator returning after extended absence benefits from a **summary of what happened while away**, similar to "summary while you were away" patterns in mail and chat apps. Raw event log scrolling for hours of activity is a poor catch-up surface.

**Trigger logic:**
- Compute `away_duration = now - last_operator_active_timestamp`.
- If `away_duration > 60 min` AND digest contains at least one notable event: surface digest modal on first dashboard load after operator return.
- Otherwise (short pause, no notable events): no modal, dashboard renders normally.

**Digest content sections:**

1. **Completed while away:** count of merged PRs, top 3 by recency or by significance (linked PR numbers), aggregate cost-per-PR if metrics available.
2. **Awaiting your review:** CANCELED entries (sorted per main triage rules), ESCALATED PRs (always top priority).
3. **Currently in flight:** which repos are CODING/WATCH/FIX right now, last state transition timestamp.
4. **Notable events:** rate limit hits, INFRA failures (with retry outcomes), ESCALATED transitions, repos that went HUNG and recovered.

**Format:** HTMX modal with two actions: "Dismiss" (close modal, dashboard normal view), "Open triage view" (close modal, navigate to CANCELED+ESCALATED filtered view per Dashboard surfacing rules above).

**Storage model:** digest computes on demand from existing data sources at modal-open time. No persistent digest storage needed:
- Completed PRs: query `metrics:run:*` records where merge timestamp falls in away window.
- CANCELED list: scan `cancellation:{repo_name}:*` keys per OBS-BE.
- ESCALATED tasks: enumerate state machine for ESCALATED state.
- In-flight: current `pipeline:{repo_name}` Redis state per repo.
- Notable events: scan event log for last away_duration window, filter to flagged event categories.

**Differentiation by off-state source (per 4-state refinement secondary insight above):** if return is from Red (short pause), digest threshold raises to e.g. 90 min so brief Red flips do not trigger modal. If return is from Cross (overnight off-hours window), digest triggers eagerly because operator typically wants full catch-up.

**Estimate:** 1 PR additional to Cancellation policy v1.1, ~3 daemon-hours. Endpoint `/api/digest?since=<timestamp>` plus modal template plus trigger JS on dashboard load. Bundle with the 4-state visual refinement when v1.1 ships.

### Transition behaviour at availability boundary

**Variant A confirmed (operator decision 2026-05-02):**

When daemon transitions auto-mode from off to active (heartbeat detected, or active_hours window opens, or override flipped):
- No automatic state mutations on existing CANCELED tasks.
- Operator sees Human Availability chip flip green; dashboard sort surfaces high-priority CANCELED entries; operator triages manually.

Rationale: minimize automation across time boundaries. Mutating state from time alone breaks event log determinism (event without causing trigger from operator or coder action). Visible UX (chip flip + dependents_count sort) achieves same triage outcome cleaner.

Variant B (auto-promote CANCELED → ESCALATED on active transition) deferred indefinitely. Implementation hook can be added later if Variant A proves insufficient in practice. Cost of revisiting later is small because the storage model already preserves cause; promotion is just a state transition over existing data.

### Out of scope for v1

- Push notifications (Telegram, email, OS native). Notification layer is separate concern, not part of orchestrator core. Companion app (Vision C) is the natural place for OS native notifications.
- External signal source plugins beyond Protocol design. Teams presence, calendar busy/free, companion app pings: each is a separate v2+ deliverable.
- Per-repo presence overrides ("repo X always halts on ESCALATE regardless of operator status"). Single global presence, single global behaviour. Per-repo override added only if specific scenario demands it.
- Auto-retry CANCELED on timer. Re-upload remains operator-initiated; otherwise infinite loops on permanently-broken specs.
- Predictive presence (calendar busy = scheduled meeting, daemon paused early). Heartbeat already covers this scenario implicitly (operator in meeting → not interacting with dashboard → eventually off).

### Estimate

3 PRs over Wave 5, on top of OBS-BE expanded (3-4 PRs ~7h):

1. `SignalSource` Protocol + ManualOverrideSource + HeartbeatSource + ActiveHoursSource + composition + Redis presence keys + failure-safe fallback. ~4 daemon-hours.
2. Human Availability indicator UI (chip placement + 3-state override switch + reasoning expand). ~2 daemon-hours.
3. Dependency-aware blocked_set + dashboard sort by dependents_count + CANCELED inline-expand integration with OBS-BE cause display. ~3 daemon-hours.

Total Cancellation policy v1: ~9 daemon-hours over 3 PRs.
Combined with OBS-BE expanded: ~16 daemon-hours over 6-7 PRs.
Wave 5 cumulative including existing OBS-AW + OBS-BB + OBS-BC: approximately 1.5-2 daemon-days when run alone.

### Cross-references

- **OBS-BE expanded:** storage model and detection paths for cause preservation. This section references the four categories (CRASH/ESCALATE/TIMEOUT/INFRA) defined there.
- **OBS-AW:** per-repo HUNG recovery button. Adjacent UX work for stuck states; Cancellation policy assumes HUNG recovery is independent.
- **OBS-BB:** FIX no-push deadlock recovery. Adjacent stuck-state work.
- **OBS-BC:** CI infra-failure classification. Determines retry vs ESCALATED vs CANCELED for INFRA trigger category.
- **Vision C (Orchestrator Companion App):** future `SignalSource` implementation candidate. Design hook in this section's Layer 3 keeps companion-app integration mechanically simple when product surface expands.

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

## Implementation Plan (post-audit, 2026-04-29) — SHIPPED 2026-04-29..2026-05-01

**Status as of 2026-05-01:** all 28 numbered PRs (PR-180..PR-207, with subdivisions PR-191a/b and PR-192a/b/c yielding 31 task files) are merged in production. Below is the original plan annotated with as-shipped notes.

**Принцип был:** finish before extend. **Pre-multi-repo readiness FIRST**, потом большие refactor. Принцип сработал — multi-repo readiness shipped перед Foundation Sprint без отвлечения на Sprint 10 SoT, PAUSED removal, или Thompson Sampling.

### Critical batch — Pre-multi-repo readiness (PR-180..PR-185) — SHIPPED

- **PR-180 SHIPPED** REST replacement for `gh pr list --json statusCheckRollup` (OBS-AC Leverage 2). 23 occurrences of REST check-runs in github_client.py. WATCH/MERGE polling now on REST core quota (5000/hr).
- **PR-181 SHIPPED** Remove tasks/QUEUE.md from git tracking. Closes OBS-2 drift. Daemon regenerates QUEUE.md from PR-*.md files each IDLE cycle.
- **PR-182 SHIPPED** diagnose_error bypass для git infra and network errors. OBS-4 closed.
- **PR-183 SHIPPED** Redis pub/sub upload trigger. Daemon main loop wakes via combined `asyncio.wait` on sleep + subscriber. Scope expanded в PR-205 (control commands wake) и PR-206 (settings save wake).
- **PR-184 SHIPPED** Adaptive IDLE polling. After 3 consecutive IDLE cycles without work — slow до 300s. Wake immediately on pub/sub event.
- **PR-185 SHIPPED** Daemon GraphQL points consumed observability.

**Exit criteria HIT:** GraphQL burn measurably reduced. Upload → daemon wake observed <2s in production.

### Important batch — Stability fixes (PR-186..PR-191) — SHIPPED

- **PR-186 SHIPPED** Recovery skip crashed-task-retry.
- **PR-187 SHIPPED** Coder exit=0 diagnostic handler. Discriminates branch-missing vs branch-exists-no-PR vs no-branch.
- **PR-188 SHIPPED** Codex bot error comment detection. WATCH polls for `chatgpt-codex-connector[bot]` "Something went wrong" messages and re-triggers `@codex review`.
- **PR-189 SHIPPED** OBS-Z fix: Codex EYES race window. Pre-push state check + EYES-specific stale threshold.
- **PR-190 SHIPPED** Asymmetric push verification in fix.py normal path.
- **PR-191a/b SHIPPED** ETag conditional requests across github_client.py (OBS-AC Leverage 3). 36 ETag occurrences in code. Most polling cycles now return 304 Not Modified, not counted against rate limit.

**Exit criteria HIT:** all known active failure modes either auto-recover or surface clearly.

### Multi-repo readiness batch (PR-192..PR-194) — SHIPPED

- **PR-192a SHIPPED** AGENTS.md section-marker append framework (`src/onboarding/markdown_sections.py`). Marker pairs `<!-- pipeline-orchestrator: managed BEGIN/END section_name -->` with validation.
- **PR-192b SHIPPED** Apply section-marker framework to pipeline-orchestrator's own AGENTS.md (`src/onboarding/agents_md_template.py`). 10 managed sections.
- **PR-192c SHIPPED** Onboarding doc + dry-run reconciliation mode. POST endpoints `/onboarding/preview` (dry-run) and `/onboarding/apply` (write). `reconcile_agents_md` function with `dry_run=True` default.
- **PR-193 SHIPPED** Multi-repo state isolation audit + fixes. Audit document at `docs/multi-repo-audit-2026-04-29.md`. Per-repo Redis keys verified, slug collision handling validated, tasks/ directory isolation verified, event log isolation verified. **Outcome:** sequential run_cycle in main loop identified as remaining blocker → PR-207.
- **PR-194 SHIPPED** Production config tracking — `config.production.yml` overlay file approach.

**Exit criteria PARTIAL → HIT 2026-05-01:** Multi-repo dashboard works correctly (PR-207). Production config reproducible from git + override. Onboarding of external repos pending current session work (megaraid + sms-gateway).

### Polish batch (PR-195..PR-204) — SHIPPED

- **PR-195 SHIPPED** push_count desync fix. UI metric reconciled with GitHub Commits tab via observed_head_shas tracking.
- **PR-196 SHIPPED** AGENTS.md prohibit draft PRs. Text in managed sections + handler-side `gh pr ready` enforcement.
- **PR-197 SHIPPED** Document WATCH STALLED substate.
- **PR-198 SHIPPED** PipelineState.MERGE dead value cleanup.
- **PR-199 SHIPPED** Event text clarity pass.
- **PR-200 SHIPPED** Task header validation — synonyms map and multi-error report.
- **PR-201 SHIPPED** Dashboard control row visual consistency. Pause/Stop/Upload aligned to flat-icon style with consistent hover.
- **PR-202 SHIPPED** WATCH adaptive polling — slow-start, fast-tail. Inverted from standard exponential backoff.
- **PR-203 SHIPPED** Compact resource limits row with tooltips. 4 chips: GH REST, GH GraphQL, Claude 5h, Claude weekly.
- **PR-204 SHIPPED** Structured per-PR outcome logging at `/data/analytics/<year>-<month>.jsonl`.

**Exit criteria HIT:** UI polished, documentation caught up, dead code removed.

### Post-polish additions (PR-205..PR-207) — SHIPPED 2026-04-30..2026-05-01

These weren't in original 2026-04-29 plan but emerged during execution:

- **PR-205 SHIPPED** Control commands publish wake events. Operator clicks (Play/Pause/Stop) wake daemon via Redis pub/sub instead of waiting for next poll cycle.
- **PR-206 SHIPPED** Settings save publishes wake events. Same pattern as PR-205 for settings updates.
- **PR-207 SHIPPED** Parallelize per-repo run_cycle. Replaces sequential `for runner: await run_cycle()` with `asyncio.create_task(runner.run_cycle())` per-runner. `in_flight` dict prevents pile-up. **This was the final multi-repo blocker** — without it, one repo's CODING (30+ min) would starve all other repos' WATCH polling.

### Deferred (sprint-scale, не в ближайшем 2-week плане)

- **Sprint F2.1 SoT direct instructions:** PR-167/168/169-equivalent — feature flag guarded refactor of how daemon talks to coder. **Still deferred** — текущая indirection через AGENTS.md works adequately.
- **Sprint F2.2 PAUSED removal:** **Still deferred** — `awaiting_start` flag не shown to be necessary. PAUSED enum still in code but works.
- **Sprint F3.2 Thompson Sampling:** **Still deferred** — epsilon-greedy adequate. Need 50+ merged PRs across both coders before posterior actually informs (current data: ~250 merged total but mostly Claude-pinned for stability during deploy).
- **GitHub App migration (OBS-AC Leverage 6):** **Still deferred** — diet (PR-180/PR-184/PR-191/PR-202) proves sufficient for solo operator. Revisit only if quota exhaustion returns OR third-party adoption becomes relevant.
- **Manifest flow for third-party adoption:** **Still deferred** — depends on App migration.
- **Nightly e2e self-testing for pipeline-orchestrator:** **Still deferred** — production stable.
- **OBS-5 gh credential helper instrumentation:** **Still deferred** — intermittent, low-impact.
- **Resource limit history charts (modal-on-click):** **Still deferred** — visual chip + reset time gives enough situational awareness.

---

## Sprint 12 — Foundation Sprint (PR-208..PR-236, generated 2026-05-01)

**Status:** task specs generated and validated. 36 task files in `/mnt/user-data/outputs/foundation-tasks/`. Not yet uploaded to daemon.

**Estimated duration:** 2-3 days at daemon's measured 15-20 PR/day throughput.

**Strategic purpose:** internal architecture cleanup before declaring multi-repo onboarding production-ready for non-author users. Three concerns:

1. **God-class decomposition** — `idle.py`, `web/app.py`, `github_client.py`, and `runner.py` accumulated to 800-1500 LOC each over ~250 PRs. Future PR scope decisions become harder as files grow.
2. **Atomic primitive missing** — `idle.py::_select_next_task` is the queue-selection-and-claim primitive but is currently inline. Extracting it makes future selector work (Thompson Sampling, force_coder override) independent of god-class state.
3. **Regression test gaps** — multi-repo isolation properties (PR-193 audit) verified manually; no automated regression coverage. Foundation Sprint adds regression tests for properties currently held by audit.

### Sprint composition (36 PRs)

- **Batch A — Guardrails (6 PRs, PR-208..PR-213):** regression tests for properties already true. Lock in current behavior before refactoring.
- **Batch B — No-behavior cleanup (3 PRs, PR-214..PR-216):** Redis keyspace consolidation, diagnosis module extraction, docstring clarity. No semantic change.
- **Batch C — Centralize behavior (9 PRs, PR-217..PR-223 with subdivisions):** atomic primitive extraction, AGENTS.md template centralization, repo-config validation centralization, error categorization. PR-218 → PR-219a → PR-219b → PR-220 are critical path.
- **Batch D — Split god modules (12 PRs, PR-224..PR-230 with subdivisions):** decompose `idle.py`, `web/app.py`, `github_client.py`, `runner.py`. Each split is mechanical move + import rewiring.
- **Batch E — UI polish + observed bugs (6 PRs, PR-231..PR-236):** polish queue from operator observations, plus PR-236 OBS-AR fix (event log spam from 304 + cached=None bug in `_etag_get`).

### Critical path

`PR-208 → PR-213 → PR-218 → PR-219a → PR-219b → PR-220 → PR-230` — 7 PRs serial, ~5 hours clock time at daemon throughput. Other 29 PRs parallel-eligible.

### Task file specs

All 36 task files in `/mnt/user-data/outputs/foundation-tasks/`. Operator copies to `~/pipeline-orchestrator/tasks/` and uploads via UI. Each spec contains: branch name, type, complexity, priority, coder eligibility, depends-on, files-to-modify, files-NOT-to-touch, success criteria, fixture list, sample assertions.

### Sprint plan document

`/mnt/user-data/outputs/foundation-tasks/SPRINT_PLAN.md` contains: dependency graph, parallel batches, risk register, acceptance criteria for each batch.

---

## Multi-repo onboarding readiness (status as of 2026-05-01)

All previously-blocking prerequisites are shipped. Multi-repo onboarding for external repos can proceed.

**Shipped prerequisites:**

- ✅ ETag conditional requests (36 occurrences) — PR-191a/b
- ✅ REST check-runs replacing statusCheckRollup (23 occurrences) — PR-180
- ✅ Adaptive WATCH polling (slow-start, fast-tail) — PR-202
- ✅ Adaptive IDLE polling — PR-184
- ✅ AGENTS.md reconciliation framework (`reconcile_agents_md` + `/onboarding/preview` + `/onboarding/apply`) — PR-192a/b/c
- ✅ Multi-repo state isolation audit + fixes — PR-193
- ✅ **Parallel per-repo run_cycle in main loop** — PR-207 (the critical final piece)
- ✅ Production config overlay file — PR-194

**Open architectural gaps surfaced during onboarding planning (2026-05-01):**

- ⚠️ AGENTS template scope leakage (orchestrator self-references in managed sections) — workaround via user-note section in target repo's AGENTS.md; long-term fix is PR-FUTURE-1.
- ⚠️ No per-repo config (coverage gate hardcoded in template) — workaround via project-specific Testing section in user's AGENTS.md; long-term fix is PR-FUTURE-2.
- ⚠️ No semantic conflict resolution at onboarding (mechanical merge only) — workaround via manual operator review; long-term fix is PR-FUTURE-3.
- ⚠️ scaffolder creates exit-0 stub for `scripts/ci.sh` — workaround via manual creation pre-onboarding; long-term fix is PR-FUTURE-4.

**Active onboarding subjects:**

- megaraid-dashboard (27 src files / 27 test files, Alembic, hardware target). Reconciled AGENTS.md prepared at `/mnt/user-data/outputs/AGENTS-megaraid-dashboard.md`.
- sms-gateway-v2 (24 src / 51 test files, Dockerfile, ModemManager+D-Bus). Reconciled AGENTS.md prepared at `/mnt/user-data/outputs/AGENTS-sms-gateway-v2.md`.

**Pre-onboarding checklist:** `/mnt/user-data/outputs/foundation-tasks/REPO_PREP_CHECKLIST.md`. Manual changes required per repo: AGENTS.md replacement, scripts/ci.sh creation with real validation (not stub), .gitignore additions.

**Sequencing decision:** sequential onboarding (megaraid first, observe 30 min, then sms-gateway). Foundation Sprint can run in parallel with onboarding once repos are stable in IDLE.

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

### Orchestrator Companion App (Vision C, added 2026-05-02)

Cross-platform desktop client surfacing daemon state and presence outside the browser dashboard. Concept emerged from Cancellation policy discussion (2026-05-02) where operator noted that explicit availability signal supplied by operator is more robust than introspection-based heartbeat. Companion app would supply that explicit signal natively.

**Form factor (initial sketch, not committed):**
- Desktop client. Tauri preferred over Electron for binary size and Rust footprint. Cross-platform Linux/macOS/Windows.
- Single primary window: Active/Off presence toggle (large, prominent), top 3 actionable items (CANCELED with high `dependents_count`, ESCALATED PRs, recent INFRA failures), live count of in-flight repos.
- Pings daemon `/api/presence/heartbeat` every 60s when window focused; stops on minimize/quit. Implements `CompanionAppSource` per `SignalSource` Protocol from Cancellation policy section.
- Optional native OS notifications on ESCALATED transition (opt-in per platform).

**Mobile companion (iOS/Android):** later phase. Requires push notification backend (FCM/APNS) which is non-trivial for self-hosted deployments. Defer until desktop client validates the workflow.

**Why deferred and why design hook only in v1:**
- Wave 5 Cancellation policy ships `SignalSource` Protocol that already accommodates plugging in companion-app source. Mechanical integration cost when companion app is built later: write one Source class and register it.
- Companion app itself is product-surface expansion, not orchestrator core. Deserves attention only when heartbeat + manual override v1 surfaces real gaps in operator workflow.
- Build only if and when self-hosted operator base demands cross-machine awareness (notebook closed, daemon on home server, status visible from phone or work machine).

**Estimate:** approximately 2-3 weeks for initial desktop client; a separate subproject outside Foundation Sprint, Wave 1-7, and Vision A streams. Not a roadmap PR series. Revisit when Vision A ships and product is stable enough that auxiliary surfaces are worth the maintenance overhead.

### Conversational morning triage (Vision D, added 2026-05-02)

Endgame product surface where operator interacts with orchestrator through natural conversation, primarily over Telegram (text + voice messages), instead of (or alongside) browser dashboard. Concept emerged from operator framing 2026-05-02: wake up, open Telegram, see overnight summary already waiting, reply with voice or text to triage. No keyboard, no Safari tabs, no manual UI.

**Why this matters strategically:**

Vision D is the **logical consumer** of the substrate already designed in Cancellation policy + OBS-BE expanded. The same `/api/digest`, `/api/cancellation/{repo}/{task}`, `/api/presence/*` endpoints that power browser modal also power conversational surface. JSON identical, render different: dashboard renders modal, Telegram bot renders narrative messages, future voice agent renders TTS.

Strategic moat reinforcement: a competing orchestrator now needs to reproduce not only routing intelligence and measurement data, but also a structured-digest substrate without which voice/conversational layer is impossible to build cleanly. Voice surface is downstream proof that internal API design is right.

**Stage gating (each stage shippable independently, each adds value):**

**Stage D.1 — Telegram digest bot (close horizon, ~1-2 weekends).** Simple push notification flow: when daemon detects operator long-away return condition (per Cancellation policy welcome-back digest trigger), bot pushes a text message via Telegram with the same content the modal would show. Operator reads, switches context. No interactivity beyond reading. Stack: existing `python-telegram-bot` library, daemon registers a webhook endpoint, bot polls `/api/digest` on schedule or on operator-presence-transition Redis pub/sub. Implementation cost ~6-8 hours.

**Stage D.2 — Telegram interactive triage (medium horizon, ~1-2 weeks).** Bot accepts text and voice replies. Voice transcribed via Whisper (local on DGX Spark to keep self-hosted property, OR OpenAI Whisper API for managed deployment). Reply intent parsed into action options shown for each CANCELED/ESCALATED entry: "re-upload", "permanently cancel", "rewrite spec", "show me the cause", "defer to tomorrow". Bot translates operator intent into orchestrator API calls. Implements `TelegramSessionSource` per `SignalSource` Protocol so a recent voice/text exchange counts as active heartbeat (presence flips green automatically when operator engages with bot). Implementation cost ~3-5 days.

**Stage D.3 — full voice agent with reasoning narrative (far horizon).** Bot/agent generates spoken digest narratives ("PR-220 cancelled three hours ago because... it blocks 5 dependent PRs... last similar issue resolved by re-upload with production config... want me to do that?"), accepts free-form voice instructions ("re-upload PR-220 and skip PR-225 for today"), parses ambiguity and asks clarifying questions ("you said skip PR-225, that has 3 dependents, are you sure?"). Voice tone conversational rather than corporate. Implementation requires LLM-based dialogue manager on top of orchestrator API, a more substantial subproject. ~3-4 weeks.

**Stack rationale:**

Telegram-first (over iOS Shortcuts, Twilio/IVR, custom WebRTC):
- Operator already heavy Telegram user (sms-gateway-v2 system memory).
- Cross-platform (phone, desktop, web) without per-platform build.
- Voice messages built-in primitive, no STT pipeline before custom processing.
- Free for personal scale; bot infrastructure is one process.
- Self-hosted compatible; bot runs as a sidecar service in same docker-compose stack.

iOS Shortcuts + Siri considered: native feel, but iOS-only and brittle to OS updates. Defer as alternative client when D.2 ships.

Twilio/IVR considered: closest to literal "позвонил по виртуальному помощнику", but adds telephony cost and infrastructure complexity disproportionate to MVP. Defer as Stage D.4 if Vision D proves valuable enough.

**Cross-references:**

- `/api/digest` from Cancellation policy welcome-back digest subsection: same endpoint, two consumers.
- `cancellation:{repo_name}:{task_id}` Redis storage from OBS-BE expanded: bot reads cause text directly.
- `SignalSource` Protocol from Cancellation policy: TelegramSessionSource registered alongside HeartbeatSource and ManualOverrideSource.
- Vision C "Orchestrator Companion App": parallel product surface, not competing. Desktop visual surface and conversational voice surface address different operator contexts (focused work vs morning triage from bed).

**Defer rationale and ordering:**

Vision D is **architecturally close** because substrate exists; **strategically defer** because Vision A multi-vendor routing must ship first to make the product itself worth a conversational interface. Order: Vision A → Vision C (companion app, optional) → Vision D.1 (Telegram digest, low cost trial) → Vision D.2 (interactive triage, validates conversational UX) → Vision D.3 (voice agent, only if D.2 traction justifies).

Stage D.1 specifically is **eligible for opportunistic implementation** at any point after Cancellation policy v1 ships, because cost is low (~1-2 weekends) and value is immediate for operator's own workflow. Does not need to wait behind Vision A; can run in parallel as personal-use improvement.

**Estimate summary:**
- D.1: ~6-8 hours, opportunistic post-Cancellation-policy-v1.
- D.2: ~3-5 days, after Vision A initial vendor plugin ships.
- D.3: ~3-4 weeks, requires demand validation from D.2 usage.
- D.4 (telephony): only if Vision D becomes core product surface, not before.

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

### Multi-tier agent hierarchy (Vision E, added 2026-04-26, classified as Vision 2026-05-02)

Direction crystallized in conversation 2026-04-26 evening. Aleksei's framing: "звать human раньше — а он должен звать другого агента, который имеет [memory access, full architecture nav, time/tokens for cross-file reasoning, escape capability]."

This is a refinement of the **Tester role** Vision item — broader and more specific. Not just review-time second opinion, but **always-available diagnostic agent** that coder can escalate to mid-cycle.

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

## Lessons learned (compacted from forensics, 2026-05-02)

Compact appendix preserving actionable lessons from extended forensics that previously lived in deleted Active investigations + Work Modes blocks. Detailed post-mortems removed during 2026-05-02 cleanup; brief one-line entries in Active OBS items above retain status.

### Test infrastructure
- **Test fixture state pollution before architectural defects.** When test failure pattern looks like a state-machine bug but the symptom is reproducible only after specific test ordering, suspect test fixture state pollution first. Capture-and-read-actual-logs approach (stack-logs.txt) was decisive in OBS-AA root cause; without it, the architectural-fix hypothesis would have shipped without solving the actual problem.
- **Prefer `docker compose exec -T <container> <cmd>`** over python clients connecting to discovered container IPs. Subprocess approach uses container's own network namespace and works identically from CI runner host and developer desktop. Python-from-host requires port mappings or `docker inspect` IP discovery, both fail in subtle ways.

### Diagnostics under stress
- **Check git log before declaring flaky.** Fix commits in the failure window mean deterministic failures, not flakiness. PR-180 self-healing convergence was 4 deterministic edge cases discovered sequentially, mistaken for transient noise initially.
- **Enumerate edge cases with fixtures in task spec.** Avoids multi-cycle discovery in production. Each FIX cycle on edge case = ~30 min daemon time + token cost.
- **Read full stdout before diagnosing.** Avoid speculation without data ("мы гадаем вместо того чтобы читать").

### Architecture and deploy discipline
- **Solve systemically, not with quick patches.** MICRO PRs that bypass full analysis create new problems. The PR-181 v1 attempt (host pytest connecting to internal docker IP) failed because of insufficient infrastructure analysis; v2 (container exec) shipped after fundamental rethink.
- **Deploy immediately after architectural merges, not batched.** PR-181 (QUEUE.md untrack) had a 1-hour livelock window because deploy was delayed; stale production + fresh main = interaction bugs.
- **Read file before writing patch in long debug sessions.** Cached snapshot drifts from user actual state; always re-read user current file before generating patches.

### Production lessons (from 2026-04-28 session)
- **Production config gap:** ~15 daemon overrides existed only as local file on production host, never committed. `git reset --hard` reverted them to upstream defaults. Production behavior not reproducible from git alone. PR-194 shipped overlay approach as fix.
- **Deploy checkout vs daemon `/data/repos/.../tasks/` distinction:** daemon works with own clone in docker volume; deploy-time `~/pipeline-orchestrator/tasks/` may contain different file set. Don't conflate when investigating queue discrepancies.
- **N>=3 verification reruns rule** for race condition fixes: one green CI run is not validation. Test could pass on lucky timing pre-fix. Require 3+ green reruns on same commit before merge.
- **Single-step on stateful operations:** rebase, merge, deploy not in `&&` chains. Each command output reviewed before next.

### Multi-repo discovery pattern (2026-05-01 session)
- Author production testing on 3 repos uncovered 11 OBS items in 4 hours. Cost of "разведка боем" was low: no data loss, daemon kept working through bugs.
- Pattern: author production → bugs surface → tests built → external user exposure. Multi-testbed test infrastructure (Sprint 16) ships before any non-author alpha user is exposed.
- Multi-repo coordination, GraphQL diet headroom, shared auth volumes all validated by this session.

### Codex review behaviour (recorded for awareness)
- Codex reviews are non-deterministic on identical code: EYES → CHANGES_REQUESTED → APPROVED → CHANGES_REQUESTED transitions happen without any push between them. Operator's deliberate choice (2026-05-02): keep this behaviour because intermediate codex comments often catch missed details. Pre-merge sync re-trigger in `merge.py:170-195` provides defense-in-depth against approval-on-stale-HEAD.
- EYES race window: dual-trigger (codex auto-trigger + daemon `@codex review` post) sometimes causes EYES-stuck state. PR-189 shipped pre-push state check + EYES-specific stale threshold mitigations.

## Architectural future work — multi-repo + per-repo config (added 2026-05-01)

### Coder plugin extensibility — Add Coder from presets (added 2026-05-01 late evening)

**Operator's idea:** extend the per-repo/per-app settings concept down to coder level — enable "Add Coder" UI that picks from preset list (Claude, Codex, Qwen, Aider, local LLMs via Ollama, etc.) and instantiates the chosen plugin. Also: enable multiple instances of same plugin with different auth (e.g. 2-3 Claude Max accounts running in parallel for 3x throughput).

**Foundation already exists in code (verified 2026-05-01):**

- `src/coder_registry.py::CoderPlugin` Protocol defines clean interface: `name`, `display_name`, `models`, `run_planned_pr`, `fix_review`, `check_auth`, `create_usage_provider`, `rate_limit_patterns`.
- `CoderRegistry` provides registration/lookup pattern.
- `src/coders/__init__.py::build_coder_registry()` factory registers `ClaudePlugin` and `CodexPlugin`.
- Two plugins implement the Protocol cleanly.

**Architecture is plugin-shaped.** Adding new coders is a much smaller effort than starting from scratch.

**What blocks dynamic "Add Coder" today:**

1. **Hardcoded `CoderType` enum** (`src/config.py:19`) — `CLAUDE` and `CODEX` are baked into config schema. Adding a third coder requires adding an enum value. Not plugin-extensible from config.
2. **`if coder_name == "claude"` branches** (`src/daemon/handlers/coding.py:115` and similar) — Claude-specific logic for breach monitoring, session tracking, weekly threshold. Tight coupling.
3. **`disabled_coders` config field** assumes enum-based disable list, not arbitrary plugin name disable.
4. **Multi-account same-coder is structurally absent.** `ClaudePlugin` instantiated once, uses single global `CLAUDE_CONFIG_DIR`. No concept of "claude account 1" vs "claude account 2".

**Two distinct visions emerge from this idea:**

#### Vision A: Multi-vendor preset library (Qwen, Aider, Ollama, etc.)

UI: "Add Coder" → dropdown with presets → register plugin in registry → operator can assign per-repo.

Per-plugin implementation cost:
- **Qwen-via-API:** low (similar pattern to Claude through Anthropic API; just different endpoint and auth).
- **Aider:** medium (Python tool, well-documented programmatic API).
- **Local LLM via Ollama:** medium (less standardized; need to handle model selection, context window management).
- **Cursor/Continue:** high (IDE-bound, no clean CLI; probably not viable as headless coder).

To enable Vision A:
- Replace `CoderType` enum with dynamic plugin-name string in config.
- Refactor `if coder_name == "claude"` branches into capability-based dispatch (e.g. `plugin.supports_breach_monitor()` → returns Optional callable; default None).
- Move Claude-specific logic into `ClaudePlugin`.
- Per new plugin: implement Protocol methods (planned_pr, fix_review, auth, rate limits, usage tracking).

**Estimated:** ~3-4 PRs for the plugin-extensibility refactor (config, dispatch, capability flags). Then ~2-3 PRs per new vendor plugin added. Initial vendor (Qwen, since it has familiar API shape): 5-7 PRs total (refactor + Qwen plugin + tests). Subsequent vendors: 2-3 PRs each.

**Strategic significance:** this is the **cross-vendor routing thesis** in concrete form. Not just a feature — a positioning anchor. Anthropic does intra-vendor routing (opusplan); only an independent orchestrator routes Claude → Codex → Qwen → local. Vision A is the technical substrate for that positioning.

**Wave placement (added 2026-05-01):** Vision A is **post-Wave-7**. Plugin extensibility is a substantial architectural shift; should not be undertaken before:
- Foundation Sprint complete (god-class decomposition makes plugin extraction cleaner)
- OBS-AS through OBS-BB fixes shipped (stable production baseline)
- Multi-testbed infrastructure exists (regression coverage for plugin-swap scenarios)

**Critical sequencing — Plugin extensibility is a Thompson Sampling prerequisite (added 2026-05-01):**

Thompson Sampling bandit selector (Sprint F3.2) needs **measurement substrate across multiple coder/model options** to be meaningful. Currently the system only has Claude and Codex, both pinned per-repo by config. Bandit selection between two fixed options is degenerate — there is no "routing decision space" for the bandit to optimize over.

Plugin extensibility (Vision A) creates the routing decision space:
- Adding Qwen as a third coder gives the bandit 3 arms to pull
- Adding local LLMs (Ollama) creates a "low-cost arm" that the bandit can route low-complexity tasks to
- Per-coder model selection (Claude opus vs sonnet vs haiku, Codex gpt-5.4 vs 5.5 vs 5.3-spark) creates intra-coder routing options
- **Intra-vendor model routing matters as much as cross-vendor routing** (added 2026-05-01) — see Vision A.2 below

#### Vision A.2: Intra-vendor model routing (added 2026-05-01)

**Operator observation:** routing decisions should distinguish not only "which provider" but also "which model within provider" based on task characteristics. Opus / Sonnet / Haiku for Claude; GPT-5.5 / GPT-5.4 / 5.3-Codex-Spark for Codex; qwen3-coder-plus / qwen3-thinking / qwen3-32b-dense for Qwen.

**This is architecturally important and may have larger product impact than cross-vendor routing.**

**Foundation in code already exists (verified 2026-05-01):**

- `ClaudePlugin.models = ["opus", "sonnet"]` — list, not single. Protocol shape is already plural.
- `CodexPlugin.models = ["", "gpt-5.4", "gpt-5.3-codex"]` — multiple models.
- Config has `claude_model: str = "opus"` and `codex_model: str = ""` — per-repo static selection.

**What's missing:**

- Static per-repo model in config; no per-task dynamic selection.
- Haiku model not yet in `ClaudePlugin.models`.
- UsageProvider tracks plugin-level usage, not per-model sub-usage.
- Bandit posterior space currently pluginscale (Codex Beta(81,19), Claude Beta(76,24)); not (plugin × model)-scale.

**Why intra-vendor routing matters more than cross-vendor:**

1. **No vendor switch friction.** Operator already pays Anthropic. Routing trivial bugs to Sonnet vs Opus is pure win — no political "you're recommending the competitor" friction. Cross-vendor routing has procurement/preference friction; intra-vendor doesn't.

2. **Cost savings dramatic at model level.** Anthropic pricing illustrates:
   - Opus: $15 / $75 per MTok input/output
   - Sonnet: $3 / $15 per MTok
   - Haiku: ~$1 / $5 per MTok (estimated based on prior tier ratios)
   - Routing trivial bugfix from Opus to Haiku = **~15x cost reduction** with minimal quality loss for that task class.
   
3. **Closer fit to operator's good-enough thesis.** Industrial chiller analogy: choose C3 for office room, C4 for data center. Same brand, different sizing. Pipeline-orchestrator: choose Sonnet for standard feature PR, Opus for architectural refactor. Same provider, different model. This is the exact mental model the cost-per-merged-PR thesis trades on.

4. **Easier adoption story.** "We picked Sonnet for this PR because complexity was low" is comprehensible. "We routed your PR to Qwen instead of Claude" requires more explanation and trust.

**Routing intuition (not final, illustrative):**

| Task pattern | Likely good model |
|---|---|
| Typo / lint / formatting | Haiku, gpt-5.3-spark, or local Ollama |
| Test scaffolding (mechanical) | Sonnet, gpt-5.4, qwen3-coder-plus |
| Standard feature PR (new endpoint, CRUD) | Sonnet, gpt-5.4 |
| Bugfix requiring code understanding | Sonnet with thinking, gpt-5.4 |
| Cross-cutting refactor | Opus, gpt-5.5 |
| Architectural decision | Opus with thinking, gpt-5.5 + reasoning, qwen3-thinking |

**Bandit sample size implication:**

Current bandit has 2 arms (Claude, Codex), each with ~100 PRs of history → reasonably confident posteriors.

With (plugin × model) decomposition:
- ClaudePlugin × {opus, sonnet, haiku} = 3 arms
- CodexPlugin × {gpt-5.4, gpt-5.5, 5.3-spark} = 3 arms
- QwenPlugin × {coder-plus, thinking, 32b} = 3 arms

Total ~9 arms minimum. To reach ~100 PRs per arm requires ~900 PRs total — 9x current data substrate. This is **acute sample-size problem** for bandit.

**Workarounds:**

1. **Hierarchical posteriors:** model the (plugin × model) effect as `vendor_baseline + model_offset`. Pool data across models within same vendor for vendor-level effect; smaller posteriors for model-specific deltas. Reduces effective sample size requirement substantially.
2. **Task-stratified posteriors:** separate posteriors per (model, task_type, complexity_bucket). Many cells empty initially; bandit defaults to explore mode for empty cells.
3. **Informative cold-start priors:** use vendor pricing tier as initial prior. Haiku starts at "fast and cheap" (high prior on success rate for trivial tasks, low prior for complex). Opus starts at "slow and expensive" (low prior on cost, high prior on quality). Bandit refines from there.
4. **Defer Thompson until enough data:** don't start bandit until ~50 PRs per (plugin × model) combo exist. Until then use heuristic routing (e.g. complexity bucket → tier mapping).

**Architectural changes needed (over Vision A baseline):**

1. **Per-task model selection logic** — new component, lives in `_select_next_task` or bandit module. Reads task profile (complexity, type, files_touched), picks (plugin, model) tuple. ~3-4 PRs.
2. **Cost model per (plugin, model)** — UsageProvider needs sub-tracking. Currently plugin-level; needs (plugin × model) granularity for cost-per-PR breakdown. ~2 PRs.
3. **Bandit posterior space refactor** — when Thompson Sampling ships, posteriors must be (plugin × model)-keyed, not plugin-keyed. ~2 PRs (covered as part of Sprint F3.2 already).
4. **Add haiku to `ClaudePlugin.models`** — small addition, ~1 PR.

**Updated Vision A scope (combining A, A.1, A.2):**

- Plugin Protocol generalization (Option 3) — 3-4 PRs
- First API plugin (Qwen with multiple models) — 3 PRs
- Anthropic API plugin (with haiku, sonnet, opus) — 2 PRs
- Per-task model selection logic — 3-4 PRs
- Cost model per (plugin, model) — 2 PRs
- Add haiku to ClaudePlugin CLI — 1 PR

**Total Vision A: ~14-16 PRs (was ~13-15 without A.2; A.2 adds ~5-6 PRs).**

**Wave placement: A.2 ships alongside A and A.1, not deferred separately.** Per-task model selection requires the routing infrastructure that A and A.1 build; doing it as a separate later phase would create structural rework.

**Strategic significance:** intra-vendor model routing converts the cost-per-merged-PR thesis from "interesting metric" to "actionable lever." Operator sees: "PR-208 cost $0.12 with Sonnet; equivalent PR-209 cost $1.84 with Opus. Was Opus needed for PR-209?" That comparison is the **product moment** — concrete dollar savings tied to a routing decision the orchestrator made on operator's behalf. Cross-vendor routing produces similar comparison ("Claude vs Qwen") but with vendor-switch friction; intra-vendor produces it without friction.

**Implication for analytics dashboard (post-Vision-A wave):**

The cost-per-merged-PR view should default to **(plugin, model) breakdown**, not plugin-only. Operator sees:

```
This week:
  Claude Sonnet:    14 PRs, avg $0.18/PR, 71% of merges
  Claude Opus:      4 PRs,  avg $1.62/PR, 21% of merges
  Codex GPT-5.4:    2 PRs,  avg $0.40/PR, 8%  of merges

Routing recommendation: 3 of 4 Opus PRs could have been Sonnet (low complexity).
Estimated savings: $4.32 / week ($225 / year).
```

This is the actionable product surface. Without (plugin, model) granularity, this story doesn't exist.

Without Vision A, Thompson Sampling has no meaningful work to do. Sequencing is:

1. Foundation Sprint complete
2. Wave 1-7 (OBS fixes + multi-testbed)
3. Vision A first vendor (Qwen, ~5-7 PRs)
4. Vision A 2-3 more vendors (validates routing decision space, ~10 PRs)
5. **Then** Thompson Sampling becomes useful (Sprint F3.2)
6. Vision B-alt (multi-tenant) ships independently as team-deployment feature

This means **Vision A precedes Thompson by ~2-3 weeks of daemon work**, not the other way around. Thompson without Vision A is selecting between two fixed coders — could ship but provides minimal product value until routing options expand.

#### Vision A.1: CLI plugins vs API plugins — two distinct shapes (added 2026-05-01)

**Architectural distinction:** Vision A new vendors split into two technical shapes that need different Protocol abstractions:

**CLI plugins** (current pattern):
- `ClaudePlugin` invokes `claude` binary subprocess
- `CodexPlugin` invokes `codex` binary subprocess
- Subscription-based auth (Pro/Max OAuth tokens stored by CLI in config dir)
- Local subprocess with `repo_path` working directory access
- Returns `tuple[int, str, str]` = `(exit_code, stdout, stderr)`
- Rate limit = subscription quota (session/weekly window)

**API plugins** (new shape needed):
- Direct HTTP call to vendor API (Anthropic API key, OpenAI API key, Qwen API key, etc.)
- Pay-per-token billing
- No subprocess; no `repo_path` access — agent sees code through tools the daemon provides
- HTTP response (possibly streaming); no exit_code, no stderr in subprocess sense
- Rate limit = tokens-per-minute, requests-per-minute (vendor TOS, not subscription)

**Current CoderPlugin Protocol shaped for CLI subprocess.** Adapting API plugins requires either:

1. **Generalize Protocol shape:** replace `tuple[int, str, str]` with `CoderResult` dataclass that both shapes return. Daemon code stays uniform.
2. **Add capability flag:** `plugin.auth_kind() -> Literal["cli_subscription", "api_key"]` for daemon to dispatch correct auth UI flow + cost model. CLI subscription = device-flow login. API key = paste credential into UI.
3. **Per-plugin rate limit semantics:** CLI plugin reports session/weekly percent (already does); API plugin reports tokens-per-minute consumption. Different units — UsageProvider abstraction needs to handle both.

**Decision (2026-05-01, confirmed):** **Option 3 chosen — generalize current Protocol via `CoderResult` dataclass + capability flag + per-plugin rate limit abstraction.** This is the cleanest path: single Protocol, single result shape, daemon code agnostic to whether plugin is CLI or API. Plugins differ only in implementation, not in interface.

**Why this distinction matters strategically:**

- **Pricing transparency.** Cost-per-merged-PR thesis works with both shapes, but units differ: CLI = "fraction of $200/mo Max subscription used" vs API = "exact $X.YZ in tokens." Dashboard must display each meaningfully.
- **Scaling story.** CLI plugins hit subscription wall (Max = ~17 PRs/day per author). API plugins scale to vendor budget. Multi-tenant (Vision B-alt) of CLI plugins = N operators × subscription each. API plugins = N operators on shared budget. Different deployment patterns.
- **Vendor coverage.** Some vendors only offer API (no CLI tool) — Qwen, Mistral, smaller vendors. Some offer both — OpenAI (Codex CLI + GPT API), Anthropic (Claude CLI + API). Without API plugins, half the routing landscape is invisible.

**Estimated additional work over Vision A:** ~3-4 PRs for Protocol generalization + capability flag dispatch + first API plugin (Qwen via API). Subsequent API vendors: 2-3 PRs each.

**Sequencing:**
1. Vision A refactor (Protocol generalization, capability flags, dispatch) — ships before any new vendor
2. First CLI plugin (if applicable) OR first API plugin (Qwen recommended — no CLI exists)
3. Add Anthropic API plugin (parallel path to ClaudePlugin CLI; same vendor, two access modes)
4. Add OpenAI API plugin
5. More vendors as needed

**Anthropic API plugin specifically valuable:** unlocks deployment scenarios where Claude Code CLI subscription is impractical (cloud VPS deployments — see Anthropic ToS clarification, "Running it on a VPS? Use an API key"). Pipeline-orchestrator running on home server can use CLI plugin; pipeline-orchestrator running on cloud must use API plugin. **Both must be supported for product to be deployable beyond home-lab use case.**

**Critical strategic implication — managed/hosted deployment requires API plugins exclusively (added 2026-05-01):**

If pipeline-orchestrator ever becomes a hosted/managed product (SaaS where author or company runs the daemon, users connect to it), **CLI plugins become legally unavailable** under current Anthropic Consumer ToS. Two cited prohibitions:

> "Running it on a VPS? Use an API key — subscription OAuth on servers is both impractical (tokens expire) and prohibited." (Anthropic Consumer ToS guidance, Feb 2026)

> "Anthropic does not permit third-party developers to offer Claude.ai login or to route requests through Free, Pro, or Max plan credentials on behalf of their users." (Anthropic Legal Compliance docs)

This means:

| Deployment shape | CLI plugins | API plugins |
|---|---|---|
| Self-hosted on home server (current author case) | Allowed (ordinary individual usage) | Allowed |
| Self-hosted on cloud VPS | **Prohibited** (subscription on server) | Required path |
| Managed/hosted product (you run, users connect) | **Prohibited** (routing through user subscriptions on behalf) | Required path |
| Multi-tenant on shared infrastructure | **Prohibited** even with separate operator accounts (still hosted by you) | Required path |

**Product strategy fork (mid-term decision, not immediate):**

This creates two distinct product directions with different unit economics:

**Self-hosted product** (current direction):
- Distribute pipeline-orchestrator as software (open-source, source-available, AGPL+commercial).
- Operators run on their own hardware, use their CLI subscription.
- Unit cost to operator ≈ 0 marginal (subscription is sunk cost they already pay for personal use).
- Revenue model: commercial license, support contracts, hosted SaaS add-on (analytics, multi-tenant features, backup) — but core daemon remains self-hosted.

**Managed product** (potential future direction):
- Author/company runs daemon on cloud infrastructure.
- Users authenticate via Anthropic API keys, OpenAI API keys (bring-your-own-key).
- CLI plugins **completely disabled** in managed deployment configuration.
- Unit economics: pass-through API costs + your orchestration margin per PR/repo/org.
- **Cost-per-merged-PR thesis becomes cleaner business case** — operator sees real $X.YZ per PR (not "fraction of $200/mo subscription"); routing decisions tie directly to dollar-cost optimization, which is exactly what the bandit + analytics layer optimize for.

**Why the fork is not immediate:**

- Foundation Sprint and Wave 1-7 work the same for both directions.
- Vision A (multi-vendor including Anthropic API plugin) is required for **either** direction — managed needs it for legal reason, self-hosted needs it for cloud VPS support and routing thesis.
- Vision B-alt (multi-tenant) makes most sense in managed framing but is also useful for small-team self-hosted (each team member has own credentials).
- The fork point is product positioning decision: when (if ever) the author wants to launch as a hosted SaaS. That decision currently sits beyond the present roadmap horizon.

**What changes if managed becomes the path:**

1. **Disable CLI plugins in managed deployment config.** Single feature flag (`config.daemon.allow_cli_plugins: bool`). Self-hosted defaults True; managed defaults False.
2. **Onboarding flow guides API key paste** instead of CLI device-flow login. PR-FUTURE-6 auth UI must support both modes.
3. **Pricing display in dashboard** shifts from "session 32%, weekly 4%" (subscription quota indicators) to actual dollar cost accumulation per repo / per PR / per org. Analytics dashboard already in roadmap (post-Vision A) supports this naturally.
4. **Multi-account framing changes:** in managed, "each user has their own API key" is the natural model, no ToS gray area. Vision B-alt becomes the default tenancy pattern, not an edge case.

**Action: do not architect for managed-only.** Architect for both paths; the fork point is a single configuration flag plus pricing display swap, not a deep refactor. Self-hosted remains primary near-term; managed becomes optional path that ships after Vision A is mature and product-market fit is validated on self-hosted.

**Recorded as strategic note, not immediate roadmap item.** Revisit when considering hosted launch decision.

#### Per-vendor ToS comparison (verified 2026-05-01)

The "managed deployment requires API plugins" rule is **not Anthropic-specific** — same trajectory across vendors:

| Vendor | CLI subscription | API key | Self-hosted weights | Notes |
|---|---|---|---|---|
| Anthropic Claude | OK personal home only; prohibited on VPS or hosted | OK any deployment | N/A (closed weights) | Explicit prohibition Feb 2026 docs update |
| OpenAI Codex | OK personal interactive; gray area for automation | OK any deployment | N/A (closed weights) | Same trajectory as Anthropic; OpenAI silent where Anthropic was explicit, but community recognises automation = API key path |
| Qwen (Alibaba) | Free OAuth tier discontinued Jan 2026 (1000/day → 100/day, then ended) | OK via Alibaba Cloud Coding Plan or third-party (OpenRouter, Fireworks) | OK Apache 2.0 license | Self-hosted weights = clean path; hosted requires paid API |
| DeepSeek | Limited subscription tier | OK | OK on most models | Self-hosted clean; same dual-track |
| Mistral | Limited subscription | OK via La Plateforme | OK for open models (Mistral 7B) | Premium models API-only paid |
| Aider | N/A (it is a harness, not vendor) | Inherits backend's ToS | N/A (it is a harness) | Itself Apache 2.0; routes through your chosen vendor |
| Ollama / llama.cpp | N/A | N/A | Fully OK, zero ToS friction | Self-hosted models via Ollama = no vendor terms apply |

**Industry pattern across all vendors:** open weights for self-hosted; paid API for hosted; subscription tiers tightening rapidly (Anthropic Feb 2026, Qwen Jan 2026, OpenAI gradual). **API key path is the universal safe option for any deployment beyond the operator's own home machine.**

**Implication for pipeline-orchestrator:**

- CLI plugins (Claude CLI, Codex CLI, Qwen Code CLI when it existed for free) are **all on the same clock** — vendor ToS evolves toward restricting subscription use to personal interactive only.
- API plugins are **uniformly safe** across all vendors. No vendor prohibits API key use for automation.
- **Self-hosted-with-Ollama path** is the ONLY genuinely friction-free deployment — operator runs Qwen/DeepSeek/Mistral weights on their own hardware, no vendor relationship at all.

**This shapes the product positioning differently than "Claude orchestrator":**

- Pipeline-orchestrator is **vendor-agnostic by virtue of needing to be**. The product cannot rely on any single vendor's CLI being usable in non-personal contexts going forward.
- The cross-vendor routing thesis (Vision A) is not just nice-to-have — it is **architecturally required** for any deployment outside the author's home server.
- Self-hosted-with-local-Ollama becomes a credible deployment story for operators in regulated industries (finance, healthcare, government) who cannot send code to vendor APIs.

#### Self-hosted product monetization analysis (added 2026-05-01)

**Operator's question:** "Как продавать self-hosted? Брать плату данными о сделанных PR?"

This is a serious strategic question with several viable answers and one natural-feeling but problematic answer. Worth thinking through systematically.

**What is sold in self-hosted, structurally:**

**Critical correction (added 2026-05-01 late, after operator pointed out):** earlier analysis assumed "self-hosted operator = personal subscription, marginal cost ~0." This frame is **wrong for the org-deployment case** which is likely the actual paying customer. Three deployment shapes coexist:

| Deployment | Auth | Cost visibility | Routing decision pressure |
|---|---|---|---|
| Self-hosted personal CLI | Personal subscription (sunk cost) | Quota % only | Personal preference |
| Self-hosted with org API key | Org's API key (real $) | Real $ per PR | Org cost-conscious |
| Managed/hosted (BYOK) | Org's API key (real $) | Real $ per PR | Org cost-conscious |

The **org-with-API-key** case is structurally identical to managed deployment from cost-visibility and routing-decision perspective: real dollars per PR, CFO scrutiny, pressure to reduce bill. **And this case is probably where the paying customer actually lives.** Solo developer with personal subscription is hobby/early-adopter; **org with team budget is real revenue.**

**Implications of org case being primary paying customer:**

1. **Cost-per-merged-PR thesis is STRONGER in org case, not weaker.** In personal-subscription case, cost-per-PR is an abstract metric ("32% of session quota used"). In org-API-key case, it is invoice-level concrete savings ("PR-208 cost $0.42, equivalent PR-209 cost $3.18 — could PR-209 have used Sonnet?"). The thesis trades on dollar visibility, which only appears with API key auth.

2. **Intra-vendor model routing (Vision A.2) is critical from day 1, not nice-to-have.** Org's API bill exposes them directly to 15x cost gap (Opus $15/$75 vs Haiku ~$1/$5). CFO sees monthly bill jump from $5k to $8k and asks "why?" Pipeline-orchestrator with model routing = direct answer. Without model routing, the orchestrator adds spend visibility but no spend control. Visibility without control is half the product.

3. **Paid SaaS tier customer profile sharpens.** Earlier I described "operator who wants benchmarks." Real paid customer is **org spending $5k+/month on AI coding APIs** that wants to:
   - See cost-per-PR breakdown by team, repo, complexity bucket
   - Receive routing recommendations to reduce their bill (Thompson Sampling output)
   - Track productivity vs cost ratio across team
   - Audit who triggered which expensive PR
   
   ROI math becomes clean: org spending $60k/year on API, 20% routing savings = $12k/year, SaaS subscription priced $1-3k/year justifies easily. **Direct dollar ROI story, not abstract value.**

4. **Vision B-alt re-positions to enterprise governance.** Multi-tenant deployment is not "personal multi-account workaround" but **API budget governance layer for orgs**:
   - Org has team of 10 developers
   - Each developer has subscription OR org has shared API key budget
   - Pipeline-orchestrator manages auth for team, per-developer or per-team budget tracking
   - Audit logs (who triggered which PR, at what cost, against which API key)
   - SSO integration with org identity provider
   - Role-based access (junior dev cannot trigger Opus PRs above $X/day budget)
   
   This is real enterprise feature set, not personal-use feature.

5. **License decision implications.** Apache 2.0 (no AGPL viral) is **even more correct** under org-customer framing. Orgs cannot legally use AGPL software for internal proprietary work without releasing their modifications. AGPL would block org adoption entirely. Apache 2.0 keeps the door open while paid SaaS tier captures the revenue.

**Layered value structurally (revised):**

Pipeline-orchestrator as software has layered value:

1. **Orchestration code** — the daemon, ~250 PRs of state machine, plugin architecture, FSM, recovery handlers. Open-source by default.
2. **Operational knowledge** — accumulated patterns: scaffolder rules, AGENTS.md conventions, error classification, FIX cycles, session management. Embedded in code, transferred with installation.
3. **Network-effect data** — across all installations, what coder/model/task combinations work best, cost-per-PR distributions by complexity bucket. Lives only at scale (50+ installations minimum for useful aggregates, 500+ for meaningful benchmarks).
4. **Cost intelligence** — routing recommendations based on (3), competitive industry benchmarks. Sellable as data product to non-users (CTOs, VCs, vendors).

In a managed (hosted) product, user pays for (1)+(2)+(3)+(4) bundled through subscription. In self-hosted, user gets (1)+(2) for free if open-source. Value capture for self-hosted MUST come from (3)+(4), or from bundling extra services.

**Pricing model options:**

**A. Open-source + commercial license** (GitLab CE/EE pattern)
- AGPL or similar for community; paid commercial license for businesses that don't want AGPL viral clause.
- Revenue from license fees.
- Risk: AGPL can scare away companies that would otherwise contribute. Sets adversarial tone with users.

**B. Open-core** (Sentry, Hashicorp pattern)
- Free: core daemon, scaffolder, basic UI.
- Paid: analytics dashboard, multi-tenant, advanced routing, audit logs, SSO, compliance.
- Revenue: per-seat or per-org subscription.
- Risk: users reimplement paid features in community fork.

**C. Free + paid hosted SaaS addon** (Plausible, PostHog pattern)
- Self-hosted always free and full-featured.
- Optional paid hosted analytics that ingests opt-in telemetry, returns benchmark dashboards.
- Optional paid hosted multi-tenant management plane for operators running orchestrator across organisations.
- Revenue from SaaS subscription, not from software.
- Risk: few operators opt into paid hosted addon.

**D. Data-as-payment** (free unlimited + telemetry obligation)
- Free unlimited use.
- In exchange: anonymized PR metrics telemetry sent to author's servers.
- Aggregate data trains routing intelligence; eventually sold as industry benchmark product.
- Revenue from data product (sold to CTOs, VCs, vendors), not from operators directly.
- Risk: privacy concerns, GDPR compliance burden, "free product that takes your data" has reputational baggage post-Facebook era. Technical operators (the initial target audience) will disable telemetry, fragmenting data quality.

**Recommendation: Hybrid C + opt-in D, NOT pure D.**

The natural temptation is "data-as-payment" because the data really is valuable and operators don't see a direct cost. But pure D model has structural problems:

1. **GDPR exposure.** Accumulating data about developers' work product across the EU requires legal infrastructure (DPO, privacy policy, data protection agreements). For a solo developer launching a product, this is a heavy lift.
2. **Trust signal negative.** "Free product that takes your data" has bad reputation specifically among the developer audience pipeline-orchestrator targets. Operators who care enough about cost-per-PR to use the product also care about data sovereignty.
3. **Code/PR data is unusually sensitive.** Even "anonymized" PR metrics can leak proprietary patterns (release schedules, feature priorities, bug rates). Some operators legally cannot share even anonymized telemetry (regulated industries).
4. **Slow monetization curve.** Data products take years to mature. Pipeline-orchestrator needs 50+ active operators before any aggregate is meaningful, 500+ before benchmarks are sellable. Meanwhile no revenue stream covers development cost.
5. **Adversarial relationship with sophisticated users.** The most valuable users (large orgs, deep operators) will be the first to disable telemetry, fragmenting exactly the data you most need.

**Hybrid that works (C + opt-in D):**

1. **Self-hosted free, full-featured, no telemetry by default.** MIT or Apache 2.0 license. Operator runs on their infrastructure with zero data extraction.
2. **Opt-in telemetry** — operator chooses to share anonymized PR metrics. In exchange:
   - Access to **community benchmark dashboard** (compare your cost-per-PR vs industry median, by complexity bucket).
   - Access to **community-trained routing recommendations** (which coder works best for which task type, learned from aggregate).
3. **Paid SaaS analytics tier** for orgs wanting deep analytics on their own data:
   - Multi-org dashboard (CTO sees all teams' cost-per-PR).
   - Custom benchmarks compared to competitors via aggregate.
   - Audit logs, compliance reports, SSO, role-based access.
   - **This is the revenue stream.**
4. **Mid-term: data product** sold to non-users (VCs, AI vendors, consulting firms) once aggregate is meaningful (500+ operators).

**Why hybrid works:**

- Operators bring data **in exchange for value** (community benchmarks, routing intelligence) — not as payment-for-software. Different framing matters culturally.
- Orgs with serious analytics needs pay for SaaS tier, generating revenue without forcing payment from individual operators.
- Telemetry is genuinely opt-in — preserves trust with sovereignty-conscious operators.
- Two-tier value capture (community benchmarks + paid SaaS) is more resilient than single-revenue model.

**Strategic note (not immediate decision):**

Pricing model decision is **mid-term, 6-12 months out**. Current priorities (Foundation Sprint, Wave 1-7, Vision A, SQLite, Thompson) all happen before this decision needs to lock. Architecture should be **prepared** for opt-in telemetry but should **not bake it in by default**. Build the analytics surface in a way that works equally well for personal-use-only operators and for opt-in-telemetry operators.

**Action items for product preparation (low cost now, preserves option):**

1. **No telemetry by default.** Code base ships without any phone-home behaviour. Adding it later is a feature flag plus opt-in flow.
2. **Analytics surfaces are local-only by default.** Cost-per-PR dashboard works on your own SQLite data. Opt-in flow could later sync subset to shared backend.
3. **License decision deferred** but lean toward Apache 2.0 (no AGPL viral). Avoids commercial-license model adversarialism. Revenue comes from hosted SaaS tier when ready.
4. **Pricing page work** is a separate Wave (post-Vision-A, post-SQLite, post-analytics dashboard). Not addressing pricing in current roadmap.

**Recorded as strategic positioning note. Revisit when 30+ active operators exist (validation point for whether community matters as model) or when first paid SaaS feature request arrives organically.**

#### SQLite addition for long-term metrics (added 2026-05-01)

**Framing decision (2026-05-01, confirmed):** **SQLite is added alongside Redis, not migrated to.** Each tool used for its strengths. Redis remains for state machine + pub/sub (its core competencies); SQLite added for durable, queryable metrics persistence (its core competency). No replacement, no migration in the destructive sense — additive architecture with clear responsibility split.

**Tool-to-purpose mapping (target architecture):**

| Concern | Tool | Why |
|---|---|---|
| RepoState per-repo (volatile, every cycle) | **Redis** | Fast SET/GET semantics, in-memory speed for high-frequency writes |
| SSE pub/sub event stream | **Redis** | Native pub/sub primitive; SQLite has no equivalent |
| ETag cache, deadlines, locks | **Redis** | TTL semantics built-in; ephemeral by nature |
| RunRecord metrics (durable, queryable) | **SQLite** | Long-term retention without TTL loss; SQL queries for aggregation; backup-friendly |
| Profile data for Thompson posteriors | **SQLite** | Bandit needs stable history beyond 90-day Redis TTL |
| Cost-per-merged-PR analytics | **SQLite** | SQL-shape queries for time-series, cross-repo aggregations |

**Current state (verified 2026-05-01):** all persistent data lives in Redis. Three distinct usage patterns:

1. **State persistence** — `RepoState` per-repo, history (24h TTL). Stored as `pipeline:{name}` keys. Volatile, updated every cycle. **Stays in Redis.**
2. **Pub/sub** — SSE event channel (`src/events/sse.py`), progress updates published from daemon to web UI subscribers. Ephemeral messaging. **Stays in Redis.**
3. **TTL-based cache** — `MetricsStore` records (90-day TTL), recent-200 indexes per (task_id, repo_name). **Migrates to SQLite (Scenario A below).**

**Problem with metrics-in-Redis:**

- 90-day TTL **caps Thompson Sampling posterior stability**. Bandit cannot maintain reliable distributions over coder/model performance once data ages out. For long-running deployment (6+ months), bandit forgets earlier learning.
- **No query layer.** Cost-per-merged-PR aggregation, time-series cost trends, cross-repo profile comparisons all require either dumping Redis to a queryable store or in-memory aggregation in Python. Both expensive at scale.
- **No backup outside Redis volume.** If Redis crashes without persistence config, all metrics history lost. AOF/RDB persistence helps but not as durable as proper DB.
- **Analytics dashboard can't be built cleanly.** Surface like "show me cost-per-PR by complexity bucket over last quarter" requires SQL-shape data; Python-side aggregation across O(thousands) of records is slow.

**Two scenarios for adding SQLite:**

**Scenario A: SQLite for metrics only, Redis keeps state + pub/sub. (CONFIRMED 2026-05-01 — initial scope)**
- Move `MetricsStore` from Redis to SQLite.
- Schema: `RunRecord` table with all current fields + indexes on (task_id, repo_name), (started_at), (profile_id).
- Long-term retention (no TTL); manual archive/prune policy if needed later.
- Redis retains state machine, SSE pub/sub, ETag cache, deadlines.
- Net: **add** SQLite as additional store; two storage systems coexist with clear responsibility split.
- Estimated: ~3-4 PRs (schema + MetricsStore rewrite + migration script for existing Redis data + tests + backup/restore documentation).

**Scenario B: Expand SQLite to cover history + audit data (potential later, "потом" per operator decision).**
- Migrate `key_history` (24h Redis TTL) to SQLite for long-term audit trail.
- Add session log archive in SQLite (currently logged to event stream only, lost after pub/sub channel drops).
- Add operator action audit log (who clicked Stop, when, on which repo).
- State machine **stays in Redis** — its volatility profile is wrong for SQLite.
- Pub/sub **stays in Redis** — SQLite has no equivalent.
- Net: SQLite footprint grows from "metrics only" to "metrics + audit + history" for richer operator visibility.
- Estimated: ~4-5 PRs additional.

**Scenario C (Drop Redis entirely): NOT pursued.** Confirmed 2026-05-01 — keep Redis, add SQLite alongside. "Всему свой инструмент."

**Recommendation: Scenario A first, Scenario B later when audit/history needs surface.**

Why A first:
- **Most immediate value** — long-term metrics persistence enables Thompson Sampling correctly. Current 90-day TTL is a Thompson blocker for posterior stability.
- **Smallest blast radius** — touch only MetricsStore + add new SQLite schema. State machine, pub/sub, ETag cache untouched.
- **Validates SQLite operational pattern** (backup, schema migrations, query layer) before expanding scope.
- **Analytics dashboard foundation** — once metrics in SQLite, proper queries enable cost-per-PR breakdown view in operator dashboard. Big product story unlock.

Why B later (or only when needed):
- Scenario B is purely additive value (better audit, longer history). No urgent blocker.
- Defer until operational reasons (audit trail required, history queries needed for debug or product) justify the work.

**Wave placement (confirmed 2026-05-01):** SQLite addition (Scenario A) is **before Thompson Sampling**, **after Vision A**:

1. Foundation Sprint complete
2. Wave 1-7 (OBS fixes + multi-testbed)
3. Vision A — Plugin Protocol generalization (Option 3: `CoderResult` dataclass + capability flag + per-plugin rate limit abstraction)
4. Vision A — first API plugin (Qwen — no CLI exists, validates API plugin path)
5. Vision A — Anthropic API plugin (unlocks cloud VPS deployment)
6. Vision A — additional vendors as needed (OpenAI API, Mistral, etc.)
7. **SQLite addition Scenario A** (metrics-only) — unlocks long-term Thompson posteriors and analytics dashboard foundation
8. Analytics dashboard cost-per-merged-PR breakdown view (surfaced from SQLite queries)
9. Thompson Sampling (Sprint F3.2) — now has both routing decision space (Vision A) and durable measurement substrate (SQLite)
10. Vision B-alt (multi-tenant, ToS-safe) ships independently as team-deployment feature
11. SQLite Scenario B (audit/history expansion) — only when audit/history needs surface, not urgent

**Estimated total addition to post-Foundation roadmap:**
- Plugin Protocol generalization for CLI vs API: ~3-4 PRs, ~6-8h
- First API plugin (Qwen with multiple models): ~3 PRs, ~6h
- Anthropic API plugin (with haiku/sonnet/opus): ~2 PRs, ~4h
- Per-task model selection logic (Vision A.2): ~3-4 PRs, ~6-8h
- Cost model per (plugin, model) (Vision A.2): ~2 PRs, ~4h
- Add haiku to ClaudePlugin CLI: ~1 PR, ~1h
- SQLite addition Scenario A: ~3-4 PRs, ~6-8h
- Analytics dashboard cost-per-PR view ((plugin, model) breakdown default): ~3 PRs, ~5h

Total addition: ~20-23 PRs, ~38-44 daemon-hours. Combined with prior post-Foundation work (Wave 1-7 = 18-22 PRs), total post-Foundation = ~38-45 PRs / ~75-86 daemon-hours / ~4-5 daemon-days at 17 PR/day, calendar 2-3 weeks with buffers.

**Data substrate status (verified 2026-05-01 by checking src/metrics.py):**

`MetricsStore` already accumulates `RunRecord` data per coder run:
- Per-run: run_id, task_id, profile_id, task_type, complexity, started_at, ended_at, duration_ms
- Coder behaviour: fix_iterations, tokens_in, tokens_out, exit_reason, operator_intervention
- Code metrics: files_touched_count, languages_touched, diff_lines_added/deleted, test_file_ratio, had_merge_conflict
- Stage tag: currently "coder", reserved for "planner"/"reviewer"/"qa" expansion

Storage in Redis with 90-day TTL, recent-200 index per (task_id, repo_name).

**Implications:**
- ✅ Measurement substrate exists. Cost-per-merged-PR thesis has data foundation.
- ✅ Bandit can read recent records and compute posteriors when Thompson ships.
- ⚠️ 90-day TTL means long-term posterior stability is limited. Need archive layer if Thompson runs for 6+ months.
- ⚠️ No cross-repo aggregation query layer. Thompson selector would need to roll its own aggregation, or that's a prerequisite PR.
- ⚠️ No analytics surface in dashboard yet (data is there, but nothing renders it as cost-per-merged-PR breakdown for operator). Separate concern from Thompson itself, but blocks operator from understanding what bandit is doing. Probably needs its own Wave between Vision A and Thompson.

#### Vision B: Multi-account same-coder (parallel Claude accounts) — **REJECTED 2026-05-01 after ToS verification**

**Original idea:** instantiate multiple `ClaudePlugin` instances with different auth credentials (e.g. 3 Claude Max accounts running in parallel for 3x throughput).

**ToS verification (2026-05-01, web search of Anthropic Consumer ToS, Feb 2026 doc updates, and community discussion):**

This is **explicitly prohibited** under Anthropic Consumer Terms of Service:

> "Using OAuth tokens obtained through Claude Free, Pro, or Max accounts in any other product, tool, or service — including the Agent SDK — is not permitted and constitutes a violation of the Consumer Terms of Service." (Anthropic Legal Compliance, Feb 2026)

> "Anthropic's Terms of Service don't explicitly allow multiple users under one personal Claude Pro plan. If multiple people log in from different IP addresses or browsers, the system may detect unusual activity and temporarily restrict access. Account lockouts: Claude may automatically flag multiple logins or device fingerprints, resulting in forced verification or suspension."

The Feb 2026 docs update also introduced the phrase **"ordinary, individual usage"** when describing what subscription usage limits assume — coordinating multiple personal accounts to bypass a single account's rate limits is the opposite of ordinary individual usage.

**Verdict: Vision B is dead as a primary product feature.** Building it as documented "give one user 3x throughput via multi-account" would:
1. Encourage product users to violate Anthropic ToS
2. Risk account bans for those users
3. Create reputational and legal risk for the orchestrator product
4. Build product value prop on fragile gray-area workaround

**Do not implement Vision B in this form.** Strike from roadmap.

#### Vision B-alt: Multi-tenant deployment with separate operator accounts (added 2026-05-01)

**Reframed concept (ToS-safe alternative to original Vision B):**

Each operator using pipeline-orchestrator brings their own Claude account, used individually by them in compliance with Consumer ToS "ordinary individual usage." The daemon coordinates work across multiple operators (each isolated to their own workspace, repos, and credentials), but no single subscription is shared or split.

**Architecturally same as Vision B:**
- `ClaudePlugin` instance-scoped config dir per operator (`/data/auth/operator-1/claude`, `/data/auth/operator-2/claude`, ...)
- Per-operator UsageProvider, rate limit tracking, breach monitoring
- Per-operator repo access (operator 1 sees their repos, operator 2 sees their repos)

**Different product framing:**
- Not "give one user 3x throughput" (ToS-violating)
- Instead "deploy pipeline-orchestrator as a small-team coordination tool where each member's account is used by them, daemon orchestrates handoffs and visibility"
- Each operator's usage stays within their own subscription's "ordinary individual usage" envelope
- Multi-tenant boundary is the operator (human user), not the workload

**Use cases for this framing:**
- 2-3 person dev team where each has Claude Max, daemon coordinates work across team
- Family/personal where one person works on different projects under different accounts (unusual but legal)
- Multi-operator alpha deployment for testing

**Key constraint:** the daemon must NOT pool work across operators (e.g. operator 1 cannot trigger work that consumes operator 2's quota). Each operator's quota envelope is independent.

**Product positioning for this:** small-team deployment of orchestrator with each member's individual Claude subscription. Defensible against ToS scrutiny.

**Estimated:** ~5-7 PRs (multi-tenant data model + per-operator auth dirs + per-operator rate limits + workspace isolation + operator-aware UI + tests).

**Wave placement: post-Vision-A.** Vision A (multi-vendor) is bigger product story, ships first. Vision B-alt is later-stage feature for team adoption, not solo developer.



### Multi-repo testing infrastructure (added 2026-05-01 evening, **scheduled post-OBS-fixes**)

**Sequencing decision (2026-05-01):** Foundation Sprint finishes → OBS-AS/AU/AV/AW/AX/AY fixes ship → multi-testbed setup → multi-repo tests added. This ordering ensures:

1. Tests are written **after** the bugs they would have caught are fixed, so the test suite documents post-fix expected behaviour rather than inheriting pre-fix workarounds.
2. Multi-testbed harness is built on top of stable scaffolder (OBS-AX fix in place — CLAUDE.md replacement) so every new testbed onboards correctly.
3. Backend `/api/states` performance gate (`< 1 second for 10 repos`) only makes sense after OBS-AY backend fixes; otherwise tests would just confirm the known slowness.

**Order of execution (post-Sprint 12 / post-Foundation):**

```
Sprint 13 (external onboarding readiness + UI scaling + license switch):
  - OBS-AX scaffolder CLAUDE.md replace      (1 PR,  ~2h)
  - OBS-AY setInterval cleanup + /api/states (2-3 PRs, ~5h)
  - License switch MIT to Apache 2.0         (1 PR,  ~1h)  NEW 2026-05-02
  Total: 4-5 PRs, ~8 daemon-hours
  License task is mechanical (replace LICENSE file content, add NOTICE,
  update pyproject.toml license field). Operator confirmed ASAP 2026-05-02.
  Independent of OBS-AX and OBS-AY; can ship in parallel within sprint.

Sprint 14 (recovery + cancellation policy, REORDERED EARLIER 2026-05-02, EXPANDED with BK/BL/BM):
  - OBS-AW HUNG button                       (~2h)
  - OBS-BB FIX-no-push                       (~2h)
  - OBS-BC CI infra-failure classification   (~5h)
  - OBS-BE expanded cause preservation       (~7h)
  - Cancellation policy v1                   (~9h)
  - OBS-BK elif chain ordering fix           (~3h)  NEW 2026-05-02
  - OBS-BL WATCH↔HUNG circuit breaker        (~3h)  NEW 2026-05-02
  - OBS-BM CI stuck PENDING classification   (~5h)  NEW 2026-05-02
  Total: 9-11 PRs, ~27 daemon-hours
  Reordering rationale: 2026-05-02 production session demonstrated 4-hour
  hang on PR-227c due to interaction of OBS-BK (elif blocks FIX trigger),
  OBS-BL (no circuit breaker on retry loop), and OBS-BM (CI concurrency-lock
  stuck not classified). All three combined with absent Cancellation policy
  caused ~10 PR throughput loss. Sprint 14 now ships the full recovery suite
  to prevent this class of incident across all four trigger paths.

Sprint 15 (UX polish + vocabulary + DONE metrics):
  - OBS-AU spinner + OBS-AS toast/initializing
    + OBS-AZ + OBS-BA layout                 (3-4 PRs, ~5h)
  - OBS-AV synonyms + atomic upload + OBS-BF (3 PRs,  ~5h)
  - OBS-BD label create idempotency          (~1h)
  - OBS-BH event log badge text dedup        (~1-2h)  NEW 2026-05-02
  - OBS-BI per-PR metrics in DONE row (Path A locked) (~3h)  NEW 2026-05-02
  - OBS-BJ DONE list newest-first sort       (~1-2h)  NEW 2026-05-02
  Total: 7-9 PRs, ~17-19 daemon-hours

Sprint 16 (multi-testbed test infrastructure):
  - Provisioning + conftest + base patterns  (2-3 PRs, ~5h)
  - One PR per multi-repo test scenario      (5+ PRs, ~10h)
  Total: 7+ PRs, ~15 daemon-hours

Sprint 17+ (Vision A multi-vendor first slice): TBD pending strategic decision.
```

**Total Sprint 13-16: ~23-27 PRs, ~51-56 daemon-hours, ~2-3 daemon-days** at 25-30 PR/day throughput. Calendar 1.5-2 weeks with sustainable pace.

**Current testbed:** `tests/e2e/lib/coder_shim.sh` mocks Claude/Codex CLIs, drives a single testbed repo (`AlexBomber12/pipeline-orchestrator-testbed`) for e2e tests covering upload, merge, fix-escalate, redis recovery, sigkill recovery, stop/resume. All e2e tests are **single-repo**.

**Gap revealed by 2026-05-01 multi-repo session:** the production validation event (3 active repos: pipeline-orchestrator + megaraid + sms-gateway) surfaced 5 OBS items that were never caught by the single-repo e2e suite (OBS-AS, AU, AV, AW, AX, AY). These are bugs the test suite had no way to detect because the conditions only manifest with N>1 repos.

**Proposed: multi-testbed integration test infrastructure.**

Provision N additional testbed repos (`pipeline-orchestrator-testbed-1`, `-2`, ..., `-10`) with same scaffolding pattern as existing testbed. Configure e2e harness to spin up subsets of these for tests requiring multiple managed repos simultaneously.

Coverage targets a new test suite would address:

1. **Multi-repo coordination:** N concurrent daemon `run_cycle`s on separate repos do not interfere with each other's state, queue selection, or git working trees. Already informally validated in production, but not in CI.
2. **UI scaling:** dashboard with N repo cards renders correctly, polls at acceptable rates, does not freeze (OBS-AY regression test). Backend `/api/states` performance gate: must respond < 1 second for 10 repos.
3. **Resource contention:** GraphQL quota across N repos stays within budget; auth volumes not corrupted by concurrent reads; Redis pubsub channels properly scoped per-repo.
4. **Onboarding friction:** scaffolder runs idempotently on N freshly-cloned testbeds with diverse pre-existing CLAUDE.md/AGENTS.md content (matrix tests with various combinations).
5. **Cross-repo independence:** failure (HUNG, ERROR) in repo K does not block repos 1..K-1 or K+1..N from progressing.

How many testbed repos to provision: **start with 5, scale to 10 when 5 saturates**. Each additional testbed adds complexity to CI (clone time, GitHub API quota for setup/teardown). 10 is upper-bound for current architecture before per-test isolation costs become prohibitive.

**Test categorization:**

- `tests/e2e/multi/` — new directory for multi-repo tests
- `tests/e2e/multi/test_concurrent_run_cycles.py` — verify N repos run independently
- `tests/e2e/multi/test_dashboard_scaling.py` — UI performance with N cards (when OBS-AY ships)
- `tests/e2e/multi/test_graphql_budget.py` — quota stays within limits at N=5, N=10
- `tests/e2e/multi/test_one_repo_hung.py` — OBS-AW regression, HUNG in 1 of N repos doesn't block others
- `tests/e2e/multi/test_onboarding_matrix.py` — scaffolder against varied CLAUDE.md/AGENTS.md states (OBS-AX regression)

**Type:** test infrastructure. **Estimated:** 2-3 PRs to set up multi-testbed harness (provision script, conftest fixtures, base test patterns), then 1 PR per test case. ~4 PRs total = ~8 daemon-hours initially, growing linearly with each new test added.

### Intra-repo task parallelism (added 2026-05-01 evening, **DEFERRED — backlog only**)

**Decision (2026-05-01):** record as long-term architectural option, not actively planned. Multi-repo parallelism (PR-207, already shipped) gives 3x throughput when 3 repos active. Intra-repo parallelism's marginal benefit (~15-20% sprint speedup on truly independent tasks) does not justify ~8-12 PR refactor effort and state machine complexity at current product stage. Revisit when:

- A specific repo with 50+ independent tasks needs to ship faster than multi-repo can deliver
- Product reaches monorepo customers where multi-repo parallelism is structurally unavailable
- Codex/Claude review throughput becomes the binding constraint (in which case intra-repo parallelism does not help anyway, would be wrong fix)

**Current state:** daemon serializes work per-repo. State machine assumes single `current_pr` at a time. Parallelism is **across repos** (3 repos = 3x throughput) but **not within a repo**.

**Question raised by operator:** could daemon work on multiple tasks in parallel within a single repo (e.g. PR-001 and PR-002 simultaneously)?

**Architectural feasibility analysis:**

Doable but expensive. Required changes:

1. `RepoState.current_pr` becomes `current_prs: list[PRSummary]` keyed by branch.
2. State machine forks per-task: each PR has independent state (CODING/WATCH/FIX/MERGE) progressing concurrently.
3. Working tree isolation: separate clones per active task or careful branch checkout coordination — currently `repo_path` is a single working tree.
4. Resource serialization: only one Claude CLI invocation at a time per machine (rate limits, CPU); intra-repo parallelism does NOT remove this constraint, just hides it as serialized within concurrent state machines.
5. Recovery semantics: sigkill recovery becomes O(N_tasks) reconstruction instead of O(1); restart logic significantly more complex.
6. Merge serialization: still need to serialize PR merges per-repo (merge conflicts, base branch updates).

**Useful for which workloads:**

- ✅ **Independent feature PRs in different subsystems** — backend PR-001 + frontend PR-002, no overlap. Theoretically 2x faster but only if both stages (planning, coding, review, fix, merge) actually parallelize.
- ❌ **Dependent task chains** — PR-002 `Depends on: PR-001` forces sequential regardless. Most sprint plans have heavy dependency chains.
- ❌ **Overlapping file scope** — PR-001 and PR-003 both touch `src/web/app.py` — merge conflicts at PR-002 merge time inevitable.
- ⚠️ **Reviewer (Codex) bandwidth** — Codex reviews are also rate-limited; 2 PRs in flight = 2 review queue entries; not free.

**Realistic speedup:** in a 36-PR Foundation Sprint where ~30% of tasks are truly independent, intra-repo parallelism with capacity 2 might yield ~15-20% sprint completion time reduction at significant complexity cost.

**Multi-repo parallelism (already implemented, PR-207) gives 3x throughput** when 3 repos are active — same productivity gain as intra-repo parallelism level 3 within one repo, **without** the state machine complexity. Operator running 3 different projects simultaneously already gets the win.

**Recommendation:** defer intra-repo parallelism until:

- A specific repo with 50+ independent tasks needs to ship faster than multi-repo can deliver
- All multi-repo bugs (OBS-AS, AU, AV, AW, AX, AY) are resolved and product is stable on 5+ repos
- Codex review throughput becomes the binding constraint (would mean intra-repo parallelism doesn't help anyway)

The "uber-test" the operator described — N parallel tasks per repo across multiple repos — is a **valuable test target for the multi-testbed infrastructure above** even if intra-repo parallelism is not implemented immediately. Test stresses the state machine assumptions and reveals whether the singular `current_pr` invariant holds under genuinely concurrent task selection attempts (race conditions in `_select_next_task` for example).

**Type:** architecture refactor. **Estimated when prioritized:** ~8-12 PRs (RepoState refactor + state machine fork + working tree isolation + recovery refactor + tests + UI changes). Comparable to PR-FUTURE-7 (QUEUE.md elimination) in scope. Defer at least 2-3 months until product surfaces a real demand.

This section captures architectural changes that emerged from the megaraid-dashboard / sms-gateway-v2 onboarding planning. None of these are in Foundation Sprint scope (which is internal cleanup of pipeline-orchestrator itself), but all should be tackled before declaring multi-repo onboarding production-ready for non-author users.

### Architectural problem recap

When planning onboarding of two real external projects (megaraid-dashboard and sms-gateway-v2) on 2026-05-01, six architectural gaps surfaced. A seventh internal-architecture gap was identified the same day during code-level audit of QUEUE.md mechanics:

1. **AGENTS template scope leakage.** The orchestrator's `daemon_managed_content()` extracts daemon-managed sections live from pipeline-orchestrator's own AGENTS.md. That source contains pipeline-orchestrator-specific paths (`tests/e2e/`, `pipeline-orchestrator-testbed`, `src/task_status.py`, `docs/ci-setup.md`) inside the managed regions. When applied to a different repo, those self-references travel verbatim and are nonsense for that repo.

2. **Single global config conflates orchestrator-wide and repo-specific concerns.** `config.yml` has a `daemon` section (orchestrator-wide settings) and a `repos` list (per-repo settings), but per-repo settings are limited to URL, branch, coder, auto_merge, etc. Things like coverage gate percentage are currently hardcoded in the daemon-managed AGENTS.md template, which forces every onboarded repo to use the same gate (currently 100%). Different projects legitimately have different appetites: one project might want 70%, another 100%, another no gate at all.

3. **Onboarding is mechanical, not semantic.** `/onboarding/preview` and `/onboarding/apply` perform a structural merge: append marked sections at the end of AGENTS.md if not already present. They do not detect or resolve **semantic conflicts** — for example, a user's "Workflow Rules" section saying "branches use Conventional Commit prefixes" sits side-by-side with daemon-managed "Branch naming" saying "branches use pr-XXX-slug from task file". The coder reads both, sees the contradiction, and picks one — usually the more recent (daemon-managed), silently overriding the user's existing convention.

4. **scripts/ci.sh scaffold is a silent stub.** When daemon scaffolds a new repo, `scripts/ci.sh` is created as a stub that always returns exit 0 (no actual checks). Daemon thinks CI passes, coder thinks local validation OK, but no real validation runs. PRs with broken code only fail at GitHub Actions CI, wasting FIX FEEDBACK cycles. Operator must manually create real `scripts/ci.sh` before onboarding to avoid this trap.

5. **Scaffold strategy is template-driven, not repo-aware.** The fundamental problem behind #4: `scaffolder.py` copies fixed templates from `templates/` directory regardless of what the target repo already has. It doesn't read existing `pyproject.toml`, doesn't mirror existing `.github/workflows/ci.yml`, doesn't respect existing test markers or coverage thresholds. This works only for greenfield projects built from the orchestrator template. For onboarding existing work, scaffold must be **AI-driven**: detect repo state, generate additions that respect what's there, surface as MICRO PR for operator review. There is also no "observe mode" for trial inspection without committing to full daemon management.

6. **Auth setup is shell-driven, not UI-driven.** The operator must shell into the daemon container (`docker compose run --rm daemon bash`) and run `gh auth login --device-flow`, `claude auth login`, `codex auth login` manually. Tokens persist in mounted volumes, so this is a one-time setup for the author — but it makes the first-time experience for any other user inaccessible. The dashboard already shows auth status (probes for all 3 CLIs exist) but provides no flow to initiate or refresh auth from the UI. PR-FUTURE-4 (AI scaffold) and PR-FUTURE-3 (wizard) both depend on Claude/Codex/GH CLIs being authenticated; without UI-driven auth, the wizard cannot complete its steps for a fresh deployment.

7. **QUEUE.md remains as on-disk artifact with no clear value.** PR-181 untracked `tasks/QUEUE.md` from git but kept the file on disk because 4 production code paths still read it (web UI tasks panel, recovery handler, idle handler legacy fallback, merge handler "mark done"). Code-level audit on 2026-05-01 revealed: the often-cited "coder shim parses QUEUE.md" dependency is **test-only** (the production Claude/Codex CLIs do not read QUEUE.md). Web UI and recovery can equally well consume an in-memory queue snapshot. Keeping QUEUE.md on disk maintains a derived projection alongside the source-of-truth (`tasks/PR-*.md` files), creating two-source confusion, disk I/O on every IDLE cycle, and ~80 LOC of legacy migration branching for "pre-PR-181 repos that still track QUEUE.md upstream". The simplification here is subtractive: remove QUEUE.md entirely, read from PR-*.md headers directly, expose queue snapshot via API instead of file.

### PR-FUTURE-1: AGENTS template scope cleanup

**Problem:** daemon-managed sections embed orchestrator's own implementation details (test paths, testbed repo URL, internal module names). These leak into every onboarded repo's AGENTS.md and confuse coders working on those projects.

**Scope:** rewrite `src/onboarding/agents_md_template.py` so daemon-managed content describes only **orchestrator-level concerns** without project-specific references. Replace concrete paths with abstract phrasing:

- "tests/e2e/ against pipeline-orchestrator-testbed" → "the project's CI workflow on its main branch"
- "src/task_status.py CIStatus enum" → "check status from the GitHub API"
- "docs/ci-setup.md describes GitHub App setup" → remove (orchestrator-internal setup, not part of work mode contract)

The daemon-managed sections describe operational protocol shared across all managed repos: work mode triggers, daemon mode rules, ESCALATE protocol, branch naming for daemon-driven PRs, runbooks, queue stability rules. Nothing about how pipeline-orchestrator itself implements its CI or testbed.

**Approach:** add a new module `src/onboarding/orchestrator_template.py` that defines the canonical orchestrator-level content as Python constants or template strings, separate from `pipeline-orchestrator/AGENTS.md` which is now repo-specific (the orchestrator's own AGENTS.md describes orchestrator's own project, not the orchestrator template).

Migration: pipeline-orchestrator's own AGENTS.md retains marked regions but they are now generated by `daemon_managed_content()` (clean orchestrator-level content) plus orchestrator-specific user-sections describing project-specific things. This way the template stays the authoritative source for managed regions across all repos including the orchestrator itself.

**Out of scope:** everything else — this PR is purely about scoping the template content correctly.

**Type:** refactor. **Complexity:** medium. **Estimated:** 1-3 PRs, ~3 daemon-hours. Replace hardcoded paths in 57-LOC `agents_md_template.py` with abstract phrasing.

### PR-FUTURE-2: Per-repo config file with inheritance

**Problem:** per-repo settings beyond URL/branch/coder/auto_merge are currently impossible to express. Coverage gate, branch naming preference, CI script location, project-specific rate-limit thresholds — all are global or hardcoded.

**Scope:** introduce per-repo config file with explicit inheritance from global defaults.

**Architecture options to evaluate:**

Option A — **Per-repo file in repo's tasks/ directory** (e.g. `tasks/orchestrator-config.yml`). Lives in the repo itself, version-controlled with the repo. Daemon reads on every IDLE cycle. Pro: repo-self-describing, survives daemon restarts, easy for repo owner to edit. Con: edits require a PR, loop time slow.

Option B — **Per-repo section in daemon's config.yml** (extend existing `repos:` entry with arbitrary settings dict). Lives in daemon config. Pro: fast iteration via UI Settings panel. Con: repo owner cannot edit without daemon access.

Option C — **Hybrid:** per-repo file in repo for repo-owner concerns (coverage gate, branch preference, CI script path, test command, project type hints), plus daemon-side config for orchestrator-side concerns (rate-limit overrides, polling interval, daemon-internal flags).

**Recommendation:** evaluate Option C during implementation; hybrid is most flexible but also most complex.

**Inheritance model:** every per-repo setting has a global default in daemon config. Per-repo file/section can override with explicit values. UI shows for each setting: current effective value, source (global default vs per-repo override), and an "Inherit" button to revert to global.

**Settings scope (initial set):**

- `coverage_gate_percent` (default 100, override per-repo)
- `branch_naming_preference` (`pr-XXX-slug` daemon-driven vs `feat/`/`bugfix/` Conventional Commit; affects what daemon includes in branch_naming managed section)
- `ci_script_path` (default `scripts/ci.sh`, override for projects using different layouts)
- `test_command` (override default `pytest --cov=src --cov-fail-under=...`)
- `auto_merge_threshold` (per-repo override of `auto_merge` setting — could be "approve+merge", "approve only", "review only")
- `idle_poll_interval` (per-repo override)
- `escalate_threshold_hours` (when stuck PR escalates to HUNG)

**UI/UX:** Settings page redesign required:

- Click on repo card → opens repo settings panel as side drawer or modal
- Two columns: "Global default" and "Per-repo override"
- Each setting has a checkbox "Use global" or explicit value input
- "Reset all to global" button at top
- Save button persists changes; daemon reloads on next cycle

**Migration:** existing repos start with all settings inherited (no per-repo overrides). Operator gradually opts repos into specific overrides via UI.

**Out of scope:** rewriting all daemon settings as overridable per-repo. Initial set is targeted at the most-needed overrides.

**Type:** feature. **Complexity:** high. **Estimated:** 3-4 PRs, ~6-8 daemon-hours. Schema + load logic + UI drawer + tests.

### PR-FUTURE-3: Onboarding wizard with semantic conflict resolution

**Problem:** current `/onboarding/preview` shows mechanical diff (lines added/removed) but does not detect or surface semantic conflicts between user's existing AGENTS.md sections and daemon-managed sections.

Example conflict that mechanical diff misses: user's "Workflow Rules" section says "branches use Conventional Commit prefixes". Daemon-managed "Branch naming" says "branches use pr-XXX-slug". Both end up in the file. Coder picks one — usually the most recent or most specific.

**Scope:** add a wizard flow that:

1. **Detects conflicts** by checking for known overlapping section names (user has `## Workflow` or `## Branches` while daemon will add `## Branch naming`; user has `## Testing` while daemon will add `## CI gates and merge criteria`).

2. **Generates a resolution prompt** for the operator. The prompt is a predefined template like:

> Here is the existing AGENTS.md for `<repo>`:
> ```
> <user's AGENTS.md>
> ```
> The pipeline-orchestrator daemon will append the following operational sections:
> ```
> <daemon-managed sections preview>
> ```
> Identify any semantic contradictions between user content and daemon-managed content. For each conflict, propose how to resolve it: either edit the user section to defer to daemon-managed for that concern, edit the daemon-managed section to acknowledge the user convention, or add a note section explaining the split.

3. **Either runs the prompt via Claude CLI** in onboarding-helper mode (a short separate task on a feature branch in the target repo, not on main) **or shows the prompt to operator** for manual paste into a Claude/Codex chat.

4. **Surfaces the proposed merged AGENTS.md** in UI for operator approval.

5. **Applies on approval** via existing `/onboarding/apply` endpoint with the merged content.

**Detection rules (initial heuristic set):**

- User section name overlaps with daemon-managed section name (case-insensitive substring match)
- User section contains keywords from daemon-managed content domain (e.g. user mentions "branch", "CI", "review", "escalation")
- User content explicitly contradicts daemon content (hardest — requires LLM judgment, hence the prompt approach)

**Out of scope:** automatic LLM-based merge without operator review. Human-in-the-loop is mandatory for first version.

**Type:** feature. **Complexity:** high. **Estimated:** 3-4 PRs, ~6-8 daemon-hours. Wizard state machine + conflict detection UX + integration.

### PR-FUTURE-4: AI-driven onboarding scaffold (replaces template-driven scaffolder)

**Problem:** current `scaffold_repo()` in `src/daemon/scaffolder.py` is **template-driven** — it copies fixed template files from `templates/` directory regardless of repo state. This works only when the repo is built FROM the orchestrator template (greenfield case) and **breaks** when onboarding existing repos:

- Generates stub `scripts/ci.sh` that always returns exit 0, silently bypassing all real validation. Daemon thinks CI passes, coder thinks local validation OK — but PRs go through with broken code that only fails at GitHub Actions later, wasting FIX FEEDBACK cycles.
- Ignores existing `pyproject.toml` configuration (ruff, mypy, pytest settings, coverage threshold, test markers).
- Ignores existing `.github/workflows/ci.yml` which often already contains the canonical commands the project uses.
- Imposes pipeline-orchestrator's own coverage gate (100%) on projects that legitimately have different appetites (e.g. megaraid-dashboard had no coverage threshold set; sms-gateway-v2 already used 100%).
- Creates `tasks/QUEUE.md` and `scripts/make-review-artifacts.sh` even if project doesn't intend to use the orchestrator (prevents read-only inspection mode).

The current workaround is operator-time-intensive: manually create real `scripts/ci.sh` before onboarding, manually edit AGENTS.md, manually decide gitignore additions. Easy to forget; not viable for non-author users.

**Real example (2026-05-01 onboarding planning of megaraid + sms-gateway):** both projects already have full GitHub Actions CI with `ruff check`, `ruff format --check`, `mypy src`, `pytest`. They use `pyproject.toml` for tool config including pytest `addopts`. They differ in coverage threshold (megaraid: none; sms-gateway: `--cov-fail-under=100` in addopts). Template scaffolder's stub `scripts/ci.sh` would shadow all this — coder would think CI passes when running stub, while real GitHub Actions runs different (correct) commands.

**Scope:** replace template-driven scaffolder with AI-driven one that detects repo state and generates project-aware additions as a MICRO PR for operator review.

**Architecture change:**

The scaffold step becomes a **two-phase process**:

1. **Detection phase (Python code, deterministic):**
   - Scan repo root for stack indicators: `pyproject.toml`, `package.json`, `Cargo.toml`, `go.mod`, `Gemfile`, `pom.xml`, etc.
   - Scan for existing CI tooling: `.github/workflows/*.yml`, `.gitlab-ci.yml`, `Makefile` with test/ci targets, `.pre-commit-config.yaml`, `tox.ini`.
   - Read pyproject.toml `[tool.*]` sections (ruff, mypy, pytest, coverage) to extract configured commands and thresholds.
   - Read existing `scripts/ci.sh` (if exists, non-stub) — content hash check against template stub.
   - Read existing `AGENTS.md` and `CLAUDE.md` (if exist).
   - Output: structured "repo profile" object — language, package manager, CI tool, test framework, lint config, coverage threshold (if any), test markers, existing files, project intent (orchestrator-managed vs read-only inspection).

2. **Generation phase (AI-driven, via Claude/Codex CLI):**
   - Pass repo profile to coder CLI as a structured task.
   - Coder produces: proposed `scripts/ci.sh` mirroring project's existing CI commands; proposed AGENTS.md additions (managed sections + note section if user already has AGENTS.md, or full template if greenfield); proposed `.gitignore` additions; nothing else by default.
   - Coder opens MICRO PR with the additions; operator reviews and merges.

**Tiered approach — simple-to-complex (added 2026-05-01, confirmed):**

Not every repo needs AI generation. The scaffolder classifies the target repo into one of three tiers and applies the cheapest path that produces correct output. AI cost (CLI invocation + subscription quota + ~30-60s latency) is paid only when actually needed.

**Tier 0: truly empty repo** (`_head_is_unborn(repo_path)` returns True — already detected in code)
- No commits exist on any branch.
- Action: copy all template files (`AGENTS.md`, `CLAUDE.md`, `tasks/`, `scripts/`, `.gitignore` entries) directly. No detection needed; no AI invocation.
- Path: matches **current scaffolder behaviour** for unborn HEAD case. Already correct.
- Cost: instant, deterministic, no quota consumed.

**Tier 1: greenfield-ish** (commits exist but no project markers)
- Detection: count of project markers (`pyproject.toml`, `package.json`, `Cargo.toml`, `go.mod`, `.github/workflows/`, `src/`, `tests/`, `Makefile`, `AGENTS.md`, `CLAUDE.md`) is ≤ 1. Typically only README and/or LICENSE present.
- Action: copy templates + add a deliberately minimal `scripts/ci.sh` (e.g. `echo "no CI configured yet"; exit 0`) with a comment block instructing operator to fill in real commands once project structure exists.
- Path: deterministic copy with one extra step (placeholder `scripts/ci.sh`); no AI invocation.
- Cost: instant, deterministic, no quota consumed.
- Use case: operator creates a fresh repo with just `git init` and `README.md`, attaches to orchestrator from day one. Project grows under daemon coordination from scratch — daemon's first PRs naturally introduce structure that later upgrades the repo to Tier 2 territory.

**Tier 2: established project** (real codebase with multiple project markers)
- Detection: project marker count > 1. Real codebase exists with established CI, conventions, test markers, etc.
- Action: detection phase (Python deterministic) extracts repo profile. Generation phase invokes coder CLI to produce AGENTS.md additions, `scripts/ci.sh` mirroring existing CI, `.gitignore` additions. MICRO PR opened for operator review.
- Path: full AI-driven flow.
- Cost: ~30-60s CLI latency, subscription quota or API tokens consumed, MICRO PR review effort by operator.
- Use case: onboarding existing projects (megaraid, sms-gateway, any external repo with non-trivial existing structure).

**Tier classification logic (deterministic, runs first):**

```python
def detect_scaffold_tier(repo_path: str) -> Literal["empty", "greenfield", "established"]:
    if _head_is_unborn(repo_path):
        return "empty"
    markers = {
        "pyproject.toml", "package.json", "Cargo.toml", "go.mod",
        ".github/workflows", "src", "tests", "Makefile",
    }
    found = sum(1 for m in markers if (Path(repo_path) / m).exists())
    if found <= 1:
        return "greenfield"
    return "established"
```

**Why tier'ing matters strategically:**

- **Cost-conscious:** AI invocation costs daemon quota and adds latency. For Tier 0/1 cases the AI has nothing meaningful to detect — running it would waste quota for zero-information outcome.
- **Onboarding flow advertises differently:** Tier 0 path is the "start a new project on orchestrator from scratch" workflow. Tier 2 path is the "bring an existing project under orchestrator management" workflow. Different operators have different mental models; UX should make both feel native.
- **Failure surface differs:** AI generation has its own failure modes (CLI timeout, quota exhausted, coder hallucination). Tier 0/1 paths have **no** AI-specific failure modes. Most onboarding cases (especially future operator base) will probably bias toward Tier 0/1 — keeping those paths AI-free improves overall reliability.
- **Demo-friendly:** "Watch me onboard a new project in 30 seconds" demo uses Tier 0 path. AI-driven Tier 2 demo takes minutes and may surface hiccups. Both are real, but Tier 0 is the dramatic visual.

**Detection rules (Tier 2 only, when AI generation runs):**

For Python projects (most common):

- If `.github/workflows/ci.yml` exists with steps `ruff check`, `ruff format --check`, `mypy src`, `pytest`, generate `scripts/ci.sh` with **exact same commands** (no impositions).
- If `pyproject.toml` `[tool.pytest.ini_options]` `addopts` contains `--cov-fail-under=N`, propagate that N to `scripts/ci.sh`. **Do not impose orchestrator's 100%.**
- If `pyproject.toml` has no coverage threshold, propose 0 (no gate) — operator can tighten later via per-repo config (PR-FUTURE-2).
- If project has test markers (pytest `markers = [...]`), respect them: only run unit-equivalent in `scripts/ci.sh` (e.g. `pytest -m "not integration and not e2e"` if those markers exist).
- If `Makefile` has `make ci` or `make test` target, prefer wrapping that in `scripts/ci.sh` rather than reimplementing.

For Node, Rust, Go projects: mirror existing CI scripts when present.

**Idempotency:**

- If `scripts/ci.sh` already exists and content hash differs from template stub — **leave it alone**. Operator has a working CI script; don't replace.
- If existing `AGENTS.md` already has managed marker regions — apply reconciliation (existing PR-192a/b/c flow), don't regenerate from scratch.

**Critical CLAUDE.md replacement rule (verified 2026-05-01):**

The scaffolder **must overwrite `CLAUDE.md`** with the single line `Read and follow AGENTS.md in this repository.` This is non-negotiable, even if the repo has an existing CLAUDE.md with user-authored Claude-specific notes.

**Why:** Claude CLI is invoked with `--append-system-prompt-file CLAUDE.md` and a literal user prompt of `"PLANNED PR"`. CLAUDE.md content becomes the system prompt. AGENTS.md is **not** automatically attached — coder must elect to read it. When CLAUDE.md contains user-authored notes (project conventions, style preferences, technology hints), those notes compete with the redirect-to-AGENTS instruction. Coder reads CLAUDE.md as authoritative system prompt, applies the user notes, but never resolves what `"PLANNED PR"` means as a command. Result: coder asks operator for clarification ("Could you clarify what you'd like me to do? PLANNED PR alone isn't enough context"), exits 0 without push, daemon classifies as HUNG.

**Verification event (2026-05-01 evening):** both megaraid-dashboard and sms-gateway-v2 went HUNG with the clarification-question pattern after onboarding. Their original CLAUDE.md files contained 8-12 lines of user-authored Claude-specific notes (storcli flags, ModemManager D-Bus, "prefer existing modules", "long-term sustainable solutions"). Replacing CLAUDE.md with the single line `Read and follow AGENTS.md in this repository.` immediately unblocked the coder on next cycle — coder read AGENTS.md, found the daemon-managed `work_modes` section with PLANNED PR runbook definition, opened a real PR with code changes.

**User-authored Claude-specific notes belong in AGENTS.md, not CLAUDE.md.** Scaffold should:

1. Read existing `CLAUDE.md` content.
2. If it contains user-authored notes (anything beyond a redirect line), extract those notes into a new "Claude-specific guidance" section in user's portion of AGENTS.md (above the daemon-managed marker regions).
3. Replace `CLAUDE.md` with the single line `Read and follow AGENTS.md in this repository.`
4. Surface this as part of the MICRO PR diff for operator review with explicit explanation of why CLAUDE.md must be minimal.

**Pipeline-orchestrator's own CLAUDE.md** is already this single line — that is why coder works correctly on it. External repos onboarded via existing scaffolder kept their full CLAUDE.md and broke. This is a **product gap** not previously visible because pipeline-orchestrator was the only managed repo.

**UI integration (part of PR-FUTURE-3 wizard):**

After clone, before "active" toggle:

1. Wizard runs detection phase, surfaces "Repo profile" panel: detected language/CI/coverage/test markers.
2. Wizard runs generation phase via coder CLI in a separate working directory. Operator can monitor progress.
3. Wizard surfaces proposed MICRO PR diff. Operator approves, modifies, or rejects.
4. On approval, MICRO PR is opened on the target repo (not on pipeline-orchestrator). Operator merges via GitHub UI.
5. Daemon picks up the merged scaffolding on next IDLE cycle and starts the repo as active.

**Manual fallback path:**

For operators who prefer manual control or for projects where AI generation goes wrong:

- Wizard exposes "Manual scaffold" button at any point.
- Generates a checklist file `docs/orchestrator-scaffold-todo.md` in the target repo with todo items: "Create scripts/ci.sh with these commands [generated from detection]", "Update .gitignore", "Add AGENTS.md note section here".
- Operator does the changes manually in their IDE, commits, pushes; daemon picks up on next cycle.

**Why this matters strategically:**

The current template-driven scaffolder is a **product gap**, not a bug. It assumes greenfield projects built from the orchestrator template. Real users have existing projects with established CI. The AI-driven scaffold is what makes pipeline-orchestrator viable as a tool for **onboarding existing work**, not just managing greenfield work.

**Out of scope (for first version):**

- Detecting Java/Maven, Ruby/Bundler, .NET, Elixir/Mix, Haskell/Cabal stacks. Initial set: Python, Node, Rust, Go.
- Multi-language monorepos (project with both Python and Node sub-packages).
- Custom CI tools (Jenkins, CircleCI, BuildKite, Travis) — only GitHub Actions detection in v1.
- Detecting and adapting to non-trivial Makefile target chains.

**Type:** feature. **Complexity:** high. **Estimated:** 4-5 PRs, ~8-10 daemon-hours. Detection module + generation flow + integration + tests.

### PR-FUTURE-5: Read-only / observation-mode onboarding

**Problem:** current scaffolder always assumes the operator wants to put the repo under daemon-driven PR creation. There is no "just observe this repo" mode where:

- Daemon reads the repo's PR queue and CI status for monitoring/dashboard purposes.
- Daemon does NOT add `tasks/`, `scripts/`, `artifacts/`, or `.gitignore` modifications.
- Daemon does NOT create PRs in the repo.

This is useful for:

- Trial onboarding ("let me see what dashboard looks like with my repo on it before committing to use it").
- Repos where the operator wants pipeline-orchestrator to surface metrics (cost-per-merged-PR, GraphQL burn) without touching workflow.
- Testing isolation properties of multi-repo behavior without polluting target repos.

**Scope:** add a per-repo `mode` field with values `managed` (current default — daemon scaffolds, creates PRs) and `observe` (daemon reads-only, no scaffold, no PR creation). UI Settings exposes this as "Repo mode" radio: Managed (full pipeline) vs Observe (metrics only).

**Behavior changes when mode=observe:**

- Skip scaffold step entirely on add-repo.
- Skip AGENTS.md reconciliation.
- Daemon main loop polls PR list for status (read-only) but never enters CODING/FIX/MERGE states.
- Dashboard shows repo card with state="OBSERVING" (new state value), event log shows merge events from external PR creators (you, manual, other tools), GraphQL/cost metrics tracked.
- Operator can switch mode managed↔observe via settings; switching to managed triggers scaffold + onboarding wizard.

**UI/UX:**

- Repo settings panel (from PR-FUTURE-2 per-repo config) gets "Mode" toggle at top.
- "Observe" mode hides task-related UI for that repo (no upload tasks button, no QUEUE.md viewer).
- Cost/burn metrics still surface for observe-mode repos.

**Out of scope:** retroactive observe mode for repos that were originally scaffolded as managed (would require cleanup of tasks/QUEUE.md, scripts/, etc — leave for follow-up).

**Type:** feature. **Complexity:** medium. **Estimated:** 2-3 PRs, ~4 daemon-hours. Mode field + state value + UI hide tasks panel.

### PR-FUTURE-7: Eliminate tasks/QUEUE.md entirely (in-memory queue model)

**Problem:** `tasks/QUEUE.md` is a derived artifact regenerated each IDLE cycle from the structured `tasks/PR-*.md` headers, but it persists on disk and 4 production code paths still read it. After PR-181 untracked it from git, the file lost its source-of-truth role but kept its on-disk-interface role. This creates several latent issues:

1. **Two-source confusion.** `tasks/PR-*.md` files are the authoritative source. `QUEUE.md` is derived from them. But the recovery handler reads QUEUE.md on startup before PR-*.md files are parsed, creating a window where the derived view is consulted ahead of the source.
2. **Disk I/O on every IDLE cycle.** Every time daemon enters IDLE, it regenerates QUEUE.md content in-memory and writes it to disk for downstream consumers. That write costs filesystem ops, can fail (disk full, permissions, etc), and the failure handling (`_write_generated_queue_md` swallows exceptions) hides issues.
3. **Legacy migration overhead.** `_origin_queue_md_tracked()` probes git on every recovery cycle to determine whether the repo is post-PR-181 or pre-PR-181. This branching keeps a code path alive for "legacy repos that still track QUEUE.md upstream" — paying complexity tax for a state that the orchestrator's own deployment has already moved past.
4. **Onboarding friction for external repos.** Every newly onboarded repo must add `tasks/QUEUE.md` to `.gitignore` to prevent the daemon from accidentally committing it (the scaffolder's gitignore step does this, but only on repos that already have a `.gitignore`; some repos have non-standard layouts).
5. **Race vulnerability.** Multi-process scenarios (recovery + IDLE running in close succession on daemon restart) read different snapshots of QUEUE.md, potentially making different selection decisions based on stale views.

The original justification for keeping QUEUE.md after PR-181 has eroded:

- **Coder shim dependency was misidentified.** The shim that parses QUEUE.md (`tests/e2e/lib/coder_shim.sh::parse_doing_task`) is a **test-only mock coder** used inside the e2e test stack, not the production Claude/Codex CLIs. Production coders read their task scope from the prompt, not from disk. Removing QUEUE.md does not affect production coders.
- **Web UI tasks panel** reads QUEUE.md only because that was the convenient interface in PR-157 era. It can equally well be served by an in-memory snapshot exposed via an API endpoint backed by the same `_generate_queue_md` function called for read instead of write.
- **Recovery handler** reads QUEUE.md from `origin/{branch}` via `git show` to reconstruct state on startup. Post-PR-181 repos gitignore QUEUE.md, so the file is absent from origin anyway — recovery already falls through to "treat as empty queue and defer to preflight + IDLE regeneration" path. The remaining `git show` probe path exists only for legacy pre-PR-181 repos.
- **Idle handler legacy queue fallback** parses QUEUE.md to detect "visible legacy queue entries" — entries in QUEUE.md that the DAG-based selector did not produce. This catches accidental hand-edits to QUEUE.md and guards against ghosts surviving across daemon restarts. Without on-disk QUEUE.md, this concern disappears entirely (no surface to hand-edit).

**Scope:** remove `tasks/QUEUE.md` from the orchestrator's runtime model. Keep `_generate_queue_md` as a pure function (returns the rendered string). Replace all read sites with consumers of an in-memory snapshot held on `RepoState` or computed on-demand from the structured PR-*.md headers.

**Architecture:**

1. **In-memory queue model on `RepoState`.** Add `RepoState.current_queue: list[QueueTask] | None` populated by IDLE handler at the end of each cycle. Web UI and recovery read from this state instead of from disk.

2. **Recovery without QUEUE.md.** `recover_state` parses `tasks/PR-*.md` files directly via `parse_task_header` (already used by DAG selector). Reconstructs the same `list[QueueTask]` shape that QUEUE.md parsing currently produces. Drops the `_origin_queue_md_tracked()` probe and the `git show origin/{branch}:tasks/QUEUE.md` path.

3. **Web UI via API endpoint.** Add `/api/repo/{name}/queue` returning JSON of `RepoState.current_queue`. HTMX tasks panel fragment fetches from this endpoint. Removes `parse_queue(queue_path)` calls in `app.py`.

4. **Upload validation unchanged.** Operator uploads `PR-*.md` files via UI; uploads do NOT include QUEUE.md anymore (the upload validation block that handled QUEUE.md as a special filename gets simplified — only PR-*.md files are accepted).

5. **Scaffolder simplification.** Remove `tasks/QUEUE.md` from `_GITIGNORE_ENTRIES` and from the scaffolder's "create from template" step. Onboarding repos no longer need `tasks/QUEUE.md` in their gitignore.

6. **Test shim migration.** The e2e test shim's `parse_doing_task` function is rewritten to read `tasks/PR-*.md` files directly and find the one with `Status: DOING` in its frontmatter or header. This is a test-side change, not production.

7. **Merge handler simplification.** `_mark_queue_done` (currently flips `DONE` row in QUEUE.md text) becomes `RepoState.current_queue` update directly. No file write.

8. **Backward compatibility.** Repos that already have `tasks/QUEUE.md` checked into git (legacy pre-PR-181 repos) get a one-time daemon-side cleanup: on first IDLE cycle after this PR ships, daemon detects tracked QUEUE.md and opens an auto-MICRO-PR to remove it via `git rm` + commit. After merge, the file is gone from git and the local working-tree copy is deleted on next clone.

**Why this is worth doing:**

- Removes a layer of indirection that has no remaining purpose.
- Simplifies onboarding (one less file to gitignore, one less concept to explain).
- Eliminates "legacy repo" branching in recovery and idle handlers (~80 LOC of dead-end branches).
- Removes a class of race conditions (concurrent reads of stale QUEUE.md snapshots).
- Aligns the runtime model with the source-of-truth model: `tasks/PR-*.md` is the authoritative form, and the system reads that authority directly without an intermediate cached projection.

**Why this might NOT be worth doing yet:**

- The work touches recovery handler, idle handler, merge handler, web UI, scaffolder, and tests. Big surface area.
- Recovery handler has subtle invariants (state reconstruction on daemon restart) that are currently tested via QUEUE.md content. Migrating tests to PR-*.md fixtures requires care.
- The legacy migration MICRO PR (point 8) needs to handle edge cases: repos where operators have manually edited QUEUE.md, repos in mid-cycle when the migration runs, multi-repo scenarios where some are migrated and some aren't.

**Out of scope:**

- Renaming `_generate_queue_md` to something more honest like `render_queue_view`. Cosmetic; defer.
- Replacing `QueueTask` dataclass with a richer model. Different concern; defer.
- Cross-repo queue aggregation in dashboard ("show all DOING tasks across all repos"). Different feature; defer.

**Type:** refactor. **Complexity:** high. **Estimated:** 1-2 PRs, ~4 daemon-hours. 4 read-site refactor onto RepoState.current_queue snapshot + test shim migration. Optional 3rd PR for legacy auto-migration MICRO PR generator if needed.

**Strategic placement:** this PR can ship anywhere after Foundation Sprint completes. It is independent of PR-FUTURE-1 through PR-FUTURE-6 (those concern external onboarding; this concerns internal queue model). Worth doing **before** PR-FUTURE-3 (onboarding wizard) because the wizard's per-repo health check and tasks panel become simpler when QUEUE.md is gone.



**Problem:** current scaffolder always assumes the operator wants to put the repo under daemon-driven PR creation. There is no "just observe this repo" mode where:

- Daemon reads the repo's PR queue and CI status for monitoring/dashboard purposes.
- Daemon does NOT add `tasks/`, `scripts/`, `artifacts/`, or `.gitignore` modifications.
- Daemon does NOT create PRs in the repo.

This is useful for:

- Trial onboarding ("let me see what dashboard looks like with my repo on it before committing to use it").
- Repos where the operator wants pipeline-orchestrator to surface metrics (cost-per-merged-PR, GraphQL burn) without touching workflow.
- Testing isolation properties of multi-repo behavior without polluting target repos.

**Scope:** add a per-repo `mode` field with values `managed` (current default — daemon scaffolds, creates PRs) and `observe` (daemon reads-only, no scaffold, no PR creation). UI Settings exposes this as "Repo mode" radio: Managed (full pipeline) vs Observe (metrics only).

**Behavior changes when mode=observe:**

- Skip scaffold step entirely on add-repo.
- Skip AGENTS.md reconciliation.
- Daemon main loop polls PR list for status (read-only) but never enters CODING/FIX/MERGE states.
- Dashboard shows repo card with state="OBSERVING" (new state value), event log shows merge events from external PR creators (you, manual, other tools), GraphQL/cost metrics tracked.
- Operator can switch mode managed↔observe via settings; switching to managed triggers scaffold + onboarding wizard.

**UI/UX:**

- Repo settings panel (from PR-FUTURE-2 per-repo config) gets "Mode" toggle at top.
- "Observe" mode hides task-related UI for that repo (no upload tasks button, no QUEUE.md viewer).
- Cost/burn metrics still surface for observe-mode repos.

**Out of scope:** retroactive observe mode for repos that were originally scaffolded as managed (would require cleanup of tasks/QUEUE.md, scripts/, etc — leave for follow-up).

**Type:** feature. **Complexity:** medium. **Estimated:** 2-3 PRs, ~4 daemon-hours. Mode field + state value + UI hide tasks panel.

### PR-FUTURE-6: Auth flow for onboarding wizard

**Problem:** current auth setup is a **manual one-time operator step** done outside the orchestrator UI. The operator runs `docker compose run --rm daemon bash` and inside the container runs `gh auth login --device-flow`, `claude auth login`, `codex auth login`. Auth credentials are stored in mounted volumes (`CLAUDE_CONFIG_DIR=/data/auth/claude`, `GH_CONFIG_DIR=/data/auth/gh`, `codex-auth:/data/auth/.codex`) which persist across container restarts.

The UI (`src/web/app.py`) has auth status probes (`_check_claude_auth`, `_check_codex_auth`, `_check_gh_auth`) that report whether each CLI is authenticated, surfaced in dashboard. But there is **no flow** to perform initial auth or re-auth from the UI — the operator must shell into the container.

This works for author's own use (do auth once, forget about it). It **does not work** for onboarding wizard concept where:

1. New operator opens the dashboard for the first time, has not done CLI auth yet — wizard must guide them through GH/Claude/Codex auth before any onboarding step.
2. Auth tokens expire (Claude OAuth 7-day refresh, GH device-flow tokens, Codex sessions) — wizard must surface re-auth flow when probes fail.
3. Multiple operators on the same daemon (future product evolution) — each operator needs their own auth scope.

The **AI-driven scaffold (PR-FUTURE-4) depends on this**: when wizard runs Claude/Codex CLI to generate scaffold MICRO PR, it needs valid Claude/Codex auth. When MICRO PR is opened on target repo, it needs valid GH auth with write access to that repo. Without auth flow, wizard cannot complete.

**Scope:** add UI flow for initiating and re-authenticating each of the 3 auth providers without shell access.

**Architecture:**

Three auth providers, each with different flow:

1. **GitHub CLI (`gh`):** device flow. UI surfaces "Connect GitHub" button. Backend runs `gh auth login --device-flow --web` via subprocess in daemon container, captures the device code from stderr, surfaces it in UI with "open https://github.com/login/device and enter `XXXX-XXXX`" instruction. UI polls `gh auth status` every 5s; on success, surfaces "GitHub connected as @username".

2. **Claude CLI:** OAuth flow via `claude auth login`. Similar pattern — backend runs CLI, captures device code or auth URL, surfaces in UI. Polls `claude auth status`.

3. **Codex CLI:** session-based via `codex auth login`. Same pattern.

For each provider, also add **"Disconnect"** button (`gh auth logout`, `claude auth logout`, `codex auth logout`) and **"Re-auth"** combination button (logout + login).

**Onboarding wizard integration:**

Wizard step 0 (before any repo-add step) checks `_collect_auth_status()`:

- All 3 green → proceed to repo-add.
- Any provider not connected → surface "Connect <provider>" panel; block wizard until at least GH + 1 coder (Claude OR Codex) are connected.
- Auth panel shows what each provider is needed for: "GitHub: clone repos, create PRs, post review comments. Claude: AI-driven scaffold, code generation. Codex: alternative coder, PR review."

**Token expiry handling:**

Daemon main loop already polls auth status. When a probe transitions from green to red (token expired), surface a banner in UI: "Claude auth expired. [Re-authenticate]". Daemon does **not** auto-re-auth (too risky, security-relevant). Operator clicks button to start re-auth flow.

**Multi-account support (out of scope for v1):**

Future iteration: per-repo override of which GH account / Claude account / Codex account to use. Useful when operator works on org repo (separate GH account from personal). For v1, single global auth shared across all managed repos.

**Security considerations:**

- All auth flows happen inside daemon container. Tokens never leave container.
- UI surfaces device codes via SSE (or polled fetch), not via persistent storage. After auth completes, device code is forgotten.
- Re-auth flows revoke old token before issuing new one (atomic replacement, no overlap).
- Backend rate-limits auth flow attempts (1 per 30s per provider) to prevent UI spam if auth is broken.

**Why this matters strategically:**

The current "shell in and set up" model is fine for the author. For pipeline-orchestrator to be **viable as a tool for non-author users**, auth must be UI-driven. Otherwise the first-time experience is "clone the repo, copy docker-compose.yml, edit it, docker compose up, then `docker compose run --rm daemon bash`, then `gh auth login --device-flow`..." — which loses 80% of users.

**Out of scope:** SAML/SSO providers, organization-level GitHub Apps with installation IDs (mentioned in OBS-AC Leverage 6 GitHub App migration — separate workstream). v1 covers: personal access tokens via device flow for `gh`, OAuth for `claude`, session for `codex`.

**Type:** feature. **Complexity:** medium. **Estimated:** 3-4 PRs, ~6 daemon-hours. Per-provider device flow (single shared pattern) + UI polling + expiry banner. Security-sensitive but the device-flow pattern is well-trodden..

### Sequencing

The seven PRs above build on each other:

1. **PR-FUTURE-6 first** (auth flow). Without UI-driven auth, the wizard cannot run AI scaffolding (PR-FUTURE-4) which needs Claude/Codex CLI authenticated. Foundational. Can ship independently of others — improves existing single-operator UX even before wizard exists.

2. **PR-FUTURE-1 second** (AGENTS template scope cleanup). Without clean orchestrator-level template, every other onboarding effort works around dirty template.

3. **PR-FUTURE-7 second-parallel** (eliminate QUEUE.md). Internal architecture cleanup, independent of onboarding work. Ships in parallel with PR-FUTURE-1; touches recovery/idle/merge handlers + web UI + scaffolder. Worth doing before PR-FUTURE-3 wizard so the wizard's per-repo health check and tasks panel become simpler.

4. **PR-FUTURE-2 third** (per-repo config). Inheritance model unblocks coverage_gate_percent and other settings being expressible.

5. **PR-FUTURE-4 fourth** (AI-driven scaffold). Replaces template-driven scaffolder. Depends on PR-FUTURE-6 (auth) and PR-FUTURE-1 (clean template). Can ship before PR-FUTURE-3 wizard — operates as standalone CLI command initially, later integrated into wizard.

6. **PR-FUTURE-3 fifth** (onboarding wizard UI). Integrates auth flow (PR-FUTURE-6), per-repo config (PR-FUTURE-2), AI scaffold (PR-FUTURE-4), and (if PR-FUTURE-7 shipped) the simpler queue model into a coherent UI flow with semantic conflict resolution.

7. **PR-FUTURE-5 last** (observe mode). Adds per-repo `mode` field. Builds on PR-FUTURE-2 (per-repo config) and PR-FUTURE-4 (scaffold can be skipped for observe-mode repos).

Critical path: **6 → 1 → 4**. PR-FUTURE-7 parallel to 1 (independent track, same time). Rest can parallelize after that.

**Realistic timing (corrected 2026-05-01 after second pass — first pass was inflated 5x):**

The first pass at decomposition mistakenly counted each sub-task (data model, detection logic, tests, docs) as a separate PR. In reality the daemon ships **complete features in single PRs** (or modest 2-3 PR splits when scope is genuinely large). The existing onboarding framework (`reconciliation.py` + `agents_md_template.py` + `markdown_sections.py` = 247 LOC total) was built in 3 PRs (PR-192a/b/c), not 30. Same scaling applies here.

Honest sizing per PR-FUTURE:

- **PR-FUTURE-1 template cleanup:** 1-3 PRs. Replace hardcoded paths in 57-LOC `agents_md_template.py`. Optional split per-managed-section if review wants granularity. ~3 daemon-hours.
- **PR-FUTURE-2 per-repo config:** 3-4 PRs. Schema + load + UI drawer + tests. ~6-8 daemon-hours.
- **PR-FUTURE-3 wizard UI:** 3-4 PRs. Wizard state machine + conflict detection UX + integration. ~6-8 daemon-hours.
- **PR-FUTURE-4 AI scaffold:** 4-5 PRs. Detection module + generation flow (call coder CLI) + integration + tests. The work is moderate; the existing scaffolder is 382 LOC and the new flow swaps detection/generation in. ~8-10 daemon-hours.
- **PR-FUTURE-5 observe mode:** 2-3 PRs. Mode field + state value + UI hide. ~4 daemon-hours.
- **PR-FUTURE-6 auth flow:** 3-4 PRs. Per-provider device flow (one shared pattern) + UI polling + expiry banner. ~6 daemon-hours.
- **PR-FUTURE-7 QUEUE.md elimination:** 1-2 PRs. 4 read-site refactor onto `RepoState.current_queue` snapshot + test shim migration. ~4 daemon-hours.

**Total daemon work: ~17-25 PRs, ~37-44 daemon-hours = 1.5-2 daemon-working-days at 17 PR/day throughput.**

**Calendar: 1-2 weeks** with buffer for testing days between batches, strategic conversations, fixes on observed issues, plus operator's review and direction time. Calendar is dominated by buffer/review, not daemon coding.

For comparison: Foundation Sprint is 36 PRs / ~50 daemon-hours / 2-3 daemon-days. The combined PR-FUTURE batch is **smaller than Foundation Sprint** because most of these PRs are focused refactors (rename, replace, remove) not new architectural builds. Foundation is heavier because it includes regression test suites and god-class decomposition of 800+ LOC files.

### Foundation Sprint relationship

None of the seven PRs are in Foundation Sprint scope. Foundation Sprint cleans up internal architecture of pipeline-orchestrator (god classes, atomic primitives, regression tests). After Foundation, these seven PRs become natural follow-up sprint(s) before declaring multi-repo onboarding production-ready for non-author users.

Three key conceptual shifts deserve emphasis:

**Scaffold should be AI-driven, not template-driven (PR-FUTURE-4).** Current `scaffolder.py` assumes greenfield projects built from the orchestrator template. Real users have existing projects with established CI, established AGENTS.md, established conventions. The AI-driven scaffold detects what exists and generates additions that respect what's there, surfaced as a MICRO PR for operator review. This is what makes pipeline-orchestrator viable as a tool for **onboarding existing work**, not just managing greenfield work.

**Auth must be UI-driven, not shell-driven (PR-FUTURE-6).** Current "shell into container, run gh auth login" model is fine for the author. For pipeline-orchestrator to onboard non-author users, auth flow must happen in the dashboard. Without UI-driven auth, the wizard cannot run AI scaffolding which depends on Claude/Codex CLI being authenticated.

**Source-of-truth should be authoritative without intermediate projection (PR-FUTURE-7).** `tasks/PR-*.md` files are the source of truth for the queue. `tasks/QUEUE.md` was a derived projection that mattered when QUEUE.md was git-tracked (PR-181 era and before). Post-PR-181, the projection persists on disk only because legacy code paths read from disk instead of from the in-memory snapshot the daemon already computes. Removing QUEUE.md eliminates the two-source confusion, simplifies onboarding (one less file), and removes ~80 LOC of legacy-repo migration branching.

For author's own multi-repo use (megaraid-dashboard, sms-gateway-v2 onboarding), workarounds exist:

- **AGENTS template scope leakage** is mitigated by adding a "Note about daemon-managed sections" section in user's AGENTS.md above the managed block, explicitly directing coder that orchestrator-specific paths in managed sections are illustrative, not applicable to this repo.
- **Per-repo config gap** is mitigated by editing user's AGENTS.md to set the desired coverage gate explicitly in the user's `## Testing` section. Daemon-managed `## CI gates` is read alongside but user's section is project-specific source of truth.
- **Onboarding wizard absence** is mitigated by manual conflict resolution: operator reads existing AGENTS.md, identifies conflicts with daemon-managed sections, edits user's content to defer-or-clarify before applying onboarding.
- **Stub scripts/ci.sh** is mitigated by manual creation of a real `scripts/ci.sh` before adding the repo to daemon (scaffolder is idempotent and won't overwrite an existing file). For megaraid+sms-gateway, the manual `scripts/ci.sh` should mirror the project's own GitHub Actions CI commands exactly: `ruff check .`, `ruff format --check .`, `mypy src`, `pytest [project's own coverage flags]`.
- **No observe mode** is mitigated by accepting full management on add-repo. For trial inspection, can use a fork instead of the real repo.
- **Manual auth flow** is already done by the author; daemon's mounted volumes (`CLAUDE_CONFIG_DIR`, `GH_CONFIG_DIR`, `codex-auth` volume) keep tokens persistent. Author re-auths manually via `docker compose run` when tokens expire.
- **QUEUE.md still on disk** is mitigated by adding `tasks/QUEUE.md` to onboarded repo's `.gitignore` so the file does not get accidentally committed. Daemon regenerates the file each IDLE cycle; gitignored entry prevents pollution. Workaround does not address the underlying complexity tax in the codebase, only the surface artifact in the onboarded repo.

These workarounds are operator-time-intensive and do not scale to non-author users. Hence the future PRs.

 Daemon передаёт pr_id + task_file path + task body в prompt coder'у напрямую. Снижен в приоритете: Wave 2 прошёл без AGENTS.md fixes вообще, значит текущая indirection работает приемлемо. Вернуть в Tier 1 только если incident повторится несколько раз.
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

## Известные риски и осторожности (current Sprint 13/14, refreshed 2026-05-02)

### Sprint 13 - OBS-AY UI freeze fix (highest risk in batch)
Two stacked bugs: setInterval leak in `base.html` (frontend) + slow `/api/states` endpoint with sync `load_config` and sequential per-repo Redis reads (backend). Fix B (backend) requires careful coordination: caching config asynchronously, parallelizing per-repo Redis reads via `asyncio.gather`, optionally short-TTL response cache. Risk: introducing concurrency bugs in `get_all_repo_states` while removing sync I/O. Mitigation: incremental PRs, Fix A first (frontend cleanup, ~3 LOC stops the bleeding), Fix B second with regression test against pre-fix scenario (5+ repos, 6+ navigations).

### Sprint 14 - Cancellation policy v1 (highest architectural surface)
New `SignalSource` Protocol introduces extension point that future plugins (companion app, calendar, webhooks) will hook into. v1 design must not lock-in assumptions that prevent future sources. Risk: shipping v1 with implicit assumption that breaks at second SignalSource. Mitigation: design Protocol against the 3 known sources (manual override, heartbeat, active hours) plus 2 hypothetical (companion app, calendar) before locking signature.

### Sprint 14 - OBS-BK elif chain ordering fix
Reordering elif chain in `watch.py:180-189` so review-driven FIX runs independent of CI state. Risk: regress on freshly-opened-PR scenario (CI hasn't started AND review CHANGES_REQUESTED - should NOT trigger FIX, since review is on no-CI state). Existing `_has_new_codex_feedback_since_last_push()` handles this, but reordering without testing it explicitly may break. Mitigation: regression test for "fresh PR + CHANGES_REQUESTED + CI=PENDING" should NOT trigger FIX; "stale PR + CHANGES_REQUESTED + CI=PENDING + new feedback" SHOULD trigger FIX.

### Sprint 14 - OBS-BL circuit breaker on WATCH↔HUNG loop
N=3 default; but operator may want different cap per scenario. Risk: cap too low traps benign slow-codex cases; cap too high allows extended waste. Mitigation: configurable `escalation_loop_cap` field, sensible default (3), telemetry counter for "escalation circuit breaker tripped" events to validate threshold post-ship.

### Sprint 14 - OBS-BM CI stuck-PENDING classification
Classification heuristic depends on GHA API response shape; vendors change response format occasionally. Risk: classifier silently fails when GHA changes shape. Mitigation: defensive parsing with fallback to "unclassified PENDING" category; alert event when classifier hits unknown shape.

### Sprint 14 - Multi-OBS coordination (9-11 PRs in one sprint)
Sprint 14 is the largest sprint planned (27 daemon-hours, 9-11 PRs). Risk: dependency graph between OBS-AW, OBS-BB, OBS-BC, OBS-BE, OBS-BK, OBS-BL, OBS-BM, Cancellation policy creates serialization that throughput cannot collapse. Mitigation: batch parallelization plan in sprint spec generation; identify which OBS items can ship independently; ship OBS-BK + OBS-BL + OBS-BM (the recovery primitives) before Cancellation policy v1 (which consumes them as substrate).

### Sprint 13 - License switch MIT to Apache 2.0
Mechanical change. Risk: deps or contributors with explicit MIT-only constraints (none expected, all upstream deps likely Apache/MIT/BSD compatible). Mitigation: scan all upstream dependencies for license incompatibility (Apache 2.0 compatible with MIT/BSD/Apache, not GPL); coordinate before switch. ~1 PR, ~1h: replace LICENSE file content, update `pyproject.toml` license field, add NOTICE file (Apache 2.0 convention), update README badge if any. Daemon-eligible spec.

---

## Process notes (как работать с этим документом)

### Как обновлять roadmap

После каждой merged волны или significant chat-session:
1. Update "Последнее обновление" header with date + summary of changes.
2. Move just-shipped PRs из active sprint sections в Active OBS items с CLOSED status.
3. Add new OBS items observed during session.
4. Refresh sprint estimates if scope changed.
5. Verify task numbering is continuous (no reservation per established rule 2026-04-29).

### Daemon vs deploy-time tasks/

Daemon работает из `/data/repos/<slug>/tasks/` (docker volume). Deploy-time `~/pipeline-orchestrator/tasks/` может отличаться. Ground truth для queue computation = daemon volume. Не conflate the two when investigating queue discrepancies.

---

## Открытые вопросы ждущие решения (refreshed 2026-05-02)

### Strategic decisions confirmed today (2026-05-02)

- **License Apache 2.0 - CONFIRMED, ASAP, scheduled Sprint 13.** Current LICENSE is MIT (not AGPL as memory implied). Action: replace with Apache 2.0 + NOTICE file. ~1 PR ~1h. Added to Sprint 13 batch alongside OBS-AX and OBS-AY (operator decision 2026-05-02).
- **Vision A timing CONFIRMED:** Sprint 17+ after Sprint 16 multi-testbed. Plugin Protocol generalization is prerequisite for Thompson Sampling and managed product fork.
- **SQLite Scenario A migration BEFORE Thompson Sampling.** Long-term posterior stability requires not 90-day TTL. Sprint slot: between Vision A first slice and Thompson Sampling work, likely Sprint 18-19 territory.
- **PR-FUTURE-7 (eliminate QUEUE.md) CONFIRMED.** In-memory queue model. Sprint slot: post-Sprint 16, parallel-eligible with Vision A first slice if scope permits.
- **PR-FUTURE-4 tier'ed scaffolder** AFTER Sprint 13 OBS-AX (CLAUDE.md replace fix). Likely Sprint 16-17 territory.

### Strategic decisions deferred (Vision tier, not actioning soon)

- **Monetization model** (hybrid C + opt-in D). 6-12 months horizon. Recorded as Vision direction, no sprint commitment.
- **Self-hosted vs Managed product fork.** Recorded as Vision direction tied to Vision A multi-vendor work; not actioning until plugin Protocol shipped.
- **GitHub App migration timing.** Diet PRs reduced GraphQL burn substantially; even at 3 active repos operator observed below 80% utilization. App created (`alexbomber-pipeline-orchestrator`, scopes Contents/Issues/Metadata/PR R+W, "Only on this account") but private key still pending download. Activation deferred indefinitely; revisit only if quota exhaustion returns OR third-party adoption becomes relevant.
- **Multi-tier agent (Tier 2 architect/diagnostic).** Recorded as Vision direction, not actioning soon (see Vision section).
- **Tester role / Release Qualification Agent.** Vision item, not actioning.
- **Telegram bot Vision D D.1 (digest push).** Operator confirmed Sprint 17+ slot, not opportunistic earlier.

### New questions arising during cleanup (2026-05-02)

- **Does Codex Connector's behaviour have observable patterns** beyond what OBS-Z (EYES race) and pre-merge sync re-trigger covered? Operator confirmed (2026-05-02) that codex non-determinism is acceptable as-is, no new fix work scheduled. Recorded for awareness only.
- **Lessons learned appendix (line 872) - single source of truth or redundant with Active OBS items?** Currently both exist: brief one-liners in Active OBS items (line 90+) plus consolidated lessons in appendix. Decision deferred; revisit if appendix grows or contradicts Active OBS items.

### Resolved decisions (closing entries from prior sessions)

- **Sprint nomenclature:** unified 2026-05-02. Sprint 12 = Foundation, 13 = OBS-AX + OBS-AY, 14 = recovery + cancellation, 15 = polish + DONE metrics, 16 = multi-testbed, 17+ = Vision A.
- **Sigkill multi-race resolution path** → 4 PRs merged 2026-04-28 via direct commits (legacy numbering, predates current task-file numbering). Closed.
- **Roadmap rewrite** → executed 2026-04-29 + cleanup 2026-05-02. Latest version (this document).
- **PR numbering rule** → continuous, no reservation. Established 2026-04-29.
- **Throughput baseline** → 25-30 PR/day (corrected from earlier 15-20 estimate). Recorded 2026-05-02.
