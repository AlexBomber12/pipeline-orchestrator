# Roadmap Updates — 2026-05-11 evening session

## Summary

Three separate edits to `docs/roadmap.md` to capture the PR-422 incident:

1. **Correct line 1452** — "Codex non-determinism" misclassification
2. **Add OBS-DC entry** in New OBS items section (closed by PR-422)
3. **Add Production session lessons (2026-05-11 evening)** block after 2026-05-07 session lessons

---

## Edit 1 — Correct line 1452 misclassification

**Current text (lines 1451-1453):**

```
### Codex review behaviour (recorded for awareness)
- Codex reviews are non-deterministic on identical code: EYES → CHANGES_REQUESTED → APPROVED → CHANGES_REQUESTED transitions happen without any push between them. Operator's deliberate choice (2026-05-02): keep this behaviour because intermediate codex comments often catch missed details. Pre-merge sync re-trigger in `merge.py:170-195` provides defense-in-depth against approval-on-stale-HEAD.
- EYES race window: dual-trigger (codex auto-trigger + daemon `@codex review` post) sometimes causes EYES-stuck state. PR-189 shipped pre-push state check + EYES-specific stale threshold mitigations.
```

**Replacement:**

```
### Codex review behaviour (recorded for awareness)
- **2026-05-11 correction:** earlier (2026-05-02) observation that "Codex reviews are non-deterministic on identical code: EYES → CHANGES_REQUESTED → APPROVED → CHANGES_REQUESTED transitions happen without any push between them" was **misclassified**. PR-422 incident (2026-05-11) traced the same symptom to daemon-side race condition between pre-merge sync `git push` and the subsequent `_post_codex_review` call's `get_pr_metadata` API read: GitHub REST API eventual consistency can return stale head_commit_date for several seconds after a push, causing PR-author dedup to match the operator's recent `@codex review` anchor against the old head and silently suppress the post. Codex never reviews the merge commit, body 👍 reaction stays on the prior head so `body_approved` rejects it, classifier falls through to CHANGES_REQUESTED, WATCH cycles ESCALATE indefinitely. Closed by PR-422 (bypass_author_dedup at pre-merge sync call site). Real Codex non-determinism remains, but is rarer than this race-driven artifact; future state-flap observations should investigate race before classifying as Codex-side non-determinism. Pre-merge sync re-trigger in `merge.py:172-210` remains correct defense-in-depth against approval-on-stale-HEAD, now with bypass_author_dedup=True so dedup race cannot suppress the re-trigger.
- EYES race window: dual-trigger (codex auto-trigger + daemon `@codex review` post) sometimes causes EYES-stuck state. PR-189 shipped pre-push state check + EYES-specific stale threshold mitigations.
- **Codex Connector verdict signals (verified 2026-05-11 from OpenAI docs + production):** Codex bot uses exactly 3 signals — 👍 reaction on PR body (no findings, equivalent to approval), 👀 eyes reaction (review in progress), or posted comment/review with P0/P1 findings (equivalent to CHANGES_REQUESTED). Codex never sets formal GitHub PR review state to APPROVED or CHANGES_REQUESTED — only COMMENTED. Plus a specific issue-comment text marker: `"Codex Review: Didn't find any major issues"` issue comment posted by `chatgpt-codex-connector[bot]` is the textual approval signal that accompanies the 👍 reaction. Daemon classifier `_compute_review_status` handles all three signal types correctly; the 2026-05-11 incident was about race causing Codex to never produce any signal on the new head, not about misclassification of an existing signal.
```

---

## Edit 2 — Add OBS-DC entry after OBS-DB (around line 357)

**Insert before line 359 (the PR-FUTURE-RESTART-BUTTON entry), after the OBS-DB block:**

```
- **OBS-DC** (pre-merge sync race against GitHub REST API eventual consistency on `/pulls/{pr}` after daemon-initiated push): **CLOSED 2026-05-11 by PR-422**. Pre-merge sync flow in `src/daemon/handlers/merge.py:172-210` sequence: (1) `git merge origin/{base}` produces merge commit, (2) `git push origin {pr_branch}` ships it, (3) `gh_cache._invalidate_etag_cache("repos/{repo}/pulls")` clears local cache, (4) `self._post_codex_review(number)` → `_post_codex_review_result` → `gh_prs.get_pr_metadata(...)` → REST API GET `/repos/{repo}/pulls/{pr}` happens within milliseconds of the push. GitHub side eventual consistency on read replicas can return pre-push head_sha/head_commit_date for several seconds. `_author_already_requested_review(after_iso=stale_date)` then matches the operator's recent `@codex review` anchor (created within the 5-minute dedup window for the prior head), suppresses the post with `[INFRA] Skipping duplicate @codex review for PR #{n}; PR author already requested review for this head`. Codex never reviews the merge commit. Body 👍 reaction's `created_at` falls before the new `head_commit_time` so `body_approved` rejects it. Classifier falls through to CHANGES_REQUESTED via the "Didn't find any major issues" issue comment after anchor. WATCH cycle waits review_timeout and ESCALATEs without Codex ever evaluating the merged code. **Verified on PR #420 (2026-05-11):** 👍 reaction at 23:53:53Z, merge commit committer date 23:54:03Z (10s gap), operator anchor at 23:49:10Z. Direct probe hours later confirmed `has_recent_codex_review_request(after_iso=23:54:03)` returns False (fresh data), but daemon log at 23:54:07Z showed `Skipping duplicate` — proving `get_pr_metadata` returned a non-fresh value at the exact moment of the dedup check. **Fix scope (PR-422, shipped 2026-05-11, commit 9dd4e4a):** added `bypass_author_dedup: bool = False` parameter to `_post_codex_review` and `_post_codex_review_result` in `src/daemon/handlers/hung.py`; gated the PR-author-dedup `elif` on `not bypass_author_dedup`; pre-merge sync call site in `merge.py:194-208` passes `bypass_author_dedup=True`; added log line `[MERGE] Bypass-requesting fresh @codex review on new head <sha7> after pre-merge sync.` for operator visibility. Other `_post_codex_review` call sites (handle_coding, handle_fix, EYES retrigger, stale retrigger) keep default `bypass_author_dedup=False` because they legitimately need protection against AGENTS.md-runbook double-posts. **Production verification:** PR #420 entered MERGE state after operator `@codex review` retrigger, pre-merge sync produced merge commit ef88de7, new log line appeared, `[INFRA] Posted @codex review on PR #420.` followed (not "Skipping duplicate"), Codex reviewed ef88de7 with 👍, WATCH cycle saw APPROVED, daemon merged PR #420 → IDLE → picked PR-317. End-to-end fix validated in one cycle.

- **OBS-DD** ("Codex non-determinism" classification anti-pattern in operator-recorded observations): **CLOSED 2026-05-11 as methodology lesson**. The 2026-05-02 roadmap entry that classified observed Codex review state flapping (EYES → CHANGES_REQUESTED → APPROVED → CHANGES_REQUESTED without push) as "Codex non-determinism, operator-accepted behavior" was incorrect. Real cause was the OBS-DC race surfacing as visible state flap. Misclassification persisted from 2026-05-02 to 2026-05-11 (9 days) and obscured the underlying race from earlier diagnosis. **Lesson:** when an external API appears non-deterministic and the daemon makes API calls in tight timing sequences (push + read within seconds), suspect race against API eventual consistency before classifying as external-side non-determinism. **Diagnostic procedure for similar future cases:** (a) check whether the daemon's read-after-write pattern has any timing window where stale data could be returned, (b) probe the relevant API endpoint immediately after a daemon-initiated write and compare to the value the daemon recorded, (c) only after race is ruled out, attribute to external-side behavior.
```

---

## Edit 3 — Add Production session lessons block

**Insert after line 1524 (end of 2026-05-07 session lessons), before line 1526 (`## Architectural future work` header):**

```

### Production session lessons (2026-05-11 evening — PR-422 race-condition diagnosis + Sprint 15b Phase 3 chain advance)

**Pre-merge sync race against GitHub API eventual consistency (drove OBS-DC, closed by PR-422):**
- 9-day-old "Codex non-determinism" misclassification (recorded 2026-05-02, line 1452 area) traced to daemon-side race condition. PR #420 (PR-316 implementation) entered review_timeout ESCALATE loop after Codex approval landed on the pre-pre-merge-sync head, but pre-merge sync produced a new merge commit and the subsequent `_post_codex_review` call hit the GitHub REST API eventual consistency window. PR-author dedup matched the operator's recent anchor against the stale head_commit_date and suppressed the fresh trigger.
- **Architectural lesson:** when daemon executes a write-then-read sequence against the same external API in tight timing (push + GET /pulls/N within milliseconds), assume eventual consistency unless the API documentation explicitly guarantees read-after-write. Defense-in-depth call sites that depend on freshly-pushed state must either (a) bypass API-derived dedup using locally-known authoritative state, or (b) tolerate one stale read by retrying after a short delay.
- **Action:** PR-422 shipped (4-line change in `hung.py` + call site change in `merge.py` + observability log line). Race surface eliminated at the only known affected call site. Other call sites already protected by sufficient delay between push and read.

**Misclassification as "external non-determinism" delays diagnosis for days (drove OBS-DD):**
- The 2026-05-02 observation was recorded as "Codex non-determinism, operator-accepted." Without challenge to this classification, the underlying race lived for 9 days, consuming coder budget on retries that PR-316's review_timeout-to-ERROR change made visible. PR-316 fixing a different bug accidentally exposed this race.
- **Architectural lesson:** "external system is random" is a satisfying but often wrong attribution. When daemon log shows state transitions without operator action, prefer the hypothesis "something in our own sequence-of-API-calls is racing" over "the external system is non-deterministic." External systems are usually more deterministic than internal race conditions when measured at the second granularity that matters here.
- **Action:** documented in OBS-DD. Future operator observations of apparent external non-determinism should include a verification step (probe the same API endpoint directly, compare with daemon-recorded value) before recording as external behavior.

**8+ iterations of incorrect hypotheses before correct diagnosis (Claude self-observation):**
- Diagnosing OBS-DC took 8+ iterations of incorrect root-cause hypotheses (HUNG handler dropped, PENDING state not retriggered, COMMENTED not classified, last_push_at not updated, etc.) before reaching the API-eventual-consistency hypothesis. Each incorrect hypothesis was issued as if confirmed and walked back after operator-driven verification refuted it. Operator wasted substantial time running verification commands for each iteration. Final correct diagnosis only emerged when methodology shifted from race-to-conclusion to (a) fresh code snapshot from production, (b) mathematical simulation of dedup logic with multiple possible after_iso values to prove any non-fresh value reproduces the observed log message, (c) bounded confidence claim (95%+ rather than 100%) acknowledging that the specific sub-case (stale vs empty vs API error) was undetermined but fix is correct for all three.
- **Operator process lesson:** in diagnosis sessions, Claude's first 2-3 hypotheses are statistically likely to be wrong on race-condition-class bugs. The methodology shift to "collect all relevant timestamps first, then simulate logic on known data, then propose fix" is more productive than rapid hypothesis-generation. Operator was right to repeatedly demand verification ("ты понимаешь, что ты долбоеб?") — without that pressure Claude would have shipped a misdirected fix.
- **Action:** for future race-condition-class diagnosis, mandatory pre-flight: (a) load fresh code snapshot, not memory of prior version, (b) collect all relevant timestamps from external API before forming any hypothesis, (c) simulate logic on collected data to prove a specific scenario class, (d) state confidence bound explicitly before writing fix spec. Documented as Claude-side methodology note.

**Stale code snapshot in Claude's context window vs production state (Claude self-observation):**
- Claude was working from a 2026-05-11 morning snapshot of pipeline-orchestrator. Operator merged ~10 PRs throughout the session (MICRO #412/413, PROMPT 3, PROMPT 6a, PROMPT 6b, PROMPT 7, PR-418, PR-419, plus PR-422 itself). Claude's code references became progressively stale through the session, causing several incorrect statements about current line numbers and parameter signatures. Codex VS Code agent has access to current GitHub state and saw the fresh code; Codex Connector reviewer hallucinated "changes already present" because it conflated PR description text with applied diff.
- **Operator process lesson:** for diagnosis sessions on a fast-moving codebase, operator should upload a fresh zip snapshot when starting (or when adding multiple merges during the session). Without this, Claude's references drift from production state and produce specs with off-by-N line numbers. This session's fresh zip upload mid-session (after PR-422 was scoped) unblocked Claude's accuracy on the final spec.
- **Action:** documented as process note. Snapshot upload at session start (or after major merge bursts) is a small cost that prevents large reasoning errors.

**PR-316 made OBS-DC visible by removing the retry loop that previously masked it (drove OBS-DC visibility):**
- Pre-PR-316 architecture: WATCH review_timeout → `_escalate_and_skip` → IDLE → picker re-picks same PR → another WATCH cycle on (likely now-propagated) fresh API state → race usually clears on retry. Symptom: PR sometimes takes 2-3 IDLE cycles to merge instead of 1. Easy to miss; classified as Codex non-determinism.
- Post-PR-316: WATCH review_timeout → `_transition_to_error` → terminal ERROR → operator Retry button required. Single-shot semantics. If first shot hits the race, pipeline halts visibly.
- **Architectural lesson:** terminating a tolerant retry loop exposes the race conditions it was tolerating. PR-316 was correctly designed (the retry loop was wasteful and obscured real failures), but its ship date became the visibility moment for this race. **Pattern:** when removing a retry / fallback / re-pick loop, audit all upstream paths feeding into that loop for race conditions that were being tolerated rather than fixed. Otherwise the loop removal exposes them as production incidents on the next eligible PR.
- **Action:** documented as architecture pattern. Future loop-removal PRs should include a "What this loop was masking?" section in their threat model.

**Codex Connector reviewer hallucinates `repository state` when reviewing meta-PRs (Codex behavior pattern):**
- Codex Connector reviewed PR-422 with comment "No additional code changes are needed... the trigger content is a status/summary of an already-implemented fix rather than a new requested modification." This was false — PR-422 was the implementation, not a summary. Operator's `grep -n 'bypass_author_dedup' ...` proved Codex's claim was a hallucination.
- **Operator process lesson:** Codex Connector reviews are advisory, not authoritative. When a Codex review claims "no changes needed" or "already done", verify with `grep` / `git diff` before merging. Hallucination probability appears higher on meta-PRs (PRs about the daemon itself, PRs about the review pipeline) than on substantive code PRs — possibly because Codex's training included similar self-referential text.
- **Action:** operating procedure note. Before merging any meta-PR, explicit `grep` for the expected new symbols.
```

---

## Application instructions

The three edits are independent and can be applied in any order. The simplest workflow:

1. Apply Edit 1 first (single text replacement, low risk)
2. Apply Edit 2 (insertion of 2 OBS entries)
3. Apply Edit 3 (insertion of session lessons block)

Each edit uses unique surrounding text as anchor; no ambiguity in placement.

Total additions: ~9 KB of markdown across 3 locations in `docs/roadmap.md`.

No code changes. No test changes. Pure documentation.
