"""WATCH state handler.

Mixin methods:
    handle_watch                          — poll PR status and dispatch
    _has_new_codex_feedback_since_last_push — check for new Codex comments
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone

from src.github import cache as gh_cache
from src.github import checks as gh_checks
from src.github import gh_runner
from src.github import prs as gh_prs
from src.keyspace import ci_infra_retried
from src.models import CIStatus, FeedbackCheckResult, PipelineState, ReviewStatus

logger = logging.getLogger(__name__)
_STALE_RETRIGGER_DEBOUNCE = timedelta(hours=1)
_CODEX_BOT_ERROR_RETRIGGER_COOLDOWN = timedelta(minutes=5)
_CODEX_BOT_LOGIN = "chatgpt-codex-connector[bot]"
# PR-251: TTL for the per-(repo, pr, head_sha) infra-retry marker. Keys
# self-expire so abandoned PRs (closed, merged, force-pushed) don't
# accumulate state in Redis. A week is far longer than any single
# head_sha is in flight, so the TTL never races a still-relevant retry
# decision but does reclaim space for closed PRs.
_CI_INFRA_RETRIED_TTL_SECONDS = 7 * 24 * 60 * 60
# PR-251 follow-up: grace window applied between writing the infra-retry
# marker (immediately after ``gh run rerun --failed``) and treating a
# subsequent INFRA_FAILURE classification as "persistent" enough to
# escalate to FIX. The CI status fetch is cached for
# ``_CI_STATUS_CACHE_TTL_SECONDS`` (15s); on fast WATCH cadences (the
# e2e config polls every 2s) the next cycle can still read the
# pre-rerun cached payload, see the marker, and route to ``handle_fix``
# before GitHub has reported any new check-run state. The grace window
# must be larger than the cache TTL plus enough slack for GitHub to
# acknowledge the rerun and emit fresh check-runs; 60s gives roughly
# four cache-miss refetches at the 15s TTL, after which any persisting
# INFRA_FAILURE genuinely reflects a post-rerun observation. The grace
# is bounded by the marker TTL above, so closed/abandoned PRs cannot
# accumulate state.
_INFRA_RETRY_GRACE_SECONDS = 60.0
# PR-251 follow-up: regex for extracting the GitHub Actions workflow run
# ID from a check-run ``details_url``. The canonical format is
# ``https://github.com/{owner}/{repo}/actions/runs/{run_id}/job/{job_id}``;
# matching only ``/actions/runs/<digits>`` keeps the parser robust if
# GitHub appends query parameters or trims the ``/job/...`` suffix in
# future API revisions. Non-Actions check runs (custom GitHub Apps)
# carry a different URL shape, do not match, and are silently skipped
# — we cannot rerun them with ``gh run rerun --failed`` regardless.
_DETAILS_URL_RUN_RE = re.compile(r"/actions/runs/(\d+)")
# Substring patterns matching known chatgpt-codex-connector[bot] error
# comments. When the bot fails its own review and posts one of these,
# review_status stays as EYES and the WATCH timeout would otherwise burn
# 21+ minutes; detecting the pattern lets us re-trigger @codex review
# immediately.
CODEX_BOT_ERROR_PATTERNS = (
    "Something went wrong while reviewing",
    "error reviewing this PR",
    "Please try again",
    "unable to complete review",
)


class WatchMixin:
    """Poll PR status and decide whether to merge, fix, hang, or wait."""

    @property
    def effective_watch_poll_interval(self) -> int:
        """Return the WATCH poll interval after slow-start adaptive logic.

        Anchors at ``max(_watch_entered_at, _watch_last_event_at)``.
        ``_watch_entered_at`` is set the moment the runner transitions
        into WATCH (in ``_run_cycle_body`` for handler-driven
        transitions, in ``recover_state`` for startup recovery), so
        the first poll interval after entry already reflects the slow
        cadence. Within ``watch_slow_window_sec`` of the anchor,
        returns ``watch_slow_poll_interval_sec`` — Codex Review and CI
        rarely respond in the first few minutes, so polling fast there
        wastes GitHub API quota on a scheduled wait. Past the window,
        returns ``watch_fast_poll_interval_sec`` since the result may
        arrive any second.

        Falls back to the static ``repo_config.poll_interval_sec`` only
        in defensive corner cases where no anchor is set (e.g. legacy
        test stubs) — production WATCH cycles always have an anchor.

        Stacks with the rate-limit slowdown by taking the larger of
        the watch interval and ``base * github_api_slowdown_multiplier``;
        the rate-limit ceiling always wins. ``_check_github_api_budget``
        suppresses its one-in-N cycle skip while WATCH is active so the
        slowdown is not double-applied.
        """
        anchors = [
            t
            for t in (self._watch_entered_at, self._watch_last_event_at)
            if t is not None
        ]
        if not anchors:
            return self.repo_config.poll_interval_sec
        anchor = max(anchors)
        daemon = self.app_config.daemon
        elapsed = (datetime.now(timezone.utc) - anchor).total_seconds()
        if elapsed < daemon.watch_slow_window_sec:
            target = daemon.watch_slow_poll_interval_sec
        else:
            target = daemon.watch_fast_poll_interval_sec
        if self._github_api_slowdown_attempts > 0:
            multiplier = max(1, daemon.github_api_slowdown_multiplier)
            target = max(target, self.repo_config.poll_interval_sec * multiplier)
        return target

    def _reset_watch_polling(self) -> None:
        """Clear WATCH adaptive polling anchors when leaving WATCH."""
        self._watch_entered_at = None
        self._watch_last_event_at = None
        self._watch_last_event_signature = None

    async def handle_watch(self) -> None:
        """Poll PR status and decide whether to merge, fix, hang, or wait."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            self.log_event("[WATCH] WATCH without current_pr -> IDLE.")
            return

        try:
            prs = gh_prs.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            await self._transition_to_error(
                f"get_open_prs failed: {exc}",
                save_run_record_as=None,
                publish=False,
                log_prefix="[WATCH]",
                log_message=str(exc),
            )
            return

        current_pr = self.state.current_pr
        current_number = current_pr.number
        found = next((p for p in prs if p.number == current_number), None)
        if found is not None:
            self._observe_watch_event_signature(found)
        if found is None:
            merged = gh_prs.is_pr_merged(self.owner_repo, current_number)
            if merged is True:
                await self._save_current_run_record("success_merged")
                self.log_event(
                    f"[WATCH] PR #{current_number} merged externally -> IDLE."
                )
            elif merged is False:
                await self._save_current_run_record("closed_unmerged")
                self.log_event(
                    f"[WATCH] PR #{current_number} closed without merge "
                    f"-> IDLE."
                )
            else:
                self.log_event(
                    f"[WATCH] PR #{current_number} no longer open (state "
                    f"unknown) -> IDLE."
                )
            self._current_run_record = None
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.state.state = PipelineState.IDLE
            return

        merged_shas, merged_push_count = current_pr.merge_observed_pushes(found)
        found = found.model_copy(
            update={
                "fix_iteration_count": current_pr.fix_iteration_count,
                "no_push_fix_count": current_pr.no_push_fix_count,
                "observed_head_shas": merged_shas,
                "push_count": merged_push_count,
                # Preserve OBS-BL (PR-249) counter across the GitHub-fetched
                # PR refresh. ``_observe_watch_event_signature`` has already
                # zeroed ``current_pr.watch_retrigger_count`` if a fresh
                # event arrived this cycle, so reading it here naturally
                # resets the new ``found`` on real progress and preserves
                # the count when the PR is still stuck.
                "watch_retrigger_count": current_pr.watch_retrigger_count,
            }
        )
        self.state.current_pr = found
        # Retry rehydrate every cycle so a transient commit-time fetch
        # failure during ``recover_state`` doesn't permanently leave
        # ``_last_push_at`` unset.
        if (
            self._last_push_at is None
            or self._last_push_at_pr_number != found.number
        ):
            self._rehydrate_last_push_at(found)

        await self._maybe_reclassify_stuck_pending(found)
        ci = found.ci_status
        review = found.review_status
        review_allows_merge = review == ReviewStatus.APPROVED or (
            self.repo_config.allow_merge_without_review
            and review == ReviewStatus.PENDING
        )
        if ci == CIStatus.SUCCESS and review_allows_merge:
            if self.repo_config.auto_merge:
                await self.handle_merge()
            else:
                self.log_event(
                    f"[WATCH] PR #{found.number} green but auto_merge "
                    f"disabled; awaiting manual merge."
                )
            return
        # Fork (cross-repo) PRs can't be fixed locally.
        if found.is_cross_repository:
            if (
                ci == CIStatus.FAILURE
                or ci == CIStatus.INFRA_FAILURE
                or review == ReviewStatus.CHANGES_REQUESTED
            ):
                self.log_event(
                    f"[WATCH] PR #{found.number} fork PR cannot be "
                    f"auto-fixed (review={review.value}, ci={ci.value}); "
                    f"waiting for review timeout."
                )
        elif ci == CIStatus.FAILURE:
            await self.handle_fix()
            return
        elif ci == CIStatus.INFRA_FAILURE:
            # PR-251 (OBS-BC): a single infra-class failure (cancelled
            # / action_required / stale conclusion, or known infra
            # annotation keyword) earns one workflow rerun per
            # ``head_sha``. A second INFRA_FAILURE classification on the
            # same SHA means the rerun also failed — treat as a real
            # logic FAILURE and route to FIX so the coder can act on it.
            #
            # PR-251 follow-up: the marker is gated by
            # ``_INFRA_RETRY_GRACE_SECONDS`` so the cached pre-rerun CI
            # payload (TTL 15s) cannot trigger FIX escalation before
            # GitHub has reported any post-rerun state. While the grace
            # window is in flight WATCH stays put and re-polls.
            marker_exists, grace_elapsed = await self._infra_retry_attempted(
                found.number, found.head_sha
            )
            if marker_exists and grace_elapsed:
                self.log_event(
                    f"[WATCH] PR #{found.number} CI INFRA_FAILURE "
                    f"persisted after retry; routing to FIX as "
                    f"effective FAILURE."
                )
                # Downgrade in-memory ci_status to FAILURE so the FIX
                # prompt builder injects CI logs (it gates that section
                # on ``ci_status == CIStatus.FAILURE``); leaving it as
                # INFRA_FAILURE would drop the very logs the coder
                # needs for the effective-failure handoff.
                found.ci_status = CIStatus.FAILURE
                await self.handle_fix()
                return
            if marker_exists:
                self.log_event(
                    f"[WATCH] PR #{found.number} CI INFRA_FAILURE "
                    f"observed within retry grace window; awaiting "
                    f"fresh post-rerun status."
                )
                return
            self.log_event(
                f"[WATCH] PR #{found.number} CI INFRA_FAILURE "
                f"detected; retrying workflow once before routing to "
                f"FIX."
            )
            self._retry_failed_workflow(found.number, found.head_sha)
            await self._mark_infra_retry_attempted(
                found.number, found.head_sha
            )
            return
        elif review == ReviewStatus.CHANGES_REQUESTED:
            result = self._has_new_codex_feedback_since_last_push()
            if result == FeedbackCheckResult.NEW:
                await self.handle_fix()
                return
            if result == FeedbackCheckResult.UNKNOWN:
                self.log_event(
                    f"[WATCH] PR #{found.number} CHANGES_REQUESTED but "
                    f"feedback check failed; staying in WATCH, will retry "
                    f"next cycle."
                )
                return
            self.log_event(
                f"[WATCH] PR #{found.number} CHANGES_REQUESTED but no new "
                f"Codex feedback since last push; waiting for fresh review."
            )
            self._maybe_retrigger_stale_review(found.number)
        elif ci == CIStatus.PENDING:
            pass

        # EYES is the only state where the bot may have errored mid-review;
        # gating avoids paginating issue comments on every WATCH poll.
        # Both retrigger paths post ``@codex review`` with
        # ``bypass_same_head_dedup=True`` and would otherwise emit two
        # back-to-back trigger comments when both conditions are true in
        # the same poll cycle (author-dedup does not deduplicate the
        # second post when the daemon and PR author have different
        # logins). Run bot-error first because it is the more specific
        # signal; fall through to stale-review only when bot-error did
        # not post.
        if review == ReviewStatus.EYES:
            posted = self._maybe_retrigger_on_codex_bot_error(found.number)
            if not posted:
                self._maybe_retrigger_stale_review(found.number)

        last_activity = found.last_activity or self.state.last_updated
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        elapsed_min = (now - last_activity).total_seconds() / 60
        timeout_min = (
            self.repo_config.review_timeout_min
            if self.repo_config.review_timeout_min is not None
            else self.app_config.daemon.review_timeout_min
        )
        if elapsed_min >= timeout_min:
            await self._escalate_and_skip(
                f"PR #{found.number} hung after {elapsed_min:.0f}m "
                f"(review={review.value}, ci={ci.value})",
                error_message_override=None,
                apply_escalated_label=False,
                set_pr_escalated_flag=False,
                log_message=(
                    f"PR #{found.number} hung after {elapsed_min:.0f}m "
                    f"(review={review.value}, ci={ci.value})."
                ),
                cancellation_subsource="review_timeout",
            )
        else:
            self.log_event(
                f"[WATCH] PR #{found.number} waiting "
                f"(review={review.value}, ci={ci.value}, "
                f"{elapsed_min:.0f}/{timeout_min}m)."
            )

    async def _maybe_reclassify_stuck_pending(self, found: object) -> None:
        """Upgrade ``found.ci_status`` to FAILURE when CI has been PENDING too long.

        PR-250 (OBS-BM): GitHub Actions occasionally registers a check-run
        as ``queued`` or ``in_progress`` and never publishes a terminal
        state. WATCH would otherwise poll forever; instead we track the
        first-seen-PENDING timestamp per ``head_sha`` in Redis and, once
        ``daemon.ci_pending_max_min`` minutes have elapsed on the same
        SHA, rewrite the in-memory ``ci_status`` to FAILURE so the
        existing FAILURE branch routes through ``handle_fix``. The raw
        REST classifier is left untouched — only the daemon-side view
        of CI is affected.
        """
        head_sha = getattr(found, "head_sha", "") or ""
        if not head_sha:
            return
        pending_max_seconds = self.app_config.daemon.ci_pending_max_min * 60
        runs_payload, statuses_payload, fetch_ok = (
            gh_checks._fetch_ci_status_rest(self.owner_repo, head_sha)
        )
        reclassified, reason = await gh_checks.classify_ci_status_with_age(
            self.owner_repo,
            found.number,
            head_sha,
            self.redis,
            pending_max_seconds,
            runs_payload,
            statuses_payload,
            empty_is_success=self.repo_config.allow_merge_without_checks,
            fetch_ok=fetch_ok,
        )
        if reason != "stuck_pending":
            return
        age_min = pending_max_seconds // 60
        sha_short = head_sha[:7]
        self.log_event(
            f"[WATCH] PR #{found.number} CI reclassified PENDING -> FAILURE "
            f"(stuck_pending: {age_min}min on sha {sha_short})."
        )
        found.ci_status = reclassified

    def _observe_watch_event_signature(self, found: object) -> None:
        """Update ``_watch_last_event_at`` when polled PR signature changes.

        The signature combines the fields ``handle_watch`` already keys
        state transitions off of: PR number, CI conclusion, review
        status, and ``last_activity``. A change in any of them between
        cycles means a real GitHub event arrived (push, review, comment,
        CI conclusion change), so the slow-start window restarts from
        ``now`` and the next interval check uses the fast-tail cadence.

        First-cycle observation only records the baseline signature; an
        event is reported only on the second-and-later cycle when the
        signature differs from the prior one.
        """
        signature = (
            getattr(found, "number", None),
            getattr(found, "ci_status", None),
            getattr(found, "review_status", None),
            getattr(found, "last_activity", None),
        )
        prior = self._watch_last_event_signature
        self._watch_last_event_signature = signature
        if prior is not None and prior != signature:
            self._watch_last_event_at = datetime.now(timezone.utc)
            # OBS-BL (PR-249): a real GitHub event = "make progress";
            # zero the WATCH<->HUNG retrigger cap counter so genuine
            # codex activity always restores the full retry budget.
            current_pr = self.state.current_pr
            if current_pr is not None:
                current_pr.watch_retrigger_count = 0

    def _has_new_codex_feedback_since_last_push(self) -> FeedbackCheckResult:
        """Check whether Codex posted any comment after ``self._last_push_at``.

        Returns a three-state :class:`FeedbackCheckResult`:
        - ``NEW``     – new Codex feedback exists after last push
        - ``NONE``    – no Codex activity after last push
        - ``UNKNOWN`` – API call failed; caller should stay in WATCH
        """
        if self.state.current_pr is None:
            return FeedbackCheckResult.NONE
        last_activity = self._last_push_at
        if last_activity is None:
            return FeedbackCheckResult.NEW
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        try:
            comments = gh_cache._gh_api_paginated(
                f"repos/{self.owner_repo}/issues/"
                f"{self.state.current_pr.number}/comments"
            ) or []
            review_comments = gh_cache._gh_api_paginated(
                f"repos/{self.owner_repo}/pulls/"
                f"{self.state.current_pr.number}/comments"
            ) or []
        except Exception:
            logger.warning(
                "GitHub API error checking Codex feedback for PR #%s; "
                "returning UNKNOWN",
                self.state.current_pr.number,
                exc_info=True,
            )
            return FeedbackCheckResult.UNKNOWN
        for c in reversed(comments + review_comments):
            user = (c.get("user") or {}).get("login", "")
            if "codex" not in user.lower():
                continue
            created = gh_runner._parse_iso(c.get("created_at"))
            if created is None:
                continue
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            if created > last_activity:
                return FeedbackCheckResult.NEW
        return FeedbackCheckResult.NONE

    def _maybe_retrigger_stale_review(self, pr_number: int) -> bool:
        """Re-trigger ``@codex review`` when a stale review blocks progress.

        EYES is the OBS-Z race-window state — Codex acknowledged the
        request with the eyes reaction but never posted a verdict, often
        because the auto-trigger and our mention raced and one was
        dropped. EYES recovery is fast, so it uses a shorter threshold
        (``stale_review_threshold_eyes_min``) than the legitimate-review
        case (``stale_review_threshold_min``) where the human reviewer
        may simply be slow.

        Returns ``True`` when ``@codex review`` was posted on this call
        so the caller can suppress redundant retriggers in the same
        WATCH cycle.
        """
        current_pr = self.state.current_pr
        if current_pr is None:
            return False
        if current_pr.review_status == ReviewStatus.CHANGES_REQUESTED:
            stale_minutes = self.app_config.daemon.stale_review_threshold_min
        elif current_pr.review_status == ReviewStatus.EYES:
            stale_minutes = self.app_config.daemon.stale_review_threshold_eyes_min
        else:
            return False

        last_push_age_seconds = gh_prs.get_last_push_age_seconds(
            self.owner_repo,
            pr_number,
        )
        if last_push_age_seconds is None:
            return False

        now = datetime.now(timezone.utc)
        stale_after = timedelta(minutes=stale_minutes)
        if last_push_age_seconds < stale_after.total_seconds():
            return False

        last_retrigger_at = self.state.last_stale_retrigger_at
        if last_retrigger_at is not None:
            if last_retrigger_at.tzinfo is None:
                last_retrigger_at = last_retrigger_at.replace(
                    tzinfo=timezone.utc
                )
            if now - last_retrigger_at < _STALE_RETRIGGER_DEBOUNCE:
                return False

        self.log_event(
            f"[WATCH] Stale CHANGES_REQUESTED on PR #{pr_number}; "
            f"re-triggering @codex review."
        )
        success, posted, retry_at = self._post_codex_review_result(
            pr_number,
            bypass_same_head_dedup=True,
        )
        self.state.last_stale_retrigger_at = now
        return posted

    def _maybe_retrigger_on_codex_bot_error(self, pr_number: int) -> bool:
        """Re-trigger ``@codex review`` when chatgpt-codex-connector[bot]
        posted an error comment (e.g. "Something went wrong while reviewing")
        instead of a verdict. Only matches comments authored by the codex bot
        itself, and applies a 5-minute per-PR cooldown to avoid loops on a
        permanent Codex outage.

        Returns ``True`` when ``@codex review`` was posted on this call
        so the caller can suppress redundant retriggers in the same
        WATCH cycle.
        """
        try:
            comments = gh_cache._gh_api_paginated(
                f"repos/{self.owner_repo}/issues/{pr_number}/comments"
            ) or []
        except Exception:
            logger.warning(
                "GitHub API error checking codex bot error comments for "
                "PR #%s",
                pr_number,
                exc_info=True,
            )
            return False

        latest_error_at: datetime | None = None
        for c in comments:
            user = (c.get("user") or {}).get("login", "")
            if user != _CODEX_BOT_LOGIN:
                continue
            body = c.get("body") or ""
            if not any(pat in body for pat in CODEX_BOT_ERROR_PATTERNS):
                continue
            created = gh_runner._parse_iso(c.get("created_at"))
            if created is None:
                continue
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            if latest_error_at is None or created > latest_error_at:
                latest_error_at = created

        if latest_error_at is None:
            return False

        last_retrigger_at = self.state.last_codex_retrigger_at
        if last_retrigger_at is not None:
            if last_retrigger_at.tzinfo is None:
                last_retrigger_at = last_retrigger_at.replace(
                    tzinfo=timezone.utc
                )
            if latest_error_at <= last_retrigger_at:
                return False
            now = datetime.now(timezone.utc)
            if now - last_retrigger_at < _CODEX_BOT_ERROR_RETRIGGER_COOLDOWN:
                return False

        self.log_event(
            f"[WATCH] Codex bot error comment on PR #{pr_number}; "
            f"re-triggering @codex review."
        )
        success, posted, _retry_at = self._post_codex_review_result(
            pr_number,
            bypass_same_head_dedup=True,
        )
        if success:
            self.state.last_codex_retrigger_at = datetime.now(timezone.utc)
        return posted

    async def _infra_retry_attempted(
        self, pr_number: int, head_sha: str
    ) -> tuple[bool, bool]:
        """Return ``(marker_exists, grace_elapsed)`` for the infra-retry marker.

        PR-251 (OBS-BC) + follow-up. The marker stores the wall-clock
        timestamp at which WATCH issued ``gh run rerun --failed`` for
        ``head_sha``; ``grace_elapsed`` is ``True`` only after
        ``_INFRA_RETRY_GRACE_SECONDS`` have passed since that
        timestamp, ensuring that an immediate re-poll on a fast WATCH
        cadence cannot escalate to FIX while the CI fetch is still
        serving the pre-rerun cached payload.

        Without ``redis`` or ``head_sha`` we conservatively report
        ``(True, True)`` so the caller routes straight to FIX rather than
        looping retries against a missing tracker. Legacy markers written
        before the timestamp format (value ``"1"``) are also treated as
        ``(True, True)`` — they came from a prior daemon version where
        the grace window did not exist, and we cannot recover the
        original rerun time.
        """
        if self.redis is None or not head_sha:
            return True, True
        key = ci_infra_retried(self.owner_repo, pr_number, head_sha)
        existing = await self.redis.get(key)
        if existing is None:
            return False, False
        try:
            marker_ts = float(existing)
        except (TypeError, ValueError):
            return True, True
        elapsed = time.time() - marker_ts
        return True, elapsed >= _INFRA_RETRY_GRACE_SECONDS

    async def _mark_infra_retry_attempted(
        self, pr_number: int, head_sha: str
    ) -> None:
        """Record that WATCH issued the one-shot infra retry for ``head_sha``.

        Stores the current wall-clock timestamp so :meth:`_infra_retry_attempted`
        can apply ``_INFRA_RETRY_GRACE_SECONDS`` before reporting the
        retry as "elapsed enough to escalate" — see that helper's
        docstring for the rationale.
        """
        if self.redis is None or not head_sha:
            return
        key = ci_infra_retried(self.owner_repo, pr_number, head_sha)
        await self.redis.set(
            key, str(time.time()), ex=_CI_INFRA_RETRIED_TTL_SECONDS
        )

    def _retry_failed_workflow(self, pr_number: int, head_sha: str) -> bool:
        """Re-run the failed jobs of failing workflow runs attached to ``head_sha``.

        PR-251 (OBS-BC). Returns ``True`` when at least one ``gh run
        rerun --failed`` succeeded; ``False`` when no failing check-run
        was found, no Actions workflow run IDs could be extracted, or
        every rerun call failed. The caller marks the infra-retry
        attempted unconditionally so a transient ``gh`` outage does not
        loop the WATCH cycle.

        Workflow run IDs are derived from the per-commit ``check-runs``
        REST payload (``GET /repos/{repo}/commits/{sha}/check-runs``)
        rather than ``gh run list --commit``. The check-runs endpoint is
        keyed to the actual head SHA the daemon already tracks, so it
        surfaces every failing job regardless of the trigger event;
        ``gh run list --commit`` keys on the ``workflow_run.head_sha``
        field, which for ``pull_request`` events can point at the
        synthetic merge commit instead of ``pull_request.head.sha`` and
        therefore returns nothing for the most common PR workflow
        configuration. Driving the rerun off the same check-runs that
        produced the INFRA_FAILURE classification also guarantees we
        only rerun jobs the WATCH gate actually saw fail — no risk of
        firing an unrelated ``push``/``schedule`` workflow that shares
        the SHA but never appeared in the check rollup.

        Each failing check-run's ``details_url`` (canonical shape
        ``.../actions/runs/{run_id}/job/{job_id}``) is parsed to extract
        the workflow run ID; non-Actions check runs (custom GitHub
        Apps) match a different URL shape, fall through, and are
        skipped. Run IDs are deduplicated so a workflow with several
        failing matrix jobs receives a single ``gh run rerun --failed``
        call.
        """
        if not head_sha:
            return False
        check_runs, _statuses, _fetch_ok = gh_checks._fetch_ci_status_rest(
            self.owner_repo, head_sha
        )
        failing_runs = [
            run
            for run in check_runs
            if isinstance(run, dict)
            and (
                str(run.get("conclusion") or "").upper()
                in gh_checks._REST_CI_FAILURE_STATES
                or str(run.get("status") or "").upper()
                in gh_checks._REST_CI_FAILURE_STATES
            )
        ]
        if not failing_runs:
            self.log_event(
                f"[WATCH] PR #{pr_number} infra retry skipped — no "
                f"failing check-run found for sha {head_sha[:7]}."
            )
            return False
        run_ids: list[int] = []
        seen: set[int] = set()
        for run in failing_runs:
            url = run.get("details_url") or run.get("html_url") or ""
            match = _DETAILS_URL_RUN_RE.search(str(url))
            if not match:
                continue
            run_id = int(match.group(1))
            if run_id in seen:
                continue
            seen.add(run_id)
            run_ids.append(run_id)
        if not run_ids:
            self.log_event(
                f"[WATCH] PR #{pr_number} infra retry skipped — failing "
                f"check-runs on sha {head_sha[:7]} have no Actions "
                f"workflow run ID in details_url."
            )
            return False
        rerun_count = 0
        for run_id in run_ids:
            try:
                gh_runner.run_gh(
                    ["run", "rerun", "--failed", str(run_id)],
                    repo=self.owner_repo,
                )
            except RuntimeError as exc:
                self.log_event(
                    f"[WATCH] PR #{pr_number} infra retry rerun failed "
                    f"for run {run_id}: {exc}."
                )
                continue
            rerun_count += 1
        if rerun_count == 0:
            return False
        self.log_event(
            f"[WATCH] PR #{pr_number} infra retry: re-ran failed jobs "
            f"of {rerun_count} workflow run(s) on sha {head_sha[:7]}."
        )
        return True
