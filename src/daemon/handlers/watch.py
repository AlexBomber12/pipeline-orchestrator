"""WATCH state handler.

Mixin methods:
    handle_watch                          — poll PR status and dispatch
    _has_new_codex_feedback_since_last_push — check for new Codex comments
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from src.github import cache as gh_cache
from src.github import gh_runner
from src.github import prs as gh_prs
from src.models import CIStatus, FeedbackCheckResult, PipelineState, ReviewStatus

logger = logging.getLogger(__name__)
_STALE_RETRIGGER_DEBOUNCE = timedelta(hours=1)
_CODEX_BOT_ERROR_RETRIGGER_COOLDOWN = timedelta(minutes=5)
_CODEX_BOT_LOGIN = "chatgpt-codex-connector[bot]"
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
            if ci == CIStatus.FAILURE or review == ReviewStatus.CHANGES_REQUESTED:
                self.log_event(
                    f"[WATCH] PR #{found.number} fork PR cannot be "
                    f"auto-fixed (review={review.value}, ci={ci.value}); "
                    f"waiting for review timeout."
                )
        elif ci == CIStatus.FAILURE:
            await self.handle_fix()
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
            await self._escalate_to_hung(
                f"PR #{found.number} hung after {elapsed_min:.0f}m "
                f"(review={review.value}, ci={ci.value})",
                error_message_override=None,
                apply_escalated_label=False,
                set_pr_escalated_flag=False,
                log_message=(
                    f"PR #{found.number} hung after {elapsed_min:.0f}m "
                    f"(review={review.value}, ci={ci.value})."
                ),
            )
        else:
            self.log_event(
                f"[WATCH] PR #{found.number} waiting "
                f"(review={review.value}, ci={ci.value}, "
                f"{elapsed_min:.0f}/{timeout_min}m)."
            )

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
