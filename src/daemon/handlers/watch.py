"""WATCH state handler.

Mixin methods:
    handle_watch                          — poll PR status and dispatch
    _has_new_codex_feedback_since_last_push — check for new Codex comments
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from src import github_client
from src.models import CIStatus, FeedbackCheckResult, PipelineState, ReviewStatus

logger = logging.getLogger(__name__)


class WatchMixin:
    """Poll PR status and decide whether to merge, fix, hang, or wait."""

    async def handle_watch(self) -> None:
        """Poll PR status and decide whether to merge, fix, hang, or wait."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            self.log_event("WATCH without current_pr -> IDLE")
            return

        try:
            prs = github_client.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"get_open_prs failed: {exc}"
            self.log_event(str(exc))
            return

        current_number = self.state.current_pr.number
        found = next((p for p in prs if p.number == current_number), None)
        if found is None:
            self.log_event(f"PR #{current_number} no longer open -> IDLE")
            self.state.current_pr = None
            self.state.current_task = None
            self.state.state = PipelineState.IDLE
            return

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
        if ci == CIStatus.SUCCESS and review == ReviewStatus.APPROVED:
            if self.repo_config.auto_merge:
                await self.handle_merge()
            else:
                self.log_event(
                    f"PR #{found.number} green but auto_merge disabled; "
                    "awaiting manual merge"
                )
            return
        # Fork (cross-repo) PRs can't be fixed locally.
        if found.is_cross_repository:
            if ci == CIStatus.FAILURE or review == ReviewStatus.CHANGES_REQUESTED:
                self.log_event(
                    f"PR #{found.number} fork PR cannot be auto-fixed "
                    f"(review={review.value}, ci={ci.value}); "
                    "waiting for review timeout"
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
                    f"PR #{found.number} CHANGES_REQUESTED but feedback check "
                    "failed; staying in WATCH, will retry next cycle"
                )
                return
            self.log_event(
                f"PR #{found.number} CHANGES_REQUESTED but no new "
                "Codex feedback since last push; waiting for fresh review"
            )

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
            self.state.state = PipelineState.HUNG
            self.log_event(
                f"PR #{found.number} hung after {elapsed_min:.0f}m "
                f"(review={review.value}, ci={ci.value})"
            )
        else:
            self.log_event(
                f"PR #{found.number} waiting "
                f"(review={review.value}, ci={ci.value}, "
                f"{elapsed_min:.0f}/{timeout_min}m)"
            )

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
            comments = github_client._gh_api_paginated(
                f"repos/{self.owner_repo}/issues/"
                f"{self.state.current_pr.number}/comments"
            ) or []
            review_comments = github_client._gh_api_paginated(
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
            created = github_client._parse_iso(c.get("created_at"))
            if created is None:
                continue
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            if created > last_activity:
                return FeedbackCheckResult.NEW
        return FeedbackCheckResult.NONE
