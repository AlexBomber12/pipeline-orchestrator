"""HUNG state handler and Codex review posting.

Mixin methods:
    handle_hung        — nudge reviewer or escalate
    _post_codex_review — post @codex review on a PR
"""

from __future__ import annotations

from datetime import datetime, timezone

from src import github_client
from src.models import PipelineState


class HungMixin:
    """Nudge the reviewer with ``@codex review`` or give up, per config."""

    def _post_codex_review(self, pr_number: int) -> bool:
        """Post ``@codex review`` on ``pr_number``.

        Called after PR creation (``handle_coding``) and after every
        fix push (``handle_fix``) so Codex kicks off a review for each
        iteration instead of relying on the GitHub-side Automatic
        Reviews trigger (which we want configured for PR creation only
        to avoid duplicate reviews).

        Skips posting when the PR author already has a recent
        ``@codex review`` comment — Claude's PLANNED PR runbook posts
        that trigger itself and an immediate daemon-side repost would
        queue a duplicate Codex review.

        Returns ``True`` on success and ``False`` on a logged failure.
        The caller decides whether a failure is fatal: after a fix
        push it must be, otherwise the next ``handle_watch`` cycle
        still sees the prior ``CHANGES_REQUESTED`` signal and loops
        back into ``handle_fix`` immediately, pushing a new fix every
        poll interval without ever re-requesting a review. After PR
        creation it can stay a warning because Codex Automatic Reviews
        still fires on the creation event itself.
        """
        try:
            metadata = github_client.get_pr_metadata(
                self.owner_repo, pr_number
            )
            pr_author = metadata.get("author", "")
            head_commit_iso = metadata.get("head_commit_date", "")
            if pr_author and github_client.has_recent_codex_review_request(
                self.owner_repo,
                pr_number,
                pr_author=pr_author,
                within_minutes=5,
                after_iso=head_commit_iso or None,
            ):
                self.log_event(
                    f"Skipping duplicate @codex review on PR #{pr_number}"
                )
                return True
        except Exception as exc:
            self.log_event(
                f"Dedup check failed on PR #{pr_number}: {exc}"
            )

        try:
            github_client.post_comment(
                self.owner_repo, pr_number, "@codex review"
            )
            self.log_event(f"Posted @codex review on PR #{pr_number}")
            return True
        except Exception as exc:
            self.log_event(
                f"Warning: failed to post @codex review on PR "
                f"#{pr_number}: {exc}"
            )
            return False

    async def handle_hung(self) -> None:
        """Nudge the reviewer with ``@codex review`` or give up, per config."""
        if (
            self.app_config.daemon.hung_fallback_codex_review
            and self.state.current_pr is not None
        ):
            try:
                github_client.post_comment(
                    self.owner_repo,
                    self.state.current_pr.number,
                    "@codex review",
                )
            except Exception as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"post_comment failed: {exc}"
                self.log_event(str(exc))
                return
            self.state.current_pr.last_activity = datetime.now(timezone.utc)
            self.state.state = PipelineState.WATCH
            self.log_event("posted @codex review -> WATCH")
            return

        if self.state.current_pr is not None:
            try:
                result = github_client.run_gh(
                    [
                        "pr",
                        "view",
                        str(self.state.current_pr.number),
                        "--json",
                        "state",
                    ],
                    repo=self.owner_repo,
                )
            except Exception as exc:
                self.log_event(
                    f"hung: failed to check PR state: {exc}; staying HUNG"
                )
                return

            pr_state = ""
            if isinstance(result, dict):
                pr_state = str(result.get("state") or "").upper()

            if pr_state in ("MERGED", "CLOSED"):
                self.log_event(
                    f"PR #{self.state.current_pr.number} {pr_state} "
                    "by operator -> IDLE"
                )
                self.state.current_pr = None
                self.state.current_task = None
                self.state.state = PipelineState.IDLE
                return

        self.log_event(
            "hung fallback disabled; leaving runner in HUNG for operator action. "
            "Resolve the PR manually or re-enable hung_fallback_codex_review."
        )
