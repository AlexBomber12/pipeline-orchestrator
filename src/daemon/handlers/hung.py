"""HUNG state handler and Codex review posting.

Mixin methods:
    handle_hung        — nudge reviewer or escalate
    _post_codex_review — post @codex review on a PR
"""

from __future__ import annotations

from datetime import datetime, timezone

from src import github_client
from src.daemon import git_ops
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
        current_pr = self.state.current_pr
        cache_dedup_key = False
        head_sha: str | None = None
        pr_author = ""
        head_commit_date = ""
        try:
            head_sha = git_ops._git(
                self.repo_path, "rev-parse", "HEAD"
            ).stdout.strip() or None
        except Exception:
            head_sha = None
        metadata = github_client.get_pr_metadata(self.owner_repo, pr_number)
        if isinstance(metadata, dict):
            pr_author = str(metadata.get("author") or "")
            head_commit_date = str(metadata.get("head_commit_date") or "")
        if head_sha is None:
            self.log_event(
                f"Warning: failed to resolve HEAD for PR #{pr_number}; "
                "posting @codex review without dedup"
            )
        elif (
            pr_author
            and head_commit_date
            and github_client.has_recent_codex_review_request(
                self.owner_repo,
                pr_number,
                pr_author=pr_author,
                after_iso=head_commit_date,
            )
        ):
            self._last_codex_review_pr = pr_number
            self._last_codex_review_head_sha = head_sha
            self.log_event(
                f"Skipping duplicate @codex review for PR #{pr_number}; "
                "PR author already requested review for this head"
            )
            return True
        elif (
            self._last_codex_review_pr == pr_number
            and self._last_codex_review_head_sha == head_sha
        ):
            self.log_event(
                f"Skipping duplicate @codex review for PR #{pr_number}"
            )
            return True

        if head_sha is not None:
            self._last_codex_review_pr = pr_number
            self._last_codex_review_head_sha = head_sha
            cache_dedup_key = True

        try:
            if current_pr is not None and current_pr.number == pr_number:
                current_pr.last_activity = datetime.now(timezone.utc)
            github_client.post_comment(
                self.owner_repo, pr_number, "@codex review"
            )
            self.log_event(f"Posted @codex review on PR #{pr_number}")
            return True
        except Exception as exc:
            if cache_dedup_key:
                self._last_codex_review_pr = None
                self._last_codex_review_head_sha = None
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
