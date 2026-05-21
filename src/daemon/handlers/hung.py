"""Codex review posting helpers.

Mixin methods:
    _post_codex_review            — post @codex review on a PR
    _should_skip_codex_review_post — fail-open EYES race-window dedup gate
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.daemon import git_ops
from src.github import comments as gh_comments
from src.github import gh_runner, reactions
from src.github import prs as gh_prs


def _author_already_requested_review(
    owner_repo: str,
    pr_number: int,
    pr_author: str,
    head_commit_date: str,
) -> bool:
    """Treat author-trigger dedup as best-effort and fail open."""
    try:
        return gh_comments.has_recent_codex_review_request(
            owner_repo,
            pr_number,
            pr_author=pr_author,
            after_iso=head_commit_date,
        )
    except Exception:
        return False


def _author_recent_review_requested_at(
    owner_repo: str,
    pr_number: int,
    pr_author: str,
    head_commit_date: str,
) -> datetime | None:
    """Best-effort timestamp for a recent PR-author trigger on this head."""
    try:
        return gh_comments.get_recent_codex_review_request_time(
            owner_repo,
            pr_number,
            pr_author=pr_author,
            after_iso=head_commit_date,
        )
    except Exception:
        return None


class HungMixin:
    """Shared reviewer nudging helpers for active PR handlers."""

    def _should_skip_codex_review_post(self, pr_number: int) -> bool:
        """Return ``True`` when a fresh Codex EYES reaction covers the head.

        Handles the OBS-Z race: Codex's auto-trigger on PR creation and the
        daemon's own ``@codex review`` mention can fire near-simultaneously
        and Codex sometimes drops one request silently, leaving the PR
        stuck in EYES until the general stale-review threshold fires. A
        pre-post probe of the PR body's reactions lets the daemon skip the
        duplicate when the auto-trigger already landed.

        Anchors freshness on the branch's last push time, not the head
        commit's committer date. Cherry-picked, amended, and rebased
        commits routinely carry committer dates older than the push that
        actually published them, so committer-date gating could classify
        a stale EYES reaction as fresh on a brand-new push and silently
        skip the trigger. The activity API's ``pushed_at`` reflects when
        the branch was actually updated, which is what the gate needs.

        Fails open (returns ``False``) on any GitHub API error or when
        the push time cannot be resolved, so a transient outage cannot
        suppress a needed mention.
        """
        try:
            codex_reactions = reactions._get_codex_issue_reactions(
                self.owner_repo, pr_number,
            )
        except Exception:
            return False
        eyes_reactions = [
            reaction for reaction in codex_reactions
            if reactions._is_reaction_content(reaction, "eyes")
        ]
        if not eyes_reactions:
            return False
        try:
            last_push_time = gh_prs.get_pr_last_push_time(
                self.owner_repo, pr_number,
            )
        except Exception:
            return False
        if last_push_time is None:
            return False
        if last_push_time.tzinfo is None:
            last_push_time = last_push_time.replace(tzinfo=timezone.utc)
        for reaction in eyes_reactions:
            reaction_time = gh_runner._parse_iso(
                reaction.get("created_at")
            )
            if reaction_time is None:
                continue
            if reaction_time.tzinfo is None:
                reaction_time = reaction_time.replace(tzinfo=timezone.utc)
            if reaction_time >= last_push_time:
                return True
        return False

    def _post_codex_review_result(
        self,
        pr_number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        """Post ``@codex review`` and report success/post/retry timing."""
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
        try:
            metadata = gh_prs.get_pr_metadata(
                self.owner_repo, pr_number
            )
            if isinstance(metadata, dict):
                pr_author = str(metadata.get("author") or "")
                head_commit_date = str(
                    metadata.get("head_commit_date") or ""
                )
        except Exception as exc:
            self.log_event(
                f"[INFRA] Warning: failed to load PR metadata for "
                f"@codex review dedup on PR #{pr_number}: {exc}; "
                f"posting without PR-author dedup."
            )
        if head_sha is None:
            self.log_event(
                f"[INFRA] Warning: failed to resolve HEAD for PR "
                f"#{pr_number}; posting @codex review without dedup."
            )
        elif (
            not bypass_author_dedup
            and pr_author
            and head_commit_date
            and _author_already_requested_review(
                self.owner_repo,
                pr_number,
                pr_author,
                head_commit_date,
            )
        ):
            self._last_codex_review_pr = pr_number
            self._last_codex_review_head_sha = head_sha
            self.log_event(
                f"[INFRA] Skipping duplicate @codex review for PR "
                f"#{pr_number}; PR author already requested review for "
                f"this head."
            )
            requested_at = _author_recent_review_requested_at(
                self.owner_repo,
                pr_number,
                pr_author,
                head_commit_date,
            )
            retry_at = None
            if requested_at is not None:
                retry_at = requested_at + timedelta(minutes=5)
            return True, False, retry_at
        elif (
            not bypass_same_head_dedup
            and self._last_codex_review_pr == pr_number
            and self._last_codex_review_head_sha == head_sha
        ):
            self.log_event(
                f"[INFRA] Skipping duplicate @codex review for PR "
                f"#{pr_number}."
            )
            return True, False, None

        if head_sha is not None:
            self._last_codex_review_pr = pr_number
            self._last_codex_review_head_sha = head_sha
            cache_dedup_key = True

        try:
            if current_pr is not None and current_pr.number == pr_number:
                current_pr.last_activity = datetime.now(timezone.utc)
            gh_comments.post_comment(
                self.owner_repo, pr_number, "@codex review"
            )
            kind = (
                "force_repost"
                if bypass_same_head_dedup or bypass_author_dedup
                else "review_post"
            )
            self.log_event(
                f"Posted @codex review on PR #{pr_number}.",
                tier="infra",
                kind=kind,
            )
            return True, True, None
        except Exception as exc:
            if cache_dedup_key:
                self._last_codex_review_pr = None
                self._last_codex_review_head_sha = None
            self.log_event(
                f"[INFRA] Warning: failed to post @codex review on PR "
                f"#{pr_number}: {exc}."
            )
            return False, False, None

    def _post_codex_review(
        self,
        pr_number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> bool:
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
        success, _posted, _retry_at = self._post_codex_review_result(
            pr_number,
            bypass_same_head_dedup=bypass_same_head_dedup,
            bypass_author_dedup=bypass_author_dedup,
        )
        return success
