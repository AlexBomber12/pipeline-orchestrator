"""MERGE state handler and queue-sync operations.

Mixin methods:
    handle_merge                    — merge PR and return to IDLE
    _mark_queue_done                — mark task DONE in the local QUEUE.md
    _resolve_pending_queue_sync     — poll legacy queue-sync PR status
    _escalate_queue_sync_if_expired — escalate to ERROR on timeout
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from src import claude_cli, codex_cli, github_client
from src.daemon import git_ops
from src.models import PipelineState
from src.queue_parser import mark_task_done
from src.retry import retry_transient

# Upper bound on how long an open queue-sync remediation PR may sit
# unresolved before ``_resolve_pending_queue_sync`` escalates to
# ERROR.
_QUEUE_SYNC_MAX_WAIT_SEC = 3600

logger = logging.getLogger(__name__)


class MergeMixin:
    """Merge the current PR and return to IDLE."""

    async def handle_merge(self) -> None:
        """Merge the current PR and return to IDLE."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            return

        number = self.state.current_pr.number
        pr_branch = self.state.current_pr.branch
        base = self.repo_config.branch
        if not self.state.current_pr.is_cross_repository:
            try:
                retry_transient(
                    lambda: git_ops._git(
                        self.repo_path,
                        "fetch", "--prune", "origin", base, pr_branch,
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {base} {pr_branch}",
                )
                git_ops._git(self.repo_path, "checkout", pr_branch)
                git_ops._git(
                    self.repo_path,
                    "reset", "--hard", f"origin/{pr_branch}",
                )
                merge_result = git_ops._git(
                    self.repo_path,
                    "merge", f"origin/{base}", "--no-edit",
                    timeout=60, check=False,
                )
                sync_produced_commit = False
                if merge_result.returncode != 0:
                    if "CONFLICT" in (
                        merge_result.stdout + merge_result.stderr
                    ):
                        selected = self._get_auxiliary_coder()
                        if selected is None:
                            git_ops._git(
                                self.repo_path,
                                "merge", "--abort",
                                check=False,
                            )
                            self.state.state = PipelineState.ERROR
                            self.state.error_message = (
                                "No eligible coder available for merge conflict "
                                "resolution"
                            )
                            await self._save_current_run_record("error")
                            self.log_event(f"[MERGE] {self.state.error_message}.")
                            return
                        coder_name, _plugin = selected
                        if not await self._check_rate_limit(
                            proactive_coder=coder_name
                        ):
                            git_ops._git(
                                self.repo_path,
                                "merge", "--abort",
                                check=False,
                            )
                            return
                        self.log_event(
                            "[MERGE] Merge conflict with main, resolving..."
                        )
                        if self._current_run_record is not None:
                            self._current_run_record.had_merge_conflict = True
                        prompt = (
                            "Resolve all merge conflicts in the working "
                            "tree. Keep both sides where possible. "
                            "Run scripts/ci.sh to verify."
                        )
                        if coder_name == "claude":
                            code, _stdout, _stderr = await claude_cli.run_claude_async(
                                prompt,
                                self.repo_path,
                                timeout=300,
                                model=self.app_config.daemon.claude_model,
                                system_prompt_file=None,
                            )
                        else:
                            code, _stdout, _stderr = await codex_cli.run_codex_async(
                                prompt,
                                self.repo_path,
                                timeout=300,
                                model=self.app_config.daemon.codex_model,
                            )
                        if code != 0:
                            self._detect_rate_limit(_stderr, coder_name=coder_name)
                            git_ops._git(
                                self.repo_path,
                                "merge", "--abort",
                                check=False,
                            )
                            if self.state.rate_limited_until is not None:
                                self.state.state = PipelineState.PAUSED
                                self.state.error_message = None
                                await self._save_current_run_record("rate_limit")
                                self.log_event(
                                    f"[RATE-LIMIT] Rate limit pause "
                                    f"active until "
                                    f"{self.state.rate_limited_until.isoformat()}."
                                )
                                return
                            self.state.state = PipelineState.ERROR
                            self.state.error_message = (
                                "Merge conflict resolution failed"
                            )
                            await self._save_current_run_record("error")
                            self.log_event(f"[MERGE] {self.state.error_message}.")
                            return
                        sync_produced_commit = True
                    else:
                        self.state.state = PipelineState.ERROR
                        self.state.error_message = (
                            f"git merge origin/{base} failed: "
                            f"{merge_result.stderr.strip()}"
                        )
                        await self._save_current_run_record("error")
                        self.log_event(f"[MERGE] {self.state.error_message}.")
                        return
                else:
                    sync_produced_commit = (
                        "Already up to date" not in merge_result.stdout
                    )

                if sync_produced_commit:
                    retry_transient(
                        lambda: git_ops._git(
                            self.repo_path,
                            "push", "origin", pr_branch,
                            timeout=60,
                        ),
                        operation_name=f"git push origin {pr_branch}",
                    )
                    # Pre-merge sync just rewrote the PR's head SHA;
                    # the cached ``repos/{repo}/pulls`` pages still hold
                    # the prior ``updated_at`` and ``head.sha`` values,
                    # so drop them now to keep WATCH polling honest.
                    github_client._invalidate_etag_cache(
                        f"repos/{self.owner_repo}/pulls"
                    )
                    self.state.state = PipelineState.WATCH
                    self.log_event(
                        f"[MERGE] Pre-merge sync pushed new commits to PR "
                        f"#{number}; returning to WATCH to re-verify "
                        f"gates."
                    )
                    if not self._post_codex_review(number):
                        self.state.state = PipelineState.ERROR
                        self.state.error_message = (
                            f"Failed to post @codex review on PR "
                            f"#{number} after pre-merge sync push; "
                            "manual review trigger required to avoid "
                            "merging on stale approval"
                        )
                    return
            except (subprocess.CalledProcessError,
                    subprocess.TimeoutExpired, OSError, RuntimeError) as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"Pre-merge sync failed: {exc}"
                await self._save_current_run_record("error")
                self.log_event(f"[MERGE] {self.state.error_message}.")
                return

        merged_diff_stats = self._compute_diff_stats(base)
        self.log_event(f"[MERGE] Merging PR #{number}.")
        try:
            github_client.run_gh(
                ["pr", "ready", str(number)],
                repo=self.owner_repo,
            )
        except Exception as exc:
            logger.debug(
                "Best-effort gh pr ready failed for PR #%s in %s: %s",
                number,
                self.owner_repo,
                exc,
            )
        try:
            github_client.merge_pr(self.owner_repo, number)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"merge_pr failed: {exc}"
            await self._save_current_run_record("error")
            self.log_event(f"[MERGE] {exc}.")
            return

        try:
            self._mark_queue_done()
        except Exception as exc:
            self.log_event(f"[MERGE] Warning: queue-sync step failed: {exc}.")

        await self._save_current_run_record(
            "success_merged",
            diff_stats=merged_diff_stats,
            base_branch=base,
        )
        self._current_run_record = None
        self.state.current_pr = None
        self.state.current_task = None
        self.state.state = PipelineState.IDLE
        self.log_event(f"[MERGE] Merged PR #{number} -> IDLE.")

    def _mark_queue_done(self) -> None:
        """Mark the merged task DONE in the local QUEUE.md only.

        QUEUE.md is gitignored (PR-181) and is regenerated each IDLE
        cycle from structured task headers, so this update never
        commits or pushes. The in-place tweak keeps read consumers
        (dashboard, recovery) consistent with the just-merged status
        between handle_merge and the next IDLE tick.

        On legacy repos that still track ``tasks/QUEUE.md`` upstream
        (``.gitignore`` does not retroactively untrack files), an
        in-place rewrite would dirty the working tree without ever
        being committed or pushed; the next cycle's preflight would
        then move the runner to ERROR before ``handle_idle`` ran. Skip
        the rewrite in that case — the tracked snapshot on origin
        remains the source of truth until the legacy repo migrates the
        file out of git, mirroring ``_write_generated_queue_md``.
        """
        if self.state.current_task is None:
            return
        # ``None`` means the cat-file probe itself failed (transient git
        # slowness); treat as if tracked so we skip the rewrite. A
        # legacy repo with a flaky probe would otherwise dirty the
        # working tree on every merge and trip preflight into ERROR.
        # Post-PR-181 repos only lose one in-place tweak; the next IDLE
        # cycle regenerates QUEUE.md from headers anyway.
        if self._origin_queue_md_tracked() is not False:
            return
        pr_id = self.state.current_task.pr_id

        queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
        if not queue_path.exists():
            return
        try:
            content = queue_path.read_text()
        except OSError as exc:
            self.log_event(
                f"[MERGE] Warning: read QUEUE.md to mark {pr_id} DONE "
                f"failed: {exc}."
            )
            return

        updated = mark_task_done(content, pr_id)
        if updated is None or updated == content:
            return

        try:
            queue_path.write_text(updated)
        except OSError as exc:
            self.log_event(
                f"[MERGE] Warning: write QUEUE.md to mark {pr_id} DONE "
                f"failed: {exc}."
            )
            return
        self.log_event(f"[MERGE] Marked {pr_id} DONE in QUEUE.md.")

    def _resolve_pending_queue_sync(self) -> bool:
        """Poll the outstanding queue-sync PR and gate IDLE dispatch.

        Returns True when resolved, False when still pending.
        """
        branch = self.state.pending_queue_sync_branch
        if branch is None:
            return True

        try:
            result = github_client.run_gh(
                ["pr", "view", branch, "--json", "state,mergedAt"],
                repo=self.owner_repo,
            )
        except Exception as exc:
            self.log_event(
                f"[MERGE] queue-sync PR {branch} view failed: {exc}."
            )
            self._escalate_queue_sync_if_expired(branch)
            return False

        state = ""
        merged_at = None
        if isinstance(result, dict):
            state = str(result.get("state") or "").upper()
            merged_at = result.get("mergedAt")

        if state == "MERGED" or merged_at:
            self.state.pending_queue_sync_branch = None
            self.state.pending_queue_sync_started_at = None
            self.log_event(f"[MERGE] Queue-sync PR merged ({branch}).")
            return True

        if state == "CLOSED":
            self.state.pending_queue_sync_branch = None
            self.state.pending_queue_sync_started_at = None
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"queue-sync PR {branch} closed without merging"
            )
            self.log_event(f"[MERGE] {self.state.error_message}.")
            return False

        self._escalate_queue_sync_if_expired(branch)
        return False

    def _escalate_queue_sync_if_expired(self, branch: str) -> None:
        started = self.state.pending_queue_sync_started_at
        if started is None:
            return
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        if elapsed <= _QUEUE_SYNC_MAX_WAIT_SEC:
            return
        self.state.pending_queue_sync_branch = None
        self.state.pending_queue_sync_started_at = None
        self.state.state = PipelineState.ERROR
        self.state.error_message = (
            f"queue-sync PR {branch} unresolved after "
            f"{int(elapsed)}s (max {_QUEUE_SYNC_MAX_WAIT_SEC}s)"
        )
        self.log_event(f"[MERGE] {self.state.error_message}.")
