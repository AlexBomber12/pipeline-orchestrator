"""MERGE state handler and queue-sync operations.

Mixin methods:
    handle_merge                    — merge PR and return to IDLE
    _mark_task_done_in_snapshot     — mark task DONE in RepoState.current_queue
    _resolve_pending_queue_sync     — poll legacy queue-sync PR status
    _escalate_queue_sync_if_expired — escalate to ERROR on timeout
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timezone

from src import claude_cli, codex_cli
from src.analytics import log_merged_pr
from src.analytics.coder_version import detect_coder_extension_version
from src.branch_context import BranchContext
from src.cancellation import delete_retry_count, delete_task_spec_hash
from src.config import CoderType
from src.daemon import git_ops
from src.github import cache as gh_cache
from src.github import gh_runner
from src.github import prs as gh_prs
from src.models import PipelineState, TaskStatus
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

        logger.info(
            "[BRANCH] handle_merge: %s",
            BranchContext.from_runner(self).log_summary(),
        )

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
                            await self._transition_to_error(
                                (
                                    "No eligible coder available for merge "
                                    "conflict resolution"
                                ),
                                publish=False,
                                log_prefix="[MERGE]",
                            )
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
                                await self._clear_error_message_on_recovery(
                                    log_prefix="[MERGE]",
                                    reason="rate-limit pause during merge conflict resolution",
                                )
                                await self._save_current_run_record("rate_limit")
                                self.log_event(
                                    f"[RATE-LIMIT] Rate limit pause "
                                    f"active until "
                                    f"{self.state.rate_limited_until.isoformat()}."
                                )
                                return
                            await self._transition_to_error(
                                "Merge conflict resolution failed",
                                publish=False,
                                log_prefix="[MERGE]",
                            )
                            return
                        sync_produced_commit = True
                    else:
                        await self._transition_to_error(
                            (
                                f"git merge origin/{base} failed: "
                                f"{merge_result.stderr.strip()}"
                            ),
                            publish=False,
                            log_prefix="[MERGE]",
                        )
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
                    gh_cache._invalidate_etag_cache(
                        f"repos/{self.owner_repo}/pulls"
                    )
                    self.state.state = PipelineState.WATCH
                    self.log_event(
                        f"[MERGE] Pre-merge sync pushed new commits to PR "
                        f"#{number}; returning to WATCH to re-verify "
                        f"gates."
                    )
                    try:
                        new_head_sha = git_ops._git(
                            self.repo_path, "rev-parse", "HEAD"
                        ).stdout.strip()
                    except Exception:
                        new_head_sha = ""
                    self.log_event(
                        f"[MERGE] Bypass-requesting fresh @codex review on "
                        f"new head "
                        f"{new_head_sha[:7] or '<unknown>'} after "
                        f"pre-merge sync."
                    )
                    if not self._post_codex_review(
                        number,
                        bypass_author_dedup=True,
                    ):
                        await self._transition_to_error(
                            (
                                f"Failed to post @codex review on PR "
                                f"#{number} after pre-merge sync push; "
                                "manual review trigger required to avoid "
                                "merging on stale approval"
                            ),
                            save_run_record_as=None,
                            publish=False,
                            log_prefix="[MERGE]",
                        )
                    return
            except (subprocess.CalledProcessError,
                    subprocess.TimeoutExpired, OSError, RuntimeError) as exc:
                await self._transition_to_error(
                    f"Pre-merge sync failed: {exc}",
                    publish=False,
                    log_prefix="[MERGE]",
                )
                return

        merged_diff_stats = self._compute_diff_stats(base)
        self.log_event(f"[MERGE] Merging PR #{number}.")
        try:
            gh_runner.run_gh(
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
            gh_prs.merge_pr(self.owner_repo, number)
        except Exception as exc:
            await self._transition_to_error(
                f"merge_pr failed: {exc}",
                publish=False,
                log_prefix="[MERGE]",
                log_message=str(exc),
            )
            return

        current_task = self.state.current_task
        if current_task is not None and current_task.task_file:
            try:
                await self._commit_task_status_change(
                    current_task,
                    "DONE",
                    "PR merged",
                )
            except Exception as exc:
                self.log_event(
                    f"[ERROR] Failed to write status:DONE to "
                    f"{current_task.task_file}: {exc}"
                )

        self._mark_task_done_in_snapshot()

        await self._save_current_run_record(
            "success_merged",
            diff_stats=merged_diff_stats,
            base_branch=base,
        )
        merged_at = datetime.now(timezone.utc)
        try:
            log_merged_pr(self._build_outcome_record(merged_at))
        except Exception as exc:
            self.log_event(
                f"[ANALYTICS] outcome log for PR #{number} failed: {exc}."
            )
        if current_task is not None:
            try:
                await delete_task_spec_hash(
                    self.redis, self.name, current_task.pr_id
                )
                await delete_retry_count(self.redis, self.name, current_task.pr_id)
            except Exception as exc:
                self.log_event(
                    f"[MERGE] Failed to clear retry metadata for "
                    f"{current_task.pr_id}: {exc}."
                )
        self._current_run_record = None
        self.state.current_task = None
        self._reset_runner_local_task_counters()
        self.state.state = PipelineState.IDLE
        self.log_event(f"[MERGE] Merged PR #{number} -> IDLE.")

    def _build_outcome_record(self, merged_at: datetime) -> dict:
        """Assemble the outcome dict for the just-merged PR.

        Pulls fields from the finalized run record and runner state.
        Fields the daemon does not yet track are written as ``None`` so
        the schema row stays complete; future PRs can fill them in
        without changing the file format.
        """
        record = self._current_run_record
        pr = self.state.current_pr
        task = self.state.current_task
        coder_name, model = self._resolve_outcome_coder_and_model(record)

        wall_clock_seconds: int | None = None
        if record is not None and record.duration_ms is not None:
            wall_clock_seconds = max(record.duration_ms // 1000, 0)

        # ``fix_iteration_count`` counts FIX entries the daemon drove on
        # this PR; each one is preceded by exactly one ``@codex review``
        # trigger, plus the initial post-create review. Using the PR-side
        # counter (rather than the run record) survives a coder restart
        # where the run record was re-initialized mid-PR.
        codex_review_iterations: int | None = None
        if pr is not None:
            codex_review_iterations = pr.fix_iteration_count + 1

        pr_id_value = ""
        if task is not None and task.pr_id:
            pr_id_value = task.pr_id
        elif pr is not None and pr.pr_id:
            pr_id_value = pr.pr_id

        return {
            "pr_id": pr_id_value,
            # ``log_merged_pr`` recomputes this from pr_id+repo_slug; the
            # placeholder keeps the dict complete for the schema check.
            "task_id_hash": "",
            "repo_slug": self.name,
            "merged_at": merged_at.isoformat(),
            "coder": coder_name,
            "coder_model_string": model,
            "coder_extension_version": detect_coder_extension_version(coder_name),
            "task_type": (record.task_type if record is not None else "") or "",
            "task_complexity": (record.complexity if record is not None else "") or "",
            "fix_iterations": (
                record.fix_iterations if record is not None else 0
            ),
            "ci_runs_total": None,
            "ci_runs_failed": None,
            "wall_clock_seconds": wall_clock_seconds,
            "files_changed": (
                record.files_touched_count if record is not None else 0
            ),
            "lines_added": (
                record.diff_lines_added if record is not None else 0
            ),
            "lines_removed": (
                record.diff_lines_deleted if record is not None else 0
            ),
            "review_blocker_count": None,
            "review_nit_count": None,
            "codex_review_iterations": codex_review_iterations,
            "tokens_estimate": None,
            "outcome": "merged",
        }

    def _resolve_outcome_coder_and_model(self, record) -> tuple[str, str]:
        """Return the (coder, model) pair that actually ran the PR.

        ``_get_coder()`` may switch away from the configured default at
        run time — task-level pinning, exploration, or rate-limit
        fallback can all select a non-default coder — and the chosen
        pair is captured in the run record's
        ``profile_id`` (``"<coder>:<model>:container"``) when CODING
        starts. Reading from there keeps merged outcome rows aligned
        with the run that produced them so later model/version-level
        analytics are not mislabeled. Fall back to the repo/daemon
        default only when no run record exists (e.g. recovery paths
        that build an outcome row without a CODING pass on this
        process).
        """
        if record is not None and record.profile_id:
            parts = record.profile_id.split(":")
            if len(parts) >= 2 and parts[0] and parts[1]:
                return parts[0], parts[1]
        configured_coder = (
            self.repo_config.coder or self.app_config.daemon.coder
        )
        coder_name = configured_coder.value
        model = (
            self.app_config.daemon.codex_model
            if coder_name == CoderType.CODEX.value
            else self.app_config.daemon.claude_model
        )
        return coder_name, model

    def _mark_task_done_in_snapshot(self) -> None:
        """Flip the merged task to DONE in ``state.current_queue``.

        The next IDLE cycle rebuilds the snapshot from task headers and
        the GitHub merge state anyway, so this is just a between-tick
        tweak so dashboard consumers see the merge before the next
        cycle publishes a fresh snapshot.

        Reassigns ``state.current_queue`` after the mutation so the
        ``RepoState.__setattr__`` hook re-stamps
        ``current_queue_snapshot_at``; without that, dashboard clients
        that treat ``snapshot_at`` as a change token would see the new
        DONE status under the old timestamp.
        """
        task = self.state.current_task
        if task is None:
            return
        snapshot = self.state.current_queue
        if not snapshot:
            return
        for index, queued in enumerate(snapshot):
            if queued.pr_id == task.pr_id and queued.status != TaskStatus.DONE:
                snapshot[index] = queued.model_copy(
                    update={"status": TaskStatus.DONE}
                )
                self.state.current_queue = snapshot
                break

    async def _resolve_pending_queue_sync(self) -> bool:
        """Poll the outstanding queue-sync PR and gate IDLE dispatch.

        Returns True when resolved, False when still pending.
        """
        branch = self.state.pending_queue_sync_branch
        if branch is None:
            return True

        try:
            result = gh_runner.run_gh(
                ["pr", "view", branch, "--json", "state,mergedAt"],
                repo=self.owner_repo,
            )
        except Exception as exc:
            self.log_event(
                f"[MERGE] queue-sync PR {branch} view failed: {exc}."
            )
            await self._escalate_queue_sync_if_expired(branch)
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
            await self._transition_to_error(
                f"queue-sync PR {branch} closed without merging",
                save_run_record_as=None,
                publish=False,
                log_prefix="[MERGE]",
            )
            return False

        await self._escalate_queue_sync_if_expired(branch)
        return False

    async def _escalate_queue_sync_if_expired(self, branch: str) -> None:
        started = self.state.pending_queue_sync_started_at
        if started is None:
            return
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        if elapsed <= _QUEUE_SYNC_MAX_WAIT_SEC:
            return
        self.state.pending_queue_sync_branch = None
        self.state.pending_queue_sync_started_at = None
        await self._transition_to_error(
            (
                f"queue-sync PR {branch} unresolved after "
                f"{int(elapsed)}s (max {_QUEUE_SYNC_MAX_WAIT_SEC}s)"
            ),
            save_run_record_as=None,
            publish=False,
            log_prefix="[MERGE]",
        )
