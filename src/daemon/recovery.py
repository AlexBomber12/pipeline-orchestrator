"""State recovery on daemon startup.

Mixin methods:
    recover_state                — reconstruct state from QUEUE.md + GitHub
    _preserve_crashed_run_commits — push unpushed commits before re-CODING
    _rehydrate_last_push_at      — seed _last_push_at from PR head commit
"""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

from src import github_client
from src.daemon import git_ops
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError
from src.task_status import get_merged_pr_ids


class RecoveryMixin:
    """State recovery on daemon startup."""

    def _drop_ghost_queue_entries(
        self, tasks: list[QueueTask]
    ) -> list[QueueTask]:
        """Drop QUEUE.md entries whose declared task file is missing.

        Since PR-181, ``tasks/QUEUE.md`` is gitignored and survives
        ``sync_to_main``'s ``git reset --hard`` + ``git clean -fd``. A
        snapshot from a prior cycle (or a prior CI run sharing the
        daemon volume) can therefore reference tasks/PR-*.md files that
        no longer exist after the base branch was wiped. ``handle_idle``
        already filters such ghosts before dispatch (see PR-181 follow-
        up); recovery must apply the same rule before deciding to
        resurrect a DOING task or match a DONE task to an open PR,
        otherwise a stale DOING entry from a previous test would drag
        the daemon back onto a deleted task instead of staying IDLE.

        Entries without an explicit ``Tasks file:`` line keep their
        pre-PR-181 fallback semantics — the legacy migration paths
        cannot be verified against a file path.

        The caller is responsible for skipping this filter when the queue
        was sourced from ``origin/{branch}`` (legacy tracked-QUEUE repos):
        in that case the working tree may be parked on a feature branch
        whose checkout legitimately lacks task files referenced by the
        base-branch queue, and applying this local-existence test would
        drop in-flight work and detach the daemon from its active PR.
        """
        kept: list[QueueTask] = []
        for queued in tasks:
            if (
                queued.task_file is not None
                and not (Path(self.repo_path) / queued.task_file).is_file()
            ):
                self.log_event(
                    f"recover_state: ignoring ghost QUEUE.md entry "
                    f"{queued.pr_id} (no {queued.task_file} on disk)"
                )
                continue
            kept.append(queued)
        return kept

    def _is_doing_already_merged(self, doing: QueueTask) -> bool:
        """Return ``True`` when ``doing``'s PR is already merged on origin.

        The QUEUE.md snapshot consulted by ``recover_state`` can lag the
        true merge state: on legacy tracked-QUEUE repos
        ``_mark_queue_done`` skips its in-place rewrite to keep the
        working tree clean for preflight, so origin/{branch}'s queue
        keeps the just-merged task pinned at DOING. Without this probe,
        ``recover_state`` would treat the stale entry as interrupted
        work and re-enter CODING for an already-merged task on every
        daemon restart, redoing completed work and creating duplicate
        follow-up activity.

        Probe failures (missing ref, transient git error) report
        ``False`` so the caller falls through to the existing recovery
        paths instead of dropping a real interrupted DOING entry.
        """
        try:
            merged = get_merged_pr_ids(
                self.repo_path,
                self.repo_config.branch,
                {doing.pr_id},
            )
        except (RuntimeError, OSError, subprocess.SubprocessError):
            return False
        return doing.pr_id in merged

    async def recover_state(self) -> bool:
        """Reconstruct state from QUEUE.md + GitHub on daemon startup.

        Decision tree:

        1. If QUEUE.md has a DOING task:
           - Matching open PR on that branch -> WATCH (runner resumes
             polling the existing PR).
           - No matching PR -> CODING + re-run ``handle_coding()``
             (Claude CLI run was interrupted before pushing).
        2. If no DOING task but an open PR whose branch matches a DONE
           task exists -> WATCH (task marked DONE locally but PR not yet
           merged). Unrelated open PRs are ignored.
        3. Otherwise, stay IDLE.

        Runs before ``preflight`` so that even a dirty working tree left
        behind by a crashed cycle does not block recovery. Runs exactly
        once per process on success (see ``_recovered`` in ``__init__``).

        Returns ``True`` once discovery has completed (whether or not a
        subsequent re-run of ``handle_coding`` then failed — that failure
        is handled through the normal ERROR path and must not trigger a
        second, non-idempotent recovery attempt).  Returns ``False`` when
        discovery could not complete — either a transient GitHub outage
        during ``get_open_prs``, or a ``QueueValidationError`` from a
        malformed queue.  In both cases ``run_cycle`` leaves
        ``_recovered`` unset and retries next cycle, but still processes
        pending uploads so an operator can fix the queue via the
        dashboard.  Without this distinction, a transient GitHub error at
        startup would strand the runner detached from an in-flight PR
        and later allow ``handle_error`` to SKIP/FIX it onto new queue
        work.
        """
        strict = self.app_config.daemon.strict_queue_validation
        # Probe the queue source ONCE and reuse the result for both the
        # parse-source decision (origin/{branch} vs working tree) and the
        # ghost-filter decision below. A second independent probe inside
        # ``_parse_base_queue`` could disagree under transient git
        # slowness, parsing the queue from ``origin/{branch}`` while
        # recovery still applied the local-existence ghost filter and
        # dropped real DOING/DONE entries on a feature-branch checkout.
        # The probe can also report ``None`` (timeout/OSError) — that is
        # genuinely "unknown", not "untracked": collapsing it to
        # ``False`` would route a legacy repo into the working-tree path
        # where a feature-branch checkout (or missing ``tasks/QUEUE.md``)
        # would yield a stale/empty queue and detach the daemon from
        # in-flight DOING work. Treat ``None`` as ERROR so the next
        # cycle re-probes once git is responsive.
        queue_from_origin = self._origin_queue_md_tracked()
        if queue_from_origin is None:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                "recover_state: tasks/QUEUE.md tracking probe failed; "
                "retrying next cycle"
            )
            self.log_event(
                "recover_state: tasks/QUEUE.md tracking probe failed; "
                "retrying next cycle"
            )
            return False
        try:
            tasks = self._parse_base_queue(
                strict=strict, queue_from_origin=queue_from_origin,
            )
        except QueueValidationError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"recover_state: queue validation failed: {exc}"
            )
            self.log_event(f"recover_state: queue validation failed: {exc}")
            return False
        if tasks is None:
            if queue_from_origin:
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    "recover_state: read QUEUE.md from origin failed"
                )
                self.log_event(
                    "recover_state: read QUEUE.md from origin failed"
                )
                return False
            # Post-PR-181 repos gitignore ``tasks/QUEUE.md`` and rely on
            # ``handle_idle`` to regenerate it from PR-*.md headers each
            # cycle. A missing snapshot on the working tree therefore
            # signals "scaffolding hasn't reached IDLE yet", not a fatal
            # state. Returning False here would deadlock recovery: when
            # the daemon restarts onto a dirty worktree,
            # ``ensure_repo_cloned`` defers scaffolding (and so the file
            # is never recreated), and ``run_cycle`` exits before
            # ``preflight`` can run its dirty-tree auto-reset, so the
            # runner would loop indefinitely in ERROR/retry. Fall
            # through with an empty task list so the cycle reaches
            # preflight; once the tree self-heals, ``handle_idle``
            # rebuilds QUEUE.md and re-matches any open PR by branch.
            self.log_event(
                "recover_state: tasks/QUEUE.md absent in working tree; "
                "treating as empty queue and deferring to preflight + "
                "IDLE regeneration"
            )
            tasks = []

        try:
            prs = github_client.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"recover_state: get_open_prs failed: {exc}"
            self.log_event(f"recover_state failed: {exc}")
            return False
        # Ghost filtering uses local task-file existence, which is only a
        # safe signal for post-PR-181 repos (QUEUE.md gitignored, parsed
        # from the working tree). On legacy tracked-QUEUE repos the queue
        # we just parsed came from ``origin/{branch}``; the local checkout
        # may legitimately be parked on a feature branch whose tree lacks
        # task files referenced there, and dropping those entries would
        # detach recovery from active DOING/DONE work.
        if not queue_from_origin:
            tasks = self._drop_ghost_queue_entries(tasks)
        self._set_queue_progress(
            sum(1 for t in tasks if t.status == TaskStatus.DONE),
            len(tasks),
        )

        doing = next((t for t in tasks if t.status == TaskStatus.DOING), None)

        pending_sync = next(
            (p for p in prs if (p.branch or "").startswith("queue-done-")),
            None,
        )
        if pending_sync is not None:
            self.state.pending_queue_sync_branch = pending_sync.branch
            self.state.pending_queue_sync_started_at = (
                pending_sync.last_activity
                or datetime.now(timezone.utc)
            )
            self.log_event(
                f"Recovered pending queue-sync branch: {pending_sync.branch}"
            )

        if doing is not None:
            self.state.current_task = doing
            matching = (
                next((p for p in prs if p.branch == doing.branch), None)
                if doing.branch
                else None
            )
            if matching is not None:
                self.state.current_pr = matching
                await self._restore_current_run_record()
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(matching)
                self.log_event(
                    f"Recovered: DOING task {doing.pr_id} "
                    f"-> WATCH PR #{matching.number}"
                )
                return True

            if self._is_doing_already_merged(doing):
                self.log_event(
                    f"recover_state: ignoring stale DOING entry "
                    f"{doing.pr_id} (already merged on "
                    f"origin/{self.repo_config.branch})"
                )
                self.state.current_task = None
                doing = None

        if doing is not None:
            if self.state.user_paused:
                if doing.branch and not self._preserve_crashed_run_commits(
                    doing.branch
                ):
                    self.state.state = PipelineState.ERROR
                    self.state.error_message = (
                        f"recover_state: could not preserve crashed-run "
                        f"commits on {doing.branch!r}; refusing to defer "
                        "CODING while paused"
                    )
                    self.log_event(self.state.error_message)
                    return True
                self.state.current_pr = None
                self.state.state = PipelineState.IDLE
                self.log_event(
                    f"Recovered: DOING task {doing.pr_id}, no PR "
                    "but user_paused -> defer CODING until resume"
                )
                return True

            self.state.state = PipelineState.CODING
            self.log_event(
                f"Recovered: DOING task {doing.pr_id}, no PR "
                "-> re-running CODING"
            )
            if doing.branch and not self._preserve_crashed_run_commits(
                doing.branch
            ):
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    f"recover_state: could not preserve crashed-run "
                    f"commits on {doing.branch!r}; refusing to re-run "
                    "CODING"
                )
                self.log_event(self.state.error_message)
                return True
            await self.handle_coding()
            return True

        queued_by_branch = {
            t.branch: t
            for t in tasks
            if t.branch and t.status in (TaskStatus.TODO, TaskStatus.DONE)
        }
        recoverable = next(
            (
                (pr, queued_by_branch[pr.branch])
                for pr in prs
                if pr.branch in queued_by_branch
            ),
            None,
        )
        if recoverable is not None:
            matched_pr, matched_task = recoverable
            self.state.current_pr = matched_pr
            self.state.current_task = matched_task
            await self._restore_current_run_record()
            self.state.state = PipelineState.WATCH
            self._rehydrate_last_push_at(matched_pr)
            self.log_event(
                f"Recovered: {matched_task.status.value} task "
                f"{matched_task.pr_id} -> WATCH PR #{matched_pr.number}"
            )
            return True

        self.state.state = PipelineState.IDLE
        self.state.error_message = None
        self.state.current_task = None
        self.state.current_pr = None
        self._error_diagnose_count = 0

        if prs:
            self.log_event(
                f"Recovered: {len(prs)} open PR(s) not matched to any "
                "queued task -> IDLE"
            )
        else:
            self.log_event("Recovered: no DOING tasks, no open PRs -> IDLE")
        return True

    def _preserve_crashed_run_commits(self, branch: str) -> bool:
        """Push any unpushed commits on ``branch`` to origin.

        Called from ``recover_state`` before re-running ``handle_coding``
        after a crash. Claude's PLANNED PR flow creates the branch from
        ``origin/main``, which would orphan local-only commits. Pushing
        them first preserves the work on origin so nothing is lost even
        if Claude later resets the local branch.

        Returns ``True`` when it is safe for the caller to proceed with
        re-running CODING (no local branch to preserve, or push
        succeeded). Returns ``False`` when the caller MUST NOT proceed:
        the task targets the base branch (malformed task header that
        would let Claude reset ``main``) or the preserve push failed in
        a way that may have left commits orphan-only on local.
        """
        if branch == self.repo_config.branch:
            self.log_event(
                f"Refusing to preserve crashed-run commits on base "
                f"branch {branch!r}"
            )
            return False

        try:
            probe = git_ops._git(
                self.repo_path,
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/heads/{branch}",
                timeout=10,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            self.log_event(
                f"Could not probe local branch {branch}: {exc}"
            )
            return False
        if probe.returncode != 0:
            return True

        try:
            git_ops._git(
                self.repo_path,
                "push",
                "origin",
                f"{branch}:{branch}",
                timeout=120,
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            self.log_event(
                f"Failed to preserve unpushed commits on {branch}: {exc}"
            )
            return False

        self.log_event(f"Preserved crashed-run commits on {branch}")
        return True

    def _rehydrate_last_push_at(self, pr: PRInfo) -> None:
        """Seed ``_last_push_at`` from the PR's head commit's committer
        date when we don't already have a fresher in-memory value.

        Needed on daemon restart (``__init__`` resets
        ``_last_push_at`` to ``None``) and when ``handle_coding`` hands
        off to WATCH on a freshly-created PR: without this rehydrate,
        ``_has_new_codex_feedback_since_last_push`` would hit its
        ``None`` default and return ``True`` on every cycle, triggering
        ``handle_fix`` on pre-restart Codex feedback.

        Falling back to ``pr.last_activity`` here is intentionally
        avoided: ``last_activity`` comes from GitHub's ``updatedAt``,
        which advances whenever Codex posts a comment, so a transient
        commit-time fetch failure plus a pending Codex P1/P2 post
        would seed the baseline to the feedback timestamp and make
        the next ``_has_new_codex_feedback_since_last_push`` return
        False, silently skipping the fix. When the fetch fails we
        leave ``_last_push_at`` unset; ``handle_watch`` calls this
        helper each cycle so the rehydrate retries naturally on the
        next poll instead of latching a wrong value.
        """
        try:
            metadata = github_client.get_pr_metadata(
                self.owner_repo, pr.number
            )
            head_iso = metadata.get("head_commit_date", "")
        except Exception:
            head_iso = ""
        head_time = github_client._parse_iso(head_iso) if head_iso else None
        if head_time is not None and head_time.tzinfo is None:
            head_time = head_time.replace(tzinfo=timezone.utc)
        if self._last_push_at_pr_number != pr.number:
            self._last_push_at = head_time
            self._last_push_at_pr_number = pr.number
            return
        if head_time is None:
            return
        if self._last_push_at is None or head_time > self._last_push_at:
            self._last_push_at = head_time
