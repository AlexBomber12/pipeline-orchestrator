"""State recovery on daemon startup.

Mixin methods:
    recover_state                — reconstruct state from task headers + GitHub
    _preserve_crashed_run_commits — push unpushed commits before re-CODING
    _rehydrate_last_push_at      — seed _last_push_at from PR head commit
"""

from __future__ import annotations

import logging
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from src.branch_context import BranchContext
from src.cancellation import (
    SUBSOURCE_VOCABULARY,
    CancellationCause,
    classify_cancellation_subsource,
    get_cancellation_cause,
    get_current_run_started_at,
)
from src.daemon import git_ops
from src.github import gh_runner
from src.github import prs as gh_prs
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.queue_parser import QueueValidationError, parse_task_header
from src.task_status import (
    _resolve_merged_state,
    derive_task_status,
    get_merged_pr_ids,
)

logger = logging.getLogger(__name__)

_PR_NUMBER_RE = re.compile(r"^PR-(\d+)(.*)$")


def _task_pr_sort_key(pr_id: str) -> tuple[int, int | str, str]:
    match = _PR_NUMBER_RE.match(pr_id)
    if match is None:
        return (1, pr_id, "")
    return (0, int(match.group(1)), match.group(2))


class RecoveryMixin:
    """State recovery on daemon startup."""

    def _parse_tasks_from_headers(self) -> list[QueueTask] | None:
        """Parse structured task headers into a recovered queue snapshot.

        Recovery reads task files directly; ``tasks/QUEUE.md`` is only a
        generated dashboard/review view.
        """
        repo_root = Path(self.repo_path)
        task_dir = repo_root / "tasks"
        if not task_dir.is_dir():
            return None

        headers = []
        task_files: dict[str, str] = {}
        for task_file in sorted(task_dir.glob("PR-*.md")):
            try:
                header = parse_task_header(task_file)
            except QueueValidationError as exc:
                if not (
                    self._is_missing_task_header_error(exc)
                    or self._is_legacy_unstructured_task_error(exc)
                ):
                    raise
                continue
            if header.pr_id != task_file.stem:
                raise QueueValidationError(
                    [
                        f"{task_file}: header PR ID {header.pr_id!r} "
                        f"does not match task file {task_file.stem!r}"
                    ]
                )
            headers.append(header)
            task_files[header.pr_id] = task_file.relative_to(repo_root).as_posix()

        if not headers:
            return None

        for header in headers:
            if header.frontmatter_status == "error":
                self._crashed_task_pr_ids.add(header.pr_id)

        merged_state = _resolve_merged_state(
            self.repo_path,
            self.repo_config.branch,
            self.owner_repo,
            {
                pr_id
                for header in headers
                for pr_id in (header.pr_id, *header.depends_on)
            },
            headers,
            log_event=self.log_event,
        )
        open_prs = list(getattr(self, "_idle_open_prs", ()))
        merged_prs = list(getattr(self, "_idle_merged_prs", ()))
        current_task_pr_id = (
            self.state.current_task.pr_id
            if self.state.current_task is not None
            else None
        )
        stopped_task_pr_ids = set(getattr(self, "_user_stopped_task_pr_ids", set()))
        if current_task_pr_id in stopped_task_pr_ids:
            current_task_pr_id = None
        crashed_task_pr_ids = set(getattr(self, "_crashed_task_pr_ids", set()))

        tasks: list[QueueTask] = []
        for header in headers:
            status = derive_task_status(
                header,
                merged_state,
                open_prs,
                merged_prs,
                current_task_pr_id=current_task_pr_id,
            )
            if (
                header.pr_id in crashed_task_pr_ids
                and status in (TaskStatus.TODO, TaskStatus.ERROR)
            ):
                status = TaskStatus.ERROR
            tasks.append(
                QueueTask(
                    pr_id=header.pr_id,
                    title=header.title,
                    status=status,
                    task_file=task_files[header.pr_id],
                    depends_on=list(header.depends_on),
                    branch=header.branch,
                    priority=header.priority,
                )
            )

        return sorted(
            tasks,
            key=lambda task: (task.priority, _task_pr_sort_key(task.pr_id)),
        )

    def _is_doing_already_merged(self, doing: QueueTask) -> bool:
        """Return ``True`` when ``doing``'s PR is already merged on origin.

        The task status derived from headers can lag the true merge
        state when a daemon restart races a just-merged task. Without
        this probe, ``recover_state`` would treat the stale entry as
        interrupted work and re-enter CODING for an already-merged task
        on every daemon restart, redoing completed work and creating
        duplicate follow-up activity.

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
        """Reconstruct state from task headers + GitHub.

        Decision tree:

        1. If a DOING task is present:
           - Matching open PR on that branch -> WATCH (runner resumes
             polling the existing PR).
           - No matching PR -> mark crashed/ERROR and stay IDLE.
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

        The recovery source is the structured ``tasks/PR-*.md`` headers.
        ``tasks/QUEUE.md`` is a generated view and is not consulted by
        startup recovery.
        """
        # PR-272 follow-up: clear any stale ``info/expected-branch`` marker
        # left behind by a SIGKILL/OOM/crash mid-CODING. ``handle_coding``
        # cleans the marker in a ``finally`` block on every normal exit,
        # but a daemon kill skips Python teardown and the marker survives.
        # The next IDLE cycle's ``process_pending_uploads`` issues a
        # daemon-driven ``git push origin {branch}`` for upload commits;
        # the pre-push hook reads the stale marker, sees HEAD on the base
        # branch != the dead task's expected branch, and rejects the push,
        # stalling the runner in IDLE indefinitely. The marker is only
        # valid during an active CODING dispatch, so clearing it on
        # startup recovery is always correct: if recovery re-enters
        # CODING through the DOING-with-matching-PR path the next handle
        # cycle rewrites the marker before invoking the coder.
        self._cleanup_expected_branch()
        return await self._recover_state_headers()

    async def _hydrate_current_task_from_persisted_state(self) -> None:
        """Restore ``state.current_task`` from the published Redis snapshot.

        ``publish_state`` writes the live ``RepoState`` to
        ``pipeline_state(self.name)`` at the end of every cycle, so the
        last persisted snapshot is the authoritative record of which
        task was active when the daemon went down. The runner re-builds
        a fresh ``RepoState`` on startup with ``current_task=None``;
        without this rehydrate the headers helper's
        ``current_task_pr_id`` input is always ``None``, and a pre-push
        crash mid-CODING (no open PR yet) re-derives the task as
        ``TODO`` rather than ``DOING``.

        Hydration is gated on a persisted ``CODING`` state because that
        is the only crash-relevant scenario: ``current_task`` is set
        without an open PR only between IDLE→CODING dispatch and the
        coder's first push. Snapshots from WATCH/FIX/MERGE always have
        a matching open PR by construction, so ``derive_task_status``
        already returns ``DOING`` via ``find_matching_open_pr`` without
        the ``current_task_pr_id`` fallback. Hydrating those snapshots
        would misclassify a benign restart as a crash if the PR has
        since been closed externally: ``derive_task_status`` would fall
        through to the ``current_task_pr_id`` branch, return ``DOING``,
        and ``_apply_recovery_decisions`` would mark the task
        ``ERROR`` and force manual re-upload.

        Best-effort: Redis read errors, missing snapshots, and corrupt
        payloads all leave ``state.current_task`` untouched. This is
        deliberately narrow — we only hydrate ``current_task`` because
        ``_apply_recovery_decisions`` overwrites it from the parsed task
        list afterwards, so a stale value cannot leak past recovery
        even when the snapshot lags reality (e.g. the task was merged
        externally between crash and restart, in which case
        ``derive_task_status`` returns ``DONE`` first via the merged-
        state probe and the hydrated value is irrelevant).
        """
        if self.state.current_task is not None:
            return
        try:
            raw = await self.redis.get(pipeline_state(self.name))
        except Exception:
            return
        if not raw:
            return
        try:
            persisted = RepoState.model_validate_json(raw)
        except Exception:
            return
        if persisted.state != PipelineState.CODING:
            return
        if persisted.current_task is not None:
            self.state.current_task = persisted.current_task

    async def _recover_state_headers(self) -> bool:
        """Apply state using the headers-derived task list.

        Skips the QUEUE.md probe entirely: the headers helper reads
        ``tasks/PR-*.md`` directly and computes ``DOING``/``TODO``/
        ``DONE`` via ``derive_task_status`` against the same merged-
        state input the IDLE cycle uses. ``state.current_queue`` is
        hydrated from the result so post-restart state matches the
        in-memory snapshot model introduced in PR-263.
        """
        try:
            prs = gh_prs.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            await self._transition_to_error(
                f"recover_state: get_open_prs failed: {exc}",
                save_run_record_as=None,
                publish=False,
                log_prefix="[INFRA]",
                log_message=f"recover_state failed: {exc}",
            )
            return False
        # PR-266b crash-no-PR fix: the headers helper derives ``DOING``
        # via ``current_task_pr_id`` when no matching open PR exists.
        # ``state.current_task`` is reset on daemon restart, so without
        # this rehydrate a pre-push crash (no open PR yet) re-derives
        # the previously DOING task as ``TODO``; ``_apply_recovery_
        # decisions`` only runs the PR-186 crash path on a DOING entry,
        # so the task would be silently re-dispatched into a crash loop
        # instead of being marked ERROR pending manual re-upload.
        # Run after the ``get_open_prs`` probe so a transient GitHub
        # outage during recovery still surfaces as ERROR with no
        # current_task.
        await self._hydrate_current_task_from_persisted_state()
        await self._hydrate_status_write_failed_task_pr_ids()
        # The helper consults ``_idle_open_prs`` to derive each task's
        # status from the live PR set. Populate it from the recovery
        # fetch and reset on exit so ``handle_idle`` does not later read
        # a stale recovery-time snapshot.
        prior_open_prs = getattr(self, "_idle_open_prs", None)
        prior_merged_prs = getattr(self, "_idle_merged_prs", None)
        self._idle_open_prs = list(prs)
        self._idle_merged_prs = []
        try:
            try:
                tasks = self._parse_tasks_from_headers()
            except QueueValidationError as exc:
                await self._transition_to_error(
                    f"recover_state: queue validation failed: {exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[INFRA]",
                )
                return False
        finally:
            if prior_open_prs is None:
                if hasattr(self, "_idle_open_prs"):
                    delattr(self, "_idle_open_prs")
            else:
                self._idle_open_prs = prior_open_prs
            if prior_merged_prs is None:
                if hasattr(self, "_idle_merged_prs"):
                    delattr(self, "_idle_merged_prs")
            else:
                self._idle_merged_prs = prior_merged_prs

        if tasks is None:
            self.log_event(
                "[INFRA] recover_state: no tasks/PR-*.md headers parsed; "
                "treating as empty queue and deferring to preflight + "
                "IDLE regeneration."
            )
            tasks = []
        await self._apply_recovery_decisions(tasks, prs)
        # PR-263: hydrate the in-memory queue snapshot so the dashboard's
        # /api/repo/{name}/queue endpoint reflects post-restart state
        # immediately, without waiting for the next IDLE cycle.
        self.state.current_queue = list(tasks)
        return True

    async def _apply_recovery_decisions(
        self, tasks: list[QueueTask], prs: list[PRInfo]
    ) -> None:
        """Apply the shared post-parse decision tree to runner state.

        The body mirrors the original recover_state decision tree.
        """
        self._set_queue_progress(
            sum(1 for t in tasks if t.status == TaskStatus.DONE),
            len(tasks),
        )

        # PR-186 Codex P1: rehydrate the crashed-task set from any ERROR
        # entries in the parsed queue. The crashed-task cancellation is what
        # tells ``_select_next_task_from_dag`` to skip the task on the next
        # IDLE cycle; without this rehydrate, a daemon restart after one
        # IDLE cycle has already written ERROR to QUEUE.md would start
        # with an empty set, ``recover_state`` would see no DOING entry to
        # re-mark, and the selector would recompute the task as TODO and
        # dispatch it again — defeating the "manual re-upload required"
        # contract and reintroducing the crash loop. The set is cleared
        # only when the user re-uploads the task file (see ``repo_ops``).
        for queued in tasks:
            if queued.status == TaskStatus.ERROR:
                self._crashed_task_pr_ids.add(queued.pr_id)

        status_write_failed_task_pr_ids = getattr(
            self,
            "_status_write_failed_task_pr_ids",
            set(),
        )
        if status_write_failed_task_pr_ids:
            changed = False
            for index, queued in enumerate(tasks):
                if queued.pr_id not in status_write_failed_task_pr_ids:
                    continue
                if queued.status == TaskStatus.DONE:
                    status_write_failed_task_pr_ids.discard(queued.pr_id)
                    changed = True
                    continue
                if queued.status != TaskStatus.ERROR:
                    tasks[index] = queued.model_copy(
                        update={"status": TaskStatus.ERROR}
                    )
                    changed = True
            if changed:
                await self._persist_status_write_failed_task_pr_ids()

        doing = next((t for t in tasks if t.status == TaskStatus.DOING), None)

        # Record any outstanding queue-sync PR before any DOING-path early
        # return below. ``handle_idle`` gates dispatch on
        # ``pending_queue_sync_branch``; if recovery exits early without
        # setting it (e.g. the operator-recovered branch immediately
        # below), the daemon would resume dispatching new work while a
        # ``queue-done-*`` PR is still open, regressing the prior ordering
        # contract that pending sync is always seen before subsequent
        # transitions.
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
                f"[INFRA] Recovered pending queue-sync branch: "
                f"{pending_sync.branch}."
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
                # PR-202: anchor the slow-start window at recovery
                # time so the daemon's next poll interval already uses
                # the slow cadence (the cycle-end transition detector
                # in ``_run_cycle_body`` does not see this transition
                # because recovery returns before ``pre_state`` is
                # captured).
                self._watch_entered_at = datetime.now(timezone.utc)
                self._rehydrate_last_push_at(matching)
                self.log_event(
                    f"[INFRA] Recovered: DOING task {doing.pr_id} "
                    f"-> WATCH PR #{matching.number}."
                )
                return

            if self._is_doing_already_merged(doing):
                self.log_event(
                    f"[INFRA] recover_state: ignoring stale DOING entry "
                    f"{doing.pr_id} (already merged on "
                    f"origin/{self.repo_config.branch})."
                )
                self.state.current_task = None
                self._reset_runner_local_task_counters()
                doing = None

        if doing is not None:
            if self.state.user_paused:
                if doing.branch and not self._preserve_crashed_run_commits(
                    doing.branch
                ):
                    await self._transition_to_error(
                        (
                            f"recover_state: could not preserve crashed-run "
                            f"commits on {doing.branch!r}; refusing to defer "
                            "CODING while paused"
                        ),
                        save_run_record_as=None,
                        publish=False,
                        log_prefix="[INFRA]",
                    )
                    return
                self.state.current_pr = None
                self.state.state = PipelineState.IDLE
                self.log_event(
                    f"[INFRA] Recovered: DOING task {doing.pr_id}, no PR "
                    f"but user_paused -> defer CODING until resume."
                )
                return

            # PR-186: A DOING task with no matching PR after recovery is a
            # crash signature (subprocess kill, OOM, daemon restart mid-
            # CODING). Re-running CODING here used to loop the same crash
            # forever; instead preserve any unpushed commits on origin so
            # the work is not lost, then mark the task crashed/ERROR so
            # the next IDLE cycle skips it. The user re-uploads the task
            # file to retry.
            if doing.branch and not self._preserve_crashed_run_commits(
                doing.branch
            ):
                await self._transition_to_error(
                    (
                        f"recover_state: could not preserve crashed-run "
                        f"commits on {doing.branch!r}; refusing to mark "
                        "ERROR"
                    ),
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[INFRA]",
                )
                return
            # Capture branch surfaces before clearing ``current_task``
            # so the cancellation diagnostic names the task branch the
            # crash was associated with rather than ``<absent>``.
            ctx = BranchContext.from_runner(self)
            # PR-318: dispatch the recovery branch off ``payload.subsource``
            # (PR-315 collapsed ``category`` to a single ``ERROR``).
            # ``_dispatch_recovery_branch`` defensively reads the recorded
            # cause, warns on any non-ERROR category (legacy record that
            # escaped the startup migration), and returns either ``crash``
            # or ``operator_attention``. Both branches mark the task ERROR
            # — the distinction is the operator-visible log line so the
            # dashboard surfaces whether the previous run died abruptly
            # or was deliberately parked by a detector.
            branch_kind = await self._dispatch_recovery_branch(doing.pr_id)
            self._crashed_task_pr_ids.add(doing.pr_id)
            # PR-266b crash-no-PR fix: reflect the cancellation in the
            # in-memory tasks list so the headers-mode current_queue
            # snapshot does not display ``DOING`` for a task the runner
            # just gave up on. The IDLE selector applies the same
            # override on its next cycle; doing it eagerly here keeps
            # post-restart dashboards consistent without waiting for
            # the next poll.
            for i, t in enumerate(tasks):
                if t.pr_id == doing.pr_id and t.status == TaskStatus.DOING:
                    tasks[i] = t.model_copy(update={"status": TaskStatus.ERROR})
                    break
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.state.state = PipelineState.IDLE
            if branch_kind == "crash":
                self.log_event(
                    f"[INFRA] Task {doing.pr_id} crashed, marking ERROR. "
                    f"Manually re-upload to retry. "
                    f"({ctx.log_summary()})"
                )
            else:
                self.log_event(
                    f"[INFRA] Task {doing.pr_id} parked for operator "
                    f"attention, marking ERROR. Manually re-upload to "
                    f"retry. ({ctx.log_summary()})"
                )
            return

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
            # PR-202: see DOING-recovery branch above for rationale.
            self._watch_entered_at = datetime.now(timezone.utc)
            self._rehydrate_last_push_at(matched_pr)
            self.log_event(
                f"[INFRA] Recovered: {matched_task.status.value} task "
                f"{matched_task.pr_id} -> WATCH PR #{matched_pr.number}."
            )
            return

        self.state.state = PipelineState.IDLE
        self.state.current_task = None
        self._reset_runner_local_task_counters()

        if prs:
            self.log_event(
                f"[INFRA] Recovered: {len(prs)} open PR(s) not matched "
                f"to any queued task -> IDLE."
            )
        else:
            self.log_event(
                "[INFRA] Recovered: no DOING tasks, no open PRs -> IDLE."
            )

    async def _dispatch_recovery_branch(self, task_pr_id: str) -> str:
        """Read ``task_pr_id``'s cancellation cause and return the dispatch branch.

        PR-318: returns ``"operator_attention"`` only when a cause record
        exists, its ``payload.subsource`` is a deliberate detector park
        signal (review_timeout, fix_iteration_cap, ...), AND the cause's
        ``created_at`` is at or after the dispatch timestamp recorded by
        ``handle_coding`` for the current run. Every other code path
        returns ``"crash"``:

        * no cause record exists — the daemon was killed before
          ``_transition_to_error`` ran (the canonical mid-CODING crash);
        * Redis read error — back-compatible default matching the pre-
          PR-318 single-branch behavior;
        * cause exists with ``payload.subsource == "crash"`` — the
          explicit crash signal;
        * cause exists with empty/unknown subsource — degrade per the
          PR-318 edge-case to the safer crash-recovery log line;
        * cause exists with a non-crash subsource but predates the
          current dispatch (or no dispatch timestamp is recorded) —
          the cause is stale from a previous run whose best-effort
          ``safe_delete_cancellation_cause`` cleanup failed, so trusting
          it would misclassify a real mid-CODING crash as operator-
          attention and hide the crash signal dashboards depend on.

        Defaulting unknown signals to ``"crash"`` keeps recovery's log
        line back-compatible: dashboards have grepped on the "crashed,
        marking ERROR" line since PR-186, and the only PR-318-intended
        deviation is the deliberate non-crash subsource.

        ``classify_cancellation_subsource`` already logs an ``[INFRA]``
        warning when the cause carries a legacy ``category`` value the
        PR-315 startup migration should have rewritten. The log function
        is threaded through so the warning lands on the same operator
        event stream as the rest of recovery diagnostics rather than the
        module logger.
        """
        try:
            cause = await get_cancellation_cause(
                self.redis, self.name, task_pr_id
            )
        except Exception as exc:
            self.log_event(
                f"[INFRA] recover_state: failed to read cancellation "
                f"cause for {task_pr_id}: {exc}. Falling back to "
                f"crash-recovery branch."
            )
            return "crash"
        if cause is None:
            return "crash"
        subsource = classify_cancellation_subsource(cause, log=self.log_event)
        if subsource == "crash":
            return "crash"
        if subsource not in SUBSOURCE_VOCABULARY:
            self.log_event(
                f"[INFRA] recover_state: cancellation cause for "
                f"{task_pr_id} has empty/unrecognized subsource "
                f"{subsource!r}; falling back to crash-recovery branch."
            )
            return "crash"
        if not await self._cause_belongs_to_current_run(cause, task_pr_id):
            return "crash"
        return "operator_attention"

    async def _cause_belongs_to_current_run(
        self, cause: CancellationCause, task_pr_id: str
    ) -> bool:
        """Return ``True`` iff ``cause`` was written during the current run.

        PR-318 fix-feedback: ``safe_delete_cancellation_cause`` is best-
        effort and swallows Redis failures, so a non-crash cause recorded
        by an earlier run may persist across retries. If the next run
        then dies mid-CODING before writing a fresh cause, the recovery
        handler would observe the stale cause and misclassify a real
        crash as deliberate operator-attention parking.

        Treat a non-crash cause as authoritative only when we can prove
        it belongs to the current dispatch. The proof is the per-task
        ``current_run_started_at`` timestamp written by ``handle_coding``
        on every dispatch: if ``cause.created_at`` predates that
        timestamp (or the timestamp is absent or unreadable), the cause
        does not belong to this run.

        ``False`` therefore covers four sub-cases, each logged via the
        same ``[INFRA]`` event stream as other recovery diagnostics:

        * Redis read failure for the run-start key — back-compatible
          conservative default;
        * no run-start key recorded — pre-fix records, or first dispatch
          after a deployment that introduced the marker;
        * cause has malformed ``created_at`` — corrupt record;
        * cause was created strictly before the recorded dispatch — the
          canonical stale-cause scenario.
        """
        try:
            started_at = await get_current_run_started_at(
                self.redis, self.name, task_pr_id
            )
        except Exception as exc:
            self.log_event(
                f"[INFRA] recover_state: failed to read current_run "
                f"start for {task_pr_id}: {exc}. Treating cause as "
                f"stale and falling back to crash-recovery branch."
            )
            return False
        if started_at is None:
            self.log_event(
                f"[INFRA] recover_state: no current_run start recorded "
                f"for {task_pr_id}; cannot prove non-crash cause belongs "
                f"to the current run, falling back to crash-recovery "
                f"branch."
            )
            return False
        try:
            cause_created_at = datetime.fromisoformat(cause.created_at)
        except (TypeError, ValueError):
            self.log_event(
                f"[INFRA] recover_state: cancellation cause for "
                f"{task_pr_id} has malformed created_at "
                f"{cause.created_at!r}; treating as stale and falling "
                f"back to crash-recovery branch."
            )
            return False
        if cause_created_at.tzinfo is None:
            cause_created_at = cause_created_at.replace(tzinfo=timezone.utc)
        if cause_created_at < started_at:
            self.log_event(
                f"[INFRA] recover_state: cancellation cause for "
                f"{task_pr_id} created at {cause.created_at} predates "
                f"current run start {started_at.isoformat()}; treating "
                f"as stale and falling back to crash-recovery branch."
            )
            return False
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
                f"[INFRA] Refusing to preserve crashed-run commits on "
                f"base branch {branch!r}."
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
                f"[INFRA] Could not probe local branch {branch}: {exc}."
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
                f"[INFRA] Failed to preserve unpushed commits on "
                f"{branch}: {exc}."
            )
            return False

        self.log_event(
            f"[INFRA] Preserved crashed-run commits on {branch}."
        )
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
            metadata = gh_prs.get_pr_metadata(
                self.owner_repo, pr.number
            )
            head_iso = metadata.get("head_commit_date", "")
        except Exception:
            head_iso = ""
        head_time = gh_runner._parse_iso(head_iso) if head_iso else None
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
