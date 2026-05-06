"""State recovery on daemon startup.

Mixin methods:
    recover_state                — reconstruct state from QUEUE.md + GitHub
    _preserve_crashed_run_commits — push unpushed commits before re-CODING
    _rehydrate_last_push_at      — seed _last_push_at from PR head commit
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from src.branch_context import BranchContext
from src.daemon import git_ops
from src.github import gh_runner
from src.github import prs as gh_prs
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError, parse_task_header
from src.task_status import (
    _resolve_merged_state,
    derive_task_status,
    get_merged_pr_ids,
)

logger = logging.getLogger(__name__)

_PR_NUMBER_RE = re.compile(r"^PR-(\d+)(.*)$")

# PR-266b: feature flags for recover_state source-switch + audit mode.
RECOVERY_HEADERS_ENV = "PIPELINE_RECOVERY_FROM_HEADERS"
RECOVERY_AUDIT_ENV = "PIPELINE_RECOVERY_AUDIT"

RECOVERY_MODE_LEGACY_ONLY = "LEGACY_ONLY"
RECOVERY_MODE_HEADERS_ONLY = "HEADERS_ONLY"
RECOVERY_MODE_AUDIT_LEGACY_APPLIES = "AUDIT_LEGACY_APPLIES"
RECOVERY_MODE_AUDIT_HEADERS_APPLIES = "AUDIT_HEADERS_APPLIES"


def _read_recovery_flag(name: str) -> bool:
    """Parse a 0/1 recovery flag from the environment, defaulting to off.

    Values other than ``"0"``/``"1"`` (or unset) fall back to ``False`` and
    log a warning so a typo in ``docker-compose.yml`` does not silently
    flip the daemon into a different recovery mode.
    """
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return False
    if raw == "1":
        return True
    if raw == "0":
        return False
    logger.warning(
        "%s=%r is not in (0, 1); falling back to default 0", name, raw
    )
    return False


def _resolve_recovery_mode() -> str:
    """Return the recover_state mode for the current process environment."""
    audit = _read_recovery_flag(RECOVERY_AUDIT_ENV)
    headers = _read_recovery_flag(RECOVERY_HEADERS_ENV)
    if audit and headers:
        return RECOVERY_MODE_AUDIT_HEADERS_APPLIES
    if audit:
        return RECOVERY_MODE_AUDIT_LEGACY_APPLIES
    if headers:
        return RECOVERY_MODE_HEADERS_ONLY
    return RECOVERY_MODE_LEGACY_ONLY


def _recovery_audit_diff(
    legacy_tasks: list[QueueTask],
    new_tasks: list[QueueTask],
    prs: list[PRInfo],
    recovered_set: set[str],
) -> dict | None:
    """Compare legacy vs new recovery projections; return diff or ``None``.

    The diff is structured (JSON-loggable) so operators viewing
    ``[AUDIT] recover_state divergence:`` events can filter on specific
    fields. Returns ``None`` on parity to keep the audit log silent
    while the new path matches.
    """
    legacy_proj = _project_recovery_decision(legacy_tasks, prs, recovered_set)
    new_proj = _project_recovery_decision(new_tasks, prs, recovered_set)
    diff: dict = {}
    for field in (
        "pipeline_state",
        "current_task_pr_id",
        "current_pr_number",
        "pending_queue_sync_branch",
    ):
        if legacy_proj[field] != new_proj[field]:
            diff[field] = {"legacy": legacy_proj[field], "new": new_proj[field]}
    if len(legacy_tasks) != len(new_tasks):
        diff["current_queue_length"] = {
            "legacy": len(legacy_tasks),
            "new": len(new_tasks),
        }
    legacy_status = {task.pr_id: task.status.value for task in legacy_tasks}
    new_status = {task.pr_id: task.status.value for task in new_tasks}
    drift = []
    for pr_id in sorted(set(legacy_status) | set(new_status)):
        if legacy_status.get(pr_id) != new_status.get(pr_id):
            drift.append(
                {
                    "pr_id": pr_id,
                    "legacy_status": legacy_status.get(pr_id),
                    "new_status": new_status.get(pr_id),
                }
            )
    if drift:
        diff["current_queue_status_drift"] = drift
    return diff or None


def _project_recovery_decision(
    tasks: list[QueueTask],
    prs: list[PRInfo],
    recovered_set: set[str],
) -> dict:
    """Project the post-recovery state implied by ``tasks`` + ``prs``.

    Pure function — no side effects, no probes. Mirrors the decision
    tree in ``RecoveryMixin._apply_recovery_decisions`` at a level that
    is sufficient for audit diff comparison: the side-effecting
    ``_is_doing_already_merged`` API probe is intentionally excluded so
    legacy and new projections are scored against the same input
    snapshot.
    """
    pending_sync_branch = next(
        (pr.branch for pr in prs if (pr.branch or "").startswith("queue-done-")),
        None,
    )
    doing = next((task for task in tasks if task.status == TaskStatus.DOING), None)
    if doing is not None and doing.pr_id in recovered_set:
        return {
            "pipeline_state": PipelineState.IDLE.value,
            "current_task_pr_id": None,
            "current_pr_number": None,
            "pending_queue_sync_branch": pending_sync_branch,
        }
    if doing is not None:
        matching = (
            next((pr for pr in prs if pr.branch == doing.branch), None)
            if doing.branch
            else None
        )
        if matching is not None:
            return {
                "pipeline_state": PipelineState.WATCH.value,
                "current_task_pr_id": doing.pr_id,
                "current_pr_number": matching.number,
                "pending_queue_sync_branch": pending_sync_branch,
            }
        return {
            "pipeline_state": PipelineState.IDLE.value,
            "current_task_pr_id": None,
            "current_pr_number": None,
            "pending_queue_sync_branch": pending_sync_branch,
        }
    queued_by_branch = {
        task.branch: task
        for task in tasks
        if task.branch and task.status in (TaskStatus.TODO, TaskStatus.DONE)
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
        return {
            "pipeline_state": PipelineState.WATCH.value,
            "current_task_pr_id": matched_task.pr_id,
            "current_pr_number": matched_pr.number,
            "pending_queue_sync_branch": pending_sync_branch,
        }
    return {
        "pipeline_state": PipelineState.IDLE.value,
        "current_task_pr_id": None,
        "current_pr_number": None,
        "pending_queue_sync_branch": pending_sync_branch,
    }


def _task_pr_sort_key(pr_id: str) -> tuple[int, int | str, str]:
    match = _PR_NUMBER_RE.match(pr_id)
    if match is None:
        return (1, pr_id, "")
    return (0, int(match.group(1)), match.group(2))


class RecoveryMixin:
    """State recovery on daemon startup."""

    def _parse_tasks_from_headers(self) -> list[QueueTask] | None:
        """Parse structured task headers into a recovered queue snapshot.

        This is intentionally not wired into ``recover_state`` yet. PR-266a
        adds the behavior-neutral helper and tests; the production source
        switch lands in the next PR behind audit-mode comparison.
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
        recovered_task_pr_ids = set(getattr(self, "_recovered_task_pr_ids", set()))

        tasks: list[QueueTask] = []
        for header in headers:
            status = derive_task_status(
                header,
                merged_state,
                open_prs,
                merged_prs,
                current_task_pr_id=current_task_pr_id,
            )
            if header.pr_id in recovered_task_pr_ids and status != TaskStatus.DONE:
                status = TaskStatus.CANCELED
            if (
                header.pr_id in crashed_task_pr_ids
                and status in (TaskStatus.TODO, TaskStatus.CANCELED)
            ):
                status = TaskStatus.CANCELED
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
                    f"[INFRA] recover_state: ignoring ghost QUEUE.md "
                    f"entry {queued.pr_id} (no {queued.task_file} on "
                    f"disk)."
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
        """Reconstruct state from QUEUE.md / task headers + GitHub.

        Decision tree (shared by both legacy and headers paths):

        1. If a DOING task is present:
           - Matching open PR on that branch -> WATCH (runner resumes
             polling the existing PR).
           - No matching PR -> mark crashed/CANCELED and stay IDLE.
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

        PR-266b: which queue source applies state is controlled by two
        env flags read at process start. The default is ``LEGACY_ONLY``,
        byte-identical to pre-PR-266 behavior. Audit modes run both
        paths in parallel and emit ``[AUDIT] recover_state divergence:``
        events when their projections differ.
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
        # PR-247 follow-up: hydrate the operator-recovered task set from
        # Redis before any DOING-task decision. The HUNG recover button
        # writes through ``_persist_recovered_task_pr_ids``; without this
        # load the marker is process-local and a daemon restart between
        # the click and the user's task re-upload would lose it,
        # collapsing the stronger "abandon until re-upload" override
        # back into the PR-186 crashed-task path which intentionally
        # discards on a still-open PR re-deriving DOING.
        await self._load_recovered_task_pr_ids()

        mode = _resolve_recovery_mode()

        if mode == RECOVERY_MODE_LEGACY_ONLY:
            ok, _, _ = await self._recover_state_legacy()
            return ok

        if mode == RECOVERY_MODE_HEADERS_ONLY:
            ok, _, _ = await self._recover_state_headers()
            return ok

        if mode == RECOVERY_MODE_AUDIT_LEGACY_APPLIES:
            ok, applied_tasks, prs = await self._recover_state_legacy()
            if ok:
                self._emit_audit_diff(
                    mode,
                    applied_path="legacy",
                    applied_tasks=applied_tasks,
                    prs=prs,
                )
            return ok

        # RECOVERY_MODE_AUDIT_HEADERS_APPLIES
        ok, applied_tasks, prs = await self._recover_state_headers()
        if ok:
            self._emit_audit_diff(
                mode,
                applied_path="new",
                applied_tasks=applied_tasks,
                prs=prs,
            )
        return ok

    async def _recover_state_legacy(
        self,
    ) -> tuple[bool, list[QueueTask], list[PRInfo]]:
        """Apply state using the legacy ``_parse_base_queue`` path.

        Returns ``(success, tasks, prs)``: the tasks list is the parsed
        legacy queue (after ghost filtering), and ``prs`` is the live
        ``get_open_prs`` snapshot used to apply state. Both are exposed
        so the audit-diff comparator can score the projection without
        depending on ``self._idle_open_prs``, which the legacy path does
        not populate.
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
            await self._transition_to_error(
                (
                    "recover_state: tasks/QUEUE.md tracking probe failed; "
                    "retrying next cycle"
                ),
                save_run_record_as=None,
                publish=False,
                log_prefix="[INFRA]",
            )
            return False, [], []
        try:
            tasks = self._parse_base_queue(
                strict=strict, queue_from_origin=queue_from_origin,
            )
        except QueueValidationError as exc:
            await self._transition_to_error(
                f"recover_state: queue validation failed: {exc}",
                save_run_record_as=None,
                publish=False,
                log_prefix="[INFRA]",
            )
            return False, [], []
        if tasks is None:
            if queue_from_origin:
                await self._transition_to_error(
                    "recover_state: read QUEUE.md from origin failed",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[INFRA]",
                )
                return False, [], []
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
                "[INFRA] recover_state: tasks/QUEUE.md absent in working "
                "tree; treating as empty queue and deferring to "
                "preflight + IDLE regeneration."
            )
            tasks = []

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
            return False, [], []
        # Ghost filtering uses local task-file existence, which is only a
        # safe signal for post-PR-181 repos (QUEUE.md gitignored, parsed
        # from the working tree). On legacy tracked-QUEUE repos the queue
        # we just parsed came from ``origin/{branch}``; the local checkout
        # may legitimately be parked on a feature branch whose tree lacks
        # task files referenced there, and dropping those entries would
        # detach recovery from active DOING/DONE work.
        if not queue_from_origin:
            tasks = self._drop_ghost_queue_entries(tasks)
        await self._apply_recovery_decisions(tasks, prs)
        return True, tasks, list(prs)

    async def _recover_state_headers(
        self,
    ) -> tuple[bool, list[QueueTask], list[PRInfo]]:
        """Apply state using the headers-derived task list (PR-266b).

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
            return False, [], []
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
                return False, [], []
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
        return True, tasks, list(prs)

    async def _apply_recovery_decisions(
        self, tasks: list[QueueTask], prs: list[PRInfo]
    ) -> None:
        """Apply the shared post-parse decision tree to runner state.

        Both LEGACY_ONLY and HEADERS_ONLY modes converge here once they
        have produced a task list and an open-PR list. The body mirrors
        the original recover_state decision tree so legacy behavior is
        preserved exactly.
        """
        self._set_queue_progress(
            sum(1 for t in tasks if t.status == TaskStatus.DONE),
            len(tasks),
        )

        # PR-186 Codex P1: rehydrate the crashed-task set from any CANCELED
        # entries in the parsed queue. The crashed-task cancellation is what
        # tells ``_select_next_task_from_dag`` to skip the task on the next
        # IDLE cycle; without this rehydrate, a daemon restart after one
        # IDLE cycle has already written CANCELED to QUEUE.md would start
        # with an empty set, ``recover_state`` would see no DOING entry to
        # re-mark, and the selector would recompute the task as TODO and
        # dispatch it again — defeating the "manual re-upload required"
        # contract and reintroducing the crash loop. The set is cleared
        # only when the user re-uploads the task file (see ``repo_ops``).
        for queued in tasks:
            if queued.status == TaskStatus.CANCELED:
                self._crashed_task_pr_ids.add(queued.pr_id)

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

        # PR-247 follow-up: a DOING entry whose PR-ID is in the operator-
        # recovered set is one the operator already abandoned via the
        # HUNG recover button. The IDLE cycle that would have rewritten
        # the QUEUE.md row to CANCELED never ran (or its snapshot was
        # not yet visible), so the row still reads DOING. Treat it the
        # same as no DOING entry — staying IDLE — so neither the WATCH
        # re-attach nor the PR-186 crashed-task path runs against the
        # abandoned task. The IDLE selector's stricter override will
        # surface CANCELED on the next cycle.
        if doing is not None and doing.pr_id in self._recovered_task_pr_ids:
            ctx = BranchContext.from_runner(self)
            self.log_event(
                f"[INFRA] Operator-recovered task {doing.pr_id} still DOING "
                f"in queue; staying IDLE pending re-upload. "
                f"({ctx.log_summary()})"
            )
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.state.state = PipelineState.IDLE
            return

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
            # the work is not lost, then mark the task crashed/CANCELED so
            # the next IDLE cycle skips it. The user re-uploads the task
            # file to retry.
            if doing.branch and not self._preserve_crashed_run_commits(
                doing.branch
            ):
                await self._transition_to_error(
                    (
                        f"recover_state: could not preserve crashed-run "
                        f"commits on {doing.branch!r}; refusing to mark "
                        "CANCELED"
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
            self._crashed_task_pr_ids.add(doing.pr_id)
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.state.state = PipelineState.IDLE
            self.log_event(
                f"[INFRA] Task {doing.pr_id} crashed, marking CANCELED. "
                f"Manually re-upload to retry. "
                f"({ctx.log_summary()})"
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

    def _emit_audit_diff(
        self,
        mode: str,
        *,
        applied_path: str,
        applied_tasks: list[QueueTask],
        prs: list[PRInfo],
    ) -> None:
        """Run the inactive recovery path as a dry-run and emit any diff.

        Side-effect free for the inactive path: the dry-run helper just
        produces a task list, which feeds ``_recovery_audit_diff``
        alongside the applied path's task list. Differences are logged
        as ``[AUDIT] recover_state divergence: <json>``; parity is
        silent so audit logs stay grep-friendly.

        ``prs`` is the live ``get_open_prs`` snapshot the applied path
        used. It is threaded in explicitly because ``self._idle_open_prs``
        is unreliable here: the legacy path never sets it, and the
        headers path restores it before returning. Reading the attribute
        directly would feed ``_recovery_audit_diff`` an empty or stale
        PR list and fabricate divergences whenever recoverability
        depends on open PR branches.
        """
        if applied_path == "legacy":
            # ``_parse_tasks_from_headers`` consults ``_idle_open_prs``
            # to derive each task's status. Seed it from the live PR
            # snapshot and restore on exit so the dry-run sees the same
            # PR set the applied path used.
            prior_open_prs = getattr(self, "_idle_open_prs", None)
            prior_merged_prs = getattr(self, "_idle_merged_prs", None)
            self._idle_open_prs = list(prs)
            self._idle_merged_prs = []
            try:
                try:
                    new_tasks = self._parse_tasks_from_headers() or []
                except Exception as exc:
                    self.log_event(
                        f"[AUDIT] recover_state new-path dry-run failed: {exc}"
                    )
                    return
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
            legacy_tasks = applied_tasks
        else:
            strict = self.app_config.daemon.strict_queue_validation
            # Mirror ``_recover_state_legacy``'s queue-source decision
            # exactly so the dry-run scores against the same input the
            # legacy applied path would have used. Probing once and
            # threading the result into ``_parse_base_queue`` keeps the
            # parse source (origin/{branch} vs working tree) and the
            # downstream ghost-filter decision in lockstep, the same
            # invariant the legacy applied path enforces.
            queue_from_origin = self._origin_queue_md_tracked()
            if queue_from_origin is None:
                # Indeterminate probe: legacy applies would have
                # transitioned to ERROR, so any comparison here would
                # measure a snapshot legacy never actually used. Skip
                # silently rather than fabricate divergences.
                self.log_event(
                    "[AUDIT] recover_state legacy-path dry-run skipped: "
                    "tasks/QUEUE.md tracking probe failed"
                )
                return
            try:
                parsed = self._parse_base_queue(
                    strict=strict, queue_from_origin=queue_from_origin,
                )
            except Exception as exc:
                self.log_event(
                    f"[AUDIT] recover_state legacy-path dry-run failed: {exc}"
                )
                return
            legacy_tasks = parsed or []
            if not queue_from_origin:
                # Mirror ``_drop_ghost_queue_entries`` inline so a stale
                # ``tasks/QUEUE.md`` ghost row (post-PR-181 repos
                # gitignore the file; ``sync_to_main`` does not wipe
                # it) does not fabricate ``[AUDIT] recover_state
                # divergence`` events that legacy recovery would have
                # filtered before any decision. Inlining avoids the
                # helper's ``[INFRA] recover_state: ignoring ghost``
                # log emission, which would lie about an "actual
                # recovery" decision in modes where the legacy path is
                # only a dry-run.
                repo_root = Path(self.repo_path)
                legacy_tasks = [
                    task
                    for task in legacy_tasks
                    if task.task_file is None
                    or (repo_root / task.task_file).is_file()
                ]
            new_tasks = applied_tasks

        diff = _recovery_audit_diff(
            legacy_tasks,
            new_tasks,
            prs,
            set(self._recovered_task_pr_ids),
        )
        if diff is None:
            return
        payload = {
            "audit": "recover_state",
            "mode": mode,
            "diff": diff,
        }
        self.log_event(
            f"[AUDIT] recover_state divergence: {json.dumps(payload, sort_keys=True)}"
        )

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
