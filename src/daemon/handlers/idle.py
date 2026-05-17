"""IDLE state handler and PAUSED state handler.

Mixin methods:
    handle_idle   — pick next task and transition to CODING
    handle_paused — wait for rate-limit window, then resume
"""

from __future__ import annotations

import asyncio
import re
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src.daemon.backups import create_repo_bundle, prune_old_bundles
from src.daemon.main_commit_audit import (
    audit_main_commit_shas,
    list_recent_main_commit_shas,
    load_audited_shas_from_redis,
    mark_shas_audited_in_redis,
    record_audit_findings_in_redis,
)
from src.daemon.selector import SelectionContext, candidate_coders
from src.dag import get_eligible_tasks
from src.github import prs as gh_prs
from src.inhibitor import InhibitorType, WorkInhibitor, is_work_inhibited
from src.models import PipelineState, QueueTask, TaskStatus
from src.onboarding.markdown_sections import MarkerError
from src.onboarding.reconciliation import reconcile_agents_md
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    parse_task_header,
)
from src.task_status import (
    _resolve_merged_state,
    derive_task_status,
    find_matching_open_pr,
)

# Number of consecutive HTTP 304 cycles on get_merged_prs before the IDLE
# handler emits a degraded-detection event, plus the cadence at which the
# event is re-emitted while the failure persists. Sized to swallow brief
# edge-cache stalemates while still surfacing genuine multi-minute outages.
_IDLE_MERGED_PR_304_WARN_AT = 10
_IDLE_MERGED_PR_304_WARN_EVERY = 50


class IdleMixin:
    """Handle IDLE state: sync, pick next task, dispatch to CODING."""

    def _preserve_fix_iteration_count(self, pr):
        """Carry the iteration counters forward when reattaching the same PR."""
        current_pr = self.state.current_pr
        if current_pr is None or current_pr.number != pr.number:
            return pr
        merged_shas, merged_push_count = current_pr.merge_observed_pushes(pr)
        return pr.model_copy(
            update={
                "fix_iteration_count": current_pr.fix_iteration_count,
                "no_push_fix_count": current_pr.no_push_fix_count,
                "observed_head_shas": merged_shas,
                "push_count": merged_push_count,
            }
        )

    async def _resolve_rate_limit_error_state(
        self,
        *,
        log_prefix: str,
        label: str,
    ) -> bool:
        """Process ``state.error_message`` during rate-limit recovery."""
        if self.state.error_message is None:
            return False
        lowered = self.state.error_message.lower()
        is_rate_limit_msg = (
            "rate limit" in lowered or re.search(r"\b429\b", lowered)
        )
        if is_rate_limit_msg:
            await self._clear_error_message_on_recovery(
                log_prefix=log_prefix,
                reason=f"{label}, cleared legacy rate-limit error",
            )
            return False
        await self._transition_to_error(
            self.state.error_message,
            save_run_record_as=None,
            publish=False,
            log_prefix=f"{log_prefix} {label} -> ERROR (preserved context):",
        )
        return True

    def _scan_task_specs_for_agents_md_drift(self) -> None:
        """Run the AGENTS.md anti-pattern scan over ``tasks/PR-*.md``.

        PR-260: hooks ``reconcile_agents_md`` into the IDLE cycle in
        dry-run mode so the per-spec scan in
        ``_scan_existing_task_specs`` actually reaches production.
        Dry-run keeps the working tree clean (AGENTS.md is tracked in
        target repos and operator-driven via ``/onboarding/apply``);
        the scan still fires because the hook is gated only on
        ``log_event_fn``.

        Output is buffered and fingerprinted across cycles before
        reaching ``self.log_event``. ``log_event``'s built-in dedup
        only collapses *consecutive* identical entries, but other
        ``[INFRA]`` events fire between scans on every cycle, so
        identical drift findings would otherwise re-emit each pass and
        push newer operational signals out of the 100-entry history
        cap. Caching the fingerprint and flushing only when it changes
        keeps the operator-visible warning fresh on first appearance
        and on every change of state, while staying silent when
        nothing moved.

        Failures in this scan must never block dispatch: a malformed
        AGENTS.md (operator-introduced bad managed markers) raises
        ``MarkerError`` from ``apply_managed_regions``; ``OSError``
        covers transient filesystem hiccups; ``UnicodeError`` covers
        AGENTS.md or a ``tasks/PR-*.md`` containing non-UTF-8 bytes
        (``Path.read_text`` decodes with the platform default and
        raises ``UnicodeDecodeError`` on bad encodings). All three are
        surfaced as a single ``[AGENTS-SCAN]`` event and swallowed so
        the rest of IDLE proceeds normally; they participate in the
        same fingerprint so a stuck error does not repeat each cycle.
        """
        agents_path = Path(self.repo_path) / "AGENTS.md"
        pending: list[str] = []
        try:
            reconcile_agents_md(
                agents_path,
                dry_run=True,
                log_event_fn=pending.append,
            )
        except MarkerError as exc:
            pending.append(
                f"[AGENTS-SCAN] Skipping drift scan: malformed managed "
                f"markers in {agents_path}: {exc}."
            )
        except OSError as exc:
            pending.append(
                f"[AGENTS-SCAN] Skipping drift scan: failed to read "
                f"{agents_path}: {exc}."
            )
        except UnicodeError as exc:
            pending.append(
                f"[AGENTS-SCAN] Skipping drift scan: non-UTF-8 content "
                f"in {agents_path} or {Path(self.repo_path) / 'tasks'}: "
                f"{exc}."
            )

        fingerprint = tuple(pending)
        last_fingerprint = getattr(
            self, "_last_agents_scan_fingerprint", None,
        )
        if fingerprint == last_fingerprint:
            return
        self._last_agents_scan_fingerprint = fingerprint
        for event in pending:
            self.log_event(event)

    async def _audit_main_commits_if_due(self) -> None:
        self._main_commit_audit_counter = (
            getattr(self, "_main_commit_audit_counter", 0) + 1
        )
        audit_interval = (
            self.app_config.daemon.main_commit_audit_interval_idle_cycles
        )
        if self._main_commit_audit_counter < audit_interval:
            return

        repo_key = self.name
        lookback_n = self.app_config.daemon.main_commit_audit_lookback_n
        audited_shas = await load_audited_shas_from_redis(
            self.redis,
            repo_key,
            self.repo_config.branch,
        )
        try:
            recent_shas = await asyncio.to_thread(
                list_recent_main_commit_shas,
                self.owner_repo,
                lookback_n,
                self.repo_config.branch,
            )
            findings, checked_shas = await asyncio.to_thread(
                audit_main_commit_shas,
                self.owner_repo,
                recent_shas,
                audited_shas,
                self.repo_config.branch,
            )
        except Exception as exc:
            self.log_event(
                f"[AUDIT] [MAIN-COMMIT-AUDIT] Skipping audit for "
                f"{self.owner_repo}: {exc}"
            )
            if getattr(self, "_main_commit_audit_retry_pending", False):
                self._main_commit_audit_retry_pending = False
                self._main_commit_audit_counter = 0
            else:
                self._main_commit_audit_retry_pending = True
                self._main_commit_audit_counter = max(audit_interval - 1, 0)
            return

        for finding in findings:
            self.log_event(
                f"[AUDIT] [MAIN-COMMIT-AUDIT] VIOLATION "
                f"{finding.violation_category}: {finding.short_sha} "
                f'"{finding.message_first_line}"'
            )
        await record_audit_findings_in_redis(
            self.redis,
            repo_key,
            findings,
            self.repo_config.branch,
        )
        await mark_shas_audited_in_redis(
            self.redis,
            repo_key,
            checked_shas,
            self.repo_config.branch,
        )
        self._main_commit_audit_retry_pending = False
        self._main_commit_audit_counter = 0

    async def _run_git_bundle_backup_if_due(self) -> None:
        config = self.app_config.daemon
        if not (config.git_bundle_backup_enabled and config.git_bundle_backup_dir):
            return
        # Track the last successful run on a monotonic clock so the
        # cadence stays anchored to wall-clock seconds. A cycle counter
        # cannot work here because the runner's IDLE cadence
        # (``effective_idle_poll_interval``) changes mid-run when the
        # adaptive extended-idle threshold fires or the GitHub rate-limit
        # slowdown engages. Comparing elapsed seconds keeps the
        # configured ``git_bundle_backup_interval_hours`` honored across
        # those transitions.
        now = time.monotonic()
        last_run = getattr(self, "_git_bundle_backup_last_run_at", None)
        target_seconds = config.git_bundle_backup_interval_hours * 3600
        if last_run is None:
            # First IDLE cycle after runner start: anchor the clock so
            # the first backup fires after the configured interval
            # rather than immediately.
            self._git_bundle_backup_last_run_at = now
            return
        if now - last_run < target_seconds:
            return
        try:
            bundle_path = await create_repo_bundle(
                repo_path=self.repo_path,
                repo_name=self.name,
                backup_dir=config.git_bundle_backup_dir,
            )
        except Exception as exc:
            # Leave ``last_run`` untouched so the next IDLE cycle retries
            # instead of waiting another full interval.
            self.log_event(f"[BACKUP] bundle creation crashed: {exc}")
            return
        if bundle_path is None:
            self.log_event(
                "[BACKUP] git bundle failed; will retry next cycle"
            )
            return
        self._git_bundle_backup_last_run_at = now
        self.log_event(f"[BACKUP] git bundle created: {bundle_path.name}")
        try:
            removed = await prune_old_bundles(
                backup_dir=config.git_bundle_backup_dir,
                repo_name=self.name,
                daily_retention=config.git_bundle_backup_daily_retention,
                weekly_retention=config.git_bundle_backup_weekly_retention,
            )
        except Exception as exc:
            self.log_event(f"[BACKUP] prune failed: {exc}")
            return
        if removed > 0:
            self.log_event(f"[BACKUP] pruned {removed} old bundles")

    @staticmethod
    def _validate_task_file_header_match(task_file: Path, header_pr_id: str) -> None:
        expected_pr_id = task_file.stem
        if header_pr_id != expected_pr_id:
            raise QueueValidationError(
                [
                    f"{task_file}: header PR ID {header_pr_id!r} "
                    f"does not match task file {expected_pr_id!r}"
                ]
            )

    @staticmethod
    def _is_missing_task_header_error(exc: QueueValidationError) -> bool:
        return bool(exc.issues) and all(
            "missing task header like '# PR-123: Title'" in issue
            for issue in exc.issues
        )

    @staticmethod
    def _is_legacy_unstructured_task_error(exc: QueueValidationError) -> bool:
        allowed_suffixes = {
            ": missing Branch",
            ": missing Type",
            ": missing Complexity",
            ": missing Depends on",
        }
        return bool(exc.issues) and all(
            any(issue.endswith(suffix) for suffix in allowed_suffixes)
            for issue in exc.issues
        )

    @staticmethod
    def _filter_dag_headers_with_available_dependencies(
        headers: list,
        skipped_legacy_pr_ids: set[str],
        task_dir: Path,
        merged_pr_ids: set[str],
    ) -> tuple[list, dict[str, list[str]]]:
        unresolved_deps_map: dict[str, list[str]] = {}
        structured_pr_ids = {header.pr_id for header in headers}

        changed = True
        while changed:
            changed = False
            for header in headers:
                unresolved_deps = set(unresolved_deps_map.get(header.pr_id, ()))
                for dependency in header.depends_on:
                    if dependency in merged_pr_ids:
                        continue
                    if dependency in skipped_legacy_pr_ids:
                        unresolved_deps.add(dependency)
                        continue
                    if dependency not in structured_pr_ids:
                        if not (task_dir / f"{dependency}.md").exists():
                            unresolved_deps.add(dependency)
                        continue
                    unresolved_deps.update(unresolved_deps_map.get(dependency, ()))
                next_unresolved = sorted(unresolved_deps)
                if next_unresolved != unresolved_deps_map.get(header.pr_id, []):
                    unresolved_deps_map[header.pr_id] = next_unresolved
                    changed = True

        return headers, unresolved_deps_map

    async def _select_next_task_from_dag(self) -> QueueTask | None:
        """Pick the next eligible task from structured task headers."""
        self._idle_dag_tasks = None
        self._idle_dag_headers = None
        self._idle_dag_statuses = None
        task_dir = Path(self.repo_path) / "tasks"
        if not task_dir.is_dir():
            return None

        headers = []
        skipped_legacy_pr_ids: set[str] = set()
        task_files: dict[str, str] = {}
        repo_root = Path(self.repo_path)
        for task_file in sorted(task_dir.glob("PR-*.md")):
            try:
                header = parse_task_header(task_file)
            except QueueValidationError as exc:
                if not (
                    self._is_missing_task_header_error(exc)
                    or self._is_legacy_unstructured_task_error(exc)
                ):
                    raise
                skipped_legacy_pr_ids.add(task_file.stem)
                continue
            self._validate_task_file_header_match(task_file, header.pr_id)
            headers.append(header)
            task_files[header.pr_id] = task_file.relative_to(repo_root).as_posix()

        if not headers:
            return None

        state = _resolve_merged_state(
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
        if not state.api_available and not getattr(
            self, "_idle_degraded_done_check_logged", False,
        ):
            self.log_event(
                "[INFRA] Operating without gh API done-check; relying on "
                "git log convention scan only"
            )
            self._idle_degraded_done_check_logged = True
        merged_pr_ids = state.merged_pr_ids
        headers, unresolved_deps_map = self._filter_dag_headers_with_available_dependencies(
            headers,
            skipped_legacy_pr_ids,
            task_dir,
            merged_pr_ids,
        )

        try:
            dag_headers = [
                replace(
                    header,
                    depends_on=[
                        dependency
                        for dependency in header.depends_on
                        if dependency not in merged_pr_ids
                    ],
                )
                for header in headers
            ]
            eligibility_dag_headers = [
                replace(
                    header,
                    depends_on=list(unresolved_deps_map[header.pr_id]),
                )
                if header.pr_id in unresolved_deps_map
                else header
                for header in dag_headers
            ]
            dag_header_ids = {header.pr_id for header in dag_headers}
            synthetic_blocker_headers = [
                TaskHeader(
                    pr_id=dependency,
                    title=f"Missing dependency {dependency}",
                    branch="",
                    task_type="feature",
                    complexity="low",
                    depends_on=[],
                    priority=9999,
                    coder="any",
                )
                for dependency in sorted(
                    {
                        dependency
                        for unresolved_deps in unresolved_deps_map.values()
                        for dependency in unresolved_deps
                        if dependency not in dag_header_ids
                    }
                )
            ]
            eligibility_headers = [*eligibility_dag_headers, *synthetic_blocker_headers]
            merged_pr_ids = {
                pr_id for pr_id in merged_pr_ids if pr_id in {header.pr_id for header in headers}
            }
            open_prs = list(getattr(self, "_idle_open_prs", ()))
            merged_prs = list(getattr(self, "_idle_merged_prs", ()))
            current_task_pr_id = (
                self.state.current_task.pr_id
                if self.state.current_task is not None
                else None
            )
            stopped_task_pr_ids = getattr(self, "_user_stopped_task_pr_ids", set())
            if current_task_pr_id in stopped_task_pr_ids:
                current_task_pr_id = None
            crashed_task_pr_ids = getattr(self, "_crashed_task_pr_ids", set())
            hydrate_status_write_failed = getattr(
                self,
                "_hydrate_status_write_failed_task_pr_ids",
                None,
            )
            if hydrate_status_write_failed is not None:
                await hydrate_status_write_failed()
            status_write_failed_task_pr_ids = getattr(
                self,
                "_status_write_failed_task_pr_ids",
                set(),
            )
            statuses = {
                header.pr_id: derive_task_status(
                    header,
                    state,
                    open_prs,
                    merged_prs,
                    current_task_pr_id=current_task_pr_id,
                )
                for header in headers
            }
            statuses.update(
                {
                    header.pr_id: TaskStatus.ERROR
                    for header in synthetic_blocker_headers
                }
            )
            frontmatter_statuses = {
                header.pr_id: header.frontmatter_status for header in headers
            }
            # PR-186: Recovery marks DOING-without-PR tasks crashed before
            # transitioning to IDLE. Override their derived status to
            # ERROR here so get_eligible_tasks excludes them and the
            # snapshot surfaces the ERROR state to the dashboard.
            # Existing DONE rulings (e.g. the merged PR landed before
            # recovery resumed) win — DONE is terminal, never downgraded
            # to ERROR. A DOING ruling means ``derive_task_status``
            # matched a now-visible open PR (e.g. ``get_open_prs`` was
            # stale on the recovery cycle and the PR surfaced later);
            # preserving DOING lets the runner resume WATCH/merge for
            # that real PR rather than stranding it behind the crashed
            # flag, and clearing the flag ensures the next selector pick
            # treats the task as live again.
            for pr_id in list(statuses.keys()):
                if pr_id not in crashed_task_pr_ids:
                    continue
                if frontmatter_statuses.get(pr_id) == "todo":
                    crashed_task_pr_ids.discard(pr_id)
                    continue
                if statuses[pr_id] in (TaskStatus.DONE, TaskStatus.DOING):
                    crashed_task_pr_ids.discard(pr_id)
                    continue
                statuses[pr_id] = TaskStatus.ERROR
            # If an explicit escalation/cancel path could not write the
            # durable task-file status, keep the task parked in memory
            # until the operator re-uploads it. A still-open PR must not
            # make the task live again.
            for pr_id in list(statuses.keys()):
                if pr_id not in status_write_failed_task_pr_ids:
                    continue
                if statuses[pr_id] == TaskStatus.DONE:
                    status_write_failed_task_pr_ids.discard(pr_id)
                    continue
                statuses[pr_id] = TaskStatus.ERROR
            eligible = [
                header
                for header in get_eligible_tasks(eligibility_headers, statuses)
                if header.pr_id in dag_header_ids
            ]
            stopped_eligible = [
                header
                for header in eligible
                if header.pr_id in stopped_task_pr_ids
            ]
            eligible = [
                header
                for header in eligible
                if header.pr_id not in stopped_task_pr_ids
            ]
        except ValueError as exc:
            raise QueueValidationError([str(exc)]) from exc

        self._idle_dag_headers = list(dag_headers)
        self._idle_dag_statuses = dict(statuses)
        self._idle_dag_tasks = [
            self._queue_task_from_header(
                header,
                statuses[header.pr_id],
                task_files,
                unresolved_deps_map.get(header.pr_id, []),
            )
            for header in dag_headers
        ]
        doing_tasks = [
            task
            for task in self._idle_dag_tasks
            if task.status == TaskStatus.DOING
        ]
        if doing_tasks:
            stopped_task_pr_ids.clear()
            return doing_tasks[0]
        if eligible:
            stopped_task_pr_ids.clear()
            picked = eligible[0]
            return self._queue_task_from_header(
                picked,
                TaskStatus.TODO,
                task_files,
                unresolved_deps_map.get(picked.pr_id, []),
            )
        if stopped_eligible:
            stopped_task_pr_ids.clear()
            picked = stopped_eligible[0]
            return self._queue_task_from_header(
                picked,
                TaskStatus.TODO,
                task_files,
                unresolved_deps_map.get(picked.pr_id, []),
            )
        return None

    def _queue_task_from_header(
        self,
        header,
        status: TaskStatus,
        task_files: dict[str, str],
        unresolved_deps: list[str] | None = None,
    ) -> QueueTask:
        return QueueTask(
            pr_id=header.pr_id,
            title=header.title,
            status=status,
            task_file=task_files[header.pr_id],
            depends_on=list(header.depends_on),
            unresolved_deps=list(unresolved_deps or []),
            branch=header.branch,
        )

    async def _select_next_task_or_attach(
        self,
        prs,
        merged_prs,
    ) -> QueueTask | None:
        """High-stakes IDLE decision: pick next task or reattach.

        Returns the ``QueueTask`` to dispatch into CODING, or ``None``
        when the handler must return without dispatching — either
        nothing is actionable (logging "No tasks available", optionally
        attaching ``current_pr`` to a manual-work open PR) or an
        existing open PR matched the chosen task and the runner has
        already transitioned to WATCH.
        """
        self._idle_open_prs = prs
        self._idle_merged_prs = merged_prs
        try:
            task = await self._select_next_task_from_dag()
        except (
            OSError,
            RuntimeError,
            QueueValidationError,
            subprocess.TimeoutExpired,
        ) as exc:
            await self._transition_to_error(
                f"Task selection failed: {exc}",
                save_run_record_as=None,
                publish=False,
                log_prefix="[INFRA]",
            )
            return None
        finally:
            self._idle_open_prs = []
            self._idle_merged_prs = []

        dag_tasks = list(self._idle_dag_tasks or [])

        # Mark the dispatched task DOING in the snapshot so dashboard
        # consumers see the transition without waiting for the next
        # IDLE cycle.
        statuses_for_dispatch = getattr(self, "_idle_dag_statuses", None)
        if (
            task is not None
            and statuses_for_dispatch is not None
            and statuses_for_dispatch.get(task.pr_id) == TaskStatus.TODO
        ):
            statuses_for_dispatch[task.pr_id] = TaskStatus.DOING
            for i, queued in enumerate(dag_tasks):
                if queued.pr_id == task.pr_id:
                    dag_tasks[i] = queued.model_copy(
                        update={"status": TaskStatus.DOING}
                    )
                    break
            task = task.model_copy(update={"status": TaskStatus.DOING})

        self._set_queue_progress(
            sum(1 for t in dag_tasks if t.status == TaskStatus.DONE),
            len(dag_tasks),
        )
        self.state.current_queue = list(dag_tasks)

        if task is None:
            self.log_event("[INFRA] No tasks available.")
            if prs:
                done_branches = {
                    t.branch for t in dag_tasks
                    if t.status == TaskStatus.DONE and t.branch
                }
                match = next(
                    (pr for pr in prs if pr.branch in done_branches), None
                )
                selected = match or prs[0]
                self.state.current_pr = self._preserve_fix_iteration_count(selected)
                self.log_event(
                    f"[INFRA] IDLE: {len(prs)} open PR(s) detected "
                    f"(manual work)."
                )
            else:
                self.state.current_pr = None
            return None

        self.state.current_task = task
        task_branch = task.branch
        if task_branch:
            existing = find_matching_open_pr(
                task.pr_id,
                task_branch,
                prs,
            )
            if existing is not None:
                self.state.current_pr = self._preserve_fix_iteration_count(existing)
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(self.state.current_pr)
                self.log_event(
                    f"[INFRA] Task {task.pr_id} has existing open PR "
                    f"#{existing.number} on {task_branch!r} -> WATCH "
                    f"(no duplicate CODING)."
                )
                await self.publish_state()
                return None

        return task

    async def handle_idle(self) -> None:
        """Hard-sync to ``origin/{branch}``, pick the next task, hand off."""
        self._error_diagnose_policy.reset(self)
        self._idle_degraded_done_check_logged = False
        # The 304 streak counts only cycles that actually reached
        # ``get_merged_prs`` and saw HTTP 304. Reset by default so any
        # other outcome — success, non-304 failure, or an early return
        # that skips the merged-PR fetch entirely (user_paused,
        # pending_queue_sync still unresolved, ``sync_to_main`` or
        # ``get_open_prs`` failed earlier this cycle) — breaks the
        # streak. Reset first so every early-return path below shares
        # the same semantics; the 304 branch later restores the prior
        # count and increments it.
        prev_merged_pr_304_streak = getattr(
            self, "_idle_merged_pr_304_streak", 0,
        )
        self._idle_merged_pr_304_streak = 0
        # PR-330a: per-repo feature flag selects the unified inhibitor
        # check over the legacy ``user_paused`` branch. The unified
        # gate evaluates the inhibitor list once per coder that
        # dispatch would actually consider — task pin, repo pin, and
        # ``disabled_coders`` narrow the candidate set the same way
        # ``selector.eligible_coders`` does, so a per-coder
        # ``RATE_LIMIT`` on the only runnable coder short-circuits IDLE
        # instead of being masked by a registered-but-unrunnable coder
        # appearing unblocked. ``candidate_coders`` deliberately skips
        # rate-limit/auth probes; per-coder rate-limit semantics flow
        # through ``is_work_inhibited`` below. The flag stays False by
        # default until PR-330d flips production; canary repos opt in
        # earlier via ``feature_flags.use_unified_inhibitor_check``.
        if self.repo_config.feature_flags.use_unified_inhibitor_check:
            ctx = SelectionContext(
                registry=self._registry,
                repo_config=self.repo_config,
                app_config=self.app_config,
                state=self.state,
                rng=self._selector_rng,
                auth_statuses=self._auth_status_cache or None,
                task_coder_pin=self._active_task_coder_pin(),
            )
            candidates = candidate_coders(ctx)
            if not candidates:
                self.log_event(
                    "[INFRA] IDLE inhibited by no eligible coder"
                )
                return
            # ``GITHUB_BUDGET_SLOWDOWN`` is a polling-cadence throttle
            # enforced by ``_check_github_api_budget`` at the runner-cycle
            # layer (skip one-in-N IDLE cycles between the slowdown and
            # pause thresholds); during the cycles that do reach
            # ``handle_idle`` the slowdown must not short-circuit dispatch
            # or the daemon stops working entirely instead of merely
            # polling less often.
            blocking_by_coder: dict[str, list[WorkInhibitor]] = {}
            for name in candidates:
                _, blocking = is_work_inhibited(self.state, coder=name)
                hard_blocking = [
                    inh
                    for inh in blocking
                    if inh.inhibitor_type
                    != InhibitorType.GITHUB_BUDGET_SLOWDOWN
                ]
                if not hard_blocking:
                    blocking_by_coder.clear()
                    break
                blocking_by_coder[name] = hard_blocking
            if blocking_by_coder:
                blocking_types = sorted({
                    inh.inhibitor_type.value
                    for blockers in blocking_by_coder.values()
                    for inh in blockers
                })
                self.log_event(
                    f"[INFRA] IDLE inhibited by {blocking_types}"
                )
                return
        else:
            if self.state.user_paused:
                return
        if self.state.pending_queue_sync_branch is not None:
            if not await self._resolve_pending_queue_sync():
                return

        try:
            self.sync_to_main()
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            RuntimeError,
        ) as exc:
            await self._transition_to_error(
                f"sync_to_main failed: {exc}",
                save_run_record_as=None,
                publish=False,
                log_prefix="[INFRA]",
            )
            return

        upload_result = await self.process_pending_uploads()
        if upload_result is None:
            self._idle_dispatch_deferred = True
            self.log_event(
                "[INFRA] Pending upload failed; skipping task dispatch "
                "to retry next cycle."
            )
            return
        if upload_result:
            try:
                self.sync_to_main()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                RuntimeError,
            ) as exc:
                await self._transition_to_error(
                    f"sync_to_main after upload failed: {exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[INFRA]",
                )
                return

        try:
            prs = gh_prs.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            self.log_event(
                f"[INFRA] IDLE: open PR check failed: {exc}; deferring "
                f"task dispatch."
            )
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self._idle_dispatch_deferred = True
            return
        open_pr_snapshot = tuple(sorted((pr.number, pr.branch) for pr in prs))
        previous_open_pr_snapshot = getattr(self, "_idle_open_pr_snapshot", None)
        refresh_merged_prs = (
            previous_open_pr_snapshot is not None
            and previous_open_pr_snapshot != open_pr_snapshot
        )
        try:
            merged_prs = gh_prs.get_merged_prs(
                self.owner_repo,
                self.repo_config.branch,
                refresh=refresh_merged_prs,
            )
        except Exception as exc:
            if "HTTP 304" in str(exc):
                # Upstream cache miss that slipped past _etag_get's retry.
                # Transient 304s (a stale edge cache for one cycle) are
                # noise; persistent ones mean merged-PR detection is stuck
                # on local heuristics, which miss squash/custom-title
                # merges. Suppress the first few, then surface a degraded
                # signal once the streak shows the failure isn't blowing
                # over.
                streak = prev_merged_pr_304_streak + 1
                self._idle_merged_pr_304_streak = streak
                # Re-emit cadence is measured from the threshold crossing,
                # not from streak=0. Otherwise the first repeat after the
                # initial warning at WARN_AT lands at WARN_EVERY (e.g. 40
                # cycles after WARN_AT=10 with WARN_EVERY=50), undercutting
                # the configured spacing.
                cycles_since_warn = streak - _IDLE_MERGED_PR_304_WARN_AT
                if cycles_since_warn >= 0 and (
                    cycles_since_warn % _IDLE_MERGED_PR_304_WARN_EVERY == 0
                ):
                    self.log_event(
                        f"[INFRA] IDLE: merged PR check returned HTTP 304 "
                        f"for {streak} consecutive cycles; merged-PR "
                        f"detection degraded (squash/custom-title merges "
                        f"may be missed) while falling back to local "
                        f"heuristics."
                    )
                merged_prs = []
            else:
                self.log_event(
                    f"[INFRA] IDLE: merged PR check failed: {exc}; "
                    f"continuing with local merged-status heuristics."
                )
                merged_prs = []
        else:
            self._idle_open_pr_snapshot = open_pr_snapshot

        await self._audit_main_commits_if_due()
        await self._run_git_bundle_backup_if_due()

        task = await self._select_next_task_or_attach(prs, merged_prs)
        if task is None:
            return

        await self._refresh_user_paused_from_redis()
        if self.state.user_paused:
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.log_event(
                f"[INFRA] Pause requested while preparing {task.pr_id}; "
                f"deferring CODING."
            )
            return

        pin = self._active_task_coder_pin()
        if pin in ("claude", "codex"):
            await self._refresh_auth_status_cache()
            if self._select_coder(allow_exploration=False) is None:
                self.state.current_pr = None
                message = (
                    f"Task {task.pr_id} pinned to {pin} but coder unavailable"
                )
                await self._commit_and_park_in_error(
                    message,
                    subsource="infra_failure",
                )
                return

        self.state.state = PipelineState.CODING
        self.log_event(f"[INFRA] Picked task {task.pr_id}: {task.title}.")
        await self.publish_state()
        await self.handle_coding()

    async def handle_paused(self) -> None:
        """Wait for rate limit window to expire, then resume previous flow."""
        # PR-330b: per-repo feature flag selects the unified inhibitor
        # exit check over the legacy ``user_paused``/``rate_limited_until``
        # branches. PAUSED is a global state — any inhibitor (per-coder
        # or global) keeps the repo paused, so the gate calls
        # ``is_work_inhibited`` with ``coder=None`` and treats a clean
        # inhibitor list as the signal to transition back to IDLE.
        # ``GITHUB_BUDGET_SLOWDOWN`` is excluded for the same reason as
        # the IDLE gate: it is a polling-cadence throttle enforced at
        # ``_check_github_api_budget`` and was never part of the legacy
        # PAUSED entry/exit conditions. The flag stays False by default
        # until PR-330d flips production; canary repos opt in earlier
        # via ``feature_flags.use_unified_inhibitor_check``.
        if self.repo_config.feature_flags.use_unified_inhibitor_check:
            _, blocking = is_work_inhibited(self.state, coder=None)
            hard_blocking = [
                inh
                for inh in blocking
                if inh.inhibitor_type
                != InhibitorType.GITHUB_BUDGET_SLOWDOWN
            ]
            if hard_blocking:
                blocking_types = sorted(
                    {inh.inhibitor_type.value for inh in hard_blocking}
                )
                if not getattr(self, "_paused_inhibited_logged", False):
                    self.log_event(
                        f"[INFRA] PAUSED inhibited by {blocking_types}"
                    )
                    self._paused_inhibited_logged = True
                return
            self._paused_inhibited_logged = False
            # Mirror the legacy expired-window resume below: stale
            # rate-limit metadata must be cleared before the IDLE
            # transition, otherwise ``run_cycle`` keeps reading
            # ``rate_limited_until != None`` as a live pause signal
            # (forcing ``ERROR -> PAUSED`` and similar branches) until
            # ``_check_rate_limit`` happens to run. Inhibitors with
            # ``expires_at`` already cleared themselves out of
            # ``derive_active_inhibitors``; the scalar fields persist.
            # The per-coder typed dict and legacy set are cleared for
            # the same reason: ``selector._is_rate_limited`` consults
            # ``rate_limited_coder_until`` first and falls through to
            # ``rate_limited_coders`` on a miss, so a stale entry left
            # in either container would keep ``eligible_coders``
            # returning empty for a repo pinned to that coder (logging
            # ``no eligible coder`` on every IDLE tick) even though
            # the unified gate has already accepted the resume.
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self.state.rate_limit_reactive_coder = None
            self.state.rate_limited_coders.clear()
            self.state.rate_limited_coder_until.clear()
            # Route any lingering ``error_message`` through the
            # rate-limit recovery resolver before the IDLE transition.
            # ``run_cycle`` parks runners in PAUSED when an ERROR cycle
            # finds ``rate_limited_until`` set, preserving the original
            # ``error_message``. When the inhibitors clear, a
            # non-rate-limit message means the underlying fault is
            # still unresolved and the runner must return to ERROR
            # instead of silently dispatching from IDLE — the legacy
            # expired-window path enforces this and the unified path
            # must match.
            self._error_diagnose_policy.reset(self)
            if await self._resolve_rate_limit_error_state(
                log_prefix="[RATE-LIMIT]",
                label="PAUSED inhibitors cleared, resuming",
            ):
                return
            self.log_event("[INFRA] PAUSED inhibitors cleared -> IDLE.")
            self.state.state = PipelineState.IDLE
            return
        if self.state.user_paused:
            if not getattr(self, "_user_pause_logged", False):
                self.log_event("[INFRA] Paused. Press Play to resume.")
                self._user_pause_logged = True
            return
        if self.state.rate_limited_until is None:
            self.log_event(
                "[INFRA] PAUSED without rate_limited_until -> IDLE."
            )
            self.state.state = PipelineState.IDLE
            return
        pause_coder = self.state.rate_limit_reactive_coder or "claude"
        await self._refresh_auth_status_cache()
        selected = self._select_coder()
        coder_name = selected[0] if selected is not None else pause_coder
        diagnosis_pause = (
            self.state.error_message is not None
            and pause_coder == "claude"
        )
        other_coder = (
            not diagnosis_pause
            and pause_coder != coder_name
        )
        clearable = other_coder
        if clearable:
            self._error_diagnose_policy.reset(self)
            self._claude_usage_provider.invalidate_cache()
            self._codex_usage_provider.invalidate_cache()
            label = (
                f"{coder_name.capitalize()} active while {pause_coder} remains "
                f"rate-limited until {self.state.rate_limited_until.isoformat()}"
            )
            if pause_coder not in self.state.rate_limited_coder_until:
                self.state.rate_limited_coders.add(pause_coder)
                self.state.rate_limited_coder_until[pause_coder] = (
                    self.state.rate_limited_until
                )
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self.state.rate_limit_reactive_coder = None
            if await self._resolve_rate_limit_error_state(
                log_prefix="[RATE-LIMIT]", label=label
            ):
                return
            if (
                self.state.current_pr is not None
                and self.state.current_task is not None
                and self.state.current_pr.branch == self.state.current_task.branch
            ):
                self.state.state = PipelineState.WATCH
                self.log_event(f"[RATE-LIMIT] {label} -> WATCH.")
            else:
                self.state.state = PipelineState.IDLE
                self.log_event(f"[RATE-LIMIT] {label} -> IDLE.")
            return
        if datetime.now(timezone.utc) < self.state.rate_limited_until:
            remaining = (
                self.state.rate_limited_until - datetime.now(timezone.utc)
            ).total_seconds()
            self.log_event(
                f"[RATE-LIMIT] Paused, resuming in {int(remaining)}s."
            )
            return
        # Window expired: resume to appropriate state
        self.state.rate_limited_coders.discard(pause_coder)
        self.state.rate_limited_until = None
        self.state.rate_limit_reactive = False
        self.state.rate_limit_reactive_coder = None
        self._error_diagnose_policy.reset(self)
        if await self._resolve_rate_limit_error_state(
            log_prefix="[RATE-LIMIT]", label="Rate limit expired, resuming"
        ):
            return
        if (
            self.state.current_pr is not None
            and self.state.current_task is not None
            and self.state.current_pr.branch == self.state.current_task.branch
        ):
            self.state.state = PipelineState.WATCH
            self.log_event(
                "[RATE-LIMIT] Rate limit expired, resuming -> WATCH."
            )
        else:
            self.state.state = PipelineState.IDLE
            self.log_event(
                "[RATE-LIMIT] Rate limit expired, resuming -> IDLE."
            )
