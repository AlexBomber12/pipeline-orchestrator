"""IDLE state handler and PAUSED state handler.

Mixin methods:
    handle_idle   — pick next task and transition to CODING
    handle_paused — wait for rate-limit window, then resume
"""

from __future__ import annotations

import inspect
import re
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src.dag import get_eligible_tasks
from src.github import prs as gh_prs
from src.models import PipelineState, QueueTask, TaskStatus
from src.onboarding.markdown_sections import MarkerError
from src.onboarding.reconciliation import reconcile_agents_md
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    get_next_task,
    parse_queue,
    parse_task_header,
)
from src.task_status import (
    _resolve_merged_state,
    derive_queue_task_statuses,
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

    @staticmethod
    def _generate_queue_md(
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> str:
        """Render a visually compatible QUEUE.md from structured task headers."""
        lines = ["# Task Queue\n"]
        for header in headers:
            lines.append(f"## {header.pr_id}: {header.title}")
            lines.append(f"- Status: {statuses[header.pr_id].value}")
            lines.append(f"- Tasks file: tasks/{header.pr_id}.md")
            lines.append(f"- Branch: {header.branch}")
            if header.depends_on:
                lines.append(f"- Depends on: {', '.join(header.depends_on)}")
            lines.append("")
        return "\n".join(lines)

    def _write_generated_queue_md(
        self,
        headers: list[TaskHeader],
        statuses: dict[str, TaskStatus],
    ) -> bool:
        """Write the regenerated QUEUE.md to disk for read-side consumers.

        QUEUE.md is gitignored (PR-181); the daemon never commits or
        pushes it. The file is rewritten only when its contents would
        change, so repeated IDLE cycles on a stable queue are no-ops on
        disk.

        On legacy repos that still track ``tasks/QUEUE.md`` upstream
        (``.gitignore`` does not retroactively untrack files), writing
        here would dirty the working tree on every cycle, push preflight
        into ERROR, and block normal dispatch. Skip the write in that
        case — the tracked snapshot on origin remains the source of
        truth until the legacy repo migrates the file out of git.

        Returns ``True`` once the on-disk queue reflects the generated
        content. The boolean return is preserved so existing callers
        treat the operation as always-successful (no push step exists
        to fail).
        """
        queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
        self._idle_generated_queue_needs_resync = False
        # ``None`` means the cat-file probe itself failed (transient git
        # slowness); treat as if tracked so the regenerate is skipped
        # for this cycle. A legacy repo whose probe is briefly flaky
        # would otherwise have its working tree dirtied on every IDLE
        # tick. Post-PR-181 repos lose only one cycle of regeneration
        # and self-heal on the next tick once the probe succeeds.
        tracked = self._origin_queue_md_tracked()
        if tracked is not False:
            if tracked is True and not getattr(
                self, "_legacy_tracked_queue_md_logged", False,
            ):
                self.log_event(
                    f"[INFRA] Skipping QUEUE.md regeneration: still "
                    f"tracked on origin/{self.repo_config.branch}; "
                    f"untrack via 'git rm --cached tasks/QUEUE.md' to "
                    f"enable daemon-side regeneration."
                )
                self._legacy_tracked_queue_md_logged = True
            return True

        content = self._generate_queue_md(headers, statuses)
        existing = (
            queue_path.read_text(encoding="utf-8")
            if queue_path.exists()
            else None
        )
        if existing == content:
            return True

        queue_path.parent.mkdir(parents=True, exist_ok=True)
        queue_path.write_text(content, encoding="utf-8")
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
    def _queue_md_contains_visible_legacy_entries(
        queue_path: str | Path,
        structured_pr_ids: set[str],
        ignored_pr_ids: set[str] | None = None,
    ) -> bool:
        path = Path(queue_path)
        if not path.is_file():
            return False
        skip = structured_pr_ids if ignored_pr_ids is None else (
            structured_pr_ids | ignored_pr_ids
        )
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^##\s+(PR-[A-Za-z0-9_.-]+)\b", raw_line.rstrip())
            if match and match.group(1) not in skip:
                return True
        return False

    @staticmethod
    def _filter_dag_headers_with_available_dependencies(
        headers: list,
        skipped_legacy_pr_ids: set[str],
        task_dir: Path,
        merged_pr_ids: set[str],
    ) -> list:
        blocked_pr_ids: set[str] = set()
        structured_pr_ids = {header.pr_id for header in headers}

        changed = True
        while changed:
            changed = False
            available_pr_ids = structured_pr_ids - blocked_pr_ids
            for header in headers:
                if header.pr_id in blocked_pr_ids:
                    continue
                for dependency in header.depends_on:
                    if dependency in available_pr_ids:
                        continue
                    if dependency in merged_pr_ids:
                        continue
                    if dependency in blocked_pr_ids:
                        blocked_pr_ids.add(header.pr_id)
                        changed = True
                        break
                    if dependency in skipped_legacy_pr_ids:
                        blocked_pr_ids.add(header.pr_id)
                        changed = True
                        break
                    if not (task_dir / f"{dependency}.md").exists():
                        blocked_pr_ids.add(header.pr_id)
                        changed = True
                        break

        return [header for header in headers if header.pr_id not in blocked_pr_ids]

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
        headers = self._filter_dag_headers_with_available_dependencies(
            headers,
            skipped_legacy_pr_ids,
            task_dir,
            merged_pr_ids,
        )
        if not headers:
            return None

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
            # PR-186: Recovery marks DOING-without-PR tasks crashed before
            # transitioning to IDLE. Override their derived status to
            # CANCELED here so get_eligible_tasks excludes them and the
            # regenerated QUEUE.md surfaces the CANCELED state to the
            # dashboard. Existing DONE rulings (e.g. the merged PR landed
            # before recovery resumed) win — DONE is terminal, never
            # downgraded to CANCELED. A DOING ruling means
            # ``derive_task_status`` matched a now-visible open PR (e.g.
            # ``get_open_prs`` was stale on the recovery cycle and the PR
            # surfaced later); preserving DOING lets the runner resume
            # WATCH/merge for that real PR rather than stranding it
            # behind the crashed flag, and clearing the flag ensures the
            # next selector pick treats the task as live again.
            for pr_id in list(statuses.keys()):
                if pr_id not in crashed_task_pr_ids:
                    continue
                if statuses[pr_id] in (TaskStatus.DONE, TaskStatus.DOING):
                    crashed_task_pr_ids.discard(pr_id)
                    continue
                statuses[pr_id] = TaskStatus.CANCELED
            # PR-247 follow-up: Operator-initiated HUNG recovery records
            # the trapped task in ``_recovered_task_pr_ids``. Force its
            # status to CANCELED unconditionally (except DONE — a real
            # merge wins) so the still-open PR cannot re-derive DOING and
            # bounce the runner back into WATCH on the same stuck work
            # item. This is intentionally stronger than the PR-186
            # crashed-task override above: there the open PR may be a
            # stale-API artifact worth honoring; here it is the trapped
            # PR the operator just abandoned.
            recovered_task_pr_ids = getattr(
                self, "_recovered_task_pr_ids", set()
            )
            for pr_id in list(statuses.keys()):
                if pr_id not in recovered_task_pr_ids:
                    continue
                if statuses[pr_id] == TaskStatus.DONE:
                    recovered_task_pr_ids.discard(pr_id)
                    continue
                statuses[pr_id] = TaskStatus.CANCELED
            eligible = get_eligible_tasks(dag_headers, statuses)
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
            self._queue_task_from_header(header, statuses[header.pr_id], task_files)
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
            return self._queue_task_from_header(picked, TaskStatus.TODO, task_files)
        if stopped_eligible:
            stopped_task_pr_ids.clear()
            picked = stopped_eligible[0]
            return self._queue_task_from_header(picked, TaskStatus.TODO, task_files)
        return None

    def _queue_task_from_header(
        self,
        header,
        status: TaskStatus,
        task_files: dict[str, str],
    ) -> QueueTask:
        return QueueTask(
            pr_id=header.pr_id,
            title=header.title,
            status=status,
            task_file=task_files[header.pr_id],
            depends_on=list(header.depends_on),
            branch=header.branch,
        )

    def _check_legacy_queue_status(
        self,
        ghost_legacy_pr_ids: set[str] | None = None,
        parse_error: str | None = None,
    ) -> tuple[bool, str | None]:
        """Return ``(has_visible_legacy_entries, parse_error_or_None)``.

        Single source of truth for the legacy-queue gating decision.
        Three scenarios:

        - ``(True, None)``: visible legacy rows present (``## PR-*``
          headers whose PR ID is not in ``self._idle_dag_tasks`` and
          not in the optional ghost set).
        - ``(False, error_text)``: caller flagged a ``parse_queue``
          failure and no visible legacy entries are present
          (``parse_error`` echoed back).
        - ``(False, None)``: clean — no legacy rows, no parse failure.

        Visible-legacy precedence: when both ``parse_error`` is set
        and visible legacy is detected, returns ``(True, None)``. The
        regex scan does not depend on parse success and is the
        dominant gating signal for ``_write_generated_queue_md``.

        The caller (``_select_next_task_or_attach``) supplies
        ``parse_error`` from its own ``parse_queue`` exception so the
        helper does not re-parse the queue file each call.
        """
        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        structured_pr_ids = {
            queued.pr_id for queued in self._idle_dag_tasks or []
        }
        visible = self._queue_md_contains_visible_legacy_entries(
            queue_path, structured_pr_ids, ghost_legacy_pr_ids,
        )
        if visible:
            return True, None
        return False, parse_error

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

        Encapsulates the dispatch decision tree previously inline in
        ``handle_idle``: DAG selection, legacy queue parse + derive +
        ghost detection + ``get_next_task``, DAG-vs-legacy combine,
        DOING marking, queue progress, generated QUEUE.md write, and
        existing-PR reattach.
        """
        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        strict = self.app_config.daemon.strict_queue_validation
        dag_task = None
        dag_tasks = None
        self._idle_open_prs = prs
        self._idle_merged_prs = merged_prs
        try:
            dag_task = await self._select_next_task_from_dag()
            dag_tasks = self._idle_dag_tasks
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

        tasks: list[QueueTask] = []
        queue_task = None
        structured_pr_ids = {queued.pr_id for queued in dag_tasks or []}
        has_legacy_queue_tasks = False
        legacy_queue_check_succeeded = False
        try:
            tasks = parse_queue(queue_path, strict=strict)
        except QueueValidationError as exc:
            if dag_tasks is None:
                await self._transition_to_error(
                    str(exc),
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[INFRA] Queue validation failed:",
                )
                return None
            self.log_event(
                f"[INFRA] Queue validation failed after DAG selection; "
                f"continuing with DAG task: {exc}."
            )
            visible, _ = self._check_legacy_queue_status(parse_error=str(exc))
            legacy_queue_check_succeeded = not visible
        else:
            try:
                derive_args = (
                    tasks,
                    self.repo_path,
                    self.repo_config.branch,
                )
                derive_sig_params = inspect.signature(
                    derive_queue_task_statuses
                ).parameters
                if "owner_repo" in derive_sig_params:
                    derive_args = (*derive_args, self.owner_repo)
                derive_args = (*derive_args, prs)
                derive_kwargs: dict[str, object] = {}
                if "log_event" in derive_sig_params:
                    derive_kwargs["log_event"] = self.log_event
                if "current_task_pr_id" in derive_sig_params:
                    derive_kwargs["current_task_pr_id"] = (
                        self.state.current_task.pr_id
                        if self.state.current_task is not None
                        else None
                    )
                if "merged_prs" in derive_sig_params:
                    tasks = derive_queue_task_statuses(
                        *derive_args,
                        merged_prs,
                        **derive_kwargs,
                    )
                else:
                    tasks = derive_queue_task_statuses(*derive_args)
            except (
                OSError,
                RuntimeError,
                QueueValidationError,
                subprocess.TimeoutExpired,
            ) as exc:
                if dag_tasks is None:
                    await self._transition_to_error(
                        f"Task status derivation failed: {exc}",
                        save_run_record_as=None,
                        publish=False,
                        log_prefix="[INFRA]",
                    )
                    return None
                self.log_event(
                    f"[INFRA] Task status derivation failed after DAG "
                    f"selection; continuing with DAG tasks: {exc}."
                )
                tasks = []
                queue_task = None
                visible, _ = self._check_legacy_queue_status()
                legacy_queue_check_succeeded = not visible
            else:
                # Ghost entries (whose declared task file is missing on disk)
                # are not real legacy tasks: they're stale residue from a
                # prior cycle that survived ``sync_to_main`` because QUEUE.md
                # is gitignored (PR-181). Treating them as legacy would
                # block ``_write_generated_queue_md`` from rewriting the
                # file, leaving the shim's ``parse_doing_task`` stuck on a
                # stale DOING entry and creating a PR for the wrong branch.
                # Drop ghosts before ``get_next_task`` so the selector
                # advances to a real legacy entry behind a stale DOING ghost
                # instead of stalling IDLE on "no tasks available".
                ghost_legacy_pr_ids = {
                    queued.pr_id
                    for queued in tasks
                    if queued.pr_id not in structured_pr_ids
                    and queued.task_file is not None
                    and not (Path(self.repo_path) / queued.task_file).is_file()
                }
                for queued in tasks:
                    if queued.pr_id in ghost_legacy_pr_ids:
                        self.log_event(
                            f"[INFRA] Ignoring ghost legacy QUEUE.md "
                            f"entry {queued.pr_id} (no "
                            f"{queued.task_file} on disk)."
                        )
                non_ghost_tasks = (
                    [t for t in tasks if t.pr_id not in ghost_legacy_pr_ids]
                    if ghost_legacy_pr_ids
                    else tasks
                )
                queue_task = get_next_task(non_ghost_tasks)
                visible, _ = self._check_legacy_queue_status(
                    ghost_legacy_pr_ids,
                )
                has_legacy_queue_tasks = any(
                    queued.pr_id not in structured_pr_ids
                    for queued in non_ghost_tasks
                ) or visible
                legacy_queue_check_succeeded = not visible

        task = dag_task
        if queue_task is not None:
            queue_task_is_legacy = queue_task.pr_id not in structured_pr_ids
            if task is None and (dag_tasks is None or queue_task_is_legacy):
                task = queue_task
            elif (
                task is not None
                and task.status != TaskStatus.DOING
                and queue_task_is_legacy
            ):
                task = queue_task

        # The shim parses QUEUE.md to find its DOING task and exits 0 if it
        # sees only TODO. Mark the dispatched structured task DOING in the
        # statuses dict before _write_generated_queue_md runs.
        statuses_for_dispatch = getattr(self, "_idle_dag_statuses", None)
        if (
            task is not None
            and statuses_for_dispatch is not None
            and statuses_for_dispatch.get(task.pr_id) == TaskStatus.TODO
        ):
            statuses_for_dispatch[task.pr_id] = TaskStatus.DOING
            for i, queued in enumerate(dag_tasks or ()):
                if queued.pr_id == task.pr_id:
                    dag_tasks[i] = queued.model_copy(
                        update={"status": TaskStatus.DOING}
                    )
                    break
            task = task.model_copy(update={"status": TaskStatus.DOING})

        if has_legacy_queue_tasks or dag_tasks is None:
            queue_tasks = tasks
        else:
            queue_tasks = dag_tasks
        self._set_queue_progress(
            sum(1 for t in queue_tasks if t.status == TaskStatus.DONE),
            len(queue_tasks),
        )
        self.state.current_queue = list(queue_tasks)
        generated_headers = getattr(self, "_idle_dag_headers", None)
        generated_statuses = getattr(self, "_idle_dag_statuses", None)
        if (
            generated_headers
            and generated_statuses
            and legacy_queue_check_succeeded
            and not has_legacy_queue_tasks
        ):
            try:
                self._write_generated_queue_md(
                    generated_headers,
                    generated_statuses,
                )
            except OSError as exc:
                await self._transition_to_error(
                    f"QUEUE.md auto-generation failed: {exc}",
                    save_run_record_as=None,
                    publish=False,
                    log_prefix="[INFRA]",
                )
                return None

        if task is None:
            self.log_event("[INFRA] No tasks available.")
            if prs:
                done_branches = {
                    t.branch for t in queue_tasks
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

        self._scan_task_specs_for_agents_md_drift()

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
                await self._escalate_to_hung(
                    message,
                    apply_escalated_label=False,
                    set_pr_escalated_flag=False,
                    log_message=f"{message}.",
                )
                return

        self.state.state = PipelineState.CODING
        self.log_event(f"[INFRA] Picked task {task.pr_id}: {task.title}.")
        await self.publish_state()
        await self.handle_coding()

    async def handle_paused(self) -> None:
        """Wait for rate limit window to expire, then resume previous flow."""
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
            if self.state.error_message:
                lowered = self.state.error_message.lower()
                is_rate_limit_msg = (
                    "rate limit" in lowered or re.search(r"\b429\b", lowered)
                )
                if is_rate_limit_msg:
                    self.state.error_message = None
                    self.log_event(
                        f"[RATE-LIMIT] {label}, cleared legacy "
                        f"rate-limit error."
                    )
                else:
                    await self._transition_to_error(
                        self.state.error_message,
                        save_run_record_as=None,
                        publish=False,
                        log_prefix=(
                            f"[RATE-LIMIT] {label} -> ERROR "
                            f"(preserved context):"
                        ),
                    )
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
        if self.state.error_message:
            lowered = self.state.error_message.lower()
            is_rate_limit_msg = (
                "rate limit" in lowered or re.search(r"\b429\b", lowered)
            )
            if is_rate_limit_msg:
                self.state.error_message = None
                self.log_event(
                    "[RATE-LIMIT] Rate limit expired, cleared legacy "
                    "rate-limit error."
                )
            else:
                await self._transition_to_error(
                    self.state.error_message,
                    save_run_record_as=None,
                    publish=False,
                    log_prefix=(
                        "[RATE-LIMIT] Rate limit expired, resuming -> ERROR "
                        "(preserved context):"
                    ),
                )
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
