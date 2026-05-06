"""CODING state handler.

Mixin methods:
    handle_coding — run PLANNED PR via the active coder CLI
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from src.branch_context import BranchContext
from src.cancellation import CancellationCause, classify_infra_exception
from src.coder_registry import CoderPlugin
from src.daemon import git_ops
from src.daemon.handlers import CoderUnavailable
from src.github import cache as gh_cache
from src.github import gh_runner
from src.github import prs as gh_prs
from src.models import PipelineState


def _resolve_task_file_under_repo(repo_path: str, task_file: str) -> Path:
    """Resolve ``task_file`` under ``repo_path`` rejecting traversal/escape.

    The queue parser does not validate ``task_file`` paths, so a malicious
    or mistaken queue entry could otherwise point CODING at an absolute
    path or a ``..``-prefixed path that escapes the cloned repo and leaks
    arbitrary host content into the inline ``run_auto_pr`` prompt.

    Raises ``ValueError`` if ``task_file`` is absolute, contains ``..``
    segments, escapes ``repo_path`` after symlink resolution, or fails to
    resolve at all (e.g. a symlink loop, which ``Path.resolve`` surfaces
    as ``RuntimeError``, or a permission/IO error surfaced as ``OSError``).
    Normalizing those resolver exceptions here keeps the caller's error
    handling uniform — ``handle_coding`` only has to catch ``ValueError``
    to route every malformed task path through the controlled
    ``_transition_to_error`` path instead of letting the exception escape
    and crash the cycle.
    """
    candidate = Path(task_file)
    if candidate.is_absolute():
        raise ValueError(f"absolute path not allowed: {task_file!r}")
    if any(part == ".." for part in candidate.parts):
        raise ValueError(f"path traversal not allowed: {task_file!r}")
    try:
        repo_root = Path(repo_path).resolve()
        resolved = (repo_root / candidate).resolve()
    except (RuntimeError, OSError) as exc:
        raise ValueError(
            f"cannot resolve task path {task_file!r}: {exc}"
        ) from exc
    try:
        resolved.relative_to(repo_root)
    except ValueError as exc:
        raise ValueError(
            f"path escapes repo root: {task_file!r}"
        ) from exc
    return resolved


def _local_branch_exists(repo_path: str, branch: str) -> bool:
    """Return ``True`` if ``refs/heads/{branch}`` exists in ``repo_path``.

    A non-zero exit (or any git-side error) is treated as "not present" so
    the diagnostic conservatively reports a missing branch when the probe
    itself is unreliable.
    """
    try:
        probe = git_ops._git(
            repo_path,
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/heads/{branch}",
            timeout=10,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return False
    return probe.returncode == 0


def _remote_branch_exists(repo_path: str, branch: str) -> bool:
    """Return ``True`` if ``origin`` reports ``refs/heads/{branch}`` exists.

    Uses ``git ls-remote --exit-code`` so a missing ref returns rc=2 and a
    transient transport failure returns rc>=128 — both interpreted as "not
    visible upstream" by the caller, which routes A/B to HUNG without
    risking a false-positive PR-creation attempt against a non-existent
    upstream branch.
    """
    try:
        probe = git_ops._git(
            repo_path,
            "ls-remote",
            "--exit-code",
            "--heads",
            "origin",
            f"refs/heads/{branch}",
            timeout=30,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return False
    return probe.returncode == 0 and bool(probe.stdout.strip())


class CodingMixin:
    """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH."""

    async def handle_coding(self) -> None:
        """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH.

        The coder owns the full git workflow per AGENTS.md: branch creation,
        commit, push, and PR creation. The daemon must not pre-create the
        task branch — doing so conflicts with AGENTS.md step 4 ("create
        branch from origin/main"). After the CLI returns 0 we poll GitHub
        for the PR; because the list API is eventually consistent, we
        retry a few times before surfacing an ERROR.

        Reads top-down as a state-machine flow with three phases:

        1. ``_prepare_coder_invocation`` — breach env allocation, kwargs
           build. Auth refresh happens before ``_get_coder`` so the
           selector sees fresh statuses; ``_start_current_run_record``
           runs before the branch guard so the missing-branch ERROR
           transition still emits run telemetry; and
           ``_check_rate_limit`` runs before the branch guard so an
           active or renewed rate-limit window pauses the runner instead
           of being overridden by the ERROR transition.
        2. ``_run_coder_with_supervision`` — subprocess plus stop and
           breach monitors; resolves user-stop and breach pauses.
        3. ``_post_coder_resolution`` — CLI log save, exit classification,
           PR lookup or daemon-side PR creation, run record save.
        """
        self._stop_requested = False
        current_pr_id = (
            self.state.current_task.pr_id
            if self.state.current_task is not None
            else None
        )
        # Refresh auth before _get_coder so the selector sees fresh
        # statuses; selecting first would let a stale/empty auth cache
        # pick an ineligible coder that no later refresh can undo.
        await self._refresh_auth_status_cache()
        coder_name, plugin = self._get_coder()

        # Start the run record before the branch guard so a malformed
        # task (no Branch:) still produces error telemetry — otherwise
        # the ``_save_current_run_record("error")`` inside
        # ``_transition_to_error`` would no-op against an absent record
        # and the missing-branch path would silently lose its run.
        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
        )
        self._start_current_run_record(coder_name, plugin_run_kwargs["model"])

        # Run the rate-limit gate before the branch guard so an active
        # or renewed pause window is honored as PAUSED. If this ran
        # after the guard, a malformed task would short-circuit through
        # ``_transition_to_error`` and bypass the cooldown that
        # ``_check_rate_limit`` is responsible for enforcing.
        if not await self._check_rate_limit(proactive_coder=coder_name):
            await self._save_current_run_record("rate_limit")
            return

        target_branch = (
            self.state.current_task.branch if self.state.current_task else None
        )
        if not target_branch:
            await self._transition_to_error(
                "Current task has no branch; cannot identify PR",
                publish=False,
                log_prefix="[CODING]",
            )
            return

        # AUTO PR dispatch: read the task spec inline before invoking the
        # coder so the daemon supplies the canonical pr_id, task_file, and
        # task_body explicitly. This replaces the prior PLANNED PR
        # indirection where the coder discovered its task via QUEUE.md.
        # Honor ``current_task.task_file`` (parsed from ``tasks/QUEUE.md`` by
        # IDLE) so a queued entry pointing at a non-default location still
        # resolves correctly; only fall back to ``tasks/{pr_id}.md`` when
        # the queue entry omits the path (legacy entries).
        assert self.state.current_task is not None
        pr_id = self.state.current_task.pr_id
        task_file = self.state.current_task.task_file or f"tasks/{pr_id}.md"
        try:
            task_body_path = _resolve_task_file_under_repo(
                self.repo_path, task_file
            )
        except ValueError as exc:
            await self._transition_to_error(
                f"Invalid task file {task_file!r}: {exc}",
                publish=False,
                log_prefix="[CODING]",
            )
            return
        try:
            task_body = task_body_path.read_text(encoding="utf-8")
        except (OSError, ValueError) as exc:
            # ``ValueError`` covers ``UnicodeDecodeError`` (a ``ValueError``
            # subclass) raised when ``tasks/{pr_id}.md`` contains non-UTF-8
            # bytes. Treat decode failures the same as I/O failures so the
            # daemon records a deterministic task-file error and routes
            # through ``_transition_to_error`` instead of letting the
            # exception escape and crash ``run_cycle``.
            await self._transition_to_error(
                f"Cannot read task file {task_file}: {exc}",
                publish=False,
                log_prefix="[CODING]",
            )
            return

        try:
            coder_kwargs = await self._prepare_coder_invocation(
                coder_name, plugin
            )
        except CoderUnavailable:
            return

        self._write_active_pr_runtime_file(pr_id)
        self._write_expected_branch(target_branch)
        # Expected-branch cleanup belongs in a finally so the pre-push
        # hook does not reject later operator or daemon pushes whose HEAD
        # no longer matches this dispatch branch.
        try:
            result = await self._run_coder_with_supervision(
                coder_name,
                plugin,
                coder_kwargs,
                target_branch=target_branch,
                current_pr_id=current_pr_id,
                pr_id=pr_id,
                task_file=task_file,
                task_body=task_body,
            )
            if result is None:
                return

            code, stdout, stderr = result
            await self._post_coder_resolution(
                coder_name,
                code,
                stdout,
                stderr,
                target_branch=target_branch,
                current_pr_id=current_pr_id,
            )
        finally:
            self._cleanup_expected_branch()

    async def _prepare_coder_invocation(
        self,
        coder_name: str,
        plugin: CoderPlugin,
    ) -> dict[str, Any]:
        """Allocate breach env, build kwargs.

        Returns the kwargs dict ready to pass to ``plugin.run_auto_pr``.
        Allocates the breach env via ``_breach_env`` and stores it on
        ``self`` so ``_run_coder_with_supervision`` can wire monitors and
        teardown without re-creating it.

        Auth refresh, run-record start, the rate-limit gate, and the
        branch guard all happen in ``handle_coding`` before this helper
        so the selector sees fresh statuses, the branch-guard ERROR path
        still records run telemetry, and an active rate-limit window
        keeps the runner PAUSED instead of falling through to the ERROR
        transition.
        """
        self.log_event(f"[CODING] [{coder_name}] Starting PLANNED PR.")

        breach_dir, breach_run_id = self._breach_env()
        self._current_breach_dir = breach_dir
        self._current_breach_run_id = breach_run_id

        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
            breach_dir=breach_dir,
            breach_run_id=breach_run_id,
        )
        return {
            **plugin_run_kwargs,
            "timeout": self.app_config.daemon.planned_pr_timeout_sec,
            "on_process_start": self._track_current_coder_process,
        }

    async def _run_coder_with_supervision(
        self,
        coder_name: str,
        plugin: CoderPlugin,
        coder_kwargs: dict[str, Any],
        *,
        target_branch: str,
        current_pr_id: str | None,
        pr_id: str,
        task_file: str,
        task_body: str,
    ) -> tuple[int, str, str] | None:
        """Run the coder subprocess under stop and breach supervision.

        Returns ``(code, stdout, stderr)`` on normal completion. Returns
        ``None`` when the run was either cancelled by an explicit user
        stop (state moved to PAUSED) or aborted by an in-flight rate-limit
        breach detected during or shortly after the subprocess (state
        moved to PAUSED, run record saved as ``"rate_limit"``).
        """
        breach_dir = self._current_breach_dir
        breach_run_id = self._current_breach_run_id
        breach_flag: dict[str, bool] = {"breached": False}

        heartbeat = asyncio.create_task(self._publish_while_waiting("CODING"))
        cli_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
            plugin.run_auto_pr(
                self.repo_path,
                pr_id=pr_id,
                task_file=task_file,
                task_body=task_body,
                **coder_kwargs,
            )
        )
        breach_monitor: asyncio.Task[None] | None = None
        if plugin.supports_breach_lifecycle:
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir, breach_run_id, cli_task, breach_flag,
                )
            )
        stop_monitor = asyncio.create_task(self._monitor_stop_request(cli_task))
        try:
            code, stdout, stderr = await cli_task
        except asyncio.CancelledError:
            if self._stop_requested:
                if current_pr_id is not None:
                    self._user_stopped_task_pr_ids.add(current_pr_id)
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                await self._save_current_run_record("error")
                self.log_event("[CODING] CODING aborted: user stop requested.")
                return None
            if not breach_flag["breached"]:
                raise
            await self._record_pre_pause_pr(
                target_branch, "before breach-cancel pause"
            )
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"[CODING] CODING aborted: in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return None
        finally:
            stop_monitor.cancel()
            if breach_monitor is not None:
                breach_monitor.cancel()
            heartbeat.cancel()
            self._current_coder_process = None
            if plugin.supports_breach_lifecycle:
                self._check_late_breach(breach_dir, breach_run_id, breach_flag)
                self._cleanup_breach_marker(breach_dir, breach_run_id)

        if breach_flag["breached"]:
            await self._record_pre_pause_pr(
                target_branch, "before late-breach pause"
            )
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"[CODING] CODING paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return None
        return (code, stdout, stderr)

    async def _record_pre_pause_pr(
        self,
        target_branch: str,
        log_suffix: str,
    ) -> None:
        """Best-effort: record a PR opened before a breach-induced pause.

        PR list visibility is eventually consistent, so retry up to 3
        times. Failures are silently ignored — the pause is still correct
        without the PR record, and the runner picks up the PR on the next
        cycle once the rate-limit window clears.
        """
        for attempt in range(3):
            try:
                prs = gh_prs.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
                match = next(
                    (pr for pr in prs if pr.branch == target_branch),
                    None,
                )
                if match:
                    self.state.current_pr = match
                    self.log_event(
                        f"[CODING] Recorded PR #{match.number} {log_suffix}."
                    )
                    return
            except Exception:
                pass  # best-effort; the pause is still correct
            if attempt < 2:
                await asyncio.sleep(5)

    def _expected_branch_path(self) -> Path:
        """Return the path of the daemon's ``expected-branch`` marker file.

        The file is read by the pre-push hook installed by the
        scaffolder; the hook aborts a push when ``HEAD`` is on a
        different branch than the daemon expected for the active task.

        Resolves the marker via ``git rev-parse --git-path
        info/expected-branch`` so linked worktrees (where ``.git`` is a
        file pointing into ``<main-repo>/.git/worktrees/<name>/``) and
        repos created with ``--separate-git-dir`` land the marker in
        the per-checkout ``info/`` directory git actually uses. The
        hardcoded ``Path(repo_path) / ".git" / "info"`` layout would
        otherwise raise ``NotADirectoryError`` on every write — the
        ``OSError`` catch in ``_write_expected_branch`` swallows it and
        silently disables push validation for the entire CODING run.
        Falls back to the legacy hardcoded path on probe failure
        (non-zero rc, empty stdout, ``OSError``/``SubprocessError``)
        so test contexts that point at a synthetic non-git directory
        still get a Path; the caller's existing ``OSError`` handling
        then surfaces the real failure on the actual write attempt.
        """
        repo_root = Path(self.repo_path)
        fallback = repo_root / ".git" / "info" / "expected-branch"
        try:
            probe = git_ops._git(
                self.repo_path,
                "rev-parse",
                "--git-path",
                "info/expected-branch",
                timeout=10,
                check=False,
            )
        except (subprocess.SubprocessError, OSError):
            return fallback
        if probe.returncode != 0:
            return fallback
        raw = (probe.stdout or "").strip()
        if not raw:
            return fallback
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = repo_root / candidate
        return candidate

    def _write_expected_branch(self, branch: str) -> None:
        """Write ``branch`` to the expected-branch marker for the pre-push hook.

        Defense-in-depth: PR-271 already steers the coder to the right
        branch via the ``AUTO PR`` prompt headers, so a write failure
        here only weakens the local push gate. Log a warning and let the
        dispatch proceed instead of routing to ERROR — refusing to
        dispatch on a missing ``info/`` directory would regress against
        the prompt-level protections that already cover the primary
        scope-expansion path.
        """
        try:
            self._expected_branch_path().write_text(
                branch + "\n", encoding="utf-8"
            )
        except OSError as exc:
            self.log_event(
                f"[CODING] expected-branch write failed ({exc}); pre-push "
                f"hook disabled for this dispatch — continuing."
            )

    def _active_pr_runtime_path(self) -> Path:
        """Return the e2e shim runtime marker path for the active PR id."""
        return Path(self.repo_path) / ".daemon-runtime" / "active-pr-id"

    def _e2e_shim_enabled(self) -> bool:
        """Return whether test-mode shim integration is enabled."""
        return os.environ.get("PIPELINE_E2E_SHIM") == "1"

    def _write_active_pr_runtime_file(self, pr_id: str) -> None:
        """Write the active PR id for the e2e coder shim in test mode only."""
        if not self._e2e_shim_enabled():
            return
        try:
            marker = self._active_pr_runtime_path()
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(pr_id + "\n", encoding="utf-8")
        except OSError as exc:
            self.log_event(
                f"[CODING] active-pr-id runtime write failed ({exc}); "
                "e2e shim task discovery may fall back to task headers."
            )

    def _cleanup_expected_branch(self) -> None:
        """Remove the expected-branch marker after coder resolution.

        Cleanup runs regardless of coder exit code so manual operator
        pushes between dispatches are not blocked by a stale marker.
        ``missing_ok=True`` swallows ``FileNotFoundError`` so a missing
        file (e.g. write failed earlier in the cycle) does not raise;
        the broader ``OSError`` catch covers permission/IsADirectory
        edge cases left behind when the write itself collided with a
        non-file at the marker path.
        """
        try:
            self._expected_branch_path().unlink(missing_ok=True)
        except OSError as exc:
            self.log_event(
                f"[CODING] expected-branch cleanup failed ({exc}); "
                f"manual git operations on this worktree may be gated "
                f"until the marker is removed."
            )

    async def _post_coder_resolution(
        self,
        coder_name: str,
        code: int,
        stdout: str,
        stderr: str,
        *,
        target_branch: str,
        current_pr_id: str | None,
    ) -> None:
        """CLI log save, exit classification, PR lookup, WATCH handoff.

        Either transitions to WATCH (PR found or daemon-created) or
        returns after a state transition to PAUSED, ERROR, or HUNG via
        the appropriate primitive (``_transition_to_error``,
        ``_diagnose_exit_zero_no_pr``). The expected-branch marker is
        cleaned up in ``handle_coding``'s ``finally`` block so every
        post-write exit path is covered, including the pause shortcuts
        in ``_run_coder_with_supervision`` that bypass this method.
        """
        async def pause_for_stop_if_requested() -> bool:
            if self._stop_requested:
                requested = True
            else:
                requested = await self._pop_stop_request()
                if requested:
                    self._stop_requested = True
                    self.state.user_paused = True
                    self.log_event(
                        "[CODING] User stop requested after coder exit; "
                        "honoring persisted stop."
                    )
            if not requested:
                return False
            if current_pr_id is not None:
                self._user_stopped_task_pr_ids.add(current_pr_id)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("error")
            self.log_event("[CODING] CODING aborted: user stop requested.")
            return True

        await self._save_cli_log(
            stdout, stderr, f"PLANNED PR output [{coder_name}]"
        )
        if not await pause_for_stop_if_requested():
            await self._refresh_user_paused_from_redis()
            if self.state.user_paused:
                self.log_event(
                    "[CODING] User pause persisted during coder exit; "
                    "finishing current run before honoring pause."
                )
        if await pause_for_stop_if_requested():
            return
        if code != 0:
            self._detect_rate_limit(stderr, coder_name=coder_name)
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                await self._save_current_run_record("rate_limit")
                self.log_event(
                    f"[RATE-LIMIT] Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}."
                )
                return
            await self._transition_to_error(
                stderr.strip() or f"{coder_name} exit {code}",
                publish=False,
                log_prefix=f"[CODING] [{coder_name}] CLI failed:",
            )
            return

        # The coder just exited; if it ran ``gh pr create`` from its own
        # subprocess the daemon's ETag cache for ``repos/{repo}/pulls``
        # still reflects the pre-create state. Invalidate before polling
        # so the first ``get_open_prs`` REST fallback returns a fresh 200
        # instead of a 304-cached page that omits the new PR.
        gh_cache._invalidate_etag_cache(f"repos/{self.owner_repo}/pulls")
        candidate = None
        for attempt in range(3):
            if await pause_for_stop_if_requested():
                return
            try:
                prs = gh_prs.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
            except Exception as exc:
                await self._transition_to_error(
                    f"get_open_prs failed: {exc}",
                    publish=False,
                    log_prefix="[CODING]",
                    cancellation_cause=classify_infra_exception(exc),
                )
                return
            candidate = next(
                (pr for pr in prs if pr.branch == target_branch), None
            )
            if candidate is not None:
                break
            if attempt < 2:
                self.log_event(
                    f"[CODING] PR not found for {target_branch!r}, "
                    f"retrying in 5s ({attempt + 1}/3)."
                )
                await asyncio.sleep(5)

        if await pause_for_stop_if_requested():
            return
        if candidate is None:
            # The branch-mismatch check is layered into
            # ``_diagnose_exit_zero_no_pr`` (only on the case-A/B path
            # where no upstream branch exists) instead of firing here
            # unconditionally: a recoverable case-C run that pushed
            # ``task_branch`` but exited on a different local branch
            # (e.g. switched back to ``main`` before exit) must still
            # reach the daemon ``gh pr create`` recovery, since
            # ``--head task_branch`` operates on the upstream ref
            # regardless of the local checkout.
            await self._diagnose_exit_zero_no_pr(
                target_branch, coder_name, pause_for_stop_if_requested
            )
            return

        self.state.current_pr = candidate
        self.state.state = PipelineState.WATCH
        self._rehydrate_last_push_at(candidate)
        await self._save_current_run_record("coding_complete")
        self.log_event(f"[CODING] Opened PR #{candidate.number} -> WATCH.")
        if self._should_skip_codex_review_post(candidate.number):
            self.log_event(
                "[CODING] Codex auto-trigger detected, skipping duplicate "
                "@codex review post."
            )
        else:
            self._post_codex_review(candidate.number)

    async def _diagnose_exit_zero_no_pr(
        self,
        target_branch: str,
        coder_name: str,
        pause_for_stop_if_requested: Callable[[], Awaitable[bool]],
    ) -> None:
        """Resolve the "coder exited 0 but no PR" outcome by branch state.

        Distinguishes three cases the runner used to collapse into a single
        HUNG transition: (A) coder did nothing — neither a local branch nor
        an upstream ref exists; (B) coder created the branch locally but
        never pushed; (C) coder pushed a branch but failed to open the PR.
        Cases A and B route to HUNG because we cannot trust the local tree
        or push outcome without inspection. Case C is auto-recoverable —
        the daemon issues ``gh pr create`` itself and hands off to WATCH on
        success, falling back to HUNG when PR creation fails.

        ``pause_for_stop_if_requested`` is rechecked before any state-changing
        side effect (PR creation, sleep between visibility retries, final
        WATCH transition) so a user stop pressed after coder exit but before
        the daemon's own PR creation is honored — otherwise the daemon would
        open an unwanted PR after an explicit stop.
        """
        local_exists = _local_branch_exists(self.repo_path, target_branch)
        remote_exists = _remote_branch_exists(self.repo_path, target_branch)

        # ``_remote_branch_exists`` can block up to its 30s timeout; a stop
        # pressed during that window must still be honored before either A/B
        # routes to HUNG or C proceeds to PR creation, otherwise the user's
        # stop is silently overridden by an unwanted state transition.
        if await pause_for_stop_if_requested():
            return

        if not remote_exists:
            ctx = BranchContext.from_runner(self)
            if ctx.mismatch_reason is not None:
                # Daemon recovery is unavailable (no upstream
                # ``task_branch``) AND the runner ended on a different
                # branch surface than the task declares — surface the
                # divergence explicitly so an operator sees the named
                # mismatch instead of the generic "did nothing" /
                # "no push" verdict.
                self.log_event(
                    f"[BRANCH] mismatch detected: {ctx.mismatch_reason}; "
                    f"{ctx.log_summary()}"
                )
                message = (
                    f"[{coder_name}] Branch mismatch: "
                    f"{ctx.mismatch_reason} ({ctx.log_summary()})"
                )
                await self._save_current_run_record("error")
                await self._escalate_to_hung(
                    message,
                    apply_escalated_label=False,
                    set_pr_escalated_flag=False,
                    log_message=f"{message}.",
                )
                return
            if not local_exists:
                message = (
                    f"[{coder_name}] Coder exited 0 but did nothing — "
                    f"escalating ({ctx.log_summary()})"
                )
            else:
                message = (
                    f"[{coder_name}] Coder exited 0 with local branch but "
                    f"no push — escalating ({ctx.log_summary()})"
                )
            await self._save_current_run_record("error")
            await self._escalate_to_hung(
                message,
                apply_escalated_label=False,
                set_pr_escalated_flag=False,
                log_message=f"{message}.",
            )
            return

        self.log_event(
            f"[CODING] [{coder_name}] Coder exited 0 with branch but no "
            f"PR — daemon creating PR."
        )
        if not await self._daemon_create_pr_for_branch(
            target_branch, coder_name
        ):
            return

        # GitHub's PR list endpoints are eventually consistent, so a PR
        # that gh pr create just opened may be temporarily absent. Retry
        # the same bounded 3x/5s schedule used earlier in handle_coding
        # before declaring the PR missing. Each iteration honors a pending
        # stop request so a user pressing stop during the visibility window
        # is not overridden by a WATCH handoff. A transient exception from
        # ``get_open_prs`` (network blip, rate-limit fallback) is folded
        # into the same loop so a single failure cannot park the runner as
        # HUNG when the PR has already been created and only the read path
        # is briefly unhealthy.
        candidate = None
        last_list_exc: Exception | None = None
        for attempt in range(3):
            if await pause_for_stop_if_requested():
                return
            try:
                prs = gh_prs.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
            except Exception as exc:
                last_list_exc = exc
                self.log_event(
                    f"[CODING] [{coder_name}] Daemon-created PR list "
                    f"failed for {target_branch!r}: {exc} "
                    f"({attempt + 1}/3)."
                )
                if attempt < 2:
                    await asyncio.sleep(5)
                continue
            last_list_exc = None
            candidate = next(
                (pr for pr in prs if pr.branch == target_branch), None
            )
            if candidate is not None:
                break
            if attempt < 2:
                self.log_event(
                    f"[CODING] [{coder_name}] Daemon-created PR not "
                    f"visible yet for {target_branch!r}, retrying in 5s "
                    f"({attempt + 1}/3)."
                )
                await asyncio.sleep(5)

        if await pause_for_stop_if_requested():
            return
        if candidate is None:
            if last_list_exc is not None:
                # Every list attempt raised — the PR was created but we
                # cannot confirm its visibility. Degrade to ERROR (which
                # the daemon retries) rather than HUNG (manual park) so a
                # transient read outage does not strand the task.
                infra_cause = classify_infra_exception(last_list_exc)
                if infra_cause is None:
                    infra_cause = CancellationCause(
                        category="INFRA",
                        payload={
                            "subsystem": "gh_api",
                            "retry_count": 3,
                            "last_attempt_iso": datetime.now(
                                timezone.utc
                            ).isoformat(),
                            "error_class": type(last_list_exc).__name__,
                            "error_message": str(last_list_exc)[:500],
                        },
                    )
                await self._transition_to_error(
                    (
                        f"[{coder_name}] Daemon-created PR list failed after "
                        f"3 attempts: {last_list_exc}"
                    ),
                    publish=False,
                    log_prefix="[CODING]",
                    cancellation_cause=infra_cause,
                )
            else:
                message = (
                    f"[{coder_name}] Daemon-created PR not found for branch "
                    f"{target_branch!r}"
                )
                await self._save_current_run_record("error")
                await self._escalate_to_hung(
                    message,
                    apply_escalated_label=False,
                    set_pr_escalated_flag=False,
                    log_message=f"{message}.",
                )
            return

        self.state.current_pr = candidate
        self.state.state = PipelineState.WATCH
        self._rehydrate_last_push_at(candidate)
        await self._save_current_run_record("coding_complete")
        self.log_event(
            f"[CODING] Daemon opened PR #{candidate.number} for "
            f"{target_branch!r} -> WATCH."
        )
        if self._should_skip_codex_review_post(candidate.number):
            self.log_event(
                "[CODING] Codex auto-trigger detected, skipping duplicate "
                "@codex review post."
            )
        else:
            self._post_codex_review(candidate.number)

    async def _daemon_create_pr_for_branch(
        self,
        target_branch: str,
        coder_name: str,
    ) -> bool:
        """Run ``gh pr create`` against an already-pushed branch.

        Returns ``True`` on success. ``gh pr create`` exits non-zero with an
        "already exists" message when a PR for the same head branch is
        already open (often because the earlier list lagged GitHub PR
        visibility); that case is treated as success so the caller's
        post-create visibility loop can pick up the existing PR rather than
        parking the runner as HUNG. On any other failure the runner is
        transitioned to HUNG with the gh error and the run record saved,
        matching the ESCALATE-style handling the diagnostic uses for cases
        A and B — a failed creation is not silently retried.
        """
        task = self.state.current_task
        # The diagnostic only runs after handle_coding's target_branch guard,
        # so current_task is always populated when we reach this method.
        assert task is not None
        pr_title = f"{task.pr_id}: {task.title}" if task.title else task.pr_id
        if task.task_file:
            body = (
                f"Auto-created by pipeline-orchestrator after coder exit=0 "
                f"with no PR. See `{task.task_file}` for the planned scope."
            )
        else:
            body = (
                "Auto-created by pipeline-orchestrator after coder exit=0 "
                "with no PR."
            )

        base_branch = self.repo_config.branch
        try:
            gh_runner.run_gh(
                [
                    "pr",
                    "create",
                    "--base",
                    base_branch,
                    "--head",
                    target_branch,
                    "--title",
                    pr_title,
                    "--body",
                    body,
                ],
                repo=self.owner_repo,
            )
        except (RuntimeError, subprocess.SubprocessError, OSError) as exc:
            if "already exists" in str(exc).lower():
                self.log_event(
                    f"[CODING] [{coder_name}] gh pr create reports PR "
                    f"already exists for {target_branch!r}; reusing "
                    f"existing PR."
                )
                # The daemon's last list view did not include this PR yet
                # but the upstream "already exists" reply confirms it does;
                # drop any cached pages so the post-create visibility loop
                # fetches a fresh 200 instead of looping on a 304-cached
                # page that still misses it.
                gh_cache._invalidate_etag_cache(
                    f"repos/{self.owner_repo}/pulls"
                )
                return True
            message = (
                f"[{coder_name}] Daemon PR creation failed for "
                f"{target_branch!r}: {exc}"
            )
            await self._save_current_run_record("error")
            await self._escalate_to_hung(
                message,
                apply_escalated_label=False,
                set_pr_escalated_flag=False,
                log_message=f"{message}.",
            )
            return False
        gh_cache._invalidate_etag_cache(
            f"repos/{self.owner_repo}/pulls"
        )
        return True
