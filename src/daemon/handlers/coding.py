"""CODING state handler.

Mixin methods:
    handle_coding — run PLANNED PR via the active coder CLI
"""

from __future__ import annotations

import asyncio
import subprocess
from typing import Awaitable, Callable

from src import github_client
from src.daemon import git_ops
from src.models import PipelineState


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
        """
        self._stop_requested = False
        current_pr_id = self.state.current_task.pr_id if self.state.current_task is not None else None
        await self._refresh_auth_status_cache()
        coder_name, plugin = self._get_coder()
        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
        )
        model = plugin_run_kwargs["model"]
        self._start_current_run_record(coder_name, model)
        if not await self._check_rate_limit(proactive_coder=coder_name):
            await self._save_current_run_record("rate_limit")
            return

        self.log_event(f"[CODING] [{coder_name}] Starting PLANNED PR.")

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

        breach_dir, breach_run_id = self._breach_env()
        breach_flag: dict[str, bool] = {"breached": False}

        heartbeat = asyncio.create_task(self._publish_while_waiting("CODING"))
        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
            breach_dir=breach_dir,
            breach_run_id=breach_run_id,
        )
        coder_kwargs: dict[str, object] = {
            **plugin_run_kwargs,
            "timeout": self.app_config.daemon.planned_pr_timeout_sec,
            "on_process_start": self._track_current_coder_process,
        }
        cli_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
            plugin.run_planned_pr(
                self.repo_path,
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
                return
            if not breach_flag["breached"]:
                raise
            # Record the PR if Claude already created one before cancellation,
            # so it enters WATCH/auto-merge flow after pause expiry.
            # Retry up to 3 times — PR list visibility is eventually consistent.
            if target_branch:
                for _attempt in range(3):
                    try:
                        prs = github_client.get_open_prs(
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
                                f"[CODING] Recorded PR #{match.number} "
                                f"before breach-cancel pause."
                            )
                            break
                    except Exception:
                        pass  # best-effort; the pause is still correct
                    if _attempt < 2:
                        await asyncio.sleep(5)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"[CODING] CODING aborted: in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return
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
            # Record the PR if the coder already created one, so it is not
            # orphaned while the runner is paused.
            # Retry up to 3 times — PR list visibility is eventually consistent.
            if target_branch:
                for _attempt in range(3):
                    try:
                        prs = github_client.get_open_prs(
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
                                f"[CODING] Recorded PR #{match.number} "
                                f"before late-breach pause."
                            )
                            break
                    except Exception:
                        pass  # best-effort; the pause is still correct
                    if _attempt < 2:
                        await asyncio.sleep(5)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"[CODING] CODING paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return

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

        await self._save_cli_log(stdout, stderr, f"PLANNED PR output [{coder_name}]")
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
        github_client._invalidate_etag_cache(
            f"repos/{self.owner_repo}/pulls"
        )
        candidate = None
        for attempt in range(3):
            if await pause_for_stop_if_requested():
                return
            try:
                prs = github_client.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
            except Exception as exc:
                await self._transition_to_error(
                    f"get_open_prs failed: {exc}",
                    publish=False,
                    log_prefix="[CODING]",
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
            if not local_exists:
                message = (
                    f"[{coder_name}] Coder exited 0 but did nothing — "
                    f"escalating"
                )
            else:
                message = (
                    f"[{coder_name}] Coder exited 0 with local branch but "
                    f"no push — escalating"
                )
            self.state.state = PipelineState.HUNG
            self.state.error_message = message
            await self._save_current_run_record("error")
            self.log_event(f"[ESCALATE] {message}.")
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
                prs = github_client.get_open_prs(
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
                await self._transition_to_error(
                    (
                        f"[{coder_name}] Daemon-created PR list failed after "
                        f"3 attempts: {last_list_exc}"
                    ),
                    publish=False,
                    log_prefix="[CODING]",
                )
            else:
                message = (
                    f"[{coder_name}] Daemon-created PR not found for branch "
                    f"{target_branch!r}"
                )
                self.state.state = PipelineState.HUNG
                self.state.error_message = message
                await self._save_current_run_record("error")
                self.log_event(f"[ESCALATE] {message}.")
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
            github_client.run_gh(
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
                github_client._invalidate_etag_cache(
                    f"repos/{self.owner_repo}/pulls"
                )
                return True
            message = (
                f"[{coder_name}] Daemon PR creation failed for "
                f"{target_branch!r}: {exc}"
            )
            self.state.state = PipelineState.HUNG
            self.state.error_message = message
            await self._save_current_run_record("error")
            self.log_event(f"[ESCALATE] {message}.")
            return False
        github_client._invalidate_etag_cache(
            f"repos/{self.owner_repo}/pulls"
        )
        return True
