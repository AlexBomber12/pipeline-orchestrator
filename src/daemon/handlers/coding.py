"""CODING state handler.

Mixin methods:
    handle_coding — run PLANNED PR via the active coder CLI
"""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from src.branch_context import BranchContext
from src.cancellation import (
    CancellationCause,
    classify_infra_exception,
    delete_current_run_started_at,
    get_task_spec_hash,
    record_current_run_started_at,
    record_task_spec_hash,
    task_spec_content_hash,
)
from src.coder_registry import CoderPlugin
from src.daemon import fix_escalation, git_ops
from src.daemon.guardrails import scan_stdout
from src.daemon.handlers import CoderUnavailable
from src.daemon.quarantine import apply_quarantine_label_for_violation
from src.daemon.selector import CoderSelectionUnavailable
from src.github import prs as gh_prs
from src.models import PipelineState, PRInfo, PublicationAttempt
from src.process_supervisor import (
    ProcessSupervisionError,
    SupervisedProcess,
    cancelled_process_result,
)
from src.subsource_registry import SuppressionReason


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


def _remote_branch_exists(repo_path: str, branch: str) -> bool | None:
    """Return presence, proven absence (rc=2), or unknown on probe failure."""
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
        return None
    if probe.returncode == 2:
        return False
    if probe.returncode == 0 and probe.stdout.strip():
        return True
    return None


def _local_branch_head_sha(repo_path: str, branch: str) -> str | None:
    """Return the local task branch commit when it is a full GitHub SHA."""
    try:
        probe = git_ops._git(
            repo_path,
            "rev-parse",
            "--verify",
            f"refs/heads/{branch}^{{commit}}",
            timeout=10,
            check=False,
        )
    except (subprocess.SubprocessError, OSError):
        return None
    sha = probe.stdout.strip() if probe.returncode == 0 else ""
    if len(sha) != 40 or any(char not in "0123456789abcdefABCDEF" for char in sha):
        return None
    return sha


@dataclass(frozen=True)
class _PublicationResolution:
    """Final targeted publication state after coder cleanup."""

    failure: Exception | str | None = None
    cancelled_by_publication: bool = False


def _fresh_ready_publication(
    publications: list[gh_prs.BranchPublication],
    *,
    baseline_numbers: set[int],
    not_before: datetime,
    expected_head_sha: str | None,
) -> gh_prs.BranchPublication | None:
    """Select verified evidence created during the active CODING dispatch."""
    candidates = [
        publication
        for publication in publications
        if publication.is_verified_ready
        and publication.number not in baseline_numbers
        and publication.created_at is not None
        and publication.created_at >= not_before
        and publication.head_sha == expected_head_sha
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda publication: (publication.created_at, publication.number),
    )


class CodingMixin:
    """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH."""

    def _begin_publication_attempt(
        self, *, started_at: datetime, baseline_numbers: set[int] | None = None,
    ) -> None:
        task = self.state.current_task
        invocation = self.state.active_invocation
        if task is None or not task.branch or invocation is None:
            return
        self.state.publication_attempt = PublicationAttempt(
            repo=self.owner_repo, base=self.repo_config.branch or "main",
            task_id=task.pr_id, branch=task.branch,
            run_id=invocation.run_id, attempt_index=invocation.attempt_index,
            started_at=started_at, baseline_numbers=baseline_numbers,
            pr_number=self.state.current_pr.number if invocation.phase == "fix"
            and self.state.current_pr is not None
            and self.state.current_pr.branch == task.branch
            and self.state.current_pr.pr_id in {None, task.pr_id} else None,
        )
        if self.state.publication_attempt.pr_number is not None:
            self.state.publication_attempt.candidate = self.state.current_pr.model_copy(deep=True)
            self.state.publication_attempt.head_sha = self.state.current_pr.head_sha

    async def _retain_observed_publication(self, observed: gh_prs.BranchPublication) -> None:
        """Checkpoint a fresh monitor proof before a later read can omit it."""
        attempt = self.state.publication_attempt
        if attempt is None:
            return
        if attempt.head_sha != observed.head_sha:
            attempt.verified = False
        attempt.head_sha = observed.head_sha
        if self._publication_matches_attempt(observed):
            attempt.pr_number = observed.number
            attempt.candidate = observed.to_pr_info()
            await self._resolve_published_attempt(resolve_terminal=False)
            await self.publish_state()

    async def _capture_published_attempt(
        self, observed: gh_prs.BranchPublication | None = None,
    ) -> None:
        """Persist positive identity before any failure/cleanup handoff."""
        attempt = self.state.publication_attempt
        if attempt is None:
            return
        local_head = await asyncio.to_thread(
            _local_branch_head_sha, self.repo_path, attempt.branch,
        )
        if local_head is not None and local_head != attempt.head_sha:
            # Keep the known remote head and persist the local candidate so
            # reconciliation can retry after GitHub catches up with a push.
            if attempt.pr_number is not None:
                attempt.unverified_head_sha = local_head
            else:
                attempt.verified = False
                attempt.head_sha = local_head
        if (observed is not None and self._publication_matches_attempt(observed)):
            attempt.pr_number = observed.number
            attempt.candidate = observed.to_pr_info()
        await self._resolve_published_attempt(resolve_terminal=False)
        await self.publish_state()

    def _publication_matches_attempt(
        self, item: gh_prs.BranchPublication, *, allow_headless: bool = False,
    ) -> bool:
        attempt = self.state.publication_attempt
        return bool(
            attempt is not None and attempt.baseline_numbers is not None
            and attempt.pr_number in {None, item.number}
            and item.number not in attempt.baseline_numbers
            and item.created_at is not None and item.created_at >= attempt.started_at
            and item.base_branch == attempt.base and item.head_branch == attempt.branch
            and (item.state in {"MERGED", "CLOSED"} or item.is_verified_ready)
            and (item.head_sha == attempt.head_sha or (
                allow_headless and attempt.head_sha is None
                and re.fullmatch(r"[0-9a-fA-F]{40}", item.head_sha) is not None
            )) and not item.is_cross_repository
            and gh_prs.extract_queue_pr_id(item.title) == attempt.task_id
        )

    def _publication_attempt_matches_dispatch(self) -> bool:
        attempt = self.state.publication_attempt
        task = self.state.current_task
        invocation = self.state.active_invocation
        return bool(
            attempt is not None and task is not None and invocation is not None
            and attempt.run_id
            and (attempt.repo, attempt.base, attempt.task_id, attempt.branch,
                 attempt.run_id, attempt.attempt_index) == (
                     self.owner_repo, self.repo_config.branch or "main",
                     task.pr_id, task.branch, invocation.run_id, invocation.attempt_index,
                 )
        )

    async def _resolve_published_attempt(self, *, resolve_terminal: bool = True) -> bool:
        """Reconcile exact evidence; absence never authorizes another create."""
        attempt = self.state.publication_attempt
        if not self._publication_attempt_matches_dispatch():
            return False
        try:
            if attempt.head_sha is None:
                # A restart can precede capture even after the coder published.
                # Prefer the exact local task ref; if it is gone, use only one
                # fresh, non-baseline publication for this dispatch's identity.
                attempt.head_sha = await asyncio.to_thread(
                    _local_branch_head_sha, self.repo_path, attempt.branch,
                )
            if attempt.pr_number is None or attempt.head_sha is None:
                if attempt.baseline_numbers is None:
                    return False
                publications = await asyncio.to_thread(
                    gh_prs.get_branch_publications,
                    attempt.repo, attempt.base, attempt.branch,
                )
                matches = [item for item in publications if self._publication_matches_attempt(
                    item, allow_headless=True,
                )]
                if len(matches) != 1:
                    return False
                candidate = matches[0].to_pr_info()
                # Keep positive identity even if the exact read is unavailable.
                attempt.head_sha = candidate.head_sha
                attempt.pr_number = candidate.number
                attempt.candidate = candidate
            current = attempt.candidate
            if (current is None or current.number != attempt.pr_number
                or current.branch != attempt.branch or current.is_cross_repository
                or current.pr_id not in {None, attempt.task_id}):
                return False
            if attempt.unverified_head_sha is not None:
                try:
                    candidate_outcome = await asyncio.to_thread(
                        gh_prs.get_exact_pr_outcome,
                        attempt.repo, attempt.base, attempt.pr_number,
                        attempt.unverified_head_sha,
                    )
                except Exception as exc:
                    self.log_event(f"[INFRA] FIX head verification deferred: {exc}")
                else:
                    if (candidate_outcome.discovery_complete
                        and candidate_outcome.state in {
                            gh_prs.ExactPRState.OPEN, gh_prs.ExactPRState.MERGED,
                            gh_prs.ExactPRState.CLOSED,
                        }):
                        attempt.head_sha = attempt.unverified_head_sha
                        attempt.unverified_head_sha = None
                        attempt.verified = False
            outcome = await asyncio.to_thread(
                gh_prs.get_exact_pr_outcome,
                attempt.repo, attempt.base, attempt.pr_number, attempt.head_sha,
            )
            if (outcome.discovery_complete and outcome.state in {
                gh_prs.ExactPRState.MERGED, gh_prs.ExactPRState.CLOSED,
            }):
                if attempt.unverified_head_sha is not None:
                    return False
                current.head_sha = attempt.head_sha
                self.state.current_pr = current
                attempt.verified = True
                # Capture may run before process cleanup. Only consuming paths
                # may finalize the task and release its publication hold, but
                # deferred terminal bookkeeping still needs the verified head.
                if not resolve_terminal:
                    return False
                attempt.pending = False
                await self._handle_external_pr_resolution(current, outcome.state.value)
                return True
            if outcome.discovery_complete and outcome.state == gh_prs.ExactPRState.OPEN:
                current.head_sha = attempt.head_sha
                attempt.verified = True
                self.state.current_pr = current
                # The old remote head proves ownership, but cannot release a
                # FIX hold while its newer local commit remains unverified.
                return attempt.unverified_head_sha is None
            return False
        except Exception as exc:
            self.log_event(f"[INFRA] Reconciliation deferred: {exc}")
            return False

    async def _release_unpublished_attempt(self, target_branch: str) -> None:
        """After cleanup, require both branch absence and all-state PR absence."""
        attempt = self.state.publication_attempt
        if (attempt is not None and attempt.branch == target_branch
            and attempt.pr_number is None and attempt.candidate is None):
            remote_exists = await asyncio.to_thread(
                _remote_branch_exists, self.repo_path, target_branch,
            )
            if remote_exists is False and attempt.baseline_numbers is not None:
                # A closed PR survives deletion of its head branch. Recheck all
                # states after the ref probe; failed/earlier discovery cannot
                # authorize another create. Retain even candidates whose HEAD
                # or metadata cannot yet be matched to this dispatch.
                try:
                    publications = await asyncio.to_thread(
                        gh_prs.get_branch_publications,
                        attempt.repo, attempt.base, attempt.branch,
                    )
                except Exception as exc:
                    self.log_event(f"[INFRA] Unpublished attempt release deferred: {exc}")
                    return
                if not any(item.number not in attempt.baseline_numbers for item in publications):
                    attempt.pending = False
                    # Checkpoint before callers await terminal bookkeeping:
                    # a headless attempt cannot repeat reconciliation on restart.
                    await self.publish_state()

    async def _release_abandoned_publication_attempt(self) -> None:
        """Prove absence after quiescence, including a pre-launch crash."""
        attempt = self.state.publication_attempt
        if (not self._publication_attempt_matches_dispatch()
            or attempt.pr_number is not None or attempt.candidate is not None
            or self._coder_invocation_active):
            return
        if await self._confirm_current_coder_cleanup("publication recovery"):
            await self._release_unpublished_attempt(attempt.branch)

    async def _hold_unresolved_publication(self) -> bool:
        """Prevent IDLE/CODING from losing an unfinished publication attempt."""
        attempt = self.state.publication_attempt
        if attempt is None or not attempt.pending:
            return False
        resolved = await self._resolve_published_attempt()
        if resolved and self.state.current_task is None:
            return True
        if not resolved:
            await self._release_abandoned_publication_attempt()
            if not attempt.pending:
                return False
        if self.state.user_paused or self.state.state == PipelineState.PAUSED:
            if self.state.user_paused and not self._user_pause_logged:
                self.log_event("[INFRA] Paused. Press Play to resume.")
                self._user_pause_logged = True
            await self.publish_state()
            return True
        if resolved and not attempt.failure and not self.state.error_message:
            if self.state.active_invocation.phase == "fix":
                # Play resumes the retained FIX; it is not CODING completion
                # and an unchanged head must not request another review.
                attempt.pending = False
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(self.state.current_pr)
                self.log_event("[FIX] Recovered retained PR -> WATCH.")
                await self.publish_state()
                return True
            await self._handoff_coding_pr_to_watch(
                self.state.current_pr, "Recovered published PR -> WATCH.",
            )
            return True
        await self._transition_to_error(
            attempt.failure or self.state.error_message
            or "Publication ownership unresolved; retaining coder attempt",
            save_run_record_as=None, publish=True, log_prefix="[INFRA]",
        )
        return True

    async def handle_coding(self) -> None:
        """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH.

        The coder owns the full git workflow per AGENTS.md: branch creation,
        commit, push, and PR creation. The daemon must not pre-create the
        task branch — doing so conflicts with AGENTS.md step 4 ("create
        branch from origin/main"). After the CLI exits, targeted publication
        and exact identity reads retain the owned PR. Incomplete discovery holds the attempt.

        Reads top-down as a state-machine flow with three phases:

        1. ``_prepare_coder_invocation`` — breach env allocation, kwargs
           build. Auth refresh happens before ``_get_coder`` so the
           selector sees fresh statuses; ``_start_current_run_record``
           runs before the branch guard so the missing-branch ERROR
           transition still emits run telemetry; and
           ``usage_gate`` runs before the branch guard so an
           active or renewed usage pause window pauses the runner instead
           of being overridden by the ERROR transition.
        2. ``_run_coder_with_supervision`` — subprocess plus stop and
           breach monitors; resolves user-stop and breach pauses.
        3. ``_post_coder_resolution`` — CLI log save, exit classification,
           publication reconciliation and run record save.
        """
        if await self._hold_unresolved_publication():
            return
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
        try:
            coder_name, plugin = self._get_coder()
        except CoderSelectionUnavailable as exc:
            await self._transition_to_error(
                str(exc),
                publish=False,
                log_prefix="[CODING]",
            )
            return

        # Start the run record before the branch guard so a malformed
        # task (no Branch:) still produces error telemetry — otherwise
        # the ``_save_current_run_record("error")`` inside
        # ``_transition_to_error`` would no-op against an absent record
        # and the missing-branch path would silently lose its run.
        plugin_run_kwargs = plugin.build_run_kwargs(
            daemon_config=self.app_config.daemon,
        )
        self._start_current_run_record(coder_name, plugin_run_kwargs["model"])

        # Run the usage gate before the branch guard so an active
        # or renewed pause window is honored as PAUSED. If this ran
        # after the guard, a malformed task would short-circuit through
        # ``_transition_to_error`` and bypass the cooldown that
        # ``usage_gate`` is responsible for enforcing.
        if not await self.usage_gate(proactive_coder=coder_name):
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
            task_bytes = task_body_path.read_bytes()
            task_body = task_bytes.decode("utf-8")
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
        task_hash = task_spec_content_hash(task_body)
        try:
            previous_task_hash = await get_task_spec_hash(
                self.redis, self.name, pr_id
            )
        except Exception:
            previous_task_hash = None
        try:
            await record_task_spec_hash(self.redis, self.name, pr_id, task_hash)
        except Exception as exc:
            await self._transition_to_error(
                f"Cannot persist task spec hash for {pr_id}: {exc}",
                publish=False,
                log_prefix="[CODING]",
            )
            return
        # PR-318 fix-feedback: anchor the current run's dispatch timestamp
        # so ``_dispatch_recovery_branch`` can distinguish a stale non-crash
        # cause (left over from a prior run after a best-effort delete
        # failed) from one written during this dispatch. Best-effort write:
        # a Redis blip here only weakens the staleness check, and recovery
        # already degrades a missing marker to the conservative crash
        # branch, so a transient outage must not park the task. When the
        # fresh write fails any previously-stored marker would still anchor
        # the staleness check against an older dispatch, so a stale non-
        # crash cause from a prior run could be wrongly treated as
        # belonging to this run; invalidate the marker on failure so
        # recovery falls back to the conservative crash branch instead.
        try:
            await record_current_run_started_at(self.redis, self.name, pr_id)
        except Exception as exc:
            try:
                await delete_current_run_started_at(self.redis, self.name, pr_id)
            except Exception as delete_exc:
                self.log_event(
                    f"[CODING] current_run_started_at write failed for "
                    f"{pr_id}: {exc}; marker invalidation also failed: "
                    f"{delete_exc}. A stale non-crash cause from a prior "
                    f"run may be wrongly classified as belonging to this "
                    f"run on recovery."
                )
            else:
                self.log_event(
                    f"[CODING] current_run_started_at write failed for "
                    f"{pr_id}: {exc}; stale marker cleared so recovery "
                    f"falls back to crash-branch on this run."
                )
        await self._prepare_current_run_record_dispatch_metadata(
            task_hash=task_hash,
            previous_task_hash=previous_task_hash,
        )

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

            code, stdout, stderr, publication = result
            await self._post_coder_resolution(
                coder_name,
                code,
                stdout,
                stderr,
                target_branch=target_branch,
                current_pr_id=current_pr_id,
                publication=publication,
            )
        finally:
            if (
                self._current_coder_supervised_process is None
                and self._current_coder_process is None
                and self._coder_cleanup_failure_detail is None
            ):
                self._cleanup_expected_branch()
            else:
                self.log_event(
                    "[INFRA] Preserving expected-branch marker while "
                    "coder cleanup remains unconfirmed."
                )

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
            "on_supervised_process_start": (
                self._track_current_coder_supervised_process
            ),
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
    ) -> tuple[int, str, str, _PublicationResolution | None] | None:
        """Run the coder under stop, breach, and publication supervision."""
        breach_dir = self._current_breach_dir
        breach_run_id = self._current_breach_run_id
        breach_flag: dict[str, bool] = {"breached": False}

        base_branch = self.repo_config.branch or "main"
        publication_not_before = datetime.now(timezone.utc).replace(
            microsecond=0
        )
        publication_baseline: set[int] | None
        try:
            baseline = await asyncio.to_thread(
                gh_prs.get_branch_publications,
                self.owner_repo,
                base_branch,
                target_branch,
            )
        except Exception as exc:
            self.log_event(
                f"[CODING] Publication baseline unavailable for "
                f"{target_branch!r}: {exc}; deferring coder invocation."
            )
            # No coder has launched. Persist queue eligibility rather than a
            # CODING task that recovery would classify as an interrupted run.
            self.state.current_task = None
            self._reset_runner_local_task_counters()
            self.state.state = PipelineState.IDLE
            await self.publish_state()
            return None
        else:
            publication_baseline = {item.number for item in baseline}

        if not self._reserve_coder_credentials(
            coder_name,
            invocation_kwargs=coder_kwargs,
        ):
            self.log_event(
                "[CODING] Coder invocation deferred while credential login "
                "owns its credential location."
            )
            return None
        heartbeat: asyncio.Task[None] | None = None
        invocation_supervised_process: SupervisedProcess | None = None
        configured_process_callback = coder_kwargs.get(
            "on_supervised_process_start"
        )

        def retain_invocation_process(managed: SupervisedProcess) -> None:
            nonlocal invocation_supervised_process
            invocation_supervised_process = managed
            if configured_process_callback is not None:
                configured_process_callback(managed)

        run_record = self._current_run_record
        previous_active_invocation = self.state.active_invocation
        previous_publication_attempt = self.state.publication_attempt
        captured_snapshot = None
        try:
            captured_snapshot = self._capture_invocation_snapshot(
                coder_name,
                coder_kwargs,
                phase="coding",
            )
            # Persist the snapshot identity before creating an unfinished
            # metrics checkpoint. Recovery requires the snapshot run_id to
            # finalize that exact record after a crash; the reverse order
            # leaves an unidentifiable checkpoint if the daemon dies between
            # these writes.
            self._begin_publication_attempt(
                started_at=publication_not_before,
                baseline_numbers=publication_baseline,
            )
            await self.publish_state()
            await self._checkpoint_current_run_record()
            heartbeat = asyncio.create_task(
                self._publish_while_waiting("CODING")
            )
            self._coder_invocation_active = True
            cli_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
                plugin.run_auto_pr(
                    self.repo_path,
                    pr_id=pr_id,
                    task_file=task_file,
                    task_body=task_body,
                    **{
                        **coder_kwargs,
                        "on_supervised_process_start": retain_invocation_process,
                    },
                )
            )
        except BaseException:
            if captured_snapshot is not None:
                self.state.active_invocation = previous_active_invocation
                self.state.publication_attempt = previous_publication_attempt
                if (
                    run_record is not None
                    and run_record.invocations
                    and run_record.invocations[-1] == captured_snapshot
                ):
                    run_record.invocations.pop()
                rollback_writes = [self.publish_state()]
                if run_record is not None:
                    rollback_writes.append(self._checkpoint_current_run_record())
                await asyncio.gather(*rollback_writes, return_exceptions=True)
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            self._coder_invocation_active = False
            self._release_coder_credentials()
            raise
        assert heartbeat is not None
        publication_monitors: list[
            asyncio.Task[gh_prs.BranchPublication | None]
        ] = []

        def start_publication_monitor() -> asyncio.Task[
            gh_prs.BranchPublication | None
        ]:
            monitor = asyncio.create_task(
                self._monitor_coding_publication(
                    cli_task,
                    base_branch=base_branch,
                    target_branch=target_branch,
                    baseline_numbers=publication_baseline,
                    not_before=publication_not_before,
                )
            )
            publication_monitors.append(monitor)
            return monitor

        async def refresh_publication() -> gh_prs.BranchPublication | None:
            assert publication_baseline is not None
            publications = await asyncio.to_thread(
                gh_prs.get_branch_publications,
                self.owner_repo,
                base_branch,
                target_branch,
            )
            local_head = await asyncio.to_thread(
                _local_branch_head_sha,
                self.repo_path,
                target_branch,
            )
            return _fresh_ready_publication(
                publications,
                baseline_numbers=publication_baseline,
                not_before=publication_not_before,
                expected_head_sha=local_head,
            )

        async def refresh_as_coder_completes() -> None:
            nonlocal publication_observed
            if self._stop_requested or breach_flag["breached"]:
                return
            try:
                publication_observed = await refresh_publication()
            except Exception as exc:
                self.log_event(
                    "[CODING] Publication recheck failed as coder completed "
                    f"under publication observation: {exc}; using ordinary "
                    "completion handling."
                )
            else:
                if publication_observed is not None:
                    await self._retain_observed_publication(publication_observed)
                    self.log_event(
                        "[CODING] Verified publication as coder completed "
                        "under publication observation."
                    )

        publication_monitor = start_publication_monitor()
        breach_monitor: asyncio.Task[None] | None = None
        if plugin.supports_breach_lifecycle:
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir,
                    breach_run_id,
                    plugin.name,
                    cli_task,
                    breach_flag,
                )
            )
        stop_monitor = asyncio.create_task(self._monitor_stop_request(cli_task))
        result: tuple[int, str, str] | None = None
        cancellation: asyncio.CancelledError | None = None
        interruption: str | None = None
        publication_observed: gh_prs.BranchPublication | None = None
        publication_cancel_requested = False
        publication_result_missing = False
        publication_process_failure: str | None = None
        cleanup_confirmed = False
        try:
            while True:
                completed, _ = await asyncio.wait(
                    {cli_task, publication_monitor},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if cli_task in completed:
                    await refresh_as_coder_completes()
                    break
                publication_observed = publication_monitor.result()
                if publication_observed is None:
                    break
                await self._retain_observed_publication(publication_observed)
                self.log_event(
                    f"[CODING] Verified publication of PR "
                    f"#{publication_observed.number}; allowing coder "
                    "natural-exit grace."
                )
                completed, _ = await asyncio.wait(
                    {cli_task},
                    timeout=self.app_config.daemon.coder_terminate_grace_sec,
                )
                if cli_task in completed:
                    break
                try:
                    publication_observed = await refresh_publication()
                except Exception as exc:
                    publication_observed = None
                    self.log_event(
                        f"[CODING] Publication recheck failed before "
                        f"cleanup: {exc}; coder will continue."
                    )
                if publication_observed is not None:
                    self.log_event(
                        f"[CODING] Publication grace elapsed; "
                        f"terminating owned coder group for PR "
                        f"#{publication_observed.number}."
                    )
                    await self._terminate_current_coder()
                    publication_cancel_requested = cli_task.cancel()
                    break
                self.log_event(
                    "[CODING] Publication no longer matches the local task "
                    "branch after grace; coder will continue under renewed "
                    "publication observation."
                )
                completed, _ = await asyncio.wait(
                    {cli_task},
                    timeout=self.app_config.daemon.fix_poll_interval_sec,
                )
                if cli_task in completed:
                    await refresh_as_coder_completes()
                    break
                publication_monitor = start_publication_monitor()
            result = await cli_task
        except asyncio.CancelledError as exc:
            if self._stop_requested:
                interruption = "stop"
            elif breach_flag["breached"]:
                interruption = "breach"
            elif publication_cancel_requested:
                captured = cancelled_process_result(exc)
                if captured is None:
                    publication_result_missing = True
                else:
                    result = (
                        captured.returncode,
                        captured.stdout.decode("utf-8", errors="replace"),
                        captured.stderr.decode("utf-8", errors="replace"),
                    )
                    publication_process_failure = captured.failure_detail
                    if captured.timed_out:
                        publication_process_failure = (
                            publication_process_failure
                            or "coder timeout overlapped publication cleanup"
                        )
            else:
                cancellation = exc
        finally:
            stop_monitor.cancel()
            for publication_monitor in publication_monitors:
                publication_monitor.cancel()
            if breach_monitor is not None:
                breach_monitor.cancel()
            heartbeat.cancel()
            monitors = [stop_monitor, heartbeat, *publication_monitors]
            if breach_monitor is not None:
                monitors.append(breach_monitor)
            await asyncio.gather(*monitors, return_exceptions=True)
            self._coder_invocation_active = False
            managed = (
                invocation_supervised_process
                or self._current_coder_supervised_process
            )
            supervision_failure = (
                getattr(managed, "supervision_failure", None)
                if managed is not None
                else None
            )
            if (
                publication_process_failure is None
                and isinstance(supervision_failure, ProcessSupervisionError)
            ):
                publication_process_failure = str(supervision_failure)
            if result is not None:
                self._record_unconfirmed_launch_cleanup(result)
            if plugin.supports_breach_lifecycle:
                self._check_late_breach(
                    breach_dir,
                    breach_run_id,
                    plugin.name,
                    breach_flag,
                )
                self._cleanup_breach_marker(breach_dir, breach_run_id)
            try:
                await self._capture_published_attempt(publication_observed)
            finally:
                cleanup_confirmed = await self._confirm_current_coder_cleanup(
                    "CODING completion"
                )

        if cancellation is not None:
            raise cancellation
        if not cleanup_confirmed:
            return None

        if interruption == "stop":
            await self._release_unpublished_attempt(target_branch)
            self.state.user_paused = True
            if current_pr_id is not None:
                self._user_stopped_task_pr_ids.add(current_pr_id)
                await self._suppress_task(
                    current_pr_id,
                    SuppressionReason.OPERATOR_STOPPED,
                    {"source": "stop_requested_during_coder_run"},
                )
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[CODING]",
                reason="user stop requested during coder run",
            )
            await self._save_current_run_record("error")
            self.log_event("[CODING] CODING aborted: user stop requested.")
            return None

        if interruption == "breach":
            await self._release_unpublished_attempt(target_branch)
            await self._record_pre_pause_pr(
                target_branch, "before breach-cancel pause"
            )
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[CODING]",
                reason="in-flight rate limit breach",
            )
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"[CODING] CODING aborted: in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return None

        if breach_flag["breached"]:
            await self._release_unpublished_attempt(target_branch)
            await self._record_pre_pause_pr(
                target_branch, "before late-breach pause"
            )
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[CODING]",
                reason="late in-flight rate limit breach",
            )
            await self._save_current_run_record("rate_limit")
            self.log_event(
                f"[CODING] CODING paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}."
            )
            return None
        if publication_result_missing:
            await self._transition_to_error(
                "Publication handoff cancelled the coder without a captured "
                "process result",
                publish=False,
                log_prefix="[CODING]",
            )
            return None
        assert result is not None
        if publication_observed is None:
            return (*result, None)
        assert publication_baseline is not None

        if publication_process_failure is not None:
            return (
                *result,
                _PublicationResolution(
                    failure=f"Process supervision failed: "
                    f"{publication_process_failure}",
                    cancelled_by_publication=True,
                ),
            )

        try:
            refreshed = await asyncio.to_thread(
                gh_prs.get_branch_publications,
                self.owner_repo,
                base_branch,
                target_branch,
            )
            local_head = await asyncio.to_thread(
                _local_branch_head_sha,
                self.repo_path,
                target_branch,
            )
        except Exception as exc:
            publication = _PublicationResolution(
                failure=exc, cancelled_by_publication=publication_cancel_requested,
            )
        else:
            final_evidence = _fresh_ready_publication(
                refreshed,
                baseline_numbers=publication_baseline,
                not_before=publication_not_before,
                expected_head_sha=local_head,
            )
            if final_evidence is None:
                publication = _PublicationResolution(
                    failure=(
                        "Verified publication changed or disappeared after "
                        "coder cleanup, or its head does not match the local "
                        "task branch"
                    ), cancelled_by_publication=publication_cancel_requested,
                )
            else:
                publication = _PublicationResolution(
                    cancelled_by_publication=publication_cancel_requested,
                )
        return (*result, publication)

    async def _monitor_coding_publication(
        self,
        cli_task: asyncio.Task[tuple[int, str, str]],
        *,
        base_branch: str,
        target_branch: str,
        baseline_numbers: set[int] | None,
        not_before: datetime,
    ) -> gh_prs.BranchPublication | None:
        """Observe a newly-created ready PR without blocking the event loop."""
        if baseline_numbers is None:
            await asyncio.wait({cli_task})
            return None
        while not cli_task.done():
            try:
                publications = await asyncio.to_thread(
                    gh_prs.get_branch_publications,
                    self.owner_repo,
                    base_branch,
                    target_branch,
                )
            except Exception as exc:
                self.log_event(
                    f"[CODING] Publication observation failed for "
                    f"{target_branch!r}: {exc}."
                )
            else:
                local_head = await asyncio.to_thread(
                    _local_branch_head_sha,
                    self.repo_path,
                    target_branch,
                )
                candidate = _fresh_ready_publication(
                    publications,
                    baseline_numbers=baseline_numbers,
                    not_before=not_before,
                    expected_head_sha=local_head,
                )
                if candidate is not None:
                    return candidate
            await asyncio.sleep(self.app_config.daemon.fix_poll_interval_sec)
        return None

    async def _record_pre_pause_pr(
        self,
        target_branch: str,
        log_suffix: str,
    ) -> None:
        """Retain only dispatch-bound publication evidence before pausing."""
        attempt = self.state.publication_attempt
        if attempt is None or attempt.branch != target_branch:
            return
        if await self._resolve_published_attempt() and self.state.current_pr is not None:
            self.log_event(
                f"[CODING] Recorded PR #{self.state.current_pr.number} {log_suffix}."
            )

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
        publication: _PublicationResolution | None = None,
    ) -> None:
        """CLI log save, exit classification, PR lookup, WATCH handoff.

        Either transitions to WATCH (PR found or daemon-created) or
        returns after a state transition to PAUSED, ERROR, or IDLE via
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
            await self._release_unpublished_attempt(target_branch)
            self.state.user_paused = True
            if current_pr_id is not None:
                self._user_stopped_task_pr_ids.add(current_pr_id)
                await self._suppress_task(
                    current_pr_id,
                    SuppressionReason.OPERATOR_STOPPED,
                    {"source": "stop_requested_after_coder_exit"},
                )
            self.state.state = PipelineState.PAUSED
            await self._clear_error_message_on_recovery(
                log_prefix="[CODING]",
                reason="user stop requested after coder exit",
            )
            await self._save_current_run_record("error")
            self.log_event("[CODING] CODING aborted: user stop requested.")
            return True

        await self._save_cli_log(
            stdout, stderr, f"PLANNED PR output [{coder_name}]"
        )
        violations = scan_stdout(f"{stdout}\n{stderr}")
        if violations:
            first = violations[0]
            cause = f"GUARDRAIL: {first.category}: {first.excerpt}"
            for violation in violations:
                self.log_event(
                    f"tier={violation.tier} "
                    f"{violation.category}: {violation.excerpt}",
                    tier="guardrail",
                    kind=violation.category,
                )
            if await pause_for_stop_if_requested():
                return
            protected_pr = self.state.current_pr
            if protected_pr is None and self._publication_attempt_matches_dispatch():
                # Discovery already bounded this candidate to the dispatch.
                # An unavailable exact read must not bypass quarantine.
                protected_pr = self.state.publication_attempt.candidate
            if protected_pr is not None:
                pr_number = protected_pr.number
                self.state.quarantined_prs.add(pr_number)
                task_id = (
                    protected_pr.pr_id
                    or (self.state.current_task.pr_id if self.state.current_task else "")
                )
                await self._suppress_task(
                    task_id,
                    SuppressionReason.GUARDRAIL,
                    {"pr_number": pr_number, "reason_text": cause},
                )
                apply_quarantine_label_for_violation(self, pr_number, first)
            await self._transition_to_error(
                cause,
                publish=False,
                log_prefix="[CODING]",
                cancellation_cause=CancellationCause(
                    category="ERROR",
                    payload={"subsource": "guardrail", "reason_text": cause},
                ),
                commit_task_status=True,
            )
            return
        if not await pause_for_stop_if_requested():
            await self._refresh_user_paused_from_redis()
            if self.state.user_paused:
                self.log_event(
                    "[CODING] User pause persisted during coder exit; "
                    "finishing current run before honoring pause."
                )
        if await pause_for_stop_if_requested():
            return
        escalate_reason = fix_escalation.parse_escalate_marker(stdout)
        if escalate_reason is not None:
            await self._release_unpublished_attempt(target_branch)
            await self._transition_to_error(
                escalate_reason, publish=False, log_prefix="[CODING]",
                cancellation_cause=CancellationCause(
                    category="ERROR", payload={"subsource": "coder_escalate",
                                               "reason_text": escalate_reason},
                ), commit_task_status=True,
            )
            return

        if code != 0 and not (publication and publication.cancelled_by_publication):
            resolved = await self._resolve_published_attempt()
            if resolved and self.state.current_pr is None:
                return
            await self._release_unpublished_attempt(target_branch)
            self._detect_rate_limit(stderr, coder_name=coder_name)
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                await self._clear_error_message_on_recovery(
                    log_prefix="[CODING]",
                    reason="rate-limit pause after coder failure",
                )
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

        if publication is not None:
            if publication.failure is not None:
                failure = publication.failure
                await self._transition_to_error(
                    f"Publication handoff verification failed: {failure}",
                    publish=False,
                    log_prefix="[CODING]",
                    cancellation_cause=(
                        classify_infra_exception(failure)
                        if isinstance(failure, Exception)
                        else None
                    ),
                )
                return
        # A bounded positive identity can be retained despite list omissions.
        if self.state.publication_attempt is not None:
            resolved = await self._resolve_published_attempt()
            if resolved and self.state.current_pr is None:
                return
            if resolved and self.state.current_pr is not None:
                if await pause_for_stop_if_requested():
                    return
                await self._handoff_coding_pr_to_watch(
                    self.state.current_pr, "Retained published PR -> WATCH.",
                )
                return
            await self._diagnose_exit_zero_no_pr(
                target_branch, coder_name, pause_for_stop_if_requested,
            )
            return
        await self._transition_to_error(
            "Missing publication attempt identity", publish=False, log_prefix="[CODING]",
        )

    async def _handoff_coding_pr_to_watch(
        self,
        candidate: PRInfo,
        message: str,
    ) -> None:
        """Apply the shared CODING-to-WATCH bookkeeping for a verified PR."""
        self.state.current_pr = candidate
        if self.state.publication_attempt is not None:
            self.state.publication_attempt.pr_number = candidate.number
            self.state.publication_attempt.pending = False
        self.state.state = PipelineState.WATCH
        self._rehydrate_last_push_at(candidate)
        await self._save_current_run_record("coding_complete")
        self.log_event(message, tier="state", kind="transition")
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
        """Diagnose no-push failures and hold ambiguous publication.

        A pushed branch with an omitted PR is not permission to create again.
        Stop remains authoritative before diagnosing or retaining the attempt.
        """
        local_exists = _local_branch_exists(self.repo_path, target_branch)
        remote_exists = _remote_branch_exists(self.repo_path, target_branch)

        # ``_remote_branch_exists`` can block up to its 30s timeout; a stop
        # pressed during that window must still be honored before either A/B
        # parks the attempt, so the user stop remains authoritative.
        if await pause_for_stop_if_requested():
            return

        if remote_exists is False:
            await self._release_unpublished_attempt(target_branch)
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
                await self._commit_and_park_in_error(
                    message,
                    subsource="no_push_deadlock",
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
            await self._commit_and_park_in_error(
                message,
                subsource="no_push_deadlock",
            )
            return

        await self._transition_to_error(
            f"[{coder_name}] Publication ownership unresolved for {target_branch!r}; "
            "retaining attempt without repeating PR creation",
            publish=False, log_prefix="[CODING]",
        )
