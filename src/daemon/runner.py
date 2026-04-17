"""Per-repository pipeline state machine.

One ``PipelineRunner`` instance exists per connected repository. The daemon
main loop calls ``run_cycle`` once per poll interval; each cycle clones or
fetches the repo, runs a preflight check, and dispatches on the persisted
state (``IDLE``, ``WATCH``, ``HUNG``, ``PAUSED``, or ``ERROR``). Transient states
(``CODING``, ``FIX``, ``MERGE``) are resolved within a single cycle and
never persisted across cycles.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path

import redis.asyncio as aioredis

from src import claude_cli, codex_cli, github_client
from src.config import AppConfig, CoderType, RepoConfig
from src.daemon import scaffolder
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.queue_parser import (
    QueueValidationError,
    get_next_task,
    mark_task_done,
    parse_queue,
    parse_queue_text,
)
from src.retry import retry_transient
from src.usage import OAuthUsageProvider
from src.utils import repo_slug_from_url

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    OTHER = "other"


def _classify_error(context: str) -> ErrorCategory:
    lowered = context.lower()
    if "rate limit" in lowered or re.search(r"\b429\b", lowered):
        return ErrorCategory.RATE_LIMIT
    if "timeout" in lowered:
        return ErrorCategory.TIMEOUT
    return ErrorCategory.OTHER


_TRANSIENT_STATES = {
    PipelineState.CODING,
    PipelineState.FIX,
    PipelineState.MERGE,
}

_HISTORY_LIMIT = 100

# Timeout for ``scripts/ci.sh`` on the auto-commit path. Git probes
# stay at 120s (they should return in milliseconds on a healthy repo),
# but the CI gate runs the user repo's full test suite and 120s is
# too tight for moderate-sized projects — a real test suite exceeding
# the cap would flip the runner to ERROR even when the code is valid,
# reintroducing the operator intervention this safety net is meant to
# eliminate. 30 minutes accommodates realistic CI runs without
# abandoning the upper bound entirely.
_CI_SCRIPT_TIMEOUT_SEC = 1800

# Upper bound on how long an open queue-sync remediation PR may sit
# unresolved before ``_resolve_pending_queue_sync`` escalates to
# ERROR.
_QUEUE_SYNC_MAX_WAIT_SEC = 3600

# After this many consecutive cycles of a dirty working tree,
# ``preflight`` hard-resets the repo to ``origin/{branch}`` and
# returns IDLE instead of ERROR. Without this safety net a single
# interrupted Claude run can leave the runner stuck in ERROR forever.
_DIRTY_CYCLES_BEFORE_AUTO_RESET = 3

# Directory where the statusline hook writes breach marker files.
_BREACH_DIR = "/tmp/pipeline-breach"

# Poll interval for the in-flight breach monitor (seconds).
_BREACH_POLL_SEC = 2


def repo_owner_from_url(url: str) -> str:
    """Return ``owner/repo`` for a GitHub URL."""
    return github_client.get_repo_full_name(url)


# Substring used to detect the one fetch failure we want to tolerate
# before scaffolding has succeeded: ``git fetch origin {branch}`` on a
# remote that does not yet have ``refs/heads/{branch}`` exits with this
# message. Every other fetch failure (auth, network, transport) must
# raise so the runner does not silently proceed with stale local state.
_FETCH_MISSING_REF_NEEDLE = "couldn't find remote ref"


def _git(
    repo_path: str,
    *args: str,
    timeout: int = 30,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run ``git <args>`` in ``repo_path`` with the runner's standard flags.

    Centralises the capture_output/text/timeout/cwd shape repeated across
    every git invocation in this module so call-sites stay readable and
    a single place owns the defaults. ``OSError`` (missing git binary,
    missing cwd, permission errors) is intentionally not caught here —
    callers that cannot tolerate it must translate it to their own
    structured error state.
    """
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
        cwd=repo_path,
    )


def _base_branch_ahead_of_origin(repo_path: str, branch: str) -> bool:
    """Return ``True`` if ``refs/heads/{branch}`` has commits not yet
    on ``refs/remotes/origin/{branch}``.

    Used after a successful fetch to detect the "stranded scaffolding
    commit across restart" state: the base branch has a local commit
    that never reached the remote. Unlike
    ``scaffolder._local_has_unpushed_commits``, this probe compares
    the BASE branch refs directly rather than whatever branch
    ``HEAD`` currently points at, so a legitimate mid-CODING restart
    (HEAD on a feature branch, base branch clean and in sync) does
    not get reset.

    Any probe failure — non-zero exit, non-integer output, or
    ``TimeoutExpired`` on a stalled git invocation — returns
    ``True`` to err on the side of running the scaffold retry
    rather than silently accepting the fs-seeded
    ``_scaffolded=True``. scaffold_repo is idempotent and the retry
    will either push the stranded commit, no-op on a synced repo,
    or defer on a dirty tree. Returning False on a probe error
    would let the runner declare scaffolding done while the remote
    is actually behind, and ``recover_state`` would keep reading
    stale data from ``origin/{branch}:tasks/QUEUE.md``.
    """
    try:
        local = _git(
            repo_path,
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/heads/{branch}",
            check=False,
        )
        if local.returncode != 0:
            # No local base branch yet. The ordinary scaffold retry
            # path will create it (either via checkout or
            # symbolic-ref), so treat this as "needs retry" too.
            return True
        remote = _git(
            repo_path,
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/remotes/origin/{branch}",
            check=False,
        )
        if remote.returncode != 0:
            # Remote ref missing after a "successful" fetch is
            # weird — the missing-ref tolerance upstream in
            # ensure_repo_cloned usually catches this before we get
            # here. Err on the side of retry.
            return True
        ahead = _git(
            repo_path,
            "rev-list",
            "--count",
            f"refs/remotes/origin/{branch}..refs/heads/{branch}",
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        logger.warning(
            "_base_branch_ahead_of_origin: %s timed out; treating "
            "as ahead to force scaffold retry",
            exc.cmd,
        )
        return True

    if ahead.returncode != 0:
        logger.warning(
            "_base_branch_ahead_of_origin: rev-list probe failed "
            "(rc=%s); treating as ahead to force scaffold retry",
            ahead.returncode,
        )
        return True
    try:
        return int(ahead.stdout.strip()) > 0
    except ValueError:
        logger.warning(
            "_base_branch_ahead_of_origin: rev-list produced "
            "non-integer output %r; treating as ahead",
            ahead.stdout,
        )
        return True


def _working_tree_dirty(repo_path: str) -> bool:
    """Return ``True`` if ``git status --porcelain`` reports any change.

    Used by ``ensure_repo_cloned`` to defer scaffolding on a restart
    that finds a partially-scaffolded repo (``_repo_looks_scaffolded``
    is False) but also finds uncommitted edits from an interrupted
    coding cycle. Running ``scaffolder.scaffold_repo`` in that state
    would fail on the upfront ``git checkout {branch}`` and raise
    RuntimeError every cycle, so ``recover_state`` and ``preflight``
    would never get to run and the runner would be stuck reporting
    ``scaffold_repo failed`` instead of the real crash-recovery
    condition. Returning True here tells the caller to defer
    scaffolding until the tree is clean again.

    Any git failure (command error, timeout) returns False so the
    scaffold retry path still runs — a broken git is a separate
    problem that scaffold_repo's own error handling will surface.
    """
    try:
        result = _git(repo_path, "status", "--porcelain")
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return bool(result.stdout.strip())


def _repo_looks_scaffolded(repo_path: str) -> bool:
    """Return ``True`` if ``repo_path`` already contains every file
    that ``scaffolder.scaffold_repo`` would commit upstream.

    A daemon restart on a previously-scaffolded clone must not re-run
    ``scaffolder.scaffold_repo``: its upfront ``git checkout {branch}``
    would fail on the dirty working tree left by an interrupted coding
    cycle, masking the real crash-recovery path and stranding the
    runner in ERROR with a confusing "scaffold_repo failed" message
    instead of letting ``recover_state`` do its job. We infer the
    "already scaffolded" signal from the local filesystem so it
    survives process restarts (``_scaffolded`` itself is in-memory).

    The probe must cover **every** asset scaffold_repo is responsible
    for, not just the three most visible ones — otherwise a partially
    provisioned repo (pre-existing ``AGENTS.md`` + ``tasks/QUEUE.md``
    + ``scripts/ci.sh`` but no ``scripts/make-review-artifacts.sh`` or
    no ``artifacts/`` entry in ``.gitignore``) would permanently skip
    scaffolding on restart, leaving the missing files uncreated and
    letting later artifact generation dirty the working tree until
    ``preflight`` forces ERROR. ``artifacts/`` itself is intentionally
    not checked because it is gitignored and can be deleted at any
    time — scaffold_repo handles the recreate-without-commit case
    idempotently.
    """
    path = Path(repo_path)
    if not path.exists():
        return False
    has_agents = (path / "AGENTS.md").exists() or (path / "CLAUDE.md").exists()
    has_queue = (path / "tasks" / "QUEUE.md").exists()
    has_ci = (path / "scripts" / "ci.sh").exists()
    has_review_artifacts = (
        path / "scripts" / "make-review-artifacts.sh"
    ).exists()
    gitignore = path / ".gitignore"
    has_gitignore_artifacts = (
        gitignore.exists()
        and "artifacts/" in gitignore.read_text().splitlines()
    )
    return (
        has_agents
        and has_queue
        and has_ci
        and has_review_artifacts
        and has_gitignore_artifacts
    )


class PipelineRunner:
    """State machine for one repository."""

    def __init__(
        self,
        repo_config: RepoConfig,
        app_config: AppConfig,
        redis_client: aioredis.Redis,
    ) -> None:
        self.repo_config = repo_config
        self._app_config = app_config
        self.redis = redis_client
        self.name = repo_slug_from_url(repo_config.url)
        self.owner_repo = repo_owner_from_url(repo_config.url)
        self.repo_path = f"/data/repos/{self.name}"
        # Migrate clone path from old basename-only format to owner__repo.
        old_basename = repo_config.url.rstrip("/").rsplit("/", 1)[-1]
        if old_basename.endswith(".git"):
            old_basename = old_basename[:-4]
        old_path = Path(f"/data/repos/{old_basename}")
        new_path = Path(self.repo_path)
        if old_basename != self.name and old_path.exists() and not new_path.exists():
            try:
                result = subprocess.run(
                    ["git", "-C", str(old_path), "remote", "get-url", "origin"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                old_origin = result.stdout.strip()
                if repo_slug_from_url(old_origin) != self.name:
                    logger.warning(
                        "Legacy clone %s has origin %s, expected %s — skipping migration",
                        old_path,
                        old_origin,
                        repo_config.url,
                    )
                else:
                    shutil.move(str(old_path), str(new_path))
                    logger.info("Migrated clone path %s -> %s", old_path, new_path)
            except Exception:
                logger.warning("Could not verify origin for %s — skipping migration", old_path)
        if new_path.exists():
            if not (new_path / ".git").exists():
                logger.warning("Removing non-git directory %s", new_path)
                shutil.rmtree(new_path, ignore_errors=True)
            else:
                try:
                    result = subprocess.run(
                        ["git", "-C", str(new_path), "remote", "get-url", "origin"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                        check=True,
                    )
                    current_origin = result.stdout.strip()
                    if repo_slug_from_url(current_origin) != self.name:
                        logger.warning(
                            "Clone %s has origin %s, expected %s — removing stale clone",
                            new_path,
                            current_origin,
                            repo_config.url,
                        )
                        shutil.rmtree(new_path)
                except Exception:
                    logger.warning("Could not verify origin for %s", new_path)
        self._old_basename = old_basename
        self.state = RepoState(
            url=repo_config.url,
            name=self.name,
            last_updated=datetime.now(timezone.utc),
        )
        # One-shot guard so recover_state runs exactly once per process, on
        # the first cycle after startup. Reconstructing state from QUEUE.md +
        # GitHub on every cycle would clobber in-memory progress made in
        # earlier cycles of the same run.
        self._recovered = False
        # Scaffold retry gate. Seeded from a local-filesystem probe so a
        # daemon restart on an already-scaffolded clone does NOT call
        # ``scaffolder.scaffold_repo`` again: scaffold_repo begins with
        # ``git checkout {branch}``, which fails on the dirty working
        # tree left by an interrupted coding cycle, and that failure
        # would then mask the real crash-recovery path handled by
        # ``recover_state``. When ``_scaffolded`` is False (fresh clone,
        # or a restart on a repo missing scaffolding files),
        # ``ensure_repo_cloned`` calls ``scaffold_repo`` on every cycle
        # until it succeeds and then sets this to True. scaffold_repo
        # itself is idempotent at the remote level: on retry it detects
        # a stranded commit from a prior cycle and re-pushes it; once
        # fully sync'd it is a cheap no-op.
        self._scaffolded = _repo_looks_scaffolded(self.repo_path)
        # Consecutive cycles preflight has observed a dirty working
        # tree. Used to trigger auto-recovery at
        # ``_DIRTY_CYCLES_BEFORE_AUTO_RESET`` so a stuck ERROR state
        # (e.g. Claude CLI crash mid-edit leaving uncommitted files)
        # no longer requires operator intervention. Reset to 0 on any
        # clean preflight or after a successful auto-reset.
        self._consecutive_dirty_cycles = 0
        # Consecutive AI-diagnosis attempts in ``handle_error``. Capped
        # so a persistent ERROR cannot spin forever on infrastructure
        # the CLI cannot classify. Reset on any transition out of
        # ERROR (IDLE/FIX verdicts, successful recovery).
        self._error_diagnose_count = 0
        # Timestamp of the most recent fix push from this runner.
        # Used by ``_has_new_codex_feedback_since_last_push`` so the
        # guard is not tricked by ``current_pr.last_activity`` being
        # overwritten with GitHub's ``updatedAt`` (which advances every
        # time Codex posts, masking whether Codex feedback is fresher
        # than our last push).
        self._last_push_at: datetime | None = None
        # PR number the ``_last_push_at`` timestamp belongs to. Without
        # this, a stale timestamp from a prior PR leaks into freshness
        # checks on a newly-tracked PR — e.g. finishing PR-A and then
        # recovering an older PR-B. Any mismatch forces
        # ``_rehydrate_last_push_at`` to unconditionally replace both
        # fields with the new PR's baseline, instead of keeping the
        # newer-but-wrong timestamp under the "only update if newer"
        # gate.
        self._last_push_at_pr_number: int | None = None
        self._usage_degraded_logged = False
        self._usage_provider = OAuthUsageProvider(
            credentials_path=str(
                Path(self.app_config.auth.claude_config_dir) / ".credentials.json"
            ),
            user_agent=self.app_config.daemon.usage_api_user_agent,
            beta_header=self.app_config.daemon.usage_api_beta_header,
            cache_ttl_sec=self._app_config.daemon.usage_api_cache_ttl_sec,
        )

    @property
    def app_config(self) -> AppConfig:
        return self._app_config

    @app_config.setter
    def app_config(self, value: AppConfig) -> None:
        old = self._app_config
        self._app_config = value
        if (
            value.daemon.usage_api_user_agent != old.daemon.usage_api_user_agent
            or value.daemon.usage_api_beta_header != old.daemon.usage_api_beta_header
            or value.daemon.usage_api_cache_ttl_sec != old.daemon.usage_api_cache_ttl_sec
            or value.auth.claude_config_dir != old.auth.claude_config_dir
        ):
            self._usage_provider = OAuthUsageProvider(
                credentials_path=str(
                    Path(value.auth.claude_config_dir) / ".credentials.json"
                ),
                user_agent=value.daemon.usage_api_user_agent,
                beta_header=value.daemon.usage_api_beta_header,
                cache_ttl_sec=value.daemon.usage_api_cache_ttl_sec,
            )

    def _get_coder(self) -> tuple[str, object]:
        """Return ``(coder_name, coder_module)`` for the active coder.

        Per-repo ``coder`` overrides the daemon-level default.  Returns
        either ``("claude", claude_cli)`` or ``("codex", codex_cli)``.
        """
        coder = self.repo_config.coder or self.app_config.daemon.coder
        if coder == CoderType.CODEX:
            return "codex", codex_cli
        return "claude", claude_cli

    async def publish_state(self) -> None:
        """Serialize ``self.state`` and write it to Redis."""
        self.state.active = self.repo_config.active
        self.state.last_updated = datetime.now(timezone.utc)
        coder = self.repo_config.coder or self.app_config.daemon.coder
        self.state.coder = coder.value
        # Only probe usage for active repos to avoid unnecessary HTTP
        # calls (and potential timeout latency) for idle/inactive repos.
        if self.repo_config.active:
            snap = await asyncio.to_thread(self._usage_provider.fetch)
            if snap is not None:
                self.state.usage_session_percent = snap.session_percent
                self.state.usage_session_resets_at = snap.session_resets_at
                self.state.usage_weekly_percent = snap.weekly_percent
                self.state.usage_weekly_resets_at = snap.weekly_resets_at
            else:
                self.state.usage_session_percent = None
                self.state.usage_session_resets_at = None
                self.state.usage_weekly_percent = None
                self.state.usage_weekly_resets_at = None
            self.state.usage_api_degraded = self._usage_provider.consecutive_failures >= 10
        if not self.repo_config.active:
            data = self.state.model_dump()
            data["state"] = PipelineState.IDLE.value
            payload = RepoState(**data).model_dump_json()
        else:
            payload = self.state.model_dump_json()
        await self.redis.set(f"pipeline:{self.name}", payload)
        if self._old_basename != self.name:
            try:
                old_key = f"pipeline:{self._old_basename}"
                old_data = await self.redis.get(old_key)
                owns_old_key = False
                if old_data:
                    old_state = json.loads(old_data)
                    old_url = old_state.get("url", "")
                    if repo_slug_from_url(old_url) == self.name:
                        await self.redis.delete(old_key)
                        owns_old_key = True
                if owns_old_key:
                    old_upload = f"upload:{self._old_basename}:pending"
                    new_upload = f"upload:{self.name}:pending"
                    if await self.redis.exists(old_upload):
                        await self.redis.renamenx(old_upload, new_upload)
            except Exception:
                pass

    async def _save_cli_log(self, stdout: str, stderr: str, label: str) -> None:
        _MAX_CLI_LOG_BYTES = 64 * 1024  # 64 KB cap per entry
        ts = datetime.now(timezone.utc).isoformat()
        key_latest = f"cli_log:{self.name}:latest"
        key_history = f"cli_log:{self.name}:{ts}"
        marker = "[truncated]\n"
        combined = f"=== STDOUT ===\n{stdout}\n\n=== STDERR ===\n{stderr}"
        raw = combined.encode("utf-8", errors="replace")
        if len(raw) > _MAX_CLI_LOG_BYTES:
            tail_budget = _MAX_CLI_LOG_BYTES - len(marker.encode("utf-8"))
            raw = raw[-tail_budget:]
            combined = marker + raw.decode("utf-8", errors="replace")
        try:
            await self.redis.set(key_latest, combined, ex=3600)
            await self.redis.set(key_history, combined, ex=86400)
        except Exception:
            logger.warning("Failed to save CLI log for %s", self.name)
        if combined.strip():
            first_lines = combined.strip()[:200]
            self.log_event(f"{label}: {first_lines}")

    def log_event(self, event: str) -> None:
        """Append an event to ``state.history`` (capped) and log it."""
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "state": self.state.state.value,
            "event": event,
        }
        self.state.history.append(entry)
        if len(self.state.history) > _HISTORY_LIMIT:
            self.state.history = self.state.history[-_HISTORY_LIMIT:]
        logger.info("[%s] %s", self.name, event)

    async def _publish_while_waiting(self, label: str) -> None:
        """Publish state every 30s while a long-running CLI call is active."""
        while True:
            await asyncio.sleep(30)
            self.log_event(f"{label}...")
            try:
                await self.publish_state()
            except Exception:
                logger.warning("[%s] heartbeat publish failed, will retry", self.name)

    async def ensure_repo_cloned(self) -> None:
        """Clone the repo if missing, otherwise fetch ``origin/{branch}``.

        Also retries scaffolding on every cycle until ``_scaffolded`` is
        set. See ``_scaffolded`` in ``__init__`` for the reasoning.
        """
        path = Path(self.repo_path)
        if not path.exists():
            # ``git clone`` runs before ``self.repo_path`` exists, so it
            # cannot use ``_git`` (which sets ``cwd=repo_path`` and would
            # fail with ``FileNotFoundError`` before git is even invoked).
            def _do_clone() -> None:
                # Remove any partial clone left by a previous failed attempt
                # so git doesn't error with "destination path already exists".
                if path.exists():
                    shutil.rmtree(path)
                subprocess.run(
                    ["git", "clone", self.repo_config.url, self.repo_path],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=True,
                )
            try:
                retry_transient(_do_clone, operation_name="git clone")
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip()
                raise RuntimeError(f"git clone failed: {detail}") from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("git clone timed out") from exc
        else:
            fetch_missing_ref = False
            try:
                retry_transient(
                    lambda: _git(
                        self.repo_path,
                        "fetch",
                        "origin",
                        self.repo_config.branch,
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {self.repo_config.branch}",
                )
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip()
                if _FETCH_MISSING_REF_NEEDLE in detail.lower():
                    # ``origin/{branch}`` does not exist on the remote.
                    # Typical cause: a prior cycle cloned this repo and
                    # committed scaffolding locally, but the initial
                    # push failed transiently so the branch never got
                    # published upstream. Fall through to the scaffold
                    # retry block below which will re-push the stranded
                    # commit. Narrow on the exact needle so auth /
                    # transport / network errors still raise — those
                    # would otherwise let the runner proceed with stale
                    # local ``origin/{branch}`` data and
                    # ``recover_state`` could make decisions from an
                    # outdated queue snapshot.
                    fetch_missing_ref = True
                    self.log_event(
                        f"git fetch: {detail}; will retry scaffold"
                    )
                else:
                    raise RuntimeError(
                        f"git fetch failed: {detail}"
                    ) from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("git fetch timed out") from exc

            if fetch_missing_ref:
                # The only way to get ``origin/{branch}`` published is
                # for scaffold_repo to re-push the local commit. Reset
                # the scaffold gate (which may have been seeded True
                # by ``_repo_looks_scaffolded`` at __init__ because
                # the files are on disk) so the retry block below
                # actually runs.
                self._scaffolded = False
            elif self._scaffolded and _base_branch_ahead_of_origin(
                self.repo_path, self.repo_config.branch
            ):
                # Fetch succeeded and ``origin/{branch}`` does exist
                # upstream, but the local base branch has commits not
                # yet on origin. This is the stranded-scaffold-across-
                # restart case where origin/{branch}``already existed
                # (so the missing-ref path above did not trigger) but
                # a prior cycle's scaffolding push failed while the
                # remote branch was otherwise present. Without this
                # reset, the fs-seeded ``_scaffolded=True`` at
                # ``__init__`` would skip the retry block forever and
                # ``recover_state`` would keep reading stale data from
                # ``origin/{branch}:tasks/QUEUE.md``. scaffold_repo is
                # idempotent at the remote level so a spurious reset
                # degrades to a fast no-op push on the retry.
                self._scaffolded = False
                self.log_event(
                    f"local {self.repo_config.branch} ahead of "
                    "origin, re-running scaffold to re-push stranded "
                    "commits"
                )

        # Scaffold on every cycle until ``_scaffolded`` is set.
        # scaffold_repo is idempotent at both the local and remote
        # level: on a fresh clone it creates the orchestrator files
        # and pushes; on retry after a transient push failure it
        # re-pushes the stranded commit; once fully sync'd it is a
        # cheap no-op. Failures must not be swallowed — we raise
        # ``RuntimeError`` so ``run_cycle`` flips the runner to ERROR
        # with a visible message, and leave ``_scaffolded`` unset so
        # the next cycle retries.
        #
        # BUT: a restart that finds a partially-scaffolded repo
        # (``_repo_looks_scaffolded`` was False so __init__ left
        # ``_scaffolded`` False) AND a dirty working tree from an
        # interrupted coding cycle would raise ``scaffold_repo
        # failed`` every cycle, because ``scaffold_repo`` starts with
        # ``git checkout {branch}`` which hits "Your local changes
        # would be overwritten". That masks the real crash-recovery
        # condition: ``recover_state`` and ``preflight`` never get to
        # run and the runner sits permanently ERROR on a scaffold
        # error instead of the real dirty-tree error. Defer scaffold
        # when the tree is dirty so the state machine can proceed,
        # and let a later cycle retry once the tree has been cleaned
        # up by recovery/fix loops or a manual operator intervention.
        if not self._scaffolded:
            if not path.exists() or not _working_tree_dirty(self.repo_path):
                try:
                    actions = scaffolder.scaffold_repo(
                        self.repo_path, self.repo_config.branch
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"scaffold_repo failed: {exc}"
                    ) from exc
                self._scaffolded = True
                if actions:
                    self.log_event(
                        f"scaffold_repo created: {', '.join(actions)}"
                    )
            else:
                self.log_event(
                    "scaffold_repo deferred: working tree dirty, letting "
                    "recover_state and preflight run first"
                )

        if self._scaffolded and not _working_tree_dirty(self.repo_path):
            try:
                if scaffolder.ensure_claude_md(
                    self.repo_path, self.repo_config.branch
                ):
                    self.log_event("backfilled CLAUDE.md for legacy repo")
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                raise RuntimeError(
                    f"CLAUDE.md backfill failed: {exc}"
                ) from exc

    def sync_to_main(self) -> None:
        """Hard-sync the working tree to ``origin/{branch}``.

        Only safe to call when the runner is IDLE (no active Claude working
        branch to clobber). Uses ``git reset --hard`` instead of ``git pull``
        so that any stray local modifications from a prior crashed cycle are
        discarded deterministically, guaranteeing QUEUE.md and tasks/ reflect
        the tip of the base branch before ``parse_queue`` reads them.

        Raises the underlying ``subprocess`` exception on failure so the
        caller can translate it into ERROR state with appropriate context.
        ``OSError`` (missing git binary, missing cwd) is translated to
        ``RuntimeError`` so it cannot escape to ``daemon.main``'s generic
        handler without the runner's state being updated to ERROR by the
        caller.
        """
        branch = self.repo_config.branch
        try:
            retry_transient(
                lambda: _git(self.repo_path, "fetch", "origin", branch, timeout=60),
                operation_name=f"git fetch origin {branch}",
            )
            _git(self.repo_path, "checkout", branch)
            _git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            # ``git reset --hard`` only discards tracked-file changes;
            # untracked files (e.g. artifacts left by a crashed Claude
            # run) survive and would poison the next preflight as a
            # dirty tree. ``git clean -fd`` after the reset guarantees
            # the working copy truly matches origin/{branch}.
            _git(self.repo_path, "clean", "-fd")
        except OSError as exc:
            raise RuntimeError(f"sync_to_main OS error: {exc}") from exc

    def _parse_base_queue(
        self, *, strict: bool = False
    ) -> list[QueueTask] | None:
        """Return QUEUE.md parsed from ``origin/{branch}``, or ``None``.

        ``recover_state`` runs before ``preflight``, so the working tree
        may be dirty or checked out on a different branch than
        ``repo_config.branch``:

        - A fresh ``git clone`` lands HEAD on the remote's default
          branch (``origin/HEAD``), which may not match the configured
          base branch. ``ensure_repo_cloned`` does not checkout after
          clone, so an un-guarded ``parse_queue`` would read the default
          branch's QUEUE.md and miss in-flight tasks tracked on a
          different configured branch.
        - A crashed prior cycle may have left the tree on a feature
          branch with uncommitted edits.

        Reading via ``git show origin/{branch}:tasks/QUEUE.md`` sidesteps
        both: it yields the authoritative queue snapshot from the
        configured base branch without touching the working tree, so
        recovery stays non-destructive. Returns ``None`` when the read
        fails (ref missing, timeout, tasks/QUEUE.md absent on base),
        letting the caller translate the failure into a retryable ERROR.

        When *strict* is ``True``, ``parse_queue_text`` runs the full
        validation suite (duplicate IDs/branches, missing deps, cycles).
        A ``QueueValidationError`` is propagated to the caller so
        recovery can transition to ``ERROR`` instead of driving
        execution on a malformed queue.
        """
        branch = self.repo_config.branch
        try:
            result = _git(
                self.repo_path,
                "show",
                f"origin/{branch}:tasks/QUEUE.md",
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return None
        return parse_queue_text(result.stdout, strict=strict)

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
        try:
            tasks = self._parse_base_queue(strict=strict)
        except QueueValidationError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"recover_state: queue validation failed: {exc}"
            )
            self.log_event(f"recover_state: queue validation failed: {exc}")
            # Return False so _recovered stays unset and the next cycle
            # retries recovery.  This allows the daemon to self-heal once
            # an operator fixes the queue (e.g. via dashboard upload or
            # push).  run_cycle processes pending uploads even on a failed
            # recovery so the upload path is not blocked.
            return False
        if tasks is None:
            # Read failure on origin/{branch}:tasks/QUEUE.md. Treat as a
            # retryable discovery error: returning False leaves
            # _recovered unset, and the next cycle will try again. A
            # half-recovered state based on an empty or stale queue
            # could otherwise let handle_idle pick new work while an
            # in-flight PR is still open.
            branch = self.repo_config.branch
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"recover_state: read QUEUE.md from origin/{branch} failed"
            )
            self.log_event(
                f"recover_state: read QUEUE.md from origin/{branch} failed"
            )
            return False

        self.state.queue_done = sum(
            1 for t in tasks if t.status == TaskStatus.DONE
        )
        self.state.queue_total = len(tasks)

        doing = next((t for t in tasks if t.status == TaskStatus.DOING), None)

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
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(matching)
                self.log_event(
                    f"Recovered: DOING task {doing.pr_id} "
                    f"-> WATCH PR #{matching.number}"
                )
                return True

            self.state.state = PipelineState.CODING
            self.log_event(
                f"Recovered: DOING task {doing.pr_id}, no PR "
                "-> re-running CODING"
            )
            # Claude's PLANNED PR flow creates the branch from origin/main
            # (per AGENTS.md), which would orphan any unpushed local
            # commits from the crashed run. Push them to origin first so
            # the work is durable — even if Claude then resets the local
            # ref, the commits remain reachable on origin. If the
            # preserve step refuses or fails, stop in ERROR rather than
            # let handle_coding orphan potential crash commits.
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
            # Even if handle_coding left the runner in ERROR, discovery
            # itself completed and must not be retried: re-running
            # handle_coding a second time on the next cycle could create
            # a duplicate PR for the same task.
            return True

        # No DOING task in QUEUE.md. In this workflow a task's status
        # flips TODO -> DONE in a single commit as part of its own
        # implementation PR — QUEUE.md on main never occupies DOING —
        # so the queue's base view shows an in-flight task as TODO until
        # its implementation PR merges. Match open PRs against any
        # queued task (TODO or DONE), not DONE-only: if we miss the
        # TODO case, recovery falls back to clean-slate IDLE and the
        # next cycle's handle_idle re-runs PLANNED PR on the already-
        # open PR, running claude_cli a second time on active work.
        # The queue-match guard still applies: unrelated open PRs
        # (human contributors, dependabot, etc.) whose branch is not
        # in QUEUE.md are ignored so WATCH cannot hijack them.
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
            self.state.state = PipelineState.WATCH
            self._rehydrate_last_push_at(matched_pr)
            self.log_event(
                f"Recovered: {matched_task.status.value} task "
                f"{matched_task.pr_id} -> WATCH PR #{matched_pr.number}"
            )
            return True

        # Clean-slate recovery: no in-flight work to resume. Reset the
        # runner to IDLE explicitly — a prior cycle's failed discovery
        # may have left self.state.state == ERROR with an error_message
        # set, and the WATCH/CODING branches above restore state only on
        # their own code paths. Without this reset, a successful retry
        # that lands in clean-slate would return True and run_cycle
        # would publish the still-ERROR state; the runner would then
        # stop making queue progress (and with error_handler_use_ai
        # disabled, handle_error is a no-op so it would stay stuck).
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

    def preflight(self) -> bool:
        """Return ``True`` iff the working tree is clean."""
        try:
            result = _git(self.repo_path, "status", "--porcelain")
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            # ``OSError`` (missing git binary, missing cwd, permission
            # errors) otherwise escapes to daemon.main's generic
            # handler and leaves the runner state stale.
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"preflight failed: {exc}"
            self.log_event(f"preflight failed: {exc}")
            return False

        dirty = result.stdout.strip()
        if dirty:
            self._consecutive_dirty_cycles += 1
            if self._consecutive_dirty_cycles >= _DIRTY_CYCLES_BEFORE_AUTO_RESET:
                self.log_event(
                    f"Dirty tree persisted {self._consecutive_dirty_cycles} "
                    "cycles, auto-resetting to recover"
                )
                if self._auto_reset_dirty_tree():
                    return True
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"working tree dirty: {dirty}"
            self.log_event("preflight: dirty working tree")
            return False
        self._consecutive_dirty_cycles = 0
        return True

    def _auto_reset_dirty_tree(self) -> bool:
        """Hard-reset the working tree to ``origin/{branch}``.

        Called by ``preflight`` once the consecutive-dirty counter
        crosses ``_DIRTY_CYCLES_BEFORE_AUTO_RESET``. On success the
        runner resumes the state it was in before the dirty-tree
        stall: WATCH when an open PR was being tracked, IDLE
        otherwise. Dropping back to IDLE unconditionally would make
        the next cycle re-pick the still-TODO task from
        ``origin/{base}:tasks/QUEUE.md`` and open a duplicate PR.
        On failure the counter is left untouched and the caller
        falls through to the usual ERROR path.
        """
        branch = self.repo_config.branch
        try:
            # ``--force`` so a dirty PR-branch working tree cannot
            # block the switch back to ``branch``. Without it a failing
            # checkout would leave HEAD on the feature branch while the
            # next ``git reset --hard origin/{branch}`` moves THAT
            # feature branch's tip to ``origin/{branch}``, corrupting
            # the tracked PR branch. Paired with ``check=True`` so any
            # residual checkout failure aborts the reset chain instead
            # of silently proceeding on the wrong ref.
            _git(self.repo_path, "checkout", "--force", branch)
            _git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            _git(self.repo_path, "clean", "-fd")
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            self.log_event(f"Auto-recovery failed: {exc}")
            return False
        self._consecutive_dirty_cycles = 0
        if self.state.current_pr is not None:
            resumed = PipelineState.WATCH
        else:
            resumed = PipelineState.IDLE
        self.state.state = resumed
        self.state.error_message = None
        self.log_event(f"Auto-recovered from dirty tree -> {resumed.value}")
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
        the task targets the base branch (malformed QUEUE.md entry that
        would let Claude reset ``main``) or the preserve push failed in
        a way that may have left commits orphan-only on local.
        """
        # Mirror the hard guard in ``_mark_queue_done``: a malformed
        # QUEUE.md entry with ``Branch: main`` would otherwise cause this
        # method to push straight to the base branch during recovery,
        # bypassing every PR/review gate.
        if branch == self.repo_config.branch:
            self.log_event(
                f"Refusing to preserve crashed-run commits on base "
                f"branch {branch!r}"
            )
            return False

        try:
            probe = _git(
                self.repo_path,
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/heads/{branch}",
                timeout=10,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            # The probe itself failed (git binary missing, repo locked,
            # etc.). We cannot tell whether the local branch holds
            # unpushed commits, so refuse to proceed rather than risk
            # Claude orphaning crash commits.
            self.log_event(
                f"Could not probe local branch {branch}: {exc}"
            )
            return False
        if probe.returncode != 0:
            # Local branch does not exist — the crash happened before
            # Claude's first commit, so there is nothing to preserve.
            return True

        try:
            _git(
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

    async def run_cycle(self) -> None:
        """Advance the state machine by one step."""
        try:
            await self.ensure_repo_cloned()
        except RuntimeError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = str(exc)
            self.log_event(f"ensure_repo_cloned failed: {exc}")
            await self.publish_state()
            return

        if not self._recovered:
            # Run before preflight: a crashed cycle may have left a dirty
            # working tree, and recover_state needs to reconstruct the
            # in-memory state from QUEUE.md + GitHub regardless.
            recovery_complete = await self.recover_state()
            if not recovery_complete:
                # Discovery phase failed (transient GitHub outage or
                # queue validation error).  Leave _recovered unset so the
                # next cycle retries discovery.  Attempt pending uploads
                # so operators can fix a malformed queue via dashboard,
                # but use _safe=True to skip the destructive
                # ``git reset --hard`` error handler in
                # process_pending_uploads — the working tree may contain
                # uncommitted crash-recovery work that must not be
                # discarded.
                has_pending = False
                try:
                    raw = await self.redis.get(f"upload:{self.name}:pending")
                    has_pending = bool(raw)
                except Exception:
                    pass
                if has_pending:
                    branch = self.repo_config.branch
                    on_base = False
                    try:
                        head_ref = _git(
                            self.repo_path, "rev-parse", "--abbrev-ref",
                            "HEAD",
                        ).stdout.strip()
                        if head_ref == branch:
                            on_base = True
                        else:
                            # Non-destructive checkout: do NOT reset/clean
                            # the working tree before switching.  If
                            # checkout fails (dirty tree from a crashed
                            # coding cycle), the upload stays in Redis and
                            # will be applied once recovery succeeds and
                            # handle_idle runs sync_to_main normally.
                            _git(self.repo_path, "checkout", branch)
                            on_base = True
                    except Exception:
                        pass
                    if on_base:
                        await self.process_pending_uploads(_safe=True)
                await self.publish_state()
                return
            self._recovered = True
            # Stop the cycle after publishing the recovered state.
            # Dispatching on the recovery cycle would run claude_cli
            # (via handle_watch -> handle_fix, handle_coding, etc.)
            # against a working tree that preflight has NOT validated —
            # the exact crash case recover_state is built for can
            # legitimately leave leftover edits from a mid-coding
            # interruption, and fix_review would push those into the
            # recovered PR. The next cycle runs preflight normally
            # before any dispatch: a clean tree proceeds, a dirty tree
            # flips to ERROR for operator intervention before any
            # Claude-driven write hits the repo. IDLE recovery also
            # waits for the next cycle, at which point handle_idle's
            # sync_to_main hard-resets any leftover state before
            # parse_queue runs.
            await self.publish_state()
            return

        if not self.preflight():
            await self.publish_state()
            return

        if self.state.state in _TRANSIENT_STATES:
            self.log_event(
                f"resetting stale transient state {self.state.state.value} -> IDLE"
            )
            self.state.state = PipelineState.IDLE

        current = self.state.state
        if current == PipelineState.IDLE:
            await self.handle_idle()
        elif current == PipelineState.WATCH:
            await self.handle_watch()
        elif current == PipelineState.HUNG:
            await self.handle_hung()
        elif current == PipelineState.PAUSED:
            await self.handle_paused()
        elif current == PipelineState.ERROR:
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.log_event("Legacy ERROR + rate_limited_until -> PAUSED")
            elif self.app_config.daemon.error_handler_use_ai:
                await self.handle_error()

        await self.publish_state()

    _DELETE_IF_UNCHANGED_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

    async def _delete_upload_if_unchanged(self, key: str, expected: bytes | str) -> bool:
        """Delete ``key`` only if its value still matches ``expected``."""
        try:
            result = await self.redis.eval(
                self._DELETE_IF_UNCHANGED_LUA, 1, key, expected,
            )
            return bool(result)
        except Exception:
            logger.warning("%s: CAS delete failed for %s, falling back", self.name, key)
            try:
                current = await self.redis.get(key)
                if current == expected:
                    await self.redis.delete(key)
                    return True
            except Exception:
                pass
            return False

    async def process_pending_uploads(
        self, *, _safe: bool = False,
    ) -> bool | None:
        """Commit and push any files staged by the web upload endpoint.

        Returns ``True`` if an upload was pushed, ``False`` if there was
        nothing pending, or ``None`` if a pending upload failed (caller
        should skip task dispatch so it retries next cycle).

        When *_safe* is ``True`` the error handler skips the destructive
        ``git reset --hard origin/{branch}`` cleanup.  This is used by
        the recovery-failure path where the working tree may contain
        uncommitted crash-recovery work that must not be discarded.
        """
        key = f"upload:{self.name}:pending"
        try:
            raw = await self.redis.get(key)
        except Exception:
            logger.warning("%s: Redis error checking pending uploads", self.name)
            return None
        if not raw:
            return False

        try:
            manifest = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.error("%s: corrupt upload manifest, discarding", self.name)
            await self.redis.delete(key)
            return False

        staging_dir = Path(manifest["staging_dir"]) if "staging_dir" in manifest else Path("/data/uploads") / self.name
        filenames: list[str] = manifest.get("files", [])
        if not filenames or not staging_dir.is_dir():
            logger.warning("%s: upload manifest has no files or staging dir missing", self.name)
            await self.redis.delete(key)
            return False

        branch = self.repo_config.branch
        try:
            tasks_dir = Path(self.repo_path) / "tasks"
            tasks_dir.mkdir(exist_ok=True)
            for fname in filenames:
                src = staging_dir / fname
                if src.is_file():
                    shutil.copy2(str(src), str(tasks_dir / fname))

            _git(
                self.repo_path,
                "add",
                *[f"tasks/{fn}" for fn in filenames],
            )
            commit_result = _git(
                self.repo_path,
                "commit",
                "-m",
                "chore: upload sprint tasks via dashboard",
                check=False,
            )
            if commit_result.returncode != 0:
                combined = f"{commit_result.stderr}\n{commit_result.stdout}"
                if "nothing to commit" not in combined:
                    raise RuntimeError(combined.strip())
            retry_transient(
                lambda: _git(self.repo_path, "push", "origin", branch, timeout=60),
                operation_name=f"git push origin {branch}",
            )
            self.log_event(f"Pushed uploaded task files: {filenames}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, RuntimeError) as exc:
            logger.error("%s: upload git operations failed: %s", self.name, exc)
            self.log_event(f"Upload push failed: {exc}")
            if not _safe:
                try:
                    _git(
                        self.repo_path,
                        "reset",
                        "--hard",
                        f"origin/{branch}",
                        check=False,
                    )
                except Exception:
                    pass
            return None

        deleted = await self._delete_upload_if_unchanged(key, raw)
        if deleted:
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            return True

        self.log_event("Newer upload pending; blocking dispatch to process it next cycle")
        return None

    async def handle_idle(self) -> None:
        """Hard-sync to ``origin/{branch}``, pick the next task, hand off."""
        self._error_diagnose_count = 0
        if self.state.pending_queue_sync_branch is not None:
            if not self._resolve_pending_queue_sync():
                return

        # sync_to_main is only safe in IDLE state: it runs git reset --hard
        # on the base branch, which would destroy any in-flight Claude work
        # on a feature branch. We are IDLE here, so that's fine.
        try:
            self.sync_to_main()
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            RuntimeError,
        ) as exc:
            # ``RuntimeError`` covers the translated ``OSError`` path
            # from ``sync_to_main`` (missing git binary, missing cwd).
            # Without it that exception would escape to daemon.main's
            # generic handler and leave the runner state stale.
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"sync_to_main failed: {exc}"
            self.log_event(f"sync_to_main failed: {exc}")
            return

        upload_result = await self.process_pending_uploads()
        if upload_result is None:
            self.log_event("Pending upload failed; skipping task dispatch to retry next cycle")
            return
        if upload_result:
            try:
                self.sync_to_main()
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                RuntimeError,
            ) as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"sync_to_main after upload failed: {exc}"
                self.log_event(f"sync_to_main after upload failed: {exc}")
                return

        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        strict = self.app_config.daemon.strict_queue_validation
        try:
            tasks = parse_queue(queue_path, strict=strict)
        except QueueValidationError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = str(exc)
            self.log_event(f"Queue validation failed: {exc}")
            return
        self.state.queue_done = sum(
            1 for t in tasks if t.status == TaskStatus.DONE
        )
        self.state.queue_total = len(tasks)
        task = get_next_task(tasks)
        if task is None:
            self.log_event("No tasks available")
            try:
                prs = github_client.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
            except Exception as exc:
                self.log_event(f"IDLE: open PR check failed: {exc}")
                self.state.current_pr = None
                return
            if prs:
                # Prefer a PR whose branch matches a DONE task, otherwise
                # take the first open PR.
                done_branches = {
                    t.branch for t in tasks
                    if t.status == TaskStatus.DONE and t.branch
                }
                match = next(
                    (pr for pr in prs if pr.branch in done_branches), None
                )
                self.state.current_pr = match or prs[0]
                self.log_event(
                    f"IDLE: {len(prs)} open PR(s) detected (manual work)"
                )
            else:
                self.state.current_pr = None
            return

        self.state.current_task = task
        task_branch = task.branch

        if task_branch:
            try:
                prs = github_client.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
                existing = next(
                    (p for p in prs if p.branch == task_branch), None
                )
            except Exception as exc:
                self.log_event(
                    f"IDLE: open PR check failed: {exc}; deferring task dispatch"
                )
                self.state.current_task = None
                return

            if existing is not None:
                self.state.current_pr = existing
                self.state.state = PipelineState.WATCH
                self._rehydrate_last_push_at(existing)
                self.log_event(
                    f"Task {task.pr_id} has existing open PR #{existing.number} "
                    f"on {task_branch!r} -> WATCH (no duplicate CODING)"
                )
                await self.publish_state()
                return

        self.state.state = PipelineState.CODING
        self.log_event(f"Picked task {task.pr_id}: {task.title}")
        await self.publish_state()
        await self.handle_coding()

    async def _proactive_usage_check(self) -> bool:
        """Return True if CLI calls are allowed, False if usage threshold breached.

        Fail-open: returns True when the provider cannot reach the endpoint,
        deferring to the reactive _detect_rate_limit on stderr after the CLI run.
        """
        snapshot = await asyncio.to_thread(self._usage_provider.fetch)
        if snapshot is None:
            if (
                self._usage_provider.consecutive_failures >= 10
                and not self._usage_degraded_logged
            ):
                self._usage_degraded_logged = True
                self.log_event(
                    "Usage API degraded (10 consecutive failures), "
                    "falling back to reactive rate-limit detection"
                )
            return True
        self._usage_degraded_logged = False
        session_threshold = self.app_config.daemon.rate_limit_session_pause_percent
        weekly_threshold = self.app_config.daemon.rate_limit_weekly_pause_percent
        breached = None
        resets_at = 0
        if snapshot.session_percent >= session_threshold:
            breached = "session"
            resets_at = snapshot.session_resets_at
        elif snapshot.weekly_percent >= weekly_threshold:
            breached = "weekly"
            resets_at = snapshot.weekly_resets_at
        if breached is None:
            return True
        self.state.rate_limited_until = datetime.fromtimestamp(resets_at, tz=timezone.utc)
        # Only preserve error_message when pausing from ERROR state so
        # handle_paused correctly resumes to ERROR; clear stale error
        # context from non-ERROR states to avoid incorrect ERROR resume.
        if self.state.state != PipelineState.ERROR:
            self.state.error_message = None
        self.state.state = PipelineState.PAUSED
        self.log_event(
            f"Proactive pause: {breached} usage at "
            f"{snapshot.session_percent if breached == 'session' else snapshot.weekly_percent}%, "
            f"resumes at {self.state.rate_limited_until.isoformat()}"
        )
        return False

    async def _check_rate_limit(self) -> bool:
        """Return True if CLI calls are allowed, False if rate-limited."""
        coder = self.repo_config.coder or self.app_config.daemon.coder
        if self.state.rate_limited_until is not None:
            if coder == CoderType.CODEX and not self.state.rate_limit_reactive:
                # Claude-originated pause does not apply to Codex;
                # reactive pauses (from stderr 429s) still apply.
                self.state.rate_limited_until = None
                self.state.rate_limit_reactive = False
                self._usage_provider.invalidate_cache()
                if self.state.state == PipelineState.PAUSED:
                    self.state.state = PipelineState.IDLE
                self.log_event("Codex active, clearing Claude rate-limit pause")
                return True
            if datetime.now(timezone.utc) < self.state.rate_limited_until:
                if self.state.state != PipelineState.PAUSED:
                    self.state.state = PipelineState.PAUSED
                remaining = (self.state.rate_limited_until - datetime.now(timezone.utc)).total_seconds()
                self.log_event(f"Rate limited, resuming in {int(remaining)}s")
                return False
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self._usage_provider.invalidate_cache()
            self.log_event("Rate limit window expired, resuming")
        # Proactive OAuth check only applies to the Claude provider
        if coder == CoderType.CODEX:
            return True
        return await self._proactive_usage_check()

    def _detect_rate_limit(self, stderr: str) -> None:
        """Set rate-limit pause if stderr contains rate-limit signals."""
        session_threshold = self.app_config.daemon.rate_limit_session_pause_percent
        weekly_threshold = self.app_config.daemon.rate_limit_weekly_pause_percent
        lower = stderr.lower()
        triggered = False
        limit_type = "session"

        if re.search(r"\b429\b", stderr):
            triggered = True
            limit_type = "session"

        m = re.search(
            r"(\d{1,3})%\s*(?:of\s+)?(?:your\s+)?(?:(weekly|week|session|5-hour)\s+)?rate\s*limit"
            r"|(?:(weekly|week|session|5-hour)\s+)?rate\s*limit\s+(?:at\s+)?(\d{1,3})%",
            lower,
        )
        if not triggered and m:
            pct = int(m.group(1) or m.group(4))
            qualifier = m.group(2) or m.group(3) or ""
            if qualifier in ("weekly", "week"):
                limit_type = "weekly"
                triggered = pct >= weekly_threshold
            else:
                limit_type = "session"
                triggered = pct >= session_threshold

        if not triggered and not m and "rate limit" in lower:
            if "weekly" in lower or "week" in lower:
                limit_type = "weekly"
            triggered = True

        if triggered:
            pause_min = 30
            self.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=pause_min)
            self.state.rate_limit_reactive = True
            self.log_event(f"Rate limit detected ({limit_type}), pausing for {pause_min} min")

    async def handle_paused(self) -> None:
        """Wait for rate limit window to expire, then resume previous flow."""
        if self.state.rate_limited_until is None:
            self.log_event("PAUSED without rate_limited_until -> IDLE")
            self.state.state = PipelineState.IDLE
            return
        # Codex is unaffected by Claude-originated pauses, but reactive
        # pauses (stderr 429s from Codex itself) must still be honoured.
        coder = self.repo_config.coder or self.app_config.daemon.coder
        if coder == CoderType.CODEX and not self.state.rate_limit_reactive:
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self._usage_provider.invalidate_cache()
            self.state.state = PipelineState.IDLE
            self.log_event("Codex active, clearing Claude rate-limit pause -> IDLE")
            return
        if datetime.now(timezone.utc) < self.state.rate_limited_until:
            remaining = (
                self.state.rate_limited_until - datetime.now(timezone.utc)
            ).total_seconds()
            self.log_event(f"Paused, resuming in {int(remaining)}s")
            return
        # Window expired: resume to appropriate state
        self.state.rate_limited_until = None
        self.state.rate_limit_reactive = False
        self._error_diagnose_count = 0
        if self.state.error_message:
            lowered = self.state.error_message.lower()
            is_rate_limit_msg = (
                "rate limit" in lowered or re.search(r"\b429\b", lowered)
            )
            if is_rate_limit_msg:
                # Legacy rate-limit error messages would deadlock in ERROR
                # because handle_error skips diagnosis for rate-limit text.
                self.state.error_message = None
                self.log_event(
                    "Rate limit expired, cleared legacy rate-limit error"
                )
            else:
                self.state.state = PipelineState.ERROR
                self.log_event(
                    "Rate limit expired, resuming -> ERROR (preserved context)"
                )
                return
        if (
            self.state.current_pr is not None
            and self.state.current_task is not None
            and self.state.current_pr.branch == self.state.current_task.branch
        ):
            self.state.state = PipelineState.WATCH
            self.log_event("Rate limit expired, resuming -> WATCH")
        else:
            self.state.state = PipelineState.IDLE
            self.log_event("Rate limit expired, resuming -> IDLE")

    async def handle_coding(self) -> None:
        """Run ``PLANNED PR`` via the active coder CLI and hand off to WATCH.

        The coder owns the full git workflow per AGENTS.md: branch creation,
        commit, push, and PR creation. The daemon must not pre-create the
        task branch — doing so conflicts with AGENTS.md step 4 ("create
        branch from origin/main"). After the CLI returns 0 we poll GitHub
        for the PR; because the list API is eventually consistent, we
        retry a few times before surfacing an ERROR.
        """
        if not await self._check_rate_limit():
            return

        coder_name, coder_module = self._get_coder()
        self.log_event(f"[{coder_name}] Starting PLANNED PR")

        target_branch = (
            self.state.current_task.branch if self.state.current_task else None
        )
        if not target_branch:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                "Current task has no branch; cannot identify PR"
            )
            self.log_event(self.state.error_message)
            return

        breach_dir, breach_run_id = self._breach_env()
        breach_flag: dict[str, bool] = {"breached": False}

        model = (
            self.app_config.daemon.codex_model
            if coder_name == "codex"
            else self.app_config.daemon.claude_model
        )

        heartbeat = asyncio.create_task(self._publish_while_waiting("CODING"))
        coder_kwargs: dict[str, object] = {
            "model": model,
            "timeout": self.app_config.daemon.planned_pr_timeout_sec,
        }
        if coder_name == "claude":
            coder_kwargs.update(
                breach_dir=breach_dir,
                breach_run_id=breach_run_id,
                session_threshold=self.app_config.daemon.rate_limit_session_pause_percent,
                weekly_threshold=self.app_config.daemon.rate_limit_weekly_pause_percent,
            )
        claude_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
            coder_module.run_planned_pr_async(
                self.repo_path,
                **coder_kwargs,
            )
        )
        breach_monitor: asyncio.Task[None] | None = None
        if coder_name == "claude":
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir, breach_run_id, claude_task, breach_flag,
                )
            )
        try:
            code, stdout, stderr = await claude_task
        except asyncio.CancelledError:
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
                                f"Recorded PR #{match.number} before breach-cancel pause"
                            )
                            break
                    except Exception:
                        pass  # best-effort; the pause is still correct
                    if _attempt < 2:
                        await asyncio.sleep(5)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            self.log_event(
                f"CODING aborted: in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}"
            )
            return
        finally:
            if breach_monitor is not None:
                breach_monitor.cancel()
            heartbeat.cancel()
            if coder_name == "claude":
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
                                f"Recorded PR #{match.number} before late-breach pause"
                            )
                            break
                    except Exception:
                        pass  # best-effort; the pause is still correct
                    if _attempt < 2:
                        await asyncio.sleep(5)
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            self.log_event(
                f"CODING paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}"
            )
            return
        await self._save_cli_log(stdout, stderr, f"PLANNED PR output [{coder_name}]")
        self._detect_rate_limit(stderr)
        if code != 0:
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                self.log_event(
                    f"Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}"
                )
                return
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"{coder_name} exit {code}"
            self.log_event(f"[{coder_name}] CLI failed: {self.state.error_message}")
            return

        candidate = None
        for attempt in range(3):
            try:
                prs = github_client.get_open_prs(
                    self.owner_repo,
                    allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
                )
            except Exception as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"get_open_prs failed: {exc}"
                self.log_event(str(exc))
                return
            candidate = next(
                (pr for pr in prs if pr.branch == target_branch), None
            )
            if candidate is not None:
                break
            if attempt < 2:
                self.log_event(
                    f"PR not found for {target_branch!r}, "
                    f"retrying in 5s ({attempt + 1}/3)"
                )
                await asyncio.sleep(5)

        if candidate is None:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"Claude CLI succeeded but no PR found for branch {target_branch!r}"
            )
            self.log_event(self.state.error_message)
            return

        self.state.current_pr = candidate
        self.state.state = PipelineState.WATCH
        self._rehydrate_last_push_at(candidate)
        self.log_event(f"Opened PR #{candidate.number} -> WATCH")
        self._post_codex_review(candidate.number)

    async def handle_watch(self) -> None:
        """Poll PR status and decide whether to merge, fix, hang, or wait."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            self.log_event("WATCH without current_pr -> IDLE")
            return

        try:
            prs = github_client.get_open_prs(
                self.owner_repo,
                allow_merge_without_checks=self.repo_config.allow_merge_without_checks,
            )
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"get_open_prs failed: {exc}"
            self.log_event(str(exc))
            return

        current_number = self.state.current_pr.number
        found = next((p for p in prs if p.number == current_number), None)
        if found is None:
            self.log_event(f"PR #{current_number} no longer open -> IDLE")
            self.state.current_pr = None
            self.state.current_task = None
            self.state.state = PipelineState.IDLE
            return

        self.state.current_pr = found
        # Retry rehydrate every cycle so a transient commit-time fetch
        # failure during ``recover_state`` doesn't permanently leave
        # ``_last_push_at`` unset (which would default
        # ``_has_new_codex_feedback_since_last_push`` to True and
        # stale-fix loop forever). Also retry on PR-number mismatch:
        # a PR switch whose first rehydrate attempt failed transiently
        # would otherwise keep the previous PR's timestamp indefinitely
        # here, silently skipping legitimate feedback on the new PR.
        if (
            self._last_push_at is None
            or self._last_push_at_pr_number != found.number
        ):
            self._rehydrate_last_push_at(found)

        ci = found.ci_status
        review = found.review_status
        if ci == CIStatus.SUCCESS and review == ReviewStatus.APPROVED:
            if self.repo_config.auto_merge:
                await self.handle_merge()
            else:
                # Honor repositories configured with auto_merge: false.
                # Stay in WATCH so a human can merge manually; handle_watch
                # will return to IDLE on the next cycle once the PR closes.
                self.log_event(
                    f"PR #{found.number} green but auto_merge disabled; "
                    "awaiting manual merge"
                )
            return
        # Fork (cross-repo) PRs can't be fixed locally — the head branch
        # lives on the contributor's remote. Routing them into handle_fix
        # (which would no-op and return to WATCH) creates a poll-rate skip
        # loop that never reaches the timeout/HUNG escalation below. Fall
        # through to the waiting logic instead so fork PRs eventually flip
        # to HUNG and surface for operator action.
        if found.is_cross_repository:
            if ci == CIStatus.FAILURE or review == ReviewStatus.CHANGES_REQUESTED:
                self.log_event(
                    f"PR #{found.number} fork PR cannot be auto-fixed "
                    f"(review={review.value}, ci={ci.value}); "
                    "waiting for review timeout"
                )
        elif ci == CIStatus.FAILURE:
            await self.handle_fix()
            return
        elif review == ReviewStatus.CHANGES_REQUESTED:
            result = self._has_new_codex_feedback_since_last_push()
            if result == FeedbackCheckResult.NEW:
                await self.handle_fix()
                return
            if result == FeedbackCheckResult.UNKNOWN:
                self.log_event(
                    f"PR #{found.number} CHANGES_REQUESTED but feedback check "
                    "failed; staying in WATCH, will retry next cycle"
                )
                return
            # NONE: no new feedback since last push. Don't trigger FIX,
            # but fall through to the timeout check below so a truly
            # stuck CHANGES_REQUESTED state still escalates to HUNG
            # instead of pinning the runner in WATCH indefinitely.
            self.log_event(
                f"PR #{found.number} CHANGES_REQUESTED but no new "
                "Codex feedback since last push; waiting for fresh review"
            )

        # Any remaining combination is a waiting state that should still be
        # subject to the review timeout. This explicitly includes
        # ``APPROVED + ci PENDING``: previously that pair matched none of the
        # branches above and the runner could block on permanently pending CI
        # without ever transitioning to ``HUNG``.
        last_activity = found.last_activity or self.state.last_updated
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        elapsed_min = (now - last_activity).total_seconds() / 60
        # ``RepoConfig.review_timeout_min`` is an optional per-repo
        # override; when unset, fall back to the daemon-level default
        # (``daemon.review_timeout_min``) so PR-016's Settings UI control
        # actually steers hung-detection for every repo that has not
        # opted into a custom timeout.
        timeout_min = (
            self.repo_config.review_timeout_min
            if self.repo_config.review_timeout_min is not None
            else self.app_config.daemon.review_timeout_min
        )
        if elapsed_min >= timeout_min:
            self.state.state = PipelineState.HUNG
            self.log_event(
                f"PR #{found.number} hung after {elapsed_min:.0f}m "
                f"(review={review.value}, ci={ci.value})"
            )
        else:
            self.log_event(
                f"PR #{found.number} waiting "
                f"(review={review.value}, ci={ci.value}, "
                f"{elapsed_min:.0f}/{timeout_min}m)"
            )

    async def _monitor_fix_idle(
        self,
        pr_number: int,
        idle_limit: int,
        target: asyncio.Task,  # type: ignore[type-arg]
        idle_flag: dict[str, bool],
    ) -> None:
        """Cancel *target* if no new push is detected within *idle_limit* seconds."""
        # Prime the SHA tracker so the first poll can detect a change.
        primed = False
        try:
            await asyncio.to_thread(
                github_client.get_branch_last_push_time,
                self.owner_repo, pr_number,
            )
            primed = True
        except github_client.GitHubPollError:
            pass

        poll_interval = min(60, idle_limit)
        # Seed from actual last push time so already-idle PRs don't get
        # a full extra window.  Cap the backdate to leave at least one
        # poll cycle so a fresh FIX start can observe real activity.
        now = time.monotonic()
        head_age = await asyncio.to_thread(
            github_client.get_last_push_age_seconds,
            self.owner_repo, pr_number,
        )
        if head_age is not None:
            backdate = min(head_age, idle_limit - poll_interval)
            last_known_push = now - max(0.0, backdate)
        else:
            last_known_push = now
        while True:
            await asyncio.sleep(poll_interval)
            try:
                latest_push_at = await asyncio.to_thread(
                    github_client.get_branch_last_push_time,
                    self.owner_repo, pr_number,
                )
                if not primed:
                    primed = True
                    if latest_push_at is not None:
                        last_known_push = time.monotonic()
            except github_client.GitHubPollError:
                self.log_event("FIX: GitHub API poll failed, preserving deadline")
                latest_push_at = None
            if latest_push_at is not None and latest_push_at > last_known_push:
                last_known_push = latest_push_at
                self.log_event("FIX: Claude pushed, resetting idle timer")
            elapsed = time.monotonic() - last_known_push
            if elapsed >= idle_limit:
                self.log_event(
                    f"FIX: idle timeout ({idle_limit}s since last push), killing"
                )
                idle_flag["timed_out"] = True
                target.cancel()
                return

    async def _monitor_inflight_breach(
        self,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        """Cancel *claude_task* if the statusline hook writes a breach marker."""
        marker = Path(breach_dir) / f"{run_id}.breach"
        while not claude_task.done():
            if marker.is_file():
                try:
                    data = json.loads(marker.read_text())
                except (OSError, json.JSONDecodeError):
                    await asyncio.sleep(_BREACH_POLL_SEC)
                    continue
                resets_at = data.get("resets_at", 0)
                if resets_at:
                    self.state.rate_limited_until = datetime.fromtimestamp(
                        resets_at, tz=timezone.utc
                    )
                else:
                    self.state.rate_limited_until = (
                        datetime.now(timezone.utc) + timedelta(minutes=30)
                    )
                breach_type = data.get("type", "session")
                pct_key = "session_pct" if breach_type == "session" else "weekly_pct"
                pct_val = data.get(pct_key, "?")
                self.log_event(
                    f"In-flight breach: {breach_type} at {pct_val}%, "
                    f"killing Claude CLI"
                )
                breach_flag["breached"] = True
                claude_task.cancel()
                return
            await asyncio.sleep(_BREACH_POLL_SEC)

    def _breach_env(self) -> tuple[str, str]:
        """Return ``(breach_dir, run_id)`` for an in-flight breach monitor."""
        breach_dir = _BREACH_DIR
        Path(breach_dir).mkdir(parents=True, exist_ok=True)
        run_id = uuid.uuid4().hex[:12]
        return breach_dir, run_id

    def _check_late_breach(
        self, breach_dir: str, run_id: str, breach_flag: dict[str, bool],
    ) -> None:
        """Final synchronous check for a breach marker the poll loop missed."""
        if breach_flag["breached"]:
            return
        marker = Path(breach_dir) / f"{run_id}.breach"
        if not marker.is_file():
            return
        # Retry a few times — the hook may still be writing the file.
        data = None
        for _ in range(3):
            try:
                data = json.loads(marker.read_text())
                break
            except (OSError, json.JSONDecodeError):
                time.sleep(0.1)
        if data is None:
            return
        resets_at = data.get("resets_at", 0)
        if resets_at:
            self.state.rate_limited_until = datetime.fromtimestamp(
                resets_at, tz=timezone.utc
            )
        else:
            self.state.rate_limited_until = (
                datetime.now(timezone.utc) + timedelta(minutes=30)
            )
        breach_type = data.get("type", "session")
        pct_key = "session_pct" if breach_type == "session" else "weekly_pct"
        pct_val = data.get(pct_key, "?")
        self.log_event(
            f"Late in-flight breach detected: {breach_type} at {pct_val}%"
        )
        breach_flag["breached"] = True

    def _cleanup_breach_marker(self, breach_dir: str, run_id: str) -> None:
        """Remove the breach marker file for a completed run."""
        marker = Path(breach_dir) / f"{run_id}.breach"
        try:
            marker.unlink(missing_ok=True)
        except OSError:
            pass

    async def handle_fix(self) -> None:
        """Run ``FIX REVIEW`` via the active coder CLI and return to WATCH."""
        if not await self._check_rate_limit():
            return

        coder_name, coder_module = self._get_coder()

        if (
            self.state.current_pr is not None
            and self.state.current_pr.is_cross_repository
        ):
            self.log_event(
                f"Skipping FIX for cross-repo PR #{self.state.current_pr.number}"
            )
            self.state.state = PipelineState.WATCH
            return

        self.state.state = PipelineState.FIX
        self.log_event(f"[{coder_name}] entering FIX")
        await self.publish_state()

        # ``sync_to_main`` left the repo on the base branch. The coder must run
        # against the PR's HEAD, so refresh the local PR branch from origin
        # before invoking ``fix_review``. fetch + checkout + hard-reset
        # guarantees the local branch matches the remote exactly, avoiding
        # rejected pushes when origin has moved forward (e.g. Codex commit,
        # another operator, or a previous daemon instance pushed).
        #
        # Cross-repo (fork) PRs are skipped: the head branch lives on the
        # contributor's fork, not the daemon's ``origin``, so ``git checkout``
        # would fail with a pathspec error and trap the runner in ERROR for
        # every fork PR. The auto-commit block below already skips push for
        # the same reason — keep the two guards aligned.
        if (
            self.state.current_pr is not None
            and not self.state.current_pr.is_cross_repository
        ):
            branch = self.state.current_pr.branch
            try:
                retry_transient(
                    lambda: _git(
                        self.repo_path,
                        "fetch", "origin",
                        f"+refs/heads/{branch}:refs/remotes/origin/{branch}",
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {branch}",
                )
                _git(self.repo_path, "checkout", branch)
                _git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
                RuntimeError,
            ) as exc:
                stderr = getattr(exc, "stderr", "") or ""
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    f"git refresh {branch} failed: {stderr.strip() or exc}"
                )
                self.log_event(self.state.error_message)
                return

        # Capture HEAD before running Claude so we can detect whether a
        # commit actually happened (PR-050).
        head_before = ""
        try:
            head_before = _git(
                self.repo_path, "rev-parse", "HEAD"
            ).stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
            # Empty string falls through: the post-check will then treat any
            # resolvable HEAD as "changed" (safer bias than treating as no-op).
            pass

        idle_limit = self.app_config.daemon.fix_idle_timeout_sec
        pr_number = (
            self.state.current_pr.number if self.state.current_pr else 0
        )

        breach_dir, breach_run_id = self._breach_env()
        breach_flag: dict[str, bool] = {"breached": False}
        idle_flag: dict[str, bool] = {"timed_out": False}

        model = (
            self.app_config.daemon.codex_model
            if coder_name == "codex"
            else self.app_config.daemon.claude_model
        )

        heartbeat = asyncio.create_task(self._publish_while_waiting("FIX"))
        fix_kwargs: dict[str, object] = {"model": model}
        if coder_name == "claude":
            fix_kwargs.update(
                breach_dir=breach_dir,
                breach_run_id=breach_run_id,
                session_threshold=self.app_config.daemon.rate_limit_session_pause_percent,
                weekly_threshold=self.app_config.daemon.rate_limit_weekly_pause_percent,
            )
        claude_task: asyncio.Task[tuple[int, str, str]] = asyncio.create_task(
            coder_module.fix_review_async(
                self.repo_path,
                **fix_kwargs,
            )
        )
        idle_monitor = asyncio.create_task(
            self._monitor_fix_idle(pr_number, idle_limit, claude_task, idle_flag)
        )
        breach_monitor: asyncio.Task[None] | None = None
        if coder_name == "claude":
            breach_monitor = asyncio.create_task(
                self._monitor_inflight_breach(
                    breach_dir, breach_run_id, claude_task, breach_flag,
                )
            )
        try:
            code, stdout, stderr = await claude_task
        except asyncio.CancelledError:
            if breach_flag["breached"]:
                # Refresh the feedback baseline from HEAD so that any push
                # Claude made before cancellation is accounted for, without
                # unconditionally advancing past real Codex feedback.
                if self.state.current_pr is not None:
                    self._rehydrate_last_push_at(self.state.current_pr)
                    # If Claude pushed before cancellation, trigger a Codex
                    # review so the PR doesn't sit in CHANGES_REQUESTED with
                    # no fresh review request after pause expiry.
                    try:
                        head_now = _git(
                            self.repo_path, "rev-parse", "HEAD"
                        ).stdout.strip()
                    except Exception:
                        head_now = ""
                    if head_before and head_now and head_before != head_now:
                        if not self._post_codex_review(
                            self.state.current_pr.number
                        ):
                            self.state.state = PipelineState.ERROR
                            self.state.error_message = (
                                f"Failed to post @codex review on PR "
                                f"#{self.state.current_pr.number} after "
                                "breach-cancel fix push; manual review "
                                "trigger required"
                            )
                            self.log_event(self.state.error_message)
                            return
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                self.log_event(
                    f"FIX aborted: in-flight rate limit breach, "
                    f"paused until {self.state.rate_limited_until}"
                )
                return
            if not idle_flag["timed_out"]:
                raise
            # Mark that we came from idle timeout; don't return yet so
            # _check_late_breach in finally can detect a marker written
            # near exit.  We re-check breach_flag after the finally block.
            code, stdout, stderr = 1, "", ""
        finally:
            if breach_monitor is not None:
                breach_monitor.cancel()
            idle_monitor.cancel()
            heartbeat.cancel()
            if coder_name == "claude":
                self._check_late_breach(breach_dir, breach_run_id, breach_flag)
                self._cleanup_breach_marker(breach_dir, breach_run_id)
        if breach_flag["breached"]:
            # Refresh the feedback baseline from HEAD so that any push
            # Claude made before the breach is accounted for, without
            # unconditionally advancing past real Codex feedback.
            if self.state.current_pr is not None:
                self._rehydrate_last_push_at(self.state.current_pr)
                # If Claude pushed before the breach, trigger a Codex review
                # so the PR doesn't sit in CHANGES_REQUESTED with no fresh
                # review request after pause expiry.
                try:
                    head_now = _git(
                        self.repo_path, "rev-parse", "HEAD"
                    ).stdout.strip()
                except Exception:
                    head_now = ""
                if head_before and head_now and head_before != head_now:
                    if not self._post_codex_review(
                        self.state.current_pr.number
                    ):
                        self.state.state = PipelineState.ERROR
                        self.state.error_message = (
                            f"Failed to post @codex review on PR "
                            f"#{self.state.current_pr.number} after "
                            "late-breach fix push; manual review "
                            "trigger required"
                        )
                        self.log_event(self.state.error_message)
                        return
            self.state.state = PipelineState.PAUSED
            self.state.error_message = None
            self.log_event(
                f"FIX paused: late in-flight rate limit breach, "
                f"paused until {self.state.rate_limited_until}"
            )
            return
        if idle_flag["timed_out"]:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"FIX idle timeout: no push for {idle_limit}s"
            )
            self.log_event(self.state.error_message)
            await self._save_cli_log("", "", "FIX idle timeout")
            return
        await self._save_cli_log(stdout, stderr, f"FIX REVIEW output [{coder_name}]")
        self._detect_rate_limit(stderr)
        if code != 0:
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.state.error_message = None
                self.log_event(
                    f"Rate limit pause active until "
                    f"{self.state.rate_limited_until.isoformat()}"
                )
                return
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"{coder_name} exit {code}"
            self.log_event(f"[{coder_name}] fix_review failed: {self.state.error_message}")
            return

        # Verify HEAD actually moved before treating as a push (PR-050).
        head_after = ""
        try:
            head_after = _git(
                self.repo_path, "rev-parse", "HEAD"
            ).stdout.strip()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"rev-parse after fix failed: {exc}"
            self.log_event(self.state.error_message)
            return

        if head_before and head_before == head_after:
            # Advance the feedback baseline so that the existing Codex
            # CHANGES_REQUESTED comment is no longer considered "new" by
            # _has_new_codex_feedback_since_last_push().  Without this,
            # a no-op FIX (no commit) would leave _last_push_at stale
            # and handle_watch would re-enter handle_fix on the next poll.
            self._last_push_at = datetime.now(timezone.utc)
            self.state.state = PipelineState.WATCH
            self.log_event(
                "FIX REVIEW exited 0 but HEAD unchanged; "
                "no push, skipping @codex review"
            )
            return

        # HEAD moved: proceed with push accounting and post @codex review.
        push_time = datetime.now(timezone.utc)
        self._last_push_at = push_time
        if self.state.current_pr is not None:
            self._last_push_at_pr_number = self.state.current_pr.number
            self.state.current_pr.push_count += 1
            self.state.current_pr.last_activity = push_time
            iteration = self.state.current_pr.push_count
        else:
            iteration = 0

        self.state.state = PipelineState.WATCH
        self.log_event(f"Fix pushed, iteration #{iteration}")
        # A post failure here must be fatal, unlike after PR creation:
        # the PR is still in ``CHANGES_REQUESTED`` from the prior Codex
        # review, so if we cannot re-request a review, the next
        # ``handle_watch`` cycle will loop right back into
        # ``handle_fix`` and keep pushing fixes without ever waiting
        # on a new review. Surface an ERROR that operators can resolve
        # (e.g. by manually posting ``@codex review``).
        if (
            self.state.current_pr is not None
            and not self._post_codex_review(self.state.current_pr.number)
        ):
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"Failed to post @codex review on PR "
                f"#{self.state.current_pr.number} after fix push; "
                "manual review trigger required to avoid fix/push loop"
            )

    async def handle_merge(self) -> None:
        """Merge the current PR and return to IDLE."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            return

        number = self.state.current_pr.number
        pr_branch = self.state.current_pr.branch
        base = self.repo_config.branch
        # Cross-repo (fork) PRs: the head branch lives on the
        # contributor's fork and is not writable via ``origin``, so
        # local fetch/checkout/push of ``pr_branch`` would fail. Skip
        # the sync and rely on ``gh pr merge`` (which handles forks)
        # below. Mirrors the cross-repo guard in ``handle_fix``.
        if not self.state.current_pr.is_cross_repository:
            try:
                # Fetch both base and the PR head. Without the head
                # refresh, a stale local ``pr_branch`` (e.g. after
                # daemon restart + ``recover_state`` resuming WATCH)
                # causes a non-fast-forward on push and blocks merge
                # even when the remote PR is otherwise mergeable.
                retry_transient(
                    lambda: _git(
                        self.repo_path,
                        "fetch", "origin", base, pr_branch,
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {base} {pr_branch}",
                )
                _git(self.repo_path, "checkout", pr_branch)
                _git(
                    self.repo_path,
                    "reset", "--hard", f"origin/{pr_branch}",
                )
                merge_result = _git(
                    self.repo_path,
                    "merge", f"origin/{base}", "--no-edit",
                    timeout=60, check=False,
                )
                sync_produced_commit = False
                if merge_result.returncode != 0:
                    if "CONFLICT" in (
                        merge_result.stdout + merge_result.stderr
                    ):
                        if not await self._check_rate_limit():
                            _git(
                                self.repo_path,
                                "merge", "--abort",
                                check=False,
                            )
                            return
                        self.log_event(
                            "Merge conflict with main, resolving..."
                        )
                        code, _stdout, _stderr = claude_cli.run_claude(
                            "Resolve all merge conflicts in the working "
                            "tree. Keep both sides where possible. "
                            "Run scripts/ci.sh to verify.",
                            self.repo_path,
                            timeout=300,
                            model=self.app_config.daemon.claude_model,
                        )
                        if code != 0:
                            _git(
                                self.repo_path,
                                "merge", "--abort",
                                check=False,
                            )
                            self.state.state = PipelineState.ERROR
                            self.state.error_message = (
                                "Merge conflict resolution failed"
                            )
                            self.log_event(self.state.error_message)
                            return
                        sync_produced_commit = True
                    else:
                        self.state.state = PipelineState.ERROR
                        self.state.error_message = (
                            f"git merge origin/{base} failed: "
                            f"{merge_result.stderr.strip()}"
                        )
                        self.log_event(self.state.error_message)
                        return
                else:
                    sync_produced_commit = (
                        "Already up to date" not in merge_result.stdout
                    )

                if sync_produced_commit:
                    retry_transient(
                        lambda: _git(
                            self.repo_path,
                            "push", "origin", pr_branch,
                            timeout=60,
                        ),
                        operation_name=f"git push origin {pr_branch}",
                    )
                    # The new commit invalidates any previously observed
                    # green/approved gate state (branch protection may
                    # require up-to-date checks or dismiss approvals on
                    # new commits). Return to WATCH so the next cycle
                    # re-verifies gates against the refreshed HEAD
                    # instead of attempting an immediate merge that
                    # would fail and drop the runner into ERROR.
                    #
                    # Post ``@codex review`` before the transition so
                    # the refreshed HEAD gets a fresh review pass —
                    # without it, ``get_pr_review_status`` still
                    # reports the prior anchor ``+1`` as APPROVED and
                    # the next cycle could merge on stale approval.
                    # Mirror ``handle_fix`` and treat a post failure
                    # as fatal to avoid a silent fix/push loop.
                    self.state.state = PipelineState.WATCH
                    self.log_event(
                        f"Pre-merge sync pushed new commits to PR "
                        f"#{number}; returning to WATCH to re-verify "
                        "gates"
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
                self.log_event(self.state.error_message)
                return

        self.log_event(f"Merging PR #{number}")
        try:
            github_client.merge_pr(self.owner_repo, number)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"merge_pr failed: {exc}"
            self.log_event(str(exc))
            return

        try:
            self._mark_queue_done()
        except Exception as exc:
            self.log_event(f"Warning: queue-sync step failed: {exc}")

        self.state.current_pr = None
        self.state.current_task = None
        self.state.state = PipelineState.IDLE
        self.log_event(f"Merged PR #{number} -> IDLE")

    def _mark_queue_done(self) -> None:
        """Mark the merged task DONE in QUEUE.md.

        Tries a direct push to the base branch first. If that fails
        (e.g. branch protection), falls back to a remediation PR on a
        ``queue-done-{pr_id}`` branch with auto-merge. Sets
        ``pending_queue_sync_branch`` eagerly so ``handle_idle`` gates
        dispatch until the update lands on base.
        """
        if self.state.current_task is None:
            return
        pr_id = self.state.current_task.pr_id
        base = self.repo_config.branch

        slug = re.sub(r"[^a-z0-9-]", "-", pr_id.lower())
        remediation_branch = f"queue-done-{slug}"

        self.state.pending_queue_sync_branch = remediation_branch
        self.state.pending_queue_sync_started_at = datetime.now(timezone.utc)

        try:
            retry_transient(
                lambda: _git(self.repo_path, "fetch", "origin", base),
                operation_name=f"git fetch origin {base}",
            )
            _git(self.repo_path, "checkout", base)
            _git(self.repo_path, "reset", "--hard", f"origin/{base}")

            queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
            if not queue_path.exists():
                self.state.pending_queue_sync_branch = None
                self.state.pending_queue_sync_started_at = None
                return
            content = queue_path.read_text()

            updated = mark_task_done(content, pr_id)
            if updated is None or updated == content:
                self.state.pending_queue_sync_branch = None
                self.state.pending_queue_sync_started_at = None
                return

            queue_path.write_text(updated)
            _git(self.repo_path, "add", "tasks/QUEUE.md")
            _git(self.repo_path, "commit", "-m", f"{pr_id}: mark DONE")

            push_result = _git(
                self.repo_path, "push", "origin", base,
                timeout=60, check=False,
            )
            if push_result.returncode == 0:
                self.state.pending_queue_sync_branch = None
                self.state.pending_queue_sync_started_at = None
                self.log_event(f"Marked {pr_id} DONE in QUEUE.md")
                _git(self.repo_path, "checkout", base, check=False)
                return

            self.log_event(
                f"Direct push to {base} rejected; "
                "falling back to remediation PR"
            )
            _git(self.repo_path, "checkout", "-B", remediation_branch)
            retry_transient(
                lambda: _git(
                    self.repo_path,
                    "push", "--force-with-lease", "-u",
                    "origin", remediation_branch,
                ),
                operation_name=f"git push origin {remediation_branch}",
            )
            github_client.run_gh(
                ["pr", "create",
                 "--base", base,
                 "--head", remediation_branch,
                 "--title", f"{pr_id}: mark DONE in QUEUE.md",
                 "--body",
                 f"Post-merge queue sync for {pr_id} "
                 "(auto-generated by the daemon)."],
                repo=self.owner_repo,
            )
            try:
                github_client.run_gh(
                    ["pr", "merge", remediation_branch,
                     "--squash", "--delete-branch", "--auto"],
                    repo=self.owner_repo,
                )
            except Exception as auto_exc:
                self.log_event(
                    f"queue-sync --auto rejected ({auto_exc}); "
                    "attempting immediate merge"
                )
                try:
                    github_client.run_gh(
                        ["pr", "merge", remediation_branch,
                         "--squash", "--delete-branch"],
                        repo=self.owner_repo,
                    )
                except Exception:
                    self.log_event(
                        "queue-sync immediate merge also failed; "
                        "PR left open for later resolution"
                    )
        except Exception:
            _git(
                self.repo_path,
                "reset", "--hard", f"origin/{base}",
                check=False,
            )
            _git(self.repo_path, "checkout", base, check=False)
            raise

        _git(self.repo_path, "checkout", base, check=False)
        _git(
            self.repo_path,
            "reset", "--hard", f"origin/{base}",
            check=False,
        )
        self.log_event(
            f"Opened queue-done PR for {pr_id} "
            f"(branch {remediation_branch}); awaiting auto-merge"
        )

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
            self.log_event(f"queue-sync PR {branch} view failed: {exc}")
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
            self.log_event(f"Queue-sync PR merged ({branch})")
            return True

        if state == "CLOSED":
            self.state.pending_queue_sync_branch = None
            self.state.pending_queue_sync_started_at = None
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"queue-sync PR {branch} closed without merging"
            )
            self.log_event(self.state.error_message)
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
        self.log_event(self.state.error_message)

    def _post_codex_review(self, pr_number: int) -> bool:
        """Post ``@codex review`` on ``pr_number``.

        Called after PR creation (``handle_coding``) and after every
        fix push (``handle_fix``) so Codex kicks off a review for each
        iteration instead of relying on the GitHub-side Automatic
        Reviews trigger (which we want configured for PR creation only
        to avoid duplicate reviews).

        Skips posting when the PR author already has a recent
        ``@codex review`` comment — Claude's PLANNED PR runbook posts
        that trigger itself and an immediate daemon-side repost would
        queue a duplicate Codex review.

        Returns ``True`` on success and ``False`` on a logged failure.
        The caller decides whether a failure is fatal: after a fix
        push it must be, otherwise the next ``handle_watch`` cycle
        still sees the prior ``CHANGES_REQUESTED`` signal and loops
        back into ``handle_fix`` immediately, pushing a new fix every
        poll interval without ever re-requesting a review. After PR
        creation it can stay a warning because Codex Automatic Reviews
        still fires on the creation event itself.
        """
        try:
            metadata = github_client.get_pr_metadata(
                self.owner_repo, pr_number
            )
            pr_author = metadata.get("author", "")
            head_commit_iso = metadata.get("head_commit_date", "")
            if pr_author and github_client.has_recent_codex_review_request(
                self.owner_repo,
                pr_number,
                pr_author=pr_author,
                within_minutes=5,
                after_iso=head_commit_iso or None,
            ):
                self.log_event(
                    f"Skipping duplicate @codex review on PR #{pr_number}"
                )
                return True
        except Exception as exc:
            self.log_event(
                f"Dedup check failed on PR #{pr_number}: {exc}"
            )

        try:
            github_client.post_comment(
                self.owner_repo, pr_number, "@codex review"
            )
            self.log_event(f"Posted @codex review on PR #{pr_number}")
            return True
        except Exception as exc:
            self.log_event(
                f"Warning: failed to post @codex review on PR "
                f"#{pr_number}: {exc}"
            )
            return False

    async def handle_hung(self) -> None:
        """Nudge the reviewer with ``@codex review`` or give up, per config."""
        if (
            self.app_config.daemon.hung_fallback_codex_review
            and self.state.current_pr is not None
        ):
            try:
                github_client.post_comment(
                    self.owner_repo,
                    self.state.current_pr.number,
                    "@codex review",
                )
            except Exception as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"post_comment failed: {exc}"
                self.log_event(str(exc))
                return
            self.state.current_pr.last_activity = datetime.now(timezone.utc)
            self.state.state = PipelineState.WATCH
            self.log_event("posted @codex review -> WATCH")
            return

        # With fallback disabled, check whether the operator has already
        # resolved the PR (closed or merged).  If so, transition to IDLE
        # so queue progress can resume.
        if self.state.current_pr is not None:
            try:
                result = github_client.run_gh(
                    [
                        "pr",
                        "view",
                        str(self.state.current_pr.number),
                        "--json",
                        "state",
                    ],
                    repo=self.owner_repo,
                )
            except Exception as exc:
                self.log_event(
                    f"hung: failed to check PR state: {exc}; staying HUNG"
                )
                return

            pr_state = ""
            if isinstance(result, dict):
                pr_state = str(result.get("state") or "").upper()

            if pr_state in ("MERGED", "CLOSED"):
                self.log_event(
                    f"PR #{self.state.current_pr.number} {pr_state} "
                    "by operator -> IDLE"
                )
                self.state.current_pr = None
                self.state.current_task = None
                self.state.state = PipelineState.IDLE
                return

        self.log_event(
            "hung fallback disabled; leaving runner in HUNG for operator action. "
            "Resolve the PR manually or re-enable hung_fallback_codex_review."
        )
        # state remains HUNG, current_pr and current_task preserved

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
        # Different PR: unconditionally replace. The "only update if
        # newer" rule below is only safe when both timestamps belong
        # to the same PR — otherwise a stale last_push_at from a
        # previously-tracked PR would leak into the new PR's
        # freshness check and silently skip legitimate feedback.
        # When the fetch fails on a switch we clear rather than keep
        # the previous PR's value: a None baseline lets the next
        # handle_watch cycle retry the rehydrate and, in the
        # meantime, ``_has_new_codex_feedback_since_last_push``
        # returns True so one fix attempt runs (and then
        # handle_fix's own push sets a proper baseline).
        if self._last_push_at_pr_number != pr.number:
            self._last_push_at = head_time
            self._last_push_at_pr_number = pr.number
            return
        if head_time is None:
            return
        if self._last_push_at is None or head_time > self._last_push_at:
            self._last_push_at = head_time

    def _has_new_codex_feedback_since_last_push(self) -> FeedbackCheckResult:
        """Check whether Codex posted any comment after ``self._last_push_at``.

        Returns a three-state :class:`FeedbackCheckResult`:
        - ``NEW``     – new Codex feedback exists after last push
        - ``NONE``    – no Codex activity after last push
        - ``UNKNOWN`` – API call failed; caller should stay in WATCH
        """
        if self.state.current_pr is None:
            return FeedbackCheckResult.NONE
        last_activity = self._last_push_at
        if last_activity is None:
            return FeedbackCheckResult.NEW
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        try:
            comments = github_client._gh_api_paginated(
                f"repos/{self.owner_repo}/issues/"
                f"{self.state.current_pr.number}/comments"
            ) or []
            review_comments = github_client._gh_api_paginated(
                f"repos/{self.owner_repo}/pulls/"
                f"{self.state.current_pr.number}/comments"
            ) or []
        except Exception:
            logger.warning(
                "GitHub API error checking Codex feedback for PR #%s; "
                "returning UNKNOWN",
                self.state.current_pr.number,
                exc_info=True,
            )
            return FeedbackCheckResult.UNKNOWN
        for c in reversed(comments + review_comments):
            user = (c.get("user") or {}).get("login", "")
            if "codex" not in user.lower():
                continue
            created = github_client._parse_iso(c.get("created_at"))
            if created is None:
                continue
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            if created > last_activity:
                return FeedbackCheckResult.NEW
        return FeedbackCheckResult.NONE

    async def handle_error(self, error_context: str | None = None) -> None:
        """Ask the claude CLI whether to FIX, SKIP, or ESCALATE the error."""
        if not await self._check_rate_limit():
            return

        context = error_context or self.state.error_message or "Unknown error"
        category = _classify_error(context)
        if category == ErrorCategory.RATE_LIMIT:
            self.log_event("Skipping AI diagnosis for rate-limit error")
            return
        if category == ErrorCategory.TIMEOUT:
            self.log_event(
                "Skipping AI diagnosis for timeout error; "
                "will retry on next cycle"
            )
            return
        self._error_diagnose_count += 1
        if self._error_diagnose_count > 3:
            self.log_event(
                "diagnose_error: max attempts (3) reached, staying ERROR"
            )
            return
        code, stdout, stderr = await claude_cli.diagnose_error_async(
            self.repo_path, context, model=self.app_config.daemon.claude_model
        )
        self._detect_rate_limit(stderr)
        if self.state.rate_limited_until is not None:
            self.state.state = PipelineState.PAUSED
            # Preserve error_message so handle_paused resumes to ERROR
            self.log_event(
                f"Rate limit pause active until "
                f"{self.state.rate_limited_until.isoformat()}"
            )
            return
        if code != 0:
            self.log_event(
                f"diagnose_error CLI failed: {stderr.strip() or f'exit {code}'}"
            )
            return

        verdict = claude_cli.parse_diagnosis(stdout)
        if verdict == "SKIP":
            self.state.current_task = None
            self.state.current_pr = None
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            self._error_diagnose_count = 0
            self.log_event("diagnose_error: SKIP -> IDLE")
        elif verdict == "FIX":
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            self._error_diagnose_count = 0
            summary = stdout.strip().splitlines()[-1] if stdout.strip() else ""
            self.log_event(f"diagnose_error: FIX -> IDLE ({summary[:80]})")
        else:  # ESCALATE
            self.log_event("diagnose_error: ESCALATE, keeping ERROR state")
