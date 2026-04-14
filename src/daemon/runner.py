"""Per-repository pipeline state machine.

One ``PipelineRunner`` instance exists per connected repository. The daemon
main loop calls ``run_cycle`` once per poll interval; each cycle clones or
fetches the repo, runs a preflight check, and dispatches on the persisted
state (``IDLE``, ``WATCH``, ``HUNG``, or ``ERROR``). Transient states
(``CODING``, ``FIX``, ``MERGE``) are resolved within a single cycle and
never persisted across cycles.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis

from src import claude_cli, github_client
from src.config import AppConfig, RepoConfig
from src.daemon import scaffolder
from src.models import (
    CIStatus,
    PipelineState,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.queue_parser import (
    get_next_task,
    mark_task_done,
    parse_queue,
    parse_queue_text,
)
from src.utils import repo_name_from_url

logger = logging.getLogger(__name__)

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
# ERROR. Without this bound, a permanently-open queue-sync PR (stuck
# checks, stuck review, etc.) would keep ``handle_idle`` from ever
# selecting a new task again for this repo, starving the queue.
# Sized generously enough to absorb normal review + CI cycles on
# slow repos without false escalations.
_QUEUE_SYNC_MAX_WAIT_SEC = 3600


def repo_owner_from_url(url: str) -> str:
    """Return ``owner/repo`` for a GitHub URL."""
    return github_client.get_repo_full_name(url)


# Substring used to detect the one fetch failure we want to tolerate
# before scaffolding has succeeded: ``git fetch origin {branch}`` on a
# remote that does not yet have ``refs/heads/{branch}`` exits with this
# message. Every other fetch failure (auth, network, transport) must
# raise so the runner does not silently proceed with stale local state.
_FETCH_MISSING_REF_NEEDLE = "couldn't find remote ref"


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
        local = subprocess.run(
            [
                "git",
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/heads/{branch}",
            ],
            capture_output=True,
            text=True,
            cwd=repo_path,
            check=False,
            timeout=30,
        )
        if local.returncode != 0:
            # No local base branch yet. The ordinary scaffold retry
            # path will create it (either via checkout or
            # symbolic-ref), so treat this as "needs retry" too.
            return True
        remote = subprocess.run(
            [
                "git",
                "rev-parse",
                "--verify",
                "--quiet",
                f"refs/remotes/origin/{branch}",
            ],
            capture_output=True,
            text=True,
            cwd=repo_path,
            check=False,
            timeout=30,
        )
        if remote.returncode != 0:
            # Remote ref missing after a "successful" fetch is
            # weird — the missing-ref tolerance upstream in
            # ensure_repo_cloned usually catches this before we get
            # here. Err on the side of retry.
            return True
        ahead = subprocess.run(
            [
                "git",
                "rev-list",
                "--count",
                f"refs/remotes/origin/{branch}..refs/heads/{branch}",
            ],
            capture_output=True,
            text=True,
            cwd=repo_path,
            check=False,
            timeout=30,
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
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            cwd=repo_path,
            check=True,
            timeout=30,
        )
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
        self.app_config = app_config
        self.redis = redis_client
        self.name = repo_name_from_url(repo_config.url)
        self.owner_repo = repo_owner_from_url(repo_config.url)
        self.repo_path = f"/data/repos/{self.name}"
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

    async def publish_state(self) -> None:
        """Serialize ``self.state`` and write it to Redis."""
        self.state.active = self.repo_config.active
        self.state.last_updated = datetime.now(timezone.utc)
        if not self.repo_config.active:
            data = self.state.model_dump()
            data["state"] = PipelineState.IDLE.value
            payload = RepoState(**data).model_dump_json()
        else:
            payload = self.state.model_dump_json()
        await self.redis.set(f"pipeline:{self.name}", payload)

    async def _save_cli_log(self, stdout: str, label: str) -> None:
        _MAX_CLI_LOG_BYTES = 64 * 1024  # 64 KB cap per entry
        ts = datetime.now(timezone.utc).isoformat()
        key_latest = f"cli_log:{self.name}:latest"
        key_history = f"cli_log:{self.name}:{ts}"
        marker = "[truncated]\n"
        raw = stdout.encode("utf-8", errors="replace")
        if len(raw) > _MAX_CLI_LOG_BYTES:
            tail_budget = _MAX_CLI_LOG_BYTES - len(marker.encode("utf-8"))
            raw = raw[-tail_budget:]
            stdout = marker + raw.decode("utf-8", errors="replace")
        try:
            await self.redis.set(key_latest, stdout, ex=3600)
            await self.redis.set(key_history, stdout, ex=86400)
        except Exception:
            logger.warning("Failed to save CLI log for %s", self.name)
        if stdout.strip():
            first_lines = stdout.strip()[:200]
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

    async def ensure_repo_cloned(self) -> None:
        """Clone the repo if missing, otherwise fetch ``origin/{branch}``.

        Also retries scaffolding on every cycle until ``_scaffolded`` is
        set. See ``_scaffolded`` in ``__init__`` for the reasoning.
        """
        path = Path(self.repo_path)
        if not path.exists():
            try:
                subprocess.run(
                    ["git", "clone", self.repo_config.url, self.repo_path],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=True,
                )
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip()
                raise RuntimeError(f"git clone failed: {detail}") from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("git clone timed out") from exc
        else:
            fetch_missing_ref = False
            try:
                subprocess.run(
                    ["git", "fetch", "origin", self.repo_config.branch],
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=True,
                    cwd=self.repo_path,
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

    def sync_to_main(self) -> None:
        """Hard-sync the working tree to ``origin/{branch}``.

        Only safe to call when the runner is IDLE (no active Claude working
        branch to clobber). Uses ``git reset --hard`` instead of ``git pull``
        so that any stray local modifications from a prior crashed cycle are
        discarded deterministically, guaranteeing QUEUE.md and tasks/ reflect
        the tip of the base branch before ``parse_queue`` reads them.

        Raises the underlying ``subprocess`` exception on failure so the
        caller can translate it into ERROR state with appropriate context.
        """
        branch = self.repo_config.branch
        subprocess.run(
            ["git", "fetch", "origin", branch],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
            cwd=self.repo_path,
        )
        subprocess.run(
            ["git", "checkout", branch],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
            cwd=self.repo_path,
        )
        subprocess.run(
            ["git", "reset", "--hard", f"origin/{branch}"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
            cwd=self.repo_path,
        )
        # ``git reset --hard`` only discards tracked-file changes; untracked
        # files (e.g. artifacts left by a crashed Claude run) survive and
        # would poison the next preflight as a dirty tree. ``git clean -fd``
        # removes them so the working copy truly matches origin/{branch}.
        subprocess.run(
            ["git", "clean", "-fd"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
            cwd=self.repo_path,
        )

    def _parse_base_queue(self) -> list[QueueTask] | None:
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
        """
        branch = self.repo_config.branch
        try:
            result = subprocess.run(
                ["git", "show", f"origin/{branch}:tasks/QUEUE.md"],
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
                cwd=self.repo_path,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return None
        return parse_queue_text(result.stdout)

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
        second, non-idempotent recovery attempt). Returns ``False`` only
        when discovery itself could not run — typically a transient
        GitHub outage during ``get_open_prs`` — so ``run_cycle`` can
        leave ``_recovered`` unset and retry on the next cycle. Without
        this distinction, a transient GitHub error at startup would
        strand the runner detached from an in-flight PR and later allow
        ``handle_error`` to SKIP/FIX it onto new queue work.
        """
        tasks = self._parse_base_queue()
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
            prs = github_client.get_open_prs(self.owner_repo)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"recover_state: get_open_prs failed: {exc}"
            self.log_event(f"recover_state failed: {exc}")
            return False

        # Rebuild ``pending_queue_sync_branch`` from any open
        # ``queue-done-*`` PR on the remote. The marker lives on
        # ``RepoState``, which is re-created empty at startup, so
        # without this step a daemon restart between ``_mark_queue_done``
        # opening the remediation PR and that PR merging would skip
        # ``_resolve_pending_queue_sync`` in ``handle_idle`` and let
        # the just-merged task be re-picked before ``origin/{base}``
        # has the DONE update.
        pending_sync = next(
            (p for p in prs if (p.branch or "").startswith("queue-done-")),
            None,
        )
        if pending_sync is not None:
            self.state.pending_queue_sync_branch = pending_sync.branch
            # Prefer the PR's original creation time so restart does
            # not silently reset the wait window; fall back to "now"
            # when the PRInfo payload did not carry an activity
            # timestamp.
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
            result = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
                cwd=self.repo_path,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"preflight failed: {exc}"
            self.log_event(f"preflight failed: {exc}")
            return False

        dirty = result.stdout.strip()
        if dirty:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"working tree dirty: {dirty}"
            self.log_event("preflight: dirty working tree")
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
        the task targets the base branch (malformed QUEUE.md entry that
        would let Claude reset ``main``) or the preserve push failed in
        a way that may have left commits orphan-only on local.
        """
        # Mirror the hard guard in ``_commit_and_push_dirty``: a malformed
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
            probe = subprocess.run(
                [
                    "git",
                    "rev-parse",
                    "--verify",
                    "--quiet",
                    f"refs/heads/{branch}",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=self.repo_path,
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
            subprocess.run(
                ["git", "push", "origin", f"{branch}:{branch}"],
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
                cwd=self.repo_path,
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

    def _commit_and_push_dirty(self, message: str, expected_branch: str) -> bool:
        """Commit and push any uncommitted changes left in the working tree.

        Claude CLI runs (``run_planned_pr``, ``fix_review``) are supposed
        to commit and push their own work, but occasionally exit 0 while
        leaving edits uncommitted. Without this safety net, the next
        cycle's ``preflight`` flips the runner to ERROR with "working
        tree dirty" and operator intervention is needed. Running
        ``scripts/ci.sh`` before committing ensures we never push broken
        code as part of the auto-commit path.

        ``expected_branch`` is the branch HEAD must be on for the push
        to be safe. ``handle_idle`` hard-syncs to ``main`` before
        handing off to CODING, so a Claude CLI run that exits 0 without
        switching branches would otherwise cause this method to commit
        straight onto ``main`` and push it upstream — bypassing every
        PR / review gate in the pipeline. Validating HEAD against the
        caller's expected branch catches that class of failure before
        any write hits the repo.

        Returns ``True`` after a successful commit + push, ``False``
        when the tree is already clean (nothing to do) or when an error
        has been translated into ``ERROR`` state for the caller to bail
        on.
        """
        # Hard guard: a malformed queue entry with ``Branch:`` set to
        # the configured base branch (e.g. ``Branch: main``) would pass
        # the HEAD-equals-expected-branch check below — HEAD is on
        # ``main`` after ``sync_to_main``, and expected_branch is also
        # ``main`` — letting the method commit + push to the base
        # branch directly, bypassing every PR/review gate. Refuse
        # unconditionally before any git/ci.sh work runs.
        if expected_branch == self.repo_config.branch:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"auto-commit aborted: refusing to push to base branch "
                f"{expected_branch!r}"
            )
            self.log_event(self.state.error_message)
            return False

        try:
            status = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
                cwd=self.repo_path,
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            # ``OSError`` covers ``FileNotFoundError`` (cwd missing, git
            # binary missing) and ``PermissionError``. Without it those
            # escape as unhandled exceptions and bypass the structured
            # ERROR-state translation the caller relies on.
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"auto-commit git status failed: {exc}"
            self.log_event(self.state.error_message)
            return False

        if not status.stdout.strip():
            return False

        try:
            head = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
                cwd=self.repo_path,
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"auto-commit git rev-parse failed: {exc}"
            )
            self.log_event(self.state.error_message)
            return False

        current_branch = head.stdout.strip()
        if current_branch != expected_branch:
            # Refuse to commit/push when HEAD is on the wrong branch.
            # The most dangerous instance is HEAD still on ``main``
            # from ``sync_to_main`` because Claude CLI exited 0 without
            # creating the feature branch; pushing that would publish
            # uncommitted local edits straight to the base branch.
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"auto-commit aborted: HEAD on {current_branch!r}, "
                f"expected {expected_branch!r}"
            )
            self.log_event(self.state.error_message)
            return False

        try:
            subprocess.run(
                ["scripts/ci.sh"],
                capture_output=True,
                text=True,
                timeout=_CI_SCRIPT_TIMEOUT_SEC,
                check=True,
                cwd=self.repo_path,
            )
        except subprocess.CalledProcessError:
            self.state.state = PipelineState.ERROR
            self.state.error_message = "CI failed on auto-commit"
            self.log_event("CI failed on auto-commit")
            return False
        except subprocess.TimeoutExpired as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"auto-commit ci.sh timed out: {exc}"
            self.log_event(self.state.error_message)
            return False
        except OSError as exc:
            # ``scripts/ci.sh`` missing or non-executable in the dirty
            # tree we are about to auto-commit. Distinct from
            # ``CalledProcessError`` (script ran and exited non-zero) —
            # the script never executed, so the "CI failed" phrasing
            # would be misleading.
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"auto-commit ci.sh could not run: {exc}"
            self.log_event(self.state.error_message)
            return False

        try:
            subprocess.run(
                ["git", "add", "-A"],
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
                cwd=self.repo_path,
            )
            subprocess.run(
                ["git", "commit", "-m", message],
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
                cwd=self.repo_path,
            )
            subprocess.run(
                # Push the validated branch by name rather than ``HEAD``
                # so a pre-push hook that re-points HEAD mid-operation
                # still cannot divert the push onto the base branch.
                ["git", "push", "origin", f"{expected_branch}:{expected_branch}"],
                capture_output=True,
                text=True,
                timeout=120,
                check=True,
                cwd=self.repo_path,
            )
        except (
            subprocess.CalledProcessError,
            subprocess.TimeoutExpired,
            OSError,
        ) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"auto-commit git operation failed: {exc}"
            self.log_event(self.state.error_message)
            return False

        self.log_event(f"auto-committed and pushed: {message}")
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
                # Discovery phase failed transiently (e.g. GitHub
                # unreachable). Leave _recovered unset so the next cycle
                # retries discovery before the runner drifts away from an
                # in-flight PR.
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
        elif current == PipelineState.ERROR:
            if self.app_config.daemon.error_handler_use_ai:
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

    async def process_pending_uploads(self) -> bool | None:
        """Commit and push any files staged by the web upload endpoint.

        Returns ``True`` if an upload was pushed, ``False`` if there was
        nothing pending, or ``None`` if a pending upload failed (caller
        should skip task dispatch so it retries next cycle).
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

            subprocess.run(
                ["git", "add"] + [f"tasks/{fn}" for fn in filenames],
                cwd=self.repo_path,
                capture_output=True, text=True, timeout=30, check=True,
            )
            commit_result = subprocess.run(
                ["git", "commit", "-m", "chore: upload sprint tasks via dashboard"],
                cwd=self.repo_path,
                capture_output=True, text=True, timeout=30, check=False,
            )
            if commit_result.returncode != 0:
                combined = f"{commit_result.stderr}\n{commit_result.stdout}"
                if "nothing to commit" not in combined:
                    raise RuntimeError(combined.strip())
            subprocess.run(
                ["git", "push", "origin", branch],
                cwd=self.repo_path,
                capture_output=True, text=True, timeout=60, check=True,
            )
            self.log_event(f"Pushed uploaded task files: {filenames}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, RuntimeError) as exc:
            logger.error("%s: upload git operations failed: %s", self.name, exc)
            self.log_event(f"Upload push failed: {exc}")
            try:
                subprocess.run(
                    ["git", "reset", "--hard", f"origin/{branch}"],
                    cwd=self.repo_path,
                    capture_output=True, text=True, timeout=30, check=False,
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
        # A queue-sync remediation PR may still be unmerged from a prior
        # cycle. Resolve it before picking a new task so the daemon does
        # not re-run a task whose DONE status has not yet landed on
        # ``origin/{base}``.
        if self.state.pending_queue_sync_branch is not None:
            if not self._resolve_pending_queue_sync():
                return

        # sync_to_main is only safe in IDLE state: it runs git reset --hard
        # on the base branch, which would destroy any in-flight Claude work
        # on a feature branch. We are IDLE here, so that's fine.
        try:
            self.sync_to_main()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
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
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"sync_to_main after upload failed: {exc}"
                self.log_event(f"sync_to_main after upload failed: {exc}")
                return

        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        tasks = parse_queue(queue_path)
        self.state.queue_done = sum(
            1 for t in tasks if t.status == TaskStatus.DONE
        )
        self.state.queue_total = len(tasks)
        task = get_next_task(tasks)
        if task is None:
            self.log_event("No tasks available")
            try:
                prs = github_client.get_open_prs(self.owner_repo)
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
        self.state.state = PipelineState.CODING
        self.log_event(f"Picked task {task.pr_id}: {task.title}")
        await self.publish_state()
        await self.handle_coding()

    async def handle_coding(self) -> None:
        """Run ``PLANNED PR`` via the claude CLI and hand off to WATCH.

        Claude owns the full git workflow per AGENTS.md: branch creation,
        commit, push, and PR creation. The daemon must not pre-create the
        task branch — doing so conflicts with AGENTS.md step 4 ("create
        branch from origin/main"). After the CLI returns 0 we poll GitHub
        for the PR; because the list API is eventually consistent, we
        retry a few times before surfacing an ERROR.
        """
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

        code, stdout, stderr = claude_cli.run_planned_pr(self.repo_path)
        await self._save_cli_log(stdout, "PLANNED PR output")
        if code != 0:
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"claude exit {code}"
            self.log_event(f"claude CLI failed: {self.state.error_message}")
            return

        candidate = None
        for attempt in range(3):
            try:
                prs = github_client.get_open_prs(self.owner_repo)
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
        self.log_event(f"Opened PR #{candidate.number} -> WATCH")
        self._post_codex_review(candidate.number)

    async def handle_watch(self) -> None:
        """Poll PR status and decide whether to merge, fix, hang, or wait."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            self.log_event("WATCH without current_pr -> IDLE")
            return

        try:
            prs = github_client.get_open_prs(self.owner_repo)
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
        if review == ReviewStatus.CHANGES_REQUESTED or ci == CIStatus.FAILURE:
            await self.handle_fix()
            return

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

    async def handle_fix(self) -> None:
        """Run ``FIX REVIEW`` via the claude CLI and return to WATCH."""
        self.state.state = PipelineState.FIX
        self.log_event("entering FIX")
        await self.publish_state()

        # ``sync_to_main`` left the repo on the base branch. Claude must run
        # against the PR's HEAD, so check out the PR branch before invoking
        # ``fix_review``; otherwise Claude would patch base and the auto-commit
        # safety net would refuse to push (or worse, push to the base branch).
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
            try:
                subprocess.run(
                    ["git", "checkout", self.state.current_pr.branch],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True,
                    cwd=self.repo_path,
                )
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                # Cover non-zero exit (CalledProcessError), I/O stalls or lock
                # contention exceeding 30s (TimeoutExpired), and missing git
                # binary / unreadable cwd (OSError). Without these, the bare
                # exception escapes run_cycle and the runner never publishes
                # a clear FIX-stage ERROR.
                stderr = getattr(exc, "stderr", "") or ""
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    f"git checkout {self.state.current_pr.branch} failed: "
                    f"{stderr.strip() or exc}"
                )
                self.log_event(self.state.error_message)
                return

        code, stdout, stderr = claude_cli.fix_review(self.repo_path)
        await self._save_cli_log(stdout, "FIX REVIEW output")
        if code != 0:
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"claude exit {code}"
            self.log_event(f"fix_review failed: {self.state.error_message}")
            return

        if self.state.current_pr is not None:
            self.state.current_pr.push_count += 1
            self.state.current_pr.last_activity = datetime.now(timezone.utc)
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
            # Opening the remediation PR failed before auto-merge even
            # had a chance to complete. Parking the task in ERROR so
            # the operator resolves the remediation is preferable to
            # clearing state and risking re-pick of the merged task on
            # the next cycle.
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"queue-sync failed: {exc}"
            self.log_event(f"queue-sync failed: {exc}")
            return

        self.state.current_pr = None
        self.state.current_task = None
        self.state.state = PipelineState.IDLE
        self.log_event(f"Merged PR #{number} -> IDLE")

    def _mark_queue_done(self) -> None:
        """Open a remediation PR flipping the merged task's QUEUE.md
        status to ``DONE`` and record it in ``pending_queue_sync_branch``
        for asynchronous resolution.

        The runner elsewhere (``_commit_and_push_dirty``,
        ``_preserve_crashed_run_commits``) refuses to push directly to
        the base branch. Routing the queue-sync change through a
        dedicated ``queue-done-{pr_id}`` branch + PR + squash auto-merge
        keeps that invariant and works under branch protection.

        This method does not block waiting for the remediation PR to
        merge. ``handle_idle`` polls ``pending_queue_sync_branch`` each
        cycle via ``_resolve_pending_queue_sync`` and defers task
        selection until the queue-sync PR lands (or closes/times out).
        Waiting here synchronously would stall every other repo's cycle
        on one repo's branch-protection checks, so the wait is
        distributed across cycles instead.
        """
        if self.state.current_task is None:
            return
        pr_id = self.state.current_task.pr_id
        base = self.repo_config.branch

        subprocess.run(
            ["git", "fetch", "origin", base],
            capture_output=True, text=True, timeout=30,
            check=True, cwd=self.repo_path,
        )
        subprocess.run(
            ["git", "checkout", base],
            capture_output=True, text=True, timeout=30,
            check=True, cwd=self.repo_path,
        )
        subprocess.run(
            ["git", "reset", "--hard", f"origin/{base}"],
            capture_output=True, text=True, timeout=30,
            check=True, cwd=self.repo_path,
        )

        queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
        if not queue_path.exists():
            return
        content = queue_path.read_text()

        updated = mark_task_done(content, pr_id)
        if updated is None or updated == content:
            return

        import re as _re
        slug = _re.sub(r"[^a-z0-9-]", "-", pr_id.lower())
        remediation_branch = f"queue-done-{slug}"

        try:
            subprocess.run(
                ["git", "checkout", "-B", remediation_branch],
                capture_output=True, text=True, timeout=30,
                check=True, cwd=self.repo_path,
            )
            queue_path.write_text(updated)
            subprocess.run(
                ["git", "add", "tasks/QUEUE.md"],
                capture_output=True, text=True, timeout=30,
                check=True, cwd=self.repo_path,
            )
            subprocess.run(
                ["git", "commit", "-m", f"{pr_id}: mark DONE"],
                capture_output=True, text=True, timeout=30,
                check=True, cwd=self.repo_path,
            )
            subprocess.run(
                ["git", "push", "--force-with-lease", "-u",
                 "origin", remediation_branch],
                capture_output=True, text=True, timeout=30,
                check=True, cwd=self.repo_path,
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
                # ``--auto`` is rejected in repos where GitHub auto-merge
                # is disabled at the repo level. Fall back to an
                # immediate merge; if that also fails (checks pending,
                # required review, etc.), leave the PR open and let
                # ``_resolve_pending_queue_sync`` gate the runner until
                # operator intervention or checks clear.
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
                except Exception as merge_exc:
                    self.log_event(
                        f"queue-sync immediate merge also failed "
                        f"({merge_exc}); PR left open for later "
                        "resolution"
                    )
        except Exception:
            # Reset the tree and return to base so a partial
            # write/commit/push cannot poison the next preflight. Use
            # check=False so reset/checkout errors do not mask the
            # original failure that the caller logs.
            subprocess.run(
                ["git", "reset", "--hard", f"origin/{base}"],
                capture_output=True, text=True, timeout=30,
                check=False, cwd=self.repo_path,
            )
            subprocess.run(
                ["git", "checkout", base],
                capture_output=True, text=True, timeout=30,
                check=False, cwd=self.repo_path,
            )
            raise

        subprocess.run(
            ["git", "checkout", base],
            capture_output=True, text=True, timeout=30,
            check=True, cwd=self.repo_path,
        )
        self.state.pending_queue_sync_branch = remediation_branch
        self.state.pending_queue_sync_started_at = datetime.now(timezone.utc)
        self.log_event(
            f"Opened queue-done PR for {pr_id} "
            f"(branch {remediation_branch}); awaiting auto-merge"
        )

    def _resolve_pending_queue_sync(self) -> bool:
        """Poll the outstanding queue-sync PR and gate IDLE dispatch.

        Returns ``True`` when the remediation is resolved (merged, or
        no-longer pending for any reason) and the caller should proceed
        with normal IDLE logic. Returns ``False`` when the PR is still
        open — the runner stays IDLE without selecting a new task so
        the merged task does not get re-picked while ``origin/{base}``
        has not yet received the DONE update.

        Runs once per cycle so a slow branch-protection check in one
        repo does not block the daemon's sequential loop across other
        repos.
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
            self.log_event(
                f"queue-sync PR {branch} view failed: {exc}"
            )
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
            # Closed without merging (e.g. operator declined). Clear
            # the marker so the runner is not permanently blocked;
            # log and flip to ERROR so the operator notices before any
            # duplicate work happens.
            self.state.pending_queue_sync_branch = None
            self.state.pending_queue_sync_started_at = None
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"queue-sync PR {branch} closed without merging"
            )
            self.log_event(self.state.error_message)
            return False

        # OPEN / auto-merge waiting on checks. Escalate to ERROR if
        # the PR has been pending past ``_QUEUE_SYNC_MAX_WAIT_SEC``;
        # without a deadline a stuck remediation PR (failed checks,
        # missing required approval) would keep the runner from ever
        # selecting new tasks for this repo, starving the queue.
        started = self.state.pending_queue_sync_started_at
        if started is not None:
            elapsed = (
                datetime.now(timezone.utc) - started
            ).total_seconds()
            if elapsed > _QUEUE_SYNC_MAX_WAIT_SEC:
                self.state.pending_queue_sync_branch = None
                self.state.pending_queue_sync_started_at = None
                self.state.state = PipelineState.ERROR
                self.state.error_message = (
                    f"queue-sync PR {branch} still open after "
                    f"{int(elapsed)}s (max {_QUEUE_SYNC_MAX_WAIT_SEC}s)"
                )
                self.log_event(self.state.error_message)
                return False

        return False

    def _post_codex_review(self, pr_number: int) -> bool:
        """Post ``@codex review`` on ``pr_number``.

        Called after PR creation (``handle_coding``) and after every
        fix push (``handle_fix``) so Codex kicks off a review for each
        iteration instead of relying on the GitHub-side Automatic
        Reviews trigger (which we want configured for PR creation only
        to avoid duplicate reviews).

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

        self.log_event("hung fallback disabled, skipping")
        self.state.current_pr = None
        self.state.current_task = None
        self.state.state = PipelineState.IDLE

    async def handle_error(self, error_context: str | None = None) -> None:
        """Ask the claude CLI whether to FIX, SKIP, or ESCALATE the error."""
        context = error_context or self.state.error_message or "Unknown error"
        code, stdout, stderr = claude_cli.diagnose_error(self.repo_path, context)
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
            self.log_event("diagnose_error: SKIP -> IDLE")
        elif verdict == "FIX":
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            summary = stdout.strip().splitlines()[-1] if stdout.strip() else ""
            self.log_event(f"diagnose_error: FIX -> IDLE ({summary[:80]})")
        else:  # ESCALATE
            self.log_event("diagnose_error: ESCALATE, keeping ERROR state")
