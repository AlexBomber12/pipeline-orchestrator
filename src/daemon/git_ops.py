"""Module-level git helper functions.

Stateless functions extracted from ``runner.py`` during PR-057 decomposition.
These are not methods — they take explicit arguments and have no ``self``.

Functions:
    repo_owner_from_url — extract ``owner/repo`` from a GitHub URL
    _git              — run ``git <args>`` with standard flags
    _base_branch_ahead_of_origin — check local branch vs remote
    _working_tree_dirty — check working tree for uncommitted changes
    _repo_looks_scaffolded — check if repo has scaffolder-created files
"""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

from src import github_client

logger = logging.getLogger(__name__)

# Substring used to detect the one fetch failure we want to tolerate
# before scaffolding has succeeded: ``git fetch origin {branch}`` on a
# remote that does not yet have ``refs/heads/{branch}`` exits with this
# message. Every other fetch failure (auth, network, transport) must
# raise so the runner does not silently proceed with stale local state.
_FETCH_MISSING_REF_NEEDLE = "couldn't find remote ref"


def repo_owner_from_url(url: str) -> str:
    """Return ``owner/repo`` for a GitHub URL."""
    return github_client.get_repo_full_name(url)


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
