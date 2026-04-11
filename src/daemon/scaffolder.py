"""Bootstrap pipeline orchestrator structure inside a freshly cloned repo.

When the daemon clones a repository for the first time it may land on a
repo that does not yet have the directories and helper files the pipeline
runbook expects (``AGENTS.md``, ``tasks/QUEUE.md``, ``scripts/ci.sh``,
``scripts/make-review-artifacts.sh``, ``artifacts/``). ``scaffold_repo``
fills those gaps from the bundled ``templates/`` directory, commits the
additions, and pushes them to the same branch. It is idempotent: on every
subsequent call it skips anything already present and returns an empty
action list without creating a commit.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"

_GITIGNORE_ENTRY = "artifacts/"
_COMMIT_MESSAGE = "chore: initialize pipeline orchestrator structure"

# Timeouts for git subprocess calls, mirroring the limits used elsewhere
# in the daemon (see ``PipelineRunner``). Without an explicit timeout a
# stalled network operation or an auth prompt during the first-clone
# scaffolding path could block ``ensure_repo_cloned`` indefinitely and
# freeze that runner cycle. Local operations (checkout/add/commit)
# finish quickly, so 30s is generous; push is the one call that crosses
# the network and gets the higher 120s ceiling.
_LOCAL_GIT_TIMEOUT = 30
_PUSH_GIT_TIMEOUT = 120


def _copy_template(src_name: str, dest: Path, executable: bool = False) -> None:
    """Copy ``templates/<src_name>`` to ``dest`` and chmod if requested."""
    shutil.copyfile(TEMPLATES_DIR / src_name, dest)
    if executable:
        dest.chmod(0o755)


def _run_git(
    repo_path: str,
    *args: str,
    timeout: int = _LOCAL_GIT_TIMEOUT,
) -> subprocess.CompletedProcess[str]:
    """Run a git command inside ``repo_path`` and capture output.

    ``timeout`` is required (defaulting to the local-operation limit);
    network-facing calls like ``push`` must pass the higher push limit
    explicitly so a stalled remote cannot hang the runner cycle.
    """
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        cwd=repo_path,
        check=True,
        timeout=timeout,
    )


def _head_is_unborn(repo_path: str) -> bool:
    """Return ``True`` if ``repo_path`` has no commits on any branch.

    A freshly cloned empty repository leaves ``HEAD`` pointing at a
    symbolic ref that does not yet resolve to a commit. ``git rev-parse
    --verify HEAD`` returns non-zero in that state. We run it with
    ``check=False`` so the caller can branch on the result without
    catching an exception.
    """
    result = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", "HEAD"],
        capture_output=True,
        text=True,
        cwd=repo_path,
        check=False,
        timeout=_LOCAL_GIT_TIMEOUT,
    )
    return result.returncode != 0


def _local_has_unpushed_commits(repo_path: str, branch: str) -> bool:
    """Return ``True`` if the local branch is ahead of ``origin/{branch}``.

    Two situations both count as "ahead":

    - ``refs/remotes/origin/{branch}`` does not exist at all: the
      initial scaffolding push never created the branch upstream.
      Typically this means a prior cycle committed locally but the
      push failed transiently (timeout, auth blip). If ``HEAD`` has
      any commit we must push to publish it; if ``HEAD`` is also
      unborn there is nothing to push yet.
    - ``refs/remotes/origin/{branch}`` exists but ``git rev-list
      --count origin/{branch}..HEAD`` is greater than zero: a stranded
      commit from a prior cycle never reached the remote.

    Both probes use ``check=False`` so missing refs and transient git
    failures don't raise out of the idempotency check itself.
    """
    remote_ref = f"refs/remotes/origin/{branch}"
    exists = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", remote_ref],
        capture_output=True,
        text=True,
        cwd=repo_path,
        check=False,
        timeout=_LOCAL_GIT_TIMEOUT,
    )
    if exists.returncode != 0:
        return not _head_is_unborn(repo_path)

    ahead = subprocess.run(
        ["git", "rev-list", "--count", f"{remote_ref}..HEAD"],
        capture_output=True,
        text=True,
        cwd=repo_path,
        check=False,
        timeout=_LOCAL_GIT_TIMEOUT,
    )
    if ahead.returncode != 0:
        return False
    try:
        return int(ahead.stdout.strip()) > 0
    except ValueError:
        return False


def scaffold_repo(repo_path: str, branch: str) -> list[str]:
    """Create any missing pipeline orchestrator files in ``repo_path``.

    ``branch`` is the configured base branch for the repository (from
    ``repo_config.branch``) — scaffolding is checked out, committed, and
    pushed against it rather than whatever branch ``git clone`` happened
    to land on. A fresh clone leaves ``HEAD`` on the remote's default
    branch, which is not necessarily the configured base branch; without
    an explicit checkout the scaffolding commit can end up on the wrong
    branch, leaving ``origin/{branch}`` without ``tasks/QUEUE.md`` and
    later recovery/preflight logic reading stale state.

    Returns the list of relative paths created or edited. If any of
    those entries are trackable by git (i.e. not just empty
    directories), the new state is staged, committed, and pushed to
    ``branch``. Returns an empty list (and performs no git writes) when
    nothing needed to change, keeping repeated calls idempotent.
    """
    # Check out the configured base branch before inspecting the working
    # tree. On a fresh clone the auto-created local branch may differ
    # from ``branch``; passing the branch name to ``git checkout``
    # creates a local tracking branch if one does not yet exist. If the
    # tree already sits on ``branch`` this is a cheap no-op.
    #
    # Empty-repo onboarding: when the daemon clones a brand-new GitHub
    # repository with no commits, ``HEAD`` is unborn and ``git checkout
    # <branch>`` fails with ``pathspec ... did not match`` because no
    # refs exist yet. Catching that failure and checking for unborn
    # ``HEAD`` lets us recover by pointing ``HEAD`` at the configured
    # branch via ``symbolic-ref``; the scaffolding commit below then
    # materializes the branch, and the push publishes it upstream.
    try:
        _run_git(repo_path, "checkout", branch)
    except subprocess.CalledProcessError:
        if not _head_is_unborn(repo_path):
            # Non-empty repo where the configured branch genuinely
            # does not exist locally or on origin — that is a real
            # misconfiguration and must surface as an error.
            raise
        _run_git(
            repo_path,
            "symbolic-ref",
            "HEAD",
            f"refs/heads/{branch}",
        )

    repo = Path(repo_path)
    created: list[str] = []

    agents = repo / "AGENTS.md"
    claude = repo / "CLAUDE.md"
    if not agents.exists() and not claude.exists():
        _copy_template("AGENTS.md", agents)
        created.append("AGENTS.md")

    tasks_dir = repo / "tasks"
    if not tasks_dir.exists():
        tasks_dir.mkdir(parents=True)
        created.append("tasks/")
    queue = tasks_dir / "QUEUE.md"
    if not queue.exists():
        _copy_template("QUEUE.md", queue)
        created.append("tasks/QUEUE.md")

    scripts_dir = repo / "scripts"
    if not scripts_dir.exists():
        scripts_dir.mkdir(parents=True)
        created.append("scripts/")
    ci_script = scripts_dir / "ci.sh"
    if not ci_script.exists():
        _copy_template("ci.sh", ci_script, executable=True)
        created.append("scripts/ci.sh")
    review_script = scripts_dir / "make-review-artifacts.sh"
    if not review_script.exists():
        _copy_template(
            "make-review-artifacts.sh", review_script, executable=True
        )
        created.append("scripts/make-review-artifacts.sh")

    artifacts_dir = repo / "artifacts"
    if not artifacts_dir.exists():
        artifacts_dir.mkdir(parents=True)
        created.append("artifacts/")

    gitignore = repo / ".gitignore"
    gitignore_touched = False
    if gitignore.exists():
        existing = gitignore.read_text()
        lines = existing.splitlines()
        if _GITIGNORE_ENTRY not in lines:
            suffix = "" if existing.endswith("\n") or existing == "" else "\n"
            gitignore.write_text(existing + suffix + _GITIGNORE_ENTRY + "\n")
            gitignore_touched = True
    else:
        gitignore.write_text(_GITIGNORE_ENTRY + "\n")
        gitignore_touched = True
    if gitignore_touched:
        created.append(".gitignore")

    # Stage only the concrete files and edits we produced. Directory
    # entries in ``created`` are kept for the returned log but are not
    # passed to ``git add`` directly — git tracks files, not empty
    # directories, and new directories become visible through the files
    # inside them that we also created above. ``artifacts/`` in
    # particular is an empty directory covered by the ``.gitignore``
    # entry, so there is nothing for git to track there.
    to_stage = [path for path in created if not path.endswith("/")]

    if to_stage:
        # New scaffolding to commit. Running ``git add``/``commit`` on
        # an empty ``to_stage`` is skipped because commit would fail
        # with "nothing to commit" — covered by the else branch
        # (``_local_has_unpushed_commits``) which still runs and may
        # push a stranded commit.
        try:
            _run_git(repo_path, "add", *to_stage)
            _run_git(repo_path, "commit", "-m", _COMMIT_MESSAGE)
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            logger.warning("scaffold_repo git commit failed: %s", detail)
            raise
        except subprocess.TimeoutExpired as exc:
            logger.warning("scaffold_repo git commit timed out: %s", exc.cmd)
            raise

    # Push whenever the local branch is ahead of ``origin/{branch}``.
    # This single probe covers three distinct situations:
    #
    # - Happy path: we just committed new scaffolding and need to
    #   publish it.
    # - Retry path: a prior cycle committed scaffolding but the push
    #   failed transiently (timeout, auth blip). The files are
    #   already on disk and the commit is already in place so
    #   ``created`` may be empty this time, but ``origin/{branch}``
    #   is still missing the scaffolding commit. Re-push the stranded
    #   commit so ``_parse_base_queue`` can read
    #   ``origin/{branch}:tasks/QUEUE.md`` on the next cycle.
    # - No-op path: the repo is fully provisioned locally and already
    #   in sync with the remote — skip the push entirely, preserving
    #   scaffold_repo's idempotent "do nothing on a clean repo"
    #   promise.
    if _local_has_unpushed_commits(repo_path, branch):
        try:
            _run_git(
                repo_path,
                "push",
                "origin",
                branch,
                timeout=_PUSH_GIT_TIMEOUT,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            logger.warning("scaffold_repo git push failed: %s", detail)
            raise
        except subprocess.TimeoutExpired as exc:
            logger.warning("scaffold_repo git push timed out: %s", exc.cmd)
            raise

    if created:
        logger.info("scaffold_repo created: %s", ", ".join(created))
    return created
