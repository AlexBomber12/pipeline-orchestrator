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


def _copy_template(src_name: str, dest: Path, executable: bool = False) -> None:
    """Copy ``templates/<src_name>`` to ``dest`` and chmod if requested."""
    shutil.copyfile(TEMPLATES_DIR / src_name, dest)
    if executable:
        dest.chmod(0o755)


def _run_git(repo_path: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run a git command inside ``repo_path`` and capture output."""
    return subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        cwd=repo_path,
        check=True,
    )


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
    _run_git(repo_path, "checkout", branch)

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

    if not created:
        return []

    # Stage only the concrete files and edits we produced. Directory
    # entries in ``created`` are kept for the returned log but are not
    # passed to ``git add`` directly — git tracks files, not empty
    # directories, and new directories become visible through the files
    # inside them that we also created above. ``artifacts/`` in
    # particular is an empty directory covered by the ``.gitignore``
    # entry, so there is nothing for git to track there.
    to_stage = [path for path in created if not path.endswith("/")]
    if not to_stage:
        # Only untrackable entries (e.g. a lone ``artifacts/`` directory)
        # were created. Running ``git add``/``commit`` here would fail
        # with "nothing to commit", surfacing as a scaffolding error
        # even though nothing was actually wrong — the filesystem now
        # matches the runbook and the next cycle sees a clean no-op.
        logger.info("scaffold_repo created (no git writes): %s", ", ".join(created))
        return created

    try:
        _run_git(repo_path, "add", *to_stage)
        _run_git(repo_path, "commit", "-m", _COMMIT_MESSAGE)
        _run_git(repo_path, "push", "origin", branch)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        logger.warning("scaffold_repo git step failed: %s", detail)
        raise

    logger.info("scaffold_repo created: %s", ", ".join(created))
    return created
