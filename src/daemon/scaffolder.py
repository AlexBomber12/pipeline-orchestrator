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


def _current_branch(repo_path: str) -> str:
    """Return the current branch name for ``repo_path``."""
    result = _run_git(repo_path, "rev-parse", "--abbrev-ref", "HEAD")
    return result.stdout.strip()


def scaffold_repo(repo_path: str) -> list[str]:
    """Create any missing pipeline orchestrator files in ``repo_path``.

    Returns the list of relative paths created or edited. If the list is
    non-empty, the new state is staged, committed, and pushed to the
    current branch. Returns an empty list (and performs no git writes)
    when nothing needed to change, keeping repeated calls idempotent.
    """
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
    try:
        _run_git(repo_path, "add", *to_stage)
        _run_git(repo_path, "commit", "-m", _COMMIT_MESSAGE)
        branch = _current_branch(repo_path)
        _run_git(repo_path, "push", "origin", branch)
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "").strip()
        logger.warning("scaffold_repo git step failed: %s", detail)
        raise

    logger.info("scaffold_repo created: %s", ", ".join(created))
    return created
