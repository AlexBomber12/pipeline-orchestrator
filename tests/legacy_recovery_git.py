"""Real Git fixture for retained legacy FIX completion regressions."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from types import SimpleNamespace

from src.queue_parser import write_frontmatter_status


def git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def legacy_repo(tmp_path: Path, *, remote_error: bool = False) -> SimpleNamespace:
    remote, repo, writer = (tmp_path / name for name in ("remote.git", "repo", "writer"))
    git(tmp_path, "init", "--bare", str(remote))
    git(tmp_path, "init", "-b", "main", str(repo))
    git(repo, "config", "user.name", "Legacy Test")
    git(repo, "config", "user.email", "legacy@example.test")
    task = repo / "tasks/PR-100.md"
    task.parent.mkdir()
    task.write_text(
        "---\nstatus: TODO\n---\n\n# PR-100: Original generation\n\n"
        "Branch: pr-100-size\n- Type: feature\n- Complexity: medium\n"
        "- Depends on: none\n- Priority: 2\n- Coder: codex\n",
        encoding="utf-8",
    )
    git(repo, "add", "tasks/PR-100.md")
    git(repo, "commit", "-m", "original task")
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "push", "-u", "origin", "main")
    git(repo, "checkout", "-b", "pr-100-size")
    (repo / "implementation.py").write_text("published = True\n", encoding="utf-8")
    git(repo, "add", "implementation.py")
    git(repo, "commit", "-m", "published implementation")
    git(repo, "push", "-u", "origin", "pr-100-size")
    head = git(repo, "rev-parse", "HEAD")
    git(tmp_path, "clone", "-b", "main", str(remote), str(writer))
    git(writer, "config", "user.name", "Task Writer")
    git(writer, "config", "user.email", "writer@example.test")
    remote_task = writer / "tasks/PR-100.md"
    if remote_error:
        write_frontmatter_status(remote_task, "ERROR", "crash")
        git(writer, "add", "tasks/PR-100.md")
        git(writer, "commit", "-m", "legacy artificial error")
        git(writer, "push", "origin", "main")
    revision = hashlib.sha256(remote_task.read_bytes()).hexdigest()
    return SimpleNamespace(
        remote=remote, repo=repo, writer=writer, task=task,
        remote_task=remote_task, head=head, revision=revision,
    )
