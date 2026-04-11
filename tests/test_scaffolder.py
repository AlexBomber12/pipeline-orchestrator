"""Tests for src/daemon/scaffolder.py."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from src.daemon import scaffolder


class _FakeCompletedProcess:
    def __init__(
        self,
        args: list[str] | None = None,
        stdout: str = "main\n",
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.args = args or []
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def _patch_git(
    monkeypatch: pytest.MonkeyPatch, branch: str = "main"
) -> list[list[str]]:
    """Capture git subprocess calls issued by scaffolder."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        stdout = ""
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            stdout = f"{branch}\n"
        return _FakeCompletedProcess(args=cmd, stdout=stdout)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)
    return calls


def _init_empty_repo(tmp_path: Path) -> Path:
    """Create an empty directory stand-in for a freshly cloned repo."""
    repo = tmp_path / "sample-repo"
    repo.mkdir()
    return repo


def test_scaffold_repo_creates_all_files_when_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    calls = _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo))

    # All baseline files must be on disk after scaffolding.
    assert (repo / "AGENTS.md").exists()
    assert (repo / "tasks").is_dir()
    assert (repo / "tasks" / "QUEUE.md").exists()
    assert (repo / "scripts").is_dir()
    assert (repo / "scripts" / "ci.sh").exists()
    assert (repo / "scripts" / "make-review-artifacts.sh").exists()
    assert (repo / "artifacts").is_dir()
    assert (repo / ".gitignore").exists()
    assert "artifacts/" in (repo / ".gitignore").read_text().splitlines()

    # Shell helpers must be executable so bash can run them directly.
    assert (repo / "scripts" / "ci.sh").stat().st_mode & 0o111
    assert (repo / "scripts" / "make-review-artifacts.sh").stat().st_mode & 0o111

    # Every tracked path (directories filtered out) should be reported as
    # an action so the caller can log it.
    for path in (
        "AGENTS.md",
        "tasks/QUEUE.md",
        "scripts/ci.sh",
        "scripts/make-review-artifacts.sh",
        ".gitignore",
    ):
        assert path in actions

    # git add must stage exactly the files we created, not the directory
    # entries that appear in the action log.
    add_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "add"]]
    assert len(add_cmds) == 1
    staged = add_cmds[0][2:]
    assert "AGENTS.md" in staged
    assert "tasks/QUEUE.md" in staged
    assert "scripts/ci.sh" in staged
    assert "scripts/make-review-artifacts.sh" in staged
    assert ".gitignore" in staged
    assert "tasks/" not in staged
    assert "artifacts/" not in staged

    # Commit and push must follow add, in that order.
    subcommands = [cmd[1] for cmd in calls if cmd[0] == "git"]
    assert subcommands.index("add") < subcommands.index("commit")
    assert subcommands.index("commit") < subcommands.index("push")
    push_cmd = next(cmd for cmd in calls if cmd[:2] == ["git", "push"])
    assert push_cmd == ["git", "push", "origin", "main"]


def test_scaffold_repo_preserves_existing_agents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    existing = "# Custom AGENTS\n"
    (repo / "AGENTS.md").write_text(existing)
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo))

    assert (repo / "AGENTS.md").read_text() == existing
    assert "AGENTS.md" not in actions


def test_scaffold_repo_accepts_claude_md_as_agents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLAUDE.md is accepted in place of AGENTS.md (either satisfies the
    runbook)."""
    repo = _init_empty_repo(tmp_path)
    (repo / "CLAUDE.md").write_text("# Project rules\n")
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo))

    assert not (repo / "AGENTS.md").exists()
    assert "AGENTS.md" not in actions


def test_scaffold_repo_preserves_existing_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    (repo / "tasks").mkdir()
    existing = "# Task Queue\n\n## PR-001\n- Status: DOING\n"
    (repo / "tasks" / "QUEUE.md").write_text(existing)
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo))

    assert (repo / "tasks" / "QUEUE.md").read_text() == existing
    assert "tasks/QUEUE.md" not in actions


def test_scaffold_repo_preserves_existing_ci_sh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    (repo / "scripts").mkdir()
    existing = "#!/usr/bin/env bash\nmake test\n"
    (repo / "scripts" / "ci.sh").write_text(existing)
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo))

    assert (repo / "scripts" / "ci.sh").read_text() == existing
    assert "scripts/ci.sh" not in actions


def test_scaffold_repo_skips_commit_when_fully_provisioned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fully-provisioned repo yields an empty action list and performs
    no git writes — scaffolding is idempotent across repeated calls."""
    repo = _init_empty_repo(tmp_path)
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "ci.sh").chmod(0o755)
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / "scripts" / "make-review-artifacts.sh").chmod(0o755)
    (repo / "artifacts").mkdir()
    (repo / ".gitignore").write_text("artifacts/\n")
    calls = _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo))

    assert actions == []
    # No git writes at all when nothing changed.
    assert calls == []


def test_scaffold_repo_appends_artifacts_without_duplicating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    existing = "node_modules/\n*.pyc\n"
    (repo / ".gitignore").write_text(existing)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo))

    lines = (repo / ".gitignore").read_text().splitlines()
    assert lines.count("artifacts/") == 1
    assert "node_modules/" in lines
    assert "*.pyc" in lines

    # Running it again must not add a second entry.
    _patch_git(monkeypatch)
    scaffolder.scaffold_repo(str(repo))
    lines_after = (repo / ".gitignore").read_text().splitlines()
    assert lines_after.count("artifacts/") == 1


def test_scaffold_repo_propagates_git_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="push denied")
        stdout = "main\n" if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"] else ""
        return _FakeCompletedProcess(args=cmd, stdout=stdout)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        scaffolder.scaffold_repo(str(repo))
