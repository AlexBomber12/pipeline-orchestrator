"""Tests for src.daemon.git_ops."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.daemon import git_ops


def _cp(
    args: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=args,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def test_repo_owner_from_url_delegates_to_github_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        git_ops.github_client,
        "get_repo_full_name",
        lambda url: f"parsed:{url}",
    )

    assert (
        git_ops.repo_owner_from_url("https://github.com/octo/demo.git")
        == "parsed:https://github.com/octo/demo.git"
    )


def test_git_runs_subprocess_with_standard_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["cmd"] = cmd
        captured["kwargs"] = kwargs
        return _cp(cmd, stdout="ok\n")

    monkeypatch.setattr(git_ops.subprocess, "run", fake_run)

    result = git_ops._git("/tmp/repo", "status", "--porcelain", timeout=12, check=False)

    assert result.stdout == "ok\n"
    assert captured["cmd"] == ["git", "status", "--porcelain"]
    assert captured["kwargs"] == {
        "capture_output": True,
        "text": True,
        "timeout": 12,
        "check": False,
        "cwd": "/tmp/repo",
    }


def test_base_branch_ahead_returns_false_when_branches_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, ...]] = []
    responses = iter(
        [
            _cp(["git", "rev-parse"], stdout="local\n"),
            _cp(["git", "rev-parse"], stdout="remote\n"),
            _cp(["git", "rev-list"], stdout="0\n"),
        ]
    )

    def fake_git(repo_path: str, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        assert repo_path == "/repo"
        assert kwargs == {"check": False}
        return next(responses)

    monkeypatch.setattr(git_ops, "_git", fake_git)

    assert git_ops._base_branch_ahead_of_origin("/repo", "main") is False
    assert calls == [
        ("rev-parse", "--verify", "--quiet", "refs/heads/main"),
        ("rev-parse", "--verify", "--quiet", "refs/remotes/origin/main"),
        ("rev-list", "--count", "refs/remotes/origin/main..refs/heads/main"),
    ]


def test_base_branch_ahead_returns_true_when_rev_list_reports_commits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            _cp(["git", "rev-parse"], stdout="local\n"),
            _cp(["git", "rev-parse"], stdout="remote\n"),
            _cp(["git", "rev-list"], stdout="2\n"),
        ]
    )

    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: next(responses),
    )

    assert git_ops._base_branch_ahead_of_origin("/repo", "main") is True


def test_base_branch_ahead_returns_true_when_local_branch_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: _cp(["git", "rev-parse"], returncode=1),
    )

    assert git_ops._base_branch_ahead_of_origin("/repo", "main") is True


def test_base_branch_ahead_returns_true_when_remote_branch_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            _cp(["git", "rev-parse"], stdout="local\n"),
            _cp(["git", "rev-parse"], returncode=1),
        ]
    )

    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: next(responses),
    )

    assert git_ops._base_branch_ahead_of_origin("/repo", "main") is True


def test_base_branch_ahead_returns_true_when_rev_list_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    responses = iter(
        [
            _cp(["git", "rev-parse"], stdout="local\n"),
            _cp(["git", "rev-parse"], stdout="remote\n"),
            _cp(["git", "rev-list"], returncode=9),
        ]
    )
    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: next(responses),
    )

    with caplog.at_level("WARNING"):
        result = git_ops._base_branch_ahead_of_origin("/repo", "main")

    assert result is True
    assert "rev-list probe failed" in caplog.text


def test_base_branch_ahead_returns_true_on_non_integer_rev_list_output(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    responses = iter(
        [
            _cp(["git", "rev-parse"], stdout="local\n"),
            _cp(["git", "rev-parse"], stdout="remote\n"),
            _cp(["git", "rev-list"], stdout="not-a-number\n"),
        ]
    )
    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: next(responses),
    )

    with caplog.at_level("WARNING"):
        result = git_ops._base_branch_ahead_of_origin("/repo", "main")

    assert result is True
    assert "non-integer output" in caplog.text


def test_base_branch_ahead_returns_true_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fake_git(repo_path: str, *args: str, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["git", *args], timeout=30)

    monkeypatch.setattr(git_ops, "_git", fake_git)

    with caplog.at_level("WARNING"):
        result = git_ops._base_branch_ahead_of_origin("/repo", "main")

    assert result is True
    assert "timed out" in caplog.text


def test_working_tree_dirty_returns_true_when_status_has_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: _cp(["git", "status"], stdout=" M file.py\n"),
    )

    assert git_ops._working_tree_dirty("/repo") is True


def test_working_tree_dirty_returns_false_when_status_is_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *_args, **_kwargs: _cp(["git", "status"], stdout=""),
    )

    assert git_ops._working_tree_dirty("/repo") is False


def test_working_tree_dirty_returns_false_on_called_process_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(repo_path: str, *args: str, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(
            returncode=1,
            cmd=["git", *args],
        )

    monkeypatch.setattr(git_ops, "_git", fake_git)

    assert git_ops._working_tree_dirty("/repo") is False


def test_working_tree_dirty_returns_false_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(repo_path: str, *args: str, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["git", *args], timeout=30)

    monkeypatch.setattr(git_ops, "_git", fake_git)

    assert git_ops._working_tree_dirty("/repo") is False


def test_repo_looks_scaffolded_returns_false_when_repo_missing(tmp_path: Path) -> None:
    assert git_ops._repo_looks_scaffolded(str(tmp_path / "missing")) is False


def test_repo_looks_scaffolded_returns_true_when_all_scaffold_files_exist(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "AGENTS.md").write_text("agent\n", encoding="utf-8")
    (repo / "tasks" / "QUEUE.md").write_text("queue\n", encoding="utf-8")
    (repo / "scripts" / "ci.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/bin/sh\n",
        encoding="utf-8",
    )
    (repo / ".gitignore").write_text("artifacts/\n", encoding="utf-8")

    assert git_ops._repo_looks_scaffolded(str(repo)) is True


def test_repo_looks_scaffolded_accepts_claude_file_instead_of_agents(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "CLAUDE.md").write_text("agent\n", encoding="utf-8")
    (repo / "tasks" / "QUEUE.md").write_text("queue\n", encoding="utf-8")
    (repo / "scripts" / "ci.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/bin/sh\n",
        encoding="utf-8",
    )
    (repo / ".gitignore").write_text("artifacts/\n", encoding="utf-8")

    assert git_ops._repo_looks_scaffolded(str(repo)) is True


def test_repo_looks_scaffolded_returns_false_when_gitignore_lacks_artifacts(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "AGENTS.md").write_text("agent\n", encoding="utf-8")
    (repo / "tasks" / "QUEUE.md").write_text("queue\n", encoding="utf-8")
    (repo / "scripts" / "ci.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/bin/sh\n",
        encoding="utf-8",
    )
    (repo / ".gitignore").write_text("*.pyc\n", encoding="utf-8")

    assert git_ops._repo_looks_scaffolded(str(repo)) is False
