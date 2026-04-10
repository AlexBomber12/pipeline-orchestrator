"""Tests for src/github_client.py."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.github_client import get_repo_full_name, run_gh


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "", stderr: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def test_get_repo_full_name_with_git_suffix() -> None:
    url = "https://github.com/AlexBomber12/lan-transcriber.git"
    assert get_repo_full_name(url) == "AlexBomber12/lan-transcriber"


def test_get_repo_full_name_without_git_suffix() -> None:
    url = "https://github.com/AlexBomber12/lan-transcriber"
    assert get_repo_full_name(url) == "AlexBomber12/lan-transcriber"


def test_get_repo_full_name_with_trailing_slash() -> None:
    url = "https://github.com/AlexBomber12/lan-transcriber/"
    assert get_repo_full_name(url) == "AlexBomber12/lan-transcriber"


def test_get_repo_full_name_ssh_url() -> None:
    url = "git@github.com:AlexBomber12/lan-transcriber.git"
    assert get_repo_full_name(url) == "AlexBomber12/lan-transcriber"


def test_get_repo_full_name_invalid_raises() -> None:
    with pytest.raises(ValueError):
        get_repo_full_name("https://example.com/not/github")


def test_run_gh_raises_on_nonzero_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="boom", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="boom"):
        run_gh(["pr", "list"])


def test_run_gh_parses_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(stdout='[{"number": 7}]')

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = run_gh(["pr", "list", "--json", "number"], repo="owner/name")

    assert result == [{"number": 7}]
    assert captured["cmd"] == [
        "gh",
        "pr",
        "list",
        "--json",
        "number",
        "-R",
        "owner/name",
    ]


def test_run_gh_returns_raw_string_when_not_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout="ok\n")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert run_gh(["auth", "status"]) == "ok"
