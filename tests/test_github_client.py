"""Tests for src/github_client.py."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.github_client import get_pr_review_status, get_repo_full_name, run_gh
from src.models import ReviewStatus


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


def test_get_pr_review_status_paginates_and_slurps_gh_api_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both the comments and reactions lookups must use --paginate --slurp.

    Without --slurp, multi-page responses are written as several JSON
    documents back-to-back, which json.loads cannot parse — so the function
    would silently return PENDING. The fake mimics --slurp's behavior:
    pages are wrapped in an outer array.
    """
    import json as _json

    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = cmd[-1]
        if path.endswith("/comments"):
            pages = [
                [{"id": 1, "user": {"login": "user"}, "body": "hi"}],
                [{"id": 2, "user": {"login": "chatgpt-codex-bot"}, "body": ""}],
            ]
        elif path.endswith("/reactions"):
            pages = [[{"content": "+1"}]]
        else:
            pages = []
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42) == ReviewStatus.APPROVED

    assert len(invocations) == 2
    for cmd in invocations:
        assert "--paginate" in cmd, f"missing --paginate in {cmd}"
        assert "--slurp" in cmd, f"missing --slurp in {cmd}"


def test_get_pr_review_status_pending_when_no_codex_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PR with no Codex bot comments should resolve to PENDING."""
    import json as _json

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_json.dumps([[{"id": 1, "user": {"login": "human"}, "body": "hi"}]])
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42) == ReviewStatus.PENDING
