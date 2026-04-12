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


def test_get_pr_review_status_approved_via_first_author_comment_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex +1 reaction on the first PR-author issue comment → APPROVED.

    All gh api calls must use --paginate --slurp so multi-page responses
    are parseable as a single JSON document.
    """
    import json as _json

    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = cmd[-1]
        if "issues" in path and path.endswith("/comments"):
            pages = [
                [{"id": 10, "user": {"login": "author"}, "body": "@codex review"}],
                [{"id": 20, "user": {"login": "chatgpt-codex-bot"}, "body": "LGTM"}],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            pages = []
        elif path.endswith("/reactions"):
            pages = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            pages = []
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )

    # 2 comment fetches (issue + review) + 1 reaction fetch
    assert len(invocations) == 3
    for cmd in invocations:
        assert "--paginate" in cmd, f"missing --paginate in {cmd}"
        assert "--slurp" in cmd, f"missing --slurp in {cmd}"


def test_get_pr_review_status_skips_teammate_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A teammate's comment before the PR author's should be ignored."""
    import json as _json

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = cmd[-1]
        if "issues" in path and path.endswith("/comments"):
            pages = [
                [
                    {"id": 5, "user": {"login": "teammate"}, "body": "looks interesting"},
                    {"id": 10, "user": {"login": "author"}, "body": "@codex review"},
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            pages = []
        elif "comments/10/reactions" in path:
            pages = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            # Should never reach teammate's comment
            pages = []
        else:
            pages = []
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )


def test_get_pr_review_status_pending_when_no_codex_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PR with an author comment but no Codex reaction should resolve to PENDING."""
    import json as _json

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_json.dumps([[{"id": 1, "user": {"login": "author"}, "body": "hi"}]])
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


def test_get_pr_review_status_changes_requested_on_p1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Codex comment containing P1 after the anchor → CHANGES_REQUESTED."""
    import json as _json

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = cmd[-1]
        if "issues" in path and path.endswith("/comments"):
            pages = [
                [
                    {
                        "id": 10,
                        "user": {"login": "author"},
                        "body": "please review",
                        "created_at": "2026-01-01T00:00:00Z",
                    },
                    {
                        "id": 20,
                        "user": {"login": "chatgpt-codex-bot"},
                        "body": "P1: fix this",
                        "created_at": "2026-01-01T00:01:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            pages = []
        elif path.endswith("/reactions"):
            pages = []
        else:
            pages = []
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.CHANGES_REQUESTED
    )


def test_get_pr_review_status_ignores_stale_p1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Codex P1 comment posted before the anchor should not count."""
    import json as _json

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = cmd[-1]
        if "issues" in path and path.endswith("/comments"):
            pages = [
                [
                    {
                        "id": 5,
                        "user": {"login": "chatgpt-codex-bot"},
                        "body": "P1: old issue",
                        "created_at": "2026-01-01T00:00:00Z",
                    },
                    {
                        "id": 10,
                        "user": {"login": "author"},
                        "body": "@codex review",
                        "created_at": "2026-01-01T00:05:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            pages = []
        elif path.endswith("/reactions"):
            pages = []
        else:
            pages = []
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.PENDING
    )


def test_get_pr_review_status_handles_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """404 errors from gh api should be caught, resulting in PENDING."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="HTTP 404", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42) == ReviewStatus.PENDING


def test_get_pr_review_status_propagates_non_404_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-404 errors (auth, rate-limit, network) must propagate."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="HTTP 403 rate limit exceeded", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="403"):
        get_pr_review_status("owner/name", 42)
