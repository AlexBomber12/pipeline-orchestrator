"""Tests for src/github_client.py."""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.github_client import (
    clear_review_status_cache,
    get_pr_author,
    get_pr_head_commit_iso,
    get_pr_metadata,
    get_pr_review_status,
    get_repo_full_name,
    has_recent_codex_review_request,
    merge_pr,
    run_gh,
)
from src.models import ReviewStatus


def _find_api_path(cmd: list[str]) -> str:
    """Extract the API path from a gh command, handling --jq args."""
    for arg in cmd:
        if arg.startswith("repos/"):
            return arg
    return ""


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


def test_get_pr_review_status_approved_via_pr_body_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex +1 reaction on the PR body (issue-level) → APPROVED without needing comments."""
    import json as _json

    clear_review_status_cache()
    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [{"content": "+1", "user": {"login": "chatgpt-codex-connector"}}]
        elif "issues" in path and path.endswith("/comments"):
            data = [[]]
        elif "pulls" in path and path.endswith("/comments"):
            data = [[]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )

    assert any("issues/42/reactions" in arg for arg in invocations[0])


def test_get_pr_review_status_approved_via_first_author_comment_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex +1 reaction on the first PR-author issue comment → APPROVED.

    All gh api calls must use --paginate so multi-page responses
    are parseable as a single JSON document.
    """
    import json as _json

    clear_review_status_cache()
    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = []
        elif "issues" in path and path.endswith("/comments"):
            data = [
                [{"id": 10, "user": {"login": "author"}, "body": "@codex review"}],
                [{"id": 20, "user": {"login": "chatgpt-codex-bot"}, "body": "LGTM"}],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif path.endswith("/reactions"):
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )

    assert len(invocations) == 4
    for cmd in invocations:
        assert "--paginate" in cmd, f"missing --paginate in {cmd}"


def test_get_pr_review_status_skips_teammate_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A teammate's comment before the PR author's should be ignored."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [
                [
                    {"id": 5, "user": {"login": "teammate"}, "body": "looks interesting"},
                    {"id": 10, "user": {"login": "author"}, "body": "@codex review"},
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )


def test_get_pr_review_status_ignores_non_trigger_author_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrelated author follow-up after the trigger should not become the anchor."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [
                [
                    {"id": 10, "user": {"login": "author"}, "body": "@codex review"},
                    {"id": 15, "user": {"login": "author"}, "body": "actually nvm, still WIP"},
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

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

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/reactions"):
            return _FakeCompletedProcess(stdout=_json.dumps([]))
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

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [
                [
                    {
                        "id": 10,
                        "user": {"login": "author"},
                        "body": "@codex review",
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
            data = []
        elif path.endswith("/reactions"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

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

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [
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
            data = []
        elif path.endswith("/reactions"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.PENDING
    )


def test_get_pr_review_status_uses_latest_author_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multi-round PR: latest author comment is the anchor, old +1 ignored."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [
                [
                    {
                        "id": 10,
                        "user": {"login": "author"},
                        "body": "@codex review",
                        "created_at": "2026-01-01T00:00:00Z",
                    },
                    {
                        "id": 20,
                        "user": {"login": "author"},
                        "body": "@codex review",
                        "created_at": "2026-01-01T01:00:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/20/reactions" in path:
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.PENDING
    )


def test_get_pr_review_status_handles_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """404 errors from gh api should be caught, resulting in PENDING."""
    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="HTTP 404", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42) == ReviewStatus.PENDING


def test_get_pr_review_status_propagates_non_404_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-404 errors (auth, rate-limit, network) must propagate."""
    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="HTTP 403 rate limit exceeded", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="403"):
        get_pr_review_status("owner/name", 42)


def test_get_pr_review_status_propagates_error_on_pr_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 403 on PR #404 must not be swallowed by the 404 check."""
    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stderr="HTTP 403 rate limit exceeded", returncode=1
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="403"):
        get_pr_review_status("owner/name", 404)


def _is_commits_path(cmd: list[str]) -> bool:
    """Return True if ``gh api repos/.../commits/<sha> --jq ...``."""
    for arg in cmd:
        if "/commits/" in arg:
            return True
    return False


def test_body_plus_one_before_head_commit_is_stale(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """+1 reaction created BEFORE the head commit's committer date must
    be treated as stale — the approval predates the current push."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-02T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.PENDING
    )


def test_body_plus_one_after_head_commit_approves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """+1 reaction created AFTER the head commit's committer date must
    be treated as approval of the current push."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-03T00:00:00Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="aabbcc112233"
        )
        == ReviewStatus.APPROVED
    )


def test_body_plus_one_no_commit_time_trusts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Can't fetch commit time → trust the +1 reaction (APPROVED)."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stderr="boom", returncode=1)
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-03T00:00:00Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="deadbeef"
        )
        == ReviewStatus.APPROVED
    )


def test_find_codex_plus_one_picks_newest() -> None:
    """_find_codex_plus_one_reaction must return the most recent +1."""
    from src.github_client import _find_codex_plus_one_reaction

    reactions = [
        {
            "content": "+1",
            "user": {"login": "chatgpt-codex-connector"},
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "content": "+1",
            "user": {"login": "chatgpt-codex-connector"},
            "created_at": "2026-01-05T00:00:00Z",
        },
        {
            "content": "+1",
            "user": {"login": "someone-else"},
            "created_at": "2026-01-10T00:00:00Z",
        },
    ]
    best = _find_codex_plus_one_reaction(reactions)
    assert best is not None
    assert best["created_at"] == "2026-01-05T00:00:00Z"


def test_approval_without_head_sha(monkeypatch: pytest.MonkeyPatch) -> None:
    """+1 reaction with no head_sha → APPROVED (backward compatible)."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif "issues" in path and path.endswith("/comments"):
            data = [[]]
        elif "pulls" in path and path.endswith("/comments"):
            data = [[]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )


def test_merge_pr_uses_squash(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    merge_pr("owner/name", 42)

    assert captured["cmd"] == [
        "gh",
        "pr",
        "merge",
        "42",
        "--squash",
        "--delete-branch",
        "-R",
        "owner/name",
    ]


def _iso_utc_now_minus(seconds: int) -> str:
    from datetime import datetime, timedelta, timezone as _tz

    return (
        datetime.now(_tz.utc) - timedelta(seconds=seconds)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_has_recent_codex_review_request_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PR-author ``@codex review`` comment within the window counts
    as a recent request — the caller must skip posting another one."""
    import json as _json

    pages = [
        [
            {
                "user": {"login": "author"},
                "body": "@codex review",
                "created_at": _iso_utc_now_minus(60),
            }
        ]
    ]

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        has_recent_codex_review_request(
            "owner/name", 42, pr_author="author", within_minutes=5
        )
        is True
    )


def test_has_recent_codex_review_request_false_too_old(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A matching comment older than ``within_minutes`` must not count."""
    import json as _json

    pages = [
        [
            {
                "user": {"login": "author"},
                "body": "@codex review",
                "created_at": _iso_utc_now_minus(10 * 60),
            }
        ]
    ]

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        has_recent_codex_review_request(
            "owner/name", 42, pr_author="author", within_minutes=5
        )
        is False
    )


def test_has_recent_codex_review_request_false_no_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no PR-author ``@codex review`` comment exists at all the
    helper returns False so the daemon posts the trigger itself."""
    import json as _json

    pages = [
        [
            {
                "user": {"login": "someone-else"},
                "body": "@codex review",
                "created_at": _iso_utc_now_minus(60),
            },
            {
                "user": {"login": "author"},
                "body": "looks good",
                "created_at": _iso_utc_now_minus(60),
            },
        ]
    ]

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        has_recent_codex_review_request(
            "owner/name", 42, pr_author="author", within_minutes=5
        )
        is False
    )


def test_get_pr_author_returns_login(monkeypatch: pytest.MonkeyPatch) -> None:
    """``get_pr_author`` must read the login from PR metadata, not from
    the daemon's ``gh`` identity, so dedup works when Claude CLI ran
    under a different auth context than the daemon."""
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(stdout='"claude-cli-bot"')

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_author("owner/name", 42) == "claude-cli-bot"
    assert captured, "gh must be invoked"
    assert any("repos/owner/name/pulls/42" in arg for arg in captured[0])


def test_get_pr_author_returns_empty_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``gh api`` failure must not crash the caller — the dedup path
    simply skips when no author can be resolved."""
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout="", stderr="not found", returncode=1
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_author("owner/name", 42) == ""


def test_has_recent_codex_review_request_respects_after_iso(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Comments created at or before ``after_iso`` must not count as
    duplicates. This is what lets the daemon re-request a review for a
    new commit even when its own prior trigger for an earlier commit is
    still within the time window and shares the PR author login."""
    import json as _json

    pages = [
        [
            {
                "user": {"login": "same-user"},
                "body": "@codex review",
                "created_at": _iso_utc_now_minus(60),
            }
        ]
    ]

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    from datetime import datetime, timedelta, timezone as _tz

    just_now = (
        datetime.now(_tz.utc) - timedelta(seconds=10)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    assert (
        has_recent_codex_review_request(
            "owner/name",
            42,
            pr_author="same-user",
            within_minutes=5,
            after_iso=just_now,
        )
        is False
    )


def test_get_pr_head_commit_iso_returns_committer_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Should fetch ``.head.sha`` then ``.commit.committer.date`` and
    return the ISO timestamp unchanged."""
    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = next(
            (arg for arg in cmd if arg.startswith("repos/")), ""
        )
        if path.endswith("/pulls/42"):
            return _FakeCompletedProcess(stdout="abc1234")
        if path.startswith("repos/owner/name/commits/"):
            return _FakeCompletedProcess(stdout="2026-04-14T13:37:00Z")
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_head_commit_iso("owner/name", 42)
        == "2026-04-14T13:37:00Z"
    )
    assert any("repos/owner/name/pulls/42" in a for a in invocations[0])
    assert any(
        "repos/owner/name/commits/abc1234" in a for a in invocations[1]
    )


def test_get_pr_head_commit_iso_returns_empty_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Errors from either lookup must not propagate — the caller
    treats "" as "no constraint" and the dedup filter degrades
    gracefully to pure time-window matching."""
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout="", stderr="boom", returncode=1
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_head_commit_iso("owner/name", 42) == ""


def test_body_plus_one_stale_after_force_push_to_old_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force-push that moves head to an older commit must NOT silently
    reinstate an old +1 reaction. Even if reaction_time > committer.date
    (the old commit's stale date), the last Codex review's submission
    time is recent, so the reaction must beat THAT threshold."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2024-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-10T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-connector"},
                        "commit_id": "otherSha1234",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="oldSha5678"
        )
        == ReviewStatus.PENDING
    )


def test_body_plus_one_approved_when_codex_review_on_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A formal Codex review whose commit_id matches the current head is
    unconditional approval — no need to compare reaction times at all."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-10T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-connector"},
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="currentHead"
        )
        == ReviewStatus.APPROVED
    )


def test_body_plus_one_same_second_as_head_approves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """+1 reaction created in the SAME second as the head commit's
    committer date must count as fresh. GitHub timestamps are
    second-granular, so a strict ``>`` would mark the valid case stale."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-02T12:34:56Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-connector"},
                    "created_at": "2026-01-02T12:34:56Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="abc"
        )
        == ReviewStatus.APPROVED
    )


def test_review_status_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated calls within 30s return cached result without extra API calls."""
    import json as _json

    clear_review_status_cache()
    call_count = 0

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        nonlocal call_count
        call_count += 1
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {"content": "+1", "user": {"login": "chatgpt-codex-connector"}}
            ]
        elif "issues" in path and path.endswith("/comments"):
            data = [[]]
        elif "pulls" in path and path.endswith("/comments"):
            data = [[]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    result1 = get_pr_review_status(
        "owner/name", 42, pr_author="author", head_sha="sha123"
    )
    calls_after_first = call_count

    result2 = get_pr_review_status(
        "owner/name", 42, pr_author="author", head_sha="sha123"
    )

    assert result1 == ReviewStatus.APPROVED
    assert result2 == ReviewStatus.APPROVED
    assert call_count == calls_after_first


def test_get_pr_metadata_single_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_pr_metadata returns author + head_sha from a single PR API call
    plus one commit API call for the date."""
    import json as _json

    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42"):
            return _FakeCompletedProcess(
                stdout=_json.dumps({"author": "alice", "head_sha": "abc123"})
            )
        if "/commits/" in path:
            return _FakeCompletedProcess(stdout="2026-04-15T12:00:00Z")
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = get_pr_metadata("owner/name", 42)
    assert result["author"] == "alice"
    assert result["head_sha"] == "abc123"
    assert result["head_commit_date"] == "2026-04-15T12:00:00Z"
    assert len(invocations) == 2


def test_get_pr_metadata_returns_empty_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_pr_metadata gracefully returns empty fields on API failure."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="boom", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = get_pr_metadata("owner/name", 42)
    assert result == {"author": "", "head_sha": "", "head_commit_date": ""}
