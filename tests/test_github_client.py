"""Tests for src/github_client.py."""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta
from datetime import timezone as _tz
from typing import Any

import pytest
import src.github_client as github_client
from src.github_client import (
    _compute_review_status,
    _fetch_ci_status_rest,
    _get_codex_issue_reactions,
    _get_codex_review_signals,
    _get_latest_codex_review_info,
    _is_codex_user,
    _is_plus_one,
    _is_reaction_content,
    _map_rest_ci_status_to_enum,
    _parse_iso,
    clear_ci_status_cache,
    clear_last_known_sha,
    clear_merged_prs_cache,
    clear_review_status_cache,
    get_branch_last_push_time,
    get_last_push_age_seconds,
    get_pr_last_push_time,
    get_merged_prs,
    get_open_prs,
    get_pr_author,
    get_pr_head_commit_iso,
    get_pr_metadata,
    get_pr_review_status,
    get_repo_full_name,
    has_recent_codex_review_request,
    is_pr_merged,
    merge_pr,
    post_comment,
    pr_state,
    run_gh,
)
from src.models import CIStatus, ReviewStatus


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


@pytest.fixture(autouse=True)
def _clear_ci_status_cache_between_tests() -> None:
    """Drop the (repo, sha) CI status cache so per-test ``run_gh`` patches
    are not shadowed by a result a previous test populated."""
    clear_ci_status_cache()


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


def test_get_merged_prs_paginates_closed_prs_without_fixed_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    captured: dict[str, str] = {}

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        captured["path"] = path
        return [
            {
                "number": 101,
                "title": "PR-101: shipped work",
                "merged_at": "2026-04-18T10:00:00Z",
                "head": {
                    "ref": "pr-101-shipped-work",
                    "repo": {"fork": False},
                },
                "base": {"ref": "main"},
            },
            {
                "number": 102,
                "title": "closed without merge",
                "merged_at": None,
                "head": {
                    "ref": "pr-102-closed",
                    "repo": {"fork": False},
                },
                "base": {"ref": "main"},
            },
            {
                "number": 103,
                "title": "custom squash title",
                "merged_at": "2026-04-18T11:00:00Z",
                "head": {
                    "ref": "pr-103-custom-title",
                    "repo": {"fork": True},
                },
                "base": {"ref": "release"},
            },
        ]

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    prs = get_merged_prs("owner/name")

    assert captured["path"] == "repos/owner/name/pulls?state=closed&per_page=100"
    assert [pr.number for pr in prs] == [101, 103]
    assert prs[0].pr_id == "PR-101"
    assert prs[0].branch == "pr-101-shipped-work"
    assert prs[1].pr_id is None
    assert prs[1].is_cross_repository is True


def test_get_merged_prs_filters_by_base_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    def fake_paginated(path: str) -> list[dict[str, Any]]:
        assert path == "repos/owner/name/pulls?state=closed&base=main&per_page=100"
        return [
            {
                "number": 101,
                "title": "PR-101: shipped work",
                "merged_at": "2026-04-18T10:00:00Z",
                "head": {
                    "ref": "pr-101-shipped-work",
                    "repo": {"fork": False},
                },
                "base": {"ref": "main"},
            },
            {
                "number": 102,
                "title": "PR-102: merged elsewhere",
                "merged_at": "2026-04-18T11:00:00Z",
                "head": {
                    "ref": "pr-102-release-work",
                    "repo": {"fork": False},
                },
                "base": {"ref": "release"},
            },
        ]

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    prs = get_merged_prs("owner/name", base_branch="main")

    assert [pr.number for pr in prs] == [101]


def test_get_merged_prs_url_encodes_base_branch_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        assert (
            path
            == "repos/owner/name/pulls?state=closed&base=release%2F2026.04&per_page=100"
        )
        return []

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert get_merged_prs("owner/name", base_branch="release/2026.04") == []


def test_get_merged_prs_handles_deleted_head_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    def fake_paginated(path: str) -> list[dict[str, Any]]:
        assert path == "repos/owner/name/pulls?state=closed&per_page=100"
        return [
            {
                "number": 104,
                "title": "PR-104: merged from deleted fork",
                "merged_at": "2026-04-18T12:00:00Z",
                "head": {
                    "ref": "pr-104-deleted-fork",
                    "repo": None,
                },
                "base": {"ref": "main"},
            }
        ]

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    prs = get_merged_prs("owner/name")

    assert len(prs) == 1
    assert prs[0].number == 104
    assert prs[0].branch == "pr-104-deleted-fork"
    assert prs[0].is_cross_repository is False


def test_get_merged_prs_raises_when_github_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        raise RuntimeError(f"boom: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="boom"):
        get_merged_prs("owner/name", base_branch="main")


def test_is_pr_merged_true_when_merged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda args: {"state": "closed", "merged": True},
    )

    assert is_pr_merged("owner/name", 12) is True


def test_is_pr_merged_false_when_closed_unmerged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda args: {"state": "closed", "merged": False},
    )

    assert is_pr_merged("owner/name", 12) is False


def test_is_pr_merged_none_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str]) -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    assert is_pr_merged("owner/name", 12) is None


def test_is_pr_merged_none_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str]) -> dict[str, object]:
        raise subprocess.TimeoutExpired(cmd=args, timeout=30)

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    assert is_pr_merged("owner/name", 12) is None


def test_is_pr_merged_none_on_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str]) -> dict[str, object]:
        raise OSError("gh not found")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    assert is_pr_merged("owner/name", 12) is None


def test_is_pr_merged_none_on_malformed_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda args: "{not-json")

    assert is_pr_merged("owner/name", 12) is None


def test_get_merged_prs_uses_cache_within_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    calls = 0

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        assert path == "repos/owner/name/pulls?state=closed&base=main&per_page=100"
        return [
            {
                "number": 101,
                "title": "PR-101: shipped work",
                "merged_at": "2026-04-18T10:00:00Z",
                "head": {
                    "ref": "pr-101-shipped-work",
                    "repo": {"fork": False},
                },
                "base": {"ref": "main"},
            }
        ]

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    first = get_merged_prs("owner/name", base_branch="main")
    second = get_merged_prs("owner/name", base_branch="main")

    assert calls == 1
    assert [pr.number for pr in first] == [101]
    assert [pr.number for pr in second] == [101]


def test_clear_merged_prs_cache_forces_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    calls = 0

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return [
            {
                "number": 100 + calls,
                "title": f"PR-{100 + calls}: shipped work",
                "merged_at": "2026-04-18T10:00:00Z",
                "head": {
                    "ref": f"pr-{100 + calls}-shipped-work",
                    "repo": {"fork": False},
                },
                "base": {"ref": "main"},
            }
        ]

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    first = get_merged_prs("owner/name", base_branch="main")
    clear_merged_prs_cache()
    second = get_merged_prs("owner/name", base_branch="main")

    assert calls == 2
    assert [pr.number for pr in first] == [101]
    assert [pr.number for pr in second] == [102]


def test_get_merged_prs_refresh_bypasses_cache_and_replaces_cached_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    calls = 0

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        nonlocal calls
        calls += 1
        return [
            {
                "number": 100 + calls,
                "title": f"PR-{100 + calls}: shipped work",
                "merged_at": "2026-04-18T10:00:00Z",
                "head": {
                    "ref": f"pr-{100 + calls}-shipped-work",
                    "repo": {"fork": False},
                },
                "base": {"ref": "main"},
            }
        ]

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    first = get_merged_prs("owner/name", base_branch="main")
    refreshed = get_merged_prs(
        "owner/name",
        base_branch="main",
        refresh=True,
    )
    cached = get_merged_prs("owner/name", base_branch="main")

    assert calls == 2
    assert [pr.number for pr in first] == [101]
    assert [pr.number for pr in refreshed] == [102]
    assert [pr.number for pr in cached] == [102]


def test_is_codex_user_matches_bot_logins() -> None:
    assert _is_codex_user({"login": "codex"}) is True
    assert _is_codex_user({"login": "chatgpt-codex-conn"}) is True
    assert _is_codex_user({"login": "codex-bot"}) is True
    assert _is_codex_user({"login": "mycodexbot"}) is True
    assert _is_codex_user({"login": "not-codex-related-thing"}) is True


def test_is_codex_user_rejects_non_codex() -> None:
    assert _is_codex_user({"login": "AlexBomber12"}) is False
    assert _is_codex_user({"login": "dependabot"}) is False
    assert _is_codex_user({"login": "codec-reviewer"}) is False
    assert _is_codex_user(None) is False


def test_plus_one_requires_exact_content() -> None:
    assert _is_plus_one({"content": "+1", "user": {"login": "codex-bot"}}) is True
    assert _is_plus_one({"content": "thumbsup", "user": {"login": "codex-bot"}}) is False
    assert _is_plus_one({"content": "heart", "user": {"login": "codex-bot"}}) is False


def test_plus_one_requires_codex_user() -> None:
    assert _is_plus_one({"content": "+1", "user": {"login": "AlexBomber12"}}) is False
    assert _is_plus_one({"content": "+1", "user": {"login": "codex-bot"}}) is True


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

    assert any(
        "issues/42/reactions" in arg
        for cmd in invocations
        for arg in cmd
    )


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
    assert not any(cmd[-1].endswith("/pulls/42/reviews") for cmd in invocations)
    for cmd in invocations:
        assert "--paginate" in cmd, f"missing --paginate in {cmd}"


def test_review_api_without_reaction_stays_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A formal Codex APPROVED review alone should not count as approval."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-bot"},
                        "state": "APPROVED",
                        "commit_id": "bbbbbb2222",
                        "submitted_at": "2026-01-02T00:00:00Z",
                    }
                ]
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
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.PENDING
    )


def test_review_api_approved_requires_matching_head_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A formal APPROVED review for another sha must not auto-approve."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-bot"},
                        "state": "APPROVED",
                        "commit_id": "oldsha1111",
                        "submitted_at": "2026-01-02T00:00:00Z",
                    }
                ]
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
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.PENDING
    )


def test_review_api_approval_does_not_override_post_anchor_codex_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A formal APPROVED review should not beat newer Codex feedback."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-bot"},
                        "state": "APPROVED",
                        "commit_id": "bbbbbb2222",
                        "submitted_at": "2026-01-02T00:00:00Z",
                    }
                ]
            ]
        elif "issues" in path and path.endswith("/comments"):
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
                        "body": "P1: still broken",
                        "created_at": "2026-01-03T00:00:00Z",
                    },
                ]
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.CHANGES_REQUESTED
    )


def test_review_api_approved_beats_older_post_anchor_codex_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Older Codex comments still block without a +1 approval signal."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-bot"},
                        "state": "APPROVED",
                        "commit_id": "bbbbbb2222",
                        "submitted_at": "2026-01-03T00:00:00Z",
                    }
                ]
            ]
        elif "issues" in path and path.endswith("/comments"):
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
                        "body": "P1: earlier finding",
                        "created_at": "2026-01-02T00:00:00Z",
                    },
                ]
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.CHANGES_REQUESTED
    )


def test_latest_codex_review_state_overrides_older_approval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A newer CHANGES_REQUESTED review must beat an older APPROVED review."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [[
                {
                    "user": {"login": "chatgpt-codex-bot"},
                    "state": "APPROVED",
                    "commit_id": "bbbbbb2222",
                    "submitted_at": "2026-01-02T00:00:00Z",
                },
                {
                    "user": {"login": "chatgpt-codex-bot"},
                    "state": "CHANGES_REQUESTED",
                    "commit_id": "bbbbbb2222",
                    "submitted_at": "2026-01-03T00:00:00Z",
                },
            ]]
        elif "issues" in path and path.endswith("/comments"):
            data = [[]]
        elif "pulls" in path and path.endswith("/comments"):
            data = [[]]
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


def test_review_api_errors_do_not_block_reaction_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-404 review API failures should fall back to reactions/comments."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            return _FakeCompletedProcess(
                stderr="HTTP 403 rate limit exceeded", returncode=1
            )
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": {"login": "chatgpt-codex-bot"},
                    "created_at": "2026-01-03T00:00:00Z",
                }
            ]
        elif _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        elif "issues" in path and path.endswith("/comments"):
            data = [[]]
        elif "pulls" in path and path.endswith("/comments"):
            data = [[]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.APPROVED
    )


def test_review_api_approved_does_not_trust_unknown_head_commit_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown head commit time must not approve a mismatched review SHA."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stderr="boom", returncode=1)
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": {"login": "chatgpt-codex-bot"},
                        "state": "APPROVED",
                        "commit_id": "oldsha1111",
                        "submitted_at": "2026-01-02T00:00:00Z",
                    }
                ]
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
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.PENDING
    )


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


def test_review_status_changes_requested_without_p1_p2_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex comment without P1/P2 after anchor, no reactions -> CHANGES_REQUESTED."""
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
                        "body": "Looks fine, consider renaming this variable",
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


def test_review_status_ignores_codex_onboarding_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
                        "user": {"login": "chatgpt-codex-connector"},
                        "body": (
                            "To use Codex here, create a Codex account "
                            "and connect to github."
                        ),
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
        == ReviewStatus.PENDING
    )


def test_review_status_pending_when_no_codex_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No comments after anchor, no reactions -> PENDING."""
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

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


def test_review_status_approved_wins_over_codex_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valid +1 reaction plus Codex comment after anchor -> APPROVED."""
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
                        "body": "Looks good overall",
                        "created_at": "2026-01-01T00:01:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}, "created_at": "2026-01-01T00:02:00Z"}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.APPROVED
    )


def test_review_status_eyes_wins_over_codex_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Eyes reaction plus Codex comment after anchor -> EYES."""
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
                        "body": "Reviewing now",
                        "created_at": "2026-01-01T00:01:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.EYES
    )


def test_body_eyes_wins_over_anchor_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PR-body eyes signal should beat an anchor +1 while review is in progress."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [[
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.EYES
    )


def test_anchor_eyes_wins_over_body_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An anchor eyes signal should beat a PR-body +1 while review is in progress."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if "issues" in path and path.endswith("/comments"):
            data = [[
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "eyes", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            data = [[{"content": "+1", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status("owner/name", 42, pr_author="author")
        == ReviewStatus.EYES
    )


def test_review_api_with_body_eyes_stays_eyes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Formal APPROVED review alone should not beat body-level eyes."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [[
                {
                    "user": {"login": "chatgpt-codex-bot"},
                    "state": "APPROVED",
                    "commit_id": "bbbbbb2222",
                    "submitted_at": "2026-01-02T00:00:00Z",
                }
            ]]
        elif "issues" in path and path.endswith("/comments"):
            data = [[
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.EYES
    )


def test_review_api_with_anchor_eyes_stays_eyes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Formal APPROVED review alone should not beat anchor eyes."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [[
                {
                    "user": {"login": "chatgpt-codex-bot"},
                    "state": "APPROVED",
                    "commit_id": "bbbbbb2222",
                    "submitted_at": "2026-01-02T00:00:00Z",
                }
            ]]
        elif "issues" in path and path.endswith("/comments"):
            data = [[
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "eyes", "user": {"login": "chatgpt-codex-bot"}}]]
        elif path.endswith("/reactions"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="bbbbbb2222"
        )
        == ReviewStatus.EYES
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


def test_get_codex_issue_reactions_returns_empty_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("net/http: TLS handshake timeout")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert _get_codex_issue_reactions("owner/name", 42) == []


def test_get_codex_issue_reactions_logs_warning_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("net/http: TLS handshake timeout")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)
    caplog.set_level(logging.WARNING)

    assert _get_codex_issue_reactions("owner/name", 42) == []
    assert any(
        record.levelno == logging.WARNING
        and record.getMessage()
        == (
            "Reactions fetch degraded for PR 42 in owner/name: "
            "net/http: TLS handshake timeout"
        )
        for record in caplog.records
    )


def test_compute_review_status_propagates_non_transient_body_reactions_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            raise RuntimeError("HTTP 403 rate limit exceeded")
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="403"):
        _compute_review_status("owner/name", 42, "author", "")


def test_compute_review_status_degrades_when_anchor_reactions_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        if path.endswith("/issues/comments/10/reactions"):
            raise RuntimeError("i/o timeout")
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert (
        _compute_review_status("owner/name", 42, "author", "")
        == ReviewStatus.PENDING
    )


def test_compute_review_status_propagates_non_transient_anchor_reactions_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        if path.endswith("/issues/comments/10/reactions"):
            raise RuntimeError("HTTP 403 rate limit exceeded")
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="403"):
        _compute_review_status("owner/name", 42, "author", "")


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


def test_no_plus_one_does_not_fetch_head_commit_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review and commit-time lookups should stay lazy when no +1 path needs them."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            raise AssertionError("commit lookup should not run without +1")
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            raise AssertionError("review lookup should not run without +1")
        elif "issues" in path and path.endswith("/comments"):
            data = [
                [
                    {
                        "id": 10,
                        "user": {"login": "author"},
                        "body": "@codex review",
                        "created_at": "2026-01-01T00:00:00Z",
                    }
                ]
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
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="deadbeef"
        )
        == ReviewStatus.PENDING
    )


def test_body_eyes_returns_before_comment_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A body-level eyes signal should not depend on later comment API calls."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            raise AssertionError("review lookup should not run for body eyes only")
        if "issues" in path and path.endswith("/comments"):
            raise AssertionError("issue comments should not be fetched after body eyes")
        if "pulls" in path and path.endswith("/comments"):
            raise AssertionError("review comments should not be fetched after body eyes")
        if path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": {"login": "chatgpt-codex-bot"}}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.EYES


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
        body = '{"user": {"login": "claude-cli-bot"}}'
        stdout = (
            "HTTP/2.0 200 OK\r\n"
            'ETag: W/"abc"\r\n'
            "\r\n"
            f"{body}"
        )
        return _FakeCompletedProcess(stdout=stdout)

    monkeypatch.setattr(subprocess, "run", fake_run)
    github_client.clear_etag_cache()

    assert get_pr_author("owner/name", 42) == "claude-cli-bot"
    assert captured, "gh must be invoked"
    assert any("repos/owner/name/pulls/42" in arg for arg in captured[0])
    assert "--include" in captured[0]


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


def test_get_pr_author_returns_empty_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``subprocess.TimeoutExpired`` from ``run_gh`` must degrade to "".

    Otherwise the timeout would bubble out of ``get_latest_codex_feedback``
    and abort ``handle_fix`` before the coder runs, contradicting the
    intended best-effort behavior of omitting unavailable context.
    """
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=30)

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_author("owner/name", 42) == ""


def test_get_pr_author_returns_empty_on_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing ``gh`` binary (OSError) must degrade to "" rather than crash."""
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise FileNotFoundError("gh: command not found")

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


# ---------------------------------------------------------------------------
# _map_rest_ci_status_to_enum tests
# ---------------------------------------------------------------------------


def test_map_rest_ci_status_empty_defaults_to_pending() -> None:
    """No check-runs and no commit statuses must default to PENDING."""
    assert _map_rest_ci_status_to_enum([], {"state": "pending", "statuses": []}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([], {}) == CIStatus.PENDING


def test_map_rest_ci_status_empty_with_flag_returns_success() -> None:
    """Empty REST signals with empty_is_success=True must return SUCCESS."""
    assert (
        _map_rest_ci_status_to_enum([], {"state": "pending", "statuses": []}, empty_is_success=True)
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_handles_non_dict_status_payload() -> None:
    """A non-dict status payload (e.g. ``None``) collapses to PENDING/SUCCESS."""
    assert _map_rest_ci_status_to_enum([], None) == CIStatus.PENDING  # type: ignore[arg-type]
    assert (
        _map_rest_ci_status_to_enum([], None, empty_is_success=True)  # type: ignore[arg-type]
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_failed_fetch_follows_empty_is_success() -> None:
    """``fetch_ok=False`` with empty payloads must follow ``empty_is_success``.

    Aligns with ``_get_open_prs_rest``, which already returns SUCCESS for
    ``allow_merge_without_checks=True`` whenever the GraphQL primary
    fetch is unavailable. A transient REST-budget squeeze in the e2e
    suite (``poll_interval_sec=2``, per-token quota shared across runs)
    must not strand WATCH on a testbed PR that has no checks at all.
    """
    assert (
        _map_rest_ci_status_to_enum(
            [], {}, empty_is_success=True, fetch_ok=False
        )
        == CIStatus.SUCCESS
    )
    assert (
        _map_rest_ci_status_to_enum(
            [], {}, empty_is_success=False, fetch_ok=False
        )
        == CIStatus.PENDING
    )


# ---------------------------------------------------------------------------
# retry integration tests (PR-054)
# ---------------------------------------------------------------------------


def test_gh_api_paginated_retries_on_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """_gh_api_paginated retries on transient 503 then succeeds."""
    from src.github_client import _gh_api_paginated

    calls: list[int] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(1)
        if len(calls) == 1:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="HTTP 503 Service Unavailable"
            )
        return _FakeCompletedProcess(
            stdout='[[{"id": 1}]]',
            returncode=0,
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    result = _gh_api_paginated("repos/test/owner/issues/1/comments")
    assert result == [{"id": 1}]
    assert len(calls) == 2


def test_gh_api_paginated_fails_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """_gh_api_paginated raises RuntimeError after all retries exhausted."""
    from src.github_client import _gh_api_paginated

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise subprocess.CalledProcessError(
            1, cmd, stderr="503 Service Unavailable"
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        _gh_api_paginated("repos/test/owner/issues/1/comments")


def test_begin_review_cache_cycle_initializes_and_increments() -> None:
    clear_review_status_cache()

    github_client._begin_review_cache_cycle()
    assert github_client._review_status_cache_cycle == 1

    github_client._begin_review_cache_cycle()
    assert github_client._review_status_cache_cycle == 2


def test_is_reaction_content_rejects_non_dict() -> None:
    assert _is_reaction_content(None, "+1") is False


def test_get_open_prs_returns_prinfo_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = [
        {"number": 0},
        {
            "number": 42,
            "title": "PR-110: Add coverage",
            "headRefName": "feature-branch",
            "headRefOid": "abc123",
            "url": "https://example.test/pr/42",
            "updatedAt": "2026-04-18T11:22:33Z",
            "commits": [{}, {}],
            "author": {"login": "alice"},
            "labels": [{"name": "escalated"}],
            "isCrossRepository": True,
        },
    ]
    captured_pr_list_args: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if args and args[0] == "pr":
            captured_pr_list_args.append(list(args))
            return raw
        raise AssertionError(f"unexpected run_gh call: {args}")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr(
        "src.github_client._fetch_ci_status_rest",
        lambda repo, sha: ([], {}, True),
    )
    monkeypatch.setattr(
        "src.github_client.get_pr_review_status",
        lambda repo, number, pr_author, head_sha: ReviewStatus.APPROVED,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert [pr.number for pr in prs] == [42]
    assert prs[0].branch == "feature-branch"
    assert prs[0].pr_id == "PR-110"
    assert prs[0].ci_status == CIStatus.SUCCESS
    assert prs[0].review_status == ReviewStatus.APPROVED
    assert prs[0].commits_count == 2
    assert prs[0].push_count == 1
    assert prs[0].observed_head_shas == {"abc123"}
    assert prs[0].url == "https://example.test/pr/42"
    assert prs[0].last_activity == datetime(2026, 4, 18, 11, 22, 33, tzinfo=_tz.utc)
    assert prs[0].is_escalated is True
    assert prs[0].is_cross_repository is True
    assert len(captured_pr_list_args) == 1
    fields_arg = captured_pr_list_args[0][captured_pr_list_args[0].index("--json") + 1]
    assert "statusCheckRollup" not in fields_arg


def test_get_open_prs_invokes_rest_helper_with_head_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each PR's head SHA is passed through to the REST CI status fetch."""
    raw = [
        {
            "number": 7,
            "title": "PR-7: foo",
            "headRefName": "bar",
            "headRefOid": "deadbeef",
            "url": "u",
            "updatedAt": "2026-04-18T00:00:00Z",
            "commits": [],
            "author": {"login": "a"},
            "labels": [],
            "isCrossRepository": False,
        }
    ]
    captured: list[tuple[str, str]] = []

    def fake_fetch(repo: str, sha: str) -> tuple[list[dict], dict, bool]:
        captured.append((repo, sha))
        return (
            [{"conclusion": "failure"}],
            {"state": "failure", "statuses": []},
            True,
        )

    monkeypatch.setattr("src.github_client.run_gh", lambda *a, **kw: raw)
    monkeypatch.setattr("src.github_client._fetch_ci_status_rest", fake_fetch)
    monkeypatch.setattr(
        "src.github_client.get_pr_review_status",
        lambda repo, number, pr_author, head_sha: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name")

    assert captured == [("owner/name", "deadbeef")]
    assert prs[0].ci_status == CIStatus.FAILURE


def test_get_open_prs_rest_fetch_failure_follows_allow_merge_without_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """REST CI fetch failure must follow ``allow_merge_without_checks``.

    When ``_fetch_ci_status_rest`` reports both endpoints failed and the
    repo opts into ``allow_merge_without_checks``, ``get_open_prs`` must
    surface ``CIStatus.SUCCESS`` to match the GraphQL-rate-limit fallback
    in ``_get_open_prs_rest``. Without this alignment the daemon stalls
    in WATCH on every transient REST-budget squeeze (the e2e suite hit
    this with ``poll_interval_sec=2`` and a quota shared across runs).
    """
    raw = [
        {
            "number": 7,
            "title": "PR-7: foo",
            "headRefName": "bar",
            "headRefOid": "deadbeef",
            "url": "u",
            "updatedAt": "2026-04-18T00:00:00Z",
            "commits": [],
            "author": {"login": "a"},
            "labels": [],
            "isCrossRepository": False,
        }
    ]

    monkeypatch.setattr("src.github_client.run_gh", lambda *a, **kw: raw)
    monkeypatch.setattr(
        "src.github_client._fetch_ci_status_rest",
        lambda repo, sha: ([], {}, False),
    )
    monkeypatch.setattr(
        "src.github_client.get_pr_review_status",
        lambda repo, number, pr_author, head_sha: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert prs[0].ci_status == CIStatus.SUCCESS


def test_get_open_prs_falls_back_to_rest_on_graphql_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("GraphQL: API rate limit exceeded")

    monkeypatch.setattr("src.github_client.run_gh", fail_graphql)
    monkeypatch.setattr(
        "src.github_client._gh_api_paginated",
        lambda path: [
            {"number": 0},
            {
                "number": 42,
                "title": "PR-110: Add coverage",
                "head": {
                    "ref": "feature-branch",
                    "sha": "abc123",
                    "repo": {"fork": True},
                },
                "html_url": "https://example.test/pr/42",
                "updated_at": "2026-04-18T11:22:33Z",
                "user": {"login": "alice"},
                "labels": [{"name": "escalated"}],
            }
        ],
    )
    monkeypatch.setattr(
        "src.github_client.get_pr_review_status",
        lambda repo, number, pr_author, head_sha: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert [pr.number for pr in prs] == [42]
    assert prs[0].branch == "feature-branch"
    assert prs[0].ci_status == CIStatus.SUCCESS
    assert prs[0].review_status == ReviewStatus.PENDING
    assert prs[0].last_activity == datetime(2026, 4, 18, 11, 22, 33, tzinfo=_tz.utc)
    assert prs[0].is_escalated is True
    assert prs[0].is_cross_repository is True


def test_get_open_prs_propagates_non_rate_limit_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("gh failed")

    monkeypatch.setattr("src.github_client.run_gh", fail_graphql)

    with pytest.raises(RuntimeError, match="gh failed"):
        get_open_prs("owner/name")


def test_get_open_prs_rest_fallback_returns_empty_for_unexpected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("GraphQL: API rate limit exceeded")

    monkeypatch.setattr("src.github_client.run_gh", fail_graphql)
    monkeypatch.setattr("src.github_client._gh_api_paginated", lambda path: None)

    assert get_open_prs("owner/name") == []


def test_get_open_prs_returns_empty_for_non_list_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: {"items": []})
    assert get_open_prs("owner/name") == []


def test_get_merged_prs_raises_on_unexpected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    monkeypatch.setattr("src.github_client._gh_api_paginated", lambda path: None)

    with pytest.raises(RuntimeError, match="unexpected payload"):
        get_merged_prs("owner/name", refresh=True)


def test_get_merged_prs_skips_zero_number_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    monkeypatch.setattr(
        "src.github_client._gh_api_paginated",
        lambda path: [
            {
                "number": 0,
                "title": "PR-000: skip me",
                "merged_at": "2026-04-18T00:00:00Z",
                "base": {"ref": "main"},
                "head": {"ref": "branch", "repo": {"fork": False}},
            }
        ],
    )

    assert get_merged_prs("owner/name", refresh=True) == []


def test_is_pr_merged_returns_none_for_non_string_non_dict_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: ["bad"])
    assert is_pr_merged("owner/name", 42) is None


def test_is_pr_merged_returns_none_for_open_unmerged_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: {"state": "open", "merged": False},
    )
    assert is_pr_merged("owner/name", 42) is None


def test_pr_state_returns_dict_for_merged_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, str | None]:
        captured.append((args, kwargs))
        return {
            "state": "merged",
            "mergedAt": "2026-04-26T12:00:00Z",
            "closedAt": "2026-04-26T12:00:00Z",
        }

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    result = pr_state("owner/name", 42)

    assert result == {
        "state": "MERGED",
        "mergedAt": "2026-04-26T12:00:00Z",
        "closedAt": "2026-04-26T12:00:00Z",
    }
    assert captured == [(
        ["pr", "view", "42", "--json", "state,mergedAt,closedAt"],
        {"repo": "owner/name"},
    )]


def test_pr_state_returns_dict_for_open_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: {"state": "open", "mergedAt": None, "closedAt": None},
    )

    assert pr_state("owner/name", 42) == {
        "state": "OPEN",
        "mergedAt": None,
        "closedAt": None,
    }


def test_pr_state_parses_string_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: (
            '{"state": "closed", "mergedAt": null, '
            '"closedAt": "2026-04-26T13:00:00Z"}'
        ),
    )

    assert pr_state("owner/name", 42) == {
        "state": "CLOSED",
        "mergedAt": None,
        "closedAt": "2026-04-26T13:00:00Z",
    }


def test_pr_state_returns_none_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise subprocess.TimeoutExpired(cmd=args, timeout=30)

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_on_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise OSError("gh missing")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_for_malformed_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh", lambda *args, **kwargs: "{not-json"
    )
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_for_unexpected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh", lambda *args, **kwargs: ["unexpected"]
    )
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_when_state_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: {"mergedAt": None, "closedAt": None},
    )
    assert pr_state("owner/name", 42) is None


def test_pr_state_normalizes_non_string_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: {
            "state": "closed",
            "mergedAt": 12345,
            "closedAt": None,
        },
    )

    assert pr_state("owner/name", 42) == {
        "state": "CLOSED",
        "mergedAt": None,
        "closedAt": None,
    }


def test_get_pr_review_status_propagates_issue_comment_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            raise RuntimeError("boom")
        return []

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="boom"):
        get_pr_review_status("owner/name", 42, pr_author="author")


def test_get_pr_review_status_propagates_review_comment_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            return []
        if path.endswith("/pulls/42/comments"):
            raise RuntimeError("boom")
        return []

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="boom"):
        get_pr_review_status("owner/name", 42, pr_author="author")


def test_get_pr_review_status_ignores_anchor_reaction_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    anchor = {
        "id": 99,
        "body": "@codex review",
        "created_at": "2026-04-18T00:00:00Z",
        "user": {"login": "author"},
    }

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            return [anchor]
        if path.endswith("/pulls/42/comments"):
            return []
        if path.endswith("/issues/comments/99/reactions"):
            raise RuntimeError("HTTP 404 not found")
        return []

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


def test_post_comment_uses_pr_comment_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], str | None]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, timeout: int = 30) -> str:
        calls.append((args, repo))
        return ""

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    post_comment("owner/name", 42, "hello")

    assert calls == [(["pr", "comment", "42", "--body", "hello"], "owner/name")]


def test_get_pr_author_returns_empty_for_non_string_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: {"login": "alice"})
    assert get_pr_author("owner/name", 42) == ""


def test_get_pr_head_commit_iso_returns_empty_when_head_sha_missing_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: {"sha": "abc"})
    assert get_pr_head_commit_iso("owner/name", 42) == ""


def test_get_pr_head_commit_iso_returns_empty_when_commit_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], repo: str | None = None, timeout: int = 30) -> object:
        if any("/pulls/" in a for a in args):
            return {"head": {"sha": "abc123"}}
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    assert get_pr_head_commit_iso("owner/name", 42) == ""


def test_get_pr_metadata_returns_empty_on_invalid_json_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: "{not-json")

    assert get_pr_metadata("owner/name", 42) == {
        "author": "",
        "head_sha": "",
        "head_commit_date": "",
    }


def test_get_pr_metadata_parses_json_string_without_commit_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: '{"author": "alice", "head_sha": ""}',
    )

    assert get_pr_metadata("owner/name", 42) == {
        "author": "alice",
        "head_sha": "",
        "head_commit_date": "",
    }


def test_get_pr_metadata_returns_empty_on_non_mapping_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: ["bad"])

    assert get_pr_metadata("owner/name", 42) == {
        "author": "",
        "head_sha": "",
        "head_commit_date": "",
    }


def test_get_pr_metadata_ignores_commit_date_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], repo: str | None = None, timeout: int = 30) -> dict:
        if "/pulls/" in args[1]:
            return {"author": "alice", "head_sha": "abc123"}
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    assert get_pr_metadata("owner/name", 42) == {
        "author": "alice",
        "head_sha": "abc123",
        "head_commit_date": "",
    }


def test_get_branch_last_push_time_tracks_new_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_last_known_sha()
    shas = iter(["sha1", "sha1", "sha2"])
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: next(shas))
    monkeypatch.setattr("src.github_client.time.monotonic", lambda: 123.45)

    assert get_branch_last_push_time("owner/name", 42) is None
    assert get_branch_last_push_time("owner/name", 42) is None
    assert get_branch_last_push_time("owner/name", 42) == 123.45


def test_get_branch_last_push_time_returns_none_for_empty_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: "")
    assert get_branch_last_push_time("owner/name", 42) is None


def test_get_branch_last_push_time_propagates_poll_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(github_client.GitHubPollError, match="boom"):
        get_branch_last_push_time("owner/name", 42)


def test_clear_last_known_sha_resets_tracking() -> None:
    github_client._last_known_sha["owner/name#42"] = "sha1"
    clear_last_known_sha()
    assert github_client._last_known_sha == {}


def test_get_last_push_age_seconds_returns_none_without_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: "")
    assert get_last_push_age_seconds("owner/name", 42) is None


def test_get_last_push_age_seconds_returns_computed_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz: _tz | None = None) -> datetime:
            return cls(2026, 4, 19, 12, 0, 0, tzinfo=tz)

    responses = iter(["feature-branch", "2026-04-19T11:59:30Z"])
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr("src.github_client.datetime", _FakeDateTime)

    assert get_last_push_age_seconds("owner/name", 42) == 30.0


def test_get_last_push_age_seconds_returns_none_for_empty_push_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", ""])
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: next(responses))
    assert get_last_push_age_seconds("owner/name", 42) is None


def test_get_last_push_age_seconds_returns_none_on_parse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "not-a-date"])
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: next(responses))
    assert get_last_push_age_seconds("owner/name", 42) is None


def test_get_pr_last_push_time_returns_parsed_datetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "2026-04-30T11:59:30Z"])
    monkeypatch.setattr(
        "src.github_client.run_gh", lambda *args, **kwargs: next(responses)
    )

    result = get_pr_last_push_time("owner/name", 42)

    assert result == datetime(2026, 4, 30, 11, 59, 30, tzinfo=_tz.utc)


def test_get_pr_last_push_time_returns_none_without_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github_client.run_gh", lambda *args, **kwargs: "")
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_when_activity_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", ""])
    monkeypatch.setattr(
        "src.github_client.run_gh", lambda *args, **kwargs: next(responses)
    )
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_on_parse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "not-a-date"])
    monkeypatch.setattr(
        "src.github_client.run_gh", lambda *args, **kwargs: next(responses)
    )
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_on_run_gh_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_a: Any, **_kw: Any) -> str:
        raise RuntimeError("gh boom")

    monkeypatch.setattr("src.github_client.run_gh", boom)
    assert get_pr_last_push_time("owner/name", 42) is None


def test_has_recent_codex_review_request_returns_false_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("HTTP 404 not found")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert has_recent_codex_review_request("owner/name", 42, "author") is False


def test_has_recent_codex_review_request_propagates_non_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="boom"):
        has_recent_codex_review_request("owner/name", 42, "author")


def test_has_recent_codex_review_request_skips_invalid_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client._gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "author"},
                "body": "@codex review",
                "created_at": "not-a-date",
            }
        ],
    )

    assert has_recent_codex_review_request("owner/name", 42, "author") is False


def test_has_recent_codex_review_request_handles_naive_datetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz: _tz | None = None) -> datetime:
            return cls(2026, 4, 19, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(
        "src.github_client._gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "author"},
                "body": "@codex review",
                "created_at": "2026-04-19T11:59:30",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github_client._parse_iso",
        lambda value: datetime(2026, 4, 19, 11, 59, 30),
    )
    monkeypatch.setattr("src.github_client.datetime", _FakeDateTime)

    assert has_recent_codex_review_request("owner/name", 42, "author") is True


def test_gh_api_paginated_returns_none_for_non_list_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.github_client import _gh_api_paginated

    monkeypatch.setattr(
        "src.github_client.retry_transient",
        lambda func, operation_name=None: {"items": []},
    )

    assert _gh_api_paginated("repos/test/owner/issues/1/comments") is None


def test_get_codex_review_signals_returns_empty_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("HTTP 404 not found")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert _get_codex_review_signals("owner/name", 42) == {
        "latest_sha": "",
        "latest_time": None,
        "latest_state": "",
    }


def test_get_codex_review_signals_skips_non_codex_and_invalid_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github_client._gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "alice"},
                "commit_id": "sha1",
                "submitted_at": "2026-04-19T11:59:00Z",
                "state": "approved",
            },
            {
                "user": {"login": "chatgpt-codex-connector"},
                "commit_id": "sha2",
                "submitted_at": "bad-timestamp",
                "state": "approved",
            },
        ],
    )

    assert _get_codex_review_signals("owner/name", 42) == {
        "latest_sha": "",
        "latest_time": None,
        "latest_state": "",
    }


def test_get_latest_codex_review_info_returns_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    submitted_at = datetime(2026, 4, 19, 11, 0, 0, tzinfo=_tz.utc)
    monkeypatch.setattr(
        "src.github_client._get_codex_review_signals",
        lambda repo, pr_number: {
            "latest_sha": "sha123",
            "latest_time": submitted_at,
            "latest_state": "APPROVED",
        },
    )

    assert _get_latest_codex_review_info("owner/name", 42) == ("sha123", submitted_at)


def test_map_rest_ci_status_failure_states_take_precedence() -> None:
    assert (
        _map_rest_ci_status_to_enum(
            ["ignore-me", {"conclusion": "failure"}, {"conclusion": "success"}],
            {"state": "success", "statuses": [{"state": "success"}]},
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_failure_from_commit_status_only() -> None:
    """A failing legacy commit status alone is enough to map to FAILURE."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "failure", "statuses": [{"state": "failure"}]},
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_action_required_treated_as_failure() -> None:
    """``action_required`` is a check-run failure conclusion in REST."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "action_required"}], {}
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_success_requires_all_states_success_like() -> None:
    assert (
        _map_rest_ci_status_to_enum(
            [
                {"conclusion": "neutral"},
                {"conclusion": "skipped"},
                {"status": "completed"},
            ],
            {"state": "success", "statuses": [{"state": "success"}]},
        )
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_pending_when_states_missing_or_mixed() -> None:
    assert _map_rest_ci_status_to_enum([{}, {"conclusion": ""}], {}) == CIStatus.PENDING
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}, {"status": "in_progress"}], {}
        )
        == CIStatus.PENDING
    )


def test_map_rest_ci_status_pending_from_status_in_progress() -> None:
    """A check-run still ``in_progress`` keeps the rollup PENDING."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"status": "in_progress"}],
            {"state": "pending", "statuses": [{"state": "pending"}]},
        )
        == CIStatus.PENDING
    )


def test_map_rest_ci_status_skips_non_dict_entries() -> None:
    """Garbage entries in either list are tolerated and ignored."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}, "garbage"],
            {"state": "success", "statuses": [{"state": "success"}, 7]},
        )
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_combined_state_failure_overrides_paginated_statuses() -> None:
    """``status_payload['state']`` must outrank the embedded statuses list.

    The combined-status endpoint caps ``statuses`` at the first page while
    ``state`` aggregates every context. A repo with many legacy status
    contexts can show success-only entries on page 1 with ``state='failure'``
    surfaced from a context past the cap; honoring ``state`` keeps that
    failure from slipping past WATCH/MERGE.
    """
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "failure", "statuses": [{"state": "success"}]},
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_combined_state_error_treated_as_failure() -> None:
    """The combined ``state='error'`` value must map to FAILURE."""
    assert (
        _map_rest_ci_status_to_enum(
            [],
            {"state": "error", "statuses": [{"state": "success"}]},
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_combined_state_pending_keeps_rollup_pending() -> None:
    """A combined ``state='pending'`` keeps the rollup PENDING."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "pending", "statuses": [{"state": "success"}]},
        )
        == CIStatus.PENDING
    )


def test_map_rest_ci_status_combined_state_ignored_when_no_statuses() -> None:
    """Synthetic ``state='pending'`` from an empty statuses list is ignored.

    GitHub returns ``state='pending'`` by default when a commit has zero
    legacy statuses; that synthetic value must not override successful
    check-runs as the only signal.
    """
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "pending", "statuses": []},
        )
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_stale_failure_in_history_does_not_override_combined_success() -> None:
    """Reverse-chronological ``statuses`` history must not force FAILURE.

    The combined-status endpoint returns every per-context status in
    reverse chronological order, so a context that flipped failure ->
    success on retry shows both entries with the failure listed first.
    The aggregate ``state`` already reduces to the latest per context;
    iterating over the full history and treating any ``failure`` as
    terminal would block green PRs whose latest statuses are all
    success. Trusting ``state`` and ignoring the per-entry list keeps
    that path green.
    """
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {
                "state": "success",
                "statuses": [
                    {"context": "ci/foo", "state": "success"},
                    {"context": "ci/foo", "state": "failure"},
                ],
            },
        )
        == CIStatus.SUCCESS
    )


def test_fetch_ci_status_rest_combines_check_runs_and_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_fetch_ci_status_rest`` flattens check-runs and parses status."""
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(list(args))
        if any("check-runs" in a for a in args):
            return [
                {
                    "total_count": 2,
                    "check_runs": [
                        {"id": 1, "conclusion": "success"},
                        {"id": 2, "conclusion": "neutral"},
                    ],
                },
                {
                    "total_count": 1,
                    "check_runs": [
                        {"id": 3, "status": "in_progress"},
                    ],
                },
            ]
        return {"state": "pending", "statuses": [{"state": "pending"}]}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert [r["id"] for r in check_runs] == [1, 2, 3]
    assert status_payload == {"state": "pending", "statuses": [{"state": "pending"}]}
    assert any("--paginate" in c for c in calls)
    assert any("per_page=100" in a for c in calls for a in c)
    assert any("--include" in c for c in calls)
    assert fetch_ok is True


def test_fetch_ci_status_rest_returns_empty_for_blank_sha() -> None:
    """A missing SHA short-circuits both REST calls."""
    assert _fetch_ci_status_rest("owner/name", "") == ([], {}, True)


def test_fetch_ci_status_rest_degrades_on_check_runs_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A check-runs API failure leaves an empty list but still fetches status."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            raise RuntimeError("HTTP 503")
        return {"state": "success", "statuses": [{"state": "success"}]}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == {"state": "success", "statuses": [{"state": "success"}]}
    assert fetch_ok is True


def test_fetch_ci_status_rest_degrades_on_status_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A combined-status API failure leaves an empty status payload."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [{"check_runs": [{"conclusion": "success"}]}]
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == [{"conclusion": "success"}]
    assert status_payload == {}
    assert fetch_ok is True


def test_fetch_ci_status_rest_marks_fetch_failure_when_both_endpoints_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both endpoints raising must surface as ``fetch_ok=False``.

    The flag is retained for observability/telemetry even though the
    mapper currently folds it back into ``empty_is_success``; callers
    that surface "fetch failed" diagnostics still need this signal.
    """

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        raise RuntimeError("HTTP 403")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == {}
    assert fetch_ok is False


def test_fetch_ci_status_rest_partial_failure_trusts_empty_survivor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One endpoint raising while the survivor returns an empty signal must
    still surface ``fetch_ok=True``.

    The testbed's GitHub App grants ``Commit statuses`` but not ``Checks``, so
    ``check-runs`` raises 403 while ``status`` legitimately reports zero
    contexts. Treating that as a fetch failure permanently blocked the
    auto-merge gate even when the operator opted into
    ``allow_merge_without_checks``. Trusting the surviving endpoint's empty
    report restores the previous "no checks = green when explicitly allowed"
    semantics; the both-endpoints-failed case below remains the fetch-failure
    safety net.
    """

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            raise RuntimeError("HTTP 403")
        return {"state": "pending", "statuses": []}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == {"state": "pending", "statuses": []}
    assert fetch_ok is True


def test_fetch_ci_status_rest_partial_failure_status_side_with_empty_check_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror: ``status`` fails, ``check-runs`` returns empty — fetch still ok."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [{"check_runs": []}]
        raise RuntimeError("HTTP 403")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == {}
    assert fetch_ok is True


def test_fetch_ci_status_rest_parses_string_status_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run_gh`` may return raw JSON text; ``_fetch_ci_status_rest`` parses it."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [{"check_runs": []}]
        return '{"state": "success", "statuses": [{"state": "success"}]}'

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    _, status_payload, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert status_payload == {"state": "success", "statuses": [{"state": "success"}]}


def test_fetch_ci_status_rest_string_status_invalid_json_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed string status payload degrades to an empty dict."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [{"check_runs": []}]
        return "not-json"

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    _, status_payload, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert status_payload == {}


def test_fetch_ci_status_rest_ignores_non_list_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``gh api --slurp`` returns an unexpected shape, fall back to empty."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return {"unexpected": True}
        return {"state": "pending", "statuses": []}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    check_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []


def test_fetch_ci_status_rest_skips_non_dict_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-dict and non-list ``check_runs`` entries are tolerated."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [
                "garbage",
                {"check_runs": "also-garbage"},
                {"check_runs": [{"conclusion": "success"}, "junk"]},
            ]
        return {}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    check_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == [{"conclusion": "success"}]


def test_fetch_ci_status_rest_caches_per_repo_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeat calls within the TTL must not re-issue ``gh api`` requests.

    Regression guard: at ``poll_interval_sec=2`` (test config), refetching
    on every cycle exhausts the 5000/hour REST budget within minutes and
    pauses the daemon, blocking integration tests that wait for the
    runner to log ``Paused. Press Play to resume.`` after a ``/stop``.
    """
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(list(args))
        if any("check-runs" in a for a in args):
            return [{"check_runs": [{"conclusion": "success"}]}]
        return {"state": "success", "statuses": [{"state": "success"}]}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    first = _fetch_ci_status_rest("owner/name", "abc123")
    second = _fetch_ci_status_rest("owner/name", "abc123")
    third = _fetch_ci_status_rest("owner/name", "abc123")

    assert first == second == third
    assert len(calls) == 2  # one check-runs + one status, served from cache after


def test_fetch_ci_status_rest_cache_misses_on_new_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A different head SHA (e.g. after a push) must bypass the cache."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            sha = next(a for a in args if "check-runs" in a).split("/")[-2]
            return [{"check_runs": [{"conclusion": "success", "id": sha}]}]
        return {"state": "success", "statuses": [{"state": "success"}]}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    first_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    second_runs, _, _ = _fetch_ci_status_rest("owner/name", "def456")

    assert first_runs[0]["id"] == "abc123"
    assert second_runs[0]["id"] == "def456"


def test_fetch_ci_status_rest_cache_expires_after_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached entry older than the TTL must be refetched, so PENDING -> SUCCESS
    transitions on the same SHA are observed without an upstream push."""
    state = {"calls": 0}

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            state["calls"] += 1
            return [{"check_runs": [{"conclusion": f"call_{state['calls']}"}]}]
        return {"state": "pending", "statuses": []}

    fake_now = {"value": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["value"]

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github_client.time.monotonic", fake_monotonic)

    first, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    fake_now["value"] += 5.0
    cached, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert first == cached  # within TTL: cached

    fake_now["value"] += 100.0  # past 15s TTL
    refreshed, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert refreshed != first


def test_clear_ci_status_cache_forces_refetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``clear_ci_status_cache`` drops the in-memory entries (used by tests)."""
    state = {"calls": 0}

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            state["calls"] += 1
            return [{"check_runs": [{"conclusion": f"call_{state['calls']}"}]}]
        return {}

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)

    _fetch_ci_status_rest("owner/name", "abc123")
    clear_ci_status_cache()
    _fetch_ci_status_rest("owner/name", "abc123")

    assert state["calls"] == 2


def test_fetch_ci_status_rest_evicts_expired_entries_for_old_shas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expired entries for previous head SHAs must be dropped when a new
    SHA misses the cache.

    Regression guard: without sweeping, a long-running daemon would leak
    one entry (with its full check-run payload) per push for every
    watched repo, since lookups only touch the currently requested key.
    """
    from src.github_client import _ci_status_cache

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [{"check_runs": [{"conclusion": "success"}]}]
        return {"state": "success", "statuses": [{"state": "success"}]}

    fake_now = {"value": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["value"]

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github_client.time.monotonic", fake_monotonic)

    _fetch_ci_status_rest("owner/name", "sha-old")
    assert ("owner/name", "sha-old") in _ci_status_cache

    fake_now["value"] += 100.0  # past 15s TTL
    _fetch_ci_status_rest("owner/name", "sha-new")

    # Old key swept on the new write; only the fresh entry remains.
    assert ("owner/name", "sha-old") not in _ci_status_cache
    assert ("owner/name", "sha-new") in _ci_status_cache


def test_fetch_ci_status_rest_eviction_preserves_unexpired_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entries that are still inside the TTL must not be swept when a
    cache miss for a different SHA triggers eviction."""
    from src.github_client import _ci_status_cache

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [{"check_runs": [{"conclusion": "success"}]}]
        return {"state": "success", "statuses": [{"state": "success"}]}

    fake_now = {"value": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["value"]

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github_client.time.monotonic", fake_monotonic)

    _fetch_ci_status_rest("owner/name", "sha-fresh")
    fake_now["value"] += 1.0  # still well inside the 15s TTL
    _fetch_ci_status_rest("owner/name", "sha-other")

    assert ("owner/name", "sha-fresh") in _ci_status_cache
    assert ("owner/name", "sha-other") in _ci_status_cache


def test_parse_iso_returns_none_for_invalid_string() -> None:
    assert _parse_iso("not-a-date") is None


def test_get_current_rate_limit_budget_returns_persisted_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    from src.daemon.github_rate_limit import (
        BUDGET_REDIS_KEY,
        RateLimitBudget,
    )

    class _FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

    redis = _FakeRedis()
    redis.store[BUDGET_REDIS_KEY] = RateLimitBudget(
        installation_id=None,
        remaining=42,
        limit=5000,
        reset_at=datetime.fromtimestamp(1745683200, tz=_tz.utc),
    ).to_redis_payload()

    result = asyncio.run(github_client.get_current_rate_limit_budget(redis))
    assert result is not None
    assert result.remaining == 42


def test_get_current_rate_limit_budget_none_when_no_observation() -> None:
    import asyncio

    class _FakeRedis:
        async def get(self, key: str) -> str | None:
            return None

    assert asyncio.run(
        github_client.get_current_rate_limit_budget(_FakeRedis())
    ) is None


def _bucket(remaining: int, limit: int = 5000, reset: int = 1745683200) -> dict:
    return {"remaining": remaining, "limit": limit, "reset": reset}


def test_fetch_rate_limit_budget_parses_dict_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: {
            "core": _bucket(remaining=4321),
            "graphql": _bucket(remaining=4900),
        },
    )
    budget = github_client.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 4321
    assert budget.limit == 5000


def test_fetch_rate_limit_budget_parses_string_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: (
            '{"core": {"remaining": 100, "limit": 5000, "reset": 0},'
            ' "graphql": {"remaining": 4500, "limit": 5000, "reset": 0}}'
        ),
    )
    budget = github_client.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 100


def test_fetch_rate_limit_budget_returns_graphql_when_more_constrained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GraphQL exhaustion must surface even when REST/core is healthy."""
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: {
            "core": _bucket(remaining=4900),
            "graphql": _bucket(remaining=10),
        },
    )
    budget = github_client.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 10


def test_fetch_rate_limit_budget_falls_back_when_one_bucket_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: {"core": _bucket(remaining=4321)},
    )
    budget = github_client.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 4321


def test_fetch_rate_limit_budget_returns_none_for_invalid_json_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client, "run_gh", lambda args, **kw: "not-json"
    )
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_for_unexpected_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_client, "run_gh", lambda args, **kw: [1, 2])
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_for_missing_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: {"core": {"remaining": 10}},
    )
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_when_both_buckets_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_client, "run_gh", lambda args, **kw: {})
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_when_gh_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(args: list[str], **kw: Any) -> None:
        raise RuntimeError("API rate limit exceeded")

    monkeypatch.setattr(github_client, "run_gh", _raise)
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_when_gh_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(args: list[str], **kw: Any) -> None:
        raise OSError("gh missing")

    monkeypatch.setattr(github_client, "run_gh", _raise)
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_on_malformed_int(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: {
            "core": {"remaining": "abc", "limit": 5000, "reset": 0},
            "graphql": {"remaining": "xyz", "limit": 5000, "reset": 0},
        },
    )
    assert github_client.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_buckets_returns_each_bucket_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both buckets surface independently so the dashboard renders each chip."""
    monkeypatch.setattr(
        github_client,
        "run_gh",
        lambda args, **kw: {
            "core": _bucket(remaining=4321),
            "graphql": _bucket(remaining=120),
        },
    )
    rest, graphql = github_client.fetch_rate_limit_buckets()
    assert rest is not None and rest.remaining == 4321
    assert graphql is not None and graphql.remaining == 120


def test_fetch_rate_limit_buckets_returns_pair_of_none_on_gh_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(args: list[str], **kw: Any) -> None:
        raise RuntimeError("gh down")

    monkeypatch.setattr(github_client, "run_gh", _raise)
    assert github_client.fetch_rate_limit_buckets() == (None, None)


def test_fetch_rate_limit_buckets_returns_pair_of_none_on_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_client, "run_gh", lambda args, **kw: "not-json")
    assert github_client.fetch_rate_limit_buckets() == (None, None)


def test_fetch_rate_limit_buckets_returns_pair_of_none_on_unexpected_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(github_client, "run_gh", lambda args, **kw: [1])
    assert github_client.fetch_rate_limit_buckets() == (None, None)


def test_get_latest_codex_feedback_collects_post_anchor_codex_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue and review comments authored by Codex after the anchor are joined."""
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "codex-bot"},
                    "body": "stale before-anchor feedback",
                    "created_at": "2026-04-26T00:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-04-27T00:00:00Z",
                },
                {
                    "id": 3,
                    "user": {"login": "codex-bot"},
                    "body": "P1: rename foo",
                    "created_at": "2026-04-27T01:00:00Z",
                },
                {
                    "id": 4,
                    "user": {"login": "teammate"},
                    "body": "looks good",
                    "created_at": "2026-04-27T02:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return [
                {
                    "id": 5,
                    "user": {"login": "codex-bot"},
                    "body": "P2: extract helper",
                    "created_at": "2026-04-27T03:00:00Z",
                }
            ]
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    out = github_client.get_latest_codex_feedback("owner/name", 42)
    assert out == "P1: rename foo\n\nP2: extract helper"


def test_get_latest_codex_feedback_returns_none_when_no_codex_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-04-27T00:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert github_client.get_latest_codex_feedback("owner/name", 42) is None


def test_get_latest_codex_feedback_skips_onboarding_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-04-27T00:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "codex-bot"},
                    "body": "Please create a Codex account and connect to github.",
                    "created_at": "2026-04-27T01:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert github_client.get_latest_codex_feedback("owner/name", 42) is None


def test_get_latest_codex_feedback_returns_all_codex_comments_when_no_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No PR-author ``@codex review`` trigger: every Codex comment counts."""
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "codex-bot"},
                    "body": "feedback before any anchor",
                    "created_at": "2026-04-27T01:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    out = github_client.get_latest_codex_feedback("owner/name", 42)
    assert out == "feedback before any anchor"


def test_get_latest_codex_feedback_skips_non_author_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ``@codex review`` posted by a teammate is not the anchor."""
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "codex-bot"},
                    "body": "P1: real feedback",
                    "created_at": "2026-04-27T00:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "teammate"},
                    "body": "@codex review",
                    "created_at": "2026-04-27T02:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    out = github_client.get_latest_codex_feedback("owner/name", 42)
    assert out == "P1: real feedback"


def test_get_latest_codex_feedback_returns_none_when_endpoints_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("api blew up")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert github_client.get_latest_codex_feedback("owner/name", 42) is None


def test_get_latest_codex_feedback_returns_none_when_endpoints_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint ``TimeoutExpired`` / ``OSError`` must degrade to ``None``,
    not bubble out and abort the FIX cycle before the coder runs.
    """
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    raised: list[type[BaseException]] = []
    exceptions: list[BaseException] = [
        subprocess.TimeoutExpired(cmd=["gh"], timeout=30),
        FileNotFoundError("gh: command not found"),
    ]

    def fake_paginated(path: str) -> list[dict]:
        if not exceptions:
            raise AssertionError("unexpected extra call")
        exc = exceptions.pop(0)
        raised.append(type(exc))
        raise exc

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert github_client.get_latest_codex_feedback("owner/name", 42) is None
    assert raised == [subprocess.TimeoutExpired, FileNotFoundError]


def test_get_latest_codex_feedback_truncates_oversized_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Joined feedback must be capped to avoid ``Argument list too long``
    when the FIX prompt embeds it as a single CLI argument.
    """
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    big_body = "x" * 6000

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-04-27T00:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "codex-bot"},
                    "body": big_body,
                    "created_at": "2026-04-27T01:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    out = github_client.get_latest_codex_feedback("owner/name", 42)
    assert out is not None
    assert out.startswith("[truncated]\n")
    assert len(out) == len("[truncated]\n") + github_client._REVIEW_FEEDBACK_TRUNCATE_CHARS
    assert out.endswith("x" * 100)


def test_get_latest_codex_feedback_skips_empty_codex_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        github_client, "get_pr_author", lambda repo, n: "author"
    )

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-04-27T00:00:00Z",
                },
                {
                    "id": 2,
                    "user": {"login": "codex-bot"},
                    "body": "   ",
                    "created_at": "2026-04-27T01:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github_client._gh_api_paginated", fake_paginated)

    assert github_client.get_latest_codex_feedback("owner/name", 42) is None


# ---------------------------------------------------------------------------
# _etag_get conditional-request helper tests (PR-191a)
# ---------------------------------------------------------------------------


def _build_include_response(
    body: str,
    *,
    status: int = 200,
    etag: str | None = 'W/"v1"',
) -> str:
    """Compose a ``gh api --include`` style response."""
    reason = {200: "OK", 304: "Not Modified", 500: "Server Error"}.get(status, "OK")
    head = f"HTTP/2.0 {status} {reason}\r\nDate: now\r\n"
    if etag is not None:
        head += f"ETag: {etag}\r\n"
    return f"{head}\r\n{body}"


@pytest.fixture(autouse=True)
def _clear_etag_cache_between_tests() -> None:
    github_client.clear_etag_cache()


def test_etag_get_first_call_populates_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First call has no ``If-None-Match``, parses 200 body, caches the ETag."""
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(
            stdout=_build_include_response('{"merged": true}', etag='W/"abc"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = github_client._etag_get("repos/owner/name/pulls/42")

    assert payload == {"merged": True}
    assert "--include" in captured[0]
    assert not any("If-None-Match" in arg for arg in captured[0])
    assert github_client._etag_cache["repos/owner/name/pulls/42"] == (
        'W/"abc"',
        {"merged": True},
    )


def test_etag_get_second_call_sends_if_none_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached ETag must be echoed back as ``If-None-Match`` on the next call."""
    captured: list[list[str]] = []
    responses = iter(
        [
            _build_include_response('{"merged": false}', etag='W/"v1"'),
            _build_include_response("", status=304, etag='W/"v1"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    github_client._etag_get("repos/owner/name/pulls/7")
    github_client._etag_get("repos/owner/name/pulls/7")

    assert 'If-None-Match: W/"v1"' in captured[1]


def test_etag_get_304_returns_cached_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 304 response must short-circuit to the cached payload."""
    responses = iter(
        [
            _build_include_response('{"merged": true, "n": 1}', etag='W/"v1"'),
            _build_include_response("", status=304, etag='W/"v1"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    first = github_client._etag_get("repos/owner/name/pulls/9")
    second = github_client._etag_get("repos/owner/name/pulls/9")

    assert first == {"merged": True, "n": 1}
    assert second == {"merged": True, "n": 1}


def test_etag_get_200_with_new_etag_replaces_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh 200 response must overwrite the cached ETag and payload."""
    responses = iter(
        [
            _build_include_response('{"v": 1}', etag='W/"v1"'),
            _build_include_response('{"v": 2}', etag='W/"v2"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    first = github_client._etag_get("repos/owner/name/commits/abc")
    second = github_client._etag_get("repos/owner/name/commits/abc")

    assert first == {"v": 1}
    assert second == {"v": 2}
    assert github_client._etag_cache["repos/owner/name/commits/abc"] == (
        'W/"v2"',
        {"v": 2},
    )


def test_etag_get_evicts_oldest_when_max_entries_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cache must drop the least-recently-used entry past ``_ETAG_CACHE_MAX_ENTRIES``."""
    monkeypatch.setattr(github_client, "_ETAG_CACHE_MAX_ENTRIES", 3)
    counter = {"i": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        counter["i"] += 1
        body = f'{{"i": {counter["i"]}}}'
        return _FakeCompletedProcess(
            stdout=_build_include_response(body, etag=f'W/"e{counter["i"]}"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    github_client._etag_get("repos/x/y/pulls/1")
    github_client._etag_get("repos/x/y/pulls/2")
    github_client._etag_get("repos/x/y/pulls/3")
    github_client._etag_get("repos/x/y/pulls/4")  # forces eviction of /pulls/1

    assert "repos/x/y/pulls/1" not in github_client._etag_cache
    assert {
        "repos/x/y/pulls/2",
        "repos/x/y/pulls/3",
        "repos/x/y/pulls/4",
    } <= set(github_client._etag_cache.keys())


def test_etag_get_returns_none_on_unparseable_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 200 with malformed JSON must not crash and must not poison the cache."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response("{not-json", etag='W/"v1"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert github_client._etag_get("repos/owner/name/pulls/1") is None
    assert "repos/owner/name/pulls/1" not in github_client._etag_cache


def test_etag_get_returns_none_when_304_without_prior_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 304 with no cached payload (e.g. server-side hiccup) must yield None."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response("", status=304, etag='W/"v1"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert github_client._etag_get("repos/owner/name/pulls/3") is None


def test_etag_get_passthrough_for_pre_parsed_run_gh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``run_gh`` is stubbed to return a parsed object (no HTTP head),
    ``_etag_get`` must surface it directly so call-site tests retain their
    semantics without crafting raw ``--include`` strings."""
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda args: {"merged": True, "state": "closed"},
    )

    payload = github_client._etag_get("repos/owner/name/pulls/5")
    assert payload == {"merged": True, "state": "closed"}
    # Cache stays empty because the test bypassed the --include path.
    assert "repos/owner/name/pulls/5" not in github_client._etag_cache


def test_etag_get_returns_none_on_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 5xx response (rare; gh normally raises) yields None and leaves cache untouched."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response("server error", status=500, etag=None)
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert github_client._etag_get("repos/owner/name/pulls/8") is None
    assert "repos/owner/name/pulls/8" not in github_client._etag_cache


def test_etag_get_empty_200_body_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 200 with no body (degenerate) yields None rather than crashing on JSON."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response("", status=200, etag='W/"v1"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert github_client._etag_get("repos/owner/name/pulls/9") is None


# ---------------------------------------------------------------------------
# _etag_get_paginated + _invalidate_etag_cache (PR-191b: list endpoints)
# ---------------------------------------------------------------------------


def test_etag_get_paginated_walks_pages_and_caches_each(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each page must round-trip its own ETag and land in ``_etag_cache``."""
    captured: list[list[str]] = []
    page1_body = "[" + ",".join(f'{{"n": {i}}}' for i in range(100)) + "]"
    page2_body = '[{"n": 100}, {"n": 101}]'
    responses = iter(
        [
            _build_include_response(page1_body, etag='W/"p1"'),
            _build_include_response(page2_body, etag='W/"p2"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = github_client._etag_get_paginated(
        "repos/owner/name/pulls?state=open&per_page=100"
    )

    assert items is not None
    assert [item["n"] for item in items] == list(range(102))
    assert len(captured) == 2
    assert (
        "repos/owner/name/pulls?state=open&per_page=100&page=1"
        in github_client._etag_cache
    )
    assert (
        "repos/owner/name/pulls?state=open&per_page=100&page=2"
        in github_client._etag_cache
    )


def test_etag_get_paginated_304_returns_cached_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 304 on a previously-fetched page must surface the cached payload."""
    page1_body = "[" + ",".join(f'{{"n": {i}}}' for i in range(100)) + "]"
    page2_body = '[{"n": 100}]'
    responses = iter(
        [
            _build_include_response(page1_body, etag='W/"p1"'),
            _build_include_response(page2_body, etag='W/"p2"'),
            _build_include_response("", status=304, etag='W/"p1"'),
            _build_include_response("", status=304, etag='W/"p2"'),
        ]
    )
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    base = "repos/owner/name/pulls?state=open&per_page=100"
    first = github_client._etag_get_paginated(base)
    second = github_client._etag_get_paginated(base)

    assert first == [{"n": i} for i in range(101)]
    assert second == first
    # Second walk must echo the cached ETags via If-None-Match.
    assert any('If-None-Match: W/"p1"' in arg for arg in captured[2])
    assert any('If-None-Match: W/"p2"' in arg for arg in captured[3])


def test_etag_get_paginated_stops_when_short_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A page shorter than ``per_page`` ends the walk without an extra call."""
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(
            stdout=_build_include_response('[{"n": 1}]', etag='W/"only"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = github_client._etag_get_paginated(
        "repos/owner/name/pulls?state=closed&per_page=100"
    )

    assert items == [{"n": 1}]
    assert len(captured) == 1


def test_etag_get_paginated_walks_past_legacy_100_page_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The walk must follow ``gh api --paginate`` semantics: no hard page cap.

    Capping at 100 pages with ``per_page=100`` would silently truncate
    ``repos/{repo}/pulls?state=closed`` lookups on large repos at 10,000
    items, hiding merged history that ``get_merged_prs`` relies on. The
    short-page heuristic is the only termination signal.
    """
    full_pages = 150  # well past the removed 100-page cap
    full_body = '[{"n": 1}, {"n": 2}]'  # per_page=2 to keep memory small
    short_body = '[{"n": 99}]'
    state = {"calls": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        state["calls"] += 1
        body = full_body if state["calls"] <= full_pages else short_body
        return _FakeCompletedProcess(
            stdout=_build_include_response(body, etag=f'W/"p{state["calls"]}"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = github_client._etag_get_paginated(
        "repos/owner/name/pulls?state=closed&per_page=2"
    )

    assert items is not None
    assert len(items) == full_pages * 2 + 1
    assert state["calls"] == full_pages + 1


def test_etag_get_paginated_uses_default_per_page_when_unspecified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``per_page=`` in the URL, GitHub's 30-default terminates the walk."""
    body = "[" + ",".join(f'{{"n": {i}}}' for i in range(15)) + "]"

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response(body, etag='W/"single"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = github_client._etag_get_paginated("repos/owner/name/pulls")

    assert items == [{"n": i} for i in range(15)]


def test_etag_get_paginated_first_page_none_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A None payload on the first page (e.g. 5xx) yields None overall."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response("server error", status=500, etag=None)
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        github_client._etag_get_paginated(
            "repos/owner/name/pulls?state=open&per_page=100"
        )
        is None
    )


def test_etag_get_paginated_first_page_non_list_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An object body (not a JSON array) on the first page yields None."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stdout=_build_include_response('{"unexpected": true}', etag='W/"v1"')
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        github_client._etag_get_paginated(
            "repos/owner/name/pulls?state=open&per_page=100"
        )
        is None
    )


def test_etag_get_paginated_later_page_none_surfaces_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A mid-walk failure surfaces the items collected so far rather than dropping all."""
    page1_body = "[" + ",".join(f'{{"n": {i}}}' for i in range(100)) + "]"
    responses = iter(
        [
            _build_include_response(page1_body, etag='W/"p1"'),
            _build_include_response("server error", status=500, etag=None),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = github_client._etag_get_paginated(
        "repos/owner/name/pulls?state=open&per_page=100"
    )
    assert items == [{"n": i} for i in range(100)]


def test_etag_get_paginated_later_page_non_list_breaks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-list response on a later page stops the walk with what was collected."""
    page1_body = "[" + ",".join(f'{{"n": {i}}}' for i in range(100)) + "]"
    responses = iter(
        [
            _build_include_response(page1_body, etag='W/"p1"'),
            _build_include_response('{"oops": 1}', etag='W/"p2"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = github_client._etag_get_paginated(
        "repos/owner/name/pulls?state=open&per_page=100"
    )
    assert items == [{"n": i} for i in range(100)]


def test_etag_get_paginated_first_page_runtime_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first-page hard ``gh`` failure must propagate so callers can react."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="boom", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="boom"):
        github_client._etag_get_paginated(
            "repos/owner/name/pulls?state=open&per_page=100"
        )


def test_etag_get_paginated_later_page_runtime_error_breaks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later-page hard failure leaves earlier items intact."""
    page1_body = "[" + ",".join(f'{{"n": {i}}}' for i in range(100)) + "]"
    state = {"calls": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        state["calls"] += 1
        if state["calls"] == 1:
            return _FakeCompletedProcess(
                stdout=_build_include_response(page1_body, etag='W/"p1"')
            )
        return _FakeCompletedProcess(stderr="transient", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        "src.github_client.is_transient_error", lambda exc: False
    )

    items = github_client._etag_get_paginated(
        "repos/owner/name/pulls?state=open&per_page=100"
    )
    assert items == [{"n": i} for i in range(100)]


def test_gh_api_paginated_routes_pulls_list_through_etag_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_gh_api_paginated`` must dispatch top-level pulls list paths to ``_etag_get_paginated``."""
    routed: list[str] = []

    monkeypatch.setattr(
        "src.github_client._etag_get_paginated",
        lambda path: routed.append(path) or [{"n": 1}],
    )

    result = github_client._gh_api_paginated(
        "repos/owner/name/pulls?state=open&per_page=100"
    )

    assert result == [{"n": 1}]
    assert routed == ["repos/owner/name/pulls?state=open&per_page=100"]


def test_gh_api_paginated_keeps_legacy_slurp_for_other_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Sub-resource lists (comments, reactions) must keep the slurp flow."""
    routed: list[str] = []

    def fake_etag_helper(path: str) -> list[dict]:
        routed.append(path)
        raise AssertionError("should not be called for sub-resource paths")

    monkeypatch.setattr("src.github_client._etag_get_paginated", fake_etag_helper)
    monkeypatch.setattr(
        "src.github_client.run_gh",
        lambda args: [[{"id": 1}], [{"id": 2}]],
    )

    result = github_client._gh_api_paginated(
        "repos/owner/name/issues/42/comments"
    )

    assert result == [{"id": 1}, {"id": 2}]
    assert routed == []


def test_invalidate_etag_cache_drops_matching_prefixes_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_invalidate_etag_cache`` must remove only the prefix-matching entries."""
    github_client._etag_cache_put(
        "repos/owner/name/pulls?state=open&per_page=100&page=1",
        'W/"a"',
        [{"n": 1}],
    )
    github_client._etag_cache_put(
        "repos/owner/name/pulls?state=closed&page=1",
        'W/"b"',
        [{"n": 2}],
    )
    github_client._etag_cache_put(
        "repos/owner/name/issues/42/comments",
        'W/"c"',
        [{"id": 3}],
    )

    github_client._invalidate_etag_cache("repos/owner/name/pulls")

    assert "repos/owner/name/issues/42/comments" in github_client._etag_cache
    assert not any(
        key.startswith("repos/owner/name/pulls")
        for key in github_client._etag_cache
    )


def test_invalidate_etag_cache_no_op_when_prefix_absent() -> None:
    """A prefix that matches nothing must leave the cache untouched."""
    github_client._etag_cache_put(
        "repos/owner/name/pulls?state=open&page=1",
        'W/"a"',
        [{"n": 1}],
    )

    github_client._invalidate_etag_cache("repos/different/repo/pulls")

    assert "repos/owner/name/pulls?state=open&page=1" in github_client._etag_cache


def test_merge_pr_invalidates_pulls_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful ``merge_pr`` drops cached ``repos/{repo}/pulls`` entries."""
    github_client._etag_cache_put(
        "repos/owner/name/pulls?state=open&per_page=100&page=1",
        'W/"a"',
        [{"n": 1}],
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    github_client.merge_pr("owner/name", 42)

    assert (
        "repos/owner/name/pulls?state=open&per_page=100&page=1"
        not in github_client._etag_cache
    )


def test_get_pr_metadata_extracts_nested_user_and_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production payload: nested ``.user.login`` and ``.head.sha`` are extracted."""
    pr_body = (
        '{"user": {"login": "alice"}, "head": {"sha": "abc123"}}'
    )
    commit_body = '{"commit": {"committer": {"date": "2026-04-15T12:00:00Z"}}}'

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = next((a for a in cmd if a.startswith("repos/")), "")
        if "/pulls/" in path:
            return _FakeCompletedProcess(
                stdout=_build_include_response(pr_body, etag='W/"p1"')
            )
        if "/commits/" in path:
            return _FakeCompletedProcess(
                stdout=_build_include_response(commit_body, etag='W/"c1"')
            )
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_metadata("owner/name", 42) == {
        "author": "alice",
        "head_sha": "abc123",
        "head_commit_date": "2026-04-15T12:00:00Z",
    }
