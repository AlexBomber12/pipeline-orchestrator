"""Tests for the split src.github.* modules (legacy github_client surface)."""

from __future__ import annotations

import json
import logging
import subprocess
from datetime import datetime, timedelta
from datetime import timezone as _tz
from typing import Any

import pytest
from src.config import TrustedReviewerIdentity
from src.github import cache, checks, comments, prs, rate_limit, reactions, reviews  # noqa: F401
from src.github import comments as gh_comments
from src.github.cache import clear_etag_cache  # noqa: F401 — used in tests
from src.github.checks import (
    _fetch_ci_status_rest,
    _map_rest_ci_status_to_enum,
    _retrieve_ci_status_evidence,
    clear_ci_status_cache,
)
from src.github.comments import (
    has_recent_codex_review_request,
    post_comment,
)
from src.github.gh_runner import (
    _extract_commit_date,
    _parse_iso,
    get_repo_full_name,
    run_gh,
)
from src.github.prs import (
    ExpectedHeadMismatch,
    clear_last_known_sha,
    clear_merged_prs_cache,
    get_branch_last_push_time,
    get_last_push_age_seconds,
    get_merged_prs,
    get_open_prs,
    get_pr_author,
    get_pr_head_commit_iso,
    get_pr_last_push_time,
    get_pr_metadata,
    is_pr_merged,
    merge_pr,
    pr_state,
)
from src.github.reactions import (
    _get_codex_issue_reactions,
    _is_codex_user,
    _is_plus_one,
    _is_reaction_content,
)
from src.github.reviewer_policy import ReviewerPolicy
from src.github.reviews import (
    _compute_review_status,
    _get_codex_review_signals,
    _get_latest_codex_review_info,
    clear_review_status_cache,
    get_pr_review_status,
)
from src.models import CIStatus, ReviewStatus

TRUSTED_REVIEWER_ID = 199175422
SECOND_TRUSTED_REVIEWER_ID = 200200200
_REAL_GET_REVIEW_PUSH_TIME = reviews._get_pr_push_time


def _ci_retrieval(
    repo: str,
    sha: str,
    check_runs: list[dict] | None = None,
    status_payload: dict | None = None,
    *,
    complete: bool = True,
) -> checks._CiRetrieval:
    source = checks._CiSourceResult(complete, not check_runs)
    status_source = checks._CiSourceResult(
        complete, not (status_payload or {}).get("statuses")
    )
    return checks._make_ci_retrieval(
        repo,
        sha,
        0.0,
        check_runs or [],
        status_payload or {},
        source,
        status_source,
    )


@pytest.fixture(autouse=True)
def _default_review_push_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep review-status tests deterministic without querying branch activity."""
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 1, 1, tzinfo=_tz.utc),
    )


def _find_api_path(cmd: list[str]) -> str:
    """Extract the API path from a gh command, handling --jq args."""
    for arg in cmd:
        if arg.startswith("repos/"):
            return arg
    return ""


def _codex_user(login: str = "chatgpt-codex-connector[bot]") -> dict[str, int | str]:
    return {"id": TRUSTED_REVIEWER_ID, "login": login}


def _reviewer_user(user_id: int, login: str) -> dict[str, int | str]:
    return {"id": user_id, "login": login}


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


def _ci_check_page(
    runs: list[object],
    *,
    sha: str = "abc123",
    total_count: int | None = None,
) -> dict:
    normalized: list[object] = []
    for run in runs:
        if isinstance(run, dict):
            run = {"head_sha": sha, **run}
        normalized.append(run)
    return {
        "total_count": len(runs) if total_count is None else total_count,
        "check_runs": normalized,
    }


def _ci_status_page(
    state: str,
    statuses: list[object],
    *,
    sha: str = "abc123",
    total_count: int | None = None,
) -> dict:
    return {
        "state": state,
        "sha": sha,
        "total_count": len(statuses) if total_count is None else total_count,
        "statuses": statuses,
    }


def _ci_sha_from_args(args: list[str]) -> str:
    path = next(arg for arg in args if "/commits/" in arg)
    return path.split("/commits/", 1)[1].split("/", 1)[0]


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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    prs = get_merged_prs("owner/name", base_branch="main")

    assert [pr.number for pr in prs] == [101]


def test_get_merged_prs_url_encodes_base_branch_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        assert path == "repos/owner/name/pulls?state=closed&base=release%2F2026.04&per_page=100"
        return []

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="boom"):
        get_merged_prs("owner/name", base_branch="main")


def test_is_pr_merged_true_when_merged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args: {"state": "closed", "merged": True},
    )

    assert is_pr_merged("owner/name", 12) is True


def test_is_pr_merged_false_when_closed_unmerged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args: {"state": "closed", "merged": False},
    )

    assert is_pr_merged("owner/name", 12) is False


def test_is_pr_merged_none_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str]) -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert is_pr_merged("owner/name", 12) is None


def test_is_pr_merged_none_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str]) -> dict[str, object]:
        raise subprocess.TimeoutExpired(cmd=args, timeout=30)

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert is_pr_merged("owner/name", 12) is None


def test_is_pr_merged_none_on_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str]) -> dict[str, object]:
        raise OSError("gh not found")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert is_pr_merged("owner/name", 12) is None


def test_is_pr_merged_none_on_malformed_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args: "{not-json")

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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


def test_is_codex_user_matches_trusted_id_after_rename() -> None:
    assert _is_codex_user(_codex_user("codex")) is True
    assert _is_codex_user(_codex_user("chatgpt-codex-conn")) is True
    assert _is_codex_user(_codex_user("codex-bot")) is True
    assert _is_codex_user(_codex_user("mycodexbot")) is True
    assert _is_codex_user(_codex_user("not-codex-related-thing")) is True


def test_is_codex_user_rejects_untrusted_and_malformed_ids() -> None:
    assert _is_codex_user({"login": "AlexBomber12"}) is False
    assert (
        _is_codex_user(
            {
                "id": TRUSTED_REVIEWER_ID + 1,
                "login": "chatgpt-codex-connector[bot]",
            }
        )
        is False
    )
    assert _is_codex_user({"login": "dependabot"}) is False
    assert _is_codex_user({"login": "codec-reviewer"}) is False
    assert _is_codex_user(None) is False


def test_plus_one_requires_exact_content() -> None:
    assert _is_plus_one({"content": "+1", "user": _codex_user("codex-bot")}) is True
    assert _is_plus_one({"content": "thumbsup", "user": _codex_user("codex-bot")}) is False
    assert _is_plus_one({"content": "heart", "user": _codex_user("codex-bot")}) is False


def test_plus_one_requires_codex_user() -> None:
    assert _is_plus_one({"content": "+1", "user": {"login": "AlexBomber12"}}) is False
    assert _is_plus_one({"content": "+1", "user": _codex_user("codex-bot")}) is True


def test_get_pr_review_status_approved_via_pr_body_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex +1 reaction on the PR body (issue-level) → APPROVED without needing comments."""
    import json as _json

    clear_review_status_cache()
    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-02T00:00:00Z",
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
        get_pr_review_status(
            "owner/name", 42, pr_author="author", head_sha="currentHead"
        )
        == ReviewStatus.APPROVED
    )

    assert any("issues/42/reactions" in arg for cmd in invocations for arg in cmd)


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
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = []
        elif "issues" in path and path.endswith("/comments"):
            data = [
                [{"id": 10, "user": {"login": "author"}, "body": "@codex review"}],
                [{"id": 20, "user": _codex_user("chatgpt-codex-bot"), "body": "LGTM"}],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif path.endswith("/reactions"):
            data = [
                [
                    {
                        "content": "+1",
                        "user": _codex_user("chatgpt-codex-bot"),
                        "created_at": "2026-01-02T00:00:00Z",
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

    paginated_invocations = [cmd for cmd in invocations if not _is_commits_path(cmd)]
    assert len(paginated_invocations) == 4
    assert not any(cmd[-1].endswith("/pulls/42/reviews") for cmd in invocations)
    for cmd in paginated_invocations:
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
                        "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.PENDING


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
                        "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.PENDING


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
                        "user": _codex_user("chatgpt-codex-bot"),
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
                        "user": _codex_user("chatgpt-codex-bot"),
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
        get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222")
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
                        "user": _codex_user("chatgpt-codex-bot"),
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
                        "user": _codex_user("chatgpt-codex-bot"),
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
        get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222")
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
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-bot"),
                        "state": "APPROVED",
                        "commit_id": "bbbbbb2222",
                        "submitted_at": "2026-01-02T00:00:00Z",
                    },
                    {
                        "user": _codex_user("chatgpt-codex-bot"),
                        "state": "CHANGES_REQUESTED",
                        "commit_id": "bbbbbb2222",
                        "submitted_at": "2026-01-03T00:00:00Z",
                    },
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

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.PENDING


def test_review_api_errors_do_not_block_reaction_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-404 review API failures should fall back to reactions/comments."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            return _FakeCompletedProcess(stderr="HTTP 403 rate limit exceeded", returncode=1)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.APPROVED


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
                        "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.PENDING


def test_get_pr_review_status_skips_teammate_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A teammate's comment before the PR author's should be ignored."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
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
            data = [
                [
                    {
                        "content": "+1",
                        "user": _codex_user("chatgpt-codex-bot"),
                        "created_at": "2026-01-02T00:00:00Z",
                    }
                ]
            ]
        elif path.endswith("/reactions"):
            data = []
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


def test_get_pr_review_status_ignores_non_trigger_author_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrelated author follow-up after the trigger should not become the anchor."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
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
            data = [
                [
                    {
                        "content": "+1",
                        "user": _codex_user("chatgpt-codex-bot"),
                        "created_at": "2026-01-02T00:00:00Z",
                    }
                ]
            ]
        elif path.endswith("/reactions"):
            data = []
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
        return _FakeCompletedProcess(stdout=_json.dumps([[{"id": 1, "user": {"login": "author"}, "body": "hi"}]]))

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
                        "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.CHANGES_REQUESTED


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
                        "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


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
            data = [[{"content": "+1", "user": _codex_user("chatgpt-codex-bot")}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


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
                        "user": _codex_user("chatgpt-codex-bot"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.CHANGES_REQUESTED


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
                        "user": _codex_user("chatgpt-codex-connector"),
                        "body": ("To use Codex here, create a Codex account and connect to github."),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


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
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
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
                        "user": _codex_user("chatgpt-codex-bot"),
                        "body": "Looks good overall",
                        "created_at": "2026-01-01T00:01:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": _codex_user("chatgpt-codex-bot")}]]
        elif path.endswith("/reactions"):
            data = [[{"content": "+1", "user": _codex_user("chatgpt-codex-bot"), "created_at": "2026-01-01T00:02:00Z"}]]
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
                        "user": _codex_user("chatgpt-codex-bot"),
                        "body": "Reviewing now",
                        "created_at": "2026-01-01T00:01:00Z",
                    },
                ],
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": _codex_user("chatgpt-codex-bot")}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.EYES


def test_body_eyes_wins_over_anchor_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PR-body eyes signal should beat an anchor +1 while review is in progress."""
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
                    }
                ]
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "+1", "user": _codex_user("chatgpt-codex-bot")}]]
        elif path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": _codex_user("chatgpt-codex-bot")}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.EYES


def test_anchor_eyes_wins_over_body_plus_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An anchor eyes signal should beat a PR-body +1 while review is in progress."""
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
                    }
                ]
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "eyes", "user": _codex_user("chatgpt-codex-bot")}]]
        elif path.endswith("/reactions"):
            data = [[{"content": "+1", "user": _codex_user("chatgpt-codex-bot")}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.EYES


def test_review_api_with_body_eyes_stays_eyes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Formal APPROVED review alone should not beat body-level eyes."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-bot"),
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
                    }
                ]
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif path.endswith("/reactions"):
            data = [[{"content": "eyes", "user": _codex_user("chatgpt-codex-bot")}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.EYES


def test_review_api_with_anchor_eyes_stays_eyes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Formal APPROVED review alone should not beat anchor eyes."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-bot"),
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
                    }
                ]
            ]
        elif "pulls" in path and path.endswith("/comments"):
            data = []
        elif "comments/10/reactions" in path:
            data = [[{"content": "eyes", "user": _codex_user("chatgpt-codex-bot")}]]
        elif path.endswith("/reactions"):
            data = []
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.EYES


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
        return _FakeCompletedProcess(stderr="HTTP 403 rate limit exceeded", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="403"):
        get_pr_review_status("owner/name", 404)


def test_get_codex_issue_reactions_returns_empty_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("net/http: TLS handshake timeout")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert _get_codex_issue_reactions("owner/name", 42) == []


def test_get_codex_issue_reactions_logs_warning_on_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("net/http: TLS handshake timeout")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)
    caplog.set_level(logging.WARNING)

    assert _get_codex_issue_reactions("owner/name", 42) == []
    assert any(
        record.levelno == logging.WARNING
        and record.getMessage() == ("Reactions fetch degraded for PR 42 in owner/name: net/http: TLS handshake timeout")
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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert _compute_review_status("owner/name", 42, "author", "") == ReviewStatus.PENDING


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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="403"):
        _compute_review_status("owner/name", 42, "author", "")


def _is_commits_path(cmd: list[str]) -> bool:
    """Return True if ``gh api repos/.../commits/<sha> --jq ...``."""
    for arg in cmd:
        if "/commits/" in arg:
            return True
    return False


def test_body_plus_one_before_latest_push_is_stale_even_if_commit_is_older(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Push time, not an older rebased commit date, controls freshness."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 1, 2, tzinfo=_tz.utc),
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            raise AssertionError("review freshness must not query commit time")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="bbbbbb2222") == ReviewStatus.PENDING


def test_body_plus_one_after_latest_push_approves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A +1 created after the latest branch push approves that push."""
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
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-03T00:00:00Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="aabbcc112233") == ReviewStatus.APPROVED


def test_body_plus_one_no_push_time_stays_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unavailable push time cannot prove that the +1 is current."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: None,
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stderr="boom", returncode=1)
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-03T00:00:00Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="deadbeef") == ReviewStatus.PENDING


@pytest.mark.parametrize("created_at", [None, "not-a-timestamp"])
def test_body_plus_one_missing_or_malformed_reaction_time_stays_pending(
    monkeypatch: pytest.MonkeyPatch,
    created_at: str | None,
) -> None:
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
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": created_at,
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name",
            42,
            pr_author="author",
            head_sha="deadbeef",
        )
        == ReviewStatus.PENDING
    )


def test_no_plus_one_does_not_fetch_latest_push_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review and push-time lookups should stay lazy when no +1 path needs them."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: (_ for _ in ()).throw(
            AssertionError("push-time lookup should not run without +1")
        ),
    )

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

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="deadbeef") == ReviewStatus.PENDING


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
            data = [[{"content": "eyes", "user": _codex_user("chatgpt-codex-bot")}]]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.EYES


def test_find_codex_plus_one_picks_newest() -> None:
    """_find_codex_plus_one_reaction must return the most recent +1."""
    from src.github.reactions import _find_codex_plus_one_reaction

    items = [
        {
            "content": "+1",
            "user": _codex_user("chatgpt-codex-connector"),
            "created_at": "2026-01-01T00:00:00Z",
        },
        {
            "content": "+1",
            "user": _codex_user("chatgpt-codex-connector"),
            "created_at": "2026-01-05T00:00:00Z",
        },
        {
            "content": "+1",
            "user": {"login": "someone-else"},
            "created_at": "2026-01-10T00:00:00Z",
        },
    ]
    best = _find_codex_plus_one_reaction(items)
    assert best is not None
    assert best["created_at"] == "2026-01-05T00:00:00Z"


def test_approval_without_head_sha_stays_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a current head SHA there is no freshness threshold."""
    import json as _json

    clear_review_status_cache()

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
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

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


def test_merge_pr_uses_squash_and_expected_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}
    expected = "a" * 40

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured["cmd"] = cmd
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    merge_pr("owner/name", 42, expected)

    assert captured["cmd"] == [
        "gh",
        "pr",
        "merge",
        "42",
        "--squash",
        "--delete-branch",
        "--match-head-commit",
        expected,
        "-R",
        "owner/name",
    ]


def test_merge_pr_rejects_missing_full_expected_head() -> None:
    with pytest.raises(ValueError, match="expected_head_sha"):
        merge_pr("owner/name", 42, "abc123")


def test_merge_pr_classifies_expected_head_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "b" * 40

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stderr=(
                "pull request head commit does not match expected head SHA"
            ),
            returncode=1,
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(ExpectedHeadMismatch):
        merge_pr("owner/name", 42, expected)


def test_merge_pr_reraises_non_head_mismatch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = "d" * 40

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(
            stderr="authentication failed",
            returncode=1,
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="authentication failed"):
        merge_pr("owner/name", 42, expected)


def _iso_utc_now_minus(seconds: int) -> str:
    return (datetime.now(_tz.utc) - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


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

    assert has_recent_codex_review_request("owner/name", 42, pr_author="author", within_minutes=5) is True


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

    assert has_recent_codex_review_request("owner/name", 42, pr_author="author", within_minutes=5) is False


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

    assert has_recent_codex_review_request("owner/name", 42, pr_author="author", within_minutes=5) is False


def test_get_pr_author_returns_login(monkeypatch: pytest.MonkeyPatch) -> None:
    """``get_pr_author`` must read the login from PR metadata, not from
    the daemon's ``gh`` identity, so dedup works when Claude CLI ran
    under a different auth context than the daemon."""
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        body = '{"user": {"login": "claude-cli-bot"}}'
        stdout = f'HTTP/2.0 200 OK\r\nETag: W/"abc"\r\n\r\n{body}'
        return _FakeCompletedProcess(stdout=stdout)

    monkeypatch.setattr(subprocess, "run", fake_run)
    cache.clear_etag_cache()

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
        return _FakeCompletedProcess(stdout="", stderr="not found", returncode=1)

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
    """Comments created strictly before ``after_iso`` must not count as
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

    just_now = (datetime.now(_tz.utc) - timedelta(seconds=10)).strftime("%Y-%m-%dT%H:%M:%SZ")

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


def test_has_recent_codex_review_request_counts_same_second(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-239 regression: a comment created in the same UTC second as
    ``after_iso`` must count as a valid trigger. GitHub timestamps are
    second-granular, so a coder posting ``@codex review`` within one
    second of pushing the head commit produces an equal stringified
    timestamp; ``<=`` would have skipped it and the daemon would have
    posted a duplicate trigger."""
    import json as _json

    same_second = _iso_utc_now_minus(30)
    pages = [
        [
            {
                "user": {"login": "same-user"},
                "body": "@codex review",
                "created_at": same_second,
            }
        ]
    ]

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_json.dumps(pages))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        has_recent_codex_review_request(
            "owner/name",
            42,
            pr_author="same-user",
            within_minutes=5,
            after_iso=same_second,
        )
        is True
    )


def test_get_pr_head_commit_iso_returns_committer_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Should fetch ``.head.sha`` then ``.commit.committer.date`` and
    return the ISO timestamp unchanged."""
    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = next((arg for arg in cmd if arg.startswith("repos/")), "")
        if path.endswith("/pulls/42"):
            return _FakeCompletedProcess(stdout="abc1234")
        if path.startswith("repos/owner/name/commits/"):
            return _FakeCompletedProcess(stdout="2026-04-14T13:37:00Z")
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_head_commit_iso("owner/name", 42) == "2026-04-14T13:37:00Z"
    assert any("repos/owner/name/pulls/42" in a for a in invocations[0])
    assert any("repos/owner/name/commits/abc1234" in a for a in invocations[1])


def test_get_pr_head_commit_iso_returns_empty_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Errors from either lookup must not propagate — the caller
    treats "" as "no constraint" and the dedup filter degrades
    gracefully to pure time-window matching."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout="", stderr="boom", returncode=1)

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
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-10T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-connector"),
                        "commit_id": "otherSha1234",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="oldSha5678") == ReviewStatus.PENDING


def test_body_plus_one_review_on_head_still_requires_fresh_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A current-head review must not revive a stale PR-body +1."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 1, 10, tzinfo=_tz.utc),
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-10T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-connector"),
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="currentHead") == ReviewStatus.PENDING


def test_body_plus_one_review_on_head_approves_when_reaction_is_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 1, 10, tzinfo=_tz.utc),
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-10T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-10T00:00:01Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-connector"),
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="currentHead") == ReviewStatus.APPROVED


def test_body_plus_one_review_on_head_no_push_time_stays_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: None,
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stderr="boom", returncode=1)
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": _codex_user("chatgpt-codex-connector"),
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="currentHead") == ReviewStatus.PENDING


def test_unverifiable_body_plus_one_still_surfaces_current_findings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: None,
    )
    actor = _codex_user("chatgpt-codex-connector")

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stderr="boom", returncode=1)
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": actor,
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": actor,
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                    }
                ]
            ]
        elif path.endswith("/issues/42/comments"):
            data = [
                [
                    {
                        "user": actor,
                        "body": "P1: current finding",
                        "created_at": "2026-02-15T00:00:01Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name",
            42,
            pr_author="author",
            head_sha="currentHead",
        )
        == ReviewStatus.CHANGES_REQUESTED
    )


@pytest.mark.parametrize(
    "reaction_time",
    [None, "not-a-timestamp", "2026-01-01T00:00:00Z"],
)
def test_anchor_plus_one_requires_verifiable_current_head_freshness(
    monkeypatch: pytest.MonkeyPatch,
    reaction_time: str | None,
) -> None:
    actor = _codex_user("chatgpt-codex-connector")

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-02-01T00:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        if path.endswith("/issues/comments/10/reactions"):
            return [
                {
                    "content": "+1",
                    "user": actor,
                    "created_at": reaction_time,
                }
            ]
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 2, 1, tzinfo=_tz.utc),
    )

    assert (
        _compute_review_status("owner/name", 42, "author", "currentHead")
        == ReviewStatus.PENDING
    )


def test_anchor_plus_one_with_fresh_timestamp_approves_current_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actor = _codex_user("chatgpt-codex-connector")

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/reactions"):
            return []
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 10,
                    "user": {"login": "author"},
                    "body": "@codex review",
                    "created_at": "2026-02-01T00:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        if path.endswith("/issues/comments/10/reactions"):
            return [
                {
                    "content": "+1",
                    "user": actor,
                    "created_at": "2026-02-01T00:00:01Z",
                }
            ]
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 2, 1, tzinfo=_tz.utc),
    )

    assert (
        _compute_review_status("owner/name", 42, "author", "currentHead")
        == ReviewStatus.APPROVED
    )


def test_body_plus_one_same_actor_current_review_with_findings_stays_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A same-actor current-head COMMENTED review cannot refresh an old +1."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 2, 1, tzinfo=_tz.utc),
    )
    actor = _codex_user("chatgpt-codex-connector")

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-02-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": actor,
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": actor,
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                        "state": "COMMENTED",
                    }
                ]
            ]
        elif path.endswith("/pulls/42/comments"):
            data = [
                [
                    {
                        "user": actor,
                        "body": "P1: needs a fix",
                        "created_at": "2026-02-15T00:00:01Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name",
            42,
            pr_author="author",
            head_sha="currentHead",
        )
        == ReviewStatus.CHANGES_REQUESTED
    )


def test_body_plus_one_review_sha_must_match_reacting_actor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A current-head review by actor B must not refresh actor A's stale +1."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(2026, 2, 1, tzinfo=_tz.utc),
    )
    actor_a = _reviewer_user(TRUSTED_REVIEWER_ID, "chatgpt-codex-connector")
    actor_b = _reviewer_user(SECOND_TRUSTED_REVIEWER_ID, "codex-second-reviewer")
    policy = ReviewerPolicy(
        [
            TrustedReviewerIdentity(user_id=TRUSTED_REVIEWER_ID, login="codex"),
            TrustedReviewerIdentity(
                user_id=SECOND_TRUSTED_REVIEWER_ID,
                login="codex-second-reviewer",
            ),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-02-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": actor_a,
                    "created_at": "2026-01-01T00:00:00Z",
                }
            ]
        elif path.endswith("/pulls/42/reviews"):
            data = [
                [
                    {
                        "user": actor_b,
                        "commit_id": "currentHead",
                        "submitted_at": "2026-02-15T00:00:00Z",
                        "state": "COMMENTED",
                    }
                ]
            ]
        elif path.endswith("/pulls/42/comments"):
            data = [
                [
                    {
                        "user": actor_b,
                        "body": "P1: needs a fix",
                        "created_at": "2026-02-15T00:00:01Z",
                    }
                ]
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert (
        get_pr_review_status(
            "owner/name",
            42,
            pr_author="author",
            head_sha="currentHead",
            policy=policy,
        )
        == ReviewStatus.CHANGES_REQUESTED
    )


def test_body_plus_one_same_second_as_latest_push_approves(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reaction in the push's UTC second counts as fresh."""
    import json as _json

    clear_review_status_cache()
    monkeypatch.setattr(
        reviews,
        "_get_pr_push_time",
        lambda repo, pr_number: datetime(
            2026, 1, 2, 12, 34, 56, tzinfo=_tz.utc
        ),
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-02T12:34:56Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-02T12:34:56Z",
                }
            ]
        else:
            data = []
        return _FakeCompletedProcess(stdout=_json.dumps(data))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_review_status("owner/name", 42, pr_author="author", head_sha="abc") == ReviewStatus.APPROVED


def test_review_status_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated calls within 30s return cached result without extra API calls."""
    import json as _json

    clear_review_status_cache()
    call_count = 0

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        nonlocal call_count
        call_count += 1
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-02T00:00:00Z",
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

    result1 = get_pr_review_status("owner/name", 42, pr_author="author", head_sha="sha123")
    calls_after_first = call_count

    result2 = get_pr_review_status("owner/name", 42, pr_author="author", head_sha="sha123")

    assert result1 == ReviewStatus.APPROVED
    assert result2 == ReviewStatus.APPROVED
    assert call_count == calls_after_first


def test_review_status_cache_is_scoped_to_reviewer_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json as _json

    clear_review_status_cache()
    call_count = 0

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        nonlocal call_count
        call_count += 1
        if _is_commits_path(cmd):
            return _FakeCompletedProcess(stdout="2026-01-01T00:00:00Z")
        path = _find_api_path(cmd)
        if path.endswith("/issues/42/reactions"):
            data = [
                {
                    "content": "+1",
                    "user": _codex_user("chatgpt-codex-connector"),
                    "created_at": "2026-01-02T00:00:00Z",
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

    trusted_policy = ReviewerPolicy(
        [TrustedReviewerIdentity(user_id=TRUSTED_REVIEWER_ID, login="codex")]
    )
    removed_reviewer_policy = ReviewerPolicy(
        [TrustedReviewerIdentity(user_id=404, login="other-reviewer")]
    )

    result1 = get_pr_review_status(
        "owner/name",
        42,
        pr_author="author",
        head_sha="sha123",
        policy=trusted_policy,
    )
    calls_after_first = call_count
    result2 = get_pr_review_status(
        "owner/name",
        42,
        pr_author="author",
        head_sha="sha123",
        policy=removed_reviewer_policy,
    )

    assert result1 == ReviewStatus.APPROVED
    assert result2 == ReviewStatus.PENDING
    assert call_count > calls_after_first


def test_get_pr_metadata_single_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_pr_metadata returns author + head_sha from a single PR API call
    plus one commit API call for the date."""
    import json as _json

    invocations: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        invocations.append(cmd)
        path = _find_api_path(cmd)
        if path.endswith("/pulls/42"):
            return _FakeCompletedProcess(stdout=_json.dumps({"author": "alice", "head_sha": "abc123"}))
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
        _map_rest_ci_status_to_enum([], {"state": "pending", "statuses": []}, empty_is_success=True) == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_handles_non_dict_status_payload() -> None:
    """A non-dict status payload (e.g. ``None``) collapses to PENDING/SUCCESS."""
    assert _map_rest_ci_status_to_enum([], None) == CIStatus.PENDING  # type: ignore[arg-type]
    assert (
        _map_rest_ci_status_to_enum([], None, empty_is_success=True)  # type: ignore[arg-type]
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_failed_fetch_stays_pending() -> None:
    assert _map_rest_ci_status_to_enum([], {}, empty_is_success=True, fetch_ok=False) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([], {}, empty_is_success=False, fetch_ok=False) == CIStatus.PENDING


def test_classify_ci_retrieval_accepts_unique_identified_required_checks() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "name": "unit",
                "conclusion": "success",
                "head_sha": sha,
                "app": {"id": 1},
            }
        ],
        {
            "state": "success",
            "statuses": [
                {
                    "context": "integration",
                    "state": "success",
                    "creator": {"login": "ci-bot"},
                }
            ],
        },
    )

    assert (
        checks._classify_ci_retrieval(
            retrieval,
            required_contexts=["unit", "integration"],
        )
        == CIStatus.SUCCESS
    )


def test_classify_ci_retrieval_uses_successful_latest_rerun() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "id": 1,
                "name": "unit",
                "conclusion": "failure",
                "head_sha": sha,
                "app": {"id": 1},
                "run_attempt": 1,
                "completed_at": "2026-10-06T10:00:00Z",
            },
            {
                "id": 2,
                "name": "unit",
                "conclusion": "success",
                "head_sha": sha,
                "app": {"id": 1},
                "run_attempt": 2,
                "completed_at": "2026-10-06T11:00:00Z",
            },
        ],
        {"state": "pending", "statuses": []},
    )

    assert _map_rest_ci_status_to_enum(
        retrieval.check_runs,
        retrieval.status_payload,
    ) == CIStatus.FAILURE
    assert (
        checks._classify_ci_retrieval(retrieval, required_contexts=["unit"])
        == CIStatus.SUCCESS
    )


def test_classify_ci_retrieval_preserves_current_infra_failure() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "id": 1,
                "name": "unit",
                "conclusion": "cancelled",
                "head_sha": sha,
                "app": {"id": 1},
                "completed_at": "2026-10-06T11:00:00Z",
            }
        ],
        {"state": "pending", "statuses": []},
    )

    assert (
        checks._classify_ci_retrieval(retrieval, required_contexts=["unit"])
        == CIStatus.INFRA_FAILURE
    )


@pytest.mark.parametrize("statuses", [[], [{}]])
def test_classify_ci_retrieval_preserves_known_aggregate_failure(
    statuses: list[dict],
) -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [],
        {"state": "failure", "statuses": statuses},
    )

    assert retrieval.evidence.policy_result == CIStatus.FAILURE
    assert (
        checks._classify_ci_retrieval(retrieval, required_contexts=["unit"])
        == CIStatus.FAILURE
    )


def test_classify_ci_retrieval_aggregate_failure_dominates_infra_run() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "id": 1,
                "name": "unit",
                "conclusion": "cancelled",
                "head_sha": sha,
                "app": {"id": 1},
            }
        ],
        {"state": "failure", "statuses": []},
    )

    assert checks._classify_ci_retrieval(retrieval) == CIStatus.FAILURE


def test_classify_ci_retrieval_treats_legacy_status_failure_as_logic() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [],
        {
            "state": "pending",
            "statuses": [
                {
                    "context": "legacy",
                    "state": "failure",
                    "creator": {"login": "ci-bot"},
                }
            ],
        },
    )

    assert checks._classify_ci_retrieval(retrieval) == CIStatus.FAILURE


def test_classify_ci_retrieval_treats_ambiguous_run_id_as_logic() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "id": 1,
                "name": "unit",
                "conclusion": conclusion,
                "head_sha": sha,
                "app": {"id": 1},
                "completed_at": completed_at,
            }
            for conclusion, completed_at in (
                ("failure", "2026-10-06T10:00:00Z"),
                ("cancelled", "2026-10-06T11:00:00Z"),
            )
        ],
        {"state": "pending", "statuses": []},
    )

    assert checks._classify_ci_retrieval(retrieval) == CIStatus.FAILURE


def test_classify_ci_retrieval_preserves_current_logic_failure() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "id": 1,
                "name": "unit",
                "conclusion": "failure",
                "head_sha": sha,
                "app": {"id": 1},
            }
        ],
        {"state": "pending", "statuses": []},
    )

    assert checks._classify_ci_retrieval(retrieval) == CIStatus.FAILURE


def test_classify_ci_retrieval_uses_latest_attempt_for_infra_failure() -> None:
    sha = "a" * 40
    retrieval = _ci_retrieval(
        "owner/name",
        sha,
        [
            {
                "id": 1,
                "name": "unit",
                "conclusion": "failure",
                "head_sha": sha,
                "app": {"id": 1},
                "run_attempt": 1,
                "completed_at": "2026-10-06T10:00:00Z",
            },
            {
                "id": 2,
                "name": "unit",
                "conclusion": "cancelled",
                "head_sha": sha,
                "app": {"id": 1},
                "run_attempt": 2,
                "completed_at": "2026-10-06T11:00:00Z",
            },
        ],
        {"state": "pending", "statuses": []},
    )

    assert _map_rest_ci_status_to_enum(
        retrieval.check_runs,
        retrieval.status_payload,
    ) == CIStatus.FAILURE
    assert (
        checks._classify_ci_retrieval(retrieval, required_contexts=["unit"])
        == CIStatus.INFRA_FAILURE
    )


# ---------------------------------------------------------------------------
# retry integration tests (PR-054)
# ---------------------------------------------------------------------------


def test_gh_api_paginated_retries_on_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """_gh_api_paginated retries on transient 503 then succeeds."""
    from src.github.cache import _gh_api_paginated

    calls: list[int] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(1)
        if len(calls) == 1:
            raise subprocess.CalledProcessError(1, cmd, stderr="HTTP 503 Service Unavailable")
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
    from src.github.cache import _gh_api_paginated

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise subprocess.CalledProcessError(1, cmd, stderr="503 Service Unavailable")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        _gh_api_paginated("repos/test/owner/issues/1/comments")


def test_begin_review_cache_cycle_initializes_and_increments() -> None:
    clear_review_status_cache()

    reviews._begin_review_cache_cycle()
    assert reviews._review_status_cache_cycle == 1

    reviews._begin_review_cache_cycle()
    assert reviews._review_status_cache_cycle == 2


def test_is_reaction_content_rejects_non_dict() -> None:
    assert _is_reaction_content(None, "+1") is False


def test_get_open_prs_returns_prinfo_objects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    head_sha = "a" * 40
    raw = [
        {"number": 0},
        {
            "number": 42,
            "title": "PR-110: Add coverage",
            "headRefName": "feature-branch",
            "headRefOid": head_sha,
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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(repo, sha),
    )
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.APPROVED,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert [pr.number for pr in prs] == [42]
    assert prs[0].branch == "feature-branch"
    assert prs[0].pr_id == "PR-110"
    assert prs[0].ci_status == CIStatus.SUCCESS
    assert prs[0].review_status == ReviewStatus.APPROVED
    assert prs[0].commits_count == 2
    assert prs[0].push_count == 1
    assert prs[0].observed_head_shas == {head_sha}
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
    sha = "d" * 40
    raw = [
        {
            "number": 7,
            "title": "PR-7: foo",
            "headRefName": "bar",
            "headRefOid": sha,
            "url": "u",
            "updatedAt": "2026-04-18T00:00:00Z",
            "commits": [],
            "author": {"login": "a"},
            "labels": [],
            "isCrossRepository": False,
        }
    ]
    captured: list[tuple[str, str]] = []

    def fake_fetch(repo: str, sha: str) -> checks._CiRetrieval:
        captured.append((repo, sha))
        return _ci_retrieval(
            repo,
            sha,
            [
                {
                    "name": "unit",
                    "conclusion": "failure",
                    "head_sha": sha,
                    "app": {"id": 1},
                }
            ],
            {"state": "failure", "statuses": []},
        )

    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: raw)
    monkeypatch.setattr("src.github.checks._retrieve_ci_status_evidence", fake_fetch)
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name")

    assert captured == [("owner/name", sha)]
    assert prs[0].ci_status == CIStatus.FAILURE


@pytest.mark.parametrize(
    "runs",
    [
        [{"name": "unit", "conclusion": "success"}],
        [
            {"name": "unit", "conclusion": "success", "app": {"id": 1}},
            {"name": "unit", "conclusion": "success", "app": {"id": 2}},
        ],
    ],
)
def test_get_open_prs_rejects_untrusted_required_check_provenance(
    monkeypatch: pytest.MonkeyPatch,
    runs: list[dict],
) -> None:
    head_sha = "d" * 40
    raw = [
        {
            "number": 7,
            "title": "PR-7: foo",
            "headRefName": "bar",
            "headRefOid": head_sha,
            "url": "u",
            "updatedAt": "2026-04-18T00:00:00Z",
            "commits": [],
            "author": {"login": "a"},
            "labels": [],
            "isCrossRepository": False,
        }
    ]

    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: raw)
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(
            repo,
            sha,
            [dict(run, head_sha=sha) for run in runs],
            {"state": "success", "statuses": []},
        ),
    )
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
    )

    prs = get_open_prs(
        "owner/name",
        allow_merge_without_checks=True,
        required_checks=["unit"],
    )

    assert prs[0].ci_status == CIStatus.PENDING


def test_get_open_prs_rest_fetch_failure_follows_allow_merge_without_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: raw)
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(repo, sha, complete=False),
    )
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert prs[0].ci_status == CIStatus.PENDING


def test_get_open_prs_falls_back_to_rest_on_graphql_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    head_sha = "a" * 40

    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("GraphQL: API rate limit exceeded")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_graphql)
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            {"number": 0},
            {
                "number": 42,
                "title": "PR-110: Add coverage",
                "head": {
                    "ref": "feature-branch",
                    "sha": head_sha,
                    "repo": {"fork": True},
                },
                "html_url": "https://example.test/pr/42",
                "updated_at": "2026-04-18T11:22:33Z",
                "user": {"login": "alice"},
                "labels": [{"name": "escalated"}],
            },
        ],
    )
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(repo, sha),
    )
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert [pr.number for pr in prs] == [42]
    assert prs[0].branch == "feature-branch"
    assert prs[0].ci_status == CIStatus.SUCCESS
    assert prs[0].review_status == ReviewStatus.PENDING
    assert prs[0].last_activity == datetime(2026, 4, 18, 11, 22, 33, tzinfo=_tz.utc)
    assert prs[0].is_escalated is True
    assert prs[0].is_cross_repository is True


@pytest.mark.parametrize(
    ("check_runs", "status_payload"),
    [
        ([], {"state": "success", "statuses": [{"state": "success"}]}),
        ([{"name": "unit", "conclusion": "success"}], {}),
    ],
)
def test_get_open_prs_rest_fallback_requires_complete_ci_retrieval(
    monkeypatch: pytest.MonkeyPatch,
    check_runs: list[dict],
    status_payload: dict,
) -> None:
    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("GraphQL: API rate limit exceeded")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_graphql)
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            {
                "number": 42,
                "title": "PR-110: Add coverage",
                "head": {
                    "ref": "feature-branch",
                    "sha": "abc123",
                    "repo": {"fork": False},
                },
                "html_url": "https://example.test/pr/42",
                "updated_at": "2026-04-18T11:22:33Z",
                "user": {"login": "alice"},
                "labels": [],
            },
        ],
    )
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(
            repo, sha, check_runs, status_payload, complete=False
        ),
    )
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert prs[0].ci_status == CIStatus.PENDING


@pytest.mark.parametrize(
    "runs",
    [
        [{"name": "unit", "conclusion": "success"}],
        [
            {"name": "unit", "conclusion": "success", "app": {"id": 1}},
            {"name": "unit", "conclusion": "success", "app": {"id": 2}},
        ],
    ],
)
def test_get_open_prs_rest_fallback_rejects_untrusted_required_check_provenance(
    monkeypatch: pytest.MonkeyPatch,
    runs: list[dict],
) -> None:
    head_sha = "a" * 40

    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("GraphQL: API rate limit exceeded")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_graphql)
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            {
                "number": 42,
                "title": "PR-110: Add coverage",
                "head": {
                    "ref": "feature-branch",
                    "sha": head_sha,
                    "repo": {"fork": False},
                },
                "html_url": "https://example.test/pr/42",
                "updated_at": "2026-04-18T11:22:33Z",
                "user": {"login": "alice"},
                "labels": [],
            },
        ],
    )
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(
            repo,
            sha,
            [dict(run, head_sha=sha) for run in runs],
            {"state": "success", "statuses": []},
        ),
    )
    monkeypatch.setattr(
        "src.github.reviews.get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
    )

    prs = get_open_prs(
        "owner/name",
        allow_merge_without_checks=True,
        required_checks=["unit"],
    )

    assert prs[0].ci_status == CIStatus.PENDING


def test_get_open_prs_propagates_non_rate_limit_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("gh failed")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_graphql)

    with pytest.raises(RuntimeError, match="gh failed"):
        get_open_prs("owner/name")


def test_get_open_prs_rest_fallback_returns_empty_for_unexpected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_graphql(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("GraphQL: API rate limit exceeded")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_graphql)
    monkeypatch.setattr("src.github.cache._gh_api_paginated", lambda path: None)

    assert get_open_prs("owner/name") == []


def test_get_open_prs_returns_empty_for_non_list_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: {"items": []})
    assert get_open_prs("owner/name") == []


def test_get_merged_prs_raises_on_unexpected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    monkeypatch.setattr("src.github.cache._gh_api_paginated", lambda path: None)

    with pytest.raises(RuntimeError, match="unexpected payload"):
        get_merged_prs("owner/name", refresh=True)


def test_get_merged_prs_skips_zero_number_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_merged_prs_cache()
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: ["bad"])
    assert is_pr_merged("owner/name", 42) is None


def test_is_pr_merged_returns_none_for_open_unmerged_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    result = pr_state("owner/name", 42)

    assert result == {
        "state": "MERGED",
        "mergedAt": "2026-04-26T12:00:00Z",
        "closedAt": "2026-04-26T12:00:00Z",
    }
    assert captured == [
        (
            ["pr", "view", "42", "--json", "state,mergedAt,closedAt"],
            {"repo": "owner/name"},
        )
    ]


def test_pr_state_returns_dict_for_open_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *args, **kwargs: {"state": "open", "mergedAt": None, "closedAt": None},
    )

    assert pr_state("owner/name", 42) == {
        "state": "OPEN",
        "mergedAt": None,
        "closedAt": None,
    }


def test_pr_state_parses_string_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *args, **kwargs: ('{"state": "closed", "mergedAt": null, "closedAt": "2026-04-26T13:00:00Z"}'),
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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise subprocess.TimeoutExpired(cmd=args, timeout=30)

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_on_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        raise OSError("gh missing")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_for_malformed_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "{not-json")
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_for_unexpected_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: ["unexpected"])
    assert pr_state("owner/name", 42) is None


def test_pr_state_returns_none_when_state_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *args, **kwargs: {"mergedAt": None, "closedAt": None},
    )
    assert pr_state("owner/name", 42) is None


def test_pr_state_normalizes_non_string_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert get_pr_review_status("owner/name", 42, pr_author="author") == ReviewStatus.PENDING


def test_post_comment_uses_pr_comment_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], str | None]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, timeout: int = 30) -> str:
        calls.append((args, repo))
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    post_comment("owner/name", 42, "hello")

    assert calls == [(["pr", "comment", "42", "--body", "hello"], "owner/name")]


def test_get_pr_author_returns_empty_for_non_string_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: {"login": "alice"})
    assert get_pr_author("owner/name", 42) == ""


def test_get_pr_head_commit_iso_returns_empty_when_head_sha_missing_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: {"sha": "abc"})
    assert get_pr_head_commit_iso("owner/name", 42) == ""


def test_get_pr_head_commit_iso_returns_empty_when_commit_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], repo: str | None = None, timeout: int = 30) -> object:
        if any("/pulls/" in a for a in args):
            return {"head": {"sha": "abc123"}}
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert get_pr_head_commit_iso("owner/name", 42) == ""


def test_get_pr_metadata_returns_empty_on_invalid_json_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "{not-json")

    assert get_pr_metadata("owner/name", 42) == {
        "author": "",
        "head_sha": "",
        "head_commit_date": "",
    }


def test_get_pr_metadata_parses_json_string_without_commit_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
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
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: ["bad"])

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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: next(shas))
    monkeypatch.setattr("src.github.prs.time.monotonic", lambda: 123.45)

    assert get_branch_last_push_time("owner/name", 42) is None
    assert get_branch_last_push_time("owner/name", 42) is None
    assert get_branch_last_push_time("owner/name", 42) == 123.45


def test_get_branch_last_push_time_returns_none_for_empty_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")
    assert get_branch_last_push_time("owner/name", 42) is None


def test_get_branch_last_push_time_propagates_poll_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    with pytest.raises(prs.GitHubPollError, match="boom"):
        get_branch_last_push_time("owner/name", 42)


def test_clear_last_known_sha_resets_tracking() -> None:
    prs._last_known_sha["owner/name#42"] = "sha1"
    clear_last_known_sha()
    assert prs._last_known_sha == {}


def test_get_last_push_age_seconds_returns_none_without_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")
    assert get_last_push_age_seconds("owner/name", 42) is None


def test_get_last_push_age_seconds_returns_computed_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz: _tz | None = None) -> datetime:
            return cls(2026, 4, 19, 12, 0, 0, tzinfo=tz)

    responses = iter(["feature-branch", "2026-04-19T11:59:30Z", ""])
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr("src.github.prs.datetime", _FakeDateTime)

    assert get_last_push_age_seconds("owner/name", 42) == 30.0


def test_get_last_push_age_seconds_returns_none_for_empty_push_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "", ""])
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: next(responses))
    assert get_last_push_age_seconds("owner/name", 42) is None


def test_get_last_push_age_seconds_returns_none_on_parse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "not-a-date"])
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: next(responses))
    assert get_last_push_age_seconds("owner/name", 42) is None


def test_get_pr_last_push_time_returns_parsed_datetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            {"branch": "feature-branch", "repo": "fork-owner/fork-repo"},
            "2026-04-30T11:59:30Z",
            "",
        ]
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> object:
        calls.append(args)
        return next(responses)

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    result = get_pr_last_push_time("owner/name", 42)

    assert result == datetime(2026, 4, 30, 11, 59, 30, tzinfo=_tz.utc)
    assert calls[1][1].startswith("repos/fork-owner/fork-repo/activity?")


def test_review_freshness_delegates_to_pr_push_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = datetime(2026, 4, 30, 11, 59, 30, tzinfo=_tz.utc)
    calls: list[tuple[str, int]] = []

    def fake_last_push(repo: str, pr_number: int) -> datetime:
        calls.append((repo, pr_number))
        return expected

    monkeypatch.setattr(prs, "get_pr_last_push_time", fake_last_push)

    assert _REAL_GET_REVIEW_PUSH_TIME("owner/name", 42) == expected
    assert calls == [("owner/name", 42)]


def test_get_pr_last_push_time_returns_none_without_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: "")
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_without_head_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *args, **kwargs: {"branch": "feature", "repo": None},
    )
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_when_activity_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "", ""])
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: next(responses))
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_on_parse_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-branch", "not-a-date"])
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: next(responses))
    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_on_run_gh_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_a: Any, **_kw: Any) -> str:
        raise RuntimeError("gh boom")

    monkeypatch.setattr("src.github.gh_runner.run_gh", boom)
    assert get_pr_last_push_time("owner/name", 42) is None


def test_has_recent_codex_review_request_returns_false_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("HTTP 404 not found")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert has_recent_codex_review_request("owner/name", 42, "author") is False


def test_has_recent_codex_review_request_propagates_non_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    with pytest.raises(RuntimeError, match="boom"):
        has_recent_codex_review_request("owner/name", 42, "author")


def test_has_recent_codex_review_request_skips_invalid_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
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
        "src.github.cache._gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "author"},
                "body": "@codex review",
                "created_at": "2026-04-19T11:59:30",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.gh_runner._parse_iso",
        lambda value: datetime(2026, 4, 19, 11, 59, 30),
    )
    monkeypatch.setattr("src.github.comments.datetime", _FakeDateTime)

    assert has_recent_codex_review_request("owner/name", 42, "author") is True


def test_gh_api_paginated_returns_none_for_non_list_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.github.cache import _gh_api_paginated

    monkeypatch.setattr(
        "src.github.cache.retry_transient",
        lambda func, operation_name=None: {"items": []},
    )

    assert _gh_api_paginated("repos/test/owner/issues/1/comments") is None


def test_get_codex_review_signals_returns_empty_on_404(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("HTTP 404 not found")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert _get_codex_review_signals("owner/name", 42) == {
        "latest_sha": "",
        "latest_time": None,
        "latest_state": "",
    }


def test_get_codex_review_signals_skips_non_codex_and_invalid_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            {
                "user": {"login": "alice"},
                "commit_id": "sha1",
                "submitted_at": "2026-04-19T11:59:00Z",
                "state": "approved",
            },
            {
                "user": _codex_user("chatgpt-codex-connector"),
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
        "src.github.reviews._get_codex_review_signals",
        lambda repo, pr_number, policy=None: {
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
    """``action_required`` is a check-run failure conclusion in REST.

    PR-251 (OBS-BC) refines ``action_required`` to INFRA_FAILURE when it
    is the *only* failing signal, since the conclusion typically means
    the workflow boot pre-flight failed (GitHub App permissions, etc.)
    rather than a code-level bug. Combined with a logic-class failing
    run, the rollup still resolves to FAILURE.
    """
    assert (
        _map_rest_ci_status_to_enum([{"conclusion": "action_required"}], {})
        == CIStatus.INFRA_FAILURE
    )
    assert (
        _map_rest_ci_status_to_enum(
            [
                {"conclusion": "action_required"},
                {"conclusion": "failure"},
            ],
            {},
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_success_requires_all_states_success_like() -> None:
    assert (
        _map_rest_ci_status_to_enum(
            [
                {"conclusion": "neutral"},
                {"conclusion": "skipped"},
                {"status": "completed", "conclusion": "success"},
            ],
            {"state": "success", "statuses": [{"state": "success"}]},
        )
        == CIStatus.SUCCESS
    )


def test_map_rest_ci_status_pending_when_states_missing_or_mixed() -> None:
    assert _map_rest_ci_status_to_enum([{}, {"conclusion": ""}], {}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([{"conclusion": "nonsense"}], {}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([{"conclusion": "success"}, {"status": "in_progress"}], {}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([{"status": "success"}], {}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([{"conclusion": 1}], {}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([{"status": 1}], {}) == CIStatus.PENDING
    assert _map_rest_ci_status_to_enum([{"status": "completed"}], {}) == CIStatus.PENDING
    assert (
        _map_rest_ci_status_to_enum([{"conclusion": None, "status": "completed"}], {})
        == CIStatus.PENDING
    )
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success", "status": "success"}],
            {"state": "success", "statuses": [{"state": "success"}]},
        )
        == CIStatus.PENDING
    )
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success", "status": "in_progress"}],
            {"state": "success", "statuses": [{"state": "success"}]},
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


def test_map_rest_ci_status_rejects_non_dict_check_run_entries() -> None:
    """Malformed check-run entries cannot satisfy the merge gate."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}, "garbage"],
            {"state": "success", "statuses": [{"state": "success"}]},
        )
        == CIStatus.PENDING
    )


def test_map_rest_ci_status_rejects_malformed_status_entries() -> None:
    """Malformed commit-status evidence cannot satisfy the merge gate."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "success", "statuses": [{}]},
        )
        == CIStatus.PENDING
    )
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "success", "statuses": [{"state": ""}]},
        )
        == CIStatus.PENDING
    )
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "failure"}],
            {"state": "success", "statuses": [{}]},
        )
        == CIStatus.FAILURE
    )


def test_map_rest_ci_status_rejects_unsupported_status_entries() -> None:
    """Unsupported commit-status states cannot satisfy the merge gate."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "success", "statuses": [{"state": "nonsense"}]},
        )
        == CIStatus.PENDING
    )


def test_map_rest_ci_status_preserves_failure_with_malformed_status_entries() -> None:
    """Known aggregate commit-status failures still route to failure handling."""
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "failure", "statuses": [{}]},
        )
        == CIStatus.FAILURE
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
    assert (
        _map_rest_ci_status_to_enum(
            [{"conclusion": "success"}],
            {"state": "failure", "statuses": []},
        )
        == CIStatus.FAILURE
    )
    assert (
        _map_rest_ci_status_to_enum(
            [],
            {"state": "failure", "statuses": []},
            empty_is_success=True,
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
            path = next(a for a in args if "check-runs" in a)
            if path.endswith("page=1"):
                return _ci_check_page(
                    [
                        {"id": run_id, "conclusion": "success"}
                        for run_id in range(100)
                    ],
                    total_count=101,
                )
            return _ci_check_page(
                [{"id": 100, "status": "in_progress"}],
                total_count=101,
            )
        return _ci_status_page("pending", [{"state": "pending"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert [r["id"] for r in check_runs] == list(range(101))
    assert status_payload == _ci_status_page("pending", [{"state": "pending"}])
    assert any("page=1" in a for c in calls for a in c)
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
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == _ci_status_page("success", [{"state": "success"}])
    assert fetch_ok is False


def test_fetch_ci_status_rest_degrades_on_status_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A combined-status API failure leaves an empty status payload."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([{"conclusion": "success"}])
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == [{"head_sha": "abc123", "conclusion": "success"}]
    assert status_payload == {}
    assert fetch_ok is False


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

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == {}
    assert fetch_ok is False


def test_fetch_ci_status_rest_partial_failure_keeps_empty_survivor_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One endpoint raising keeps the surviving empty source visible but incomplete."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            raise RuntimeError("HTTP 403")
        return _ci_status_page("pending", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == _ci_status_page("pending", [])
    assert fetch_ok is False


def test_fetch_ci_status_rest_partial_failure_status_side_with_empty_check_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mirror: ``status`` fails, ``check-runs`` returns empty — fetch still ok."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([])
        raise RuntimeError("HTTP 403")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert status_payload == {}
    assert fetch_ok is False


def test_fetch_ci_status_rest_parses_string_status_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``run_gh`` may return raw JSON text; ``_fetch_ci_status_rest`` parses it."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([])
        return json.dumps(_ci_status_page("success", [{"state": "success"}]))

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    _, status_payload, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert status_payload == _ci_status_page("success", [{"state": "success"}])


def test_fetch_ci_status_rest_string_status_invalid_json_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed string status payload degrades to an empty dict."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([])
        return "not-json"

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    _, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert status_payload == {}
    assert fetch_ok is False


def test_fetch_ci_status_rest_ignores_non_list_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``gh api --slurp`` returns an unexpected shape, fall back to empty."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return {"unexpected": True}
        return _ci_status_page("pending", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []


def test_fetch_ci_status_rest_rejects_nonempty_outer_array(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A plain single-page request cannot legitimately return an outer array."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return [_ci_check_page([{"conclusion": "success"}])]
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, _, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert check_runs == []
    assert fetch_ok is False


def test_fetch_ci_status_rest_empty_page_array_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A zero-page check-runs array is malformed, not an ``IndexError``."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return []
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert check_runs == []
    assert status_payload == _ci_status_page("success", [{"state": "success"}])
    assert fetch_ok is False


def test_fetch_ci_status_rest_rejects_truncated_check_run_total(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return {"total_count": 1, "check_runs": []}
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", "abc123")
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert retrieval.check_runs_source.complete is False
    assert retrieval.check_runs_source.error == "malformed"
    assert fetch_ok is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.PENDING


def test_fetch_ci_status_rest_rejects_invalid_check_run_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Check-run status and conclusion values are validated by field."""

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([{"status": "success"}])
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert check_runs == []
    assert status_payload == _ci_status_page("success", [{"state": "success"}])
    assert fetch_ok is False


def test_fetch_ci_status_rest_validates_all_present_check_run_state_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Valid conclusions cannot mask invalid or inconsistent statuses."""
    sha = "a" * 40

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return _ci_check_page(
                [
                    {
                        "id": 1,
                        "name": "invalid-domain",
                        "head_sha": sha,
                        "conclusion": "success",
                        "status": "success",
                    },
                    {
                        "id": 2,
                        "name": "inconsistent",
                        "head_sha": sha,
                        "conclusion": "success",
                        "status": "in_progress",
                    },
                    {
                        "id": 3,
                        "name": "running",
                        "head_sha": sha,
                        "conclusion": None,
                        "status": "in_progress",
                    },
                    {
                        "id": 4,
                        "name": "complete",
                        "head_sha": sha,
                        "conclusion": "success",
                        "status": "completed",
                    },
                    {
                        "id": 5,
                        "name": "missing-conclusion",
                        "head_sha": sha,
                        "status": "completed",
                    },
                ],
                sha=sha,
            )
        return _ci_status_page("success", [{"state": "success"}], sha=sha)

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", sha)
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", sha)

    assert [run["id"] for run in check_runs] == [3, 4]
    assert status_payload == _ci_status_page("success", [{"state": "success"}], sha=sha)
    assert fetch_ok is False
    assert retrieval.evidence.repo == "owner/name"
    assert retrieval.evidence.sha == sha
    assert retrieval.evidence.sources_complete is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.PENDING


def test_fetch_ci_status_rest_hydrates_annotations_for_failing_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: ``GET /commits/{sha}/check-runs`` returns
    ``annotations_count`` + ``annotations_url`` but never the annotation
    messages. ``_fetch_ci_status_rest`` must follow ``annotations_url``
    (constructed from the check-run id) so ``_is_infra_failure``'s
    keyword path runs against real REST payloads.
    """
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(list(args))
        joined = " ".join(args)
        if "check-runs" in joined and "annotations" in joined:
            return [
                {
                    "message": "Runner offline; could not start the job.",
                    "annotation_level": "failure",
                }
            ]
        if "check-runs" in joined:
            return _ci_check_page(
                [
                    {
                        "id": 42,
                        "conclusion": "failure",
                        "annotations_count": 1,
                        "annotations_url": (
                            "https://api.github.com/repos/owner/name/"
                            "check-runs/42/annotations"
                        ),
                    }
                ]
            )
        return _ci_status_page("success", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert len(check_runs) == 1
    annotations = check_runs[0].get("annotations")
    assert isinstance(annotations, list)
    assert annotations and "Runner offline" in annotations[0]["message"]
    # The hydration step must have queried the per-check-run annotations
    # endpoint constructed from ``id`` (not blindly hitting the
    # ``annotations_url`` host string), and must bound the page size so
    # large lint/test runs don't paginate every WATCH cycle.
    annotation_calls = [
        c for c in calls
        if any(
            "check-runs/42/annotations" in a and "per_page=" in a
            for a in c
        )
    ]
    assert annotation_calls, "expected a bounded annotations fetch"
    # Single non-paginated call (no ``--paginate``/``--slurp``) so the
    # worst case is one extra REST request per failing check-run.
    for call in annotation_calls:
        assert "--paginate" not in call
        assert "--slurp" not in call


def test_fetch_ci_status_rest_skips_annotation_hydration_for_passing_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: hydration is gated to failing non-infra
    conclusions so passing builds don't pay the extra REST round-trip."""
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(list(args))
        if any("check-runs" in a for a in args):
            return _ci_check_page(
                [
                    {
                        "id": 7,
                        "conclusion": "success",
                        "annotations_count": 3,
                        "annotations_url": (
                            "https://api.github.com/repos/owner/name/"
                            "check-runs/7/annotations"
                        ),
                    }
                ]
            )
        return _ci_status_page("success", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert "annotations" not in check_runs[0]
    assert all("annotations" not in a for c in calls for a in c)


def test_fetch_ci_status_rest_skips_hydration_for_infra_conclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: ``cancelled`` already classifies as infra by
    conclusion alone; no need to spend a REST call hydrating
    annotations the classifier won't consult."""
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(list(args))
        if any("check-runs" in a for a in args):
            return _ci_check_page(
                [
                    {
                        "id": 9,
                        "conclusion": "cancelled",
                        "annotations_count": 2,
                        "annotations_url": (
                            "https://api.github.com/repos/owner/name/"
                            "check-runs/9/annotations"
                        ),
                    }
                ]
            )
        return _ci_status_page("pending", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    assert "annotations" not in check_runs[0]
    assert all("annotations" not in a for c in calls for a in c)


def test_fetch_ci_status_rest_hydration_swallows_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a transient failure hitting the annotations
    endpoint must not propagate; the check-run is left without
    ``annotations`` so the classifier falls back to ``FAILURE`` (the
    safe default), and the rest of the CI fetch still succeeds.
    """

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        joined = " ".join(args)
        if "check-runs" in joined and "annotations" in joined:
            raise RuntimeError("HTTP 503")
        if "check-runs" in joined:
            return _ci_check_page(
                [
                    {
                        "id": 11,
                        "conclusion": "failure",
                        "annotations_count": 1,
                        "annotations_url": (
                            "https://api.github.com/repos/owner/name/"
                            "check-runs/11/annotations"
                        ),
                    }
                ]
            )
        return _ci_status_page("success", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, _, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")
    assert "annotations" not in check_runs[0]
    assert fetch_ok is True


def test_retrieve_ci_status_evidence_distinguishes_source_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_empty(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([])
        return _ci_status_page("pending", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_empty)
    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")
    assert evidence.check_runs_source.empty is True
    assert evidence.status_source.complete is True

    clear_ci_status_cache()

    def fake_forbidden(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            raise RuntimeError("HTTP 403 Forbidden")
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_forbidden)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)
    assert _retrieve_ci_status_evidence("owner/name", "abc123").check_runs_source.error == "forbidden"

    clear_ci_status_cache()

    def fake_timeout(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([])
        raise RuntimeError("net/http: TLS handshake timeout")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_timeout)
    assert _retrieve_ci_status_evidence("owner/name", "abc123").status_source.error == "timeout"

    clear_ci_status_cache()

    def fake_malformed(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([])
        return "not-json"

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_malformed)
    assert _retrieve_ci_status_evidence("owner/name", "abc123").status_source.error == "malformed"


def test_retrieve_ci_status_evidence_reuses_contract_across_cache_and_304(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    status_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return _ci_check_page(
                [
                    {
                        "id": 1,
                        "name": "unit",
                        "head_sha": sha,
                        "status": "completed",
                        "conclusion": "success",
                        "app": {"id": 1},
                    }
                ],
                sha=sha,
            )
        status_calls.append(list(args))
        if len(status_calls) == 1:
            body = json.dumps(
                _ci_status_page(
                    "success",
                    [
                        {
                            "context": "legacy",
                            "state": "success",
                            "sha": sha,
                            "creator": {"login": "ci-bot"},
                        }
                    ],
                    sha=sha,
                )
            )
            return _build_include_response(body, etag='W/"status-v1"')
        return _build_include_response("", status=304, etag='W/"status-v1"')

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    first = _retrieve_ci_status_evidence("owner/name", sha)
    cached = _retrieve_ci_status_evidence("owner/name", sha)

    assert cached is first
    assert first.evidence.repo == "owner/name"
    assert first.evidence.sha == sha
    assert first.evidence.sources_complete is True
    assert first.evidence.policy_result == CIStatus.SUCCESS
    assert first.evidence.observed_at == cached.evidence.observed_at
    assert len(status_calls) == 1

    clear_ci_status_cache()
    revalidated = _retrieve_ci_status_evidence("owner/name", sha)

    assert revalidated.status_payload == first.status_payload
    assert revalidated.evidence.repo == first.evidence.repo
    assert revalidated.evidence.sha == first.evidence.sha
    assert revalidated.evidence.sources_complete is True
    assert revalidated.evidence.policy_result == CIStatus.SUCCESS
    assert revalidated.evidence.observed_at == first.evidence.observed_at
    assert any("If-None-Match" in arg for arg in status_calls[-1])


@pytest.mark.parametrize(
    ("aggregate_state", "later_context", "later_time", "expected"),
    [
        ("success", "legacy", "2026-10-04T11:00:00Z", CIStatus.SUCCESS),
        ("failure", "integration", "2026-10-04T13:00:00Z", CIStatus.FAILURE),
    ],
)
def test_retrieve_ci_status_evidence_paginates_combined_statuses(
    monkeypatch: pytest.MonkeyPatch,
    aggregate_state: str,
    later_context: str,
    later_time: str,
    expected: CIStatus,
) -> None:
    sha = "a" * 40
    status_calls: list[str] = []

    def status(context: str, state: str, updated_at: str) -> dict:
        return {
            "context": context,
            "state": state,
            "sha": sha,
            "updated_at": updated_at,
            "creator": {"login": "ci-bot"},
        }

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return _ci_check_page([], sha=sha)
        path = next(arg for arg in args if "/status?" in arg)
        status_calls.append(path)
        if path.endswith("page=1"):
            return {
                "state": aggregate_state,
                "sha": sha,
                "total_count": 101,
                "statuses": [
                    status("legacy", "success", "2026-10-04T12:00:00Z"),
                    *[
                        status(f"context-{index}", "success", "2026-10-04T12:00:00Z")
                        for index in range(99)
                    ],
                ],
            }
        return {
            "state": aggregate_state,
            "sha": sha,
            "total_count": 101,
            "statuses": [
                status(
                    later_context,
                    "failure",
                    later_time,
                )
            ],
        }

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", sha)
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", sha)

    assert check_runs == []
    assert len(status_payload["statuses"]) == 101
    assert fetch_ok is True
    assert retrieval.status_source.complete is True
    assert retrieval.evidence.sources_complete is True
    assert retrieval.evidence.policy_result == expected
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == expected
    assert any(path.endswith("page=2") for path in status_calls)


@pytest.mark.parametrize(
    "responses",
    [
        [{"state": "success", "total_count": True, "statuses": []}],
        [
            {"state": "success", "total_count": 101, "statuses": [{}] * 100},
            {"state": "success", "total_count": 100, "statuses": [{}]},
        ],
        [{"state": "success", "total_count": 1, "statuses": [{}, {}]}],
    ],
)
def test_status_page_walker_rejects_inconsistent_totals(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[dict],
) -> None:
    payloads = iter(responses)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args: next(payloads))

    evidence = cache._etag_get_object_pages_evidence(
        "repos/owner/name/commits/abc/status?per_page=100",
        "statuses",
    )

    assert evidence.complete is False
    assert evidence.error == "malformed"


@pytest.mark.parametrize("source", ["check_runs", "status"])
def test_retrieve_ci_status_evidence_requires_total_count(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            if source == "check_runs":
                return {
                    "check_runs": [
                        {"head_sha": "abc123", "conclusion": "success"}
                    ]
                }
            return _ci_check_page([{"conclusion": "success"}])
        if source == "status":
            return {
                "state": "success",
                "sha": "abc123",
                "statuses": [{"state": "success"}],
            }
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", "abc123")
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest(
        "owner/name", "abc123"
    )
    source_result = (
        retrieval.check_runs_source
        if source == "check_runs"
        else retrieval.status_source
    )

    assert source_result.complete is False
    assert source_result.error == "malformed"
    assert retrieval.evidence.sources_complete is False
    assert retrieval.evidence.policy_result == CIStatus.PENDING
    assert fetch_ok is False
    assert (
        _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok)
        == CIStatus.PENDING
    )


@pytest.mark.parametrize("source", ["check_runs", "status"])
def test_retrieve_ci_status_evidence_rejects_payload_for_another_sha(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            run_sha = "oldsha" if source == "check_runs" else "abc123"
            return _ci_check_page([{"conclusion": "success"}], sha=run_sha)
        status_sha = "oldsha" if source == "status" else "abc123"
        return _ci_status_page(
            "success", [{"state": "success"}], sha=status_sha
        )

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", "abc123")
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest(
        "owner/name", "abc123"
    )
    source_result = (
        retrieval.check_runs_source
        if source == "check_runs"
        else retrieval.status_source
    )

    assert source_result.complete is False
    assert source_result.error == "malformed"
    assert retrieval.evidence.sources_complete is False
    assert retrieval.evidence.policy_result == CIStatus.PENDING
    assert fetch_ok is False
    if source == "check_runs":
        assert check_runs == []
    else:
        assert retrieval.status_payload["sha"] == "oldsha"
        assert status_payload == {}
    assert (
        _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok)
        == CIStatus.PENDING
    )


def test_retrieve_ci_status_evidence_does_not_attribute_foreign_aggregate_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested_sha = "a" * 40
    foreign_sha = "b" * 40

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return _ci_check_page(
                [{"name": "unit", "conclusion": "success", "app": {"id": 1}}],
                sha=requested_sha,
            )
        return _ci_status_page(
            "failure",
            [
                {
                    "context": "legacy",
                    "state": "failure",
                    "sha": foreign_sha,
                    "creator": {"login": "ci-bot"},
                }
            ],
            sha=foreign_sha,
        )

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", requested_sha)
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest(
        "owner/name", requested_sha
    )

    assert retrieval.status_source.sha_matches is False
    assert retrieval.status_payload["state"] == "failure"
    assert retrieval.evidence.policy_result == CIStatus.PENDING
    assert retrieval.evidence.pending_reason == "sources_incomplete"
    assert status_payload == {}
    assert fetch_ok is False
    assert (
        _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok)
        == CIStatus.PENDING
    )


def test_fetch_ci_status_rest_later_status_page_failure_preserves_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return _ci_check_page([])
        path = next(arg for arg in args if "/status?" in arg)
        if path.endswith("page=1"):
            return {
                "state": "failure",
                "sha": "abc123",
                "total_count": 101,
                "statuses": [{"state": "success"}] * 100,
            }
        raise RuntimeError("HTTP 503")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    retrieval = _retrieve_ci_status_evidence("owner/name", "abc123")
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert retrieval.status_source.complete is False
    assert "HTTP 503" in retrieval.status_source.error
    assert retrieval.evidence.policy_result == CIStatus.FAILURE
    assert len(status_payload["statuses"]) == 100
    assert fetch_ok is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.FAILURE


def test_retrieve_ci_status_evidence_rejects_invalid_statuses_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence([], True, True))
    monkeypatch.setattr(
        cache,
        "_etag_get",
        lambda path: {"state": "success", "sha": "abc123", "total_count": 1, "statuses": "bad"},
    )

    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")

    assert evidence.status_source.error == "malformed"


def test_retrieve_ci_status_evidence_preserves_empty_aggregate_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in arg for arg in args):
            return _ci_check_page([{"conclusion": "success"}])
        return _ci_status_page("failure", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    retrieval = _retrieve_ci_status_evidence("owner/name", "abc123")
    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert retrieval.status_source.complete is False
    assert retrieval.status_source.error == "malformed"
    assert retrieval.evidence.policy_result == CIStatus.FAILURE
    assert retrieval.evidence.pending_reason is None
    assert status_payload == _ci_status_page("failure", [])
    assert fetch_ok is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.FAILURE


def test_retrieve_ci_status_evidence_rejects_malformed_status_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence([], True, True))
    monkeypatch.setattr(cache, "_etag_get", lambda path: _ci_status_page("success", [{}]))

    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")

    assert evidence.status_source.complete is False
    assert evidence.status_source.error == "malformed"
    assert evidence.status_payload == {}


def test_retrieve_ci_status_evidence_rejects_empty_aggregate_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence([], True, True))
    monkeypatch.setattr(
        cache,
        "_etag_get",
        lambda path: _ci_status_page("", [{"state": "success"}]),
    )

    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")

    assert evidence.status_source.complete is False
    assert evidence.status_source.error == "malformed"
    assert evidence.status_payload == {}


def test_retrieve_ci_status_evidence_rejects_unsupported_status_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence([], True, True))
    monkeypatch.setattr(
        cache,
        "_etag_get",
        lambda path: _ci_status_page("success", [{"state": "nonsense"}]),
    )

    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")

    assert evidence.status_source.complete is False
    assert evidence.status_source.error == "malformed"
    assert evidence.status_payload == {}


def test_retrieve_ci_status_evidence_preserves_failure_with_malformed_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence([], True, True))
    monkeypatch.setattr(cache, "_etag_get", lambda path: _ci_status_page("failure", [{}]))

    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")

    assert evidence.status_source.complete is False
    assert evidence.status_source.error == "malformed"
    assert evidence.evidence.policy_result == CIStatus.FAILURE
    assert evidence.status_payload == _ci_status_page("failure", [{}])


def test_retrieve_ci_status_evidence_rejects_non_object_status_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence([], True, True))
    monkeypatch.setattr(cache, "_etag_get", lambda path: _ci_status_page("success", [7]))

    evidence = _retrieve_ci_status_evidence("owner/name", "abc123")

    assert evidence.status_source.error == "malformed"
    assert evidence.status_payload == {}


def test_fetch_ci_status_rest_second_page_failure_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        path = next((arg for arg in args if "check-runs" in arg), "")
        if path.endswith("page=1"):
            return _ci_check_page(
                [
                    {"id": run_id, "conclusion": "success"}
                    for run_id in range(100)
                ],
                total_count=101,
            )
        if path:
            raise RuntimeError("HTTP 503")
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert len(check_runs) == 100
    assert status_payload == _ci_status_page("success", [{"state": "success"}])
    assert fetch_ok is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.PENDING


def test_fetch_ci_status_rest_later_outer_array_preserves_known_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        path = next((arg for arg in args if "check-runs" in arg), "")
        if path.endswith("page=1"):
            return _ci_check_page(
                [
                    {"id": 0, "conclusion": "failure"},
                    *[
                        {"id": run_id, "conclusion": "success"}
                        for run_id in range(1, 100)
                    ],
                ],
                total_count=101,
            )
        if path:
            return [{"check_runs": [{"id": 100, "conclusion": "success"}]}]
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert len(check_runs) == 100
    assert check_runs[0]["conclusion"] == "failure"
    assert fetch_ok is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.FAILURE


def test_fetch_ci_status_rest_malformed_check_runs_field_is_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pages = [
        {"check_runs": "bad"},
        {"check_runs": [{"head_sha": "abc123", "conclusion": "success"}, "bad"]},
    ]
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence(pages, True, False))
    monkeypatch.setattr(cache, "_etag_get", lambda path: _ci_status_page("success", []))

    check_runs, _, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert check_runs == [{"head_sha": "abc123", "conclusion": "success"}]
    assert fetch_ok is False


def test_fetch_ci_status_rest_partial_page_preserves_known_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_paginated(path: str) -> cache.PaginatedEvidence:
        return cache.PaginatedEvidence(
            [_ci_check_page([{"conclusion": "failure"}])],
            complete=False,
            empty=False,
            error="HTTP 503",
        )

    monkeypatch.setattr("src.github.cache._gh_api_paginated_evidence", fake_paginated)
    monkeypatch.setattr(
        "src.github.cache._etag_get",
        lambda path: _ci_status_page("success", [{"state": "success"}]),
    )

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert check_runs == [{"head_sha": "abc123", "conclusion": "failure"}]
    assert fetch_ok is False
    assert _map_rest_ci_status_to_enum(check_runs, status_payload, fetch_ok=fetch_ok) == CIStatus.FAILURE


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
            return _ci_check_page([{"conclusion": "success"}])
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
        sha = _ci_sha_from_args(args)
        if any("check-runs" in a for a in args):
            return _ci_check_page([{"conclusion": "success", "id": sha}], sha=sha)
        return _ci_status_page("success", [{"state": "success"}], sha=sha)

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    first_runs, _, _ = _fetch_ci_status_rest("owner/name", "abc123")
    second_runs, _, _ = _fetch_ci_status_rest("owner/name", "def456")

    assert first_runs[0]["id"] == "abc123"
    assert second_runs[0]["id"] == "def456"


def test_fetch_ci_status_rest_ignores_wrong_sha_cache_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A poisoned cache entry under the right key but wrong SHA is not reused."""
    from src.github.checks import _ci_status_cache

    monkeypatch.setattr("src.github.checks.time.monotonic", lambda: 1000.0)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kwargs: _ci_check_page([{"conclusion": "success", "id": "oldsha"}])
        if any("check-runs" in arg for arg in args)
        else _ci_status_page("success", [{"state": "success"}]),
    )
    _retrieve_ci_status_evidence("owner/name", "abc123")
    cached = _ci_status_cache[("owner/name", "abc123")]
    _ci_status_cache[("owner/name", "abc123")] = cached._replace(
        evidence=cached.evidence._replace(sha="oldsha")
    )

    monkeypatch.setattr("src.github.checks.time.monotonic", lambda: 1001.0)

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            return _ci_check_page([{"conclusion": "failure", "id": "abc123"}])
        return _ci_status_page("success", [{"state": "success"}])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    check_runs, _, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert check_runs == [
        {"head_sha": "abc123", "conclusion": "failure", "id": "abc123"}
    ]
    assert fetch_ok is True


def test_fetch_ci_status_rest_expired_cache_entry_refetches_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Expired complete evidence cannot silently acquire current provenance."""
    monkeypatch.setattr("src.github.checks.time.monotonic", lambda: 1000.0)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kwargs: _ci_check_page([{"conclusion": "success"}])
        if any("check-runs" in arg for arg in args)
        else _ci_status_page("success", [{"state": "success"}]),
    )
    _retrieve_ci_status_evidence("owner/name", "abc123")

    monkeypatch.setattr("src.github.checks.time.monotonic", lambda: 1100.0)

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        raise RuntimeError("HTTP 403 Forbidden")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    check_runs, status_payload, fetch_ok = _fetch_ci_status_rest("owner/name", "abc123")

    assert check_runs == []
    assert status_payload == {}
    assert fetch_ok is False


def test_fetch_ci_status_rest_cache_expires_after_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cached entry older than the TTL must be refetched, so PENDING -> SUCCESS
    transitions on the same SHA are observed without an upstream push."""
    state = {"calls": 0}

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if any("check-runs" in a for a in args):
            state["calls"] += 1
            return _ci_check_page(
                [{"conclusion": "success", "name": f"call_{state['calls']}"}]
            )
        return _ci_status_page("pending", [])

    fake_now = {"value": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["value"]

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github.prs.time.monotonic", fake_monotonic)

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
            return _ci_check_page(
                [{"conclusion": "success", "name": f"call_{state['calls']}"}]
            )
        return _ci_status_page("success", [])

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

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
    from src.github.checks import _ci_status_cache

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        sha = _ci_sha_from_args(args)
        if any("check-runs" in a for a in args):
            return _ci_check_page([{"conclusion": "success"}], sha=sha)
        return _ci_status_page("success", [{"state": "success"}], sha=sha)

    fake_now = {"value": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["value"]

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github.prs.time.monotonic", fake_monotonic)

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
    from src.github.checks import _ci_status_cache

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        sha = _ci_sha_from_args(args)
        if any("check-runs" in a for a in args):
            return _ci_check_page([{"conclusion": "success"}], sha=sha)
        return _ci_status_page("success", [{"state": "success"}], sha=sha)

    fake_now = {"value": 1000.0}

    def fake_monotonic() -> float:
        return fake_now["value"]

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr("src.github.prs.time.monotonic", fake_monotonic)

    _fetch_ci_status_rest("owner/name", "sha-fresh")
    fake_now["value"] += 1.0  # still well inside the 15s TTL
    _fetch_ci_status_rest("owner/name", "sha-other")

    assert ("owner/name", "sha-fresh") in _ci_status_cache
    assert ("owner/name", "sha-other") in _ci_status_cache


def test_parse_iso_returns_none_for_invalid_string() -> None:
    assert _parse_iso("not-a-date") is None


def test_extract_commit_date_returns_empty_for_malformed_payload() -> None:
    assert _extract_commit_date([]) == ""


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

    result = asyncio.run(rate_limit.get_current_rate_limit_budget(redis))
    assert result is not None
    assert result.remaining == 42


def test_get_current_rate_limit_budget_none_when_no_observation() -> None:
    import asyncio

    class _FakeRedis:
        async def get(self, key: str) -> str | None:
            return None

    assert asyncio.run(rate_limit.get_current_rate_limit_budget(_FakeRedis())) is None


def _bucket(remaining: int, limit: int = 5000, reset: int = 1745683200) -> dict:
    return {"remaining": remaining, "limit": limit, "reset": reset}


def test_fetch_rate_limit_budget_parses_dict_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: {
            "core": _bucket(remaining=4321),
            "graphql": _bucket(remaining=4900),
        },
    )
    budget = rate_limit.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 4321
    assert budget.limit == 5000


def test_fetch_rate_limit_budget_parses_string_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: (
            '{"core": {"remaining": 100, "limit": 5000, "reset": 0},'
            ' "graphql": {"remaining": 4500, "limit": 5000, "reset": 0}}'
        ),
    )
    budget = rate_limit.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 100


def test_fetch_rate_limit_budget_returns_graphql_when_more_constrained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GraphQL exhaustion must surface even when REST/core is healthy."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: {
            "core": _bucket(remaining=4900),
            "graphql": _bucket(remaining=10),
        },
    )
    budget = rate_limit.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 10


def test_fetch_rate_limit_budget_falls_back_when_one_bucket_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: {"core": _bucket(remaining=4321)},
    )
    budget = rate_limit.fetch_rate_limit_budget()
    assert budget is not None
    assert budget.remaining == 4321


def test_fetch_rate_limit_budget_returns_none_for_invalid_json_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args, **kw: "not-json")
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_for_unexpected_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args, **kw: [1, 2])
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_for_missing_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: {"core": {"remaining": 10}},
    )
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_when_both_buckets_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args, **kw: {})
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_when_gh_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(args: list[str], **kw: Any) -> None:
        raise RuntimeError("API rate limit exceeded")

    monkeypatch.setattr("src.github.gh_runner.run_gh", _raise)
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_when_gh_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(args: list[str], **kw: Any) -> None:
        raise OSError("gh missing")

    monkeypatch.setattr("src.github.gh_runner.run_gh", _raise)
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_budget_returns_none_on_malformed_int(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: {
            "core": {"remaining": "abc", "limit": 5000, "reset": 0},
            "graphql": {"remaining": "xyz", "limit": 5000, "reset": 0},
        },
    )
    assert rate_limit.fetch_rate_limit_budget() is None


def test_fetch_rate_limit_buckets_returns_each_bucket_separately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both buckets surface independently so the dashboard renders each chip."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args, **kw: {
            "core": _bucket(remaining=4321),
            "graphql": _bucket(remaining=120),
        },
    )
    rest, graphql = rate_limit.fetch_rate_limit_buckets()
    assert rest is not None and rest.remaining == 4321
    assert graphql is not None and graphql.remaining == 120


def test_fetch_rate_limit_buckets_returns_pair_of_none_on_gh_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(args: list[str], **kw: Any) -> None:
        raise RuntimeError("gh down")

    monkeypatch.setattr("src.github.gh_runner.run_gh", _raise)
    assert rate_limit.fetch_rate_limit_buckets() == (None, None)


def test_fetch_rate_limit_buckets_returns_pair_of_none_on_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args, **kw: "not-json")
    assert rate_limit.fetch_rate_limit_buckets() == (None, None)


def test_fetch_rate_limit_buckets_returns_pair_of_none_on_unexpected_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda args, **kw: [1])
    assert rate_limit.fetch_rate_limit_buckets() == (None, None)


def test_get_latest_codex_feedback_collects_post_anchor_codex_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue and review comments authored by Codex after the anchor are joined."""
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": _codex_user("codex-bot"),
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
                    "user": _codex_user("codex-bot"),
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
                    "user": _codex_user("codex-bot"),
                    "body": "P2: extract helper",
                    "created_at": "2026-04-27T03:00:00Z",
                }
            ]
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    out = comments.get_latest_codex_feedback("owner/name", 42)
    assert out == "P1: rename foo\n\nP2: extract helper"


def test_get_latest_codex_feedback_returns_none_when_no_codex_comments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert comments.get_latest_codex_feedback("owner/name", 42) is None


def test_get_latest_codex_feedback_skips_onboarding_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

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
                    "user": _codex_user("codex-bot"),
                    "body": "Please create a Codex account and connect to github.",
                    "created_at": "2026-04-27T01:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert comments.get_latest_codex_feedback("owner/name", 42) is None


def test_get_latest_codex_feedback_returns_all_codex_comments_when_no_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No PR-author ``@codex review`` trigger: every Codex comment counts."""
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": _codex_user("codex-bot"),
                    "body": "feedback before any anchor",
                    "created_at": "2026-04-27T01:00:00Z",
                }
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    out = comments.get_latest_codex_feedback("owner/name", 42)
    assert out == "feedback before any anchor"


def test_get_latest_codex_feedback_skips_non_author_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ``@codex review`` posted by a teammate is not the anchor."""
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

    def fake_paginated(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return [
                {
                    "id": 1,
                    "user": _codex_user("codex-bot"),
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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    out = comments.get_latest_codex_feedback("owner/name", 42)
    assert out == "P1: real feedback"


def test_get_latest_codex_feedback_returns_none_when_endpoints_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

    def fake_paginated(path: str) -> list[dict]:
        raise RuntimeError("api blew up")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert comments.get_latest_codex_feedback("owner/name", 42) is None


def test_get_latest_codex_feedback_returns_none_when_endpoints_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Endpoint ``TimeoutExpired`` / ``OSError`` must degrade to ``None``,
    not bubble out and abort the FIX cycle before the coder runs.
    """
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

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

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert comments.get_latest_codex_feedback("owner/name", 42) is None
    assert raised == [subprocess.TimeoutExpired, FileNotFoundError]


def test_get_latest_codex_feedback_truncates_oversized_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Joined feedback must be capped to avoid ``Argument list too long``
    when the FIX prompt embeds it as a single CLI argument.
    """
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

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
                    "user": _codex_user("codex-bot"),
                    "body": big_body,
                    "created_at": "2026-04-27T01:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    out = comments.get_latest_codex_feedback("owner/name", 42)
    assert out is not None
    assert out.startswith("[truncated]\n")
    assert len(out) == len("[truncated]\n") + gh_comments._REVIEW_FEEDBACK_TRUNCATE_CHARS
    assert out.endswith("x" * 100)


def test_get_latest_codex_feedback_skips_empty_codex_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_pr_author", lambda repo, n: "author")

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
                    "user": _codex_user("codex-bot"),
                    "body": "   ",
                    "created_at": "2026-04-27T01:00:00Z",
                },
            ]
        if path.endswith("/pulls/42/comments"):
            return []
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)

    assert comments.get_latest_codex_feedback("owner/name", 42) is None


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
    cache.clear_etag_cache()


def test_etag_get_first_call_populates_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First call has no ``If-None-Match``, parses 200 body, caches the ETag."""
    captured: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(stdout=_build_include_response('{"merged": true}', etag='W/"abc"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = cache._etag_get("repos/owner/name/pulls/42")

    assert payload == {"merged": True}
    assert "--include" in captured[0]
    assert not any("If-None-Match" in arg for arg in captured[0])
    cached = cache._etag_cache["repos/owner/name/pulls/42"]
    assert cached.etag == 'W/"abc"'
    assert cached.payload == {"merged": True}
    assert cached.observed_at.tzinfo is not None


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

    cache._etag_get("repos/owner/name/pulls/7")
    cache._etag_get("repos/owner/name/pulls/7")

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

    first = cache._etag_get("repos/owner/name/pulls/9")
    second = cache._etag_get("repos/owner/name/pulls/9")

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

    first = cache._etag_get("repos/owner/name/commits/abc")
    second = cache._etag_get("repos/owner/name/commits/abc")

    assert first == {"v": 1}
    assert second == {"v": 2}
    cached = cache._etag_cache["repos/owner/name/commits/abc"]
    assert cached.etag == 'W/"v2"'
    assert cached.payload == {"v": 2}


def test_etag_get_evicts_oldest_when_max_entries_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cache must drop the least-recently-used entry past ``_ETAG_CACHE_MAX_ENTRIES``."""
    monkeypatch.setattr("src.github.cache._ETAG_CACHE_MAX_ENTRIES", 3)
    counter = {"i": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        counter["i"] += 1
        body = f'{{"i": {counter["i"]}}}'
        return _FakeCompletedProcess(stdout=_build_include_response(body, etag=f'W/"e{counter["i"]}"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    cache._etag_get("repos/x/y/pulls/1")
    cache._etag_get("repos/x/y/pulls/2")
    cache._etag_get("repos/x/y/pulls/3")
    cache._etag_get("repos/x/y/pulls/4")  # forces eviction of /pulls/1

    assert "repos/x/y/pulls/1" not in cache._etag_cache
    assert {
        "repos/x/y/pulls/2",
        "repos/x/y/pulls/3",
        "repos/x/y/pulls/4",
    } <= set(cache._etag_cache.keys())


def test_etag_get_returns_none_on_unparseable_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 200 with malformed JSON must not crash and must not poison the cache."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_build_include_response("{not-json", etag='W/"v1"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get("repos/owner/name/pulls/1") is None
    assert "repos/owner/name/pulls/1" not in cache._etag_cache


def test_etag_get_304_without_cache_retries_without_if_none_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 304 with no cached payload must retry without ``If-None-Match`` and parse the fresh body (PR-236)."""
    captured: list[list[str]] = []
    responses = iter(
        [
            _build_include_response("", status=304, etag='W/"v1"'),
            _build_include_response('{"merged": true}', etag='W/"v2"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append(cmd)
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    payload = cache._etag_get("repos/owner/name/pulls/3")

    assert payload == {"merged": True}
    assert len(captured) == 2
    assert not any("If-None-Match" in arg for arg in captured[1])
    cached = cache._etag_cache["repos/owner/name/pulls/3"]
    assert cached.etag == 'W/"v2"'
    assert cached.payload == {"merged": True}


def test_etag_get_304_no_cache_retry_returns_none_on_non_2xx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the no-cache retry itself fails with non-2xx, ``_etag_get`` returns None (PR-236)."""
    responses = iter(
        [
            _build_include_response("", status=304, etag='W/"v1"'),
            _build_include_response("server error", status=500, etag=None),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get("repos/owner/name/pulls/3") is None
    assert "repos/owner/name/pulls/3" not in cache._etag_cache


def test_etag_get_304_no_cache_retry_returns_none_on_empty_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the retry returns 200 with empty body, ``_etag_get`` returns None (PR-236)."""
    responses = iter(
        [
            _build_include_response("", status=304, etag='W/"v1"'),
            _build_include_response("", status=200, etag='W/"v2"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get("repos/owner/name/pulls/3") is None


def test_etag_get_304_no_cache_retry_returns_none_on_unparseable_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the retry returns malformed JSON, ``_etag_get`` returns None (PR-236)."""
    responses = iter(
        [
            _build_include_response("", status=304, etag='W/"v1"'),
            _build_include_response("{not-json", etag='W/"v2"'),
        ]
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=next(responses))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get("repos/owner/name/pulls/3") is None
    assert "repos/owner/name/pulls/3" not in cache._etag_cache


def test_etag_get_304_no_cache_retry_passes_through_pre_parsed_run_gh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If a test stubs ``run_gh`` to return a parsed object, the retry surfaces it directly (PR-236)."""
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str]) -> object:
        calls.append(args)
        if len(calls) == 1:
            return _build_include_response("", status=304, etag='W/"v1"')
        return {"merged": True}

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    payload = cache._etag_get("repos/owner/name/pulls/4")

    assert payload == {"merged": True}
    assert len(calls) == 2
    assert not any("If-None-Match" in arg for arg in calls[1])


def test_etag_get_passthrough_for_pre_parsed_run_gh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``run_gh`` is stubbed to return a parsed object (no HTTP head),
    ``_etag_get`` must surface it directly so call-site tests retain their
    semantics without crafting raw ``--include`` strings."""
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args: {"merged": True, "state": "closed"},
    )

    payload = cache._etag_get("repos/owner/name/pulls/5")
    assert payload == {"merged": True, "state": "closed"}
    # Cache stays empty because the test bypassed the --include path.
    assert "repos/owner/name/pulls/5" not in cache._etag_cache


def test_etag_get_returns_none_on_5xx(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 5xx response (rare; gh normally raises) yields None and leaves cache untouched."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_build_include_response("server error", status=500, etag=None))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get("repos/owner/name/pulls/8") is None
    assert "repos/owner/name/pulls/8" not in cache._etag_cache


def test_etag_get_empty_200_body_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 200 with no body (degenerate) yields None rather than crashing on JSON."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_build_include_response("", status=200, etag='W/"v1"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get("repos/owner/name/pulls/9") is None


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

    items = cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100")

    assert items is not None
    assert [item["n"] for item in items] == list(range(102))
    assert len(captured) == 2
    assert "repos/owner/name/pulls?state=open&per_page=100&page=1" in cache._etag_cache
    assert "repos/owner/name/pulls?state=open&per_page=100&page=2" in cache._etag_cache


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
    first = cache._etag_get_paginated(base)
    second = cache._etag_get_paginated(base)

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
        return _FakeCompletedProcess(stdout=_build_include_response('[{"n": 1}]', etag='W/"only"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = cache._etag_get_paginated("repos/owner/name/pulls?state=closed&per_page=100")

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
        return _FakeCompletedProcess(stdout=_build_include_response(body, etag=f'W/"p{state["calls"]}"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = cache._etag_get_paginated("repos/owner/name/pulls?state=closed&per_page=2")

    assert items is not None
    assert len(items) == full_pages * 2 + 1
    assert state["calls"] == full_pages + 1


def test_etag_get_paginated_uses_default_per_page_when_unspecified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``per_page=`` in the URL, GitHub's 30-default terminates the walk."""
    body = "[" + ",".join(f'{{"n": {i}}}' for i in range(15)) + "]"

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_build_include_response(body, etag='W/"single"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    items = cache._etag_get_paginated("repos/owner/name/pulls")

    assert items == [{"n": i} for i in range(15)]


def test_etag_get_paginated_first_page_none_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A None payload on the first page (e.g. 5xx) yields None overall."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_build_include_response("server error", status=500, etag=None))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100") is None


def test_etag_get_paginated_first_page_non_list_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An object body (not a JSON array) on the first page yields None."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout=_build_include_response('{"unexpected": true}', etag='W/"v1"'))

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100") is None


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

    items = cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100")
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

    items = cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100")
    assert items == [{"n": i} for i in range(100)]


def test_etag_get_paginated_first_page_runtime_error_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A first-page hard ``gh`` failure must propagate so callers can react."""

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stderr="boom", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="boom"):
        cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100")


def test_etag_get_paginated_later_page_runtime_error_breaks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later-page hard failure leaves earlier items intact."""
    page1_body = "[" + ",".join(f'{{"n": {i}}}' for i in range(100)) + "]"
    state = {"calls": 0}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        state["calls"] += 1
        if state["calls"] == 1:
            return _FakeCompletedProcess(stdout=_build_include_response(page1_body, etag='W/"p1"'))
        return _FakeCompletedProcess(stderr="transient", returncode=1)

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("src.retry.is_transient_error", lambda exc: False)

    items = cache._etag_get_paginated("repos/owner/name/pulls?state=open&per_page=100")
    assert items == [{"n": i} for i in range(100)]


def test_gh_api_paginated_routes_pulls_list_through_etag_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_gh_api_paginated`` must dispatch top-level pulls list paths to ``_etag_get_paginated``."""
    routed: list[str] = []

    monkeypatch.setattr(
        "src.github.cache._etag_get_paginated",
        lambda path: routed.append(path) or [{"n": 1}],
    )

    result = cache._gh_api_paginated("repos/owner/name/pulls?state=open&per_page=100")

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

    monkeypatch.setattr("src.github.cache._etag_get_paginated", fake_etag_helper)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda args: [[{"id": 1}], [{"id": 2}]],
    )

    result = cache._gh_api_paginated("repos/owner/name/issues/42/comments")

    assert result == [{"id": 1}, {"id": 2}]
    assert routed == []


def test_invalidate_etag_cache_drops_matching_prefixes_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_invalidate_etag_cache`` must remove only the prefix-matching entries."""
    cache._etag_cache_put(
        "repos/owner/name/pulls?state=open&per_page=100&page=1",
        'W/"a"',
        [{"n": 1}],
    )
    cache._etag_cache_put(
        "repos/owner/name/pulls?state=closed&page=1",
        'W/"b"',
        [{"n": 2}],
    )
    cache._etag_cache_put(
        "repos/owner/name/issues/42/comments",
        'W/"c"',
        [{"id": 3}],
    )

    cache._invalidate_etag_cache("repos/owner/name/pulls")

    assert "repos/owner/name/issues/42/comments" in cache._etag_cache
    assert not any(key.startswith("repos/owner/name/pulls") for key in cache._etag_cache)


def test_invalidate_etag_cache_no_op_when_prefix_absent() -> None:
    """A prefix that matches nothing must leave the cache untouched."""
    cache._etag_cache_put(
        "repos/owner/name/pulls?state=open&page=1",
        'W/"a"',
        [{"n": 1}],
    )

    cache._invalidate_etag_cache("repos/different/repo/pulls")

    assert "repos/owner/name/pulls?state=open&page=1" in cache._etag_cache


def test_merge_pr_invalidates_pulls_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful ``merge_pr`` drops cached ``repos/{repo}/pulls`` entries."""
    cache._etag_cache_put(
        "repos/owner/name/pulls?state=open&per_page=100&page=1",
        'W/"a"',
        [{"n": 1}],
    )

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    prs.merge_pr("owner/name", 42, "c" * 40)

    assert "repos/owner/name/pulls?state=open&per_page=100&page=1" not in cache._etag_cache


def test_get_pr_metadata_extracts_nested_user_and_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production payload: nested ``.user.login`` and ``.head.sha`` are extracted."""
    pr_body = '{"user": {"login": "alice"}, "head": {"sha": "abc123"}}'
    commit_body = '{"commit": {"committer": {"date": "2026-04-15T12:00:00Z"}}}'

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        path = next((a for a in cmd if a.startswith("repos/")), "")
        if "/pulls/" in path:
            return _FakeCompletedProcess(stdout=_build_include_response(pr_body, etag='W/"p1"'))
        if "/commits/" in path:
            return _FakeCompletedProcess(stdout=_build_include_response(commit_body, etag='W/"c1"'))
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert get_pr_metadata("owner/name", 42) == {
        "author": "alice",
        "head_sha": "abc123",
        "head_commit_date": "2026-04-15T12:00:00Z",
    }
