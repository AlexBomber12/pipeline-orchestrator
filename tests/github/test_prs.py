from __future__ import annotations

import json
import subprocess
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.config import AppConfig, DaemonConfig, TrustedReviewerIdentity
from src.github import cache
from src.github import prs as gh_prs
from src.github.reviewer_policy import ReviewerPolicy
from src.models import ReviewStatus


@pytest.mark.parametrize(
    ("state", "merged", "merge_sha", "expected"),
    [
        ("open", False, None, gh_prs.ExactPRState.OPEN),
        ("closed", False, None, gh_prs.ExactPRState.CLOSED),
        ("closed", True, "b" * 40, gh_prs.ExactPRState.MERGED),
        ("closed", True, None, gh_prs.ExactPRState.UNKNOWN),
    ],
)
def test_exact_pr_outcome_by_identity(
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    merged: bool,
    merge_sha: str | None,
    expected: gh_prs.ExactPRState,
) -> None:
    payload = {
        "number": 42,
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
        "head": {"sha": "a" * 40},
        "state": state,
        "merged": merged,
        "merge_commit_sha": merge_sha,
        "merged_at": "2020-01-31T23:59:00Z",
    }
    monkeypatch.setattr(cache, "_etag_get", lambda path: payload)
    result = gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "a" * 40)
    assert result.merged_at == (datetime(2020, 1, 31, 23, 59, tzinfo=timezone.utc)
                                if expected == gh_prs.ExactPRState.MERGED else None)
    assert result.state == expected
    assert result.discovery_complete is (expected != gh_prs.ExactPRState.UNKNOWN)
    assert result.merge_commit_sha == (merge_sha if expected == gh_prs.ExactPRState.MERGED else None)
    payload["base"] = {"ref": "release"}
    assert gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "a" * 40).state == gh_prs.ExactPRState.UNKNOWN


def test_exact_pr_outcome_permission_error_and_stale_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    def denied(path: str) -> object:
        raise RuntimeError("HTTP 403")

    monkeypatch.setattr(cache, "_etag_get", denied)
    assert gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "a" * 40).state == gh_prs.ExactPRState.UNKNOWN
    monkeypatch.setattr(
        cache,
        "_etag_get",
        lambda path: {
            "number": 42,
            "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
            "head": {"sha": "a" * 40},
            "state": "closed",
            "merged": False,
        },
    )
    monkeypatch.setattr(
        cache, "_etag_payload_observed_at", lambda path, payload: datetime(2020, 1, 1, tzinfo=timezone.utc)
    )
    assert gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "a" * 40).state == gh_prs.ExactPRState.UNKNOWN


def test_merged_discovery_rejects_truncated_and_malformed_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    path = "repos/octo/demo/pulls?state=closed&base=main&per_page=100"
    calls: list[str] = []

    def page(url: str) -> object:
        calls.append(url)
        if url.endswith("page=1"):
            return [{"number": n} for n in range(100)]
        raise RuntimeError("HTTP 403")

    monkeypatch.setattr(cache, "_etag_get", page)
    result = gh_prs.get_merged_prs_discovery("octo/demo", "main")
    assert result.complete is False and result.prs == []
    assert calls == [f"{path}&page=1", f"{path}&page=2"]
    monkeypatch.setattr(cache, "_etag_get", lambda url: [{"number": 1, "merged_at": "2026-01-01"}])
    assert gh_prs.get_merged_prs_discovery("octo/demo", "main").complete is False


def test_merged_discovery_accepts_complete_identity_and_rejects_stale_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = [{
        "number": 42, "title": "PR-42: done", "merged_at": "2026-10-01T00:00:00Z",
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}}, "head": {"ref": "pr-42-done", "sha": "a" * 40},
        "merge_commit_sha": "b" * 40,
    }]
    monkeypatch.setattr(cache, "_etag_get", lambda path: payload)
    result = gh_prs.get_merged_prs_discovery("octo/demo", "main")
    assert result.complete is True
    assert (result.repo, result.base_branch) == ("octo/demo", "main")
    assert [(pr.number, pr.head_sha, pr.merge_commit_sha) for pr in result.prs] == [
        (42, "a" * 40, "b" * 40)
    ]
    monkeypatch.setattr(
        cache, "_etag_payload_observed_at",
        lambda path, payload: datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    stale = gh_prs.get_merged_prs_discovery("octo/demo", "main")
    assert stale.complete is False and stale.reason == "stale cache"


def test_unknown_exact_payload_and_invalid_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cache, "_etag_get", lambda path: "bad")
    assert gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "a" * 40).state == gh_prs.ExactPRState.UNKNOWN
    assert gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "short").reason == "invalid identity"


@pytest.mark.parametrize("change", ["base", "head"])
def test_merged_discovery_rejects_identity_mismatch(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    payload = {
        "number": 42, "title": "PR-42: done", "merged_at": "2026-10-01T00:00:00Z",
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}}, "head": {"ref": "pr-42-done", "sha": "a" * 40},
        "merge_commit_sha": "b" * 40,
    }
    if change == "base":
        payload["base"] = {"ref": "release"}
    else:
        payload["head"] = {"ref": "pr-42-done", "sha": "short"}
    monkeypatch.setattr(cache, "_etag_get", lambda path: [payload])
    assert gh_prs.get_merged_prs_discovery("octo/demo", "main").complete is False


def test_merged_discovery_rejects_non_mapping_page_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cache, "_etag_get", lambda path: ["bad"])
    assert gh_prs.get_merged_prs_discovery("octo/demo", "main").complete is False


@pytest.mark.parametrize("number", [0, -1])
@pytest.mark.parametrize("valid_first", [False, True])
def test_merged_discovery_rejects_nonpositive_pr_number(
    monkeypatch: pytest.MonkeyPatch, number: int, valid_first: bool,
) -> None:
    valid = {
        "number": 42, "title": "PR-42: done", "merged_at": "2026-10-01T00:00:00Z",
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}}, "head": {"ref": "pr-42-done", "sha": "a" * 40},
        "merge_commit_sha": "b" * 40,
    }
    payload = ([valid] if valid_first else []) + [{**valid, "number": number}]
    monkeypatch.setattr(cache, "_etag_get", lambda path: payload)
    result = gh_prs.get_merged_prs_discovery("octo/demo", "main")
    assert result.complete is False
    assert result.prs == []
    assert result.reason == "malformed entry"


@pytest.mark.parametrize("merged_at", ["invalid", 123, True, [], {}])
def test_merged_discovery_rejects_malformed_merge_timestamp(
    monkeypatch: pytest.MonkeyPatch, merged_at: object,
) -> None:
    valid = {
        "number": 42, "title": "PR-42: done", "merged_at": "2026-10-01T00:00:00Z",
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}}, "head": {"ref": "pr-42-done", "sha": "a" * 40},
        "merge_commit_sha": "b" * 40,
    }
    monkeypatch.setattr(cache, "_etag_get", lambda path: [valid, {**valid, "merged_at": merged_at}])
    result = gh_prs.get_merged_prs_discovery("octo/demo", "main")
    assert result.complete is False
    assert result.prs == []
    assert result.reason == "malformed entry"
    # Compatibility callers retain their permissive timestamp behavior.
    assert gh_prs._merged_pr_infos([{**valid, "merged_at": merged_at}], "main")[0].last_activity is None


@pytest.mark.parametrize("stderr_304", [False, True])
@pytest.mark.parametrize("reader", ["exact", "discovery"])
def test_pr_readers_accept_successful_revalidation_after_ttl(
    monkeypatch: pytest.MonkeyPatch, reader: str, stderr_304: bool,
) -> None:
    clock = datetime(2026, 10, 1, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> datetime:
            return clock

    monkeypatch.setattr(cache, "datetime", Clock)
    monkeypatch.setattr(gh_prs, "datetime", Clock)
    monkeypatch.setattr(cache, "_etag_cache", OrderedDict())
    payload = {
        "number": 42, "title": "PR-42: done", "merged_at": "2026-10-01T00:00:00Z",
        "base": {"ref": "main", "repo": {"full_name": "octo/demo"}}, "head": {"ref": "pr-42-done", "sha": "a" * 40},
        "merge_commit_sha": "b" * 40, "state": "closed", "merged": True,
    }
    calls: list[list[str]] = []

    def transport(args: list[str]) -> str:
        calls.append(args)
        if any("If-None-Match" in arg for arg in args):
            if stderr_304:
                raise RuntimeError("gh: HTTP 304")
            return "HTTP/2.0 304 Not Modified\r\n\r\n"
        # Discovery walks a full first page and a short second page.
        body = payload if reader == "exact" else ([payload] * 100 if args[1].endswith("page=1") else [])
        return 'HTTP/2.0 200 OK\r\nETag: "v1"\r\n\r\n' + json.dumps(body)

    monkeypatch.setattr(cache.gh_runner, "run_gh", transport)
    for _ in range(3):
        if reader == "exact":
            result = gh_prs.get_exact_pr_outcome("octo/demo", "main", 42, "a" * 40)
            assert result.state == gh_prs.ExactPRState.MERGED
            assert result.merge_commit_sha == "b" * 40
        else:
            discovery = gh_prs.get_merged_prs_discovery("octo/demo", "main")
            assert discovery.complete is True
            assert len(discovery.prs) == 100
            assert all(pr.number == 42 and pr.merge_commit_sha == "b" * 40 for pr in discovery.prs)
        assert all(entry.observed_at == clock for entry in cache._etag_cache.values())
        clock += timedelta(seconds=61)
    page_count = 1 if reader == "exact" else 2
    assert len(calls) == 3 * page_count
    assert all('If-None-Match: "v1"' in args for args in calls[page_count:])


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def test_get_branch_publications_uses_targeted_bounded_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], str | None, int]] = []
    sha = "a" * 40

    def fake_run_gh(args: list[str], repo: str | None = None, timeout: int = 30) -> list[object]:
        calls.append((args, repo, timeout))
        return [
            {
                "number": 42,
                "title": "PR-393: publication handoff",
                "baseRefName": "main",
                "headRefName": "feat/publication",
                "headRefOid": sha,
                "state": "OPEN",
                "isDraft": False,
                "isCrossRepository": False,
                "createdAt": "2026-10-04T12:00:00Z",
                "url": "https://github.com/octo/demo/pull/42",
            },
            {
                "number": 43,
                "baseRefName": "release",
                "headRefName": "feat/publication",
            },
            "malformed",
            {
                "number": True,
                "baseRefName": "main",
                "headRefName": "feat/publication",
            },
        ]

    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", fake_run_gh)

    [publication] = gh_prs.get_branch_publications("octo/demo", "main", "feat/publication")

    assert publication.is_verified_ready is True
    assert publication.created_at == datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
    assert publication.to_pr_info().head_sha == sha
    args, repo, timeout = calls[0]
    assert repo == "octo/demo"
    assert timeout == 10
    assert args[:8] == [
        "pr",
        "list",
        "--state",
        "all",
        "--base",
        "main",
        "--head",
        "feat/publication",
    ]
    assert "checks" not in args[-1]
    assert "reviewDecision" not in args[-1]


def test_get_branch_publications_rejects_non_list_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", lambda *a, **kw: "bad")

    assert gh_prs.get_branch_publications("octo/demo", "main", "feat/x") == []


@pytest.mark.parametrize(
    ("overrides", "verified"),
    [
        ({"state": "CLOSED"}, False),
        ({"isDraft": True}, False),
        ({"isCrossRepository": True}, False),
        ({"headRefOid": "short"}, False),
        ({"createdAt": "not-a-date"}, False),
        ({}, True),
    ],
)
def test_branch_publication_requires_verifiable_ready_evidence(overrides: dict[str, object], verified: bool) -> None:
    values: dict[str, object] = {
        "number": 7,
        "title": "PR-007: ready",
        "base_branch": "main",
        "head_branch": "feat/ready",
        "head_sha": "b" * 40,
        "state": "OPEN",
        "is_draft": False,
        "is_cross_repository": False,
        "created_at": datetime(2026, 10, 4, tzinfo=timezone.utc),
        "url": "https://github.com/octo/demo/pull/7",
    }
    key_map = {
        "isDraft": "is_draft",
        "isCrossRepository": "is_cross_repository",
        "headRefOid": "head_sha",
        "createdAt": "created_at",
    }
    for key, value in overrides.items():
        values[key_map.get(key, key)] = value
    publication = gh_prs.BranchPublication(**values)  # type: ignore[arg-type]

    assert publication.is_verified_ready is verified


def test_get_pr_diff_invokes_gh_cli_with_pr_number_and_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-290a: the helper must shell out to ``gh pr diff <num> --repo``
    so the diff text reaches the dispatcher in the same unified-diff
    shape ``git diff`` would emit locally."""
    calls: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append((list(cmd), kwargs))
        return _FakeCompletedProcess(stdout="diff --git a/x b/x\n+y\n")

    monkeypatch.setattr(gh_prs.subprocess, "run", fake_run)

    out = gh_prs.get_pr_diff("octo/demo", 42)

    assert out == "diff --git a/x b/x\n+y\n"
    assert calls == [
        (
            ["gh", "pr", "diff", "42", "--repo", "octo/demo"],
            {"capture_output": True, "text": True, "check": True, "timeout": 30},
        )
    ]


def test_get_pr_diff_propagates_subprocess_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-zero exit from ``gh`` must raise so the WATCH wrapper can
    leave ``diff_scanned_at_sha`` unchanged and retry on the next
    cycle."""

    def boom(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd)

    monkeypatch.setattr(gh_prs.subprocess, "run", boom)

    with pytest.raises(subprocess.CalledProcessError):
        gh_prs.get_pr_diff("octo/demo", 7)


def test_get_open_prs_preserves_quarantine_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        gh_prs.gh_runner,
        "run_gh",
        lambda cmd, **kwargs: [
            {
                "number": 7,
                "title": "PR-007: guarded",
                "headRefName": "pr-007-guarded",
                "headRefOid": "abc123",
                "url": "https://github.com/octo/demo/pull/7",
                "updatedAt": "2026-05-21T00:00:00Z",
                "commits": [{"oid": "abc123"}],
                "author": {"login": "alice"},
                "isCrossRepository": False,
                "labels": [
                    {"name": "quarantine:large_diff"},
                    {"name": "needs-review"},
                ],
            }
        ],
    )
    monkeypatch.setattr(
        gh_prs.checks,
        "_retrieve_ci_status_evidence",
        lambda repo, sha: object(),
    )
    monkeypatch.setattr(
        gh_prs.checks,
        "_classify_ci_retrieval",
        lambda *args, **kwargs: gh_prs.checks.CIStatus.PENDING,
    )
    monkeypatch.setattr(
        gh_prs.reviews,
        "get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None, publication_evidence=None: ReviewStatus.PENDING,
    )

    [pr] = gh_prs.get_open_prs("octo/demo")

    assert pr.number == 7
    assert pr.quarantine_labels == {"quarantine:large_diff"}


def test_get_open_prs_snapshots_reviewer_policy_once_per_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_loads = 0
    observed_policies: list[ReviewerPolicy | None] = []

    def fake_load_config() -> AppConfig:
        nonlocal config_loads
        config_loads += 1
        return AppConfig(
            daemon=DaemonConfig(trusted_reviewer_identities=[TrustedReviewerIdentity(user_id=123, login="codex[bot]")])
        )

    monkeypatch.setattr(gh_prs, "load_config", fake_load_config)
    monkeypatch.setattr(
        gh_prs.gh_runner,
        "run_gh",
        lambda cmd, **kwargs: [
            {
                "number": 7,
                "title": "PR-007: first",
                "headRefName": "pr-007-first",
                "headRefOid": "abc123",
                "url": "https://github.com/octo/demo/pull/7",
                "updatedAt": "2026-05-21T00:00:00Z",
                "commits": [{"oid": "abc123"}],
                "author": {"login": "alice"},
                "isCrossRepository": False,
                "labels": [],
            },
            {
                "number": 8,
                "title": "PR-008: second",
                "headRefName": "pr-008-second",
                "headRefOid": "def456",
                "url": "https://github.com/octo/demo/pull/8",
                "updatedAt": "2026-05-21T00:00:01Z",
                "commits": [{"oid": "def456"}],
                "author": {"login": "alice"},
                "isCrossRepository": False,
                "labels": [],
            },
        ],
    )
    monkeypatch.setattr(
        gh_prs.checks,
        "_retrieve_ci_status_evidence",
        lambda repo, sha: object(),
    )
    monkeypatch.setattr(
        gh_prs.checks,
        "_classify_ci_retrieval",
        lambda *args, **kwargs: gh_prs.checks.CIStatus.PENDING,
    )

    def fake_review_status(
        repo: str,
        number: int,
        pr_author: str = "",
        head_sha: str = "",
        policy: ReviewerPolicy | None = None,
        publication_evidence: object | None = None,
    ) -> ReviewStatus:
        observed_policies.append(policy)
        return ReviewStatus.PENDING

    monkeypatch.setattr(
        gh_prs.reviews,
        "get_pr_review_status",
        fake_review_status,
    )

    prs = gh_prs.get_open_prs("octo/demo")

    assert [pr.number for pr in prs] == [7, 8]
    assert config_loads == 1
    assert len(observed_policies) == 2
    assert observed_policies[0] is observed_policies[1]
    assert observed_policies[0] is not None


@pytest.mark.parametrize("case,expected", [
    ("none", False), ("auto", True), ("queue", True), ("error", None),
    ("missing", None), ("head", None), ("transport", None), ("null", None),
])
def test_merge_admission_is_exact_and_fail_closed(monkeypatch, case, expected):
    pr = {"headRefOid": "a" * 40, "autoMergeRequest": None, "mergeQueueEntry": None}
    result = {"data": {"repository": {"pullRequest": pr}}}
    if case in {"auto", "queue"}:
        pr["autoMergeRequest" if case == "auto" else "mergeQueueEntry"] = {"id": "queued"}
    if case == "error":
        result["errors"] = [{"message": "partial result"}]
    if case == "missing":
        del pr["mergeQueueEntry"]
    if case == "head":
        pr["headRefOid"] = "b" * 40
    if case == "null":
        result = None

    def transport(args, **kwargs):
        assert args[:2] == ["api", "graphql"]
        assert kwargs == {}  # gh api has no -R flag; identity is in query variables.
        assert "owner=octo" in args and "name=demo" in args
        assert "number=42" in args
        if case == "transport":
            raise RuntimeError("offline")
        return result

    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", transport)
    assert gh_prs.get_pr_merge_admission("octo/demo", 42, "a" * 40) is expected
