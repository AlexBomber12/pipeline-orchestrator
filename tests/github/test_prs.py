from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from typing import Any

import pytest
from src.config import AppConfig, DaemonConfig, TrustedReviewerIdentity
from src.github import prs as gh_prs
from src.github.reviewer_policy import ReviewerPolicy
from src.models import ReviewStatus


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


def test_get_branch_publications_uses_targeted_bounded_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], str | None, int]] = []
    sha = "a" * 40

    def fake_run_gh(
        args: list[str], repo: str | None = None, timeout: int = 30
    ) -> list[object]:
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

    [publication] = gh_prs.get_branch_publications(
        "octo/demo", "main", "feat/publication"
    )

    assert publication.is_verified_ready is True
    assert publication.created_at == datetime(
        2026, 10, 4, 12, tzinfo=timezone.utc
    )
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
def test_branch_publication_requires_verifiable_ready_evidence(
    overrides: dict[str, object], verified: bool
) -> None:
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
        "_fetch_ci_status_rest",
        lambda repo, sha: ([], [], True),
    )
    monkeypatch.setattr(
        gh_prs.reviews,
        "get_pr_review_status",
        lambda repo, number, pr_author, head_sha, policy=None: ReviewStatus.PENDING,
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
            daemon=DaemonConfig(
                trusted_reviewer_identities=[
                    TrustedReviewerIdentity(user_id=123, login="codex[bot]")
                ]
            )
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
        "_fetch_ci_status_rest",
        lambda repo, sha: ([], [], True),
    )

    def fake_review_status(
        repo: str,
        number: int,
        pr_author: str = "",
        head_sha: str = "",
        policy: ReviewerPolicy | None = None,
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
