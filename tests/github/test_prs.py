from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.github import prs as gh_prs
from src.models import ReviewStatus


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "", returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode


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
        lambda repo, number, pr_author, head_sha: ReviewStatus.PENDING,
    )

    [pr] = gh_prs.get_open_prs("octo/demo")

    assert pr.number == 7
    assert pr.quarantine_labels == {"quarantine:large_diff"}
