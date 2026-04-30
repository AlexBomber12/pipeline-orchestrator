"""Tests for the daemon-side observed head SHA accounting (PR-195).

Covers the four scenarios called out in tasks/PR-195.md:

1. Single push lands → ``push_count == 1``.
2. Five distinct force-pushes → ``push_count == 5``.
3. Cross-restart persistence (Redis-backed via ``RepoState`` JSON
   round-trip) preserves the observed-SHA set.
4. ``github_client.get_open_prs`` populates the set with the API's
   current head SHA on the initial fetch.
"""

from __future__ import annotations

from typing import Any

import pytest
from src.github_client import get_open_prs
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    RepoState,
    ReviewStatus,
)


def test_single_push_records_head_sha() -> None:
    pr = PRInfo(number=1, branch="pr-001")
    pr.record_observed_head("aaa111")

    assert pr.observed_head_shas == {"aaa111"}
    assert pr.push_count == 1


def test_five_distinct_force_pushes_count_as_five() -> None:
    pr = PRInfo(number=2, branch="pr-002")
    for sha in ("a1", "b2", "c3", "d4", "e5"):
        pr.record_observed_head(sha)

    assert pr.observed_head_shas == {"a1", "b2", "c3", "d4", "e5"}
    assert pr.push_count == 5


def test_repeated_same_sha_does_not_inflate_count() -> None:
    pr = PRInfo(number=3, branch="pr-003")
    pr.record_observed_head("same-sha")
    pr.record_observed_head("same-sha")
    pr.record_observed_head("same-sha")

    assert pr.observed_head_shas == {"same-sha"}
    assert pr.push_count == 1


def test_empty_sha_after_known_push_still_increments_count() -> None:
    """Rev-parse failure after a real push must not drop the increment.

    Regression for the diagnose-error auto-fix path: after a successful
    push, ``git rev-parse HEAD`` can intermittently fail (timeout,
    ``OSError``, non-zero exit) and the daemon then calls
    ``record_observed_head("")``. The set's cardinality is unchanged,
    so the legacy ``push_count += 1`` must still fire.
    """
    pr = PRInfo(number=4, branch="pr-004")
    pr.record_observed_head("first-sha")
    assert pr.push_count == 1

    pr.record_observed_head("")

    assert pr.observed_head_shas == {"first-sha"}
    assert pr.push_count == 2

    pr.record_observed_head("")

    assert pr.observed_head_shas == {"first-sha"}
    assert pr.push_count == 3


def test_cross_restart_persistence_preserves_observed_set() -> None:
    """RepoState round-trips through Redis as JSON; the SHA set must survive."""
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="repo",
        state=PipelineState.WATCH,
        current_pr=PRInfo(
            number=42,
            branch="pr-042",
            observed_head_shas={"sha-a", "sha-b", "sha-c"},
            push_count=3,
        ),
    )

    payload = state.model_dump_json()
    restored = RepoState.model_validate_json(payload)

    assert restored.current_pr is not None
    assert restored.current_pr.observed_head_shas == {"sha-a", "sha-b", "sha-c"}
    assert restored.current_pr.push_count == 3


def test_get_open_prs_populates_observed_head_shas_with_current_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = [
        {
            "number": 7,
            "title": "PR-195: foo",
            "headRefName": "pr-195",
            "headRefOid": "current-head-sha",
            "url": "https://example.test/pr/7",
            "updatedAt": "2026-04-30T00:00:00Z",
            "commits": [{}, {}, {}],
            "author": {"login": "alice"},
            "labels": [],
            "isCrossRepository": False,
        },
    ]

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        if args and args[0] == "pr":
            return raw
        raise AssertionError(f"unexpected run_gh call: {args}")

    monkeypatch.setattr("src.github_client.run_gh", fake_run_gh)
    monkeypatch.setattr(
        "src.github_client._fetch_ci_status_rest",
        lambda repo, sha: ([], {}, True),
    )
    monkeypatch.setattr(
        "src.github_client.get_pr_review_status",
        lambda repo, number, pr_author, head_sha: ReviewStatus.PENDING,
    )

    prs = get_open_prs("owner/name", allow_merge_without_checks=True)

    assert len(prs) == 1
    assert prs[0].observed_head_shas == {"current-head-sha"}
    assert prs[0].push_count == 1
    assert prs[0].ci_status == CIStatus.SUCCESS
