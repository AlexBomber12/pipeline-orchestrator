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


def test_empty_sha_is_noop_until_polling_catches_up() -> None:
    """Rev-parse failure after a real push must not double-count.

    The diagnose-error auto-fix path can call
    ``record_observed_head("")`` when ``git rev-parse HEAD`` fails
    after a successful push (timeout, ``OSError``, non-zero exit).
    Bumping ``push_count`` here would double-count: on the next poll,
    ``merge_observed_pushes`` sees the real head SHA as a new
    observation and would increment ``push_count`` a second time for
    the same real push. The fix is to make the empty-SHA case a
    deliberate no-op and rely on the polling merge to count the push
    exactly once when the real SHA is observed.
    """
    pr = PRInfo(number=4, branch="pr-004")
    pr.record_observed_head("first-sha")
    assert pr.push_count == 1

    pr.record_observed_head("")

    assert pr.observed_head_shas == {"first-sha"}
    assert pr.push_count == 1

    pr.record_observed_head("")

    assert pr.observed_head_shas == {"first-sha"}
    assert pr.push_count == 1


def test_empty_sha_then_polled_real_sha_counts_push_exactly_once() -> None:
    """Regression: empty-SHA fallback + polling merge previously double-counted.

    The diagnose-error auto-fix path pushes, then the post-push
    ``git rev-parse HEAD`` fails and the daemon calls
    ``record_observed_head("")``. On the next WATCH/IDLE refresh,
    ``get_open_prs`` returns the real head SHA and
    ``merge_observed_pushes`` is called. The previous implementation
    bumped ``push_count`` from both sites, counting one real push
    twice. After the fix the empty-SHA case is a no-op and the polling
    merge alone counts the push once.
    """
    persisted = PRInfo(number=10, branch="pr-010")
    persisted.record_observed_head("")
    assert persisted.push_count == 0
    assert persisted.observed_head_shas == set()

    polled = PRInfo(
        number=10,
        branch="pr-010",
        push_count=1,
        observed_head_shas={"real-head-sha"},
    )

    merged_shas, push_count = persisted.merge_observed_pushes(polled)

    assert merged_shas == {"real-head-sha"}
    assert push_count == 1


def test_upgrade_from_pre_pr195_state_counts_new_pushes() -> None:
    """Pre-PR-195 PRInfo (push_count > 0, empty SHA set) must keep counting.

    Persisted dashboards that pre-date PR-195 carry ``push_count`` from
    the legacy ``+= 1`` accounting but ``observed_head_shas`` defaults
    to an empty set after the schema bump. The first new push records a
    fresh SHA whose set cardinality (1) is below the legacy
    ``push_count``; the previous ``max(len, push_count)`` formula left
    the counter frozen for many real pushes. Each new SHA must bump
    ``push_count`` by 1 from its current value so the dashboard
    ``Pushes`` metric stays in sync after upgrade.
    """
    pr = PRInfo(
        number=9,
        branch="pr-009",
        push_count=5,
        observed_head_shas=set(),
    )

    pr.record_observed_head("post-upgrade-sha-1")
    assert pr.push_count == 6
    assert pr.observed_head_shas == {"post-upgrade-sha-1"}

    pr.record_observed_head("post-upgrade-sha-2")
    assert pr.push_count == 7
    assert pr.observed_head_shas == {
        "post-upgrade-sha-1",
        "post-upgrade-sha-2",
    }

    pr.record_observed_head("post-upgrade-sha-2")
    assert pr.push_count == 7

    pr.record_observed_head("")
    assert pr.push_count == 7


def test_merge_observed_pushes_counts_new_sha_against_legacy_count() -> None:
    """Pre-PR-195 ``self`` (legacy ``push_count``, empty SHA set) merging
    a freshly polled PRInfo (single new SHA, ``push_count=1``) must bump
    the counter — the previous ``max(len, push_count)`` formula froze at
    the legacy value until the SHA set caught up.
    """
    persisted = PRInfo(
        number=11,
        branch="pr-011",
        push_count=5,
        observed_head_shas=set(),
    )
    polled = PRInfo(
        number=11,
        branch="pr-011",
        push_count=1,
        observed_head_shas={"freshly-polled-sha"},
    )

    merged_shas, push_count = persisted.merge_observed_pushes(polled)

    assert merged_shas == {"freshly-polled-sha"}
    assert push_count == 6


def test_merge_observed_pushes_repeated_sha_does_not_inflate_count() -> None:
    persisted = PRInfo(
        number=12,
        branch="pr-012",
        push_count=3,
        observed_head_shas={"sha-a", "sha-b", "sha-c"},
    )
    polled = PRInfo(
        number=12,
        branch="pr-012",
        push_count=1,
        observed_head_shas={"sha-a"},
    )

    merged_shas, push_count = persisted.merge_observed_pushes(polled)

    assert merged_shas == {"sha-a", "sha-b", "sha-c"}
    assert push_count == 3


def test_merge_observed_pushes_with_no_new_observation_keeps_count() -> None:
    persisted = PRInfo(
        number=13,
        branch="pr-013",
        push_count=4,
        observed_head_shas={"sha-a", "sha-b"},
    )
    polled = PRInfo(
        number=13,
        branch="pr-013",
        push_count=0,
        observed_head_shas=set(),
    )

    merged_shas, push_count = persisted.merge_observed_pushes(polled)

    assert merged_shas == {"sha-a", "sha-b"}
    assert push_count == 4


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
