from __future__ import annotations

from typing import Any

import pytest

from src.github import GhPrMergedBranchesUnavailable, gh_pr_get_merged_branches


def test_returns_subset_for_mixed_state_input(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> list[dict[str, object]]:
        return [
            {
                "number": 1,
                "headRefName": "feature/one",
                "mergedAt": "2026-05-01T12:00:00Z",
            },
            {"number": 2, "headRefName": "feature-two", "mergedAt": None},
            {
                "number": 3,
                "headRefName": "bugfix.three",
                "mergedAt": "2026-05-01T13:00:00Z",
            },
        ]

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches(
        "owner/name",
        ["feature/one", "feature-two", "bugfix.three"],
    ) == {"feature/one", "bugfix.three"}


def test_empty_branches_skip_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_run_gh(args: list[str], **kwargs: Any) -> None:
        raise AssertionError("run_gh should not be called for empty input")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_run_gh)

    assert gh_pr_get_merged_branches("owner/name", []) == set()


def test_chunks_long_branch_lists_into_expected_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    searches: list[str] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> list[dict[str, object]]:
        searches.append(args[args.index("--search") + 1])
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    branches = [f"branch-{index:02d}" for index in range(50)]

    assert gh_pr_get_merged_branches("owner/name", branches) == set()
    assert [search.count("head:") for search in searches] == [20, 20, 10]
    assert [search.count(" OR ") for search in searches] == [19, 19, 9]
    assert searches[0].split()[0] == "head:branch-00"
    assert searches[2].split()[-1] == "head:branch-49"


def test_raises_unavailable_with_original_runtime_error_chained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = RuntimeError("stderr: GraphQL API rate limit exceeded")

    def fake_run_gh(args: list[str], **kwargs: Any) -> None:
        raise original

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    with pytest.raises(
        GhPrMergedBranchesUnavailable,
        match="GraphQL API rate limit exceeded",
    ) as exc_info:
        gh_pr_get_merged_branches("owner/name", ["feature/one"])
    assert exc_info.value.__cause__ is original


@pytest.mark.parametrize("branch", ["bad branch", "bad;branch", "$(rm)"])
def test_rejects_unsafe_branch_names_before_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    branch: str,
) -> None:
    def fail_run_gh(args: list[str], **kwargs: Any) -> None:
        raise AssertionError("run_gh should not be called for invalid input")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_run_gh)

    with pytest.raises(ValueError, match="Invalid branch name"):
        gh_pr_get_merged_branches("owner/name", ["safe-branch", branch])


def test_search_query_and_run_gh_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_gh(args: list[str], **kwargs: Any) -> list[dict[str, object]]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches("owner/name", ["feat/a", "bug-b"]) == set()

    args = captured["args"]
    assert isinstance(args, list)
    assert args[:2] == ["pr", "list"]
    assert args[args.index("--state") + 1] == "merged"
    assert args[args.index("--search") + 1] == "head:feat/a OR head:bug-b"
    assert args[args.index("--json") + 1] == "number,headRefName,mergedAt"
    assert args[args.index("--limit") + 1] == "40"
    assert captured["kwargs"] == {"repo": "owner/name"}


def test_excludes_closed_not_merged_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> list[dict[str, object]]:
        return [{"number": 9, "headRefName": "closed-not-merged", "mergedAt": None}]

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches("owner/name", ["closed-not-merged"]) == set()
