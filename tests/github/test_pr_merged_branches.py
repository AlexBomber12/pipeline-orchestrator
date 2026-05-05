from __future__ import annotations

from typing import Any

import pytest

from src.github import GhPrMergedBranchesUnavailable, gh_pr_get_merged_branches


def test_returns_subset_for_mixed_state_input(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, object]:
        return {
            "data": {
                "repository": {
                    "b0": {
                        "nodes": [
                            {
                                "headRefName": "feature/one",
                                "mergedAt": "2026-05-01T12:00:00Z",
                            }
                        ]
                    },
                    "b1": {"nodes": [{"headRefName": "feature-two", "mergedAt": None}]},
                    "b2": {
                        "nodes": [
                            {
                                "headRefName": "bugfix.three",
                                "mergedAt": "2026-05-01T13:00:00Z",
                            }
                        ]
                    },
                }
            }
        }

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
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, object]:
        calls.append(args)
        return {"data": {"repository": {}}}

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    branches = [f"branch-{index:02d}" for index in range(50)]

    assert gh_pr_get_merged_branches("owner/name", branches) == set()
    branch_vars = [
        [arg for arg in call if arg.startswith("branch")]
        for call in calls
    ]
    assert [len(args) for args in branch_vars] == [20, 20, 10]
    assert branch_vars[0][0] == "branch0=branch-00"
    assert branch_vars[2][-1] == "branch9=branch-49"


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


@pytest.mark.parametrize(
    "branch",
    ["bad branch", "-bad", "bad..branch", "bad.lock"],
)
def test_rejects_invalid_branch_names_before_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    branch: str,
) -> None:
    def fail_run_gh(args: list[str], **kwargs: Any) -> None:
        raise AssertionError("run_gh should not be called for invalid input")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_run_gh)

    with pytest.raises(ValueError, match="Invalid branch name"):
        gh_pr_get_merged_branches("owner/name", ["safe-branch", branch])


def test_accepts_valid_git_branch_names_with_metacharacters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, object]:
        captured["args"] = args
        return {
            "data": {
                "repository": {
                    "b0": {
                        "nodes": [
                            {
                                "headRefName": "feat/$user",
                                "mergedAt": "2026-05-01T12:00:00Z",
                            }
                        ]
                    },
                    "b1": {
                        "nodes": [
                            {
                                "headRefName": "feat+one",
                                "mergedAt": "2026-05-01T13:00:00Z",
                            }
                        ]
                    },
                    "b2": {
                        "nodes": [
                            {
                                "headRefName": "feat;two",
                                "mergedAt": "2026-05-01T14:00:00Z",
                            }
                        ]
                    },
                    "b3": {
                        "nodes": [
                            {
                                "headRefName": "@",
                                "mergedAt": "2026-05-01T15:00:00Z",
                            }
                        ]
                    },
                }
            }
        }

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches(
        "owner/name",
        ["feat/$user", "feat+one", "feat;two", "@"],
    ) == {"feat/$user", "feat+one", "feat;two", "@"}
    assert "branch0=feat/$user" in captured["args"]
    assert "branch1=feat+one" in captured["args"]
    assert "branch2=feat;two" in captured["args"]
    assert "branch3=@" in captured["args"]


def test_graphql_query_and_run_gh_options(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, object]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return {"data": {"repository": {}}}

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches("owner/name", ["feat/a", "bug-b"]) == set()

    args = captured["args"]
    assert isinstance(args, list)
    assert args[:2] == ["api", "graphql"]
    assert "--limit" not in args
    query = args[args.index("-f") + 1]
    assert "headRefName: $branch0" in query
    assert "headRefName: $branch1" in query
    assert "states: MERGED" in query
    assert "nodes { headRefName mergedAt }" in query
    assert "owner=owner" in args
    assert "repo=name" in args
    assert "branch0=feat/a" in args
    assert "branch1=bug-b" in args
    assert captured["kwargs"] == {"repo": "owner/name"}


def test_graphql_variables_are_sent_as_raw_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, object]:
        captured["args"] = args
        return {"data": {"repository": {}}}

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches("owner/name", ["123", "@topic"]) == set()

    args = captured["args"]
    assert isinstance(args, list)
    assert "-F" not in args
    assert args.count("-f") == 5
    assert "branch0=123" in args
    assert "branch1=@topic" in args


def test_excludes_closed_not_merged_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(args: list[str], **kwargs: Any) -> dict[str, object]:
        return {
            "data": {
                "repository": {
                    "b0": {
                        "nodes": [
                            {"headRefName": "closed-not-merged", "mergedAt": None}
                        ]
                    }
                }
            }
        }

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert gh_pr_get_merged_branches("owner/name", ["closed-not-merged"]) == set()
