from __future__ import annotations

import pytest

from src.task_status import _branch_name_matches_task_pr_id


def test_empty_pr_id_returns_false() -> None:
    assert not _branch_name_matches_task_pr_id("", "pr-263-foo")


def test_branch_shorter_than_pr_id_returns_false() -> None:
    assert not _branch_name_matches_task_pr_id("PR-263", "pr-26")


def test_branch_equals_pr_id_returns_true() -> None:
    assert _branch_name_matches_task_pr_id("PR-263", "PR-263")


def test_branch_equals_pr_id_lowercase_returns_true() -> None:
    assert _branch_name_matches_task_pr_id("PR-263", "pr-263")


@pytest.mark.parametrize(
    "branch",
    ["pr-263-feat", "pr-263_feat", "pr-263.feat", "pr-263/feat"],
)
def test_branch_with_separator_after_pr_id_returns_true(branch: str) -> None:
    assert _branch_name_matches_task_pr_id("PR-263", branch)


def test_branch_with_no_separator_returns_false() -> None:
    assert not _branch_name_matches_task_pr_id("PR-263", "pr-2630-feat")


@pytest.mark.parametrize("branch", ["pr-263-5", "pr-263-5-feature", "pr_263.5"])
def test_underscore_dot_normalization_in_pr_id(branch: str) -> None:
    assert _branch_name_matches_task_pr_id("PR_263.5", branch)


def test_unicode_branch_returns_false_without_match() -> None:
    assert _branch_name_matches_task_pr_id("PR-263", "pr-263-фикс")
    assert not _branch_name_matches_task_pr_id("PR-263", "фикс-263")


@pytest.mark.parametrize("branch", ["pr-263+rebase", "pr-263=v2", "pr-263#1"])
def test_special_chars_per_git_check_ref_format(branch: str) -> None:
    assert not _branch_name_matches_task_pr_id("PR-263", branch)
