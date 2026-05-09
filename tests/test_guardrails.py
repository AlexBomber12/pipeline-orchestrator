from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from src.daemon.guardrails import (
    GuardrailViolation,
    _command_tokens_after_git_push,
    scan_stdout,
)


def _categories(stdout: str, *, default_branch: str = "main") -> list[str]:
    return [
        violation.category
        for violation in scan_stdout(stdout, default_branch=default_branch)
    ]


def test_scan_stdout_gh_repo_create_returns_repo_create_violation() -> None:
    assert scan_stdout("gh repo create AlexBomber12/test") == [
        GuardrailViolation(
            tier=1,
            category="repo_create",
            excerpt="gh repo create AlexBomber12/test",
            rule="Coder stdout contains forbidden GitHub repository creation.",
        )
    ]


@pytest.mark.parametrize(
    ("stdout", "categories"),
    [
        ("gh repo delete AlexBomber12/test", ["repo_delete"]),
        ("git push --force origin HEAD:main", ["force_push_main"]),
        ("git push --force-with-lease origin refs/heads/main", ["force_push_main"]),
        ("git push origin +HEAD:main", ["force_push_main"]),
        ("I force-pushed to main during cleanup", ["force_push_main"]),
        ("git push origin :main", ["branch_delete_main"]),
        ("git push upstream :main", ["branch_delete_main"]),
        ("git push --delete origin main", ["branch_delete_main"]),
        ("git push origin --delete main", ["branch_delete_main"]),
        ("git push upstream -d refs/heads/main", ["branch_delete_main"]),
        ("git push -d upstream main", ["branch_delete_main"]),
        ("git commit -m foo\ngit push origin main", ["direct_commit_main"]),
        ("git commit -m foo\ngit push -- origin main", ["direct_commit_main"]),
        ("git commit -m foo\ngit push -u origin main", ["direct_commit_main"]),
    ],
)
def test_scan_stdout_forbidden_patterns(
    stdout: str,
    categories: list[str],
) -> None:
    assert _categories(stdout) == categories


@pytest.mark.parametrize(
    "stdout",
    [
        "python -m pytest -q\nruff check .\nCI green",
        "git commit -m foo\ngh pr create\ngit push origin main",
        "git push --force origin feature/thing",
        "git push --force main feature/thing",
        "git commit -m foo\ngit push origin feature/thing",
        "git commit -m foo\npython -m pytest -q",
        "git push origin --delete feature/thing",
        "git push upstream :feature/thing",
        "git commit -m foo\ngit push origin -o main feature/thing",
        "git commit -m foo\ngit push origin --push-option main feature/thing",
        "git commit -m foo\ngit push --repo origin main",
    ],
)
def test_scan_stdout_clean_or_non_default_branch_output_returns_empty(
    stdout: str,
) -> None:
    assert scan_stdout(stdout) == []


def test_scan_stdout_multiple_violations_returns_all() -> None:
    assert _categories("gh repo create test\ngit push --force origin main") == [
        "repo_create",
        "force_push_main",
    ]


def test_scan_stdout_uses_configured_default_branch() -> None:
    stdout = "\n".join(
        [
            "git push --force origin HEAD:trunk",
            "git push upstream --delete refs/heads/trunk",
            "git commit -m foo",
            "git push origin trunk",
        ]
    )

    assert _categories(stdout, default_branch="trunk") == [
        "force_push_main",
        "branch_delete_main",
        "direct_commit_main",
    ]


def test_scan_stdout_configured_default_branch_does_not_flag_main() -> None:
    stdout = "\n".join(
        [
            "git push --force origin HEAD:main",
            "git push upstream --delete refs/heads/main",
            "git commit -m foo",
            "git push origin main",
        ]
    )

    assert _categories(stdout, default_branch="trunk") == []


def test_scan_stdout_negation_does_not_suppress() -> None:
    assert _categories("I would never run gh repo create on its own") == [
        "repo_create"
    ]


def test_command_tokens_after_git_push_returns_empty_for_non_push_line() -> None:
    assert _command_tokens_after_git_push("python -m pytest -q") == []


def test_guardrail_violation_dataclass_frozen() -> None:
    violation = GuardrailViolation(1, "repo_create", "gh repo create x", "rule")

    with pytest.raises(FrozenInstanceError):
        violation.category = "repo_delete"  # type: ignore[misc]


def test_scan_stdout_excerpt_truncation_200_chars() -> None:
    violation = scan_stdout(f"gh repo create {'x' * 250}")[0]

    assert len(violation.excerpt) <= 200
