from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from src.daemon.guardrails import GuardrailViolation, scan_stdout


def test_guardrail_violation_dataclass_frozen() -> None:
    violation = GuardrailViolation(
        tier=1,
        category="repo_create",
        excerpt="gh repo create demo",
        rule="GitHub CLI repository creation invocation",
    )

    with pytest.raises(FrozenInstanceError):
        violation.category = "other"  # type: ignore[misc]


def test_scan_stdout_repo_create_pattern_matches() -> None:
    violations = scan_stdout("gh repo create octo/demo\n")

    assert len(violations) == 1
    assert violations[0].category == "repo_create"


def test_scan_stdout_repo_create_pattern_allows_horizontal_whitespace() -> None:
    violations = scan_stdout("$ gh\trepo\tcreate octo/demo\n")

    assert len(violations) == 1
    assert violations[0].category == "repo_create"


def test_scan_stdout_repo_create_pattern_does_not_cross_newlines() -> None:
    stdout = "wrapped prose:\ngh\nrepo\ncreate octo/demo\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_repo_create_pattern_ignores_help_and_prose() -> None:
    stdout = (
        "gh repo --help\n"
        "  gh repo create [<name>] [flags]\n"
        "Do not run gh repo create for this task.\n"
    )

    assert scan_stdout(stdout) == []


def test_scan_stdout_repo_create_pattern_accepts_shell_prompts() -> None:
    stdout = "$ gh repo create first\n> gh repo create second\n+ gh repo create third\n"

    violations = scan_stdout(stdout)

    assert [violation.category for violation in violations] == [
        "repo_create",
        "repo_create",
        "repo_create",
    ]


def test_scan_stdout_repo_create_pattern_accepts_repeated_xtrace_prefix() -> None:
    violations = scan_stdout("++ gh repo create octo/demo\n")

    assert len(violations) == 1
    assert violations[0].category == "repo_create"


def test_scan_stdout_no_match_returns_empty_list() -> None:
    stdout = (
        "python -m ruff check .\n"
        "python -m pytest -q\n"
        "M src/daemon/handlers/coding.py\n"
        "scripts/ci.sh exited 0\n"
    )

    assert scan_stdout(stdout) == []


def test_scan_stdout_excerpt_truncation_200_chars() -> None:
    stdout = "gh repo create " + ("very-long-argument-" * 30)

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert len(violations[0].excerpt) <= 200


def test_scan_stdout_multiple_matches_in_one_input_returns_all() -> None:
    stdout = (
        "gh repo create first\n"
        "GH   REPO   CREATE second\n"
        "+ gh repo create third suffix\n"
    )

    violations = scan_stdout(stdout)

    assert [violation.category for violation in violations] == [
        "repo_create",
        "repo_create",
        "repo_create",
    ]


def test_scan_stdout_negation_does_not_suppress() -> None:
    stdout = "gh repo create octo/demo # I would never do this intentionally\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "repo_create"
