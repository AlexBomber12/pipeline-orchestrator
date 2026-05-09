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


def test_scan_stdout_repo_delete_pattern_matches() -> None:
    violations = scan_stdout("gh repo delete octo/demo\n")

    assert len(violations) == 1
    assert violations[0].category == "repo_delete"


def test_scan_stdout_repo_delete_pattern_ignores_help_and_prose() -> None:
    stdout = (
        "gh repo --help\n"
        "  gh repo delete [<repository>] [flags]\n"
        "Do not run gh repo delete for this task.\n"
    )

    assert scan_stdout(stdout) == []


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
    stdout = "$ gh repo create first\n> gh repo create second\n"

    violations = scan_stdout(stdout)

    assert [violation.category for violation in violations] == [
        "repo_create",
        "repo_create",
    ]


def test_scan_stdout_repo_create_pattern_accepts_repeated_xtrace_prefix() -> None:
    violations = scan_stdout("++ gh repo create octo/demo\n")

    assert len(violations) == 1
    assert violations[0].category == "repo_create"


def test_scan_stdout_repo_create_pattern_ignores_diff_added_lines() -> None:
    stdout = "+gh repo create octo/demo\n+ gh repo create octo/demo\n"

    assert scan_stdout(stdout) == []


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
        "++ gh repo create third suffix\n"
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


def test_scan_stdout_force_push_main_standard_form() -> None:
    violations = scan_stdout("git push --force origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_with_lease_main_flagged() -> None:
    violations = scan_stdout("git push origin main --force-with-lease\n")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_to_feature_branch_not_flagged() -> None:
    stdout = "git push --force origin feature/xyz\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_force_push_main_prose_not_flagged() -> None:
    stdout = "Reminder: do not run git push --force origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_default_via_colon_refspec() -> None:
    violations = scan_stdout("git push origin :main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_default_via_dash_d_flag() -> None:
    violations = scan_stdout("git push -d origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_default_remote_first_flag() -> None:
    violations = scan_stdout("git push origin --delete main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_feature_not_flagged() -> None:
    stdout = "git push --delete origin feature/xyz\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_main_prose_not_flagged() -> None:
    stdout = "Reminder: do not run git push origin :main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_default_no_pr_create() -> None:
    stdout = "git commit -m guardrail\npython -m pytest -q\ngit push origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_with_pr_create_between() -> None:
    stdout = "git commit -m guardrail\ngh pr create --fill\ngit push origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_with_pr_create_dry_run_still_flagged() -> None:
    stdout = "git commit -m guardrail\ngh pr create --dry-run\ngit push origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_with_pr_create_help_still_flagged() -> None:
    stdout = "git commit -m guardrail\ngh pr create --help\ngit push origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_amend_excluded() -> None:
    stdout = "git commit --amend --no-edit\ngit push origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_main_checklist_not_flagged() -> None:
    stdout = (
        "Checklist:\n"
        "1. Run git commit -m guardrail\n"
        "2. Run tests\n"
        "3. Run git push origin main\n"
    )

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_amend_with_force_push_caught_by_force_rule() -> None:
    stdout = "git commit --amend --no-edit\ngit push --force origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_remote_named_main_not_misclassified() -> None:
    stdout = "git push main feature-branch\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_multiple_distinct_violations_returns_all() -> None:
    stdout = "gh repo create octo/demo\ngit push --force origin main\n"

    violations = scan_stdout(stdout)

    assert [violation.category for violation in violations] == [
        "force_push_main",
        "repo_create",
    ]


def test_scan_stdout_repo_delete_command_line_negation_does_not_suppress() -> None:
    stdout = "gh repo delete octo/demo # I would never do this intentionally\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "repo_delete"


def test_scan_stdout_repo_delete_prose_not_flagged() -> None:
    stdout = "Never run this manually: gh repo delete octo/demo\n"

    assert scan_stdout(stdout) == []
