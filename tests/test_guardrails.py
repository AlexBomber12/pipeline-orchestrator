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


def test_scan_stdout_branch_delete_default_via_colon_refspec() -> None:
    violations = scan_stdout("git push origin :refs/heads/main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize("refspec", ["+:main", "+:refs/heads/main"])
def test_scan_stdout_branch_delete_default_via_forced_empty_src_refspec(
    refspec: str,
) -> None:
    violations = scan_stdout(f"git push origin {refspec}\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_default_via_dash_d_flag() -> None:
    violations = scan_stdout("git push --delete origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_default_via_dash_d_short_flag() -> None:
    violations = scan_stdout("git push origin -d main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize("delete_flag", ["--delete", "-d"])
def test_scan_stdout_branch_delete_default_with_delete_flag_after_ref(
    delete_flag: str,
) -> None:
    violations = scan_stdout(f"git push origin main {delete_flag}\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize("delete_flag", ["-df", "-fd"])
def test_scan_stdout_branch_delete_default_via_clustered_short_delete_flag(
    delete_flag: str,
) -> None:
    violations = scan_stdout(f"git push {delete_flag} origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_push_option_attached_value_not_delete_flag() -> None:
    assert scan_stdout("git push -odeploy origin main\n") == []


def test_scan_stdout_branch_delete_push_option_attached_value_not_dry_run() -> None:
    violations = scan_stdout("git push -onotify --delete origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize("option", ["-o", "--push-option", "--receive-pack"])
def test_scan_stdout_branch_delete_option_value_main_not_ref(option: str) -> None:
    stdout = f"git push origin {option} main --delete feature-x\n"

    assert scan_stdout(stdout) == []


@pytest.mark.parametrize("option", ["--push-option=main", "--receive-pack=main"])
def test_scan_stdout_branch_delete_attached_long_option_value_not_ref(
    option: str,
) -> None:
    stdout = f"git push origin {option} --delete feature-x\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_option_value_before_colon_refspec_not_remote() -> None:
    assert scan_stdout("git push -o origin :main\n") == []


def test_scan_stdout_branch_delete_with_option_value_still_flags_main_ref() -> None:
    violations = scan_stdout("git push -o ci origin --delete main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_push_option_value_not_dry_run() -> None:
    violations = scan_stdout("git push -o -notify --delete origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_push_option_value_not_delete_flag() -> None:
    assert scan_stdout("git push -o -debug origin main\n") == []


@pytest.mark.parametrize("delete_flag", ["--delete", "-d"])
def test_scan_stdout_branch_delete_without_ref_not_flagged(delete_flag: str) -> None:
    stdout = f"git push {delete_flag} main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_colon_refspec_without_remote_not_flagged() -> None:
    assert scan_stdout("git push :main\n") == []


def test_scan_stdout_branch_delete_remote_named_main_not_flagged() -> None:
    stdout = "git push --delete main feature-x\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_without_target_not_flagged() -> None:
    assert scan_stdout("git push --delete\n") == []


def test_scan_stdout_branch_delete_feature_not_flagged() -> None:
    stdout = "git push --delete origin pr-123-something\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_main_dry_run_not_flagged() -> None:
    stdout = "git push --dry-run --delete origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_main_clustered_short_dry_run_not_flagged() -> None:
    stdout = "git push origin :main -nq\n"

    assert scan_stdout(stdout) == []


@pytest.mark.parametrize(
    "stdout",
    [
        "git push --delete origin MAIN\n",
        "git push origin :MAIN\n",
    ],
)
def test_scan_stdout_branch_delete_main_is_case_sensitive(stdout: str) -> None:
    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_unclosed_quote_not_flagged() -> None:
    assert scan_stdout('git push --delete origin "main\n') == []


def test_scan_stdout_branch_delete_long_nonmatching_push_returns_empty() -> None:
    stdout = "git push " + " ".join(f"feature-{index}" for index in range(1000))

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_main_prose_not_flagged() -> None:
    stdout = "The next example mentions --delete main without a push command.\n"

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


def test_scan_stdout_repo_delete_negation_does_not_suppress() -> None:
    stdout = "gh repo delete octo/demo # I would never do this intentionally\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "repo_delete"
