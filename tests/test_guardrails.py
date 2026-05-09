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


def test_scan_stdout_force_push_main_same_line_chain_flagged() -> None:
    violations = scan_stdout("echo ok && git push --force origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_with_lease_main_flagged() -> None:
    violations = scan_stdout("git push origin main --force-with-lease\n")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_to_feature_branch_not_flagged() -> None:
    stdout = "git push --force origin feature/xyz\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_force_push_main_current_branch_without_refspec_flagged() -> None:
    violations = scan_stdout("git push --force origin\n", current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_feature_current_branch_without_refspec_not_flagged() -> None:
    stdout = "git push --force origin\n"

    assert scan_stdout(stdout, current_branch="feature/xyz") == []


def test_scan_stdout_force_push_tracks_checkout_to_main_before_no_refspec_push() -> None:
    stdout = "git checkout main\ngit push --force origin\n"

    violations = scan_stdout(stdout, current_branch="feature/xyz")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_tracks_same_line_switch_to_main() -> None:
    stdout = "git switch main && git push --force origin\n"

    violations = scan_stdout(stdout, current_branch="feature/xyz")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_keeps_confirmed_switch_before_later_error() -> None:
    stdout = (
        "git switch main\n"
        "Switched to branch 'main'\n"
        "git push --force origin\n"
        "fatal: unrelated later failure\n"
    )

    violations = scan_stdout(stdout, current_branch="feature/xyz")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_tracks_checkout_away_from_main_before_push() -> None:
    stdout = (
        "git checkout feature/xyz\n"
        "Switched to branch 'feature/xyz'\n"
        "git push --force origin\n"
    )

    assert scan_stdout(stdout, current_branch="main") == []


@pytest.mark.parametrize("branch_command", ["git switch -q", "git checkout --quiet"])
def test_scan_stdout_force_push_tracks_quiet_branch_change_away_from_main(
    branch_command: str,
) -> None:
    stdout = f"{branch_command} feature/xyz\ngit push --force origin\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_force_push_tracks_checkout_before_delayed_stderr_feedback() -> None:
    stdout_then_stderr = (
        "git checkout feature/xyz\n"
        "git push --force origin\n"
        "Switched to branch 'feature/xyz'\n"
    )

    assert scan_stdout(stdout_then_stderr, current_branch="main") == []


def test_scan_stdout_force_push_does_not_treat_checkout_path_as_branch() -> None:
    stdout = "git checkout README.md\ngit push --force origin\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_does_not_treat_slash_path_as_branch() -> None:
    stdout = "git checkout src/daemon/guardrails.py\ngit push --force origin\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_discards_checkout_path_before_switch() -> None:
    stdout = "git checkout README.md\ngit switch feature/xyz\ngit push --force origin\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_force_push_tracks_confirmed_checkout_pathlike_branch() -> None:
    stdout = (
        "git checkout README.md\n"
        "Already on 'README.md'\n"
        "git push --force origin\n"
    )

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_force_push_tracks_quiet_dotted_branch_without_feedback() -> None:
    stdout = "git checkout -q release/v1.2\ngit push --force origin\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_force_push_tracks_switch_dash_to_previous_branch() -> None:
    stdout = "git switch main\ngit switch -\ngit push --force origin\n"

    assert scan_stdout(stdout, current_branch="feature/xyz") == []


def test_scan_stdout_force_push_keeps_branch_when_switch_dash_has_no_previous() -> None:
    stdout = "git switch -\ngit push --force origin\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


@pytest.mark.parametrize(
    "failure",
    [
        "error: pathspec 'main' did not match any file(s) known to git",
        "fatal: invalid reference: main",
        "error: Your local changes would be overwritten; not switching branches",
    ],
)
def test_scan_stdout_force_push_ignores_failed_switch_before_push(
    failure: str,
) -> None:
    stdout = f"git switch main\n{failure}\ngit push --force origin\n"

    assert scan_stdout(stdout, current_branch="feature/xyz") == []


def test_scan_stdout_force_push_ignores_delayed_failed_switch_stderr() -> None:
    stdout_then_stderr = (
        "git switch main\n"
        "git push --force origin\n"
        "error: pathspec 'main' did not match any file(s) known to git\n"
    )

    assert scan_stdout(stdout_then_stderr, current_branch="feature/xyz") == []


def test_scan_stdout_force_push_ignores_delayed_fatal_switch_stderr() -> None:
    stdout_then_stderr = (
        "git switch main\n"
        "git push --force origin\n"
        "fatal: invalid reference: main\n"
    )

    assert scan_stdout(stdout_then_stderr, current_branch="feature/xyz") == []


@pytest.mark.parametrize("branch_command", ["git switch -c", "git checkout -b"])
def test_scan_stdout_force_push_tracks_branch_create_away_from_main_before_push(
    branch_command: str,
) -> None:
    stdout = f"{branch_command} feature/xyz\ngit push --force origin\n"

    assert scan_stdout(stdout, current_branch="main") == []


@pytest.mark.parametrize("branch_command", ["git switch -C", "git checkout -B"])
def test_scan_stdout_force_push_tracks_branch_create_to_main_before_push(
    branch_command: str,
) -> None:
    stdout = f"{branch_command} main\ngit push --force origin\n"

    violations = scan_stdout(stdout, current_branch="feature/xyz")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_main_current_branch_head_refspec_flagged() -> None:
    violations = scan_stdout("git push --force origin HEAD\n", current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "force_push_main"


def test_scan_stdout_force_push_tags_mode_on_main_not_flagged() -> None:
    stdout = "git push --force --tags\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_force_push_main_prose_not_flagged() -> None:
    stdout = "Reminder: do not run git push --force origin main\n"

    assert scan_stdout(stdout) == []


@pytest.mark.parametrize("dry_run_flag", ["--dry-run", "-n"])
def test_scan_stdout_force_push_main_dry_run_not_flagged(dry_run_flag: str) -> None:
    stdout = f"git push {dry_run_flag} --force origin main\n"

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


@pytest.mark.parametrize("dry_run_flag", ["--dry-run", "-n"])
def test_scan_stdout_branch_delete_main_dry_run_not_flagged(
    dry_run_flag: str,
) -> None:
    stdout = f"git push {dry_run_flag} origin :main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_default_no_pr_create() -> None:
    stdout = "git commit -m guardrail\npython -m pytest -q\ngit push origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


@pytest.mark.parametrize("separator", ["&&", ";", "||"])
def test_scan_stdout_direct_commit_main_same_line_push_flagged(separator: str) -> None:
    stdout = f"git commit -m guardrail {separator} git push origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_same_line_no_refspec_push_flagged() -> None:
    stdout = "git commit -m guardrail && git push origin\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_same_line_ignores_push_in_commit_message() -> None:
    stdout = 'git commit -m "docs: mention git push origin main"\n'

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_same_line_ignores_escaped_separator() -> None:
    stdout = r"git commit -m docs\; git push origin main" "\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_same_line_pr_create_before_push_not_flagged() -> None:
    stdout = "git commit -m guardrail && gh pr create --fill && git push origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_same_line_pr_create_in_message_still_flagged() -> None:
    stdout = 'git commit -m "mention gh pr create" && git push origin main\n'

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_same_line_pr_create_dry_run_still_flagged() -> None:
    stdout = (
        "git commit -m guardrail && gh pr create --dry-run && git push origin main\n"
    )

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_with_pr_create_between() -> None:
    stdout = "git commit -m guardrail\ngh pr create --fill\ngit push origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_with_failed_pr_create_still_flagged() -> None:
    stdout = (
        "git commit -m guardrail\n"
        "gh pr create --fill\n"
        "error: failed to create pull request\n"
        "git push origin main\n"
    )

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


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


@pytest.mark.parametrize("web_flag", ["--web", "-w"])
def test_scan_stdout_direct_commit_with_pr_create_web_not_flagged(
    web_flag: str,
) -> None:
    stdout = f"git commit -m guardrail\ngh pr create {web_flag}\ngit push origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_direct_commit_with_pr_create_head_flag_not_flagged() -> None:
    stdout = "git commit -m guardrail\ngh pr create -H user:branch\ngit push origin main\n"

    assert scan_stdout(stdout) == []


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


def test_scan_stdout_direct_commit_main_current_branch_without_refspec_flagged() -> None:
    stdout = "git commit -m guardrail\ngit push origin\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_feature_branch_without_refspec_not_flagged() -> None:
    stdout = "git commit -m guardrail\ngit push origin\n"

    assert scan_stdout(stdout, current_branch="feature/xyz") == []


def test_scan_stdout_direct_commit_tracks_switch_to_main_before_no_refspec_push() -> None:
    stdout = "git commit -m guardrail\ngit switch main\ngit push origin\n"

    violations = scan_stdout(stdout, current_branch="feature/xyz")

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_tracks_switch_away_from_main_before_push() -> None:
    stdout = "git commit -m guardrail\ngit switch feature/xyz\ngit push origin\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_direct_commit_does_not_treat_checkout_path_as_branch() -> None:
    stdout = "git commit -m guardrail\ngit checkout README.md\ngit push origin\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_tracks_checkout_dash_to_previous_branch() -> None:
    stdout = "git commit -m guardrail\ngit checkout main\ngit checkout -\ngit push origin\n"

    assert scan_stdout(stdout, current_branch="feature/xyz") == []


def test_scan_stdout_direct_commit_ignores_failed_checkout_before_push() -> None:
    stdout = (
        "git commit -m guardrail\n"
        "git checkout main\n"
        "error: pathspec 'main' did not match any file(s) known to git\n"
        "git push origin\n"
    )

    assert scan_stdout(stdout, current_branch="feature/xyz") == []


@pytest.mark.parametrize("branch_command", ["git switch -c", "git checkout -b"])
def test_scan_stdout_direct_commit_tracks_branch_create_away_from_main_before_push(
    branch_command: str,
) -> None:
    stdout = f"git commit -m guardrail\n{branch_command} feature/xyz\ngit push origin\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_direct_commit_main_current_branch_head_refspec_flagged() -> None:
    stdout = "git commit -m guardrail\ngit push origin HEAD\n"

    violations = scan_stdout(stdout, current_branch="main")

    assert len(violations) == 1
    assert violations[0].category == "direct_commit_main"


def test_scan_stdout_direct_commit_tags_mode_on_main_not_flagged() -> None:
    stdout = "git commit -m guardrail\ngit push --tags\n"

    assert scan_stdout(stdout, current_branch="main") == []


def test_scan_stdout_direct_commit_main_ignores_non_push_followup() -> None:
    stdout = "git commit -m guardrail\npython -m pytest -q\n"

    assert scan_stdout(stdout, current_branch="main") == []


@pytest.mark.parametrize("dry_run_flag", ["--dry-run", "-n"])
def test_scan_stdout_direct_commit_main_dry_run_push_not_flagged(
    dry_run_flag: str,
) -> None:
    stdout = f"git commit -m guardrail\ngit push {dry_run_flag} origin main\n"

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
