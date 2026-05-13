from __future__ import annotations

import re
from dataclasses import FrozenInstanceError

import pytest
from src.config import DaemonConfig
from src.daemon import guardrails
from src.daemon.guardrails import GuardrailViolation, scan_pr_diff, scan_stdout

WORKFLOW_DIFF_HEADER = (
    "diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml\n"
)


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


def test_scan_stdout_branch_delete_default_via_heads_ref_alias() -> None:
    violations = scan_stdout("git push --delete origin heads/main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize(
    "stdout",
    [
        "git -C /repo push --delete origin main\n",
        "git --git-dir=/repo/.git push origin :main\n",
        "git -c core.sshCommand=ssh push origin :main\n",
        "git --no-pager -C /repo push origin +:refs/heads/main\n",
    ],
)
def test_scan_stdout_branch_delete_with_git_global_options(stdout: str) -> None:
    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize(
    "stdout",
    [
        "GIT_SSH_COMMAND='ssh -i key' git push --delete origin main\n",
        "env FOO=bar git push origin :main\n",
        "env -i git push --delete origin main\n",
        "env --ignore-environment FOO=bar git push origin :main\n",
        "env -u FOO git push origin +:main\n",
        "$ FOO=bar BAR=baz git push origin +:refs/heads/main\n",
    ],
)
def test_scan_stdout_branch_delete_with_shell_env_prefix(stdout: str) -> None:
    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_env_prefix_before_other_command_not_flagged() -> None:
    stdout = "GIT_SSH_COMMAND='ssh -i key' echo git push --delete origin main\n"

    assert scan_stdout(stdout) == []


def test_scan_stdout_branch_delete_push_word_after_other_git_command_not_flagged() -> None:
    assert scan_stdout("git commit -m push --delete origin main\n") == []


def test_scan_stdout_branch_delete_git_without_subcommand_not_flagged() -> None:
    assert scan_stdout("git -C /repo\n") == []


@pytest.mark.parametrize("option", ["--help", "--html-path", "--man-path", "--version"])
def test_scan_stdout_branch_delete_git_terminal_option_not_flagged(option: str) -> None:
    assert scan_stdout(f"git {option} push --delete origin main\n") == []


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


def test_scan_stdout_branch_delete_option_value_before_colon_refspec_still_flagged() -> None:
    violations = scan_stdout("git push -o origin :main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


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


def test_scan_stdout_branch_delete_colon_refspec_without_remote_flagged() -> None:
    violations = scan_stdout("git push :main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize(
    "stdout",
    [
        "git push origin :main; echo ok\n",
        "git push --delete origin main; echo ok\n",
    ],
)
def test_scan_stdout_branch_delete_before_shell_separator(stdout: str) -> None:
    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


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


def test_scan_stdout_branch_delete_no_dry_run_after_dry_run_still_flagged() -> None:
    stdout = "git push --dry-run --no-dry-run --delete origin main\n"

    violations = scan_stdout(stdout)

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


@pytest.mark.parametrize("repo_option", ["--repo=origin", "--repo origin"])
def test_scan_stdout_branch_delete_repo_option_with_delete_flag(
    repo_option: str,
) -> None:
    violations = scan_stdout(f"git push {repo_option} --delete main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_repo_option_with_colon_refspec() -> None:
    violations = scan_stdout("git push --repo=origin :main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_no_delete_after_delete_not_flagged() -> None:
    assert scan_stdout("git push --delete --no-delete origin main\n") == []


def test_scan_stdout_branch_delete_delete_after_no_delete_still_flagged() -> None:
    violations = scan_stdout("git push --no-delete --delete origin main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


def test_scan_stdout_branch_delete_after_end_of_options_not_delete_flag() -> None:
    assert scan_stdout("git push origin -- --delete main\n") == []


def test_scan_stdout_branch_delete_colon_refspec_after_end_of_options_flagged() -> None:
    violations = scan_stdout("git push origin -- :main\n")

    assert len(violations) == 1
    assert violations[0].category == "branch_delete_main"


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


def test_scan_pr_diff_empty_catalogue_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-290a skeleton: with no diff patterns registered, scan_pr_diff
    never reports a violation, even on diffs that contain content
    PR-290b/c will later flag."""
    monkeypatch.setattr(guardrails, "_DIFF_PATTERNS", {})
    monkeypatch.setattr(guardrails, "_DIFF_RULES", {})
    sample_diff = (
        "diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml\n"
        "@@ -1,2 +1,3 @@\n"
        "+permissions: write-all\n"
    )

    assert scan_pr_diff(sample_diff) == []


def _assert_diff_categories(diff_text: str, categories: list[str]) -> None:
    assert [violation.category for violation in scan_pr_diff(diff_text)] == categories


def _diff_for_file(path: str, additions: int = 0, deletions: int = 0) -> str:
    return (
        f"diff --git a/{path} b/{path}\n"
        f"--- a/{path}\n"
        f"+++ b/{path}\n"
        "@@ -1,1 +1,1 @@\n"
        + "".join(f"-old {index}\n" for index in range(deletions))
        + "".join(f"+new {index}\n" for index in range(additions))
    )


def test_count_diff_size_simple_diff() -> None:
    diff_text = _diff_for_file("src/a.py", additions=2, deletions=1) + _diff_for_file(
        "src/b.py", additions=1, deletions=2
    )

    assert guardrails._count_diff_size(diff_text) == (3, 3, 2, 2)


def test_count_diff_size_excludes_file_headers() -> None:
    diff_text = (
        "diff --git a/src/a.py b/src/a.py\n"
        "--- a/src/a.py\n"
        "+++ b/src/a.py\n"
        "@@ -1,1 +1,1 @@\n"
    )

    assert guardrails._count_diff_size(diff_text) == (0, 0, 1, 0)


def test_count_diff_size_excludes_quoted_file_headers() -> None:
    diff_text = (
        'diff --git "a/src/path with space.py" "b/src/path with space.py"\n'
        '--- "a/src/path with space.py"\n'
        '+++ "b/src/path with space.py"\n'
        "@@ -1,1 +1,1 @@\n"
    )

    assert guardrails._count_diff_size(diff_text) == (0, 0, 1, 0)


def test_diff_file_header_rejects_invalid_or_empty_header_paths() -> None:
    assert not guardrails._is_diff_file_header('+++ "unterminated')
    assert not guardrails._is_diff_file_header("+++ ")


def test_count_diff_size_counts_added_and_deleted_lines_with_header_prefixes() -> None:
    diff_text = (
        "diff --git a/src/a.py b/src/a.py\n"
        "--- a/src/a.py\n"
        "+++ b/src/a.py\n"
        "@@ -1,1 +1,1 @@\n"
        "+++counter\n"
        "---old_value\n"
    )

    assert guardrails._count_diff_size(diff_text) == (1, 1, 1, 1)


def test_count_diff_size_counts_added_lines_that_look_like_file_headers() -> None:
    diff_text = (
        "diff --git a/src/a.py b/src/a.py\n"
        "--- a/src/a.py\n"
        "+++ b/src/a.py\n"
        "@@ -1,1 +1,1 @@\n"
        "+++ b/not-a-header.py\n"
    )

    assert guardrails._count_diff_size(diff_text) == (1, 0, 1, 1)


def test_count_diff_size_ignores_malformed_diff_git_headers() -> None:
    diff_text = (
        'diff --git "a/src/unclosed.py b/src/unclosed.py\n'
        "diff --git a/src/only-one-path.py\n"
        "+added\n"
    )

    assert guardrails._count_diff_size(diff_text) == (1, 0, 0, 0)


def test_classify_lockfile_exempt_package_lock() -> None:
    diff_text = _diff_for_file("frontend/package-lock.json", additions=1)

    assert guardrails._classify_files_lockfile_exempt(diff_text) == {
        "frontend/package-lock.json"
    }


def test_count_additions_in_paths_counts_added_lines_with_header_prefixes() -> None:
    diff_text = (
        "diff --git a/package-lock.json b/package-lock.json\n"
        "--- a/package-lock.json\n"
        "+++ b/package-lock.json\n"
        "@@ -1,1 +1,1 @@\n"
        "+++lockfile_value\n"
    )

    assert guardrails._count_additions_in_paths(diff_text, {"package-lock.json"}) == 1


def test_count_additions_in_paths_counts_added_lines_that_look_like_headers() -> None:
    diff_text = (
        "diff --git a/package-lock.json b/package-lock.json\n"
        "--- a/package-lock.json\n"
        "+++ b/package-lock.json\n"
        "@@ -1,1 +1,1 @@\n"
        "+++ b/not-a-header.json\n"
    )

    assert guardrails._count_additions_in_paths(diff_text, {"package-lock.json"}) == 1


def test_classify_lockfile_exempt_requirements_txt() -> None:
    diff_text = _diff_for_file("requirements.txt", additions=1) + _diff_for_file(
        "requirements-test.txt", additions=1
    )

    assert guardrails._classify_files_lockfile_exempt(diff_text) == {
        "requirements.txt",
        "requirements-test.txt",
    }


def test_classify_lockfile_exempt_non_lockfile() -> None:
    diff_text = _diff_for_file("src/foo.py", additions=1)

    assert guardrails._classify_files_lockfile_exempt(diff_text) == set()


def test_classify_lockfile_exempt_rejects_suffix_only_matches() -> None:
    diff_text = _diff_for_file("src/myrequirements.txt", additions=1) + _diff_for_file(
        "docs/foo-package-lock.json",
        additions=1,
    )

    assert guardrails._classify_files_lockfile_exempt(diff_text) == set()


def test_scan_pr_diff_under_threshold_no_violation() -> None:
    diff_text = "".join(
        _diff_for_file(f"src/file_{index}.py", additions=20) for index in range(5)
    )

    assert "large_diff_threshold" not in [
        violation.category for violation in scan_pr_diff(diff_text)
    ]


def test_scan_pr_diff_uses_supplied_daemon_config_thresholds() -> None:
    diff_text = _diff_for_file("src/configured.py", additions=120)
    daemon_config = DaemonConfig(
        large_diff_addition_threshold=100,
        large_diff_files_threshold=30,
    )

    violations = scan_pr_diff(diff_text, daemon_config=daemon_config)

    assert [violation.category for violation in violations] == [
        "large_diff_threshold"
    ]
    assert "Threshold: +100 LOC or 30 files." in violations[0].excerpt


def test_scan_pr_diff_over_addition_threshold_flagged() -> None:
    diff_text = _diff_for_file("src/large.py", additions=1600)

    _assert_diff_categories(diff_text, ["large_diff_threshold"])


def test_scan_pr_diff_over_file_threshold_flagged() -> None:
    diff_text = "".join(
        _diff_for_file(f"src/file_{index}.py", additions=50) for index in range(31)
    )

    _assert_diff_categories(diff_text, ["large_diff_threshold"])


def test_scan_pr_diff_lockfile_additions_excluded() -> None:
    diff_text = _diff_for_file("package-lock.json", additions=2000)

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_mixed_lockfile_and_source() -> None:
    diff_text = _diff_for_file(
        "package-lock.json", additions=1500
    ) + _diff_for_file("src/source.py", additions=200)

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_excerpt_includes_counts() -> None:
    diff_text = _diff_for_file("package-lock.json", additions=100) + _diff_for_file(
        "src/large.py", additions=1600
    )

    violations = scan_pr_diff(diff_text)

    assert len(violations) == 1
    assert violations[0].category == "large_diff_threshold"
    assert "+1600 LOC" in violations[0].excerpt
    assert "2 files" in violations[0].excerpt
    assert "1 lockfile excluded" in violations[0].excerpt


def test_scan_pr_diff_tier_1_violations_still_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        + "+permissions: write-all\n"
        + _diff_for_file("src/large.py", additions=1600)
    )

    _assert_diff_categories(
        diff_text, ["permissions_escalation", "large_diff_threshold"]
    )


def _secret_diff(secret_value: str, assignment_name: str = "TOKEN") -> str:
    return (
        "diff --git a/src/settings.py b/src/settings.py\n"
        "--- a/src/settings.py\n"
        "+++ b/src/settings.py\n"
        "@@ -1,1 +1,2 @@\n"
        f'+{assignment_name} = "{secret_value}"\n'
    )


def _assert_secret_violation_redacted(
    secret_value: str,
    category: str,
    assignment_name: str = "TOKEN",
) -> GuardrailViolation:
    violations = scan_pr_diff(_secret_diff(secret_value, assignment_name))

    assert len(violations) == 1
    assert violations[0].tier == 2
    assert violations[0].category == category
    assert secret_value not in violations[0].excerpt
    return violations[0]


def test_scan_pr_diff_detects_github_pat_classic() -> None:
    placeholder = "ghp_" + "A" * 36

    violation = _assert_secret_violation_redacted(
        placeholder,
        "github_pat_classic",
    )

    assert violation.excerpt == "src/settings.py:5 (category: github_pat_classic)"


def test_scan_pr_diff_detects_aws_access_key() -> None:
    placeholder = "AKIA" + "A" * 16

    _assert_secret_violation_redacted(placeholder, "aws_access_key")


def test_scan_pr_diff_detects_anthropic_api_key() -> None:
    placeholder = "sk-ant-" + "A" * 30

    _assert_secret_violation_redacted(placeholder, "anthropic_api_key")


def test_scan_pr_diff_detects_hyphenated_openai_api_keys() -> None:
    placeholders = [
        "sk-proj-" + ("A" * 24) + "-" + ("B" * 24),
        "sk-svcacct-" + ("A" * 20) + "-" + ("B" * 20),
    ]

    for placeholder in placeholders:
        _assert_secret_violation_redacted(placeholder, "openai_api_key")


def test_scan_pr_diff_addition_only_no_context_lines() -> None:
    placeholder = "ghp_" + "A" * 36
    diff_text = (
        "diff --git a/src/settings.py b/src/settings.py\n"
        "--- a/src/settings.py\n"
        "+++ b/src/settings.py\n"
        "@@ -1,2 +1,1 @@\n"
        f' TOKEN = "{placeholder}"\n'
        f'-OLD_TOKEN = "{placeholder}"\n'
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_clean_diff_no_violations() -> None:
    diff_text = _secret_diff("not-a-token")

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_excerpt_never_contains_secret_value() -> None:
    positive_cases = [
        ("ghp_" + "A" * 36, "github_pat_classic"),
        ("AKIA" + "A" * 16, "aws_access_key"),
        ("sk-ant-" + "A" * 30, "anthropic_api_key"),
        ("sk-proj-" + ("A" * 24) + "-" + ("B" * 24), "openai_api_key"),
    ]

    for secret_value, category in positive_cases:
        _assert_secret_violation_redacted(secret_value, category)


def _slack_token(kind: str, segments: list[str], suffix: str) -> str:
    return "xo" + kind + "-" + "-".join(segments) + "-" + suffix


def test_scan_pr_diff_detects_slack_user_token() -> None:
    placeholder = _slack_token(
        "xp",
        ["1234567890", "2345678901", "3456789012"],
        "a" * 32,
    )

    _assert_secret_violation_redacted(placeholder, "slack_token_user")


def test_scan_pr_diff_detects_modern_slack_user_token() -> None:
    placeholder = _slack_token(
        "xp",
        ["11111111111", "22222222222", "33333333333", "44444444444"],
        "Ab" * 20,
    )

    _assert_secret_violation_redacted(placeholder, "slack_token_user")


def test_scan_pr_diff_detects_slack_bot_token() -> None:
    placeholder = _slack_token(
        "xb",
        ["1234567890", "2345678901"],
        "A" * 24,
    )

    _assert_secret_violation_redacted(placeholder, "slack_token_bot")


def test_scan_pr_diff_detects_modern_slack_bot_token() -> None:
    placeholder = _slack_token(
        "xb",
        ["11111111111", "22222222222", "33333333333"],
        "abcdef0123456789abcdef0123456789",
    )

    _assert_secret_violation_redacted(placeholder, "slack_token_bot")


def test_scan_pr_diff_detects_slack_webhook() -> None:
    placeholder = (
        "https://hooks.slack.com/services/T0123456789/B0123456789/" + ("A" * 24)
    )

    _assert_secret_violation_redacted(placeholder, "slack_webhook")


def test_scan_pr_diff_detects_govslack_webhook() -> None:
    placeholder = (
        "https://hooks.slack-gov.com/services/T0123456789/B0123456789/"
        + ("A" * 24)
    )

    _assert_secret_violation_redacted(placeholder, "slack_webhook")


def test_scan_pr_diff_detects_slack_oauth_webhook() -> None:
    placeholder = "https://hooks.slack.com/T0123456789/B0123456789/" + ("A" * 24)

    _assert_secret_violation_redacted(placeholder, "slack_webhook")


def test_scan_pr_diff_detects_stripe_secret_key() -> None:
    placeholder = "sk_test_" + ("A" * 24)

    _assert_secret_violation_redacted(placeholder, "stripe_secret_key")


def test_scan_pr_diff_detects_google_api_key() -> None:
    placeholder = "AIza" + ("A" * 35)

    _assert_secret_violation_redacted(placeholder, "google_api_key")


def test_scan_pr_diff_detects_jwt_like() -> None:
    placeholder = "eyJ" + ("A" * 10) + "." + ("B" * 10) + "." + ("C" * 10)

    _assert_secret_violation_redacted(placeholder, "jwt_like", "JWT_TOKEN")


def test_scan_pr_diff_ignores_bare_jwt_like_sample() -> None:
    placeholder = "eyJ" + ("A" * 10) + "." + ("B" * 10) + "." + ("C" * 10)

    assert scan_pr_diff(_secret_diff(placeholder, "EXAMPLE_VALUE")) == []


def test_scan_pr_diff_group_b_excerpt_never_contains_secret_value() -> None:
    positive_cases = [
        (
            _slack_token(
                "xp",
                ["1234567890", "2345678901", "3456789012"],
                "a" * 32,
            ),
            "slack_token_user",
        ),
        (
            _slack_token(
                "xp",
                ["11111111111", "22222222222", "33333333333", "44444444444"],
                "Ab" * 20,
            ),
            "slack_token_user",
        ),
        (
            _slack_token(
                "xb",
                ["1234567890", "2345678901"],
                "A" * 24,
            ),
            "slack_token_bot",
        ),
        (
            _slack_token(
                "xb",
                ["11111111111", "22222222222", "33333333333"],
                "abcdef0123456789abcdef0123456789",
            ),
            "slack_token_bot",
        ),
        (
            "https://hooks.slack.com/services/T0123456789/B0123456789/"
            + ("A" * 24),
            "slack_webhook",
        ),
        (
            "https://hooks.slack-gov.com/services/T0123456789/B0123456789/"
            + ("A" * 24),
            "slack_webhook",
        ),
        (
            "https://hooks.slack.com/T0123456789/B0123456789/" + ("A" * 24),
            "slack_webhook",
        ),
        ("sk_test_" + ("A" * 24), "stripe_secret_key", "TOKEN"),
        ("rk_live_" + ("A" * 24), "stripe_restricted_key", "TOKEN"),
        ("AIza" + ("A" * 35), "google_api_key", "TOKEN"),
        (
            "eyJ" + ("A" * 10) + "." + ("B" * 10) + "." + ("C" * 10),
            "jwt_like",
            "JWT_TOKEN",
        ),
    ]

    for positive_case in positive_cases:
        secret_value, category, *assignment_name = positive_case
        _assert_secret_violation_redacted(
            secret_value,
            category,
            assignment_name[0] if assignment_name else "TOKEN",
        )


def test_workflow_permission_context_helpers_handle_non_yaml_lines() -> None:
    assert guardrails._diff_yaml_line("+++ b/.github/workflows/ci.yml") is None
    assert guardrails._yaml_key("not a mapping") is None
    assert guardrails._visible_yaml_context(
        ["-jobs:", " # comment", " jobs:"], 3
    ) == [(0, "jobs")]
    assert not guardrails._is_workflow_permission_key_context([], 0, "not a mapping")
    assert not guardrails._is_workflow_permission_key_context([], 0, "contents: write")
    assert not guardrails._is_workflow_jobs_flow_permission_escalation(
        [], 0, "not a mapping"
    )
    assert not guardrails._is_workflow_jobs_flow_permission_escalation(
        [], 0, "jobs: { build: { runs-on: ubuntu-latest } }"
    )
    assert not guardrails._is_workflow_jobs_flow_permission_escalation(
        [], 0, "jobs: { build: { permissions: *read_only } }"
    )
    assert guardrails._jobs_flow_fragment(
        ["+jobs: {}", "+env: {}"], 0, "jobs: {}"
    ) == "jobs: {}"
    assert guardrails._jobs_flow_fragment(
        ["+jobs: {", "@@ -1,2 +1,3 @@", "-  old: {}", "+  build: {}"],
        0,
        "jobs: {",
    ) == "jobs: { build: {}"
    assert not guardrails._is_workflow_job_flow_permission_escalation(
        [], 0, "not a mapping"
    )
    assert not guardrails._is_workflow_job_flow_permission_escalation(
        [], 0, "  build: { permissions: write-all }"
    )
    assert not guardrails._is_workflow_job_flow_permission_escalation(
        [" jobs:", "+  build: { runs-on: ubuntu-latest }"],
        1,
        "  build: { runs-on: ubuntu-latest }",
    )
    assert not guardrails._is_workflow_job_flow_permission_escalation(
        [" jobs:", "+  build: { permissions: *full_write }"],
        1,
        "  build: { permissions: *full_write }",
    )
    assert guardrails._is_workflow_job_flow_permission_escalation(
        [
            " env:",
            "   FULL_WRITE: &full_write write-all",
            " jobs:",
            "+  build: { permissions: *full_write }",
        ],
        3,
        "  build: { permissions: *full_write }",
    )


def test_workflow_permission_alias_helpers_resolve_only_visible_values() -> None:
    lines = [
        " env:",
        "-  OLD: &old_write write",
        "   READ_ONLY: &read_only read",
        "   FULL: &full { contents: write }",
        "   BLOCK: &block",
        "@@ non-yaml hunk metadata",
        "-    issues: write",
        "     # visible comment",
        "     ",
        "     contents: write",
        "+permissions: *full",
    ]

    assert guardrails._yaml_flow_permission_map_escalates("{ contents: write }")
    assert not guardrails._yaml_flow_permission_map_escalates(
        '{ name: "contents: write" }'
    )
    assert (
        guardrails._mask_yaml_quoted_colon_values('"permissions": write-all')
        == '"permissions": write-all'
    )
    assert (
        guardrails._mask_yaml_quoted_colon_values('name: "permissions: write-all"')
        == 'name: "                      "'
    )
    assert (
        guardrails._mask_yaml_quoted_colon_values('name: "permissions: write-all')
        == 'name: "permissions: write-all'
    )
    assert guardrails._yaml_anchor_values_before(lines, 10) == {
        "read_only": "read",
        "full": "{ contents: write }",
        "block": "contents: write",
    }
    assert not guardrails._yaml_alias_resolves_to_escalation(
        lines, 10, "contents: write", top_level_permissions=False
    )
    assert guardrails._yaml_alias_resolves_to_escalation(
        lines, 10, "permissions: *full", top_level_permissions=True
    )
    assert guardrails._visible_permission_alias_reference_escalates(
        [
            "- permissions:",
            " permissions:",
            "@@ -1,3 +1,4 @@",
            "-  issues: *write_value",
            "   # comment",
            "+  contents: *write_value",
        ],
        "write_value",
        top_level_permissions=False,
    )
    assert guardrails._anchor_value_edit_escalates_in_section(
        " permissions: *perm\n+env:\n+  PERM: &perm write-all\n",
        "+  PERM: &perm write-all\n",
    )


def test_workflow_permission_context_helpers_track_active_yaml_stack() -> None:
    assert guardrails._visible_yaml_context(
        [
            " jobs:",
            "   build:",
            "     steps:",
            " on:",
            "   workflow_call:",
            "     inputs:",
        ],
        6,
    ) == [(0, "on"), (2, "workflow_call"), (4, "inputs")]


def test_workflow_permission_context_helpers_use_hunk_header_yaml_context() -> None:
    assert guardrails._visible_yaml_context(
        [
            "@@ -20,6 +20,7 @@ jobs:",
            "   build:",
            "     permissions:",
        ],
        3,
    ) == [(0, "jobs"), (2, "build"), (4, "permissions")]


def test_workflow_permission_context_ignores_nested_jobs_keys() -> None:
    lines = [
        " on:",
        "   workflow_call:",
        "     inputs:",
        "       jobs:",
        "+        permissions: write-all",
    ]

    assert not guardrails._is_workflow_permission_key_context(
        lines,
        4,
        "        permissions: write-all",
    )


def test_workflow_permission_context_accepts_indented_root_key_without_ancestor() -> None:
    match_text = "+  permissions: write-all\n"

    assert guardrails._match_has_workflow_permission_context(match_text)


def test_workflow_permission_context_skips_non_diff_parent_lines() -> None:
    match_text = " permissions:\n@@ -1,2 +1,3 @@\n+  contents: write\n"

    assert guardrails._match_has_workflow_permission_context(match_text)


def test_workflow_permission_context_skips_non_added_block_scalar_lines() -> None:
    match_text = (
        " permissions:\n"
        "+  contents: |-\n"
        "@@ -1,2 +1,3 @@\n"
        "-    read\n"
        "+    write\n"
    )

    assert guardrails._match_has_workflow_permission_context(match_text)


def test_workflow_permission_context_rejects_scope_not_under_parent() -> None:
    match_text = "   permissions:\n+  contents: write\n"

    assert not guardrails._match_has_workflow_permission_context(match_text)


def test_workflow_read_to_write_helper_rejects_non_key_scope_line() -> None:
    assert not guardrails._replaces_read_scope_with_write(
        ["-  contents: read", "+  write"],
        1,
        "  write",
    )


def test_contextless_scope_addition_helper_rejects_non_key_scope_line() -> None:
    assert not guardrails._is_contextless_permission_scope_addition(
        ["+  write"],
        0,
        "  write",
    )


def test_workflow_read_to_write_helper_skips_non_yaml_context_lines() -> None:
    assert guardrails._replaces_read_scope_with_write(
        ["@@ -1,3 +1,3 @@", "-  statuses: read", "+  statuses: write"],
        2,
        "  statuses: write",
    )


def test_workflow_read_to_write_helper_skips_keyless_diff_lines() -> None:
    assert guardrails._replaces_read_scope_with_write(
        ["+}", "-  statuses: read", "+  statuses: write"],
        2,
        "  statuses: write",
    )


def test_workflow_read_to_write_helper_rejects_parent_before_replacement() -> None:
    assert not guardrails._replaces_read_scope_with_write(
        [" env:", "+  statuses: write"],
        1,
        "  statuses: write",
    )


def test_workflow_read_to_write_helper_accepts_visible_permissions_parent() -> None:
    assert guardrails._replaces_read_scope_with_write(
        [" permissions:", "-  statuses: read", "+  statuses: write"],
        2,
        "  statuses: write",
    )


def test_scan_pr_diff_workflow_permissions_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_indented_root_permissions_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+  permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_quoted_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions: \"write-all\"\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_tagged_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions: !!str write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_quoted_key_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + '@@ -1,2 +1,3 @@\n+"permissions": write-all\n'
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_spaced_separator_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions : write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_anchored_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions: &all write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_job_permissions_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,6 +1,7 @@\n"
        + " jobs:\n"
        + "   build:\n"
        + "+    permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_reserved_word_job_id_permissions_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,6 +1,7 @@\n"
        + " jobs:\n"
        + "   run:\n"
        + "+    permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_call_input_named_jobs_permissions_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        + " on:\n"
        + "   workflow_call:\n"
        + "     inputs:\n"
        + "       jobs:\n"
        + "+        permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_inline_jobs_permission_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        + "+jobs: { build: { permissions: write-all } }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_jobs_permission_string_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        + '+jobs: { build: { name: "permissions: write-all", runs-on: ubuntu-latest } }\n'
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_indented_inline_jobs_permission_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        + "+  jobs: { build: { permissions: write-all } }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_jobs_permission_scope_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        + "+jobs: { build: { permissions: { contents: write } } }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_multiline_jobs_flow_permission_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,5 @@\n"
        + "+jobs: {\n"
        + "+  build: { permissions: write-all }\n"
        + "+}\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_multiline_jobs_flow_permission_scope_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,5 @@\n"
        + "+jobs: {\n"
        + "+  build: { permissions: { contents: write } }\n"
        + "+}\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_job_flow_permission_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        + " jobs:\n"
        + "+  build: { permissions: write-all, runs-on: ubuntu-latest }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_indented_jobs_job_flow_permission_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        + "   jobs:\n"
        + "+    build: { permissions: write-all, runs-on: ubuntu-latest }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_job_flow_permission_scope_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        + " jobs:\n"
        + "+  build: { permissions: { contents: write }, runs-on: ubuntu-latest }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_jobs_permission_alias_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        + "+jobs: { build: { permissions: *full_write } }\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_inline_jobs_permission_alias_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,5 @@\n"
        + "+env:\n"
        + "+  FULL_WRITE: &full_write write-all\n"
        + "+jobs: { build: { permissions: *full_write } }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_rename_into_workflows_flagged() -> None:
    diff_text = (
        "diff --git a/scripts/build.yml b/.github/workflows/build.yml\n"
        "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_wrong_case_directory_not_flagged() -> None:
    diff_text = (
        "diff --git a/.github/Workflows/ci.yml b/.github/Workflows/ci.yml\n"
        "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_permissions_quoted_path_flagged() -> None:
    diff_text = (
        'diff --git "a/.github/workflows/caf\\303\\251.yml" '
        '"b/.github/workflows/caf\\303\\251.yml"\n'
        "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_quoted_path_contents_write_flagged() -> None:
    diff_text = (
        'diff --git "a/.github/workflows/caf\\303\\251.yml" '
        '"b/.github/workflows/caf\\303\\251.yml"\n'
        "@@ -1,3 +1,4 @@\n permissions:\n+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_permissions_map_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        "+permissions: { contents: write }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_permissions_map_quoted_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        "+permissions: { contents: \"write\" }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_permissions_map_quoted_key_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        '+permissions: { "contents": write }\n'
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_inline_permissions_map_anchor_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        "+permissions: &full { contents: write }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_anchored_block_permissions_map_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,4 @@\n"
        "+permissions: &full\n"
        "+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_multiline_flow_permissions_map_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,4 @@\n"
        "+permissions: {\n"
        "+  contents: write\n"
        "+}\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_multiline_flow_permissions_trailing_comma_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,5 @@\n"
        "+permissions: {\n"
        "+  contents: write,\n"
        "+  issues: read\n"
        "+}\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_alias_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        + " env:\n"
        + "   FULL_WRITE: &full_write write-all\n"
        "+permissions: *full_write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_block_alias_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,4 +1,5 @@\n"
        + " env: &full_write\n"
        + "   contents: write\n"
        "+permissions: *full_write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_unresolved_alias_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n"
        "+permissions: *full_write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_non_escalating_alias_before_scope_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,6 +1,8 @@\n"
        + " env:\n"
        + "   READ_ONLY: &ro read\n"
        + "+permissions: *ro\n"
        + " permissions:\n"
        + "+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_anchor_value_edit_to_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,7 @@\n"
        + " env:\n"
        + "-  PERM: &perm read\n"
        + "+  PERM: &perm write\n"
        + " permissions: { contents: *perm }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_anchor_value_edit_to_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,7 @@\n"
        + " env:\n"
        + "-  PERM: &perm read-all\n"
        + "+  PERM: &perm write-all\n"
        + " permissions: *perm\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_block_anchor_value_edit_to_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,9 +1,9 @@\n"
        + " env:\n"
        + "   PERM: &perm\n"
        + "-    read\n"
        + "+    write\n"
        + " permissions: { contents: *perm }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_block_anchor_value_edit_helper_rejects_without_block_anchor() -> None:
    assert not guardrails._block_anchor_value_edit_escalates(
        [" env:", "+  write"],
        1,
        "  write",
    )


def test_block_anchor_value_edit_helper_skips_non_parent_lines() -> None:
    assert not guardrails._block_anchor_value_edit_escalates(
        ["@@ -1,2 +1,2 @@", "-  PERM: &perm", " # comment", "   nested: &perm", "+  write"],
        4,
        "  write",
    )


def test_block_anchor_value_edit_helper_rejects_without_visible_anchor() -> None:
    assert not guardrails._block_anchor_value_edit_escalates(
        ["@@ -1,2 +1,2 @@", "+  write"],
        1,
        "  write",
    )


def test_anchor_value_edit_in_section_detects_block_anchor_context() -> None:
    section_text = (
        WORKFLOW_DIFF_HEADER
        + " env:\n"
        + "   PERM: &perm\n"
        + "+    write\n"
        + " permissions: { contents: *perm }\n"
    )

    assert guardrails._anchor_value_edit_escalates_in_section(
        section_text,
        "   PERM: &perm\n+    write\n",
    )


def test_scan_pr_diff_workflow_anchor_value_edit_uses_file_section_context() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,5 +1,5 @@\n"
        + " env:\n"
        + "-  PERM: &perm read-all\n"
        + "+  PERM: &perm write-all\n"
        + "@@ -40,5 +40,5 @@\n"
        + " permissions: *perm\n"
        + " jobs:\n"
        + "   build:\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])
    second_section = "diff --git a/docs/readme.md b/docs/readme.md\n+doc\n"
    combined_diff = diff_text + "\n" + second_section
    assert guardrails._diff_file_section_at(
        combined_diff, combined_diff.index("+doc")
    ).startswith(second_section)


def test_scan_pr_diff_workflow_permissions_block_scalar_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,4 @@\n"
        "+permissions: |-\n"
        "+  write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_block_scalar_indent_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,4 @@\n"
        "+permissions: |2-\n"
        "+    write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_job_permissions_block_scalar_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,6 +1,8 @@\n"
        " jobs:\n"
        "   build:\n"
        "+    permissions: |-\n"
        "+      write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_contents_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_tagged_contents_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: !!str write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_block_scalar_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,5 @@\n"
        " permissions:\n"
        "+  contents: |-\n"
        "+    write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_block_scalar_indent_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,5 @@\n"
        " permissions:\n"
        "+  contents: >1+\n"
        "+   write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_spaced_separator_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents : write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_anchored_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: &w write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_alias_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,4 +1,5 @@\n"
        + " env:\n"
        + "   FULL_WRITE: &full_write write\n"
        + " permissions:\n"
        + "+  contents: *full_write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_block_alias_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,5 +1,6 @@\n"
        + " env:\n"
        + "   FULL_WRITE: &full_write\n"
        + "     write\n"
        + " permissions:\n"
        + "+  contents: *full_write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_alias_read_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,4 +1,5 @@\n"
        + " env:\n"
        + "   READ_ONLY: &read_only read\n"
        + " permissions:\n"
        + "+  contents: *read_only\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_scope_block_alias_read_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,5 +1,6 @@\n"
        + " env:\n"
        + "   READ_ONLY: &read_only\n"
        + "     read\n"
        + " permissions:\n"
        + "+  contents: *read_only\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_scope_unresolved_alias_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        + " permissions:\n"
        + "+  contents: *full_write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_permission_read_replaced_with_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,3 @@\n"
        + " permissions:\n"
        + "-  contents: read\n"
        + "+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_contents_replacement_without_parent_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -40,3 +40,3 @@\n"
        + "-  contents: read\n"
        + "+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_job_level_contents_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        " jobs:\n"
        "   build:\n"
        "     permissions:\n"
        "+      contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_indented_root_job_level_contents_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        "   jobs:\n"
        "     build:\n"
        "       permissions:\n"
        "+        contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_job_level_scope_with_jobs_hunk_header_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -20,6 +20,7 @@ jobs:\n"
        "   build:\n"
        "     permissions:\n"
        "+      contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_wide_indent_job_permission_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        " jobs:\n"
        "     build:\n"
        "       permissions:\n"
        "+        contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_deep_indent_job_permission_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        " jobs:\n"
        "             build:\n"
        "               permissions:\n"
        "+                contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_existing_job_permission_block_addition_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,8 +1,9 @@\n"
        " jobs:\n"
        "   build:\n"
        "     permissions:\n"
        "       contents: read\n"
        "+      issues: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_nested_job_permissions_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,8 +1,10 @@\n"
        " jobs:\n"
        "   build:\n"
        "     concurrency:\n"
        "+      permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_permission_block_with_read_only_scopes_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,5 +1,6 @@\n"
        + " permissions:\n"
        + "   models: read\n"
        + "   vulnerability-alerts: read\n"
        + "+  issues: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permission_child_flow_map_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        + " permissions:\n"
        + "+  { contents: write }\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_quoted_scope_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: 'write'\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_quoted_scope_key_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + '@@ -1,3 +1,4 @@\n permissions:\n+  "contents": write\n'
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_read_to_write_without_parent_context_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -20,3 +20,3 @@\n-  pull-requests: read\n+  pull-requests: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_none_to_write_without_parent_context_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -20,3 +20,3 @@\n-  contents: none\n+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_addition_without_parent_context_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -20,6 +20,7 @@\n"
        + "+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_env_replacement_named_write_scope_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,4 +1,4 @@\n"
        + " env:\n"
        + "-  contents: read\n"
        + "+  contents: write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_id_token_write_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/oidc.yml b/.github/workflows/oidc.yml\n"
        "@@ -1,3 +1,4 @@\n permissions:\n+  id-token: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_packages_write_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/publish.yml b/.github/workflows/publish.yml\n"
        "@@ -1,3 +1,4 @@\n permissions:\n+  packages: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_additional_write_scope_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/pages.yml b/.github/workflows/pages.yml\n"
        "@@ -1,3 +1,4 @@\n permissions:\n+  pages: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_existing_permission_on_context_line_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_permission_in_deletion_line_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,2 @@\n-permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_read_permission_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: read\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_run_write_output_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        "+      run: write-output.sh\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_script_literal_permission_word_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,4 @@\n"
        "+          contents: write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_reusable_input_named_contents_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        " jobs:\n"
        "   call:\n"
        "     uses: org/repo/.github/workflows/build.yml@v1\n"
        "     with:\n"
        "+      contents: write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_reusable_input_named_permissions_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        " jobs:\n"
        "   call:\n"
        "     uses: org/repo/.github/workflows/build.yml@v1\n"
        "     with:\n"
        "+      permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_call_input_permission_after_jobs_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,10 +1,11 @@\n"
        " jobs:\n"
        "   build:\n"
        "     runs-on: ubuntu-latest\n"
        " on:\n"
        "   workflow_call:\n"
        "     inputs:\n"
        "       token:\n"
        "+        permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_matrix_include_permission_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,11 +1,12 @@\n"
        " jobs:\n"
        "   test:\n"
        "     strategy:\n"
        "       matrix:\n"
        "         include:\n"
        "           - os: ubuntu-latest\n"
        "+            permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_job_output_named_permissions_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,7 +1,8 @@\n"
        " jobs:\n"
        "   build:\n"
        "     outputs:\n"
        "+      permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_job_secret_named_permissions_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,8 +1,9 @@\n"
        " jobs:\n"
        "   call:\n"
        "     uses: org/repo/.github/workflows/build.yml@v1\n"
        "     secrets:\n"
        "+      permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_env_named_contents_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,4 +1,5 @@\n"
        " env:\n"
        "+  contents: write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_env_named_write_scope_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,4 +1,5 @@\n"
        " env:\n"
        "+  issues: write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_unrelated_permissions_block_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,12 +1,13 @@\n"
        " permissions:\n"
        "   contents: read\n"
        " jobs:\n"
        "   call:\n"
        "     uses: org/repo/.github/workflows/build.yml@v1\n"
        "     with:\n"
        "+      contents: write\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_non_workflow_permission_write_not_flagged() -> None:
    diff_text = (
        "diff --git a/docs/example.yml b/docs/example.yml\n"
        "@@ -1,3 +1,4 @@\n"
        "+  contents: write\n"
        "diff --git a/policies/actions.md b/policies/actions.md\n"
        "@@ -1,3 +1,4 @@\n"
        "+permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_nested_workflow_path_permission_not_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/sub/ci.yml b/.github/workflows/sub/ci.yml\n"
        "@@ -1,2 +1,3 @@\n"
        "+permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_disabled_workflow_path_permission_not_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/ci.yml.disabled "
        "b/.github/workflows/ci.yml.disabled\n"
        "@@ -1,2 +1,3 @@\n"
        "+permissions: write-all\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_yml_deletion_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        +
        "deleted file mode 100644\n--- a/.github/workflows/ci.yml\n+++ /dev/null\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_yaml_extension_deletion_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/ci.yaml b/.github/workflows/ci.yaml\n"
        "deleted file mode 100644\n--- a/.github/workflows/ci.yaml\n+++ /dev/null\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_quoted_path_deletion_flagged() -> None:
    diff_text = (
        'diff --git "a/.github/workflows/caf\\303\\251.yml" '
        '"b/.github/workflows/caf\\303\\251.yml"\n'
        'deleted file mode 100644\n--- "a/.github/workflows/caf\\303\\251.yml"\n'
        "+++ /dev/null\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_rename_out_of_directory_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/ci.yml b/docs/ci.yml\n"
        "similarity index 100%\n"
        "rename from .github/workflows/ci.yml\n"
        "rename to docs/ci.yml\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_quoted_path_rename_out_of_directory_flagged() -> None:
    diff_text = (
        'diff --git "a/.github/workflows/caf\\303\\251.yml" "b/docs/cafe.yml"\n'
        "similarity index 100%\n"
        'rename from ".github/workflows/caf\\303\\251.yml"\n'
        "rename to docs/cafe.yml\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_rename_into_subdirectory_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/ci.yml b/.github/workflows/sub/ci.yml\n"
        "similarity index 100%\n"
        "rename from .github/workflows/ci.yml\n"
        "rename to .github/workflows/sub/ci.yml\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_rename_to_wrong_case_directory_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/ci.yml b/.github/Workflows/ci.yml\n"
        "similarity index 100%\n"
        "rename from .github/workflows/ci.yml\n"
        "rename to .github/Workflows/ci.yml\n"
    )

    _assert_diff_categories(diff_text, ["workflow_destruction"])


def test_scan_pr_diff_workflow_rename_within_directory_not_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/old.yml b/.github/workflows/new.yml\n"
        "similarity index 100%\n"
        "rename from .github/workflows/old.yml\n"
        "rename to .github/workflows/new.yml\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_quoted_path_rename_within_directory_not_flagged() -> None:
    diff_text = (
        'diff --git "a/.github/workflows/caf\\303\\251.yml" '
        '"b/.github/workflows/cafe.yml"\n'
        "similarity index 100%\n"
        'rename from ".github/workflows/caf\\303\\251.yml"\n'
        'rename to ".github/workflows/cafe.yml"\n'
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_workflow_modification_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        +
        "--- a/.github/workflows/ci.yml\n+++ b/.github/workflows/ci.yml\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_branch_protection_modification_flagged() -> None:
    diff_text = (
        "diff --git a/.github/branch-protection.yml "
        "b/.github/branch-protection.yml\n"
        "--- a/.github/branch-protection.yml\n"
        "+++ b/.github/branch-protection.yml\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_branch_protection_underscore_variant_flagged() -> None:
    diff_text = (
        "diff --git a/.github/branch_protection.yaml "
        "b/.github/branch_protection.yaml\n"
        "--- a/.github/branch_protection.yaml\n"
        "+++ b/.github/branch_protection.yaml\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_settings_yml_modification_flagged() -> None:
    diff_text = (
        "diff --git a/.github/settings.yml b/.github/settings.yml\n"
        "--- a/.github/settings.yml\n"
        "+++ b/.github/settings.yml\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_branch_protection_deletion_flagged() -> None:
    diff_text = (
        "diff --git a/.github/branch-protection.yml "
        "b/.github/branch-protection.yml\n"
        "deleted file mode 100644\n"
        "--- a/.github/branch-protection.yml\n"
        "+++ /dev/null\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_branch_protection_creation_flagged() -> None:
    diff_text = (
        "diff --git a/.github/settings.yml b/.github/settings.yml\n"
        "new file mode 100644\n"
        "--- /dev/null\n"
        "+++ b/.github/settings.yml\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_branch_protection_rename_only_from_flagged() -> None:
    diff_text = (
        "diff --git a/.github/branch-protection.yml b/docs/branch-protection.yml\n"
        "similarity index 100%\n"
        "rename from .github/branch-protection.yml\n"
        "rename to docs/branch-protection.yml\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_branch_protection_rename_only_to_flagged() -> None:
    diff_text = (
        "diff --git a/docs/settings.yml b/.github/settings.yml\n"
        "similarity index 100%\n"
        "rename from docs/settings.yml\n"
        "rename to .github/settings.yml\n"
    )

    _assert_diff_categories(diff_text, ["branch_protection_modification"])


def test_scan_pr_diff_unrelated_file_not_flagged() -> None:
    diff_text = (
        "diff --git a/src/foo.py b/src/foo.py\n"
        "--- a/src/foo.py\n"
        "+++ b/src/foo.py\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_documented_workflow_deletion_header_not_flagged() -> None:
    diff_text = (
        "diff --git a/docs/guide.md b/docs/guide.md\n"
        "@@ -1,4 +1,4 @@\n"
        " ```diff\n"
        " --- a/.github/workflows/ci.yml\n"
        " +++ /dev/null\n"
        " ```\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_non_workflow_yml_deletion_not_flagged() -> None:
    diff_text = (
        "diff --git a/some-other.yml b/some-other.yml\n"
        "--- a/some-other.yml\n+++ /dev/null\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_non_workflow_path_yml_deletion_not_flagged() -> None:
    diff_text = (
        "diff --git a/docs/example.yml b/docs/example.yml\n"
        "--- a/docs/example.yml\n+++ /dev/null\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_main_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@main\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_master_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@master\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_head_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@HEAD\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_branch_name_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@develop\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_key_after_step_name_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "+      - name: Checkout\n"
        "+        uses: actions/checkout@main\n"
    )

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_double_quoted_main_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + '+      - uses: "actions/checkout@main"\n'

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_single_quoted_main_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: 'actions/checkout@main'\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_quoted_uses_key_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + '+      - "uses": actions/checkout@main\n'

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_spaced_uses_key_separator_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses : actions/checkout@main\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_anchored_value_main_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: &checkout actions/checkout@main\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_tagged_value_main_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: !!str actions/checkout@main\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_composite_action_unpinned_uses_flagged() -> None:
    diff_text = (
        "diff --git a/.github/actions/build/action.yml "
        "b/.github/actions/build/action.yml\n"
        "@@ -1,3 +1,4 @@\n"
        "+    - uses: actions/checkout@main\n"
    )

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_top_level_composite_action_unpinned_uses_flagged() -> None:
    diff_text = (
        "diff --git a/.github/actions/action.yml b/.github/actions/action.yml\n"
        "@@ -1,3 +1,4 @@\n"
        "+    - uses: actions/checkout@main\n"
    )

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_flow_mapping_main_ref_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - { uses: actions/checkout@main }\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_flow_mapping_semver_ref_not_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - { uses: actions/checkout@v1 }\n"

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_flow_mapping_quoted_value_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + '+      - { name: "{ uses: actions/checkout@main }" }\n'
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_semver_tag_v1_not_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@v1\n"

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_semver_tag_v1_2_3_not_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@v1.2.3\n"

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_commit_sha_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "+      - uses: actions/checkout@a1b2c3d4e5f6789012345678901234567890abcd\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_short_sha_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "+      - uses: actions/checkout@a1b2c3d\n"

    _assert_diff_categories(diff_text, ["dangerous_action_external_install"])


def test_scan_pr_diff_action_in_context_line_not_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "  - uses: actions/checkout@main\n"

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_in_deletion_line_not_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "-  - uses: actions/checkout@main\n"

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_in_run_block_scalar_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,5 @@\n"
        "       - run: |\n"
        "+          cat <<'YAML'\n"
        "+          uses: actions/checkout@main\n"
        "+          YAML\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_nested_in_run_block_scalar_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,3 +1,5 @@\n"
        "       - run: |\n"
        "+          cat <<'YAML'\n"
        "+            uses: actions/checkout@main\n"
        "+          YAML\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_action_block_scalar_helper_ignores_non_yaml_lines() -> None:
    assert not guardrails._yaml_line_is_in_block_scalar(["diff --git a b"], 0)


def test_scan_pr_diff_action_block_scalar_helper_skips_deleted_and_comment_context() -> None:
    lines = [
        "-      - run: |",
        "       # comment",
        "       ",
        "       uses: actions/checkout@main",
    ]

    assert not guardrails._yaml_line_is_in_block_scalar(lines, 3)


def test_scan_pr_diff_action_uses_helper_handles_out_of_range_match() -> None:
    assert not guardrails._action_uses_match_is_yaml_key(WORKFLOW_DIFF_HEADER, 999)


def test_scan_pr_diff_action_uses_helper_ignores_diff_header_match() -> None:
    assert not guardrails._action_uses_match_is_yaml_key(WORKFLOW_DIFF_HEADER, 0)


def test_scan_pr_diff_action_example_in_docs_not_flagged() -> None:
    diff_text = (
        "diff --git a/docs/actions.md b/docs/actions.md\n"
        "--- a/docs/actions.md\n"
        "+++ b/docs/actions.md\n"
        "@@ -1,2 +1,3 @@\n"
        "+      - uses: actions/checkout@main\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_both_patterns_in_same_diff_returns_both() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        +
        "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
        "diff --git a/.github/workflows/old.yml b/.github/workflows/old.yml\n"
        "deleted file mode 100644\n--- a/.github/workflows/old.yml\n+++ /dev/null\n"
    )

    _assert_diff_categories(
        diff_text,
        [
            "permissions_escalation",
            "workflow_destruction",
        ],
    )


def test_scan_pr_diff_all_four_patterns_no_overlap() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        + "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
        "diff --git a/.github/workflows/old.yml b/.github/workflows/old.yml\n"
        "deleted file mode 100644\n"
        "--- a/.github/workflows/old.yml\n"
        "+++ /dev/null\n"
        "diff --git a/.github/branch-protection.yml "
        "b/.github/branch-protection.yml\n"
        "--- a/.github/branch-protection.yml\n"
        "+++ b/.github/branch-protection.yml\n"
        "diff --git a/.github/workflows/release.yml "
        "b/.github/workflows/release.yml\n"
        "@@ -1,2 +1,3 @@\n"
        "+      - uses: actions/checkout@main\n"
    )

    _assert_diff_categories(
        diff_text,
        [
            "branch_protection_modification",
            "dangerous_action_external_install",
            "permissions_escalation",
            "workflow_destruction",
        ],
    )


def test_scan_pr_diff_clean_diff_returns_empty_list() -> None:
    diff_text = (
        "diff --git a/src/example.py b/src/example.py\n"
        "index 1111111..2222222 100644\n"
        "--- a/src/example.py\n"
        "+++ b/src/example.py\n"
        "@@ -1,3 +1,4 @@\n"
        " def run() -> None:\n"
        "+    print('ok')\n"
        "     return None\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_clean_realistic_diff_returns_empty_list() -> None:
    diff_text = (
        "diff --git a/src/daemon/recovery.py b/src/daemon/recovery.py\n"
        "index 1111111..2222222 100644\n"
        "--- a/src/daemon/recovery.py\n"
        "+++ b/src/daemon/recovery.py\n"
        "@@ -10,6 +10,8 @@ def recover_state() -> None:\n"
        "     tasks = load_tasks()\n"
        "+    for task in tasks:\n"
        "+        task.refresh_status()\n"
        "     return None\n"
        "diff --git a/tests/test_recovery.py b/tests/test_recovery.py\n"
        "index 3333333..4444444 100644\n"
        "--- a/tests/test_recovery.py\n"
        "+++ b/tests/test_recovery.py\n"
        "@@ -1,3 +1,6 @@\n"
        " def test_recover_state() -> None:\n"
        "+    result = recover_state()\n"
        "+\n"
        "+    assert result is None\n"
    )

    assert scan_pr_diff(diff_text) == []


def test_scan_pr_diff_helper_is_callable_with_unified_diff() -> None:
    """The dispatcher signature must mirror scan_stdout so callers can
    treat both diff and stdout signals uniformly."""
    diff_text = (
        "diff --git a/src/a.py b/src/a.py\n"
        "@@ -1,1 +1,1 @@\n"
        "-old\n"
        "+new\n"
    )

    result = scan_pr_diff(diff_text)

    assert isinstance(result, list)


def test_scan_pr_diff_populated_catalogue_emits_violation_with_clipped_excerpt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When PR-290b/c register a pattern, scan_pr_diff surfaces a
    GuardrailViolation with the diff-matching excerpt clipped to the
    shared 200-char limit."""
    monkeypatch.setattr(
        guardrails,
        "_DIFF_PATTERNS",
        {"workflow_permissions_write_all": re.compile(r"permissions:\s*write-all")},
    )
    monkeypatch.setattr(
        guardrails,
        "_DIFF_RULES",
        {"workflow_permissions_write_all": "Workflow permissions write-all"},
    )
    diff_text = (
        "diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml\n"
        "+permissions: write-all\n"
    )

    violations = scan_pr_diff(diff_text)

    assert len(violations) == 1
    violation = violations[0]
    assert isinstance(violation, GuardrailViolation)
    assert violation.tier == 1
    assert violation.category == "workflow_permissions_write_all"
    assert violation.excerpt == "permissions: write-all"
    assert violation.rule == "Workflow permissions write-all"


def test_scan_pr_diff_excerpt_truncation_200_chars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The excerpt clip helper enforces the same 200-char cap as
    scan_stdout so dashboard payloads stay bounded regardless of which
    signal produced the violation."""
    monkeypatch.setattr(
        guardrails,
        "_DIFF_PATTERNS",
        {"long": re.compile(r"X+")},
    )
    monkeypatch.setattr(guardrails, "_DIFF_RULES", {"long": ""})
    diff_text = "+" + ("X" * 500)

    violations = scan_pr_diff(diff_text)

    assert len(violations) == 1
    assert len(violations[0].excerpt) <= 200
