from __future__ import annotations

import re
from dataclasses import FrozenInstanceError

import pytest

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


def test_scan_pr_diff_workflow_permissions_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_quoted_write_all_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,2 +1,3 @@\n+permissions: \"write-all\"\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_permissions_rename_into_workflows_flagged() -> None:
    diff_text = (
        "diff --git a/scripts/build.yml b/.github/workflows/build.yml\n"
        "@@ -1,2 +1,3 @@\n+permissions: write-all\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_contents_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: write\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_quoted_scope_write_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER + "@@ -1,3 +1,4 @@\n permissions:\n+  contents: 'write'\n"
    )

    _assert_diff_categories(diff_text, ["permissions_escalation"])


def test_scan_pr_diff_workflow_scope_write_without_parent_context_flagged() -> None:
    diff_text = WORKFLOW_DIFF_HEADER + "@@ -20,3 +20,4 @@\n+  issues: write\n"

    _assert_diff_categories(diff_text, ["permissions_escalation"])


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


def test_scan_pr_diff_workflow_rename_out_of_directory_flagged() -> None:
    diff_text = (
        "diff --git a/.github/workflows/ci.yml b/docs/ci.yml\n"
        "similarity index 100%\n"
        "rename from .github/workflows/ci.yml\n"
        "rename to docs/ci.yml\n"
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


def test_scan_pr_diff_workflow_modification_not_flagged() -> None:
    diff_text = (
        WORKFLOW_DIFF_HEADER
        +
        "--- a/.github/workflows/ci.yml\n+++ b/.github/workflows/ci.yml\n"
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
