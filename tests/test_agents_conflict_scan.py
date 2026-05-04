"""Tests for src.mcp.scans.scan_for_conflicts.

Each anti-pattern recognized by the scanner has at least one positive
case below; the clean-spec case anchors the false-positive direction.
"""

from __future__ import annotations

from src.mcp.scans import scan_for_conflicts


_CLEAN_SPEC = """# PR-999: Example task

Branch: pr-999-example
- Type: feature
- Complexity: low
- Depends on: none
- Priority: 3
- Coder: claude

## Problem

Implement a small refactor of the runner.

## Scope

Single change to src/runner.py.
"""


def test_clean_spec_returns_no_violations():
    assert scan_for_conflicts(_CLEAN_SPEC) == []


def test_draft_pr_flag_detected():
    body = "Run `gh pr create --draft --title 'wip'` to open the PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" in types


def test_draft_pr_text_detected():
    body = "Create a draft PR while iterating, then mark ready."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" in types


def test_force_push_main_detected():
    body = "If history diverges, run git push --force origin main."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_detected_force_after_main():
    body = "Recovery: git push origin main --force when needed."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_detected_short_flag_before_main():
    body = "Run git push -f origin main to overwrite history."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_detected_short_flag_after_main():
    body = "Run git push origin main -f when in doubt."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_short_flag_requires_standalone_token():
    """``-foo`` must not satisfy the ``-f`` arm of the alternation."""
    body = "git push --some-flag=-foo origin main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_branch_substring_main_not_flagged():
    """``feature/main-fix`` shares the substring ``main`` but is a
    distinct branch; the force flag must not mark it as targeting
    the protected ``main`` branch."""
    body = "git push --force origin feature/main-fix"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_dashed_branch_with_main_substring_not_flagged():
    """``release-main-2026`` contains ``main`` between dashes; not the
    protected branch."""
    body = "git push -f origin release-main-2026"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_src_main_dst_other_not_flagged():
    """``main:other`` pushes ``main`` to ``other`` — destination is
    ``other``, not main. Already covered for the ``+`` form; verify
    the ``--force`` form too."""
    body = "git push --force origin main:other"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_full_ref_with_force_flag_detected():
    """``--force ... refs/heads/main`` resolves to dst ``main``."""
    body = "git push --force origin refs/heads/main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_two_sided_refspec_detected():
    """``master:main`` pushes master TO main — destination is the
    protected branch."""
    body = "git push --force origin master:main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_refspec_plus_with_dst_main():
    """``+HEAD:main`` is a force-push per ``git push -h``."""
    body = "Recovery: git push origin +HEAD:main when histories diverge."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_refspec_plus_main_shorthand():
    """``+main`` (no colon) is shorthand force-push to main."""
    body = "git push origin +main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_refspec_plus_full_ref():
    """``+refs/heads/main`` resolves to dst ``main``."""
    body = "git push origin +refs/heads/main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_refspec_plus_dst_not_main_ignored():
    """``+main:other`` updates ``other``, not ``main`` - do not flag."""
    body = "git push origin +main:other"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_force_if_includes_alone_not_flagged():
    """``--force-if-includes`` alone is a no-op (only meaningful with
    ``--force-with-lease``); it must not satisfy the ``--force`` arm."""
    body = "git push --force-if-includes origin main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_force_with_lease_detected():
    """``--force-with-lease`` is still a force-push and must be flagged."""
    body = "git push --force-with-lease origin main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_feature_branch_then_prose_about_main_not_flagged():
    """``main`` mentioned in prose after the push command (separated
    by a comma) is not part of the refspec, so a feature-branch
    force-push must not be flagged."""
    body = (
        "git push --force-with-lease origin feature/foo, "
        "then open a PR to main."
    )
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_feature_branch_inline_comment_about_main_not_flagged():
    """``#`` opens a shell comment, so ``main`` after it is prose, not
    part of the refspec; a feature-branch force-push followed by an
    inline comment that mentions ``main`` must not be flagged."""
    body = (
        "git push --force-with-lease origin feature/foo "
        "# then open PR to main"
    )
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_detected_with_many_flags_before_refspec():
    """A force-push to ``main`` with six or more flag tokens before the
    refspec must still be flagged. Prior to this PR the scanner walked
    at most five intermediate arg tokens, which let real-world
    flag-heavy invocations slip past detection. The cap is removed so
    flag stuffing no longer bypasses the guardrail."""
    body = (
        "git push --force --set-upstream --atomic --follow-tags "
        "--verbose --quiet origin main"
    )
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_feature_branch_separatorless_prose_flagged_as_false_positive():
    """Prose continuation without a sentence separator (``,``, ``;``,
    ``|``, ``&``, or a ``#`` shell comment) is flagged as a false
    positive. The scanner unbounded its arg-token walk so flag-heavy
    real force-pushes cannot bypass detection; the trade-off is that
    a benign feature-branch push followed by separator-less prose
    that ends with ``main.`` will be flagged. Per the task spec's v1
    notes, false positives on don't-do-this commentary are
    acceptable -- the operator dismisses them -- while a force-push
    to main slipping past the scanner is not."""
    body = (
        "git push --force-with-lease origin feature/foo "
        "then open PR to main."
    )
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_force_push_main_alt_detected():
    body = "Operator may force-push to main as a last resort."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main_alt" in types


def test_no_verify_detected():
    body = "If pre-commit complains, run git commit --no-verify -m 'fix'."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "no_verify_commit" in types


def test_skip_ci_detected():
    body = "We can skip CI for this trivial change."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" in types


def test_skip_ci_negated_do_not_not_flagged():
    """``Do not skip CI`` restates the AGENTS.md rule -- it must not
    be flagged as a violation. Prior to this PR the regex matched
    any ``skip CI`` regardless of context, which rejected valid
    task specs that documented the rule in negative form."""
    body = "Do not skip CI under any circumstances."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_dont_not_flagged():
    body = "Don't skip CI even on trivial changes."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_never_not_flagged():
    body = "Never skip CI on this branch."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_cannot_not_flagged():
    body = "You cannot skip CI; the daemon enforces it."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_must_not_not_flagged():
    body = "Coders must not skip CI before opening a PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_should_not_not_flagged():
    body = "Reviewers should not skip CI even when in a hurry."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_bypass_form_not_flagged():
    """Negation lookbehinds apply to all three verbs (skip / bypass /
    ignore), not just ``skip``."""
    body = "Do not bypass CI by retrying the failed run."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_negated_typographic_apostrophe_not_flagged():
    """Smart-quote apostrophe (``’``) in ``don’t`` must also
    suppress the match."""
    body = "Don’t skip CI under any circumstances."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_marker_detected():
    body = "Title: Refactor stub [skip ci]"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci_commit_msg" in types


def test_ci_skip_marker_detected():
    body = "Commit message: 'docs tweak [ci skip]'"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci_commit_msg" in types


def test_auto_merge_dirty_detected():
    body = "Configure auto-merge even when checks are failing."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "auto_merge_dirty" in types


def test_multiple_violations_all_returned():
    body = (
        "Step 1: gh pr create --draft --title 'wip'.\n"
        "Step 2: git commit --no-verify when needed.\n"
        "Step 3: skip CI if it is flaky.\n"
    )
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert {"draft_pr_flag", "no_verify_commit", "skip_ci"}.issubset(types)


def test_case_insensitive_match():
    body = "Run GH PR CREATE --DRAFT to open the PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" in types


def test_excerpt_truncated_to_80_chars():
    long_line = "gh pr create --draft " + "x" * 200
    violations = scan_for_conflicts(long_line)
    assert violations, "expected at least one violation"
    for v in violations:
        assert len(v.line_excerpt) <= 80


def test_violation_carries_rule_reference():
    """Each violation includes a non-empty AGENTS.md rule reference."""
    body = "gh pr create --draft"
    violations = scan_for_conflicts(body)
    assert violations
    for v in violations:
        assert v.rule
        assert isinstance(v.rule, str)


def test_force_push_main_detected_across_shell_continuation():
    """``\\<newline>`` shell continuation must not let a force-push to
    main escape detection by splitting the flag and refspec across
    physical lines."""
    body = "git push --force \\\n    origin main\n"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_draft_pr_flag_detected_across_shell_continuation():
    """Draft-PR flag split across a shell continuation must still match."""
    body = "gh pr create \\\n    --draft --title wip\n"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" in types


def test_no_verify_detected_across_shell_continuation():
    """``--no-verify`` on a continued ``git commit`` must still match."""
    body = "git commit -m 'fix' \\\n    --no-verify\n"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "no_verify_commit" in types
