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
