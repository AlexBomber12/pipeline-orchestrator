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


def test_draft_pr_text_negated_do_not_not_flagged():
    """``Do not create a draft PR`` restates the AGENTS.md rule and
    must not be flagged. Prior to this fix the regex matched any
    ``create ... draft PR`` phrase regardless of context, rejecting
    valid task specs that documented the rule in negative form."""
    body = "Do not create a draft PR — open it ready."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" not in types


def test_draft_pr_text_negated_dont_not_flagged():
    body = "Don't create a draft PR even when iterating."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" not in types


def test_draft_pr_text_negated_never_not_flagged():
    body = "Never create a draft PR; PRs must be ready."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" not in types


def test_draft_pr_text_negated_must_not_not_flagged():
    body = "Coders must not create a draft PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" not in types


def test_draft_pr_text_negated_typographic_apostrophe_not_flagged():
    """Smart-quote apostrophe (``’``) in ``don’t`` must also
    suppress the match."""
    body = "Don’t create a draft PR while iterating."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" not in types


def test_scan_flags_create_draft_pull_request():
    body = "Coders should create a draft pull request first."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pull_request_text" in types


def test_scan_flags_create_the_draft_pull_request():
    body = "Coders should create the draft pull request first."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pull_request_text" in types


def test_scan_negated_create_draft_pull_request_not_flagged():
    body = "Coders must never create a draft pull request."
    assert scan_for_conflicts(body) == []


def test_scan_create_draft_pr_short_form_still_uses_existing_pattern():
    body = "Coders should create a draft PR first."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" in types


def test_scan_flags_convert_pr_to_draft():
    body = "After tests pass, convert the PR to draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_convert" in types


def test_scan_flags_convert_pull_request_to_draft():
    body = "After tests pass, convert pull request to draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_convert" in types


def test_scan_flags_converted_pr_to_draft_past_tense():
    body = "Codex converted the PR to draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_convert" in types


def test_scan_flags_converting_pr_to_draft_progressive():
    body = "Continue after converting the PR to draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_convert" in types


def test_scan_negated_do_not_convert_pr_to_draft_not_flagged():
    body = "Do not convert PR to draft."
    assert scan_for_conflicts(body) == []


def test_scan_open_as_draft_without_pr_context_not_flagged():
    body = "Open as draft architecture notes before implementation."
    assert scan_for_conflicts(body) == []


def test_scan_flags_open_as_draft_clause():
    body = "Open as draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_pr_as_draft():
    body = "Open the PR as draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_it_as_a_draft_clause():
    body = "Open it as a draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_as_draft_with_then_continuation():
    body = "Open as draft then request review."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_as_draft_with_comma_continuation():
    body = "Open as draft, then request review."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_as_draft_pr():
    body = "Open as draft PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_as_draft_pr_with_and_continuation():
    body = "Open as draft PR and request review."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_pull_request_as_a_draft():
    body = "Open the pull request as a draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_flags_open_a_pull_request_as_draft():
    body = "Open a pull request as draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_open_as" in types


def test_scan_negated_do_not_open_as_draft_not_flagged():
    body = "Do not open the PR as draft."
    assert scan_for_conflicts(body) == []


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


def test_force_push_main_dot_suffix_branch_not_flagged():
    """``main.old`` is a distinct branch from the protected ``main``;
    a force-push to ``main.old`` must not be flagged. Prior to this
    fix the boundary class did not include ``.``-then-word-char, so
    dot-suffixed branch names were treated as the protected ``main``
    branch."""
    body = "git push --force origin main.old"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_dot_suffix_branch_short_flag_not_flagged():
    """Same as above but with ``-f`` short flag."""
    body = "git push -f origin main.fix"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_dot_suffix_branch_plus_form_not_flagged():
    """``+main.old`` is the plus-prefix force form on a distinct
    branch; must not be flagged."""
    body = "git push origin +main.old"
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


def test_force_push_main_remote_named_main_not_flagged():
    """``git push --force main feature/foo`` pushes ``feature/foo`` to
    a remote named ``main``; the destination branch is ``feature/foo``,
    not the protected ``main`` branch. ``git push`` syntax is
    ``git push [<repository> [<refspec>...]]``, so the first non-flag
    positional is the remote name and only subsequent positionals are
    refspecs. Repos whose remote is named ``main`` (instead of the
    default ``origin``) must not have compliant specs rejected by the
    scanner."""
    body = "git push --force main feature/foo"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_remote_named_main_short_flag_not_flagged():
    """Same as above, with the ``-f`` short flag."""
    body = "git push -f main feature/foo"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_remote_named_main_no_refspec_not_flagged():
    """``git push --force main`` (no explicit refspec) targets a
    remote named ``main``; the destination branch is the upstream of
    the current branch, which cannot be determined from the spec.
    Per the v1 trade-off the scanner does not flag this case,
    otherwise valid specs that push to a main-named remote would be
    rejected outright."""
    body = "git push --force main"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_remote_and_refspec_both_main_flagged():
    """``git push --force main main`` -- first ``main`` is the remote
    name, second ``main`` is the refspec targeting the protected
    branch. The destination IS ``main``, so the scanner must still
    flag this."""
    body = "git push --force main main"
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


def test_draft_pr_flag_negated_do_not_not_flagged():
    """``Do not run gh pr create --draft`` restates the AGENTS.md
    rule -- with a verb (``run``) between the negation and the
    command -- and must not be flagged. Prior to this fix command
    patterns matched the literal ``gh pr create --draft`` regardless
    of context, rejecting compliant specs that documented the rule
    in negative form."""
    body = "Do not run gh pr create --draft when iterating."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" not in types


def test_draft_pr_flag_negated_never_not_flagged():
    body = "Never invoke gh pr create --draft on this repo."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" not in types


def test_no_verify_negated_never_use_not_flagged():
    """``Never use git commit --no-verify`` restates the AGENTS.md
    rule -- with a verb (``use``) between the negation and the
    command -- and must not be flagged."""
    body = "Never use git commit --no-verify in this repo."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "no_verify_commit" not in types


def test_no_verify_negated_do_not_not_flagged():
    body = "Do not run git commit --no-verify; the hook is mandatory."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "no_verify_commit" not in types


def test_force_push_main_negated_not_flagged():
    """``Do not run git push --force origin main`` restates the
    AGENTS.md rule and must not be flagged."""
    body = "Do not run git push --force origin main."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" not in types


def test_force_push_main_alt_negated_not_flagged():
    """``Never force-push to main`` restates the AGENTS.md rule and
    must not be flagged. The prose pattern previously fired on any
    ``force-push ... main`` mention regardless of negation."""
    body = "Never force-push to main."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main_alt" not in types


def test_auto_merge_dirty_negated_not_flagged():
    """``Do not configure auto-merge with failing checks`` restates
    the AGENTS.md rule and must not be flagged."""
    body = "Do not configure auto-merge even when checks are failing."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "auto_merge_dirty" not in types


def test_negation_in_previous_clause_does_not_suppress():
    """A negation in a PRIOR clause (separated by ``.``) must not
    suppress a subsequent positive instruction. The clause boundary
    confines the negation lookup, so ``Do not skip CI. Run skip CI
    when forced.`` flags the second occurrence."""
    body = "Do not skip CI. Skip CI when CI is broken."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    # First occurrence is suppressed (Do not skip CI); second is not.
    assert "skip_ci" in types
    # Exactly one violation, not two.
    skip_ci_count = sum(
        1 for v in violations if v.violation_type == "skip_ci"
    )
    assert skip_ci_count == 1


def test_unrelated_negation_in_prior_subclause_does_not_suppress():
    """A negation that grammatically belongs to a prior sub-clause
    (separated from the match by a comma) must not suppress the
    match. ``If tests can not pass quickly, skip CI for this change.``
    contains ``can not``, but it negates ``pass quickly``, not the
    matched ``skip CI`` instruction; the scanner must still flag the
    real violation. Prior to this fix the negation lookup walked back
    to the closest sentence-level boundary and treated the entire
    comma-spanning sentence as a single clause, so any unrelated
    negation in an introductory sub-clause silently suppressed every
    pattern."""
    body = "If tests can not pass quickly, skip CI for this change."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" in types


def test_unrelated_negation_in_prior_subclause_force_push_not_suppressed():
    """Same shape as the ``skip_ci`` case but for the ``force_push_main``
    pattern: an unrelated negation in an introductory sub-clause must
    not suppress a real force-push to main."""
    body = (
        "If you do not have a clean tree, "
        "git push --force origin main to recover."
    )
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_unrelated_negation_in_prior_subclause_draft_pr_not_suppressed():
    """Same shape but for the ``draft_pr_text`` pattern: ``don't``
    governs ``forget``, not ``create a draft PR``."""
    body = "If you don't forget the title, create a draft PR for review."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" in types


def test_skip_ci_double_negative_dont_forget_to_flagged():
    """``Don't forget to skip CI`` contains a lexical negation but the
    inverter ``forget to`` flips the semantics: the spec is requiring
    the violation. Prior to this fix the bare negation suppressed the
    match and a conflicting spec passed ``validate_task_spec``."""
    body = "Don't forget to skip CI on trivial changes."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" in types


def test_draft_pr_flag_double_negative_never_fail_to_flagged():
    """``Never fail to run gh pr create --draft`` is a double-negative
    instruction requiring the AGENTS.md violation. The inverter
    ``fail to`` cancels ``Never`` and the violation must be flagged."""
    body = "Never fail to run gh pr create --draft on this branch."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" in types


def test_force_push_main_double_negative_dont_forget_flagged():
    """``Don't forget to git push --force origin main`` requires the
    force-push despite the leading ``Don't``."""
    body = "Don't forget to git push --force origin main when stuck."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "force_push_main" in types


def test_no_verify_double_negative_must_not_neglect_to_flagged():
    """``Coders must not neglect to run git commit --no-verify`` is a
    double-negative instruction requiring the violation. ``neglect to``
    cancels ``must not``."""
    body = "Coders must not neglect to run git commit --no-verify."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "no_verify_commit" in types


def test_draft_pr_text_double_negative_dont_hesitate_flagged():
    """``Don't hesitate to create a draft PR`` requires creating a
    draft PR — the inverter ``hesitate to`` flips the negation."""
    body = "Don't hesitate to create a draft PR while iterating."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_text" in types


def test_skip_ci_negation_with_unrelated_forget_does_not_fire():
    """``forget`` without ``to`` is not the inverter pattern. ``Do
    not skip CI; I won't forget that rule.`` keeps the suppression on
    the first sub-clause because no inverter sits between ``Do not``
    and ``skip CI``."""
    body = "Do not skip CI under any circumstances."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_avoid_purpose_clause_flagged():
    """``To avoid delays, skip CI`` instructs the operator to skip CI;
    ``avoid`` governs ``delays``, not ``skip CI``. ``avoid`` is not a
    reliable negation token and must not suppress the match, otherwise
    a compliant-looking purpose clause silently lets a real violation
    pass ``validate_task_spec``."""
    body = "To avoid delays, skip CI on trivial changes."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" in types


def test_draft_pr_avoid_purpose_clause_flagged():
    """``Avoid merge conflicts by running gh pr create --draft``
    instructs the operator to open a draft PR; ``avoid`` governs
    ``merge conflicts``, not the command. The scanner must still flag
    the draft-PR violation."""
    body = "Avoid merge conflicts by running gh pr create --draft."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "draft_pr_flag" in types


def test_skip_ci_nearest_negation_governs_match_not_first():
    """``Don't forget to run tests and do not skip CI`` contains two
    negations: the leading ``Don't ... forget to`` is a double-negative
    re-asserting ``run tests``, while the trailing ``do not`` directly
    prohibits ``skip CI``. The scanner must use the NEAREST negation
    (``do not``) when evaluating the inverter check, not the first
    (``Don't``); otherwise the leading inverter cancels the later
    prohibition and a compliant spec is rejected as a false
    positive."""
    body = "Don't forget to run tests and do not skip CI."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


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


def test_skip_ci_bracketed_marker_not_flagged_as_skip_ci():
    """``[skip ci]`` is the bracketed marker form already covered by
    ``skip_ci_commit_msg``. The plain-language ``skip_ci`` pattern must
    NOT also fire on it, otherwise compliant specs that document the
    prohibition (e.g. ``Do not use [skip ci] markers``) produce a
    duplicate finding and ``validate_task_spec`` returns valid=False
    for prose that restates the rule."""
    body = "Do not use [skip ci] markers in commit messages."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci" not in types


def test_skip_ci_marker_detected():
    body = "Title: Refactor stub [skip ci]"
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "skip_ci_commit_msg" in types
    # Bracketed marker must NOT also fire ``skip_ci``; that would be a
    # duplicate finding for the same offending substring.
    assert "skip_ci" not in types


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


def test_scan_flags_force_merge():
    body = "force-merge the PR after review."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_flags_force_merge_with_space():
    body = "force merge regardless of CI."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_flags_force_merge_with_multiple_spaces():
    body = "force   merge the PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_flags_bare_force_merge_now():
    body = "please force merge now."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_flags_merge_despite_failing_checks():
    body = "merge despite failing checks."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_flags_merge_with_red_ci():
    body = "merge with red CI."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_flags_merge_with_broken_tests():
    body = "merge with broken tests."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_negated_do_not_force_merge_not_flagged():
    body = "Coders must not force-merge under any circumstances."
    assert scan_for_conflicts(body) == []


def test_scan_force_merge_commit_guidance_not_flagged():
    body = "Use --no-ff to force merge commits when preserving branch history."
    assert scan_for_conflicts(body) == []


def test_scan_force_merge_pr_commit_guidance_not_flagged():
    body = "Use --no-ff to force merge PR commits for traceability."
    assert scan_for_conflicts(body) == []


def test_scan_force_merge_qualified_commit_guidance_not_flagged():
    body = "Use --no-ff to force merge feature branch commits for traceability."
    assert scan_for_conflicts(body) == []


def test_scan_force_merge_pr_commits_with_failing_checks_flagged():
    body = "force-merge the PR commits even with failing checks."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_no_ff_force_merge_commits_with_failing_checks_flagged():
    body = "Use --no-ff to force merge commits even with failing checks."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_no_ff_force_merge_commits_with_wrapped_failing_checks_flagged():
    body = "Use --no-ff to force merge commits\neven with failing checks."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


def test_scan_no_ff_force_merge_commits_with_checks_enabled_not_flagged():
    body = "Use --no-ff to force merge commits with checks enabled."
    assert scan_for_conflicts(body) == []


def test_scan_no_ff_force_merge_then_force_merge_pr_flagged():
    body = "Use --no-ff to force merge commits for history, then force-merge the PR."
    violations = scan_for_conflicts(body)
    types = {v.violation_type for v in violations}
    assert "merge_dirty_alt" in types


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
