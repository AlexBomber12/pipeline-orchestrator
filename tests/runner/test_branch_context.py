"""PR-209: Branch mismatch regression tests.

These tests pin the divergence-diagnostic behavior delivered by the
PR-222 ``BranchContext`` refactor. The 4 branch concepts each test
tracks:

- ``base_branch``        — ``repo_config.branch`` (typically ``"main"``)
- ``task_branch``        — ``QueueTask.branch`` (from the task header)
- ``current_git_branch`` — branch checked out on the working tree
                           (``git rev-parse --abbrev-ref HEAD``)
- ``pr_head_branch``     — ``PRInfo.branch`` (the PR head, if any)

After PR-222, scenarios where ``BranchContext.from_runner`` can name
all four surfaces (CODING case-A and the dirty-tree auto-recovery)
surface them in their terminal log entry. Sibling tests with
``@pytest.mark.xfail(strict=True)`` cover the residual gaps where one
of the four surfaces is genuinely unreachable from
``BranchContext`` alone (a remote branch pushed to a suffixed name
without a PR; the open PR list visited from ``recover_state`` before
``current_pr`` is set). Those siblings continue to XFAIL strictly so
the day BranchContext grows to cover them, the resulting XPASS forces
the marker to be dropped.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
from typing import Any

import pytest
from src.daemon import git_ops
from src.daemon import recovery as recovery_module  # noqa: F401  (import sanity)
from src.daemon import runner as runner_module
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)
from src.task_status import find_matching_open_pr

from tests.runner import _helpers as h

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BASE_BRANCH = "main"
TASK_BRANCH = "pr-foo"
WRONG_BRANCH = "pr-foo-2"  # the OBS-AI scenario suffix
FEATURE_BRANCH = "pr-bar"  # used by the dirty-tree test

XFAIL_BRANCH_CONTEXT_REASON = (
    "PR-222 BranchContext only surfaces ``pr_head_branch`` from "
    "``state.current_pr.branch``; this scenario's expected value is "
    "reachable only via a wider context surface (raw ``ls-remote`` "
    "output or the ``recover_state`` open-PR list). The strict=True "
    "marker stays so the day BranchContext grows to cover it the "
    "resulting XPASS forces this marker to be dropped."
)


def _runner_with_task(
    monkeypatch: pytest.MonkeyPatch,
    *,
    task_branch: str = TASK_BRANCH,
    open_prs: list[PRInfo] | None = None,
) -> Any:
    """Build a PipelineRunner stubbed to run ``handle_coding`` once.

    Mirrors the helper in ``tests/test_coding.py`` so the regression
    tests sit on the same fixture surface as the existing CODING tests.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **_kw: list(open_prs or []),
    )

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(runner_module.asyncio, "sleep", _sleep)
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-FOO",
        title="Branch context regression",
        status=TaskStatus.DOING,
        branch=task_branch,
        task_file="tasks/PR-FOO.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    return runner


def _patch_branch_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    local_branch: str | None,
    remote_branch: str | None,
) -> None:
    """Stub ``git_ops._git`` so branch existence probes are deterministic.

    ``local_branch`` and ``remote_branch`` name the only branches that
    exist locally / on origin respectively. ``None`` means no branch is
    present in that location.
    """

    def fake_git(repo_path: str, *args: str, **_kwargs: Any):
        if args[:3] == ("rev-parse", "--verify", "--quiet"):
            ref = args[3] if len(args) > 3 else ""
            wanted = ref.removeprefix("refs/heads/")
            rc = 0 if local_branch is not None and wanted == local_branch else 1
            return subprocess.CompletedProcess(args=list(args), returncode=rc, stdout="", stderr="")
        if args[:1] == ("ls-remote",):
            ref = args[-1] if args else ""
            wanted = ref.removeprefix("refs/heads/")
            if remote_branch is not None and wanted == remote_branch:
                return subprocess.CompletedProcess(
                    args=list(args),
                    returncode=0,
                    stdout=f"abcdef refs/heads/{remote_branch}\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(args=list(args), returncode=2, stdout="", stderr="")
        return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")

    monkeypatch.setattr(git_ops, "_git", fake_git)


def _log_text(runner: Any) -> str:
    """Return the runner's history concatenated for substring assertions."""
    return "\n".join(entry["event"] for entry in runner.state.history)


def _diagnostic_for(runner: Any, marker: str) -> str:
    """Return the most recent runner log entry containing ``marker``.

    The branch-context check looks at this specific entry as the
    divergence/cancellation diagnostic, so unrelated retry log lines
    (e.g. ``"PR not found for 'pr-foo'"``) that happen to mention a
    branch in passing cannot accidentally satisfy the assertion and
    falsely flip the xfail to XPASS without the diagnostic itself
    actually being fixed.
    """
    for entry in reversed(runner.state.history):
        event = entry.get("event", "")
        if marker in event:
            return event
    return ""


# ---------------------------------------------------------------------------
# Scenario builders — shared between current-behavior tests and their
# strict-xfail siblings so the setup cannot drift between the two.
# ---------------------------------------------------------------------------


def _run_coder_no_target_scenario(monkeypatch: pytest.MonkeyPatch) -> Any:
    runner = _runner_with_task(monkeypatch, task_branch=TASK_BRANCH)
    _patch_branch_state(monkeypatch, local_branch=None, remote_branch=None)
    asyncio.run(runner.handle_coding())
    return runner


def _run_coder_wrong_remote_scenario(monkeypatch: pytest.MonkeyPatch) -> Any:
    runner = _runner_with_task(monkeypatch, task_branch=TASK_BRANCH)
    _patch_branch_state(monkeypatch, local_branch=None, remote_branch=WRONG_BRANCH)
    asyncio.run(runner.handle_coding())
    return runner


def _run_recovery_branch_mismatch_scenario(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, list[str]]:
    doing = QueueTask(
        pr_id="PR-FOO",
        title="In flight",
        status=TaskStatus.DOING,
        branch=TASK_BRANCH,
    )
    open_pr = PRInfo(
        number=99,
        branch=WRONG_BRANCH,
        title="Suffixed branch PR",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **_kw: [open_pr],
    )

    runner = h._make_runner()
    runner.repo_config = runner.repo_config.model_copy(update={"branch": BASE_BRANCH})
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: [doing]  # type: ignore[method-assign]
    # Preserve must succeed (no local branch present) so the path
    # progresses to the CANCELED transition rather than stranding in
    # ERROR; that is the case being documented here.
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]

    coding_calls: list[str] = []

    async def fake_coding() -> None:  # pragma: no cover - must not fire
        coding_calls.append("coding")

    runner.handle_coding = fake_coding  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())
    return runner, coding_calls


def _run_dirty_tree_scenario(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, list[list[str]]]:
    git_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **_kwargs: Any) -> Any:
        git_commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        if cmd[:2] == ["git", "rev-parse"] and "--abbrev-ref" in cmd:
            return h._FakeCompletedProcess(args=cmd, stdout=f"{FEATURE_BRANCH}\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = h._make_runner()
    runner.repo_config = runner.repo_config.model_copy(update={"branch": BASE_BRANCH})
    runner.state.current_task = QueueTask(
        pr_id="PR-FOO",
        title="Active task",
        status=TaskStatus.DOING,
        branch=TASK_BRANCH,
    )

    # Two dirty cycles -> ERROR; the third triggers auto-reset -> IDLE.
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert asyncio.run(runner.preflight()) is True

    return runner, git_commands


# ---------------------------------------------------------------------------
# Test 1 — coder exits 0 on wrong local branch, target branch absent
# ---------------------------------------------------------------------------


def test_coder_exit_zero_with_no_target_branch_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Scenario: ``handle_coding`` runs the coder which exits 0 but
    never creates the task branch locally and never pushes anywhere.
    Neither ``refs/heads/pr-foo`` nor ``origin/pr-foo`` exists.

    Branch values in this scenario:

    - base_branch         = "main"        (PR base, present)
    - task_branch         = "pr-foo"      (declared in QueueTask, NOT realized)
    - current_git_branch  = (absent)      (no checkout occurred for pr-foo)
    - pr_head_branch      = (absent)      (no PR exists)

    The runner transitions to HUNG via the case-A diagnostic in
    ``coding.py`` and the ``[ESCALATE]`` log line names every surface
    via ``BranchContext.log_summary`` so an operator can tell from a
    single log entry which branches were known when the diagnostic
    fired (PR-222 acceptance check).
    """
    runner = _run_coder_no_target_scenario(monkeypatch)

    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")
    log = _log_text(runner)
    assert "[ESCALATE]" in log
    assert "did nothing" in log


def test_coder_exit_zero_with_no_target_branch_branch_context_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-222: BranchContext threads all 4 branches through [ESCALATE]."""
    runner = _run_coder_no_target_scenario(monkeypatch)
    _assert_branch_context_in_diagnostic(
        _diagnostic_for(runner, "[ESCALATE]"),
        runner.state.error_message or "",
        base_branch=BASE_BRANCH,
        task_branch=TASK_BRANCH,
        current_git_branch=None,
        pr_head_branch=None,
    )


# ---------------------------------------------------------------------------
# Test 2 — coder pushes wrong remote branch, target branch absent
# ---------------------------------------------------------------------------


def test_coder_pushed_wrong_branch_target_absent_marks_hung_silently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Documenting current behavior at 2026-05-01.

    Scenario (the OBS-AI scenario): the coder pushed to a suffixed
    remote branch (``pr-foo-2``) — not the task's declared branch
    (``pr-foo``). From the daemon's vantage point ``pr-foo`` exists
    neither locally nor on origin, so it cannot tell the difference
    between "coder did nothing" and "coder pushed under the wrong
    name". This test pins that ambiguity so a future BranchContext PR
    can change the verdict (e.g. fail-fast with a "wrong branch
    pushed" diagnostic) with full regression coverage.

    Branch values in this scenario:

    - base_branch         = "main"        (PR base, present)
    - task_branch         = "pr-foo"      (declared, NOT visible anywhere)
    - current_git_branch  = (unknown)     (daemon never inspects HEAD here)
    - pr_head_branch      = "pr-foo-2"    (remote branch pushed by coder,
                                          but invisible to this code path
                                          because no PR exists yet)
    """
    runner = _run_coder_wrong_remote_scenario(monkeypatch)

    # Current behavior — same outcome as test 1 because the daemon does
    # not see the wrong branch. This collapse is precisely the OBS-AI
    # ambiguity.
    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")
    log = _log_text(runner)
    assert "[ESCALATE]" in log


@pytest.mark.xfail(strict=True, reason=XFAIL_BRANCH_CONTEXT_REASON)
def test_coder_pushed_wrong_branch_branch_context_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict-xfail: BranchContext PR will surface ``pr-foo-2`` alongside
    the missing target in the [ESCALATE] diagnostic."""
    runner = _run_coder_wrong_remote_scenario(monkeypatch)
    _assert_branch_context_in_diagnostic(
        _diagnostic_for(runner, "[ESCALATE]"),
        runner.state.error_message or "",
        base_branch=BASE_BRANCH,
        task_branch=TASK_BRANCH,
        current_git_branch=None,
        pr_head_branch=WRONG_BRANCH,
    )


# ---------------------------------------------------------------------------
# Test 3 — PR exists for branch != active task branch
# ---------------------------------------------------------------------------


def test_open_pr_on_wrong_branch_does_not_match_task() -> None:
    """Documenting current behavior at 2026-05-01.

    Scenario: an open PR exists with head ``pr-foo-2`` while the active
    QueueTask declares ``Branch: pr-foo``. ``handle_idle`` matches PRs
    via :func:`find_matching_open_pr`, which compares ``pr.branch`` to
    ``task.branch`` byte-for-byte — so the suffixed branch is NOT
    matched and the task remains unmatched.

    Branch values in this scenario:

    - base_branch         = "main"        (PR base, present)
    - task_branch         = "pr-foo"      (queue task)
    - current_git_branch  = "main"        (assumed; not consulted by the
                                          matcher today)
    - pr_head_branch      = "pr-foo-2"    (open PR's head)

    The matcher returns ``None``, demonstrating that the daemon cannot
    today rescue an OBS-AI scenario by associating a suffixed branch
    PR with the original task. The future BranchContext PR will decide
    whether to fuzzy-match, hard-fail, or surface the divergence
    explicitly.
    """
    base_branch = BASE_BRANCH
    task_branch = TASK_BRANCH
    pr_head_branch = WRONG_BRANCH
    pr = PRInfo(
        number=314,
        branch=pr_head_branch,
        title="Suffixed branch PR",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )

    matched = find_matching_open_pr("PR-FOO", task_branch, [pr])

    assert matched is None
    # The matcher does not consider base_branch or current_git_branch;
    # documenting that explicitly so the future BranchContext PR knows
    # those inputs were intentionally ignored.
    assert base_branch == "main"
    # Sanity: the only divergence is task_branch vs pr_head_branch.
    assert task_branch != pr_head_branch

    # Same matcher, same task, but with the matching PR present —
    # establishes the positive baseline so the negative case above is
    # unambiguously about branch divergence and not about pr_id wiring.
    matching_pr = PRInfo(
        number=315,
        branch=task_branch,
        title="Correct branch PR",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    assert find_matching_open_pr("PR-FOO", task_branch, [pr, matching_pr]) is matching_pr


# ---------------------------------------------------------------------------
# Test 4 — recovery finds current_task.branch != current_pr.branch
# ---------------------------------------------------------------------------


def test_recover_state_with_branch_mismatch_marks_task_canceled_and_idles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Documenting current behavior at 2026-05-01.

    Scenario: ``recover_state`` parses a queue with a DOING task whose
    ``branch`` is ``pr-foo``; the only open PR on the remote has head
    ``pr-foo-2``. The branch-by-branch match in ``recovery.py`` returns
    no candidate, so the DOING task is treated as a crash and marked
    CANCELED, the daemon settles back to IDLE, and the user must
    manually re-upload to retry.

    Branch values in this scenario:

    - base_branch         = "main"        (repo_config.branch)
    - task_branch         = "pr-foo"      (DOING entry in queue)
    - current_git_branch  = (absent)      (recovery does not inspect HEAD)
    - pr_head_branch      = "pr-foo-2"    (open PR head; ignored by matcher)

    Deterministic outcome: the queue entry's branch wins for the
    cancellation log message; the PR head branch is silently dropped.
    The current log line names the ``pr_id`` ("PR-FOO") but does NOT
    name either branch — that branch-context gap is captured by the
    strict-xfail sibling below.
    """
    runner, coding_calls = _run_recovery_branch_mismatch_scenario(monkeypatch)

    assert coding_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    log = _log_text(runner)
    assert ("[INFRA] Task PR-FOO crashed, marking CANCELED. Manually re-upload to retry.") in log


@pytest.mark.xfail(strict=True, reason=XFAIL_BRANCH_CONTEXT_REASON)
def test_recover_state_with_branch_mismatch_branch_context_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Strict-xfail: BranchContext PR will name both branches in the
    cancellation diagnostic."""
    runner, _ = _run_recovery_branch_mismatch_scenario(monkeypatch)
    _assert_branch_context_in_diagnostic(
        _diagnostic_for(runner, "marking CANCELED"),
        runner.state.error_message or "",
        base_branch=BASE_BRANCH,
        task_branch=TASK_BRANCH,
        current_git_branch=None,
        pr_head_branch=WRONG_BRANCH,
    )


# ---------------------------------------------------------------------------
# Test 5 — dirty tree on unexpected branch, 3 cycles trigger reset
# ---------------------------------------------------------------------------


def test_dirty_tree_on_feature_branch_resets_after_three_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Documenting current behavior at 2026-05-01.

    Scenario: the working tree has uncommitted changes and HEAD is on
    feature branch ``pr-bar`` while the active task header declares
    ``Branch: pr-foo``. After ``_DIRTY_CYCLES_BEFORE_AUTO_RESET`` (3)
    consecutive dirty preflights, ``_auto_reset_dirty_tree`` checks
    out ``main`` with ``--force``, hard-resets to ``origin/main``, and
    cleans untracked files. The runner resumes IDLE because no PR was
    being tracked at the time of reset.

    Branch values in this scenario:

    - base_branch         = "main"        (reset target)
    - task_branch         = "pr-foo"      (active task header)
    - current_git_branch  = "pr-bar"      (HEAD before reset; OBS-AI gap:
                                          neither name reaches the log)
    - pr_head_branch      = (absent)      (no PR yet)

    The reset itself is correct behavior — it preserves nothing about
    the ``pr-bar`` work, which is in line with the existing
    ``_preserve_crashed_run_commits`` contract that only fires from
    ``recover_state``. The OBS-AI gap captured by the strict-xfail
    sibling is purely observability: the auto-recovery log line does
    not name the actual branch (``pr-bar``) nor the expected branch
    (``pr-foo``), so an operator cannot tell from the event log which
    feature branch was wiped.
    """
    runner, git_commands = _run_dirty_tree_scenario(monkeypatch)

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner._consecutive_dirty_cycles == 0
    # The reset chain ran with the base branch name only.
    assert any(cmd[:4] == ["git", "checkout", "--force", BASE_BRANCH] for cmd in git_commands)
    assert any(cmd[:3] == ["git", "reset", "--hard"] and cmd[-1] == f"origin/{BASE_BRANCH}" for cmd in git_commands)
    assert any(cmd[:3] == ["git", "clean", "-fd"] for cmd in git_commands)
    log = _log_text(runner)
    assert "Auto-recovered from dirty tree -> IDLE" in log


def test_dirty_tree_on_feature_branch_branch_context_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-222: the auto-recovery diagnostic names the wiped feature
    branch and the expected task branch via ``BranchContext.log_summary``."""
    runner, _ = _run_dirty_tree_scenario(monkeypatch)
    _assert_branch_context_in_diagnostic(
        _diagnostic_for(runner, "Auto-recovered from dirty tree"),
        runner.state.error_message or "",
        base_branch=BASE_BRANCH,
        task_branch=TASK_BRANCH,
        current_git_branch=FEATURE_BRANCH,
        pr_head_branch=None,
    )


# ---------------------------------------------------------------------------
# Branch-context assertion helper
# ---------------------------------------------------------------------------


def _assert_branch_context_in_diagnostic(
    diagnostic: str,
    error_message: str,
    *,
    base_branch: str,
    task_branch: str | None,
    current_git_branch: str | None,
    pr_head_branch: str | None,
) -> None:
    """Assert all 4 branch values appear in the divergence diagnostic.

    Scoped strictly to the divergence/cancellation log entry (plus the
    surfaced ``error_message``) rather than the entire event history.
    Earlier informational log lines — e.g. ``"PR not found for 'pr-foo',
    retrying in 5s"`` — already mention branches in passing, so a
    whole-history check would let those satisfy the assertion even
    though the final diagnostic still omits branch context.

    Both present and absent slots are matched as labeled tokens
    (``<label>=<value>`` or ``<label>: <value>``), not bare substrings.
    A raw substring check would treat ``pr_head_branch=pr-foo-2`` as
    proof that ``task_branch=pr-foo`` was logged (because ``"pr-foo"``
    is a prefix of ``"pr-foo-2"``), letting overlapping branch names
    flip a strict xfail to XPASS without the diagnostic actually
    naming the task branch. The labeled form plus a no-trailing-``[\\w-]``
    lookahead pins each value to its own slot.

    The future BranchContext PR will thread base / task / git / PR
    branch identifiers through each divergence diagnostic. Until then,
    this helper is the failing assertion that drives the surrounding
    ``@pytest.mark.xfail(strict=True)``: every present branch must be
    labeled at least once in this single diagnostic, and every ``None``
    slot must be named individually as absent — the loop does not
    short-circuit, so one ``"absent"`` mention cannot cover multiple
    ``None`` fields.

    When the diagnostic is fixed the assertion will pass, the test will
    XPASS, and ``strict=True`` will surface that as a hard failure —
    the explicit signal to drop the xfail marker. This is intentionally
    NOT a runtime ``pytest.xfail()`` call: that variant reports a plain
    ``PASS`` once the gap closes and would let stale guardrails linger
    silently, defeating the regression value of this suite.
    """
    haystack = f"{diagnostic}\n{error_message}"
    missing: list[str] = []
    for label, value in (
        ("base_branch", base_branch),
        ("task_branch", task_branch),
        ("current_git_branch", current_git_branch),
        ("pr_head_branch", pr_head_branch),
    ):
        if value is None:
            # Pair the label with "absent" via an explicit ``=`` or ``:``
            # separator (allowing optional ``<>`` brackets). A bare
            # global ``"absent"`` substring elsewhere in the haystack
            # must NOT satisfy this slot, otherwise a future
            # BranchContext implementation that names every label but
            # marks only one as absent would falsely flip every xfail
            # to XPASS and weaken the regression guard.
            absent_marker = re.compile(
                rf"\b{re.escape(label)}\b\s*[=:]\s*<?absent\b",
                re.IGNORECASE,
            )
            if not absent_marker.search(haystack):
                missing.append(f"{label}=<absent> not explicitly logged")
        else:
            # Require a labeled token (``label=value`` / ``label: value``)
            # rather than a raw substring. The negative lookahead for
            # ``[\w-]`` ensures ``task_branch=pr-foo`` is not satisfied
            # by ``pr_head_branch=pr-foo-2`` (prefix overlap), which
            # would otherwise weaken the regression guard for the
            # OBS-AI scenario where the wrong remote branch shares a
            # prefix with the declared task branch. Optional opening
            # quote/bracket characters tolerate common formatting
            # variants like ``label="value"`` or ``label=<value>``.
            present_marker = re.compile(
                rf"\b{re.escape(label)}\b\s*[=:]\s*[<\"']?"
                rf"{re.escape(value)}(?![\w-])",
                re.IGNORECASE,
            )
            if not present_marker.search(haystack):
                missing.append(f"{label}={value!r} not labeled")
    assert not missing, "BranchContext diagnostic is missing required branch identifiers: " + ", ".join(missing)
