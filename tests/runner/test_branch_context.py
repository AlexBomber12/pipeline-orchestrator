"""PR-209: Branch mismatch regression tests.

These tests document current behavior at 2026-05-01, not desired behavior.
They establish a regression baseline before the BranchContext refactor
lands so that a single canonical branch-mismatch detection path can be
added with explicit confidence about what changes.

The 4 branch concepts each test tracks:

- ``base_branch``        — ``repo_config.branch`` (typically ``"main"``)
- ``task_branch``        — ``QueueTask.branch`` (from the task header)
- ``current_git_branch`` — branch checked out on the working tree
                           (``git rev-parse --abbrev-ref HEAD``)
- ``pr_head_branch``     — ``PRInfo.branch`` (the PR head, if any)

Each test asserts the resulting ``state``, ``error_message``, and log
event text at the end of the cycle. Where the current handler does NOT
include a branch value in the log (the OBS-AI architectural ambiguity
this PR captures), the assertion is wrapped in ``pytest.mark.xfail``
so the gap is recorded without blocking the PR; the future
BranchContext PR will flip those xfails into XPASS as branch context is
threaded through the affected log lines.
"""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any

import pytest
from src import github_client
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

from tests import test_runner as h


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

BASE_BRANCH = "main"
TASK_BRANCH = "pr-foo"
WRONG_BRANCH = "pr-foo-2"  # the OBS-AI scenario suffix
FEATURE_BRANCH = "pr-bar"  # used by the dirty-tree test


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
    monkeypatch.setattr(
        h.claude_cli, "run_planned_pr_async", h._async_cli_result(0, "ok", "")
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
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
            return subprocess.CompletedProcess(
                args=list(args), returncode=rc, stdout="", stderr=""
            )
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
            return subprocess.CompletedProcess(
                args=list(args), returncode=2, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=list(args), returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(git_ops, "_git", fake_git)


def _log_text(runner: Any) -> str:
    """Return the runner's history concatenated for substring assertions."""
    return "\n".join(entry["event"] for entry in runner.state.history)


def _diagnostic_for(runner: Any, marker: str) -> str:
    """Return the most recent runner log entry containing ``marker``.

    The branch-context xfail helper checks only this specific entry as
    the divergence/cancellation diagnostic, so unrelated retry log lines
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
# Test 1 — coder exits 0 on wrong local branch, target branch absent
# ---------------------------------------------------------------------------


def test_coder_exit_zero_with_no_target_branch_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Documenting current behavior at 2026-05-01.

    Scenario: ``handle_coding`` runs the coder which exits 0 but never
    creates the task branch locally and never pushes anywhere. Neither
    ``refs/heads/pr-foo`` nor ``origin/pr-foo`` exists.

    Branch values in this scenario:

    - base_branch         = "main"        (PR base, present)
    - task_branch         = "pr-foo"      (declared in QueueTask, NOT realized)
    - current_git_branch  = (absent)      (no checkout occurred for pr-foo)
    - pr_head_branch      = (absent)      (no PR exists)

    The runner correctly transitions to HUNG via the case-A diagnostic
    in ``coding.py``, and ``error_message`` contains the canonical
    "did nothing" wording. The OBS-AI gap captured here is that the
    error_message and log line do NOT name the missing target branch,
    so a future ``BranchContext`` PR can thread it through. Those
    branch-context assertions are wrapped in ``xfail`` below.
    """
    base_branch = BASE_BRANCH
    task_branch = TASK_BRANCH
    runner = _runner_with_task(monkeypatch, task_branch=task_branch)
    _patch_branch_state(
        monkeypatch, local_branch=None, remote_branch=None
    )

    asyncio.run(runner.handle_coding())

    # Current behavior — passes today.
    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")
    log = _log_text(runner)
    assert "[ESCALATE]" in log
    assert "did nothing" in log
    # base_branch is the PR target; not part of this code path's log.
    assert base_branch not in (runner.state.error_message or "")

    # Branch-context gap (OBS-AI). Future BranchContext PR will include
    # all 4 branch values explicitly in the diagnostic. The check is
    # scoped to the [ESCALATE] line so retry logs that already mention
    # ``pr-foo`` cannot mask the gap.
    _assert_branch_context_in_diagnostic_xfail(
        _diagnostic_for(runner, "[ESCALATE]"),
        runner.state.error_message or "",
        base_branch=base_branch,
        task_branch=task_branch,
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
    base_branch = BASE_BRANCH
    task_branch = TASK_BRANCH
    wrong_branch = WRONG_BRANCH
    # Coder pushed to ``pr-foo-2`` but did not push ``pr-foo``. The
    # daemon's case-A/B probe only asks about ``pr-foo``, so it sees
    # "no local, no remote" and routes to HUNG without knowing
    # ``pr-foo-2`` exists at all.
    runner = _runner_with_task(monkeypatch, task_branch=task_branch)
    _patch_branch_state(
        monkeypatch, local_branch=None, remote_branch=wrong_branch
    )

    asyncio.run(runner.handle_coding())

    # Current behavior — same outcome as test 1 because the daemon does
    # not see the wrong branch. This collapse is precisely the OBS-AI
    # ambiguity.
    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")
    log = _log_text(runner)
    assert "[ESCALATE]" in log
    # The wrong branch (``pr-foo-2``) was never observed by the
    # diagnostic — it is silently invisible at 2026-05-01.
    assert wrong_branch not in log
    assert wrong_branch not in (runner.state.error_message or "")

    # Branch-context gap. Future BranchContext PR is expected to detect
    # ``pr-foo-2`` as the actual remote branch and surface it alongside
    # the missing target. Scoped to the [ESCALATE] line so retry logs
    # that already mention ``pr-foo`` cannot mask the gap.
    _assert_branch_context_in_diagnostic_xfail(
        _diagnostic_for(runner, "[ESCALATE]"),
        runner.state.error_message or "",
        base_branch=base_branch,
        task_branch=task_branch,
        current_git_branch=None,
        pr_head_branch=wrong_branch,
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
    assert (
        find_matching_open_pr("PR-FOO", task_branch, [pr, matching_pr])
        is matching_pr
    )


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
    name either branch — that branch-context gap is the xfail below.
    """
    base_branch = BASE_BRANCH
    task_branch = TASK_BRANCH
    pr_head_branch = WRONG_BRANCH

    doing = QueueTask(
        pr_id="PR-FOO",
        title="In flight",
        status=TaskStatus.DOING,
        branch=task_branch,
    )
    open_pr = PRInfo(
        number=99,
        branch=pr_head_branch,
        title="Suffixed branch PR",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **_kw: [open_pr],
    )

    runner = h._make_runner()
    runner.repo_config = runner.repo_config.model_copy(
        update={"branch": base_branch}
    )
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: [doing]  # type: ignore[method-assign]
    # Preserve must succeed (no local branch present) so the path
    # progresses to the CANCELED transition rather than stranding in
    # ERROR; that is the case being documented here.
    runner._preserve_crashed_run_commits = lambda branch: True  # type: ignore[method-assign]

    coding_calls: list[str] = []

    async def fake_coding() -> None:  # pragma: no cover - must not fire
        coding_calls.append("coding")

    runner.handle_coding = fake_coding  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    # Current behavior — passes today.
    assert coding_calls == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    log = _log_text(runner)
    assert (
        "[INFRA] Task PR-FOO crashed, marking CANCELED. "
        "Manually re-upload to retry."
    ) in log
    # The unrelated open PR's branch is intentionally ignored by the
    # matcher; documenting it does not bleed into the cancellation log.
    assert pr_head_branch not in log

    _assert_branch_context_in_diagnostic_xfail(
        _diagnostic_for(runner, "marking CANCELED"),
        runner.state.error_message or "",
        base_branch=base_branch,
        task_branch=task_branch,
        current_git_branch=None,
        pr_head_branch=pr_head_branch,
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
    ``recover_state``. The OBS-AI gap is purely observability: the
    auto-recovery log line does not name the actual branch (``pr-bar``)
    nor the expected branch (``pr-foo``), so an operator cannot tell
    from the event log which feature branch was wiped.
    """
    base_branch = BASE_BRANCH
    task_branch = TASK_BRANCH
    current_git_branch = FEATURE_BRANCH

    git_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **_kwargs: Any) -> Any:
        git_commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        if cmd[:2] == ["git", "rev-parse"] and "--abbrev-ref" in cmd:
            return h._FakeCompletedProcess(
                args=cmd, stdout=f"{current_git_branch}\n", returncode=0
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    runner = h._make_runner()
    runner.repo_config = runner.repo_config.model_copy(
        update={"branch": base_branch}
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-FOO",
        title="Active task",
        status=TaskStatus.DOING,
        branch=task_branch,
    )

    # Two dirty cycles -> ERROR; the third triggers auto-reset -> IDLE.
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert asyncio.run(runner.preflight()) is True

    # Current behavior — passes today.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert runner._consecutive_dirty_cycles == 0
    # The reset chain ran with the base branch name only.
    assert any(
        cmd[:4] == ["git", "checkout", "--force", base_branch]
        for cmd in git_commands
    )
    assert any(
        cmd[:3] == ["git", "reset", "--hard"]
        and cmd[-1] == f"origin/{base_branch}"
        for cmd in git_commands
    )
    assert any(cmd[:3] == ["git", "clean", "-fd"] for cmd in git_commands)
    log = _log_text(runner)
    assert "Auto-recovered from dirty tree -> IDLE" in log
    # Documenting the OBS-AI gap: the reset log line does not name the
    # feature branch that was wiped, nor the task branch that should
    # have been on HEAD.
    assert current_git_branch not in log
    assert task_branch not in log

    _assert_branch_context_in_diagnostic_xfail(
        _diagnostic_for(runner, "Auto-recovered from dirty tree"),
        runner.state.error_message or "",
        base_branch=base_branch,
        task_branch=task_branch,
        current_git_branch=current_git_branch,
        pr_head_branch=None,
    )


# ---------------------------------------------------------------------------
# Branch-context xfail helper
# ---------------------------------------------------------------------------


def _assert_branch_context_in_diagnostic_xfail(
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
    though the final diagnostic still omits branch context. That would
    flip these xfails to XPASS without the regression actually being
    fixed.

    The future BranchContext PR will thread base / task / git / PR
    branch identifiers through each divergence diagnostic. Until then,
    every present branch must appear at least once in this single
    diagnostic, and every ``None`` slot must be named individually as
    absent — the loop does not short-circuit, so one ``"absent"``
    mention cannot cover multiple ``None`` fields.
    """
    haystack = f"{diagnostic}\n{error_message}"
    haystack_lower = haystack.lower()
    missing: list[str] = []
    if base_branch and base_branch not in haystack:
        missing.append(f"base_branch={base_branch!r}")
    if task_branch and task_branch not in haystack:
        missing.append(f"task_branch={task_branch!r}")
    if current_git_branch and current_git_branch not in haystack:
        missing.append(f"current_git_branch={current_git_branch!r}")
    if pr_head_branch and pr_head_branch not in haystack:
        missing.append(f"pr_head_branch={pr_head_branch!r}")
    for label, value in (
        ("base_branch", base_branch),
        ("task_branch", task_branch),
        ("current_git_branch", current_git_branch),
        ("pr_head_branch", pr_head_branch),
    ):
        if value is None:
            if label not in haystack_lower or "absent" not in haystack_lower:
                missing.append(f"{label}=<absent> not explicitly logged")
    if missing:
        pytest.xfail(
            "OBS-AI class gap, fixed in BranchContext PR: "
            + ", ".join(missing)
        )
