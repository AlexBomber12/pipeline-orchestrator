"""PR-212: Recovery interaction regression tests.

The daemon has multiple defense-in-depth recovery layers — preflight
dirty-tree auto-reset, ``recover_state``'s crashed-task marker, and the
three FIX-cycle escalation primitives in ``fix.py`` — each owning a
distinct counter, threshold, and terminal state. They are coordinated
by ordering in ``run_cycle`` rather than an explicit contract, so a
single incident can fire more than one layer.

These four tests pin the **current** layer composition before PR-220
replaces the three ``_escalate_fix_*`` methods with a single
``_escalate_to_hung`` primitive. Each test asserts the terminal state,
the side effect (label apply, comment post, ``is_escalated`` flag)
and the operator-visible log line so the upcoming refactor preserves
behavior verbatim:

- Test 1 — dirty-tree auto-reset composes with recovery's crashed-task
  marker without colliding (terminal state IDLE, both events logged).
- Test 2 — no-push deadlock parks in HUNG and the
  ``hung_fallback_codex_review`` path does not re-fire ``@codex review``
  thanks to the ``is_escalated`` guard in ``handle_hung``.
- Test 3 — coder ESCALATE with a label-apply failure parks in HUNG
  (not IDLE) so the in-memory ``is_escalated`` flag remains the
  load-bearing parking signal during a GitHub outage.
- Test 4 — iteration-cap reached with a label-apply failure parks in
  ERROR (the iteration-cap site has stricter durability requirements
  than the coder-initiated path because it is daemon-driven, not
  coder-self-reported).

Tests do not change production behavior; they only assert it.
"""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any

import pytest
from src.daemon import runner as runner_module
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import hung as hung_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests import test_runner as h


# ---------------------------------------------------------------------------
# Test 1 — dirty-tree auto-reset composes with crashed-task marker
# ---------------------------------------------------------------------------


def test_dirty_tree_recovery_composes_with_crashed_task_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery + preflight defense layers compose without colliding.

    Setup mirrors a daemon restart after a mid-CODING crash: a DOING
    task PR-100 in the queue with no matching open PR, and a working
    tree left dirty by the crashed coder run. Two distinct recovery
    layers must engage:

    1. ``recover_state`` calls ``_preserve_crashed_run_commits`` to
       push any unpushed work, adds PR-100 to ``_crashed_task_pr_ids``,
       transitions to IDLE.
    2. The next ``preflight`` cycle observes the dirty tree, increments
       the consecutive-dirty counter to its threshold, and fires
       ``_auto_reset_dirty_tree`` to hard-reset back to a clean IDLE.

    Pre-seeding ``_consecutive_dirty_cycles=2`` so the dirty preflight
    crosses the threshold on this single call keeps the test focused
    on the layer composition rather than the policy mechanics tested
    elsewhere.
    """
    task = QueueTask(
        pr_id="PR-100",
        title="Crashed mid-CODING",
        status=TaskStatus.DOING,
        branch="pr-100-crashed",
    )

    monkeypatch.setattr(
        runner_module.github_client,
        "get_open_prs",
        lambda repo, **kw: [],
    )

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: [task]  # type: ignore[method-assign]

    preserve_calls: list[str] = []

    def fake_preserve(branch: str) -> bool:
        preserve_calls.append(branch)
        return True

    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        fake_preserve
    )

    asyncio.run(runner.recover_state())

    assert preserve_calls == ["pr-100-crashed"]
    assert runner._crashed_task_pr_ids == {"PR-100"}
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any(
        e["event"]
        == "[INFRA] Task PR-100 crashed, marking CANCELED. "
        "Manually re-upload to retry."
        for e in runner.state.history
    )

    runner._consecutive_dirty_cycles = 2

    reset_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        reset_commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(
                args=cmd, stdout=" M src/foo.py\n", returncode=0
            )
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.IDLE
    assert runner._consecutive_dirty_cycles == 0
    assert any(
        cmd[:2] == ["git", "reset"] and "--hard" in cmd
        for cmd in reset_commands
    )
    assert any(
        "Auto-recovered from dirty tree" in e["event"]
        for e in runner.state.history
    )
    assert any(
        "PR-100 crashed, marking CANCELED" in e["event"]
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# Test 2 — no-push deadlock + HUNG fallback blocked by ``is_escalated``
# ---------------------------------------------------------------------------


def test_no_push_escalation_blocks_codex_review_fallback_in_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-push deadlock parks in HUNG and the fallback stays parked.

    ``_escalate_fix_no_push_deadlock`` is responsible for two
    coordinated effects: the immediate transition to HUNG, and setting
    ``current_pr.is_escalated=True`` so the next ``handle_hung`` cycle
    refuses to fire the ``@codex review`` fallback. Without that
    second step, the fallback would bounce the runner back through
    WATCH and immediately re-enter the FIX loop the deadlock counter
    just stopped — which is exactly the regression the
    ``is_escalated`` in-memory flag was added to prevent.
    """
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    monkeypatch.setattr(
        fix_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        fix_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )

    runner = h._make_runner()
    pr = PRInfo(number=400, branch="pr-400-feedback", no_push_fix_count=3)
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_no_push_deadlock(pr))

    expected_message = (
        "FIX deadlock: 3 consecutive no-push FIX cycles on PR #400. "
        "Coder unable to identify actionable fix. Manual review required."
    )
    assert runner.state.state == PipelineState.HUNG
    assert pr.is_escalated is True
    assert pr.no_push_fix_count == 0
    assert posted == [(runner.owner_repo, 400, expected_message)]
    assert ["pr", "edit", "400", "--add-label", "escalated"] in gh_calls
    assert any(
        e["event"] == f"[ESCALATE] {expected_message}"
        for e in runner.state.history
    )

    review_posts: list[tuple[str, int, str]] = []

    def record_or_fail(repo: str, number: int, body: str) -> None:
        review_posts.append((repo, number, body))

    monkeypatch.setattr(
        hung_module.github_client, "post_comment", record_or_fail
    )
    monkeypatch.setattr(
        hung_module.github_client,
        "run_gh",
        lambda cmd, **kwargs: {"state": "OPEN"},
    )

    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.HUNG
    assert pr.is_escalated is True
    assert review_posts == []
    assert any(
        "PR #400 escalated; staying HUNG, skipping @codex review fallback."
        in e["event"]
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# Test 3 — coder ESCALATE + label apply failure parks in HUNG
# ---------------------------------------------------------------------------


def test_coder_escalate_label_apply_failure_parks_in_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coder ESCALATE with a label-apply failure parks the PR in HUNG.

    ``_escalate_fix_coder_initiated`` ordinarily moves to IDLE on the
    success path so the next IDLE refresh can rehydrate
    ``is_escalated`` from the GitHub label. When the label apply
    soft-fails, the rehydrate would observe a missing label and drop
    the parking signal; HUNG honors the in-memory ``is_escalated``
    flag, so the runner stays parked until manual intervention.
    ``error_message`` carries both the failure context and the
    coder-supplied reason so an operator reading the dashboard can
    act on it without diving into the daemon log.
    """
    posted: list[tuple[str, int, str]] = []

    monkeypatch.setattr(
        fix_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)

    runner = h._make_runner()
    pr = PRInfo(number=401, branch="pr-401-coder-escalate")
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(
        runner._escalate_fix_coder_initiated(pr, "transient infra failure")
    )

    assert runner.state.state == PipelineState.HUNG
    assert pr.is_escalated is True
    assert runner.state.error_message is not None
    assert "FIX coder ESCALATE on PR #401" in runner.state.error_message
    assert (
        "failed to apply `escalated` label" in runner.state.error_message
    )
    assert "transient infra failure" in runner.state.error_message
    assert posted == [
        (
            runner.owner_repo,
            401,
            "Coder explicitly escalated this PR. "
            "Reason: transient infra failure. Manual review required.",
        )
    ]
    assert any(
        "failed to apply escalated label to PR #401: gh down"
        in e["event"]
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# Test 4 — iteration-cap + label apply failure transitions to ERROR
# ---------------------------------------------------------------------------


def test_iteration_cap_label_apply_failure_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iteration-cap escalation with a label-apply failure goes to ERROR.

    The iteration-cap escalation path differs intentionally from the
    coder-initiated path: a daemon-driven escalation does not have a
    self-reported parking signal to fall back on, so the failure to
    publish the durable ``escalated`` label is a hard error. The
    runner records the GitHub mutation reason in ``error_message``
    and surfaces it via a ``[FIX]`` log prefix so it is grouped with
    other FIX-handler diagnostics. PR-220 will replace this method
    with a parameterised escalation primitive; the ERROR terminal
    state must remain distinct from coder ESCALATE's HUNG.
    """
    posted: list[tuple[str, int, str]] = []
    label_create_calls: list[list[str]] = []

    monkeypatch.setattr(
        fix_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            label_create_calls.append(cmd)
            return ""
        if cmd[:2] == ["pr", "edit"]:
            raise subprocess.CalledProcessError(
                1, cmd, stderr="rate limit exceeded"
            )
        return ""

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(
        number=402,
        branch="pr-402-cap",
        fix_iteration_count=cap,
    )
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_iteration_cap(pr))

    assert runner.state.state == PipelineState.ERROR
    assert pr.is_escalated is False
    assert runner.state.error_message is not None
    assert runner.state.error_message.startswith("pr edit failed:")
    assert posted == [
        (
            runner.owner_repo,
            402,
            f"@AlexBomber12 FIX iteration cap reached ({cap}/{cap}). "
            "Escalating for manual review.",
        )
    ]
    assert label_create_calls and label_create_calls[0][:3] == [
        "label",
        "create",
        "escalated",
    ]
    assert any(
        e["event"].startswith("[FIX] pr edit failed:")
        for e in runner.state.history
    )
