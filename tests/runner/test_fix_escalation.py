"""Direct unit tests for ``src/daemon/fix_escalation.py`` (PR-230).

The escalation methods on ``FixMixin`` are thin wrappers over these
module-level functions; the wrappers are still exercised by the regression
tests in ``test_recovery.py``. This file pins the contract of the module
functions themselves so a future refactor of the wrappers (or removal of a
wrapper) does not silently regress the underlying behavior.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.daemon import fix_escalation
from src.models import PipelineState, PRInfo
from tests.runner import _helpers as h


@pytest.mark.parametrize(
    "stdout, expected",
    [
        ("doing things\nESCALATE: rate limit exceeded\n", "rate limit exceeded"),
        ("ESCALATE: foo\n\n\n", "foo"),
        ("ESCALATE: stale\nfollow-up unrelated\n", None),
        ("", None),
        ("ran tests\nall good\n", None),
        ("ESCALATE:\n", ""),
        ("escalate: typo\n", None),
        ("  ESCALATE: indented\n", None),
    ],
)
def test_parse_escalate_marker_module_level(stdout: str, expected: str | None) -> None:
    """Parser is now exposed at module level on ``fix_escalation``."""
    assert fix_escalation.parse_escalate_marker(stdout) == expected


def test_escalate_fix_no_push_deadlock_transitions_to_hung_and_posts_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No-push deadlock: HUNG + ``escalated`` label + comment + counter reset."""
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )

    runner = h._make_runner()
    pr = PRInfo(number=500, branch="pr-500", no_push_fix_count=3)
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(fix_escalation.escalate_fix_no_push_deadlock(runner, pr))

    assert runner.state.state == PipelineState.HUNG
    assert pr.is_escalated is True
    assert pr.no_push_fix_count == 0
    assert len(posted) == 1
    assert posted[0][1] == 500
    assert "FIX deadlock: 3 consecutive no-push FIX cycles" in posted[0][2]
    assert ["pr", "edit", "500", "--add-label", "escalated"] in gh_calls


def test_escalate_fix_coder_initiated_label_success_parks_in_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coder ESCALATE with successful label apply transitions to IDLE."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda cmd, **kw: "")

    runner = h._make_runner()
    pr = PRInfo(number=501, branch="pr-501")
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(
        fix_escalation.escalate_fix_coder_initiated(runner, pr, "auth expired")
    )

    assert runner.state.state == PipelineState.IDLE
    assert pr.is_escalated is True
    assert posted == [
        (
            runner.owner_repo,
            501,
            "Coder explicitly escalated this PR. Reason: auth expired. "
            "Manual review required.",
        )
    ]


def test_escalate_fix_coder_initiated_empty_reason_substitutes_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty reason gets the ``(no reason provided)`` placeholder."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda cmd, **kw: "")

    runner = h._make_runner()
    pr = PRInfo(number=502, branch="pr-502")
    runner.state.current_pr = pr

    asyncio.run(fix_escalation.escalate_fix_coder_initiated(runner, pr, ""))

    assert "Reason: (no reason provided)" in posted[0][2]


def test_escalate_fix_iteration_cap_success_path_idle_with_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iteration-cap success path: comment + label + IDLE transition."""
    posted: list[tuple[str, int, str]] = []
    gh_calls: list[list[str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kwargs: gh_calls.append(cmd) or "",
    )

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(number=503, branch="pr-503", fix_iteration_count=cap)
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(fix_escalation.escalate_fix_iteration_cap(runner, pr))

    assert runner.state.state == PipelineState.IDLE
    assert any(
        "@AlexBomber12 FIX iteration cap reached" in body
        for _, _, body in posted
    )
    assert ["pr", "edit", "503", "--add-label", "escalated"] in gh_calls


def test_escalate_fix_iteration_cap_post_comment_failure_routes_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``post_comment`` failure routes to ERROR, not HUNG/IDLE."""

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("comment service down")

    monkeypatch.setattr("src.github.comments.post_comment", boom)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda cmd, **kw: "")

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(number=504, branch="pr-504", fix_iteration_count=cap)
    runner.state.current_pr = pr

    asyncio.run(fix_escalation.escalate_fix_iteration_cap(runner, pr))

    assert runner.state.state == PipelineState.ERROR
    assert pr.is_escalated is False
    assert runner.state.error_message is not None
    assert "post_comment failed" in runner.state.error_message


def test_ensure_escalated_label_returns_false_on_apply_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Soft-failure on ``pr edit --add-label`` is reported via the return."""

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh outage")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert (
        fix_escalation.ensure_escalated_label(runner, 700, "FIX no-push")
        is False
    )
    assert any(
        "failed to apply escalated label to PR #700" in event["event"]
        for event in runner.state.history
    )
