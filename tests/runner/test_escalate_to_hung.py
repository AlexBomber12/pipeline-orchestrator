"""PR-220: ``_escalate_to_hung`` primitive tests.

These tests pin the contract for the unified HUNG escalation primitive
that PR-220 introduces. They exercise the primitive in isolation; the
PR-212 recovery interaction tests cover the migration of the three
``_escalate_fix_*`` wrappers and the inlined escalation sites in
coding/idle/watch are covered by their existing handler tests.

After PR-220 ships, escalations to ``PipelineState.HUNG`` driven by
daemon-side logic (max breach, terminal coder error, FIX iteration
cap, FIX no-push deadlock, coder ESCALATE marker, coder unavailable,
review timeout) flow through this primitive. Direct
``state.state = PipelineState.HUNG`` writes are reserved for legitimate
non-escalation transitions (e.g. operator-initiated stop).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.daemon import runner as runner_module
from src.daemon.handlers import fix as fix_module
from src.models import PipelineState, PRInfo

from tests import test_runner as h


def _install_publish_state_spy(runner: Any) -> list[None]:
    """Replace ``publish_state`` with an awaitable spy and return the call log."""
    calls: list[None] = []

    async def fake_publish() -> None:
        calls.append(None)

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def _patch_label_calls(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Capture every ``run_gh`` invocation made by ``_ensure_escalated_label``."""
    gh_calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        gh_calls.append(cmd)
        return ""

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)
    return gh_calls


def test_default_args_park_in_hung_apply_label_no_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default arguments transition to HUNG, apply the label, no comment."""
    gh_calls = _patch_label_calls(monkeypatch)
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    pr = PRInfo(number=500, branch="pr-500")
    runner.state.current_pr = pr
    publish_calls = _install_publish_state_spy(runner)

    asyncio.run(runner._escalate_to_hung("park me"))

    assert runner.state.state == PipelineState.HUNG
    assert runner.state.error_message == "park me"
    assert pr.is_escalated is True
    assert posted == []
    assert ["pr", "edit", "500", "--add-label", "escalated"] in gh_calls
    assert any(
        e["event"] == "[ESCALATE] park me" for e in runner.state.history
    )
    assert publish_calls == [None]


def test_target_state_error_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``target_state=ERROR`` parks the runner in ERROR (iteration-cap path)."""
    _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    pr = PRInfo(number=501, branch="pr-501")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "iteration cap",
            target_state=PipelineState.ERROR,
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "iteration cap"
    assert pr.is_escalated is True


def test_apply_escalated_label_calls_pr_edit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``apply_escalated_label=True`` invokes ``pr edit --add-label`` on the PR."""
    gh_calls = _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    pr = PRInfo(number=502, branch="pr-502")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "park me",
            label_create_log_prefix="custom",
        )
    )

    assert any(
        cmd[:3] == ["label", "create", "escalated"] for cmd in gh_calls
    )
    assert ["pr", "edit", "502", "--add-label", "escalated"] in gh_calls


def test_label_apply_failure_still_sets_in_memory_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed ``pr edit`` keeps ``is_escalated=True`` (durable parking signal)."""

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr(fix_module.github_client, "run_gh", fake_run_gh)

    runner = h._make_runner()
    pr = PRInfo(number=503, branch="pr-503")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    label_applied = asyncio.run(runner._escalate_to_hung("park me"))

    assert label_applied is False
    assert pr.is_escalated is True
    assert runner.state.state == PipelineState.HUNG
    assert any(
        "failed to apply escalated label to PR #503: gh down"
        in e["event"]
        for e in runner.state.history
    )


def test_post_comment_on_pr_calls_post_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``post_comment_on_pr`` forwards the body to ``github_client.post_comment``."""
    _patch_label_calls(monkeypatch)
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    runner = h._make_runner()
    pr = PRInfo(number=504, branch="pr-504")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "park me",
            post_comment_on_pr="please review",
        )
    )

    assert posted == [(runner.owner_repo, 504, "please review")]


def test_post_comment_failure_logged_not_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raising ``post_comment`` does not crash the primitive."""
    _patch_label_calls(monkeypatch)

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh down")

    monkeypatch.setattr(runner_module.github_client, "post_comment", boom)

    runner = h._make_runner()
    pr = PRInfo(number=505, branch="pr-505")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "park me",
            post_comment_on_pr="please review",
        )
    )

    assert runner.state.state == PipelineState.HUNG
    assert any(
        "failed to post escalation comment on PR #505: gh down"
        in e["event"]
        for e in runner.state.history
    )


def test_publish_state_called_at_end(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The primitive calls ``publish_state`` after every other side effect."""
    _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    pr = PRInfo(number=506, branch="pr-506")
    runner.state.current_pr = pr
    publish_calls = _install_publish_state_spy(runner)

    asyncio.run(runner._escalate_to_hung("park me"))

    assert publish_calls == [None]


def test_error_message_override_clears_error_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``error_message_override=None`` clears ``state.error_message``.

    The no-push deadlock wrapper relies on this so HUNG itself remains
    the parking signal without an operator-visible error_message
    leaking from the prior FIX cycle.
    """
    _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    pr = PRInfo(number=507, branch="pr-507")
    runner.state.current_pr = pr
    runner.state.error_message = "stale"
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "park me",
            error_message_override=None,
        )
    )

    assert runner.state.error_message is None
    assert any(
        e["event"] == "[ESCALATE] park me" for e in runner.state.history
    )


def test_log_message_overrides_log_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``log_message`` replaces the body after ``[ESCALATE]`` while error_message stays."""
    _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    pr = PRInfo(number=508, branch="pr-508")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "stored",
            log_message="logged.",
        )
    )

    assert runner.state.error_message == "stored"
    assert any(
        e["event"] == "[ESCALATE] logged." for e in runner.state.history
    )


def test_set_pr_escalated_flag_false_skips_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``set_pr_escalated_flag=False`` leaves ``PRInfo.is_escalated`` untouched."""
    _patch_label_calls(monkeypatch)
    runner = h._make_runner()
    pr = PRInfo(number=509, branch="pr-509")
    runner.state.current_pr = pr
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "park me",
            apply_escalated_label=False,
            set_pr_escalated_flag=False,
        )
    )

    assert pr.is_escalated is False
    assert runner.state.state == PipelineState.HUNG


def test_no_current_pr_skips_label_and_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With ``current_pr=None`` the primitive skips PR-scoped side effects."""
    _patch_label_calls(monkeypatch)
    posted: list[Any] = []
    monkeypatch.setattr(
        runner_module.github_client,
        "post_comment",
        lambda *a, **kw: posted.append(a),
    )

    runner = h._make_runner()
    runner.state.current_pr = None
    _install_publish_state_spy(runner)

    asyncio.run(
        runner._escalate_to_hung(
            "park me",
            post_comment_on_pr="ignored",
        )
    )

    assert runner.state.state == PipelineState.HUNG
    assert posted == []
