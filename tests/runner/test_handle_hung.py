"""PR-249: WATCH<->HUNG retrigger cap (OBS-BL).

These tests pin the contract for the per-PR ``watch_retrigger_count``
counter that ``handle_hung`` increments on every ``@codex review``
nudge and that ``handle_watch`` resets when fresh review activity
arrives. Without the cap, a codex-silent PR can cycle WATCH timeout ->
HUNG retrigger -> WATCH for hours, burning operator API budget on a
review that never arrives. After this PR ships, the runner escalates
to HUNG with the ``escalated`` label after ``daemon.watch_retrigger_cap``
cycles instead of looping.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.config import AppConfig, DaemonConfig
from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    PipelineState,
    PRInfo,
    ReviewStatus,
)

from tests.runner import _helpers as h


def _patch_label_calls(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Capture every ``run_gh`` invocation made by ``_ensure_escalated_label``."""
    gh_calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> Any:
        gh_calls.append(cmd)
        if cmd[:2] == ["pr", "view"]:
            return {"state": "OPEN"}
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    return gh_calls


def test_watch_retrigger_increments_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each successful ``@codex review`` retrigger from HUNG bumps the counter."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    runner = h._make_runner()
    # Cap of 5 so a single retrigger never escalates.
    runner.app_config.daemon.watch_retrigger_cap = 5
    runner.state.state = PipelineState.HUNG
    runner.state.current_pr = PRInfo(number=42, branch="pr-042")
    assert runner.state.current_pr.watch_retrigger_count == 0

    asyncio.run(runner.handle_hung())

    assert posted == [(runner.owner_repo, 42, "@codex review")]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.watch_retrigger_count == 1

    runner.state.state = PipelineState.HUNG
    asyncio.run(runner.handle_hung())

    assert len(posted) == 2
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.watch_retrigger_count == 2


def test_watch_retrigger_cap_escalates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At the cap, escalation fires (IDLE + ``escalated`` label) instead of post."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    gh_calls = _patch_label_calls(monkeypatch)

    runner = h._make_runner()
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(number=99, branch="pr-099", watch_retrigger_count=2)
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    # Cap reached at next_count=3 == cap=3: no @codex review post, escalate.
    assert posted == []
    assert runner.state.state == PipelineState.IDLE
    assert pr.is_escalated is True
    assert ["pr", "edit", "99", "--add-label", "escalated"] in gh_calls
    assert runner.state.error_message is not None
    assert "watch_retrigger_cap_reached" in runner.state.error_message
    assert any(
        "watch_retrigger cap reached (3/3); escalating instead of "
        "resetting WATCH" in entry["event"]
        for entry in runner.state.history
    )


def test_watch_retrigger_cap_below_threshold_still_posts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Counter just below cap still posts — only the cap-hit cycle escalates."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    runner = h._make_runner()
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(number=99, branch="pr-099", watch_retrigger_count=1)
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    # next_count=2 < cap=3: posts and transitions to WATCH.
    assert posted == [(runner.owner_repo, 99, "@codex review")]
    assert runner.state.state == PipelineState.WATCH
    assert pr.is_escalated is False
    assert pr.watch_retrigger_count == 2


def test_fresh_review_resets_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Signature change in ``_observe_watch_event_signature`` resets counter."""
    fresh_pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [fresh_pr],
    )
    # CHANGES_REQUESTED + NEW codex feedback dispatches into handle_fix.
    # Stub it out so the test focuses on the reset.
    monkeypatch.setattr(
        PipelineRunner,
        "_has_new_codex_feedback_since_last_push",
        lambda self: FeedbackCheckResult.NEW,
    )

    handle_fix_calls: list[None] = []

    async def fake_handle_fix(self) -> None:
        handle_fix_calls.append(None)

    monkeypatch.setattr(PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        watch_retrigger_count=2,
    )
    # Prime the signature so the next observe sees a CHANGE.
    runner._watch_last_event_signature = (
        42,
        CIStatus.PENDING,
        ReviewStatus.EYES,
        None,
    )

    asyncio.run(runner.handle_watch())

    assert handle_fix_calls == [None]
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.watch_retrigger_count == 0


def test_no_signature_change_preserves_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A WATCH cycle that observes no fresh activity preserves the counter."""
    fresh_pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [fresh_pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 0,
    )
    monkeypatch.setattr(
        PipelineRunner,
        "_maybe_retrigger_on_codex_bot_error",
        lambda self, pr_number: False,
    )
    monkeypatch.setattr(
        PipelineRunner,
        "_maybe_retrigger_stale_review",
        lambda self, pr_number: False,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        watch_retrigger_count=2,
    )
    # Identical prior signature: ``_observe_watch_event_signature`` will
    # NOT mark the cycle as fresh, so the counter must persist.
    runner._watch_last_event_signature = (
        42,
        CIStatus.PENDING,
        ReviewStatus.EYES,
        None,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.watch_retrigger_count == 2


def test_watch_retrigger_cap_configurable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Changing ``daemon.watch_retrigger_cap`` changes the effective cap."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    _patch_label_calls(monkeypatch)

    # cap=2 escalates earlier than the default of 3.
    runner = PipelineRunner(
        h._repo_cfg(),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(watch_retrigger_cap=2),
        ),
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(number=7, branch="pr-007", watch_retrigger_count=1)
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    # next_count=2 == cap=2 -> escalates without posting.
    assert posted == []
    assert runner.state.state == PipelineState.IDLE
    assert pr.is_escalated is True
    assert any(
        "(2/2); escalating instead of resetting WATCH" in entry["event"]
        for entry in runner.state.history
    )


def test_codex_review_post_failure_does_not_increment_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing ``post_comment`` parks in ERROR and leaves the counter alone."""

    def boom(repo: str, number: int, body: str) -> None:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr("src.github.comments.post_comment", boom)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(number=5, branch="pr-005", watch_retrigger_count=1)
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    assert runner.state.state == PipelineState.ERROR
    # Counter is unchanged: the cap accounts only for *successful* nudges,
    # so a transient gh outage cannot accelerate escalation.
    assert pr.watch_retrigger_count == 1


def test_hung_refresh_resets_counter_on_fresh_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real GH event between WATCH and HUNG resets the counter via in-HUNG refresh.

    ``handle_watch`` zeroes ``watch_retrigger_count`` only while it is
    the active handler. If a Codex comment / push lands after the WATCH
    timeout escalation but before the next HUNG cycle, the counter
    would otherwise still be at the pre-event value and the cap could
    false-escalate an active PR. The refresh inside ``handle_hung``
    must reset on a signature change.
    """
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )
    fresh_pr = PRInfo(
        number=99,
        branch="pr-099",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [fresh_pr],
    )

    runner = h._make_runner()
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(
        number=99,
        branch="pr-099",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        watch_retrigger_count=2,
    )
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    # Cap would have escalated at 2->3; the signature change resets to 0
    # first, so this cycle posts and increments to 1 instead.
    assert posted == [(runner.owner_repo, 99, "@codex review")]
    assert runner.state.state == PipelineState.WATCH
    assert pr.is_escalated is False
    assert pr.watch_retrigger_count == 1


def test_hung_refresh_no_signature_change_still_escalates_at_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the refresh shows no fresh activity, the cap still fires."""
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    gh_calls = _patch_label_calls(monkeypatch)
    same_pr = PRInfo(
        number=99,
        branch="pr-099",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [same_pr],
    )

    runner = h._make_runner()
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(
        number=99,
        branch="pr-099",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        watch_retrigger_count=2,
    )
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    # Identical signature -> no reset -> cap fires as before.
    assert posted == []
    assert runner.state.state == PipelineState.IDLE
    assert pr.is_escalated is True
    assert ["pr", "edit", "99", "--add-label", "escalated"] in gh_calls


def test_hung_refresh_failure_stays_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``get_open_prs`` failure during the cap-check refresh fail-safes to HUNG.

    Without the fail-safe, a transient GH API blip could silently
    short-circuit the activity check, leaving the original code path
    free to false-escalate an active PR.
    """
    posted: list[tuple[str, int, str]] = []
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "OPEN"},
    )

    def boom(repo: str, **kw: Any) -> list[PRInfo]:
        raise RuntimeError("gh down")

    monkeypatch.setattr("src.github.prs.get_open_prs", boom)

    runner = h._make_runner()
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.state = PipelineState.HUNG
    pr = PRInfo(number=99, branch="pr-099", watch_retrigger_count=2)
    runner.state.current_pr = pr

    asyncio.run(runner.handle_hung())

    assert posted == []
    assert runner.state.state == PipelineState.HUNG
    assert pr.is_escalated is False
    assert pr.watch_retrigger_count == 2
    assert any(
        "failed to refresh PR activity for cap check" in entry["event"]
        for entry in runner.state.history
    )
