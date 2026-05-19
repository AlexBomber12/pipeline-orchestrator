"""PR-352: dashboard sub-phase visibility for the MERGE state.

The MERGE handler walks four distinct phases (pre_merge_sync,
ready_to_merge, merging, post_merge_cleanup) and the per-phase value is
published to Redis between transitions so the dashboard can render
``MERGE • pre merge sync`` while a long cycle runs.

These tests pin:

* each phase is observed on at least one ``publish_state`` call during
  the matching step
* the field clears on the natural MERGE→IDLE transition via the
  ``RepoState.__setattr__`` hook
* the field clears when ``_transition_to_error`` fires from a mid-phase
  exception (the hook fires on MERGE→ERROR too)
* the dashboard subtitle renders only when state is MERGE and
  ``merge_phase`` is set
* the value survives Pydantic JSON round-trip (Redis HSET path)
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.handlers import merge as merge_module
from src.daemon.runner import PipelineRunner
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web.app import templates

from tests.runner import _helpers as h


def _seed_runner_in_merge() -> PipelineRunner:
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=7, branch="pr-007")
    runner.state.current_task = QueueTask(
        pr_id="PR-007", title="t", status=TaskStatus.DOING
    )
    return runner


def _capture_phases(
    runner: PipelineRunner, sink: list[str | None]
) -> None:
    """Stub ``publish_state`` to record ``merge_phase`` per call."""

    async def fake_publish(self) -> None:  # type: ignore[no-untyped-def]
        sink.append(self.state.merge_phase)

    runner.publish_state = fake_publish.__get__(  # type: ignore[assignment]
        runner, PipelineRunner
    )


def _stub_successful_merge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Wire fakes so ``handle_merge`` walks the happy path top to bottom."""

    def fake_git(repo_path: str, *args: str, **kwargs: object):
        if args[:1] == ("merge",) and len(args) > 1 and str(args[1]).startswith(
            "origin/"
        ):
            return h._FakeCompletedProcess(
                stdout="Already up to date.\n", returncode=0
            )
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="deadbeef\n", returncode=0)
        return h._FakeCompletedProcess(stdout="", returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(merge_module, "retry_transient", lambda op, **_: op())
    monkeypatch.setattr(
        "src.github.cache._invalidate_etag_cache", lambda prefix: None
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(
        PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )
    monkeypatch.setattr(
        runner_module.subprocess,
        "run",
        lambda *a, **kw: h._FakeCompletedProcess(stdout="", returncode=0),
    )


def test_merge_phase_pre_merge_sync_during_rebase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_successful_merge(monkeypatch)
    runner = _seed_runner_in_merge()
    phases: list[str | None] = []
    _capture_phases(runner, phases)

    asyncio.run(runner.handle_merge())

    assert phases[0] == "pre_merge_sync"


def test_merge_phase_ready_to_merge_during_gh_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_successful_merge(monkeypatch)
    runner = _seed_runner_in_merge()
    phases: list[str | None] = []
    _capture_phases(runner, phases)

    asyncio.run(runner.handle_merge())

    assert "ready_to_merge" in phases
    assert phases.index("ready_to_merge") > phases.index("pre_merge_sync")


def test_merge_phase_merging_during_gh_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_successful_merge(monkeypatch)
    runner = _seed_runner_in_merge()
    phases: list[str | None] = []
    _capture_phases(runner, phases)

    asyncio.run(runner.handle_merge())

    assert "merging" in phases
    assert phases.index("merging") > phases.index("ready_to_merge")


def test_merge_phase_post_merge_cleanup_after_merge_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_successful_merge(monkeypatch)
    runner = _seed_runner_in_merge()
    phases: list[str | None] = []
    _capture_phases(runner, phases)

    asyncio.run(runner.handle_merge())

    assert "post_merge_cleanup" in phases
    assert phases.index("post_merge_cleanup") > phases.index("merging")


def test_merge_phase_cleared_on_idle_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _stub_successful_merge(monkeypatch)
    runner = _seed_runner_in_merge()

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.merge_phase is None


def test_merge_phase_persists_across_publish_cycles() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
        state=PipelineState.MERGE,
    )
    state.merge_phase = "merging"

    restored = RepoState.model_validate_json(state.model_dump_json())

    assert restored.merge_phase == "merging"
    assert restored.state == PipelineState.MERGE


def _render_repo_cards(repo: RepoState) -> str:
    return templates.get_template("components/repo_cards.html").render(
        repos=[repo],
        resources=None,
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress={},
        css_escape=lambda v: v,
        upload_feedback_target=lambda _name: "",
        utcnow=lambda: datetime.now(timezone.utc),
    )


def test_dashboard_renders_phase_under_merge_badge() -> None:
    repo = RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
        state=PipelineState.MERGE,
    )
    repo.merge_phase = "pre_merge_sync"

    html = _render_repo_cards(repo)

    assert "pre merge sync" in html
    assert "data-merge-phase" in html


def test_dashboard_omits_phase_for_non_merge_state() -> None:
    repo = RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
        state=PipelineState.CODING,
    )
    # Field set to a non-None value, but state is not MERGE, so the
    # subtitle must NOT render. The state field is the gate the template
    # checks; ``merge_phase`` alone is never enough.
    repo.merge_phase = "pre_merge_sync"

    html = _render_repo_cards(repo)

    assert "pre merge sync" not in html
    assert "data-merge-phase" not in html


def test_merge_phase_cleared_on_error_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate an exception inside ``gh_prs.merge_pr`` mid-``merging``.

    ``_transition_to_error`` sets ``state.state = ERROR`` which triggers
    the ``__setattr__`` hook to clear ``merge_phase``. The forensic value
    survives in Redis until the next ``publish_state`` (see spec
    threat-model note), but the in-memory state must read None as soon as
    the MERGE→ERROR transition lands so the dashboard subtitle disappears.
    """

    def fake_git(repo_path: str, *args: str, **kwargs: object):
        if args[:1] == ("merge",) and len(args) > 1 and str(args[1]).startswith(
            "origin/"
        ):
            return h._FakeCompletedProcess(
                stdout="Already up to date.\n", returncode=0
            )
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="deadbeef\n", returncode=0)
        return h._FakeCompletedProcess(stdout="", returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(merge_module, "retry_transient", lambda op, **_: op())
    monkeypatch.setattr(
        "src.github.cache._invalidate_etag_cache", lambda prefix: None
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    def boom(repo: str, num: int) -> None:
        raise RuntimeError("merge_pr failed mid-flight")

    monkeypatch.setattr("src.github.prs.merge_pr", boom)
    monkeypatch.setattr(
        PipelineRunner,
        "_mark_task_done_in_snapshot",
        lambda self: None,
    )
    monkeypatch.setattr(
        runner_module.subprocess,
        "run",
        lambda *a, **kw: h._FakeCompletedProcess(stdout="", returncode=0),
    )

    runner = _seed_runner_in_merge()
    phases_observed: list[str | None] = []
    _capture_phases(runner, phases_observed)

    asyncio.run(runner.handle_merge())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.merge_phase is None
    # ``merging`` was visible on the publish that ran immediately before
    # ``merge_pr`` raised, proving the forensic value reached Redis.
    assert "merging" in phases_observed


def test_merge_phase_setattr_clears_on_transition_out_of_merge() -> None:
    """Direct ``__setattr__`` regression guard for the hook."""
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
        state=PipelineState.MERGE,
    )
    state.merge_phase = "merging"

    state.state = PipelineState.IDLE

    assert state.merge_phase is None


def test_merge_phase_setattr_preserves_on_same_state() -> None:
    """Hook fires only on MERGE→non-MERGE transitions."""
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
        state=PipelineState.MERGE,
    )
    state.merge_phase = "pre_merge_sync"

    state.state = PipelineState.MERGE

    assert state.merge_phase == "pre_merge_sync"


def test_merge_phase_default_none() -> None:
    state = RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
    )

    assert state.merge_phase is None
