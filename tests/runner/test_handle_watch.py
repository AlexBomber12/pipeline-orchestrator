"""PR-224a: handle_watch handler tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import runner as runner_module
from src.daemon.handlers import watch as watch_module
from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


def test_observe_watch_event_signature_resets_retrigger_count() -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        watch_retrigger_count=2,
    )
    prior = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    updated = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
    )

    runner._observe_watch_event_signature(prior)
    runner._observe_watch_event_signature(updated)

    assert runner.state.current_pr.watch_retrigger_count == 0


def test_handle_watch_approved_and_green_merges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        head_sha="green001",
        diff_scanned_at_sha="green001",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    merged: list[tuple[str, int]] = []

    def fake_merge(repo: str, number: int) -> None:
        merged.append((repo, number))

    monkeypatch.setattr("src.github.prs.merge_pr", fake_merge)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_task_done_in_snapshot", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert merged == [(runner.owner_repo, 5)]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_watch_without_current_pr_returns_to_idle() -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.history[-1]["event"] == "[WATCH] WATCH without current_pr -> IDLE."


def test_handle_watch_open_pr_lookup_failure_sets_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")

    def _raise(repo: str, **kwargs: Any) -> list[PRInfo]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        _raise,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: boom"
    assert runner.state.history[-1]["event"] == "[WATCH] boom."


def test_handle_watch_green_but_auto_merge_disabled_stays_watching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        head_sha="green005",
        diff_scanned_at_sha="green005",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number: merged.append((repo, number)),
    )

    runner = h._make_runner(auto_merge=False)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert any("auto_merge disabled" in e["event"] for e in runner.state.history)


def test_handle_watch_changes_requested_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert any("Fix pushed" in e["event"] for e in runner.state.history)


def test_handle_watch_preserves_fix_iteration_count_for_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        fix_iteration_count=7,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.fix_iteration_count == 7


def test_handle_watch_preserves_no_push_fix_count_for_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The no-push counter must survive the per-cycle PRInfo refresh from
    ``get_open_prs``; otherwise the deadlock circuit breaker can never
    accumulate past 1 across WATCH↔FIX cycles."""
    pr = PRInfo(
        number=9,
        branch="pr-009",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=9,
        branch="pr-009",
        no_push_fix_count=2,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 2


def test_handle_watch_counts_new_head_sha_after_pre_pr195_upgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-PR-195 persisted ``current_pr`` (``push_count > 0`` with empty
    ``observed_head_shas``) must register a freshly polled head SHA as a
    new push. The earlier ``max(len(merged), push_count)`` formula left
    the legacy counter winning until the SHA set caught up, dropping the
    polled push.
    """
    polled = PRInfo(
        number=15,
        branch="pr-015",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        observed_head_shas={"polled-head-sha"},
        push_count=1,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [polled])

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=15,
        branch="pr-015",
        push_count=4,
        observed_head_shas=set(),
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.observed_head_shas == {"polled-head-sha"}
    assert runner.state.current_pr.push_count == 5


def test_watch_changes_requested_with_pending_ci_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-248: when CI is PENDING and review is CHANGES_REQUESTED with new
    Codex feedback, CHANGES_REQUESTED takes precedence over PENDING and a
    fix is triggered. Pre-PR-248 the PENDING branch swallowed this path."""
    last_push = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    comments = [
        {
            "user": {"login": "chatgpt-codex-connector"},
            "body": "P1: missing null check",
            "created_at": recent.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    ]

    def fake_paginated(path: str) -> list[dict]:
        if "issues" in path:
            return comments
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )

    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_watch_changes_requested_with_pending_ci_no_feedback_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-248: CI=PENDING + review=CHANGES_REQUESTED + no new Codex
    feedback must NOT be swallowed by the PENDING branch — the
    CHANGES_REQUESTED no-new-feedback log line must be emitted instead."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )

    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert any(
        "CHANGES_REQUESTED but no new" in e["event"]
        for e in runner.state.history
    )


def test_handle_watch_fix_when_ci_success_and_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1
    assert any("Fix pushed" in e["event"] for e in runner.state.history)


def test_handle_watch_ci_failure_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1


def test_handle_watch_reclassified_pending_routes_to_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-250: stuck-PENDING reclassification routes WATCH through handle_fix.

    The PR's raw CI status is PENDING, but the Redis tracker shows the
    same head_sha has been PENDING longer than ``ci_pending_max_min``
    minutes. ``classify_ci_status_with_age`` reclassifies as FAILURE,
    a ``stuck_pending`` event is logged, and ``handle_fix`` is invoked.
    """
    from src.github import checks as gh_checks

    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        head_sha="cafef00d",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: ([{"status": "in_progress"}], {}, True),
    )

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.app_config.daemon.ci_pending_max_min = 30
    fixed_first_seen = 1_700_000_000.0
    key = gh_checks._pending_tracker_key(runner.owner_repo, 42, "cafef00d")
    asyncio.run(runner.redis.set(key, str(fixed_first_seen)))
    monkeypatch.setattr(
        gh_checks.time, "time", lambda: fixed_first_seen + 60 * 60
    )

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042")
    asyncio.run(runner.handle_watch())

    assert fix_calls == [None]
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.FAILURE
    history_events = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "stuck_pending" in history_events
    assert "reclassified PENDING -> FAILURE" in history_events


def test_handle_watch_pending_within_threshold_does_not_reclassify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-250: PENDING freshness short of ``ci_pending_max_min`` stays PENDING."""
    from src.github import checks as gh_checks

    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=43,
        branch="pr-043",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        head_sha="deadbeef",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: ([{"status": "in_progress"}], {}, True),
    )

    runner = h._make_runner()
    runner.app_config.daemon.ci_pending_max_min = 30
    fixed_first_seen = 1_700_000_000.0
    key = gh_checks._pending_tracker_key(runner.owner_repo, 43, "deadbeef")
    asyncio.run(runner.redis.set(key, str(fixed_first_seen)))
    # 5 min elapsed, threshold is 30 — wrapper returns PENDING with no reason.
    monkeypatch.setattr(
        gh_checks.time, "time", lambda: fixed_first_seen + 5 * 60
    )

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=43, branch="pr-043")
    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.PENDING
    history_events = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "stuck_pending" not in history_events
    assert "reclassified" not in history_events


def test_handle_watch_timeout_transitions_to_error_with_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-316 (OBS-DD): review_timeout now terminates in ERROR, not IDLE.

    The picker re-selects status:TODO tasks, so routing back to IDLE on a
    genuine stale review caused a WATCH→ESCALATE→IDLE→re-pick→WATCH loop.
    The cause payload still carries ``subsource=review_timeout`` so the
    dashboard can dispatch on the same key as before.
    """
    recorded: list[tuple[str, str, str, dict[str, object]]] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append((repo_slug, task_id, cause.category, cause.payload))

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-005",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
    )

    async def fake_commit(self, current_task, status, reason):
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    # PR-316 follow-up: the persisted park flag tells run_cycle to skip
    # the AI diagnose loop on subsequent ERROR cycles.
    assert runner.state.skip_ai_error_diagnose is True
    assert recorded == [
        (
            runner.name,
            "PR-005",
            "ERROR",
            {
                "subsource": "review_timeout",
                "reason_text": "PR #5 hung after 90m (review=EYES, ci=PENDING)",
                "previous_state": "WATCH",
                "elapsed_min": 90,
                "ci_status": "PENDING",
                "review_status": "EYES",
            },
        )
    ]


def test_watch_review_timeout_writes_frontmatter_status_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-316: review_timeout must invoke _commit_task_status_change with
    status="ERROR" (which in turn calls write_frontmatter_status) so the
    picker stops re-selecting the status:TODO task on next cycle."""
    stale = datetime.now(timezone.utc) - timedelta(minutes=45)
    pr = PRInfo(
        number=9,
        branch="pr-009",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )

    commit_calls: list[tuple[Any, str, str]] = []

    async def fake_commit(self, current_task, status, reason):
        commit_calls.append((current_task, status, reason))
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit,
    )

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=9, branch="pr-009")
    task = QueueTask(
        pr_id="PR-009",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-009",
        task_file="tasks/PR-009.md",
    )
    runner.state.current_task = task
    asyncio.run(runner.handle_watch())

    assert len(commit_calls) == 1
    recorded_task, status, _reason = commit_calls[0]
    assert getattr(recorded_task, "task_file", None) == "tasks/PR-009.md"
    assert status == "ERROR"


def test_watch_review_timeout_does_not_apply_escalated_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-316 review-feedback fix: review_timeout must NOT apply the
    ``escalated`` GitHub label.

    ``get_open_prs`` maps that label to ``PRInfo.is_escalated`` and
    ``handle_fix`` short-circuits to IDLE when that flag is true. Applying
    the label here would block the operator-Retry recovery flow, because a
    later ``CHANGES_REQUESTED`` or CI failure on the same PR could not
    re-enter FIX without manual label removal. Termination of the
    re-pick loop is delivered by the status:ERROR frontmatter write plus
    ``_transition_to_error``; the GitHub label is intentionally omitted.
    """
    stale = datetime.now(timezone.utc) - timedelta(minutes=45)
    pr = PRInfo(
        number=11,
        branch="pr-011",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    label_calls: list[tuple[int, str]] = []

    def fake_label(self, pr_number: int, label_create_log_prefix: str) -> bool:
        label_calls.append((pr_number, label_create_log_prefix))
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_ensure_escalated_label",
        fake_label,
    )

    async def fake_commit(self, current_task, status, reason):
        return True

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit,
    )

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=11, branch="pr-011")
    runner.state.current_task = QueueTask(
        pr_id="PR-011",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-011",
        task_file="tasks/PR-011.md",
    )
    asyncio.run(runner.handle_watch())

    assert label_calls == []
    assert runner.state.state == PipelineState.ERROR


def test_watch_review_timeout_status_write_exception_marks_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If _commit_task_status_change raises, watch.py must log the failure
    and fall through to _mark_status_write_failed_task instead of
    propagating the exception out of handle_watch."""
    stale = datetime.now(timezone.utc) - timedelta(minutes=45)
    pr = PRInfo(
        number=13,
        branch="pr-013",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    async def fake_commit_raise(self, current_task, status, reason):
        raise RuntimeError("git unavailable")

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit_raise,
    )

    marked: list[Any] = []

    async def fake_mark(self, current_task: Any) -> None:
        marked.append(current_task)

    monkeypatch.setattr(
        PipelineRunner,
        "_mark_status_write_failed_task",
        fake_mark,
    )

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=13, branch="pr-013")
    runner.state.current_task = QueueTask(
        pr_id="PR-013",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-013",
        task_file="tasks/PR-013.md",
    )
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert len(marked) == 1
    assert any(
        "Failed to write status:ERROR" in e["event"]
        for e in runner.state.history
    )


def test_watch_review_timeout_status_write_failure_marks_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When _commit_task_status_change soft-fails (returns False), the runner
    must record the in-memory parking fallback via _mark_status_write_failed_task
    so the picker still skips the task."""
    stale = datetime.now(timezone.utc) - timedelta(minutes=45)
    pr = PRInfo(
        number=12,
        branch="pr-012",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    async def fake_commit_fail(self, current_task, status, reason):
        return False

    monkeypatch.setattr(
        PipelineRunner,
        "_commit_task_status_change",
        fake_commit_fail,
    )

    marked: list[Any] = []

    async def fake_mark(self, current_task: Any) -> None:
        marked.append(current_task)

    monkeypatch.setattr(
        PipelineRunner,
        "_mark_status_write_failed_task",
        fake_mark,
    )

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=12, branch="pr-012")
    runner.state.current_task = QueueTask(
        pr_id="PR-012",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-012",
        task_file="tasks/PR-012.md",
    )
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert len(marked) == 1
    assert getattr(marked[0], "pr_id", None) == "PR-012"


def test_handle_watch_within_timeout_stays_watching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=fresh,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH


def test_handle_watch_naive_last_activity_is_treated_as_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recent = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=recent.replace(tzinfo=None),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert any("2/30m" in e["event"] for e in runner.state.history)


def test_handle_watch_approved_but_ci_pending_applies_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """APPROVED + CI PENDING used to fall through the branches in handle_watch,
    leaving the runner stuck in WATCH forever. It should now apply the review
    timeout and transition to ERROR (PR-316: was IDLE) when the PR stays
    pending past the configured timeout."""
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.APPROVED,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert any("review=APPROVED" in e["event"] and "ci=PENDING" in e["event"] for e in runner.state.history)


def test_handle_watch_falls_back_to_daemon_review_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a repo omits ``review_timeout_min``, hung detection must fall
    back to ``daemon.review_timeout_min``.

    Regression for a P2 Codex finding on PR-016: the runner previously
    only consulted ``self.repo_config.review_timeout_min``, so the new
    "Default review timeout" control in the Settings daemon section was
    persisted to ``config.yml`` but ignored at runtime — users thought
    they'd changed hung behavior while the daemon kept using whatever
    per-repo value the config had.
    """
    stale = datetime.now(timezone.utc) - timedelta(minutes=40)
    pr = PRInfo(
        number=7,
        branch="pr-002",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    # ``review_timeout_min=None`` on the repo → the runner must use the
    # daemon's 30-minute default. 40 minutes of inactivity is past that,
    # so the task is skipped to IDLE.
    repo_cfg = RepoConfig(
        url="https://github.com/octo/demo.git",
        branch="main",
        auto_merge=True,
        review_timeout_min=None,
        poll_interval_sec=60,
    )
    app_cfg = AppConfig(repositories=[], daemon=DaemonConfig(review_timeout_min=30))
    runner = PipelineRunner(
        repo_cfg,
        app_cfg,
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=7, branch="pr-002")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR


def test_handle_watch_repo_timeout_override_wins_over_daemon_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit per-repo ``review_timeout_min`` must override the
    daemon-level default.

    Belt-and-suspenders for the P2 fix: raising the daemon default must
    not silently shorten or lengthen the timeout on repos that pinned
    their own value via the existing per-repo Settings control (PR-015).
    """
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=8,
        branch="pr-003",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    # repo pins 120 min, daemon default is 30 min. 90 minutes of
    # inactivity is below the repo override, so the PR stays WATCH.
    repo_cfg = RepoConfig(
        url="https://github.com/octo/demo.git",
        branch="main",
        auto_merge=True,
        review_timeout_min=120,
        poll_interval_sec=60,
    )
    app_cfg = AppConfig(repositories=[], daemon=DaemonConfig(review_timeout_min=30))
    runner = PipelineRunner(
        repo_cfg,
        app_cfg,
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=8, branch="pr-003")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH


def test_handle_watch_approved_ci_pending_within_timeout_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.APPROVED,
        last_activity=fresh,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert any("waiting" in e["event"] and "review=APPROVED" in e["event"] for e in runner.state.history)


def test_handle_watch_captures_success_merged_on_external_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.is_pr_merged", lambda repo, number: True)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(recent) == 1
    assert recent[0].exit_reason == "success_merged"


def test_handle_watch_captures_closed_unmerged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.is_pr_merged", lambda repo, number: False)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert len(recent) == 1
    assert recent[0].exit_reason == "closed_unmerged"


def test_handle_watch_unknown_merge_state_logs_but_does_not_save(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.is_pr_merged", lambda repo, number: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert recent == []
    assert any("state unknown" in entry["event"] for entry in runner.state.history)


def test_handle_watch_skips_fix_no_new_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED with no new Codex P1/P2 after last push must not
    trigger handle_fix — the stale review is waiting for a fresh pass."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    # No Codex comments at all -> no new feedback.
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert any("no new Codex feedback" in e["event"] for e in runner.state.history)


def test_handle_watch_triggers_fix_new_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED with fresh P1 feedback after last push triggers fix."""
    last_push = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    comments = [
        {
            "user": {"login": "chatgpt-codex-connector"},
            "body": "P1: missing null check",
            "created_at": recent.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    ]

    def fake_paginated(path: str) -> list[dict]:
        if "issues" in path:
            return comments
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_handle_watch_still_fixes_ci_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI failure must still trigger fix regardless of Codex feedback state."""
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_handle_watch_stale_feedback_still_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + no new feedback must still escalate when the
    review timeout has elapsed. Early-returning here would pin the runner
    in WATCH forever for a sticky historical CHANGES_REQUESTED. PR-316
    moved the terminal state from IDLE to ERROR so the picker stops
    re-selecting the task."""
    last_push = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner(review_timeout_min=30)
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.ERROR


def test_handle_watch_retriggers_stale_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 11 * 60,
    )
    retriggers: list[int] = []
    bypass_flags: list[bool] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=11)
    runner._last_push_at_pr_number = pr.number

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        retriggers.append(number)
        bypass_flags.append(bypass_same_head_dedup)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == [42]
    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at is not None
    assert any(
        entry["event"]
        == (
            "[WATCH] Stale CHANGES_REQUESTED on PR #42; "
            "re-triggering @codex review (attempt 1/3)."
        )
        for entry in runner.state.history
    )


def test_handle_watch_retries_after_author_dedup_window_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 11 * 60,
    )
    bypass_flags: list[bool] = []

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    runner = h._make_runner(stale_review_threshold_min=1)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=11)
    runner._last_push_at_pr_number = pr.number
    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    requested_at = now - timedelta(minutes=1)

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        bypass_flags.append(bypass_same_head_dedup)
        return True, False, requested_at + timedelta(minutes=5)

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at == now


def test_handle_watch_debounces_failed_stale_review_retrigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 11 * 60,
    )
    bypass_flags: list[bool] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=11)
    runner._last_push_at_pr_number = pr.number

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        bypass_flags.append(bypass_same_head_dedup)
        return False, False, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at == now


def test_handle_watch_does_not_retrigger_recent_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 5 * 60,
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=5)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_does_not_retrigger_within_debounce_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 60 * 60,
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = now - timedelta(minutes=30)
    runner._last_push_at = now - timedelta(hours=1)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at == now - timedelta(minutes=30)


def test_handle_watch_normalizes_naive_stale_retrigger_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 60 * 60,
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = datetime(2026, 4, 21, 11, 30)
    runner._last_push_at = now - timedelta(hours=1)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []


def test_handle_watch_retriggers_stale_eyes_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: EYES persisting past stale_review_threshold_eyes_min retriggers."""
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 6 * 60,
    )

    retriggers: list[int] = []
    bypass_flags: list[bool] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        retriggers.append(number)
        bypass_flags.append(bypass_same_head_dedup)
        return True, True, None

    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=6)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == [42]
    assert bypass_flags == [True]
    assert runner.state.last_stale_retrigger_at == now


def test_handle_watch_does_not_retrigger_eyes_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EYES younger than stale_review_threshold_eyes_min must not retrigger."""
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 2 * 60,
    )

    retriggers: list[int] = []
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=2)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_does_not_retrigger_changes_requested_below_default_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED below the default 10-minute threshold must not retrigger.

    Confirms EYES's shorter 5-minute threshold is not applied to
    CHANGES_REQUESTED — a push 6 minutes old is past the EYES threshold
    but still inside the CHANGES_REQUESTED threshold, so no retrigger.
    """
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 6 * 60,
    )

    retriggers: list[int] = []
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=6)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


@pytest.mark.parametrize(
    "review_status",
    [ReviewStatus.APPROVED, ReviewStatus.PENDING],
)
def test_handle_watch_does_not_retrigger_for_non_changes_requested_status(
    monkeypatch: pytest.MonkeyPatch,
    review_status: ReviewStatus,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=review_status,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []


def test_handle_watch_allows_pending_review_only_when_repo_bypasses_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=42,
        branch="pr-042-test",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="green042",
        diff_scanned_at_sha="green042",
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    merged: list[int] = []

    async def fake_handle_merge() -> None:
        merged.append(42)

    runner = h._make_runner(allow_merge_without_review=True)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_merge = fake_handle_merge  # type: ignore[method-assign]

    asyncio.run(runner.handle_watch())

    assert merged == [42]


def test_handle_watch_does_not_bypass_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=42,
        branch="pr-042-test",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    merged: list[int] = []

    async def fake_handle_merge() -> None:
        merged.append(42)

    runner = h._make_runner(allow_merge_without_review=True)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner._last_push_at_pr_number = pr.number
    runner.handle_merge = fake_handle_merge  # type: ignore[method-assign]

    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH


def test_handle_watch_retries_rehydrate_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If recover_state's rehydrate failed (e.g. transient API hiccup),
    handle_watch must retry so a stuck-None last_push_at doesn't stale-fix
    loop forever."""
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": "2026-04-14T18:00:00Z"},
    )

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    assert runner._last_push_at is None
    asyncio.run(runner.handle_watch())
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T18:00:00+00:00"


def test_handle_watch_falls_through_for_fork_with_ci_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI failure on a fork PR must NOT call handle_fix (which would no-op
    and create a skip loop). It must fall through to the waiting/timeout
    logic so the PR can escalate. PR-316 moved the terminal state from
    IDLE to ERROR so the picker stops re-selecting the task."""
    past = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=88,
        branch="fork:feature",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
        last_activity=past,
        is_cross_repository=True,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner(review_timeout_min=30)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.ERROR


def test_handle_watch_falls_through_for_fork_with_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED on a fork PR must also fall through to timeout
    instead of being routed into handle_fix (even with fresh feedback).
    PR-316: terminal state is ERROR (was IDLE)."""
    past = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=88,
        branch="fork:feature",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=past,
        is_cross_repository=True,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": ""},
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner(review_timeout_min=30)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.ERROR


def test_handle_watch_rehydrates_on_pr_number_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If handle_watch is tracking a different PR than the last-push
    number, rehydrate must fire even when _last_push_at is non-None —
    otherwise a transient rehydrate failure on the prior PR switch
    would keep the stale previous-PR timestamp forever."""
    head_iso = "2026-04-14T18:00:00Z"
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    pr = PRInfo(
        number=55,
        branch="pr-new",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    runner = h._make_runner()
    # Stale last_push_at from a previously-tracked PR (different number).
    runner._last_push_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 999
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.handle_watch())

    assert runner._last_push_at_pr_number == 55
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T18:00:00+00:00"


def test_handle_watch_stays_in_watch_on_unknown_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + UNKNOWN feedback check -> stay in WATCH, no FIX."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        _raise,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any("feedback check failed" in e["event"] for e in runner.state.history)


def test_handle_watch_skips_hung_timeout_on_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + UNKNOWN + elapsed > timeout_min -> stay WATCH, not HUNG.

    When the observation itself is unreliable we cannot trust the elapsed
    time either. The runner must stay in WATCH and retry next cycle.
    """
    last_push = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        _raise,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner(review_timeout_min=30)
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any("feedback check failed" in e["event"] for e in runner.state.history)


def test_handle_watch_retriggers_on_codex_bot_error_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [h._codex_bot_error_comment()],
    )

    posted: list[tuple[int, bool]] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append((number, bypass_same_head_dedup))
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == [(42, True)]
    assert runner.state.last_codex_retrigger_at is not None
    assert any("Codex bot error comment on PR #42" in entry["event"] for entry in runner.state.history)


def test_handle_watch_does_not_retrigger_on_non_matching_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(body="LGTM, all clear from Codex"),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_does_not_retrigger_on_non_bot_author(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(user="some-human"),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_codex_bot_error_cooldown_blocks_rapid_retriggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(
                created_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        ],
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_codex_retrigger_at = now - timedelta(minutes=2)
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at == now - timedelta(minutes=2)


def test_handle_watch_codex_bot_error_skips_already_handled_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An error comment older than ``last_codex_retrigger_at`` was already
    handled in a prior cycle and must not retrigger again."""
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(
                created_at="2026-04-30T11:00:00Z",
            ),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_codex_retrigger_at = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []


def test_handle_watch_codex_bot_error_comment_api_failure_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list[dict[str, Any]]:
        raise RuntimeError("api boom")

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        _raise,
    )

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH

    with caplog.at_level("WARNING", logger=watch_module.logger.name):
        asyncio.run(runner.handle_watch())

    assert any("codex bot error comments for PR #42" in record.message for record in caplog.records)
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_codex_bot_error_skips_unparseable_created_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.gh_runner._parse_iso",
        lambda value: None,
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(),
            h._codex_bot_error_comment(),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_codex_bot_error_skips_marker_on_post_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed @codex review post must not update
    ``last_codex_retrigger_at``, so a later cycle can retry the same
    error comment instead of treating it as already handled.
    """
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [h._codex_bot_error_comment()],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return False, False, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == [42]
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_codex_bot_error_normalizes_naive_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Naive timestamps from GitHub and naive ``last_codex_retrigger_at``
    are both treated as UTC, so the cooldown comparison still works."""
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    naive_created = datetime(2026, 4, 30, 13, 0)
    frozen_now = datetime(2026, 4, 30, 13, 30, tzinfo=timezone.utc)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return frozen_now if tz is None else frozen_now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.gh_runner._parse_iso",
        lambda value: naive_created if value else None,
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [h._codex_bot_error_comment()],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_codex_retrigger_at = datetime(2026, 4, 30, 12, 0)
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == [42]
    assert runner.state.last_codex_retrigger_at == frozen_now


def test_handle_watch_eyes_skips_stale_review_after_bot_error_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When a Codex bot-error retrigger fires in the EYES branch, the stale-
    review retrigger must not also fire in the same cycle. Both paths post
    ``@codex review`` with ``bypass_same_head_dedup=True``; without
    mutual exclusion the daemon would emit two trigger comments back-to-
    back when both conditions are simultaneously true (e.g. an error
    comment plus a push old enough to cross the EYES threshold).
    """
    now = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    pr = h._codex_bot_pr()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [h._codex_bot_error_comment()],
    )
    # Push age well past the EYES stale threshold so the stale path WOULD
    # otherwise fire if it were called.
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 10 * 60,
    )

    posts: list[tuple[int, bool]] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posts.append((number, bypass_same_head_dedup))
        return True, True, None

    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=10)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posts == [(42, True)]
    assert runner.state.last_codex_retrigger_at == now
    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_eyes_runs_stale_review_when_bot_error_does_not_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the Codex bot-error retrigger does not post (e.g. cooldown blocked
    after a prior retrigger), the stale-review retrigger must still run as
    a fallback so a stuck EYES review eventually recovers."""
    now = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    pr = h._codex_bot_pr()

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    # An error comment older than ``last_codex_retrigger_at`` is treated
    # as already handled, so bot-error returns without posting.
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(created_at="2026-04-30T11:00:00Z"),
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 6 * 60,
    )

    posts: list[tuple[int, bool]] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posts.append((number, bypass_same_head_dedup))
        return True, True, None

    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_codex_retrigger_at = now - timedelta(minutes=30)
    runner._last_push_at = now - timedelta(minutes=6)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posts == [(42, True)]
    assert runner.state.last_stale_retrigger_at == now


@pytest.mark.parametrize(
    "review",
    [ReviewStatus.PENDING, ReviewStatus.APPROVED, ReviewStatus.CHANGES_REQUESTED],
)
def test_handle_watch_skips_codex_bot_error_check_outside_eyes(
    monkeypatch: pytest.MonkeyPatch,
    review: ReviewStatus,
) -> None:
    """The codex-bot-error fetch must be gated to ``review == EYES`` so
    the daemon does not paginate ``issues/{pr}/comments`` on every WATCH
    poll during normal CI=PENDING/PR-waiting states."""
    pr = h._codex_bot_pr(review=review)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    api_calls: list[str] = []

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        api_calls.append(path)
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: None,
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert all("issues/42/comments" not in path for path in api_calls)
    assert runner.state.last_codex_retrigger_at is None


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — handle_watch group
# ---------------------------------------------------------------------------


def test_run_cycle_dispatches_watch_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    publishes: list[str] = []
    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.WATCH
    handler_name = "handle_watch"

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handler() -> None:
        calls.append(handler_name)

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)
    monkeypatch.setattr(runner, handler_name, fake_handler)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == [handler_name]
    assert publishes == ["published"]


def test_effective_watch_poll_interval_is_slow_immediately_after_entry() -> None:
    """First WATCH cycle returns the slow interval (default 300s)."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc)

    assert runner.effective_watch_poll_interval == 300


def test_effective_watch_poll_interval_still_slow_inside_window() -> None:
    """Four minutes after WATCH entry still uses the slow interval."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=4)

    assert runner.effective_watch_poll_interval == 300


def test_effective_watch_poll_interval_becomes_fast_past_window() -> None:
    """Past 5 min the slow window closes and polling drops to fast (45s)."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=5, seconds=1)

    assert runner.effective_watch_poll_interval == 45


def test_effective_watch_poll_interval_event_resets_slow_window() -> None:
    """A detected GitHub event re-anchors the slow window from event time."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    # WATCH entered 6 minutes ago — past the slow window.
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=6)
    assert runner.effective_watch_poll_interval == 45

    # Prime a baseline signature, then observe a different signature.
    baseline = PRInfo(number=42, branch="pr-042", ci_status=CIStatus.PENDING)
    runner._observe_watch_event_signature(baseline)
    changed = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
    )
    runner._observe_watch_event_signature(changed)

    assert runner._watch_last_event_at is not None
    # Event reset → new anchor is recent → back inside slow window.
    assert runner.effective_watch_poll_interval == 300


def test_watch_polling_anchors_cleared_on_transition_out_of_watch() -> None:
    """Leaving WATCH wipes the adaptive polling anchors for the next session."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc)
    runner._watch_last_event_at = datetime.now(timezone.utc)
    runner._watch_last_event_signature = (42, CIStatus.PENDING, ReviewStatus.PENDING, None)

    runner._reset_watch_polling()

    assert runner._watch_entered_at is None
    assert runner._watch_last_event_at is None
    assert runner._watch_last_event_signature is None
    # No anchor → falls back to the static base interval.
    assert runner.effective_watch_poll_interval == 60


def test_run_cycle_resets_watch_anchors_when_state_leaves_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_run_cycle_body`` clears watch anchors when the cycle exits WATCH."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)
    runner._recovered = True
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    runner._watch_last_event_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner._watch_last_event_signature = (42, CIStatus.PENDING, ReviewStatus.PENDING, None)
    runner.state.state = PipelineState.WATCH

    async def _noop_cloned(self: PipelineRunner) -> None:
        return None

    async def _budget_ok(self: PipelineRunner) -> bool:
        return True

    async def _no_user_paused(self: PipelineRunner) -> None:
        self.state.user_paused = False

    async def _preflight_ok(self: PipelineRunner) -> bool:
        return True

    async def _watch_to_idle(self: PipelineRunner) -> None:
        self.state.state = PipelineState.IDLE

    async def _publish_state(self: PipelineRunner) -> None:
        return None

    monkeypatch.setattr(PipelineRunner, "ensure_repo_cloned", _noop_cloned)
    monkeypatch.setattr(PipelineRunner, "_check_github_api_budget", _budget_ok)
    monkeypatch.setattr(PipelineRunner, "_refresh_user_paused_from_redis", _no_user_paused)
    monkeypatch.setattr(PipelineRunner, "preflight", _preflight_ok)
    monkeypatch.setattr(PipelineRunner, "handle_watch", _watch_to_idle)
    monkeypatch.setattr(PipelineRunner, "publish_state", _publish_state)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner._watch_entered_at is None
    assert runner._watch_last_event_at is None
    assert runner._watch_last_event_signature is None


def test_run_cycle_anchors_watch_entered_at_on_transition_into_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_run_cycle_body`` anchors ``_watch_entered_at`` at the transition.

    A handler that pushes the runner from a non-WATCH state into WATCH
    must leave the anchor set before ``_run_cycle_body`` returns so the
    daemon's *next* call to ``_runner_poll_interval`` already sees the
    slow cadence — otherwise the first interval after entry uses the
    fast base poll and the slow-start window is wasted.
    """
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)
    runner._recovered = True
    runner.state.state = PipelineState.IDLE
    assert runner._watch_entered_at is None

    async def _noop_cloned(self: PipelineRunner) -> None:
        return None

    async def _budget_ok(self: PipelineRunner) -> bool:
        return True

    async def _no_user_paused(self: PipelineRunner) -> None:
        self.state.user_paused = False

    async def _preflight_ok(self: PipelineRunner) -> bool:
        return True

    async def _idle_to_watch(self: PipelineRunner) -> None:
        self.state.state = PipelineState.WATCH

    async def _publish_state(self: PipelineRunner) -> None:
        return None

    monkeypatch.setattr(PipelineRunner, "ensure_repo_cloned", _noop_cloned)
    monkeypatch.setattr(PipelineRunner, "_check_github_api_budget", _budget_ok)
    monkeypatch.setattr(PipelineRunner, "_refresh_user_paused_from_redis", _no_user_paused)
    monkeypatch.setattr(PipelineRunner, "preflight", _preflight_ok)
    monkeypatch.setattr(PipelineRunner, "handle_idle", _idle_to_watch)
    monkeypatch.setattr(PipelineRunner, "publish_state", _publish_state)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.WATCH
    assert runner._watch_entered_at is not None
    # The next poll interval — computed by the daemon main loop *after*
    # this cycle — already sees the slow cadence, not the base fallback.
    assert runner.effective_watch_poll_interval == 300


def test_check_budget_no_skip_when_state_is_watch() -> None:
    """WATCH cadence already absorbs the slowdown; do not skip cycles.

    ``effective_watch_poll_interval`` already takes
    ``max(target, base * multiplier)`` when the rate-limit slowdown is
    active. If ``_check_github_api_budget`` also skipped (multiplier-1)
    out of every multiplier cycles, the effective poll spacing would
    become ``effective_watch_poll_interval * multiplier`` and could
    delay merge/fix transitions by an hour or more when quota is low.
    """
    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 10
    h._set_budget(runner, h._budget(remaining=500, limit=5000))  # 10%

    runner.state.state = PipelineState.WATCH

    decisions = [asyncio.run(runner._check_github_api_budget()) for _ in range(5)]

    assert decisions == [True] * 5
    assert runner._github_api_slowdown_cycle == 0
    assert runner._github_api_slowdown_attempts == 5


def _patch_infra_failure_pr(
    monkeypatch: pytest.MonkeyPatch,
    pr_number: int = 77,
    head_sha: str = "feedface",
    *,
    check_runs: list[dict] | None = None,
) -> PRInfo:
    """Wire ``get_open_prs`` and ``_fetch_ci_status_rest`` so WATCH sees
    a single infra-class failing check-run. Returns the polled PR so the
    caller can also seed ``runner.state.current_pr`` with the same PR
    number/branch.

    ``check_runs`` defaults to a single ``cancelled`` Actions check-run
    whose ``details_url`` carries workflow run ID ``9876``; this matches
    the run ID asserted by the WATCH integration test below. Pass
    ``check_runs=[]`` to drive the "no failing check-run" branch.
    """
    pr = PRInfo(
        number=pr_number,
        branch=f"pr-{pr_number:03d}",
        ci_status=CIStatus.INFRA_FAILURE,
        review_status=ReviewStatus.PENDING,
        head_sha=head_sha,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    if check_runs is None:
        check_runs = [
            {
                "conclusion": "cancelled",
                "details_url": (
                    "https://github.com/example/repo/actions/runs/9876/"
                    "job/12345"
                ),
            }
        ]
    # ``_maybe_reclassify_stuck_pending`` and ``_retry_failed_workflow``
    # both call this; the cached payload feeds the retry helper too.
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: (list(check_runs), {}, True),
    )
    return pr


def test_first_infra_failure_retries_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: a fresh INFRA_FAILURE on a head_sha triggers ``gh run rerun
    --failed`` once; ``handle_fix`` is NOT invoked, the infra-retry
    marker is written to Redis, and a log line records the retry."""
    pr = _patch_infra_failure_pr(monkeypatch)

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    assert fix_calls == []
    # PR-251 follow-up: workflow run ID is parsed from the failing
    # check-run's ``details_url`` (Actions job URL). No ``gh run list``
    # call is issued — that endpoint mis-keys ``pull_request`` runs to
    # the merge commit and would silently miss the failing PR run.
    assert all(args[:2] != ["run", "list"] for args in gh_calls)
    assert ["run", "rerun", "--failed", "9876"] in gh_calls
    # Retry marker is in Redis. PR-251 follow-up: the marker now stores
    # the wall-clock rerun timestamp (float seconds) so a grace window
    # can be applied before the next cycle treats INFRA_FAILURE as
    # "persistent". Older "1" markers are treated as
    # ``(exists, elapsed)`` for backwards compatibility, but new
    # writes always carry a numeric value.
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    raw = asyncio.run(runner.redis.get(key))
    assert raw is not None
    marker_ts = float(raw)
    assert marker_ts > 0
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "INFRA_FAILURE detected" in history
    assert "infra retry: re-ran failed jobs" in history


def test_second_infra_failure_routes_to_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: a second INFRA_FAILURE classification on the same head_sha
    (the prior retry didn't help) routes WATCH straight through
    ``handle_fix`` without issuing another ``gh run rerun``."""
    pr = _patch_infra_failure_pr(monkeypatch, pr_number=78, head_sha="d00df00d")

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    # Pre-seed the marker with a wall-clock timestamp older than the
    # grace window so the next cycle treats the retry as "elapsed
    # enough to escalate." A legacy "1" marker would also work (it's
    # treated as elapsed for backwards compatibility) but the realistic
    # daemon write-path stores a timestamp.
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    stale_marker_ts = (
        time.time() - watch_module._INFRA_RETRY_GRACE_SECONDS - 1.0
    )
    asyncio.run(runner.redis.set(key, str(stale_marker_ts)))

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    assert fix_calls == [None]
    # No workflow rerun calls were made — straight to FIX.
    assert all(args[:2] != ["run", "rerun"] for args in gh_calls)
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "INFRA_FAILURE persisted after retry" in history
    # PR-251 follow-up: ``handle_fix`` reads ``ci_status`` to decide
    # whether to inject CI logs into the FIX prompt; the persisted-INFRA
    # path must downgrade to FAILURE before invoking it so the coder
    # actually receives the failure logs the daemon promised in the
    # "effective FAILURE" log line.
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.FAILURE


def test_infra_failure_on_fork_pr_logs_no_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: fork PRs cannot be auto-fixed; INFRA_FAILURE on a fork
    falls through to the manual-wait log without issuing a rerun."""
    pr = PRInfo(
        number=79,
        branch="pr-079",
        ci_status=CIStatus.INFRA_FAILURE,
        review_status=ReviewStatus.PENDING,
        head_sha="cafed00d",
        last_activity=datetime.now(timezone.utc),
        is_cross_repository=True,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: ([{"conclusion": "cancelled"}], {}, True),
    )

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    assert fix_calls == []
    assert all(args[:2] != ["run", "rerun"] for args in gh_calls)
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "fork PR cannot be auto-fixed" in history
    assert "ci=INFRA_FAILURE" in history


def test_infra_retry_attempted_returns_true_when_redis_or_sha_missing() -> None:
    """PR-251: helpers conservatively report ``(True, True)`` when state
    cannot be persisted, so the caller routes straight to FIX rather
    than re-running the workflow forever against a missing tracker."""
    runner = h._make_runner()
    assert asyncio.run(runner._infra_retry_attempted(11, "")) == (True, True)
    runner.redis = None  # type: ignore[assignment]
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (True, True)


def test_infra_retry_attempted_returns_false_when_marker_absent() -> None:
    """PR-251 follow-up: with redis available but no marker, both flags are
    False so the caller proceeds to issue the rerun + write the marker."""
    runner = h._make_runner()
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (False, False)


def test_infra_retry_attempted_within_grace_window_returns_marker_only() -> None:
    """PR-251 follow-up: a marker written less than
    ``_INFRA_RETRY_GRACE_SECONDS`` ago reports ``(True, False)`` so the
    WATCH cycle stays put while the cached pre-rerun CI payload is
    still in play (CI cache TTL is 15s; e2e config polls every 2s)."""
    runner = h._make_runner()
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, 11, "abc")
    asyncio.run(runner.redis.set(key, str(time.time())))
    marker_exists, grace_elapsed = asyncio.run(
        runner._infra_retry_attempted(11, "abc")
    )
    assert marker_exists is True
    assert grace_elapsed is False


def test_infra_retry_attempted_handles_legacy_marker_value() -> None:
    """PR-251 follow-up: legacy markers written by an older daemon
    (value ``"1"``) parse as float ``1.0`` — a timestamp at the unix
    epoch — so the elapsed comparison naturally returns
    ``(True, True)``. This preserves the prior single-bit semantics
    so an in-flight upgrade can't get stuck in WATCH on a sha that
    already exhausted its retry budget."""
    runner = h._make_runner()
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, 11, "abc")
    asyncio.run(runner.redis.set(key, "1"))
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (True, True)


def test_infra_retry_attempted_handles_corrupted_marker_value() -> None:
    """PR-251 follow-up: a non-numeric marker (Redis corruption or
    operator override) cannot be parsed as a timestamp; the helper
    treats it as ``(True, True)`` so the caller routes to FIX rather
    than crashing the WATCH cycle on the bad value."""
    runner = h._make_runner()
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, 11, "abc")
    asyncio.run(runner.redis.set(key, "not-a-number"))
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (True, True)


def test_infra_failure_within_grace_window_stays_in_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up (review fix): when the marker exists but the
    grace window has not elapsed, WATCH must NOT escalate to FIX and
    must NOT issue another rerun. This prevents fast-cadence pollers
    (e2e config polls every 2s, well below the 15s CI cache TTL) from
    routing to ``handle_fix`` based on the pre-rerun cached payload."""
    pr = _patch_infra_failure_pr(monkeypatch, pr_number=81, head_sha="beadface")

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    # Marker was written "just now" (well within the grace window).
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    asyncio.run(runner.redis.set(key, str(time.time())))

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    # Did NOT escalate to FIX, did NOT issue another rerun.
    assert fix_calls == []
    assert all(args[:2] != ["run", "rerun"] for args in gh_calls)
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "within retry grace window" in history
    # ci_status must remain INFRA_FAILURE — the downgrade to FAILURE
    # only happens on the persisted-after-retry path.
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.INFRA_FAILURE


def test_mark_infra_retry_attempted_noops_when_redis_or_sha_missing() -> None:
    """PR-251: marking is a no-op without ``redis`` or ``head_sha``; the
    Redis double records no writes."""
    runner = h._make_runner()
    asyncio.run(runner._mark_infra_retry_attempted(11, ""))
    assert runner.redis.writes == []
    runner.redis = None  # type: ignore[assignment]
    asyncio.run(runner._mark_infra_retry_attempted(11, "abc"))


def _patch_check_runs(
    monkeypatch: pytest.MonkeyPatch,
    check_runs: list[dict],
) -> None:
    """Replace ``_fetch_ci_status_rest`` with a stub returning ``check_runs``.

    PR-251 follow-up: ``_retry_failed_workflow`` reads check-runs (not
    ``gh run list``) to find Actions workflow run IDs. Tests that
    exercise the helper directly install this stub so the helper sees
    the desired failing-check-run shape.
    """
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: (list(check_runs), {}, True),
    )


def _actions_url(run_id: int, job_id: int = 1) -> str:
    """Return a canonical GitHub Actions check-run ``details_url``."""
    return (
        f"https://github.com/example/repo/actions/runs/{run_id}/job/{job_id}"
    )


def test_retry_failed_workflow_short_circuits_without_head_sha() -> None:
    """PR-251: an empty ``head_sha`` returns ``False`` with no fetch."""
    runner = h._make_runner()
    assert runner._retry_failed_workflow(11, "") is False


def test_retry_failed_workflow_handles_no_failing_check_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: when the cached check-runs payload has no
    failing entries (transient race against the classifier), the helper
    logs and returns ``False`` without issuing ``gh run rerun``."""
    runner = h._make_runner()
    _patch_check_runs(monkeypatch, [])
    rerun_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        rerun_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert rerun_calls == []
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no failing check-run found" in history


def test_retry_failed_workflow_skips_check_runs_without_actions_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a failing check-run from a non-Actions GitHub
    App carries a ``details_url`` outside the ``/actions/runs/<id>``
    shape; the helper cannot rerun it via ``gh run rerun --failed`` and
    must skip rather than crash."""
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {
                "conclusion": "cancelled",
                "details_url": "https://example.com/custom-app/check/42",
            }
        ],
    )
    rerun_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        rerun_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert rerun_calls == []
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no Actions workflow run ID" in history


def test_retry_failed_workflow_handles_rerun_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: when every ``gh run rerun --failed`` call raises, the
    helper logs the failure and returns ``False`` without crashing."""
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(555)},
        ],
    )

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        raise RuntimeError("permission denied")

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is False
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "infra retry rerun failed" in history


def test_retry_failed_workflow_extracts_run_id_from_details_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: the workflow run ID is parsed from the failing
    check-run's ``details_url``. This is the only reliable selection
    path for ``pull_request`` workflows — ``gh run list --commit``
    keys on ``workflow_run.head_sha`` which for PR events points at
    the synthetic merge commit, not at ``pull_request.head.sha`` the
    daemon tracks.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(9876, 12345)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    assert calls == [["run", "rerun", "--failed", "9876"]]


def test_retry_failed_workflow_dedupes_jobs_in_same_workflow_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a workflow with several failing matrix jobs
    posts one check-run per job, all sharing the same ``run_id`` in
    ``details_url``. ``gh run rerun --failed <run_id>`` operates on the
    whole run, so the helper must dedupe the run IDs before calling.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(700, 1)},
            {"conclusion": "failure", "details_url": _actions_url(700, 2)},
            {"conclusion": "stale", "details_url": _actions_url(800, 3)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    assert calls == [
        ["run", "rerun", "--failed", "700"],
        ["run", "rerun", "--failed", "800"],
    ]


def test_retry_failed_workflow_skips_succeeding_check_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: only failing check-runs feed the rerun set;
    the helper must not invoke ``gh run rerun`` against runs whose
    conclusion is ``success`` or ``neutral`` (e.g. a re-dispatch that
    superseded the failure that drove the INFRA_FAILURE classification).
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "success", "details_url": _actions_url(1)},
            {"conclusion": "neutral", "details_url": _actions_url(2)},
            {"conclusion": "skipped", "details_url": _actions_url(3)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert calls == []
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no failing check-run found" in history


def test_retry_failed_workflow_matches_failing_status_with_empty_conclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: when a check-run carries an empty
    ``conclusion`` (e.g. an in-flight job that GitHub reports with only
    ``status``), a failure-class ``status`` value still selects the run
    for rerun. Without this branch the WATCH cycle would consume its
    one-shot retry marker without ever calling ``gh run rerun``.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {
                "conclusion": None,
                "status": "failed",
                "details_url": _actions_url(7),
            },
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    assert calls == [["run", "rerun", "--failed", "7"]]


def test_retry_failed_workflow_partial_rerun_failure_returns_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: when several workflow runs need rerunning and
    one of the calls fails, the helper logs that failure but reports
    success as long as at least one rerun went through — partial
    progress is still progress against the one-shot retry budget.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(900)},
            {"conclusion": "stale", "details_url": _actions_url(901)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        if args == ["run", "rerun", "--failed", "900"]:
            raise RuntimeError("transient")
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "infra retry rerun failed" in history
    assert "infra retry: re-ran failed jobs" in history


def test_retry_failed_workflow_handles_no_run_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: when the failing check-runs carry no Actions workflow
    run IDs (non-Actions GitHub Apps only), the retry helper logs and
    returns ``False``; the marker is still written by the caller so the
    next cycle routes to FIX."""
    pr = _patch_infra_failure_pr(
        monkeypatch,
        pr_number=80,
        head_sha="11223344",
        check_runs=[
            {
                "conclusion": "cancelled",
                "details_url": "https://example.com/custom-app/check/1",
            }
        ],
    )

    monkeypatch.setattr(
        watch_module.gh_runner,
        "run_gh",
        lambda args, repo=None, **kw: "",
    )

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    # First cycle: no rerun was issued (no run found), but the marker
    # is written (with a timestamp) so a later cycle — once the grace
    # window has elapsed — goes straight to FIX.
    assert fix_calls == []
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    raw = asyncio.run(runner.redis.get(key))
    assert raw is not None
    assert float(raw) > 0
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no Actions workflow run ID" in history


# ---------------------------------------------------------------------------
# PR-290a: _scan_pr_diff_once (diff content scan infrastructure)
# ---------------------------------------------------------------------------


def test_scan_pr_diff_once_skips_when_no_current_pr() -> None:
    """The dispatcher cannot scan a diff for a PR that does not exist;
    in that case the call returns ``False`` and changes no state."""
    runner = h._make_runner()
    runner.state.current_pr = None

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False


def test_scan_pr_diff_once_skips_when_cache_matches_head_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SHA-keyed cache hit: when ``diff_scanned_at_sha`` equals the
    current ``head_sha`` the scan must short-circuit (no gh CLI call)
    so each WATCH cycle costs nothing on a HEAD it already cleared."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"x": _re.compile(r"x")},
    )

    called: list[int] = []

    def fail_fetch(repo: str, number: int) -> str:
        called.append(number)
        raise AssertionError("get_pr_diff must not be called on cache hit")

    monkeypatch.setattr("src.github.prs.get_pr_diff", fail_fetch)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=7,
        branch="pr-007",
        head_sha="cafe1234",
        diff_scanned_at_sha="cafe1234",
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False
    assert called == []


def test_scan_pr_diff_once_runs_when_cache_mismatches_head_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh HEAD SHA (new coder push) re-arms the scan. The dispatcher
    fetches the diff, runs the catalogue (empty in this assertion, so
    no violation), and updates ``diff_scanned_at_sha`` to the new SHA
    on success."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"never_match": _re.compile(r"NEVER_MATCH_TOKEN")},
    )
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_RULES",
        {"never_match": "test rule"},
    )

    fetched: list[tuple[str, int]] = []

    def fake_fetch(repo: str, number: int) -> str:
        fetched.append((repo, number))
        return "diff --git a/x b/x\n+innocuous\n"

    monkeypatch.setattr("src.github.prs.get_pr_diff", fake_fetch)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=8,
        branch="pr-008",
        head_sha="deadbeef",
        diff_scanned_at_sha="oldcafe1",
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is True
    assert fetched == [(runner.owner_repo, 8)]
    assert runner.state.current_pr.diff_scanned_at_sha == "deadbeef"
    assert runner.state.state != PipelineState.ERROR


def test_scan_pr_diff_once_leaves_cache_unchanged_on_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``gh pr diff`` failure must NOT mark the HEAD as
    scanned; otherwise the next WATCH cycle would skip the SHA and a
    prohibited diff could slip past the catalogue. Fetch failures are
    retried on subsequent cycles."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"x": _re.compile(r"x")},
    )

    def boom(repo: str, number: int) -> str:
        raise RuntimeError("gh CLI timed out")

    monkeypatch.setattr("src.github.prs.get_pr_diff", boom)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=9,
        branch="pr-009",
        head_sha="freshSHA",
        diff_scanned_at_sha="prior",
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False
    assert runner.state.current_pr.diff_scanned_at_sha == "prior"


def test_scan_pr_diff_once_empty_catalogue_does_not_mark_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-290a skeleton: with an empty pattern catalogue, the dispatcher
    must NOT record ``diff_scanned_at_sha``. Catalogue contents change at
    deploy time; marking a HEAD as scanned while patterns are empty would
    let the cache-hit gate short-circuit the same HEAD forever after the
    PR-290b/c rollout populated the rules, bypassing the new catalogue
    until a fresh push moved HEAD."""
    monkeypatch.setattr(watch_module.guardrails, "_DIFF_PATTERNS", {})
    monkeypatch.setattr(watch_module.guardrails, "_DIFF_RULES", {})

    def fail_fetch(repo: str, number: int) -> str:
        raise AssertionError("get_pr_diff must not be called on empty catalogue")

    monkeypatch.setattr("src.github.prs.get_pr_diff", fail_fetch)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=10,
        branch="pr-010",
        head_sha="bee1bee1",
        diff_scanned_at_sha=None,
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False
    assert runner.state.current_pr.diff_scanned_at_sha is None


def test_scan_pr_diff_once_violation_transitions_to_error_with_guardrail_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the catalogue is populated and the diff matches, the
    dispatcher must transition through ``_transition_to_error`` with a
    structured ``CancellationCause`` whose ``payload.subsource`` is
    ``"guardrail"`` (PR-315/PR-320 single-ERROR-category model)."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {
            "workflow_permissions_write_all": _re.compile(
                r"permissions:\s*write-all"
            )
        },
    )
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_RULES",
        {"workflow_permissions_write_all": "Workflow permissions write-all"},
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_diff",
        lambda repo, number: (
            "diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml\n"
            "+permissions: write-all\n"
        ),
    )

    captured: list[dict[str, Any]] = []

    async def fake_transition(
        self: Any,
        message: str,
        **kwargs: Any,
    ) -> None:
        captured.append({"message": message, **kwargs})
        self.state.state = PipelineState.ERROR

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_transition_to_error",
        fake_transition,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=11,
        branch="pr-011",
        head_sha="newsha01",
        diff_scanned_at_sha=None,
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is True
    assert len(captured) == 1
    call = captured[0]
    assert "[GUARDRAIL]" in call["message"]
    assert "workflow_permissions_write_all" in call["message"]
    cause = call["cancellation_cause"]
    assert cause.category == "ERROR"
    assert cause.payload["subsource"] == "guardrail"
    assert cause.payload["tier"] == 1
    assert cause.payload["category"] == "workflow_permissions_write_all"
    assert cause.payload["excerpt"] == "permissions: write-all"
    # Cache must update on a successful scan, even when a violation fires,
    # so the same HEAD does not re-run the scan after an operator Retry.
    assert runner.state.current_pr.diff_scanned_at_sha == "newsha01"


def test_handle_watch_invokes_diff_scan_once_per_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The WATCH cycle must call ``_scan_pr_diff_once`` exactly once per
    poll. With an empty catalogue this is a no-op that still updates
    the SHA cache so future cycles short-circuit cheaply."""
    pr = PRInfo(
        number=12,
        branch="pr-012",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="poll0001",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    invocations: list[int] = []

    async def fake_scan(self: Any) -> bool:
        invocations.append(1)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=12, branch="pr-012")
    asyncio.run(runner.handle_watch())

    assert invocations == [1]


def test_handle_watch_returns_when_diff_scan_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the diff scan finds a violation and transitions to ERROR, the
    WATCH cycle must short-circuit; otherwise the trailing merge / fix
    / timeout dispatch could re-enter handling on a now-ERROR state."""
    pr = PRInfo(
        number=13,
        branch="pr-013",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        last_activity=datetime.now(timezone.utc),
        head_sha="violate1",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    async def fake_scan(self: Any) -> bool:
        self.state.state = PipelineState.ERROR
        return True

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number: merged.append((repo, number)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=13, branch="pr-013")
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.ERROR


def test_handle_watch_preserves_diff_scanned_at_sha_when_head_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_open_prs`` returns a fresh ``PRInfo`` whose
    ``diff_scanned_at_sha`` defaults to ``None``. When the polled HEAD
    matches the in-memory HEAD, ``handle_watch`` must carry the cached
    SHA forward through ``model_copy`` so the ``_scan_pr_diff_once``
    cache gate keeps holding and ``gh pr diff`` is not re-run on every
    poll for an unchanged HEAD."""
    pr = PRInfo(
        number=21,
        branch="pr-021",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="stable01",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    observed: list[str | None] = []

    async def fake_scan(self: Any) -> bool:
        observed.append(self.state.current_pr.diff_scanned_at_sha)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=21,
        branch="pr-021",
        head_sha="stable01",
        diff_scanned_at_sha="stable01",
    )
    asyncio.run(runner.handle_watch())

    assert observed == ["stable01"]
    assert runner.state.current_pr.diff_scanned_at_sha == "stable01"


def test_handle_watch_drops_diff_scanned_at_sha_when_head_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh coder push (new HEAD SHA) must re-arm the diff scan:
    the cached ``diff_scanned_at_sha`` from the prior HEAD must NOT be
    carried forward, otherwise a prohibited diff introduced by the
    new push could slip past the catalogue."""
    pr = PRInfo(
        number=22,
        branch="pr-022",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="newhead2",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    observed: list[str | None] = []

    async def fake_scan(self: Any) -> bool:
        observed.append(self.state.current_pr.diff_scanned_at_sha)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=22,
        branch="pr-022",
        head_sha="oldhead1",
        diff_scanned_at_sha="oldhead1",
    )
    asyncio.run(runner.handle_watch())

    assert observed == [None]
    assert runner.state.current_pr.diff_scanned_at_sha is None


def test_handle_watch_preserves_diff_scanned_at_sha_when_head_sha_empty_on_both_sides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``PRInfo`` models a transiently empty ``head_sha`` for ``gh``
    payloads that omit the SHA. When both the in-memory and polled HEAD
    SHA are empty strings the HEAD has not changed, so the cache-carry
    logic must preserve ``diff_scanned_at_sha`` (equality alone, no
    truthy guard) — otherwise ``_scan_pr_diff_once`` would re-run
    ``gh pr diff`` on every WATCH cycle for the same PR state and, with
    populated diff guardrails, repeatedly hit fetch failures and hold
    merges noisily."""
    pr = PRInfo(
        number=23,
        branch="pr-023",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    observed: list[str | None] = []

    async def fake_scan(self: Any) -> bool:
        observed.append(self.state.current_pr.diff_scanned_at_sha)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=23,
        branch="pr-023",
        head_sha="",
        diff_scanned_at_sha="cachedsha",
    )
    asyncio.run(runner.handle_watch())

    assert observed == ["cachedsha"]
    assert runner.state.current_pr.diff_scanned_at_sha == "cachedsha"


def test_handle_watch_holds_merge_when_diff_fetch_fails_with_populated_patterns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``gh pr diff`` failure on a merge-eligible cycle must
    NOT fall through to the green-path merge logic when ``_DIFF_PATTERNS``
    is populated. The diff scan is the only enforcement point for the
    diff catalogue; treating a fetch failure as equivalent to a clean
    scan would let a coder slip prohibited content past the catalogue by
    timing a ``gh`` outage with an otherwise-green PR.
    """
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"workflow_permissions_write_all": _re.compile(r"permissions:\s*write-all")},
    )
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_RULES",
        {"workflow_permissions_write_all": "Workflow permissions write-all"},
    )

    def boom(repo: str, number: int) -> str:
        raise RuntimeError("gh CLI timed out")

    monkeypatch.setattr("src.github.prs.get_pr_diff", boom)

    pr = PRInfo(
        number=33,
        branch="pr-033",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        last_activity=datetime.now(timezone.utc),
        head_sha="freshhd1",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number: merged.append((repo, number)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=33,
        branch="pr-033",
        head_sha="freshhd1",
        diff_scanned_at_sha=None,
    )
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.diff_scanned_at_sha != "freshhd1"
    assert any(
        "merge held: diff scan did not complete" in entry.get("event", "")
        for entry in runner.state.history
    )


def test_pending_review_triggers_stale_retrigger_with_bypass_author_dedup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PENDING review past the stale threshold re-triggers @codex review with
    both bypass flags set, restoring the HUNG-fallback behavior lost in
    Sprint 15b Phase 2."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="pendinghd",
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    bypass_flags: list[dict[str, bool]] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        bypass_flags.append(
            {
                "bypass_same_head_dedup": bypass_same_head_dedup,
                "bypass_author_dedup": bypass_author_dedup,
            }
        )
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is True
    assert bypass_flags == [
        {"bypass_same_head_dedup": True, "bypass_author_dedup": True}
    ]
    assert runner.state.last_stale_retrigger_at is not None
    assert any(
        entry["event"]
        == (
            "[WATCH] Stale PENDING on PR #42; "
            "re-triggering @codex review (attempt 1/3)."
        )
        for entry in runner.state.history
    )


def test_pending_review_retrigger_respects_hung_fallback_codex_review_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``hung_fallback_codex_review`` is False the PENDING-stale path
    must not retrigger, preserving the post-Phase-2 opt-out for operators
    who explicitly disabled HUNG retries."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="pendinghd",
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = False
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    calls: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        calls.append(number)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert calls == []
    assert not any(
        "Stale PENDING" in entry.get("event", "")
        for entry in runner.state.history
    )


def test_pending_review_retrigger_respects_stale_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PENDING review whose last push is younger than the stale threshold
    must not retrigger; the recency gate is the same one the
    CHANGES_REQUESTED path uses."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="pendinghd",
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 300,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    calls: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        calls.append(number)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert calls == []


def test_handle_watch_pending_review_calls_stale_retrigger(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The WATCH dispatch must invoke ``_maybe_retrigger_stale_review`` when
    review_status is PENDING and CI is green-or-pending; otherwise the
    PENDING branch would silently fall through and the retrigger would
    never fire."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )

    retrigger_calls: list[int] = []

    async def fake_stale(pr_number: int) -> bool:
        retrigger_calls.append(pr_number)
        return True

    runner = h._make_runner(review_timeout_min=120)
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=12)
    runner._last_push_at_pr_number = pr.number
    runner._maybe_retrigger_stale_review = fake_stale  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retrigger_calls == [42]


def test_changes_requested_retrigger_still_passes_bypass_author_dedup_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: the CHANGES_REQUESTED stale-retrigger path now also passes
    ``bypass_author_dedup=True``, consistent with the EYES path. This
    matches the rationale of PR-422 — when retrigger fires past the stale
    threshold the dedup window (5 min) has long expired, so author-dedup
    must not block the deliberate retry."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
        head_sha="changeshd",
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    runner = h._make_runner()
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    bypass_flags: list[dict[str, bool]] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        bypass_flags.append(
            {
                "bypass_same_head_dedup": bypass_same_head_dedup,
                "bypass_author_dedup": bypass_author_dedup,
            }
        )
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert bypass_flags[-1] == {
        "bypass_same_head_dedup": True,
        "bypass_author_dedup": True,
    }


def _freeze_watch_datetime(
    monkeypatch: pytest.MonkeyPatch, now: datetime
) -> None:
    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)


def test_pending_retrigger_increments_watch_retrigger_count_on_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful @codex review retrigger bumps the per-PR counter so the
    cap can eventually fire on the third stale cycle."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="pendinghd",
        watch_retrigger_count=0,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is True
    assert runner.state.current_pr.watch_retrigger_count == 1
    assert any(
        entry["event"]
        == (
            "[WATCH] Stale PENDING on PR #42; "
            "re-triggering @codex review (attempt 1/3)."
        )
        for entry in runner.state.history
    )


def test_pending_retrigger_does_not_increment_on_dedup_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``_post_codex_review_result`` skips via dedup (posted=False) the
    counter must NOT advance — counter accuracy reflects wasted retries
    only, not bypassed ones."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="pendinghd",
        watch_retrigger_count=0,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        return True, False, now + timedelta(minutes=2)

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert runner.state.current_pr.watch_retrigger_count == 0


def test_pending_retrigger_cap_reached_escalates_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Third stale cycle on a permanently-silent Codex Connector escalates
    through ``_commit_and_park_in_error`` instead of re-posting @codex review."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="pendinghd",
        watch_retrigger_count=3,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    park_calls: list[dict[str, Any]] = []

    async def fake_park(
        message: str,
        *,
        subsource: str,
        log_message: str | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        park_calls.append(
            {
                "message": message,
                "subsource": subsource,
                "log_message": log_message,
                "extra_payload": extra_payload,
            }
        )
        if log_message is not None:
            runner.log_event(log_message)

    runner._commit_and_park_in_error = fake_park  # type: ignore[assignment]

    post_calls: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        post_calls.append(number)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert post_calls == []
    assert len(park_calls) == 1
    call = park_calls[0]
    assert "watch_retrigger_cap_reached: 3 cycles" in call["message"]
    assert call["subsource"] == "watch_retrigger_cap"
    assert call["extra_payload"] == {
        "review_status": "PENDING",
        "retrigger_count": 3,
        "cap": 3,
    }
    assert any(
        entry["event"]
        == (
            "PR #42 watch_retrigger cap reached (3/3); escalating to "
            "ERROR instead of re-triggering @codex review."
        )
        for entry in runner.state.history
    )


def test_changes_requested_retrigger_cap_reached_escalates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cap covers CHANGES_REQUESTED, not just PENDING — Codex going silent on
    a real review-requested PR is the same failure mode."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
        head_sha="changeshd",
        watch_retrigger_count=3,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner()
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    park_calls: list[dict[str, Any]] = []

    async def fake_park(
        message: str,
        *,
        subsource: str,
        log_message: str | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        park_calls.append({"subsource": subsource, "extra_payload": extra_payload})

    runner._commit_and_park_in_error = fake_park  # type: ignore[assignment]

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        raise AssertionError("cap branch must not call _post_codex_review_result")

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert len(park_calls) == 1
    assert park_calls[0]["subsource"] == "watch_retrigger_cap"
    assert park_calls[0]["extra_payload"]["review_status"] == "CHANGES_REQUESTED"


def test_eyes_retrigger_cap_reached_escalates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cap covers EYES too — using the shorter eyes-specific threshold."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=now,
        head_sha="eyeshd",
        watch_retrigger_count=3,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 6 * 60,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner()
    runner.app_config.daemon.stale_review_threshold_eyes_min = 5
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None

    park_calls: list[dict[str, Any]] = []

    async def fake_park(
        message: str,
        *,
        subsource: str,
        log_message: str | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        park_calls.append({"subsource": subsource, "extra_payload": extra_payload})

    runner._commit_and_park_in_error = fake_park  # type: ignore[assignment]

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        raise AssertionError("cap branch must not call _post_codex_review_result")

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    result = asyncio.run(runner._maybe_retrigger_stale_review(42))

    assert result is False
    assert len(park_calls) == 1
    assert park_calls[0]["subsource"] == "watch_retrigger_cap"
    assert park_calls[0]["extra_payload"]["review_status"] == "EYES"


def test_pending_retrigger_counter_resets_on_fresh_review_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Genuine GitHub event observed between WATCH cycles zeros the counter
    so the next stale cycle gets the full retry budget back."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=now,
        head_sha="freshhd",
        watch_retrigger_count=2,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 60,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner(review_timeout_min=120)
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_eyes_min = 5
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=12)
    runner._last_push_at_pr_number = pr.number
    runner._watch_last_event_signature = (
        42,
        CIStatus.PENDING,
        ReviewStatus.PENDING,
        now - timedelta(minutes=20),
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.watch_retrigger_count == 0


def test_call_sites_in_handle_watch_use_await(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Smoke test: handle_watch must ``await`` the now-async retrigger helper
    for each review state that dispatches to it. A missing ``await`` would
    yield a coroutine warning or a never-called record."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    _freeze_watch_datetime(monkeypatch, now)

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )

    states = [
        (ReviewStatus.CHANGES_REQUESTED, CIStatus.SUCCESS),
        (ReviewStatus.EYES, CIStatus.PENDING),
        (ReviewStatus.PENDING, CIStatus.PENDING),
    ]

    for review_status, ci_status in states:
        pr = PRInfo(
            number=42,
            branch="pr-042-fix",
            ci_status=ci_status,
            review_status=review_status,
            last_activity=now,
            head_sha="hd",
        )
        monkeypatch.setattr(
            "src.github.prs.get_open_prs",
            lambda repo, **kw: [pr],
        )

        retrigger_calls: list[int] = []

        async def fake_stale(pr_number: int) -> bool:
            retrigger_calls.append(pr_number)
            return False

        runner = h._make_runner(review_timeout_min=120)
        runner.app_config.daemon.hung_fallback_codex_review = True
        runner.state.current_pr = pr
        runner.state.state = PipelineState.WATCH
        runner._last_push_at = now - timedelta(minutes=12)
        runner._last_push_at_pr_number = pr.number
        runner._maybe_retrigger_stale_review = fake_stale  # type: ignore[assignment]
        runner._maybe_retrigger_on_codex_bot_error = (  # type: ignore[assignment]
            lambda pr_number: False
        )

        asyncio.run(runner.handle_watch())

        assert retrigger_calls == [42], (
            f"retrigger not awaited for review={review_status.value}, "
            f"ci={ci_status.value}"
        )


@pytest.mark.parametrize(
    "review_status,ci_status",
    [
        (ReviewStatus.PENDING, CIStatus.PENDING),
        (ReviewStatus.EYES, CIStatus.PENDING),
        (ReviewStatus.CHANGES_REQUESTED, CIStatus.SUCCESS),
    ],
)
def test_handle_watch_returns_after_cap_reached_escalation(
    monkeypatch: pytest.MonkeyPatch,
    review_status: ReviewStatus,
    ci_status: CIStatus,
) -> None:
    """When the cap branch fires inside ``_maybe_retrigger_stale_review`` the
    runner state transitions to ERROR; ``handle_watch`` must short-circuit
    before the review-timeout branch, otherwise a second escalation would
    overwrite the cap message and set ``skip_ai_error_diagnose=True``
    (turning a task-level park into a global stop-the-world ERROR).
    Covers all three review states that dispatch to the stale-retrigger
    helper, so each early-return guard is exercised."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    last_activity = now - timedelta(hours=3)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=ci_status,
        review_status=review_status,
        last_activity=last_activity,
        head_sha="caphd",
        watch_retrigger_count=3,
    )
    _freeze_watch_datetime(monkeypatch, now)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )

    runner = h._make_runner(review_timeout_min=20)
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.stale_review_threshold_eyes_min = 5
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = None
    runner._last_push_at = last_activity
    runner._last_push_at_pr_number = pr.number

    park_calls: list[dict[str, Any]] = []

    async def fake_park(
        message: str,
        *,
        subsource: str,
        log_message: str | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        park_calls.append({"subsource": subsource, "message": message})
        runner.state.state = PipelineState.ERROR

    runner._commit_and_park_in_error = fake_park  # type: ignore[assignment]

    transition_calls: list[str] = []

    async def fake_transition_to_error(
        message: str,
        *,
        save_run_record_as: str | None = None,
        log_prefix: str = "[ERROR]",
        log_message: str | None = None,
        cancellation_cause: Any = None,
    ) -> None:
        transition_calls.append(message)
        runner.state.state = PipelineState.ERROR

    runner._transition_to_error = fake_transition_to_error  # type: ignore[assignment]

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        raise AssertionError(
            "cap branch must not call _post_codex_review_result"
        )

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert len(park_calls) == 1
    assert park_calls[0]["subsource"] == "watch_retrigger_cap"
    assert transition_calls == []


def test_pending_retrigger_cap_permits_exactly_cap_retriggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A configured cap of N must allow exactly N actual retrigger posts
    before escalating. Regression for the off-by-one where ``next_count >=
    cap`` short-circuited at cycle N (only N-1 posts) and made
    ``watch_retrigger_cap=1`` post zero retriggers."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
        head_sha="caphd",
        watch_retrigger_count=0,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 700,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner()
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_min = 10
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH

    post_calls: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        post_calls.append(number)
        return True, True, None

    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    park_calls: list[str] = []

    async def fake_park(
        message: str,
        *,
        subsource: str,
        log_message: str | None = None,
        extra_payload: dict[str, Any] | None = None,
    ) -> None:
        park_calls.append(subsource)

    runner._commit_and_park_in_error = fake_park  # type: ignore[assignment]

    for cycle in range(1, 5):
        runner.state.last_stale_retrigger_at = None
        result = asyncio.run(runner._maybe_retrigger_stale_review(42))
        if cycle <= 3:
            assert result is True, f"cycle {cycle} should post"
            assert runner.state.current_pr.watch_retrigger_count == cycle
        else:
            assert result is False, "cycle 4 must hit cap"

    assert len(post_calls) == 3
    assert park_calls == ["watch_retrigger_cap"]
