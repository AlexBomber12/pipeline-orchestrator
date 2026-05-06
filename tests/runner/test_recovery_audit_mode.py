"""PR-266b: env-flag dispatch and audit-mode comparison for recover_state."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from src.daemon import recovery as recovery_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.task_status import MergedState

from tests.runner import _helpers as h


def _set_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    audit: str | None = None,
    headers: str | None = None,
) -> None:
    if audit is None:
        monkeypatch.delenv(recovery_module.RECOVERY_AUDIT_ENV, raising=False)
    else:
        monkeypatch.setenv(recovery_module.RECOVERY_AUDIT_ENV, audit)
    if headers is None:
        monkeypatch.delenv(recovery_module.RECOVERY_HEADERS_ENV, raising=False)
    else:
        monkeypatch.setenv(recovery_module.RECOVERY_HEADERS_ENV, headers)


def _stub_open_prs(monkeypatch: pytest.MonkeyPatch, prs: list[PRInfo]) -> None:
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: list(prs)
    )


def _write_pr_md(repo: Path, pr_id: str, branch: str, *, priority: int = 3) -> None:
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)
    (task_dir / f"{pr_id}.md").write_text(
        "\n".join(
            [
                f"# {pr_id}: title",
                "",
                f"Branch: {branch}",
                "- Type: feature",
                "- Complexity: low",
                "- Depends on: none",
                f"- Priority: {priority}",
                "- Coder: codex",
                "",
                "## Problem",
                "Body.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _stub_resolve_merged_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    merged_pr_ids: set[str] | None = None,
    merged_branches: set[str] | None = None,
) -> None:
    def fake_resolve(*args: object, **kwargs: object) -> MergedState:
        return MergedState(
            set(merged_pr_ids or ()), set(merged_branches or ()), True
        )

    monkeypatch.setattr(recovery_module, "_resolve_merged_state", fake_resolve)


def test_resolve_recovery_mode_defaults_legacy_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch)
    assert (
        recovery_module._resolve_recovery_mode()
        == recovery_module.RECOVERY_MODE_LEGACY_ONLY
    )


def test_resolve_recovery_mode_audit_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, audit="1", headers="0")
    assert (
        recovery_module._resolve_recovery_mode()
        == recovery_module.RECOVERY_MODE_AUDIT_LEGACY_APPLIES
    )


def test_resolve_recovery_mode_headers_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, audit="0", headers="1")
    assert (
        recovery_module._resolve_recovery_mode()
        == recovery_module.RECOVERY_MODE_HEADERS_ONLY
    )


def test_resolve_recovery_mode_both_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, audit="1", headers="1")
    assert (
        recovery_module._resolve_recovery_mode()
        == recovery_module.RECOVERY_MODE_AUDIT_HEADERS_APPLIES
    )


def test_resolve_recovery_mode_invalid_env_value(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _set_env(monkeypatch, audit="yes", headers="0")
    caplog.set_level("WARNING", logger=recovery_module.logger.name)
    mode = recovery_module._resolve_recovery_mode()
    assert mode == recovery_module.RECOVERY_MODE_LEGACY_ONLY
    assert any(
        "PIPELINE_RECOVERY_AUDIT" in record.message
        and "not in (0, 1)" in record.message
        for record in caplog.records
    )


def test_resolve_recovery_mode_empty_env_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_env(monkeypatch, audit="", headers="")
    assert (
        recovery_module._resolve_recovery_mode()
        == recovery_module.RECOVERY_MODE_LEGACY_ONLY
    )


def test_legacy_only_mode_skips_new_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """In LEGACY_ONLY mode the headers helper must not be called."""
    _set_env(monkeypatch)
    _stub_open_prs(monkeypatch, [])
    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: []  # type: ignore[method-assign]

    def _boom(self: Any) -> None:
        raise AssertionError("_parse_tasks_from_headers must not run")

    monkeypatch.setattr(
        recovery_module.RecoveryMixin, "_parse_tasks_from_headers", _boom
    )

    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.state == PipelineState.IDLE


def test_audit_legacy_applies_mode_runs_both(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """audit=1 headers=0: legacy applies state, helper runs as dry-run."""
    _set_env(monkeypatch, audit="1", headers="0")
    _stub_open_prs(monkeypatch, [])
    legacy_called: list[bool] = []
    new_called: list[bool] = []

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]

    def fake_parse_base(**kwargs: Any) -> list[QueueTask]:
        legacy_called.append(True)
        return []

    def fake_parse_headers() -> list[QueueTask]:
        new_called.append(True)
        return []

    runner._parse_base_queue = fake_parse_base  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = fake_parse_headers  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert legacy_called == [True]
    assert new_called == [True]
    # Legacy applies state; headers result is discarded.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_queue is None  # legacy path leaves it untouched


def test_audit_diff_emits_on_divergence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A status drift between legacy and headers must surface as [AUDIT]."""
    _set_env(monkeypatch, audit="1", headers="0")
    _stub_open_prs(monkeypatch, [])
    legacy_tasks = [
        QueueTask(
            pr_id="PR-005",
            title="Diverging task",
            status=TaskStatus.TODO,
            branch="pr-005",
            task_file="tasks/PR-005.md",
        )
    ]
    new_tasks = [
        QueueTask(
            pr_id="PR-005",
            title="Diverging task",
            status=TaskStatus.DONE,
            branch="pr-005",
            task_file="tasks/PR-005.md",
        )
    ]

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(legacy_tasks)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    audit_events = [
        e["event"] for e in runner.state.history
        if e["event"].startswith("[AUDIT] recover_state divergence:")
    ]
    assert len(audit_events) == 1
    payload = json.loads(audit_events[0].split(": ", 1)[1])
    assert payload["audit"] == "recover_state"
    assert payload["mode"] == recovery_module.RECOVERY_MODE_AUDIT_LEGACY_APPLIES
    drift = payload["diff"]["current_queue_status_drift"]
    assert drift == [
        {
            "pr_id": "PR-005",
            "legacy_status": "TODO",
            "new_status": "DONE",
        }
    ]


def test_audit_diff_silent_on_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Identical projections must not log any [AUDIT] event."""
    _set_env(monkeypatch, audit="1", headers="0")
    _stub_open_prs(monkeypatch, [])
    tasks = [
        QueueTask(
            pr_id="PR-005",
            title="Aligned task",
            status=TaskStatus.TODO,
            branch="pr-005",
            task_file="tasks/PR-005.md",
        )
    ]

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(tasks)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert not any(
        e["event"].startswith("[AUDIT] recover_state divergence:")
        for e in runner.state.history
    )


def test_headers_only_mode_skips_legacy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """audit=0 headers=1: legacy helper must not run."""
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    _stub_resolve_merged_state(monkeypatch)

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-001", "pr-001-branch")
    runner = h._make_runner()
    runner.repo_path = str(repo)

    def _boom_legacy(**kwargs: Any) -> None:
        raise AssertionError("_parse_base_queue must not run")

    runner._parse_base_queue = _boom_legacy  # type: ignore[method-assign]

    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.state == PipelineState.IDLE


def test_headers_only_hydrates_current_queue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """state.current_queue must reflect the headers-derived snapshot."""
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    _stub_resolve_merged_state(monkeypatch)

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-001", "pr-001-branch", priority=2)
    _write_pr_md(repo, "PR-002", "pr-002-branch", priority=3)
    runner = h._make_runner()
    runner.repo_path = str(repo)

    asyncio.run(runner.recover_state())

    assert runner.state.current_queue is not None
    assert [t.pr_id for t in runner.state.current_queue] == [
        "PR-001",
        "PR-002",
    ]


def test_audit_headers_applies_mode_logs_diffs_but_uses_new(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """audit=1 headers=1: new path applies state; legacy is dry-run."""
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    legacy_tasks = [
        QueueTask(
            pr_id="PR-010",
            title="task",
            status=TaskStatus.TODO,
            branch="pr-010",
            task_file="tasks/PR-010.md",
        )
    ]
    new_tasks = [
        QueueTask(
            pr_id="PR-010",
            title="task",
            status=TaskStatus.DONE,
            branch="pr-010",
            task_file="tasks/PR-010.md",
        )
    ]

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(legacy_tasks)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    # New path applied state — current_queue holds the new task list.
    assert runner.state.current_queue is not None
    assert runner.state.current_queue[0].status == TaskStatus.DONE
    audit_events = [
        e["event"] for e in runner.state.history
        if e["event"].startswith("[AUDIT] recover_state divergence:")
    ]
    assert len(audit_events) == 1
    payload = json.loads(audit_events[0].split(": ", 1)[1])
    assert payload["mode"] == recovery_module.RECOVERY_MODE_AUDIT_HEADERS_APPLIES


def test_recovery_audit_diff_pure_function() -> None:
    """The diff helper is a pure function over (legacy, new, prs, set)."""
    legacy = [
        QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
        )
    ]
    new = [
        QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DONE, branch="pr-001"
        )
    ]
    prs: list[PRInfo] = []
    diff = recovery_module._recovery_audit_diff(legacy, new, prs, set())
    assert diff is not None
    assert diff["current_queue_status_drift"] == [
        {
            "pr_id": "PR-001",
            "legacy_status": "DOING",
            "new_status": "DONE",
        }
    ]
    # Both projections settle in IDLE for these inputs (DOING with no
    # matching PR + no recovered set + no recoverable open PR), so the
    # diff omits ``pipeline_state``.
    assert "pipeline_state" not in diff
    assert recovery_module._recovery_audit_diff(legacy, legacy, prs, set()) is None


def test_recovery_audit_diff_length_change() -> None:
    legacy: list[QueueTask] = []
    new = [
        QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.TODO, branch="pr-001"
        )
    ]
    diff = recovery_module._recovery_audit_diff(legacy, new, [], set())
    assert diff is not None
    assert diff["current_queue_length"] == {"legacy": 0, "new": 1}


def test_recovery_audit_diff_pipeline_state_field() -> None:
    legacy = [
        QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING, branch="pr-001"
        )
    ]
    new = [
        QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.TODO, branch="pr-001"
        )
    ]
    prs = [PRInfo(number=1, branch="pr-001")]
    diff = recovery_module._recovery_audit_diff(legacy, new, prs, set())
    assert diff is not None
    # Legacy: DOING with matching PR -> WATCH.
    # New: TODO with matching PR -> recoverable -> WATCH (same).
    # The current_queue_status_drift must still surface the difference.
    assert "current_queue_status_drift" in diff


def test_recovery_audit_diff_pipeline_state_diverges() -> None:
    """Legacy WATCH vs new IDLE surfaces pipeline_state in the diff."""
    legacy = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.DOING,
            branch="pr-001",
        )
    ]
    new = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.TODO,
            branch="pr-different",
        )
    ]
    prs = [PRInfo(number=1, branch="pr-001")]
    diff = recovery_module._recovery_audit_diff(legacy, new, prs, set())
    assert diff is not None
    assert diff["pipeline_state"] == {"legacy": "WATCH", "new": "IDLE"}
    assert diff["current_task_pr_id"] == {"legacy": "PR-001", "new": None}
    assert diff["current_pr_number"] == {"legacy": 1, "new": None}


def test_project_recovery_decision_recovered_doing_stays_idle() -> None:
    """A DOING task in the recovered set projects to IDLE."""
    tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.DOING,
            branch="pr-001",
        )
    ]
    prs = [PRInfo(number=1, branch="pr-001")]
    proj = recovery_module._project_recovery_decision(tasks, prs, {"PR-001"})
    assert proj == {
        "pipeline_state": "IDLE",
        "current_task_pr_id": None,
        "current_pr_number": None,
        "pending_queue_sync_branch": None,
    }


def test_project_recovery_decision_doing_branchless_falls_through() -> None:
    """A DOING task with no branch can never match an open PR."""
    tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.DOING,
            branch=None,
        )
    ]
    prs = [PRInfo(number=1, branch="pr-001")]
    proj = recovery_module._project_recovery_decision(tasks, prs, set())
    assert proj["pipeline_state"] == "IDLE"
    assert proj["current_task_pr_id"] is None


def test_project_recovery_decision_recoverable_match() -> None:
    tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.TODO,
            branch="pr-001",
        )
    ]
    prs = [PRInfo(number=42, branch="pr-001")]
    proj = recovery_module._project_recovery_decision(tasks, prs, set())
    assert proj == {
        "pipeline_state": "WATCH",
        "current_task_pr_id": "PR-001",
        "current_pr_number": 42,
        "pending_queue_sync_branch": None,
    }


def test_project_recovery_decision_pending_queue_sync() -> None:
    prs = [PRInfo(number=200, branch="queue-done-20260201")]
    proj = recovery_module._project_recovery_decision([], prs, set())
    assert proj["pending_queue_sync_branch"] == "queue-done-20260201"


def test_emit_audit_diff_legacy_dry_run_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A legacy dry-run that raises is logged as [AUDIT] failure, not [AUDIT] divergence."""
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    new_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.TODO,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        )
    ]

    def _boom(**kwargs: Any) -> None:
        raise RuntimeError("legacy parse blew up")

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = _boom  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert any(
        "recover_state legacy-path dry-run failed" in e["event"]
        for e in runner.state.history
    )


def test_emit_audit_diff_new_dry_run_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A headers-helper dry-run that raises is logged as [AUDIT] failure."""
    _set_env(monkeypatch, audit="1", headers="0")
    _stub_open_prs(monkeypatch, [])

    def _boom() -> None:
        raise RuntimeError("headers parse blew up")

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: []  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = _boom  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert any(
        "recover_state new-path dry-run failed" in e["event"]
        for e in runner.state.history
    )


def test_headers_only_get_open_prs_failure_returns_false(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A GitHub failure during get_open_prs must return False so the cycle retries."""
    _set_env(monkeypatch, audit="0", headers="1")

    def _boom(repo: str, **kwargs: Any) -> None:
        raise RuntimeError("gh down")

    monkeypatch.setattr("src.github.prs.get_open_prs", _boom)
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    assert asyncio.run(runner.recover_state()) is False
    assert runner.state.state == PipelineState.ERROR


def test_headers_only_validation_error_returns_false(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A QueueValidationError from the headers helper transitions to ERROR."""
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])

    from src.queue_parser import QueueValidationError

    def _boom() -> None:
        raise QueueValidationError(["bad header"])

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = _boom  # type: ignore[method-assign]

    assert asyncio.run(runner.recover_state()) is False
    assert runner.state.state == PipelineState.ERROR


def test_headers_only_no_tasks_settles_idle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Empty header parse settles in IDLE with empty current_queue."""
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_queue == []


def test_headers_only_preserves_prior_idle_open_prs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The recover_state path must not leak the recovery PRs into _idle_open_prs."""
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [PRInfo(number=1, branch="x")])
    _stub_resolve_merged_state(monkeypatch)
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    sentinel: list[PRInfo] = [PRInfo(number=99, branch="prior")]
    runner._idle_open_prs = sentinel
    runner._idle_merged_prs = []

    asyncio.run(runner.recover_state())

    assert runner._idle_open_prs is sentinel


def test_audit_legacy_applies_uses_live_prs_not_stale_idle_attr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The audit diff must score against the live ``get_open_prs`` snapshot.

    Regression: ``_emit_audit_diff`` previously read ``_idle_open_prs``,
    which the legacy applies path never sets. The diff therefore ran
    against ``[]`` and missed any divergence whose detection depended on
    matching open PR branches, undermining the rollout gate.
    """
    _set_env(monkeypatch, audit="1", headers="0")
    live_prs = [PRInfo(number=42, branch="pr-005")]
    _stub_open_prs(monkeypatch, live_prs)
    legacy_tasks = [
        QueueTask(
            pr_id="PR-005",
            title="Branch matches live PR",
            status=TaskStatus.TODO,
            branch="pr-005",
            task_file="tasks/PR-005.md",
        )
    ]
    new_tasks = [
        QueueTask(
            pr_id="PR-005",
            title="Branch does not match live PR",
            status=TaskStatus.TODO,
            branch="pr-different",
            task_file="tasks/PR-005.md",
        )
    ]

    runner = h._make_runner()
    # Pre-seed stale empty snapshots so any reliance on these attributes
    # would silently feed ``[]`` to the audit comparator and miss the
    # branch-driven divergence below. Also asserts that the dry-run
    # restores both attributes to their prior values on exit.
    stale_open: list[PRInfo] = []
    stale_merged: list[PRInfo] = []
    runner._idle_open_prs = stale_open
    runner._idle_merged_prs = stale_merged
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(legacy_tasks)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    audit_events = [
        e["event"]
        for e in runner.state.history
        if e["event"].startswith("[AUDIT] recover_state divergence:")
    ]
    assert len(audit_events) == 1
    payload = json.loads(audit_events[0].split(": ", 1)[1])
    diff = payload["diff"]
    # legacy: TODO with matching live PR -> recoverable -> WATCH on PR-005.
    # new: TODO with no matching PR -> IDLE.
    # The audit must detect this only if the live prs reach the comparator.
    assert diff["pipeline_state"] == {"legacy": "WATCH", "new": "IDLE"}
    assert diff["current_pr_number"] == {"legacy": 42, "new": None}
    # Dry-run must restore both attributes to their prior values.
    assert runner._idle_open_prs is stale_open
    assert runner._idle_merged_prs is stale_merged


def test_audit_headers_applies_uses_live_prs_after_idle_restore(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Headers-applies audit must use the live PR snapshot, not the restored attr.

    The headers path restores ``_idle_open_prs`` to its prior value
    before returning, so by the time ``_emit_audit_diff`` runs the
    attribute no longer reflects the recovery-time PR set.
    """
    _set_env(monkeypatch, audit="1", headers="1")
    live_prs = [PRInfo(number=7, branch="pr-010")]
    _stub_open_prs(monkeypatch, live_prs)

    legacy_tasks = [
        QueueTask(
            pr_id="PR-010",
            title="legacy branch points elsewhere",
            status=TaskStatus.TODO,
            branch="pr-stale",
            task_file="tasks/PR-010.md",
        )
    ]
    new_tasks = [
        QueueTask(
            pr_id="PR-010",
            title="new branch matches live PR",
            status=TaskStatus.TODO,
            branch="pr-010",
            task_file="tasks/PR-010.md",
        )
    ]

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(legacy_tasks)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    audit_events = [
        e["event"]
        for e in runner.state.history
        if e["event"].startswith("[AUDIT] recover_state divergence:")
    ]
    assert len(audit_events) == 1
    payload = json.loads(audit_events[0].split(": ", 1)[1])
    diff = payload["diff"]
    # legacy: TODO no matching PR -> IDLE.
    # new: TODO matching PR -> recoverable -> WATCH on PR-010.
    assert diff["pipeline_state"] == {"legacy": "IDLE", "new": "WATCH"}
    assert diff["current_pr_number"] == {"legacy": None, "new": 7}


def test_audit_dry_run_drops_ghost_entries_on_local_queue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Audit dry-run must mirror legacy's ghost-entry filter on local queues.

    Regression: ``_emit_audit_diff`` previously called
    ``_parse_base_queue`` without mirroring ``_recover_state_legacy``'s
    ``queue_from_origin`` probe and ``_drop_ghost_queue_entries`` step.
    On post-PR-181 repos (``tasks/QUEUE.md`` is gitignored and survives
    ``sync_to_main``'s ``git reset --hard`` / ``git clean -fd``), a
    stale row referencing a deleted ``tasks/PR-XXX.md`` would surface
    as an ``[AUDIT] recover_state divergence`` even though the actual
    legacy recovery would have dropped it before any decision,
    polluting the rollout signal with phantom divergences.
    """
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    (repo / "tasks" / "PR-001.md").write_text("placeholder", encoding="utf-8")
    # PR-999.md is intentionally NOT created — it's a ghost entry.

    legacy_with_ghost = [
        QueueTask(
            pr_id="PR-001",
            title="real",
            status=TaskStatus.TODO,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        ),
        QueueTask(
            pr_id="PR-999",
            title="ghost",
            status=TaskStatus.TODO,
            branch="pr-999",
            task_file="tasks/PR-999.md",
        ),
    ]
    new_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="real",
            status=TaskStatus.TODO,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        ),
    ]

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(legacy_with_ghost)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    # Without the ghost filter the audit would have logged a
    # ``current_queue_length`` divergence (1 vs 2). With it, parity
    # holds and the audit log stays silent.
    assert not any(
        e["event"].startswith("[AUDIT] recover_state divergence:")
        for e in runner.state.history
    )


def test_audit_dry_run_keeps_origin_tracked_queue_unfiltered(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """On origin-tracked queues the dry-run must NOT apply the ghost filter.

    The local-existence test is unsafe when the queue snapshot came
    from ``origin/{branch}`` (the working tree may legitimately be
    parked on a feature branch whose checkout lacks task files
    referenced by the base-branch queue). Applying the filter there
    would drop real DOING/DONE entries and hide legitimate divergence.
    """
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    # PR-001.md intentionally absent from the working tree.

    legacy_origin_snapshot = [
        QueueTask(
            pr_id="PR-001",
            title="origin task",
            status=TaskStatus.DOING,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        ),
    ]
    new_tasks: list[QueueTask] = []

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: list(legacy_origin_snapshot)  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    audit_events = [
        e["event"]
        for e in runner.state.history
        if e["event"].startswith("[AUDIT] recover_state divergence:")
    ]
    assert len(audit_events) == 1
    payload = json.loads(audit_events[0].split(": ", 1)[1])
    assert payload["diff"]["current_queue_length"] == {"legacy": 1, "new": 0}


def test_audit_dry_run_skips_when_queue_probe_indeterminate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A ``None`` probe result must skip the dry-run, not fabricate divergence.

    Mirrors ``_recover_state_legacy``: when the probe is indeterminate
    legacy would transition to ERROR, so the comparator has no
    legitimate snapshot to score against. Calling ``_parse_base_queue``
    anyway would fall back to a possibly-stale working-tree copy and
    log spurious divergences.
    """
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    new_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="task",
            status=TaskStatus.TODO,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        ),
    ]

    parse_calls: list[bool] = []

    def _parse(**kwargs: Any) -> list[QueueTask]:
        parse_calls.append(True)
        return []

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._origin_queue_md_tracked = lambda: None  # type: ignore[method-assign]
    runner._parse_base_queue = _parse  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert parse_calls == []
    assert any(
        "legacy-path dry-run skipped" in e["event"]
        and "tracking probe failed" in e["event"]
        for e in runner.state.history
    )
    assert not any(
        e["event"].startswith("[AUDIT] recover_state divergence:")
        for e in runner.state.history
    )


def test_audit_dry_run_skips_when_origin_snapshot_unreadable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A ``None`` parse on an origin-tracked queue must skip the dry-run.

    ``_parse_base_queue`` returns ``None`` from a tracked snapshot when
    ``git show origin/{branch}:tasks/QUEUE.md`` fails (missing ref or
    transient git error). The legacy applied path transitions to ERROR
    in that case, so the comparator has no legitimate input to score
    against. Coercing ``None`` to ``[]`` here would fabricate
    ``current_queue_length`` divergences whenever the live header
    snapshot is non-empty.
    """
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    new_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="task",
            status=TaskStatus.TODO,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        ),
    ]

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._origin_queue_md_tracked = lambda: True  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: None  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: list(new_tasks)  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert any(
        "legacy-path dry-run skipped" in e["event"]
        and "read QUEUE.md from origin failed" in e["event"]
        for e in runner.state.history
    )
    assert not any(
        e["event"].startswith("[AUDIT] recover_state divergence:")
        for e in runner.state.history
    )


def test_audit_dry_run_treats_missing_local_queue_as_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A ``None`` parse on a working-tree queue mirrors legacy's empty fallback.

    On post-PR-181 repos the legacy applied path treats a missing
    ``tasks/QUEUE.md`` as an empty queue (deferred to preflight + IDLE
    regeneration). The audit dry-run must mirror that — coercing
    ``None`` to ``[]`` is the right behavior here, not a skip.
    """
    _set_env(monkeypatch, audit="1", headers="1")
    _stub_open_prs(monkeypatch, [])

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **kwargs: None  # type: ignore[method-assign]
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    assert not any(
        "legacy-path dry-run skipped" in e["event"]
        for e in runner.state.history
    )
    assert not any(
        e["event"].startswith("[AUDIT] recover_state divergence:")
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# PR-266b crash-no-PR semantics in HEADERS_ONLY mode
# ---------------------------------------------------------------------------


def _persist_current_task(runner: Any, pr_id: str, branch: str) -> None:
    """Seed the published RepoState snapshot with a DOING ``current_task``.

    Mirrors what ``publish_state`` writes mid-CODING, so a fresh runner
    started after a crash can rehydrate the marker exactly as it would
    on a real daemon restart.
    """
    from src.keyspace import pipeline_state as _pipeline_state_key
    from src.models import RepoState

    persisted = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id=pr_id,
            title="Crashed before push",
            status=TaskStatus.DOING,
            branch=branch,
            task_file=f"tasks/{pr_id}.md",
        ),
    )
    asyncio.run(
        runner.redis.set(
            _pipeline_state_key(runner.name), persisted.model_dump_json()
        )
    )


def test_headers_only_mode_marks_pre_push_crash_canceled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Pre-push crash on a fresh restart routes through PR-186 crash path.

    When ``PIPELINE_RECOVERY_FROM_HEADERS=1`` and the daemon was killed
    mid-CODING before any PR was opened, the runner's in-memory
    ``state.current_task`` resets to ``None`` on restart. Without
    rehydrating the published snapshot, the headers helper would
    derive the task back to ``TODO`` (no open PR + no
    ``current_task_pr_id``), the DOING-with-no-PR crash path in
    ``_apply_recovery_decisions`` would never run, and the next IDLE
    cycle would silently re-dispatch the task into another doomed
    CODING run. Hydrating ``state.current_task`` from
    ``pipeline_state(repo)`` restores the legacy behavior: task is
    canceled, added to ``_crashed_task_pr_ids``, and the daemon stays
    IDLE pending manual re-upload.
    """
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    _stub_resolve_merged_state(monkeypatch)

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-186", "pr-186-crash-no-pr", priority=1)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    _persist_current_task(runner, "PR-186", "pr-186-crash-no-pr")

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner._crashed_task_pr_ids == {"PR-186"}
    assert runner.state.current_queue is not None
    crash_entry = next(
        (t for t in runner.state.current_queue if t.pr_id == "PR-186"),
        None,
    )
    assert crash_entry is not None
    assert crash_entry.status == TaskStatus.CANCELED
    assert any(
        "Task PR-186 crashed, marking CANCELED. Manually re-upload to retry"
        in e["event"]
        for e in runner.state.history
    )


def test_headers_only_mode_pre_push_crash_resumes_watch_when_pr_visible(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A late-visible PR after the same crash resumes WATCH, not CANCELED.

    If the recovery ``get_open_prs`` snapshot does include a matching
    PR (e.g. push raced with the kill), the rehydrated
    ``current_task_pr_id`` derives DOING via the open-PR match and
    ``_apply_recovery_decisions`` reattaches WATCH. The hydrated
    marker must not force CANCELED for tasks that actually have live
    PRs.
    """
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(
        monkeypatch,
        [PRInfo(number=42, branch="pr-186-crash-no-pr", pr_id="PR-186")],
    )
    _stub_resolve_merged_state(monkeypatch)
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {
            "author": "",
            "head_sha": "",
            "head_commit_date": "2026-04-30T12:00:00Z",
        },
    )

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-186", "pr-186-crash-no-pr", priority=1)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    _persist_current_task(runner, "PR-186", "pr-186-crash-no-pr")

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-186"
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42
    assert "PR-186" not in runner._crashed_task_pr_ids


def test_headers_only_mode_does_not_synthesize_crash_when_redis_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """No persisted snapshot means no rehydrate; task derives ``TODO``.

    A fresh repo (no prior crash) must not pick up a hydrated
    ``current_task`` from another runner. The helper should be a
    no-op when ``pipeline_state(repo)`` is missing, leaving the task
    derivation to the normal PR/header-based path.
    """
    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    _stub_resolve_merged_state(monkeypatch)

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-001", "pr-001-fresh", priority=1)
    runner = h._make_runner()
    runner.repo_path = str(repo)

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner._crashed_task_pr_ids == set()
    assert runner.state.current_queue is not None
    assert runner.state.current_queue[0].status == TaskStatus.TODO


def test_hydrate_current_task_ignores_corrupt_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupt ``pipeline_state`` payload must not abort recovery.

    Defense-in-depth: the helper is best-effort. A malformed JSON or
    a schema mismatch should leave ``state.current_task`` untouched
    rather than raise — recovery still has to make progress for the
    operator to fix the underlying issue.
    """
    from src.keyspace import pipeline_state as _pipeline_state_key

    runner = h._make_runner()
    asyncio.run(
        runner.redis.set(_pipeline_state_key(runner.name), "{not json")
    )
    assert runner.state.current_task is None

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task is None


def test_hydrate_current_task_swallows_redis_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis read failure must not block recovery startup.

    Mirrors ``_load_recovered_task_pr_ids``'s defensive contract: the
    rehydrate is best-effort, so a transient Redis outage downgrades
    to "no hydration" rather than aborting recovery before the
    headers parser even runs.
    """
    runner = h._make_runner()

    async def _boom_get(*args: object, **kwargs: object) -> None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner.redis, "get", _boom_get)
    assert runner.state.current_task is None

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task is None


def test_hydrate_current_task_skips_when_already_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An already-set ``current_task`` is the source of truth.

    The hydrate is intentionally narrow: it only fills the gap left
    by a fresh ``RepoState`` on restart. If something earlier in the
    cycle already set ``current_task``, the helper must not clobber
    it with a stale Redis snapshot.
    """
    from src.keyspace import pipeline_state as _pipeline_state_key
    from src.models import RepoState

    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-LIVE",
        title="active",
        status=TaskStatus.DOING,
        branch="pr-live",
    )
    persisted = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-STALE",
            title="stale",
            status=TaskStatus.DOING,
            branch="pr-stale",
        ),
    )
    asyncio.run(
        runner.redis.set(
            _pipeline_state_key(runner.name), persisted.model_dump_json()
        )
    )

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task is not None
    assert runner.state.current_task.pr_id == "PR-LIVE"


def test_headers_only_mode_does_not_hydrate_when_persisted_idle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A persisted snapshot with ``current_task=None`` must not change behavior.

    The last persisted state of a healthy IDLE daemon is
    ``current_task=None``. The helper must remain a no-op in that
    case so a TODO task is not falsely upgraded to DOING.
    """
    from src.keyspace import pipeline_state as _pipeline_state_key
    from src.models import RepoState

    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    _stub_resolve_merged_state(monkeypatch)

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-001", "pr-001", priority=1)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    persisted = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.IDLE,
        current_task=None,
    )
    asyncio.run(
        runner.redis.set(
            _pipeline_state_key(runner.name), persisted.model_dump_json()
        )
    )

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner._crashed_task_pr_ids == set()
    assert runner.state.current_queue is not None
    assert runner.state.current_queue[0].status == TaskStatus.TODO


def test_headers_only_mode_does_not_hydrate_non_coding_persisted_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A WATCH snapshot whose PR vanished must not be marked CANCELED.

    Hydrating ``current_task`` for any persisted ``state`` would
    misclassify a benign restart as a mid-CODING crash whenever the
    PR is no longer open: ``derive_task_status`` would fall through
    to the ``current_task_pr_id`` branch, return ``DOING``, and
    ``_apply_recovery_decisions`` would force manual re-upload via
    the PR-186 crash path. The hydrate must be gated on
    ``PipelineState.CODING`` — the only state where ``current_task``
    is set without an open PR.
    """
    from src.keyspace import pipeline_state as _pipeline_state_key
    from src.models import RepoState

    _set_env(monkeypatch, audit="0", headers="1")
    _stub_open_prs(monkeypatch, [])
    _stub_resolve_merged_state(monkeypatch)

    repo = tmp_path / "repo"
    repo.mkdir()
    _write_pr_md(repo, "PR-200", "pr-200-watch-pr-gone", priority=1)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    persisted = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.WATCH,
        current_task=QueueTask(
            pr_id="PR-200",
            title="WATCH snapshot",
            status=TaskStatus.DOING,
            branch="pr-200-watch-pr-gone",
            task_file="tasks/PR-200.md",
        ),
    )
    asyncio.run(
        runner.redis.set(
            _pipeline_state_key(runner.name), persisted.model_dump_json()
        )
    )

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert "PR-200" not in runner._crashed_task_pr_ids
    assert runner.state.current_queue is not None
    entry = next(
        (t for t in runner.state.current_queue if t.pr_id == "PR-200"),
        None,
    )
    assert entry is not None
    assert entry.status == TaskStatus.TODO


def test_hydrate_current_task_skips_non_coding_persisted_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The helper itself must skip hydration when persisted state != CODING.

    Direct unit-level guard so the gating cannot regress without a
    failing test, independent of the full ``recover_state`` flow.
    """
    from src.keyspace import pipeline_state as _pipeline_state_key
    from src.models import RepoState

    runner = h._make_runner()
    persisted = RepoState(
        url=runner.repo_config.url,
        name=runner.name,
        state=PipelineState.WATCH,
        current_task=QueueTask(
            pr_id="PR-STALE",
            title="stale watch",
            status=TaskStatus.DOING,
            branch="pr-stale",
        ),
    )
    asyncio.run(
        runner.redis.set(
            _pipeline_state_key(runner.name), persisted.model_dump_json()
        )
    )
    assert runner.state.current_task is None

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task is None
