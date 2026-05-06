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
