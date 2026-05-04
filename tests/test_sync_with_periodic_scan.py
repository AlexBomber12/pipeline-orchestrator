"""Tests for the ``log_event_fn`` hook on ``reconcile_agents_md``.

PR-260 wires :func:`_scan_existing_task_specs` into the AGENTS.md
reconciliation flow as an opt-in side effect: when the caller passes a
``log_event_fn`` the scan runs after the rewrite; when it does not, the
flow stays bit-for-bit identical to its pre-PR-260 behaviour. These
tests pin both halves of that contract so a future refactor cannot
silently invert the gate.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from src.onboarding.reconciliation import reconcile_agents_md


def test_sync_calls_scan_at_end(tmp_path: Path) -> None:
    target = tmp_path / "AGENTS.md"
    events: list[str] = []
    log_fn = events.append

    with patch(
        "src.onboarding.reconciliation._scan_existing_task_specs"
    ) as mock_scan:
        reconcile_agents_md(target, dry_run=False, log_event_fn=log_fn)

    assert mock_scan.call_count == 1
    args, _kwargs = mock_scan.call_args
    assert args[0] == target.parent
    assert args[1] is log_fn
    assert target.exists(), "rewrite must run before the scan hook fires"


def test_sync_skips_scan_when_log_event_fn_none(tmp_path: Path) -> None:
    target = tmp_path / "AGENTS.md"

    with patch(
        "src.onboarding.reconciliation._scan_existing_task_specs"
    ) as mock_scan:
        reconcile_agents_md(target, dry_run=False)

    assert mock_scan.call_count == 0


def test_sync_emits_scan_events_through_provided_log_fn(tmp_path: Path) -> None:
    """End-to-end: a real spec file containing a violation produces a
    real ``[AGENTS-SCAN]`` event when the caller threads the log
    function through. Catches a regression where the hook fires but
    the events do not reach the caller's log function."""
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-099.md").write_text(
        "# old spec\n\nSkip CI on this branch.\n", encoding="utf-8"
    )
    target = tmp_path / "AGENTS.md"
    events: list[str] = []

    reconcile_agents_md(target, dry_run=False, log_event_fn=events.append)

    scan_events = [e for e in events if e.startswith("[AGENTS-SCAN]")]
    assert any("PR-099.md" in e and "skip_ci" in e for e in scan_events)


def test_sync_runs_scan_on_dry_run_when_log_fn_provided(tmp_path: Path) -> None:
    """``dry_run=True`` does not write AGENTS.md, but the scan still
    runs when a log function is supplied so operators can preview
    drift findings without committing a rewrite."""
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-100.md").write_text(
        "# old spec\n\nSkip CI on this branch.\n", encoding="utf-8"
    )
    target = tmp_path / "AGENTS.md"
    events: list[str] = []

    reconcile_agents_md(target, dry_run=True, log_event_fn=events.append)

    assert not target.exists()
    assert any(
        e.startswith("[AGENTS-SCAN]") and "PR-100.md" in e for e in events
    )
