"""Unit tests for ``src/branch_context.py``."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace
from typing import Any

import pytest

from src import branch_context as bc
from src.branch_context import BranchContext
from src.models import PRInfo


def _runner_stub(
    *,
    base_branch: str = "main",
    task_branch: str | None = "pr-foo",
    pr_branch: str | None = None,
    repo_path: str = "/tmp/does-not-exist",
) -> SimpleNamespace:
    current_task = (
        SimpleNamespace(branch=task_branch) if task_branch is not None else None
    )
    current_pr = (
        PRInfo(number=1, branch=pr_branch)
        if pr_branch is not None
        else None
    )
    return SimpleNamespace(
        repo_config=SimpleNamespace(branch=base_branch),
        state=SimpleNamespace(
            current_task=current_task,
            current_pr=current_pr,
        ),
        repo_path=repo_path,
    )


def _patch_git(
    monkeypatch: pytest.MonkeyPatch,
    *,
    current_branch: str | None = None,
    local_exists: bool = False,
    remote_exists: bool = False,
) -> None:
    """Stub ``subprocess.run`` so the three branch probes are deterministic."""

    def fake_run(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            stdout = f"{current_branch}\n" if current_branch else ""
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout=stdout, stderr=""
            )
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            ref = cmd[4] if len(cmd) > 4 else ""
            if ref.startswith("refs/heads/"):
                rc = 0 if local_exists else 1
            elif ref.startswith("refs/remotes/origin/"):
                rc = 0 if remote_exists else 1
            else:
                rc = 1
            return subprocess.CompletedProcess(
                args=cmd, returncode=rc, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(bc.subprocess, "run", fake_run)


def test_from_runner_populates_all_six_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``from_runner`` reads base, task, current-git, PR head, and both
    existence flags into a fully populated dataclass."""
    runner = _runner_stub(
        base_branch="main", task_branch="pr-foo", pr_branch="pr-foo"
    )
    _patch_git(
        monkeypatch,
        current_branch="pr-foo",
        local_exists=True,
        remote_exists=True,
    )

    ctx = BranchContext.from_runner(runner)

    assert ctx.base_branch == "main"
    assert ctx.task_branch == "pr-foo"
    assert ctx.current_git_branch == "pr-foo"
    assert ctx.pr_head_branch == "pr-foo"
    assert ctx.local_branch_exists is True
    assert ctx.remote_branch_exists is True


def test_mismatch_reason_none_when_branches_agree(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_stub(task_branch="pr-foo", pr_branch="pr-foo")
    _patch_git(monkeypatch, current_branch="pr-foo")

    ctx = BranchContext.from_runner(runner)

    assert ctx.mismatch_reason is None


def test_mismatch_reason_flags_task_vs_current_git(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_stub(task_branch="pr-foo", pr_branch=None)
    _patch_git(monkeypatch, current_branch="pr-bar")

    ctx = BranchContext.from_runner(runner)

    reason = ctx.mismatch_reason
    assert reason is not None
    assert "task_branch=pr-foo" in reason
    assert "current_git_branch=pr-bar" in reason


def test_mismatch_reason_flags_task_vs_pr_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``current_git_branch`` is unknown but the task/PR pair diverges
    explicitly — the mismatch must still be reported."""
    runner = _runner_stub(task_branch="pr-foo", pr_branch="pr-foo-2")
    _patch_git(monkeypatch, current_branch=None)

    ctx = BranchContext.from_runner(runner)

    reason = ctx.mismatch_reason
    assert reason is not None
    assert "task_branch=pr-foo" in reason
    assert "pr_head_branch=pr-foo-2" in reason


def test_mismatch_reason_none_when_only_one_branch_known(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Conservative detection: missing surfaces are not divergence."""
    runner = _runner_stub(task_branch="pr-foo", pr_branch=None)
    _patch_git(monkeypatch, current_branch=None)

    ctx = BranchContext.from_runner(runner)

    assert ctx.mismatch_reason is None


def test_log_summary_labels_all_four_surfaces(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_stub(
        base_branch="main", task_branch="pr-foo", pr_branch="pr-foo"
    )
    _patch_git(monkeypatch, current_branch="pr-foo")

    summary = BranchContext.from_runner(runner).log_summary()

    assert "\n" not in summary
    assert "base_branch=main" in summary
    assert "task_branch=pr-foo" in summary
    assert "current_git_branch=pr-foo" in summary
    assert "pr_head_branch=pr-foo" in summary


def test_log_summary_marks_unknown_surfaces_as_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_stub(
        base_branch="main", task_branch=None, pr_branch=None
    )
    _patch_git(monkeypatch, current_branch=None)

    summary = BranchContext.from_runner(runner).log_summary()

    assert "task_branch=<absent>" in summary
    assert "current_git_branch=<absent>" in summary
    assert "pr_head_branch=<absent>" in summary


def test_from_runner_handles_missing_repo_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing ``repo_path`` must not crash ``from_runner``: subprocess
    failures collapse to ``current_git_branch=None`` and existence flags
    of ``False`` so callers can still use the dataclass for diagnostics."""

    def raise_filenotfound(*args: Any, **kwargs: Any) -> None:
        raise FileNotFoundError("no such directory")

    monkeypatch.setattr(bc.subprocess, "run", raise_filenotfound)
    runner = _runner_stub(task_branch="pr-foo", pr_branch=None)

    ctx = BranchContext.from_runner(runner)

    assert ctx.current_git_branch is None
    assert ctx.local_branch_exists is False
    assert ctx.remote_branch_exists is False


def test_from_runner_uses_default_main_when_repo_config_branch_is_falsy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner_stub(base_branch="", task_branch=None, pr_branch=None)
    _patch_git(monkeypatch, current_branch=None)

    ctx = BranchContext.from_runner(runner)

    assert ctx.base_branch == "main"


def test_check_remote_branch_exists_returns_false_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_timeout(*args: Any, **kwargs: Any) -> None:
        raise subprocess.TimeoutExpired(cmd=args[0] if args else [], timeout=5)

    monkeypatch.setattr(bc.subprocess, "run", raise_timeout)

    assert bc._check_remote_branch_exists("/tmp", "pr-foo") is False


def test_check_local_branch_exists_returns_false_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def raise_timeout(*args: Any, **kwargs: Any) -> None:
        raise subprocess.TimeoutExpired(cmd=args[0] if args else [], timeout=5)

    monkeypatch.setattr(bc.subprocess, "run", raise_timeout)

    assert bc._check_local_branch_exists("/tmp", "pr-foo") is False


def test_read_current_git_branch_returns_none_on_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0] if args else [], returncode=128, stdout="", stderr=""
        )

    monkeypatch.setattr(bc.subprocess, "run", fake_run)

    assert bc._read_current_git_branch("/tmp") is None


def test_read_current_git_branch_returns_none_on_empty_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0] if args else [], returncode=0, stdout="\n", stderr=""
        )

    monkeypatch.setattr(bc.subprocess, "run", fake_run)

    assert bc._read_current_git_branch("/tmp") is None
