"""Unit tests for tests/e2e/lib/testbed_reset.py.

The module under test lives at tests/e2e/lib/testbed_reset.py. Both
tests/e2e/__init__.py and tests/e2e/lib/__init__.py exist so the module
is importable as ``tests.e2e.lib.testbed_reset``. These unit tests live
under tests/ root (not tests/e2e/) so they are collected by the default
pytest invocation; tests/e2e/ is excluded by ``norecursedirs``.

All subprocess.run calls are mocked; no real GitHub API or git calls happen.
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.e2e.lib import testbed_reset
from tests.e2e.lib.testbed_reset import (
    TESTBED_REPO,
    close_all_open_prs,
    delete_non_main_branches,
    reset_testbed_full,
    wipe_tasks_dir_on_main,
)


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def test_close_all_open_prs_closes_each_listed_number() -> None:
    listing = _completed(stdout="101\n102\n103\n")
    close_ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, close_ok, close_ok, close_ok]
        assert close_all_open_prs() == 3
    calls = run.call_args_list
    assert calls[0].args[0][:3] == ["gh", "pr", "list"]
    assert "-R" in calls[0].args[0] and TESTBED_REPO in calls[0].args[0]
    # An explicit --limit prevents gh's default of 30 from silently truncating
    # the listing on a busy testbed.
    list_cmd = calls[0].args[0]
    assert "--limit" in list_cmd
    limit_value = list_cmd[list_cmd.index("--limit") + 1]
    assert int(limit_value) >= 1000
    for i, n in enumerate(("101", "102", "103"), start=1):
        cmd = calls[i].args[0]
        assert cmd[:3] == ["gh", "pr", "close"]
        assert cmd[3] == n
        assert "--delete-branch" in cmd
        assert calls[i].kwargs.get("timeout") == 30


def test_close_all_open_prs_skips_blank_lines_and_failures() -> None:
    listing = _completed(stdout="\n101\n\n102\n")
    fail = _completed(returncode=1, stderr="boom")
    ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, fail, ok]
        assert close_all_open_prs() == 1


def test_close_all_open_prs_empty_listing() -> None:
    listing = _completed(stdout="")
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing]
        assert close_all_open_prs() == 0


def test_delete_non_main_branches_filters_main_and_blank() -> None:
    listing = _completed(stdout="main\n\nfeature-a\nfeature-b\n")
    ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, ok, ok]
        assert delete_non_main_branches() == 2
    calls = run.call_args_list
    # The branch listing must use --paginate so gh fetches every page rather
    # than the default 30 results.
    assert calls[0].args[0][:2] == ["gh", "api"]
    assert "--paginate" in calls[0].args[0]
    delete_targets = [c.args[0][-1] for c in calls[1:]]
    assert delete_targets == [
        f"repos/{TESTBED_REPO}/git/refs/heads/feature-a",
        f"repos/{TESTBED_REPO}/git/refs/heads/feature-b",
    ]
    for c in calls[1:]:
        assert "-X" in c.args[0] and "DELETE" in c.args[0]
        assert c.kwargs.get("timeout") == 30


def test_delete_non_main_branches_counts_only_successful_deletes() -> None:
    listing = _completed(stdout="alpha\nbeta\n")
    fail = _completed(returncode=1)
    ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, fail, ok]
        assert delete_non_main_branches() == 1


def test_wipe_tasks_dir_returns_false_when_clone_fails() -> None:
    clone_fail = _completed(returncode=128, stderr="auth required")
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [clone_fail]
        assert wipe_tasks_dir_on_main() is False


def test_wipe_tasks_dir_returns_false_when_tasks_missing(tmp_path) -> None:
    clone_ok = _completed(returncode=0)
    cloned_repo = tmp_path / "repo-clone"
    cloned_repo.mkdir()  # mimic git clone result; no tasks/ subdir present

    with (
        patch.object(testbed_reset.tempfile, "mkdtemp", return_value=str(tmp_path)),
        patch.object(testbed_reset.subprocess, "run") as run,
    ):
        # Only the clone call should occur before the early return.
        def fake_run(cmd, *a, **kw):
            if cmd[:2] == ["git", "clone"]:
                # The function expects the clone to land at workdir/"repo".
                target = cmd[-1]
                from pathlib import Path as _P

                _P(target).mkdir(parents=True, exist_ok=True)
                return clone_ok
            raise AssertionError(f"unexpected subprocess call: {cmd}")

        run.side_effect = fake_run
        assert wipe_tasks_dir_on_main() is False


def test_wipe_tasks_dir_returns_false_when_commit_fails(tmp_path) -> None:
    clone_ok = _completed(returncode=0)
    config_ok = _completed(returncode=0)
    rm_ok = _completed(returncode=0)
    commit_fail = _completed(returncode=1, stderr="nothing to commit")

    with (
        patch.object(testbed_reset.tempfile, "mkdtemp", return_value=str(tmp_path)),
        patch.object(testbed_reset.subprocess, "run") as run,
    ):
        seq = iter([clone_ok, config_ok, config_ok, rm_ok, commit_fail])

        def fake_run(cmd, *a, **kw):
            from pathlib import Path as _P

            if cmd[:2] == ["git", "clone"]:
                target = cmd[-1]
                _P(target).mkdir(parents=True, exist_ok=True)
                (_P(target) / "tasks").mkdir(parents=True, exist_ok=True)
            return next(seq)

        run.side_effect = fake_run
        assert wipe_tasks_dir_on_main() is False


def test_wipe_tasks_dir_returns_true_after_successful_push(tmp_path) -> None:
    clone_ok = _completed(returncode=0)
    config_ok = _completed(returncode=0)
    rm_ok = _completed(returncode=0)
    commit_ok = _completed(returncode=0)
    push_ok = _completed(returncode=0)

    with (
        patch.object(testbed_reset.tempfile, "mkdtemp", return_value=str(tmp_path)),
        patch.object(testbed_reset.subprocess, "run") as run,
    ):
        seq = iter([clone_ok, config_ok, config_ok, rm_ok, commit_ok, push_ok])

        def fake_run(cmd, *a, **kw):
            from pathlib import Path as _P

            if cmd[:2] == ["git", "clone"]:
                target = cmd[-1]
                _P(target).mkdir(parents=True, exist_ok=True)
                (_P(target) / "tasks").mkdir(parents=True, exist_ok=True)
            return next(seq)

        run.side_effect = fake_run
        assert wipe_tasks_dir_on_main() is True


def test_wipe_tasks_dir_returns_false_when_push_fails(tmp_path) -> None:
    clone_ok = _completed(returncode=0)
    config_ok = _completed(returncode=0)
    rm_ok = _completed(returncode=0)
    commit_ok = _completed(returncode=0)
    push_fail = _completed(returncode=1, stderr="rejected")

    with (
        patch.object(testbed_reset.tempfile, "mkdtemp", return_value=str(tmp_path)),
        patch.object(testbed_reset.subprocess, "run") as run,
    ):
        seq = iter([clone_ok, config_ok, config_ok, rm_ok, commit_ok, push_fail])

        def fake_run(cmd, *a, **kw):
            from pathlib import Path as _P

            if cmd[:2] == ["git", "clone"]:
                target = cmd[-1]
                _P(target).mkdir(parents=True, exist_ok=True)
                (_P(target) / "tasks").mkdir(parents=True, exist_ok=True)
            return next(seq)

        run.side_effect = fake_run
        assert wipe_tasks_dir_on_main() is False


def test_wipe_tasks_dir_cleans_workdir_even_on_exception(tmp_path) -> None:
    rmtree_mock = MagicMock()
    with (
        patch.object(testbed_reset.tempfile, "mkdtemp", return_value=str(tmp_path)),
        patch.object(testbed_reset.shutil, "rmtree", rmtree_mock),
        patch.object(testbed_reset.subprocess, "run", side_effect=RuntimeError("explode")),
    ):
        with pytest.raises(RuntimeError):
            wipe_tasks_dir_on_main()
    rmtree_mock.assert_called_once()
    assert Path(rmtree_mock.call_args.args[0]) == tmp_path
    assert rmtree_mock.call_args.kwargs.get("ignore_errors") is True


def test_reset_testbed_full_aggregates_all_helpers() -> None:
    with (
        patch.object(testbed_reset, "close_all_open_prs", return_value=2) as close,
        patch.object(testbed_reset, "delete_non_main_branches", return_value=3) as delete,
        patch.object(testbed_reset, "wipe_tasks_dir_on_main", return_value=True) as wipe,
    ):
        result = reset_testbed_full()
    assert result == {"prs_closed": 2, "branches_deleted": 3, "tasks_wiped": True}
    close.assert_called_once_with()
    delete.assert_called_once_with()
    wipe.assert_called_once_with()
