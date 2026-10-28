"""Unit tests for tests/e2e/lib/testbed_reset.py.

The module under test lives at tests/e2e/lib/testbed_reset.py. Both
tests/e2e/__init__.py and tests/e2e/lib/__init__.py exist so the module
is importable as ``tests.e2e.lib.testbed_reset``. These unit tests live
under tests/ root (not tests/e2e/) so they are collected by the default
pytest invocation; tests/e2e/ is excluded by ``norecursedirs``.

All subprocess.run calls are mocked; no real GitHub API or git calls happen.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tests.e2e.lib import testbed_reset
from tests.e2e.lib.testbed_reset import (
    TESTBED_REPO,
    TESTBED_URL,
    _clone_url,
    _gh_env,
    _git_env,
    clear_testbed_redis_state,
    close_all_open_prs,
    delete_non_main_branches,
    reset_testbed_full,
    wipe_tasks_dir_on_main,
)


def _completed(returncode: int = 0, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


def test_clear_testbed_redis_state_runs_keys_then_del(monkeypatch) -> None:
    calls: list[tuple[tuple[str, ...], dict]] = []

    def fake_run(cmd, **kwargs):
        calls.append((tuple(cmd), kwargs))
        if len(calls) == 1:
            return _completed(stdout="\n control:slug:a \n\ncontrol:slug:b\n")
        return _completed(stdout="4\n")

    monkeypatch.setattr(testbed_reset.subprocess, "run", fake_run)

    assert clear_testbed_redis_state("slug") == 4

    base_cmd = (
        "docker",
        "compose",
        "-f",
        "docker-compose.test.yml",
        "exec",
        "-T",
        "redis-test",
        "redis-cli",
    )
    assert calls == [
        (
            (*base_cmd, "KEYS", "control:slug:*"),
            {"capture_output": True, "text": True, "check": False, "timeout": 10},
        ),
        (
            (
                *base_cmd,
                "DEL",
                "pipeline:slug",
                "upload:slug:pending",
                "error_rate:slug",
                "error_rate_last_auto_pause:slug",
                "control:slug:a",
                "control:slug:b",
            ),
            {"capture_output": True, "text": True, "check": False, "timeout": 10},
        ),
    ]


def test_clear_testbed_redis_state_raises_when_keys_fails(monkeypatch) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_run(cmd, **kwargs):
        calls.append(tuple(cmd))
        return _completed(returncode=1, stderr="no redis")

    monkeypatch.setattr(testbed_reset.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="redis KEYS failed"):
        clear_testbed_redis_state("slug")
    assert len(calls) == 1


def test_clear_testbed_redis_state_raises_when_del_fails(monkeypatch) -> None:
    def fake_run(cmd, **kwargs):
        if "KEYS" in cmd:
            return _completed(stdout="control:slug:a\n")
        return _completed(returncode=1, stderr="del failed")

    monkeypatch.setattr(testbed_reset.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="redis DEL failed"):
        clear_testbed_redis_state("slug")


def test_clear_testbed_redis_state_raises_on_subprocess_exception(monkeypatch) -> None:
    def fake_run(cmd, **kwargs):
        raise subprocess.TimeoutExpired(cmd=cmd, timeout=10)

    monkeypatch.setattr(testbed_reset.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="redis KEYS failed"):
        clear_testbed_redis_state("slug")


def test_clear_testbed_redis_state_raises_on_unparseable_del_stdout(monkeypatch) -> None:
    def fake_run(cmd, **kwargs):
        if "KEYS" in cmd:
            return _completed(stdout="")
        return _completed(stdout="not-an-int\n")

    monkeypatch.setattr(testbed_reset.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="non-integer output"):
        clear_testbed_redis_state("slug")


def test_close_all_open_prs_closes_each_listed_number() -> None:
    listing = _completed(stdout="101\tbranch-a\n102\tbranch-b\n103\tmain\n")
    close_ok = _completed(returncode=0)
    delete_ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, close_ok, delete_ok, close_ok, delete_ok, close_ok]
        assert close_all_open_prs() == 3
    calls = run.call_args_list
    assert calls[0].args[0][:3] == ["gh", "api", "--paginate"]
    list_cmd = calls[0].args[0]
    assert f"repos/{TESTBED_REPO}/pulls?state=open&per_page=100" in list_cmd
    close_calls = [c for c in calls[1:] if "PATCH" in c.args[0]]
    delete_calls = [c for c in calls[1:] if "DELETE" in c.args[0]]
    for call, n in zip(close_calls, ("101", "102", "103"), strict=True):
        cmd = call.args[0]
        assert cmd[:4] == ["gh", "api", "-X", "PATCH"]
        assert cmd[4] == f"repos/{TESTBED_REPO}/pulls/{n}"
        assert call.kwargs.get("timeout") == 30
    assert [c.args[0][-1] for c in delete_calls] == [
        f"repos/{TESTBED_REPO}/git/refs/heads/branch-a",
        f"repos/{TESTBED_REPO}/git/refs/heads/branch-b",
    ]
    for call in delete_calls:
        assert call.kwargs.get("timeout") == 30


def test_close_all_open_prs_url_encodes_deleted_branch_name() -> None:
    listing = _completed(stdout="101\tfeature/x\n")
    ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, ok, ok]
        assert close_all_open_prs() == 1
    delete_cmd = run.call_args_list[2].args[0]
    assert delete_cmd[-1] == f"repos/{TESTBED_REPO}/git/refs/heads/feature%2Fx"


def test_close_all_open_prs_skips_blank_lines_and_failures() -> None:
    listing = _completed(stdout="\n101\tbranch-a\n\n102\tbranch-b\n")
    fail = _completed(returncode=1, stderr="boom")
    ok = _completed(returncode=0)
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing, fail, ok, ok, ok]
        assert close_all_open_prs() == 1


def test_close_all_open_prs_empty_listing() -> None:
    listing = _completed(stdout="")
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing]
        assert close_all_open_prs() == 0


def test_close_all_open_prs_raises_when_listing_fails() -> None:
    # Surfacing the listing failure is what stops the session from silently
    # running e2e tests against a polluted testbed (Codex P2 on PR-157).
    listing_fail = _completed(returncode=1, stderr="auth required")
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing_fail]
        with pytest.raises(RuntimeError, match="gh api listing failed"):
            close_all_open_prs()


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


def test_delete_non_main_branches_raises_when_listing_fails() -> None:
    # Same rationale as the close_all_open_prs listing-failure case: a silent
    # zero would mask gh API/auth regressions and let stale branches survive.
    listing_fail = _completed(returncode=1, stderr="HTTP 401")
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing_fail]
        with pytest.raises(RuntimeError, match="gh api listing failed"):
            delete_non_main_branches()


def test_wipe_tasks_dir_raises_when_clone_fails() -> None:
    clone_fail = _completed(returncode=128, stderr="auth required")
    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [clone_fail]
        with pytest.raises(RuntimeError, match="git clone failed"):
            wipe_tasks_dir_on_main()


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


def test_wipe_tasks_dir_raises_when_commit_fails(tmp_path) -> None:
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
        with pytest.raises(RuntimeError, match="git commit failed"):
            wipe_tasks_dir_on_main()


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


def test_wipe_tasks_dir_raises_when_push_fails(tmp_path) -> None:
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
        with pytest.raises(RuntimeError, match="git push failed"):
            wipe_tasks_dir_on_main()


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


def test_clone_url_returns_plain_url_without_token(monkeypatch) -> None:
    monkeypatch.delenv("GH_TOKEN", raising=False)
    assert _clone_url() == TESTBED_URL


def test_clone_url_embeds_gh_token_when_set(monkeypatch) -> None:
    monkeypatch.setenv("GH_TOKEN", "  ghs_secret  ")
    assert _clone_url() == (
        f"https://x-access-token:ghs_secret@github.com/{TESTBED_REPO}.git"
    )


def test_gh_env_returns_none_without_token(monkeypatch) -> None:
    monkeypatch.delenv("GH_TOKEN", raising=False)

    assert _gh_env() is None


def test_gh_env_pins_both_cli_token_names(monkeypatch) -> None:
    monkeypatch.setenv("GH_TOKEN", "  ghs_testbed  ")
    monkeypatch.setenv("GITHUB_TOKEN", "stale-default-token")

    env = _gh_env()

    assert env is not None
    assert env["GH_TOKEN"] == "ghs_testbed"
    assert env["GITHUB_TOKEN"] == "ghs_testbed"


def test_git_env_clears_checkout_extraheader() -> None:
    env = _git_env()

    assert env["GIT_CONFIG_COUNT"] == "1"
    assert env["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
    assert env["GIT_CONFIG_VALUE_0"] == ""


def test_close_all_open_prs_passes_pinned_gh_env(monkeypatch) -> None:
    monkeypatch.setenv("GH_TOKEN", "ghs_testbed")
    listing = _completed(stdout="")

    with patch.object(testbed_reset.subprocess, "run") as run:
        run.side_effect = [listing]
        assert close_all_open_prs() == 0

    assert run.call_args.kwargs["env"]["GH_TOKEN"] == "ghs_testbed"
    assert run.call_args.kwargs["env"]["GITHUB_TOKEN"] == "ghs_testbed"


def test_wipe_tasks_dir_uses_authenticated_clone_url_when_token_set(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("GH_TOKEN", "ghs_xyz")
    clone_ok = _completed(returncode=0)
    config_ok = _completed(returncode=0)
    rm_ok = _completed(returncode=0)
    commit_ok = _completed(returncode=0)
    push_ok = _completed(returncode=0)

    captured: dict = {}

    with (
        patch.object(testbed_reset.tempfile, "mkdtemp", return_value=str(tmp_path)),
        patch.object(testbed_reset.subprocess, "run") as run,
    ):
        seq = iter([clone_ok, config_ok, config_ok, rm_ok, commit_ok, push_ok])

        def fake_run(cmd, *a, **kw):
            from pathlib import Path as _P

            if cmd[:2] == ["git", "clone"]:
                captured["clone_cmd"] = list(cmd)
                captured["clone_env"] = kw.get("env")
                target = cmd[-1]
                _P(target).mkdir(parents=True, exist_ok=True)
                (_P(target) / "tasks").mkdir(parents=True, exist_ok=True)
            if cmd[:4] == ["git", "-C", str(tmp_path / "repo"), "push"]:
                captured["push_env"] = kw.get("env")
            return next(seq)

        run.side_effect = fake_run
        assert wipe_tasks_dir_on_main() is True

    clone_cmd = captured["clone_cmd"]
    assert clone_cmd[:4] == ["git", "clone", "--depth", "1"]
    assert clone_cmd[4] == (
        f"https://x-access-token:ghs_xyz@github.com/{TESTBED_REPO}.git"
    )
    assert captured["clone_env"]["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
    assert captured["push_env"]["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"


def test_reset_testbed_full_aggregates_all_helpers() -> None:
    with (
        patch.object(testbed_reset, "close_all_open_prs", return_value=2) as close,
        patch.object(testbed_reset, "delete_non_main_branches", return_value=3) as delete,
        patch.object(testbed_reset, "wipe_tasks_dir_on_main", return_value=True) as wipe,
    ):
        result = reset_testbed_full("slug")
    assert result == {"prs_closed": 2, "branches_deleted": 3, "tasks_wiped": True}
    close.assert_called_once_with()
    delete.assert_called_once_with()
    wipe.assert_called_once_with()


def test_reset_testbed_session_resets_repo_and_redis_once() -> None:
    module_name = "tests.e2e.conftest"
    calls: list[str] = []

    def reset_full(slug: str) -> dict:
        calls.append(f"reset:{slug}")
        return {"prs_closed": 0}

    def clear_redis(slug: str) -> int:
        calls.append(f"clear:{slug}")
        return 2

    def restart(cmd, **kwargs):
        calls.append(cmd[-2])
        assert cmd == ["docker", "compose", "-f", "docker-compose.test.yml", cmd[-2], "daemon-test"]
        assert cmd[-2] in {"stop", "start"}
        assert kwargs["check"] is True
        return _completed()

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)

        with (
            patch.object(
                e2e_conftest, "_stop_daemon_and_wait_paused",
                side_effect=lambda slug: calls.append(f"stop:{slug}"),
            ),
            patch.object(e2e_conftest, "reset_testbed_full", side_effect=reset_full),
            patch.object(e2e_conftest, "clear_testbed_redis_state", side_effect=clear_redis),
            patch.object(e2e_conftest.subprocess, "run", side_effect=restart),
        ):
            fixture = e2e_conftest._reset_testbed_session.__wrapped__()
            assert next(fixture) == {"prs_closed": 0, "redis_keys_deleted": 2}
            with pytest.raises(StopIteration):
                next(fixture)
    finally:
        sys.modules.pop(module_name, None)

    assert calls == [
        f"stop:{e2e_conftest.TESTBED_SLUG}",
        "stop",
        f"reset:{e2e_conftest.TESTBED_SLUG}",
        f"clear:{e2e_conftest.TESTBED_SLUG}",
        "start",
    ]


def test_reset_testbed_fixture_resets_before_and_clears_after() -> None:
    module_name = "tests.e2e.conftest"
    calls: list[str] = []

    def reset_full(slug: str) -> dict:
        calls.append(f"reset:{slug}")
        return {}

    def clear_redis(slug: str) -> int:
        calls.append(f"clear:{slug}")
        return 0

    def stop_daemon(slug: str) -> None:
        calls.append(f"stop:{slug}")

    def resume_daemon(slug: str) -> None:
        calls.append(f"resume:{slug}")

    def service_command(cmd, **kwargs):
        assert cmd[:4] == ["docker", "compose", "-f", "docker-compose.test.yml"]
        assert cmd[-1] == "daemon-test"
        assert kwargs["check"] is True
        calls.append(f"service:{cmd[-2]}")
        return _completed()

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)

        with (
            patch.object(e2e_conftest, "_stop_daemon_and_wait_paused", side_effect=stop_daemon),
            patch.object(e2e_conftest, "_resume_daemon", side_effect=resume_daemon),
            patch.object(e2e_conftest, "reset_testbed_full", side_effect=reset_full),
            patch.object(e2e_conftest, "clear_testbed_redis_state", side_effect=clear_redis),
            patch.object(e2e_conftest.subprocess, "run", side_effect=service_command),
            patch.object(e2e_conftest, "_wait_for_fresh_idle",
                         side_effect=lambda slug, **kwargs: calls.append(f"ready:{slug}")),
        ):
            fixture = e2e_conftest.reset_testbed.__wrapped__()
            next(fixture)
            with pytest.raises(StopIteration):
                next(fixture)
    finally:
        sys.modules.pop(module_name, None)

    assert calls == [
        f"stop:{e2e_conftest.TESTBED_SLUG}",
        "service:stop",
        f"reset:{e2e_conftest.TESTBED_SLUG}",
        f"clear:{e2e_conftest.TESTBED_SLUG}",
        "service:start",
        f"resume:{e2e_conftest.TESTBED_SLUG}",
        f"ready:{e2e_conftest.TESTBED_SLUG}",
        f"clear:{e2e_conftest.TESTBED_SLUG}",
    ]


def test_reset_testbed_fixture_resumes_when_stop_wait_fails() -> None:
    module_name = "tests.e2e.conftest"
    calls: list[str] = []

    def stop_daemon(slug: str) -> None:
        calls.append(f"stop:{slug}")
        raise RuntimeError("stop timed out")

    def resume_daemon(slug: str) -> None:
        calls.append(f"resume:{slug}")
        raise RuntimeError("resume failed")

    def unexpected_call(slug: str):
        calls.append(f"unexpected:{slug}")

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)

        with (
            patch.object(e2e_conftest, "_stop_daemon_and_wait_paused", side_effect=stop_daemon),
            patch.object(e2e_conftest, "_resume_daemon", side_effect=resume_daemon),
            patch.object(e2e_conftest, "reset_testbed_full", side_effect=unexpected_call),
            patch.object(e2e_conftest, "clear_testbed_redis_state", side_effect=unexpected_call),
        ):
            fixture = e2e_conftest.reset_testbed.__wrapped__()
            with pytest.raises(RuntimeError, match="stop timed out") as excinfo:
                next(fixture)
    finally:
        sys.modules.pop(module_name, None)

    assert calls == [
        f"stop:{e2e_conftest.TESTBED_SLUG}",
        f"resume:{e2e_conftest.TESTBED_SLUG}",
    ]
    assert excinfo.value.__notes__ == ["resume failed after reset setup error: resume failed"]


@pytest.mark.parametrize("failure", ["stop", "reset", "clear", "start", "ready"])
def test_reset_fixture_does_not_run_with_partial_reset(failure: str) -> None:
    module_name = "tests.e2e.conftest"
    calls = []

    def stage(name):
        calls.append(name)
        if name == failure:
            raise RuntimeError(f"{name} unavailable")

    def service_command(cmd, **kwargs):
        stage(cmd[-2])
        return _completed()

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            module = importlib.import_module(module_name)
        with (
            patch.object(module, "_stop_daemon_and_wait_paused", side_effect=lambda _: stage("paused")),
            patch.object(module.subprocess, "run", side_effect=service_command),
            patch.object(module, "reset_testbed_full", side_effect=lambda _: stage("reset")),
            patch.object(module, "clear_testbed_redis_state", side_effect=lambda _: stage("clear")),
            patch.object(module, "_resume_daemon", side_effect=lambda _: stage("resume")),
            patch.object(module, "_wait_for_fresh_idle", side_effect=lambda *a, **kw: stage("ready")),
        ):
            fixture = module.reset_testbed.__wrapped__()
            with pytest.raises(RuntimeError, match=f"{failure} unavailable"):
                next(fixture)
    finally:
        sys.modules.pop(module_name, None)
    expected = {
        "stop": ["paused", "stop", "resume"],
        "reset": ["paused", "stop", "reset", "start", "resume"],
        "clear": ["paused", "stop", "reset", "clear", "start", "resume"],
        "start": ["paused", "stop", "reset", "clear", "start"],
        "ready": ["paused", "stop", "reset", "clear", "start", "resume", "ready"],
    }
    assert calls == expected[failure]


def test_fresh_idle_wait_rejects_control_snapshot_and_unready_runner() -> None:
    module_name = "tests.e2e.conftest"
    after = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fresh = {"name": "slug", "state": "IDLE", "user_paused": False,
             "current_task": None, "current_pr": None,
             "last_updated": (after + timedelta(seconds=1)).isoformat()}
    snapshots = [
        {**fresh, "last_updated": after.isoformat()},
        {**fresh, "last_updated": (after - timedelta(seconds=1)).isoformat()},
        {**fresh, "state": "CODING"},
        {**fresh, "user_paused": True},
        {**fresh, "current_task": {"pr_id": "PR-001"}},
        {**fresh, "current_pr": {"number": 1}},
        {**fresh, "last_updated": "invalid"},
        {**fresh, "last_updated": "2026-01-01T00:00:01"},
        {**fresh, "last_updated": None},
        fresh,
    ]
    class Response:
        def __init__(self, row):
            self.row = row
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def read(self, *args):
            import json
            return json.dumps({"states": [self.row]}).encode()
    responses = [Response(row) for row in snapshots]
    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            module = importlib.import_module(module_name)
        with (
            patch.object(module.urllib.request, "urlopen", side_effect=responses) as read,
            patch.object(module.time, "sleep") as sleep,
        ):
            module._wait_for_fresh_idle("slug", after=after, timeout_sec=1)
        assert read.call_count == len(snapshots)
        assert sleep.call_count == len(snapshots) - 1
    finally:
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize("publication_delay", [65, 135])
def test_fresh_idle_wait_allows_slow_scan_but_requires_new_publication(publication_delay) -> None:
    module_name = "tests.e2e.conftest"
    after = datetime(2026, 1, 1, tzinfo=timezone.utc)
    elapsed = 0
    observations = []

    def advance(seconds):
        nonlocal elapsed
        elapsed += seconds

    def response(*args, **kwargs):
        observations.append(elapsed)
        # A valid-looking IDLE stub must remain insufficient throughout
        # the slow scan; only the runner's new publication permits return.
        updated = after + timedelta(seconds=1) if elapsed >= publication_delay else after
        payload = {"states": [{
            "name": "slug", "state": "IDLE", "user_paused": False,
            "current_task": None, "current_pr": None,
            "last_updated": updated.isoformat(),
        }]}
        import json
        result = MagicMock()
        result.__enter__.return_value.read.return_value = json.dumps(payload).encode()
        return result

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            module = importlib.import_module(module_name)
        with (
            patch.object(module.urllib.request, "urlopen", side_effect=response),
            patch.object(module.time, "monotonic", side_effect=lambda: elapsed),
            patch.object(module.time, "sleep", side_effect=advance),
        ):
            module._wait_for_fresh_idle("slug", after=after)
        assert observations[0] == 0
        assert observations[-1] == publication_delay
    finally:
        sys.modules.pop(module_name, None)


def test_fresh_idle_wait_times_out_on_unavailable_daemon() -> None:
    module_name = "tests.e2e.conftest"
    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            module = importlib.import_module(module_name)
        with (
            patch.object(module.urllib.request, "urlopen", side_effect=module.urllib.error.URLError("not ready")),
            patch.object(module.time, "monotonic", side_effect=[0, 0, 2]),
            patch.object(module.time, "sleep"),
        ):
            with pytest.raises(RuntimeError, match="fresh IDLE"):
                module._wait_for_fresh_idle("slug", after=datetime.now(timezone.utc), timeout_sec=1)
    finally:
        sys.modules.pop(module_name, None)


def test_shared_config_fixture_resets_testbed_after_dispatch() -> None:
    module_name = "tests.e2e.conftest"
    calls: list[str] = []
    baseline = b"original config"

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)

        with (
            patch.object(e2e_conftest, "_shared_config_bytes", return_value=baseline),
            patch.object(
                e2e_conftest,
                "_config_digest",
                return_value=e2e_conftest.hashlib.sha256(baseline).hexdigest(),
            ),
            patch.object(
                e2e_conftest,
                "_stop_daemon_and_wait_paused",
                side_effect=lambda slug: calls.append(f"stop:{slug}"),
            ),
            patch.object(e2e_conftest, "reset_testbed_full", side_effect=lambda slug: calls.append(f"reset:{slug}")),
            patch.object(
                e2e_conftest,
                "clear_testbed_redis_state",
                side_effect=lambda slug: calls.append(f"clear:{slug}"),
            ),
            patch.object(e2e_conftest.subprocess, "run"),
            patch.object(e2e_conftest, "_resume_daemon", side_effect=lambda slug: calls.append(f"resume:{slug}")),
        ):
            fixture = e2e_conftest.shared_test_config.__wrapped__(None, lambda: None)
            next(fixture)
            with pytest.raises(StopIteration):
                next(fixture)
    finally:
        sys.modules.pop(module_name, None)

    assert calls == [
        f"{action}:{e2e_conftest.TESTBED_SLUG}"
        for action in ("stop", "reset", "clear", "resume")
    ]


def test_shared_config_fixture_waits_for_runner_to_apply_restored_config(tmp_path) -> None:
    module_name = "tests.e2e.conftest"
    calls: list[str] = []
    config_path = tmp_path / "config.test.yml"
    baseline = Path("config.test.yml").read_bytes()
    config_path.write_bytes(baseline)

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)

        with (
            patch.object(e2e_conftest, "TEST_DATA_DIR", tmp_path),
            patch.object(e2e_conftest, "SHARED_CONFIG_PATH", config_path),
            patch.object(e2e_conftest, "_shared_config_bytes", side_effect=config_path.read_bytes),
            patch.object(e2e_conftest, "_stop_daemon_and_wait_paused", side_effect=lambda _: calls.append("stop")),
            patch.object(e2e_conftest, "reset_testbed_full", side_effect=lambda _: calls.append("reset")),
            patch.object(e2e_conftest, "clear_testbed_redis_state", side_effect=lambda _: calls.append("clear")),
            patch.object(e2e_conftest, "_runner_apply_count", side_effect=lambda: 7),
            patch.object(e2e_conftest, "_daemon_reload_count", return_value=3),
            patch.object(e2e_conftest, "_resume_daemon", side_effect=lambda _: calls.append("resume")),
            patch.object(
                e2e_conftest.subprocess, "run",
                side_effect=lambda command, **kwargs: calls.append(command[-2] + ":" + command[-1]),
            ),
            patch.object(
                e2e_conftest,
                "_wait_for_fresh_idle",
                side_effect=lambda *args, **kwargs: calls.append("fresh-idle"),
            ),
        ):
            fixture = e2e_conftest.shared_test_config.__wrapped__(None, lambda: None)
            _, expect_settings_coder_write, note_settings_write, _ = next(fixture)
            from src.config import update_repository

            repo_url = "https://github.com/AlexBomber12/pipeline-orchestrator-testbed.git"
            expect_settings_coder_write(repo_url, "codex")
            update_repository(repo_url, path=str(config_path), coder="codex")
            note_settings_write()
            with pytest.raises(StopIteration):
                next(fixture)
    finally:
        sys.modules.pop(module_name, None)

    assert config_path.read_bytes() == baseline
    assert calls == ["stop", "stop:daemon-test", "reset", "clear", "start:daemon-test", "resume", "fresh-idle"]


@pytest.mark.parametrize("write_completed", [False, True])
def test_shared_config_fixture_restores_after_settings_request_failure(tmp_path, write_completed: bool) -> None:
    module_name = "tests.e2e.conftest"
    config_path = tmp_path / "config.test.yml"
    baseline = Path("config.test.yml").read_bytes()
    config_path.write_bytes(baseline)
    repo_url = "https://github.com/AlexBomber12/pipeline-orchestrator-testbed.git"

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)
        with (
            patch.object(e2e_conftest, "TEST_DATA_DIR", tmp_path),
            patch.object(e2e_conftest, "SHARED_CONFIG_PATH", config_path),
            patch.object(e2e_conftest, "_shared_config_bytes", side_effect=config_path.read_bytes),
            patch.object(e2e_conftest, "_stop_daemon_and_wait_paused"),
            patch.object(e2e_conftest, "reset_testbed_full"),
            patch.object(e2e_conftest, "clear_testbed_redis_state"),
            patch.object(e2e_conftest, "_runner_apply_count", return_value=7),
            patch.object(e2e_conftest, "_daemon_reload_count", return_value=3),
            patch.object(e2e_conftest, "_wait_for_daemon_reload"),
            patch.object(e2e_conftest, "_resume_daemon"),
            patch.object(e2e_conftest.subprocess, "run") as restart,
            patch.object(e2e_conftest, "_wait_for_fresh_idle") as wait_for_idle,
        ):
            fixture = e2e_conftest.shared_test_config.__wrapped__(None, lambda: None)
            _, expect_settings_coder_write, _, _ = next(fixture)
            expect_settings_coder_write(repo_url, "codex")
            if write_completed:
                from src.config import update_repository

                update_repository(repo_url, path=str(config_path), coder="codex")
            with pytest.raises(StopIteration):
                next(fixture)
            assert config_path.read_bytes() == baseline
            assert wait_for_idle.called == write_completed
            assert [call.args[0][-2:] for call in restart.call_args_list] == [
                ["stop", "daemon-test"], ["start", "daemon-test"],
            ]
    finally:
        sys.modules.pop(module_name, None)


def test_wait_for_runner_apply_requires_new_acknowledgment() -> None:
    module_name = "tests.e2e.conftest"
    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)
        with (
            patch.object(e2e_conftest, "_runner_apply_count", side_effect=[7, 7, 8]) as counts,
            patch.object(e2e_conftest.time, "sleep") as sleep,
        ):
            e2e_conftest._wait_for_runner_apply(7)
        assert counts.call_count == 3
        assert sleep.call_count == 2
    finally:
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize("acknowledgment", ["apply", "reload"])
def test_wait_for_runner_config_ack_requires_new_runner_event(acknowledgment: str) -> None:
    module_name = "tests.e2e.conftest"
    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace()}):
            e2e_conftest = importlib.import_module(module_name)
        apply_counts = [7, 8] if acknowledgment == "apply" else [7, 7]
        reload_counts = [3, 4] if acknowledgment == "reload" else [3, 3]
        with (
            patch.object(e2e_conftest, "_runner_apply_count", side_effect=apply_counts),
            patch.object(e2e_conftest, "_runner_reload_count", side_effect=reload_counts),
            patch.object(e2e_conftest.time, "sleep") as sleep,
        ):
            e2e_conftest._wait_for_runner_config_ack(7, 3)
        sleep.assert_called_once_with(0.5)
    finally:
        sys.modules.pop(module_name, None)


@pytest.mark.parametrize("terminal_state", ["WATCH", "ERROR"])
def test_stop_daemon_waits_for_runner_pause_ack(terminal_state) -> None:
    module_name = "tests.e2e.conftest"
    posts: list[tuple[str, int]] = []
    opens: list[tuple[str, int]] = []
    sleeps: list[float] = []

    def post(url: str, timeout: int):
        posts.append((url, timeout))
        return types.SimpleNamespace(status_code=204)

    class _Response:
        def __init__(self, body: bytes) -> None:
            self.body = body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self) -> bytes:
            return self.body

    sys.modules.pop(module_name, None)
    try:
        with patch.dict(sys.modules, {"requests": types.SimpleNamespace(post=post)}):
            e2e_conftest = importlib.import_module(module_name)

        slug = e2e_conftest.TESTBED_SLUG
        pause_event = "held in ERROR" if terminal_state == "ERROR" else "[INFRA] Paused. Press Play to resume."
        responses = iter(
            [
                f'[{{"name": "{slug}", "state": "CODING", "user_paused": true}}]'.encode(),
                f'[{{"name": "{slug}", "state": "ERROR", "user_paused": false}}]'.encode(),
                (
                    f'[{{"name": "{slug}", "state": "IDLE", "user_paused": true, '
                    '"history": [{"event": "Stop requested. Aborting run; working tree may be left dirty."}]'
                    "}]"
                ).encode(),
                (
                    f'[{{"name": "{slug}", "state": "{terminal_state}", "user_paused": true, '
                    f'"history": [{{"event": "{pause_event}"}}]}}]'
                ).encode(),
            ]
        )

        def urlopen(url: str, timeout: int):
            opens.append((url, timeout))
            return _Response(next(responses))

        with (
            patch.object(e2e_conftest.urllib.request, "urlopen", side_effect=urlopen),
            patch.object(e2e_conftest.time, "sleep", side_effect=sleeps.append),
        ):
            e2e_conftest._stop_daemon_and_wait_paused(slug, timeout_sec=1)
    finally:
        sys.modules.pop(module_name, None)

    assert posts == [(f"{e2e_conftest.TEST_DASHBOARD_URL}/repos/{slug}/stop", 10)]
    assert opens == [(f"{e2e_conftest.TEST_DASHBOARD_URL}/api/states", 5)] * 4
    assert sleeps == [0.5, 0.5, 0.5]
