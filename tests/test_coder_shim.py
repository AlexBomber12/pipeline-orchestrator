import os
import subprocess
from pathlib import Path

import pytest

from tests.e2e.lib import coder_shim as shim_control

SHIM = Path(__file__).parent / "e2e" / "lib" / "coder_shim.sh"


@pytest.mark.parametrize("event", [
    "[INFRA] Unpublished local branch cleanup deferred: worktree is dirty",
    "[RECOVERY] Terminal release wait; re-observing safety evidence next cycle.",
])
@pytest.mark.parametrize("missing", [
    None, "cleanup", "task", "task_branch", "attempt", "attempt_branch", "failure", "error", "hold",
])
def test_dirty_continuity_requires_bound_cleanup_and_dirty_hold(event, missing):
    from tests.e2e.test_failure_continuity import _dirty_held_with_cleanup

    state = {
        "state": "ERROR",
        "current_task": {"pr_id": "PR-42", "branch": "pr-42-dirty-root"},
        "publication_attempt": {
            "task_id": "PR-42", "branch": "pr-42-dirty-root", "pending": True,
            "exit_classification": "failure", "cleanup_confirmed": True,
        },
        "history": [{"event": event}],
    }
    if missing == "cleanup":
        state["publication_attempt"]["cleanup_confirmed"] = False
    elif missing in {"task", "task_branch"}:
        state["current_task"]["pr_id" if missing == "task" else "branch"] = "other"
    elif missing in {"attempt", "attempt_branch"}:
        state["publication_attempt"]["task_id" if missing == "attempt" else "branch"] = "other"
    elif missing == "failure":
        state["publication_attempt"]["exit_classification"] = "success"
    elif missing == "error":
        state["state"] = "IDLE"
    elif missing == "hold":
        state["history"] = [{"event": "bound cleanup/work proof required."}]
    assert _dirty_held_with_cleanup(state, "PR-42", "pr-42-dirty-root") is (missing is None)


def _write_task(
    repo: Path,
    pr_id: str,
    *,
    branch: str,
    status: str | None = "TODO",
    body: str = "",
) -> None:
    tasks = repo / "tasks"
    tasks.mkdir(parents=True, exist_ok=True)
    status_line = "" if status is None else f"- Status: {status}\n"
    (tasks / f"{pr_id}.md").write_text(
        f"# {pr_id}: Sample\n\n"
        f"Branch: {branch}\n"
        f"{status_line}"
        f"{body}",
        encoding="utf-8",
    )


def _parse_doing_task(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            f"source {SHIM}; parse_doing_task {repo}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_claude_auth_status_reports_sanitized_authenticated_fixture(
    tmp_path: Path,
) -> None:
    claude = tmp_path / "claude"
    claude.symlink_to(SHIM)

    result = subprocess.run(
        [str(claude), "auth", "status"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == '{"loggedIn":true,"authMethod":"claude.ai"}\n'
    assert result.stderr == ""


def test_codex_auth_status_is_accepted_by_daemon_probe(tmp_path: Path) -> None:
    from src.coders.codex import _authentication_mode

    codex = tmp_path / "codex"
    codex.symlink_to(SHIM)

    result = subprocess.run(
        [str(codex), "login", "status"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert _authentication_mode(result.stdout.strip()) == "chatgpt"


def test_shim_reads_active_pr_from_runtime_file(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-004", branch="pr-004", status="TODO")
    _write_task(tmp_path, "PR-005", branch="pr-005", status=None)
    runtime_dir = tmp_path / ".daemon-runtime"
    runtime_dir.mkdir()
    (runtime_dir / "active-pr-id").write_text("PR-005\n", encoding="utf-8")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_honors_runtime_file_before_doing_metadata(
    tmp_path: Path,
) -> None:
    _write_task(tmp_path, "PR-001", branch="pr-001", status=None)
    _write_task(tmp_path, "PR-002", branch="pr-002", status="DOING")
    runtime_dir = tmp_path / ".daemon-runtime"
    runtime_dir.mkdir()
    (runtime_dir / "active-pr-id").write_text("PR-001\n", encoding="utf-8")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-001\tpr-001\n"


def test_shim_falls_back_to_status_doing_in_pr_md(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status="DOING")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_fails_fast_with_only_statusless_pr_md(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status=None)

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 1
    assert result.stdout == ""


def test_shim_ignores_stale_runtime_file_before_fallback(
    tmp_path: Path,
) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status="DOING")
    runtime_dir = tmp_path / ".daemon-runtime"
    runtime_dir.mkdir()
    (runtime_dir / "active-pr-id").write_text("PR-999\n", encoding="utf-8")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_returns_nothing_when_no_doing_task(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status="TODO")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 1
    assert result.stdout == ""


def test_shim_handles_multiple_doing_status(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-010", branch="pr-010", status="DOING")
    _write_task(tmp_path, "PR-002", branch="pr-002", status="DOING")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-002\tpr-002\n"


def test_shim_ignores_status_doing_outside_task_metadata(tmp_path: Path) -> None:
    _write_task(
        tmp_path,
        "PR-001",
        branch="pr-001",
        status="TODO",
        body="\n## Notes\n\n- Status: DOING\n",
    )
    _write_task(tmp_path, "PR-002", branch="pr-002", status="DOING")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-002\tpr-002\n"


@pytest.mark.parametrize("scenario", ["run_success", "run_success_pending_ci"])
def test_success_shim_separates_branch_creation_from_head_publication(tmp_path: Path, scenario: str) -> None:
    remote = tmp_path / "remote.git"
    repo = tmp_path / "repo"

    def git(*args: str, cwd: Path = tmp_path) -> str:
        return subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True, check=True,
        ).stdout.strip()

    git("init", "--bare", "--initial-branch=main", str(remote))
    git("config", "core.logAllRefUpdates", "true", cwd=remote)
    git("clone", str(remote), str(repo))
    git("config", "user.name", "Fixture", cwd=repo)
    git("config", "user.email", "fixture@test.invalid", cwd=repo)
    git("commit", "--allow-empty", "-m", "base", cwd=repo)
    base = git("rev-parse", "HEAD", cwd=repo)
    git("push", "origin", "main", cwd=repo)

    result = subprocess.run(
        ["bash", "-c", '''
source "$1"
ensure_pr_url() { printf 'https://example.invalid/pull/42'; }
gh() { return 0; }
$2 PR-42 pr-42-success
''', "bash", str(SHIM.resolve()), scenario],
        cwd=repo, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    head = git("rev-parse", "refs/heads/pr-42-success", cwd=remote)
    assert head != base
    assert "PR-42" in git("show", f"{head}:tests/e2e-shim-marker.txt", cwd=remote)
    events = (remote / "logs/refs/heads/pr-42-success").read_text().splitlines()
    assert len(events) == 2
    created, pushed = (event.split() for event in events)
    assert created[1] == base and pushed[0] == base and pushed[1] == head
    created_at, pushed_at = (int(event.split("\t", 1)[0].rsplit(" ", 2)[1]) for event in events)
    assert pushed_at > created_at


def test_crash_shim_signals_before_wait_and_never_publishes_on_timeout(tmp_path: Path) -> None:
    scenario = tmp_path / "scenario"
    result = subprocess.run(
        ["bash", "-c", '''
source "$1"
git() { echo "unexpected git side effect" >&2; exit 9; }
sleep() {
    [[ "$1" == 120 ]] || exit 8
    [[ "$(cat "${PO_SHIM_SCENARIO_FILE}.started")" == $'PR-42\tpr-42-crash' ]] || exit 7
}
run_crash_before_publish PR-42 pr-42-crash
''', "bash", str(SHIM.resolve())],
        cwd=tmp_path, capture_output=True, text=True, timeout=5,
        env={**os.environ, "PO_SHIM_SCENARIO_FILE": str(scenario)},
    )
    assert result.returncode == 1, result.stderr
    assert not result.stderr
    assert Path(str(scenario) + ".started").read_text() == "PR-42\tpr-42-crash\n"


def test_crash_shim_handshake_is_bound_and_cleaned_on_failure(tmp_path: Path, monkeypatch) -> None:
    scenario, started = tmp_path / "scenario", tmp_path / "scenario.started"
    monkeypatch.setattr(shim_control, "SHIM_SCENARIO_PATH", scenario)
    monkeypatch.setattr(shim_control, "SHIM_STARTED_PATH", started)
    scenario.write_text("success\n")
    started.write_text("PR-42\tpr-42-crash\n")
    with pytest.raises(RuntimeError, match="fixture failure"):
        with shim_control.coder_shim("crash_before_publish"):
            assert not shim_control.crash_coder_started("PR-42", "pr-42-crash")
            started.write_text("partial")
            assert not shim_control.crash_coder_started("PR-42", "pr-42-crash")
            started.write_text("PR-42\tpr-42-crash\n")
            assert shim_control.crash_coder_started("PR-42", "pr-42-crash")
            assert not shim_control.crash_coder_started("PR-43", "pr-42-crash")
            assert not shim_control.crash_coder_started("PR-42", "other")
            raise RuntimeError("fixture failure")
    assert not started.exists()
    assert scenario.read_text() == "success\n"


@pytest.mark.parametrize("outcome", [
    "recovered", "stale", "missing_crash", "root_redispatched", "dependency_dispatched",
])
def test_multitask_crash_oracle_handles_delayed_recovery_without_accepting_wrong_outcomes(monkeypatch, outcome):
    """Replay the 120-second CI boundary through the complete e2e assertion path."""
    from contextlib import contextmanager
    from types import SimpleNamespace

    from tests.e2e import test_sigkill_recovery_multitask as scenario

    now = [0.0]
    phase = ["initial"]
    root = "PR-42"
    before = "2026-10-27T00:17:12+00:00"
    after = "2026-10-27T00:19:14+00:00"
    monkeypatch.setattr(scenario, "time", SimpleNamespace(
        time=lambda: 42, monotonic=lambda: now[0], sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
    ))

    @contextmanager
    def shim(name):
        if name == "success":
            phase[0] = "next_dispatch"
        yield

    def command(args, **kwargs):
        if args[:2] == ["docker", "kill"]:
            phase[0] = "recovering"
        return SimpleNamespace(returncode=0)

    def state():
        if phase[0] == "next_dispatch":
            next_task = {"root_redispatched": root, "dependency_dispatched": "PR-44"}.get(outcome, "PR-43")
            return {"state": "CODING", "current_task": {"pr_id": next_task}, "last_updated": after}
        if phase[0] == "initial" or now[0] < 121:
            return {"state": "CODING", "current_task": {"pr_id": root}, "last_updated": before}
        return {"state": "IDLE", "current_task": None,
                "last_updated": before if outcome == "stale" else after,
                "history": [] if outcome == "missing_crash" else [{
                    "event": f"[INFRA] Task {root} crashed, marking ERROR. Manually re-upload to retry.",
                }]}

    monkeypatch.setattr(scenario, "coder_shim", shim)
    monkeypatch.setattr(scenario, "crash_coder_started", lambda pr_id, branch: pr_id == root)
    monkeypatch.setattr(scenario.subprocess, "run", command)
    running = iter(["false", "true"])
    monkeypatch.setattr(scenario, "_is_daemon_running", lambda: next(running))
    monkeypatch.setattr(scenario, "_daemon_exit_code", lambda: 137)
    monkeypatch.setattr(scenario, "_fetch_tasks_panel", lambda *args: (200, '\n'.join(
        f'<div id="task-content-{status}-PR-{number}"></div>'
        for status, number in [("error", 42), ("todo", 43), ("todo", 44), ("todo", 45)]
    )))

    def run():
        scenario.test_sigkill_during_coding_with_multitask_queue_recovers_and_progresses(
            "http://test.invalid", "testbed", lambda *args, **kwargs: None,
            state, lambda path: 201, lambda *args, **kwargs: "fixture.zip", None,
        )

    if outcome == "recovered":
        run()
        assert 120 < now[0] < 150
    else:
        with pytest.raises(AssertionError):
            run()


def test_failure_shim_leaves_bound_clean_branch_preserved_on_remote_base(tmp_path):
    remote, repo = tmp_path / "remote.git", tmp_path / "repo"

    def git(*args, cwd=tmp_path):
        return subprocess.run(
            ["git", *args], cwd=cwd, capture_output=True, text=True, check=True,
        ).stdout.strip()

    git("init", "--bare", "--initial-branch=main", str(remote))
    git("clone", str(remote), str(repo))
    git("config", "user.name", "Fixture", cwd=repo)
    git("config", "user.email", "fixture@test.invalid", cwd=repo)
    _write_task(repo, "PR-42", branch="pr-42-failure", status="DOING")
    git("add", "tasks", cwd=repo)
    git("commit", "-m", "task", cwd=repo)
    git("push", "origin", "main", cwd=repo)
    result = subprocess.run(
        ["bash", "-c", 'source "$1"; REPO_DIR="$2"; main --print fixture',
         "claude", str(SHIM.resolve()), str(repo)],
        cwd=repo, capture_output=True, text=True, timeout=30,
        env={**os.environ, "SHIM_SCENARIO": "exit_nonzero", "PO_SHIM_SCENARIO_FILE": ""},
    )
    assert result.returncode == 1, result.stderr
    assert "shim: simulating coder failure" in result.stderr
    assert git("branch", "--show-current", cwd=repo) == "pr-42-failure"
    assert git("status", "--porcelain", cwd=repo) == ""
    assert git("rev-parse", "HEAD", cwd=repo) == git("rev-parse", "refs/heads/main", cwd=remote)
    assert git("ls-remote", "--heads", "origin", "refs/heads/pr-42-failure", cwd=repo) == ""


@pytest.mark.parametrize("outcome", ["released", "missing_error", "missing_dependent", "lost_cause", "idle_only"])
def test_clean_failure_e2e_oracle_requires_retained_terminal_outcome(monkeypatch, outcome):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from tests.e2e import test_cancellation_cause_crash as scenario

    now, reads = [0.0], [0]
    monkeypatch.setattr(scenario, "time", SimpleNamespace(
        time=lambda: 42, monotonic=lambda: now[0],
        sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
    ))

    @contextmanager
    def shim(name):
        yield

    def cancellations(*args):
        reads[0] += 1
        if outcome == "lost_cause" and reads[0] > 3:
            return []
        return [{"task_id": "PR-42", "category": "ERROR", "dependents_count": 1,
                 "payload": {"subsource": "crash" if reads[0] <= 3 else "diagnose_exhausted"}}]

    class Panel:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self):
            return "\n".join(
                f'<div id="task-content-{status}-PR-{number}"></div>'
                for status, number, missing in [
                    ("error", 42, "missing_error"), ("todo", 43, "missing_dependent"),
                ] if outcome != missing
            ).encode()

    monkeypatch.setattr(scenario, "coder_shim", shim)
    monkeypatch.setattr(scenario, "_fetch_cancellations", cancellations)
    monkeypatch.setattr(scenario.urllib.request, "urlopen", lambda *a, **kw: Panel())

    def run():
        scenario.test_coder_crash_records_cancellation_cause_and_propagates(
            "http://test.invalid", "testbed", lambda *a, **kw: None,
            lambda: {"state": "IDLE", "current_task": None, "history": [] if outcome == "idle_only" else [{
                "event": "[RECOVERY] Failed attempt retained; independent work released.",
            }]},
            lambda path: 201, lambda *a, **kw: "fixture.zip", None,
        )

    if outcome == "released":
        run()
    else:
        with pytest.raises(AssertionError):
            run()


async def test_settings_shim_catalogue_uses_production_discovery(tmp_path):
    from src.coders.claude_models import discover_claude_models

    (tmp_path / "claude").symlink_to(SHIM.resolve())
    models = await discover_claude_models(env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"})
    assert [model.invocation_id for model in models] == ["opus", "sonnet"]
    assert [[effort.name for effort in model.reasoning_efforts] for model in models] == [
        ["low", "high"], ["low", "high"],
    ]


async def test_settings_shim_refresh_recovers_cached_discovery_failure(tmp_path, monkeypatch):
    from src.coder_registry import ModelCatalogUnavailable
    from src.coders.claude import ClaudePlugin
    from src.config import AppConfig
    from src.web.services.model_catalog import ModelCatalogCache

    (tmp_path / "claude").symlink_to(SHIM.resolve())
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    calls = []

    async def daemon_loader(plugin, **kwargs):
        calls.append(plugin.name)
        if len(calls) == 1:
            raise ModelCatalogUnavailable("test daemon restarting")
        return await plugin.get_model_catalog(**kwargs)

    cache = ModelCatalogCache(loader=daemon_loader)
    plugin = ClaudePlugin()
    kwargs = {"config": AppConfig(), "config_path": str(tmp_path / "config.yml")}
    try:
        assert (await cache.get(plugin, **kwargs)).status == "unavailable"
        assert (await cache.get(plugin, **kwargs)).models == ()
        assert calls == ["claude"]
        refreshed = await cache.get(plugin, refresh=True, **kwargs)
        assert refreshed.status == "available"
        assert [model.invocation_id for model in refreshed.models] == ["opus", "sonnet"]
        assert calls == ["claude", "claude"]
    finally:
        await cache.close()


def test_settings_shim_records_bound_launch_without_prompt(tmp_path):
    import json

    scenario = tmp_path / "scenario"
    scenario.write_text("hang\n")
    _write_task(tmp_path, "PR-42", branch="pr-42-e2e-settings-first", status="DOING")
    result = subprocess.run(
        ["bash", "-c", '''
source "$1"
REPO_DIR="$2"
sleep() { [[ "$1" == 120 ]]; }
main --print "private prompt excluded from evidence" --model sonnet --effort high
''', "claude", str(SHIM.resolve()), str(tmp_path)],
        cwd=tmp_path, capture_output=True, text=True, timeout=5,
        env={**os.environ, "PO_SHIM_SCENARIO_FILE": str(scenario)},
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(Path(str(scenario) + ".settings-started").read_text()) == {
        "pr_id": "PR-42", "branch": "pr-42-e2e-settings-first",
        "plugin_id": "claude", "model": "sonnet", "effort": "high",
    }
    assert not Path(str(scenario) + ".settings-started.tmp").exists()


def test_settings_overlay_oracle_runs_with_production_config(tmp_path):
    from tests.e2e.test_shared_config import test_settings_overlay_precedence_is_disposable

    test_settings_overlay_precedence_is_disposable(tmp_path)


@pytest.mark.parametrize("outcome", [
    "fresh", "stale_only", "wrong_task", "wrong_branch", "wrong_plugin",
    "wrong_phase", "missing_run", "wrong_effort",
])
def test_settings_launch_oracle_requires_fresh_bound_snapshot(tmp_path, monkeypatch, outcome):
    import json
    from types import SimpleNamespace

    from tests.e2e import test_shared_config as scenario

    now, reads = [0.0], [0]
    monkeypatch.setattr(scenario, "time", SimpleNamespace(
        monotonic=lambda: now[0],
        sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
    ))
    launch_path = tmp_path / "launch.json"
    cli = {"pr_id": "PR-43", "branch": "pr-43-next", "plugin_id": "claude",
           "model": "sonnet", "effort": "high"}
    launch_path.write_text(json.dumps(cli))

    def get_state():
        reads[0] += 1
        # Reproduce CI: new task identity is published with the previous run's
        # low snapshot. CLI evidence already exists when this state is read.
        stale = reads[0] == 1 or outcome == "stale_only"
        state = {
            "state": "CODING",
            "current_task": {"pr_id": "PR-43", "branch": "pr-43-next"},
            "active_invocation": {
                "plugin_id": "claude", "model_override": "sonnet",
                "reasoning_effort_override": "low" if stale else "high",
                "run_id": "old-run" if stale else "new-run", "phase": "coding",
            },
        }
        if not stale:
            if outcome in {"wrong_task", "wrong_branch"}:
                state["current_task"]["pr_id" if outcome == "wrong_task" else "branch"] = "other"
            elif outcome in {"wrong_plugin", "wrong_phase", "missing_run", "wrong_effort"}:
                key, value = {
                    "wrong_plugin": ("plugin_id", "codex"),
                    "wrong_phase": ("phase", "fix"),
                    "missing_run": ("run_id", None),
                    "wrong_effort": ("reasoning_effort_override", "low"),
                }[outcome]
                state["active_invocation"][key] = value
        return state

    def run():
        return scenario._wait_for_settings_launch(
            get_state, launch_path, "PR-43", "pr-43-next", "old-run",
        )

    if outcome == "fresh":
        state, record = run()
        assert reads[0] == 2
        assert state["active_invocation"]["run_id"] == "new-run"
        assert record == cli
    else:
        with pytest.raises(AssertionError):
            run()


def test_fix_shim_preserves_remote_history_and_replaces_only_owned_marker(tmp_path):
    def git(*args, cwd=tmp_path):
        return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True).stdout.strip()

    remote, repo = tmp_path / "remote.git", tmp_path / "repo"
    git("init", "--bare", "--initial-branch=main", str(remote))
    git("clone", str(remote), str(repo))
    git("config", "user.name", "Fixture", cwd=repo)
    git("config", "user.email", "fixture@test.invalid", cwd=repo)
    (repo / "tests").mkdir()
    marker = repo / "tests/e2e-shim-marker.txt"
    other = "shim marker for PR-420 at unchanged"
    marker.write_text(f"{other}\nshim marker for PR-42 at old\n")
    git("add", "tests", cwd=repo)
    git("commit", "-m", "original", cwd=repo)
    git("push", "origin", "HEAD:pr-42-fix", cwd=repo)
    old = git("rev-parse", "HEAD", cwd=repo)
    result = subprocess.run(
        ["bash", "-c", '''
source "$1"
ensure_pr_url() { printf 'https://example.invalid/pull/42'; }
gh() { printf '%s\n' "$*" >> gh-calls; }
run_fix_existing_pr PR-42 pr-42-fix true
''', "bash", str(SHIM.resolve())],
        cwd=repo, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stderr
    head = git("rev-parse", "refs/heads/pr-42-fix", cwd=remote)
    assert head != old
    assert git("rev-parse", f"{head}^", cwd=remote) == old
    lines = git("show", f"{head}:tests/e2e-shim-marker.txt", cwd=remote).splitlines()
    assert lines[0] == other and len(lines) == 2
    assert lines[1].startswith("shim marker for PR-42 at ") and not lines[1].endswith("old")
    calls = (repo / "gh-calls").read_text()
    assert f"statuses/{head} -f state=pending" in calls
    assert calls.index("state=pending") < calls.index("@codex review")
