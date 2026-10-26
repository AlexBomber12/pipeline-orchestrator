import os
import subprocess
from pathlib import Path

import pytest

from tests.e2e.lib import coder_shim as shim_control

SHIM = Path(__file__).parent / "e2e" / "lib" / "coder_shim.sh"


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


def test_success_shim_separates_branch_creation_from_head_publication(tmp_path: Path) -> None:
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
run_success PR-42 pr-42-success
''', "bash", str(SHIM.resolve())],
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
