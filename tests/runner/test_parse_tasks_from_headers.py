from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.daemon import recovery as recovery_module
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError
from src.task_status import MergedState

from tests.runner import _helpers as h


def _write_task_file(task_dir: Path, task: dict[str, Any]) -> None:
    path = task_dir / f"{task['pr_id']}.md"
    if task.get("legacy_unstructured"):
        path.write_text(f"# {task['pr_id']}: {task['title']}\n\nLegacy body\n")
        return
    depends_on = task.get("depends_on", [])
    depends_value = ", ".join(depends_on) if depends_on else "none"
    frontmatter = ["---"]
    if task.get("frontmatter_status"):
        frontmatter.append(f"status: {task['frontmatter_status']}")
    frontmatter.extend(["---", ""])
    path.write_text(
        "\n".join(
            frontmatter
            + [
                f"# {task['pr_id']}: {task['title']}",
                "",
                f"Branch: {task['branch']}",
                f"- Type: {task.get('type', 'refactor')}",
                f"- Complexity: {task.get('complexity', 'low')}",
                f"- Depends on: {depends_value}",
                f"- Priority: {task.get('priority', 3)}",
                f"- Coder: {task.get('coder', 'codex')}",
                "",
                "## Problem",
                "Fixture task body.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _populate_tasks(repo: Path, tasks: list[dict[str, Any]]) -> None:
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)
    for task in tasks:
        _write_task_file(task_dir, task)


def _task_projection(task: QueueTask) -> dict[str, Any]:
    return {
        "pr_id": task.pr_id,
        "title": task.title,
        "status": task.status.value,
        "task_file": task.task_file,
        "depends_on": task.depends_on,
        "branch": task.branch,
        "priority": task.priority,
    }


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True,
    ).stdout.strip()


def test_restart_reads_remote_error_without_touching_coder_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = tmp_path / "remote.git"
    _git(tmp_path, "init", "--bare", str(remote))
    coder = tmp_path / "coder"
    writer = tmp_path / "writer"
    _git(tmp_path, "clone", str(remote), str(coder))
    _git(coder, "config", "user.email", "test@example.com")
    _git(coder, "config", "user.name", "Test")
    _populate_tasks(coder, [{
        "pr_id": "PR-042", "title": "Remote error", "branch": "pr-042-work",
        "frontmatter_status": "TODO",
    }])
    (coder / "work.txt").write_text("base\n")
    _git(coder, "add", "tasks", "work.txt")
    _git(coder, "commit", "-m", "initial")
    _git(coder, "branch", "-M", "main")
    _git(coder, "push", "-u", "origin", "main")
    _git(tmp_path, "clone", "--branch", "main", str(remote), str(writer))
    _git(writer, "config", "user.email", "test@example.com")
    _git(writer, "config", "user.name", "Test")
    task_path = writer / "tasks" / "PR-042.md"
    task_path.write_text(task_path.read_text().replace("status: TODO", "status: ERROR"))
    _git(writer, "add", "tasks/PR-042.md")
    _git(writer, "commit", "-m", "park task")
    _git(writer, "push", "origin", "main")

    _git(coder, "checkout", "-b", "pr-042-work")
    local_task = coder / "tasks" / "PR-042.md"
    local_task.write_text(local_task.read_text().replace("status: TODO", "status: DOING"))
    _git(coder, "add", "tasks/PR-042.md")
    _git(coder, "commit", "-m", "coder task")
    (coder / "work.txt").write_text("staged\n")
    _git(coder, "add", "work.txt")
    (coder / "work.txt").write_text("unstaged\n")
    (coder / "untracked.bin").write_bytes(b"\x00\xff")
    _git(coder, "fetch", "origin", "main")
    before = (
        _git(coder, "rev-parse", "HEAD"), _git(coder, "branch", "--show-current"),
        _git(coder, "status", "--porcelain"), _git(coder, "show", ":work.txt"),
        local_task.read_bytes(), (coder / "work.txt").read_bytes(),
        (coder / "untracked.bin").read_bytes(),
    )

    runner = h._make_runner()
    runner.repo_path = str(coder)
    monkeypatch.setattr(
        recovery_module, "_resolve_merged_state",
        lambda *_args, **_kwargs: MergedState(set(), set(), True),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *_args, **_kwargs: [PRInfo(number=42, branch="pr-042-work")],
    )
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.current_queue[0].status == TaskStatus.ERROR
    assert runner.state.state != PipelineState.WATCH
    assert before == (
        _git(coder, "rev-parse", "HEAD"), _git(coder, "branch", "--show-current"),
        _git(coder, "status", "--porcelain"), _git(coder, "show", ":work.txt"),
        local_task.read_bytes(), (coder / "work.txt").read_bytes(),
        (coder / "untracked.bin").read_bytes(),
    )


def test_restart_rejects_invalid_remote_base_without_using_local_headers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _git(tmp_path, "init", "-b", "main")
    _populate_tasks(tmp_path, [{
        "pr_id": "PR-042", "title": "Local only", "branch": "pr-042-work",
    }])
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.repo_config.branch = "-invalid"
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_args, **_kwargs: [])

    assert asyncio.run(runner.recover_state()) is False
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_queue is None
    assert "remote task headers unavailable" in (runner.state.error_message or "")


def _runner_for_fixture(
    tmp_path: Path,
    before: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
):
    runner = h._make_runner()
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    runner.repo_path = str(repo)
    _populate_tasks(repo, before.get("tasks", []))
    runner._idle_open_prs = [
        PRInfo(**pr) for pr in before.get("open_prs", [])
    ]
    runner._crashed_task_pr_ids = set(before.get("crashed_task_pr_ids", []))
    if before.get("current_task_pr_id"):
        runner.state.current_task = QueueTask(
            pr_id=before["current_task_pr_id"],
            title="current",
            status=TaskStatus.DOING,
        )

    def fake_resolve(
        repo_path: str,
        base_branch: str,
        owner_repo: str,
        candidate_pr_ids,
        headers,
        *,
        log_event,
    ) -> MergedState:
        return MergedState(
            set(before.get("merged_pr_ids_via_git_log", [])),
            set(before.get("merged_branches_via_api", [])),
            True,
        )

    monkeypatch.setattr(recovery_module, "_resolve_merged_state", fake_resolve)
    return runner, repo


def test_helper_returns_none_on_missing_tasks_dir(tmp_path: Path) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    repo.mkdir()
    runner.repo_path = str(repo)

    assert runner._parse_tasks_from_headers() is None


def test_helper_returns_empty_when_no_pr_files(tmp_path: Path) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    runner.repo_path = str(repo)

    assert runner._parse_tasks_from_headers() is None


def test_helper_skips_file_without_task_header(tmp_path: Path) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    tasks_dir = repo / "tasks"
    tasks_dir.mkdir(parents=True)
    (tasks_dir / "PR-001.md").write_text("\n", encoding="utf-8")
    runner.repo_path = str(repo)

    assert runner._parse_tasks_from_headers() is None


def test_helper_skips_legacy_unstructured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-001",
                "title": "Valid one",
                "branch": "pr-001-valid",
                "priority": 2,
            },
            {
                "pr_id": "PR-002",
                "title": "Legacy",
                "branch": "pr-002-legacy",
                "legacy_unstructured": True,
            },
            {
                "pr_id": "PR-003",
                "title": "Valid two",
                "branch": "pr-003-valid",
                "priority": 3,
            },
            {
                "pr_id": "PR-004",
                "title": "Valid three",
                "branch": "pr-004-valid",
                "priority": 4,
            },
        ]
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()
    assert tasks is not None
    assert [task.pr_id for task in tasks] == ["PR-001", "PR-003", "PR-004"]


def test_helper_applies_merged_state_via_resolve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-005",
                "title": "Merged by git",
                "branch": "pr-005-merged",
                "priority": 3,
            }
        ],
        "merged_pr_ids_via_git_log": ["PR-005"],
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert tasks[0].status == TaskStatus.DONE


def test_helper_applies_crashed_set_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-007",
                "title": "Crashed",
                "branch": "pr-007-crashed",
                "priority": 3,
            }
        ],
        "crashed_task_pr_ids": ["PR-007"],
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert tasks[0].status == TaskStatus.ERROR


def test_helper_orders_by_priority_then_pr_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-010",
                "title": "Later number",
                "branch": "pr-010",
                "priority": 2,
            },
            {
                "pr_id": "PR-ABC",
                "title": "Non numeric suffix",
                "branch": "pr-abc",
                "priority": 2,
            },
            {
                "pr_id": "PR-002",
                "title": "Earlier number",
                "branch": "pr-002",
                "priority": 2,
            },
            {
                "pr_id": "PR-001",
                "title": "Higher priority",
                "branch": "pr-001",
                "priority": 1,
            },
        ]
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert [task.pr_id for task in tasks] == [
        "PR-001",
        "PR-002",
        "PR-010",
        "PR-ABC",
    ]


def test_helper_raises_non_legacy_header_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-011",
                "title": "Invalid coder",
                "branch": "pr-011-invalid-coder",
                "coder": "invalid/coder",
            }
        ]
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    with pytest.raises(QueueValidationError, match="invalid Coder"):
        runner._parse_tasks_from_headers()


def test_helper_raises_header_file_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True)
    runner.repo_path = str(repo)
    _write_task_file(
        task_dir,
        {
            "pr_id": "PR-013",
            "title": "Wrong file",
            "branch": "pr-013-wrong-file",
        },
    )
    (task_dir / "PR-012.md").write_text(
        (task_dir / "PR-013.md").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (task_dir / "PR-013.md").unlink()

    monkeypatch.setattr(
        recovery_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), set(), True),
    )

    with pytest.raises(QueueValidationError, match="does not match task file"):
        runner._parse_tasks_from_headers()


def test_helper_ignores_stopped_current_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-014",
                "title": "Stopped current task",
                "branch": "pr-014-stopped",
            }
        ],
        "current_task_pr_id": "PR-014",
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)
    runner._user_stopped_task_pr_ids = {"PR-014"}

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert tasks[0].status == TaskStatus.TODO


def test_helper_against_each_golden_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery_golden_cases: list[tuple[str, dict, dict]],
) -> None:
    for scenario_name, before, expected in recovery_golden_cases:
        runner, _repo = _runner_for_fixture(
            tmp_path / scenario_name,
            before,
            monkeypatch,
        )

        tasks = runner._parse_tasks_from_headers()
        current_queue = [] if tasks is None else [_task_projection(task) for task in tasks]

        assert current_queue == expected["current_queue"], scenario_name


@pytest.mark.parametrize("entry", ["idle", "recovery"])
def test_legacy_history_does_not_reenter_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entry: str,
) -> None:
    import asyncio

    from src.daemon.handlers import idle as idle_module

    before = {
        "tasks": [
            {"pr_id": "PR-001", "title": "Archive", "legacy_unstructured": True},
            {"pr_id": "PR-002", "title": "Merged", "branch": "pr-002-merged"},
        ],
        "merged_branches_via_api": ["pr-002-merged"],
    }
    runner, repo = _runner_for_fixture(tmp_path, before, monkeypatch)
    path = repo / "tasks" / "PR-002.md"
    path.write_text(path.read_text().removeprefix("---\n---\n"))
    contents = {p.name: p.read_bytes() for p in (repo / "tasks").glob("PR-*.md")}
    monkeypatch.setattr(
        idle_module, "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), {"pr-002-merged"}, True),
    )
    if entry == "idle":
        task = asyncio.run(h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG(runner))
        assert task is None
        tasks = runner._idle_dag_tasks
    else:
        tasks = runner._parse_tasks_from_headers()
    assert [(t.pr_id, t.status) for t in tasks] == [("PR-002", TaskStatus.DONE)]
    assert {p.name: p.read_bytes() for p in (repo / "tasks").glob("PR-*.md")} == contents


@pytest.mark.parametrize("entry", ["idle", "recovery"])
def test_api_outage_preserves_queue_without_dispatch_or_task_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entry: str, isolate_analytics_dir,
) -> None:
    import asyncio

    from src.daemon.handlers import idle as idle_module

    before = {
        "tasks": [{"pr_id": "PR-002", "title": "Merged", "branch": "pr-002-merged"}],
    }
    runner, repo = _runner_for_fixture(tmp_path, before, monkeypatch)
    previous = QueueTask(
        pr_id="PR-002", title="Merged", status=TaskStatus.DONE,
        task_file="tasks/PR-002.md", branch="pr-002-merged",
    )
    # This is a historical merge with already-completed outcome accounting.
    import json

    from src.analytics.outcome_logger import compute_task_id_hash

    isolate_analytics_dir.mkdir(parents=True, exist_ok=True)
    (isolate_analytics_dir / "2026-04.jsonl").write_text(json.dumps({
        "task_id_hash": compute_task_id_hash(previous.pr_id, runner.name),
    }) + "\n")
    runner.state.current_queue = [previous]
    runner.state.queue_done = 1
    runner.state.queue_total = 1
    path = repo / "tasks" / "PR-002.md"
    original = path.read_bytes()
    for module in (idle_module, recovery_module):
        monkeypatch.setattr(
            module, "_resolve_merged_state",
            lambda *args, **kwargs: MergedState(set(), set(), False),
        )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        idle_module.IdleMixin, "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )
    async def unexpected_failure(*args, **kwargs):
        pytest.fail("A missing merge probe must not fail the task")
    monkeypatch.setattr(runner, "_transition_to_error", unexpected_failure)

    if entry == "idle":
        assert asyncio.run(runner._select_next_task_or_attach([], [])) is None
    else:
        assert asyncio.run(runner._recover_state_headers()) is False
    assert runner.state.current_queue == [previous]
    assert (runner.state.queue_done, runner.state.queue_total) == (1, 1)
    assert runner.state.current_task is None
    assert path.read_bytes() == original
    assert any("merge status unavailable" in event["event"] for event in runner.state.history)

    for module in (idle_module, recovery_module):
        monkeypatch.setattr(
            module, "_resolve_merged_state",
            lambda *args, **kwargs: MergedState(set(), {"pr-002-merged"}, True),
        )
    assert asyncio.run(runner._recover_state_headers()) is True
    assert runner.state.current_queue[0].status == TaskStatus.DONE
    assert runner.state.current_task is None


def test_restart_discovers_merged_unprefixed_pr_from_todo_header(tmp_path, monkeypatch):
    from unittest.mock import AsyncMock

    from src.github import prs as gh_prs

    _populate_tasks(tmp_path, [{
        "pr_id": "PR-401", "title": "Interrupted merge", "branch": "feature-completion",
        "frontmatter_status": "TODO",
    }])
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    # Neither the branch nor squash subject encodes the task ID. The real
    # header/status parser therefore derives TODO despite merged branch evidence.
    monkeypatch.setattr(recovery_module, "_resolve_merged_state",
                        lambda *a, **kw: MergedState({"feature-completion"}, set(), True))
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: [])
    pr = PRInfo(number=629, pr_id=None, title="Complete feature",
                branch="feature-completion", head_sha="a" * 40)
    monkeypatch.setattr(gh_prs, "get_merged_prs_discovery", lambda *a: gh_prs.PRDiscovery(
        runner.owner_repo, "main", [pr], True,
    ))
    monkeypatch.setattr(runner._metrics_store, "list_task_runs", AsyncMock(return_value=[]))
    monkeypatch.setattr(runner, "_cleanup_expected_branch", lambda: None)

    assert runner._parse_tasks_from_headers()[0].status == TaskStatus.TODO
    assert asyncio.run(runner.recover_state())
    assert runner.state.state == PipelineState.MERGE
    assert runner.state.current_task.pr_id == "PR-401"
    assert runner.state.current_pr == pr
    assert runner.state.merge_phase == "post_merge_cleanup"
