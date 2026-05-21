from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from src.models import QueueTask, TaskStatus
from src.web import app as web_app
from src.web.routes import repo_control


class FakeRedis:
    def __init__(self, done_index: dict[str, str | None]) -> None:
        self.done_index = done_index

    async def hget(self, key: str, field: str) -> str | None:
        assert key == "done_index:example__alpha"
        return self.done_index.get(field)


class FailingRedis:
    async def hget(self, key: str, field: str) -> str | None:
        raise RuntimeError("redis unavailable")


def _task(pr_id: str, status: TaskStatus) -> QueueTask:
    return QueueTask(pr_id=pr_id, title=pr_id, status=status)


async def _panel_context(
    tasks: list[QueueTask],
    merged_at_by_pr_id: dict[str, str | None],
) -> dict[str, object]:
    return await repo_control._build_tasks_panel_context(
        "example__alpha",
        tasks,
        redis_client=FakeRedis(merged_at_by_pr_id),  # type: ignore[arg-type]
        retry_cap=3,
    )


def _status_order(context: dict[str, object], status: str) -> list[str]:
    tasks_by_status = context["tasks_by_status"]
    assert isinstance(tasks_by_status, dict)
    views = tasks_by_status[status]
    assert isinstance(views, list)
    return [view["pr_id"] for view in views if isinstance(view, dict)]


@pytest.mark.asyncio
async def test_done_list_sorted_newest_first() -> None:
    tasks = [
        _task("PR-001", TaskStatus.DONE),
        _task("PR-002", TaskStatus.DONE),
        _task("PR-003", TaskStatus.DONE),
    ]
    context = await _panel_context(
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": '{"merged_at": "2026-05-03T10:00:00+00:00"}',
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "done") == ["PR-002", "PR-003", "PR-001"]


@pytest.mark.asyncio
async def test_done_list_with_missing_merged_at_falls_back() -> None:
    tasks = [
        _task("PR-001", TaskStatus.DONE),
        _task("PR-002", TaskStatus.DONE),
    ]
    context = await _panel_context(
        tasks,
        {"PR-001": "2026-05-01T10:00:00+00:00"},
    )

    assert _status_order(context, "done") == ["PR-001", "PR-002"]


@pytest.mark.asyncio
async def test_todo_list_order_unchanged() -> None:
    tasks = [
        _task("PR-001", TaskStatus.TODO),
        _task("PR-002", TaskStatus.TODO),
        _task("PR-003", TaskStatus.TODO),
    ]
    context = await _panel_context(
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "todo") == ["PR-001", "PR-002", "PR-003"]


@pytest.mark.asyncio
async def test_doing_list_order_unchanged() -> None:
    tasks = [
        _task("PR-001", TaskStatus.DOING),
        _task("PR-002", TaskStatus.DOING),
        _task("PR-003", TaskStatus.DOING),
    ]
    context = await _panel_context(
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "doing") == ["PR-001", "PR-002", "PR-003"]


@pytest.mark.asyncio
async def test_error_list_order_unchanged() -> None:
    tasks = [
        _task("PR-001", TaskStatus.ERROR),
        _task("PR-002", TaskStatus.ERROR),
        _task("PR-003", TaskStatus.ERROR),
    ]
    context = await _panel_context(
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "error") == ["PR-001", "PR-002", "PR-003"]


@pytest.mark.asyncio
async def test_empty_done_list() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.TODO)],
        {},
    )

    assert _status_order(context, "done") == []


@pytest.mark.asyncio
async def test_done_index_missing_or_unavailable_returns_none() -> None:
    assert (
        await repo_control._load_done_index_merged_at("example__alpha", "PR-001", None)
        is None
    )
    assert (
        await repo_control._load_done_index_merged_at(
            "example__alpha",
            "PR-001",
            FailingRedis(),  # type: ignore[arg-type]
        )
        is None
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, None),
        (b"\xff", None),
        ("", None),
        (123, None),
        ('"2026-05-04T10:00:00+00:00"', "2026-05-04T10:00:00+00:00"),
        ('{"mergedAt": "2026-05-05T10:00:00+00:00"}', "2026-05-05T10:00:00+00:00"),
        ("[]", None),
    ],
)
def test_extract_merged_at_shapes(raw: object, expected: str | None) -> None:
    assert repo_control._extract_merged_at(raw) == expected


def test_load_git_merged_at_rejects_invalid_task_id() -> None:
    assert repo_control._load_git_merged_at("example__alpha", "not-a-pr") is None


def test_load_git_merged_at_handles_config_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "example__alpha"
    repo_root.mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path))
    monkeypatch.setattr(repo_control, "load_config", lambda path: (_ for _ in ()).throw(RuntimeError("bad config")))
    seen_refs: list[str] = []

    def fake_git_log(repo_root: Path, target_ref: str, pr_id: str) -> str | None:
        seen_refs.append(target_ref)
        return None

    monkeypatch.setattr(repo_control, "_git_log_merged_at_for_ref", fake_git_log)

    assert repo_control._load_git_merged_at("example__alpha", "PR-001") is None
    assert seen_refs == ["origin/main", "main"]


def test_load_git_merged_at_uses_configured_branch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "example__alpha"
    repo_root.mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path))
    monkeypatch.setattr(repo_control, "load_config", lambda path: object())
    monkeypatch.setattr(
        repo_control,
        "_find_repo_config_by_name",
        lambda cfg, name: SimpleNamespace(branch="release"),
    )

    def fake_git_log(repo_root: Path, target_ref: str, pr_id: str) -> str | None:
        return "2026-05-06T10:00:00+00:00" if target_ref == "origin/release" else None

    monkeypatch.setattr(repo_control, "_git_log_merged_at_for_ref", fake_git_log)

    assert (
        repo_control._load_git_merged_at("example__alpha", "PR-001")
        == "2026-05-06T10:00:00+00:00"
    )


def test_git_log_merged_at_for_ref_handles_subprocess_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired("git", 5)

    monkeypatch.setattr(repo_control.subprocess, "run", fail_run)

    assert repo_control._git_log_merged_at_for_ref(tmp_path, "origin/main", "PR-001") is None


@pytest.mark.parametrize(
    "result",
    [
        subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="fatal"),
        subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""),
        subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="2026-05-06T10:00:00+00:00\x00PR-002: other\n",
            stderr="",
        ),
    ],
)
def test_git_log_merged_at_for_ref_ignores_unusable_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    result: subprocess.CompletedProcess[str],
) -> None:
    monkeypatch.setattr(repo_control.subprocess, "run", lambda *args, **kwargs: result)

    assert repo_control._git_log_merged_at_for_ref(tmp_path, "origin/main", "PR-001") is None


def test_git_log_merged_at_for_ref_returns_commit_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = subprocess.CompletedProcess(
        args=[],
        returncode=0,
        stdout="2026-05-06T10:00:00+00:00\x00PR-001: merged task\n",
        stderr="",
    )
    monkeypatch.setattr(repo_control.subprocess, "run", lambda *args, **kwargs: result)

    assert (
        repo_control._git_log_merged_at_for_ref(tmp_path, "origin/main", "PR-001")
        == "2026-05-06T10:00:00+00:00"
    )
