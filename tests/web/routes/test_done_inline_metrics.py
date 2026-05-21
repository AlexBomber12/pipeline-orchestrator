from __future__ import annotations

from typing import Any

import pytest

from src.models import QueueTask, TaskStatus
from src.web import app as web_app
from src.web.routes import repo_control


class FakeRedis:
    def __init__(
        self,
        *,
        zsets: dict[str, list[tuple[str, int]]] | None = None,
        hashes: dict[str, dict[str, object]] | None = None,
    ) -> None:
        self.zsets = zsets or {}
        self.hashes = hashes or {}

    async def hget(self, key: str, field: str) -> str | None:
        return None

    async def zrevrange(self, key: str, start: int, end: int) -> list[str]:
        entries = sorted(
            self.zsets.get(key, []),
            key=lambda entry: entry[1],
            reverse=True,
        )
        return [run_id for run_id, _score in entries][start : end + 1]

    async def hgetall(self, key: str) -> dict[str, object]:
        return self.hashes.get(key, {})


class FailingMetricsRedis(FakeRedis):
    async def zrevrange(self, key: str, start: int, end: int) -> list[str]:
        raise RuntimeError("redis unavailable")


class LegacyMetricsRedis:
    def __init__(
        self,
        *,
        sets: dict[str, set[object]] | None = None,
        strings: dict[str, object] | None = None,
    ) -> None:
        self.sets = sets or {}
        self.strings = strings or {}

    async def smembers(self, key: str) -> set[object]:
        return self.sets.get(key, set())

    async def get(self, key: str) -> object | None:
        return self.strings.get(key)


def _task(pr_id: str, status: TaskStatus) -> QueueTask:
    return QueueTask(
        pr_id=pr_id,
        title=f"{pr_id} title",
        status=status,
        branch=f"{pr_id.lower()}-branch",
    )


async def _panel_context(
    tasks: list[QueueTask],
    redis_client: FakeRedis,
) -> dict[str, Any]:
    return await repo_control._build_tasks_panel_context(
        "example__alpha",
        tasks,
        redis_client=redis_client,  # type: ignore[arg-type]
        retry_cap=3,
    )


def _views(context: dict[str, Any], status: str) -> list[dict[str, Any]]:
    tasks_by_status = context["tasks_by_status"]
    assert isinstance(tasks_by_status, dict)
    views = tasks_by_status[status]
    assert isinstance(views, list)
    return views


def _redis_with_metrics(
    pr_id: str = "PR-001",
    run_id: str = "run-1",
    *,
    score: int = 100,
    record: dict[str, object] | None = None,
) -> FakeRedis:
    return FakeRedis(
        zsets={
            f"metrics:task_runs:example__alpha:{pr_id}": [(run_id, score)],
        },
        hashes={
            f"metrics:run:{run_id}": record
            or {
                "coder": "codex",
                "model": "gpt-5",
                "duration_ms": "12345",
                "fix_iterations": "2",
                "exit_reason": "merged",
            },
        },
    )


@pytest.mark.asyncio
async def test_done_view_includes_metrics_when_record_exists() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.DONE)],
        _redis_with_metrics(),
    )

    assert _views(context, "done")[0]["metrics"] == {
        "coder": "codex",
        "model": "gpt-5",
        "duration_ms": "12345",
        "fix_iterations": "2",
        "exit_reason": "merged",
    }


@pytest.mark.asyncio
async def test_done_view_metrics_none_when_no_record() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.DONE)],
        FakeRedis(),
    )

    assert _views(context, "done")[0]["metrics"] is None


@pytest.mark.asyncio
async def test_done_view_metrics_picks_most_recent_run() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.DONE)],
        FakeRedis(
            zsets={
                "metrics:task_runs:example__alpha:PR-001": [
                    ("run-1", 100),
                    ("run-2", 200),
                    ("run-3", 300),
                ],
            },
            hashes={
                "metrics:run:run-1": {"coder": "claude"},
                "metrics:run:run-2": {"coder": "codex"},
                "metrics:run:run-3": {"coder": "latest", "model": "gpt-5"},
            },
        ),
    )

    assert _views(context, "done")[0]["metrics"]["coder"] == "latest"
    assert _views(context, "done")[0]["metrics"]["model"] == "gpt-5"


@pytest.mark.asyncio
async def test_todo_view_metrics_field_absent() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.TODO)],
        _redis_with_metrics(),
    )

    assert "metrics" not in _views(context, "todo")[0]


@pytest.mark.asyncio
async def test_doing_view_metrics_field_absent() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.DOING)],
        _redis_with_metrics(),
    )

    assert "metrics" not in _views(context, "doing")[0]


@pytest.mark.asyncio
async def test_metrics_dict_handles_missing_subset() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.DONE)],
        _redis_with_metrics(
            record={
                "coder": "codex",
                "duration_ms": "12345",
            },
        ),
    )

    assert _views(context, "done")[0]["metrics"] == {
        "coder": "codex",
        "model": None,
        "duration_ms": "12345",
        "fix_iterations": None,
        "exit_reason": None,
    }


@pytest.mark.asyncio
async def test_recent_metrics_returns_empty_without_redis() -> None:
    assert (
        await repo_control._recent_metrics_by_task_id(
            "example__alpha",
            None,
            ["PR-001"],
        )
        == {}
    )


@pytest.mark.asyncio
async def test_recent_metrics_skips_redis_errors() -> None:
    assert (
        await repo_control._recent_metrics_by_task_id(
            "example__alpha",
            FailingMetricsRedis(),  # type: ignore[arg-type]
            ["PR-001"],
        )
        == {}
    )


@pytest.mark.asyncio
async def test_recent_metrics_skips_missing_record_for_run_id() -> None:
    assert (
        await repo_control._recent_metrics_by_task_id(
            "example__alpha",
            FakeRedis(
                zsets={
                    "metrics:task_runs:example__alpha:PR-001": [
                        ("missing-run", 100),
                    ],
                }
            ),  # type: ignore[arg-type]
            ["PR-001"],
        )
        == {}
    )


@pytest.mark.asyncio
async def test_metrics_lookup_decodes_bytes_hash_records() -> None:
    context = await _panel_context(
        [_task("PR-001", TaskStatus.DONE)],
        FakeRedis(
            zsets={
                "metrics:task_runs:example__alpha:PR-001": [
                    (b"run-bytes", 100),  # type: ignore[list-item]
                ],
            },
            hashes={
                "metrics:run:run-bytes": {
                    b"coder": b"codex",
                    b"duration_ms": b"12345",
                },
            },
        ),
    )

    assert _views(context, "done")[0]["metrics"]["coder"] == "codex"
    assert _views(context, "done")[0]["metrics"]["duration_ms"] == "12345"


@pytest.mark.asyncio
async def test_metrics_lookup_supports_legacy_set_and_json_records() -> None:
    redis = LegacyMetricsRedis(
        sets={
            "metrics:task_runs:example__alpha:PR-001": {
                "run-old",
                b"run-new",
            },
        },
        strings={
            "metrics:run:run-old": (
                '{"profile_id": "claude:opus:container", '
                '"started_at": "2026-05-01T10:00:00+00:00", '
                '"duration_ms": 1000, "fix_iterations": 0, '
                '"exit_reason": "error"}'
            ),
            "metrics:run:run-new": (
                '{"profile_id": "codex:gpt-5:container", '
                '"ended_at": "2026-05-02T10:00:00+00:00", '
                '"duration_ms": 2000, "fix_iterations": 1, '
                '"exit_reason": "merged"}'
            ),
        },
    )

    context = await _panel_context(
        [_task("PR-001", TaskStatus.DONE)],
        redis,  # type: ignore[arg-type]
    )

    assert _views(context, "done")[0]["metrics"] == {
        "coder": "codex",
        "model": "gpt-5",
        "duration_ms": 2000,
        "fix_iterations": 1,
        "exit_reason": "merged",
    }


@pytest.mark.asyncio
async def test_metrics_lookup_skips_legacy_records_without_json_dict() -> None:
    redis = LegacyMetricsRedis(
        sets={"metrics:task_runs:example__alpha:PR-001": {"run-missing", "run-list"}},
        strings={"metrics:run:run-list": "[]"},
    )

    assert (
        await repo_control._recent_metrics_by_task_id(
            "example__alpha",
            redis,  # type: ignore[arg-type]
            ["PR-001"],
        )
        == {}
    )


def _render_tasks_panel(task_metrics: dict[str, object] | None) -> str:
    template = web_app.templates.get_template("components/tasks_panel.html")
    return template.render(
        repo_name="example__alpha",
        tasks_total=1,
        retry_cap=3,
        tasks_by_status={
            "doing": [],
            "todo": [],
            "error": [],
            "done": [
                {
                    "pr_id": "PR-001",
                    "title": "Inline metrics",
                    "branch": "pr-001-inline-metrics",
                    "metrics": task_metrics,
                }
            ],
        },
    )


def test_template_renders_metrics_row_when_present() -> None:
    rendered = _render_tasks_panel(
        {
            "coder": "codex",
            "model": "gpt-5",
            "duration_ms": "12345",
            "fix_iterations": "2",
            "exit_reason": "merged",
        }
    )

    assert "codex" in rendered
    assert "gpt-5" in rendered
    assert "12s" in rendered
    assert "2 fix" in rendered
    assert "merged" in rendered


def test_template_omits_metrics_row_when_absent() -> None:
    rendered = _render_tasks_panel(None)

    assert "codex" not in rendered
    assert "gpt-5" not in rendered
    assert "2 fix" not in rendered
