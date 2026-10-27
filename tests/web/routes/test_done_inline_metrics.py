from __future__ import annotations

import json
from dataclasses import asdict, replace
from typing import Any

import pytest
from src.metrics import RunRecord
from src.models import QueueTask, TaskStatus
from src.web import app as web_app
from src.web.routes import repo_control


class MetricsRedis:
    """Only the writer's SET index and JSON string record transport."""

    def __init__(self, records: list[RunRecord], *, broken_task: str = ""):
        self.records = records
        self.broken_task = broken_task

    async def hget(self, key, field):
        return None

    async def get(self, key):
        return None

    async def smembers(self, key):
        if key.endswith(":" + self.broken_task):
            raise ConnectionError("storage down")
        return {r.run_id.encode() for r in self.records if key.endswith(":" + r.task_id)}

    async def mget(self, keys):
        return [json.dumps(asdict(r)).encode() for r in self.records
                if "metrics:run:" + r.run_id in keys]


def _record(**changes) -> RunRecord:
    return replace(RunRecord(
        run_id="run-1", task_id="PR-001", profile_id="codex:gpt-5:container",
        task_type="bugfix", complexity="low", started_at="2026-05-01T10:00:00+00:00",
        ended_at=None, duration_ms=12345, fix_iterations=2, tokens_in=0,
        tokens_out=0, exit_reason="success_merged", operator_intervention=False,
        repo_name="example__alpha",
    ), **changes)


def _task(pr_id: str, status: TaskStatus) -> QueueTask:
    return QueueTask(pr_id=pr_id, title=f"{pr_id} title", status=status,
                     branch=f"{pr_id.lower()}-branch")


async def _panel(tasks, redis_client):
    return await repo_control._build_tasks_panel_context(
        "example__alpha", tasks, redis_client=redis_client, retry_cap=3,
    )


def _views(context: dict[str, Any], status: str):
    return context["tasks_by_status"][status]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [TaskStatus.DONE, TaskStatus.ERROR])
async def test_latest_failed_fix_is_visible(status):
    context = await _panel([_task("PR-001", status)], MetricsRedis([
        _record(ended_at="2026-05-02T10:00:00+00:00"),
        _record(run_id="run-2", started_at="2026-05-03T10:00:00+00:00",
                run_phase="fix", exit_reason="error", outcome="failed", cause="CRASH"),
    ]))
    metrics = _views(context, status.value.lower())[0]["metrics"]
    assert metrics == {"coder": "codex", "model": "gpt-5", "duration_ms": 12345,
                       "fix_iterations": 2, "exit_reason": "error"}
    rendered = web_app.templates.get_template("components/tasks_panel.html").render(
        **context,
    )
    assert "codex" in rendered and "error</span>" in rendered and "2 fix" in rendered


@pytest.mark.asyncio
async def test_recency_uses_end_time_then_run_id():
    records = [
        _record(run_id="a", ended_at="2026-05-04", duration_ms=10),
        _record(run_id="z", ended_at="2026-05-04", duration_ms=20),
        _record(run_id="new-start", started_at="2026-05-03", duration_ms=30),
    ]
    for ordered in (records, list(reversed(records))):
        result = await repo_control._recent_metrics_by_task_id(
            "example__alpha", MetricsRedis(ordered), ["PR-001"],
        )
        assert result["PR-001"]["duration_ms"] == 20


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [TaskStatus.TODO, TaskStatus.DOING])
async def test_active_tasks_have_no_terminal_metrics(status):
    context = await _panel([_task("PR-001", status)], MetricsRedis([_record()]))
    assert "metrics" not in _views(context, status.value.lower())[0]


@pytest.mark.asyncio
async def test_missing_and_down_storage_preserve_tasks_and_unrelated_metrics():
    tasks = [_task(pr_id, TaskStatus.DONE) for pr_id in ("PR-001", "PR-002", "PR-003")]
    context = await _panel(tasks, MetricsRedis([_record()], broken_task="PR-002"))
    views = _views(context, "done")
    assert [v["pr_id"] for v in views] == [t.pr_id for t in tasks]
    assert views[0]["metrics"]["coder"] == "codex"
    assert views[1]["metrics"] is None and views[2]["metrics"] is None
    assert await repo_control._recent_metrics_by_task_id("example__alpha", None, ["PR-001"]) == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["[]", "{", '{"run_id": "incomplete"}', None])
async def test_bad_or_expired_record_does_not_hide_unrelated_tasks(raw):
    class BadRecordRedis(MetricsRedis):
        async def mget(self, keys):
            if "metrics:run:bad" in keys:
                return [raw]
            return await super().mget(keys)

    context = await _panel(
        [_task("PR-002", TaskStatus.DONE), _task("PR-001", TaskStatus.DONE)],
        BadRecordRedis([_record(), _record(run_id="bad", task_id="PR-002")]),
    )
    assert _views(context, "done")[0]["metrics"] is None
    assert _views(context, "done")[1]["metrics"]["coder"] == "codex"


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,duration", [("", None), ("codex", 0)])
async def test_unavailable_fields_are_not_defaulted(profile, duration):
    result = await repo_control._recent_metrics_by_task_id(
        "example__alpha", MetricsRedis([_record(profile_id=profile, duration_ms=duration)]), ["PR-001"],
    )
    assert result["PR-001"]["duration_ms"] == duration
    assert result["PR-001"]["model"] is None
    assert result["PR-001"]["coder"] == (profile or None)
    assert "cost" not in result["PR-001"]


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


@pytest.mark.parametrize("duration,expected", [(None, False), (0, True)])
def test_template_distinguishes_missing_duration_from_zero(duration, expected):
    rendered = _render_tasks_panel({
        "coder": None, "model": None, "duration_ms": duration,
        "fix_iterations": 0, "exit_reason": "error",
    })
    assert ("0s</span>" in rendered) is expected
    assert "0 fix" in rendered
    assert "$0" not in rendered
