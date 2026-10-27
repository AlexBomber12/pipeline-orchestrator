"""Real MetricsStore SET/string round trip through the task panel renderer."""

import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import redis.asyncio as aioredis
from src.metrics import MetricsStore, RunRecord
from src.models import QueueTask, TaskStatus
from src.web import app as web_app
from src.web.routes.repo_control import _build_tasks_panel_context


def test_task_metrics_writer_reader_round_trip() -> None:
    """Use isolated DB 15 and delete only keys owned by this test."""
    address = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "pipeline-orchestrator-redis-test"],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()
    assert address
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(lambda: asyncio.run(_exercise(address))).result(timeout=30)


async def _exercise(address: str, port: int = 6379) -> None:
    slug = "metrics-test-" + uuid4().hex
    redis = aioredis.Redis(host=address, port=port, db=15, decode_responses=False)
    store = MetricsStore(redis)
    record = RunRecord(
        run_id=slug + "-old", task_id="PR-001", profile_id="codex:gpt-5:container",
        task_type="bugfix", complexity="low", started_at="2026-05-01T10:00:00+00:00",
        ended_at=None, duration_ms=12000, fix_iterations=0, tokens_in=0,
        tokens_out=0, exit_reason="success_merged", operator_intervention=False,
        repo_name=slug,
    )
    keys = [f"metrics:repo:{slug}:PR"]
    keys += [f"metrics:task_runs:{slug}:PR-00{i}" for i in (1, 2)]
    keys += [f"metrics:run:{slug}-{suffix}" for suffix in ("old", "new", "bad")]
    try:
        await store.save(record)
        await store.save(replace(
            record, run_id=slug + "-new", started_at="2026-05-02T10:00:00+00:00",
            duration_ms=None, fix_iterations=2, run_phase="fix", attempt_index=2,
            exit_reason="error", outcome="failed", cause="CRASH",
        ))
        await store.save(replace(record, run_id=slug + "-bad", task_id="PR-002"))
        await redis.set(f"metrics:run:{slug}-bad", "{malformed")
        assert await redis.type(f"metrics:task_runs:{slug}:PR-001") == b"set"
        assert await redis.type(f"metrics:run:{slug}-new") == b"string"
        before = {key: await redis.dump(key) for key in keys}
        context = await _build_tasks_panel_context(
            slug, [QueueTask(pr_id=f"PR-00{i}", title=f"Task {i}",
                             status=TaskStatus.DONE if i != 1 else TaskStatus.ERROR,
                             branch=f"branch-{i}") for i in (1, 2, 3)],
            redis_client=redis, retry_cap=3,
        )
        metrics = context["tasks_by_status"]["error"][0]["metrics"]
        assert metrics["exit_reason"] == "error" and metrics["fix_iterations"] == 2
        assert metrics["duration_ms"] is None and "cost" not in metrics
        assert all(v["metrics"] is None for v in context["tasks_by_status"]["done"])
        rendered = web_app.templates.get_template("components/tasks_panel.html").render(
            **context,
        )
        assert all(pr_id in rendered for pr_id in ("PR-001", "PR-002", "PR-003"))
        assert "codex" in rendered and "gpt-5" in rendered and "2 fix" in rendered
        assert "error</span>" in rendered and "12s</span>" not in rendered
        assert "$0" not in rendered
        assert before == {key: await redis.dump(key) for key in keys}
    finally:
        await redis.delete(*keys)
        await redis.aclose()
