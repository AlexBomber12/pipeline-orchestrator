"""Real MetricsStore SET/string round trip through the task panel renderer."""

import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pytest
import redis.asyncio as aioredis
from src.metrics import MetricsStore, RunRecord
from src.models import InvocationSnapshot, QueueTask, TaskStatus
from src.web import app as web_app
from src.web.routes.repo_control import _build_tasks_panel_context


@pytest.mark.parametrize("with_dispatch_evidence", [True, False], ids=["current", "legacy"])
def test_task_metrics_writer_reader_round_trip(with_dispatch_evidence: bool) -> None:
    """Use isolated DB 15 and delete only keys owned by this test."""
    address = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "pipeline-orchestrator-redis-test"],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()
    assert address
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(lambda: asyncio.run(_exercise(
            address, with_dispatch_evidence=with_dispatch_evidence,
        ))).result(timeout=30)


async def _exercise(address: str, port: int = 6379, *, with_dispatch_evidence: bool = True) -> None:
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
            # This continuation inherited counter 2 but dispatched only FIX 2.
            invocations=[InvocationSnapshot(
                plugin_id="codex", model_override="gpt-5", reasoning_effort_override=None,
                run_id=slug + "-new", attempt_index=2, phase="fix", fix_iteration=2,
            )] if with_dispatch_evidence else [],
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
        assert metrics["exit_reason"] == "error"
        assert metrics["attempt_count"] == 2
        assert metrics["fix_iterations"] == (1 if with_dispatch_evidence else None)
        assert metrics["telemetry"]["fix_iterations"] == {
            "status": "partial" if with_dispatch_evidence else "unknown",
            "known_runs": 1 if with_dispatch_evidence else 0, "total_runs": 2,
        }
        assert metrics["duration_ms"] == 12000
        assert metrics["telemetry"]["duration_ms"] == {
            "status": "partial", "known_runs": 1, "total_runs": 2,
        }
        assert metrics["metered_cost_usd"] is None and metrics["subscription_quota"] is None
        assert all(v["metrics"] is None for v in context["tasks_by_status"]["done"])
        rendered = web_app.templates.get_template("components/tasks_panel.html").render(
            **context,
        )
        assert all(pr_id in rendered for pr_id in ("PR-001", "PR-002", "PR-003"))
        assert "codex" in rendered and "gpt-5" in rendered and "2 runs" in rendered
        assert ("1 fix" if with_dispatch_evidence else "fix unknown") in rendered
        assert "error</span>" in rendered and "12s" in rendered
        assert "partial; 1/2 runs known" in rendered
        assert "API price unavailable" in rendered and "Subscription quota unavailable" in rendered
        assert "$0" not in rendered
        assert before == {key: await redis.dump(key) for key in keys}
    finally:
        await redis.delete(*keys)
        await redis.aclose()
