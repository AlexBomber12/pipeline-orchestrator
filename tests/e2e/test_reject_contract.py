"""Real Redis characterization in the existing serialized GHA stack.

No live PR, taskbed lifecycle, or production rejection is exercised here.
"""

import asyncio
import subprocess
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
import redis.asyncio as aioredis
from src.cancellation.storage import cause_key, index_key
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus

from tests.test_reject_contract import exercise_close_transport


@pytest.mark.parametrize("outcome", ["confirmed", "lost_response", "not_applied"])
def test_reject_redis_receipt_expires_and_cannot_recover_close(monkeypatch, outcome, record_property):
    record_property("tested_sha", subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True, timeout=5,
    ).stdout.strip())
    address = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "pipeline-orchestrator-redis-test"],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()
    assert address

    async def exercise():
        redis = aioredis.Redis(host=address, port=6379, db=15, decode_responses=True)
        slug = f"reject-proof__{uuid.uuid4().hex}"
        state = RepoState(
            name=slug, url="https://github.com/octo/demo", state=PipelineState.ERROR,
            current_task=QueueTask(pr_id="PR-42", title="Proof", branch="fix/pr-42", status=TaskStatus.ERROR),
            current_pr=PRInfo(number=42, branch="fix/pr-42", pr_id="PR-42", head_sha="a" * 40),
        )
        cause = cause_key(slug, "PR-42")
        keys = [cause, index_key(slug), pipeline_state(slug)]
        try:
            await redis.ping()
            external = await exercise_close_transport(
                redis, state, SimpleNamespace(url=state.url), monkeypatch, outcome,
            )
            assert await redis.ttl(cause) > 0  # Cancellation history is not permanent intent.
            await redis.delete(*keys)  # Selective loss, never FLUSHDB on shared Redis.
            assert all(value is None for value in await redis.mget(*keys))
            from src.web.routes import repo_control

            replay = await repo_control._reject_guardrail_decision(
                slug, "PR-42", SimpleNamespace(url=state.url), redis,
            )
            assert replay.status_code == 404
            assert external["state"] == ("OPEN" if outcome == "not_applied" else "CLOSED")
        finally:
            await redis.delete(*keys)
            await redis.aclose()

    # The Playwright session can already own the main thread's asyncio loop.
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(lambda: asyncio.run(exercise())).result(timeout=30)
