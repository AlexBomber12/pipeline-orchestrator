"""Global control feedback in Chromium and pause CAS against isolated Redis."""

import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import redis.asyncio as aioredis
from playwright.sync_api import Page, expect
from src.keyspace import control_stop, pipeline_state, repo_events_history
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web.routes.daemon_control import _write_user_paused

from tests.runner._helpers import _make_runner


def test_global_control_feedback(page: Page, dashboard_url: str) -> None:
    page.goto(dashboard_url, wait_until="domcontentloaded")
    page.wait_for_function("Boolean(window.htmx)")
    page.route("**/daemon/drain-progress", lambda route: route.fulfill(
        status=503, json={"repos": [{"name": "beta", "state": None}]},
    ))
    responses = [
        (207, [{"name": "alpha", "status": "accepted"},
               {"name": "beta", "status": "failed", "reason": "corrupt_state"}]),
        (503, [{"name": "beta", "status": "unavailable", "reason": "redis_unavailable"}]),
        (200, [{"name": "alpha", "status": "accepted"}]),
    ]
    for action, (status, results) in zip(["pause", "resume", "stop"], responses):
        page.route(f"**/daemon/{action}", lambda route, request, code=status, rows=results: route.fulfill(
            status=code, json={"results": rows},
        ))
    page.locator('[data-action="pause-all"]').click()
    expect(page.locator('#pause-all-affected-count')).to_have_text('?')
    page.locator('[data-pause-all-confirm]').click()
    output = page.locator('#pause-all-modal [role="status"]')
    expect(output).to_contain_text('Accepted 1/2 requests')
    expect(output).to_contain_text('beta (failed: corrupt_state)')
    expect(page.locator('[data-pause-all-confirm]')).to_be_enabled()
    page.locator('[data-pause-all-cancel]').click()
    page.locator('[data-action="resume-all"]').click()
    expect(page.locator('#global-control-result')).to_contain_text('Accepted 0/1 requests')
    expect(page.locator('#global-control-result')).to_contain_text('redis_unavailable')
    page.locator('[data-action="stop-all"]').click()
    page.locator('[data-stop-all-confirm]').click()
    expect(page.locator('#stop-all-modal [role="status"]')).to_contain_text(
        'Accepted 1/1 requests. Daemon application is pending.'
    )
    page.locator('[data-stop-all-cancel]').click()
    page.route("**/daemon/resume", lambda route: route.abort("failed"))
    page.locator('[data-action="resume-all"]').click()
    expect(page.locator('#global-control-result')).to_contain_text('Requests are unconfirmed')
    expect(page.locator('[data-action="resume-all"]')).to_be_enabled()


async def _exercise_concurrent_pause(redis: aioredis.Redis) -> None:
    """Force a daemon publication after the web transaction reads, before EXEC."""
    runner = _make_runner()
    runner.redis = redis
    runner.name = runner._old_basename = 'pr414__' + uuid4().hex
    runner.state.name = runner.name
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id='PR-414', status=TaskStatus.DOING, title='In flight', task_file='tasks/PR-414.md', branch='pr-414',
    )
    runner.state.current_pr = PRInfo(number=414, branch='pr-414', head_sha='a' * 40)
    key = pipeline_state(runner.name)
    original_transaction = redis.transaction
    reads = 0

    async def racing_transaction(callback, *keys, **kwargs):
        async def after_read(pipe):
            nonlocal reads
            original_get = type(pipe).get.__get__(pipe)

            async def get(key):
                nonlocal reads
                raw = await original_get(key)
                reads += 1
                if reads == 1:
                    # Publish using the production daemon writer, without recursively
                    # intercepting its transaction; the web's WATCH must now retry.
                    redis.transaction = original_transaction
                    runner.state.current_pr.head_sha = 'b' * 40
                    await runner.publish_state()
                return raw

            pipe.get = get
            await callback(pipe)
        return await original_transaction(after_read, *keys, **kwargs)

    try:
        await redis.set(key, runner.state.model_dump_json())
        redis.transaction = racing_transaction
        result = await asyncio.wait_for(_write_user_paused(
            redis, runner.name, runner.state.url, user_paused=True, clear_stop=False,
        ), timeout=10)
        assert result['status'] == 'accepted'
        assert reads == 2, 'Redis WATCH conflict must retry against fresh state'
        # A later stale in-memory daemon publication must also retain pause intent.
        runner.state.user_paused = False
        await runner.publish_state()
        state = RepoState.model_validate_json(await redis.get(key))
        assert state.user_paused is True
        assert state.state == PipelineState.CODING
        assert state.current_task.pr_id == 'PR-414'
        assert state.current_pr.number == 414
        assert state.current_pr.head_sha == 'b' * 40
        await redis.set(control_stop(runner.name), '1', ex=60)
        result = await _write_user_paused(
            redis, runner.name, runner.state.url, user_paused=False, clear_stop=True,
        )
        assert result['status'] == 'accepted'
        assert await redis.get(control_stop(runner.name)) is None
        assert RepoState.model_validate_json(await redis.get(key)).user_paused is False
    finally:
        redis.transaction = original_transaction
        await redis.delete(key, control_stop(runner.name), repo_events_history(runner.name))


def test_global_pause_concurrency_in_isolated_redis() -> None:
    # Dedicated e2e Redis DB 15; only the unique fixture-owned keys are deleted.
    address = subprocess.run(
        ['docker', 'inspect', '--format', '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}',
         'pipeline-orchestrator-redis-test'],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()
    assert address

    async def exercise():
        async with aioredis.Redis(host=address, db=15, decode_responses=True) as redis:
            await _exercise_concurrent_pause(redis)

    # Playwright's session-scoped sync fixture may own this thread's event loop.
    # Create, use and close the Redis client on a dedicated thread and loop.
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(lambda: asyncio.run(exercise())).result(timeout=30)
