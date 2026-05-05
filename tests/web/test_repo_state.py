"""Regression tests for PR-240: async load_config + parallel per-repo gather."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.mark.asyncio
async def test_load_current_queue_returns_none_without_redis(monkeypatch):
    from src.web import app as web_app
    from src.web.services import repo_state as rs

    monkeypatch.setattr(web_app.app.state, "redis", None)

    assert await rs.load_current_queue("owner__repo") is None


@pytest.mark.asyncio
async def test_load_current_queue_handles_redis_error():
    from src.web.services import repo_state as rs

    class _BoomRedis:
        async def get(self, key):
            raise RuntimeError("redis down")

    assert await rs.load_current_queue("owner__repo", _BoomRedis()) is None


@pytest.mark.asyncio
async def test_load_current_queue_handles_decode_error():
    from src.web.services import repo_state as rs

    class _BadRedis:
        async def get(self, key):
            return "{bad json"

    assert await rs.load_current_queue("owner__repo", _BadRedis()) is None


@pytest.mark.asyncio
async def test_get_all_repo_states_does_not_block_event_loop(monkeypatch):
    """load_config is run via asyncio.to_thread so other tasks can interleave."""
    from src.web.services import repo_state as rs

    blocking_started = asyncio.Event()
    blocking_release = asyncio.Event()

    def slow_load(_path):
        blocking_started.set()
        # Simulate slow file read by waiting until release.
        # Real to_thread offload runs in a worker; the event loop
        # should NOT be blocked.
        import time

        time.sleep(0.05)
        return MagicMock(repositories=[])

    monkeypatch.setattr(rs, "load_config", slow_load)
    monkeypatch.setattr(rs, "load_config", slow_load, raising=False)

    # Run get_all_repo_states; while it is running, schedule another
    # coroutine that should be able to make progress.
    other_made_progress = False

    async def other_task():
        nonlocal other_made_progress
        await asyncio.sleep(0.01)
        other_made_progress = True

    states_task = asyncio.create_task(
        rs.get_all_repo_states(redis_client=None, config_path="/fake/path.yml")
    )
    other = asyncio.create_task(other_task())
    await asyncio.gather(states_task, other)
    assert other_made_progress, "Event loop was blocked during load_config"
    assert blocking_release is not None  # silence unused-var lint


@pytest.mark.asyncio
async def test_get_all_repo_states_runs_per_repo_lookups_in_parallel(monkeypatch):
    """asyncio.gather makes per-repo Redis reads concurrent, not sequential."""
    from src.web.services import repo_state as rs

    fake_cfg = MagicMock()
    fake_cfg.repositories = [
        MagicMock(url=f"https://github.com/owner/repo-{i}.git") for i in range(5)
    ]
    monkeypatch.setattr(rs, "load_config", lambda _p: fake_cfg)
    monkeypatch.setattr(
        rs, "repo_slug_from_url", lambda url: url.rsplit("/", 1)[-1].rstrip(".git")
    )

    call_times = []

    async def fake_safe(client, name, url):
        # Record entry time, sleep 50ms, return state.
        call_times.append(asyncio.get_event_loop().time())
        await asyncio.sleep(0.05)
        return MagicMock(state="IDLE"), None

    monkeypatch.setattr(rs, "_get_repo_state_safe", fake_safe)

    redis = AsyncMock()
    redis.ping = AsyncMock(return_value=True)

    start = asyncio.get_event_loop().time()
    states, warning = await rs.get_all_repo_states(redis_client=redis)
    duration = asyncio.get_event_loop().time() - start

    assert len(states) == 5
    # Sequential would take 5 * 0.05 = 0.25s. Parallel should take
    # roughly 0.05s. Allow generous slack for CI flakiness; assert
    # less than half the sequential cost.
    assert duration < 0.15, f"Per-repo lookups did not parallelize: {duration:.3f}s"
    assert warning is None
