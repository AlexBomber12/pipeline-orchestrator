"""Stop regressions; STOP_TEST_REDIS_URL repeats these against isolated Redis.

The real transport run waits beyond the retired TTL. Point it only at a
fixture-owned Redis instance; each test removes its own namespaced keys.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from src.keyspace import control_stop, pipeline_state
from src.models import PipelineState, PRInfo, PublicationWatermark, QueueTask, RepoState, TaskStatus
from src.web.routes import daemon_control, repo_control

from tests.runner import _helpers as h


@pytest.fixture
async def stopped_runner():
    runner = h._make_runner()
    runner.name = runner.state.name = runner._old_basename = f"stop-test-{uuid4().hex}"
    url = os.environ.get("STOP_TEST_REDIS_URL")
    if url:
        runner.redis = Redis.from_url(url, decode_responses=True)
    await runner.publish_state()
    try:
        yield runner
    finally:
        if url:
            keys = [key async for key in runner.redis.scan_iter(match=f"*{runner.name}*")]
            if keys:
                await runner.redis.delete(*keys)
            await runner.redis.aclose()


async def submit(runner, monkeypatch, scope, *, resume=False):
    if scope == "global":
        result = await daemon_control._write_user_paused(
            runner.redis, runner.name, runner.state.url,
            user_paused=not resume, clear_stop=resume, set_stop=not resume,
        )
        assert result["status"] == "accepted"
    else:
        async def target(*args):
            return runner.redis, pipeline_state(runner.name), runner.state.url

        monkeypatch.setattr(repo_control, "_apply_repo_control_update", target)
        result = await repo_control._update_repo_pause_state(
            SimpleNamespace(), runner.name, user_paused=not resume,
            stop_action="clear" if resume else "set",
        )
        assert isinstance(result, RepoState)


@pytest.mark.parametrize("scope", ["global", "repo"])
async def test_stop_survives_downtime_restart_and_duplicate(stopped_runner, monkeypatch, scope):
    runner = stopped_runner
    await submit(runner, monkeypatch, scope)
    key = control_stop(runner.name)
    assert await runner.redis.ttl(key) == -1
    # No daemon reads during downtime. Real Redis must retain the actual key
    # beyond 60 seconds, rather than merely advancing a Python clock.
    if os.environ.get("STOP_TEST_REDIS_URL"):
        await asyncio.sleep(61)
    assert await runner.redis.get(key) == "1"
    snapshot = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    fresh = h._make_runner()
    fresh.name = fresh._old_basename = runner.name
    fresh.redis, fresh.state = runner.redis, snapshot
    await submit(fresh, monkeypatch, scope)
    assert await fresh._pop_stop_request()
    assert await fresh._pop_stop_request()  # observation is no longer consumption
    assert await runner.redis.get(key) == "1"
    await fresh.publish_state()
    assert await runner.redis.get(key) is None
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert saved.user_paused


@pytest.mark.parametrize("phase", [PipelineState.CODING, PipelineState.FIX, PipelineState.MERGE, PipelineState.ERROR])
async def test_stop_waits_for_cleanup_and_park(stopped_runner, monkeypatch, phase):
    runner = stopped_runner
    runner.state.state = phase
    await runner.publish_state()
    await submit(runner, monkeypatch, "global")
    runner._coder_invocation_active = True
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) == "1"
    runner._coder_invocation_active = False
    await runner.publish_state()  # exit bookkeeping still owns the phase
    assert await runner.redis.get(control_stop(runner.name)) == "1"
    runner.state.state = PipelineState.PAUSED
    runner._coder_cleanup_failure_detail = "quiescence unconfirmed"
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) == "1"
    runner._coder_cleanup_failure_detail = None
    runner._current_coder_process = object()
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) == "1"
    runner._current_coder_process = None
    runner._current_coder_supervised_process = object()
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) == "1"
    runner._current_coder_supervised_process = None
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) is None


@pytest.mark.parametrize("scope", ["global", "repo"])
async def test_resume_cancels_old_observed_stop(stopped_runner, monkeypatch, scope):
    runner = stopped_runner
    await submit(runner, monkeypatch, scope)
    assert await runner._pop_stop_request()
    await submit(runner, monkeypatch, scope, resume=True)
    # Model the old reader finishing after Resume. Publishing must merge the
    # current intent instead of resurrecting the pause it observed earlier.
    runner.state.user_paused = True
    runner.state.state = PipelineState.PAUSED
    await runner.publish_state()
    assert not runner.state.user_paused
    assert not await runner._pop_stop_request()
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert not saved.user_paused


async def test_unconfirmed_pause_publication_retains_stop(stopped_runner, monkeypatch):
    runner = stopped_runner
    await submit(runner, monkeypatch, "global")
    transaction = runner.redis.transaction

    async def unavailable(callback, *keys, **kwargs):
        raise ConnectionError("EXEC unavailable")

    monkeypatch.setattr(runner.redis, "transaction", unavailable)
    with pytest.raises(ConnectionError, match="EXEC unavailable"):
        await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) == "1"
    monkeypatch.setattr(runner.redis, "transaction", transaction)
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) is None


async def test_resume_during_acknowledgment_retries_cas(stopped_runner, monkeypatch):
    runner = stopped_runner
    await submit(runner, monkeypatch, "global")
    refresh = runner._refresh_user_paused_from_redis
    resumed = False

    async def resume_after_watched_read():
        nonlocal resumed
        if not resumed:
            resumed = True
            await submit(runner, monkeypatch, "global", resume=True)
        await refresh()

    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", resume_after_watched_read)
    await runner.publish_state()
    assert resumed
    assert not runner.state.user_paused
    assert await runner.redis.get(control_stop(runner.name)) is None
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert not saved.user_paused


@pytest.mark.parametrize("confirmed", [False, True])
async def test_auxiliary_stop_retains_signal_until_supervisor_confirms(
    stopped_runner, monkeypatch, confirmed,
):
    runner = stopped_runner
    await submit(runner, monkeypatch, "global")
    process = SimpleNamespace(returncode=None)

    async def cleanup(**kwargs):
        # The signal must still be present while the supervisor is cleaning.
        assert await runner.redis.get(control_stop(runner.name)) == "1"
        return SimpleNamespace(quiescent=confirmed, detail="test cleanup")

    runner._current_coder_process = process
    runner._current_coder_supervised_process = SimpleNamespace(process=process, cleanup=cleanup)

    async def helper():
        await asyncio.Event().wait()

    result = await runner._await_auxiliary_coder(
        helper(), cleanup_context="stop regression", log_prefix="[TEST]",
        credential_reservation_held=True,
    )
    assert result is None
    await runner.publish_state()
    assert await runner.redis.get(control_stop(runner.name)) == (None if confirmed else "1")
    assert runner.state.state == (PipelineState.PAUSED if confirmed else PipelineState.ERROR)


@pytest.mark.parametrize("snapshot_kind", ["current", "legacy", "absent"])
async def test_startup_stop_parks_snapshot_until_resume(stopped_runner, monkeypatch, snapshot_kind):
    import json
    from unittest.mock import AsyncMock

    runner = stopped_runner
    state_key = pipeline_state(runner.name)
    runner.state.current_task = QueueTask(
        pr_id="PR-419", title="startup Stop", status=TaskStatus.DOING,
        branch="pr-419-recover-pending-uploads", task_file="tasks/PR-419.md",
    )
    runner.state.current_pr = PRInfo(
        number=419, branch=runner.state.current_task.branch, head_sha="a" * 40,
    )
    runner.state.publication_watermark = PublicationWatermark(
        repo=runner.name, head_repo=runner.name, pr_number=419,
        ref=runner.state.current_task.branch, head_sha="a" * 40,
        published_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    data = runner.state.model_dump(mode="json")
    data.update(state="CODING", last_updated="2026-01-01T00:00:00Z")
    # Unknown and omitted fields must survive without model normalization.
    data["recovery_provenance"] = {"head": "a" * 40}
    if snapshot_kind == "legacy":
        data.pop("publication_watermark", None)
        data.pop("publication_attempt", None)
    if snapshot_kind == "absent":
        await runner.redis.delete(state_key)
    else:
        await runner.redis.set(state_key, json.dumps(data))
    await runner.redis.set(control_stop(runner.name), "1")
    runner._recovered = False
    for method in ("ensure_repo_cloned", "recover_state", "handle_coding", "process_pending_uploads"):
        monkeypatch.setattr(runner, method, AsyncMock())
    runner.recover_state.return_value = False
    for _ in range(3):
        await runner.run_cycle()
    saved = json.loads(await runner.redis.get(state_key))
    assert saved["state"] == "PAUSED"
    assert saved["user_paused"]
    assert saved["last_updated"] != data["last_updated"]
    if snapshot_kind != "absent":
        for key in ("state", "user_paused", "last_updated"):
            saved.pop(key)
            data.pop(key)
        assert saved == data
    assert await runner.redis.get(control_stop(runner.name)) is None
    assert not runner._recovered
    assert runner.recover_state.await_count == 2
    runner.handle_coding.assert_not_awaited()
    runner.process_pending_uploads.assert_not_awaited()
    await submit(runner, monkeypatch, "global", resume=True)
    runner.recover_state.reset_mock()
    runner.recover_state.return_value = True
    await runner.run_cycle()
    runner.recover_state.assert_awaited_once()
    assert runner._recovered


async def test_startup_stop_rechecks_resume_before_ack(stopped_runner, monkeypatch):
    runner = stopped_runner
    await submit(runner, monkeypatch, "global")
    original = runner.redis.transaction

    async def resume_before_transaction(callback, *keys, **kwargs):
        monkeypatch.setattr(runner.redis, "transaction", original)
        await submit(runner, monkeypatch, "global", resume=True)
        return await original(callback, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", resume_before_transaction)
    runner._recovered = False
    await runner._park_startup_stop()
    assert not runner.state.user_paused
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert not saved.user_paused
    assert saved.state == PipelineState.IDLE


async def test_startup_stop_failed_transaction_retains_snapshot(stopped_runner, monkeypatch):
    runner = stopped_runner
    await submit(runner, monkeypatch, "global")
    before = await runner.redis.get(pipeline_state(runner.name))

    async def unavailable(*args, **kwargs):
        raise ConnectionError("EXEC unavailable")

    monkeypatch.setattr(runner.redis, "transaction", unavailable)
    with pytest.raises(ConnectionError, match="EXEC unavailable"):
        await runner._park_startup_stop()
    assert await runner.redis.get(pipeline_state(runner.name)) == before
    assert await runner.redis.get(control_stop(runner.name)) == "1"


async def test_recovered_stop_uses_normal_publication(stopped_runner, monkeypatch):
    from unittest.mock import AsyncMock

    runner = stopped_runner
    runner._recovered = True
    await submit(runner, monkeypatch, "global")
    monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock())
    monkeypatch.setattr(runner, "handle_idle", AsyncMock())
    await runner.run_cycle()
    saved = RepoState.model_validate_json(await runner.redis.get(pipeline_state(runner.name)))
    assert saved.user_paused
    assert await runner.redis.get(control_stop(runner.name)) is None
    runner.handle_idle.assert_not_awaited()
