"""PR-224b: publish_state and Redis serialization tests.

Mechanical move from tests/test_runner.py. Helpers live in
``tests/runner/_helpers.py``.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest
from src.models import PipelineState, PRInfo, RepoState

from tests.runner._helpers import (
    _allow_all_coder_auth,
    _FakeRedis,
    _make_runner,
    runner_module,
)


def test_publish_state_skips_progress_update_when_value_was_already_published(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, str, dict[str, int], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, int],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner._last_published_queue_progress = (1, 2)
    runner._set_queue_progress(1, 2)

    asyncio.run(runner.publish_state())

    assert published == []
    assert runner._queue_progress_dirty is False


def test_publish_state_writes_to_redis() -> None:
    runner = _make_runner()
    asyncio.run(runner.publish_state())

    assert isinstance(runner.redis, _FakeRedis)
    assert len(runner.redis.writes) == 1
    key, payload = runner.redis.writes[0]
    assert key == f"pipeline:{runner.name}"
    assert runner.name in payload


def test_publish_state_keeps_selected_fallback_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _allow_all_coder_auth(monkeypatch)
    runner = _make_runner()
    runner.state.rate_limited_coders.add("claude")

    name, _plugin = runner._get_coder()
    asyncio.run(runner.publish_state())

    assert name == "codex"
    assert runner.state.coder == "codex"


def test_publish_state_for_inactive_repo_forces_idle_payload() -> None:
    runner = _make_runner(active=False)
    runner.state.state = PipelineState.ERROR

    asyncio.run(runner.publish_state())

    assert runner.redis.writes
    _key, payload = runner.redis.writes[-1]
    state = json.loads(payload)
    assert state["state"] == PipelineState.IDLE.value
    assert runner.state.state == PipelineState.ERROR


def test_publish_state_migrates_owned_legacy_upload_key() -> None:
    runner = _make_runner(url="https://github.com/octo/demo-renamed.git")
    assert isinstance(runner.redis, _FakeRedis)
    runner._old_basename = "demo"
    runner.redis.store["pipeline:demo"] = json.dumps(
        {"url": "https://github.com/octo/demo-renamed.git"}
    )
    runner.redis.store["upload:demo:pending"] = "pending"

    asyncio.run(runner.publish_state())

    assert "pipeline:demo" not in runner.redis.store
    assert f"pipeline:{runner.name}" in runner.redis.store
    assert "upload:demo:pending" not in runner.redis.store
    assert runner.redis.store[f"upload:{runner.name}:pending"] == "pending"


def test_publish_state_ignores_legacy_upload_migration_error() -> None:
    class _BrokenRenameRedis(_FakeRedis):
        async def exists(self, key: str) -> int:
            raise RuntimeError("rename failed")

    runner = _make_runner(url="https://github.com/octo/demo-renamed.git")
    runner.redis = _BrokenRenameRedis()
    runner._old_basename = "demo"
    runner.redis.store["pipeline:demo"] = json.dumps(
        {"url": "https://github.com/octo/demo-renamed.git"}
    )
    runner.redis.store["upload:demo:pending"] = "pending"

    asyncio.run(runner.publish_state())

    assert runner.redis.store[f"pipeline:{runner.name}"]
    assert runner.redis.store["upload:demo:pending"] == "pending"


def test_publish_state_ignores_invalid_persisted_state_during_transaction() -> None:
    runner = _make_runner()
    runner.redis.store[f"pipeline:{runner.name}"] = "{not-json"

    asyncio.run(runner.publish_state())

    stored = RepoState.model_validate_json(runner.redis.store[f"pipeline:{runner.name}"])
    assert stored.name == runner.name


def test_publish_while_waiting_handles_publish_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warnings: list[str] = []
    sleep_calls = {"count": 0}
    runner = _make_runner()

    async def fake_sleep(seconds: float) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] > 1:
            raise asyncio.CancelledError()

    async def fake_publish_state() -> None:
        raise RuntimeError("redis offline")

    monkeypatch.setattr(runner_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(runner._publish_while_waiting("heartbeat"))

    assert warnings == [f"[{runner.name}] heartbeat publish failed, will retry"]


def test_repo_state_resets_codex_retrigger_on_pr_transition() -> None:
    state = RepoState(
        url="https://github.com/octo/demo",
        name="octo__demo",
        last_updated=datetime.now(timezone.utc),
    )
    state.current_pr = PRInfo(number=1, branch="pr-001")
    state.last_codex_retrigger_at = datetime.now(timezone.utc)

    state.current_pr = PRInfo(number=2, branch="pr-002")

    assert state.last_codex_retrigger_at is None
