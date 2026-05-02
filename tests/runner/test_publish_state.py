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
    runner._last_published_state_value = runner.state.state.value
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


def test_publish_state_emits_state_change_on_first_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first publish_state call seeds _last_published_state_value and
    emits an SSE state_change so reconnecting dashboards see the current
    state without waiting for the next transition."""
    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert state_events == [
        (runner.name, "state_change", {"state": runner.state.state.value}, runner.redis)
    ]
    assert runner._last_published_state_value == runner.state.state.value


def test_publish_state_emits_state_change_on_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    asyncio.run(runner.publish_state())
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.publish_state())
    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert [event[2]["state"] for event in state_events] == ["IDLE", "WATCH"]


def test_publish_state_drains_pending_event_log_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner._last_published_state_value = runner.state.state.value
    runner.log_event("first event")
    runner.log_event("second event")

    asyncio.run(runner.publish_state())

    appended = [event for event in published if event[1] == "event_log_append"]
    assert [event[2]["entry"]["event"] for event in appended] == [
        "first event",
        "second event",
    ]
    assert runner._pending_event_log_entries == []


def test_publish_pending_event_log_entries_requeues_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient publish failure must keep the unsent entries (failed
    plus not-yet-attempted) at the head of the queue so a later cycle
    can retry — otherwise live event_log_append events are silently
    dropped while history is already persisted in state.history."""
    call_count = {"n": 0}
    published: list[dict[str, object]] = []

    async def _flaky_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise RuntimeError("redis offline")
        published.append(payload)

    monkeypatch.setattr(runner_module, "publish_repo_event", _flaky_publish_repo_event)

    runner = _make_runner()
    runner._last_published_state_value = runner.state.state.value
    runner.log_event("first event")
    runner.log_event("second event")
    runner.log_event("third event")

    with pytest.raises(RuntimeError, match="redis offline"):
        asyncio.run(runner.publish_state())

    # First entry was published; second failed mid-flight and must be
    # retained alongside the third (not-yet-attempted) entry.
    assert [payload["entry"]["event"] for payload in published] == ["first event"]
    assert [
        entry["event"] for entry in runner._pending_event_log_entries
    ] == ["second event", "third event"]


def test_publish_pending_event_log_entries_retry_drains_remainder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a transient failure clears, the next publish_state must
    drain the re-queued entries in original FIFO order."""
    fail_next = {"flag": True}
    published: list[str] = []

    async def _publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        if event_type != "event_log_append":
            return
        if fail_next["flag"]:
            fail_next["flag"] = False
            raise RuntimeError("redis offline")
        published.append(payload["entry"]["event"])

    monkeypatch.setattr(runner_module, "publish_repo_event", _publish_repo_event)

    runner = _make_runner()
    runner._last_published_state_value = runner.state.state.value
    runner.log_event("first event")
    runner.log_event("second event")

    with pytest.raises(RuntimeError, match="redis offline"):
        asyncio.run(runner.publish_state())

    # Retry succeeds and drains both entries in FIFO order.
    asyncio.run(runner.publish_state())
    assert published == ["first event", "second event"]
    assert runner._pending_event_log_entries == []


def test_log_event_dedup_queues_updated_entry_for_event_log_append() -> None:
    """Dedup must still publish an event_log_append so live counter
    updates ("waiting 1/20m" -> "waiting 2/20m") reach subscribers."""
    runner = _make_runner()
    runner.log_event("waiting (1/20m)")
    runner.log_event("waiting (2/20m)")

    queued = [
        (entry["event"], entry["count"])
        for entry in runner._pending_event_log_entries
    ]
    assert queued == [("waiting (1/20m)", 1), ("waiting (2/20m)", 2)]


def test_save_current_run_record_emits_pr_metrics_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Finalizing a RunRecord publishes pr_metrics_update so the dashboard's
    Recent PRs panel refreshes via SSE rather than the legacy 60s poll."""
    from src.metrics import RunRecord

    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    async def _fake_save(record: object) -> None:
        return None

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner._current_run_record = RunRecord(
        run_id="run-1",
        task_id="PR-300",
        profile_id="claude:claude-opus-4-7:container",
        task_type="feature",
        complexity="low",
        started_at=datetime.now(timezone.utc).isoformat(),
        ended_at=None,
        duration_ms=None,
        fix_iterations=0,
        tokens_in=0,
        tokens_out=0,
        exit_reason="",
        operator_intervention=False,
        repo_name=runner.name,
        stage="coder",
    )
    monkeypatch.setattr(runner._metrics_store, "save", _fake_save)

    asyncio.run(runner._save_current_run_record("coding_complete"))

    metrics_events = [event for event in published if event[1] == "pr_metrics_update"]
    assert metrics_events == [
        (
            runner.name,
            "pr_metrics_update",
            {"task_id": "PR-300", "exit_reason": "coding_complete"},
            runner.redis,
        )
    ]


def test_save_current_run_record_noop_when_no_active_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without an active RunRecord nothing publishes, matching the
    early-return contract."""
    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner()
    runner._current_run_record = None

    asyncio.run(runner._save_current_run_record("error"))

    assert published == []


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
