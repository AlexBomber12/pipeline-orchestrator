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
    runner._last_published_state_signature = (
        runner.state.state.value,
        (),
        (None, None, False),
        None,
    )
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
    """The first publish_state call seeds _last_published_state_signature
    and emits an SSE state_change so reconnecting dashboards see the
    current state without waiting for the next transition."""
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
    assert runner._last_published_state_signature == (
        runner.state.state.value,
        (),
        (None, None, False),
        None,
    )


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


def test_publish_state_emits_state_change_on_pr_field_change_in_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """While the runner stays in WATCH, ``handle_watch`` still mutates
    ``current_pr`` (CI conclusion, review status, push count) as it
    polls. Each visible mutation must publish a fresh state_change so
    the repo-detail summary refreshes — otherwise CI/review changes
    would only surface on a state.state transition or manual reload."""
    from src.models import CIStatus, PRInfo, ReviewStatus

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
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        push_count=1,
        commits_count=3,
    )
    asyncio.run(runner.publish_state())

    # Identical PR signature on the next cycle -> no extra publish.
    asyncio.run(runner.publish_state())

    # CI conclusion changes mid-WATCH -> must publish.
    runner.state.current_pr = runner.state.current_pr.model_copy(
        update={"ci_status": CIStatus.SUCCESS}
    )
    asyncio.run(runner.publish_state())

    # Review status changes mid-WATCH -> must publish.
    runner.state.current_pr = runner.state.current_pr.model_copy(
        update={"review_status": ReviewStatus.CHANGES_REQUESTED}
    )
    asyncio.run(runner.publish_state())

    # New push observed mid-WATCH -> must publish.
    runner.state.current_pr = runner.state.current_pr.model_copy(
        update={"push_count": 2}
    )
    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert len(state_events) == 4
    assert all(event[2] == {"state": "WATCH"} for event in state_events)


def test_publish_state_emits_state_change_on_merge_phase_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-352: while the runner stays in MERGE, ``handle_merge`` advances
    ``state.merge_phase`` through pre_merge_sync → ready_to_merge → merging
    → post_merge_cleanup. ``state.state`` stays MERGE and the PR/usage
    fields do not move during these sub-steps, so without ``merge_phase``
    in the SSE signature the dashboard cards would stay on the previous
    phase until the 30s polling fallback fires."""
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
    runner.state.state = PipelineState.MERGE
    runner.state.merge_phase = "pre_merge_sync"
    asyncio.run(runner.publish_state())

    # Identical phase on the next cycle -> no extra publish.
    asyncio.run(runner.publish_state())

    runner.state.merge_phase = "ready_to_merge"
    asyncio.run(runner.publish_state())

    runner.state.merge_phase = "merging"
    asyncio.run(runner.publish_state())

    runner.state.merge_phase = "post_merge_cleanup"
    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert len(state_events) == 4
    assert all(event[2] == {"state": "MERGE"} for event in state_events)


def test_publish_state_emits_state_change_on_usage_field_change(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """publish_state refreshes usage_session_percent / usage_weekly_percent
    / usage_api_degraded every cycle. The summary's rate-limit badge
    renders all three, so an unchanged state.state plus unchanged PR
    signature must NOT mask a usage refresh — otherwise the badge
    can stay stale through a long WATCH/IDLE stretch until an unrelated
    transition fires."""
    from tests.runner._helpers import _FakeUsageProvider

    class _UsageSnap:
        def __init__(
            self,
            session_percent: int | None,
            weekly_percent: int | None,
        ) -> None:
            self.session_percent = session_percent
            self.session_resets_at = None
            self.weekly_percent = weekly_percent
            self.weekly_resets_at = None

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
    runner.state.state = PipelineState.WATCH
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=_UsageSnap(40, 25)
    )
    asyncio.run(runner.publish_state())

    # Identical usage on the next cycle -> no extra publish.
    asyncio.run(runner.publish_state())

    # Session percent ticks up -> must publish so the badge refreshes.
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=_UsageSnap(55, 25)
    )
    asyncio.run(runner.publish_state())

    # Weekly percent changes -> must publish.
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=_UsageSnap(55, 30)
    )
    asyncio.run(runner.publish_state())

    # Usage API degrades -> must publish so the warning badge appears.
    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=_UsageSnap(55, 30), failures=10
    )
    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert len(state_events) == 4
    assert all(event[2] == {"state": "WATCH"} for event in state_events)


def test_publish_state_change_for_inactive_repo_emits_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inactive repos publish IDLE in the persisted payload — the
    state_change SSE must mirror that, not leak self.state.state.value
    (e.g. ERROR), or repo.html stays stuck on the pre-deactivation
    state until manual reload."""
    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(runner_module, "publish_repo_event", _fake_publish_repo_event)

    runner = _make_runner(active=False)
    runner.state.state = PipelineState.ERROR

    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert state_events == [
        (runner.name, "state_change", {"state": PipelineState.IDLE.value}, runner.redis)
    ]
    assert runner._last_published_state_signature == (
        PipelineState.IDLE.value,
        (),
        (None, None, False),
        None,
    )


def test_publish_state_change_emits_idle_on_deactivation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Toggling active=False after a live state must publish a
    transition to IDLE so SSE-driven views see the deactivation."""
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
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.publish_state())

    runner.repo_config.active = False
    asyncio.run(runner.publish_state())

    state_events = [event for event in published if event[1] == "state_change"]
    assert [event[2]["state"] for event in state_events] == [
        PipelineState.WATCH.value,
        PipelineState.IDLE.value,
    ]


def test_publish_state_change_swallows_publish_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A state_change SSE failure must not abort publish_state: the
    authoritative state was already persisted to Redis above, so a
    transient pub/sub blip should not turn a UI notification miss into
    control-flow failure that stalls the runner cycle. The signature
    stays unchanged so the next cycle retries automatically."""
    warnings: list[str] = []
    call_count = {"n": 0}

    async def _failing_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        if event_type != "state_change":
            return
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("redis pubsub down")

    monkeypatch.setattr(runner_module, "publish_repo_event", _failing_publish_repo_event)

    runner = _make_runner()
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )

    asyncio.run(runner.publish_state())

    assert any("state_change publish failed" in entry for entry in warnings)
    assert runner._last_published_state_signature is None

    asyncio.run(runner.publish_state())

    assert call_count["n"] == 2
    assert runner._last_published_state_signature == (
        runner.state.state.value,
        (),
        (None, None, False),
        None,
    )


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
    runner._last_published_state_signature = (
        runner.state.state.value,
        (),
        (None, None, False),
        None,
    )
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
    dropped while history is already persisted in state.history. The
    failure is logged and swallowed: state was already persisted before
    we got here, and re-raising would abort the runner cycle on every
    transient pub/sub blip."""
    call_count = {"n": 0}
    published: list[dict[str, object]] = []
    warnings: list[str] = []

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
    runner._last_published_state_signature = (
        runner.state.state.value,
        (),
        (None, None, False),
        None,
    )
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )
    runner.log_event("first event")
    runner.log_event("second event")
    runner.log_event("third event")

    asyncio.run(runner.publish_state())

    # First entry was published; second failed mid-flight and must be
    # retained alongside the third (not-yet-attempted) entry.
    assert [payload["entry"]["event"] for payload in published] == ["first event"]
    assert [
        entry["event"] for entry in runner._pending_event_log_entries
    ] == ["second event", "third event"]
    assert any(
        "event_log_append publish failed" in entry for entry in warnings
    )


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
    runner._last_published_state_signature = (
        runner.state.state.value,
        (),
        (None, None, False),
        None,
    )
    runner.log_event("first event")
    runner.log_event("second event")

    # Failure is swallowed (state already persisted; abort would stall
    # the runner). Entries stay queued for the next cycle.
    asyncio.run(runner.publish_state())
    assert published == []
    assert [
        entry["event"] for entry in runner._pending_event_log_entries
    ] == ["first event", "second event"]

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


def test_save_current_run_record_swallows_publish_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pr_metrics_update SSE failure must not abort the cycle: the
    run record was already persisted, and callers depend on
    _save_current_run_record returning so post-save logic (state
    transitions, review trigger, etc.) still runs. Otherwise a
    transient Redis pub/sub blip turns a UI notification miss into
    control-flow failure."""
    from src.metrics import RunRecord

    warnings: list[str] = []

    async def _failing_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        raise RuntimeError("redis pubsub down")

    async def _fake_save(record: object) -> None:
        return None

    monkeypatch.setattr(runner_module, "publish_repo_event", _failing_publish_repo_event)

    runner = _make_runner()
    monkeypatch.setattr(runner._metrics_store, "save", _fake_save)
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )
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

    asyncio.run(runner._save_current_run_record("coding_complete"))

    assert any("pr_metrics_update publish failed" in entry for entry in warnings)


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


def test_publish_state_populates_inhibitors() -> None:
    """User-paused state must surface as a USER_PAUSE inhibitor on the
    persisted RepoState payload."""
    from src.inhibitor import InhibitorType

    runner = _make_runner()
    runner.state.user_paused = True

    asyncio.run(runner.publish_state())

    assert isinstance(runner.redis, _FakeRedis)
    _key, payload = runner.redis.writes[-1]
    persisted = RepoState.model_validate_json(payload)
    assert len(persisted.active_inhibitors) == 1
    entry = persisted.active_inhibitors[0]
    assert entry.inhibitor_type is InhibitorType.USER_PAUSE
    assert entry.source_key == f"state:{runner.name}.user_paused"
    assert runner.state.active_inhibitors == persisted.active_inhibitors


def test_publish_state_derives_inhibitors_after_redis_refresh() -> None:
    """Regression: ``derive_active_inhibitors`` must run *after*
    ``_refresh_user_paused_from_redis`` (and after the transaction branch
    merges the persisted pause flag) so the inhibitor list cannot disagree
    with the ``user_paused`` field on the same payload. The in-memory
    ``user_paused`` is stale (False), but Redis already holds True — the
    persisted snapshot must surface USER_PAUSE."""
    from src.inhibitor import InhibitorType

    runner = _make_runner()
    assert isinstance(runner.redis, _FakeRedis)
    state_key = f"pipeline:{runner.name}"
    persisted = RepoState(
        url=runner.state.url,
        name=runner.name,
        last_updated=datetime.now(timezone.utc),
        user_paused=True,
    )
    runner.redis.store[state_key] = persisted.model_dump_json()
    runner.state.user_paused = False

    asyncio.run(runner.publish_state())

    _key, payload = runner.redis.writes[-1]
    written = RepoState.model_validate_json(payload)
    assert written.user_paused is True
    assert len(written.active_inhibitors) == 1
    assert written.active_inhibitors[0].inhibitor_type is InhibitorType.USER_PAUSE


def test_publish_state_handles_derive_exception_gracefully(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``derive_active_inhibitors`` raises, ``publish_state`` must
    still persist state (with an empty inhibitor list) rather than crash
    the runner cycle. A bad derivation cannot freeze the daemon."""
    warnings: list[str] = []

    async def _boom(*args: object, **kwargs: object) -> list[object]:
        raise RuntimeError("derive failed")

    monkeypatch.setattr(runner_module, "derive_active_inhibitors", _boom)

    runner = _make_runner()
    runner.state.user_paused = True
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )

    asyncio.run(runner.publish_state())

    assert isinstance(runner.redis, _FakeRedis)
    _key, payload = runner.redis.writes[-1]
    persisted = RepoState.model_validate_json(payload)
    assert persisted.active_inhibitors == []
    assert runner.state.active_inhibitors == []
    assert any(
        "derive_active_inhibitors failed" in entry for entry in warnings
    )


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
