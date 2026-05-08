"""Tests for PR-253 OBS-BE detection wiring.

The four detection paths (CRASH/ESCALATE/TIMEOUT/INFRA) write a
structured CancellationCause to Redis when the task transition fires.
PR-252 substrate already exercised separately; this file pins the
wiring contract from each detection point through to a recorded cause.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pytest

from src.cancellation import (
    CRASH_PAYLOAD_MESSAGE_MAX,
    CancellationCause,
    cause_key,
    classify_infra_exception,
    delete_cancellation_cause,
    index_key,
    safe_delete_cancellation_cause,
    safe_record_cancellation_cause,
    truncate_for_payload,
)
from src.daemon import error_rate_tracker
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module
from src.daemon.handlers import error as error_module
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import fix as fix_handler_module
from src.daemon.handlers.coding import CodingMixin
from src.models import (
    PipelineState,
    PRInfo,
    QueueTask,
    TaskStatus,
)

from tests.runner import _helpers as h


class _FakePipeline:
    def __init__(self, store: "_FakeRedisWithPipeline") -> None:
        self._store = store
        self._ops: list[tuple] = []

    def set(self, key: str, value: str, ex: int | None = None) -> "_FakePipeline":
        self._ops.append(("set", key, value, ex))
        return self

    def zadd(self, key: str, mapping: dict[str, float]) -> "_FakePipeline":
        self._ops.append(("zadd", key, dict(mapping)))
        return self

    def zremrangebyscore(self, key: str, min_score, max_score) -> "_FakePipeline":
        self._ops.append(("zremrangebyscore", key, min_score, max_score))
        return self

    def expire(self, key: str, seconds: int) -> "_FakePipeline":
        self._ops.append(("expire", key, seconds))
        return self

    async def execute(self) -> list:
        results: list[Any] = []
        for op in self._ops:
            if op[0] == "set":
                _, key, value, _ex = op
                self._store.values[key] = value
                results.append(True)
            elif op[0] == "zadd":
                _, key, mapping = op
                bucket = self._store.zsets.setdefault(key, {})
                bucket.update(mapping)
                results.append(len(mapping))
            elif op[0] == "zremrangebyscore":
                results.append(0)
            elif op[0] == "expire":
                results.append(True)
        self._ops.clear()
        return results


class _FakeRedisWithPipeline:
    """FakeRedis that supports both runner-side ops and pipeline writes."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def pipeline(self) -> _FakePipeline:
        return _FakePipeline(self)

    async def delete(self, key: str) -> int:
        if key in self.values:
            del self.values[key]
            return 1
        return 0

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        bucket.update(mapping)
        return len(mapping)

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def zremrangebyscore(self, key: str, min_score: Any, max_score: Any) -> int:
        self.zsets.setdefault(key, {})
        return 0


def _captured_safe_record(monkeypatch: pytest.MonkeyPatch) -> list[CancellationCause]:
    """Patch safe_record_cancellation_cause everywhere it is imported."""
    captured: list[CancellationCause] = []

    async def fake_safe(
        redis_client: Any,
        repo_slug: str,
        task_id: str,
        cause: CancellationCause,
        *,
        log: Any = None,
    ) -> None:
        captured.append(cause)

    monkeypatch.setattr(runner_module, "safe_record_cancellation_cause", fake_safe)
    monkeypatch.setattr(fix_handler_module, "safe_record_cancellation_cause", fake_safe)
    return captured


def _doing_task(pr_id: str = "PR-100") -> QueueTask:
    return QueueTask(pr_id=pr_id, title="example", status=TaskStatus.DOING)


def _stub_runner_publish_and_save(runner: Any) -> None:
    async def _noop(*_a: Any, **_k: Any) -> None:
        return None

    runner.publish_state = _noop  # type: ignore[method-assign]
    runner._save_current_run_record = _noop  # type: ignore[method-assign]


def test_crash_wiring_writes_correct_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-101")

    asyncio.run(runner._transition_to_error("subprocess crashed"))

    assert len(captured) == 1
    cause = captured[0]
    assert cause.category == "CRASH"
    assert cause.payload == {"error_message": "subprocess crashed"}


def test_transition_to_error_skips_cause_when_no_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    assert runner.state.current_task is None

    asyncio.run(runner._transition_to_error("orphan failure"))

    assert captured == []


def test_transition_to_error_uses_cancellation_cause_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-102")
    override = CancellationCause(
        category="INFRA",
        payload={"subsystem": "gh_api", "retry_count": 3},
    )

    asyncio.run(
        runner._transition_to_error(
            "gh down", cancellation_cause=override
        )
    )

    assert len(captured) == 1
    assert captured[0] is override


def test_timeout_wiring_writes_limit_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-103")

    timeout_cause = CancellationCause(
        category="TIMEOUT",
        payload={
            "limit_type": "fix_idle",
            "duration_elapsed_sec": 600,
            "active_phase": PipelineState.FIX.value,
        },
    )
    asyncio.run(
        runner._transition_to_error(
            "FIX idle timeout: no push for 600s",
            save_run_record_as=None,
            publish=False,
            log_prefix="[FIX]",
            cancellation_cause=timeout_cause,
        )
    )

    assert len(captured) == 1
    cause = captured[0]
    assert cause.category == "TIMEOUT"
    assert cause.payload["limit_type"] == "fix_idle"
    assert cause.payload["duration_elapsed_sec"] == 600
    assert cause.payload["active_phase"] == PipelineState.FIX.value


def test_escalate_wiring_writes_reason_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fix.py wires an ESCALATE cause when parse_escalate_marker hits."""
    captured = _captured_safe_record(monkeypatch)

    head_calls = iter(["aaa000", "bbb111"])
    h._patch_fix_with_stdout(
        monkeypatch,
        stdout="pushed a fix\nESCALATE: cannot resolve\n",
        head_seq=lambda: next(head_calls),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=304, branch="pr-304")
    runner.state.current_task = _doing_task("PR-304")
    monkeypatch.setattr(
        runner, "_post_codex_review", lambda pr_number: True
    )

    asyncio.run(runner.handle_fix())

    escalate_writes = [c for c in captured if c.category == "ESCALATE"]
    assert [c.payload for c in escalate_writes] == [
        {"subsource": "coder", "reason_text": "cannot resolve"},
        {
            "subsource": "daemon",
            "reason_text": "FIX coder ESCALATE on PR #304: cannot resolve. Moving to IDLE.",
            "previous_state": "FIX",
        },
    ]


def test_infra_classifier_recognizes_retry_exhaustion() -> None:
    exc = RuntimeError("gh api repos/x/y failed after 3 attempts: connection reset")
    cause = classify_infra_exception(exc)
    assert cause is not None
    assert cause.category == "INFRA"
    assert cause.payload["subsystem"] == "gh_api"
    assert cause.payload["retry_count"] == 3
    assert cause.payload["error_class"] == "RuntimeError"


def test_infra_classifier_returns_none_for_non_retry_exception() -> None:
    assert classify_infra_exception(ValueError("just a bug")) is None


def test_infra_wiring_writes_subsystem(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """coding.py classifies retry-exhaustion as INFRA at get_open_prs site."""
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-104")

    exc = RuntimeError("gh api pulls failed after 3 attempts: timed out")
    infra_cause = classify_infra_exception(exc)

    asyncio.run(
        runner._transition_to_error(
            f"get_open_prs failed: {exc}",
            publish=False,
            log_prefix="[CODING]",
            cancellation_cause=infra_cause,
        )
    )

    assert len(captured) == 1
    cause = captured[0]
    assert cause.category == "INFRA"
    assert cause.payload["subsystem"] == "gh_api"
    assert cause.payload["retry_count"] == 3


def test_storage_failure_does_not_block_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis outage during cause write must log but never raise."""

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("redis down")

    from src.cancellation import storage as storage_module

    monkeypatch.setattr(
        storage_module, "record_cancellation_cause", boom
    )

    logged: list[str] = []
    asyncio.run(
        safe_record_cancellation_cause(
            object(),
            "alpha",
            "PR-200",
            CancellationCause(category="CRASH", payload={"error_message": "x"}),
            log=logged.append,
        )
    )

    assert any(
        "Failed to record cancellation cause (CRASH)" in line for line in logged
    )


def test_safe_record_logs_via_module_logger_when_no_log_callback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without a log callback, failures fall through to the module logger."""
    from src.cancellation import storage as storage_module

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("redis bus")

    monkeypatch.setattr(
        storage_module, "record_cancellation_cause", boom
    )

    with caplog.at_level("WARNING", logger="src.cancellation"):
        asyncio.run(
            safe_record_cancellation_cause(
                object(),
                "alpha",
                "PR-201",
                CancellationCause(category="INFRA", payload={"subsystem": "gh_api"}),
            )
        )

    assert any(
        "Failed to record cancellation cause (INFRA)" in record.message
        for record in caplog.records
    )


def test_safe_record_succeeds_when_redis_pipeline_works() -> None:
    redis = _FakeRedisWithPipeline()

    asyncio.run(
        safe_record_cancellation_cause(
            redis,
            "alpha",
            "PR-205",
            CancellationCause(
                category="CRASH",
                payload={"error_message": "boom"},
            ),
        )
    )

    stored = redis.values["cancellation:alpha:PR-205"]
    assert "PR-205" in stored
    assert "CRASH" in stored
    tracker = redis.zsets[error_rate_tracker.key("alpha")]
    assert list(tracker) == ["PR-205"]
    assert tracker["PR-205"] == pytest.approx(
        datetime.fromisoformat(
            CancellationCause.from_redis(stored).created_at
        ).timestamp()
    )


def test_safe_delete_clears_error_rate_tracker_event() -> None:
    redis = _FakeRedisWithPipeline()

    asyncio.run(
        safe_record_cancellation_cause(
            redis,
            "alpha",
            "PR-207",
            CancellationCause(category="CRASH", payload={"error_message": "boom"}),
        )
    )

    asyncio.run(safe_delete_cancellation_cause(redis, "alpha", "PR-207"))

    assert redis.zsets[error_rate_tracker.key("alpha")] == {}


def test_safe_record_logs_tracker_failure_via_module_logger(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Tracker failures are best-effort and use the module logger by default."""

    async def boom(*_a: Any, **_k: Any) -> None:
        raise RuntimeError("tracker down")

    monkeypatch.setattr(error_rate_tracker, "record", boom)

    with caplog.at_level("WARNING", logger="src.cancellation"):
        asyncio.run(
            safe_record_cancellation_cause(
                _FakeRedisWithPipeline(),
                "alpha",
                "PR-206",
                CancellationCause(
                    category="CRASH",
                    payload={"error_message": "boom"},
                ),
            )
        )

    assert any(
        "Failed to record ERROR-rate event (CRASH)" in record.message
        for record in caplog.records
    )


def test_coding_handler_imports_classifier_and_cause() -> None:
    """Smoke test: coding.py exports the symbols PR-253 wiring relies on."""
    assert coding_module.classify_infra_exception is classify_infra_exception
    assert coding_module.CancellationCause is CancellationCause


def test_fix_handler_imports_cause_helpers() -> None:
    """Smoke test: fix.py wires the same cancellation symbols."""
    assert fix_module.CancellationCause is CancellationCause
    assert fix_module.safe_record_cancellation_cause is not None


def test_delete_cancellation_cause_drops_key_and_index() -> None:
    """The storage primitive removes both the cause key and its index entry."""
    redis = _FakeRedisWithPipeline()
    redis.values[cause_key("alpha", "PR-300")] = "{}"
    redis.zsets[index_key("alpha")] = {"PR-300": 1.0, "PR-301": 2.0}

    asyncio.run(delete_cancellation_cause(redis, "alpha", "PR-300"))

    assert cause_key("alpha", "PR-300") not in redis.values
    assert "PR-300" not in redis.zsets[index_key("alpha")]
    # Sibling task entries are untouched.
    assert redis.zsets[index_key("alpha")]["PR-301"] == 2.0


def test_safe_delete_cancellation_cause_swallows_redis_failure() -> None:
    """A Redis outage during cleanup must log but never raise."""

    class _BoomRedis:
        async def delete(self, key: str) -> int:
            raise RuntimeError("redis down")

        async def zrem(self, key: str, *members: str) -> int:
            return 0

    logged: list[str] = []
    asyncio.run(
        safe_delete_cancellation_cause(
            _BoomRedis(), "alpha", "PR-302", log=logged.append,
        )
    )

    assert any(
        "Failed to clear cancellation cause for PR-302" in line
        for line in logged
    )


def test_safe_delete_cancellation_cause_falls_back_to_module_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Without a log callback, failures fall through to the module logger."""

    class _BoomRedis:
        async def delete(self, key: str) -> int:
            raise RuntimeError("redis bus")

        async def zrem(self, key: str, *members: str) -> int:
            return 0

    with caplog.at_level("WARNING", logger="src.cancellation"):
        asyncio.run(
            safe_delete_cancellation_cause(_BoomRedis(), "alpha", "PR-303")
        )

    assert any(
        "Failed to clear cancellation cause for PR-303" in record.message
        for record in caplog.records
    )


def test_safe_delete_cancellation_cause_logs_tracker_cleanup_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Tracker cleanup is best-effort and logs via the module logger."""

    class _TrackerBoomRedis:
        async def delete(self, key: str) -> int:
            return 1

        async def zrem(self, key: str, *members: str) -> int:
            if key.startswith("error_rate:"):
                raise RuntimeError("tracker down")
            return 1

    with caplog.at_level("WARNING", logger="src.cancellation"):
        asyncio.run(
            safe_delete_cancellation_cause(
                _TrackerBoomRedis(), "alpha", "PR-304"
            )
        )

    assert any(
        "Failed to clear ERROR-rate event for PR-304" in record.message
        for record in caplog.records
    )


def test_safe_delete_logs_tracker_cleanup_failure_to_callback() -> None:
    """Tracker cleanup failures use the provided log callback when present."""

    class _TrackerBoomRedis:
        async def delete(self, key: str) -> int:
            return 1

        async def zrem(self, key: str, *members: str) -> int:
            if key.startswith("error_rate:"):
                raise RuntimeError("tracker down")
            return 1

    logged: list[str] = []
    asyncio.run(
        safe_delete_cancellation_cause(
            _TrackerBoomRedis(), "alpha", "PR-305", log=logged.append
        )
    )

    assert any(
        "Failed to clear ERROR-rate event for PR-305" in line for line in logged
    )


def test_truncate_for_payload_keeps_short_messages_intact() -> None:
    assert truncate_for_payload("short") == "short"


def test_truncate_for_payload_tail_truncates_long_messages() -> None:
    body = "abcdef" + "x" * 10_000 + "TAIL"
    truncated = truncate_for_payload(body)
    # The tail (where stderr's actionable content typically lives) is kept;
    # the head and surplus middle are discarded.
    assert truncated.endswith("TAIL")
    assert truncated.startswith("[truncated]\n")
    assert len(truncated) <= len("[truncated]\n") + CRASH_PAYLOAD_MESSAGE_MAX


def test_transition_to_error_truncates_default_crash_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Huge stderr-like messages must not be persisted unbounded into Redis."""
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-303")

    huge = "starthead" + "y" * 50_000 + "ENDTAIL"
    asyncio.run(runner._transition_to_error(huge))

    assert len(captured) == 1
    cause = captured[0]
    assert cause.category == "CRASH"
    payload_message = cause.payload["error_message"]
    assert len(payload_message) <= len("[truncated]\n") + CRASH_PAYLOAD_MESSAGE_MAX
    assert payload_message.endswith("ENDTAIL")
    # The original error_message on state.state is intact — only the
    # persisted cancellation payload is bounded (Codex P2 on PR-253).
    assert runner.state.error_message == huge


def test_transition_to_error_does_not_truncate_override_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit cancellation_cause is persisted as-is."""
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-304")

    override = CancellationCause(
        category="TIMEOUT",
        payload={"limit_type": "fix_idle", "extra": "x" * 9000},
    )
    asyncio.run(
        runner._transition_to_error("doesn't matter", cancellation_cause=override)
    )

    assert captured == [override]
    # Caller's payload survives intact — truncation only applies to the
    # default CRASH payload built by _transition_to_error itself.
    assert len(captured[0].payload["extra"]) == 9000


def test_transition_to_error_preserves_existing_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First-cause-wins: a later _transition_to_error must not overwrite."""
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-320")

    prior = CancellationCause(
        category="ESCALATE",
        payload={"reason_text": "first failure"},
        created_at="2026-05-04T08:00:00+00:00",
        task_id="PR-320",
        repo_slug=runner.name,
    )
    runner.redis.store[cause_key(runner.name, "PR-320")] = prior.to_redis()

    asyncio.run(runner._transition_to_error("transient retry CRASH"))

    assert captured == [], (
        "later _transition_to_error must not overwrite first cause"
    )


def test_transition_to_error_writes_when_redis_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Read-side failure must not silently drop the cause write."""
    captured = _captured_safe_record(monkeypatch)

    async def boom(
        redis_client: Any, repo_slug: str, task_id: str
    ) -> CancellationCause | None:
        raise RuntimeError("redis read down")

    monkeypatch.setattr(runner_module, "get_cancellation_cause", boom)

    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-321")

    asyncio.run(runner._transition_to_error("subprocess crashed"))

    assert len(captured) == 1
    assert captured[0].category == "CRASH"


def _make_clear_cause_runner(monkeypatch: pytest.MonkeyPatch) -> tuple[Any, list[str]]:
    """Make a runner stubbed enough to exercise handle_error IDLE-retry paths."""
    deleted: list[str] = []

    async def fake_safe_delete(
        redis_client: Any,
        repo_slug: str,
        task_id: str,
        *,
        log: Any = None,
    ) -> None:
        deleted.append(task_id)

    # error.py imported the symbol into its namespace; patch it there so
    # the bound reference inside handle_error resolves to the fake.
    monkeypatch.setattr(
        error_module, "safe_delete_cancellation_cause", fake_safe_delete
    )
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-310")
    runner.state.state = PipelineState.ERROR
    return runner, deleted


def test_handle_error_clears_cause_on_infra_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, deleted = _make_clear_cause_runner(monkeypatch)
    runner.state.error_message = "git fetch origin main: connection reset"

    asyncio.run(runner.handle_error())

    assert deleted == ["PR-310"]
    assert runner.state.state == PipelineState.IDLE


def test_handle_error_clears_cause_on_rate_limit_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, deleted = _make_clear_cause_runner(monkeypatch)
    runner.state.error_message = "rate limit exceeded (429)"

    asyncio.run(runner.handle_error())

    assert deleted == ["PR-310"]
    assert runner.state.state == PipelineState.IDLE


def test_handle_error_clears_cause_on_timeout_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, deleted = _make_clear_cause_runner(monkeypatch)
    runner.state.error_message = "operation timeout after 600s"

    asyncio.run(runner.handle_error())

    assert deleted == ["PR-310"]
    assert runner.state.state == PipelineState.IDLE


def test_handle_error_skips_cleanup_when_no_current_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry path with no current_task has no cause to clear."""
    runner, deleted = _make_clear_cause_runner(monkeypatch)
    runner.state.current_task = None
    runner.state.error_message = "git fetch origin main: connection reset"

    asyncio.run(runner.handle_error())

    assert deleted == []
    assert runner.state.state == PipelineState.IDLE


def test_handle_error_cleanup_swallows_redis_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis outage during cleanup must not block the IDLE retry transition."""

    class _BoomRedis:
        async def delete(self, key: str) -> int:
            raise RuntimeError("redis down")

        async def zrem(self, key: str, *members: str) -> int:
            return 0

    runner = h._make_runner()
    runner.redis = _BoomRedis()  # type: ignore[assignment]
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-311")
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "git fetch origin main: connection reset"

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert any(
        "Failed to clear cancellation cause for PR-311" in entry["event"]
        for entry in runner.state.history
    )


def test_error_handler_imports_safe_delete() -> None:
    """Smoke test: error.py wires the cleanup helper for retry paths."""
    from src.cancellation import safe_delete_cancellation_cause as sdcc

    assert error_module.safe_delete_cancellation_cause is sdcc
