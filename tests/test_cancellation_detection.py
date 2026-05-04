"""Tests for PR-253 OBS-BE detection wiring.

The four detection paths (CRASH/ESCALATE/TIMEOUT/INFRA) write a
structured CancellationCause to Redis when the task transition fires.
PR-252 substrate already exercised separately; this file pins the
wiring contract from each detection point through to a recorded cause.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.cancellation import (
    CancellationCause,
    classify_infra_exception,
    safe_record_cancellation_cause,
)
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module
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
    assert len(escalate_writes) == 1
    assert escalate_writes[0].payload == {"reason_text": "cannot resolve"}


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


def test_coding_handler_imports_classifier_and_cause() -> None:
    """Smoke test: coding.py exports the symbols PR-253 wiring relies on."""
    assert coding_module.classify_infra_exception is classify_infra_exception
    assert coding_module.CancellationCause is CancellationCause


def test_fix_handler_imports_cause_helpers() -> None:
    """Smoke test: fix.py wires the same cancellation symbols."""
    assert fix_module.CancellationCause is CancellationCause
    assert fix_module.safe_record_cancellation_cause is not None
