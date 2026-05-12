"""PR-318: ``recover_state``'s DOING-no-PR path dispatches via subsource.

The defensive cause read happens in ``RecoveryMixin._dispatch_recovery_branch``;
the integration must surface the dispatch via two distinct ``[INFRA]`` log
lines so the dashboard distinguishes daemon-crashed recoveries (``Task X
crashed, marking ERROR``) from detector-parked recoveries (``Task X parked
for operator attention, marking ERROR``).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.cancellation import CancellationCause
from src.cancellation.storage import cause_key
from src.models import PipelineState, QueueTask, TaskStatus

from tests.runner import _helpers as h


def _crashed_doing_runner(monkeypatch: pytest.MonkeyPatch) -> Any:
    task = QueueTask(
        pr_id="PR-318",
        title="Crashed mid-CODING",
        status=TaskStatus.DOING,
        branch="pr-318-crashed",
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )
    runner = h._make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    return runner


def _seed_cause(runner: Any, cause: CancellationCause) -> None:
    """Write ``cause`` directly into the runner's fake Redis store.

    Bypasses ``record_cancellation_cause`` because the ``_FakeRedis`` in
    ``tests/runner/_helpers.py`` does not implement ``pipeline()``; the
    integration paths under test only call ``get_cancellation_cause``,
    which reads through ``redis.get``.
    """
    runner.redis.store[cause_key(runner.name, "PR-318")] = cause.to_redis()


def test_dispatch_crash_subsource_logs_crashed_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recorded ``subsource="crash"`` cause keeps the crash log line."""
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "crash", "error_message": "killed"},
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events
    assert not any(
        "parked for operator attention" in ev for ev in events
    ), events
    assert runner.state.state == PipelineState.IDLE
    assert runner._crashed_task_pr_ids == {"PR-318"}


@pytest.mark.parametrize(
    "subsource",
    [
        "coder_escalate",
        "guardrail",
        "review_timeout",
        "fix_idle_timeout",
        "fix_iteration_cap",
        "no_push_deadlock",
        "infra_failure",
    ],
)
def test_dispatch_non_crash_subsource_logs_operator_attention(
    monkeypatch: pytest.MonkeyPatch, subsource: str
) -> None:
    """A non-crash subsource routes to the operator-attention log line."""
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": subsource, "reason_text": "parked"},
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        ev.startswith("[INFRA] Task PR-318 parked for operator attention,")
        for ev in events
    ), events
    assert not any("Task PR-318 crashed" in ev for ev in events), events
    assert runner.state.state == PipelineState.IDLE


def test_dispatch_no_cause_keeps_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing cause record (daemon killed pre-write) stays the crash branch."""
    runner = _crashed_doing_runner(monkeypatch)

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events


def test_dispatch_legacy_escalate_category_record_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-PR-315 legacy ``category=ESCALATE`` record triggers the warning."""
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner,
        CancellationCause(
            category="ESCALATE",
            payload={"reason_text": "legacy"},
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "[INFRA] cancellation cause has non-ERROR category 'ESCALATE'" in ev
        for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 parked for operator attention,")
        for ev in events
    ), events


def test_dispatch_redis_failure_falls_back_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis read failure preserves the pre-PR-318 crash log line.

    Recovery cannot prove the previous run was a deliberate park if it
    cannot read Redis; defaulting to ``crash`` keeps the back-compatible
    log line that dashboards have grepped on since PR-186 and that
    legacy tests assert against.
    """
    runner = _crashed_doing_runner(monkeypatch)

    async def boom(key: str) -> Any:
        raise RuntimeError("redis down")

    runner.redis.get = boom  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "failed to read cancellation cause for PR-318" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events


def test_dispatch_empty_subsource_routes_to_operator_attention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ERROR cause with no subsource degrades to operator-attention."""
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner, CancellationCause(category="ERROR", payload={})
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        ev.startswith("[INFRA] Task PR-318 parked for operator attention,")
        for ev in events
    ), events


def test_dispatch_invalid_subsource_with_legacy_crash_logs_crashed_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Malformed subsource still recovers the crash log via legacy_category.

    Forward-incompatible/corrupt ``payload.subsource`` values must not
    suppress the crash signal when ``payload.legacy_category`` records the
    original detector as ``CRASH``. The defensive vocabulary check lets the
    legacy fallback win so the operator still sees the crash log line.
    """
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "crsh", "legacy_category": "CRASH"},
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events
    assert not any(
        "parked for operator attention" in ev for ev in events
    ), events
