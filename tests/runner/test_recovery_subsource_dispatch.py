"""PR-318: ``recover_state``'s DOING-no-PR path dispatches via subsource.

The defensive cause read happens in ``RecoveryMixin._dispatch_recovery_branch``;
the integration must surface the dispatch via two distinct ``[INFRA]`` log
lines so the dashboard distinguishes daemon-crashed recoveries (``Task X
crashed, marking ERROR``) from detector-parked recoveries (``Task X parked
for operator attention, marking ERROR``).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.cancellation import CancellationCause
from src.cancellation.storage import cause_key, current_run_started_at_key
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


def _seed_current_run_started_at(runner: Any, started_at: datetime) -> None:
    """Seed the per-task dispatch timestamp recorded by ``handle_coding``."""
    runner.redis.store[
        current_run_started_at_key(runner.name, "PR-318")
    ] = started_at.isoformat()


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
    """A non-crash subsource routes to the operator-attention log line.

    Seeds a ``current_run_started_at`` marker that predates the cause's
    ``created_at`` so the PR-318 fix-feedback staleness check accepts
    the cause as belonging to the current run.
    """
    runner = _crashed_doing_runner(monkeypatch)
    cause_created_at = datetime.now(timezone.utc)
    _seed_current_run_started_at(
        runner, cause_created_at - timedelta(seconds=30)
    )
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": subsource, "reason_text": "parked"},
            created_at=cause_created_at.isoformat(),
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
    cause_created_at = datetime.now(timezone.utc)
    _seed_current_run_started_at(
        runner, cause_created_at - timedelta(seconds=30)
    )
    _seed_cause(
        runner,
        CancellationCause(
            category="ESCALATE",
            payload={"reason_text": "legacy"},
            created_at=cause_created_at.isoformat(),
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


def test_dispatch_empty_subsource_routes_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ERROR cause with no subsource falls back to the crash branch.

    Regression for the PR-318 review feedback: ``classify_cancellation_subsource``
    returns ``""`` for malformed or incomplete cause payloads, and routing
    that through the operator-attention branch would mask a real mid-CODING
    crash behind the parked-for-operator-attention log line. The conservative
    fallback documented by ``_dispatch_recovery_branch`` requires the crash
    log line so dashboards still surface the crash signal.
    """
    runner = _crashed_doing_runner(monkeypatch)
    cause_created_at = datetime.now(timezone.utc)
    _seed_current_run_started_at(
        runner, cause_created_at - timedelta(seconds=30)
    )
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={},
            created_at=cause_created_at.isoformat(),
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "empty/unrecognized subsource" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events
    assert not any(
        "parked for operator attention" in ev for ev in events
    ), events


def test_dispatch_unrecognized_subsource_routes_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ERROR cause with a forward-incompatible subsource falls back to crash.

    A subsource string that is not in ``SUBSOURCE_VOCABULARY`` (operator
    typo, detector value written by a newer daemon, partial migration)
    must not silently route to the operator-attention branch. The
    conservative fallback contract for ``_dispatch_recovery_branch``
    requires the crash log line so a real mid-CODING crash that wrote a
    corrupt cause is still surfaced to dashboards.
    """
    runner = _crashed_doing_runner(monkeypatch)
    cause_created_at = datetime.now(timezone.utc)
    _seed_current_run_started_at(
        runner, cause_created_at - timedelta(seconds=30)
    )
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "made_up_signal"},
            created_at=cause_created_at.isoformat(),
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "empty/unrecognized subsource" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events
    assert not any(
        "parked for operator attention" in ev for ev in events
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


def test_dispatch_stale_non_crash_cause_falls_back_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-crash cause that predates the current run is treated as stale.

    Regression for the PR-318 review feedback: ``safe_delete_cancellation_cause``
    is best-effort and may leave a stale non-crash cause in Redis across
    retries. If the next run then dies mid-CODING before writing a fresh
    cause, trusting the stale value would misclassify a real crash as
    operator-attention and hide the crash signal dashboards depend on.
    Recovery must compare ``cause.created_at`` against the dispatch
    timestamp and fall back to the crash branch when the cause predates
    the current run.
    """
    runner = _crashed_doing_runner(monkeypatch)
    now = datetime.now(timezone.utc)
    _seed_current_run_started_at(runner, now)
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "review_timeout", "reason_text": "stale"},
            created_at=(now - timedelta(minutes=10)).isoformat(),
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "predates current run start" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events
    assert not any(
        "parked for operator attention" in ev for ev in events
    ), events


def test_dispatch_missing_current_run_marker_falls_back_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-crash cause without a recorded dispatch timestamp is treated as stale.

    The absence of ``current_run_started_at`` means recovery cannot prove
    the cause belongs to the current run; per the PR-318 review feedback
    this must fall back to the crash branch rather than trust the
    non-crash subsource.
    """
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "review_timeout", "reason_text": "unknown age"},
            created_at=datetime.now(timezone.utc).isoformat(),
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "no current_run start recorded for PR-318" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events


def test_dispatch_current_run_marker_read_failure_falls_back_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis failure reading the dispatch timestamp falls back to crash.

    When the cause read succeeds but the run-start read raises, recovery
    cannot prove freshness; the safer default is the crash branch so the
    operator-attention log line cannot mask a real crash.
    """
    runner = _crashed_doing_runner(monkeypatch)
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "review_timeout", "reason_text": "redis fail"},
            created_at=datetime.now(timezone.utc).isoformat(),
        ),
    )
    original_get = runner.redis.get

    async def selective_boom(key: str) -> Any:
        if key.startswith("current_run_started_at:"):
            raise RuntimeError("redis down")
        return await original_get(key)

    runner.redis.get = selective_boom  # type: ignore[method-assign]

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "failed to read current_run start for PR-318" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events


def test_dispatch_malformed_cause_created_at_falls_back_to_crash_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupt ``cause.created_at`` cannot prove freshness; fall back to crash."""
    runner = _crashed_doing_runner(monkeypatch)
    _seed_current_run_started_at(runner, datetime.now(timezone.utc))
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "review_timeout"},
            created_at="not-a-timestamp",
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        "malformed created_at" in ev for ev in events
    ), events
    assert any(
        ev.startswith("[INFRA] Task PR-318 crashed, marking ERROR.")
        for ev in events
    ), events


def test_dispatch_naive_cause_created_at_treated_as_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cause.created_at`` without timezone info is interpreted as UTC.

    Legacy records written before timezone-aware ISO strings landed must
    still pass the staleness check when their wall-clock time is at or
    after the dispatch timestamp. Otherwise a naive timestamp would
    routinely fail ``cause_created_at < started_at`` and force every
    operator-attention recovery through the crash branch.
    """
    runner = _crashed_doing_runner(monkeypatch)
    now = datetime.now(timezone.utc)
    _seed_current_run_started_at(runner, now - timedelta(seconds=30))
    naive_iso = now.replace(tzinfo=None).isoformat()
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "review_timeout"},
            created_at=naive_iso,
        ),
    )

    asyncio.run(runner.recover_state())

    events = [e["event"] for e in runner.state.history]
    assert any(
        ev.startswith("[INFRA] Task PR-318 parked for operator attention,")
        for ev in events
    ), events


def test_dispatch_recovery_branch_does_not_refresh_cause_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-345 fix-feedback: recovery classification must not pin the cause.

    ``recover_state`` runs on every daemon restart and inspects the
    cancellation cause to classify the DOING-no-PR branch. If that read
    inherited the ``refresh_ttl=True`` default it would extend the cause
    to the 90-day forensic window whenever the daemon restarts, defeating
    natural eviction at 30 days. Verify recovery forwards
    ``refresh_ttl=False``.
    """
    runner = _crashed_doing_runner(monkeypatch)
    cause_created_at = datetime.now(timezone.utc)
    _seed_current_run_started_at(
        runner, cause_created_at - timedelta(seconds=30)
    )
    _seed_cause(
        runner,
        CancellationCause(
            category="ERROR",
            payload={"subsource": "review_timeout", "reason_text": "parked"},
            created_at=cause_created_at.isoformat(),
        ),
    )

    captured: list[dict[str, object]] = []
    from src.cancellation.storage import get_cancellation_cause as real_get

    async def spy(
        redis_client: Any,
        repo_slug: str,
        task_id: str,
        *,
        refresh_ttl: bool = True,
    ) -> Any:
        captured.append({"task_id": task_id, "refresh_ttl": refresh_ttl})
        return await real_get(
            redis_client, repo_slug, task_id, refresh_ttl=refresh_ttl
        )

    monkeypatch.setattr("src.daemon.recovery.get_cancellation_cause", spy)
    asyncio.run(runner.recover_state())

    relevant = [c for c in captured if c["task_id"] == "PR-318"]
    assert relevant, captured
    assert all(c["refresh_ttl"] is False for c in relevant), relevant
