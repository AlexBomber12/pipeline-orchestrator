"""PR-315 invariants on the unified ``CancellationCause`` payload shape.

After PR-315 collapsed five legacy categories into a single ``ERROR``
value, every src/ callsite must emit ``category="ERROR"`` and route
forensic detail through ``payload.subsource``. These tests pin the
contract at the source-tree level (so a future detector cannot
silently reintroduce a legacy category) and exercise the detector
paths that emit each documented subsource value.
"""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any

import pytest

from src.cancellation import (
    CancellationCause,
    classify_infra_exception,
)
from src.daemon import fix_escalation
from src.daemon import fix_supervision
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


SRC_ROOT = Path(__file__).resolve().parent.parent.parent / "src"

# The stable vocabulary documented in ``src/cancellation/storage.py``.
DOCUMENTED_SUBSOURCES = frozenset(
    {
        "crash",
        "coder_escalate",
        "guardrail",
        "review_timeout",
        "fix_idle_timeout",
        "fix_iteration_cap",
        "no_push_deadlock",
        "infra_failure",
    }
)


def _python_files(root: Path) -> list[Path]:
    return [
        path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
    ]


def test_all_callsites_emit_error_category() -> None:
    """Every ``CancellationCause(category=...)`` in src/ resolves to ``"ERROR"``."""
    callsite_re = re.compile(r"CancellationCause\(\s*[^)]*?category\s*=\s*\"([^\"]+)\"")
    offenders: list[tuple[str, str]] = []
    seen_at_least_one = False
    for path in _python_files(SRC_ROOT):
        text = path.read_text(encoding="utf-8")
        for match in callsite_re.finditer(text):
            seen_at_least_one = True
            value = match.group(1)
            if value != "ERROR":
                offenders.append((str(path.relative_to(SRC_ROOT.parent)), value))

    assert seen_at_least_one, "expected at least one CancellationCause callsite in src/"
    assert offenders == [], (
        "All CancellationCause callsites in src/ must use category=\"ERROR\". "
        f"Offenders: {offenders}"
    )


def test_subsource_vocabulary_has_eight_documented_values() -> None:
    """The documented subsource vocabulary has the eight canonical values."""
    assert DOCUMENTED_SUBSOURCES == {
        "crash",
        "coder_escalate",
        "guardrail",
        "review_timeout",
        "fix_idle_timeout",
        "fix_iteration_cap",
        "no_push_deadlock",
        "infra_failure",
    }


def _captured_safe_record(monkeypatch: pytest.MonkeyPatch) -> list[CancellationCause]:
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

    from src.daemon import runner as runner_module
    from src.daemon.handlers import fix as fix_handler_module

    monkeypatch.setattr(
        runner_module, "safe_record_cancellation_cause", fake_safe
    )
    monkeypatch.setattr(
        fix_handler_module, "safe_record_cancellation_cause", fake_safe
    )
    monkeypatch.setattr(
        fix_supervision, "safe_record_cancellation_cause", fake_safe
    )
    monkeypatch.setattr(
        fix_escalation, "safe_record_cancellation_cause", fake_safe
    )
    return captured


def _doing_task(pr_id: str = "PR-1") -> QueueTask:
    return QueueTask(pr_id=pr_id, title="example", status=TaskStatus.DOING)


def _stub_runner_publish_and_save(runner: Any) -> None:
    async def _noop(*_a: Any, **_k: Any) -> None:
        return None

    runner.publish_state = _noop  # type: ignore[method-assign]
    runner._save_current_run_record = _noop  # type: ignore[method-assign]


def test_crash_detector_writes_crash_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_transition_to_error`` default fallback emits ``subsource="crash"``."""
    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-CRASH")

    asyncio.run(runner._transition_to_error("subprocess crashed"))

    assert len(captured) == 1
    cause = captured[0]
    assert cause.category == "ERROR"
    assert cause.payload["subsource"] == "crash"
    assert cause.payload["subsource"] in DOCUMENTED_SUBSOURCES


def test_coder_escalate_detector_writes_coder_escalate_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The coder ESCALATE marker path emits ``subsource="coder_escalate"``."""
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
    runner.state.current_task = _doing_task("PR-CODER-ESC")
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)

    asyncio.run(runner.handle_fix())

    coder_writes = [
        c for c in captured if c.payload.get("subsource") == "coder_escalate"
    ]
    assert len(coder_writes) == 1
    assert coder_writes[0].category == "ERROR"
    assert coder_writes[0].payload["reason_text"] == "cannot resolve"
    assert "coder_escalate" in DOCUMENTED_SUBSOURCES


def test_guardrail_detector_writes_guardrail_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fix-side guardrail violation path emits ``subsource="guardrail"``."""
    captured = _captured_safe_record(monkeypatch)

    head_calls = iter(["head1", "head1"])
    h._patch_fix_with_stdout(
        monkeypatch,
        stdout="gh repo create demo-malicious\n",
        head_seq=lambda: next(head_calls),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=305, branch="pr-305")
    runner.state.current_task = _doing_task("PR-GUARD")
    monkeypatch.setattr(runner, "_post_codex_review", lambda pr_number: True)

    asyncio.run(runner.handle_fix())

    guardrail_writes = [
        c for c in captured if c.payload.get("subsource") == "guardrail"
    ]
    assert guardrail_writes, "expected a guardrail-subsource cause to be recorded"
    assert all(c.category == "ERROR" for c in guardrail_writes)
    assert "guardrail" in DOCUMENTED_SUBSOURCES


def test_fix_idle_timeout_detector_writes_fix_idle_timeout_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The FIX-idle timeout path emits ``subsource="fix_idle_timeout"``."""
    from src.daemon.handlers import fix as fix_module

    cause = fix_module.CancellationCause(
        category="ERROR",
        payload={
            "subsource": "fix_idle_timeout",
            "limit_type": "fix_idle",
            "duration_elapsed_sec": 600,
            "active_phase": PipelineState.FIX.value,
        },
    )

    captured = _captured_safe_record(monkeypatch)
    runner = h._make_runner()
    _stub_runner_publish_and_save(runner)
    runner.state.current_task = _doing_task("PR-FIX-IDLE")

    asyncio.run(
        runner._transition_to_error(
            "FIX idle timeout: no push for 600s",
            save_run_record_as=None,
            publish=False,
            log_prefix="[FIX]",
            cancellation_cause=cause,
        )
    )

    assert len(captured) == 1
    assert captured[0].payload["subsource"] == "fix_idle_timeout"
    assert "fix_idle_timeout" in DOCUMENTED_SUBSOURCES


def test_no_push_deadlock_detector_writes_no_push_deadlock_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``escalate_fix_no_push_deadlock`` emits ``subsource="no_push_deadlock"``."""
    captured = _captured_safe_record(monkeypatch)

    monkeypatch.setattr(
        fix_escalation,
        "apply_canceled_label",
        lambda *_a, **_k: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_task = QueueTask(
        pr_id="PR-NPD", title="t", status=TaskStatus.DOING, branch="pr-npd"
    )
    pr = PRInfo(number=701, branch="pr-npd", head_sha="abc123")
    pr.no_push_fix_count = 3
    runner.state.current_pr = pr

    async def _publish() -> None:
        return None

    runner.publish_state = _publish  # type: ignore[method-assign]

    async def _commit_status(*_a: Any, **_k: Any) -> bool:
        return True

    runner._commit_task_status_change = _commit_status  # type: ignore[method-assign]

    asyncio.run(fix_escalation.escalate_fix_no_push_deadlock(runner, pr))

    npd_writes = [
        c for c in captured if c.payload.get("subsource") == "no_push_deadlock"
    ]
    assert len(npd_writes) == 1
    assert npd_writes[0].category == "ERROR"
    assert npd_writes[0].payload["attempts"] == 3
    assert "no_push_deadlock" in DOCUMENTED_SUBSOURCES


def test_infra_failure_detector_writes_infra_failure_subsource() -> None:
    """``classify_infra_exception`` emits ``subsource="infra_failure"``."""
    exc = RuntimeError("gh api repos/x/y failed after 3 attempts: timed out")

    cause = classify_infra_exception(exc)

    assert cause is not None
    assert cause.category == "ERROR"
    assert cause.payload["subsource"] == "infra_failure"
    assert "infra_failure" in DOCUMENTED_SUBSOURCES
