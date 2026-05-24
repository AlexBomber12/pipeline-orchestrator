"""PR-317: ``_commit_and_park_in_error`` primitive + converted callsites.

PR-317 deletes the legacy ``_escalate_and_skip`` primitive and routes
the seven remaining ESCALATE→IDLE→re-pick callsites (coding/idle/fix)
through ``_commit_and_park_in_error``. The helper records a structured
``CancellationCause`` and writes ``status:ERROR`` to the task
frontmatter so the IDLE picker stops re-selecting the failed task.

PR-317 review feedback (P1): the helper deliberately does NOT set
``skip_ai_error_diagnose`` — that flag is reserved for the WATCH
``review_timeout`` operator-park (set inline in ``handlers/watch.py``).
Task-level escalations must not block unrelated queued tasks on the
same runner; ``handle_error`` is allowed to run on the next ERROR
cycle and route per its standard logic. These tests pin (a) the
helper's behavior directly and (b) the terminal ERROR state + subsource
for each converted callsite so future refactors cannot reopen the loop.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from src.cancellation import CancellationCause
from src.cancellation.storage import cause_key
from src.daemon import fix_escalation
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def test_no_escalate_and_skip_in_production_code() -> None:
    """PR-317 acceptance: the legacy primitive is fully removed from src/.

    Comments referencing the historical name are permitted (they describe
    the migration); active method definitions or call expressions are not.
    """
    src_root = Path(__file__).resolve().parent.parent.parent / "src"
    offenders: list[str] = []
    for path in src_root.rglob("*.py"):
        for lineno, line in enumerate(
            path.read_text(encoding="utf-8").splitlines(), start=1
        ):
            if "_escalate_and_skip" not in line:
                continue
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if "``_escalate_and_skip``" in line:
                # Markdown-style docstring reference; historical only.
                continue
            offenders.append(f"{path.relative_to(src_root.parent)}:{lineno}: {line}")
    assert offenders == [], (
        "PR-317 deleted the _escalate_and_skip primitive; remaining "
        f"production references: {offenders}"
    )


def test_commit_and_park_in_error_writes_status_error_and_transitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The primitive writes status:ERROR and transitions to ERROR.

    PR-317 review (P1): the helper must not set
    ``skip_ai_error_diagnose`` — only the WATCH review_timeout park
    sets that flag (see ``handlers/watch.py``).
    """
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-900",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-900",
        task_file="tasks/PR-900.md",
    )

    status_writes: list[tuple[Any, str, str]] = []

    async def fake_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        status_writes.append((task, status, reason))
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        fake_commit,
    )

    async def noop_record(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "src.cancellation.record_cancellation_cause", noop_record
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "branch mismatch",
            subsource="no_push_deadlock",
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert runner.state.error_message == "branch mismatch"
    assert status_writes == [
        (runner.state.current_task, "ERROR", "branch mismatch")
    ]
    assert any(
        e["event"] == "[ESCALATE] branch mismatch." for e in runner.state.history
    )


def test_commit_and_park_in_error_records_subsource_with_previous_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation cause carries subsource + previous_state + reason_text."""
    recorded: list[CancellationCause] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append(cause)

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = PRInfo(number=901, branch="pr-901")
    runner.state.current_task = QueueTask(
        pr_id="PR-901",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-901",
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "operator-only park",
            subsource="coder_escalate",
            extra_payload={"pr_number": 901},
        )
    )

    assert len(recorded) == 1
    cause = recorded[0]
    assert cause.category == "ERROR"
    assert cause.payload == {
        "subsource": "coder_escalate",
        "reason_text": "operator-only park",
        "previous_state": "FIX",
        "pr_number": 901,
    }


def test_commit_and_park_in_error_preserves_existing_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_transition_to_error``'s first-cause-wins rule applies here too."""
    runner = h._make_runner()
    runner.state.state = PipelineState.FIX
    runner.state.current_task = QueueTask(
        pr_id="PR-902",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-902",
    )
    runner.state.current_pr = PRInfo(number=902, branch="pr-902")
    prior_cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "coder_escalate", "reason_text": "first"},
    )
    prior_cause.task_id = "PR-902"
    prior_cause.repo_slug = runner.name
    runner.redis.store[cause_key(runner.name, "PR-902")] = prior_cause.to_redis()

    async def noop_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        noop_commit,
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "daemon wrapper",
            subsource="coder_escalate",
        )
    )

    stored = CancellationCause.from_redis(
        runner.redis.store[cause_key(runner.name, "PR-902")]
    )
    assert stored.payload == {"subsource": "coder_escalate", "reason_text": "first"}


def test_commit_and_park_in_error_no_current_task_skips_status_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``current_task`` the primitive cannot write status:ERROR.

    The runner still transitions to ERROR — the missing durability write is
    a fallback for handler sites that already cleared the task handle.
    """
    runner = h._make_runner()
    runner.state.current_task = None
    runner.state.current_pr = None

    commit_calls: list[Any] = []

    async def fake_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        commit_calls.append(task)
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        fake_commit,
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "no task",
            subsource="infra_failure",
        )
    )

    assert commit_calls == []
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False


def test_commit_and_park_in_error_marks_status_write_fallback_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``_commit_task_status_change`` returns False the runner records
    a suppression so the picker stays away from the task."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-903",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-903",
        task_file="tasks/PR-903.md",
    )

    async def fake_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        return False

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        fake_commit,
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "fallback",
            subsource="infra_failure",
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-903"))
    assert record is not None


def test_commit_and_park_in_error_saves_run_record_before_status_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run record must capture task-branch ``HEAD``, not the post-checkout base SHA.

    ``_commit_task_status_change`` issues ``git checkout -f <base>`` +
    ``git reset --hard origin/<base>`` before committing the daemon-side
    status:ERROR write. ``_save_current_run_record`` captures
    ``HEAD`` at save time via ``_git_rev_parse``, so the helper must
    finalize the run record before invoking the status-change git
    operations — otherwise the record stamps the base-branch SHA and
    OBS-DD run-level attribution for parked CODING/FIX failures points
    at the wrong commit.
    """
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-905",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-905",
        task_file="tasks/PR-905.md",
    )
    runner._start_current_run_record("claude", "opus")

    head_state = {"sha": "task-branch-sha"}

    def fake_rev_parse(self, ref):  # type: ignore[no-untyped-def]
        if ref == "HEAD":
            return head_state["sha"]
        return ""

    monkeypatch.setattr(
        type(runner),
        "_git_rev_parse",
        fake_rev_parse,
    )

    async def fake_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        # Mirror the production behavior: checkout/reset to origin/<base>
        # advances HEAD to the base-branch SHA before the function returns.
        head_state["sha"] = "base-branch-sha"
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        fake_commit,
    )

    async def noop_record(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "src.cancellation.record_cancellation_cause", noop_record
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "boom",
            subsource="no_push_deadlock",
        )
    )

    assert runner._current_run_record is not None
    assert runner._current_run_record.head_sha == "task-branch-sha"


def test_commit_and_park_in_error_logs_status_write_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raising ``_commit_task_status_change`` is logged + fallback recorded."""
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-904",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-904",
        task_file="tasks/PR-904.md",
    )

    async def fail_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError("checkout refused")

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        fail_commit,
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "fail logged",
            subsource="infra_failure",
        )
    )

    record = asyncio.run(runner._suppression_record_for_task("PR-904"))
    assert record is not None
    assert any(
        "[ERROR] Failed to write status:ERROR to tasks/PR-904.md: checkout refused"
        in entry["event"]
        for entry in runner.state.history
    )


# ---------------------------------------------------------------------------
# Coding callsite conversions (PR-317): branch mismatch / no-push / no-PR /
# PR-creation-failed all park in ERROR.
# ---------------------------------------------------------------------------


def _make_coding_runner_with_task(monkeypatch: pytest.MonkeyPatch) -> Any:
    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_task = QueueTask(
        pr_id="PR-CODING",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-coding",
        task_file="tasks/PR-CODING.md",
    )

    async def noop_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        noop_commit,
    )

    async def noop_record(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "src.cancellation.record_cancellation_cause", noop_record
    )
    return runner


def test_coding_branch_mismatch_parks_in_error_with_no_push_deadlock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_coding_runner_with_task(monkeypatch)
    asyncio.run(
        runner._commit_and_park_in_error(
            "[claude] Branch mismatch: pr-coding != pr-different (...)",
            subsource="no_push_deadlock",
        )
    )
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False


@pytest.mark.parametrize(
    "subsource",
    ["no_push_deadlock", "infra_failure"],
)
def test_coding_callsite_subsources_park_in_error(
    monkeypatch: pytest.MonkeyPatch, subsource: str
) -> None:
    """Every coding.py converted callsite parks the runner in ERROR.

    The 4 converted sites use either ``no_push_deadlock`` (branch
    mismatch, did-nothing/no-push) or ``infra_failure`` (PR-not-visible,
    gh-pr-create-failed). Both end in the same terminal state because
    PR-317 collapsed them through the shared primitive."""
    runner = _make_coding_runner_with_task(monkeypatch)
    asyncio.run(
        runner._commit_and_park_in_error(
            f"site exercising {subsource}",
            subsource=subsource,
        )
    )
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert runner.state.error_message == f"site exercising {subsource}"


# ---------------------------------------------------------------------------
# Idle callsite conversion (PR-317): pinned coder unavailable parks in ERROR.
# ---------------------------------------------------------------------------


def test_idle_pinned_coder_unavailable_parks_in_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE
    runner.state.current_task = QueueTask(
        pr_id="PR-IDLE",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-idle",
    )

    async def noop_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        noop_commit,
    )

    asyncio.run(
        runner._commit_and_park_in_error(
            "Task PR-IDLE pinned to codex but coder unavailable",
            subsource="infra_failure",
        )
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False


# ---------------------------------------------------------------------------
# Fix escalation callsite conversions (PR-317): coder ESCALATE + iteration
# cap both park in ERROR with the canonical subsource.
# ---------------------------------------------------------------------------


def test_fix_coder_escalate_marker_parks_in_error_with_coder_escalate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``escalate_fix_coder_initiated`` parks in ERROR after PR-317."""
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda cmd, **kw: "")
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    recorded: list[CancellationCause] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append(cause)

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    runner = h._make_runner()
    pr = PRInfo(number=910, branch="pr-910")
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr
    runner.state.current_task = QueueTask(
        pr_id="PR-910",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-910",
    )

    async def noop_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        noop_commit,
    )

    asyncio.run(
        fix_escalation.escalate_fix_coder_initiated(runner, pr, "boom")
    )

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert pr.is_escalated is True
    assert recorded and recorded[0].payload["subsource"] == "coder_escalate"


def test_fix_iteration_cap_parks_in_error_with_fix_iteration_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``escalate_fix_iteration_cap`` parks in ERROR after PR-317."""
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda cmd, **kw: "")
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    recorded: list[CancellationCause] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append(cause)

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(number=911, branch="pr-911", fix_iteration_count=cap)
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr
    runner.state.current_task = QueueTask(
        pr_id="PR-911",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-911",
    )

    async def noop_commit(self, task, status, reason, **kwargs):  # type: ignore[no-untyped-def]
        return True

    monkeypatch.setattr(
        type(runner),
        "_commit_task_status_change",
        noop_commit,
    )

    asyncio.run(fix_escalation.escalate_fix_iteration_cap(runner, pr))

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert pr.is_escalated is True
    assert recorded and recorded[0].payload["subsource"] == "fix_iteration_cap"
    assert recorded[0].payload["iteration_count"] == cap
