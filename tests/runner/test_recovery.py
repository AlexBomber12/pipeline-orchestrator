"""PR-212: Recovery interaction regression tests.

The daemon has multiple defense-in-depth recovery layers — preflight
dirty-tree auto-reset, ``recover_state``'s crashed-task marker, and the
three FIX-cycle escalation primitives in ``fix.py`` — each owning a
distinct counter, threshold, and terminal state. They are coordinated
by ordering in ``run_cycle`` rather than an explicit contract, so a
single incident can fire more than one layer.

These four tests pin the **current** layer composition before PR-220
replaces the three ``_escalate_fix_*`` methods with a single
``_escalate_to_hung`` primitive. Each test asserts the terminal state,
the side effect (label apply, comment post, ``is_escalated`` flag)
and the operator-visible log line so the upcoming refactor preserves
behavior verbatim:

- Test 1 — dirty-tree auto-reset composes with recovery's crashed-task
  marker without colliding (terminal state IDLE, both events logged).
- Test 2 — no-push deadlock parks in HUNG and the
  ``hung_fallback_codex_review`` path does not re-fire ``@codex review``
  thanks to the ``is_escalated`` guard in ``handle_hung``.
- Test 3 — coder ESCALATE with a label-apply failure parks in HUNG
  (not IDLE) so the in-memory ``is_escalated`` flag remains the
  load-bearing parking signal during a GitHub outage.
- Test 4 — iteration-cap reached with a label-apply failure parks in
  ERROR (the iteration-cap site has stricter durability requirements
  than the coder-initiated path because it is daemon-driven, not
  coder-self-reported).

Tests do not change production behavior; they only assert it.
"""

from __future__ import annotations

import asyncio
import json

# PR-224a: imports needed by tests moved from tests/test_runner.py
import random  # noqa: F401
import re  # noqa: F401
import subprocess
import time  # noqa: F401
import types  # noqa: F401
from pathlib import Path
from typing import Any

import pytest
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module  # noqa: F401,F811
from src.daemon.handlers import error as error_module  # noqa: F401,F811
from src.daemon.handlers import idle as idle_module  # noqa: F811
from src.daemon.handlers import merge as merge_module  # noqa: F401,F811
from src.daemon.handlers import watch as watch_module  # noqa: F401,F811
from src.models import (
    PipelineState,  # noqa: F811
    PRInfo,  # noqa: F811
    QueueTask,  # noqa: F811
    TaskStatus,  # noqa: F811
)
from src.task_status import MergedState

from tests.runner import _helpers as h


def _merged_state(
    pr_ids: set[str] | None = None,
    branches: set[str] | None = None,
    *,
    api_available: bool = True,
) -> MergedState:
    return MergedState(set(pr_ids or ()), set(branches or ()), api_available)


# ---------------------------------------------------------------------------
# Test 1 — dirty-tree auto-reset composes with crashed-task marker
# ---------------------------------------------------------------------------


def test_dirty_tree_recovery_composes_with_crashed_task_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery + preflight defense layers compose without colliding.

    Setup mirrors a daemon restart after a mid-CODING crash: a DOING
    task PR-100 in the queue with no matching open PR, and a working
    tree left dirty by the crashed coder run. Two distinct recovery
    layers must engage:

    1. ``recover_state`` calls ``_preserve_crashed_run_commits`` to
       push any unpushed work, adds PR-100 to ``_crashed_task_pr_ids``,
       transitions to IDLE.
    2. The next ``preflight`` cycle observes the dirty tree, increments
       the consecutive-dirty counter to its threshold, and fires
       ``_auto_reset_dirty_tree`` to hard-reset back to a clean IDLE.

    Pre-seeding ``_consecutive_dirty_cycles=2`` so the dirty preflight
    crosses the threshold on this single call keeps the test focused
    on the layer composition rather than the policy mechanics tested
    elsewhere.
    """
    task = QueueTask(
        pr_id="PR-100",
        title="Crashed mid-CODING",
        status=TaskStatus.DOING,
        branch="pr-100-crashed",
    )

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: [task]  # type: ignore[method-assign]

    preserve_calls: list[str] = []

    def fake_preserve(branch: str) -> bool:
        preserve_calls.append(branch)
        return True

    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        fake_preserve
    )

    asyncio.run(runner.recover_state())

    assert preserve_calls == ["pr-100-crashed"]
    assert runner._crashed_task_pr_ids == {"PR-100"}
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any(
        e["event"].startswith("[INFRA] Task PR-100 crashed, marking CANCELED. Manually re-upload to retry.")
        for e in runner.state.history
    )

    runner._consecutive_dirty_cycles = 2

    reset_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        reset_commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.IDLE
    assert runner._consecutive_dirty_cycles == 0
    assert any(cmd[:2] == ["git", "reset"] and "--hard" in cmd for cmd in reset_commands)
    assert any("Auto-recovered from dirty tree" in e["event"] for e in runner.state.history)
    assert any("PR-100 crashed, marking CANCELED" in e["event"] for e in runner.state.history)


# ---------------------------------------------------------------------------
# Test 2 — no-push deadlock cancels the task and returns to IDLE (PR-258)
# ---------------------------------------------------------------------------


def test_no_push_escalation_cancels_task_and_returns_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-258 (OBS-BB) replaces the prior HUNG transition with a
    cancellation policy v1 transition: write a ``NO_PUSH_DEADLOCK``
    cause, mark the task CANCELED via ``_recovered_task_pr_ids``, and
    return to IDLE so the daemon picks up the next pickable task.
    """
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh", lambda cmd, **kwargs: ""
    )

    recorded: list[tuple[str, str, str]] = []

    async def fake_record(redis_client, repo_slug, task_id, cause):
        recorded.append((repo_slug, task_id, cause.category))

    monkeypatch.setattr(
        "src.cancellation.record_cancellation_cause", fake_record
    )

    runner = h._make_runner()
    pr = PRInfo(number=400, branch="pr-400-feedback", no_push_fix_count=3)
    runner.state.state = PipelineState.FIX
    runner.state.current_task = QueueTask(
        pr_id="PR-400",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-400-feedback",
    )
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_no_push_deadlock(pr))

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert pr.no_push_fix_count == 0
    assert "PR-400" in runner._recovered_task_pr_ids
    assert recorded == [(runner.name, "PR-400", "NO_PUSH_DEADLOCK")]
    assert any(
        "PR #400 no-push deadlock after 3 attempts; canceling task"
        in e["event"]
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# Test 3 — coder ESCALATE + label apply failure parks in HUNG
# ---------------------------------------------------------------------------


def test_coder_escalate_label_apply_failure_parks_in_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coder ESCALATE with a label-apply failure parks the PR in HUNG.

    ``_escalate_fix_coder_initiated`` ordinarily moves to IDLE on the
    success path so the next IDLE refresh can rehydrate
    ``is_escalated`` from the GitHub label. When the label apply
    soft-fails, the rehydrate would observe a missing label and drop
    the parking signal; HUNG honors the in-memory ``is_escalated``
    flag, so the runner stays parked until manual intervention.
    ``error_message`` carries both the failure context and the
    coder-supplied reason so an operator reading the dashboard can
    act on it without diving into the daemon log.
    """
    posted: list[tuple[str, int, str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    pr = PRInfo(number=401, branch="pr-401-coder-escalate")
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_coder_initiated(pr, "transient infra failure"))

    assert runner.state.state == PipelineState.HUNG
    assert pr.is_escalated is True
    assert runner.state.error_message is not None
    assert "FIX coder ESCALATE on PR #401" in runner.state.error_message
    assert "failed to apply `escalated` label" in runner.state.error_message
    assert "transient infra failure" in runner.state.error_message
    assert posted == [
        (
            runner.owner_repo,
            401,
            "Coder explicitly escalated this PR. Reason: transient infra failure. Manual review required.",
        )
    ]
    assert any("failed to apply escalated label to PR #401: gh down" in e["event"] for e in runner.state.history)


# ---------------------------------------------------------------------------
# Test 4 — iteration-cap + label apply failure transitions to ERROR
# ---------------------------------------------------------------------------


def test_iteration_cap_label_apply_failure_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iteration-cap escalation with a label-apply failure goes to ERROR.

    The iteration-cap escalation path differs intentionally from the
    coder-initiated path: a daemon-driven escalation does not have a
    self-reported parking signal to fall back on, so the failure to
    publish the durable ``escalated`` label is a hard error. The
    runner records the GitHub mutation reason in ``error_message``
    and surfaces it via a ``[FIX]`` log prefix so it is grouped with
    other FIX-handler diagnostics. PR-220 will replace this method
    with a parameterised escalation primitive; the ERROR terminal
    state must remain distinct from coder ESCALATE's HUNG.
    """
    posted: list[tuple[str, int, str]] = []
    label_create_calls: list[list[str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            label_create_calls.append(cmd)
            return ""
        if cmd[:2] == ["pr", "edit"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="rate limit exceeded")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(
        number=402,
        branch="pr-402-cap",
        fix_iteration_count=cap,
    )
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_iteration_cap(pr))

    assert runner.state.state == PipelineState.ERROR
    assert pr.is_escalated is False
    assert runner.state.error_message is not None
    assert runner.state.error_message.startswith("pr edit failed:")
    assert posted == [
        (
            runner.owner_repo,
            402,
            f"@AlexBomber12 FIX iteration cap reached ({cap}/{cap}). Escalating for manual review.",
        )
    ]
    assert label_create_calls and label_create_calls[0][:3] == [
        "label",
        "create",
        "escalated",
    ]
    assert any(e["event"].startswith("[FIX] pr edit failed:") for e in runner.state.history)


# ---------------------------------------------------------------------------
# PR-224a moved from tests/test_runner.py
# ---------------------------------------------------------------------------


def test_select_next_task_from_dag_skips_crashed_task_marked_canceled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-186: After recovery marks a task crashed, the next IDLE cycle's
    selector must override its derived status to CANCELED so
    get_eligible_tasks excludes it. Without the override the same crashed
    task would be re-picked as TODO and dispatched into another doomed
    CODING run on the very next cycle."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Crashed task\n\n"
        "Branch: pr-001-crashed\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Healthy follow-up\n\n"
        "Branch: pr-002-healthy\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert task.status == TaskStatus.TODO
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.CANCELED,
        "PR-002": TaskStatus.TODO,
    }
    queue_md = runner._generate_queue_md(
        runner._idle_dag_headers,
        runner._idle_dag_statuses,
    )
    assert "## PR-001" in queue_md
    assert "- Status: CANCELED" in queue_md


def test_select_next_task_from_dag_preserves_doing_for_crashed_task_with_visible_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Codex P1: a crashed task whose open PR becomes visible on a later
    cycle (e.g. ``get_open_prs`` was stale during recovery) must not be
    downgraded to CANCELED by the selector. Preserve the DOING ruling so
    the runner can resume WATCH/merge for the real PR, and clear the
    crashed flag so subsequent cycles treat the task as live again."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Crashed task with visible PR\n\n"
        "Branch: pr-001-crashed\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [PRInfo(number=42, branch="pr-001-crashed", pr_id="PR-001")]
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING
    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DOING}
    assert "PR-001" not in runner._crashed_task_pr_ids


def test_select_next_task_from_dag_cancels_recovered_task_with_visible_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-247 follow-up: HUNG recover button records the trapped task in
    ``_recovered_task_pr_ids`` and the IDLE selector must force CANCELED
    even when the still-open PR derives the task back to DOING. Without
    the unconditional override the next IDLE cycle would reattach the
    runner to WATCH on the same stuck PR — defeating the recover button.
    Distinct from ``_crashed_task_pr_ids`` whose semantics intentionally
    discard on DOING (a stale-API artifact worth honoring)."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Trapped task\n\n"
        "Branch: pr-001-trapped\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "# PR-002: Healthy follow-up\n\n"
        "Branch: pr-002-healthy\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [
        PRInfo(number=42, branch="pr-001-trapped", pr_id="PR-001"),
    ]
    runner._idle_merged_prs = []
    runner._recovered_task_pr_ids.add("PR-001")

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert task.status == TaskStatus.TODO
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.CANCELED,
        "PR-002": TaskStatus.TODO,
    }
    assert "PR-001" in runner._recovered_task_pr_ids
    queue_md = runner._generate_queue_md(
        runner._idle_dag_headers,
        runner._idle_dag_statuses,
    )
    assert "## PR-001" in queue_md
    assert "- Status: CANCELED" in queue_md


def test_select_next_task_from_dag_clears_recovered_flag_when_done(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A recovered task that ends up DONE (e.g. the PR was merged after
    the operator hit recover) must clear the recovered flag so the task
    is not perpetually marked CANCELED in the regenerated QUEUE.md."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Recovered but later merged\n\n"
        "Branch: pr-001-merged\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state({"PR-001"}))

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._recovered_task_pr_ids.add("PR-001")

    asyncio.run(runner._select_next_task_from_dag())

    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DONE}
    assert "PR-001" not in runner._recovered_task_pr_ids


def test_select_next_task_from_dag_clears_crashed_flag_when_done(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A crashed task that ends up DONE (e.g. a stale merge surfaced after
    recovery) must clear the crashed flag so the task is not perpetually
    marked CANCELED in regenerated QUEUE.md, and DONE remains terminal."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Crashed but merged\n\n"
        "Branch: pr-001-merged\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state({"PR-001"}))

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    asyncio.run(runner._select_next_task_from_dag())

    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DONE}
    assert "PR-001" not in runner._crashed_task_pr_ids


def test_recover_state_hydrates_recovered_task_pr_ids_from_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-247 follow-up: ``recover_state`` must hydrate
    ``_recovered_task_pr_ids`` from Redis on startup. Without this the
    operator-recovery marker is purely process-local and a daemon
    restart between the recover click and the user's task re-upload
    would lose the marker; ``recover_state`` would rehydrate the
    QUEUE.md ``CANCELED`` row into ``_crashed_task_pr_ids`` instead,
    and the IDLE selector would discard the stricter override on the
    still-open PR re-deriving DOING — reattaching the runner to WATCH
    on the same stuck PR. Hydrating from Redis preserves the
    ``_recovered_task_pr_ids`` override across restarts so the abandon
    contract holds."""
    import json

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: []  # type: ignore[method-assign]
    asyncio.run(
        runner.redis.set(
            f"recovered_tasks:{runner.name}",
            json.dumps(["PR-100", "PR-200"]),
        )
    )

    asyncio.run(runner.recover_state())

    assert runner._recovered_task_pr_ids == {"PR-100", "PR-200"}


def test_persist_recovered_task_pr_ids_deletes_key_when_set_is_empty(
) -> None:
    """An empty in-memory set must clear the persisted snapshot — leaving
    a stale snapshot would re-cancel the just-retried task on the next
    daemon restart."""
    runner = h._make_runner()
    asyncio.run(runner.redis.set(f"recovered_tasks:{runner.name}", "[]"))

    asyncio.run(runner._persist_recovered_task_pr_ids())

    assert f"recovered_tasks:{runner.name}" not in runner.redis.store


def test_persist_recovered_task_pr_ids_swallows_redis_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis write failure must not crash the recovery transition. The
    in-memory state still reflects the operator's intent; only the
    cross-restart guarantee is forfeited for that one failure."""
    runner = h._make_runner()
    runner._recovered_task_pr_ids.add("PR-001")

    async def _boom_set(*args: object, **kwargs: object) -> None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner.redis, "set", _boom_set)

    asyncio.run(runner._persist_recovered_task_pr_ids())

    assert runner._recovered_task_pr_ids == {"PR-001"}


def test_load_recovered_task_pr_ids_ignores_corrupt_payload(
) -> None:
    """A corrupt JSON payload in Redis must be tolerated; we cannot let a
    bad snapshot block startup recovery. The in-memory set stays empty
    and the next persist write rebuilds the snapshot cleanly."""
    runner = h._make_runner()
    asyncio.run(
        runner.redis.set(f"recovered_tasks:{runner.name}", "{not-json")
    )

    asyncio.run(runner._load_recovered_task_pr_ids())

    assert runner._recovered_task_pr_ids == set()


def test_recover_state_doing_task_in_recovered_set_stays_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-247 follow-up: a DOING entry in QUEUE.md whose PR-ID is in the
    persisted operator-recovered set must NOT be re-attached to WATCH
    even when a matching open PR is visible. The IDLE cycle that would
    have rewritten the row to CANCELED never ran (or its snapshot was
    not yet visible), so the row still reads DOING. Without this
    bypass, ``recover_state`` would dutifully reattach the runner to
    WATCH on the stuck PR — defeating the recover button across
    restarts. The IDLE selector's stricter override surfaces CANCELED
    on the next cycle."""
    import json

    task = QueueTask(
        pr_id="PR-100",
        title="Operator-abandoned mid-CODING",
        status=TaskStatus.DOING,
        branch="pr-100-stuck",
    )

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=42, branch="pr-100-stuck")],
    )

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: [task]  # type: ignore[method-assign]
    asyncio.run(
        runner.redis.set(
            f"recovered_tasks:{runner.name}",
            json.dumps(["PR-100"]),
        )
    )

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert "PR-100" in runner._recovered_task_pr_ids
    # PR-186 crashed-task path must NOT have run for this entry; the
    # stronger recovered override owns it.
    assert "PR-100" not in runner._crashed_task_pr_ids
    assert any(
        "Operator-recovered task PR-100 still DOING in queue" in entry["event"]
        for entry in runner.state.history
    )


def test_recover_state_records_pending_queue_sync_before_recovered_doing_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-247 follow-up: when both an operator-recovered DOING task and an
    open ``queue-done-*`` queue-sync PR exist at startup, the queue-sync
    marker MUST be recorded before recovery exits IDLE for the recovered
    task. ``handle_idle`` gates dispatch on
    ``state.pending_queue_sync_branch``; without this ordering the daemon
    would resume dispatching new work while the queue-sync PR is still
    open, regressing the prior contract that pending sync is recorded
    before any DOING-path early return."""
    import json
    from datetime import datetime, timezone

    task = QueueTask(
        pr_id="PR-100",
        title="Operator-abandoned mid-CODING",
        status=TaskStatus.DOING,
        branch="pr-100-stuck",
    )
    sync_started_at = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)
    pending_sync = PRInfo(
        number=301,
        branch="queue-done-20260419",
        last_activity=sync_started_at,
    )

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [
            PRInfo(number=42, branch="pr-100-stuck"),
            pending_sync,
        ],
    )

    runner = h._make_runner()
    runner._origin_queue_md_tracked = lambda: False  # type: ignore[method-assign]
    runner._parse_base_queue = lambda **_: [task]  # type: ignore[method-assign]
    asyncio.run(
        runner.redis.set(
            f"recovered_tasks:{runner.name}",
            json.dumps(["PR-100"]),
        )
    )

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.pending_queue_sync_branch == "queue-done-20260419"
    assert runner.state.pending_queue_sync_started_at == sync_started_at


def test_dirty_tree_auto_recovery_after_3_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After three consecutive dirty preflights the runner hard-resets
    to ``origin/{branch}`` and returns to IDLE instead of staying stuck
    in ERROR requiring manual intervention."""
    commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 1
    assert runner.state.state == PipelineState.ERROR

    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 2
    assert runner.state.state == PipelineState.ERROR

    assert asyncio.run(runner.preflight()) is True
    assert runner._consecutive_dirty_cycles == 0
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert any(cmd[:2] == ["git", "reset"] and "--hard" in cmd for cmd in commands)
    assert any(cmd[:3] == ["git", "clean", "-fd"] for cmd in commands)
    assert any("Auto-recovered from dirty tree" in e["event"] for e in runner.state.history)


def test_dirty_tree_auto_recovery_preserves_watch_with_open_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When auto-recovery fires while a PR is being tracked, the runner
    must resume WATCH, not IDLE. Dropping to IDLE lets the next cycle
    re-pick the still-TODO task from origin/main's QUEUE.md and open a
    duplicate PR — exactly the churn this safety net is meant to
    avoid."""

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=99, branch="pr-099-wip")
    runner.state.current_task = QueueTask(pr_id="PR-099", title="wip", status=TaskStatus.DOING, branch="pr-099-wip")

    asyncio.run(runner.preflight())
    asyncio.run(runner.preflight())
    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert runner.state.current_task is not None
    assert any("auto-recovered from dirty tree -> watch" in e["event"].lower() for e in runner.state.history)


def test_dirty_tree_counter_resets_on_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dirty-cycle counter must return to zero after any clean
    preflight so a transient glitch does not push a later cycle over
    the auto-reset threshold."""
    h._patch_subprocess(monkeypatch, stdout=" M foo.py")
    runner = h._make_runner()
    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 1

    h._patch_subprocess(monkeypatch, stdout="")
    assert asyncio.run(runner.preflight()) is True
    assert runner._consecutive_dirty_cycles == 0


def test_dirty_tree_auto_recovery_failure_stays_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the auto-reset git commands themselves fail, preflight must
    leave the runner in ERROR so the operator still sees the issue
    rather than silently declaring the tree clean."""

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        if cmd[:2] == ["git", "reset"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="reset refused")
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    asyncio.run(runner.preflight())
    asyncio.run(runner.preflight())
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert any("Auto-recovery failed" in e["event"] for e in runner.state.history)


def test_recover_state_rehydrates_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """recover_state must rehydrate _last_push_at when matching to an
    in-flight DOING task's open PR so the first post-restart handle_watch
    does not falsely fire handle_fix on pre-restart Codex feedback."""
    parsed_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.DOING,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        )
    ]
    (tmp_path / "tasks").mkdir(parents=True)
    (tmp_path / "tasks" / "PR-001.md").write_text("# PR-001\n")

    head_iso = "2026-04-10T12:00:00Z"
    monkeypatch.setattr(
        "src.github.prs.get_pr_metadata",
        lambda repo, number: {"author": "", "head_sha": "", "head_commit_date": head_iso},
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=7, branch="pr-001")],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._parse_base_queue = lambda **_: parsed_tasks  # type: ignore[method-assign]
    assert runner._last_push_at is None
    assert runner._watch_entered_at is None
    asyncio.run(runner.recover_state())
    assert runner.state.state == PipelineState.WATCH
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-10T12:00:00+00:00"
    # PR-202: recovery anchors the slow-start window so the first
    # post-restart poll already uses the slow cadence.
    assert runner._watch_entered_at is not None


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — recovery group
# ---------------------------------------------------------------------------


def test_run_cycle_resets_stale_transient_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch, stdout="")
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = h._make_runner()
    # _recovered=True skips recover_state so this test exercises the
    # defensive transient-state reset, not the (separately tested)
    # recovery path that would have caught a mid-coding crash first.
    # _scaffolded=True skips the scaffold retry in ensure_repo_cloned
    # so this test focuses on the transient-state reset rather than
    # scaffolding behavior.
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.CODING  # simulate crash mid-coding
    asyncio.run(runner.run_cycle())

    # The stale CODING state was reset and handle_idle ran to completion.
    assert runner.state.state == PipelineState.IDLE
    assert any("stale transient state" in e["event"] for e in runner.state.history)
    assert isinstance(runner.redis, h._FakeRedis)
    assert runner.redis.writes, "publish_state should have been called"


def test_run_cycle_marks_recovery_complete_and_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = h._make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert runner._recovered is True
    assert publishes == ["published"]


def test_run_cycle_runs_recovery_before_honoring_user_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    recovery_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        runner.state.user_paused = True

    async def fake_recover_state() -> bool:
        recovery_calls.append("recover")
        return True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(
        runner,
        "preflight",
        h._preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert recovery_calls == ["recover"]
    assert preflight_calls == []
    assert publishes == ["published"]
    assert runner._recovered is True
    assert not any(entry["event"] == "[INFRA] Paused. Press Play to resume." for entry in runner.state.history)


# ---------------------------------------------------------------------------
# PR-266b: HEADERS_ONLY mode parameterized over PR-266a's golden fixtures
# ---------------------------------------------------------------------------


def _write_pr_md_for_fixture(
    repo: Path, task: dict[str, Any], priority_default: int = 3
) -> None:
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)
    if task.get("legacy_unstructured"):
        (task_dir / f"{task['pr_id']}.md").write_text(
            f"# {task['pr_id']}: {task['title']}\n\nLegacy body\n"
        )
        return
    depends = task.get("depends_on") or []
    depends_value = ", ".join(depends) if depends else "none"
    (task_dir / f"{task['pr_id']}.md").write_text(
        "\n".join(
            [
                f"# {task['pr_id']}: {task['title']}",
                "",
                f"Branch: {task['branch']}",
                f"- Type: {task.get('type', 'feature')}",
                f"- Complexity: {task.get('complexity', 'low')}",
                f"- Depends on: {depends_value}",
                f"- Priority: {task.get('priority', priority_default)}",
                f"- Coder: {task.get('coder', 'codex')}",
                "",
                "## Problem",
                "Body.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _setup_headers_recovery_fixture(
    monkeypatch: pytest.MonkeyPatch,
    repo: Path,
    before: dict[str, Any],
):
    monkeypatch.setenv("PIPELINE_RECOVERY_FROM_HEADERS", "1")
    monkeypatch.delenv("PIPELINE_RECOVERY_AUDIT", raising=False)
    repo.mkdir(parents=True, exist_ok=True)
    for task in before.get("tasks", []):
        _write_pr_md_for_fixture(repo, task)
    open_prs = [PRInfo(**pr) for pr in before.get("open_prs", [])]
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo_id, **kw: list(open_prs)
    )

    from src.daemon import recovery as recovery_module

    def fake_resolve(*args: object, **kwargs: object) -> MergedState:
        return MergedState(
            set(before.get("merged_pr_ids_via_git_log", [])),
            set(before.get("merged_branches_via_api", [])),
            True,
        )

    monkeypatch.setattr(recovery_module, "_resolve_merged_state", fake_resolve)

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._crashed_task_pr_ids = set(before.get("crashed_task_pr_ids", []))
    if before.get("recovered_task_pr_ids"):
        from src.keyspace import recovered_tasks

        asyncio.run(
            runner.redis.set(
                recovered_tasks(runner.name),
                json.dumps(list(before["recovered_task_pr_ids"])),
            )
        )

    if before.get("current_task_pr_id"):
        runner.state.current_task = QueueTask(
            pr_id=before["current_task_pr_id"],
            title="current",
            status=TaskStatus.DOING,
        )

    # PR-186/PR-220 crash fixtures: skip the unpushed-commit preserve probe.
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    return runner


def _projection(runner: runner_module.PipelineRunner, tasks) -> dict[str, Any]:
    pr_id = (
        runner.state.current_task.pr_id
        if runner.state.current_task is not None
        else None
    )
    return {
        "pipeline_state": runner.state.state.value,
        "current_task_pr_id": pr_id,
        "current_pr_number": (
            runner.state.current_pr.number
            if runner.state.current_pr is not None
            else None
        ),
        "pending_queue_sync_branch": runner.state.pending_queue_sync_branch,
        "current_queue": [
            {
                "pr_id": t.pr_id,
                "title": t.title,
                "status": t.status.value,
                "task_file": t.task_file,
                "depends_on": list(t.depends_on),
                "branch": t.branch,
                "priority": t.priority,
            }
            for t in (tasks or [])
        ],
    }


def test_headers_mode_recovers_each_golden_fixture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    recovery_golden_cases,
) -> None:
    """PR-266b success criterion #3: HEADERS_ONLY recovery on each fixture
    matches the golden expected.json projection."""
    import json as _json

    for scenario_name, before, expected in recovery_golden_cases:
        repo = tmp_path / scenario_name
        runner = _setup_headers_recovery_fixture(monkeypatch, repo, before)
        asyncio.run(runner.recover_state())
        actual = _projection(runner, runner.state.current_queue)
        assert actual == expected, (
            f"{scenario_name}: {_json.dumps(actual, sort_keys=True)} != "
            f"{_json.dumps(expected, sort_keys=True)}"
        )


def test_megaraid_scenario_resolved_in_headers_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    recovery_golden_cases,
) -> None:
    """PR-266b success criterion #5: the MegaRAID golden scenario settles
    in IDLE under HEADERS_ONLY mode because the headers helper consults
    the API merged-branches set and produces DONE for the stale DOING
    entry, even though the legacy QUEUE.md row would have re-CODED."""
    case = next(
        (case for case in recovery_golden_cases if case[0] == "megaraid_already_merged_via_api"),
        None,
    )
    assert case is not None
    _, before, _ = case
    repo = tmp_path / "megaraid"
    runner = _setup_headers_recovery_fixture(monkeypatch, repo, before)

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_queue is not None
    assert [t.status for t in runner.state.current_queue] == [TaskStatus.DONE]
