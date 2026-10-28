from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from src.config import FeatureFlags
from src.daemon.runner import PipelineRunner
from src.models import PipelineState
from src.subsource_registry import SuppressionReason, is_operator_clearable

from tests.runner._helpers import _make_runner


def _task(pr_id: str = "PR-380") -> SimpleNamespace:
    return SimpleNamespace(
        pr_id=pr_id,
        task_file=f"tasks/{pr_id}.md",
        branch=f"branch-{pr_id.lower()}",
    )


def _write_task(
    repo: Path,
    task: SimpleNamespace,
    reason: SuppressionReason | None,
    *,
    status: str = "ERROR",
) -> None:
    path = repo / task.task_file
    path.parent.mkdir(parents=True, exist_ok=True)
    blocked = f"blocked_reason: {reason.value}\n" if reason is not None else ""
    path.write_text(
        f"---\nstatus: {status}\n{blocked}---\n\n"
        f"# {task.pr_id}: Test task\n\n"
        f"Branch: {task.branch}\n"
        "- Type: refactor\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )


def _runner(tmp_path: Path) -> PipelineRunner:
    runner = _make_runner(
        feature_flags=FeatureFlags(
            use_unified_inhibitor_check=False,
            use_single_error_exit=True,
        )
    )
    runner.repo_path = str(tmp_path)
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.current_task = _task()
    runner.state.error_message = "ERROR: synthetic failure"
    _write_task(tmp_path, runner.state.current_task, SuppressionReason.GUARDRAIL)
    # This fixture starts after the terminal ERROR status was confirmed.
    runner._confirmed_error_task_pr_ids.add(runner.state.current_task.pr_id)
    # These dispatcher tests use a local task fixture as their base snapshot.
    runner._remote_frontmatter_error_status_for_current_task = (  # type: ignore[method-assign]
        lambda: (*runner._frontmatter_error_status_for_current_task(), "fixture")
    )
    return runner


def _events(runner: PipelineRunner) -> list[str]:
    return [str(entry["event"]) for entry in runner.state.history]


def _stub_cycle(monkeypatch: pytest.MonkeyPatch, runner: PipelineRunner) -> None:
    async def ok() -> bool:
        return True

    async def none() -> None:
        return None

    monkeypatch.setattr(runner, "ensure_repo_cloned", none)
    monkeypatch.setattr(runner, "_check_github_api_budget", ok)
    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", none)
    monkeypatch.setattr(runner, "preflight", ok)
    monkeypatch.setattr(runner, "publish_state", none)


def _suppress(
    runner: PipelineRunner,
    reason: SuppressionReason,
    *,
    task: SimpleNamespace | None = None,
) -> None:
    task = task or runner.state.current_task
    runner.state.current_task = task
    _write_task(Path(runner.repo_path), task, reason)


def _clear_task_frontmatter(runner: PipelineRunner) -> None:
    _write_task(
        Path(runner.repo_path),
        runner.state.current_task,
        None,
        status="TODO",
    )


def test_guardrail_stays_parked_until_cleared(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    diagnose_calls = 0

    async def fake_handle_error() -> None:
        nonlocal diagnose_calls
        diagnose_calls += 1

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert diagnose_calls == 0
    assert any(
        "guardrail operator-clearable ERROR park active" in event
        for event in _events(runner)
    )

    _clear_task_frontmatter(runner)
    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert diagnose_calls == 0
    assert any(
        "operator-cleared ERROR task frontmatter -> IDLE" in event
        for event in _events(runner)
    )


def test_guardrail_does_not_loop_diagnosis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    calls = 0

    async def fake_handle_error() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    for _ in range(3):
        runner.state.state = PipelineState.ERROR
        asyncio.run(runner._run_cycle_body())

    assert calls == 0
    assert runner.state.state == PipelineState.ERROR


def test_silent_hole_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.app_config.daemon.error_handler_use_ai = False
    _suppress(runner, SuppressionReason.INFRA_FAILURE)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert any(
        "bound cleanup/work proof required"
        in event
        for event in _events(runner)
    )


def test_no_error_cycle_without_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.app_config.daemon.error_handler_use_ai = False

    for reason in SuppressionReason:
        task = _task(f"PR-{reason.value.upper()}")
        runner.state.state = PipelineState.ERROR
        runner.state.current_task = task
        runner.state.error_message = f"{reason.value} failure"
        before_state = runner.state.state
        before_logs = len(runner.state.history)
        _suppress(runner, reason, task=task)

        asyncio.run(runner._run_cycle_body())

        assert (
            runner.state.state != before_state
            or len(runner.state.history) > before_logs
        ), reason.value


def test_self_healing_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    infra = _runner(tmp_path / "infra")
    _stub_cycle(monkeypatch, infra)
    infra.state.error_message = "git fetch origin failed"
    _suppress(infra, SuppressionReason.INFRA_FAILURE)

    asyncio.run(infra._run_cycle_body())

    assert infra.state.state == PipelineState.ERROR
    assert any("bound cleanup/work proof required" in event for event in _events(infra))

    limited = _runner(tmp_path / "limited")
    _stub_cycle(monkeypatch, limited)
    limited.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(
        minutes=5
    )

    asyncio.run(limited._run_cycle_body())

    assert limited.state.state == PipelineState.PAUSED


def test_taskless_error_can_self_heal_without_remote_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.state.current_task = None
    runner.state.error_message = "git fetch origin failed"
    runner.app_config.daemon.error_handler_use_ai = False

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert any("self-healing ERROR" in event for event in _events(runner))


def test_reupload_releases_park_from_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    _suppress(runner, SuppressionReason.GUARDRAIL)
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR

    _clear_task_frontmatter(runner)
    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE


def test_frontmatter_todo_releases_even_with_stale_store_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    _clear_task_frontmatter(runner)

    async def stale_record(_task_id: str) -> SimpleNamespace:
        return SimpleNamespace(reason=SuppressionReason.GUARDRAIL)

    monkeypatch.setattr(runner, "_suppression_record_for_task", stale_record)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert any(
        "operator-cleared ERROR task frontmatter -> IDLE" in event
        for event in _events(runner)
    )


def test_review_timeout_skip_flag_parks_under_single_error_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.state.skip_ai_error_diagnose = True
    _suppress(runner, SuppressionReason.REVIEW_TIMEOUT)
    calls = 0

    async def fake_handle_error() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is True
    assert calls == 0
    assert any(
        "review_timeout operator-clearable ERROR park active" in event
        for event in _events(runner)
    )


def test_review_timeout_skip_flag_releases_under_single_error_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    runner.state.skip_ai_error_diagnose = True
    runner.state.review_timeout_repost_attempted = True
    runner.state.review_timeout_repost_at = datetime.now(timezone.utc)
    _suppress(runner, SuppressionReason.REVIEW_TIMEOUT)

    _clear_task_frontmatter(runner)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.skip_ai_error_diagnose is False
    assert runner.state.error_message is None
    assert runner.state.review_timeout_repost_attempted is False
    assert runner.state.review_timeout_repost_at is None
    assert any(
        "operator-cleared ERROR task frontmatter -> IDLE" in event
        for event in _events(runner)
    )


def test_frontmatter_fallback_after_redis_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert any(
        "guardrail operator-clearable ERROR park active" in event
        for event in _events(runner)
    )


def test_flag_off_uses_common_terminal_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    runner.repo_config.feature_flags = FeatureFlags(
        use_unified_inhibitor_check=False,
        use_single_error_exit=False,
    )
    _stub_cycle(monkeypatch, runner)
    calls: list[str] = []

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    asyncio.run(runner._run_cycle_body())

    assert calls == []
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.parametrize("flag", [False, True])
@pytest.mark.parametrize("omit_status", [False, True])
def test_unchanged_todo_diagnoses_until_bounded_exhaustion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, flag: bool, omit_status: bool,
) -> None:
    from tests.runner import _helpers as h

    runner = _runner(tmp_path)
    runner.repo_config.feature_flags.use_single_error_exit = flag
    runner.app_config.daemon.error_handler_use_ai = True
    _stub_cycle(monkeypatch, runner)
    _clear_task_frontmatter(runner)
    if omit_status:
        task_path = tmp_path / runner.state.current_task.task_file
        task_path.write_text(task_path.read_text().replace("status: TODO\n", ""))
    # A prior incident's ERROR evidence cannot authorize this new failure.
    asyncio.run(runner._transition_to_error(
        "Coder exited with nonzero status", save_run_record_as=None,
    ))
    assert not runner._confirmed_error_task_pr_ids
    h._patch_subprocess(monkeypatch)
    calls = []

    async def diagnose(*args, **kwargs):
        calls.append(args)
        return 1, "", "persistent diagnosis failure"

    for name in ("claude", "codex"):
        monkeypatch.setattr(runner._registry.get(name), "diagnose_error", diagnose)

    async def commit_status(task, status, reason, **kwargs):
        _write_task(tmp_path, task, kwargs["blocked_reason"], status=status)
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", commit_status)
    for count in range(1, 5):
        asyncio.run(runner._run_cycle_body())
        assert runner.state.state == PipelineState.ERROR
        assert runner._error_diagnose_count == count
        assert runner.state.error_message == "Coder exited with nonzero status"
        if count < 4:
            assert runner._frontmatter_error_status_for_current_task()[0] == (None if omit_status else "todo")

    assert len(calls) == 3
    assert runner._frontmatter_error_status_for_current_task() == (
        "error", SuppressionReason.DIAGNOSE_EXHAUSTED,
    )
    assert asyncio.run(runner._suppression_record_for_task(
        runner.state.current_task.pr_id,
    )).reason == SuppressionReason.DIAGNOSE_EXHAUSTED
    asyncio.run(runner._run_cycle_body())
    assert len(calls) == 3
    assert runner._error_diagnose_count == 4

    _clear_task_frontmatter(runner)
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert len(calls) == 3


def test_park_log_deduped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)
    _suppress(runner, SuppressionReason.GUARDRAIL)

    asyncio.run(runner._run_cycle_body())
    asyncio.run(runner._run_cycle_body())

    events = [
        event
        for event in _events(runner)
        if "guardrail operator-clearable ERROR park active" in event
    ]
    assert len(events) == 1


def test_operator_clearable_classification_documents_reason_table() -> None:
    operator_clearable = {
        SuppressionReason.CODER_ESCALATE,
        SuppressionReason.GUARDRAIL,
        SuppressionReason.FIX_ITERATION_CAP,
        SuppressionReason.NO_PUSH_DEADLOCK,
        SuppressionReason.OPERATOR_REJECT,
        SuppressionReason.DIAGNOSE_EXHAUSTED,
        SuppressionReason.OPERATOR_STOPPED,
    }

    for reason in SuppressionReason:
        assert is_operator_clearable(reason) is (reason in operator_clearable)


def test_operator_clearable_unknown_reason_is_false() -> None:
    assert is_operator_clearable("not_a_real_reason") is False


def test_frontmatter_helper_handles_missing_and_unsafe_tasks(
    tmp_path: Path,
) -> None:
    runner = _runner(tmp_path)

    runner.state.current_task = None
    assert runner._frontmatter_error_status_for_current_task() == (None, None)

    runner.state.current_task = SimpleNamespace(
        pr_id="PR-ABS",
        task_file=str((tmp_path / "tasks" / "PR-ABS.md").resolve()),
        branch="abs",
    )
    assert runner._frontmatter_error_status_for_current_task() == (None, None)

    runner.state.current_task = SimpleNamespace(
        pr_id="PR-PARENT",
        task_file="../tasks/PR-PARENT.md",
        branch="parent",
    )
    assert runner._frontmatter_error_status_for_current_task() == (None, None)

    runner.state.current_task = _task("PR-MISSING")
    assert runner._frontmatter_error_status_for_current_task() == (None, None)


def test_error_suppression_reason_handles_absent_task_and_store_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    runner.state.current_task = None
    assert asyncio.run(runner._error_suppression_reason()) == (None, False)

    runner.state.current_task = SimpleNamespace(pr_id="", task_file=None, branch=None)
    assert asyncio.run(runner._error_suppression_reason()) == (None, False)

    runner.state.current_task = _task("PR-STORE")

    async def fake_record(_task_id: str) -> SimpleNamespace:
        return SimpleNamespace(reason=SuppressionReason.GUARDRAIL)

    monkeypatch.setattr(runner, "_suppression_record_for_task", fake_record)

    assert asyncio.run(runner._error_suppression_reason()) == (
        SuppressionReason.GUARDRAIL,
        False,
    )


def test_error_suppression_reason_falls_back_after_store_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)

    async def boom(_task_id: str) -> None:
        raise RuntimeError("store down")

    monkeypatch.setattr(runner, "_suppression_record_for_task", boom)

    assert asyncio.run(runner._error_suppression_reason()) == (
        SuppressionReason.GUARDRAIL,
        True,
    )


def test_store_record_disappearance_cannot_authorize_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(tmp_path)
    _stub_cycle(monkeypatch, runner)

    async def fake_reason() -> tuple[SuppressionReason, bool]:
        return SuppressionReason.GUARDRAIL, False

    monkeypatch.setattr(runner, "_error_suppression_reason", fake_reason)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.ERROR
    assert any(
        "guardrail operator-clearable ERROR park active" in event
        for event in _events(runner)
    )


@pytest.fixture
def terminal_repo(tmp_path, monkeypatch):
    """Real Git preservation/status transport; GitHub observation is controlled."""
    from src.daemon import git_ops
    from src.models import InvocationSnapshot, PRInfo, PublicationAttempt, QueueTask, TaskStatus
    from src.github import prs as gh_prs
    from src.task_status import MergedState

    repo, remote = tmp_path / "repo", tmp_path / "remote.git"
    repo.mkdir()
    git = lambda *args: git_ops._git(str(repo), *args)
    git("init", "--bare", str(remote))
    git("init", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    git("remote", "add", "origin", str(remote))
    for number, dependency in [(1, "none"), (2, "none"), (3, "PR-001")]:
        task = _task(f"PR-{number:03}")
        _write_task(repo, task, None, status="TODO")
        path = repo / task.task_file
        path.write_text(path.read_text().replace("Depends on: none", f"Depends on: {dependency}"))
    git("add", "tasks")
    git("commit", "-m", "tasks")
    git("push", "origin", "main")
    git("checkout", "-b", "branch-pr-001")
    head = git("rev-parse", "HEAD").stdout.strip()
    git("push", "origin", "HEAD")
    runner = _make_runner(publication_contract=2)
    runner.repo_path = str(repo)
    runner.repo_config.url = str(remote)
    runner._recovered = runner._scaffolded = True
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="Failed A", branch="branch-pr-001",
        task_file="tasks/PR-001.md", status=TaskStatus.DOING,
    )
    runner.state.current_pr = PRInfo(number=1, branch="branch-pr-001", head_sha=head)
    runner.state.active_invocation = InvocationSnapshot(
        phase="coding", run_id="failed-A", attempt_index=1,
        plugin_id="claude", model_override=None, reasoning_effort_override=None,
    )
    runner.state.publication_attempt = PublicationAttempt(
        repo=runner.owner_repo, base="main", task_id="PR-001", branch="branch-pr-001",
        run_id="failed-A", attempt_index=1, started_at=datetime.now(timezone.utc),
        contract_version=2, expected_head_sha=head, cleanup_confirmed=True,
        pending=False, pr_number=1,
    )
    # Exercise the real isolated status writer; it must leave the task checkout alone.
    asyncio.run(runner._commit_and_park_in_error("A exhausted", subsource="fix_iteration_cap"))
    assert git("status", "--porcelain").stdout == ""
    assert git("rev-parse", "HEAD").stdout.strip() == head
    _stub_cycle(monkeypatch, runner)
    monkeypatch.setattr(gh_prs, "get_exact_pr_outcome", lambda *args, **kwargs: gh_prs.ExactPROutcome(
        runner.owner_repo, "main", 1, head, gh_prs.ExactPRState.OPEN, True,
    ))
    monkeypatch.setattr("src.daemon.handlers.idle._resolve_merged_state", lambda *args, **kwargs: MergedState(
        merged_branches=set(), merged_pr_ids=set(), api_available=True,
    ))
    return runner, git


@pytest.mark.parametrize("flag", [False, True])
def test_terminal_dispatch_releases_only_independent_work(terminal_repo, flag):
    from src.models import TaskStatus

    runner, git = terminal_repo
    runner.repo_config.feature_flags.use_single_error_exit = flag
    failed_pr = runner.state.current_pr.model_copy(deep=True)
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    retained = asyncio.run(runner._suppression_record_for_task("PR-001"))
    assert retained.reason == SuppressionReason.FIX_ITERATION_CAP
    assert retained.detail["terminal_failure"]["pr"]["number"] == 1
    assert retained.detail["terminal_failure"]["error_message"] == "A exhausted"
    git("fetch", "origin", "main")
    assert "status: ERROR" in git("show", "origin/main:tasks/PR-001.md").stdout
    # The selector sees the real updated base and the still-open failed PR.
    git("checkout", "main")
    git("pull", "--ff-only", "origin", "main")
    selected = asyncio.run(runner._select_next_task_or_attach([failed_pr], []))
    assert selected.pr_id == "PR-002"
    assert runner._idle_dag_statuses["PR-001"] == TaskStatus.ERROR
    from src.dag import get_eligible_tasks
    eligible = {item.pr_id for item in get_eligible_tasks(runner._idle_dag_headers, runner._idle_dag_statuses)}
    assert "PR-001" not in eligible
    assert "PR-003" not in eligible  # C depends on failed A, even while A's PR is open.
    assert runner._idle_dag_statuses["PR-003"] != TaskStatus.DONE
    assert not any(t.pr_id == "PR-003" and t.status == TaskStatus.DOING for t in runner.state.current_queue)
    assert asyncio.run(runner._suppression_record_for_task("PR-001")) is not None
    # Repeat selection cannot reattach exhausted A or select blocked C.
    assert asyncio.run(runner._select_next_task_or_attach([failed_pr], [])).pr_id == "PR-002"


@pytest.mark.parametrize("hold", [
    "unknown_cleanup", "unknown_child", "dirty", "remote", "storage", "read",
    "paused", "quota", "guardrail", "pending", "identity", "status", "unpublished",
])
def test_terminal_dispatch_holds_unproved_attempts(terminal_repo, monkeypatch, hold):
    runner, git = terminal_repo
    attempt = runner.state.publication_attempt
    if hold == "unknown_cleanup":
        attempt.cleanup_confirmed = False
    elif hold == "unknown_child":
        runner._current_coder_process = object()
    elif hold == "dirty":
        (Path(runner.repo_path) / "unpreserved.txt").write_text("work")
    elif hold == "remote":
        git("push", "origin", "--delete", attempt.branch)
    elif hold == "storage":
        async def ignored(*args):
            pass
        monkeypatch.setattr(runner, "_suppress_task", ignored)
    elif hold == "read":
        async def unavailable(*args):
            raise OSError("storage unavailable")
        monkeypatch.setattr(runner, "_suppression_record_for_task", unavailable)
    elif hold == "paused":
        runner.state.user_paused = True
    elif hold == "quota":
        runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=5)
    elif hold == "guardrail":
        asyncio.run(runner._commit_and_park_in_error("GUARDRAIL", subsource="guardrail"))
        asyncio.run(runner._suppress_task("PR-001", SuppressionReason.GUARDRAIL))
    elif hold == "pending":
        attempt.pending = True
    elif hold == "identity":
        attempt.run_id = "unknown-restart-owner"
    elif hold == "status":
        monkeypatch.setattr(runner, "_remote_frontmatter_error_status_for_current_task", lambda: (None, None, None))
    elif hold == "unpublished":
        (Path(runner.repo_path) / "work").write_text("unpublished")
        git("add", "work")
        git("commit", "-m", "unpublished work")
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state in (PipelineState.ERROR, PipelineState.PAUSED)
    assert runner.state.current_task.pr_id == "PR-001"
    assert runner.state.current_pr.number == 1
    if hold == "dirty":
        assert (Path(runner.repo_path) / "unpreserved.txt").read_text() == "work"


def test_terminal_release_without_pr_retains_published_work(terminal_repo, monkeypatch):
    runner, _ = terminal_repo
    runner.state.current_pr = None
    runner.state.publication_attempt.pr_number = None
    monkeypatch.setattr("src.github.prs.get_branch_publications", lambda *args: [])
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.IDLE
    record = asyncio.run(runner._suppression_record_for_task("PR-001"))
    assert record.detail["terminal_failure"]["pr"] is None
    assert record.detail["terminal_failure"]["head_sha"]


def test_terminal_technical_hold_reconciles_automatically(terminal_repo):
    runner, git = terminal_repo
    path = Path(runner.repo_path) / "saved-work"
    path.write_text("operator preserves this work elsewhere")
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR
    # Only the fixture owner removes its own scratch file after preservation.
    path.unlink()
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.IDLE
    assert git("rev-parse", "HEAD").stdout.strip()
    assert asyncio.run(runner._suppression_record_for_task("PR-001")) is not None


def test_terminal_unpublished_attempt_requires_base_preservation(terminal_repo, monkeypatch):
    runner, git = terminal_repo
    runner.state.current_pr = None
    runner.state.publication_attempt.pr_number = None
    git("push", "origin", "--delete", runner.state.publication_attempt.branch)
    monkeypatch.setattr("src.github.prs.get_branch_publications", lambda *args: [])
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.IDLE
    assert asyncio.run(runner._suppression_record_for_task("PR-001")) is not None


def test_status_repair_cannot_bypass_terminal_safety(terminal_repo, monkeypatch):
    from unittest.mock import AsyncMock

    runner, _ = terminal_repo
    writer = runner._commit_task_status_change
    runner._status_write_failed_task_pr_ids.add("PR-001")
    scratch = Path(runner.repo_path) / "unpreserved"
    scratch.write_text("retain")
    monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=False))
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR
    assert "PR-001" in runner._status_write_failed_task_pr_ids
    monkeypatch.setattr(runner, "_commit_task_status_change", writer)
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR
    assert "PR-001" not in runner._status_write_failed_task_pr_ids
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR
    assert scratch.read_text() == "retain"
    scratch.unlink()
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.IDLE


@pytest.mark.parametrize("fault", ["header", "suppression", "legacy", "ambiguous_absence", "mismatch"])
def test_terminal_release_requires_each_retained_identity(terminal_repo, monkeypatch, fault):
    runner, _ = terminal_repo
    if fault == "header":
        monkeypatch.setattr(runner, "_remote_frontmatter_error_status_for_current_task",
                            lambda: ("error", SuppressionReason.CRASH, "revision"))
    elif fault == "suppression":
        asyncio.run(runner._clear_task_suppression("PR-001"))
    elif fault == "legacy":
        runner.state.publication_attempt.contract_version = 1
    elif fault == "mismatch":
        runner.state.current_pr.head_sha = "a" * 40
    else:
        runner.state.current_pr = None
        runner.state.publication_attempt.pr_number = None
        monkeypatch.setattr("src.github.prs.get_branch_publications", lambda *args: [object(), object()])
    # Exercise the production evaluator directly so neither operator-clear
    # compatibility nor late-review admission can supply missing safety proof.
    asyncio.run(runner._release_finalized_failure(SuppressionReason.FIX_ITERATION_CAP))
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task.pr_id == "PR-001"


def test_publication_exhaustion_uses_common_release_proof(terminal_repo):
    from src.daemon import publication

    runner, _ = terminal_repo
    task = runner.state.current_task
    asyncio.run(runner._commit_task_status_change(task, "ERROR", "expired", blocked_reason=SuppressionReason.CRASH))
    asyncio.run(runner._suppress_task(task.pr_id, SuppressionReason.CRASH, {"error_message": "expired"}))
    runner.state.publication_attempt.pending = True
    runner.state.publication_attempt.exhausted = True
    asyncio.run(publication._exhaust(runner))
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    record = asyncio.run(runner._suppression_record_for_task(task.pr_id))
    assert record.reason == SuppressionReason.CRASH
    assert record.detail["terminal_failure"]["publication_attempt"]["exhausted"]
    assert record.detail["terminal_failure"]["pr"]["number"] == 1


def test_review_timeout_can_release_independent_work_without_retry(terminal_repo):
    runner, _ = terminal_repo
    task = runner.state.current_task
    asyncio.run(runner._commit_task_status_change(
        task, "ERROR", "review timed out", blocked_reason=SuppressionReason.REVIEW_TIMEOUT,
    ))
    asyncio.run(runner._suppress_task(task.pr_id, SuppressionReason.REVIEW_TIMEOUT, {"error_message": "timeout"}))
    runner.state.skip_ai_error_diagnose = True
    asyncio.run(runner._handle_single_error_exit())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert asyncio.run(runner._suppression_record_for_task(task.pr_id)).reason == SuppressionReason.REVIEW_TIMEOUT


@pytest.mark.parametrize("flag", [False, True])
def test_uncertain_restart_owner_holds_direct_and_common_error_callers(terminal_repo, monkeypatch, flag):
    from unittest.mock import AsyncMock

    runner, _ = terminal_repo
    runner.repo_config.feature_flags.use_single_error_exit = flag
    runner.state.error_message = "Persisted FIX invocation ownership is uncertain; operator Retry required"
    diagnosis = AsyncMock()
    monkeypatch.setattr(runner, "_refresh_auth_status_cache", diagnosis)
    # An archived cleanup observation cannot override unresolved restart ownership.
    assert runner.state.publication_attempt.cleanup_confirmed
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task.pr_id == "PR-001"
    assert any("Uncertain FIX invocation remains parked" in entry["event"] for entry in runner.state.history)
    asyncio.run(runner._run_cycle_body())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task.pr_id == "PR-001"
    assert runner.state.current_pr.number == 1
    diagnosis.assert_not_awaited()
    assert asyncio.run(runner._suppression_record_for_task("PR-001")) is not None


@pytest.mark.parametrize("flag", [False, True])
@pytest.mark.parametrize("reason", [SuppressionReason.REVIEW_TIMEOUT, SuppressionReason.FIX_ITERATION_CAP])
def test_lost_failure_record_cannot_release_or_impersonate_retry(terminal_repo, monkeypatch, flag, reason):
    from unittest.mock import AsyncMock

    runner, _ = terminal_repo
    task = runner.state.current_task
    runner.repo_config.feature_flags.use_single_error_exit = flag
    asyncio.run(runner._commit_task_status_change(task, "ERROR", "terminal", blocked_reason=reason))
    asyncio.run(runner._suppress_task(task.pr_id, reason, {"error_message": "terminal"}))
    runner.state.skip_ai_error_diagnose = reason == SuppressionReason.REVIEW_TIMEOUT
    # Simulate an eviction/flush after the first rich-layer observation.
    asyncio.run(runner._clear_task_suppression(task.pr_id))
    async def first_observation():
        return reason, False
    monkeypatch.setattr(runner, "_error_suppression_reason", first_observation)
    diagnose = AsyncMock()
    monkeypatch.setattr(runner, "handle_error", diagnose)
    for _ in range(2):
        asyncio.run(runner._run_cycle_body())
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.current_task.pr_id == task.pr_id
        assert runner.state.current_pr.number == 1
    assert runner._remote_frontmatter_error_status_for_current_task()[0] == "error"
    diagnose.assert_not_awaited()


@pytest.mark.parametrize("publication", ["pr", "branch", "unpublished"])
def test_terminal_observations_leave_event_loop_responsive(terminal_repo, monkeypatch, publication):
    from threading import Event, get_ident

    from src.daemon import git_ops
    from src.github import prs as gh_prs

    runner, git = terminal_repo
    if publication != "pr":
        runner.state.current_pr = None
        runner.state.publication_attempt.pr_number = None
        monkeypatch.setattr(gh_prs, "get_branch_publications", lambda *args: [])
    if publication == "unpublished":
        git("push", "origin", "--delete", runner.state.publication_attempt.branch)

    observed = set()
    failures = []
    persisted = []

    async def reconcile():
        loop = asyncio.get_running_loop()
        loop_thread = get_ident()

        def responsive_probe(label, original):
            def probe(*args, **kwargs):
                observed.add(label)
                # A separate repo cycle/stop monitor must be able to run while
                # any one of these synchronous observations waits on transport.
                progressed = Event()
                loop.call_soon_threadsafe(progressed.set)
                if get_ident() == loop_thread or not progressed.wait(timeout=5):
                    failures.append(label)
                return original(*args, **kwargs)
            return probe

        monkeypatch.setattr(runner, "_remote_frontmatter_error_status_for_current_task", responsive_probe(
            "frontmatter", runner._remote_frontmatter_error_status_for_current_task,
        ))
        monkeypatch.setattr(git_ops, "_git", responsive_probe("git", git_ops._git))
        github_probe = "get_exact_pr_outcome" if publication == "pr" else "get_branch_publications"
        monkeypatch.setattr(gh_prs, github_probe, responsive_probe("github", getattr(gh_prs, github_probe)))
        suppress = runner._suppress_task

        async def persist(*args):
            persisted.append(get_ident())
            assert get_ident() == loop_thread
            assert runner.state.state == PipelineState.ERROR
            assert runner.state.current_task.pr_id == "PR-001"
            await suppress(*args)

        monkeypatch.setattr(runner, "_suppress_task", persist)
        await runner._handle_single_error_exit()
        assert persisted == [loop_thread]

    asyncio.run(reconcile())
    assert observed == {"frontmatter", "git", "github"}
    assert failures == []
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    retained = asyncio.run(runner._suppression_record_for_task("PR-001"))
    assert retained.reason == SuppressionReason.FIX_ITERATION_CAP
    assert retained.detail["terminal_failure"]["head_sha"]


@pytest.mark.parametrize("flag", [False, True])
def test_unavailable_remote_status_does_not_start_diagnosis(tmp_path, monkeypatch, flag):
    from unittest.mock import AsyncMock

    runner = _runner(tmp_path)
    runner.repo_config.feature_flags.use_single_error_exit = flag
    runner.app_config.daemon.error_handler_use_ai = True
    _stub_cycle(monkeypatch, runner)
    _clear_task_frontmatter(runner)
    asyncio.run(runner._transition_to_error(
        "Coder exited with nonzero status", save_run_record_as=None,
    ))
    monkeypatch.setattr(runner, "_remote_frontmatter_error_status_for_current_task",
                        lambda: (None, None, None))
    diagnose = AsyncMock()
    monkeypatch.setattr(runner, "handle_error", diagnose)

    asyncio.run(runner._run_cycle_body())

    diagnose.assert_not_awaited()
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task.pr_id == "PR-380"
    assert runner.state.error_message == "Coder exited with nonzero status"
    assert any("Remote ERROR task status unavailable" in event for event in _events(runner))
