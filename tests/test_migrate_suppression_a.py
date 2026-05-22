from __future__ import annotations

from pathlib import Path

import pytest
from src.config import FeatureFlags
from src.daemon.handlers import idle as idle_module
from src.dag import get_eligible_tasks
from src.models import PRInfo, QueueTask, TaskStatus
from src.queue_parser import TaskHeader
from src.subsource_registry import SuppressionReason
from src.task_status import MergedState

from tests.runner._helpers import _make_runner


def _runner(*, single_exit: bool = True):
    return _make_runner(
        feature_flags=FeatureFlags(
            use_unified_inhibitor_check=False,
            use_single_error_exit=single_exit,
        )
    )


def _header(pr_id: str = "PR-381") -> TaskHeader:
    return TaskHeader(
        pr_id=pr_id,
        title=f"{pr_id}: migrate suppression",
        branch=f"{pr_id.lower()}-branch",
        task_type="refactor",
        complexity="low",
        depends_on=[],
        priority=1,
        coder="any",
    )


def _write_task(repo: Path, pr_id: str, *, status: str | None = None) -> None:
    frontmatter = f"---\nstatus: {status}\n---\n\n" if status else ""
    (repo / "tasks").mkdir(parents=True, exist_ok=True)
    (repo / "tasks" / f"{pr_id}.md").write_text(
        frontmatter
        + f"# {pr_id}: migrate suppression\n"
        + f"Branch: {pr_id.lower()}-branch\n"
        + "- Type: refactor\n"
        + "- Complexity: low\n"
        + "- Depends on: none\n"
        + "- Priority: 1\n"
        + "- Coder: any\n",
        encoding="utf-8",
    )


async def _store_status(runner, header: TaskHeader) -> TaskStatus:
    record = await runner._suppression_record_for_task(header.pr_id)
    if record is not None and runner._task_suppression_blocks_selection(
        record.reason
    ):
        return TaskStatus.ERROR
    return TaskStatus.TODO


async def _eligible_after_store(runner, header: TaskHeader) -> list[str]:
    statuses = {header.pr_id: await _store_status(runner, header)}
    return [task.pr_id for task in get_eligible_tasks([header], statuses)]


async def test_crashed_becomes_crash_suppression() -> None:
    runner = _runner()
    header = _header()

    await runner._suppress_task(
        header.pr_id,
        SuppressionReason.CRASH,
        {"source": "recovery_doing_without_pr"},
    )

    record = await runner._suppression_record_for_task(header.pr_id)
    assert record is not None
    assert record.reason == SuppressionReason.CRASH
    assert await _eligible_after_store(runner, header) == []


async def test_diagnose_exhausted_becomes_suppression() -> None:
    runner = _runner()
    header = _header()
    runner._error_diagnose_count = 4

    await runner._mark_diagnose_exhausted(header.pr_id)

    record = await runner._suppression_record_for_task(header.pr_id)
    assert record is not None
    assert record.reason == SuppressionReason.DIAGNOSE_EXHAUSTED
    assert record.detail["attempt_count"] == 4
    assert await runner._is_diagnose_exhausted(header.pr_id) is True
    assert await _eligible_after_store(runner, header) == []


async def test_skip_ai_becomes_review_timeout_suppression() -> None:
    runner = _runner()
    header = _header()

    await runner._suppress_task(
        header.pr_id,
        SuppressionReason.REVIEW_TIMEOUT,
        {"elapsed_min": 31, "review_status": "pending"},
    )

    record = await runner._suppression_record_for_task(header.pr_id)
    assert record is not None
    assert record.reason == SuppressionReason.REVIEW_TIMEOUT
    assert runner.state.skip_ai_error_diagnose is False
    assert await _eligible_after_store(runner, header) == []


async def test_clear_releases_all_three() -> None:
    runner = _runner()

    for reason in (
        SuppressionReason.CRASH,
        SuppressionReason.REVIEW_TIMEOUT,
        SuppressionReason.DIAGNOSE_EXHAUSTED,
    ):
        header = _header(f"PR-{reason.value.upper()}")
        await runner._suppress_task(header.pr_id, reason, {})
        assert await _eligible_after_store(runner, header) == []

        await runner._clear_task_suppression(header.pr_id)

        assert await runner._suppression_record_for_task(header.pr_id) is None
        assert await _eligible_after_store(runner, header) == [header.pr_id]


async def test_equivalence_flag_off_vs_on() -> None:
    header = _header()
    legacy = _runner(single_exit=False)
    migrated = _runner(single_exit=True)

    legacy._crashed_task_pr_ids.add(header.pr_id)
    await migrated._suppress_task(header.pr_id, SuppressionReason.CRASH, {})

    legacy_statuses = {
        header.pr_id: (
            TaskStatus.ERROR
            if header.pr_id in legacy._crashed_task_pr_ids
            else TaskStatus.TODO
        )
    }
    migrated_statuses = {header.pr_id: await _store_status(migrated, header)}

    assert get_eligible_tasks([header], legacy_statuses) == get_eligible_tasks(
        [header], migrated_statuses
    )


async def test_old_storage_removed_when_flag_on() -> None:
    runner = _runner(single_exit=True)
    header = _header()

    await runner._mark_diagnose_exhausted(header.pr_id)
    await runner._suppress_task(header.pr_id, SuppressionReason.CRASH, {})
    await runner._suppress_task("PR-RT", SuppressionReason.REVIEW_TIMEOUT, {})

    assert runner._crashed_task_pr_ids == set()
    assert runner.state.skip_ai_error_diagnose is False
    assert f"diagnose_exhausted:{runner.name}:{header.pr_id}" not in runner.redis.store


async def test_idle_selector_reads_store_for_status_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(single_exit=True)
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [
        PRInfo(number=3, branch="pr-003-branch", pr_id="PR-003"),
    ]
    runner._idle_merged_prs = []
    monkeypatch.setattr(
        idle_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), set(), True),
    )

    _write_task(tmp_path, "PR-001")
    _write_task(tmp_path, "PR-002", status="TODO")
    _write_task(tmp_path, "PR-003")
    _write_task(tmp_path, "PR-004")
    await runner._suppress_task("PR-002", SuppressionReason.CRASH, {})
    await runner._suppress_task("PR-003", SuppressionReason.CRASH, {})
    await runner._suppress_task("PR-004", SuppressionReason.CRASH, {})

    selected = await runner._select_next_task_from_dag()

    assert selected is not None
    assert selected.pr_id == "PR-003"
    assert selected.status == TaskStatus.DOING
    assert await runner._suppression_record_for_task("PR-002") is None
    assert await runner._suppression_record_for_task("PR-003") is None
    assert await runner._suppression_record_for_task("PR-004") is not None


async def test_recovery_error_status_records_crash_suppression() -> None:
    runner = _runner(single_exit=True)
    tasks = [
        QueueTask(
            pr_id="PR-381",
            title="errored task",
            status=TaskStatus.ERROR,
            branch="pr-381-branch",
        )
    ]

    await runner._apply_recovery_decisions(tasks, [])

    record = await runner._suppression_record_for_task("PR-381")
    assert record is not None
    assert record.reason == SuppressionReason.CRASH
    assert record.detail["source"] == "recovery_error_status"


async def test_recovery_doing_without_pr_records_crash_suppression(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(single_exit=True)
    task = QueueTask(
        pr_id="PR-381",
        title="doing task",
        status=TaskStatus.DOING,
        branch="pr-381-branch",
    )

    async def crash_branch(_task_id: str) -> str:
        return "crash"

    async def record_missing(_task_id: str) -> None:
        return None

    async def persist_backup() -> None:
        return None

    monkeypatch.setattr(runner, "_dispatch_recovery_branch", crash_branch)
    monkeypatch.setattr(runner, "_record_crash_cancellation_if_missing", record_missing)
    monkeypatch.setattr(runner, "_persist_pending_backup_branch_write", persist_backup)
    monkeypatch.setattr(runner, "_preserve_crashed_run_commits", lambda _branch: True)
    monkeypatch.setattr(runner, "_is_doing_already_merged", lambda _task: False)

    await runner._apply_recovery_decisions([task], [])

    record = await runner._suppression_record_for_task("PR-381")
    assert record is not None
    assert record.reason == SuppressionReason.CRASH
    assert record.detail["source"] == "recovery_doing_without_pr"


async def test_empty_suppression_ids_are_noops() -> None:
    runner = _runner(single_exit=True)

    await runner._suppress_task("", SuppressionReason.CRASH, {})
    await runner._clear_task_suppression("")

    assert runner.redis.store == {}


async def test_flag_off_exit_retry_clears_legacy_diagnose_key() -> None:
    runner = _runner(single_exit=False)
    runner.state.current_task = type("Task", (), {"pr_id": "PR-381"})()
    await runner._mark_diagnose_exhausted("PR-381")

    await runner._exit_error_to_idle_for_retry(SuppressionReason.CRASH)

    assert await runner._is_diagnose_exhausted("PR-381") is False


def test_recovery_no_longer_duplicates_crash() -> None:
    recovery = Path("src/daemon/recovery.py").read_text(encoding="utf-8")

    assert "_suppress_task" in recovery
    assert "SuppressionReason.CRASH" in recovery
    assert "rehydrate the crashed-task set" not in recovery
