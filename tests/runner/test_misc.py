"""PR-224b: Miscellaneous runner tests.

Mechanical move from tests/test_runner.py for tests that don't fit one
of the other thematic buckets in ``tests/runner/``. Helpers live in
``tests/runner/_helpers.py``.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.coder_registry import CoderRegistry
from src.coders import claude as claude_plugin_module
from src.daemon import git_ops as git_ops_module
from src.daemon import recovery_policy as recovery_policy_module
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
from src.models import (
    PipelineState,
    PRInfo,
    QueueTask,
    TaskStatus,
)

from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli



# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — misc group
# ---------------------------------------------------------------------------


def test_preflight_returns_true_on_clean_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch, stdout="")
    runner = h._make_runner()

    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None


def test_preflight_returns_false_on_dirty_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch, stdout=" M src/foo.py\n?? artifacts/")
    runner = h._make_runner()

    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert "foo.py" in (runner.state.error_message or "")
    assert runner.state.history, "log_event should append an entry"


def test_preflight_sets_error_when_git_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        raise subprocess.CalledProcessError(128, cmd, stderr="not a git repo")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR


def test_preflight_handles_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing git binary or cwd raises ``OSError`` from subprocess.run.
    Without catching it, the exception escapes to daemon.main's generic
    handler and the runner state stays stale; preflight must translate
    it into ERROR state.
    """
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        raise FileNotFoundError("git: not found")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert "preflight failed" in (runner.state.error_message or "")


def test_sync_to_main_handles_oserror(monkeypatch: pytest.MonkeyPatch) -> None:
    """``sync_to_main`` translates ``OSError`` to ``RuntimeError`` so
    the caller's structured error-state translation covers missing git
    binary / cwd instead of letting the exception escape unhandled."""
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        raise FileNotFoundError("git: not found")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    with pytest.raises(RuntimeError, match="sync_to_main OS error"):
        runner.sync_to_main()


def test_log_event_caps_history_at_100(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()

    def _label(idx: int) -> str:
        # Distinct non-numeric labels so fuzzy dedup never collapses them.
        return f"event-{chr(ord('a') + idx % 10)}-{chr(ord('a') + idx // 10)}"

    for i in range(150):
        runner.log_event(_label(i))

    assert len(runner.state.history) == 100
    assert runner.state.history[0]["event"] == _label(50)
    assert runner.state.history[-1]["event"] == _label(149)


def test_log_event_deduplicates_consecutive_identical_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime:
        _values = iter(
            [
                datetime(2026, 4, 20, 11, 55, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 20, 12, 5, 0, tzinfo=timezone.utc),
            ]
        )

        @classmethod
        def now(cls, tz: timezone) -> datetime:
            value = next(cls._values)
            return value.astimezone(tz)

    monkeypatch.setattr(runner_module, "datetime", _FakeDateTime)
    runner = h._make_runner()

    runner.log_event("No tasks available")
    runner.log_event("No tasks available")

    assert len(runner.state.history) == 1
    assert runner.state.history[0]["count"] == 2
    assert runner.state.history[0]["time"] == "2026-04-20T12:00:00+00:00"
    assert runner.state.history[0]["last_seen_at"] == "2026-04-20T12:05:00+00:00"


def test_log_event_does_not_deduplicate_when_state_changes() -> None:
    runner = h._make_runner()

    runner.state.state = PipelineState.IDLE
    runner.log_event("No tasks available")
    runner.state.state = PipelineState.WATCH
    runner.log_event("No tasks available")

    assert len(runner.state.history) == 2
    assert [entry["state"] for entry in runner.state.history] == ["IDLE", "WATCH"]
    assert all(entry.get("count", 1) == 1 for entry in runner.state.history)


def test_log_event_starts_new_counter_after_different_event() -> None:
    runner = h._make_runner()

    runner.log_event("No tasks available")
    runner.log_event("No tasks available")
    runner.log_event("Picked task PR-130")
    runner.log_event("No tasks available")

    assert len(runner.state.history) == 3
    assert runner.state.history[0]["event"] == "No tasks available"
    assert runner.state.history[0]["count"] == 2
    assert runner.state.history[1]["event"] == "Picked task PR-130"
    assert runner.state.history[1]["count"] == 1
    assert runner.state.history[2]["event"] == "No tasks available"
    assert runner.state.history[2]["count"] == 1


def test_normalize_for_dedup_replaces_numeric_runs() -> None:
    assert (
        runner_module._normalize_for_dedup("PR #221 waiting 1/20m")
        == "PR #221 waiting #/#m"
    )
    assert runner_module._normalize_for_dedup("no numbers here") == "no numbers here"
    assert runner_module._normalize_for_dedup("123") == "#"
    assert runner_module._normalize_for_dedup("1a2b3") == "#a#b#"


def test_normalize_for_dedup_preserves_pr_identifier() -> None:
    """``PR #<n>`` tokens are NOT normalized so different PRs stay distinct."""
    five = runner_module._normalize_for_dedup(
        "PR #5 waiting (review=APPROVED, ci=SUCCESS, 1/20m)"
    )
    six = runner_module._normalize_for_dedup(
        "PR #6 waiting (review=APPROVED, ci=SUCCESS, 1/20m)"
    )
    assert five != six
    assert "PR #5" in five
    assert "PR #6" in six


def test_log_event_fuzzy_dedupes_messages_differing_only_in_numbers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeDateTime:
        _values = iter(
            [
                datetime(2026, 4, 28, 11, 59, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 28, 12, 1, 0, tzinfo=timezone.utc),
            ]
        )

        @classmethod
        def now(cls, tz: timezone) -> datetime:
            return next(cls._values).astimezone(tz)

    monkeypatch.setattr(runner_module, "datetime", _FakeDateTime)
    runner = h._make_runner()

    runner.log_event("PR #221 waiting (review=EYES, ci=PENDING, 1/20m)")
    runner.log_event("PR #221 waiting (review=EYES, ci=PENDING, 2/20m)")

    assert len(runner.state.history) == 1
    entry = runner.state.history[0]
    assert entry["count"] == 2
    assert entry["event"] == "PR #221 waiting (review=EYES, ci=PENDING, 2/20m)"
    assert entry["time"] == "2026-04-28T12:00:00+00:00"
    assert entry["last_seen_at"] == "2026-04-28T12:01:00+00:00"


def test_log_event_fuzzy_dedupes_three_in_a_row() -> None:
    runner = h._make_runner()

    for i in range(1, 4):
        runner.log_event(f"PR #221 waiting ({i}/20m)")

    assert len(runner.state.history) == 1
    assert runner.state.history[0]["count"] == 3
    assert runner.state.history[0]["event"] == "PR #221 waiting (3/20m)"


def test_log_event_does_not_fuzzy_dedupe_when_non_numeric_content_differs() -> None:
    runner = h._make_runner()

    runner.log_event("PR #221 merged")
    runner.log_event("PR #221 closed")

    assert len(runner.state.history) == 2
    assert runner.state.history[0]["event"] == "PR #221 merged"
    assert runner.state.history[1]["event"] == "PR #221 closed"
    assert runner.state.history[0]["count"] == 1
    assert runner.state.history[1]["count"] == 1


def test_log_event_does_not_fuzzy_dedupe_across_pr_numbers() -> None:
    """Switching PR numbers must produce two history rows, not one merged row.

    Regression: the previous regex normalized every digit run, so consecutive
    WATCH cycles for two different PRs (e.g., PR #5 -> PR #6) with the same
    review/CI text would collapse and the earlier PR transition would be lost.
    """
    runner = h._make_runner()

    runner.log_event("PR #5 waiting (review=APPROVED, ci=SUCCESS, 1/20m)")
    runner.log_event("PR #6 waiting (review=APPROVED, ci=SUCCESS, 1/20m)")

    assert len(runner.state.history) == 2
    assert "PR #5" in runner.state.history[0]["event"]
    assert "PR #6" in runner.state.history[1]["event"]
    assert runner.state.history[0]["count"] == 1
    assert runner.state.history[1]["count"] == 1


def test_log_event_resets_count_after_fuzzy_streak_breaks() -> None:
    runner = h._make_runner()

    runner.log_event("PR #5 waiting (1/20m)")
    runner.log_event("PR #5 waiting (2/20m)")
    runner.log_event("PR #5 merged")
    runner.log_event("PR #5 waiting (3/20m)")

    assert len(runner.state.history) == 3
    assert runner.state.history[0]["count"] == 2
    assert runner.state.history[0]["event"] == "PR #5 waiting (2/20m)"
    assert runner.state.history[1]["count"] == 1
    assert runner.state.history[1]["event"] == "PR #5 merged"
    assert runner.state.history[2]["count"] == 1
    assert runner.state.history[2]["event"] == "PR #5 waiting (3/20m)"


def test_start_current_run_record_sets_stage_coder() -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    runner._start_current_run_record("claude", "opus")

    assert runner._current_run_record is not None
    assert runner._current_run_record.stage == "coder"


def test_start_current_run_record_clears_record_without_task() -> None:
    runner = h._make_runner()
    runner._current_run_record = object()  # type: ignore[assignment]

    runner._start_current_run_record("claude", "opus")

    assert runner._current_run_record is None


def test_refresh_auth_status_cache_returns_early_when_cache_is_fresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner._auth_status_cache = {"claude": {"status": "ok"}}
    runner._auth_status_cache_expires_at = datetime.now(timezone.utc) + timedelta(minutes=1)

    async def fail_to_thread(*args: object, **kwargs: object) -> object:
        raise AssertionError("asyncio.to_thread should not be called")

    monkeypatch.setattr(runner_module.asyncio, "to_thread", fail_to_thread)

    asyncio.run(runner._refresh_auth_status_cache())

    assert runner._auth_status_cache == {"claude": {"status": "ok"}}


def test_refresh_auth_status_cache_marks_plugin_probe_errors() -> None:
    class _Plugin:
        display_name = "fake"
        models: list[str] = []

        def __init__(self, name: str, status: dict[str, str] | Exception) -> None:
            self.name = name
            self._status = status

        async def run_planned_pr(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
            return (0, "", "")

        async def fix_review(self, *args: object, **kwargs: object) -> tuple[int, str, str]:
            return (0, "", "")

        def check_auth(self) -> dict[str, str]:
            if isinstance(self._status, Exception):
                raise self._status
            return self._status

        def create_usage_provider(self, **kwargs: object) -> None:
            return None

        def rate_limit_patterns(self) -> list[re.Pattern[str]]:
            return []

    registry = CoderRegistry()
    registry.register(_Plugin("claude", {"status": "ok", "detail": "ready"}))
    registry.register(_Plugin("codex", RuntimeError("boom")))
    claude_provider, codex_provider = h._usage_providers()
    runner = PipelineRunner(
        h._repo_cfg(),
        h._app_cfg(),
        h._FakeRedis(),
        claude_provider,
        codex_provider,
        registry=registry,
    )

    asyncio.run(runner._refresh_auth_status_cache())

    assert runner._auth_status_cache == {
        "claude": {"status": "ok", "detail": "ready"},
        "codex": {"status": "error"},
    }
    assert runner._auth_status_cache_expires_at is not None
    assert runner._auth_status_cache_expires_at > datetime.now(timezone.utc)


def test_compute_diff_stats_returns_populated_fields_on_clean_diff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        assert cmd == ["git", "diff", "--numstat", "origin/main...HEAD"]
        return h._FakeCompletedProcess(
            args=cmd,
            stdout="10\t2\tsrc/app.py\n3\t1\ttests/test_app.py\n-\t-\tassets/logo.png\n",
            returncode=0,
        )

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    stats = runner._compute_diff_stats("main")

    assert stats == {
        "files_touched_count": 3,
        "languages_touched": ["python"],
        "diff_lines_added": 13,
        "diff_lines_deleted": 3,
        "test_file_ratio": 0.333,
    }


def test_compute_diff_stats_returns_empty_dict_on_git_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        return h._FakeCompletedProcess(args=cmd, stderr="boom", returncode=1)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    assert runner._compute_diff_stats("main") == {}


def test_compute_diff_stats_skips_malformed_and_invalid_numstat_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        return h._FakeCompletedProcess(
            args=cmd,
            stdout="bad row\nbogus\t1\tsrc/broken.py\n2\t4\tsrc/app.py\n",
            returncode=0,
        )

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    stats = runner._compute_diff_stats("main")

    assert stats == {
        "files_touched_count": 1,
        "languages_touched": ["python"],
        "diff_lines_added": 2,
        "diff_lines_deleted": 4,
        "test_file_ratio": 0.0,
    }


def test_ext_to_language_returns_none_for_unknown_extension() -> None:
    assert PipelineRunner._ext_to_language("notes.unknown") is None


def test_save_populates_enriched_fields_on_success_merged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    monkeypatch.setattr(
        runner,
        "_compute_diff_stats",
        lambda base_branch: {
            "files_touched_count": 4,
            "languages_touched": ["python", "yaml"],
            "diff_lines_added": 20,
            "diff_lines_deleted": 5,
            "test_file_ratio": 0.25,
        },
    )

    asyncio.run(runner._save_current_run_record("success_merged"))

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].files_touched_count == 4
    assert recent[0].languages_touched == ["python", "yaml"]
    assert recent[0].diff_lines_added == 20
    assert recent[0].diff_lines_deleted == 5
    assert recent[0].test_file_ratio == 0.25
    assert recent[0].base_branch == "main"


def test_save_skips_enriched_fields_on_error_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    calls: list[str] = []

    def fake_compute(base_branch: str) -> dict[str, object]:
        calls.append(base_branch)
        return {"files_touched_count": 99}

    monkeypatch.setattr(runner, "_compute_diff_stats", fake_compute)

    asyncio.run(runner._save_current_run_record("error"))

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert calls == []
    assert len(recent) == 1
    assert recent[0].files_touched_count == 0
    assert recent[0].languages_touched == []
    assert recent[0].base_branch == ""


def test_checkpoint_current_run_record_skips_save_without_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    save_calls: list[object] = []

    async def fake_save(record: object) -> None:
        save_calls.append(record)

    monkeypatch.setattr(runner._metrics_store, "save", fake_save)

    asyncio.run(runner._checkpoint_current_run_record())

    assert save_calls == []


def test_restore_current_run_record_clears_state_without_task() -> None:
    runner = h._make_runner()
    runner._current_run_record = object()  # type: ignore[assignment]

    asyncio.run(runner._restore_current_run_record())

    assert runner._current_run_record is None


def test_restore_current_run_record_logs_metrics_lookup_failure() -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )

    async def fake_recent(**kwargs: Any) -> list[object]:
        raise RuntimeError("metrics unavailable")

    runner._metrics_store.recent = fake_recent  # type: ignore[method-assign]

    asyncio.run(runner._restore_current_run_record())

    assert runner._current_run_record is None
    assert any(
        "restore_current_run_record failed for PR-001: metrics unavailable"
        in entry["event"]
        for entry in runner.state.history
    )


def test_save_current_run_record_sets_duration_none_for_invalid_started_at() -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    assert runner._current_run_record is not None
    runner._current_run_record.started_at = "not-an-iso-timestamp"

    asyncio.run(
        runner._save_current_run_record(
            "coding_complete",
            diff_stats={},
            base_branch="main",
        )
    )

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].duration_ms is None


def test_merge_finalizes_record(monkeypatch: pytest.MonkeyPatch) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].exit_reason == "success_merged"
    assert recent[0].ended_at is not None
    assert recent[0].duration_ms is not None
    assert runner._current_run_record is None


def test_merge_calculates_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        runner_module.github_client, "merge_pr", lambda repo, num: None
    )
    monkeypatch.setattr(
        runner_module.PipelineRunner, "_mark_queue_done", lambda self: None
    )

    fixed_now = datetime(2026, 4, 18, 12, 0, 6, 500000, tzinfo=timezone.utc)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            if tz is None:
                return fixed_now.replace(tzinfo=None)
            return fixed_now.astimezone(tz)

    monkeypatch.setattr(runner_module, "datetime", _FixedDateTime)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._start_current_run_record("claude", "opus")
    assert runner._current_run_record is not None
    runner._current_run_record.started_at = "2026-04-18T12:00:00+00:00"

    asyncio.run(runner.handle_merge())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert len(recent) == 1
    assert recent[0].duration_ms == 6500


def test_preflight_routes_through_bounded_recovery_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guard: the dirty-tree site must call BoundedRecoveryPolicy
    rather than rebuilding the increment/threshold dance inline."""
    h._patch_subprocess(monkeypatch, stdout=" M src/foo.py\n")
    runner = h._make_runner()

    increment_calls: list[str] = []
    maybe_escalate_calls: list[str] = []
    orig_increment = recovery_policy_module.BoundedRecoveryPolicy.increment
    orig_maybe_escalate = recovery_policy_module.BoundedRecoveryPolicy.maybe_escalate

    def spy_increment(self: Any, ctx: Any) -> int:
        increment_calls.append(self.name)
        return orig_increment(self, ctx)

    async def spy_maybe_escalate(self: Any, ctx: Any) -> bool:
        maybe_escalate_calls.append(self.name)
        return await orig_maybe_escalate(self, ctx)

    monkeypatch.setattr(
        recovery_policy_module.BoundedRecoveryPolicy, "increment", spy_increment
    )
    monkeypatch.setattr(
        recovery_policy_module.BoundedRecoveryPolicy,
        "maybe_escalate",
        spy_maybe_escalate,
    )

    asyncio.run(runner.preflight())
    assert increment_calls == ["dirty_tree_auto_reset"]
    assert maybe_escalate_calls == ["dirty_tree_auto_reset"]


def test_save_cli_log_includes_stderr() -> None:
    """Both stdout and stderr must be saved to the CLI log."""
    runner = h._make_runner()
    asyncio.run(
        runner._save_cli_log("out text", "err text", "LABEL")
    )
    stored = runner.redis.store.get(f"cli_log:{runner.name}:latest")
    assert stored is not None
    assert "out text" in stored
    assert "err text" in stored
    assert "=== STDOUT ===" in stored
    assert "=== STDERR ===" in stored


def test_save_cli_log_truncates_and_warns_on_redis_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FailingRedis(h._FakeRedis):
        async def set(self, key: str, value: str, ex: int | None = None) -> None:
            raise OSError("disk full")

    warnings: list[str] = []
    events: list[str] = []
    runner = h._make_runner()
    runner.redis = _FailingRedis()
    runner.log_event = events.append  # type: ignore[method-assign]
    monkeypatch.setattr(
        runner_module.logger,
        "warning",
        lambda msg, *args: warnings.append(msg % args),
    )

    asyncio.run(runner._save_cli_log("x" * (70 * 1024), "err text", "LABEL"))

    assert warnings == [f"Failed to save CLI log for {runner.name}"]
    assert events and events[0].startswith("[INFRA] LABEL: [truncated]")
    # `[INFRA] ` (8 chars) is added on top of the existing 207 bound, plus
    # a trailing period.
    assert len(events[0]) <= 207 + len("[INFRA] ") + 1


def test_run_cycle_returns_after_preflight_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_false_stub)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert publishes == ["published"]


def test_sync_to_main_retries_fetch_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sync_to_main retries git fetch on transient TimeoutExpired."""
    calls: list[tuple] = []

    def fake_git(repo_path: str, *args: str, **kw: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[0] == "fetch" and len(calls) == 1:
            raise subprocess.TimeoutExpired(cmd=["git", "fetch"], timeout=60)
        return h._FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    runner = h._make_runner()
    runner.sync_to_main()

    fetch_calls = [c for c in calls if c[0] == "fetch"]
    assert len(fetch_calls) == 2


def test_sync_to_main_fails_after_retries_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sync_to_main propagates RuntimeError after retries exhausted."""

    def fake_git(repo_path: str, *args: str, **kw: Any) -> h._FakeCompletedProcess:
        if args[0] == "fetch":
            raise subprocess.TimeoutExpired(cmd=["git", "fetch"], timeout=60)
        return h._FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    runner = h._make_runner()
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        runner.sync_to_main()


def test_git_checkout_does_not_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Local git operations (checkout) are NOT wrapped in retry."""
    calls: list[tuple] = []

    def fake_git(repo_path: str, *args: str, **kw: Any) -> h._FakeCompletedProcess:
        calls.append(args)
        if args[0] == "checkout":
            raise subprocess.CalledProcessError(
                1, ["git", "checkout"], stderr="error: pathspec 'foo' did not match"
            )
        return h._FakeCompletedProcess(args=list(args), returncode=0)

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr("src.retry.time.sleep", lambda _: None)

    runner = h._make_runner()
    with pytest.raises(subprocess.CalledProcessError):
        runner.sync_to_main()

    checkout_calls = [c for c in calls if c[0] == "checkout"]
    assert len(checkout_calls) == 1


def test_pop_stop_request_returns_false_when_redis_get_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()

    async def boom_get(key: str) -> str | None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(runner.redis, "get", boom_get)

    assert asyncio.run(runner._pop_stop_request()) is False


def test_pop_stop_request_returns_true_when_delete_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.redis.store[f"control:{runner.name}:stop"] = "1"

    async def boom_delete(key: str) -> int:
        raise RuntimeError("delete failed")

    monkeypatch.setattr(runner.redis, "delete", boom_delete)

    assert asyncio.run(runner._pop_stop_request()) is True
