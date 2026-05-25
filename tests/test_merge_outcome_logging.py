"""Integration test: handle_merge appends a row to the analytics log."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from src.analytics.outcome_logger import compute_task_id_hash
from src.config import AppConfig, CoderType, DaemonConfig, RepoConfig
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.lists: dict[str, list[str]] = {}
        self.sets: dict[str, set[str]] = {}

    async def set(self, key: str, value: str, ex: int | None = None, nx: bool = False) -> bool:
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def mget(self, keys: list[str]) -> list[str | None]:
        return [self.store.get(key) for key in keys]

    async def sadd(self, key: str, value: str) -> int:
        bucket = self.sets.setdefault(key, set())
        before = len(bucket)
        bucket.add(value)
        return len(bucket) - before

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))

    async def delete(self, *keys: str) -> int:
        removed = 0
        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1
        return removed

    async def lpush(self, key: str, value: str) -> int:
        bucket = self.lists.setdefault(key, [])
        bucket.insert(0, value)
        return len(bucket)

    async def lrem(self, key: str, count: int, value: str) -> int:
        values = self.lists.setdefault(key, [])
        kept = [item for item in values if item != value]
        removed = len(values) - len(kept)
        self.lists[key] = kept
        return removed

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        return values[start : stop + 1]

    async def ltrim(self, key: str, start: int, stop: int) -> None:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        self.lists[key] = values[start : stop + 1]

    async def publish(self, key: str, value: str) -> int:
        return 1


class _FakeUsageProvider:
    consecutive_failures = 0

    def fetch(self) -> object | None:
        return None

    def invalidate_cache(self) -> None:
        return None


def _make_runner() -> PipelineRunner:
    return PipelineRunner(
        RepoConfig(
            url="https://github.com/octo/demo.git",
            branch="main",
            auto_merge=True,
            poll_interval_sec=60,
            review_timeout_min=30,
            coder=CoderType.CLAUDE,
        ),
        AppConfig(
            repositories=[],
            daemon=DaemonConfig(
                claude_model="claude-opus-4-7",
                codex_model="gpt-5-codex",
            ),
        ),
        _FakeRedis(),
        _FakeUsageProvider(),
        _FakeUsageProvider(),
    )


def _patch_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub git plumbing the same way test_runner.py's _patch_subprocess does.

    Only the calls handle_merge takes are stubbed — diff stats, merge
    plumbing, and the queue-md probe.
    """

    class _CP:
        def __init__(self, stdout: str = "", returncode: int = 0) -> None:
            self.stdout = stdout
            self.returncode = returncode
            self.stderr = ""

    def fake_run(cmd: list[str], **_kwargs: Any) -> _CP:
        if cmd[:2] == ["git", "diff"]:
            # numstat output: 2 files, 5+/3-, one a Python file.
            return _CP(stdout="5\t3\tsrc/foo.py\n0\t0\ttests/test_foo.py\n")
        if cmd[:2] == ["git", "merge"] and cmd[2].startswith("origin/"):
            return _CP(stdout="Already up to date.\n")
        if cmd[:3] == ["git", "cat-file", "-e"]:
            return _CP(returncode=1)
        return _CP()

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)


def test_handle_merge_appends_outcome_row(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    isolate_analytics_dir: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "MERGED"},
    )
    monkeypatch.setattr(PipelineRunner, "_mark_task_done_in_snapshot", lambda self: None)
    # detect_coder_extension_version: avoid relying on host npm.
    monkeypatch.setattr(
        "src.daemon.handlers.merge.detect_coder_extension_version",
        lambda coder: "0.0.0-test",
    )

    runner = _make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-204-x",
        pr_id="PR-204",
        fix_iteration_count=2,
    )
    runner.state.current_task = QueueTask(
        pr_id="PR-204",
        title="Structured outcome logging",
        status=TaskStatus.DOING,
        branch="pr-204-x",
        task_file="tasks/PR-204.md",
    )
    runner._start_current_run_record("claude", "claude-opus-4-7")
    assert runner._current_run_record is not None
    runner._current_run_record.task_type = "feature"
    runner._current_run_record.complexity = "low"
    runner._current_run_record.fix_iterations = 2

    asyncio.run(runner.handle_merge())

    files = sorted((isolate_analytics_dir).glob("*.jsonl"))
    assert len(files) == 1
    rows = files[0].read_text().splitlines()
    assert len(rows) == 1
    parsed = json.loads(rows[0])

    assert parsed["pr_id"] == "PR-204"
    assert parsed["repo_slug"] == runner.name
    assert parsed["task_id_hash"] == compute_task_id_hash("PR-204", runner.name)
    assert parsed["coder"] == "claude"
    assert parsed["coder_model_string"] == "claude-opus-4-7"
    assert parsed["coder_extension_version"] == "0.0.0-test"
    assert parsed["task_type"] == "feature"
    assert parsed["task_complexity"] == "low"
    assert parsed["fix_iterations"] == 2
    assert parsed["files_changed"] == 2
    assert parsed["lines_added"] == 5
    assert parsed["lines_removed"] == 3
    assert parsed["codex_review_iterations"] == 3  # 2 fix iters + initial
    assert parsed["outcome"] == "merged"
    # Unimplemented fields default to JSON null.
    assert parsed["ci_runs_total"] is None
    assert parsed["ci_runs_failed"] is None
    assert parsed["review_blocker_count"] is None
    assert parsed["review_nit_count"] is None
    assert parsed["tokens_estimate"] is None
    # wall_clock_seconds is computed from the run record duration.
    assert isinstance(parsed["wall_clock_seconds"], int)
    assert parsed["wall_clock_seconds"] >= 0


def test_handle_merge_outcome_log_failure_does_not_block_merge(
    monkeypatch: pytest.MonkeyPatch,
    isolate_analytics_dir: Path,
) -> None:
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: None)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: {"state": "MERGED"},
    )
    monkeypatch.setattr(PipelineRunner, "_mark_task_done_in_snapshot", lambda self: None)

    def boom(_record: dict) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr("src.daemon.handlers.merge.log_merged_pr", boom)

    runner = _make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=99, branch="pr-204-y", pr_id="PR-204")
    runner.state.current_task = QueueTask(
        pr_id="PR-204",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-204-y",
        task_file="tasks/PR-204.md",
    )
    runner._start_current_run_record("claude", "claude-opus-4-7")

    asyncio.run(runner.handle_merge())

    # Merge still completed successfully despite the analytics error.
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    # Failure was logged to state.history.
    events = [entry["event"] for entry in runner.state.history]
    assert any("[ANALYTICS]" in event for event in events), events


def test_build_outcome_record_handles_codex_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Coder = codex resolves to the codex_model from app_config."""
    _patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        "src.daemon.handlers.merge.detect_coder_extension_version",
        lambda coder: None,
    )

    runner = _make_runner()
    runner.repo_config = runner.repo_config.model_copy(update={"coder": CoderType.CODEX})
    runner.state.current_pr = PRInfo(number=1, branch="b", pr_id="PR-204", fix_iteration_count=0)
    runner.state.current_task = QueueTask(
        pr_id="PR-204",
        title="t",
        status=TaskStatus.DOING,
        branch="b",
    )
    runner._start_current_run_record("codex", "gpt-5-codex")

    from datetime import datetime, timezone

    record = runner._build_outcome_record(datetime(2026, 4, 29, 14, 25, 23, tzinfo=timezone.utc))

    assert record["coder"] == "codex"
    assert record["coder_model_string"] == "gpt-5-codex"
    assert record["codex_review_iterations"] == 1
    assert record["coder_extension_version"] is None


def test_build_outcome_record_handles_no_run_record_or_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: missing run_record / current_pr still produces a valid row."""
    monkeypatch.setattr(
        "src.daemon.handlers.merge.detect_coder_extension_version",
        lambda coder: None,
    )

    runner = _make_runner()
    # Leave current_pr / current_task / current_run_record as None.

    from datetime import datetime, timezone

    record = runner._build_outcome_record(datetime(2026, 4, 29, 14, 25, 23, tzinfo=timezone.utc))

    assert record["pr_id"] == ""
    assert record["fix_iterations"] == 0
    assert record["files_changed"] == 0
    assert record["wall_clock_seconds"] is None
    assert record["codex_review_iterations"] is None
    # Coder string still falls back to the daemon default (claude).
    assert record["coder"] == "claude"


def test_build_outcome_record_falls_back_to_pr_pr_id_when_task_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If current_task is None but current_pr carries pr_id, use the PR's id."""
    monkeypatch.setattr(
        "src.daemon.handlers.merge.detect_coder_extension_version",
        lambda coder: None,
    )

    runner = _make_runner()
    runner.state.current_pr = PRInfo(number=7, branch="b", pr_id="PR-204", fix_iteration_count=1)
    # current_task intentionally left as None.

    from datetime import datetime, timezone

    record = runner._build_outcome_record(datetime(2026, 4, 29, 14, 25, 23, tzinfo=timezone.utc))

    assert record["pr_id"] == "PR-204"
    assert record["codex_review_iterations"] == 2


def test_build_outcome_record_uses_run_record_coder_over_config_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Run-time coder selection (e.g. rate-limit fallback) must win over config.

    The runner here is configured with ``coder=CoderType.CLAUDE``, but
    the run record was started with codex/gpt-5-codex — simulating the
    case where ``_get_coder()`` switched coders due to rate limits or
    task pinning. The merged outcome row must report what actually
    ran, not the configured default, otherwise downstream
    model/version analytics get mislabeled.
    """
    monkeypatch.setattr(
        "src.daemon.handlers.merge.detect_coder_extension_version",
        lambda coder: None,
    )

    runner = _make_runner()
    # Confirm the configured default is claude, so a config-driven
    # build would mislabel this run.
    assert runner.repo_config.coder == CoderType.CLAUDE

    runner.state.current_pr = PRInfo(number=11, branch="b", pr_id="PR-204", fix_iteration_count=0)
    runner.state.current_task = QueueTask(
        pr_id="PR-204",
        title="t",
        status=TaskStatus.DOING,
        branch="b",
    )
    runner._start_current_run_record("codex", "gpt-5-codex-2026-05")

    from datetime import datetime, timezone

    record = runner._build_outcome_record(datetime(2026, 4, 29, 14, 25, 23, tzinfo=timezone.utc))

    assert record["coder"] == "codex"
    assert record["coder_model_string"] == "gpt-5-codex-2026-05"


def test_build_outcome_record_falls_back_to_config_when_run_record_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a run record, the row reverts to repo/daemon defaults."""
    monkeypatch.setattr(
        "src.daemon.handlers.merge.detect_coder_extension_version",
        lambda coder: None,
    )

    runner = _make_runner()
    runner.repo_config = runner.repo_config.model_copy(update={"coder": CoderType.CODEX})
    runner.state.rate_limit_reactive_coder = "claude"
    # Leave _current_run_record as None.

    from datetime import datetime, timezone

    record = runner._build_outcome_record(datetime(2026, 4, 29, 14, 25, 23, tzinfo=timezone.utc))

    assert record["coder"] == "codex"
    assert record["coder_model_string"] == "gpt-5-codex"
