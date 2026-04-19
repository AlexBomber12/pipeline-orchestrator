"""Focused coverage tests for ``src/daemon/rate_limit.py``."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.config import AppConfig, CoderType, DaemonConfig, RepoConfig
from src.daemon import runner as runner_module
from src.daemon.runner import PipelineRunner
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.usage import UsageSnapshot


class _FakeRedis:
    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        return None


class _FakeCompletedProcess:
    def __init__(
        self,
        args: list[str] | None = None,
        stdout: str = "",
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.args = args or []
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


class _FakeUsageProvider:
    def __init__(self, snapshot: UsageSnapshot | None = None, failures: int = 0) -> None:
        self._snapshot = snapshot
        self._consecutive_failures = failures
        self.invalidated = 0

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures

    def fetch(self) -> UsageSnapshot | None:
        return self._snapshot

    def invalidate_cache(self) -> None:
        self.invalidated += 1


def _repo_cfg(**overrides: Any) -> RepoConfig:
    base: dict[str, Any] = {
        "url": "https://github.com/octo/demo.git",
        "branch": "main",
        "auto_merge": True,
        "review_timeout_min": 30,
        "poll_interval_sec": 60,
    }
    base.update(overrides)
    return RepoConfig(**base)


def _app_cfg(**daemon_overrides: Any) -> AppConfig:
    return AppConfig(repositories=[], daemon=DaemonConfig(**daemon_overrides))


def _patch_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, stdout="0\n")
        if cmd[:2] == ["git", "merge"] and len(cmd) > 2 and cmd[2].startswith("origin/"):
            return _FakeCompletedProcess(args=cmd, stdout="Already up to date.\n")
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)


def _make_runner(
    monkeypatch: pytest.MonkeyPatch,
    *,
    coder: CoderType | None = CoderType.CLAUDE,
    claude_provider: _FakeUsageProvider | None = None,
    codex_provider: _FakeUsageProvider | None = None,
) -> PipelineRunner:
    _patch_subprocess(monkeypatch)
    repo_overrides: dict[str, Any] = {}
    if coder is not None:
        repo_overrides["coder"] = coder
    runner = PipelineRunner(
        _repo_cfg(**repo_overrides),
        _app_cfg(),
        _FakeRedis(),
        claude_provider or _FakeUsageProvider(),
        codex_provider or _FakeUsageProvider(),
    )
    runner._selector_rng.seed(0)
    return runner


def _events(runner: PipelineRunner) -> list[str]:
    return [entry["event"] for entry in runner.state.history]


def _todo_task(branch: str) -> QueueTask:
    return QueueTask(
        pr_id="PR-105",
        title="Coverage task",
        status=TaskStatus.TODO,
        branch=branch,
    )


def test_record_clear_and_lookup_rate_limit_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch)
    until = datetime.now(timezone.utc) + timedelta(minutes=15)

    runner._record_rate_limit("claude", until, reactive=True)

    assert runner._rate_limit_until_for("claude") == until
    assert runner.state.rate_limited_until == until
    assert runner.state.rate_limit_reactive is True
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert "claude" in runner.state.rate_limited_coders

    runner._clear_rate_limit("claude")

    assert runner._rate_limit_until_for("claude") is None
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None


def test_proactive_usage_check_logs_degradation_once_and_resets_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(
        monkeypatch,
        claude_provider=_FakeUsageProvider(snapshot=None, failures=10),
    )

    assert asyncio.run(runner._proactive_usage_check()) is True
    assert asyncio.run(runner._proactive_usage_check()) is True
    assert sum("Usage API degraded" in event for event in _events(runner)) == 1

    runner._claude_usage_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=10,
            session_resets_at=1,
            weekly_percent=10,
            weekly_resets_at=1,
            fetched_at=0,
        )
    )

    assert asyncio.run(runner._proactive_usage_check()) is True
    assert runner._usage_degraded_logged is False


def test_proactive_usage_check_session_pause_clears_non_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resets_at = 1_900_000_000
    runner = _make_runner(
        monkeypatch,
        claude_provider=_FakeUsageProvider(
            snapshot=UsageSnapshot(
                session_percent=96,
                session_resets_at=resets_at,
                weekly_percent=10,
                weekly_resets_at=resets_at + 100,
                fetched_at=0,
            )
        ),
    )
    runner.app_config.daemon.rate_limit_session_pause_percent = 95
    runner.state.state = PipelineState.CODING
    runner.state.error_message = "stale error"

    assert asyncio.run(runner._proactive_usage_check()) is False
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.error_message is None
    assert runner.state.rate_limit_reactive is False
    assert int(runner.state.rate_limited_until.timestamp()) == resets_at
    assert any("Proactive pause: session usage at 96%" in event for event in _events(runner))


def test_proactive_usage_check_weekly_pause_preserves_error_context_for_error_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resets_at = 2_000_000_000
    codex_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=40,
            session_resets_at=resets_at - 100,
            weekly_percent=100,
            weekly_resets_at=resets_at,
            fetched_at=0,
        )
    )
    runner = _make_runner(monkeypatch, codex_provider=codex_provider)
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 100
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "preserve me"

    assert asyncio.run(runner._proactive_usage_check(proactive_coder="codex")) is False
    assert runner.state.error_message == "preserve me"
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert int(runner.state.rate_limited_until.timestamp()) == resets_at
    assert any("Proactive pause: weekly usage at 100%" in event for event in _events(runner))


def test_check_rate_limit_returns_repo_config_coder_pause_and_reapplies_global_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    now = datetime.now(timezone.utc)
    codex_until = now + timedelta(minutes=5)
    runner.state.rate_limited_until = now - timedelta(minutes=1)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.update({"claude", "codex"})
    runner.state.rate_limited_coder_until = {
        "claude": now - timedelta(minutes=1),
        "codex": codex_until,
    }
    runner.state.state = PipelineState.IDLE

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.rate_limited_until == codex_until
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert runner.state.state == PipelineState.PAUSED
    assert any("Rate limit window expired, resuming" in event for event in _events(runner))


def test_check_rate_limit_expires_pause_and_falls_back_to_proactive_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    claude_provider = _FakeUsageProvider(
        snapshot=UsageSnapshot(
            session_percent=10,
            session_resets_at=10,
            weekly_percent=10,
            weekly_resets_at=20,
            fetched_at=0,
        )
    )
    codex_provider = _FakeUsageProvider()
    runner = _make_runner(
        monkeypatch,
        coder=CoderType.CODEX,
        claude_provider=claude_provider,
        codex_provider=codex_provider,
    )
    runner.state.rate_limited_until = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limit_reactive = True
    runner.state.rate_limited_coders.add("claude")
    runner.state.state = PipelineState.PAUSED

    assert asyncio.run(runner._check_rate_limit()) is True
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert claude_provider.invalidated == 1
    assert codex_provider.invalidated == 1


def test_check_rate_limit_other_coder_pause_sets_matching_watch_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    pause_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    branch = "pr-105-coverage-rate-limit"
    runner.state.rate_limited_until = pause_until
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.current_task = _todo_task(branch)
    runner.state.current_pr = PRInfo(number=105, branch=branch)
    runner.state.state = PipelineState.PAUSED

    assert asyncio.run(runner._check_rate_limit(proactive_coder="codex")) is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.rate_limited_until is None
    assert runner.state.rate_limit_reactive is False
    assert runner.state.rate_limit_reactive_coder is None
    assert runner.state.rate_limited_coder_until["claude"] == pause_until
    assert any("Codex active while claude remains rate-limited" in event for event in _events(runner))


def test_check_rate_limit_other_coder_pause_reapplies_effective_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    now = datetime.now(timezone.utc)
    runner.state.rate_limited_until = now + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.rate_limited_coders.update({"claude", "codex"})
    runner.state.rate_limited_coder_until = {
        "claude": now + timedelta(minutes=10),
        "codex": now + timedelta(minutes=5),
    }
    runner.state.state = PipelineState.IDLE

    assert asyncio.run(runner._check_rate_limit(proactive_coder="codex")) is False
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_blocks_active_diagnosis_pause_even_for_other_repo_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.error_message = "diagnosis in progress"
    runner.state.state = PipelineState.CODING

    assert asyncio.run(runner._check_rate_limit()) is False
    assert runner.state.state == PipelineState.PAUSED
    assert any("Rate limited, resuming in" in event for event in _events(runner))


def test_check_rate_limit_blocks_from_stored_effective_until_without_global_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    until = datetime.now(timezone.utc) + timedelta(minutes=5)
    runner.state.rate_limited_coders.add("codex")
    runner.state.rate_limited_coder_until["codex"] = until
    runner.state.state = PipelineState.IDLE

    assert asyncio.run(runner._check_rate_limit(proactive_coder="codex")) is False
    assert runner.state.rate_limited_until == until
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert runner.state.state == PipelineState.PAUSED


def test_check_rate_limit_clears_expired_effective_coder_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    runner.state.rate_limited_coders.add("codex")
    runner.state.rate_limited_coder_until["codex"] = (
        datetime.now(timezone.utc) - timedelta(minutes=1)
    )

    assert asyncio.run(runner._check_rate_limit(proactive_coder="codex")) is True
    assert "codex" not in runner.state.rate_limited_coders
    assert "codex" not in runner.state.rate_limited_coder_until


def test_check_rate_limit_other_coder_pause_returns_idle_without_matching_watch_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)
    pause_until = datetime.now(timezone.utc) + timedelta(minutes=10)
    runner.state.rate_limited_until = pause_until
    runner.state.rate_limit_reactive = True
    runner.state.rate_limit_reactive_coder = "claude"
    runner.state.state = PipelineState.PAUSED

    assert asyncio.run(runner._check_rate_limit(proactive_coder="codex")) is True
    assert runner.state.state == PipelineState.IDLE


def test_check_rate_limit_uses_get_coder_when_repo_coder_is_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=None)

    async def fake_proactive(*, proactive_coder: str | None = None) -> bool:
        assert proactive_coder is None
        return True

    monkeypatch.setattr(runner, "_get_coder", lambda: ("codex", object()))
    monkeypatch.setattr(runner, "_proactive_usage_check", fake_proactive)

    assert asyncio.run(runner._check_rate_limit()) is True


def test_detect_rate_limit_uses_default_configured_coder_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch)

    runner._detect_rate_limit("Error: 429 Too Many Requests")

    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive is True
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert any("[claude] Rate limit detected (session)" in event for event in _events(runner))


def test_detect_rate_limit_claude_percentage_threshold_respected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch)
    runner.app_config.daemon.rate_limit_session_pause_percent = 80
    runner.app_config.daemon.rate_limit_weekly_pause_percent = 90

    runner._detect_rate_limit("Warning: 79% of your session rate limit reached", coder_name="claude")
    assert runner.state.rate_limited_until is None

    runner._detect_rate_limit("Warning: 90% of your weekly rate limit reached", coder_name="claude")
    assert runner.state.rate_limited_until is not None
    assert any("(weekly)" in event for event in _events(runner))


def test_detect_rate_limit_codex_retry_formats_and_error_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)

    runner._detect_rate_limit(
        "Rate limit reached. Please try again in 6.379s",
        coder_name="codex",
    )
    first_until = runner.state.rate_limited_until
    assert first_until is not None

    runner._detect_rate_limit(
        "Rate limit exceeded. Please try again later this week.",
        coder_name="codex",
    )

    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert any("(session)" in event for event in _events(runner))
    assert any("(weekly)" in event for event in _events(runner))


def test_detect_rate_limit_generic_and_usage_limit_fallbacks_for_non_codex(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch)

    runner._detect_rate_limit("Weekly rate limit exceeded, retry later", coder_name="claude")
    assert runner.state.rate_limited_until is not None
    assert any("(weekly)" in event for event in _events(runner))

    runner.state.rate_limited_until = None
    runner.state.rate_limit_reactive_coder = None
    runner._detect_rate_limit("Usage limit reached for this workspace", coder_name="claude")

    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "claude"
    assert any("(session)" in event for event in _events(runner))


def test_detect_rate_limit_codex_usage_limit_without_retry_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)

    runner._detect_rate_limit("You've hit your usage limit.", coder_name="codex")

    assert runner.state.rate_limited_until is not None
    assert runner.state.rate_limit_reactive_coder == "codex"
    assert any("(session)" in event for event in _events(runner))


def test_detect_rate_limit_ignores_non_triggering_patterns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner(monkeypatch, coder=CoderType.CODEX)

    runner._detect_rate_limit("Warning: 95% of session rate limit reached", coder_name="codex")
    assert runner.state.rate_limited_until is None

    runner._detect_rate_limit("Try again in 0 seconds", coder_name="codex")
    assert runner.state.rate_limited_until is None
