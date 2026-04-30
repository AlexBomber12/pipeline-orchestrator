"""Tests for src/daemon/main.py."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import types
from typing import Any
from unittest.mock import patch

import pytest
from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import main as main_module
from src.models import PipelineState


@pytest.fixture(autouse=True)
def _disable_config_watcher(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the config-file watcher with a no-op for every test in this file.

    The real watcher polls ``config.yml`` via ``asyncio.sleep`` which the
    tests below replace with synchronous fakes that bookkeep call counts
    and raise ``_StopLoop``. Letting the watcher run in that environment
    pollutes ``sleep_calls`` and surfaces the test sentinel exception as a
    spurious task failure.
    """

    async def _noop_watcher(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        main_module, "watch_config_file_changes", _noop_watcher
    )


class _FakeRedisClient:
    """Placeholder returned by the patched ``aioredis.from_url``."""


class _FakeRunner:
    """Captures constructor args and ``run_cycle`` calls for assertions."""

    instances: list[_FakeRunner] = []

    def __init__(
        self,
        repo_config: RepoConfig,
        app_config: AppConfig,
        redis_client: Any,
        claude_usage_provider: Any,
        codex_usage_provider: Any,
    ) -> None:
        self.repo_config = repo_config
        self.app_config = app_config
        self.redis_client = redis_client
        self.claude_usage_provider = claude_usage_provider
        self.codex_usage_provider = codex_usage_provider
        from src.utils import repo_slug_from_url
        self.name = repo_slug_from_url(repo_config.url)
        self.cycles = 0
        self.state = types.SimpleNamespace(state=PipelineState.IDLE)
        _FakeRunner.instances.append(self)

    async def run_cycle(self) -> None:
        self.cycles += 1

    async def publish_state(self) -> None:
        pass

    def set_usage_providers(
        self,
        claude_usage_provider: Any,
        codex_usage_provider: Any,
    ) -> None:
        self.claude_usage_provider = claude_usage_provider
        self.codex_usage_provider = codex_usage_provider


class _StopLoop(Exception):
    """Sentinel raised by the patched ``asyncio.sleep`` to end ``main``."""


def _reset_fake_runner() -> None:
    _FakeRunner.instances = []


_REAL_ASYNCIO_SLEEP = asyncio.sleep


def _patch_main(
    monkeypatch: pytest.MonkeyPatch,
    config: AppConfig,
    runner_cls: type = _FakeRunner,
    sleep_iterations: int = 1,
) -> dict[str, Any]:
    """Wire up the common monkeypatches used by every test."""
    _reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", runner_cls)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    clock = [0.0]

    def fake_monotonic() -> float:
        return clock[0]

    monkeypatch.setattr(main_module.time, "monotonic", fake_monotonic)

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        # The main loop schedules per-runner cycles as background tasks
        # (PR-207). They only execute on the next event-loop turn, so the
        # fake sleep must yield once so scheduled cycle tasks can run before
        # we raise the stop sentinel — otherwise tests that count cycles
        # would observe zero.
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= sleep_iterations:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)
    return {"sleep_calls": sleep_calls, "clock": clock}


def _repo(url: str, **kwargs: Any) -> RepoConfig:
    kwargs.setdefault("poll_interval_sec", 1)
    return RepoConfig(url=url, **kwargs)


def test_main_creates_one_runner_per_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=7),
    )
    ctx = _patch_main(monkeypatch, config)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    assert len(_FakeRunner.instances) == 2
    names = [r.name for r in _FakeRunner.instances]
    assert names == ["octo__alpha", "octo__beta"]
    assert all(r.cycles == 1 for r in _FakeRunner.instances)
    assert ctx["sleep_calls"] == [1]


def test_main_warns_when_no_repos_configured(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = AppConfig(repositories=[], daemon=DaemonConfig(poll_interval_sec=3))
    _patch_main(monkeypatch, config)

    with caplog.at_level(logging.WARNING, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    assert _FakeRunner.instances == []
    warnings = [rec for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("No repositories configured" in rec.getMessage() for rec in warnings)


def test_main_skips_runner_whose_init_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _PickyRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            if "broken" in repo_config.url:
                raise ValueError(f"Not a recognizable GitHub URL: {repo_config.url!r}")
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )

    config = AppConfig(
        repositories=[
            _repo("not-a-valid-url-broken"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, runner_cls=_PickyRunner)

    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    # The broken repo must not produce a runner, but the good one still must
    # be built AND driven through run_cycle so one misconfigured entry cannot
    # take the whole daemon down at startup.
    assert len(_FakeRunner.instances) == 1
    assert _FakeRunner.instances[0].name == "octo__beta"
    assert _FakeRunner.instances[0].cycles == 1
    errors = [rec for rec in caplog.records if rec.levelno == logging.ERROR]
    assert any(
        "Failed to initialize runner" in rec.getMessage() and "broken" in rec.getMessage()
        for rec in errors
    )


def test_main_reload_detects_new_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the reload window a new repo must get its own runner."""
    first = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    second = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    monkeypatch.setattr(main_module, "load_config", fake_load_config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 3)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    names = sorted(r.name for r in _FakeRunner.instances)
    assert names == ["octo__alpha", "octo__beta"], names

    alpha = next(r for r in _FakeRunner.instances if r.name == "octo__alpha")
    beta = next(r for r in _FakeRunner.instances if r.name == "octo__beta")
    # Alpha built at startup + ran once per loop iteration (3 total cycles).
    assert alpha.cycles == 3
    # Beta was added at cycle 2 and only runs that cycle + the third.
    assert beta.cycles == 1
    # After the reload, alpha's app_config should point at the new object.
    assert alpha.app_config is second


def test_main_reload_drops_removed_repository(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the reload window a removed repo must stop running."""
    first = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    second = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    monkeypatch.setattr(main_module, "load_config", fake_load_config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 3)

    clock2 = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock2[0])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock2[0] += seconds + 1
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    alpha = next(r for r in _FakeRunner.instances if r.name == "octo__alpha")
    beta = next(r for r in _FakeRunner.instances if r.name == "octo__beta")
    # Beta runs on cycles 0 and 1; after reload on cycle 2 it is dropped
    # and does NOT run that cycle.
    assert beta.cycles == 2
    assert alpha.cycles == 3


def test_main_reload_recreates_shared_usage_providers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reloading config must refresh the shared providers for all runners."""
    first = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    second = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    class _PluginFactory:
        def __init__(self, prefix: str) -> None:
            self.prefix = prefix
            self.calls = 0

        def create_usage_provider(self, *, config: AppConfig) -> str:
            self.calls += 1
            return f"{self.prefix}-{self.calls}-{id(config)}"

    claude_factory = _PluginFactory("claude")
    codex_factory = _PluginFactory("codex")

    monkeypatch.setattr(main_module, "load_config", fake_load_config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 3)
    monkeypatch.setattr(main_module, "ClaudePlugin", lambda: claude_factory)
    monkeypatch.setattr(main_module, "CodexPlugin", lambda: codex_factory)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    alpha = next(r for r in _FakeRunner.instances if r.name == "octo__alpha")
    beta = next(r for r in _FakeRunner.instances if r.name == "octo__beta")

    assert alpha.claude_usage_provider == f"claude-2-{id(second)}"
    assert alpha.codex_usage_provider == f"codex-2-{id(second)}"
    assert beta.claude_usage_provider == f"claude-2-{id(second)}"
    assert beta.codex_usage_provider == f"codex-2-{id(second)}"


def test_hot_reload_updates_repo_config_coder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    second = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", coder="codex")
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    monkeypatch.setattr(main_module, "load_config", fake_load_config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 3)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    alpha = next(r for r in _FakeRunner.instances if r.name == "octo__alpha")
    assert alpha.repo_config.coder is not None
    assert alpha.repo_config.coder.value == "codex"


def test_sync_runners_stages_config_reload_when_runner_supports_it() -> None:
    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = None

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

    daemon_config = DaemonConfig(poll_interval_sec=1)
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git", coder="codex")],
        daemon=daemon_config,
    )
    runner = _StagingRunner(
        _repo("https://github.com/octo/alpha.git"),
        AppConfig(
            repositories=[_repo("https://github.com/octo/alpha.git")],
            daemon=daemon_config,
        ),
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
    )
    runner.state.state = PipelineState.WATCH

    main_module._sync_runners(
        {"https://github.com/octo/alpha": runner},
        config,
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
        registry=None,  # type: ignore[arg-type]
    )

    assert runner.staged is not None
    staged_repo, staged_app, staged_claude, staged_codex = runner.staged
    assert staged_repo.coder is not None
    assert staged_repo.coder.value == "codex"
    assert staged_app is config
    assert staged_claude == "claude-provider"
    assert staged_codex == "codex-provider"


def test_sync_runners_applies_active_flag_change_immediately() -> None:
    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = None

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

        def clear_staged_config_reload(self) -> None:
            self.staged = None

    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git", active=True, coder="codex")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    runner = _StagingRunner(
        _repo("https://github.com/octo/alpha.git", active=False),
        AppConfig(
            repositories=[_repo("https://github.com/octo/alpha.git", active=False)]
        ),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )

    main_module._sync_runners(
        {"https://github.com/octo/alpha": runner},
        config,
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
        registry=None,  # type: ignore[arg-type]
    )

    assert runner.staged is None
    assert runner.repo_config.active is True
    assert runner.repo_config.coder is not None
    assert runner.repo_config.coder.value == "codex"
    assert runner.app_config is config
    assert runner.claude_usage_provider == "claude-provider"
    assert runner.codex_usage_provider == "codex-provider"


def test_sync_runners_clears_staged_reload_after_immediate_active_update() -> None:
    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = ("stale",)

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

        def clear_staged_config_reload(self) -> None:
            self.staged = None

    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git", active=False, coder="codex")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    runner = _StagingRunner(
        _repo("https://github.com/octo/alpha.git", active=True, coder="claude"),
        AppConfig(
            repositories=[_repo("https://github.com/octo/alpha.git", active=True, coder="claude")]
        ),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )

    main_module._sync_runners(
        {"https://github.com/octo/alpha": runner},
        config,
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
        registry=None,  # type: ignore[arg-type]
    )

    assert runner.repo_config.active is False
    assert runner.repo_config.coder is not None
    assert runner.repo_config.coder.value == "codex"
    assert runner.staged is None


def test_sync_runners_applies_config_immediately_when_runner_is_in_error() -> None:
    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = None

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git", coder="codex")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    runner = _StagingRunner(
        _repo("https://github.com/octo/alpha.git", coder="claude"),
        AppConfig(
            repositories=[_repo("https://github.com/octo/alpha.git", coder="claude")]
        ),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )
    runner.state.state = PipelineState.ERROR

    main_module._sync_runners(
        {"https://github.com/octo/alpha": runner},
        config,
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
        registry=None,  # type: ignore[arg-type]
    )

    assert runner.staged is None
    assert runner.repo_config.coder is not None
    assert runner.repo_config.coder.value == "codex"
    assert runner.app_config is config


def test_sync_runners_applies_non_coder_repo_changes_immediately() -> None:
    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = None

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

        def clear_staged_config_reload(self) -> None:
            self.staged = None

    original_repo = _repo(
        "https://github.com/octo/alpha.git",
        coder="claude",
        auto_merge=True,
    )
    updated_repo = _repo(
        "https://github.com/octo/alpha.git",
        coder="codex",
        auto_merge=False,
    )
    runner = _StagingRunner(
        original_repo,
        AppConfig(repositories=[original_repo]),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )
    runner.state.state = PipelineState.WATCH
    config = AppConfig(
        repositories=[updated_repo],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    main_module._sync_runners(
        {"https://github.com/octo/alpha": runner},
        config,
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
        registry=None,  # type: ignore[arg-type]
    )

    assert runner.staged is None
    assert runner.repo_config.coder is not None
    assert runner.repo_config.coder.value == "codex"
    assert runner.repo_config.auto_merge is False
    assert runner.app_config is config
    assert runner.claude_usage_provider == "claude-provider"
    assert runner.codex_usage_provider == "codex-provider"


def test_repo_config_differs_only_in_coder_requires_coder_change() -> None:
    current = _repo("https://github.com/octo/alpha.git", coder="claude")
    same = _repo("https://github.com/octo/alpha.git", coder="claude")
    changed = _repo(
        "https://github.com/octo/alpha.git",
        coder="codex",
        auto_merge=False,
    )

    assert main_module._repo_config_differs_only_in_coder(current, same) is False
    assert (
        main_module._repo_config_differs_only_in_coder(current, changed) is False
    )


def test_app_config_differs_only_in_repo_coder_handles_non_coder_changes() -> None:
    current = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git", coder="claude")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    missing_repo = AppConfig(repositories=[], daemon=DaemonConfig(poll_interval_sec=1))
    changed_repo = AppConfig(
        repositories=[
            _repo(
                "https://github.com/octo/alpha.git",
                coder="codex",
                auto_merge=False,
            )
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    reordered_repos = AppConfig(
        repositories=[
            _repo("https://github.com/octo/beta.git"),
            _repo("https://github.com/octo/alpha.git", coder="codex"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    assert (
        main_module._app_config_differs_only_in_repo_coder(
            current,
            missing_repo,
            "https://github.com/octo/alpha",
        )
        is False
    )
    assert (
        main_module._app_config_differs_only_in_repo_coder(
            current,
            changed_repo,
            "https://github.com/octo/alpha",
        )
        is False
    )
    assert (
        main_module._app_config_differs_only_in_repo_coder(
            current,
            reordered_repos,
            "https://github.com/octo/alpha",
        )
        is False
    )


def test_app_config_differs_only_in_repo_coder_rejects_other_repo_drift() -> None:
    current = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", coder="claude"),
            _repo("https://github.com/octo/beta.git", auto_merge=True),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    updated_with_reordered_repos = AppConfig(
        repositories=[
            _repo("https://github.com/octo/beta.git", auto_merge=True),
            _repo("https://github.com/octo/alpha.git", coder="codex"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    updated_with_other_repo_change = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", coder="codex"),
            _repo("https://github.com/octo/beta.git", auto_merge=False),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    assert (
        main_module._app_config_differs_only_in_repo_coder(
            current,
            updated_with_reordered_repos,
            "https://github.com/octo/alpha",
        )
        is False
    )
    assert (
        main_module._app_config_differs_only_in_repo_coder(
            current,
            updated_with_other_repo_change,
            "https://github.com/octo/alpha",
        )
        is False
    )


def test_sync_runners_updates_watching_runner_without_staging_support() -> None:
    daemon_config = DaemonConfig(poll_interval_sec=1)
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git", coder="codex")],
        daemon=daemon_config,
    )
    runner = _FakeRunner(
        _repo("https://github.com/octo/alpha.git", coder="claude"),
        AppConfig(
            repositories=[_repo("https://github.com/octo/alpha.git", coder="claude")],
            daemon=daemon_config,
        ),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )
    runner.state.state = PipelineState.WATCH

    main_module._sync_runners(
        {"https://github.com/octo/alpha": runner},
        config,
        _FakeRedisClient(),
        "claude-provider",
        "codex-provider",
        registry=None,  # type: ignore[arg-type]
    )

    assert runner.repo_config.coder is not None
    assert runner.repo_config.coder.value == "codex"
    assert runner.app_config is config
    assert runner.claude_usage_provider == "claude-provider"
    assert runner.codex_usage_provider == "codex-provider"


def test_find_repo_config_matches_normalized_url() -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(),
    )

    found = main_module._find_repo_config(config, "https://github.com/octo/alpha/")

    assert found is not None
    assert found.url == "https://github.com/octo/alpha.git"


def test_find_repo_config_returns_none_for_unknown_repo() -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(),
    )

    found = main_module._find_repo_config(config, "https://github.com/octo/missing")

    assert found is None


def test_main_continues_when_one_runner_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FailingFirstRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            if self.name == "octo__alpha":
                raise RuntimeError("boom")

    config = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, runner_cls=_FailingFirstRunner)

    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    assert len(_FakeRunner.instances) == 2
    alpha, beta = _FakeRunner.instances
    assert alpha.cycles == 1
    assert beta.cycles == 1, "second runner must still execute after first raises"
    errors = [rec for rec in caplog.records if rec.levelno == logging.ERROR]
    assert any("octo__alpha" in rec.getMessage() for rec in errors)


# ---------- _setup_git_auth tests ----------


def test_setup_git_auth_calls_subprocess() -> None:
    """_setup_git_auth must invoke 'gh auth setup-git'."""
    with patch.object(main_module.subprocess, "run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh", "auth", "setup-git"], returncode=0, stdout="", stderr=""
        )
        main_module._setup_git_auth()

    mock_run.assert_called_once()
    args = mock_run.call_args
    assert args[0][0] == ["gh", "auth", "setup-git"]
    assert args[1]["timeout"] == 30


def test_setup_git_auth_does_not_crash_on_error() -> None:
    """_setup_git_auth must not raise on CalledProcessError."""
    with patch.object(main_module.subprocess, "run") as mock_run:
        mock_run.side_effect = subprocess.CalledProcessError(1, "gh")
        # Must not raise
        main_module._setup_git_auth()


def test_setup_git_auth_logs_warning_on_nonzero_exit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with patch.object(main_module.subprocess, "run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh", "auth", "setup-git"],
            returncode=1,
            stdout="",
            stderr="bad token\n",
        )
        with caplog.at_level(logging.WARNING, logger=main_module.logger.name):
            main_module._setup_git_auth()

    assert any("gh auth setup-git exited 1: bad token" in rec.getMessage() for rec in caplog.records)


def test_setup_git_auth_handles_timeout() -> None:
    """_setup_git_auth must not raise on TimeoutExpired."""
    with patch.object(main_module.subprocess, "run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired("gh", 30)
        main_module._setup_git_auth()


# ---------- _validate_auth tests ----------


def test_validate_auth_uses_auth_status() -> None:
    """_validate_auth must use 'claude auth status', not 'claude --version'."""
    cmds: list[list[str]] = []

    def capture_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        cmds.append(cmd)
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    with patch.object(main_module.subprocess, "run", side_effect=capture_run):
        main_module._validate_auth()

    claude_cmd = next(c for c in cmds if c[0] == "claude")
    assert claude_cmd == ["claude", "auth", "status"]


def test_validate_auth_returns_true_when_both_succeed() -> None:
    with patch.object(main_module.subprocess, "run") as mock_run:
        mock_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )
        result = main_module._validate_auth()

    assert result == {"claude": True, "gh": True}


def test_validate_auth_returns_false_on_failure() -> None:
    def failing_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(1, cmd[0])

    with patch.object(main_module.subprocess, "run", side_effect=failing_run):
        result = main_module._validate_auth()

    assert result == {"claude": False, "gh": False}


def test_validate_auth_mixed_results() -> None:
    def selective_run(cmd: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[0] == "claude":
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")
        raise subprocess.CalledProcessError(1, cmd[0])

    with patch.object(main_module.subprocess, "run", side_effect=selective_run):
        result = main_module._validate_auth()

    assert result == {"claude": True, "gh": False}


# ---------- main() calls startup functions ----------


def test_main_calls_setup_git_auth_before_runners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() must call _setup_git_auth before creating runners."""
    call_order: list[str] = []

    def tracking_setup() -> None:
        call_order.append("setup_git_auth")

    def tracking_validate() -> dict[str, bool]:
        call_order.append("validate_auth")
        return {"claude": True, "gh": True}

    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config)
    # Override the _patch_main stubs with tracking versions
    monkeypatch.setattr(main_module, "_setup_git_auth", tracking_setup)
    monkeypatch.setattr(main_module, "_validate_auth", tracking_validate)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    assert call_order.index("setup_git_auth") < call_order.index("validate_auth")
    assert len(_FakeRunner.instances) == 1


def test_main_maps_gh_config_dir_to_home(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() must mirror GH_CONFIG_DIR into GH_CONFIG_HOME for gh."""
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config)
    monkeypatch.setenv("GH_CONFIG_DIR", "/tmp/custom-gh-config")
    monkeypatch.delenv("GH_CONFIG_HOME", raising=False)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    assert os.environ["GH_CONFIG_HOME"] == "/tmp/custom-gh-config"


def test_per_repo_poll_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repos with different poll_interval_sec are polled at different rates."""
    fast_repo = RepoConfig(url="https://github.com/octo/fast", poll_interval_sec=10)
    slow_repo = RepoConfig(url="https://github.com/octo/slow", poll_interval_sec=100)

    config = AppConfig(
        repositories=[fast_repo, slow_repo],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    clock = [0.0]

    def fake_monotonic() -> float:
        return clock[0]

    monkeypatch.setattr(main_module.time, "monotonic", fake_monotonic)

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += 15
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    fast = next(r for r in _FakeRunner.instances if r.name == "octo__fast")
    slow = next(r for r in _FakeRunner.instances if r.name == "octo__slow")
    # clock: 0 (both run), +15 (fast runs, slow skipped), +30 (fast runs, slow skipped)
    assert fast.cycles == 3
    assert slow.cycles == 1
    # Sleep should use min(fastest_repo=10, daemon=1) = 1.
    assert all(s == 1 for s in sleep_calls)


def test_unpause_runs_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-enabling a paused repo triggers a run on the very next cycle."""
    repo = RepoConfig(url="https://github.com/octo/toggle", poll_interval_sec=100)
    config = AppConfig(
        repositories=[repo],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_count = [0]

    async def fake_sleep(seconds: float) -> None:
        sleep_count[0] += 1
        clock[0] += 5
        runner = _FakeRunner.instances[0]
        if sleep_count[0] == 1:
            # After first cycle (ran at t=0), pause the repo.
            runner.repo_config = RepoConfig(
                url=repo.url, poll_interval_sec=100, active=False,
            )
        elif sleep_count[0] == 2:
            # After second cycle (paused), re-enable it.
            runner.repo_config = RepoConfig(
                url=repo.url, poll_interval_sec=100, active=True,
            )
        await _REAL_ASYNCIO_SLEEP(0)
        if sleep_count[0] >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    runner = _FakeRunner.instances[0]
    # Cycle 0 (t=0): active, runs. Cycle 1 (t=5): paused, skipped.
    # Cycle 2 (t=10): re-enabled, should run immediately despite interval=100
    # because pause cleared last_run.
    assert runner.cycles == 2


# ---- Statusline hook installer tests ----


def test_install_statusline_hook_creates_settings(tmp_path: Any) -> None:
    """Hook installer creates settings.json with statusLine block."""
    main_module._install_statusline_hook(str(tmp_path))

    import json
    settings = json.loads((tmp_path / "settings.json").read_text())
    assert "statusLine" in settings
    assert settings["statusLine"]["type"] == "command"
    assert "statusline_hook.py" in settings["statusLine"]["command"]


def test_install_statusline_hook_preserves_existing_keys(tmp_path: Any) -> None:
    """Hook installer merges into existing settings without clobbering."""
    existing = {"theme": "dark", "someKey": True}
    (tmp_path / "settings.json").write_text(json.dumps(existing))

    main_module._install_statusline_hook(str(tmp_path))

    settings = json.loads((tmp_path / "settings.json").read_text())
    assert settings["theme"] == "dark"
    assert settings["someKey"] is True
    assert "statusLine" in settings


def test_install_statusline_hook_respects_operator_override(
    tmp_path: Any,
) -> None:
    """Hook installer does not overwrite a non-default statusLine command."""
    existing = {
        "statusLine": {
            "type": "command",
            "command": "ccusage --custom",
            "padding": 0,
        }
    }
    (tmp_path / "settings.json").write_text(json.dumps(existing))

    main_module._install_statusline_hook(str(tmp_path))

    settings = json.loads((tmp_path / "settings.json").read_text())
    assert settings["statusLine"]["command"] == "ccusage --custom"


def test_install_statusline_hook_recovers_from_invalid_json(tmp_path: Any) -> None:
    (tmp_path / "settings.json").write_text("{not valid json")

    main_module._install_statusline_hook(str(tmp_path))

    settings = json.loads((tmp_path / "settings.json").read_text())
    assert settings["statusLine"]["type"] == "command"


def test_clean_breach_dir_removes_stale_markers(tmp_path: Any) -> None:
    """_clean_breach_dir removes all files in the breach directory."""

    monkeypatch_breach = str(tmp_path / "breach")
    (tmp_path / "breach").mkdir()
    (tmp_path / "breach" / "stale.breach").write_text('{"type":"session"}')

    original = main_module._BREACH_DIR
    main_module._BREACH_DIR = monkeypatch_breach
    try:
        main_module._clean_breach_dir()
        # Directory should exist but be empty (old files removed)
        assert (tmp_path / "breach").is_dir()
        assert not list((tmp_path / "breach").glob("*.breach"))
    finally:
        main_module._BREACH_DIR = original


def test_clean_breach_dir_unlinks_file_marker(tmp_path: Any) -> None:
    breach_file = tmp_path / "breach-file"
    breach_file.write_text("stale")

    original = main_module._BREACH_DIR
    main_module._BREACH_DIR = str(breach_file)
    try:
        main_module._clean_breach_dir()
        assert breach_file.is_dir()
    finally:
        main_module._BREACH_DIR = original


def test_build_runner_passes_registry_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    repo = config.repositories[0]
    seen: dict[str, Any] = {}

    class _RunnerWithRegistry:
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
            registry: Any,
        ) -> None:
            seen["repo"] = repo_config
            seen["config"] = app_config
            seen["redis_client"] = redis_client
            seen["claude"] = claude_usage_provider
            seen["codex"] = codex_usage_provider
            seen["registry"] = registry

    monkeypatch.setattr(main_module, "PipelineRunner", _RunnerWithRegistry)
    registry = object()
    redis_client = object()

    runner = main_module._build_runner(
        repo,
        config,
        redis_client,
        "claude-provider",
        "codex-provider",
        registry,
    )

    assert isinstance(runner, _RunnerWithRegistry)
    assert seen == {
        "repo": repo,
        "config": config,
        "redis_client": redis_client,
        "claude": "claude-provider",
        "codex": "codex-provider",
        "registry": registry,
    }


def test_main_logs_error_when_no_auth_is_configured(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": False, "gh": False}
    )

    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    assert any("No auth configured" in rec.getMessage() for rec in caplog.records)


def test_main_logs_warning_when_statusline_install_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1, install_statusline_hook=True),
    )
    _patch_main(monkeypatch, config)
    monkeypatch.setattr(
        main_module,
        "_install_statusline_hook",
        lambda _path: (_ for _ in ()).throw(OSError("disk full")),
    )

    with caplog.at_level(logging.WARNING, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    assert any("Failed to install statusline hook" in rec.getMessage() for rec in caplog.records)


def test_main_logs_reload_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, sleep_iterations=3)
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 1)
    load_calls = {"count": 0}

    def fake_load_config() -> AppConfig:
        load_calls["count"] += 1
        if load_calls["count"] == 1:
            return config
        raise RuntimeError("reload boom")

    monkeypatch.setattr(main_module, "load_config", fake_load_config)

    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    assert any("Failed to reload config.yml" in rec.getMessage() for rec in caplog.records)


def test_main_logs_publish_state_failures_for_inactive_runner(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _InactiveFailingRunner(_FakeRunner):
        async def publish_state(self) -> None:
            raise RuntimeError("publish boom")

    config = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", active=False),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, runner_cls=_InactiveFailingRunner)

    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    runner = _FakeRunner.instances[0]
    assert runner.cycles == 0
    assert any("publish paused state failed for octo__alpha" in rec.getMessage() for rec in caplog.records)


# ---------------------------------------------------------------------------
# Wake-on-pubsub tests
# ---------------------------------------------------------------------------


def test_apply_wake_message_resets_last_run() -> None:
    last_run = {"alpha-key": 100.0, "beta-key": 200.0}
    slug_to_key = {"alpha": "alpha-key", "beta": "beta-key"}

    main_module._apply_wake_message(
        {"channel": "orchestrator:wake:alpha"}, last_run, slug_to_key
    )

    assert last_run["alpha-key"] == 0.0
    assert last_run["beta-key"] == 200.0


def test_apply_wake_message_decodes_bytes_channel() -> None:
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}

    main_module._apply_wake_message(
        {"channel": b"orchestrator:wake:alpha"}, last_run, slug_to_key
    )
    assert last_run["alpha-key"] == 0.0


def test_apply_wake_message_ignores_non_string_channel() -> None:
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}

    main_module._apply_wake_message({"channel": 42}, last_run, slug_to_key)
    main_module._apply_wake_message({}, last_run, slug_to_key)
    assert last_run["alpha-key"] == 100.0


def test_apply_wake_message_ignores_unknown_channel() -> None:
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}

    main_module._apply_wake_message(
        {"channel": "repo-events:something"}, last_run, slug_to_key
    )
    main_module._apply_wake_message(
        {"channel": "orchestrator:wake:other"}, last_run, slug_to_key
    )
    assert last_run["alpha-key"] == 100.0


class _ScriptedPubSub:
    """A pubsub stub that returns scripted ``get_message`` results."""

    def __init__(self, results: list[Any]) -> None:
        self._results = list(results)
        self.closed = False
        self.cancelled_calls = 0

    async def get_message(self, ignore_subscribe_messages: bool = True, timeout: Any = None) -> Any:
        if not self._results:
            # Block "forever" so asyncio.wait can let sleep_task win.
            await asyncio.sleep(3600)
            return None
        item = self._results.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    async def aclose(self) -> None:
        self.closed = True


async def test_wait_or_wake_falls_back_to_sleep_when_pubsub_none() -> None:
    last_run: dict[str, float] = {"k": 5.0}
    slept = []

    real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        await real_sleep(0)

    with patch.object(main_module.asyncio, "sleep", fake_sleep):
        healthy = await main_module._wait_or_wake(None, 7.0, last_run, {})

    assert healthy is True
    assert slept == [7.0]


async def test_wait_or_wake_resets_last_run_on_message() -> None:
    pubsub = _ScriptedPubSub([{"channel": "orchestrator:wake:alpha"}, None])
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}

    healthy = await main_module._wait_or_wake(pubsub, 60.0, last_run, slug_to_key)

    assert healthy is True
    assert last_run["alpha-key"] == 0.0


async def test_wait_or_wake_drains_buffered_messages() -> None:
    pubsub = _ScriptedPubSub(
        [
            {"channel": "orchestrator:wake:alpha"},
            {"channel": "orchestrator:wake:beta"},
            None,
        ]
    )
    last_run = {"alpha-key": 100.0, "beta-key": 200.0}
    slug_to_key = {"alpha": "alpha-key", "beta": "beta-key"}

    await main_module._wait_or_wake(pubsub, 60.0, last_run, slug_to_key)

    assert last_run == {"alpha-key": 0.0, "beta-key": 0.0}


async def test_wait_or_wake_drain_swallows_get_message_errors() -> None:
    pubsub = _ScriptedPubSub(
        [
            {"channel": "orchestrator:wake:alpha"},
            RuntimeError("drain boom"),
        ]
    )
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}

    healthy = await main_module._wait_or_wake(pubsub, 60.0, last_run, slug_to_key)
    assert healthy is True
    assert last_run["alpha-key"] == 0.0


async def test_wait_or_wake_marks_unhealthy_on_pubsub_error() -> None:
    pubsub = _ScriptedPubSub([RuntimeError("connection lost")])
    last_run = {"alpha-key": 100.0}

    healthy = await main_module._wait_or_wake(
        pubsub, 0.01, last_run, {"alpha": "alpha-key"}
    )

    assert healthy is False
    assert last_run["alpha-key"] == 100.0


async def test_wait_or_wake_preserves_tick_when_subscriber_errors_early() -> None:
    """A pubsub error must not short-circuit the tick.

    Otherwise the main loop tears the subscriber down and re-subscribes
    immediately, producing a tight reconnect loop during a Redis outage.
    """
    pubsub = _ScriptedPubSub([RuntimeError("connection lost")])
    last_run = {"alpha-key": 100.0}

    loop = asyncio.get_event_loop()
    start = loop.time()
    healthy = await main_module._wait_or_wake(
        pubsub, 0.05, last_run, {"alpha": "alpha-key"}
    )
    elapsed = loop.time() - start

    assert healthy is False
    # Allow scheduler slack but require most of the tick to have elapsed.
    assert elapsed >= 0.04, elapsed


async def test_wait_or_wake_swallows_sleep_error_when_subscriber_errored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sleep that itself raises must surface as a propagated exception.

    The unhealthy-path ``await sleep_task`` is wrapped in BaseException to
    avoid re-raising at the await site; the trailing inspection then
    surfaces the underlying error so it is not silently lost.
    """
    pubsub = _ScriptedPubSub([RuntimeError("connection lost")])
    last_run = {"alpha-key": 100.0}

    async def flaky_sleep(seconds: float) -> None:
        # Stay pending past the FIRST_COMPLETED return so the function
        # actually executes the unhealthy ``await sleep_task`` branch
        # before the sleep raises.
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[None] = loop.create_future()
        loop.call_later(0.01, fut.set_result, None)
        await fut
        raise RuntimeError("sleep boom")

    monkeypatch.setattr(main_module.asyncio, "sleep", flaky_sleep)

    with pytest.raises(RuntimeError, match="sleep boom"):
        await main_module._wait_or_wake(
            pubsub, 0.05, last_run, {"alpha": "alpha-key"}
        )


async def test_wait_or_wake_lets_sleep_win_when_no_messages() -> None:
    pubsub = _ScriptedPubSub([])
    last_run = {"alpha-key": 100.0}

    healthy = await main_module._wait_or_wake(
        pubsub, 0.01, last_run, {"alpha": "alpha-key"}
    )

    assert healthy is True
    assert last_run["alpha-key"] == 100.0


async def test_close_pubsub_handles_none_and_errors() -> None:
    await main_module._close_pubsub(None)

    class _Boom:
        async def aclose(self) -> None:
            raise RuntimeError("close boom")

    await main_module._close_pubsub(_Boom())


def test_main_loop_subscribes_to_active_runners(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When subscribe_wake returns a pubsub, the loop uses it."""
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config)

    subscribed_with: list[tuple[str, ...]] = []
    closed_pubsubs: list[_ScriptedPubSub] = []

    async def fake_subscribe_wake(redis_client: Any, slugs: tuple[str, ...]) -> Any:
        subscribed_with.append(tuple(slugs))
        if not slugs:
            return None
        ps = _ScriptedPubSub([{"channel": "orchestrator:wake:octo__alpha"}, None])
        return ps

    monkeypatch.setattr(main_module, "subscribe_wake", fake_subscribe_wake)

    async def tracking_close(pubsub: Any) -> None:
        if pubsub is not None:
            closed_pubsubs.append(pubsub)

    monkeypatch.setattr(main_module, "_close_pubsub", tracking_close)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    assert subscribed_with == [("octo__alpha",)]


def test_main_loop_recovers_after_unhealthy_pubsub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unhealthy pubsub should be torn down and re-subscribed on next tick."""
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, sleep_iterations=2)

    subscriptions: list[tuple[str, ...]] = []

    async def fake_subscribe_wake(redis_client: Any, slugs: tuple[str, ...]) -> Any:
        subscriptions.append(tuple(slugs))
        if not slugs:
            return None
        return _ScriptedPubSub([RuntimeError("conn lost")])

    monkeypatch.setattr(main_module, "subscribe_wake", fake_subscribe_wake)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    # First subscription on cycle 1, re-subscription on cycle 2 after unhealthy.
    assert subscriptions == [("octo__alpha",), ("octo__alpha",)]


def test_main_loop_retries_subscribe_when_initial_attempt_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``subscribe_wake`` failure must not park the loop on timed polling."""
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, sleep_iterations=3)

    subscriptions: list[tuple[str, ...]] = []

    async def fake_subscribe_wake(redis_client: Any, slugs: tuple[str, ...]) -> Any:
        subscriptions.append(tuple(slugs))
        # First call simulates a transient Redis hiccup. Subsequent calls
        # succeed so we can confirm the retry actually happens.
        if len(subscriptions) == 1:
            return None
        return _ScriptedPubSub([])

    monkeypatch.setattr(main_module, "subscribe_wake", fake_subscribe_wake)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    # The first subscribe returned None, so the loop must keep attempting
    # to subscribe on subsequent cycles instead of latching the slug set.
    assert len(subscriptions) >= 2
    assert all(slugs == ("octo__alpha",) for slugs in subscriptions)


# ---------------------------------------------------------------------------
# PR-184: adaptive IDLE polling — main-loop wiring
# ---------------------------------------------------------------------------


class _FakeIdleRunner:
    """Minimal runner double exposing the PR-184 adaptive interval API."""

    def __init__(
        self,
        *,
        state: PipelineState = PipelineState.IDLE,
        base: int = 60,
        effective: int = 60,
    ) -> None:
        self.repo_config = types.SimpleNamespace(poll_interval_sec=base)
        self.state = types.SimpleNamespace(state=state)
        self.effective_idle_poll_interval = effective
        self.idle_streak_resets = 0

    def reset_idle_streak(self) -> None:
        self.idle_streak_resets += 1


def test_runner_poll_interval_returns_effective_interval_when_idle() -> None:
    runner = _FakeIdleRunner(
        state=PipelineState.IDLE, base=60, effective=300
    )
    assert main_module._runner_poll_interval(runner) == 300


def test_runner_poll_interval_returns_static_interval_when_not_idle() -> None:
    runner = _FakeIdleRunner(
        state=PipelineState.WATCH, base=60, effective=300
    )
    assert main_module._runner_poll_interval(runner) == 60


def test_runner_poll_interval_returns_effective_watch_interval_when_watching() -> None:
    """PR-202: WATCH runners use ``effective_watch_poll_interval``."""
    runner = _FakeIdleRunner(
        state=PipelineState.WATCH, base=60, effective=60
    )
    runner.effective_watch_poll_interval = 300
    assert main_module._runner_poll_interval(runner) == 300


def test_runner_poll_interval_falls_back_when_runner_lacks_property() -> None:
    """Test stubs predating PR-184 keep using the static interval."""
    legacy = types.SimpleNamespace(
        repo_config=types.SimpleNamespace(poll_interval_sec=42),
        state=types.SimpleNamespace(state=PipelineState.IDLE),
    )
    assert main_module._runner_poll_interval(legacy) == 42


def test_apply_wake_message_resets_runner_idle_streak() -> None:
    runner = _FakeIdleRunner()
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}
    runners = {"alpha-key": runner}

    main_module._apply_wake_message(
        {"channel": "orchestrator:wake:alpha"},
        last_run,
        slug_to_key,
        runners,
    )

    assert last_run["alpha-key"] == 0.0
    assert runner.idle_streak_resets == 1


def test_apply_wake_message_skips_runners_missing_reset_method() -> None:
    """Legacy runner stubs without ``reset_idle_streak`` are tolerated."""
    legacy = types.SimpleNamespace()
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}
    runners: dict[str, Any] = {"alpha-key": legacy}

    main_module._apply_wake_message(
        {"channel": "orchestrator:wake:alpha"},
        last_run,
        slug_to_key,
        runners,
    )

    assert last_run["alpha-key"] == 0.0


async def test_wait_or_wake_resets_runner_idle_streak() -> None:
    """Wake-on-pubsub also clears ``_idle_streak`` so the next cycle is fast."""
    pubsub = _ScriptedPubSub([{"channel": "orchestrator:wake:alpha"}, None])
    last_run = {"alpha-key": 100.0}
    slug_to_key = {"alpha": "alpha-key"}
    runner = _FakeIdleRunner()
    runners = {"alpha-key": runner}

    healthy = await main_module._wait_or_wake(
        pubsub, 60.0, last_run, slug_to_key, runners
    )

    assert healthy is True
    assert last_run["alpha-key"] == 0.0
    assert runner.idle_streak_resets == 1


# ---------------------------------------------------------------------------
# PR-207: parallel per-runner run_cycle scheduling
# ---------------------------------------------------------------------------


def test_main_schedules_run_cycles_in_parallel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long-running cycle on runner A must not delay runner B's cycle.

    The previous serial loop awaited each runner in turn, so runner B was
    starved of polling for the duration of A's CODING/FIX subprocess.
    """
    a_started = asyncio.Event()
    b_started = asyncio.Event()
    release_a = asyncio.Event()

    class _ParallelRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            if self.name == "octo__alpha":
                a_started.set()
                await release_a.wait()
            else:
                b_started.set()

    config = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _ParallelRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls = [0]

    async def fake_sleep(seconds: float) -> None:
        sleep_calls[0] += 1
        clock[0] += seconds + 1
        await _REAL_ASYNCIO_SLEEP(0)
        if sleep_calls[0] == 1:
            # First wait: both cycles have been scheduled. B finished
            # almost immediately while A is still blocked on release_a —
            # exactly the property this test asserts.
            assert a_started.is_set(), "alpha cycle never started"
            assert b_started.is_set(), (
                "beta cycle was starved by alpha's blocking run_cycle"
            )
            release_a.set()
        if sleep_calls[0] >= 2:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    alpha = next(r for r in _FakeRunner.instances if r.name == "octo__alpha")
    beta = next(r for r in _FakeRunner.instances if r.name == "octo__beta")
    assert alpha.cycles >= 1
    assert beta.cycles >= 1


def test_main_does_not_double_schedule_running_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A still-running cycle must not be scheduled a second time.

    Without per-runner deduplication, a runner whose CODING cycle outlives
    one main-loop tick would have a fresh cycle stacked on top each tick,
    multiplying coder subprocesses and corrupting per-runner state.
    """
    release = asyncio.Event()

    class _BlockingRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            await release.wait()

    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _BlockingRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls = [0]

    async def fake_sleep(seconds: float) -> None:
        sleep_calls[0] += 1
        # Advance past the poll interval so the loop would re-schedule
        # the runner if it weren't deduplicated against the in-flight task.
        clock[0] += seconds + 100
        await _REAL_ASYNCIO_SLEEP(0)
        if sleep_calls[0] >= 4:
            release.set()
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    runner = _FakeRunner.instances[0]
    # Only one cycle was scheduled; subsequent ticks observed the
    # in-flight task as still running and skipped scheduling.
    assert runner.cycles == 1


def test_main_continues_other_runners_when_one_cycle_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An exception inside one runner's task does not affect another's task.

    Companion to ``test_main_continues_when_one_runner_raises``: with the
    parallel scheduler, isolation is provided by the per-runner task
    boundary, not by the previous ``try/except`` around an awaited call.
    """

    class _FailingFirstRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            if self.name == "octo__alpha":
                raise RuntimeError("alpha boom")

    config = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, runner_cls=_FailingFirstRunner)

    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        with pytest.raises(_StopLoop):
            asyncio.run(main_module.main())

    alpha = next(r for r in _FakeRunner.instances if r.name == "octo__alpha")
    beta = next(r for r in _FakeRunner.instances if r.name == "octo__beta")
    assert alpha.cycles == 1
    assert beta.cycles == 1
    errors = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
    assert any(
        "run_cycle failed for octo__alpha" in msg for msg in errors
    ), errors


def test_drain_finished_cycles_logs_runner_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``_drain_finished_cycles`` collects exceptions from completed tasks."""

    async def boom() -> None:
        raise RuntimeError("kaboom")

    async def runner_test() -> None:
        task = asyncio.create_task(boom())
        # Yield so the task can run and store its exception.
        await asyncio.sleep(0)

        runner = types.SimpleNamespace(name="octo__alpha")
        in_flight: dict[str, asyncio.Task[None]] = {"key": task}
        runners = {"key": runner}

        with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
            main_module._drain_finished_cycles(in_flight, runners)

        assert in_flight == {}
        errors = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
        assert any(
            "run_cycle failed for octo__alpha" in msg for msg in errors
        ), errors

    asyncio.run(runner_test())


def test_drain_finished_cycles_silently_collects_cancelled_task() -> None:
    """A cancelled task is intentional teardown, not a runner bug — no log."""

    async def slow() -> None:
        await asyncio.Event().wait()

    async def scenario() -> None:
        task = asyncio.create_task(slow())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        runner = types.SimpleNamespace(name="octo__alpha")
        in_flight: dict[str, asyncio.Task[None]] = {"key": task}
        runners = {"key": runner}

        # No logs — pure drain.
        main_module._drain_finished_cycles(in_flight, runners)
        assert in_flight == {}

    asyncio.run(scenario())


def test_drain_finished_cycles_keeps_running_task() -> None:
    """In-flight tasks that have not finished must remain in the dict."""

    pending = asyncio.Event()

    async def slow() -> None:
        await pending.wait()

    async def scenario() -> None:
        task = asyncio.create_task(slow())
        # Give the task one turn to start. It will not finish.
        await asyncio.sleep(0)

        in_flight: dict[str, asyncio.Task[None]] = {"key": task}
        runners: dict[str, Any] = {}

        main_module._drain_finished_cycles(in_flight, runners)

        assert "key" in in_flight, "running task must not be popped"
        pending.set()
        await task

    asyncio.run(scenario())


def test_cleanup_in_flight_for_removed_cancels_running_task() -> None:
    """Runners removed by config reload must have their cycle task cancelled.

    Otherwise a slow CODING/FIX task on a now-removed repo would keep
    holding open a coder subprocess and the coder rate-limit slot.
    """

    cancelled_observed = asyncio.Event()

    async def long_cycle() -> None:
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cancelled_observed.set()
            raise

    async def scenario() -> None:
        task = asyncio.create_task(long_cycle())
        # Let the task start so the cancellation actually goes through
        # the awaited sleep rather than stopping at task scheduling.
        await asyncio.sleep(0)

        in_flight: dict[str, asyncio.Task[None]] = {"removed-key": task}
        await main_module._cleanup_in_flight_for_removed(
            in_flight, {"removed-key"}
        )

        assert "removed-key" not in in_flight
        assert task.cancelled()
        assert cancelled_observed.is_set()

    asyncio.run(scenario())


def test_cleanup_in_flight_for_removed_drains_completed_task(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A removed runner whose cycle already finished with an exception must
    still have that exception logged so the failure is not silently lost
    when the runner disappears from the live set."""

    async def boom() -> None:
        raise RuntimeError("post-removal boom")

    async def scenario() -> None:
        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        assert task.done()

        in_flight: dict[str, asyncio.Task[None]] = {"removed-key": task}
        with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
            await main_module._cleanup_in_flight_for_removed(
                in_flight, {"removed-key"}
            )

        assert in_flight == {}
        errors = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
        assert any(
            "run_cycle failed for removed-key (runner removed)" in msg
            for msg in errors
        ), errors

    asyncio.run(scenario())


def test_cleanup_in_flight_for_removed_silently_drops_already_cancelled() -> None:
    """A task cancelled before reload reaches cleanup is silently dropped."""

    async def slow() -> None:
        await asyncio.Event().wait()

    async def scenario() -> None:
        task = asyncio.create_task(slow())
        await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert task.done()

        in_flight: dict[str, asyncio.Task[None]] = {"removed-key": task}
        await main_module._cleanup_in_flight_for_removed(
            in_flight, {"removed-key"}
        )

        assert in_flight == {}

    asyncio.run(scenario())


def test_cleanup_in_flight_for_removed_ignores_unknown_keys() -> None:
    """No-op when the removed key has no in-flight task."""

    async def scenario() -> None:
        in_flight: dict[str, asyncio.Task[None]] = {}
        await main_module._cleanup_in_flight_for_removed(
            in_flight, {"never-scheduled"}
        )
        assert in_flight == {}

    asyncio.run(scenario())


def test_main_cancels_in_flight_for_removed_runner_on_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config reload that drops a runner cancels any cycle still running for it."""

    cancelled_for: list[str] = []
    block = asyncio.Event()  # never set; use Event.wait() rather than
    # asyncio.sleep so the patched main_module.asyncio.sleep cannot turn
    # this into a fast-return inside the runner.

    class _SlowRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            try:
                await block.wait()
            except asyncio.CancelledError:
                cancelled_for.append(self.name)
                raise

    first = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git"),
            _repo("https://github.com/octo/beta.git"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    second = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    monkeypatch.setattr(main_module, "load_config", fake_load_config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _SlowRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 1)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls = [0]

    async def fake_sleep(seconds: float) -> None:
        sleep_calls[0] += 1
        # Advance past the reload window so the next iteration triggers
        # config reload and drops beta from the live set.
        clock[0] += seconds + 5
        await _REAL_ASYNCIO_SLEEP(0)
        if sleep_calls[0] >= 2:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    # Beta is cancelled by the reload path mid-loop. Alpha may also be
    # cancelled at asyncio.run shutdown (pending tasks are torn down when
    # main() ends), but the order shows beta went first — that is the
    # property under test.
    assert cancelled_for[:1] == ["octo__beta"], cancelled_for


def test_cleanup_in_flight_for_removed_propagates_caller_cancellation() -> None:
    """A SIGINT/SIGTERM cancellation of the daemon's main task while we
    await the cancelled cycle must propagate, not be swallowed by the
    blanket BaseException handler. Otherwise the daemon ignores the
    first shutdown signal whenever it lands during a config-reload
    cleanup."""

    async def long_cycle() -> None:
        await asyncio.Event().wait()

    async def scenario() -> None:
        cycle_task = asyncio.create_task(long_cycle())
        await asyncio.sleep(0)

        outer_started = asyncio.Event()

        async def outer() -> None:
            in_flight: dict[str, asyncio.Task[None]] = {"removed-key": cycle_task}
            outer_started.set()
            await main_module._cleanup_in_flight_for_removed(
                in_flight, {"removed-key"}
            )

        outer_task = asyncio.create_task(outer())
        await outer_started.wait()
        # Yield once so the cleanup is parked in ``await task``.
        await asyncio.sleep(0)
        outer_task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await outer_task

    asyncio.run(scenario())


def test_sync_runners_defers_swap_when_cycle_is_in_flight() -> None:
    """While a cycle is mid-execution, in-place mutation of repo_config
    or app_config could let the running cycle observe a mixed old/new
    snapshot across awaits. ``_sync_runners`` must route the change
    through ``stage_config_reload`` instead."""

    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = None

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

    daemon_config = DaemonConfig(poll_interval_sec=1)
    original_repo = _repo(
        "https://github.com/octo/alpha.git",
        coder="claude",
        auto_merge=True,
    )
    updated_repo = _repo(
        "https://github.com/octo/alpha.git",
        coder="claude",
        auto_merge=False,
    )
    runner = _StagingRunner(
        original_repo,
        AppConfig(repositories=[original_repo], daemon=daemon_config),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )
    runner.state.state = PipelineState.WATCH

    async def scenario() -> None:
        async def long_cycle() -> None:
            await asyncio.Event().wait()

        cycle = asyncio.create_task(long_cycle())
        try:
            await asyncio.sleep(0)
            in_flight: dict[str, asyncio.Task[None]] = {
                "https://github.com/octo/alpha": cycle,
            }
            config = AppConfig(
                repositories=[updated_repo],
                daemon=daemon_config,
            )

            main_module._sync_runners(
                {"https://github.com/octo/alpha": runner},
                config,
                _FakeRedisClient(),
                "claude-provider",
                "codex-provider",
                registry=None,  # type: ignore[arg-type]
                in_flight=in_flight,
            )

            # The non-coder change would have applied in-place if the cycle
            # were not running; with an in-flight task we must stage instead
            # so the running cycle keeps reading the original snapshot.
            assert runner.repo_config is original_repo
            assert runner.repo_config.auto_merge is True
            assert runner.staged is not None
            staged_repo, staged_app, staged_claude, staged_codex = runner.staged
            assert staged_repo is updated_repo
            assert staged_app is config
            assert staged_claude == "claude-provider"
            assert staged_codex == "codex-provider"
        finally:
            cycle.cancel()
            try:
                await cycle
            except asyncio.CancelledError:
                pass

    asyncio.run(scenario())


def test_sync_runners_applies_in_place_when_cycle_already_done() -> None:
    """A finished task in ``in_flight`` (not yet drained) is not a reason
    to defer — the cycle has already returned, so its ``repo_config``
    reads are over and an immediate swap is safe."""

    class _StagingRunner(_FakeRunner):
        def __init__(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            redis_client: Any,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            super().__init__(
                repo_config,
                app_config,
                redis_client,
                claude_usage_provider,
                codex_usage_provider,
            )
            self.staged: tuple[Any, ...] | None = None

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self.staged = (
                repo_config,
                app_config,
                claude_usage_provider,
                codex_usage_provider,
            )

        def clear_staged_config_reload(self) -> None:
            self.staged = None

    daemon_config = DaemonConfig(poll_interval_sec=1)
    runner = _StagingRunner(
        _repo("https://github.com/octo/alpha.git", coder="claude", auto_merge=True),
        AppConfig(
            repositories=[
                _repo(
                    "https://github.com/octo/alpha.git",
                    coder="claude",
                    auto_merge=True,
                )
            ],
            daemon=daemon_config,
        ),
        _FakeRedisClient(),
        "old-claude-provider",
        "old-codex-provider",
    )
    runner.state.state = PipelineState.WATCH

    async def scenario() -> None:
        async def quick_cycle() -> None:
            return None

        finished = asyncio.create_task(quick_cycle())
        await finished

        in_flight: dict[str, asyncio.Task[None]] = {
            "https://github.com/octo/alpha": finished,
        }
        config = AppConfig(
            repositories=[
                _repo(
                    "https://github.com/octo/alpha.git",
                    coder="claude",
                    auto_merge=False,
                )
            ],
            daemon=daemon_config,
        )

        main_module._sync_runners(
            {"https://github.com/octo/alpha": runner},
            config,
            _FakeRedisClient(),
            "claude-provider",
            "codex-provider",
            registry=None,  # type: ignore[arg-type]
            in_flight=in_flight,
        )

        assert runner.staged is None
        assert runner.repo_config.auto_merge is False
        assert runner.app_config is config
        assert runner.claude_usage_provider == "claude-provider"

    asyncio.run(scenario())


def test_drain_finished_cycle_applies_pending_in_flight_config() -> None:
    """The post-cycle drain must apply any config that was staged while
    the cycle was running so the next cycle starts with the new
    snapshot — otherwise the cycle finishes, the next one is scheduled,
    and the staged change waits another full cycle (or never applies if
    the runner never reaches IDLE)."""

    applied = {"called": 0}

    class _Runner:
        name = "octo__alpha"

        def _apply_staged_config_reload(self) -> None:
            applied["called"] += 1

    async def scenario() -> None:
        async def quick() -> None:
            return None

        task = asyncio.create_task(quick())
        await task

        in_flight: dict[str, asyncio.Task[None]] = {"key": task}
        runners = {"key": _Runner()}

        popped = main_module._drain_finished_cycle("key", in_flight, runners)

        assert popped is True
        assert in_flight == {}
        assert applied["called"] == 1

    asyncio.run(scenario())


def test_drain_finished_cycle_skips_running_task() -> None:
    """A still-running task is not popped and its staged config is not
    yet applied — the running cycle is still using the old snapshot."""

    applied = {"called": 0}

    class _Runner:
        name = "octo__alpha"

        def _apply_staged_config_reload(self) -> None:
            applied["called"] += 1

    pending = asyncio.Event()

    async def scenario() -> None:
        async def slow() -> None:
            await pending.wait()

        task = asyncio.create_task(slow())
        await asyncio.sleep(0)

        in_flight: dict[str, asyncio.Task[None]] = {"key": task}
        runners = {"key": _Runner()}

        popped = main_module._drain_finished_cycle("key", in_flight, runners)

        assert popped is False
        assert "key" in in_flight
        assert applied["called"] == 0

        pending.set()
        await task

    asyncio.run(scenario())


def test_main_defers_repo_config_swap_during_in_flight_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: a config reload that fires while a cycle is in flight
    must not mutate ``runner.repo_config`` until the cycle finishes."""

    cycle_started = asyncio.Event()
    cycle_block = asyncio.Event()
    observed_branches: list[str] = []

    class _SlowRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            observed_branches.append(self.repo_config.branch)
            cycle_started.set()
            await cycle_block.wait()

        def stage_config_reload(
            self,
            repo_config: RepoConfig,
            app_config: AppConfig,
            claude_usage_provider: Any,
            codex_usage_provider: Any,
        ) -> None:
            self._staged_branch = repo_config.branch
            self._pending_repo_config = repo_config
            self._pending_app_config = app_config
            self._pending_usage_providers = (
                claude_usage_provider,
                codex_usage_provider,
            )

        def _apply_staged_config_reload(self) -> None:
            if getattr(self, "_pending_repo_config", None) is None:
                return
            self.repo_config = self._pending_repo_config
            self.app_config = self._pending_app_config
            self.claude_usage_provider, self.codex_usage_provider = (
                self._pending_usage_providers
            )
            self._pending_repo_config = None
            self._pending_app_config = None
            self._pending_usage_providers = None

        def clear_staged_config_reload(self) -> None:
            self._pending_repo_config = None
            self._pending_app_config = None
            self._pending_usage_providers = None

    first = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", branch="main"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    second = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", branch="release"),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    _reset_fake_runner()
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    monkeypatch.setattr(main_module, "load_config", fake_load_config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: _FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _SlowRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 1)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls = [0]
    runner_holder: dict[str, _SlowRunner] = {}

    async def fake_sleep(seconds: float) -> None:
        sleep_calls[0] += 1
        clock[0] += seconds + 5
        await _REAL_ASYNCIO_SLEEP(0)
        if sleep_calls[0] == 1:
            # Cycle 1 has been scheduled; wait for it to start so the
            # next iteration's config reload races a real in-flight task.
            await cycle_started.wait()
            # Snapshot the runner before the reload races the cycle so
            # we can assert post-cycle that the swap was deferred until
            # after the cycle returned.
            runner_holder["r"] = _FakeRunner.instances[0]  # type: ignore[assignment]
        if sleep_calls[0] == 2:
            # The reload should already have fired in this iteration.
            # While the cycle is still blocked, the swap must NOT have
            # taken effect.
            assert runner_holder["r"].repo_config.branch == "main", (
                "in-place swap leaked into a running cycle"
            )
            cycle_block.set()
            await _REAL_ASYNCIO_SLEEP(0)
        if sleep_calls[0] >= 3:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    runner = runner_holder["r"]
    # The running cycle observed only the original branch; the deferred
    # swap landed once it returned and the next cycle picks up the new
    # branch.
    assert observed_branches[:1] == ["main"]
    assert runner.repo_config.branch == "release"


def test_apply_pending_in_flight_config_logs_apply_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A defensive guard: if the runner's apply hook raises (it should
    not, since the implementation is plain attribute assignment), the
    error is logged so a stuck pending swap is at least visible in the
    daemon log."""

    class _BadRunner:
        name = "octo__alpha"

        def _apply_staged_config_reload(self) -> None:
            raise RuntimeError("apply failed")

    runner = _BadRunner()
    with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
        main_module._apply_pending_in_flight_config(runner)  # type: ignore[arg-type]

    errors = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
    assert any(
        "Failed to apply deferred config for octo__alpha" in msg for msg in errors
    ), errors


def test_apply_pending_in_flight_config_skips_runners_without_apply_method() -> None:
    """Test stubs without ``_apply_staged_config_reload`` are a no-op so
    the drain helper does not need a hasattr check on every iteration."""

    class _MinimalRunner:
        name = "octo__alpha"

    main_module._apply_pending_in_flight_config(_MinimalRunner())  # type: ignore[arg-type]


def test_cleanup_in_flight_for_removed_logs_non_cancellation_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If a runner cycle catches ``CancelledError`` and re-raises a
    different exception (or raises one before the cancel completes),
    the cleanup must log it rather than silently dropping the failure
    when the runner is removed."""

    async def cycle() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError("post-cancel boom")

    async def scenario() -> None:
        task = asyncio.create_task(cycle())
        await asyncio.sleep(0)

        in_flight: dict[str, asyncio.Task[None]] = {"removed-key": task}
        with caplog.at_level(logging.ERROR, logger=main_module.logger.name):
            await main_module._cleanup_in_flight_for_removed(
                in_flight, {"removed-key"}
            )

        assert in_flight == {}
        errors = [rec.getMessage() for rec in caplog.records if rec.levelno == logging.ERROR]
        assert any(
            "run_cycle failed for removed-key (runner removed)" in msg
            for msg in errors
        ), errors

    asyncio.run(scenario())


def test_main_drains_cycle_finished_during_scheduling_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When an active runner's cycle finishes while the scheduling loop
    is awaiting an inactive peer's ``publish_state``, the in-line drain
    at the scheduling step must clean up the completed task before
    scheduling its replacement so the old task is not leaked into the
    next iteration's drain (where its exception would no longer be
    associated with a runner)."""

    proceed = asyncio.Event()
    inactive_publish_calls = {"n": 0}

    class _Runner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            await proceed.wait()

        async def publish_state(self) -> None:
            if not self.repo_config.active:
                inactive_publish_calls["n"] += 1
                if inactive_publish_calls["n"] >= 2:
                    proceed.set()
                    # Yield twice so the active runner's blocked cycle
                    # has a chance to wake and finish before we return
                    # control to the scheduling step.
                    await _REAL_ASYNCIO_SLEEP(0)
                    await _REAL_ASYNCIO_SLEEP(0)

    config = AppConfig(
        repositories=[
            _repo("https://github.com/octo/alpha.git", active=False),
            _repo("https://github.com/octo/beta.git", active=True),
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    _patch_main(monkeypatch, config, runner_cls=_Runner, sleep_iterations=3)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    beta = next(r for r in _FakeRunner.instances if r.name == "octo__beta")
    # Beta's first cycle finished mid-iteration during alpha's
    # publish_state; the scheduling step's in-line drain popped it and
    # scheduled the next cycle in the same pass, so we observe at
    # least 2 cycles.
    assert beta.cycles >= 2
