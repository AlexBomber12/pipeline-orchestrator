"""PR-224b: Config reload tests.

Mechanical move from tests/test_runner.py. Helpers live in
``tests/runner/_helpers.py``.
"""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from src.config import AppConfig, CoderType, DaemonConfig, RepoConfig
from src.models import PipelineState

from tests.runner._helpers import (
    _FakeUsageProvider,
    _make_runner,
    _preflight_true_stub,
    runner_module,
)


def test_reload_repo_config_if_dirty_updates_coder_at_idle_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store["control:octo__demo:config_dirty"] = "1"
    runner.repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": None}
    )

    reloaded = AppConfig(
        repositories=[
            RepoConfig.model_validate(
                {**runner.repo_config.model_dump(), "coder": "codex"}
            )
        ],
        daemon=runner.app_config.daemon,
    )
    monkeypatch.setattr(runner_module, "load_config", lambda path="config.yml": reloaded)

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX
    assert "control:octo__demo:config_dirty" not in runner.redis.store
    assert (
        runner.state.history[-1]["event"]
        == "[INFRA] Reloaded repo config from config.yml."
    )


def test_stage_config_reload_tracks_idle_boundary_flag() -> None:
    """``stage_config_reload`` records whether the staged change must wait
    for an IDLE boundary so the daemon's post-cycle drain can decide
    whether to apply immediately or keep deferring."""

    runner = _make_runner()
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    next_app_config = AppConfig(
        repositories=[next_repo_config],
        daemon=runner.app_config.daemon,
    )

    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="claude"),
        _FakeUsageProvider(snapshot="codex"),
    )
    assert runner._pending_requires_idle_boundary is False

    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="claude"),
        _FakeUsageProvider(snapshot="codex"),
        requires_idle_boundary=True,
    )
    assert runner._pending_requires_idle_boundary is True

    runner.clear_staged_config_reload()
    assert runner._pending_requires_idle_boundary is False


def test_stage_config_reload_preserves_idle_boundary_across_updates() -> None:
    """A later ``requires_idle_boundary=False`` reload must not downgrade
    a pending coder-swap deferral that was staged earlier in the same
    in-flight window."""

    runner = _make_runner()
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    next_app_config = AppConfig(
        repositories=[next_repo_config],
        daemon=runner.app_config.daemon,
    )

    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="claude"),
        _FakeUsageProvider(snapshot="codex"),
        requires_idle_boundary=True,
    )
    assert runner._pending_requires_idle_boundary is True

    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="claude-2"),
        _FakeUsageProvider(snapshot="codex-2"),
    )
    assert runner._pending_requires_idle_boundary is True

    runner.clear_staged_config_reload()
    assert runner._pending_requires_idle_boundary is False


def test_staged_config_reload_waits_until_idle_boundary() -> None:
    runner = _make_runner()
    original_coder = runner.repo_config.coder
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    next_app_config = AppConfig(
        repositories=[next_repo_config],
        daemon=runner.app_config.daemon,
    )
    next_claude = _FakeUsageProvider(snapshot="new-claude")
    next_codex = _FakeUsageProvider(snapshot="new-codex")

    runner.state.state = PipelineState.WATCH
    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        next_claude,
        next_codex,
    )

    assert runner.repo_config.coder == original_coder
    assert runner._pending_repo_config is not None

    runner.state.state = PipelineState.IDLE
    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX
    assert runner.app_config is next_app_config
    assert runner._pending_repo_config is None
    assert runner._claude_usage_provider is next_claude
    assert runner._codex_usage_provider is next_codex


def test_reload_repo_config_if_dirty_clears_missing_repo_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store["control:octo__demo:config_dirty"] = "1"
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    next_app_config = AppConfig(
        repositories=[next_repo_config],
        daemon=runner.app_config.daemon,
    )
    runner.stage_config_reload(
        next_repo_config,
        next_app_config,
        _FakeUsageProvider(snapshot="new-claude"),
        _FakeUsageProvider(snapshot="new-codex"),
    )
    monkeypatch.setattr(
        runner_module,
        "load_config",
        lambda path="config.yml": AppConfig(
            repositories=[],
            daemon=runner.app_config.daemon,
        ),
    )

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert "control:octo__demo:config_dirty" not in runner.redis.store
    assert runner.repo_config.coder == CoderType.CODEX


def test_reload_repo_config_if_dirty_supports_redis_without_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()

    class _GetOnlyRedis:
        def __init__(self, store: dict[str, str]) -> None:
            self.store = store

        async def get(self, key: str) -> str | None:
            return self.store.get(key)

        async def delete(self, key: str) -> int:
            existed = key in self.store
            self.store.pop(key, None)
            return int(existed)

    runner.redis = _GetOnlyRedis({"control:octo__demo:config_dirty": "1"})  # type: ignore[assignment]
    reloaded = AppConfig(
        repositories=[
            RepoConfig.model_validate(
                {**runner.repo_config.model_dump(), "coder": "codex"}
            )
        ],
        daemon=runner.app_config.daemon,
    )
    monkeypatch.setattr(runner_module, "load_config", lambda path="config.yml": reloaded)

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX


def test_reload_repo_config_if_dirty_applies_staged_reload_when_redis_unavailable() -> None:
    runner = _make_runner()
    next_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    runner.stage_config_reload(
        next_repo_config,
        AppConfig(repositories=[next_repo_config], daemon=runner.app_config.daemon),
        None,
        None,
    )

    async def broken_exists(key: str) -> int:
        raise RedisConnectionError("redis down")

    runner.redis.exists = broken_exists  # type: ignore[method-assign]

    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CODEX
    assert runner._pending_repo_config is None


def test_reload_repo_config_if_dirty_clears_staged_reload_after_disk_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()
    runner.redis.store["control:octo__demo:config_dirty"] = "1"
    staged_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "codex"}
    )
    staged_claude = _FakeUsageProvider(snapshot="new-claude")
    staged_codex = _FakeUsageProvider(snapshot="new-codex")
    runner.stage_config_reload(
        staged_repo_config,
        AppConfig(
            repositories=[staged_repo_config],
            daemon=runner.app_config.daemon,
        ),
        staged_claude,
        staged_codex,
    )
    disk_repo_config = RepoConfig.model_validate(
        {**runner.repo_config.model_dump(), "coder": "claude"}
    )
    refreshed_app_config = AppConfig(
        repositories=[disk_repo_config],
        daemon=DaemonConfig(
            **{
                **runner.app_config.daemon.model_dump(),
                "usage_api_cache_ttl_sec": 321,
                "usage_api_beta_header": "oauth-test-header",
            }
        ),
    )
    monkeypatch.setattr(
        runner_module,
        "load_config",
        lambda path="config.yml": refreshed_app_config,
    )

    asyncio.run(runner.reload_repo_config_if_dirty())
    asyncio.run(runner.reload_repo_config_if_dirty())

    assert runner.repo_config.coder == CoderType.CLAUDE
    assert runner._pending_repo_config is None
    assert runner._pending_app_config is None
    assert runner._pending_usage_providers is None
    assert runner._claude_usage_provider is not staged_claude
    assert runner._codex_usage_provider is not staged_codex
    assert getattr(runner._claude_usage_provider, "_cache_ttl") == 321
    assert getattr(runner._claude_usage_provider, "_beta_header") == "oauth-test-header"
    assert getattr(runner._codex_usage_provider, "_cache_ttl") == 321


def test_run_cycle_does_not_reload_dirty_config_before_error_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_reload_repo_config_if_dirty() -> None:
        calls.append("reload")

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(
        runner, "reload_repo_config_if_dirty", fake_reload_repo_config_if_dirty
    )
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == ["handle_error", "publish"]


def test_run_cycle_stops_idle_dispatch_when_dirty_reload_disables_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.IDLE

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        calls.append("refresh")

    async def fake_reload_repo_config_if_dirty() -> None:
        calls.append("reload")
        runner.repo_config.active = False

    async def fake_handle_idle() -> None:
        calls.append("handle_idle")

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(
        runner, "reload_repo_config_if_dirty", fake_reload_repo_config_if_dirty
    )
    monkeypatch.setattr(runner, "handle_idle", fake_handle_idle)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert "reload" in calls
    assert "handle_idle" not in calls
    assert calls[-1] == "publish"


def test_run_cycle_error_state_ignores_dirty_reload_until_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    runner = _make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.ERROR
    runner.state.rate_limited_until = None

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_reload_repo_config_if_dirty() -> None:
        calls.append("reload")
        runner.repo_config.active = False

    async def fake_handle_error() -> None:
        calls.append("handle_error")

    async def fake_publish_state() -> None:
        calls.append("publish")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", _preflight_true_stub)
    monkeypatch.setattr(
        runner, "reload_repo_config_if_dirty", fake_reload_repo_config_if_dirty
    )
    monkeypatch.setattr(runner, "handle_error", fake_handle_error)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == ["handle_error", "publish"]
