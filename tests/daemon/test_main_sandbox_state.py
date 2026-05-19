"""Tests for the daemon-main sandbox-state startup/reload hook (PR-353)."""

from __future__ import annotations

import asyncio
import types
from typing import Any

import pytest

from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import main as main_module
from src.models import PipelineState
from src.sandbox.runtime_state import (
    REDIS_SANDBOX_STATE_KEY,
    SandboxState,
)


_REAL_ASYNCIO_SLEEP = asyncio.sleep


class _StopLoop(Exception):
    """End the main loop deterministically inside tests."""


class _CapturingRedis:
    """Async Redis stub that records every ``set`` call."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_history: list[tuple[str, str]] = []

    async def scan_iter(self, match: str):
        if False:
            yield match

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str) -> None:
        self.set_history.append((key, value))
        self.store[key] = value


class _FakeRunner:
    """Minimal runner that records its own construction and cycles."""

    instances: list["_FakeRunner"] = []

    def __init__(
        self,
        repo_config: RepoConfig,
        app_config: AppConfig,
        redis_client: Any,
        claude_usage_provider: Any,
        codex_usage_provider: Any,
    ) -> None:
        from src.utils import repo_slug_from_url

        self.repo_config = repo_config
        self.app_config = app_config
        self.name = repo_slug_from_url(repo_config.url)
        self.cycles = 0
        self.state = types.SimpleNamespace(state=PipelineState.IDLE)
        _FakeRunner.instances.append(self)

    async def run_cycle(self) -> None:
        self.cycles += 1

    async def publish_state(self) -> None:
        return None

    def set_usage_providers(
        self,
        claude_usage_provider: Any,
        codex_usage_provider: Any,
    ) -> None:
        return None


@pytest.fixture(autouse=True)
def _disable_config_watcher(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(main_module, "watch_config_file_changes", _noop)
    monkeypatch.setattr(main_module, "watch_config_changes", _noop)


def _patch_common(
    monkeypatch: pytest.MonkeyPatch,
    load_config_fn: Any,
    redis_client: _CapturingRedis,
    sleep_iterations: int = 1,
) -> dict[str, Any]:
    _FakeRunner.instances = []
    monkeypatch.setattr(main_module, "load_config", load_config_fn)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: redis_client,
    )
    monkeypatch.setattr(main_module, "PipelineRunner", _FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    async def _zero_migration(*_args: Any, **_kwargs: Any) -> int:
        return 0

    async def _empty_backfill(*_args: Any, **_kwargs: Any) -> dict[str, int]:
        return {
            "records_scanned": 0,
            "records_migrated": 0,
            "records_skipped_already_migrated": 0,
            "records_skipped_non_hash": 0,
            "records_skipped_malformed": 0,
        }

    monkeypatch.setattr(
        main_module, "migrate_hung_to_idle_on_startup", _zero_migration
    )
    monkeypatch.setattr(
        main_module, "migrate_escalate_to_error_on_startup", _zero_migration
    )
    monkeypatch.setattr(
        main_module, "migrate_run_records_to_outcome_cause", _empty_backfill
    )

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        await _REAL_ASYNCIO_SLEEP(0)
        if len(sleep_calls) >= sleep_iterations:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)
    return {"sleep_calls": sleep_calls, "clock": clock}


def _repo(url: str, **kwargs: Any) -> RepoConfig:
    kwargs.setdefault("poll_interval_sec", 1)
    return RepoConfig(url=url, **kwargs)


def test_redis_state_written_on_startup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(
            poll_interval_sec=1, coder_filesystem_isolation=False
        ),
    )
    redis_client = _CapturingRedis()
    _patch_common(monkeypatch, lambda: config, redis_client)

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    # The disabled branch in detect_sandbox_state writes the value
    # synchronously without invoking bwrap, so the key must be present
    # by the time the first sleep tick fires.
    assert redis_client.store[REDIS_SANDBOX_STATE_KEY] == (
        SandboxState.DISABLED.value
    )
    assert (REDIS_SANDBOX_STATE_KEY, SandboxState.DISABLED.value) in (
        redis_client.set_history
    )


def test_redis_state_refreshed_on_config_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(
            poll_interval_sec=1, coder_filesystem_isolation=False
        ),
    )
    second = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(
            poll_interval_sec=1, coder_filesystem_isolation=True
        ),
    )
    load_calls = {"n": 0}

    def fake_load_config() -> AppConfig:
        load_calls["n"] += 1
        return first if load_calls["n"] == 1 else second

    redis_client = _CapturingRedis()
    # Two loop iterations so the second iteration crosses the reload
    # window (CONFIG_RELOAD_CYCLES=1 forces a reload on every tick).
    _patch_common(
        monkeypatch, fake_load_config, redis_client, sleep_iterations=2
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 1)

    # Make the sandbox probe return UNAVAILABLE on the second config
    # (isolation enabled, no bwrap on the host) so we do not depend on a
    # real ``bwrap`` binary in the test environment.
    async def _fake_detect(coder_filesystem_isolation: bool) -> SandboxState:
        if not coder_filesystem_isolation:
            return SandboxState.DISABLED
        return SandboxState.UNAVAILABLE

    monkeypatch.setattr(
        "src.sandbox.runtime_state.detect_sandbox_state", _fake_detect
    )

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    # Both the startup and post-reload writes must show up in order, so
    # the dashboard observes the transition from disabled to unavailable.
    history = [
        value
        for key, value in redis_client.set_history
        if key == REDIS_SANDBOX_STATE_KEY
    ]
    assert history == [
        SandboxState.DISABLED.value,
        SandboxState.UNAVAILABLE.value,
    ], history
    assert redis_client.store[REDIS_SANDBOX_STATE_KEY] == (
        SandboxState.UNAVAILABLE.value
    )


def test_redis_state_refreshed_when_reload_event_yields_same_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reload event with an identical AppConfig (touch config.yml or an
    # edit to a watched file that does not change the parsed config) must
    # still re-probe the sandbox so a bwrap install/removal mid-run is
    # picked up by the dashboard badge.
    config = AppConfig(
        repositories=[_repo("https://github.com/octo/alpha.git")],
        daemon=DaemonConfig(
            poll_interval_sec=1, coder_filesystem_isolation=True
        ),
    )

    def fake_load_config() -> AppConfig:
        return config

    redis_client = _CapturingRedis()
    _patch_common(
        monkeypatch, fake_load_config, redis_client, sleep_iterations=2
    )
    monkeypatch.setattr(main_module, "CONFIG_RELOAD_CYCLES", 1)

    # First probe sees bwrap (active); second probe (post-reload) finds
    # the binary gone (unavailable) to simulate an uninstall between the
    # daemon's startup probe and a later same-config reload event.
    probe_returns = [SandboxState.ACTIVE, SandboxState.UNAVAILABLE]
    probe_calls = {"n": 0}

    async def _fake_detect(coder_filesystem_isolation: bool) -> SandboxState:
        n = probe_calls["n"]
        probe_calls["n"] += 1
        return probe_returns[min(n, len(probe_returns) - 1)]

    monkeypatch.setattr(
        "src.sandbox.runtime_state.detect_sandbox_state", _fake_detect
    )

    with pytest.raises(_StopLoop):
        asyncio.run(main_module.main())

    history = [
        value
        for key, value in redis_client.set_history
        if key == REDIS_SANDBOX_STATE_KEY
    ]
    assert history == [
        SandboxState.ACTIVE.value,
        SandboxState.UNAVAILABLE.value,
    ], history
    assert redis_client.store[REDIS_SANDBOX_STATE_KEY] == (
        SandboxState.UNAVAILABLE.value
    )
