"""Tests for src/daemon/main.py."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import main as main_module


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
    ) -> None:
        self.repo_config = repo_config
        self.app_config = app_config
        self.redis_client = redis_client
        self.name = repo_config.url.rsplit("/", 1)[-1].removesuffix(".git")
        self.cycles = 0
        _FakeRunner.instances.append(self)

    async def run_cycle(self) -> None:
        self.cycles += 1


class _StopLoop(Exception):
    """Sentinel raised by the patched ``asyncio.sleep`` to end ``main``."""


def _reset_fake_runner() -> None:
    _FakeRunner.instances = []


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

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        if len(sleep_calls) >= sleep_iterations:
            raise _StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)
    return {"sleep_calls": sleep_calls}


def _repo(url: str) -> RepoConfig:
    return RepoConfig(url=url)


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
    assert names == ["alpha", "beta"]
    assert all(r.cycles == 1 for r in _FakeRunner.instances)
    assert ctx["sleep_calls"] == [7]


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
        ) -> None:
            if "broken" in repo_config.url:
                raise ValueError(f"Not a recognizable GitHub URL: {repo_config.url!r}")
            super().__init__(repo_config, app_config, redis_client)

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
    assert _FakeRunner.instances[0].name == "beta"
    assert _FakeRunner.instances[0].cycles == 1
    errors = [rec for rec in caplog.records if rec.levelno == logging.ERROR]
    assert any(
        "Failed to initialize runner" in rec.getMessage() and "broken" in rec.getMessage()
        for rec in errors
    )


def test_main_continues_when_one_runner_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _FailingFirstRunner(_FakeRunner):
        async def run_cycle(self) -> None:
            self.cycles += 1
            if self.name == "alpha":
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
    assert any("alpha" in rec.getMessage() for rec in errors)
