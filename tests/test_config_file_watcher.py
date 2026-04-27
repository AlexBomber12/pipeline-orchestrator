"""Tests for src/daemon/config_watcher.py."""

from __future__ import annotations

import asyncio
import hashlib
import os
import time
from pathlib import Path
from typing import Any

import pytest
from src.daemon import config_watcher


class _FakeRedis:
    """Minimal in-memory async Redis client for the watcher tests."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []

    async def set(self, key: str, value: str) -> None:
        self.calls.append((key, value))
        self.store[key] = value


class _BrokenSetRedis(_FakeRedis):
    async def set(self, key: str, value: str) -> None:
        self.calls.append((key, value))
        raise RuntimeError("redis offline")


def _write(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


def test_resolve_config_path_uses_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PO_CONFIG_PATH", "/tmp/custom-config.yml")
    assert config_watcher._resolve_config_path() == Path("/tmp/custom-config.yml")


def test_resolve_config_path_defaults_when_env_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PO_CONFIG_PATH", raising=False)
    assert config_watcher._resolve_config_path() == Path("config.yml")


def test_compute_sha256_matches_hashlib(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    body = "daemon: {poll_interval_sec: 5}\n"
    _write(target, body)
    assert (
        config_watcher._compute_sha256(target)
        == hashlib.sha256(body.encode("utf-8")).hexdigest()
    )


def test_config_signature_uses_sha256_only(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "value: 1\n")
    assert (
        config_watcher._config_signature(target)
        == hashlib.sha256(target.read_bytes()).hexdigest()
    )


def test_config_signature_unchanged_when_only_mtime_moves(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "value: 1\n")
    before = config_watcher._config_signature(target)
    future_ts = time.time() + 5
    os.utime(target, (future_ts, future_ts))
    assert config_watcher._config_signature(target) == before


def test_safe_signature_returns_none_for_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "absent.yml"
    assert config_watcher._safe_signature(missing) is None


def test_safe_signature_returns_none_on_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")

    def raise_oserror(path: Path) -> str:
        raise OSError("permission denied")

    monkeypatch.setattr(config_watcher, "_config_signature", raise_oserror)
    assert config_watcher._safe_signature(target) is None


def test_set_config_dirty_flags_writes_each_repo(
    tmp_path: Path,
) -> None:
    redis = _FakeRedis()
    asyncio.run(
        config_watcher._set_config_dirty_flags(redis, ["alpha", "beta"])
    )
    assert redis.store == {
        "control:alpha:config_dirty": "1",
        "control:beta:config_dirty": "1",
    }


def test_set_config_dirty_flags_logs_and_continues_on_redis_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    redis = _BrokenSetRedis()
    with caplog.at_level("WARNING", logger=config_watcher.logger.name):
        asyncio.run(
            config_watcher._set_config_dirty_flags(redis, ["alpha", "beta"])
        )
    assert [name for name, _ in redis.calls] == [
        "control:alpha:config_dirty",
        "control:beta:config_dirty",
    ]
    assert any(
        "control:alpha:config_dirty" in record.getMessage()
        for record in caplog.records
    )


def test_watch_returns_immediately_when_config_missing(tmp_path: Path) -> None:
    redis = _FakeRedis()
    missing = tmp_path / "no-config.yml"
    asyncio.run(
        config_watcher.watch_config_file_changes(
            redis,
            get_repo_names=lambda: ["alpha"],
            config_path=missing,
            interval_sec=0.001,
        )
    )
    assert redis.store == {}


def test_watch_returns_immediately_when_initial_signature_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    redis = _FakeRedis()

    def always_oserror(path: Path) -> str:
        raise OSError("boom")

    monkeypatch.setattr(config_watcher, "_config_signature", always_oserror)

    asyncio.run(
        config_watcher.watch_config_file_changes(
            redis,
            get_repo_names=lambda: ["alpha"],
            config_path=target,
            interval_sec=0.001,
        )
    )
    assert redis.store == {}


def test_watch_detects_content_change_and_flags_runners(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "planned_pr_timeout_sec: 900\n")
    redis = _FakeRedis()

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_file_changes(
                redis,
                get_repo_names=lambda: ["alpha", "beta"],
                config_path=target,
                interval_sec=0.01,
            )
        )
        await asyncio.sleep(0.03)
        # Bump mtime forward so the signature change is detectable even on
        # filesystems with coarse mtime resolution.
        future_ts = time.time() + 1
        os.utime(target, (future_ts, future_ts))
        _write(target, "planned_pr_timeout_sec: 2400\n")
        os.utime(target, (future_ts + 1, future_ts + 1))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if redis.store:
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert redis.store == {
        "control:alpha:config_dirty": "1",
        "control:beta:config_dirty": "1",
    }


def test_watch_no_op_touch_does_not_trigger(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    body = "planned_pr_timeout_sec: 900\n"
    _write(target, body)
    redis = _FakeRedis()

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_file_changes(
                redis,
                get_repo_names=lambda: ["alpha"],
                config_path=target,
                interval_sec=0.01,
            )
        )
        await asyncio.sleep(0.02)
        # Push mtime forward without touching content; sha256 stays the same.
        future_ts = time.time() + 5
        os.utime(target, (future_ts, future_ts))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert redis.store == {}


def test_watch_skips_when_repo_list_is_empty(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    redis = _FakeRedis()

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_file_changes(
                redis,
                get_repo_names=lambda: [],
                config_path=target,
                interval_sec=0.01,
            )
        )
        await asyncio.sleep(0.02)
        future_ts = time.time() + 1
        os.utime(target, (future_ts, future_ts))
        _write(target, "x: 2\n")
        os.utime(target, (future_ts + 1, future_ts + 1))
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert redis.store == {}


def test_watch_survives_file_deletion_and_resumes(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    redis = _FakeRedis()

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_file_changes(
                redis,
                get_repo_names=lambda: ["alpha"],
                config_path=target,
                interval_sec=0.01,
            )
        )
        await asyncio.sleep(0.02)
        target.unlink()
        await asyncio.sleep(0.03)
        # Watcher must still be running and able to detect re-creation.
        assert not task.done()
        future_ts = time.time() + 1
        _write(target, "x: 99\n")
        os.utime(target, (future_ts, future_ts))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if redis.store:
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert redis.store == {"control:alpha:config_dirty": "1"}


def test_watch_handles_transient_unreadable_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    redis = _FakeRedis()
    real_signature = config_watcher._config_signature
    flaky_calls = {"n": 0}

    def flaky_signature(path: Path) -> str:
        flaky_calls["n"] += 1
        if flaky_calls["n"] == 2:
            raise OSError("transient")
        return real_signature(path)

    monkeypatch.setattr(config_watcher, "_config_signature", flaky_signature)

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_file_changes(
                redis,
                get_repo_names=lambda: ["alpha"],
                config_path=target,
                interval_sec=0.01,
            )
        )
        await asyncio.sleep(0.05)
        future_ts = time.time() + 1
        _write(target, "x: 2\n")
        os.utime(target, (future_ts, future_ts))
        for _ in range(50):
            await asyncio.sleep(0.02)
            if redis.store:
                break
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(driver())
    assert redis.store == {"control:alpha:config_dirty": "1"}


def test_main_loop_spawns_watcher_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """The daemon ``main()`` must schedule the config watcher coroutine."""
    from src.config import AppConfig, DaemonConfig, RepoConfig
    from src.daemon import main as main_module
    from src.models import PipelineState
    from tests import test_daemon_main as harness

    config = AppConfig(
        repositories=[
            RepoConfig(
                url="https://github.com/octo/alpha.git", poll_interval_sec=1
            )
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )

    harness._reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", lambda: config)
    monkeypatch.setattr(
        main_module.aioredis,
        "from_url",
        lambda url, decode_responses: harness._FakeRedisClient(),
    )
    monkeypatch.setattr(main_module, "PipelineRunner", harness._FakeRunner)
    monkeypatch.setattr(main_module, "_setup_git_auth", lambda: None)
    monkeypatch.setattr(
        main_module, "_validate_auth", lambda: {"claude": True, "gh": True}
    )

    captured: dict[str, Any] = {}

    def fake_watcher(
        redis_client: Any,
        get_repo_names: Any,
        *,
        config_path: Any = None,
        interval_sec: float = 5.0,
    ) -> Any:
        captured["called"] = True
        captured["names"] = list(get_repo_names())
        captured["redis"] = redis_client

        async def _noop() -> None:
            return None

        return _noop()

    monkeypatch.setattr(main_module, "watch_config_file_changes", fake_watcher)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        if len(sleep_calls) >= 1:
            raise harness._StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(harness._StopLoop):
        asyncio.run(main_module.main())

    assert captured.get("called") is True
    assert captured.get("names") == ["octo__alpha"]
    # Ensure the runner was driven through at least one cycle so the
    # watcher really did start alongside the main loop, not in place of it.
    assert any(
        runner.cycles >= 1 for runner in harness._FakeRunner.instances
    )
    # Sanity: the runner state is wired up so the main loop did execute.
    assert harness._FakeRunner.instances[0].state.state == PipelineState.IDLE
