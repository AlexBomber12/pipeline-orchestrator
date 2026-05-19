"""Tests for the inotify-based ``watch_config_changes`` watcher (PR-342)."""

from __future__ import annotations

import asyncio
import sys
import types
from pathlib import Path
from typing import Any

import pytest
from src.daemon import config_watcher


def _write(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")


async def _wait_until(condition, timeout: float = 3.0) -> bool:
    """Spin on ``condition`` until True or until ``timeout`` elapses."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return True
        await asyncio.sleep(0.02)
    return condition()


async def _cancel(task: asyncio.Task[Any]) -> None:
    if task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_watcher_triggers_callback_on_file_modified(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    fired: list[int] = []

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_changes(
                [target], lambda: fired.append(1)
            )
        )
        # Give the watcher time to subscribe to the kernel before we mutate.
        await asyncio.sleep(0.2)
        _write(target, "x: 2\n")
        await _wait_until(lambda: bool(fired))
        await _cancel(task)

    asyncio.run(driver())
    assert fired, "callback was not fired within 2s of file modification"


def test_watcher_handles_missing_path_gracefully(tmp_path: Path) -> None:
    missing = tmp_path / "nope.yml"
    fired: list[int] = []

    async def driver() -> None:
        # No paths exist -> the function must return immediately, not block.
        await asyncio.wait_for(
            config_watcher.watch_config_changes(
                [missing], lambda: fired.append(1)
            ),
            timeout=0.5,
        )

    asyncio.run(driver())
    assert fired == []


def test_watcher_falls_back_when_watchfiles_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    fired: list[int] = []
    # ``sys.modules[name] = None`` makes ``from name import ...`` raise
    # ImportError on the next import, simulating a platform/dep where the
    # inotify library is unavailable.
    monkeypatch.setitem(sys.modules, "watchfiles", None)

    async def driver() -> None:
        with caplog.at_level("INFO", logger=config_watcher.logger.name):
            await asyncio.wait_for(
                config_watcher.watch_config_changes(
                    [target], lambda: fired.append(1)
                ),
                timeout=0.5,
            )

    asyncio.run(driver())
    assert fired == []
    assert any(
        "watchfiles unavailable" in record.getMessage()
        for record in caplog.records
    )


def test_watcher_ignores_irrelevant_changes(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    unrelated = tmp_path / "other.txt"
    _write(target, "x: 1\n")
    _write(unrelated, "hello\n")
    fired: list[int] = []

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_changes(
                [target], lambda: fired.append(1)
            )
        )
        await asyncio.sleep(0.2)
        # Mutate an unrelated path; the watcher must NOT fire.
        _write(unrelated, "world\n")
        await asyncio.sleep(0.5)
        await _cancel(task)

    asyncio.run(driver())
    assert fired == []


def test_watcher_multiple_paths_each_triggers(tmp_path: Path) -> None:
    primary = tmp_path / "config.yml"
    secondary = tmp_path / "providers.yml"
    _write(primary, "x: 1\n")
    _write(secondary, "y: 1\n")
    fired: list[str] = []

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_changes(
                [primary, secondary], lambda: fired.append("hit")
            )
        )
        await asyncio.sleep(0.2)
        _write(primary, "x: 2\n")
        await _wait_until(lambda: len(fired) >= 1)
        _write(secondary, "y: 2\n")
        await _wait_until(lambda: len(fired) >= 2)
        await _cancel(task)

    asyncio.run(driver())
    assert len(fired) >= 2


def test_callback_exception_does_not_crash_watcher(tmp_path: Path) -> None:
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    fired: list[int] = []

    def callback() -> None:
        fired.append(1)
        if len(fired) == 1:
            raise RuntimeError("simulated reload failure")

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_changes([target], callback)
        )
        await asyncio.sleep(0.2)
        _write(target, "x: 2\n")
        await _wait_until(lambda: len(fired) >= 1)
        # The first invocation raised; the watcher must still be alive
        # and re-fire on the next event.
        assert not task.done()
        _write(target, "x: 3\n")
        await _wait_until(lambda: len(fired) >= 2)
        await _cancel(task)

    asyncio.run(driver())
    assert len(fired) >= 2


def test_main_loop_picks_up_change_at_next_idle_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inotify event must trigger a config reload on the next iteration.

    The actual reload-during-CODING deferral is enforced by the existing
    ``_sync_runners`` staging path; this test verifies that the inotify
    branch in the main loop fires the same reload code (``load_config`` +
    ``_sync_runners``) without resetting the runner's poll cadence
    mid-cycle, so the existing IDLE-boundary deferral still applies.
    """
    from src.config import AppConfig, DaemonConfig, RepoConfig
    from src.daemon import main as main_module
    from tests import test_daemon_main as harness

    initial_config = AppConfig(
        repositories=[
            RepoConfig(
                url="https://github.com/octo/alpha.git",
                poll_interval_sec=1,
            )
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    edited_config = AppConfig(
        repositories=[
            RepoConfig(
                url="https://github.com/octo/alpha.git",
                poll_interval_sec=2,
            )
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    load_results = [initial_config, edited_config]

    def fake_load_config() -> AppConfig:
        if len(load_results) > 1:
            return load_results.pop(0)
        return load_results[0]

    harness._reset_fake_runner()
    monkeypatch.setattr(main_module, "load_config", fake_load_config)
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

    async def _noop_poll_watcher(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        main_module, "watch_config_file_changes", _noop_poll_watcher
    )

    captured: dict[str, Any] = {}

    async def fake_inotify_watcher(
        config_paths: list[Path],
        on_change_callback: Any,
    ) -> None:
        # Capture the wiring so the test can assert on it, then fire the
        # callback once and idle until cancelled.
        captured["paths"] = list(config_paths)
        captured["callback"] = on_change_callback
        on_change_callback()
        await asyncio.Event().wait()

    monkeypatch.setattr(
        main_module, "watch_config_changes", fake_inotify_watcher
    )

    sync_calls: list[AppConfig] = []
    original_sync = main_module._sync_runners

    def counted_sync(runners: Any, cfg: AppConfig, *args: Any, **kwargs: Any) -> None:
        sync_calls.append(cfg)
        return original_sync(runners, cfg, *args, **kwargs)

    monkeypatch.setattr(main_module, "_sync_runners", counted_sync)

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []
    _real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        await _real_sleep(0)
        # Two ticks: first runs the initial cycle (also lets the inotify
        # fake schedule the callback), second exits.
        if len(sleep_calls) >= 2:
            raise harness._StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(harness._StopLoop):
        asyncio.run(main_module.main())

    # The initial setup call plus at least one reload-triggered call.
    assert len(sync_calls) >= 2, sync_calls
    # The reload picked up the edited config rather than re-running with
    # the initial one: this is the inotify-driven hot reload at work.
    assert sync_calls[-1] is edited_config
    assert captured.get("callback") is not None
    # Paths come straight from the resolver; first entry is the canonical
    # config.yml path.
    assert captured["paths"][0] == config_watcher._resolve_config_path()


def test_main_loop_pollwatcher_paths_include_providers_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``config/providers.yml`` is added to the watch set when it exists."""
    from src.config import AppConfig, DaemonConfig, RepoConfig
    from src.daemon import main as main_module
    from tests import test_daemon_main as harness

    config = AppConfig(
        repositories=[
            RepoConfig(
                url="https://github.com/octo/alpha.git",
                poll_interval_sec=1,
            )
        ],
        daemon=DaemonConfig(poll_interval_sec=1),
    )
    harness._reset_fake_runner()
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "providers.yml").write_text("p: 1\n")

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

    async def _noop_poll_watcher(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        main_module, "watch_config_file_changes", _noop_poll_watcher
    )

    captured: dict[str, Any] = {}

    async def fake_inotify_watcher(
        config_paths: list[Path],
        on_change_callback: Any,
    ) -> None:
        captured["paths"] = list(config_paths)
        await asyncio.Event().wait()

    monkeypatch.setattr(
        main_module, "watch_config_changes", fake_inotify_watcher
    )

    clock = [0.0]
    monkeypatch.setattr(main_module.time, "monotonic", lambda: clock[0])

    sleep_calls: list[float] = []
    _real_sleep = asyncio.sleep

    async def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        clock[0] += seconds + 1
        await _real_sleep(0)
        if len(sleep_calls) >= 1:
            raise harness._StopLoop

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    with pytest.raises(harness._StopLoop):
        asyncio.run(main_module.main())

    assert captured["paths"] == [
        config_watcher._resolve_config_path(),
        Path("config/providers.yml"),
    ]


def test_watcher_returns_when_paths_list_is_empty() -> None:
    """``[]`` (no paths configured) must return immediately."""
    fired: list[int] = []

    async def driver() -> None:
        await asyncio.wait_for(
            config_watcher.watch_config_changes(
                [], lambda: fired.append(1)
            ),
            timeout=0.5,
        )

    asyncio.run(driver())
    assert fired == []


def test_watcher_skips_deleted_change_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``Change.deleted`` event must not fire the reload callback."""
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    fired: list[int] = []

    from watchfiles import Change

    batches = [
        {(Change.deleted, str(target))},
        {(Change.modified, str(target))},
    ]

    async def fake_awatch(*paths: Any, **kwargs: Any):
        for batch in batches:
            yield batch

    fake_module = types.SimpleNamespace(Change=Change, awatch=fake_awatch)
    monkeypatch.setitem(sys.modules, "watchfiles", fake_module)

    async def driver() -> None:
        await config_watcher.watch_config_changes(
            [target], lambda: fired.append(1)
        )

    asyncio.run(driver())
    # The deleted batch was skipped; the modified batch fired exactly once.
    assert fired == [1]


def test_watcher_survives_atomic_save_via_rename(tmp_path: Path) -> None:
    """Replacing ``config.yml`` via rename must not deafen the watcher.

    Many editors and config writers save atomically: write a sibling
    tmp file, then rename it over the target. That unlinks the
    original inode the watcher was tied to. A file-level inotify watch
    stops reporting subsequent edits once the inode it bound to is
    gone, while a parent-directory watch keeps reporting later edits
    on the same target path.
    """
    target = tmp_path / "config.yml"
    _write(target, "x: 1\n")
    fired: list[int] = []

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_changes(
                [target], lambda: fired.append(1)
            )
        )
        # Give the watcher time to subscribe to the parent dir before
        # the first rename.
        await asyncio.sleep(0.2)
        first_tmp = tmp_path / "config.yml.new1"
        _write(first_tmp, "x: 2\n")
        first_tmp.replace(target)
        await _wait_until(lambda: len(fired) >= 1)
        # Second atomic save against the new inode — the prior code
        # path that watched the file directly went deaf here because
        # the original inode was already unlinked by the first rename.
        second_tmp = tmp_path / "config.yml.new2"
        _write(second_tmp, "x: 3\n")
        second_tmp.replace(target)
        await _wait_until(lambda: len(fired) >= 2)
        await _cancel(task)

    asyncio.run(driver())
    assert len(fired) >= 2, fired


def test_watcher_filters_unrelated_siblings_in_same_directory(
    tmp_path: Path,
) -> None:
    """Sibling files in the watched parent dir must not fire the callback.

    Switching to parent-directory watching means the watcher sees events
    for every file in the directory; the filter must drop everything
    that does not normalise to one of the configured target paths.
    """
    target = tmp_path / "config.yml"
    sibling = tmp_path / "unrelated.txt"
    _write(target, "x: 1\n")
    _write(sibling, "hello\n")
    fired: list[int] = []

    async def driver() -> None:
        task = asyncio.create_task(
            config_watcher.watch_config_changes(
                [target], lambda: fired.append(1)
            )
        )
        await asyncio.sleep(0.2)
        _write(sibling, "world\n")
        # Give plenty of time for any spurious event to propagate.
        await asyncio.sleep(0.5)
        await _cancel(task)

    asyncio.run(driver())
    assert fired == []


def test_watcher_normalize_path_handles_resolve_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_normalize_path`` falls back to the unresolved string on OSError.

    The filter is invoked on every inotify event; a transient resolve
    failure (path vanished between event delivery and lookup) must not
    crash the watcher loop.
    """

    def boom(self: Path, *args: Any, **kwargs: Any) -> Path:
        raise OSError("simulated resolve failure")

    monkeypatch.setattr(Path, "resolve", boom)
    assert config_watcher._normalize_path("/tmp/example.yml") == "/tmp/example.yml"
