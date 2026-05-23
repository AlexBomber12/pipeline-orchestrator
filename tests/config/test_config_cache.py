"""Tests for ``load_config`` mtime cache invalidation."""

from __future__ import annotations

import os
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
import src.config as config_module
from src.config import AppConfig, invalidate_config_cache, load_config, save_config
from src.web.services import config_writer


@pytest.fixture(autouse=True)
def clear_config_cache() -> Iterator[None]:
    invalidate_config_cache()
    yield
    invalidate_config_cache()


def _write_config(path: Path, poll_interval: int) -> None:
    path.write_text(
        "daemon:\n"
        f"  poll_interval_sec: {poll_interval}\n",
        encoding="utf-8",
    )


def _bump_mtime(path: Path) -> None:
    new_time = time.time() + 2
    os.utime(path, (new_time, new_time))


def _rewrite_preserving_mtime(path: Path, poll_interval: int) -> None:
    stat = path.stat()
    _write_config(path, poll_interval)
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))


def test_cache_hit_returns_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)
    calls = 0
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    first = load_config(str(cfg_path))
    second = load_config(str(cfg_path))

    assert second == first
    assert second is not first
    assert calls == 1


def test_mutating_returned_config_does_not_change_cached_config(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)

    first = load_config(str(cfg_path))
    first.daemon.poll_interval_sec = 99

    second = load_config(str(cfg_path))

    assert second.daemon.poll_interval_sec == 5


def test_cache_miss_on_mtime_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)
    calls = 0
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 5
    _write_config(cfg_path, 7)
    _bump_mtime(cfg_path)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 7
    assert calls == 2


def test_cache_miss_when_content_changes_with_preserved_mtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)
    calls = 0
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 5
    before = cfg_path.stat()
    _rewrite_preserving_mtime(cfg_path, 7)
    after = cfg_path.stat()

    assert after.st_mtime_ns == before.st_mtime_ns
    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 7
    assert calls == 2


def test_config_file_signature_tracks_same_size_content_change(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)
    before = config_module._config_file_signature(cfg_path)
    _rewrite_preserving_mtime(cfg_path, 7)
    after = config_module._config_file_signature(cfg_path)

    assert after.mtime_ns == before.mtime_ns
    assert after.size == before.size
    assert after.content_hash != before.content_hash


def test_overlay_mtime_change_invalidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    overlay_path = tmp_path / config_module.OVERLAY_FILENAME
    _write_config(cfg_path, 5)
    overlay_path.write_text("daemon:\n  poll_interval_sec: 6\n", encoding="utf-8")
    calls = 0
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 6
    overlay_path.write_text("daemon:\n  poll_interval_sec: 8\n", encoding="utf-8")
    _bump_mtime(overlay_path)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 8
    assert calls == 2


def test_distinct_paths_cached_separately(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_path = tmp_path / "one.yml"
    second_path = tmp_path / "two.yml"
    _write_config(first_path, 5)
    _write_config(second_path, 9)
    calls: list[str] = []
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        calls.append(path)
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    assert load_config(str(first_path)).daemon.poll_interval_sec == 5
    assert load_config(str(second_path)).daemon.poll_interval_sec == 9
    assert load_config(str(first_path)).daemon.poll_interval_sec == 5
    assert load_config(str(second_path)).daemon.poll_interval_sec == 9
    assert calls == [str(first_path.resolve()), str(second_path.resolve())]


def test_explicit_invalidate_clears_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)
    calls = 0
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    load_config(str(cfg_path))
    invalidate_config_cache()
    load_config(str(cfg_path))

    assert calls == 2


def test_missing_overlay_treated_as_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    overlay_path = tmp_path / config_module.OVERLAY_FILENAME
    _write_config(cfg_path, 5)
    calls = 0
    real_load_raw = config_module._load_config_raw

    def counted_load_raw(path: str = "config.yml") -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return real_load_raw(path)

    monkeypatch.setattr(config_module, "_load_config_raw", counted_load_raw)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 5
    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 5
    overlay_path.write_text("daemon:\n  poll_interval_sec: 11\n", encoding="utf-8")
    _bump_mtime(overlay_path)

    assert load_config(str(cfg_path)).daemon.poll_interval_sec == 11
    assert calls == 2


def test_non_directory_config_parent_treated_as_missing(tmp_path: Path) -> None:
    parent_file = tmp_path / "not-a-directory"
    parent_file.write_text("not a directory", encoding="utf-8")
    cfg_path = parent_file / "config.yml"

    cfg = load_config(str(cfg_path))

    assert cfg == AppConfig()


def test_directory_config_path_treated_as_missing(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config-dir"
    cfg_path.mkdir()

    cfg = load_config(str(cfg_path))

    assert cfg == AppConfig()


def test_explicit_symlink_path_uses_overlay_next_to_link(tmp_path: Path) -> None:
    real_dir = tmp_path / "real"
    link_dir = tmp_path / "link"
    real_dir.mkdir()
    link_dir.mkdir()
    real_config = real_dir / "config.yml"
    explicit_config = link_dir / "config.yml"
    _write_config(real_config, 5)
    explicit_config.symlink_to(real_config)
    (real_dir / config_module.OVERLAY_FILENAME).write_text(
        "daemon:\n  poll_interval_sec: 6\n",
        encoding="utf-8",
    )
    (link_dir / config_module.OVERLAY_FILENAME).write_text(
        "daemon:\n  poll_interval_sec: 9\n",
        encoding="utf-8",
    )

    cfg = load_config(str(explicit_config))

    assert cfg.daemon.poll_interval_sec == 9


def test_config_writer_invalidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    calls = 0

    def counted_invalidate() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(config_writer, "invalidate_config_cache", counted_invalidate)

    config_writer.write_daemon_field(cfg_path, "poll_interval_sec", 12)

    assert calls == 1


def test_save_config_invalidates_after_successful_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    calls = 0

    def counted_invalidate() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(config_module, "invalidate_config_cache", counted_invalidate)

    save_config(AppConfig(), str(cfg_path))

    assert calls == 1


def test_concurrent_miss_single_parse(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    _write_config(cfg_path, 5)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(load_config, str(cfg_path))
        second = executor.submit(load_config, str(cfg_path))
        first_config = first.result()
        second_config = second.result()

    assert first_config == second_config
    assert load_config(str(cfg_path)) == first_config
