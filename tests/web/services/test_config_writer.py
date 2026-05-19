"""Unit tests for ``src.web.services.config_writer`` (PR-344a).

The service performs ruamel.yaml round-trip writes on ``config.yml`` for
the Spending controls endpoint. The endpoint-level test file exercises
the happy path; this file fills in the defensive branches:

* missing ``daemon:`` section on a write (must be created)
* missing ``daemon:`` section on a reset (no-op early return)
* empty / non-mapping YAML root
* atomic-write cleanup on ``os.replace`` failure
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from src.web.services import config_writer
from src.web.services.config_writer import (
    delete_daemon_fields,
    write_daemon_field,
)


def test_write_creates_daemon_section_when_missing(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories: []\n", encoding="utf-8")
    write_daemon_field(cfg, "spend_ceiling_warning_percent", 65)
    body = cfg.read_text(encoding="utf-8")
    assert "daemon:" in body
    assert "spend_ceiling_warning_percent: 65" in body


def test_write_replaces_existing_value(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "daemon:\n"
        "  spend_ceiling_warning_percent: 50\n",
        encoding="utf-8",
    )
    write_daemon_field(cfg, "spend_ceiling_warning_percent", 65)
    body = cfg.read_text(encoding="utf-8")
    assert "spend_ceiling_warning_percent: 65" in body
    assert "50" not in body.split("spend_ceiling_warning_percent")[1].splitlines()[0]


def test_delete_is_noop_when_daemon_missing(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yml"
    original = "repositories: []\n"
    cfg.write_text(original, encoding="utf-8")
    delete_daemon_fields(cfg, ["spend_ceiling_warning_percent"])
    # File body must remain a valid YAML mapping; the missing ``daemon:``
    # section is the early-return branch.
    assert "repositories" in cfg.read_text(encoding="utf-8")


def test_delete_removes_only_requested_keys(tmp_path: Path) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  spend_ceiling_session_percent: 80\n"
        "  spend_ceiling_warning_percent: 70\n",
        encoding="utf-8",
    )
    delete_daemon_fields(
        cfg,
        ["spend_ceiling_session_percent", "spend_ceiling_warning_percent"],
    )
    body = cfg.read_text(encoding="utf-8")
    assert "poll_interval_sec: 60" in body
    assert "spend_ceiling_session_percent" not in body
    assert "spend_ceiling_warning_percent" not in body


def test_load_empty_file_yields_empty_mapping(tmp_path: Path) -> None:
    """An empty config.yml must round-trip into a fresh ``daemon:`` block."""
    cfg = tmp_path / "config.yml"
    cfg.write_text("", encoding="utf-8")
    write_daemon_field(cfg, "spend_ceiling_warning_percent", 70)
    body = cfg.read_text(encoding="utf-8")
    assert "spend_ceiling_warning_percent: 70" in body


def test_write_creates_config_when_file_missing(tmp_path: Path) -> None:
    """A missing config.yml must be created on first write.

    Mirrors ``src.config.save_config``: a fresh deployment that has never
    persisted any setting (so no ``config.yml`` exists at ``CONFIG_PATH``)
    must still accept a spend-ceiling write without surfacing a 503.
    """
    cfg = tmp_path / "fresh" / "config.yml"
    assert not cfg.exists()
    write_daemon_field(cfg, "spend_ceiling_warning_percent", 65)
    body = cfg.read_text(encoding="utf-8")
    assert "daemon:" in body
    assert "spend_ceiling_warning_percent: 65" in body


def test_delete_is_noop_when_config_missing(tmp_path: Path) -> None:
    """A missing config.yml is a clean no-op for reset.

    Deleting absent keys must not crash and must not create a stub file:
    Pydantic defaults already apply once the file is absent.
    """
    cfg = tmp_path / "config.yml"
    delete_daemon_fields(cfg, ["spend_ceiling_warning_percent"])
    assert not cfg.exists()


def test_load_non_mapping_root_raises_value_error(tmp_path: Path) -> None:
    """A YAML list at the root is a hard error: config.yml must be a mapping."""
    cfg = tmp_path / "config.yml"
    cfg.write_text("- one\n- two\n", encoding="utf-8")
    with pytest.raises(ValueError, match="root must be a mapping"):
        write_daemon_field(cfg, "spend_ceiling_warning_percent", 70)


def test_atomic_write_cleans_up_tmp_file_on_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If ``os.replace`` fails, the tmp file must be removed.

    Mirrors the cleanup contract of ``src.config.save_config``: a crashed
    rename should not leave a ``config.yml.*.tmp`` zombie behind that the
    inotify watcher would see on the next directory event.
    """
    cfg = tmp_path / "config.yml"
    cfg.write_text("daemon:\n  poll_interval_sec: 60\n", encoding="utf-8")

    def boom(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(config_writer.os, "replace", boom)

    with pytest.raises(OSError, match="disk full"):
        write_daemon_field(cfg, "spend_ceiling_warning_percent", 70)

    leftover = [p for p in tmp_path.iterdir() if p.name.startswith("config.yml.")]
    assert leftover == []


def test_atomic_write_swallows_missing_tmp_during_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``FileNotFoundError`` during cleanup must not mask the original error."""
    cfg = tmp_path / "config.yml"
    cfg.write_text("daemon:\n  poll_interval_sec: 60\n", encoding="utf-8")

    def boom_replace(src: str, dst: str) -> None:
        raise OSError("disk full")

    real_unlink = os.unlink

    def fake_unlink(path: str) -> None:
        real_unlink(path)
        raise FileNotFoundError(path)

    monkeypatch.setattr(config_writer.os, "replace", boom_replace)
    monkeypatch.setattr(config_writer.os, "unlink", fake_unlink)

    with pytest.raises(OSError, match="disk full"):
        write_daemon_field(cfg, "spend_ceiling_warning_percent", 70)
