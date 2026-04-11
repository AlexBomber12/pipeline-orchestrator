"""Tests for src/config.py."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.config import (
    AppConfig,
    RepoConfig,
    add_repository,
    load_config,
    normalize_repo_url,
    remove_repository,
    save_config,
    update_daemon_config,
    update_repository,
)


def test_load_config_missing_file_returns_defaults(tmp_path: Path) -> None:
    cfg = load_config(str(tmp_path / "does-not-exist.yml"))

    assert isinstance(cfg, AppConfig)
    assert cfg.repositories == []
    assert cfg.daemon.poll_interval_sec == 60
    assert cfg.daemon.review_timeout_min == 60
    assert cfg.daemon.hung_fallback_codex_review is True
    assert cfg.daemon.error_handler_use_ai is True
    assert cfg.web.host == "0.0.0.0"
    assert cfg.web.port == 8000
    assert cfg.auth.claude_config_dir == "/data/auth/claude"
    assert cfg.auth.gh_config_dir == "/data/auth/gh"


def test_load_config_valid_yaml(tmp_path: Path) -> None:
    yaml_text = """
repositories:
  - url: https://github.com/example/repo.git
    branch: develop
    auto_merge: false
    review_timeout_min: 30
    poll_interval_sec: 15

daemon:
  poll_interval_sec: 90
  review_timeout_min: 45
  hung_fallback_codex_review: false
  error_handler_use_ai: false

web:
  host: 127.0.0.1
  port: 9000

auth:
  claude_config_dir: /tmp/claude
  gh_config_dir: /tmp/gh
"""
    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml_text, encoding="utf-8")

    cfg = load_config(str(config_file))

    assert len(cfg.repositories) == 1
    repo = cfg.repositories[0]
    assert repo.url == "https://github.com/example/repo.git"
    assert repo.branch == "develop"
    assert repo.auto_merge is False
    assert repo.review_timeout_min == 30
    assert repo.poll_interval_sec == 15

    assert cfg.daemon.poll_interval_sec == 90
    assert cfg.daemon.review_timeout_min == 45
    assert cfg.daemon.hung_fallback_codex_review is False
    assert cfg.daemon.error_handler_use_ai is False

    assert cfg.web.host == "127.0.0.1"
    assert cfg.web.port == 9000

    assert cfg.auth.claude_config_dir == "/tmp/claude"
    assert cfg.auth.gh_config_dir == "/tmp/gh"


def test_repo_config_defaults() -> None:
    repo = RepoConfig(url="https://github.com/example/repo.git")

    assert repo.url == "https://github.com/example/repo.git"
    assert repo.branch == "main"
    assert repo.auto_merge is True
    assert repo.review_timeout_min == 60
    assert repo.poll_interval_sec == 60


def test_normalize_repo_url_strips_git_and_slash() -> None:
    assert (
        normalize_repo_url("https://github.com/o/r.git")
        == "https://github.com/o/r"
    )
    assert (
        normalize_repo_url("https://github.com/o/r/")
        == "https://github.com/o/r"
    )
    assert (
        normalize_repo_url("https://github.com/o/r.git/")
        == "https://github.com/o/r"
    )
    assert (
        normalize_repo_url("  https://github.com/o/r  ")
        == "https://github.com/o/r"
    )


def test_save_config_round_trip(tmp_path: Path) -> None:
    config = AppConfig(
        repositories=[
            RepoConfig(
                url="https://github.com/octo/alpha.git",
                branch="dev",
                auto_merge=False,
                review_timeout_min=30,
                poll_interval_sec=15,
            ),
        ],
    )
    config.daemon.poll_interval_sec = 90
    config.daemon.error_handler_use_ai = False
    config.web.port = 9000
    config.auth.claude_config_dir = "/tmp/claude"

    path = tmp_path / "config.yml"
    save_config(config, str(path))

    assert path.is_file()
    loaded = load_config(str(path))
    assert loaded.model_dump() == config.model_dump()


def test_save_config_atomic_overwrites_existing(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    path.write_text("repositories: []\n", encoding="utf-8")

    new_cfg = AppConfig(
        repositories=[RepoConfig(url="https://github.com/o/r.git")]
    )
    save_config(new_cfg, str(path))

    reloaded = load_config(str(path))
    assert len(reloaded.repositories) == 1
    assert reloaded.repositories[0].url == "https://github.com/o/r.git"
    # No leftover tmp files next to the target.
    siblings = [p.name for p in tmp_path.iterdir()]
    assert siblings == ["config.yml"], siblings


def test_add_repository_appends_and_persists(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = add_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        branch="dev",
        auto_merge=False,
    )
    assert len(cfg.repositories) == 1
    assert cfg.repositories[0].branch == "dev"
    assert cfg.repositories[0].auto_merge is False

    reloaded = load_config(str(path))
    assert len(reloaded.repositories) == 1
    assert reloaded.repositories[0].url == "https://github.com/octo/alpha.git"


def test_add_repository_rejects_duplicate_normalized_url(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))

    with pytest.raises(ValueError, match="already configured"):
        add_repository("https://github.com/octo/alpha", str(path))
    with pytest.raises(ValueError, match="already configured"):
        add_repository("https://github.com/octo/alpha/", str(path))


def test_add_repository_rejects_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Unknown repository fields"):
        add_repository(
            "https://github.com/o/r.git", str(path), nonsense=True
        )


def test_remove_repository_removes_and_persists(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))
    add_repository("https://github.com/octo/beta.git", str(path))

    cfg = remove_repository("https://github.com/octo/alpha/", str(path))

    urls = [r.url for r in cfg.repositories]
    assert urls == ["https://github.com/octo/beta.git"]
    reloaded = load_config(str(path))
    assert [r.url for r in reloaded.repositories] == [
        "https://github.com/octo/beta.git"
    ]


def test_remove_repository_raises_on_missing(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Repository not found"):
        remove_repository("https://github.com/octo/missing.git", str(path))


def test_update_repository_updates_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        branch="main",
        auto_merge=True,
    )

    cfg = update_repository(
        "https://github.com/octo/alpha",
        str(path),
        branch="release",
        review_timeout_min=15,
    )
    repo = cfg.repositories[0]
    assert repo.branch == "release"
    assert repo.review_timeout_min == 15
    assert repo.auto_merge is True  # untouched
    assert repo.url == "https://github.com/octo/alpha.git"  # url untouched


def test_update_repository_raises_on_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))

    with pytest.raises(ValueError, match="Unknown repository fields"):
        update_repository(
            "https://github.com/octo/alpha.git", str(path), bogus=1
        )


def test_update_daemon_config_updates_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = update_daemon_config(
        str(path),
        poll_interval_sec=120,
        error_handler_use_ai=False,
    )
    assert cfg.daemon.poll_interval_sec == 120
    assert cfg.daemon.error_handler_use_ai is False
    # Unchanged fields keep their previous values.
    assert cfg.daemon.review_timeout_min == 60
    assert cfg.daemon.hung_fallback_codex_review is True

    reloaded = load_config(str(path))
    assert reloaded.daemon.poll_interval_sec == 120
    assert reloaded.daemon.error_handler_use_ai is False


def test_update_daemon_config_rejects_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Unknown daemon fields"):
        update_daemon_config(str(path), bogus=True)
