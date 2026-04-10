"""Tests for src/config.py."""

from __future__ import annotations

from pathlib import Path

from src.config import AppConfig, RepoConfig, load_config


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
