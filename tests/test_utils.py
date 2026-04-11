"""Tests for src/utils.py."""

from __future__ import annotations

from src.utils import repo_name_from_url


def test_repo_name_from_url_strips_git_suffix() -> None:
    assert repo_name_from_url("https://github.com/example/alpha.git") == "alpha"


def test_repo_name_from_url_without_git_suffix() -> None:
    assert repo_name_from_url("https://github.com/example/beta") == "beta"


def test_repo_name_from_url_trailing_slash() -> None:
    assert repo_name_from_url("https://github.com/example/gamma/") == "gamma"


def test_repo_name_from_url_ssh_url() -> None:
    assert repo_name_from_url("git@github.com:example/delta.git") == "delta"
