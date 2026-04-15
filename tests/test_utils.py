"""Tests for src/utils.py."""

from __future__ import annotations

from src.utils import repo_slug_from_url


def test_repo_slug_from_url_github() -> None:
    assert repo_slug_from_url("https://github.com/owner/repo.git") == "owner__repo"


def test_repo_slug_from_url_no_collision() -> None:
    assert repo_slug_from_url("https://github.com/owner-a/api") == "owner-a__api"
    assert repo_slug_from_url("https://github.com/owner-b/api") == "owner-b__api"
    assert repo_slug_from_url("https://github.com/owner-a/api") != repo_slug_from_url(
        "https://github.com/owner-b/api"
    )


def test_repo_slug_from_url_strips_git() -> None:
    assert repo_slug_from_url("https://github.com/example/alpha.git") == "example__alpha"


def test_repo_slug_from_url_strips_slash() -> None:
    assert repo_slug_from_url("https://github.com/example/gamma/") == "example__gamma"


def test_repo_slug_from_url_ssh() -> None:
    assert repo_slug_from_url("git@github.com:example/delta.git") == "example__delta"


def test_repo_slug_from_url_without_git_suffix() -> None:
    assert repo_slug_from_url("https://github.com/example/beta") == "example__beta"
