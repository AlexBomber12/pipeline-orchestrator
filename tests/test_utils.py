"""Tests for src/utils.py."""

from __future__ import annotations

from src.utils import repo_name_from_url, repo_slug_from_url


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


def test_repo_slug_from_url_handles_bare_name() -> None:
    assert repo_slug_from_url("myrepo") == "myrepo"


def test_repo_slug_from_url_handles_single_segment_with_colon() -> None:
    assert repo_slug_from_url("git@github.com:myrepo") == "myrepo"


def test_repo_name_from_url_delegates_to_slug() -> None:
    url = "https://github.com/example/omega.git"

    assert repo_name_from_url(url) == repo_slug_from_url(url)
