from __future__ import annotations

import subprocess

import pytest
from src.github import prs as gh_prs


def test_get_pr_state_returns_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", lambda *args, **kwargs: "OPEN\n")

    assert gh_prs.get_pr_state("owner/name", 42) == "OPEN"


def test_get_pr_state_returns_merged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", lambda *args, **kwargs: "MERGED\n")

    assert gh_prs.get_pr_state("owner/name", 42) == "MERGED"


def test_get_pr_state_returns_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", lambda *args, **kwargs: "CLOSED\n")

    assert gh_prs.get_pr_state("owner/name", 42) == "CLOSED"


def test_get_pr_state_returns_none_on_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_called_process_error(*args: object, **kwargs: object) -> str:
        raise subprocess.CalledProcessError(returncode=1, cmd=["gh"])

    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", raise_called_process_error)

    assert gh_prs.get_pr_state("owner/name", 42) is None


def test_get_pr_state_strips_whitespace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", lambda *args, **kwargs: "OPEN\n\n")

    assert gh_prs.get_pr_state("owner/name", 42) == "OPEN"
