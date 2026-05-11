from __future__ import annotations

import subprocess
from typing import Any

import pytest

from src.github import prs


def test_get_pr_diff_returns_string_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    diff = "diff --git a/a.py b/a.py\n+print('ok')\n"
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], **kwargs: Any) -> str:
        calls.append(args)
        return diff

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    assert prs.get_pr_diff("owner/repo", 42) == diff
    assert calls == [["pr", "diff", "42", "--repo", "owner/repo"]]


def test_get_pr_diff_returns_empty_string_on_non_string_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *args, **kwargs: None)

    assert prs.get_pr_diff("owner/repo", 42) == ""


def test_get_pr_diff_propagates_subprocess_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_run_gh(args: list[str], **kwargs: Any) -> str:
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", fail_run_gh)

    with pytest.raises(subprocess.CalledProcessError):
        prs.get_pr_diff("owner/repo", 42)
