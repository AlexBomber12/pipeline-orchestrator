from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from src.github.prs import get_pr_last_push_time


def test_get_pr_last_push_time_uses_activity_query_string(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        if "/pulls/" in cmd[2]:
            stdout = '{"branch":"feature/fix&one#two","repo":"fork-owner/fork-repo"}\n'
        elif "activity_type=push" in cmd[2]:
            stdout = "2026-05-14T10:20:30Z\n"
        else:
            stdout = "\n"
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fake_run)

    get_pr_last_push_time("owner/name", 42)

    assert calls == [
        [
            "gh",
            "api",
            "repos/owner/name/pulls/42",
            "--jq",
            '{branch: .head.ref, repo: .head.repo.full_name}',
        ],
        [
            "gh",
            "api",
            (
                "repos/fork-owner/fork-repo/activity?"
                "ref=refs%2Fheads%2Ffeature%2Ffix%26one%23two"
                "&activity_type=push&per_page=1&direction=desc"
            ),
            "--jq",
            ".[0].timestamp",
        ],
        [
            "gh",
            "api",
            (
                "repos/fork-owner/fork-repo/activity?"
                "ref=refs%2Fheads%2Ffeature%2Ffix%26one%23two"
                "&activity_type=force_push&per_page=1&direction=desc"
            ),
            "--jq",
            ".[0].timestamp",
        ],
    ]


def test_get_pr_last_push_time_returns_parsed_datetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-fix\n", "2026-05-14T10:20:30Z\n", "\n"])

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=0, stdout=next(responses), stderr="")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fake_run)

    assert get_pr_last_push_time("owner/name", 42) == datetime(
        2026,
        5,
        14,
        10,
        20,
        30,
        tzinfo=timezone.utc,
    )


def test_get_pr_last_push_time_chooses_newer_force_push_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(
        [
            "feature-fix\n",
            "2026-05-14T10:20:30Z\n",
            "2026-05-14T10:20:40Z\n",
        ]
    )
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=next(responses), stderr="")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fake_run)

    assert get_pr_last_push_time("owner/name", 42) == datetime(
        2026,
        5,
        14,
        10,
        20,
        40,
        tzinfo=timezone.utc,
    )
    assert calls[1][2] == (
        "repos/owner/name/activity?ref=refs%2Fheads%2Ffeature-fix"
        "&activity_type=push&per_page=1&direction=desc"
    )
    assert calls[2][2] == (
        "repos/owner/name/activity?ref=refs%2Fheads%2Ffeature-fix"
        "&activity_type=force_push&per_page=1&direction=desc"
    )


def test_get_pr_last_push_time_returns_none_when_activity_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = iter(["feature-fix\n", "\n", "\n"])

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(returncode=0, stdout=next(responses), stderr="")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fake_run)

    assert get_pr_last_push_time("owner/name", 42) is None


def test_get_pr_last_push_time_returns_none_on_subprocess_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        if len(calls) == 1:
            return SimpleNamespace(returncode=0, stdout="feature-fix\n", stderr="")
        return SimpleNamespace(returncode=1, stdout="", stderr="HTTP 404")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fake_run)

    assert get_pr_last_push_time("owner/name", 42) is None
