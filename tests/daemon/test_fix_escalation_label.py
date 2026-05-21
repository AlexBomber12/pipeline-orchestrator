from __future__ import annotations

import asyncio
import subprocess
from typing import Any

import pytest

from src.daemon.fix_escalation import (
    ensure_escalated_label,
    escalate_fix_iteration_cap,
)
from src.models import PRInfo
from tests.runner import _helpers as h


def _called_process_error(stderr: str | bytes) -> subprocess.CalledProcessError:
    return subprocess.CalledProcessError(
        1,
        ["gh", "label", "create", "escalated"],
        stderr=stderr,
    )


def test_label_create_already_exists_silent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise _called_process_error(
                'label with name "escalated" already exists; use --force'
            )
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert ensure_escalated_label(runner, 42, "FIX no-push") is True
    assert runner.state.history == []


def test_label_create_already_exists_uppercase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise _called_process_error("ALREADY EXISTS")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert ensure_escalated_label(runner, 42, "FIX no-push") is True
    assert runner.state.history == []


def test_label_create_auth_error_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise _called_process_error(b"401 Unauthorized")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert ensure_escalated_label(runner, 42, "FIX no-push") is True
    assert any(
        "FIX no-push label create failed:" in entry["event"]
        and "401 Unauthorized" in entry["event"]
        for entry in runner.state.history
    )


def test_label_create_network_error_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise OSError("Connection refused")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert ensure_escalated_label(runner, 42, "FIX no-push") is True
    assert any(
        "FIX no-push label create failed: Connection refused" in entry["event"]
        for entry in runner.state.history
    )


def test_label_apply_still_proceeds_after_silent_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        calls.append(cmd)
        if cmd[:3] == ["label", "create", "escalated"]:
            raise _called_process_error("already exists")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert ensure_escalated_label(runner, 42, "FIX no-push") is True
    assert ["pr", "edit", "42", "--add-label", "escalated"] in calls


def test_label_apply_error_still_logged(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("could not edit PR")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    assert ensure_escalated_label(runner, 42, "FIX no-push") is False
    assert any(
        "failed to apply escalated label to PR #42: could not edit PR"
        in entry["event"]
        for entry in runner.state.history
    )


def test_fix_cap_label_create_already_exists_silent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise _called_process_error("already exists")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr(
        "src.daemon.fix_escalation.gh_comments.post_comment",
        lambda repo, pr_number, body: None,
    )

    runner = h._make_runner()
    pr = PRInfo(
        number=42,
        branch="pr-042",
        fix_iteration_count=runner.app_config.daemon.fix_iteration_cap,
    )
    asyncio.run(escalate_fix_iteration_cap(runner, pr))

    assert not any(
        "label create" in entry["event"] for entry in runner.state.history
    )


def test_fix_cap_label_create_auth_error_logged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            raise _called_process_error("401 Unauthorized")
        return ""

    monkeypatch.setattr("src.daemon.fix_escalation.gh_runner.run_gh", fake_run_gh)
    monkeypatch.setattr(
        "src.daemon.fix_escalation.gh_comments.post_comment",
        lambda repo, pr_number, body: None,
    )

    runner = h._make_runner()
    pr = PRInfo(
        number=42,
        branch="pr-042",
        fix_iteration_count=runner.app_config.daemon.fix_iteration_cap,
    )
    asyncio.run(escalate_fix_iteration_cap(runner, pr))

    assert any(
        "FIX cap label create failed:" in entry["event"]
        and "401 Unauthorized" in entry["event"]
        for entry in runner.state.history
    )
