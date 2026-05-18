"""Tests for src.audit.operator_actions (PR-336)."""

from __future__ import annotations

import builtins
import fcntl
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.audit import operator_actions
from src.audit.operator_actions import write_audit_record


@pytest.fixture
def audit_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    target = tmp_path / "audit" / "operator-actions"
    monkeypatch.setattr(operator_actions, "AUDIT_DIR", target)
    return target


def _today_filename(audit_dir: Path) -> Path:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return audit_dir / f"{today}.jsonl"


def test_write_audit_record_creates_dated_file(audit_dir: Path) -> None:
    write_audit_record(
        action="reset_task",
        repo_slug="example__alpha",
        task_id="PR-322",
        payload={"deleted_keys": []},
    )
    target = _today_filename(audit_dir)
    assert target.exists(), "expected dated audit file to be created"


def test_write_audit_record_appends_single_line(audit_dir: Path) -> None:
    for index in range(2):
        write_audit_record(
            action="reset_task",
            repo_slug="example__alpha",
            task_id=f"PR-{index}",
            payload={"deleted_keys": []},
        )
    target = _today_filename(audit_dir)
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    parsed = [json.loads(line) for line in lines]
    assert parsed[0]["task_id"] == "PR-0"
    assert parsed[1]["task_id"] == "PR-1"


def test_write_audit_record_handles_missing_dir(audit_dir: Path) -> None:
    assert not audit_dir.exists()
    write_audit_record(
        action="reset_task",
        repo_slug="example__alpha",
        task_id="PR-322",
        payload={"deleted_keys": []},
    )
    assert audit_dir.exists()
    target = _today_filename(audit_dir)
    assert target.exists()


def test_write_audit_record_record_shape_correct(audit_dir: Path) -> None:
    payload = {
        "deleted_keys": ["cancellation:example__alpha:PR-322"],
        "closed_pr_number": 444,
        "frontmatter_pushed": True,
        "retry_count_at_reset": 3,
        "subsource_at_reset": "review_timeout",
    }
    write_audit_record(
        action="reset_task",
        repo_slug="example__alpha",
        task_id="PR-322",
        payload=payload,
    )
    target = _today_filename(audit_dir)
    record = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
    assert set(record.keys()) == {
        "timestamp",
        "action",
        "repo_slug",
        "task_id",
        "payload",
    }
    assert record["action"] == "reset_task"
    assert record["repo_slug"] == "example__alpha"
    assert record["task_id"] == "PR-322"
    assert record["payload"] == payload
    datetime.fromisoformat(record["timestamp"])


def test_write_audit_record_swallows_oserror(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_open = builtins.open

    def fake_open(path: Any, *args: Any, **kwargs: Any) -> Any:
        if str(path).startswith(str(audit_dir)):
            raise OSError("disk full")
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    write_audit_record(
        action="reset_task",
        repo_slug="example__alpha",
        task_id="PR-322",
        payload={"deleted_keys": []},
    )
    target = _today_filename(audit_dir)
    assert not target.exists()


def test_write_audit_record_fsyncs_before_unlock(
    audit_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lock must be held through flush+fsync so concurrent writers cannot
    acquire LOCK_EX while a previous record is still buffered."""
    events: list[str] = []
    real_flock = fcntl.flock
    real_fsync = os.fsync

    def trace_flock(fd: int, op: int) -> None:
        if op == fcntl.LOCK_EX:
            events.append("LOCK_EX")
        elif op == fcntl.LOCK_UN:
            events.append("LOCK_UN")
        real_flock(fd, op)

    def trace_fsync(fd: int) -> None:
        events.append("fsync")
        real_fsync(fd)

    monkeypatch.setattr(operator_actions.fcntl, "flock", trace_flock)
    monkeypatch.setattr(operator_actions.os, "fsync", trace_fsync)

    write_audit_record(
        action="reset_task",
        repo_slug="example__alpha",
        task_id="PR-322",
        payload={"deleted_keys": []},
    )

    assert events == ["LOCK_EX", "fsync", "LOCK_UN"]


def test_concurrent_writes_do_not_corrupt_file(audit_dir: Path) -> None:
    def _writer(index: int) -> None:
        write_audit_record(
            action="reset_task",
            repo_slug="example__alpha",
            task_id=f"PR-{index:03d}",
            payload={"index": index},
        )

    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(_writer, range(10)))

    target = _today_filename(audit_dir)
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 10
    parsed = [json.loads(line) for line in lines]
    indices = sorted(record["payload"]["index"] for record in parsed)
    assert indices == list(range(10))
