"""Tests for scripts/statusline_hook.py."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

HOOK_SCRIPT = str(
    Path(__file__).resolve().parent.parent / "scripts" / "statusline_hook.py"
)


def _run_hook(
    stdin_data: str = "",
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ}
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, HOOK_SCRIPT],
        input=stdin_data,
        capture_output=True,
        text=True,
        timeout=5,
        env=env,
    )


def test_hook_emits_statusline_on_empty_stdin() -> None:
    result = _run_hook("")
    assert result.returncode == 0
    assert result.stdout == "pipeline-orchestrator"


def test_hook_handles_malformed_json_gracefully() -> None:
    result = _run_hook("not json at all")
    assert result.returncode == 0
    assert result.stdout == "pipeline-orchestrator"


def test_hook_handles_missing_rate_limits_field() -> None:
    result = _run_hook(json.dumps({"some": "data"}))
    assert result.returncode == 0
    assert "s0% w0%" in result.stdout


def test_hook_no_marker_when_under_thresholds(tmp_path: Path) -> None:
    data = {
        "rate_limits": {
            "five_hour": {"used_percentage": 50, "resets_at": 9999},
            "seven_day": {"used_percentage": 30, "resets_at": 8888},
        }
    }
    result = _run_hook(
        json.dumps(data),
        env_overrides={
            "PIPELINE_BREACH_DIR": str(tmp_path),
            "PIPELINE_RUN_ID": "test-run-123",
            "PIPELINE_SESSION_THRESHOLD": "95",
            "PIPELINE_WEEKLY_THRESHOLD": "100",
        },
    )
    assert result.returncode == 0
    assert "s50% w30%" in result.stdout
    assert not list(tmp_path.glob("*.breach"))


def test_hook_writes_breach_marker_on_session_over_threshold(
    tmp_path: Path,
) -> None:
    data = {
        "rate_limits": {
            "five_hour": {"used_percentage": 96, "resets_at": 1700000000},
            "seven_day": {"used_percentage": 30, "resets_at": 8888},
        }
    }
    result = _run_hook(
        json.dumps(data),
        env_overrides={
            "PIPELINE_BREACH_DIR": str(tmp_path),
            "PIPELINE_RUN_ID": "run-abc",
            "PIPELINE_SESSION_THRESHOLD": "95",
            "PIPELINE_WEEKLY_THRESHOLD": "100",
        },
    )
    assert result.returncode == 0
    marker = tmp_path / "run-abc.breach"
    assert marker.is_file()
    breach = json.loads(marker.read_text())
    assert breach["type"] == "session"
    assert breach["resets_at"] == 1700000000
    assert breach["session_pct"] == 96
    assert breach["weekly_pct"] == 30


def test_hook_writes_breach_marker_on_weekly_over_threshold(
    tmp_path: Path,
) -> None:
    data = {
        "rate_limits": {
            "five_hour": {"used_percentage": 50, "resets_at": 9999},
            "seven_day": {"used_percentage": 101, "resets_at": 1700000000},
        }
    }
    result = _run_hook(
        json.dumps(data),
        env_overrides={
            "PIPELINE_BREACH_DIR": str(tmp_path),
            "PIPELINE_RUN_ID": "run-weekly",
            "PIPELINE_SESSION_THRESHOLD": "95",
            "PIPELINE_WEEKLY_THRESHOLD": "100",
        },
    )
    assert result.returncode == 0
    marker = tmp_path / "run-weekly.breach"
    assert marker.is_file()
    breach = json.loads(marker.read_text())
    assert breach["type"] == "weekly"
    assert breach["resets_at"] == 1700000000


def test_hook_uses_env_thresholds(tmp_path: Path) -> None:
    data = {
        "rate_limits": {
            "five_hour": {"used_percentage": 80, "resets_at": 9999},
            "seven_day": {"used_percentage": 30, "resets_at": 8888},
        }
    }
    # Lower session threshold to 75 so 80% triggers
    result = _run_hook(
        json.dumps(data),
        env_overrides={
            "PIPELINE_BREACH_DIR": str(tmp_path),
            "PIPELINE_RUN_ID": "run-env",
            "PIPELINE_SESSION_THRESHOLD": "75",
            "PIPELINE_WEEKLY_THRESHOLD": "100",
        },
    )
    assert result.returncode == 0
    marker = tmp_path / "run-env.breach"
    assert marker.is_file()


def test_hook_skips_marker_when_no_run_id(tmp_path: Path) -> None:
    data = {
        "rate_limits": {
            "five_hour": {"used_percentage": 99, "resets_at": 9999},
            "seven_day": {"used_percentage": 30, "resets_at": 8888},
        }
    }
    env = {
        "PIPELINE_BREACH_DIR": str(tmp_path),
        "PIPELINE_SESSION_THRESHOLD": "95",
        "PIPELINE_WEEKLY_THRESHOLD": "100",
    }
    # Remove PIPELINE_RUN_ID so the hook skips writing the marker
    env.pop("PIPELINE_RUN_ID", None)
    result = _run_hook(json.dumps(data), env_overrides=env)
    assert result.returncode == 0
    assert not list(tmp_path.glob("*.breach"))
