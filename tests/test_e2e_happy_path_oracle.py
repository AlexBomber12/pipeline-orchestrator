"""Hermetic checks for the external merge/DONE acceptance oracle."""

import base64
import json
import subprocess

import pytest

from tests.e2e.test_upload_and_merge import _assert_merged_task_done


@pytest.mark.parametrize("pr_state,branch,task_status,passes", [
    ("MERGED", "expected", "DONE", True),
    ("OPEN", "expected", "DONE", False),
    ("MERGED", "other", "DONE", False),
    ("MERGED", "expected", "DOING", False),
])
def test_happy_path_requires_matching_merged_pr_and_done_task(
    monkeypatch: pytest.MonkeyPatch, pr_state: str, branch: str,
    task_status: str, passes: bool,
) -> None:
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        if args[:3] == ["gh", "pr", "view"]:
            payload = {"state": pr_state, "headRefName": branch,
                       "mergedAt": "2026-10-25T00:00:00Z" if pr_state == "MERGED" else None}
        else:
            assert args == ["gh", "api", "repos/octo/demo/contents/tasks/PR-19.md?ref=main"]
            payload = {"content": base64.b64encode(
                f"---\nstatus: {task_status}\n---\n".encode()
            ).decode()}
        return subprocess.CompletedProcess(args, 0, json.dumps(payload), "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    if passes:
        _assert_merged_task_done("octo/demo", 19, "expected", "PR-19")
    else:
        with pytest.raises(AssertionError):
            _assert_merged_task_done("octo/demo", 19, "expected", "PR-19")
    assert calls[0][:4] == ["gh", "pr", "view", "19"]
