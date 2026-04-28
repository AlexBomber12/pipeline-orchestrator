"""End-to-end coverage for the coder-initiated ESCALATE protocol (PR-166).

Drives a PR through CODING → WATCH using the existing ``slow`` shim,
injects a failed CI status to engineer the WATCH → FIX transition, then
flips the shim scenario to ``escalate`` so the next FIX-cycle invocation
emits the ``ESCALATE: <reason>`` marker on stdout. The daemon is expected
to parse the marker, post the explanatory comment on the PR, apply the
``escalated`` label, and park the runner in IDLE.

This test mirrors ``test_fix_external_merge.py`` because both rely on the
testbed GitHub App's ``Commit statuses: Write`` permission to engineer
the WATCH → FIX transition. When the permission is missing the test
skips with the same shape of message; the unit coverage in
``tests/test_runner.py`` (``test_handle_fix_coder_escalate_*``) exercises
the parser and the escalation transition independently of any testbed.
"""

from __future__ import annotations

import json
import subprocess
import time

import pytest

from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
ESCALATE_DETECTION_MARGIN_SEC = 60


_PERMISSION_GAP_MESSAGE = (
    "Testbed GitHub App is missing the 'Commit statuses: Write' "
    "permission required to engineer the WATCH→FIX transition. "
    "Update the App per docs/ci-setup.md Step A and re-run. "
    "The ESCALATE protocol is exercised by the unit tests in "
    "tests/test_runner.py."
)


def _preflight_status_write_permission() -> None:
    invalid_sha = "0" * 40
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{TESTBED_REPO}/statuses/{invalid_sha}",
            "-f", "state=success",
            "-f", "context=e2e-escalate-preflight",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if (
        result.returncode != 0
        and "Resource not accessible by integration" in result.stderr
    ):
        pytest.skip(_PERMISSION_GAP_MESSAGE)


def _post_failed_status(head_sha: str) -> None:
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{TESTBED_REPO}/statuses/{head_sha}",
            "-f", "state=failure",
            "-f", "context=e2e-escalate-trigger",
            "-f", "description=Engineered failure to drive FIX",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "Resource not accessible by integration" in stderr:
            pytest.skip(_PERMISSION_GAP_MESSAGE)
        raise AssertionError(
            f"failed to post status check on {head_sha}: "
            f"rc={result.returncode}, stderr={stderr!r}"
        )


def _get_pr_head_sha(pr_number: int) -> str:
    result = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "-R", TESTBED_REPO,
            "--json", "headRefOid",
            "--jq", ".headRefOid",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to read head SHA for PR #{pr_number}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )
    sha = result.stdout.strip()
    if not sha:
        raise AssertionError(f"empty head SHA for PR #{pr_number}")
    return sha


def _pr_labels(pr_number: int) -> list[str]:
    result = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "-R", TESTBED_REPO,
            "--json", "labels",
            "--jq", "[.labels[].name]",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        return []
    try:
        return list(json.loads(result.stdout or "[]"))
    except json.JSONDecodeError:
        return []


def _pr_comment_bodies(pr_number: int) -> list[str]:
    result = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "-R", TESTBED_REPO,
            "--json", "comments",
            "--jq", "[.comments[].body]",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        return []
    try:
        return list(json.loads(result.stdout or "[]"))
    except json.JSONDecodeError:
        return []


def test_coder_escalate_marker_parks_pr_in_idle(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    _preflight_status_write_permission()
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())

    with coder_shim("slow"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-fix-coder-escalate", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        wait_for_state(["CODING"], timeout_sec=30)
        watch_entry = wait_for_state(["WATCH"], timeout_sec=120)
        watch_pr = watch_entry.get("current_pr") or {}
        pr_number = watch_pr.get("number")
        if not isinstance(pr_number, int) or pr_number <= 0:
            pytest.skip(
                f"Could not engineer FIX state: WATCH entry without a real "
                f"PR number (current_pr={watch_pr!r}). The testbed flow may "
                f"have raced past WATCH; the ESCALATE protocol is exercised "
                f"by the unit tests in tests/test_runner.py."
            )

        head_sha = _get_pr_head_sha(pr_number)
        _post_failed_status(head_sha)

        try:
            wait_for_state(["FIX"], timeout_sec=30)
        except TimeoutError:
            pytest.skip(
                f"daemon did not enter FIX after failed-status injection "
                f"on PR #{pr_number}; the WATCH→MERGE race likely won. "
                f"ESCALATE behavior is covered by unit tests."
            )

        # Switch the shim mid-flight: the next FIX-cycle invocation will
        # emit the ESCALATE marker instead of pushing more commits.
        SHIM_SCENARIO_PATH.write_text("escalate\n")

        wait_for_state(
            ["IDLE"], timeout_sec=ESCALATE_DETECTION_MARGIN_SEC,
        )

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "IDLE", (
        f"final state was {state['state']!r}, expected IDLE"
    )

    labels = _pr_labels(pr_number)
    assert "escalated" in labels, (
        f"escalated label missing from PR #{pr_number}; got labels={labels!r}"
    )
    bodies = _pr_comment_bodies(pr_number)
    assert any(
        "Coder explicitly escalated this PR" in body for body in bodies
    ), (
        f"ESCALATE comment missing from PR #{pr_number}; "
        f"got {len(bodies)} comments"
    )
