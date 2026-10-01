"""End-to-end coverage for coder-initiated ESCALATE parking in ERROR.

The ``success_pending_ci`` shim creates a real PR and publishes the
``e2e/watch-merge-gate`` pending status before CODING exits. After proving
the daemon is watching that task, branch, PR, and HEAD, this test arms the
existing ``escalate`` scenario and changes the same gate to failure. The
real FIX handler must consume the marker, park the task in ERROR with the
``coder_escalate`` outcome, and leave durable PR-side diagnostics.
"""

from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request

from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
WATCH_GATE_CONTEXT = "e2e/watch-merge-gate"
FIX_TRANSITION_DEADLINE_SEC = 30
ESCALATE_DETECTION_MARGIN_SEC = 60


def _post_failed_status(head_sha: str) -> None:
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{TESTBED_REPO}/statuses/{head_sha}",
            "-f", "state=failure",
            "-f", f"context={WATCH_GATE_CONTEXT}",
            "-f", "description=Engineered failure to drive FIX",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to post status check on {head_sha}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"status API returned invalid JSON for {head_sha}: "
            f"{result.stdout[:500]!r}"
        ) from exc
    assert isinstance(payload, dict), payload
    assert payload.get("state") == "failure", payload
    assert payload.get("context") == WATCH_GATE_CONTEXT, payload


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


def _watch_gate_state(head_sha: str) -> str:
    result = subprocess.run(
        ["gh", "api", f"repos/{TESTBED_REPO}/commits/{head_sha}/statuses"],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to read statuses on {head_sha}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )
    try:
        statuses = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(f"invalid statuses JSON: {result.stdout[:500]!r}") from exc
    assert isinstance(statuses, list), statuses
    gate = next((item for item in statuses if item.get("context") == WATCH_GATE_CONTEXT), None)
    assert gate is not None, f"{WATCH_GATE_CONTEXT!r} missing on {head_sha}"
    return str(gate.get("state"))


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
        raise AssertionError(
            f"failed to read labels for PR #{pr_number}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )
    try:
        return list(json.loads(result.stdout or "[]"))
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"invalid labels JSON for PR #{pr_number}: {result.stdout[:500]!r}"
        ) from exc


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
        raise AssertionError(
            f"failed to read comments for PR #{pr_number}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )
    try:
        return list(json.loads(result.stdout or "[]"))
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"invalid comments JSON for PR #{pr_number}: {result.stdout[:500]!r}"
        ) from exc


def _cancellation_for(dashboard_url: str, slug: str, pr_id: str) -> dict:
    url = f"{dashboard_url}/api/cancellations/{slug}"
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (
        urllib.error.URLError,
        urllib.error.HTTPError,
        TimeoutError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        raise AssertionError(f"failed to read {url}: {exc}") from exc
    assert isinstance(payload, list), payload
    cause = next(
        (item for item in payload if item.get("task_id") == pr_id), None
    )
    assert cause is not None, (
        f"no cancellation cause for {pr_id!r}; payload={payload!r}"
    )
    return cause


def test_coder_escalate_marker_parks_pr_in_error(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"
    expected_branch = f"pr-{pr_id_int}-e2e-fix-coder-escalate"

    with coder_shim("success_pending_ci"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-fix-coder-escalate", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        coding_entry = wait_for_state(["CODING"], timeout_sec=60)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == expected_pr_id, coding_task
        assert coding_task.get("branch") == expected_branch, coding_task

        watch_entry = wait_for_state(["WATCH"], timeout_sec=120)
        watch_task = watch_entry.get("current_task") or {}
        watch_pr = watch_entry.get("current_pr") or {}
        pr_number = watch_pr.get("number")
        assert watch_task.get("pr_id") == expected_pr_id, watch_task
        assert isinstance(pr_number, int) and pr_number > 0, watch_pr
        assert watch_pr.get("pr_id") == expected_pr_id, watch_pr
        assert watch_pr.get("branch") == expected_branch, watch_pr

        head_sha = _get_pr_head_sha(pr_number)
        assert watch_pr.get("head_sha") == head_sha, (
            f"WATCH head {watch_pr.get('head_sha')!r} does not match "
            f"PR #{pr_number} head {head_sha!r}"
        )
        assert _watch_gate_state(head_sha) == "pending"

        history_floor = len(watch_entry.get("history") or [])
        SHIM_SCENARIO_PATH.write_text("escalate\n")
        _post_failed_status(head_sha)
        assert _get_pr_head_sha(pr_number) == head_sha, (
            f"PR #{pr_number} HEAD changed while publishing the failure"
        )
        assert _watch_gate_state(head_sha) == "failure"

        deadline = time.monotonic() + FIX_TRANSITION_DEADLINE_SEC
        fix_entry = None
        last_entry = None
        while time.monotonic() < deadline:
            entry = get_state(testbed_slug)
            if entry is not None:
                last_entry = entry
                current_task = entry.get("current_task") or {}
                current_pr = entry.get("current_pr") or {}
                recent_history = (entry.get("history") or [])[history_floor:]
                saw_fix_history = any(
                    item.get("state") == "FIX"
                    and "entering FIX" in item.get("event", "")
                    for item in recent_history
                    if isinstance(item, dict)
                )
                same_identity = (
                    current_task.get("pr_id") == expected_pr_id
                    and current_pr.get("number") == pr_number
                    and current_pr.get("branch") == expected_branch
                )
                if same_identity and (
                    entry.get("state") == "FIX" or saw_fix_history
                ):
                    fix_entry = entry
                    break
            time.sleep(0.5)
        assert fix_entry is not None, (
            f"daemon did not enter real FIX for {expected_pr_id} / "
            f"PR #{pr_number} within {FIX_TRANSITION_DEADLINE_SEC}s; "
            f"last_entry={last_entry!r}"
        )

        wait_for_state(
            ["ERROR"], timeout_sec=ESCALATE_DETECTION_MARGIN_SEC,
        )

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "ERROR", (
        f"final state was {state['state']!r}, expected ERROR"
    )
    assert "FIX coder ESCALATE" in (state.get("error_message") or "")
    assert "e2e shim self-report" in (state.get("error_message") or "")
    assert (state.get("current_pr") or {}).get("is_escalated") is True
    cause = _cancellation_for(dashboard_url, testbed_slug, expected_pr_id)
    assert cause.get("category") == "ERROR", cause
    assert cause.get("payload", {}).get("subsource") == "coder_escalate", cause

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
