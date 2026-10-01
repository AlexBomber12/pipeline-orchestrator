"""End-to-end coverage for the FIX-cycle external-merge polling task.

PR-217 (memory entry #30) observed the daemon waste a 30-minute FIX cycle
because the user merged the PR externally while the coder process was
running. PR-165 fixes that by polling GitHub PR state from a side task and
short-circuiting the FIX cycle when a terminal state is observed.

This test reproduces the original failure shape end-to-end:
1. Drive a PR through CODING → WATCH using ``success_pending_ci`` so the
   existing merge-gate status prevents an automatic merge.
2. Arm the existing ``hang`` scenario, then change that same gate to failure
   so the daemon transitions WATCH → FIX.
3. Once the real FIX coder cycle is active, externally
   merge the PR via ``gh pr merge --admin``.
4. Verify the daemon transitions to IDLE within
   ``fix_poll_interval_sec + 10`` seconds, matching the success criterion.
"""

from __future__ import annotations

import json
import subprocess
import time

from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
WATCH_GATE_CONTEXT = "e2e/watch-merge-gate"
# Mirror the test stack's daemon.fix_poll_interval_sec from config.test.yml;
# bumping the config without bumping this constant would let stale waits
# silently keep timing out.
FIX_POLL_INTERVAL_SEC = 5
EXTERNAL_MERGE_DETECTION_MARGIN_SEC = 10


def _post_failed_status(head_sha: str) -> None:
    """Change the shim's pending WATCH merge gate to failure."""
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


def _merge_pr(pr_number: int) -> None:
    """Force-merge the PR while the daemon's FIX cycle is sleeping."""
    result = subprocess.run(
        [
            "gh", "pr", "merge", str(pr_number),
            "-R", TESTBED_REPO,
            "--squash", "--delete-branch", "--admin",
        ],
        capture_output=True, text=True, check=False, timeout=60,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to merge PR #{pr_number}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )


def test_external_merge_during_fix_returns_to_idle(
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
    expected_branch = f"pr-{pr_id_int}-e2e-fix-external-merge"

    with coder_shim("success_pending_ci"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-fix-external-merge", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        coding_entry = wait_for_state(["CODING"], timeout_sec=30)
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
        SHIM_SCENARIO_PATH.write_text("hang\n")
        _post_failed_status(head_sha)
        assert _get_pr_head_sha(pr_number) == head_sha, (
            f"PR #{pr_number} HEAD changed while publishing the failure"
        )
        assert _watch_gate_state(head_sha) == "failure"

        deadline = time.monotonic() + 30
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
                if (
                    entry.get("state") == "FIX"
                    and current_task.get("pr_id") == expected_pr_id
                    and current_pr.get("number") == pr_number
                    and current_pr.get("branch") == expected_branch
                    and saw_fix_history
                ):
                    fix_entry = entry
                    break
            time.sleep(0.5)
        assert fix_entry is not None, (
            f"FIX coder cycle was not active for {expected_pr_id} / "
            f"PR #{pr_number} within 30s; last_entry={last_entry!r}"
        )

        _merge_pr(pr_number)

        wait_for_state(
            ["IDLE"],
            timeout_sec=(
                FIX_POLL_INTERVAL_SEC + EXTERNAL_MERGE_DETECTION_MARGIN_SEC
            ),
        )

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "IDLE", (
        f"final state was {state['state']!r}, expected IDLE"
    )
    final_pr = state.get("current_pr")
    assert final_pr is None, (
        f"current_pr was not cleared after external merge: {final_pr!r} "
        f"(originally {expected_pr_id} / PR #{pr_number})"
    )
    assert state.get("current_task") is None, state.get("current_task")
