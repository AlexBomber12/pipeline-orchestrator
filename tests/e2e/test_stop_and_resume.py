import time

import requests

from tests.e2e.lib.coder_shim import coder_shim


def test_stop_during_coding_then_resume_picks_next_task(
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

    first_pr_id_int = int(time.time())

    with coder_shim("slow"):
        zip_path = make_task_zip(
            first_pr_id_int, "e2e-stop-resume-slow", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"first upload failed with status {status}"

        wait_for_state(["CODING"], timeout_sec=30)

        stop_resp = requests.post(
            f"{dashboard_url}/repos/{testbed_slug}/stop", timeout=10
        )
        assert stop_resp.status_code in (200, 204), (
            f"stop returned status {stop_resp.status_code}: {stop_resp.text!r}"
        )

        # The slow shim sleeps 30s before any side effect, so its natural
        # completion path is CODING -> WATCH -> ... -> IDLE and never visits
        # PAUSED. Requiring PAUSED within a window shorter than the shim's
        # sleep proves the in-flight run was actually interrupted; if /stop
        # regressed to a no-op the run would still be CODING here and this
        # wait would time out.
        post_stop_entry = wait_for_state(["PAUSED"], timeout_sec=20)
        assert post_stop_entry["state"] == "PAUSED", (
            f"unexpected state after stop: {post_stop_entry['state']!r}"
        )

    second_pr_id_int = first_pr_id_int + 1
    while second_pr_id_int <= int(time.time()):
        second_pr_id_int = int(time.time()) + 1
    second_expected_pr_id = f"PR-{second_pr_id_int}"

    with coder_shim("success"):
        # Queue the follow-up task BEFORE calling /resume so the daemon
        # cannot run PAUSED -> IDLE and re-select the stopped task in the
        # 2s poll window before the second upload arrives.
        zip_path = make_task_zip(
            second_pr_id_int, "e2e-stop-resume-success", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"second upload failed with status {status}"

        resume_resp = requests.post(
            f"{dashboard_url}/repos/{testbed_slug}/resume", timeout=10
        )
        assert resume_resp.status_code in (200, 204), (
            f"resume returned status {resume_resp.status_code}: "
            f"{resume_resp.text!r}"
        )

        coding_entry = wait_for_state(["CODING"], timeout_sec=60)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == second_expected_pr_id, (
            f"resumed CODING was for current_task={coding_task!r}, "
            f"expected pr_id={second_expected_pr_id!r}"
        )

        wait_for_state(["IDLE"], timeout_sec=180)

    state = get_state()
    assert state is not None, "no state entry returned for testbed at end"
    assert state["state"] == "IDLE", f"final state was {state['state']!r}"
