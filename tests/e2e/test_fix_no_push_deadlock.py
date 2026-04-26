import time

from tests.e2e.lib.coder_shim import coder_shim


def test_three_no_push_fix_cycles_park_pr_in_hung(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    """Reproduce the WATCH↔FIX deadlock observed on PR #217 (2026-04-26).

    The shim's ``no_push_fail_ci`` scenario:
      1. CODING: pushes a marker commit, opens a PR, then stamps the head
         SHA with a failing commit-status so WATCH transitions into FIX.
      2. FIX: exits 0 without pushing, simulating a coder that decides
         there is no actionable fix.

    With ``fix_no_push_cap=3`` (config.test.yml), the daemon must
    transition to HUNG on the third consecutive no-push FIX cycle
    instead of looping indefinitely.
    """
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"

    with coder_shim("no_push_fail_ci"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-fix-no-push-deadlock", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        coding_entry = wait_for_state(["CODING"], timeout_sec=30)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == expected_pr_id, (
            f"CODING was for current_task={coding_task!r}, "
            f"expected pr_id={expected_pr_id!r}"
        )

        wait_for_state(["WATCH", "FIX"], timeout_sec=90)
        hung_entry = wait_for_state(["HUNG"], timeout_sec=180)

    assert hung_entry["state"] == "HUNG", f"unexpected state: {hung_entry['state']!r}"
    history = hung_entry.get("history") or []
    deadlock_event = next(
        (entry for entry in history if "FIX deadlock" in entry.get("event", "")),
        None,
    )
    assert deadlock_event is not None, (
        "no FIX deadlock event in history: "
        f"{[e.get('event') for e in history[-10:]]}"
    )
    assert "3 consecutive no-push FIX cycles" in deadlock_event["event"]
    assert "Manual review required" in deadlock_event["event"]

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "HUNG", f"final state was {state['state']!r}"
