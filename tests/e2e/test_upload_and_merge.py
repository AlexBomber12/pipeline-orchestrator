from tests.e2e.lib.coder_shim import coder_shim


def test_full_happy_path_via_shim_succeeds(
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

    with coder_shim("success"):
        zip_path = make_task_zip(
            800, "e2e-test-upload-merge", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        wait_for_state(["CODING"], timeout_sec=30)
        wait_for_state(["WATCH"], timeout_sec=90)
        wait_for_state(["MERGE", "IDLE"], timeout_sec=180)

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "IDLE", f"final state was {state['state']!r}"
    current_pr = state.get("current_pr")
    assert current_pr is None or current_pr.get("state") == "MERGED", (
        f"unexpected current_pr after merge: {current_pr!r}"
    )
