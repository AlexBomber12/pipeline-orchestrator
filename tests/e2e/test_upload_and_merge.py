import time

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

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"
    expected_branch = f"pr-{pr_id_int}-e2e-test-upload-merge"

    with coder_shim("success"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-test-upload-merge", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        # Shared testbed dispatch was observed at ~39 seconds, so 30 seconds raced CODING.
        coding_entry = wait_for_state(["CODING"], timeout_sec=150)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == expected_pr_id, (
            f"CODING was for current_task={coding_task!r}, "
            f"expected pr_id={expected_pr_id!r}"
        )
        assert coding_task.get("branch") == expected_branch, (
            f"CODING current_task.branch={coding_task.get('branch')!r}, "
            f"expected {expected_branch!r}"
        )

        watch_entry = wait_for_state(["WATCH"], timeout_sec=90)
        watch_pr = watch_entry.get("current_pr") or {}
        assert watch_pr.get("branch") == expected_branch, (
            f"WATCH current_pr.branch={watch_pr.get('branch')!r}, "
            f"expected {expected_branch!r}"
        )

        wait_for_state(["IDLE"], timeout_sec=180)

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "IDLE", f"final state was {state['state']!r}"
    current_pr = state.get("current_pr")
    assert current_pr is None or current_pr.get("state") == "MERGED", (
        f"unexpected current_pr after merge: {current_pr!r}"
    )


def test_legacy_size_label_pr_merges_and_task_is_done(
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    """A published PR carrying an obsolete size label uses ordinary WATCH/MERGE."""
    import base64
    import json
    import subprocess

    repo = testbed_slug.replace("__", "/", 1)
    wait_for_state(["IDLE"], timeout_sec=30)
    pr_id_int = time.time_ns() // 1_000_000
    task_id = f"PR-{pr_id_int}"
    branch = f"pr-{pr_id_int}-e2e-legacy-size-label"
    with coder_shim("success_pending_ci"):
        assert upload_zip(make_task_zip(pr_id_int, "e2e-legacy-size-label")) in (200, 201)
        coding = wait_for_state(["CODING"], timeout_sec=150)
        assert (coding.get("current_task") or {}).get("pr_id") == task_id
        watch = wait_for_state(["WATCH"], timeout_sec=120)
        pr = watch.get("current_pr") or {}
        assert pr.get("branch") == branch, pr
        number = pr["number"]
        head = pr["head_sha"]
        assert len(head) == 40
        label = f"size-ok:{head}"
        created = subprocess.run(
            ["gh", "label", "create", label, "-R", repo, "--color", "0E8A16"],
            capture_output=True, text=True, timeout=30,
        )
        assert created.returncode == 0 or "already exists" in created.stderr.lower(), created.stderr
        subprocess.run(
            ["gh", "pr", "edit", str(number), "-R", repo, "--add-label", label],
            capture_output=True, text=True, check=True, timeout=30,
        )
        subprocess.run(
            ["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{head}",
             "-f", "state=success", "-f", "context=e2e/watch-merge-gate"],
            capture_output=True, text=True, check=True, timeout=30,
        )
        wait_for_state(["IDLE"], timeout_sec=240)

    result = subprocess.run(
        ["gh", "pr", "view", str(number), "-R", repo,
         "--json", "state,headRefName,mergedAt"],
        capture_output=True, text=True, check=True, timeout=30,
    )
    merged = json.loads(result.stdout)
    assert merged["headRefName"] == branch
    assert merged["state"] == "MERGED" and merged["mergedAt"], merged
    task = subprocess.run(
        ["gh", "api", f"repos/{repo}/contents/tasks/{task_id}.md?ref=main"],
        capture_output=True, text=True, check=True, timeout=30,
    )
    content = base64.b64decode(json.loads(task.stdout)["content"]).decode()
    assert content.startswith("---\nstatus: DONE\n"), content[:300]
    state = get_state()
    assert state is not None and state["state"] == "IDLE"
