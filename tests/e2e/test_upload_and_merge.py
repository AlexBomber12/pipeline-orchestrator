import base64
import json
import subprocess
import time
from pathlib import Path

import yaml

from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim
from tests.e2e.lib.review_api import MergeOutcomeApi, task_released_and_done


def _assert_merged_task_done(repo: str, number: int, branch: str, task_id: str) -> None:
    """Verify the exact PR and task reached their external terminal outcomes."""
    result = subprocess.run(
        ["gh", "pr", "view", str(number), "-R", repo,
         "--json", "state,headRefName,mergedAt"],
        capture_output=True, text=True, check=True, timeout=30,
    )
    merged = json.loads(result.stdout)
    assert merged["headRefName"] == branch, merged
    assert merged["state"] == "MERGED" and merged["mergedAt"], merged
    task = subprocess.run(
        ["gh", "api", f"repos/{repo}/contents/tasks/{task_id}.md?ref=main"],
        capture_output=True, text=True, check=True, timeout=30,
    )
    content = base64.b64decode(json.loads(task.stdout)["content"]).decode()
    assert content.startswith("---\nstatus: DONE\n"), content[:300]


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

    pr_id_int = time.time_ns()
    expected_pr_id = f"PR-{pr_id_int}"
    expected_branch = f"pr-{pr_id_int}-e2e-test-upload-merge"

    config = yaml.safe_load((Path(__file__).parent / "data/config.test.yml").read_text())
    repo = testbed_slug.replace("__", "/", 1)
    configured = next(item for item in config["repositories"] if repo in item["url"])
    # Successful-empty checks are an explicit disposable-testbed exemption;
    # this shim route does not establish strict CI policy or real LLM access.
    assert configured["allow_merge_without_checks"] is True
    outcome = MergeOutcomeApi(repo, configured["branch"])

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
        number = watch_pr["number"]
        assert isinstance(number, int) and number > 0

        watch_task = watch_entry.get("current_task") or {}
        assert watch_task.get("pr_id") == expected_pr_id and watch_task.get("branch") == expected_branch
        head = watch_pr["head_sha"]
        marker = outcome.published_marker(number, expected_branch, head, expected_pr_id)

        deadline = time.monotonic() + 180
        state = None
        while time.monotonic() < deadline:
            state = get_state()
            if task_released_and_done(state, expected_pr_id, expected_branch):
                break
            time.sleep(1)
        assert task_released_and_done(state, expected_pr_id, expected_branch), (
            f"task {expected_pr_id} / PR #{number} did not complete and release; "
            f"state={state!r}"
        )
        evidence = outcome.assert_merged(number, expected_branch, head, expected_pr_id, marker)
        print(f"matching merged upload outcome: {evidence}")


def test_legacy_size_label_pr_merges_and_task_is_done(
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    """An obsolete size label does not prevent ordinary FIX and merge."""

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
        label = "quarantine:large_diff"
        created = subprocess.run(
            ["gh", "label", "create", label, "-R", repo, "--color", "B60205"],
            capture_output=True, text=True, timeout=30,
        )
        assert created.returncode == 0 or "already exists" in created.stderr.lower(), created.stderr
        subprocess.run(
            ["gh", "pr", "edit", str(number), "-R", repo, "--add-label", label],
            capture_output=True, text=True, check=True, timeout=30,
        )
        # The pending status prevents a merge race while the legacy label is
        # attached. Failure then drives the ordinary FIX path. The successful
        # FIX shim publishes a fresh HEAD without this engineered gate.
        SHIM_SCENARIO_PATH.write_text("fix_existing_pr\n")
        status = subprocess.run(
            ["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{head}",
             "-f", "state=failure", "-f", "context=e2e/watch-merge-gate"],
            capture_output=True, text=True, check=True, timeout=30,
        )
        assert json.loads(status.stdout)["state"] == "failure"
        try:
            wait_for_state(["IDLE"], timeout_sec=240)
        except TimeoutError as exc:
            state = get_state()
            raise AssertionError(
                f"legacy PR did not finish ordinary FIX/MERGE; "
                f"state={state.get('state') if state else None!r}, "
                f"current_pr={(state or {}).get('current_pr')!r}"
            ) from exc

    state = get_state()
    assert state is not None and any(
        item.get("state") == "FIX" for item in (state.get("history") or [])
    ), "legacy PR did not enter ordinary FIX"
    _assert_merged_task_done(repo, number, branch, task_id)
    assert state["state"] == "IDLE"
