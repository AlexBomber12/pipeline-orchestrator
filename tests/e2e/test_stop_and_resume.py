import json
import subprocess
import time

import requests
from playwright.sync_api import expect

from tests.e2e.lib.coder_shim import coder_shim


def test_stop_during_coding_then_resume_picks_next_task(
    page,
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

        # Shared testbed dispatch can exceed 30s because the daemon's IDLE
        # poll interval is 60s; keep this aligned with the other upload e2e
        # tests so this case exercises stop/resume behavior, not poll timing.
        wait_for_state(["CODING"], timeout_sec=150)

        page.goto(f"{dashboard_url}/repo/{testbed_slug}", wait_until="domcontentloaded")
        page.wait_for_function("Boolean(window.htmx)")
        summary = page.locator('[data-repo-summary]')
        expect(summary).to_contain_text(f"PR-{first_pr_id_int}")
        page.once("dialog", lambda dialog: dialog.accept())
        with page.expect_response(f"**/repos/{testbed_slug}/stop") as response:
            summary.get_by_role("button", name="Stop current run", exact=True).click()
        assert response.value.status == 200
        assert response.value.request.headers.get("hx-request") == "true"

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

        # The persisted pause survives a full load as well as summary swaps.
        page.reload(wait_until="domcontentloaded")
        expect(summary.locator('[data-state-label]')).to_have_text("PAUSED")
        expect(summary.locator('[data-work-summary]')).to_contain_text(
            "Operator paused this repository."
        )
        expect(summary.locator('[data-work-summary]')).to_contain_text("Activity: Unknown")
        # Passive diagnostics remain usable and do not act as Resume.
        with page.expect_response(f"**/partials/repo/{testbed_slug}/cancellations") as diagnostics:
            page.get_by_text("Recent cancellations", exact=True).click()
        assert diagnostics.value.status == 200
        assert get_state()["user_paused"] is True
        with page.expect_response(f"**/repos/{testbed_slug}/resume") as response:
            summary.get_by_role("button", name="Resume daemon", exact=True).click()
        assert response.value.status == 200
        assert response.value.request.headers.get("hx-request") == "true"

        coding_entry = wait_for_state(["CODING"], timeout_sec=150)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == second_expected_pr_id, (
            f"resumed CODING was for current_task={coding_task!r}, "
            f"expected pr_id={second_expected_pr_id!r}"
        )

        watch_entry = wait_for_state(["WATCH"], timeout_sec=90)
        watch_pr = watch_entry.get("current_pr") or {}
        assert watch_pr.get("branch") == coding_task["branch"]
        wait_for_state(["IDLE"], timeout_sec=180)
        result = subprocess.run(
            [
                "gh", "pr", "view", str(watch_pr["number"]),
                "--repo", testbed_slug.replace("__", "/", 1),
                "--json", "state,headRefName,mergedAt",
            ],
            capture_output=True, text=True, check=True, timeout=30,
        )
        merged = json.loads(result.stdout)
        assert merged["headRefName"] == coding_task["branch"]
        assert merged["state"] == "MERGED" and merged["mergedAt"], merged

    state = get_state()
    assert state is not None, "no state entry returned for testbed at end"
    assert state["state"] == "IDLE", f"final state was {state['state']!r}"


def test_pending_upload_survives_missed_wake_restart_and_pause(
    dashboard_url, testbed_slug, get_state, upload_zip, make_task_zip, reset_testbed,
):
    """Real stack transport and Redis; coder/provider behavior uses the shim."""
    from datetime import datetime, timezone

    from tests.e2e.conftest import _stop_daemon_and_wait_paused

    def compose(*args):
        return subprocess.run(
            ["docker", "compose", "-f", "docker-compose.test.yml", *args],
            capture_output=True, text=True, check=True, timeout=60,
        ).stdout.strip()

    def redis(*args):
        return compose("exec", "-T", "redis-test", "redis-cli", "--raw", *args)

    def paused_publication(after):
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            entry = get_state() or {}
            updated = datetime.fromisoformat(entry.get("last_updated") or "1970-01-01T00:00:00+00:00")
            if updated > after:
                assert entry.get("user_paused") is True, entry
                assert entry.get("current_task") is None, entry
                assert entry.get("state") in {"IDLE", "PAUSED"}, entry
                return updated
            time.sleep(0.5)
        raise AssertionError("restarted daemon did not publish the persisted pause")

    pr_id = int(time.time())
    branch = f"pr-{pr_id}-e2e-missed-upload-wake"
    pending_key = f"upload:{testbed_slug}:pending"
    _stop_daemon_and_wait_paused(testbed_slug)
    stopped = False
    try:
        compose("stop", "daemon-test")
        stopped = True
        # No subscriber exists at acceptance: PubSub cannot retain this wake.
        assert redis("PUBSUB", "NUMSUB", f"orchestrator:wake:{testbed_slug}").splitlines()[-1] == "0"
        zip_path = make_task_zip(pr_id, "e2e-missed-upload-wake")
        assert upload_zip(zip_path) in (200, 201)
        raw = redis("GET", pending_key)
        manifest = json.loads(raw)
        manifest["files"].append(f"PR-{pr_id + 1}.md")  # proven absent sibling
        raw = json.dumps(manifest)
        redis("SET", pending_key, raw)
        restarted = datetime.now(timezone.utc)
        compose("start", "daemon-test")
        stopped = False
        updated = paused_publication(restarted)
        # A late upload wake must not act as Resume.
        redis("PUBLISH", f"orchestrator:wake:{testbed_slug}", '{"event_type":"upload"}')
        paused_publication(updated)
        assert redis("GET", pending_key) == raw

        # Drop Resume's wake too: startup must discover the accepted manifest.
        compose("stop", "daemon-test")
        stopped = True
        response = requests.post(f"{dashboard_url}/repos/{testbed_slug}/resume", timeout=10)
        assert response.status_code in (200, 204)
        with coder_shim("success"):
            compose("start", "daemon-test")
            stopped = False
            deadline = time.monotonic() + 300
            matched_pr = None
            while time.monotonic() < deadline:
                result = subprocess.run(
                    ["gh", "pr", "list", "--repo", testbed_slug.replace("__", "/", 1),
                     "--head", branch, "--state", "all", "--json", "number,state,mergedAt,headRefName"],
                    capture_output=True, text=True, check=True, timeout=30,
                )
                prs = json.loads(result.stdout)
                assert len(prs) <= 1, prs
                if prs and prs[0]["state"] == "MERGED" and prs[0]["mergedAt"]:
                    matched_pr = prs[0]
                    break
                time.sleep(2)
            assert matched_pr and matched_pr["headRefName"] == branch
            assert redis("GET", pending_key) == ""
            # Only one upload commit exists despite missed wakes and restart.
            subjects = compose(
                "exec", "-T", "daemon-test", "git", "-C", f"/data/repos/{testbed_slug}",
                "log", "origin/main", "--format=%s", "--", f"tasks/PR-{pr_id}.md",
            ).splitlines()
            assert subjects.count("tasks: upload batch (1 files)") == 1, subjects
    finally:
        if stopped:
            compose("start", "daemon-test")
