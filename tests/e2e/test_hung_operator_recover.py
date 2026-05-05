"""End-to-end coverage for the HUNG operator-recover button (PR-247).

Pins the four-boundary contract that today's CI does not exercise as a
single chain:

1. ``POST /repos/{slug}/recover`` atomically sets
   ``control:{slug}:recover`` and rejects when state != HUNG
   (``src/web/routes/repo_control.py``).
2. The HUNG handler reads-and-clears the flag via ``GETDEL`` and adds the
   trapped ``current_task.pr_id`` to ``_recovered_task_pr_ids``, persisting
   the set under ``recovered_tasks:{slug}`` so it survives restarts
   (``src/daemon/handlers/hung.py:_perform_operator_recovery`` and
   ``src/daemon/runner.py:_persist_recovered_task_pr_ids``).
3. The IDLE selector forces ``CANCELED`` for any pr_id in the recovered
   set so the still-open PR cannot re-derive ``DOING``
   (``src/daemon/handlers/idle.py``).
4. The upload path's ``difference_update(uploaded_pr_ids)`` clears the
   trimmed set and persists it, making the upload itself the explicit
   retry signal (``src/daemon/repo_ops.py``).

A regression in any single boundary (TTL drop, GETDEL split, set
persistence corrupted, upload-time clear removed) is invisible to today's
CI; this test couples them as one trip.

Note on the HUNG-entry path: the original task spec called for the
FIX-then-ESCALATE engineering trick used by ``test_fix_coder_escalate``,
but ``fix_escalation.escalate_fix_coder_initiated`` routes to ``IDLE``
on label-apply success (the testbed App has ``Pull requests: Write``,
so the apply succeeds). This test uses the WATCH review-timeout path
instead, which is the production scenario PR-247's recover button was
designed for: a PR sits in WATCH waiting for a Codex review that never
arrives, and after ``review_timeout_min`` (2 min in the test config) the
WATCH handler routes to ``HUNG`` via ``_escalate_to_hung`` with the
default ``target_state=HUNG``.
"""

from __future__ import annotations

import time

from tests.e2e.lib.coder_shim import coder_shim
from tests.e2e.lib.testbed_reset import read_recovered_task_pr_ids

# config.test.yml sets ``review_timeout_min: 2`` and ``poll_interval_sec: 2``
# at both the daemon and repo level. Reaching HUNG via the WATCH review
# timeout requires WATCH entry plus the 2-minute timeout plus one poll
# cycle. Allow generous slack so a slow CI runner does not flake.
HUNG_DEADLINE_SEC = 240
RECOVER_TO_IDLE_DEADLINE_SEC = 60
MARKER_CLEAR_DEADLINE_SEC = 30
POST_UPLOAD_CODING_DEADLINE_SEC = 60


def _last_updated(entry: dict) -> str | None:
    value = entry.get("last_updated")
    return value if isinstance(value, str) else None


def test_recover_button_transitions_hung_to_idle_and_upload_clears_marker(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    recover_repo,
    reset_testbed,
):
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    pr_id = f"PR-{pr_id_int}"

    with coder_shim("success"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-hung-operator-recover", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        wait_for_state(["CODING"], timeout_sec=30)
        watch_entry = wait_for_state(["WATCH"], timeout_sec=120)
        watch_pr = watch_entry.get("current_pr") or {}
        watch_pr_number = watch_pr.get("number")
        assert isinstance(watch_pr_number, int) and watch_pr_number > 0, (
            f"expected WATCH entry with a real PR number, got "
            f"current_pr={watch_pr!r}"
        )

        pre_recover_marker = read_recovered_task_pr_ids(testbed_slug)
        assert pr_id not in pre_recover_marker, (
            f"recovered-set marker already contains {pr_id!r} before recover "
            f"click; got {sorted(pre_recover_marker)!r}"
        )

        hung_entry = wait_for_state(["HUNG"], timeout_sec=HUNG_DEADLINE_SEC)
        current_task = hung_entry.get("current_task") or {}
        assert current_task.get("pr_id") == pr_id, (
            f"HUNG entry current_task.pr_id={current_task.get('pr_id')!r}, "
            f"expected {pr_id!r}"
        )
        pre_recover_last_updated = _last_updated(hung_entry)
        assert pre_recover_last_updated is not None, (
            f"HUNG entry missing last_updated: {hung_entry!r}"
        )

        status_code, body = recover_repo(testbed_slug)
        assert status_code == 200, (
            f"first recover POST returned {status_code}, body={body!r}"
        )

        deadline = time.monotonic() + RECOVER_TO_IDLE_DEADLINE_SEC
        idle_entry: dict | None = None
        last_seen_state = None
        while time.monotonic() < deadline:
            entry = get_state(testbed_slug)
            if entry is not None:
                last_seen_state = entry.get("state")
                if last_seen_state == "IDLE":
                    last_updated = _last_updated(entry)
                    if (
                        entry.get("current_task") is None
                        and last_updated is not None
                        and last_updated > pre_recover_last_updated
                    ):
                        idle_entry = entry
                        break
            time.sleep(1)
        assert idle_entry is not None, (
            f"runner did not reach IDLE with cleared current_task and a fresh "
            f"last_updated within {RECOVER_TO_IDLE_DEADLINE_SEC}s; "
            f"last_seen_state={last_seen_state!r}"
        )

        # Marker-add assertion: pins the write at hung.py
        # _perform_operator_recovery before current_task is cleared. A
        # regression that drops the ``add()`` call would still pass step
        # 10 (the daemon dispatches PR-A) but for the wrong reason — the
        # IDLE override would silently no-op.
        post_recover_marker = read_recovered_task_pr_ids(testbed_slug)
        assert pr_id in post_recover_marker, (
            f"recovered-set marker missing {pr_id!r} after IDLE transition; "
            f"got {sorted(post_recover_marker)!r}"
        )

        # Idempotency assertion: pins repo_control.py's
        # ``recovery_only_from_hung`` 400 branch under double-click while
        # the IDLE settling window is still in flight.
        second_status, second_body = recover_repo(testbed_slug)
        assert second_status == 400, (
            f"second recover POST in IDLE returned {second_status}, "
            f"body={second_body!r}; expected 400"
        )
        assert second_body.get("error") == "recovery_only_from_hung", (
            f"second recover POST body missing recovery_only_from_hung "
            f"error code: {second_body!r}"
        )

        # Re-upload PR-A to trigger the upload-time clear path.
        reupload_status = upload_zip(zip_path)
        assert reupload_status in (200, 201), (
            f"re-upload failed with status {reupload_status}"
        )

        # Marker-clear assertion: pins repo_ops.py's
        # ``difference_update(uploaded_pr_ids)`` plus the trimmed-set
        # persist call. A regression that removes the difference_update
        # would leave the marker present and make step 10 fail with an
        # inconclusive timeout instead of a precise marker-still-present
        # error.
        deadline = time.monotonic() + MARKER_CLEAR_DEADLINE_SEC
        marker_cleared = False
        last_marker: set[str] = post_recover_marker
        while time.monotonic() < deadline:
            last_marker = read_recovered_task_pr_ids(testbed_slug)
            if pr_id not in last_marker:
                marker_cleared = True
                break
            time.sleep(1)
        assert marker_cleared, (
            f"recovered-set marker still contains {pr_id!r} {MARKER_CLEAR_DEADLINE_SEC}s "
            f"after re-upload; got {sorted(last_marker)!r}"
        )

        # Dispatch assertion: pins the contract that upload IS the retry
        # signal. The daemon must re-pick PR-A on the next IDLE cycle
        # because (a) the marker no longer forces CANCELED, and
        # (b) the upload regenerated the task header so the DAG selector
        # finds it eligible.
        deadline = time.monotonic() + POST_UPLOAD_CODING_DEADLINE_SEC
        coding_seen = False
        last_state = None
        last_task_pr_id = None
        while time.monotonic() < deadline:
            entry = get_state(testbed_slug)
            if entry is not None:
                last_state = entry.get("state")
                task = entry.get("current_task") or {}
                last_task_pr_id = task.get("pr_id")
                if last_state == "CODING" and last_task_pr_id == pr_id:
                    coding_seen = True
                    break
            time.sleep(1)
        assert coding_seen, (
            f"daemon did not dispatch {pr_id!r} into CODING within "
            f"{POST_UPLOAD_CODING_DEADLINE_SEC}s of re-upload; "
            f"last_state={last_state!r}, last_task_pr_id={last_task_pr_id!r}"
        )
