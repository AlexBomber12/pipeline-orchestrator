"""End-to-end coverage for the cancellation-cause CRASH detection path.

Drives a coder failure through the daemon (``exit_nonzero`` shim) and
asserts the chain that PR-252 / PR-253 / PR-254 / PR-257 shipped:

* the daemon writes a ``CancellationCause`` with ``category="CRASH"``
  via ``_transition_to_error`` for the failed task,
* the dashboard's ``/api/cancellations/{repo}`` surface returns the
  recorded cause within the recent-7-days window, and
* the dependents-aware ``dependents_count`` augmentation reflects a
  downstream task whose ``Depends on:`` header points at the canceled
  root, which is the production proxy for "PR-B is in the blocked set".

The dashboard API is the assertion surface (no Redis-direct fallback)
because PR-257 already shapes its response to expose the blocked-set
signal alongside the cause: keeping the test on a single black-box
endpoint avoids drifting against the schema in ``src/cancellation``.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from tests.e2e.lib.coder_shim import coder_shim


def _fetch_cancellations(dashboard_url: str, slug: str) -> list[dict]:
    url = f"{dashboard_url}/api/cancellations/{slug}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
        return []
    return payload if isinstance(payload, list) else []


def _entry_for(payload: list[dict], task_id: str) -> dict | None:
    return next((c for c in payload if c.get("task_id") == task_id), None)


def test_coder_crash_records_cancellation_cause_and_propagates(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip_multi,
    reset_testbed,
):
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    base = int(time.time())
    pr_a_int = base
    pr_b_int = base + 1
    pr_a_id = f"PR-{pr_a_int}"
    pr_b_id = f"PR-{pr_b_int}"

    with coder_shim("exit_nonzero"):
        zip_path = make_task_zip_multi(
            [
                (pr_a_int, "e2e-cancellation-crash-root", []),
                (pr_b_int, "e2e-cancellation-crash-dep", [pr_a_id]),
            ],
            coder="any",
            priority=2,
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        # The daemon must select PR-A first (PR-B depends on it). When the
        # exit_nonzero shim returns non-zero, ``_transition_to_error``
        # records the default ``CancellationCause(category="CRASH")``
        # under ``cancellation:{slug}:PR-A`` (see
        # ``src/daemon/runner.py`` _transition_to_error and
        # ``src/daemon/handlers/coding.py`` for the producing call site).
        coding_entry = wait_for_state(["CODING"], timeout_sec=60)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == pr_a_id, (
            f"first CODING was for current_task={coding_task!r}; "
            f"expected pr_id={pr_a_id!r}"
        )

        deadline = time.monotonic() + 30
        last_payload: list[dict] = []
        cause_for_a: dict | None = None
        while time.monotonic() < deadline:
            last_payload = _fetch_cancellations(dashboard_url, testbed_slug)
            cause_for_a = _entry_for(last_payload, pr_a_id)
            if cause_for_a is not None:
                break
            time.sleep(1)
        assert cause_for_a is not None, (
            f"no cancellation cause surfaced for {pr_a_id!r} via "
            f"/api/cancellations/{testbed_slug} within 30s; "
            f"last payload={last_payload!r}; "
            f"runner state={get_state()!r}"
        )
        assert cause_for_a.get("category") == "CRASH", (
            f"unexpected category for {pr_a_id!r}: cause={cause_for_a!r}; "
            f"expected category=CRASH; runner state={get_state()!r}"
        )

        # ``dependents_count`` is computed from QUEUE.md, which the daemon
        # regenerates on its IDLE cycle (see ``handle_idle._generate_queue_md``).
        # PR-B's ``Depends on: PR-A`` line therefore lands in the closure
        # only after the daemon has folded the upload into QUEUE.md and
        # the next ``/api/cancellations`` read picks up the augmented
        # entry produced by ``_augment_causes_with_dependents``. Poll
        # until the dependents view catches up rather than reading a
        # single snapshot to avoid a flake at the cause-then-regen seam.
        deadline = time.monotonic() + 30
        dependents_count: int | None = None
        while time.monotonic() < deadline:
            payload = _fetch_cancellations(dashboard_url, testbed_slug)
            entry = _entry_for(payload, pr_a_id)
            if entry is not None:
                dependents_count = entry.get("dependents_count")
                if isinstance(dependents_count, int) and dependents_count >= 1:
                    break
            time.sleep(1)
        final_payload = _fetch_cancellations(dashboard_url, testbed_slug)
        assert isinstance(dependents_count, int) and dependents_count >= 1, (
            f"dependents_count for {pr_a_id!r} did not reach >= 1 within 30s "
            f"(expected {pr_b_id!r} to be counted as a transitively blocked "
            f"dependent); last value={dependents_count!r}; "
            f"final payload={final_payload!r}; "
            f"runner state={get_state()!r}"
        )

        # After the cause is recorded, the error handler asks the
        # auxiliary coder to diagnose; the shim's ``FIX, SKIP, or
        # ESCALATE`` short-circuit returns SKIP, which clears
        # ``current_task`` and routes the runner back to IDLE
        # (``src/daemon/handlers/error.py``). The cause itself is left in
        # place by the SKIP branch — the dashboard surfaces it for the
        # operator to inspect. Allow up to 60s to absorb the error
        # handler latency on a busy testbed.
        deadline = time.monotonic() + 60
        idle_entry = None
        last_state = None
        last_current_task: object = "<unset>"
        while time.monotonic() < deadline:
            entry = get_state()
            if entry is not None:
                last_state = entry.get("state")
                last_current_task = entry.get("current_task")
                if last_state == "IDLE" and last_current_task is None:
                    idle_entry = entry
                    break
            time.sleep(1)
        assert idle_entry is not None, (
            f"runner did not return to IDLE with current_task cleared "
            f"within 60s after the CRASH cause was recorded; "
            f"last_state={last_state!r}, last_current_task={last_current_task!r}, "
            f"final cancellations="
            f"{_fetch_cancellations(dashboard_url, testbed_slug)!r}"
        )
