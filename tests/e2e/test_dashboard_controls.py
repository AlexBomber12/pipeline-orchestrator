"""Real Chromium controls; only the mixed global transport response is stubbed.

The delayed-consumption/outage case uses real web routes and isolated Redis.
GHA owns Docker execution and records the exact checkout SHA in e2e evidence.
"""

import json
import subprocess
from datetime import datetime, timezone

from playwright.sync_api import Page, expect
from src.events import build_wake_message
from src.keyspace import control_stop, pipeline_state

from tests.e2e.conftest import _wait_for_fresh_idle


def _compose(*args):
    return subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", *args],
        check=True, capture_output=True, text=True, timeout=60,
    ).stdout.strip()


def _redis(*args):
    return _compose("exec", "-T", "redis-test", "redis-cli", "--raw", *args)


def _refresh_summary(page, slug):
    # Wait for the DOM swap, not just an HTTP response. This uses the same
    # HTMX trigger as SSE, without manufacturing HTML or a browser DOM.
    page.locator('[data-repo-summary]').evaluate("node => node.dataset.beforeRefresh = 'yes'")
    with page.expect_response(f"**/partials/repo/{slug}") as response:
        page.evaluate("htmx.trigger(document.body, 'repo:history_updated')")
    assert response.value.status == 200
    expect(page.locator('[data-repo-summary]')).not_to_have_attribute("data-before-refresh", "yes")


def test_global_control_feedback(page: Page, dashboard_url: str) -> None:
    page.goto(dashboard_url, wait_until="domcontentloaded")
    page.wait_for_function("Boolean(window.htmx)")
    page.route("**/daemon/drain-progress", lambda route: route.fulfill(
        status=503, json={"repos": [{"name": "beta", "state": None}]},
    ))
    responses = [
        (207, [{"name": "alpha", "status": "accepted"},
               {"name": "beta", "status": "failed", "reason": "corrupt_state"}]),
        (503, [{"name": "beta", "status": "unavailable", "reason": "redis_unavailable"}]),
        (200, [{"name": "alpha", "status": "accepted"}]),
    ]
    for action, (status, results) in zip(["pause", "resume", "stop"], responses):
        page.route(f"**/daemon/{action}", lambda route, request, code=status, rows=results: route.fulfill(
            status=code, json={"results": rows},
        ))
    page.locator('[data-action="pause-all"]').click()
    expect(page.locator('#pause-all-affected-count')).to_have_text('?')
    page.locator('[data-pause-all-confirm]').click()
    output = page.locator('#pause-all-modal [role="status"]')
    expect(output).to_contain_text('Accepted 1/2 requests')
    expect(output).to_contain_text('beta (failed: corrupt_state)')
    # Swap real server-rendered cards through the existing HTMX event. The
    # mixed-result receipt lives outside the grid and must remain truthful.
    with page.expect_response("**/partials/repo-list") as refresh:
        page.evaluate("htmx.trigger(document.body, 'repo:history_updated')")
    assert refresh.value.status == 200
    expect(output).to_contain_text('Accepted 1/2 requests')
    expect(output).to_contain_text('beta (failed: corrupt_state)')
    expect(page.locator('[data-pause-all-confirm]')).to_be_enabled()
    page.locator('[data-pause-all-cancel]').click()
    page.locator('[data-action="resume-all"]').click()
    expect(page.locator('#global-control-result')).to_contain_text('Accepted 0/1 requests')
    expect(page.locator('#global-control-result')).to_contain_text('redis_unavailable')
    page.locator('[data-action="stop-all"]').click()
    page.locator('[data-stop-all-confirm]').click()
    expect(page.locator('#stop-all-modal [role="status"]')).to_contain_text(
        'Accepted 1/1 requests. Daemon application is pending.'
    )
    page.locator('[data-stop-all-cancel]').click()
    page.route("**/daemon/resume", lambda route: route.abort("failed"))
    page.locator('[data-action="resume-all"]').click()
    expect(page.locator('#global-control-result')).to_contain_text('Requests are unconfirmed')
    expect(page.locator('[data-action="resume-all"]')).to_be_enabled()


def test_delayed_stop_resume_and_redis_outage(
    page, dashboard_url, testbed_slug, get_state, reset_testbed,
):
    # Only this fixture-owned service is stopped. No coder is dispatched in
    # this case: delaying consumption must not imply coder process liveness.
    _compose("stop", "daemon-test")
    redis_stopped = False
    stop_key = control_stop(testbed_slug)
    state_key = pipeline_state(testbed_slug)
    try:
        before = get_state()
        assert before["name"] == testbed_slug and before["state"] == "IDLE"
        page.goto(dashboard_url, wait_until="domcontentloaded")
        page.wait_for_function("Boolean(window.htmx)")
        page.locator('[data-action="stop-all"]').click()
        with page.expect_response("**/daemon/stop") as response:
            page.locator('[data-stop-all-confirm]').click()
        assert response.value.status == 200
        results = response.value.json()["results"]
        assert results == [{"name": testbed_slug, "status": "accepted"}]
        expect(page.locator('#stop-all-modal [role="status"]')).to_contain_text(
            "Accepted 1/1 requests. Daemon application is pending."
        )
        assert _redis("GET", stop_key) == "1"
        accepted = get_state()
        assert accepted["user_paused"] is True
        assert accepted["state"] == before["state"]
        assert accepted["current_task"] == before["current_task"]

        page.goto(f"{dashboard_url}/repo/{testbed_slug}", wait_until="domcontentloaded")
        page.wait_for_function("Boolean(window.htmx)")
        summary = page.locator('[data-repo-summary]')
        for _ in range(2):
            _refresh_summary(page, testbed_slug)
            expect(summary.locator('[data-work-summary]')).to_contain_text(
                "Operator paused this repository."
            )
            expect(summary.locator('[data-work-summary]')).to_contain_text("Activity: Unknown")
            expect(summary.get_by_role("button", name="Resume daemon", exact=True)).to_be_enabled()
            assert _redis("GET", stop_key) == "1", "refresh must not acknowledge Stop"
        page.reload(wait_until="domcontentloaded")
        with page.expect_response(f"**/repos/{testbed_slug}/resume") as response:
            summary.get_by_role("button", name="Resume daemon", exact=True).click()
        assert response.value.status == 200
        assert get_state()["user_paused"] is False
        assert _redis("GET", stop_key) == ""
        _refresh_summary(page, testbed_slug)
        expect(summary.locator('[data-work-summary]')).not_to_contain_text("Operator paused")
        # A delayed Stop notification carries no authority after Resume. The
        # durable key has been cleared; startup must publish an unpaused state.
        restarted = datetime.now(timezone.utc)
        _compose("start", "daemon-test")
        _wait_for_fresh_idle(testbed_slug, after=restarted)
        delayed = datetime.now(timezone.utc)
        subscribers = _redis(
            "PUBLISH", f"orchestrator:wake:{testbed_slug}",
            build_wake_message(testbed_slug, "stop", now=restarted),
        )
        assert int(subscribers) > 0, "old Stop must reach a live subscriber"
        _wait_for_fresh_idle(testbed_slug, after=delayed)
        assert _redis("GET", stop_key) == ""
        page.reload(wait_until="domcontentloaded")
        expect(summary.locator('[data-work-summary]')).not_to_contain_text("Operator paused")

        # Keep the daemon quiescent while taking only the isolated Redis down.
        _compose("stop", "daemon-test")
        snapshot = _redis("GET", state_key)
        assert json.loads(snapshot)["name"] == testbed_slug
        _compose("stop", "redis-test")
        redis_stopped = True
        page.goto(dashboard_url, wait_until="domcontentloaded")
        page.wait_for_function("Boolean(window.htmx)")
        with page.expect_response("**/daemon/resume", timeout=30000) as response:
            page.locator('[data-action="resume-all"]').click()
        assert response.value.status == 503
        assert response.value.json()["results"][0]["name"] == testbed_slug
        assert response.value.json()["results"][0]["status"] == "unavailable"
        expect(page.locator('#global-control-result')).to_contain_text("Accepted 0/1 requests")
        card = page.locator(f'[data-repo-card][data-repo="{testbed_slug}"]')
        expect(card).to_contain_text("unavailable")
        page.goto(f"{dashboard_url}/repo/{testbed_slug}", wait_until="domcontentloaded")
        expect(summary.locator('[data-work-summary]')).to_contain_text("Stage: Unknown")
        expect(summary.locator('[data-work-summary]')).to_contain_text("Activity: Unknown")
        expect(page.get_by_text("Recent cancellations", exact=True)).to_be_visible()
        _compose("start", "redis-test")
        redis_stopped = False
        assert _redis("GET", state_key) == snapshot, "unavailable Resume must not mutate state"
    finally:
        if redis_stopped:
            _compose("start", "redis-test")
        _compose("start", "daemon-test")
    _wait_for_fresh_idle(testbed_slug, after=datetime.now(timezone.utc))
