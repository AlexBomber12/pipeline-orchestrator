"""Chromium verifies retained cards across HTTP, transport and Redis failures."""

import re

import pytest
from playwright.sync_api import expect


@pytest.mark.parametrize("failure", ["http", "transport"])
def test_grid_refresh_failure(page, dashboard_url, failure):
    page.route("**/api/repos/*/events", lambda route: route.abort())
    page.goto(dashboard_url, wait_until="load")
    page.wait_for_function("typeof htmx !== 'undefined'")
    grid = page.locator("#repo-grid")
    expect(grid.locator("[data-repo-card]").first).to_be_visible()
    cards = grid.inner_html()
    mode = "healthy"

    def refresh(route):
        if mode == "failed":
            if failure == "transport":
                route.abort("failed")
            else:
                route.fulfill(status=503, body="Unavailable")
            return
        route.fulfill(status=200, content_type="text/html",
                      headers={"X-Observation-Available": "false" if mode == "redis" else "true",
                               "X-Observation-Time": "2026-11-02T12:34:00+00:00"},
                      body="<p>Synthetic fallback</p>" if mode == "redis" else cards)

    page.route("**/partials/repo-list", refresh)

    def poll():
        page.evaluate("htmx.trigger(document.body, 'repo:state_change')")

    poll()
    expect(grid).to_have_attribute("data-observed-at", "2026-11-02T12:34:00+00:00")
    warning = page.locator("#grid-refresh-warning")
    expect(warning).to_be_hidden()
    # Keep the exact DOM nodes through failed requests and unavailable 200s.
    page.evaluate("window.retainedCard = document.querySelector('[data-repo-card]')")
    mode = "failed"
    poll()
    expect(warning).to_be_visible()
    expect(warning).to_have_text(re.compile(r"Data from \d{2}:\d{2}; refresh unavailable. Will be retried\."))
    message = warning.inner_text()
    page.evaluate("""document.querySelector('#repo-grid').addEventListener('htmx:beforeSwap', event => {
        if (event.detail.xhr.getResponseHeader('X-Observation-Available') === 'false') {
            window.unavailableSeen = true;
        }
    })""")
    mode = "redis"
    with page.expect_response("**/partials/repo-list"):
        poll()
    page.wait_for_function("window.unavailableSeen === true")
    expect(warning).to_have_text(message)
    expect(grid.locator("[data-repo-card]").first).to_be_visible()
    assert page.evaluate("window.retainedCard === document.querySelector('[data-repo-card]')")
    mode = "healthy"
    poll()
    expect(warning).to_be_hidden()
    expect(grid.locator("[data-repo-card]").first).to_be_visible()
