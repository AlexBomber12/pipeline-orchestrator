"""Shared automation menu interactions; GHA owns Chromium execution."""

from pathlib import Path

import pytest
from playwright.sync_api import Page, expect


@pytest.fixture(autouse=True)
def local_htmx(page: Page):
    # Exercise the real pinned runtime without a first-page CDN dependency.
    # Source: https://unpkg.com/htmx.org@2.0.4/dist/htmx.min.js (0BSD).
    script = Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js'
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=script, content_type='application/javascript',
    ))


@pytest.mark.parametrize('path', ['/', '/repo/AlexBomber12__pipeline-orchestrator-testbed', '/settings'])
def test_automation_menu_cancel_and_focus(page: Page, dashboard_url: str, path: str):
    # Isolate display states and intercept mutations: this test never pauses a repo.
    page.route('**/partials/automation-status', lambda route: route.fulfill(
        body='<span data-automation-status data-paused="true" class="text-fail">'
             'Automation: panic, dispatch blocked</span>', content_type='text/html',
    ))
    posts = []
    page.route('**/daemon/*', lambda route: (
        posts.append(route.request.url) if route.request.method == 'POST' else None,
        route.fulfill(json={'repos': []}),
    ))
    page.goto(dashboard_url.rstrip('/') + path, wait_until='domcontentloaded')
    page.wait_for_function("Boolean(window.htmx)")
    toggle = page.locator('#automation-menu-toggle')
    expect(toggle).to_contain_text('Automation: panic, dispatch blocked')
    toggle.focus()
    page.keyboard.press('Enter')
    expect(page.locator('[data-action="resume-all"]')).to_be_visible()
    page.keyboard.press('Tab')
    expect(page.locator('[data-action="pause-all"]')).to_be_focused()
    page.keyboard.press('Enter')
    expect(page.locator('#pause-all-modal')).to_be_visible()
    page.locator('[data-pause-all-cancel]').click()
    expect(page.locator('#pause-all-modal')).not_to_be_visible()
    expect(toggle).to_be_focused()
    page.keyboard.press('Enter')
    page.keyboard.press('Escape')
    expect(page.locator('#automation-menu')).not_to_have_attribute('open', '')
    expect(toggle).to_be_focused()
    assert posts == []


def test_unavailable_status_keeps_menu_usable(page: Page, dashboard_url: str):
    page.route('**/partials/automation-status', lambda route: route.fulfill(
        body='<span data-paused="false">Automation status unavailable</span>',
        content_type='text/html',
    ))
    page.goto(dashboard_url, wait_until='domcontentloaded')
    page.wait_for_function("Boolean(window.htmx)")
    toggle = page.locator('#automation-menu-toggle')
    expect(toggle).to_contain_text('Automation status unavailable')
    toggle.click()
    expect(page.locator('[data-action="pause-all"]')).to_be_visible()
    expect(page.locator('[data-action="resume-all"]')).not_to_be_visible()
    page.locator('[data-action="stop-all"]').click()
    expect(page.locator('#stop-all-modal')).to_be_visible()
    page.keyboard.press('Escape')
    expect(toggle).to_be_focused()
