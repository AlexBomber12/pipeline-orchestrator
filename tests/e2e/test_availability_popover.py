"""Chromium contracts for explicit availability choices, with API transport controlled."""

import pytest
from playwright.sync_api import expect


@pytest.fixture
def availability_ui(page, dashboard_url):
    state = {"manual_override": "AUTO", "composed_state": "AVAILABLE"}
    posts = []
    held = []
    page.add_init_script("""window.EventSource = class extends EventTarget {
        constructor(url) { super(); if (url.includes('/availability/')) window.availabilityEvents = this; }
        close() {}
    };""")
    page.route("**/api/availability", lambda route: route.fulfill(json=state))

    def update(route):
        choice = route.request.url.rsplit("/", 1)[1]
        assert route.request.method == "POST"
        posts.append(choice)
        if choice == "AVAILABLE":
            held.append(route)
        else:
            state.update(manual_override=choice, composed_state=choice)
            route.fulfill(json={"ok": True})

    page.route("**/api/availability/*", update)
    page.goto(dashboard_url, wait_until="load")
    page.get_by_label("Header menu", exact=True).click()
    chip = page.locator("#availability-chip")
    expect(chip).to_have_attribute("data-manual", "AUTO")
    return chip, page.locator("#availability-popover"), state, posts, held


@pytest.mark.parametrize("width", [390, 1440])
def test_select_away_once_and_escape_returns_focus(page, availability_ui, width):
    page.set_viewport_size({"width": width, "height": 900})
    chip, popover, _, posts, _ = availability_ui
    chip.click()
    expect(popover).to_be_visible()
    expect(popover.get_by_role("button", name="Auto", exact=True)).to_have_attribute("aria-pressed", "true")
    expect(popover.locator("#availability-reason")).to_have_text(
        "Now: AVAILABLE because automatic availability signals"
    )
    assert posts == []
    popover.get_by_role("button", name="Away", exact=True).click()
    expect(popover).not_to_be_visible()
    expect(chip).to_have_attribute("data-composed", "AWAY")
    expect(chip.locator(".label")).to_have_text("AWAY")
    assert posts == ["AWAY"]
    chip.click()
    expect(popover.get_by_role("button", name="Away", exact=True)).to_have_attribute("aria-pressed", "true")
    expect(popover.locator("#availability-reason")).to_have_text("Now: AWAY because manual override")
    page.keyboard.press("Escape")
    expect(popover).not_to_be_visible()
    expect(chip).to_be_focused()
    expect(chip).to_have_attribute("aria-expanded", "false")
    assert posts == ["AWAY"]


def test_choices_serialize_and_sse_refreshes(page, availability_ui):
    chip, popover, state, posts, held = availability_ui
    chip.click()
    popover.get_by_role("button", name="Available", exact=True).click()
    page.wait_for_function("document.querySelector('#availability-chip').dataset.manual === 'AVAILABLE'")
    chip.click()
    popover.get_by_role("button", name="Away", exact=True).click()
    # A GET round trip gives the first intercepted POST time to reach the handler.
    page.evaluate("fetch('/api/availability').then(r => r.json())")
    assert posts == ["AVAILABLE"]
    assert len(held) == 1
    state.update(manual_override="AVAILABLE", composed_state="AVAILABLE")
    held.pop().fulfill(json={"ok": True})
    expect(chip).to_have_attribute("data-composed", "AWAY")
    assert posts == ["AVAILABLE", "AWAY"]
    state.update(manual_override="AUTO", composed_state="AVAILABLE", reason="active hours")
    page.evaluate("window.availabilityEvents.dispatchEvent(new Event('availability_changed'))")
    expect(chip).to_have_attribute("data-manual", "AUTO")
    chip.click()
    expect(popover.locator("#availability-reason")).to_have_text("Now: AVAILABLE because active hours")
