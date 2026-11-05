"""Responsive shared header and stats contracts; GHA runs Chromium."""

import pytest
from playwright.sync_api import expect


@pytest.mark.parametrize('width', [360, 390, 414])
@pytest.mark.parametrize('path', ['/', '/repo/AlexBomber12__pipeline-orchestrator-testbed', '/settings'])
def test_mobile_header(page, dashboard_url, width, path):
    page.set_viewport_size({'width': width, 'height': 900})
    response = page.goto(dashboard_url.rstrip('/') + path)
    assert response.status == 200
    page.wait_for_function("getComputedStyle(document.querySelector('#header-menu summary')).width === '44px'")
    assert page.evaluate('document.documentElement.scrollWidth') == width
    title = page.locator('h1').first.bounding_box()
    automation = page.locator('#automation-menu-toggle').bounding_box()
    menu = page.locator('#header-menu summary')
    box = menu.bounding_box()
    assert box['width'] == box['height'] == 44
    for element in (title, automation):
        assert element['y'] < box['y'] + box['height']
        assert element['y'] + element['height'] > box['y']
    if path == '/':
        stats = page.locator('#status-bar .grid')
        expect(stats).to_be_visible()
        assert stats.evaluate("el => getComputedStyle(el).gridTemplateColumns.split(' ').length") == 2
        cells = stats.locator(':scope > div').all()
        assert len(cells) == 4
        assert cells[0].bounding_box()['y'] == cells[1].bounding_box()['y']
        assert cells[2].bounding_box()['y'] > cells[0].bounding_box()['y']
    menu.focus()
    page.keyboard.press('Enter')
    expect(page.locator('#header-theme')).to_be_visible()
    assert page.evaluate('document.documentElement.scrollWidth') == width
    page.keyboard.press('Tab')
    expect(page.locator('#header-menu a').first).to_be_focused()
    page.keyboard.press('Escape')
    expect(menu).to_be_focused()
    expect(page.locator('#header-menu')).not_to_have_attribute('open', '')
