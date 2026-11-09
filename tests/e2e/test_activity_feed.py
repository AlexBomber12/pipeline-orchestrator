"""Responsive feed placement and event/poll refresh in the light theme."""
from pathlib import Path

from playwright.sync_api import expect


def test_activity_feed(page, dashboard_url):
    # Layout needs Tailwind on the first page load, even if its CDN is unavailable.
    # Use the actual CDN runtime so responsive utilities are still exercised.
    page.route('https://cdn.tailwindcss.com/**', lambda route: route.fulfill(
        path=Path(__file__).parent / 'assets/tailwind-3.4.17.js',
        content_type='application/javascript'))
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=Path(__file__).parent / 'assets/htmx-2.0.4.min.js',
        content_type='application/javascript'))
    page.route('**/api/repos/*/events', lambda route: route.abort())
    page.emulate_media(color_scheme='light')
    page.set_viewport_size(dict(width=1440, height=900))
    page.goto(dashboard_url, wait_until='load')
    page.wait_for_function("Boolean(window.htmx)")
    page.wait_for_function("""getComputedStyle(document.querySelector('[data-dashboard-layout]'))
        .gridTemplateColumns.split(' ').length === 2""")
    feed = page.locator('#activity-feed')
    expect(page.locator('html')).to_have_attribute('data-theme', 'light')
    expect(feed).to_be_visible()
    grid = page.locator('#repo-grid').bounding_box()
    box = feed.bounding_box()
    assert box['x'] >= grid['x'] + grid['width']
    page.set_viewport_size(dict(width=390, height=844))
    grid = page.locator('#repo-grid').bounding_box()
    box = feed.bounding_box()
    assert box['y'] >= grid['y'] + grid['height']
    assert box['x'] + box['width'] <= 390

    fragment = '<p class="text-gray-300" data-feed-test>Fresh activity</p>'
    page.route('**/partials/activity-feed', lambda route: route.fulfill(
        status=200, content_type='text/html', body=fragment))
    with page.expect_response('**/partials/activity-feed'):
        page.evaluate("htmx.trigger(document.body, 'repo:event_log_append')")
    expect(feed.locator('[data-feed-test]')).to_have_text('Fresh activity')
    colors = feed.locator('[data-feed-test]').evaluate(
        "node => [getComputedStyle(node).color, getComputedStyle(document.body).backgroundColor]")
    assert colors[0] != colors[1]
    fragment = '<p data-feed-test>Polled activity</p>'
    with page.expect_response('**/partials/activity-feed', timeout=35000):
        pass
    expect(feed.locator('[data-feed-test]')).to_have_text('Polled activity')
