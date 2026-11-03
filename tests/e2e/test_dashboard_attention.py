"""Chromium exercises the persistent empty shell and HTMX attention refresh."""
from playwright.sync_api import expect


def test_attention_refresh(page, dashboard_url):
    page.route('**/api/repos/*/events', lambda route: route.abort())
    page.goto(dashboard_url, wait_until='load')
    page.wait_for_function("typeof htmx !== 'undefined'")
    panel = page.locator('#alerts-panel')
    fragment = ''
    page.route('**/partials/alerts', lambda route: route.fulfill(
        status=200, content_type='text/html', body=fragment))

    def refresh():
        with page.expect_response('**/partials/alerts') as response:
            page.evaluate("htmx.trigger(document.body, 'repo:state_change')")
        assert response.value.status == 200

    refresh()
    expect(panel).to_be_empty()
    fragment = '<section><h2>Needs your action</h2><p data-alert-row>Decision needed</p></section>'
    refresh()
    expect(panel.locator('[data-alert-row]')).to_have_text('Decision needed')
    assert page.evaluate("""() => {
        const stats = document.querySelector('#status-bar');
        const alerts = document.querySelector('#alerts-panel');
        const grid = document.querySelector('#repo-grid');
        return !!(stats.compareDocumentPosition(alerts) & Node.DOCUMENT_POSITION_FOLLOWING)
            && !!(alerts.compareDocumentPosition(grid) & Node.DOCUMENT_POSITION_FOLLOWING);
    }""")
    fragment = ''
    refresh()
    expect(panel).to_be_empty()
