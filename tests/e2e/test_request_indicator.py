"""Chromium request feedback; route mocks never send commands to the daemon."""

from playwright.sync_api import expect


def test_pause_failure_and_read_failure(page, dashboard_url):
    page.goto(dashboard_url, wait_until='domcontentloaded')
    page.wait_for_function('Boolean(window.htmx)')
    pause = page.locator('[data-pause-all-confirm]')
    page.locator('#automation-menu-toggle').click()
    page.locator('[data-action="pause-all"]').click()
    page.route('**/daemon/pause', lambda route: route.fulfill(
        status=503, json={'detail': '<b>Redis unavailable</b>'},
    ))
    pause.click()
    page.locator('[data-pause-all-cancel]').click()
    alert = page.locator('#request-error')
    expect(alert).to_contain_text('HTTP 503: <b>Redis unavailable</b>')
    expect(alert).to_contain_text('Outcome not confirmed; refresh state before deciding')
    expect(alert.locator('b')).to_have_count(0)
    expect(alert.get_by_role('button', name='Refresh state')).to_be_visible()
    alert.get_by_role('button', name='Dismiss').click()
    expect(alert).to_be_hidden()

    page.route('**/partials/repo-list', lambda route: route.fulfill(status=503, body='Read failed'))
    with page.expect_response('**/partials/repo-list') as response:
        page.evaluate("htmx.trigger(document.body, 'repo:history_updated')")
    assert response.value.status == 503
    expect(alert).to_be_hidden()

    page.route('**/daemon/pause', lambda route: route.abort('failed'))
    page.locator('#automation-menu-toggle').click()
    page.locator('[data-action="pause-all"]').click()
    pause.click()
    page.locator('[data-pause-all-cancel]').click()
    expect(alert).to_contain_text('Transport error. Outcome not confirmed')
    alert.get_by_role('button', name='Dismiss').click()
    # The same ownership marker is used by Reset and inline Settings feedback.
    pause.evaluate("el => el.setAttribute('data-own-error', '')")
    page.locator('#automation-menu-toggle').click()
    page.locator('[data-action="pause-all"]').click()
    page.evaluate("document.body.addEventListener('htmx:sendError', () => window.ownedErrorDone = true, {once:true})")
    pause.click()
    page.wait_for_function('window.ownedErrorDone === true')
    page.locator('[data-pause-all-cancel]').click()
    expect(alert).to_be_hidden()


def test_overlapping_requests_keep_indicator_inline(page, dashboard_url):
    page.set_viewport_size({'width': 390, 'height': 844})
    page.goto(dashboard_url, wait_until='domcontentloaded')
    page.wait_for_function('Boolean(window.htmx)')
    page.route('**/indicator-read', lambda route: None)
    page.route('**/indicator-save', lambda route: None)
    page.evaluate("""() => {
        for (const [id, verb] of [['read', 'get'], ['save', 'post']]) {
            const button = document.createElement('button');
            button.id = 'indicator-' + id;
            button.setAttribute('hx-' + verb, '/indicator-' + id);
            button.setAttribute('hx-swap', 'none');
            button.textContent = id; document.querySelector('main').append(button);
            htmx.process(button);
        }
    }""")
    page.locator('#indicator-read').click()
    spinner = page.locator('nav #global-spinner')
    expect(spinner).to_contain_text('Loading…')
    expect(spinner).to_have_class('htmx-indicator global-spinner is-active')
    expect(spinner).not_to_have_css('position', 'fixed')
    page.locator('#indicator-save').click()
    expect(spinner).to_contain_text('Saving…')
    page.evaluate("htmx.trigger('#indicator-save', 'htmx:abort')")
    expect(spinner).to_contain_text('Loading…')
    expect(spinner).to_have_class('htmx-indicator global-spinner is-active')
    page.evaluate("htmx.trigger('#indicator-read', 'htmx:abort')")
    expect(spinner).to_have_class('htmx-indicator global-spinner')
