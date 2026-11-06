"""Event filters against production templates and actual HTMX inner swaps."""
from datetime import datetime, timezone
from pathlib import Path

import pytest
from playwright.sync_api import expect
from src.models import RepoState
from src.web import app as web_app


@pytest.fixture
def event_filters(page, dashboard_url):
    events = [
        {'tier': 'MERGE', 'message': 'status:DONE was not written', 'severity': 'error'},
        {'tier': 'WATCH', 'message': 'waiting for CI', 'severity': 'info'},
        {'event': '[MERGE] historical incident'},
        {'event': 'FIX -> ERROR: retained', 'kind': 'transition'},
        {'tier': 'ERROR', 'message': 'error_message cleared', 'severity': 'ok'},
        {'tier': 'INFRA', 'message': 'retry delayed', 'severity': 'warn'},
    ] + [{'tier': 'WATCH', 'message': f'waiting {i}', 'severity': 'info'} for i in range(50)]
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=Path(__file__).parent / 'assets/htmx-2.0.4.min.js',
        content_type='application/javascript',
    ))
    page.add_init_script('''window.EventSource = class {
        constructor() { this.listeners = {}; window.repoStream = this; }
        addEventListener(name, handler) { this.listeners[name] = handler; }
        close() {}
    };''')

    def document(route):
        slug = route.request.url.split('/repo/')[1]
        repo = RepoState(name=slug, url='https://github.com/example/filters')
        html = web_app.templates.get_template('repo.html').render(
            repo=repo, events=events, coders=[], effective_coder=None,
            page_rendered_at=datetime.now(timezone.utc).isoformat(),
        )
        route.fulfill(body=html, content_type='text/html')

    def fragment(route):
        slug = route.request.url.split('/repo/')[1].split('/')[0]
        body = web_app.templates.get_template('components/event_list.html').render(
            repo={'name': slug}, events=events, oob=True,
        )
        route.fulfill(body=body, content_type='text/html')

    page.route('**/repo/example__filters*', document)
    page.route('**/partials/repo/example__filters*', lambda route: route.fulfill(body=''))
    page.route('**/partials/repo/example__filters*/events', fragment)
    url = dashboard_url + '/repo/example__filters#events'
    page.goto(url, wait_until='load')
    expect(page.get_by_role('button', name='Problems only')).to_be_visible()
    return url, events


def test_severity_unknown_and_persistence(page, event_filters):
    url, _ = event_filters
    def row(text):
        return page.locator('li[data-event-severity]').filter(has_text=text)
    problems = page.get_by_role('button', name='Problems only')
    problems.focus()
    problems.press('Space')
    expect(problems).to_have_attribute('aria-pressed', 'true')
    expect(row('status:DONE')).to_be_visible()
    expect(row('retry delayed')).to_be_visible()
    expect(row('waiting for CI')).to_be_hidden()
    expect(row('error_message cleared')).to_be_hidden()
    expect(row('FIX -> ERROR')).to_be_visible()
    expect(row('FIX -> ERROR')).to_contain_text('derived from transition')
    expect(row('historical incident')).to_be_hidden()
    expect(page.get_by_text('1 events without recorded severity hidden.', exact=True)).to_be_visible()
    # Native buttons participate in Tab navigation and support Enter and Space.
    page.keyboard.press('Tab')
    expect(page.get_by_role('button', name='Event', exact=True)).to_be_focused()
    show = page.get_by_role('button', name='Show them', exact=True)
    for _ in range(5):
        page.keyboard.press('Tab')
    expect(show).to_be_focused()
    show.press('Enter')
    expect(row('historical incident')).to_be_visible()
    merge = page.get_by_role('button', name='MERGE', exact=True)
    merge.focus()
    merge.press('Space')
    expect(merge).to_have_attribute('aria-pressed', 'false')
    expect(row('status:DONE')).to_be_hidden()
    with page.expect_response('**/partials/repo/example__filters/events'):
        page.evaluate("window.repoStream.listeners.event_log_append({data: '{}'})")
    expect(merge).to_be_focused()
    expect(row('historical incident')).to_be_hidden()
    expect(row('status:DONE')).to_be_hidden()
    page.reload(wait_until='load')
    expect(problems).to_have_attribute('aria-pressed', 'true')
    expect(merge).to_have_attribute('aria-pressed', 'false')
    expect(row('status:DONE')).to_be_hidden()
    merge.click()
    expect(row('historical incident')).to_be_visible()
    page.goto(url.replace('example__filters', 'example__filters_other'), wait_until='load')
    expect(problems).to_have_attribute('aria-pressed', 'false')
    expect(row('waiting for CI')).to_be_visible()


def test_refresh_preserves_scroll_and_chip_identity(page, event_filters):
    _, events = event_filters
    wrapper = page.locator('[id^="event-list-wrapper-"]')
    wrapper.evaluate("node => { node.style.height = '100px'; node.style.overflow = 'auto'; node.scrollTop = 120; }")
    merge = page.get_by_role('button', name='MERGE', exact=True)
    merge.focus()
    merge.evaluate('node => window.originalChip = node')
    events.append({'tier': 'NEW', 'message': 'new category', 'severity': 'info'})
    with page.expect_response('**/partials/repo/example__filters/events'):
        page.evaluate("htmx.trigger(document.body, 'repo:history_updated')")
    expect(page.get_by_role('button', name='NEW', exact=True)).to_be_visible()
    expect(merge).to_be_focused()
    assert merge.evaluate('node => node === window.originalChip')
    assert wrapper.evaluate('node => node.scrollTop') == 120
    page.set_viewport_size({'width': 375, 'height': 800})
    assert merge.bounding_box()['height'] >= 44
