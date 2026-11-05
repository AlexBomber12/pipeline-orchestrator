"""Repository tabs using production HTML and HTMX with deterministic reads."""
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest
from playwright.sync_api import expect
from src.models import RepoState
from src.web import app as web_app


@pytest.fixture
def repo_tabs(page, dashboard_url):
    repo = RepoState(name='example__tabs', url='https://github.com/example/tabs')
    events = [{'event': f'Event {i}'} for i in range(80)]
    html = web_app.templates.get_template('repo.html').render(
        repo=repo, events=events, coders=[], effective_coder=None,
        page_rendered_at=datetime.now(timezone.utc).isoformat(),
    )
    reads = Counter()
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=Path(__file__).parent / 'assets/htmx-2.0.4.min.js',
        content_type='application/javascript',
    ))
    # Drive the actual EventSource relay without provider or Redis dependencies.
    page.add_init_script('''window.EventSource = class {
        constructor(url) {
            this.listeners = {};
            if (url === '/api/repos/example__tabs/events') window.repoStream = this;
        }
        addEventListener(name, handler) { this.listeners[name] = handler; }
        close() {}
    };''')

    def fragment(route):
        key = route.request.url.rsplit('/', 1)[-1]
        reads[key] += 1
        if key == 'events':
            body = web_app.templates.get_template('components/event_list.html').render(
                repo=repo, events=events, oob=True,
            )
        else:
            body = f'<p>{key} read {reads[key]}</p>'
        route.fulfill(body=body, content_type='text/html')

    page.route('**/partials/repo/example__tabs', fragment)
    page.route('**/partials/repo/example__tabs/*', fragment)
    page.route('**/repos/example__tabs/tasks', fragment)
    url = dashboard_url + '/repo/example__tabs'
    page.route(url, lambda route: route.fulfill(body=html, content_type='text/html'))
    return url, reads


def test_lazy_tabs_keyboard_and_hash(page, repo_tabs):
    url, reads = repo_tabs
    page.goto(url, wait_until='load')
    overview = page.get_by_role('tab', name='Overview')
    events = page.get_by_role('tab', name='Events')
    tasks = page.get_by_role('tab', name='Tasks')
    expect(overview).to_have_attribute('aria-selected', 'true')
    expect(tasks.locator('[data-tab-count]')).to_have_text('—')
    assert not reads['tasks'] and not reads['cli-log'] and not reads['cancellations']
    assert page.locator('#repo-event-log').count() == 0
    overview.focus()
    overview.press('ArrowRight')
    expect(events).to_be_focused()
    expect(events).to_have_attribute('aria-selected', 'false')
    with page.expect_response('**/partials/repo/example__tabs/events'):
        events.press('Enter')
    expect(page.locator('#repo-event-log')).to_be_visible()
    expect(page).to_have_url(url + '#events')
    events.press('ArrowRight')
    tasks.press('Space')
    expect(page.locator('#repo-panel-tasks')).to_contain_text('tasks read 1')
    overview.click()
    events.click()
    assert reads['events'] == 1
    tasks.click()
    expect(page.locator('#repo-panel-tasks')).to_contain_text('tasks read 1')
    coder = page.get_by_role('tab', name='Coder log')
    coder.click()
    expect(page.locator('#repo-panel-coder-log')).to_contain_text('cli-log read 1')
    overview.click()
    coder.click()
    assert reads['cli-log'] == 1
    cancel = page.get_by_role('tab', name='Cancellations')
    cancel.click()
    expect(page.locator('#repo-panel-cancellations')).to_contain_text('cancellations read 1')
    overview.click()
    cancel.click()
    expect(page.locator('#repo-panel-cancellations')).to_contain_text('cancellations read 2')
    cancel.press('End')
    expect(coder).to_be_focused()
    coder.press('ArrowRight')
    expect(overview).to_be_focused()
    overview.press('ArrowLeft')
    expect(coder).to_be_focused()
    coder.press('Home')
    expect(overview).to_be_focused()
    # Reload restores the selected tab and only loads that lazy panel.
    page.goto(url + '#tasks', wait_until='load')
    expect(tasks).to_have_attribute('aria-selected', 'true')
    expect(page.locator('#repo-panel-tasks')).to_contain_text('tasks read 2')
    page.evaluate("location.hash = 'events'")
    expect(events).to_have_attribute('aria-selected', 'true')
    expect(page.locator('#repo-event-log')).to_be_visible()


def test_hidden_panels_keep_subscriptions_and_event_scroll(page, repo_tabs):
    url, reads = repo_tabs
    with page.expect_response('**/partials/repo/example__tabs/events'):
        page.goto(url + '#events', wait_until='load')
    wrapper = page.locator('#event-list-wrapper-example__tabs')
    expect(wrapper).not_to_have_class(re.compile(r'\bhtmx-request\b'))
    # Explicit height keeps the scroll assertion independent of CDN availability.
    wrapper.evaluate("node => { node.style.height = '100px'; node.style.overflow = 'auto'; node.scrollTop = 120; }")
    assert wrapper.evaluate('node => node.scrollTop') == 120
    page.get_by_role('tab', name='Tasks').click()
    expect(page.locator('#repo-panel-tasks')).to_contain_text('tasks read 1')
    page.get_by_role('tab', name='Overview').click()
    page.get_by_role('tab', name='Overview').focus()
    for i, event in enumerate(['history_updated', 'state_change', 'event_log_append']):
        page.evaluate('''([name, stamp]) => {
            window.repoStream.listeners[name]({data: JSON.stringify({timestamp: stamp})});
        }''',
                      [event, f'2099-01-01T00:00:0{i}Z'])
        if event != 'event_log_append':
            expect(page.locator('#repo-panel-tasks')).to_contain_text(f'tasks read {i + 2}')
    expect(page.get_by_role('tab', name='Overview')).to_be_focused()
    expect(wrapper).not_to_have_class(re.compile(r'\bhtmx-request\b'))
    assert reads['events'] >= 1
    page.get_by_role('tab', name='Events').click()
    assert wrapper.evaluate('node => node.scrollTop') == 120
    expect(page.get_by_role('tab', name='Events').locator('[data-tab-count]')).to_have_text('80')
    with page.expect_response('**/partials/repo/example__tabs/events'):
        page.evaluate("htmx.trigger(document.body, 'repo:event_log_append')")
    assert wrapper.evaluate('node => node.scrollTop') == 120
