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
        elif key == 'metrics':
            body = web_app.templates.get_template('components/pr_metrics.html').render(
                repo=repo, metrics_records=[{
                    'task_id': 'PR-505', 'duration_text': '1m', 'fix_iterations': 1,
                    'exit_reason_label': 'merged', 'exit_reason_classes': '',
                    'invocations': [{
                        'plugin_id': 'codex', 'phase_label': 'FIX', 'fix_iteration': 1,
                        'model_override_label': 'fixture-model',
                        'reasoning_effort_override_label': 'high',
                    }],
                }],
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
    # Hash navigation reuses the loaded panel within the current document.
    page.goto(url + '#tasks', wait_until='load')
    expect(tasks).to_have_attribute('aria-selected', 'true')
    expect(page.locator('#repo-panel-tasks')).to_contain_text('tasks read 1')
    reads_before_reload = reads.copy()
    # Explicitly reload: goto with only a new hash does not reload the document.
    with page.expect_response('**/repos/example__tabs/tasks'):
        page.reload(wait_until='load')
    expect(tasks).to_have_attribute('aria-selected', 'true')
    expect(page.locator('#repo-panel-tasks')).to_contain_text('tasks read 2')
    assert reads['tasks'] == reads_before_reload['tasks'] + 1
    for key in ('events', 'cli-log', 'cancellations'):
        assert reads[key] == reads_before_reload[key]
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


def test_runs_lazy_load_disclosure_and_live_refresh(page, repo_tabs):
    url, reads = repo_tabs
    page.goto(url, wait_until='load')
    assert reads['metrics'] == 0
    runs = page.get_by_role('tab', name='Runs')
    with page.expect_response('**/partials/repo/example__tabs/metrics'):
        runs.click()
    panel = page.locator('#repo-panel-runs')
    expect(panel).to_contain_text('PR-505')
    details = panel.locator('details')
    expect(details).not_to_have_attribute('open', '')
    expect(panel.get_by_text('Model override: fixture-model')).not_to_be_visible()
    details.locator('summary').click()
    expect(panel.get_by_text('Model override: fixture-model')).to_be_visible()
    page.get_by_role('tab', name='Overview').click()
    runs.click()
    assert reads['metrics'] == 1
    expect(details).to_have_attribute('open', '')
    with page.expect_response('**/partials/repo/example__tabs/metrics'):
        page.evaluate("htmx.trigger(document.body, 'repo:pr_metrics_update')")
    expect(panel).to_contain_text('PR-505')
    assert reads['metrics'] == 2


@pytest.mark.parametrize('status, template, message, dismiss', [
    (200, 'upload_success.html', 'Tasks uploaded', 'Dismiss upload feedback'),
    (422, 'upload_error.html', 'Invalid task header', 'Dismiss upload error'),
])
def test_repo_page_upload_feedback(page, repo_tabs, status, template, message, dismiss):
    url, _ = repo_tabs
    feedback = web_app.templates.get_template('components/' + template).render(
        message_lines=[message], uploaded_files=['PR-507.md'] if status == 200 else [],
        validation_errors=[],
    )
    requests = []

    def upload(route):
        requests.append(route.request)
        route.fulfill(status=status, body=feedback, content_type='text/html', headers={
            'HX-Retarget': '#upload-feedback-example__tabs', 'HX-Reswap': 'innerHTML',
        })

    page.route('**/repos/example__tabs/upload-tasks', upload)
    page.goto(url + '#tasks', wait_until='load')
    panel = page.locator('#repo-panel-tasks')
    expect(panel.locator('label')).to_have_text('Upload tasks')
    with page.expect_file_chooser() as chooser:
        panel.locator('label').click()
    with page.expect_response('**/repos/example__tabs/upload-tasks'):
        chooser.value.set_files({
            'name': 'PR-507.md', 'mimeType': 'text/markdown', 'buffer': b'fixture task',
        })
    card = panel.locator('[data-upload-feedback-card]')
    expect(card).to_contain_text(message)
    expect(panel.locator('input[type=file]')).to_be_enabled()
    assert len(requests) == 1 and requests[0].method == 'POST'
    assert 'multipart/form-data' in requests[0].headers['content-type']
    assert b'PR-507.md' in requests[0].post_data_buffer
    assert b'fixture task' in requests[0].post_data_buffer
    page.get_by_role('tab', name='Overview').click()
    page.get_by_role('tab', name='Tasks').click()
    expect(card).to_contain_text(message)
    with page.expect_response('**/repos/example__tabs/tasks'):
        page.evaluate("htmx.trigger(document.body, 'repo:history_updated')")
    expect(card).to_contain_text(message)
    card.get_by_role('button', name=dismiss).click()
    expect(card).to_have_count(0)
