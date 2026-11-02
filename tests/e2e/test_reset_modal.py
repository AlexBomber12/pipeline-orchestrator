"""Reset interaction with mocked reads/writes; never invokes reset controls."""

import json

import pytest
from playwright.sync_api import Page, expect
from src.web import app as web_app


@pytest.fixture
def reset_dialog(page: Page, dashboard_url: str, testbed_slug: str):
    slug = testbed_slug
    modal = web_app.templates.env.get_template('components/reset_modal.html').module.reset_modal(
        dict(repo_slug=slug, task_id='PR-490', retry_count=2), dict(number=444),
    )
    diagnostic = web_app.templates.env.get_template('components/diagnostic_panel.html').module.diagnostic_panel(
        dict(repo_slug=slug, task_id='PR-490', retry_count=2), 3, True,
    )
    # Catch every reset POST before navigation, including accidental duplicate requests.
    posts = []
    outcome = dict(status=200, body=json.dumps(dict(frontmatter_pushed=True)))

    def post(route):
        posts.append(route.request.url)
        if outcome.get('abort'):
            route.abort('failed')
        else:
            route.fulfill(status=outcome['status'], body=outcome['body'], content_type='application/json')

    page.route('**/api/reset-task/**', post)
    page.goto(f'{dashboard_url}/repo/{slug}', wait_until='domcontentloaded')
    page.wait_for_function('Boolean(window.htmx)')
    reads = []

    def read(route):
        reads.append(route.request.url)
        body = str(diagnostic) if route.request.url.endswith('/tasks') and not posts else '<p>Current state</p>'
        route.fulfill(body=body, content_type='text/html')

    page.route(f'**/partials/repo/{slug}', read)
    page.route(f'**/repos/{slug}/tasks', read)
    page.route(f'**/repos/{slug}/tasks/PR-490/reset-confirm',
               lambda route: route.fulfill(body=str(modal), content_type='text/html'))
    # Use the real diagnostic opener inside the queue, including its production target.
    page.evaluate('''slug => {
        const queue = [...document.querySelectorAll('[hx-get]')].find(
            el => el.getAttribute('hx-get') === '/repos/' + slug + '/tasks');
        queue.closest('details').open = true;
    }''', slug)
    opener = page.locator('[data-diagnostic-panel] button[hx-get]')
    opener.evaluate("button => { button.id = 'reset-opener'; }")
    opener.click()
    expect(page.locator('[data-reset-confirm-input]')).to_be_focused()
    return page, slug, posts, outcome, reads


def _submit(dialog):
    page, slug, posts, outcome, reads = dialog
    page.locator('[data-reset-confirm-input]').fill(slug)
    # Two clicks in one task also exercise the synchronous duplicate-request guard.
    page.locator('[data-reset-submit]').evaluate('(button) => { button.click(); button.click(); }')


def test_confirmed_reset_refreshes_without_navigation(reset_dialog):
    page, slug, posts, _, reads = reset_dialog
    navigations = []
    page.on('framenavigated', lambda frame: navigations.append(frame.url))
    _submit(reset_dialog)
    expect(page.locator('[data-reset-modal]')).to_have_count(0)
    page.wait_for_function('''slug => [...document.querySelectorAll('[hx-get]')]
        .some(el => el.getAttribute('hx-get') === '/repos/' + slug + '/tasks' &&
                    el.textContent.includes('Current state'))''', arg=slug)
    assert any(url.endswith(f'/repos/{slug}/tasks') for url in reads)
    assert any(url.endswith(f'/partials/repo/{slug}') for url in reads)
    assert len(posts) == 1 and posts[0].endswith('?close_orphan_pr=true')
    assert not navigations


@pytest.mark.parametrize('status,body,abort,message', [
    (409, {'error': 'repo ERROR task changed'}, False, 'repo ERROR task changed'),
    (503, {'error': 'service unavailable'}, False, 'service unavailable'),
    (200, {}, True, 'network error'),
    (200, {'frontmatter_pushed': True, 'partial_reset': True, 'error': 'git failed'}, False, 'partly applied'),
    (200, {}, False, 'Reset not confirmed'),
    (200, 'invalid JSON', False, 'Reset not confirmed'),
])
def test_failure_retains_confirmation(reset_dialog, status, body, abort, message):
    page, slug, posts, outcome, reads = reset_dialog
    outcome.update(status=status, body=json.dumps(body), abort=abort)
    _submit(reset_dialog)
    expect(page.locator('[role="alert"]')).to_contain_text(message)
    expect(page.locator('[data-reset-confirm-input]')).to_have_value(slug)
    expect(page.locator('[data-reset-submit]')).to_be_enabled()
    expect(page.locator('[data-reset-cancel]')).to_be_enabled()
    assert len(posts) == 1
    # Wait for the actual replacement, not merely an issued refresh request.
    expect(page.locator('[data-diagnostic-panel]')).to_have_count(0)
    expect(page.locator('#repo-reset-dialog [data-reset-modal]')).to_be_visible()
    expect(page.locator('[data-reset-error]')).to_contain_text(message)
    expect(page.locator('[data-reset-confirm-input]')).to_have_value(slug)
    outcome.update(status=200, body=json.dumps(dict(frontmatter_pushed=True)), abort=False)
    page.locator('[data-reset-submit]').click()
    expect(page.locator('[data-reset-modal]')).to_have_count(0)
    assert len(posts) == 2


def test_escape_and_focus_trap(reset_dialog):
    page, _, posts, _, _ = reset_dialog
    page.keyboard.press('Shift+Tab')
    expect(page.locator('[data-reset-modal] summary')).to_be_focused()
    page.keyboard.press('Shift+Tab')
    expect(page.locator('[data-reset-cancel]')).to_be_focused()
    page.keyboard.press('Tab')
    expect(page.locator('[data-reset-modal] summary')).to_be_focused()
    page.keyboard.press('Tab')
    expect(page.locator('[data-reset-confirm-input]')).to_be_focused()
    page.keyboard.press('Escape')
    expect(page.locator('[data-reset-modal]')).to_have_count(0)
    expect(page.locator('#reset-opener')).to_be_focused()
    assert not posts


def test_active_request_cannot_be_abandoned(reset_dialog):
    page, slug, posts, _, _ = reset_dialog
    # Hold the response while testing cancellation and duplicate clicks.
    pending = []
    page.route('**/api/reset-task/**', lambda route: pending.append(route))
    _submit(reset_dialog)
    expect(page.locator('[data-reset-cancel]')).to_be_disabled()
    page.keyboard.press('Escape')
    expect(page.locator('[data-reset-modal]')).to_be_visible()
    expect(page.locator('[data-reset-submit]')).to_be_disabled()
    assert len(pending) == 1
    pending[0].fulfill(body='{"frontmatter_pushed":true}', content_type='application/json')
    expect(page.locator('[data-reset-modal]')).to_have_count(0)
    assert not posts  # The held route overrides the default mock, never the real endpoint.


def test_changed_consequences_require_new_confirmation(reset_dialog):
    page, _, posts, outcome, _ = reset_dialog
    outcome.update(status=503, body='{"partial_reset":true,"error":"git failed"}')
    page.route('**/reset-confirm', lambda route: route.fulfill(body='<p>Changed state</p>'))
    _submit(reset_dialog)
    expect(page.locator('[role="alert"]')).to_contain_text('Close and reopen Reset')
    expect(page.locator('[data-reset-submit]')).to_be_disabled()
    expect(page.locator('[data-reset-cancel]')).to_be_enabled()
    assert len(posts) == 1
