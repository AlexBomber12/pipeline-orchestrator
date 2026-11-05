"""Dashboard refresh must not interrupt native repository action menus."""
from pathlib import Path

import pytest
from playwright.sync_api import expect
from src.models import PipelineState, RepoState

from tests.web.templates.test_state_styles_canonical import _render_repo_cards


@pytest.mark.parametrize('trigger', ['repo:state_change', 'repo:history_updated', 'poll'])
@pytest.mark.parametrize('action', ['summary', 'a', 'button', 'input[type=file]'])
@pytest.mark.parametrize('dismiss', ['summary', 'outside', 'focus'])
def test_open_repo_menu_defers_refresh(page, dashboard_url, trigger, action, dismiss):
    script = Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js'
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=script, content_type='application/javascript',
    ))
    page.route('**/api/repos/*/events', lambda route: route.abort())
    page.clock.install()
    page.goto(dashboard_url, wait_until='load')
    page.wait_for_function("typeof htmx !== 'undefined'")
    grid = page.locator('#repo-grid')
    repo = RepoState(name='example__menu', url='https://github.com/example/menu', state=PipelineState.CODING)
    grid.evaluate('(node, html) => node.innerHTML = html', _render_repo_cards(repo))
    card = grid.locator('[data-repo-card]').first
    menu = card.locator('[data-card-menu]')
    menu.locator('summary').press('Enter')
    control = menu.locator(action).first
    control.focus()
    expect(control).to_be_focused()
    page.evaluate('''() => {
        window.retainedMenu = document.activeElement.closest('[data-card-menu]');
        window.retainedAction = document.activeElement;
        window.refreshAttempts = 0;
        document.querySelector('#repo-grid').addEventListener('htmx:beforeSwap', () => {
            window.refreshAttempts++;
        });
    }''')
    # The response represents activity elsewhere in the dashboard. Its marker
    # proves that a later refresh really applies after the operator closes the menu.
    cards = grid.evaluate('''node => {
        const copy = node.cloneNode(true);
        copy.querySelectorAll('[data-card-menu]').forEach(menu => menu.removeAttribute('open'));
        return copy.innerHTML;
    }''') + '<p data-refresh-marker>Updated repository data</p>'
    page.route('**/partials/repo-list', lambda route: route.fulfill(
        status=200, content_type='text/html', body=cards,
        headers={'X-Observation-Available': 'true',
                 'X-Observation-Time': '2026-11-04T12:34:00+00:00'},
    ))

    def refresh():
        if trigger == 'poll':
            page.clock.fast_forward(30_000)
        else:
            page.evaluate('(name) => htmx.trigger(document.body, name)', trigger)

    refresh()
    page.wait_for_function('window.refreshAttempts === 1')
    expect(menu).to_have_attribute('open', '')
    expect(control).to_be_focused()
    assert page.evaluate('''() => window.retainedMenu.isConnected &&
        window.retainedAction === document.activeElement''')
    expect(grid.locator('[data-refresh-marker]')).to_have_count(0)
    if dismiss == 'summary':
        menu.locator('summary').press('Enter')
    elif dismiss == 'outside':
        page.locator('h1').first.click()
    else:
        card.locator('[data-card-name]').focus()
    expect(menu).not_to_have_attribute('open', '')
    refresh()
    expect(grid.locator('[data-refresh-marker]')).to_have_text('Updated repository data')
    assert page.evaluate('!window.retainedMenu.isConnected')


@pytest.mark.parametrize('status', [204, 500])
def test_repo_menu_action_completion(page, dashboard_url, status):
    script = Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js'
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=script, content_type='application/javascript',
    ))
    page.route('**/api/repos/*/events', lambda route: route.abort())
    page.clock.install()
    page.goto(dashboard_url, wait_until='load')
    page.wait_for_function("typeof htmx !== 'undefined'")
    grid = page.locator('#repo-grid')
    repo = RepoState(name='example__menu', url='https://github.com/example/menu', state=PipelineState.CODING)
    grid.evaluate('(node, html) => { node.innerHTML = html; htmx.process(node); }', _render_repo_cards(repo))
    menu = grid.locator('[data-card-menu]')
    menu.locator('summary').press('Enter')
    repo.user_paused = True
    page.route('**/partials/repo-list', lambda route: route.fulfill(
        status=200, content_type='text/html', body=_render_repo_cards(repo),
        headers={'X-Observation-Available': 'true'},
    ))
    page.route('**/repos/example__menu/pause', lambda route: route.fulfill(status=status))
    # A pending SSE refresh must not be needed to replace Pause with Resume.
    with page.expect_response('**/repos/example__menu/pause'):
        menu.get_by_role('button', name='Pause daemon', exact=True).click()
    if status == 204:
        expect(menu).not_to_have_attribute('open', '')
        # Closed native details hide their controls from default role queries.
        # Wait for the replacement before reopening so the open-menu guard
        # cannot defer the refresh this assertion is intended to verify.
        expect(grid.get_by_role('button', name='Resume daemon', exact=True, include_hidden=True)).to_have_count(1)
        expect(grid.get_by_role('button', name='Pause daemon', exact=True, include_hidden=True)).to_have_count(0)
        menu.locator('summary').press('Enter')
        expect(menu.get_by_role('button', name='Resume daemon', exact=True)).to_be_visible()
    else:
        expect(menu).to_have_attribute('open', '')
        expect(menu.get_by_role('button', name='Pause daemon', exact=True)).to_be_visible()


def test_successful_upload_keeps_feedback(page, dashboard_url):
    from tests.web.templates.test_state_styles_canonical import templates

    script = Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js'
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=script, content_type='application/javascript',
    ))
    page.route('**/api/repos/*/events', lambda route: route.abort())
    page.clock.install()
    page.goto(dashboard_url, wait_until='load')
    page.wait_for_function("typeof htmx !== 'undefined'")
    grid = page.locator('#repo-grid')
    repo = RepoState(name='example__menu', url='https://github.com/example/menu', state=PipelineState.CODING)
    grid.evaluate('''(node, html) => {
        node.innerHTML = html;
        node.querySelector('form').setAttribute('hx-target', '#upload-feedback-test');
        node.insertAdjacentHTML('beforeend', '<div id="upload-feedback-test"></div>');
        htmx.process(node);
    }''', _render_repo_cards(repo))
    refreshes = []

    def refresh(route):
        refreshes.append(route.request.url)
        route.fulfill(status=200, content_type='text/html', body=_render_repo_cards(repo))

    page.route('**/partials/repo-list', refresh)
    feedback_html = templates.get_template('components/upload_success.html').render(
        message_lines=['Tasks accepted.'], uploaded_files=['PR-495.md'],
        commit_subject='Accept PR-495 tasks',
    )
    page.route('**/repos/example__menu/upload-tasks', lambda route: route.fulfill(
        status=200, content_type='text/html', body=feedback_html,
    ))
    menu = grid.locator('[data-card-menu]')
    menu.locator('summary').press('Enter')
    with page.expect_response('**/repos/example__menu/upload-tasks'):
        menu.locator('input[type=file]').set_input_files({
            'name': 'PR-495.md', 'mimeType': 'text/markdown', 'buffer': b'Task fixture',
        })
    expect(menu).not_to_have_attribute('open', '')
    feedback = grid.locator('[data-upload-feedback-card]')
    expect(feedback).to_contain_text('Batch files: PR-495.md.')
    expect(feedback).to_contain_text('Commit subject: Accept PR-495 tasks')
    page.clock.fast_forward(1_000)
    expect(feedback).to_be_visible()
    assert refreshes == []
