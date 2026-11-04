"""Dashboard refresh must not interrupt native repository action menus."""
from pathlib import Path

import pytest
from playwright.sync_api import expect
from src.models import PipelineState, RepoState

from tests.web.templates.test_state_styles_canonical import _render_repo_cards


@pytest.mark.parametrize('trigger', ['repo:state_change', 'repo:history_updated', 'poll'])
@pytest.mark.parametrize('action', ['summary', 'a', 'button', 'input[type=file]'])
def test_open_repo_menu_defers_refresh(page, dashboard_url, trigger, action):
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
    menu.locator('summary').press('Enter')
    expect(menu).not_to_have_attribute('open', '')
    refresh()
    expect(grid.locator('[data-refresh-marker]')).to_have_text('Updated repository data')
    assert page.evaluate('!window.retainedMenu.isConnected')
