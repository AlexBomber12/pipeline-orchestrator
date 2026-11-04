"""Chromium layout of production-rendered headers; no live pipeline mutation."""
import pytest
from playwright.sync_api import expect
from src.models import PipelineState

from tests.web.templates.test_usage_inhibitors import header_html


@pytest.mark.parametrize('width', [360, 768, 1280])
@pytest.mark.parametrize('state', [PipelineState.PAUSED, PipelineState.CODING, PipelineState.ERROR])
def test_repo_header_rows(page, dashboard_url, width, state):
    page.set_viewport_size({'width': width, 'height': 1000})
    page.goto(dashboard_url, wait_until='load')
    # Use the real dashboard's theme and CDN utilities with a deterministic
    # production Jinja render. No Redis writes or provider calls are needed.
    page.locator('main').evaluate('(node, html) => node.innerHTML = html', header_html(state))
    header = page.locator('[data-repo-header]')
    expect(header.locator('[data-coder-display]')).to_contain_text('requested')
    buttons = header.get_by_role('button')
    assert buttons.count() > 0
    expect(buttons.first).to_have_css('height', '44px' if width < 640 else '32px')
    assert header.evaluate('''node => {
        const identity = node.querySelector('[data-header-identity]').getBoundingClientRect();
        const status = node.querySelector('[data-header-status]').getBoundingClientRect();
        if (identity.bottom > status.top) return false;
        const items = [...node.querySelectorAll('[data-header-identity] > *, [data-header-status] > *')];
        const boxes = items.map(el => el.getBoundingClientRect());
        if (boxes.some(b => b.left < 0 || b.right > innerWidth)) return false;
        return boxes.every((a, i) => boxes.slice(i + 1).every(b =>
            a.right <= b.left || b.right <= a.left || a.bottom <= b.top || b.bottom <= a.top));
    }''')
    for button in buttons.all():
        expect(button.locator('svg')).to_be_visible()
        expect(button.locator('span')).to_be_visible()
    expect(header.locator('[data-usage-inhibitor]')).to_have_count(1)
    expect(header.locator('[data-inhibitor-countdown]')).to_contain_text('m remaining')
    link = header.get_by_role('link', name='Usage settings')
    link.click()
    expect(page.locator('#settings-usage')).to_be_visible()
