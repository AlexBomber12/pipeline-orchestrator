"""Production card layout and native controls; Chromium runs in GHA."""
from datetime import datetime, timezone

import pytest
from playwright.sync_api import expect
from src.models import PipelineState, PRInfo, RepoState

from tests.web.templates.test_state_styles_canonical import _render_repo_cards


@pytest.mark.parametrize('width', [360, 1280])
@pytest.mark.parametrize('state', [PipelineState.CODING, PipelineState.ERROR])
def test_repo_card_layout(page, dashboard_url, width, state):
    name = 'owner__' + 'r' * 53
    repo = RepoState(
        name=name, url='https://github.com/owner/repo.git', state=state,
        last_updated=datetime.now(timezone.utc), upload_pending_count=3,
        error_message='Upload failed: ' + 'message' * 60,
        current_pr=PRInfo(number=495, branch='pr-495', url='https://github.com/owner/repo/pull/495'),
    )
    page.set_viewport_size({'width': width, 'height': 1000})
    page.goto(dashboard_url, wait_until='load')
    page.locator('main').evaluate('(node, html) => node.innerHTML = html', _render_repo_cards(repo))
    card = page.locator('[data-repo-card]')
    # Simulate the HTMX request display state without uploading real tasks.
    card.locator('.htmx-indicator').evaluate("node => node.classList.add('htmx-request')")
    expect(card.get_by_text('Uploading...', exact=True)).to_be_visible()
    expect(card.locator('[data-upload-pending-badge]')).to_be_visible()
    expect(card.locator('[data-card-name] span')).to_have_css('overflow', 'hidden')
    assert card.evaluate('''node => {
        const boxes = ['[data-card-name]', '[data-card-state]', 'summary'].map(
            selector => node.querySelector(selector).getBoundingClientRect());
        const secondary = node.querySelector('[data-card-secondary]').getBoundingClientRect();
        return boxes.every((a, i) => a.width > 0 && a.left >= 0 && a.right <= innerWidth &&
            a.bottom <= secondary.top && boxes.slice(i + 1).every(b => a.right <= b.left));
    }''')
    link = card.get_by_role('link', name=name, exact=True)
    link.focus()
    expect(link).to_be_focused()
    page.keyboard.press('Tab')
    toggle = card.locator('summary')
    expect(toggle).to_be_focused()
    page.keyboard.press('Enter')
    expect(card.get_by_role('link', name='audit', exact=True)).to_be_visible()
    page.keyboard.press('Tab')
    expect(card.get_by_role('link', name='audit', exact=True)).to_be_focused()
    assert card.locator('[data-card-menu] button').evaluate_all('''nodes => nodes.every(node => {
        const b = node.getBoundingClientRect();
        return node.contains(document.elementFromPoint(b.x + b.width / 2, b.y + b.height / 2));
    })''')
    # Pointer hit testing also catches a stretched link covering menu controls.
    with page.expect_file_chooser():
        card.locator('label[title="Upload tasks"]').click()
    upload = card.locator('input[type=file]')
    upload.focus()
    with page.expect_file_chooser():
        page.keyboard.press('Enter')
    toggle.focus()
    page.keyboard.press('Enter')
    if state == PipelineState.ERROR:
        message = card.get_by_text(repo.error_message, exact=True)
        expect(message).to_have_css('overflow-wrap', 'anywhere')
        assert message.evaluate('node => node.scrollWidth <= node.clientWidth')
    else:
        pr_link = card.get_by_role('link', name='#495', exact=True)
        assert pr_link.evaluate('''node => {
            const b = node.getBoundingClientRect();
            return node.contains(document.elementFromPoint(b.x + b.width / 2, b.y + b.height / 2));
        }''')
        expect(pr_link).to_have_attribute('target', '_blank')
    # Native Enter navigation follows the full, untruncated repository name.
    page.route('**/repo/' + name, lambda route: route.fulfill(body='Repository detail'))
    link.focus()
    page.keyboard.press('Enter')
    expect(page).to_have_url(dashboard_url.rstrip('/') + '/repo/' + name)
