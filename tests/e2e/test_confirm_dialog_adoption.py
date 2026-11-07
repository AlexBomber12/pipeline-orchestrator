"""Shared confirmations retain each control's HTMX request contract."""

import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from playwright.sync_api import expect
from src.web import app as web_app


@pytest.mark.parametrize('action,title,danger', [
    ('retry', 'Retry', False),
    ('approve', 'Approve current PR', False),
    ('reject', 'Reject', True),
    ('remove', 'Remove', True),
])
def test_confirm_dialog_adoption(page, dashboard_url, action, title, danger):
    env = web_app.templates.env
    if action == 'retry':
        template = env.get_template('components/tasks_panel.html')
        html = template.make_module({'retry_cap': 3, 'tasks_total': 0}).task_row('confirm-fixture', {
            'pr_id': 'PR-492', 'title': 'Retry fixture', 'retry_count': 1,
            'retry_binding': 'retained-binding',
        }, 'error')
        html = f'<section id="repo-tasks-panel-confirm-fixture">{html}</section>'
        selector = '[hx-post$="/retry"]'
        expected_body = {'retry_binding': ['retained-binding']}
    elif action in ('approve', 'reject'):
        template = env.get_template('components/_guardrail_panel.html')
        html = template.make_module({'repo': {'name': 'confirm-fixture'}}).guardrail_row({
            'pr_id': 'PR-492', 'group': 'action', 'rule': 'large-diff',
            'approval_binding': 'current-binding', 'excerpt': 'Review required',
        })
        selector = f'.{action}-btn'
        expected_body = {'decision': [action]}
        if action == 'approve':
            expected_body['binding'] = ['current-binding']
    else:
        html = env.get_template('components/settings_repo_list.html').render(
            repos=[{'url': 'https://github.com/example/dialog-fixture', 'branch': 'main', 'coder': None}],
            selectable_coders=[], selectable_coder_ids=[],
        )
        html = f'<div id="settings-repo-list">{html}</div>'
        selector = '[hx-delete]'
        expected_body = {}
    assert 'hx-confirm=' not in str(html)
    script = Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js'
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=script, content_type='application/javascript',
    ))
    requests = []
    page.goto(dashboard_url, wait_until='domcontentloaded')
    page.wait_for_function('Boolean(window.htmx)')
    page.route('**/repos/confirm-fixture/**', lambda route: requests.append(route))
    # The encoded repository URL retains slashes, which a glob's single * cannot match.
    page.route(re.compile(r'.*/settings/repos\?.*'), lambda route: requests.append(route))
    page.evaluate('''html => {
        const host = document.createElement('div'); host.id = 'adoption-fixture';
        host.innerHTML = html; document.querySelector('main').prepend(host); htmx.process(host);
    }''', str(html))
    trigger = page.locator(f'#adoption-fixture {selector}')
    modal = page.get_by_role('dialog', name=title, exact=True)
    trigger.click()
    accept = modal.get_by_role('button', name=title, exact=True)
    expect(accept).to_have_class('danger' if danger else '')
    if action == 'retry':
        expect(modal).to_contain_text('Retry PR-492? Attempt 2 of 3. Same task content; '
                                      'the failure record stays for history. '
                                      'The daemon selects the safe continuation.')
        assert 'fail' not in trigger.get_attribute('class')
    elif action == 'remove':
        expect(modal).to_contain_text('Remove dialog-fixture (https://github.com/example/dialog-fixture)?')
    modal.get_by_role('button', name='Cancel', exact=True).click()
    expect(trigger).to_be_focused()
    assert requests == []
    trigger.click()
    accept.evaluate('button => { button.click(); button.click(); }')
    expect(modal).to_be_hidden()
    page.wait_for_timeout(100)
    assert len(requests) == 1
    request = requests[0].request
    assert request.method == ('DELETE' if action == 'remove' else 'POST')
    assert parse_qs(request.post_data or '') == expected_body
    path = urlparse(request.url)
    if action == 'remove':
        assert path.path == '/settings/repos'
        assert parse_qs(path.query) == {'url': ['https://github.com/example/dialog-fixture']}
    else:
        suffix = 'tasks/PR-492/retry' if action == 'retry' else 'guardrail/PR-492/decision'
        assert path.path == f'/repos/confirm-fixture/{suffix}'
    requests[0].fulfill(status=204)
