"""Real Chromium/HTMX with rendered controls; POSTs are intercepted, never dispatched."""

from pathlib import Path

import pytest
from playwright.sync_api import expect
from src.models import PipelineState, RepoState
from src.web import app as web_app


@pytest.mark.parametrize('state,action,title', [
    (PipelineState.CODING, 'stop', 'Stop'),
    (PipelineState.ERROR, 'reset-to-idle', 'Reset to IDLE'),
])
def test_shared_confirmation(page, dashboard_url, state, action, title):
    script = Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js'
    page.route('https://unpkg.com/htmx.org@2.0.4', lambda route: route.fulfill(
        path=script, content_type='application/javascript',
    ))
    requests = []
    page.route('**/repos/confirm-fixture/*', lambda route: requests.append(route))
    page.goto(dashboard_url, wait_until='domcontentloaded')
    page.wait_for_function('Boolean(window.htmx)')
    controls = web_app.templates.env.get_template('components/_controls.html').module.repo_controls(
        RepoState(url='https://github.com/example/fixture.git', name='confirm-fixture', state=state),
    )
    page.evaluate('''html => {
        const host = document.createElement('div'); host.id = 'confirm-fixture';
        host.innerHTML = html; document.querySelector('main').prepend(host); htmx.process(host);
    }''', str(controls))
    trigger = page.locator(f'#confirm-fixture [hx-post$="/{action}"]')
    modal = page.get_by_role('dialog', name=title, exact=True)
    cancel = modal.get_by_role('button', name='Cancel', exact=True)
    confirm = modal.get_by_role('button', name=title, exact=True)
    trigger.click()
    expect(page.locator('#confirm-dialog')).to_have_count(1)
    expect(cancel).to_be_focused()
    expect(modal).to_have_attribute('aria-describedby', 'confirm-dialog-description')
    expect(modal.locator('button')).to_have_text(['Cancel', title])
    expect(confirm).to_have_class('danger')
    if action == 'stop':
        expect(modal).to_contain_text('Requests cancellation of the owned running coder; retained work '
                                      'may be dirty and unresolved cleanup is reported.')
    else:
        expect(modal).to_contain_text('The current task returns to TODO.')
    page.keyboard.press('Shift+Tab')
    expect(confirm).to_be_focused()
    page.keyboard.press('Tab')
    expect(cancel).to_be_focused()
    page.keyboard.press('Tab')
    expect(confirm).to_be_focused()
    page.keyboard.press('Tab')
    expect(cancel).to_be_focused()
    cancel.click()
    expect(modal).to_be_hidden()
    expect(trigger).to_be_focused()
    trigger.click()
    page.keyboard.press('Escape')
    expect(modal).to_be_hidden()
    expect(trigger).to_be_focused()
    assert requests == []

    trigger.click()
    # Synchronous repeated activation exercises the handler guard even after close.
    confirm.evaluate('button => { button.click(); button.click(); }')
    page.wait_for_function("document.querySelector('#global-spinner').classList.contains('is-active')")
    expect(modal).to_be_hidden()
    expect(trigger).to_be_focused()
    trigger.click()
    expect(modal).to_be_hidden()  # No second confirmation while the POST is pending.
    page.wait_for_timeout(100)
    assert len(requests) == 1
    assert requests[0].request.method == 'POST'
    assert requests[0].request.url.endswith(f'/repos/confirm-fixture/{action}')
    assert requests[0].request.headers['hx-request'] == 'true'
    requests[0].fulfill(status=204)
    expect(page.locator('#global-spinner')).not_to_have_class('htmx-indicator global-spinner is-active')
    trigger.click()
    expect(modal).to_be_visible()
    # A refresh can remove the original trigger while the modal is open.
    trigger.evaluate('button => button.remove()')
    confirm.click()
    expect(modal).to_be_hidden()
    assert len(requests) == 1


@pytest.mark.parametrize('state,action,title', [
    (PipelineState.CODING, 'stop', 'Stop'),
    (PipelineState.ERROR, 'reset-to-idle', 'Reset to IDLE'),
])
@pytest.mark.parametrize('completion', ['success', 'http-error', 'transport-error'])
def test_pending_confirmation_survives_controls_refresh(page, state, action, title, completion):
    controls = web_app.templates.env.get_template('components/_controls.html').module.repo_controls(
        RepoState(url='https://github.com/example/fixture.git', name='confirm-fixture', state=state),
    )
    dialog = web_app.templates.env.get_template('components/_confirm_dialog.html').render()
    html = f'<main><div id="controls">{controls}</div></main>{dialog}'
    page.route('http://confirm.test/', lambda route: route.fulfill(body=html, content_type='text/html'))
    requests = []
    page.route('**/repos/confirm-fixture/*', lambda route: requests.append(route))
    page.goto('http://confirm.test/')
    page.add_script_tag(path=Path(__file__).parent / 'assets' / 'htmx-2.0.4.min.js')
    trigger = page.locator(f'#controls [hx-post$="/{action}"]')
    modal = page.get_by_role('dialog', name=title, exact=True)
    confirm = modal.get_by_role('button', name=title, exact=True)
    trigger.click()
    confirm.click()
    page.wait_for_function("document.querySelector('#controls [data-confirm]').classList.contains('htmx-request')")
    # Keep the original only to observe completion; replace its entire controls subtree.
    page.evaluate('''html => {
        window.originalTrigger = document.querySelector('#controls [data-confirm]');
        window.originalTrigger.addEventListener('htmx:afterRequest', () => window.requestFinished = true);
        htmx.swap('#controls', '<div id="controls">' + html + '</div>', {swapStyle: 'outerHTML'});
    }''', str(controls))
    page.wait_for_function('!window.originalTrigger.isConnected')
    trigger.click()
    expect(modal).to_be_hidden()  # Replacement cannot duplicate the pending POST.
    assert len(requests) == 1
    if completion == 'transport-error':
        requests[0].abort()
    else:
        requests[0].fulfill(status=204 if completion == 'success' else 500)
    page.wait_for_function('window.requestFinished === true')
    trigger.click()
    expect(modal).to_be_visible()
    confirm.click()
    page.wait_for_function("document.querySelector('#controls [data-confirm]').classList.contains('htmx-request')")
    assert len(requests) == 2
    assert requests[1].request.method == 'POST'
    assert requests[1].request.url.endswith(f'/repos/confirm-fixture/{action}')
    requests[1].fulfill(status=204)
