"""Chromium checks of real dashboard polling and delegated operator input.

Server TTL/composition is covered by test_operator_heartbeat.py. These checks
prove the browser emits the marker only on input, including after DOM swaps.
The serialized GHA executor records the checkout SHA in its existing evidence.
"""

def test_background_refresh_and_explicit_input(page, dashboard_url):
    # Accelerate only the existing refresh delays; leave input/actionability
    # timers on the native browser clock.
    page.add_init_script("""(() => {
        for (const name of ['setInterval', 'setTimeout']) {
            const original = window[name].bind(window);
            window[name] = (callback, delay, ...args) => original(
                callback, delay === 60000 || delay === 30000 ? 200 : delay, ...args
            );
        }
    })();""")
    requests = []
    page.on("request", lambda request: requests.append(request))
    page.goto(dashboard_url, wait_until="load")
    page.wait_for_function("typeof htmx !== 'undefined'")
    page.wait_for_function("document.querySelector('#availability-chip').dataset.manual")
    requests.clear()
    # Run the real HTMX timers and the unchanged 60-second chip refresh.
    page.wait_for_timeout(650)
    polls = [r for r in requests if '/api/' in r.url or '/partials/' in r.url]
    assert any('/api/availability' in r.url for r in polls)
    assert any(r.headers.get('hx-request') == 'true' for r in polls)
    assert all('x-operator-interaction' not in r.headers for r in polls)

    # A synthetic click must not manufacture activity.
    page.locator('#clock').dispatch_event('click')
    assert not any('x-operator-interaction' in r.headers for r in requests)

    with page.expect_request(lambda r: r.headers.get('x-operator-interaction') == 'true'):
        page.locator('#clock').click()
    page.wait_for_timeout(1100)
    # Delegation covers dynamically inserted controls without re-binding.
    page.evaluate("""() => {
        const input = document.createElement('div');
        input.id = 'presence-test-input';
        document.body.prepend(input);
        input.style.cssText = 'width:40px;height:40px';
    }""")
    with page.expect_request(lambda r: r.headers.get('x-operator-interaction') == 'true'):
        page.locator('#presence-test-input').click()
    page.wait_for_timeout(1100)
    with page.expect_request(lambda r: r.headers.get('x-operator-interaction') == 'true'):
        page.keyboard.press('Tab')
    requests.clear()
    page.wait_for_timeout(650)
    assert any('/api/availability' in r.url for r in requests)
    assert all('x-operator-interaction' not in r.headers for r in requests)
