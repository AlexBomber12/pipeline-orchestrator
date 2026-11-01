"""Chromium contracts for shared theme CSS; integration job owns execution."""

import pytest
from playwright.sync_api import Page, expect

# Composite translucent backgrounds from the root down, then foreground and
# element opacity. Current controls have no filters, images or blend modes.
CONTRAST = """el => {
    const rgb = value => {
        const c = document.createElement('canvas').getContext('2d');
        c.fillStyle = value; c.fillRect(0, 0, 1, 1);
        const p = c.getImageData(0, 0, 1, 1).data;
        return [p[0], p[1], p[2], p[3] / 255];
    };
    const over = (a, b) => a.slice(0, 3).map((v, i) => v * a[3] + b[i] * (1 - a[3]));
    const chain = []; for (let n = el; n; n = n.parentElement) chain.unshift(n);
    let bg = [255, 255, 255], parent = bg;
    for (const n of chain) {
        parent = bg;
        bg = over(rgb(getComputedStyle(n).backgroundColor), bg);
    }
    const style = getComputedStyle(el);
    let fg = over(rgb(style.color), bg);
    const opacity = Number(style.opacity);
    fg = over([...fg, opacity], parent); bg = over([...bg, opacity], parent);
    const lum = c => c.map(v => v / 255).map(v => v <= .04045 ? v / 12.92 :
        ((v + .055) / 1.055) ** 2.4).reduce((s, v, i) => s + v * [.2126,.7152,.0722][i], 0);
    return (Math.max(lum(fg), lum(bg)) + .05) / (Math.min(lum(fg), lum(bg)) + .05);
}"""


@pytest.mark.parametrize('theme', ['dark', 'light'])
def test_focus_and_computed_control_contrast(page: Page, dashboard_url: str, theme: str):
    page.goto(dashboard_url)
    page.evaluate('theme => document.documentElement.dataset.theme = theme', theme)
    # Wait for CDN utilities before measuring; failure must not silently pass.
    page.wait_for_function("getComputedStyle(document.querySelector('nav')).backgroundColor !== 'rgba(0, 0, 0, 0)'")
    page.keyboard.press('Tab')
    focused = page.locator(':focus-visible')
    expect(focused).to_have_count(1)
    expect(focused).to_have_css('outline-style', 'solid')
    expect(focused).to_have_css('outline-width', '2px')

    # Exercise solid action combinations for future scoped dialog/card adoption.
    page.evaluate("""() => {
        const host = document.createElement('div'); host.id = 'theme-probes';
        host.className = 'bg-surface-2';
        for (const role of ['accent', 'warn', 'fail']) {
            const button = document.createElement('button'); button.textContent = role;
            button.id = 'probe-' + role;
            button.className = 'px-4 py-2 bg-' + role + ' text-on-' +
                (role === 'fail' ? 'fail' : 'primary');
            host.append(button);
        }
        document.querySelector('main').prepend(host);
    }""")
    for role in ['accent', 'warn', 'fail']:
        button = page.locator(f'#probe-{role}')
        expect(button).not_to_have_css('background-color', 'rgba(0, 0, 0, 0)')
        for state in ['normal', 'hover', 'focus']:
            if state == 'hover':
                button.hover()
            elif state == 'focus':
                button.focus()
            assert button.evaluate(CONTRAST) >= 4.5, (theme, role, state)
    if theme == 'light':
        expect(page.locator('#probe-accent')).to_have_css('color', 'rgb(255, 255, 255)')
        expect(page.locator('#probe-accent')).to_have_css('background-color', 'rgb(4, 101, 175)')

    # These controls share hover classes; inspect every currently rendered action
    # without invoking endpoints or changing the testbed pipeline state.
    controls = page.locator('[aria-label="Resume daemon"], [aria-label="Pause daemon"], [aria-label="Upload tasks"]')
    assert controls.count() > 0
    for control in controls.all():
        control.hover()
        page.wait_for_timeout(200)
        assert control.evaluate(CONTRAST) >= 4.5

    page.goto(f'{dashboard_url}/settings')
    page.evaluate('theme => document.documentElement.dataset.theme = theme', theme)
    page.keyboard.press('Tab')
    field = page.locator('input:not([type="hidden"]):visible').first
    field.focus()
    expect(field).to_have_css('outline-style', 'solid')
    expect(field).to_have_css('outline-width', '2px')


def test_reduced_motion_stops_pulse_mutations(page: Page, dashboard_url: str):
    page.emulate_media(reduced_motion='reduce')
    page.goto(dashboard_url)
    page.evaluate("""() => {
        const host = document.createElement('div'); host.id = 'motion-probes';
        host.innerHTML = '<span class="pulse-dot">dot</span><span class="pulse-badge">badge</span>';
        document.body.append(host);
        window.pulseMutations = 0;
        new MutationObserver(records => window.pulseMutations += records.length)
            .observe(host, {subtree: true, attributes: true, attributeFilter: ['class']});
        document.body.dispatchEvent(new Event('htmx:afterSwap'));
    }""")
    page.wait_for_timeout(3100)
    assert page.evaluate('window.pulseMutations') == 0
    expect(page.locator('#motion-probes .dim')).to_have_count(0)
    expect(page.locator('.global-spinner-dot')).to_have_css('animation-name', 'none')
    page.emulate_media(reduced_motion='no-preference')
    expect(page.locator('#motion-probes .dim')).to_have_count(2, timeout=2500)
    page.emulate_media(reduced_motion='reduce')
    expect(page.locator('#motion-probes .dim')).to_have_count(0)
    before = page.evaluate('window.pulseMutations')
    page.wait_for_timeout(3100)
    assert page.evaluate('window.pulseMutations') == before
