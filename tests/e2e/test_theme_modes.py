"""Shared theme preference, persistence and live System updates."""

import pytest
from playwright.sync_api import expect


@pytest.mark.parametrize('stored', ['system', 'light', 'dark', None, 'invalid'])
def test_theme_modes(page, dashboard_url, stored):
    # Seed only once so subsequent navigation proves persistence.
    page.goto(dashboard_url)
    page.evaluate("""stored => {
        localStorage.removeItem('theme');
        if (stored !== null) localStorage.setItem('theme', stored);
    }""", stored)
    page.emulate_media(color_scheme='dark')
    page.goto(f'{dashboard_url}/settings')
    mode = stored if stored in ('light', 'dark') else 'system'
    root = page.locator('html')
    settings = page.locator('#theme-toggle')
    header = page.locator('#header-theme')
    expect(settings).to_have_value(mode)
    expect(header).to_have_value(mode)
    expect(root).to_have_attribute('data-theme', 'light' if mode == 'light' else 'dark')
    page.emulate_media(color_scheme='light')
    expect(root).to_have_attribute('data-theme', 'dark' if mode == 'dark' else 'light')
    settings.select_option('dark')
    expect(header).to_have_value('dark')
    page.emulate_media(color_scheme='dark')
    page.emulate_media(color_scheme='light')
    expect(root).to_have_attribute('data-theme', 'dark')
    page.locator('#header-menu summary').click()
    header.select_option('system')
    expect(settings).to_have_value('system')
    expect(root).to_have_attribute('data-theme', 'light')
    assert page.evaluate("localStorage.getItem('theme')") == 'system'
    page.reload()
    expect(settings).to_have_value('system')
    page.emulate_media(color_scheme='dark')
    expect(root).to_have_attribute('data-theme', 'dark')
    page.goto(dashboard_url)
    page.locator('#header-menu summary').click()
    expect(header).to_have_value('system')
    header.select_option('light')
    page.emulate_media(color_scheme='dark')
    expect(root).to_have_attribute('data-theme', 'light')
    page.reload()
    expect(root).to_have_attribute('data-theme', 'light')
