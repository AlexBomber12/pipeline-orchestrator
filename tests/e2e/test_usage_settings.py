"""Chromium verifies quota refresh without replacing coder controls."""

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from playwright.sync_api import Page, expect
from src.keyspace import coder_usage
from src.usage import UsageObservation, UsageSnapshot
from src.web import app as web_app
from src.web.services.usage import usage_settings_context


def _refresh_html(percent: int, status: str = "available") -> str:
    now = time.time()
    snapshot = UsageSnapshot(percent, int(now + 600), None, None, now)
    observation = UsageObservation(
        status=status, checked_at=now, last_successful_at=now,
        snapshot=snapshot if status == "available" else None,
    )

    class Observations:
        async def get(self, key):
            return observation.model_dump_json() if key == coder_usage("codex") else None

    with ThreadPoolExecutor(max_workers=1) as pool:
        context = pool.submit(
            lambda: asyncio.run(usage_settings_context(Observations(), ["codex"], 60)),
        ).result(timeout=10)
    return web_app.templates.get_template("components/settings_usage_refresh.html").render(
        coders=[SimpleNamespace(name="codex", display_name="Codex")], **context,
    )


def test_usage_refresh_preserves_settings_controls(page: Page, dashboard_url: str) -> None:
    fresh = _refresh_html(47)
    unavailable = _refresh_html(47, "unavailable")
    response = page.goto(dashboard_url + "/settings", wait_until="domcontentloaded")
    assert response is not None and response.status == 200
    page.wait_for_function("Boolean(window.htmx)")
    expect(page.locator("#settings-github")).to_be_visible()
    model = page.locator("#codex-model")
    selected = model.input_value()
    quota = page.locator("#coder-codex-usage")
    page.route("**/partials/settings/usage", lambda route: route.fulfill(
        status=200, content_type="text/html", body=fresh,
    ))
    page.evaluate("htmx.ajax('GET', '/partials/settings/usage', {swap: 'none'})")
    expect(quota).to_contain_text("47% used")
    expect(model).to_have_value(selected)
    page.unroute("**/partials/settings/usage")
    page.route("**/partials/settings/usage", lambda route: route.fulfill(
        status=200, content_type="text/html", body=unavailable,
    ))
    page.evaluate("htmx.ajax('GET', '/partials/settings/usage', {swap: 'none'})")
    expect(quota).to_contain_text("Usage data unavailable")
    expect(quota).not_to_contain_text("47% used")
    expect(model).to_have_value(selected)
    page.goto(dashboard_url, wait_until="domcontentloaded")
    expect(page.locator("[data-resource-chips]")).to_have_count(0)
