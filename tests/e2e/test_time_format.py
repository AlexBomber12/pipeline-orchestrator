"""Chromium timestamp formatting on initial render and diagnostic HTMX swaps."""

from datetime import datetime, timedelta, timezone

import pytest
from playwright.sync_api import expect
from src.models import PublicationMigration, RepoState
from src.web.app import templates


@pytest.fixture
def browser_context_args(browser_context_args):
    return {**browser_context_args, "timezone_id": "America/New_York", "locale": "en-US"}


@pytest.mark.parametrize("run_started", ["valid", "invalid", None])
def test_repo_timestamps(page, dashboard_url, run_started):
    now = datetime(2026, 11, 8, 18, 0, tzinfo=timezone.utc)
    timestamp = (now - timedelta(minutes=5)).isoformat()
    page.clock.install(time=now)
    migration = PublicationMigration(
        repo="example/alpha", pr_number=504, head_repo="example/alpha",
        ref="refs/heads/pr-504", head_sha="a" * 40, reason="Inspect publication",
        published_at=datetime.fromisoformat(timestamp) if run_started else None,
    )
    state = RepoState(url="https://github.com/example/alpha", name="example__alpha",
                      publication_migration=migration, last_updated=now)
    html = templates.get_template("repo.html").render(
        repo=state, coders=[], effective_coder=None, page_rendered_at=now.isoformat(),
    )
    diagnostic = templates.get_template("components/diagnostic_panel.html").module.diagnostic_panel(
        dict(task_id="PR-504", repo_slug=state.name,
             current_run_started_at=timestamp if run_started == "valid" else run_started), 3,
    )
    page.route("**/api/repos/example__alpha/events", lambda route: route.abort())
    page.route("**/partials/repo/example__alpha", lambda route: route.fulfill(status=204))
    page.route(dashboard_url + "/time-format", lambda route: route.fulfill(body=html, content_type="text/html"))
    page.route(dashboard_url + "/diagnostic-time", lambda route: route.fulfill(
        body=diagnostic, content_type="text/html",
    ))
    page.goto(dashboard_url + "/time-format", wait_until="load")
    publication = page.locator("[data-publication-migration] [data-ts]")
    expect(publication).to_have_text("5 min ago" if run_started else "unavailable")
    page.wait_for_function("Boolean(window.htmx)")
    page.evaluate("htmx.ajax('GET', '/diagnostic-time', {target: 'main', swap: 'beforeend'})")
    started = page.locator("[data-diagnostic-panel] [data-ts]")
    expect(started).to_have_text("5 min ago" if run_started == "valid" else "unavailable")
    for element in (publication, started):
        if element.text_content() == "unavailable":
            assert element.get_attribute("title") is None
        else:
            assert element.get_attribute("data-ts") == timestamp
            expect(element).to_have_attribute("title", "11/8/2026, 12:55:00 PM EST")
    assert timestamp not in page.locator("main").inner_text()


def test_formatter_preserves_titles_and_rejects_invalid_unix(page, dashboard_url):
    html = templates.env.from_string(
        '{% extends "base.html" %}{% block content %}'
        '<span id="existing" data-ts="2026-11-08T17:55:00+00:00" title="exact diagnostic evidence"></span>'
        '<span id="unix" data-ts-unix="1794160500"></span>'
        '{% for value in ["", "bad", "123bad", "1e100"] %}'
        '<span class="unknown" data-ts-unix="{{ value }}">unavailable</span>{% endfor %}'
        '<span class="unknown" data-ts="">unavailable</span>{% endblock %}'
    ).render()
    page.clock.install(time=datetime(2026, 11, 8, 18, 0, tzinfo=timezone.utc))
    page.route(dashboard_url + "/time-format", lambda route: route.fulfill(body=html, content_type="text/html"))
    page.goto(dashboard_url + "/time-format", wait_until="load")
    expect(page.locator("#existing")).to_have_text("5 min ago")
    expect(page.locator("#existing")).to_have_attribute("title", "exact diagnostic evidence")
    expect(page.locator("#unix")).to_have_text("5 min ago")
    expect(page.locator("#unix")).to_have_attribute("title", "11/8/2026, 12:55:00 PM EST")
    for element in page.locator(".unknown").all():
        expect(element).to_have_text("unavailable")
        assert element.get_attribute("title") is None
