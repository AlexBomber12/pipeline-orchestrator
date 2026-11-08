"""Repository cards and contained desktop tables on the real Settings page."""

import pytest
from playwright.sync_api import expect
from src.config import DaemonConfig, RepoConfig
from src.web.app import templates


@pytest.mark.parametrize("width", [360, 767, 768, 1440])
@pytest.mark.parametrize("repo_count", [0, 2])
def test_settings_repository_layout(page, dashboard_url, width, repo_count):
    page.set_viewport_size({"width": width, "height": 900})
    repos = [RepoConfig(url=f"https://github.com/long-owner/{'long-repository-' * 8}{i}")
             for i in range(repo_count)]
    fragment = templates.get_template("components/settings_repo_list.html").render(
        repos=repos, daemon=DaemonConfig(), default_coder_label="Configured provider",
        selectable_coders=[], selectable_coder_ids=[],
    )
    page.goto(f"{dashboard_url}/settings", wait_until="load")
    page.wait_for_function("Boolean(window.htmx && window.settingsSaveFeedbackInstalled)")
    # Exercise the same fragment replacement used by repository edits, without
    # mutating the shared integration configuration or cloning fixture repos.
    page.route("**/settings/repos?*", lambda route: route.fulfill(
        status=200, content_type="text/html", body=fragment,
    ))
    # Scope the fixture request to the list and await an explicit completion value.
    page.evaluate("""() => htmx.ajax('PUT', '/settings/repos?url=layout-fixture', {
        source: '#settings-repo-list', target: '#settings-repo-list', swap: 'innerHTML'
    }).then(() => true)""")
    listing = page.locator("#settings-repo-list")
    expect(listing.locator('select[name="coder"]')).to_have_count(repo_count)
    page.wait_for_function("""() => {
        const table = document.querySelector('#settings-repo-list table');
        return getComputedStyle(table).display === (innerWidth < 768 ? 'block' : 'table');
    }""")
    assert page.evaluate(
        "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
    )
    if not repo_count:
        expect(listing.get_by_text("No repositories configured. Add one below.")).to_be_visible()
    for row in listing.locator('tbody tr').all() if repo_count else []:
        assert row.evaluate("el => getComputedStyle(el).display") == (
            "grid" if width < 768 else "table-row"
        )
        for name in ("Branch", "Auto merge", "No CI merge", "Review reminder (min)", "Coder"):
            expect(row.get_by_label(name, exact=True)).to_be_visible()
        expect(row.locator('option:checked')).to_have_text(
            "Automatic — preferred default: Configured provider"
        )
        if width < 768:
            for control in row.locator("input, select, button").all():
                box = control.bounding_box()
                assert box and box["x"] >= 0 and box["x"] + box["width"] <= width
        remove = row.get_by_role("button", name="Remove", exact=True)
        remove.hover()
        assert remove.evaluate("el => getComputedStyle(el).backgroundColor") != "rgba(0, 0, 0, 0)"
    if repo_count:
        with page.expect_response(lambda response: "/settings/repos?" in response.url):
            listing.get_by_label("Branch", exact=True).first.fill("layout-check")
            listing.get_by_label("Branch", exact=True).first.blur()
        expect(listing.locator('select[name="coder"]')).to_have_count(repo_count)
        assert page.evaluate(
            "document.documentElement.scrollWidth <= document.documentElement.clientWidth"
        )
    expect(listing.get_by_role("button", name="Add Repository")).to_be_visible()
    expect(listing.locator('form input[name="url"]')).to_be_visible()
    expect(page.locator("#settings-usage-limits")).not_to_contain_text("legacy")
