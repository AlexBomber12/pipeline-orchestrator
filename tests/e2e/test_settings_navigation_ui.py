"""Browser checks for Settings navigation and the coder cards."""

import pytest

SECTIONS = (
    ("settings-coders-section", "Coders"),
    ("settings-github", "GitHub"),
    ("settings-repositories", "Repositories"),
    ("settings-automation", "Automation"),
    ("settings-usage", "Usage limits"),
    ("settings-notifications-section", "Notifications"),
    ("settings-protection-backups", "Sandbox and backup"),
    ("settings-appearance", "Appearance"),
)


def _open_settings(page, dashboard_url, *, width, theme):
    page.set_viewport_size({"width": width, "height": 900})
    page.emulate_media(color_scheme=theme)
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    response = page.goto(f"{dashboard_url}/settings", wait_until="load")
    assert response is not None and response.status == 200
    page.wait_for_function(
        """() => {
            const section = document.querySelector('#settings-coders-section');
            const nav = document.querySelector('nav[aria-label="Settings sections"]');
            const cards = document.querySelector('#settings-coders .grid');
            return section && nav && cards &&
                getComputedStyle(section.parentElement.parentElement).display === 'grid' &&
                getComputedStyle(cards).display === 'grid' &&
                getComputedStyle(nav.firstElementChild).display === 'flex';
        }""",
        timeout=15000,
    )
    assert page.locator("html").get_attribute("data-theme") == theme
    return errors


def _assert_responsive_layout(page, width, theme):
    layout = page.evaluate(
        """() => {
            const root = document.querySelector('#settings-coders-section').parentElement.parentElement;
            const nav = document.querySelector('nav[aria-label="Settings sections"]');
            const cards = document.querySelector('#settings-coders .grid');
            const body = getComputedStyle(document.body);
            return {
                rootColumns: getComputedStyle(root).gridTemplateColumns.split(' ').length,
                cardColumns: getComputedStyle(cards).gridTemplateColumns.split(' ').length,
                navDirection: getComputedStyle(nav.firstElementChild).flexDirection,
                navPosition: getComputedStyle(nav).position,
                navOverflow: getComputedStyle(nav).overflowX,
                scrollWidth: document.documentElement.scrollWidth,
                clientWidth: document.documentElement.clientWidth,
                background: body.backgroundColor,
                text: body.color,
            };
        }"""
    )
    assert layout["scrollWidth"] <= layout["clientWidth"] + 1, layout
    assert layout["background"] == (
        "rgb(245, 247, 251)" if theme == "light" else "rgb(10, 10, 15)"
    ), layout
    assert layout["text"] == (
        "rgb(17, 24, 39)" if theme == "light" else "rgb(245, 247, 251)"
    ), layout
    if width >= 1024:
        assert layout["rootColumns"] == layout["cardColumns"] == 2, layout
        assert layout["navDirection"] == "column", layout
        assert layout["navPosition"] == "sticky", layout
    else:
        assert layout["rootColumns"] == layout["cardColumns"] == 1, layout
        assert layout["navDirection"] == "row", layout
        assert layout["navOverflow"] == "auto", layout


def _assert_inside_card(control, card):
    control_box = control.bounding_box()
    card_box = card.bounding_box()
    assert control_box is not None and card_box is not None
    assert control_box["x"] >= card_box["x"] - 1
    assert control_box["x"] + control_box["width"] <= card_box["x"] + card_box["width"] + 1
    assert control_box["y"] >= card_box["y"] - 1
    assert control_box["y"] + control_box["height"] <= card_box["y"] + card_box["height"] + 1


def _assert_coder_controls(page, width):
    cards = page.locator("[data-coder-card]")
    assert cards.count() >= 2
    for card in cards.all():
        name = card.get_attribute("data-coder-card")
        assert name
        card_box = card.bounding_box()
        assert card_box is not None
        assert card_box["x"] >= -1
        assert card_box["x"] + card_box["width"] <= width + 1

        model = card.locator(f"select[id='{name}-model']")
        assert model.count() == 1
        assert card.locator(f"label[for='{name}-model']").is_visible()
        _assert_inside_card(model, card)
        if model.is_enabled():
            model.focus()
            assert model.evaluate("element => document.activeElement === element")
        else:
            assert model.get_attribute("aria-disabled") == "true"
            assert card.get_by_text("Plugin metadata is unavailable", exact=False).is_visible()

        effort = card.locator("[data-reasoning-effort-status]")
        assert effort.count() == 1
        assert effort.get_by_text("Reasoning effort", exact=True).is_visible()
        effort_select = effort.locator("select")
        if effort_select.count():
            assert effort_select.is_enabled()
            _assert_inside_card(effort_select, card)
            effort_select.focus()
            assert effort_select.evaluate("element => document.activeElement === element")
        else:
            assert effort.locator("p").first.is_visible()

        preferred = card.locator("input[type='radio'][name='coder']")
        if preferred.count():
            assert card.get_by_text("Preferred default", exact=True).is_visible()
            _assert_inside_card(preferred, card)
            preferred.focus()
            assert preferred.evaluate("element => document.activeElement === element")
        else:
            assert card.get_by_text("Unavailable", exact=False).first.is_visible()


def _assert_heading_in_viewport(page, section_id, heading):
    section = page.locator(f"section#{section_id}")
    assert section.count() == 1
    assert section.is_visible()
    title = section.locator("h2").first
    assert title.inner_text().strip() == heading
    box = title.bounding_box()
    assert box is not None
    assert box["y"] >= -1, (section_id, box)
    assert box["y"] < page.viewport_size["height"], (section_id, box)


@pytest.mark.parametrize("width", [390, 768, 1440])
@pytest.mark.parametrize("theme", ["light", "dark"])
def test_settings_navigation_and_coder_cards(
    page, dashboard_url, take_screenshot, width, theme
):
    errors = _open_settings(page, dashboard_url, width=width, theme=theme)
    _assert_responsive_layout(page, width, theme)
    _assert_coder_controls(page, width)
    page.evaluate("window.scrollTo(0, 0)")
    take_screenshot(f"settings-{width}-{theme}")

    links = page.locator('nav[aria-label="Settings sections"] a[href^="#"]')
    assert links.count() == len(SECTIONS)
    assert links.evaluate_all(
        "elements => elements.map(link => link.getAttribute('href'))"
    ) == [f"#{section_id}" for section_id, _ in SECTIONS]
    for section_id, heading in SECTIONS:
        link = page.locator(f'nav[aria-label="Settings sections"] a[href="#{section_id}"]')
        assert link.count() == 1
        link.click()
        assert page.evaluate("location.hash") == f"#{section_id}"
        _assert_heading_in_viewport(page, section_id, heading)
    assert not errors, errors


@pytest.mark.parametrize("width", [390, 1440])
def test_settings_direct_appearance_hash(page, dashboard_url, take_screenshot, width):
    page.set_viewport_size({"width": width, "height": 900})
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    response = page.goto(
        f"{dashboard_url}/settings#settings-appearance", wait_until="load"
    )
    assert response is not None and response.status == 200
    page.locator("#settings-appearance h2").wait_for()
    page.wait_for_function(
        """() => {
            const heading = document.querySelector('#settings-appearance h2');
            const box = heading.getBoundingClientRect();
            return box.top >= -1 && box.top < innerHeight;
        }"""
    )
    assert page.evaluate("location.hash") == "#settings-appearance"
    _assert_heading_in_viewport(page, "settings-appearance", "Appearance")
    take_screenshot(f"settings-direct-hash-{width}")
    assert not errors, errors


def test_settings_theme_toggle_persists_after_reload(page, dashboard_url):
    errors = _open_settings(page, dashboard_url, width=768, theme="dark")
    toggle = page.locator("#theme-toggle")
    toggle.click()
    assert page.locator("html").get_attribute("data-theme") == "light"
    page.reload(wait_until="load")
    assert page.locator("html").get_attribute("data-theme") == "light"
    assert toggle.get_attribute("aria-label") == "Switch to dark theme"
    toggle.click()
    assert page.locator("html").get_attribute("data-theme") == "dark"
    page.reload(wait_until="load")
    assert page.locator("html").get_attribute("data-theme") == "dark"
    assert not errors, errors
