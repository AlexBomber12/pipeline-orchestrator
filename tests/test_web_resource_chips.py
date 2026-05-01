"""Template-level checks for the four-chip resource row.

Exercises the rendered HTML emitted by ``components/repo_cards.html`` so a
silent regression in markup, ordering, or tooltip wiring fails the CI gate
the same way a backend bug would.
"""

from __future__ import annotations

from src.web.app import templates


def _render(resources: dict[str, dict[str, object]]) -> str:
    return templates.get_template("components/repo_cards.html").render(
        repos=[],
        resources=resources,
        css_escape=lambda v: v,
        upload_feedback_target=lambda _name: "",
        utcnow=lambda: None,
    )


def _resources(**overrides: dict[str, object]) -> dict[str, dict[str, object]]:
    base = {
        "github_rest": {
            "remaining": 4500,
            "limit": 5000,
            "percent_remaining": 90.0,
            "reset_unix": 1700000000,
            "zone": "green",
        },
        "github_graphql": {
            "remaining": 1200,
            "limit": 5000,
            "percent_remaining": 24.0,
            "reset_unix": 1700000000,
            "zone": "amber",
        },
        "claude_5h": {
            "remaining": 12,
            "limit": 100,
            "percent_remaining": 12.0,
            "reset_unix": 1700100000,
            "zone": "red",
        },
        "claude_weekly": {
            "remaining": None,
            "limit": None,
            "percent_remaining": None,
            "reset_unix": None,
            "zone": "none",
        },
    }
    base.update(overrides)
    return base


def test_resource_chip_row_renders_four_chips_in_expected_order() -> None:
    html = _render(_resources())
    expected_order = ["github_rest", "github_graphql", "claude_5h", "claude_weekly"]
    positions = [html.index(f'data-resource="{key}"') for key in expected_order]
    assert positions == sorted(positions)
    assert html.count("class=\"resource-chip\"") == 4


def test_resource_chip_tooltip_includes_absolute_values_and_reset_clock() -> None:
    html = _render(_resources())
    assert "GitHub REST API" in html
    assert "4500 / 5000 remaining" in html
    # reset_unix=1700000000 -> 2023-11-14 22:13 UTC.
    assert "Resets at 22:13 UTC" in html


def test_resource_chip_omits_reset_line_when_reset_unix_missing() -> None:
    resources = _resources(
        claude_weekly={
            "remaining": None,
            "limit": None,
            "percent_remaining": None,
            "reset_unix": None,
            "zone": "none",
        }
    )
    html = _render(resources)
    weekly_chip = html.split('data-resource="claude_weekly"', 1)[1].split(
        "</span>", 2
    )[0]
    assert "Resets at" not in weekly_chip
    # Missing-data chip renders an em dash placeholder rather than crashing.
    assert "—" in html


def test_resource_chip_zone_attribute_drives_color_coding() -> None:
    html = _render(_resources())
    assert 'data-zone="green"' in html
    assert 'data-zone="amber"' in html
    assert 'data-zone="red"' in html
    assert 'data-zone="none"' in html
