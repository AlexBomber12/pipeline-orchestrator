"""Rendered Settings labels, card layout and protection presentation."""

import re
from html import unescape

import pytest
from src.config import DaemonConfig
from src.web.app import templates


def test_settings_labels_and_heading_card_order():
    body = templates.get_template("settings.html").render(
        daemon=DaemonConfig(), auth={"gh": {"status": "ok", "detail": "Authorized"}},
        repos=[], coders=[],
    )
    nav = body.split('<nav aria-label="Settings sections"', 1)[1].split("</nav>", 1)[0]
    links = re.findall(r'<a href="#([^"]+)"[^>]*>(.*?)</a>', nav)
    assert len(links) == 8
    for target, label in links:
        section = body.split(f'<section id="{target}"', 1)[1]
        heading = re.search(r"<h2[^>]*>(.*?)</h2>", section)
        assert heading is not None
        assert unescape(heading[1]) == unescape(label)
        card = re.search(r'<div[^>]*class="[^"]*rounded-lg border border-white/5 bg-surface-2', section)
        assert card is not None and heading.end() < card.start(), target


@pytest.mark.parametrize("state,tone", [("unavailable", "fail"), ("active", "ok"), ("disabled", None)])
def test_protection_severity_and_checkbox_accent(state, tone):
    body = templates.get_template("components/settings_sandbox_backup.html").render(
        daemon=DaemonConfig(), sandbox_actual_state=state,
    )
    checkboxes = re.findall(r'<input type="checkbox"[^>]*>', body)
    assert len(checkboxes) == 2
    assert all("accent-accent" in checkbox for checkbox in checkboxes)
    badge = templates.get_template("components/sandbox_badge.html").render(sandbox_state=state)
    if tone:
        assert f"bg-{tone}/10 text-{tone}" in body
        assert f"bg-{tone}/10 text-{tone}" in badge
    else:
        assert "bwrap unavailable" not in body
