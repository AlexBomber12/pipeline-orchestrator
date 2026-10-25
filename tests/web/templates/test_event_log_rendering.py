from __future__ import annotations

import re

from src.web import app as web_app


def _render(events: list[dict[str, object]]) -> str:
    return web_app.templates.env.get_template("components/event_list.html").render(
        {"events": events, "repo": {"name": "example__repo"}}
    )


def test_renders_tier_badge_when_present() -> None:
    html = _render(
        [
            {
                "time": "2026-05-21T12:00:00+00:00",
                "state": "MERGE",
                "message": "PR #420 merged",
                "tier": "merge",
                "kind": "pr_merge",
            }
        ]
    )

    assert "Event · MERGE" in html
    assert "bg-gray-500/10" in html
    assert "pr_merge" in html


def test_renders_legacy_prefix_when_tier_absent() -> None:
    html = _render(
        [
            {
                "time": "2026-05-21T12:00:00+00:00",
                "state": "WATCH",
                "message": "[INFRA] Posted @codex review on PR #420.",
            }
        ]
    )

    assert "INFRA" in html
    assert "[INFRA]" not in html


def test_renders_kind_as_compact_secondary_tag() -> None:
    html = _render(
        [
            {
                "message": "PR #420 merged",
                "tier": "merge",
                "kind": "pr_merge",
            }
        ]
    )

    assert "text-[10px] text-gray-400" in html


def test_renders_message_without_bracket_when_tier_present() -> None:
    html = _render(
        [
            {
                "message": "PR #420 merged",
                "tier": "merge",
                "kind": "pr_merge",
            }
        ]
    )

    assert "PR #420 merged" in html
    assert "[merge]" not in html
    assert "[pr_merge]" not in html


def test_mixed_history_uses_neutral_event_categories_and_keeps_detail() -> None:
    events = [
        {"time": "11:50", "state": "ERROR",
         "event": "[ERROR] cleared error_message after Retry"},
        {"time": "11:51", "state": "WATCH",
         "event": "[ERROR] status write failed"},
        {"time": "11:52", "state": "MERGE", "event": "[MERGE] PR merged"},
        {"time": "11:53", "state": "ERROR", "event": "[INFRA] unavailable"},
        {"time": "11:54", "state": "ERROR", "event": "[RECOVERY] resumed"},
        {"time": "11:55", "state": "ERROR", "event": "plain entry"},
        {"time": "11:56", "state": "WATCH", "event": "Stop requested",
         "tier": "cancel", "count": 3, "last_seen_at": "11:57"},
    ]
    html = _render(events)
    badges = re.findall(
        r'<span class="([^"]+)"\s+data-event-category="([^"]*)">\s*([^<]+)</span>',
        html,
    )
    assert [category for _, category, _ in badges] == [
        "cancel", "", "RECOVERY", "INFRA", "MERGE", "ERROR", "ERROR",
    ]
    assert [label.strip() for _, _, label in badges] == [
        "Event · CANCEL", "Event", "Event · RECOVERY", "Event · INFRA",
        "Event · MERGE", "Event · ERROR", "Event · ERROR",
    ]
    classes = {style for style, _, _ in badges}
    assert len(classes) == 1
    style = classes.pop()
    assert "bg-gray-500/10" in style and "text-gray-300" in style
    assert "break-words" in style and "text-[10px] font-medium" in style
    assert "text-fail" not in html and "text-warn" not in html
    assert [part.strip() for part in re.findall(r'data-ts="([^"]+)"', html)] == [
        "11:57", "11:55", "11:54", "11:53", "11:52", "11:51", "11:50",
    ]
    assert "cleared error_message after Retry" in html
    assert "status write failed" in html
    assert 'title="repeated 3 times"' in html and "&times;3" in html
