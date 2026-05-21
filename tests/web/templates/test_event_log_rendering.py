from __future__ import annotations

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

    assert "bg-ok/10" in html
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
