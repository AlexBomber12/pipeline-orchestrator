from __future__ import annotations

import re
from html import unescape

import pytest
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

    assert "Event ·" not in html
    assert "MERGE" in html
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


def test_mixed_history_uses_phase_categories_and_keeps_detail() -> None:
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
        "CANCEL", "Event", "RECOVERY", "INFRA",
        "MERGE", "ERROR", "ERROR",
    ]
    for style, category, _ in badges:
        assert ("bg-ok/10" if category == "MERGE" else "bg-gray-500/10") in style
        assert "break-words" in style and "text-[10px] font-medium" in style
    assert "text-fail" not in html and "text-warn" not in html
    assert [part.strip() for part in re.findall(r'data-ts="([^"]+)"', html)] == [
        "11:57", "11:55", "11:54", "11:53", "11:52", "11:51", "11:50",
    ]
    assert "cleared error_message after Retry" in html
    assert "status write failed" in html
    assert 'title="repeated 3 times"' in html and "&times;3" in html


@pytest.mark.parametrize("recorded", [False, True])
@pytest.mark.parametrize("message,kind,severity,resolved", [
    ("FIX -> ERROR: merge conflict resolution failed; unpublished checkout retained", "transition", "error", "error"),
    ("[WATCH] PR #644 review approved; waiting for CI (PENDING).", None, "info", "unknown"),
    ("[ERROR] cleared error_message after Retry", None, "ok", "unknown"),
    ("[MERGE] Remote merge confirmed but status:DONE was not written to tasks/PR-395.md.", None, "error", "unknown"),
])
def test_recorded_severity_and_complete_messages(recorded, message, kind, severity, resolved):
    entry = {"message": message, "kind": kind}
    if recorded:
        entry["severity"] = severity
    html = _render([entry])
    row = re.search(r"<li ([^>]+)>", html)[1]
    assert f'data-event-severity="{severity if recorded else resolved}"' in row
    source = "recorded" if recorded else "transition" if resolved == "error" else "unknown"
    assert f'data-event-severity-source="{source}"' in row
    expected = message.split("] ", 1)[-1]
    assert unescape(re.search(r"<span data-event-message>(.*?)</span>", html)[1]) == expected
    assert "sr-only" not in html
    if kind == "transition":
        assert 'aria-hidden="true" class="inline-flex items-center gap-1 mr-1" data-event-transition' in html
    if not recorded and resolved == "unknown":
        assert "border-dashed" in row
    if recorded and severity == "info":
        assert "border-gray-500" in row


@pytest.mark.parametrize("message,kind", [
    ("FIX -> WATCH: ERROR recovery failed", "transition"),
    ("FIX mentioned ERROR -> WATCH: retained", "transition"),
    ("FIX -> ERROR: failed", "message"),
    ("FIX ERROR recovery failed", "transition"),
    ("FIX -> ERROR recovery failed", "transition"),
    ("FIX -> ERROR_EXTRA: failed", "transition"),
])
def test_error_words_do_not_derive_severity(message, kind):
    html = _render([{"event": message, "kind": kind}])
    assert 'data-event-severity="unknown"' in html


def test_escaping_metadata_repetitions_and_oob():
    entry = {
        "message": '[WATCH] [<coder>] <script>alert("x")</script> & retained',
        "tier": "WATCH", "coder": "<coder>", "kind": '<img src=x onerror="x">',
        "severity": "error", "time": "first", "last_seen_at": "last", "count": 4,
    }
    html = web_app.templates.env.get_template("components/event_list.html").render(
        events=[entry], repo={"name": "example__repo"}, oob=True,
    )
    assert "<script>" not in html and "<img" not in html
    message = unescape(re.search(r"<span data-event-message>(.*?)</span>", html)[1])
    assert message == '<script>alert("x")</script> & retained'
    assert 'data-event-coder>&lt;coder&gt;</span>' in html
    assert 'data-ts="last"' in html and 'title="first → last"' in html
    assert 'aria-label="repeated 4 times"' in html
    assert 'id="event-log-count-example__repo" hx-swap-oob="true"' in html
    assert '>1 events</span>' in html
    assert "Severity not recorded" in html


@pytest.mark.parametrize("category", ["CODING", "WATCH", "FIX", "MERGE", "ERROR", "INFRA"])
def test_only_phase_categories_have_hue(category):
    html = _render([{"event": f"[{category}] detail"}])
    badge = re.search(r'<span class="([^"]+)"\s+data-event-category=', html)[1]
    assert ("bg-gray-500/10" in badge) == (category in {"ERROR", "INFRA"})


def test_unicode_historical_transition():
    html = _render([{"event": "FIX → ERROR: retained", "kind": "transition"}])
    assert 'data-event-severity-source="transition"' in html


@pytest.mark.parametrize(
    "severity,expected", [("warn", "warn"), ("ok", "ok"), ("info", "info"), ("invalid", "unknown")]
)
def test_transition_does_not_override_stored_severity(severity, expected):
    html = _render([{"event": "FIX -> ERROR: retained", "kind": "transition", "severity": severity}])
    assert f'data-event-severity="{expected}"' in html
    assert 'data-event-severity-source="transition"' not in html
