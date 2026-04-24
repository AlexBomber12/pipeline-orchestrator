"""
EVENT-RECHECK-01: EVENT-LOG-01 re-verification, split into 2 time-window sub-tests.

Day 4 result: single 3-minute-wait test FAILED. Investigation revealed:
- Test already searches by message text (not state enum) — OBS-4 corrected.
- Real cause: lifecycle events get pushed out of history window or deduped during
  the 3-minute wait as polling cycles add new events.

Day 5 split:
- short (60s wait): lifecycle events should be present. Regression check for
  the basic WEB-01-v2 fix.
- long (3min wait): originally failed, confirms EVENT-LOG-01 still active beyond
  short window. This is expected to fail — confirms OBS-13 is a real bug.

If short PASSES and long FAILS — EVENT-LOG-01 partially resolved.
If both PASS — EVENT-LOG-01 fully resolved (celebrate).
If short FAILS — EVENT-LOG-01 regressed, worse than Day 4.
"""

import time

import pytest
import requests


def _fire_lifecycle_sequence(dashboard_url, testbed_slug):
    """Fire pause/resume/pause/stop/resume in sequence, return timestamps."""
    actions = ["pause", "resume", "pause", "stop", "resume"]
    timestamps = []
    for action in actions:
        url = f"{dashboard_url}/repos/{testbed_slug}/{action}"
        r = requests.post(url, timeout=5)
        timestamps.append((action, time.time(), r.status_code))
        time.sleep(2)
    return timestamps


def _count_lifecycle_events(events):
    """Count events containing pause/resume/stop markers in message."""
    pause = [e for e in events if "paus" in e.get("event", "").lower() or "paus" in e.get("message", "").lower()]
    resume = [e for e in events
              if any(kw in (e.get("event", "") + e.get("message", "")).lower()
                     for kw in ("resum", "pause canceled"))]
    stop = [e for e in events if "stop" in (e.get("event", "") + e.get("message", "")).lower()]
    return len(pause), len(resume), len(stop)


def test_event_log_01_lifecycle_persists_short_window(
    page,
    testbed_url,
    testbed_slug,
    dashboard_url,
    get_state,
    take_screenshot,
):
    """Short window: events must persist at least 60 seconds."""
    page.goto(testbed_url)
    take_screenshot("short_01_before")

    initial = get_state()
    initial_count = len(initial.get("history", [])) if initial else 0

    _fire_lifecycle_sequence(dashboard_url, testbed_slug)
    take_screenshot("short_02_after_sequence")

    time.sleep(60)
    take_screenshot("short_03_after_60s")

    final = get_state()
    history = final.get("history", [])
    new_events = history[initial_count:]

    pause_n, resume_n, stop_n = _count_lifecycle_events(new_events)
    total = pause_n + resume_n + stop_n

    assert total >= 3, \
        f"After 60 seconds, fewer than 3 lifecycle events visible. " \
        f"pause={pause_n}, resume={resume_n}, stop={stop_n}. " \
        f"Recent events: {new_events[-10:]}. " \
        f"EVENT-LOG-01 regressed vs Day 4 or WEB-01-v2 broken."


def test_event_log_01_lifecycle_persists_long_window(
    page,
    testbed_url,
    testbed_slug,
    dashboard_url,
    get_state,
    take_screenshot,
):
    """Long window: 3-minute wait. This is the Day 4 failure scenario.

    Expected behavior (if EVENT-LOG-01 fully fixed): at least 3 lifecycle
    events still visible.
    Day 4 actual: events are overwritten/deduped. This test likely still FAILS.
    A PASS here means EVENT-LOG-01 is fully resolved.
    """
    page.goto(testbed_url)
    take_screenshot("long_01_before")

    initial = get_state()
    initial_count = len(initial.get("history", [])) if initial else 0

    _fire_lifecycle_sequence(dashboard_url, testbed_slug)
    take_screenshot("long_02_after_sequence")

    time.sleep(180)
    take_screenshot("long_03_after_3min")

    final = get_state()
    history = final.get("history", [])
    new_events = history[initial_count:]

    pause_n, resume_n, stop_n = _count_lifecycle_events(new_events)
    total = pause_n + resume_n + stop_n

    assert total >= 3, \
        f"After 3 minutes, lifecycle events pushed out of visible history. " \
        f"pause={pause_n}, resume={resume_n}, stop={stop_n}. " \
        f"Recent events (last 10): {new_events[-10:]}. " \
        f"This confirms OBS-13: event history window or dedup evicts lifecycle " \
        f"events. Candidate PR to raise window size or exempt lifecycle from " \
        f"dedup/eviction."
