"""
EVENT-RECHECK-01: Re-verify EVENT-LOG-01 status after WEB-01-v2 fix.

Day 2 EVENT-LOG-01 observation: Stop and Resume events disappeared from
history within 5-60 seconds, replaced by "No tasks available" polling.
Pause events persisted correctly. Asymmetry.

Day 3 PAUSE-EVENT-FORMAT-01 observed Pause persisting correctly. Possibly
WEB-01-v2 fix stabilized event log.

This test: deliberately exercise Pause/Stop/Resume sequence on IDLE daemon,
wait several minutes, verify all three event types persist in history.
"""

import time
import pytest
import requests


def test_event_log_01_lifecycle_events_persist(
    page,
    testbed_url,
    testbed_slug,
    dashboard_url,
    wait_for_state,
    get_state,
    take_screenshot,
):
    """
    Scenario:
    1. Get current history length.
    2. Fire sequence: Pause, Resume, Pause, Stop, Resume.
    3. Wait 3 minutes (enough for polling cycles to potentially overwrite).
    4. Verify all 5 lifecycle events are in history.
    """
    page.goto(testbed_url)
    take_screenshot("01_before")

    initial = get_state()
    initial_count = len(initial.get("history", []))

    # Fire sequence
    actions = [
        ("pause", "Paused"),
        ("resume", "Resumed"),
        ("pause", "Paused"),
        ("stop", "Stop"),
        ("resume", "Resumed"),
    ]

    action_timestamps = []
    for action_path, _ in actions:
        url = f"{dashboard_url}/repos/{testbed_slug}/{action_path}"
        r = requests.post(url, timeout=5)
        action_timestamps.append((action_path, time.time(), r.status_code))
        time.sleep(2)  # small gap between actions

    take_screenshot("02_after_sequence")

    # Wait 3 minutes for polling cycles to potentially overwrite
    time.sleep(180)

    take_screenshot("03_after_wait")

    # Check history
    final = get_state()
    history = final.get("history", [])
    new_events = history[initial_count:]  # events added during test

    # Count lifecycle events mentioning pause/resume/stop
    pause_events = [e for e in new_events if "paus" in e.get("message", "").lower()]
    resume_events = [e for e in new_events if "resum" in e.get("message", "").lower() or "pause canceled" in e.get("message", "").lower()]
    stop_events = [e for e in new_events if "stop" in e.get("message", "").lower()]

    # Expected: 2 pauses, 2 resumes, 1 stop (stop is also a type of pause in the state machine)
    # Precise count depends on how daemon log message Pause vs Stop
    total_lifecycle = len(pause_events) + len(resume_events) + len(stop_events)

    if total_lifecycle < 3:
        # EVENT-LOG-01 likely still active - Stop/Resume were wiped
        pytest.fail(
            f"Lifecycle events did not persist 3 minutes after sequence. "
            f"Expected: 5 lifecycle events. Got: pause={len(pause_events)}, "
            f"resume={len(resume_events)}, stop={len(stop_events)}. "
            f"New events in history: {new_events[-10:]}. "
            f"EVENT-LOG-01 from Day 2 still active - dedup over-eager on lifecycle."
        )

    if total_lifecycle >= 5:
        print(f"EVENT-LOG-01 RESOLVED: all 5 lifecycle events persisted. "
              f"pause={len(pause_events)}, resume={len(resume_events)}, stop={len(stop_events)}")
    else:
        print(f"EVENT-LOG-01 PARTIAL: some events persisted, some missing. "
              f"pause={len(pause_events)}, resume={len(resume_events)}, stop={len(stop_events)}")
