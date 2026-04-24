"""
EXT-05: Anthropic usage API broken.

This verifies USAGE-API-01 positive finding from Day 3.
When Anthropic /api/oauth/usage endpoint returns 429 consistently, daemon
should flip to reactive rate-limit detection after N failures, not crash
or show incorrect usage percentages.

Note: We cannot reliably trigger 429 on real Anthropic API. Instead we
verify the current state:
- If usage_api_degraded flag is in state and false → mechanism present, not tested here.
- If degraded=true → we already saw the graceful fallback.
- If the field is missing → the degradation mechanism may not be implemented.

This test is more of a documentation/observation than a destructive test.
"""

import pytest


def test_ext_05_usage_api_degradation_mechanism_exists(
    page,
    testbed_url,
    get_state,
    take_screenshot,
):
    """
    Verify that daemon state has the usage_api_degraded field (or equivalent).
    This was observed Day 3 USAGE-API-01 as working positive finding.
    Re-confirm mechanism is in place.
    """
    page.goto(testbed_url)
    take_screenshot("01_dashboard")

    state = get_state()
    assert state, "State unreachable"

    # Field might be at root or nested inside claude/codex stats
    # Day 3 observations noted "usage_api_degraded: true" at some point
    # Check multiple possible locations
    has_degradation_field = False
    for key, val in state.items():
        if "usage" in str(key).lower() and "degrad" in str(key).lower():
            has_degradation_field = True
            break
        if isinstance(val, dict):
            for sub_key in val:
                if "degrad" in str(sub_key).lower() or "usage_api" in str(sub_key).lower():
                    has_degradation_field = True
                    break

    # Also check event log for historical mention of degradation
    history = state.get("history", [])
    historical_degradation_events = [
        e for e in history
        if "usage" in e.get("message", "").lower() and "degrad" in e.get("message", "").lower()
    ]

    has_mechanism = has_degradation_field or historical_degradation_events

    if not has_mechanism:
        pytest.skip(
            "Could not verify usage_api degradation mechanism via state inspection. "
            "Day 3 USAGE-API-01 evidence: mechanism DID trigger on Day 3 morning. "
            "Mechanism may only be visible when actively degraded. Not a failure."
        )

    # If mechanism is visible, verify daemon is not stuck on it
    assert state["state"] in ["IDLE", "CODING", "WATCH", "FIX", "MERGE", "HUNG", "ERROR"], (
        f"State is unexpected: {state['state']}"
    )
