"""Smoke test that confirms the e2e test stack is reachable and configured.

This test does not exercise the coder, upload tasks, or close PRs. It exists
solely to confirm that the infrastructure added by PR-153 is functional.
"""

from __future__ import annotations

import requests

VALID_STATES = {
    "PREFLIGHT",
    "IDLE",
    "CODING",
    "WATCH",
    "FIX",
    "MERGE",
    "ERROR",
    "HUNG",
    "PAUSED",
}


def test_stack_is_up_and_testbed_configured(
    dashboard_url: str, testbed_slug: str
) -> None:
    states_resp = requests.get(f"{dashboard_url}/api/states", timeout=5)
    assert states_resp.status_code == 200, states_resp.text

    entries = states_resp.json()
    assert isinstance(entries, list) and entries, "no repositories configured"

    matching = [
        e for e in entries if e.get("slug") == testbed_slug or e.get("name") == testbed_slug
    ]
    assert matching, (
        f"testbed slug {testbed_slug!r} not found in /api/states; "
        f"got {[e.get('slug') or e.get('name') for e in entries]}"
    )
    assert matching[0].get("state") in VALID_STATES, (
        f"unexpected state: {matching[0].get('state')!r}"
    )

    auth_resp = requests.get(f"{dashboard_url}/api/auth-status", timeout=5)
    assert auth_resp.status_code == 200, auth_resp.text
    auth = auth_resp.json()
    assert auth.get("claude", {}).get("status") == "ok", auth.get("claude")
    assert auth.get("gh", {}).get("status") == "ok", auth.get("gh")
