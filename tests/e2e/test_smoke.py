import json
import urllib.request


def test_smoke_dashboard_responds(dashboard_url, testbed_slug):
    known_states = {
        "IDLE",
        "CODING",
        "WATCH",
        "FIX",
        "MERGE",
        "ERROR",
        "HUNG",
        "PAUSED",
        "PREFLIGHT",
    }

    with urllib.request.urlopen(f"{dashboard_url}/api/states", timeout=5) as resp:
        assert resp.status == 200
        payload = json.loads(resp.read().decode("utf-8"))

    assert isinstance(payload, list), f"expected list, got {type(payload).__name__}"

    matching = [
        entry
        for entry in payload
        if entry.get("name") == testbed_slug or entry.get("slug") == testbed_slug
    ]
    assert matching, f"no entry with slug={testbed_slug!r} in {payload!r}"

    state = matching[0].get("state")
    assert state in known_states, f"unexpected state {state!r}"
