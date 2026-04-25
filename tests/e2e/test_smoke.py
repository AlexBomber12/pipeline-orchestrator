import json
import urllib.request


def test_smoke_dashboard_responds(dashboard_url):
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
    assert payload, f"expected at least one repo entry, got empty list: {payload!r}"

    for entry in payload:
        state = entry.get("state")
        assert state in known_states, f"unexpected state {state!r} in entry {entry!r}"
