"""Recovery admission rejects stale/foreign work without request bookkeeping."""

from copy import deepcopy
from datetime import datetime, timezone

import pytest

from tests.e2e.test_fix_restart import _recovered_watch


@pytest.mark.parametrize("mismatch", [
    None, "task", "branch", "number", "head", "publication", "publication_head", "timestamp", "state",
])
def test_recovered_fix_watch_requires_fresh_published_identity(mismatch):
    head = "a" * 40
    after = datetime(2026, 10, 29, tzinfo=timezone.utc)
    state = {
        "state": "WATCH", "last_updated": "2026-10-29T00:00:01+00:00",
        "current_task": {"pr_id": "PR-433"},
        "current_pr": {
            "number": 673, "branch": "pr-433", "head_sha": head,
            "publication_at": "2026-10-28T23:59:00+00:00", "publication_head_sha": head,
        },
    }
    changed = deepcopy(state)
    if mismatch == "task":
        changed["current_task"]["pr_id"] = "PR-foreign"
    elif mismatch in {"branch", "number"}:
        changed["current_pr"][mismatch] = "foreign"
    elif mismatch in {"head", "publication_head"}:
        key = "head_sha" if mismatch == "head" else "publication_head_sha"
        changed["current_pr"][key] = "b" * 40
    elif mismatch == "publication":
        changed["current_pr"]["publication_at"] = None
    elif mismatch == "timestamp":
        changed["last_updated"] = after.isoformat()
    elif mismatch == "state":
        changed["state"] = "FIX"
    assert bool(_recovered_watch(changed, "PR-433", "pr-433", 673, head, after)) == (mismatch is None)
