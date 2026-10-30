"""Three sequential pytest module batches, each with fresh CI credentials.

The default local invocation still collects/runs the full suite. No allowlist
is maintained: new modules and every parameterized item are assigned by the
real pytest collection, and CI verifies the disjoint union before accepting it.
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

BATCH_COUNT = 3


def pytest_addoption(parser):
    parser.addoption("--e2e-batch", type=int, choices=range(1, BATCH_COUNT + 1), default=None,
                     help="Run one of three complete module batches with its own CI credentials")


def pytest_collection_modifyitems(config, items):
    # Only the host pytest hook needs this dependency. The runtime container
    # imports the same module for its standard-library-only write probe.
    import pytest

    batch = config.getoption("--e2e-batch")
    if batch is None:
        return
    all_ids = [item.nodeid for item in items]
    modules = sorted({nodeid.split("::", 1)[0] for nodeid in all_ids})
    owned = set(modules[batch - 1::BATCH_COUNT])
    selected = [item for item in items if item.nodeid.split("::", 1)[0] in owned]
    if not selected:
        raise pytest.UsageError(f"e2e batch {batch}/{BATCH_COUNT} has no collected tests")
    evidence = config.rootpath / "tests/e2e/evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    (evidence / f"batch-{batch}-collection.json").write_text(json.dumps({
        "batch": batch, "batch_count": BATCH_COUNT,
        "all_nodeids": sorted(all_ids), "selected_nodeids": sorted(item.nodeid for item in selected),
    }, indent=2) + "\n")
    config.hook.pytest_deselected(items=[item for item in items if item not in selected])
    items[:] = selected


def verify_runtime_writes(data: Path) -> None:
    """Probe real create/append permissions without altering retained content."""
    for name in ("analytics", "audit"):
        directory = data / name
        directory.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(dir=directory) as probe:
            probe.write(b"testbed state write probe")
        for path in directory.rglob("*"):
            if path.is_file() and not path.is_symlink():
                # Existing lock/JSONL files can fail even when directory creation
                # succeeds. Append-open checks that boundary without writing.
                with path.open("ab"):
                    pass


def verify_batch_coverage(evidence: Path) -> int:
    manifests = [json.loads((evidence / f"batch-{batch}-collection.json").read_text())
                 for batch in range(1, BATCH_COUNT + 1)]
    all_ids = manifests[0]["all_nodeids"]
    selected = []
    for batch, manifest in enumerate(manifests, 1):
        if (manifest["batch"], manifest["batch_count"], manifest["all_nodeids"]) != (batch, BATCH_COUNT, all_ids):
            raise ValueError("e2e batch collections disagree")
        if not manifest["selected_nodeids"]:
            raise ValueError("empty e2e batch")
        selected.extend(manifest["selected_nodeids"])
    if not all_ids or len(set(all_ids)) != len(all_ids) or sorted(selected) != all_ids:
        raise ValueError("e2e batches duplicated or omitted collected tests")
    return len(all_ids)


if __name__ == "__main__":
    count = verify_batch_coverage(Path("tests/e2e/evidence"))
    print(f"Verified complete disjoint e2e coverage: {count} tests across {BATCH_COUNT} credential batches")
