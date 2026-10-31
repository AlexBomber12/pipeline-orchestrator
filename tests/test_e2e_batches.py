"""Exercise the real pytest batch plugin and its unchanged required-check boundary."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest
import yaml

from tests.e2e.lib.pytest_batches import BATCH_COUNT, verify_batch_coverage, verify_runtime_writes

ROOT = Path(__file__).resolve().parents[1]


def run_pytest(directory, *args):
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "tests.e2e.lib.pytest_batches", *args],
        cwd=directory, env={**os.environ, "PYTHONPATH": str(ROOT), "PYTEST_ADDOPTS": ""},
        capture_output=True, text=True, timeout=30,
    )


def test_real_pytest_batches_execute_every_parameter_once_and_include_new_modules(tmp_path):
    # New files, both standard pytest naming patterns, parameters, and multiple
    # functions are collected normally. No maintained CI allowlist can omit them.
    for name in ["test_alpha.py", "test_beta.py", "test_gamma.py", "test_new.py", "extra_test.py"]:
        (tmp_path / name).write_text(
            'from pathlib import Path\nimport pytest\n'
            '@pytest.mark.parametrize("case", ["first", "second"])\n'
            'def test_case(case):\n'
            '    with Path("executed.txt").open("a") as stream:\n'
            '        stream.write(f"{Path(__file__).name}:{case}\\n")\n'
        )
    evidence = tmp_path / "tests/e2e/evidence"
    for batch in range(1, BATCH_COUNT + 1):
        result = run_pytest(tmp_path, f"--e2e-batch={batch}", "-q")
        assert result.returncode == 0, result.stdout + result.stderr
    assert verify_batch_coverage(evidence) == 10
    executed = (tmp_path / "executed.txt").read_text().splitlines()
    assert len(executed) == 10 and len(set(executed)) == 10
    manifests = [json.loads((evidence / f"batch-{i}-collection.json").read_text())
                 for i in range(1, BATCH_COUNT + 1)]
    assert all(len(set(n.split("::")[0] for n in m["selected_nodeids"])) for m in manifests)
    assigned = Counter(n for manifest in manifests for n in manifest["selected_nodeids"])
    assert assigned == Counter(manifests[0]["all_nodeids"])
    # Ordinary local/full invocations still run every test without batching.
    result = run_pytest(tmp_path, "-q")
    assert result.returncode == 0, result.stdout + result.stderr
    assert Counter((tmp_path / "executed.txt").read_text().splitlines()) == {
        item: 2 for item in executed
    }
    # CI rejects missing, inconsistent or duplicated collection evidence.
    original = (evidence / "batch-2-collection.json").read_text()
    for field, value, message in [("all_nodeids", [], "disagree"),
                                  ("selected_nodeids", [], "empty"),
                                  ("selected_nodeids", manifests[0]["selected_nodeids"], "duplicated or omitted")]:
        broken = json.loads(original)
        broken[field] = value
        (evidence / "batch-2-collection.json").write_text(json.dumps(broken))
        with pytest.raises(ValueError, match=message):
            verify_batch_coverage(evidence)
    (evidence / "batch-2-collection.json").unlink()
    with pytest.raises(FileNotFoundError):
        verify_batch_coverage(evidence)


def test_invalid_or_empty_batch_is_not_a_green_check(tmp_path):
    (tmp_path / "test_single.py").write_text("def test_single(): pass\n")
    for batch in [0, 4, 2]:
        result = run_pytest(tmp_path, f"--e2e-batch={batch}", "-q")
        assert result.returncode != 0


def test_ci_keeps_one_serialized_gate_and_refreshes_both_roles_per_bounded_batch():
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    integration = workflow["jobs"]["integration"]
    assert integration["needs"] == "unit" and "strategy" not in integration
    assert integration["concurrency"] == {
        "group": "integration-testbed-shared", "cancel-in-progress": False, "queue": "max",
    }
    batches = [step for step in integration["steps"] if step.get("uses") == "./.github/actions/testbed-batch"]
    assert [int(step["with"]["batch"]) for step in batches] == list(range(1, BATCH_COUNT + 1))
    for step in batches:
        assert 0 < step["timeout-minutes"] <= 45 < 60
        assert "!cancelled()" in step["if"] and "build-test-stack.outcome == 'success'" in step["if"]
        assert "continue-on-error" not in step
    action = yaml.safe_load((ROOT / ".github/actions/testbed-batch/action.yml").read_text())
    steps = action["runs"]["steps"]
    mint = [step for step in steps if step.get("uses") == "actions/create-github-app-token@v1"]
    assert [step["id"] for step in mint] == ["testbed-token", "testbed-reviewer-token"]
    assert all(step["with"]["repositories"] == "pipeline-orchestrator-testbed" for step in mint)
    seed = next(step for step in steps if step.get("name") == "Seed gh auth for in-container shim")
    assert "sudo chown -R 1000:1000 tests/e2e/data\n" in seed["run"]
    assert any("verify_runtime_writes(Path(" in step.get("run", "") for step in steps)
    host = next(step for step in steps if step.get("name") == "Restore host shim control access")
    start = next(step for step in steps if step.get("name") == "Bring up test stack")
    assert steps.index(seed) < steps.index(host) < steps.index(start)
    assert 'sudo chown "$(id -u):$(id -g)" "$control"' in host["run"]
    tests = next(step for step in steps if step.get("name") == "Run e2e tests")
    assert tests["env"] == {
        "GH_TOKEN": "${{ steps.testbed-token.outputs.token }}",
        "TESTBED_REVIEWER_TOKEN": "${{ steps.testbed-reviewer-token.outputs.token }}",
    }
    assert '--e2e-batch=${{ inputs.batch }}' in tests["run"]
    assert steps[-1]["if"] == "always()" and steps[-1]["run"].endswith("down -v")
    assert any("tests.e2e.lib.pytest_batches" in step.get("run", "") for step in integration["steps"])


@pytest.mark.parametrize("readonly", ["file", "directory"])
def test_retained_state_probe_checks_real_permissions_without_changing_bytes(tmp_path, readonly):
    analytics = tmp_path / "analytics"
    audit = tmp_path / "audit"
    analytics.mkdir()
    audit.mkdir()
    lock = analytics / ".outcomes.lock"
    journal = audit / "operator-actions.jsonl"
    lock.write_bytes(b"retained lock")
    journal.write_bytes(b"retained audit\n")
    blocked = lock if readonly == "file" else audit
    blocked.chmod(0o444 if readonly == "file" else 0o555)
    try:
        with pytest.raises(PermissionError):
            verify_runtime_writes(tmp_path)
    finally:
        blocked.chmod(0o644 if readonly == "file" else 0o755)
    verify_runtime_writes(tmp_path)
    assert lock.read_bytes() == b"retained lock"
    assert journal.read_bytes() == b"retained audit\n"
    assert sorted(p.name for p in analytics.iterdir()) == [".outcomes.lock"]
    assert sorted(p.name for p in audit.iterdir()) == ["operator-actions.jsonl"]


def test_container_state_probe_imports_and_runs_without_site_packages(tmp_path):
    # daemon-test installs only production dependencies. -S reproduces the
    # stronger boundary of no third-party packages, while host pytest still
    # exercises the plugin hooks in the other regression tests.
    code = (
        'import importlib.util; assert importlib.util.find_spec("pytest") is None; '
        'from pathlib import Path; '
        'from tests.e2e.lib.pytest_batches import verify_runtime_writes; '
        f'verify_runtime_writes(Path({str(tmp_path)!r}))'
    )
    result = subprocess.run(
        [sys.executable, "-S", "-c", code], cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT)}, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr


def test_host_shim_preflight_checks_retained_control_without_changing_evidence(tmp_path, monkeypatch):
    from tests.e2e.lib import coder_shim as shim

    control = tmp_path / "shim-scenario"
    handshake = tmp_path / "shim-scenario.settings-started"
    control.write_text("fix_existing_pr\n")
    handshake.write_bytes(b"retained launch evidence")
    original_owner = handshake.stat().st_uid
    monkeypatch.setattr(shim, "SHIM_SCENARIO_PATH", control)
    action = yaml.safe_load((ROOT / ".github/actions/testbed-batch/action.yml").read_text())
    step = next(s for s in action["runs"]["steps"] if s.get("name") == "Restore host shim control access")
    probe = step["run"].split("python - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    control.chmod(0o444)
    try:
        with pytest.raises(PermissionError):
            exec(compile(probe, "workflow-host-shim-probe", "exec"), {})
    finally:
        control.chmod(0o644)
    exec(compile(probe, "workflow-host-shim-probe", "exec"), {})
    assert control.read_text() == "fix_existing_pr\n"
    assert handshake.read_bytes() == b"retained launch evidence"
    assert handshake.stat().st_uid == original_owner
