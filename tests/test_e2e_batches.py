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

from tests.e2e.lib.pytest_batches import BATCH_COUNT, verify_batch_coverage

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
    tests = next(step for step in steps if step.get("name") == "Run e2e tests")
    assert tests["env"] == {
        "GH_TOKEN": "${{ steps.testbed-token.outputs.token }}",
        "TESTBED_REVIEWER_TOKEN": "${{ steps.testbed-reviewer-token.outputs.token }}",
    }
    assert '--e2e-batch=${{ inputs.batch }}' in tests["run"]
    assert steps[-1]["if"] == "always()" and steps[-1]["run"].endswith("down -v")
    assert any("tests.e2e.lib.pytest_batches" in step.get("run", "") for step in integration["steps"])
