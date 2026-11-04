"""Typed external failures and complete-job CI verdicts; never infer from test prose."""
from __future__ import annotations

import json
import os
import re
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

from tests.e2e.lib.pytest_batches import BATCH_COUNT, verify_batch_coverage

TITLE = "pipeline-ci-outcome:v1"
TESTBED = "AlexBomber12/pipeline-orchestrator-testbed"


class ExternalDependencyError(RuntimeError):
    """An actual external operation failed with a diagnosed transient response."""

    def __init__(self, operation, reason):
        self.operation, self.reason = operation, reason
        detail = " (Internal Server Error)" if reason == "github_git_500" else ""
        super().__init__(f"external dependency unavailable: {operation}: {reason}{detail}")


class LifecycleTimeout(AssertionError, RuntimeError):
    """A bounded cross-service wait expired; the assertion is still a failure."""


def response_reason(stderr):
    # Only actual subprocess stderr from an external boundary is admitted.
    # No source excerpts, assertions, echoed commands or arbitrary bare numbers.
    for line in (stderr or "").splitlines():
        match = re.fullmatch(r"gh: (?:[^\n]* \(HTTP (500|502|503|504)\)|HTTP (500|502|503|504))", line.strip())
        if match:
            return "github_http_" + (match.group(1) or match.group(2))
        if re.fullmatch(r"remote: Internal Server Error\s*", line):
            return "github_git_500"
    return None


def require_external_response(result, operation):
    if result.returncode and (reason := response_reason(result.stderr)):
        raise ExternalDependencyError(operation, reason)


def classify_failure(exc):
    if isinstance(exc, ExternalDependencyError):
        return {"kind": "infra", "operation": exc.operation, "reason": exc.reason}
    if isinstance(exc, subprocess.TimeoutExpired):
        command = exc.cmd
        if isinstance(command, list) and command and (
            command[:2] == ["gh", "api"]
            or command[0] == "git" and "push" in command and "origin" in command
        ):
            return {"kind": "infra", "operation": "external_subprocess", "reason": "transport_timeout"}
    if isinstance(exc, LifecycleTimeout) and os.environ.get("GITHUB_ACTIONS") == "true":
        # Only explicitly identified lifecycle waits can be inconclusive when a
        # *fresh real* dependency probe fails. Ordinary assertion/DOM timeouts
        # never enter here. An available/unknown/auth-failed service preserves FIX.
        try:
            result = subprocess.run(
                ["gh", "api", f"repos/{TESTBED}/commits/main/check-runs?per_page=1"],
                text=True, capture_output=True, timeout=15, check=False,
            )
            reason = response_reason(result.stderr) if result.returncode else None
            if reason:
                return {"kind": "infra", "operation": "lifecycle_dependency_probe", "reason": reason}
        except (OSError, subprocess.TimeoutExpired):
            pass
    return {"kind": "failure"}


def source_identity():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    sha = event.get("pull_request", {}).get("head", {}).get("sha") or os.environ["GITHUB_SHA"]
    return {"repo": os.environ["GITHUB_REPOSITORY"], "sha": sha,
            "run_id": int(os.environ["GITHUB_RUN_ID"]), "attempt": int(os.environ["GITHUB_RUN_ATTEMPT"])}


def emit_job_verdict(directory):
    """All selected tests and failure stages must be present, across all batches."""
    verify_batch_coverage(directory)
    identity = source_identity()
    failures = []
    for batch in range(1, BATCH_COUNT + 1):
        manifest = json.loads((directory / f"batch-{batch}-collection.json").read_text())
        report = json.loads((directory / f"batch-{batch}-outcome.json").read_text())
        if (report["identity"] != identity or report["batch"] != batch
            or report["exitstatus"] not in (0, 1)
            or sorted(report["completed"]) != manifest["selected_nodeids"]
            or len(set(report["completed"])) != len(report["completed"])
            or report["collection_failed"] or report["skipped"]):
            raise ValueError("incomplete or conflicting CI failure evidence")
        cases = list(ET.parse(directory / f"batch-{batch}-results.xml").getroot().iter("testcase"))
        actual_failures = sum(len(case.findall("failure")) + len(case.findall("error")) for case in cases)
        if (len(cases) != len(report["completed"]) or actual_failures != len(report["failures"])
            or any(case.find("skipped") is not None for case in cases)):
            raise ValueError("pytest XML disagrees with complete failure accounting")
        failures.extend(report["failures"])
        if bool(report["failures"]) != bool(report["exitstatus"]):
            raise ValueError("pytest outcome disagrees with failure evidence")
    verdict = {**identity, "failed": len(failures),
               "infra": sum(item["classification"]["kind"] == "infra" for item in failures)}
    (directory / "ci-outcome.json").write_text(json.dumps(verdict, sort_keys=True) + "\n")
    if failures:
        message = json.dumps(verdict, separators=(",", ":"))
        # Workflow command escaping is required even though this payload has no
        # raw diagnostics. Preserve failure of the original test step.
        message = message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
        print(f"::error title={TITLE}::{message}")
    return verdict


if __name__ == "__main__":
    emit_job_verdict(Path("tests/e2e/evidence"))
