"""Read-only preflight for CI evidence access on the testbed HEAD.

This catches the #531 failure class before stricter CI-evidence consumers
depend on these endpoints. The probe deliberately uses the integration
job's GitHub App installation token and never falls back to a personal
developer token: local runs skip before touching GitHub.
"""

from __future__ import annotations

import json
import os
import subprocess
from typing import Any

import pytest

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"

pytestmark = pytest.mark.skipif(
    os.environ.get("GITHUB_ACTIONS") != "true",
    reason=(
        "CI-evidence access preflight requires the GHA integration GitHub "
        "App token; skipping to avoid substituting a personal token."
    ),
)


def _gh_json(path: str, *, jq: str | None = None, paginate: bool = False) -> Any:
    command = ["gh", "api", "-H", "Accept: application/vnd.github+json", path]
    if paginate:
        command.extend(["--paginate", "--slurp"])
    if jq is not None:
        command.extend(["--jq", jq])
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        permission_hint = ""
        if "Resource not accessible by integration" in stderr or "HTTP 403" in stderr:
            permission_hint = (
                " This is a confirmed HTTP/permission error under the "
                "testbed GitHub App identity, not a WATCH timeout symptom."
            )
        raise AssertionError(
            f"GitHub App CI-evidence preflight failed for {path!r}: "
            f"rc={result.returncode}, stderr={stderr!r}.{permission_hint}"
        )
    if jq is not None:
        return result.stdout.strip()
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise AssertionError(
            f"GitHub App CI-evidence preflight got non-JSON from {path!r}: "
            f"{result.stdout[:200]!r}"
        ) from exc


def _require_integration_app_token() -> None:
    payload = _gh_json("installation/repositories")
    repositories = payload.get("repositories") if isinstance(payload, dict) else None
    if not isinstance(repositories, list):
        raise AssertionError(
            "GH_TOKEN is not a GitHub App installation token: "
            "GET /installation/repositories did not return repositories."
        )

    full_names = {
        repo.get("full_name")
        for repo in repositories
        if isinstance(repo, dict)
    }
    if TESTBED_REPO not in full_names:
        raise AssertionError(
            f"GitHub App installation token cannot see {TESTBED_REPO}; "
            "install the App on exactly the testbed repository per docs/ci-setup.md."
        )


def test_testbed_head_ci_evidence_sources_are_readable() -> None:
    """Both CI evidence REST sources must be readable by the integration App."""
    _require_integration_app_token()

    default_branch = _gh_json(f"repos/{TESTBED_REPO}", jq=".default_branch")
    assert default_branch, f"empty default branch for {TESTBED_REPO}"

    head_sha = _gh_json(
        f"repos/{TESTBED_REPO}/commits/{default_branch}",
        jq=".sha",
    )
    assert len(head_sha) == 40, (
        f"expected 40-character HEAD SHA for {TESTBED_REPO}@{default_branch}, "
        f"got {head_sha!r}"
    )

    check_run_pages = _gh_json(
        f"repos/{TESTBED_REPO}/commits/{head_sha}/check-runs?per_page=100",
        paginate=True,
    )
    assert isinstance(check_run_pages, list), (
        f"paginated check-runs response for {head_sha} was not a list: "
        f"{check_run_pages!r}"
    )
    check_runs: list[dict[str, Any]] = []
    total_count = 0
    for page in check_run_pages:
        assert isinstance(page, dict), (
            f"check-runs page for {head_sha} was not an object: {page!r}"
        )
        assert isinstance(page.get("total_count"), int), (
            f"check-runs page for {head_sha} lacked integer total_count: "
            f"{page!r}"
        )
        assert isinstance(page.get("check_runs"), list), (
            f"check-runs page for {head_sha} lacked a check_runs list: "
            f"{page!r}"
        )
        total_count = max(total_count, page["total_count"])
        check_runs.extend(run for run in page["check_runs"] if isinstance(run, dict))
    assert len(check_runs) >= total_count, (
        f"incomplete check-runs pagination for {head_sha}: "
        f"read {len(check_runs)} of {total_count}"
    )

    combined_status = _gh_json(f"repos/{TESTBED_REPO}/commits/{head_sha}/status")
    assert isinstance(combined_status, dict), (
        f"combined-status response for {head_sha} was not an object: "
        f"{combined_status!r}"
    )
    assert isinstance(combined_status.get("state"), str), (
        f"combined-status response for {head_sha} lacked string state: "
        f"{combined_status!r}"
    )
    assert isinstance(combined_status.get("statuses"), list), (
        f"combined-status response for {head_sha} lacked a statuses list: "
        f"{combined_status!r}"
    )
