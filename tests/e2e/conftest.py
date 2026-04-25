import json
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pytest
import requests

TEST_DASHBOARD_URL = "http://localhost:18800"
TESTBED_SLUG = "AlexBomber12__pipeline-orchestrator-testbed"
TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
REPO_DIR = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = REPO_DIR / "tests/e2e/data"
EVIDENCE_DIR = REPO_DIR / "tests/e2e/evidence"


@pytest.fixture(scope="session")
def dashboard_url():
    return TEST_DASHBOARD_URL


@pytest.fixture(scope="session")
def testbed_slug():
    return TESTBED_SLUG


@pytest.fixture(scope="session")
def testbed_url():
    return f"{TEST_DASHBOARD_URL}/repo/{TESTBED_SLUG}"


@pytest.fixture
def get_state():
    def _get_state(slug=TESTBED_SLUG):
        url = f"{TEST_DASHBOARD_URL}/api/states"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
            return None
        entries = payload if isinstance(payload, list) else payload.get("states", [])
        for entry in entries:
            if entry.get("slug") == slug:
                return entry
        return None

    return _get_state


@pytest.fixture
def wait_for_state(get_state):
    def _wait_for_state(states, timeout_sec=30, slug=TESTBED_SLUG, poll_interval_sec=1.0):
        deadline = time.monotonic() + timeout_sec
        last_state = None
        while time.monotonic() < deadline:
            entry = get_state(slug)
            if entry is not None:
                last_state = entry.get("state")
                if last_state in states:
                    return entry
            time.sleep(poll_interval_sec)
        raise TimeoutError(
            f"Timed out after {timeout_sec}s waiting for state in {states!r}; "
            f"last seen state={last_state!r}"
        )

    return _wait_for_state


@pytest.fixture
def make_task_zip(tmp_path):
    def _make_task_zip(
        pr_id: int,
        title_slug: str,
        coder: str = "any",
        priority: int = 2,
    ) -> Path:
        body = (
            f"# PR-{pr_id}: {title_slug}\n"
            "\n"
            f"Branch: pr-{pr_id}-{title_slug}\n"
            "- Type: feature\n"
            "- Complexity: low\n"
            "- Depends on: none\n"
            f"- Priority: {priority}\n"
            f"- Coder: {coder}\n"
            "\n"
            "## Problem\n"
            f"e2e test placeholder for PR-{pr_id}.\n"
            "\n"
            "## Scope\n"
            "Trivial scope. Touch a marker file.\n"
            "\n"
            "## Files to create\n"
            "None.\n"
            "\n"
            "## Files to touch\n"
            "tests/e2e-shim-marker.txt: append a marker line.\n"
            "\n"
            "## Files NOT to touch\n"
            "Anything else.\n"
            "\n"
            "## Success criteria\n"
            "1. The marker file gains one line.\n"
        )
        md_name = f"PR-{pr_id}.md"
        md_path = tmp_path / md_name
        md_path.write_text(body)
        zip_path = tmp_path / f"PR-{pr_id}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(md_path, arcname=md_name)
        return zip_path

    return _make_task_zip


@pytest.fixture
def upload_zip():
    def _upload_zip(zip_path: Path, slug: str = TESTBED_SLUG) -> int:
        url = f"{TEST_DASHBOARD_URL}/repos/{slug}/upload-tasks"
        with open(zip_path, "rb") as fh:
            response = requests.post(
                url,
                files={"files": (zip_path.name, fh, "application/zip")},
                timeout=30,
            )
        return response.status_code

    return _upload_zip


@pytest.fixture
def reset_testbed():
    yield
    listing = subprocess.run(
        [
            "gh",
            "pr",
            "list",
            "-R",
            TESTBED_REPO,
            "--state",
            "open",
            "--json",
            "number",
            "--jq",
            ".[].number",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    for line in listing.stdout.splitlines():
        number = line.strip()
        if not number:
            continue
        subprocess.run(
            [
                "gh",
                "pr",
                "close",
                number,
                "-R",
                TESTBED_REPO,
                "--delete-branch",
            ],
            capture_output=True,
            text=True,
            check=False,
        )


@pytest.fixture
def take_screenshot(request):
    def _take_screenshot(name):
        import playwright.sync_api  # noqa: F401  # lazy import; PR-153c installs playwright

        page = request.getfixturevalue("page")
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        path = EVIDENCE_DIR / f"{timestamp}_{name}.png"
        page.screenshot(path=str(path))
        return path

    return _take_screenshot
