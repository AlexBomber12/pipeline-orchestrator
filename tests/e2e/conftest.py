import json
import os  # noqa: F401  # imported for upcoming PR-154 fixtures
import subprocess  # noqa: F401  # imported for upcoming PR-154 fixtures
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

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
