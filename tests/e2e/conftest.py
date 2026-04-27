import json
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pytest
import requests

from tests.e2e.lib.testbed_reset import clear_testbed_redis_state, reset_testbed_full

TEST_DASHBOARD_URL = "http://localhost:18800"
TESTBED_SLUG = "AlexBomber12__pipeline-orchestrator-testbed"
REPO_DIR = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = REPO_DIR / "tests/e2e/data"
EVIDENCE_DIR = REPO_DIR / "tests/e2e/evidence"

collect_ignore = ["data"]


def _stop_daemon_and_wait_paused(slug: str, timeout_sec: int = 30) -> None:
    response = requests.post(f"{TEST_DASHBOARD_URL}/repos/{slug}/stop", timeout=10)
    if response.status_code not in (200, 204):
        raise RuntimeError(f"failed to stop daemon for {slug}: status_code={response.status_code}")

    deadline = time.monotonic() + timeout_sec
    last_state = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{TEST_DASHBOARD_URL}/api/states", timeout=5) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
            pass
        else:
            entries = payload if isinstance(payload, list) else payload.get("states", [])
            for entry in entries:
                if entry.get("name") == slug or entry.get("slug") == slug:
                    last_state = entry.get("state")
                    if last_state == "PAUSED":
                        return
        time.sleep(0.5)

    raise RuntimeError(
        f"timed out after {timeout_sec}s waiting for daemon to pause for {slug}; "
        f"last seen state={last_state!r}"
    )


def _resume_daemon(slug: str) -> None:
    response = requests.post(f"{TEST_DASHBOARD_URL}/repos/{slug}/resume", timeout=10)
    if response.status_code not in (200, 204):
        raise RuntimeError(f"failed to resume daemon for {slug}: status_code={response.status_code}")


@pytest.fixture(scope="session", autouse=True)
def _reset_testbed_session():
    """Reset testbed to a known-clean state at session start.

    Closes open PRs, deletes non-main branches, wipes tasks/ on main, and
    clears Redis state for the testbed slug. Runs ONCE per pytest session
    before any test. The per-test reset_testbed fixture resets before each
    test and clears Redis state again at teardown.

    ``reset_testbed_full()`` raises on hard failures (listing call failed,
    clone/commit/push failed). We deliberately do NOT swallow that error:
    pytest will mark the session as errored, which is the signal we want —
    running e2e tests against a polluted testbed produces nondeterministic
    failures that are far worse than a loud setup abort.
    """
    counts = reset_testbed_full(TESTBED_SLUG)
    counts["redis_keys_deleted"] = clear_testbed_redis_state(TESTBED_SLUG)
    yield counts


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
            if entry.get("name") == slug or entry.get("slug") == slug:
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
    _stop_daemon_and_wait_paused(TESTBED_SLUG)
    try:
        reset_testbed_full(TESTBED_SLUG)
        clear_testbed_redis_state(TESTBED_SLUG)
    finally:
        _resume_daemon(TESTBED_SLUG)
    yield
    clear_testbed_redis_state(TESTBED_SLUG)


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
