import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pytest
import requests

from tests.e2e.lib.testbed_reset import (
    clear_testbed_redis_state,
    reset_testbed_full,
    wipe_tasks_dir_on_main,
)

TEST_DASHBOARD_URL = "http://localhost:18800"
TEST_REDIS_CONTAINER = "pipeline-orchestrator-redis-test"
TESTBED_SLUG = "AlexBomber12__pipeline-orchestrator-testbed"
TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
REPO_DIR = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = REPO_DIR / "tests/e2e/data"
EVIDENCE_DIR = REPO_DIR / "tests/e2e/evidence"

collect_ignore = ["data"]


def _default_test_redis_url() -> str:
    try:
        ip = subprocess.check_output(
            [
                "docker",
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{println .IPAddress}}{{end}}",
                TEST_REDIS_CONTAINER,
            ],
            text=True,
            timeout=2,
        ).split()[0]
    except (IndexError, OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return "redis://redis-test:6379/0"
    return f"redis://{ip}:6379/0"


TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL") or _default_test_redis_url()


@pytest.fixture(scope="session", autouse=True)
def _reset_testbed_session():
    """Reset testbed to a known-clean state at session start.

    Closes open PRs, deletes non-main branches, wipes tasks/ on main, and
    clears daemon Redis state. Runs ONCE per pytest session before any test.
    The per-test reset_testbed fixture handles lighter cleanup between
    individual tests.

    ``reset_testbed_full()`` raises on hard failures (listing call failed,
    clone/commit/push failed). We deliberately do NOT swallow that error:
    pytest will mark the session as errored, which is the signal we want —
    running e2e tests against a polluted testbed produces nondeterministic
    failures that are far worse than a loud setup abort.
    """
    counts = reset_testbed_full(TEST_REDIS_URL, TESTBED_SLUG)
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


def _close_open_testbed_prs() -> None:
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


def _reset_testbed_state() -> None:
    _close_open_testbed_prs()
    wipe_tasks_dir_on_main()
    clear_testbed_redis_state(TEST_REDIS_URL, TESTBED_SLUG)


@pytest.fixture
def reset_testbed():
    _reset_testbed_state()
    yield
    _reset_testbed_state()


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
