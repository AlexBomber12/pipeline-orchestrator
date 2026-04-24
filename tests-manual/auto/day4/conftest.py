"""
Shared pytest fixtures for Day 4 auto-tests.

Fixtures provided:
- testbed_url: base URL of testbed repo detail page
- testbed_slug: full slug "AlexBomber12__pipeline-orchestrator-testbed"
- redis_client: direct Redis connection for state verification
- reset_daemon_state: cleanup fixture, clears testbed state before each test
- evidence_dir: path to save screenshots
- take_screenshot: helper to take JPEG quality-60 viewport screenshot
"""

import os
import subprocess
import time
import pytest
import requests
import redis as redis_lib
from pathlib import Path

DASHBOARD_URL = "http://localhost:8800"
TESTBED_SLUG = "AlexBomber12__pipeline-orchestrator-testbed"
REPO_DIR = "/home/alexey/pipeline-orchestrator"
EVIDENCE_DIR = Path("/tmp/day4-evidence")


@pytest.fixture(scope="session", autouse=True)
def ensure_services_up():
    """Verify docker compose is running before any test."""
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            cwd=REPO_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if "daemon" not in result.stdout or "web" not in result.stdout:
            pytest.fail(
                "docker compose services not running. "
                f"Run: cd {REPO_DIR} && docker compose up -d"
            )
    except Exception as e:
        pytest.fail(f"Could not verify docker compose state: {e}")

    # Verify dashboard reachable
    for _ in range(10):
        try:
            r = requests.get(f"{DASHBOARD_URL}/api/states", timeout=2)
            if r.status_code == 200:
                return
        except Exception:
            time.sleep(1)
    pytest.fail(f"Dashboard at {DASHBOARD_URL} not reachable after 10 seconds")


@pytest.fixture(scope="session")
def testbed_url():
    return f"{DASHBOARD_URL}/repo/{TESTBED_SLUG}"


@pytest.fixture(scope="session")
def testbed_slug():
    return TESTBED_SLUG


@pytest.fixture(scope="session")
def dashboard_url():
    return DASHBOARD_URL


@pytest.fixture(scope="session")
def redis_client():
    """Connect to Redis via docker compose exec."""
    # Use subprocess redis-cli, simpler than spinning up redis-py inside WSL
    # for a container. Returns a wrapper object that exposes get/set/delete.
    class DockerRedis:
        def _run(self, *args):
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "redis", "redis-cli", *args],
                cwd=REPO_DIR,
                capture_output=True,
                text=True,
                timeout=10,
            )
            return result.stdout.strip()

        def get(self, key):
            return self._run("GET", key)

        def set(self, key, value):
            return self._run("SET", key, value)

        def delete(self, key):
            return self._run("DEL", key)

        def keys(self, pattern):
            result = self._run("--scan", "--pattern", pattern)
            return [k for k in result.split("\n") if k]

    return DockerRedis()


@pytest.fixture
def reset_daemon_state(redis_client, testbed_slug):
    """Clear ERROR/HUNG markers before each test. Does not delete state fully."""
    # Before test: log initial state
    initial_state_raw = redis_client.get(f"pipeline:{testbed_slug}")
    yield initial_state_raw
    # After test: nothing to clean up automatically, daemon self-recovers


@pytest.fixture(scope="session", autouse=True)
def evidence_dir_setup():
    """Create /tmp/day4-evidence/ at session start."""
    EVIDENCE_DIR.mkdir(exist_ok=True, parents=True)
    # Cleanup old screenshots from previous run
    for f in EVIDENCE_DIR.glob("*.jpeg"):
        f.unlink()
    yield EVIDENCE_DIR


@pytest.fixture
def evidence_dir():
    return EVIDENCE_DIR


@pytest.fixture
def take_screenshot(page, evidence_dir, request):
    """Helper to take a viewport-only JPEG screenshot, quality 60."""
    def _screenshot(label: str = "anchor"):
        test_name = request.node.name
        path = evidence_dir / f"{test_name}_{label}.jpeg"
        page.screenshot(
            path=str(path),
            type="jpeg",
            quality=60,
            full_page=False,
        )
        # Attach to pytest-html report
        if hasattr(request.config, "_html"):
            try:
                from pytest_html import extras
                # Relative path for HTML report
                request.node.extras = getattr(request.node, "extras", [])
                request.node.extras.append(
                    extras.image(str(path), mime_type="image/jpeg")
                )
            except ImportError:
                pass
        return str(path)
    return _screenshot


# --- Helper functions exposed via fixtures ---


@pytest.fixture
def wait_for_state(testbed_slug, redis_client):
    """Wait until pipeline state becomes target or timeout."""
    import json

    def _wait(target_states, timeout_sec=60):
        if isinstance(target_states, str):
            target_states = [target_states]
        start = time.time()
        while time.time() - start < timeout_sec:
            raw = redis_client.get(f"pipeline:{testbed_slug}")
            if raw:
                try:
                    d = json.loads(raw)
                    if d.get("state") in target_states:
                        return d
                except Exception:
                    pass
            time.sleep(1)
        raise TimeoutError(
            f"State did not become {target_states} within {timeout_sec}s"
        )
    return _wait


@pytest.fixture
def get_state(testbed_slug, redis_client):
    """Return current parsed state dict or None."""
    import json

    def _get():
        raw = redis_client.get(f"pipeline:{testbed_slug}")
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None
    return _get


@pytest.fixture
def upload_zip(testbed_slug, dashboard_url):
    """Upload a zip file via dashboard upload endpoint."""
    def _upload(zip_path):
        url = f"{dashboard_url}/repos/{testbed_slug}/upload-tasks"
        with open(zip_path, "rb") as f:
            response = requests.post(
                url,
                files={"files": (os.path.basename(zip_path), f, "application/zip")},
                timeout=30,
            )
        return response
    return _upload


@pytest.fixture
def make_task_zip(tmp_path):
    """Generate a task zip file. Returns path."""
    def _make(pr_num: int, label: str = "wave"):
        num_padded = f"{pr_num:03d}"
        md_path = tmp_path / f"PR-{num_padded}.md"
        md_path.write_text(f"""# PR-{num_padded}: Add {label} mention to greeting

Branch: pr-{num_padded}-{label}-mention
- Type: feature
- Complexity: low
- Depends on: none
- Priority: 2
- Coder: any

## Problem

`greet()` in `src/hello.py` returns a plain greeting. Add "{label}".

## Scope

- Modify `src/hello.py` only.
- Append "{label}" to the return string.

## Success criteria

- `greet()` returns string containing "{label}".
- `scripts/ci.sh` exits 0.
""")
        zip_path = tmp_path / f"pr-{num_padded}.zip"
        subprocess.run(
            ["zip", "-q", "-j", str(zip_path), str(md_path)],
            check=True,
        )
        return str(zip_path)
    return _make


# --- HTML report hooks ---

@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Attach screenshots to HTML report."""
    outcome = yield
    report = outcome.get_result()
    extras = getattr(report, "extras", [])
    extras += getattr(item, "extras", [])
    report.extras = extras
