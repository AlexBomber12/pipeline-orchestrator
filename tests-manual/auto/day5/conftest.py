"""
Shared pytest fixtures for Day 5 auto-tests.

Day 5 fixes incorporated vs Day 4:
- OBS-8: Screenshots embedded as base64 data URL, visible inline in HTML report.
- OBS-10: take_screenshot waits for hydration marker before capturing.
- FINDING-2: make_task_zip supports coder parameter (any/claude/codex).
- Helper reset_testbed_clean: post-test cleanup of open PRs to avoid cross-test contamination.
- Paths: /tmp/day5-evidence, /tmp/day5-report.html.
"""

import base64
import json
import os
import subprocess
import time
from pathlib import Path

import pytest
import requests

DASHBOARD_URL = "http://localhost:8800"
TESTBED_SLUG = "AlexBomber12__pipeline-orchestrator-testbed"
TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
REPO_DIR = "/home/alexey/pipeline-orchestrator"
EVIDENCE_DIR = Path("/tmp/day5-evidence")
HYDRATION_SELECTOR = "header, main, [data-state-indicator]"
HYDRATION_TIMEOUT_MS = 5000


@pytest.fixture(scope="session", autouse=True)
def ensure_services_up():
    """Verify docker compose services are running before any test.

    Uses `docker compose ps --services --filter status=running` which returns
    service names one per line. Compose-v2 safe (see OBS-6 Day 4).
    """
    try:
        result = subprocess.run(
            ["docker", "compose", "ps", "--services", "--filter", "status=running"],
            cwd=REPO_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        running = set(result.stdout.strip().splitlines())
        required = {"daemon", "web", "redis"}
        missing = required - running
        if missing:
            pytest.fail(
                f"docker compose services not running: {missing}. "
                f"Running services: {running}. "
                f"Fix: cd {REPO_DIR} && docker compose up -d"
            )
    except Exception as e:
        pytest.fail(f"Could not verify docker compose state: {e}")

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
def testbed_repo():
    return TESTBED_REPO


@pytest.fixture(scope="session")
def dashboard_url():
    return DASHBOARD_URL


@pytest.fixture(scope="session")
def redis_client():
    """Redis via docker compose exec redis-cli wrapper."""

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
    """Log pre-test state. No destructive action - daemon self-recovers.

    Day 5 note: per-test explicit cleanup is up to the test via reset_testbed_clean
    fixture below.
    """
    initial_state_raw = redis_client.get(f"pipeline:{testbed_slug}")
    yield initial_state_raw


@pytest.fixture
def reset_testbed_clean(testbed_repo, redis_client, testbed_slug, dashboard_url):
    """Close any open PRs and clear leftover task files after test.

    Call this yield-style at test end when test creates PRs or task files
    that would contaminate subsequent tests. No-op if repo is already clean.
    """

    def _cleanup():
        try:
            result = subprocess.run(
                ["gh", "pr", "list", "--repo", testbed_repo, "--state", "open",
                 "--json", "number,headRefName"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0 and result.stdout.strip():
                prs = json.loads(result.stdout)
                for pr in prs:
                    subprocess.run(
                        ["gh", "pr", "close", str(pr["number"]),
                         "--repo", testbed_repo, "--delete-branch"],
                        capture_output=True, timeout=15,
                    )
        except Exception:
            pass

        try:
            requests.post(
                f"{dashboard_url}/repos/{testbed_slug}/recover",
                timeout=5,
            )
        except Exception:
            pass

    yield _cleanup
    _cleanup()


@pytest.fixture(scope="session", autouse=True)
def evidence_dir_setup():
    """Create /tmp/day5-evidence/ at session start, clean old screenshots."""
    EVIDENCE_DIR.mkdir(exist_ok=True, parents=True)
    for f in EVIDENCE_DIR.glob("*.jpeg"):
        f.unlink()
    yield EVIDENCE_DIR


@pytest.fixture
def evidence_dir():
    return EVIDENCE_DIR


@pytest.fixture
def take_screenshot(page, evidence_dir, request):
    """Viewport-only JPEG screenshot, quality 60, with hydration wait and base64 embed.

    Day 5 improvements (vs Day 4):
    - OBS-10 fix: wait for hydration selector before capturing (prevents white screens).
    - OBS-8 fix: embed as base64 data URL so self-contained HTML report shows images inline.
    """

    def _screenshot(label: str = "anchor"):
        test_name = request.node.name
        path = evidence_dir / f"{test_name}_{label}.jpeg"

        try:
            page.wait_for_selector(HYDRATION_SELECTOR, timeout=HYDRATION_TIMEOUT_MS)
        except Exception:
            pass

        page.screenshot(
            path=str(path),
            type="jpeg",
            quality=60,
            full_page=False,
        )

        if hasattr(request.config, "_html"):
            try:
                from pytest_html import extras
                img_bytes = path.read_bytes()
                b64 = base64.b64encode(img_bytes).decode()
                data_url = f"data:image/jpeg;base64,{b64}"
                request.node.extras = getattr(request.node, "extras", [])
                request.node.extras.append(
                    extras.image(data_url, mime_type="image/jpeg")
                )
            except Exception:
                pass
        return str(path)

    return _screenshot


@pytest.fixture
def wait_for_state(testbed_slug, redis_client):
    """Wait until pipeline state becomes target or timeout."""

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
    """Generate a task zip file. Day 5: supports coder pin param (OBS-12 fix for FINDING-2).

    Args:
        pr_num: integer, e.g. 301 (Day 5 uses PR-3XX range to avoid roadmap collision)
        label: short identifier used in branch name and commit message
        coder: one of "any", "claude", "codex". Default "any".
    """

    def _make(pr_num: int, label: str = "wave", coder: str = "any"):
        num_padded = f"{pr_num:03d}"
        md_path = tmp_path / f"PR-{num_padded}.md"
        md_path.write_text(f"""# PR-{num_padded}: Add {label} mention to greeting

Branch: pr-{num_padded}-{label}-mention
- Type: feature
- Complexity: low
- Depends on: none
- Priority: 2
- Coder: {coder}

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


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Attach screenshots to HTML report."""
    outcome = yield
    report = outcome.get_result()
    extras = getattr(report, "extras", [])
    extras += getattr(item, "extras", [])
    report.extras = extras
