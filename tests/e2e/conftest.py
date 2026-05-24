import json
import subprocess
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


# 30 seconds proved insufficient on the shared testbed where dispatch was observed at ~39 seconds.
def _stop_daemon_and_wait_paused(slug: str, timeout_sec: int = 60) -> None:
    response = requests.post(f"{TEST_DASHBOARD_URL}/repos/{slug}/stop", timeout=10)
    if response.status_code not in (200, 204):
        raise RuntimeError(f"failed to stop daemon for {slug}: status_code={response.status_code}")

    deadline = time.monotonic() + timeout_sec
    last_state = None
    last_user_paused = None
    last_event = None
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
                    last_user_paused = entry.get("user_paused")
                    history = entry.get("history") or []
                    last_event = history[-1].get("event") if history and isinstance(history[-1], dict) else None
                    if last_state == "PAUSED" or (
                        last_user_paused is True
                        and last_event == "[INFRA] Paused. Press Play to resume."
                    ):
                        return
        time.sleep(0.5)

    raise RuntimeError(
        f"timed out after {timeout_sec}s waiting for daemon to pause for {slug}; "
        f"last seen state={last_state!r}, user_paused={last_user_paused!r}, "
        f"last event={last_event!r}"
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
            "---\n"
            "status: TODO\n"
            "---\n"
            "\n"
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
def make_task_zip_multi(tmp_path):
    """Build a single zip containing multiple PR-*.md task files.

    ``tasks`` is a list of ``(pr_id, title_slug, depends_on)`` tuples
    where ``depends_on`` is a list of ``"PR-N"`` strings (empty for
    independent tasks). Each entry is rendered with the same body
    template as ``make_task_zip`` so the only variations are the
    ``pr_id`` and the ``Depends on:`` line. A 4-tuple
    ``(pr_id, title_slug, depends_on, priority)`` overrides the
    default ``priority`` for that task only — needed by
    multi-task tests that exercise priority-ordered dispatch.
    """

    def _make_task_zip_multi(
        tasks: list[tuple],
        coder: str = "any",
        priority: int = 2,
    ) -> Path:
        if not tasks:
            raise ValueError("make_task_zip_multi requires at least one task")
        zip_path = tmp_path / f"PR-multi-{tasks[0][0]}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for task in tasks:
                if len(task) == 4:
                    pr_id, title_slug, depends_on, task_priority = task
                else:
                    pr_id, title_slug, depends_on = task
                    task_priority = priority
                depends_line = ", ".join(depends_on) if depends_on else "none"
                body = (
                    "---\n"
                    "status: TODO\n"
                    "---\n"
                    "\n"
                    f"# PR-{pr_id}: {title_slug}\n"
                    "\n"
                    f"Branch: pr-{pr_id}-{title_slug}\n"
                    "- Type: feature\n"
                    "- Complexity: low\n"
                    f"- Depends on: {depends_line}\n"
                    f"- Priority: {task_priority}\n"
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
                zf.write(md_path, arcname=md_name)
        return zip_path

    return _make_task_zip_multi


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
    setup_error = None
    try:
        _stop_daemon_and_wait_paused(TESTBED_SLUG)
        reset_testbed_full(TESTBED_SLUG)
        clear_testbed_redis_state(TESTBED_SLUG)
    except Exception as exc:
        setup_error = exc
        raise
    finally:
        try:
            _resume_daemon(TESTBED_SLUG)
        except Exception as exc:
            if setup_error is None:
                raise
            setup_error.add_note(f"resume failed after reset setup error: {exc}")
    yield
    clear_testbed_redis_state(TESTBED_SLUG)


@pytest.fixture
def post_review():
    """Post a PR review on the testbed via ``gh api`` and return the review id.

    Generic enough for PR-258d or future review-driven e2e tests; the
    review is posted under whatever identity the test runs as
    (developer's gh CLI locally, testbed App in CI). ``event`` accepts
    GitHub's review events such as ``REQUEST_CHANGES``, ``APPROVE``, or
    ``COMMENT``.
    """

    def _post_review(
        pr_number: int,
        event: str,
        body: str,
        repo: str = "AlexBomber12/pipeline-orchestrator-testbed",
    ) -> int:
        result = subprocess.run(
            [
                "gh", "api", "-X", "POST",
                f"repos/{repo}/pulls/{pr_number}/reviews",
                "-f", f"event={event}",
                "-f", f"body={body}",
            ],
            capture_output=True, text=True, check=False, timeout=30,
        )
        if result.returncode != 0:
            raise AssertionError(
                f"failed to post {event} review on PR #{pr_number}: "
                f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
            )
        try:
            payload = json.loads(result.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise AssertionError(
                f"gh api returned non-JSON for review on PR #{pr_number}: "
                f"{result.stdout!r}"
            ) from exc
        review_id = payload.get("id")
        if not isinstance(review_id, int):
            raise AssertionError(
                f"gh api response missing integer 'id' for review on PR "
                f"#{pr_number}: {payload!r}"
            )
        return review_id

    return _post_review


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
