import hashlib
import json
import os
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import pytest
import requests
import yaml
from src.config import AppConfig, RepoConfig, normalize_repo_url

from tests.e2e.lib.review_api import ReviewApi
from tests.e2e.lib.testbed_reset import clear_testbed_redis_state, reset_testbed_full

TEST_DASHBOARD_URL = "http://localhost:18800"
TESTBED_SLUG = "AlexBomber12__pipeline-orchestrator-testbed"
REPO_DIR = Path(__file__).resolve().parents[2]
TEST_DATA_DIR = REPO_DIR / "tests/e2e/data"
EVIDENCE_DIR = REPO_DIR / "tests/e2e/evidence"
SHARED_CONFIG_PATH = TEST_DATA_DIR / "config.test.yml"

collect_ignore = ["data"]


def _tested_sha() -> str:
    """The checkout under test, even for a failing setup fixture."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO_DIR, capture_output=True,
        text=True, check=True, timeout=5,
    )
    return result.stdout.strip()


def _bounded_testbed_state() -> dict:
    """Allowlist state metadata; event text and config can contain credentials."""
    with urllib.request.urlopen(f"{TEST_DASHBOARD_URL}/api/states", timeout=3) as response:
        payload = json.loads(response.read(256_000).decode("utf-8"))
    entries = payload if isinstance(payload, list) else payload.get("states", [])
    entry = next((row for row in entries if row.get("name") == TESTBED_SLUG or row.get("slug") == TESTBED_SLUG), None)
    if entry is None:
        return {"availability": "testbed state absent"}
    history = entry.get("history") or []
    return {
        "state": entry.get("state"),
        "current_task": {key: (entry.get("current_task") or {}).get(key) for key in ("pr_id", "branch")},
        "current_pr": {
            key: (entry.get("current_pr") or {}).get(key)
            for key in ("number", "ci_status", "review_status")
        },
        "history": [
            {key: row.get(key) for key in ("time", "state", "tier", "kind", "count")}
            for row in history[-20:] if isinstance(row, dict)
        ],
    }


def _browser_evidence(item, record: dict, destination: Path) -> None:
    page = getattr(item, "funcargs", {}).get("page")
    if page is None:
        return
    record["browser_errors"] = getattr(item, "_evidence_browser_errors", [])[-20:]
    if page.is_closed() or any(part in page.url.lower() for part in ("login", "oauth", "callback", "authorize")):
        record["screenshot"] = "skipped: page closed or authentication page"
        return
    screenshot = destination.with_suffix(".png")
    # Settings and other forms may contain credentials; mask every editable control.
    page.screenshot(
        path=str(screenshot), full_page=True,
        mask=[page.locator("input, textarea, select, [contenteditable]")],
        timeout=3000,
    )
    record["screenshot"] = screenshot.name


def _write_failure_evidence(item, phase: str) -> None:
    # Each stage is independent: collection must never replace pytest's failure.
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(item.nodeid.encode()).hexdigest()[:16]
    destination = EVIDENCE_DIR / f"failure-{key}-{phase}.json"
    record = {"test_id": item.nodeid[:300], "phase": phase, "tested_sha": None,
              "testbed": None, "browser_errors": [], "stack_logs": "stack-logs.txt when available",
              "collection_errors": []}
    for label, collect in (("tested_sha", _tested_sha), ("testbed", _bounded_testbed_state)):
        try:
            record[label] = collect()
        except Exception as exc:
            record["collection_errors"].append(f"{label}: {type(exc).__name__}")
    if phase == "teardown" and hasattr(item, "_evidence_preteardown"):
        candidate = item._evidence_preteardown
        record["browser_errors"] = candidate.get("browser_errors", [])
        if "screenshot" in candidate:
            record["screenshot"] = candidate["screenshot"]
        record["collection_errors"].extend(candidate.get("collection_errors", []))
    else:
        try:
            _browser_evidence(item, record, destination)
        except Exception as exc:
            record["collection_errors"].append(f"browser: {type(exc).__name__}")
    destination.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")


@pytest.fixture(autouse=True)
def _watch_browser_errors(request):
    if "page" not in request.fixturenames:
        yield
        return
    page = request.getfixturevalue("page")
    errors = []
    request.node._evidence_browser_errors = errors

    def remember(kind):
        if len(errors) < 20:
            errors.append(kind)

    # Record event type only. Console text and exception messages may contain secrets.
    page.on("pageerror", lambda _error: remember("pageerror"))
    page.on("console", lambda message: remember("console-error") if message.type == "error" else None)
    yield
    # The page fixture closes after this dependent fixture finalizes. Keep a
    # masked candidate until pytest reports whether teardown actually failed.
    key = hashlib.sha256(request.node.nodeid.encode()).hexdigest()[:16]
    destination = EVIDENCE_DIR / f"preteardown-{key}.json"
    candidate = {"browser_errors": errors[-20:]}
    try:
        EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
        _browser_evidence(request.node, candidate, destination)
    except Exception as exc:
        candidate["collection_errors"] = [f"browser: {type(exc).__name__}"]
    request.node._evidence_preteardown = candidate


def pytest_collectreport(report):
    if report.failed:
        try:
            _write_failure_evidence(report, "collect")
        except Exception as exc:
            print(f"e2e evidence unavailable for {report.nodeid}: {type(exc).__name__}: {exc}")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed:
        try:
            _write_failure_evidence(item, report.when)
        except Exception as exc:
            # Even a filesystem failure leaves the original report intact.
            print(f"e2e evidence unavailable for {item.nodeid}: {type(exc).__name__}: {exc}")
    if report.when == "teardown" and not report.failed:
        candidate = getattr(item, "_evidence_preteardown", {})
        screenshot = candidate.get("screenshot", "")
        if screenshot.endswith(".png"):
            (EVIDENCE_DIR / screenshot).unlink(missing_ok=True)


def _shared_config_bytes() -> bytes:
    """Read through the container that owns Settings' mode-0600 replacements."""
    result = subprocess.run(
        [
            "docker", "compose", "-f", "docker-compose.test.yml", "exec", "-T",
            "web-test", "cat", "/data/config.test.yml",
        ],
        cwd=REPO_DIR,
        capture_output=True,
        check=True,
        timeout=15,
    )
    return result.stdout


def _config_digest() -> str:
    return hashlib.sha256(_shared_config_bytes()).hexdigest()


def _daemon_log_count(message: str) -> int:
    result = subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", "logs", "--no-color", "daemon-test"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        check=True,
        timeout=15,
    )
    return result.stdout.count(message)


def _daemon_reload_count() -> int:
    return _daemon_log_count("Config change detected; reconciling runners")


def _runner_apply_count() -> int:
    return _daemon_log_count(f"[{TESTBED_SLUG}] [INFRA] Applied staged repo config.")


def _runner_reload_count() -> int:
    return _daemon_log_count(f"[{TESTBED_SLUG}] [INFRA] Reloaded repo config from config.yml.")


def _wait_for_runner_config_ack(applies_before: int, reloads_before: int) -> None:
    deadline = time.monotonic() + 70
    while time.monotonic() < deadline:
        if _runner_apply_count() > applies_before or _runner_reload_count() > reloads_before:
            return
        time.sleep(0.5)
    raise AssertionError("runner did not apply updated integration config")


def _wait_for_daemon_reload(reloads_before: int) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if _daemon_reload_count() > reloads_before:
            return
        time.sleep(0.5)
    raise AssertionError("daemon did not observe updated integration config")


@pytest.fixture
def shared_test_config(reset_testbed, get_state):
    """Restore the shared file only if this test still owns its last write."""
    baseline = _shared_config_bytes()
    baseline_digest = hashlib.sha256(baseline).hexdigest()
    expected_digest = baseline_digest
    pending_digest = None

    def expect_settings_coder_write(url: str, coder: str) -> None:
        nonlocal pending_digest
        config = AppConfig.model_validate(yaml.safe_load(baseline))
        matches = [
            index for index, repo in enumerate(config.repositories)
            if normalize_repo_url(repo.url) == normalize_repo_url(url)
        ]
        assert len(matches) == 1, "expected exactly one repository for the Settings write"
        index = matches[0]
        existing = config.repositories[index]
        config.repositories[index] = RepoConfig.model_validate({**existing.model_dump(), "coder": coder})
        payload = config.model_dump(mode="json", exclude_none=True)
        intended = yaml.dump(payload, default_flow_style=False, sort_keys=False).encode("utf-8")
        pending_digest = hashlib.sha256(intended).hexdigest()

    def note_settings_write() -> None:
        nonlocal expected_digest
        observed = _config_digest()
        assert observed == pending_digest, "Settings did not write the intended shared config"
        expected_digest = observed

    def wait_for_coder_dispatch(pr_id: str, plugin_id: str) -> dict:
        deadline = time.monotonic() + 70
        last_entry = None
        while time.monotonic() < deadline:
            last_entry = get_state()
            task = (last_entry or {}).get("current_task") or {}
            invocation = (last_entry or {}).get("active_invocation") or {}
            if (
                (last_entry or {}).get("state") == "CODING"
                and task.get("pr_id") == pr_id
                and invocation.get("plugin_id") == plugin_id
            ):
                return last_entry
            time.sleep(0.5)
        raise AssertionError(
            f"daemon did not dispatch {pr_id} with {plugin_id}; last state={last_entry!r}"
        )

    try:
        yield _shared_config_bytes, expect_settings_coder_write, note_settings_write, wait_for_coder_dispatch
    finally:
        restored = False
        applies_before = 0
        try:
            _stop_daemon_and_wait_paused(TESTBED_SLUG)
            reset_testbed_full(TESTBED_SLUG)
            clear_testbed_redis_state(TESTBED_SLUG)
            observed = _config_digest()
            assert observed == expected_digest or (expected_digest == baseline_digest and observed == pending_digest), (
                "shared config changed outside this fixture; refusing to overwrite concurrent changes"
            )
            if baseline != _shared_config_bytes():
                applies_before = _runner_apply_count()
                reloads_before = _daemon_reload_count()
                fd, temporary = tempfile.mkstemp(prefix="config.test.yml.", dir=TEST_DATA_DIR)
                try:
                    with os.fdopen(fd, "wb") as output:
                        output.write(baseline)
                    os.chmod(temporary, 0o644)
                    os.replace(temporary, SHARED_CONFIG_PATH)
                finally:
                    if os.path.exists(temporary):
                        os.unlink(temporary)
                _wait_for_daemon_reload(reloads_before)
                assert _shared_config_bytes() == baseline
                restored = True
        finally:
            _resume_daemon(TESTBED_SLUG)
        if restored:
            _wait_for_runner_apply(applies_before)


def _wait_for_runner_apply(applies_before: int) -> None:
    deadline = time.monotonic() + 70
    while time.monotonic() < deadline:
        if _runner_apply_count() > applies_before:
            return
        time.sleep(0.5)
    raise AssertionError("runner did not apply restored integration config after resume")


# 30 seconds proved insufficient on the shared testbed where dispatch was observed at ~39 seconds.
def _stop_daemon_and_wait_paused(slug: str, timeout_sec: int = 150) -> None:
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
    # The daemon may have recovered an unfinished merge from the previous CI
    # run before pytest starts. Pause it before deleting that task on main,
    # then restart so its in-memory state cannot resurrect the old merge.
    _stop_daemon_and_wait_paused(TESTBED_SLUG)
    counts = reset_testbed_full(TESTBED_SLUG)
    counts["redis_keys_deleted"] = clear_testbed_redis_state(TESTBED_SLUG)
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", "restart", "daemon-test"],
        cwd=REPO_DIR, check=True, timeout=60,
    )
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
            "task_budget:\n"
            "  version: 1\n"
            "  production_lines: 0\n"
            "  test_lines: 1\n"
            "  other_lines: 0\n"
            "  production_files: 0\n"
            "  total_files: 1\n"
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
                    "task_budget:\n"
                    "  version: 1\n"
                    "  production_lines: 0\n"
                    "  test_lines: 1\n"
                    "  other_lines: 0\n"
                    "  production_files: 0\n"
                    "  total_files: 1\n"
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
    """Fail before test setup unless the independent reviewer is configured."""
    return ReviewApi.from_environment().post_changes_requested


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
