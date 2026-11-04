"""Failure evidence is exercised through pytest's real setup/report hooks."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.e2e import conftest as evidence


def _isolated_failure(tmp_path: Path, fixture_body: str) -> tuple[subprocess.CompletedProcess, dict]:
    (tmp_path / "conftest.py").write_text(
        "from tests.e2e import conftest as source\n"
        "pytest_runtest_makereport = source.pytest_runtest_makereport\n"
        "pytest_collectreport = source.pytest_collectreport\n"
        f"source.EVIDENCE_DIR = source.Path({str(tmp_path / 'evidence')!r})\n"
        "source._bounded_testbed_state = lambda: {'state': 'CODING', 'history': []}\n"
        + fixture_body
    )
    (tmp_path / "test_failure.py").write_text(
        "def test_original_failure(broken):\n"
        "    assert False, 'original assertion must survive'\n"
    )
    env = {**os.environ, "PYTHONPATH": str(evidence.REPO_DIR)}
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-c", str(tmp_path / "pytest.ini"), "-q", str(tmp_path / "test_failure.py")],
        cwd=tmp_path, env=env, text=True, capture_output=True, timeout=30, check=False,
    )
    records = list((tmp_path / "evidence").glob("failure-*.json"))
    assert len(records) == 1, result.stdout + result.stderr
    return result, json.loads(records[0].read_text())


def test_setup_failure_records_checkout_and_keeps_original_error(tmp_path):
    result, record = _isolated_failure(
        tmp_path,
        "import pytest\n"
        "@pytest.fixture\n"
        "def broken():\n"
        "    raise RuntimeError('original fixture failure')\n",
    )
    assert result.returncode != 0
    assert "original fixture failure" in result.stdout
    assert record["phase"] == "setup"
    assert record["test_id"].endswith("test_original_failure")
    assert record["tested_sha"] == subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=evidence.REPO_DIR, text=True
    ).strip()
    assert record["testbed"]["state"] == "CODING"
    assert record["browser_errors"] == []
    assert list((tmp_path / "evidence").glob("*.png")) == []


def test_collector_error_does_not_replace_test_failure(tmp_path):
    result, record = _isolated_failure(
        tmp_path,
        "import pytest\n"
        "def unavailable():\n"
        "    raise OSError('synthetic state probe error')\n"
        "source._bounded_testbed_state = unavailable\n"
        "@pytest.fixture\n"
        "def broken():\n"
        "    return None\n",
    )
    assert result.returncode != 0
    assert "original assertion must survive" in result.stdout
    assert record["phase"] == "call"
    assert record["collection_errors"] == ["testbed: OSError"]


def test_collection_failure_records_module_and_bounded_state(tmp_path):
    (tmp_path / "conftest.py").write_text(
        "from tests.e2e import conftest as source\n"
        "pytest_collectreport = source.pytest_collectreport\n"
        f"source.EVIDENCE_DIR = source.Path({str(tmp_path / 'evidence')!r})\n"
        "source._bounded_testbed_state = lambda: {'state': 'CODING', 'history': []}\n"
    )
    (tmp_path / "test_bad.py").write_text("def broken(:\n")
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-c", str(tmp_path / "pytest.ini"), "-q", str(tmp_path / "test_bad.py")],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(evidence.REPO_DIR)},
        text=True, capture_output=True, timeout=30, check=False,
    )
    records = list((tmp_path / "evidence").glob("failure-*.json"))
    assert result.returncode != 0
    assert len(records) == 1, result.stdout + result.stderr
    record = json.loads(records[0].read_text())
    assert record["phase"] == "collect"
    assert record["test_id"].endswith("test_bad.py")
    assert record["testbed"]["state"] == "CODING"


def test_teardown_failure_retains_screenshot_taken_before_page_close(tmp_path):
    (tmp_path / "conftest.py").write_text(
        "import pytest\n"
        "from pathlib import Path\n"
        "from tests.e2e import conftest as source\n"
        "pytest_runtest_makereport = source.pytest_runtest_makereport\n"
        "_watch_browser_errors = source._watch_browser_errors\n"
        f"source.EVIDENCE_DIR = source.Path({str(tmp_path / 'evidence')!r})\n"
        "source._bounded_testbed_state = lambda: {'state': 'CODING', 'history': []}\n"
        "class Page:\n"
        "    url = 'http://localhost:18800/settings'\n"
        "    closed = False\n"
        "    def on(self, *args): pass\n"
        "    def is_closed(self): return self.closed\n"
        "    def locator(self, selector): return selector\n"
        "    def screenshot(self, **kwargs):\n"
        "        assert not self.closed\n"
        "        assert kwargs['mask'] == ['input, textarea, select, [contenteditable]']\n"
        "        Path(kwargs['path']).write_bytes(b'masked screenshot')\n"
        "@pytest.fixture\n"
        "def page(request):\n"
        "    instance = Page()\n"
        "    yield instance\n"
        "    instance.closed = True\n"
        "    if request.node.name == 'test_passes':\n"
        "        raise RuntimeError('original teardown failure')\n"
    )
    (tmp_path / "test_teardown.py").write_text(
        "def test_passes(page):\n    assert not page.is_closed()\n"
        "def test_clean(page):\n    assert not page.is_closed()\n"
    )
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-c", str(tmp_path / "pytest.ini"), "-q", str(tmp_path / "test_teardown.py")],
        cwd=tmp_path, env={**os.environ, "PYTHONPATH": str(evidence.REPO_DIR)},
        text=True, capture_output=True, timeout=30, check=False,
    )
    records = list((tmp_path / "evidence").glob("failure-*.json"))
    assert result.returncode != 0
    assert "original teardown failure" in result.stdout
    assert len(records) == 1, result.stdout + result.stderr
    record = json.loads(records[0].read_text())
    assert record["phase"] == "teardown"
    assert (tmp_path / "evidence" / record["screenshot"]).read_bytes() == b"masked screenshot"
    assert list((tmp_path / "evidence").glob("*.png")) == [tmp_path / "evidence" / record["screenshot"]]


def test_browser_capture_masks_controls_and_omits_error_payloads(tmp_path):
    class Page:
        url = "http://localhost:18800/settings"

        def is_closed(self):
            return False

        def locator(self, selector):
            assert selector == "input, textarea, select, [contenteditable]"
            return "masked controls"

        def screenshot(self, **kwargs):
            assert kwargs["mask"] == ["masked controls"]
            Path(kwargs["path"]).write_bytes(b"synthetic screenshot")

    destination = tmp_path / "failure.json"
    item = SimpleNamespace(funcargs={"page": Page()}, _evidence_browser_errors=["pageerror", "console-error"])
    record = {}
    evidence._browser_evidence(item, record, destination)
    assert record["browser_errors"] == ["pageerror", "console-error"]
    assert (tmp_path / record["screenshot"]).read_bytes() == b"synthetic screenshot"
    item.funcargs["page"].url = "http://localhost:18800/login"
    record = {}
    evidence._browser_evidence(item, record, destination)
    assert record["screenshot"].startswith("skipped")


def test_state_snapshot_excludes_event_text_and_secret_fields(monkeypatch):
    payload = [{
        "name": evidence.TESTBED_SLUG, "state": "FIX",
        "current_task": {"pr_id": "PR-900", "branch": "pr-900", "secret": "never-save"},
        "current_pr": {
            "number": 42, "ci_status": "FAILURE", "review_status": "CHANGES_REQUESTED", "secret": "never-save"
        },
        "history": [{"time": "now", "state": "FIX", "event": "secret transcript", "kind": "transition"}],
        "config": "never-save",
    }]

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _limit):
            return json.dumps(payload).encode()

    monkeypatch.setattr(evidence.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    snapshot = evidence._bounded_testbed_state()
    assert snapshot["state"] == "FIX"
    assert snapshot["current_task"]["pr_id"] == "PR-900"
    assert snapshot["current_pr"] == {
        "number": 42, "ci_status": "FAILURE", "review_status": "CHANGES_REQUESTED"
    }
    assert "secret" not in json.dumps(snapshot)
    assert snapshot["history"] == [{"time": "now", "state": "FIX", "tier": None, "kind": "transition", "count": None}]


def test_browser_listener_only_acquires_requested_page():
    class Page:
        def __init__(self):
            self.handlers = {}

        def on(self, kind, handler):
            self.handlers[kind] = handler

    page = Page()
    requested = []
    node = SimpleNamespace(nodeid="test_browser_listener")

    def get_page(name):
        requested.append(name)
        return page

    request = SimpleNamespace(fixturenames=[], getfixturevalue=get_page, node=node)
    watcher = evidence._watch_browser_errors.__wrapped__(request)
    next(watcher)
    assert requested == []
    next(watcher, None)

    request.fixturenames = ["page"]
    watcher = evidence._watch_browser_errors.__wrapped__(request)
    next(watcher)
    page.handlers["pageerror"]("sensitive exception text")
    page.handlers["console"](SimpleNamespace(type="error", text="sensitive console text"))
    assert requested == ["page"]
    assert node._evidence_browser_errors == ["pageerror", "console-error"]
    next(watcher, None)


def test_upload_rejection_retains_http_reason_in_failure_evidence(tmp_path, monkeypatch, capsys):
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    class UploadHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(400)
            self.end_headers()
            self.wfile.write(
                b'<div data-upload-feedback-card><p>Task validation failed</p>'
                b'<li>PR-900: missing dependency &amp; branch evidence</li>'
                b'<input value="do-not-retain-form-value"></div>'
            )

        def log_message(self, *_args):
            pass

    server = HTTPServer(("127.0.0.1", 0), UploadHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(evidence, "TEST_DASHBOARD_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(evidence, "EVIDENCE_DIR", tmp_path / "evidence")
    monkeypatch.setattr(evidence, "_bounded_testbed_state", lambda: {"state": "IDLE"})
    node = SimpleNamespace(nodeid="test_upload", funcargs={})
    upload = evidence.upload_zip.__wrapped__(SimpleNamespace(node=node))
    archive = tmp_path / "PR-900.zip"
    archive.write_bytes(b"controlled request")
    try:
        assert upload(archive) == 400
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    evidence._write_failure_evidence(node, "call")
    record = json.loads(next((tmp_path / "evidence").glob("failure-*.json")).read_text())
    error = record["upload_error"]
    assert error == {
        "status_code": 400, "message": "Task validation failed PR-900: missing dependency & branch evidence",
    }
    assert "HTTP 400" in capsys.readouterr().out
    assert "do-not-retain" not in json.dumps(record)


@pytest.mark.parametrize("outcome", ["success", "exhausted", "unrelated", "unmarked", "invalid", "transport"])
def test_upload_retries_only_confirmed_snapshot_rejection(tmp_path, monkeypatch, outcome):
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from threading import Thread

    reason = "Repository task snapshot is changing; retry the upload after checkout settles."
    rejected = (503, f'<div data-upload-feedback-card><p>{reason}</p></div>')
    responses = {
        "success": [rejected, rejected, (201, "accepted")],
        "exhausted": [rejected] * 5,
        "unrelated": [(503, '<div data-upload-feedback-card><p>Redis unavailable</p></div>')],
        "unmarked": [(503, f"<p>{reason}</p>")],
        "invalid": [(400, rejected[1])],
        "transport": [],
    }[outcome]
    received, sleeps = [], []
    archive_bytes = b"complete controlled ZIP payload"

    class UploadHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            status, body = responses[len(received) - 1]
            self.send_response(status)
            self.end_headers()
            self.wfile.write(body.encode())

        def log_message(self, *_args):
            pass

    server = HTTPServer(("127.0.0.1", 0), UploadHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(evidence, "TEST_DASHBOARD_URL", f"http://127.0.0.1:{server.server_port}")
    monkeypatch.setattr(evidence.time, "sleep", sleeps.append)
    node = SimpleNamespace()
    upload = evidence.upload_zip.__wrapped__(SimpleNamespace(node=node))
    archive = tmp_path / "PR-900.zip"
    archive.write_bytes(archive_bytes)
    try:
        if outcome == "transport":
            def unavailable(*args, **kwargs):
                raise evidence.requests.ConnectionError("outcome unknown")
            monkeypatch.setattr(evidence.requests, "post", unavailable)
            with pytest.raises(evidence.requests.ConnectionError, match="outcome unknown"):
                upload(archive)
        else:
            assert upload(archive) == responses[-1][0]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    assert len(received) == len(responses)
    assert all(body.count(archive_bytes) == 1 for body in received)
    assert sleeps == [1] * max(0, len(responses) - 1)
    if outcome == "success":
        assert not hasattr(node, "_evidence_upload_error")
    elif responses:
        assert node._evidence_upload_error["status_code"] == responses[-1][0]


@pytest.mark.parametrize("stderr,expected", [
    ("gh: HTTP 500", True), ("gh: Bad Gateway (HTTP 502)", True),
    ("remote: Internal Server Error        ", True), ("gh: HTTP 503", True), ("gh: HTTP 504", True),
    ("gh: HTTP 403", False), ("gh: HTTP 429", False), ("Task PR-500", False),
    ('echo "gh: HTTP 500"', False), ('assert stderr == "gh: HTTP 500"', False),
    ("HTTP 500", False), ("not an http response 503", False),
])
def test_external_failure_uses_native_diagnostic_context(stderr, expected):
    from tests.e2e.lib.ci_outcome import ExternalDependencyError, classify_failure, require_external_response
    result = subprocess.CompletedProcess(["gh", "api", "repos/octo/demo"], 1, "", stderr)
    if expected:
        with pytest.raises(ExternalDependencyError) as raised:
            require_external_response(result, "github_api")
        assert classify_failure(raised.value)["kind"] == "infra"
    else:
        require_external_response(result, "github_api")
    assert classify_failure(AssertionError(stderr)) == {"kind": "failure"}


@pytest.mark.parametrize("reason", ["github", "healthy", "auth", "unavailable"])
def test_lifecycle_timeout_probes_dependency_without_reclassifying_assertions(monkeypatch, reason):
    from tests.e2e.lib import ci_outcome
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    def probe(command, **kwargs):
        assert command[:2] == ["gh", "api"]
        if reason == "unavailable":
            raise OSError("unknown probe failure")
        return subprocess.CompletedProcess(command, 0 if reason == "healthy" else 1, "",
                                           "gh: HTTP 500" if reason == "github" else "gh: HTTP 403")
    monkeypatch.setattr(ci_outcome.subprocess, "run", probe)
    assert ci_outcome.classify_failure(ci_outcome.LifecycleTimeout("wait expired"))["kind"] == (
        "infra" if reason == "github" else "failure")
    assert ci_outcome.classify_failure(AssertionError("predicate did not succeed; gh: HTTP 500")) == {"kind": "failure"}


def test_complete_ci_report_keeps_mixed_and_missing_evidence_out_of_infra(tmp_path, monkeypatch, capsys):
    import xml.etree.ElementTree as ET
    from tests.e2e.lib import ci_outcome
    from src.github import checks
    from tests.test_ci_status_infra import _structured_check

    identity = {"repo": "octo/demo", "sha": "a" * 40, "run_id": 123, "attempt": 1}
    monkeypatch.setattr(ci_outcome, "source_identity", lambda: identity)
    nodeids = [f"tests/e2e/test_case{n}.py::test_external" for n in range(1, 4)]
    for batch, nodeid in enumerate(nodeids, 1):
        (tmp_path / f"batch-{batch}-collection.json").write_text(json.dumps({
            "batch": batch, "batch_count": 3, "all_nodeids": nodeids, "selected_nodeids": [nodeid]}))
        report = {"identity": identity, "batch": batch, "completed": [nodeid], "exitstatus": 1,
                  "collection_failed": False, "skipped": False,
                  "failures": [{"test_id": nodeid, "phase": "setup", "classification": {"kind": "infra"}}]}
        (tmp_path / f"batch-{batch}-outcome.json").write_text(json.dumps(report))
        suite = ET.Element("testsuite")
        ET.SubElement(ET.SubElement(suite, "testcase"), "error").text = "original failure retained"
        ET.ElementTree(suite).write(tmp_path / f"batch-{batch}-results.xml")
    verdict = ci_outcome.emit_job_verdict(tmp_path)
    assert verdict["failed"] == verdict["infra"] == 3
    check, _ = _structured_check(monkeypatch)
    check["annotations"][0]["message"] = json.dumps(verdict)
    assert checks._is_infra_failure(check) is True
    assert "::error title=pipeline-ci-outcome:v1::" in capsys.readouterr().out
    path = tmp_path / "batch-2-outcome.json"
    report = json.loads(path.read_text())
    report["failures"][0]["classification"] = {"kind": "failure"}
    path.write_text(json.dumps(report))
    verdict = ci_outcome.emit_job_verdict(tmp_path)
    check["annotations"][0]["message"] = json.dumps(verdict)
    assert checks._is_infra_failure(check) is False
    report["completed"] = []
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError, match="incomplete"):
        ci_outcome.emit_job_verdict(tmp_path)


@pytest.mark.parametrize("failure", ["external", "assertion", "quoted"])
def test_real_pytest_report_preserves_failure_type_and_original_exit(tmp_path, failure):
    # Run the actual pytest hooks, not a prearranged JSON verdict.
    from tests.e2e.lib.ci_outcome import TITLE
    (tmp_path / "event.json").write_text(json.dumps({"pull_request": {"head": {"sha": "a" * 40}}}))
    (tmp_path / "conftest.py").write_text(
        "from tests.e2e import conftest as source\n"
        "pytest_sessionstart = source.pytest_sessionstart\n"
        "pytest_sessionfinish = source.pytest_sessionfinish\n"
        "pytest_runtest_makereport = source.pytest_runtest_makereport\n"
        "def pytest_addoption(parser): parser.addoption('--e2e-batch', type=int)\n"
        f"source.EVIDENCE_DIR = source.Path({str(tmp_path / 'evidence')!r})\n"
        "source._bounded_testbed_state = lambda: {}\n"
    )
    body = {
        "external": "raise ExternalDependencyError('github_api', 'github_http_5xx')",
        "assertion": "assert 1 == 2",
        "quoted": "assert False, 'echo gh: HTTP 500'",
    }[failure]
    (tmp_path / "test_outcome.py").write_text(
        "from tests.e2e.lib.ci_outcome import ExternalDependencyError\n"
        f"def test_outcome():\n    {body}\n")
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    env = {**os.environ, "PYTHONPATH": str(evidence.REPO_DIR), "GITHUB_ACTIONS": "true",
           "GITHUB_EVENT_PATH": str(tmp_path / "event.json"), "GITHUB_REPOSITORY": "octo/demo",
           "GITHUB_SHA": "b" * 40, "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1"}
    result = subprocess.run([sys.executable, "-m", "pytest", "-c", str(tmp_path / "pytest.ini"),
                             "--e2e-batch=1", "-q", str(tmp_path / "test_outcome.py")],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 1, result.stdout + result.stderr
    report = json.loads((tmp_path / "evidence/batch-1-outcome.json").read_text())
    assert report["identity"]["sha"] == "a" * 40  # source PR, not synthetic checkout
    assert report["exitstatus"] == 1 and len(report["completed"]) == 1
    assert len(report["failures"]) == 1
    assert report["failures"][0]["classification"]["kind"] == ("infra" if failure == "external" else "failure")
    assert TITLE not in result.stdout  # Only whole-job aggregation can emit a verdict.


@pytest.mark.parametrize("command,expected", [
    (["gh", "api", "repos/octo/demo"], True), (["gh", "pr", "view", "1"], True),
    (["git", "-C", "/tmp/test", "push", "origin", "main"], True), (["git", "clone", "url"], True),
    (["git", "show", "origin", "push"], False), (["bash", "-c", "gh api repos/octo/demo"], False),
    ("gh api repos/octo/demo", False), (["gh", "auth", "login"], False), (["pytest"], False),
])
def test_native_command_timeout_requires_an_actual_network_operation(command, expected):
    from tests.e2e.lib.ci_outcome import classify_failure
    assert (classify_failure(subprocess.TimeoutExpired(command, 30))["kind"] == "infra") == expected


def test_native_gh_failure_and_retry_wrapper_reach_report_without_parsing_prose(monkeypatch):
    from unittest.mock import MagicMock
    from src.github.gh_runner import GhCommandError, run_gh
    from src.retry import TransientRetryExhausted, retry_transient
    from tests.e2e.lib.ci_outcome import classify_failure
    child = MagicMock(returncode=1)
    child.communicate.return_value = ("", "gh: Internal Server Error (HTTP 500)")
    child.__enter__.return_value = child
    monkeypatch.setattr("src.github.gh_runner.subprocess.Popen", lambda *a, **kw: child)
    with pytest.raises(TransientRetryExhausted) as failed:
        retry_transient(lambda: run_gh(["api", "repos/octo/demo"]), attempts=1, operation_name="gh api")
    native = failed.value.last_error
    assert isinstance(native, GhCommandError) and native.returncode == 1
    assert native.cmd == ["gh", "api", "repos/octo/demo"]
    assert classify_failure(failed.value) == {
        "kind": "infra", "operation": "github_subprocess", "reason": "github_http_500"}
    # An assertion wrapping the same cause is still a real failure.
    assertion = AssertionError("application failed")
    assertion.__cause__ = native
    assert classify_failure(assertion) == {"kind": "failure"}


@pytest.mark.parametrize("url,status,expected", [
    ("https://api.github.com/repos/octo/demo", 500, True),
    ("https://api.github.com/repos/octo/demo", 503, True),
    ("http://localhost:18800/", 500, False),
    ("https://api.github.com/", 403, False), ("https://api.github.com/", 429, False),
])
def test_native_http_response_requires_github_host_and_transient_status(url, status, expected):
    import urllib.error
    from tests.e2e.lib.ci_outcome import classify_failure
    error = urllib.error.HTTPError(url, status, "native response", None, None)
    assert (classify_failure(error)["kind"] == "infra") == expected
