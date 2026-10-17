"""Failure evidence is exercised through pytest's real setup/report hooks."""

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from tests.e2e import conftest as evidence


def _isolated_failure(tmp_path: Path, fixture_body: str) -> tuple[subprocess.CompletedProcess, dict]:
    (tmp_path / "conftest.py").write_text(
        "from tests.e2e import conftest as source\n"
        "pytest_runtest_makereport = source.pytest_runtest_makereport\n"
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
    node = SimpleNamespace()

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
