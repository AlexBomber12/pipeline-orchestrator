from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts import prepare_test_reviewer_config as reviewer_config

_BASE_CONFIG = """\
daemon:
  trusted_reviewer_identities:
    - user_id: 199175422
      login: chatgpt-codex-connector[bot]
"""


@pytest.mark.parametrize(
    ("app_slug", "user_id"),
    [
        ("first-reviewer", 101),
        ("replacement-reviewer", 202),
    ],
)
def test_resolves_selected_app_and_preserves_default_reviewer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    app_slug: str,
    user_id: int,
) -> None:
    expected_login = f"{app_slug}[bot]"
    calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(
            args,
            0,
            stdout=json.dumps(
                {"id": user_id, "login": expected_login, "type": "Bot"}
            ),
            stderr="",
        )

    monkeypatch.setattr(reviewer_config.subprocess, "run", fake_run)
    config_path = tmp_path / "config.test.yml"
    config_path.write_text(_BASE_CONFIG, encoding="utf-8")

    identity = reviewer_config.resolve_reviewer_identity(app_slug)
    reviewer_config.add_reviewer_to_test_config(config_path, identity)

    assert calls == [["gh", "api", f"users/{app_slug}%5Bbot%5D"]]
    identities = yaml.safe_load(config_path.read_text(encoding="utf-8"))["daemon"][
        "trusted_reviewer_identities"
    ]
    assert identities == [
        {
            "user_id": 199175422,
            "login": "chatgpt-codex-connector[bot]",
        },
        {"user_id": user_id, "login": expected_login},
    ]


@pytest.mark.parametrize(
    ("returncode", "stdout", "message"),
    [
        (1, "", "lookup failed"),
        (0, "not-json", "malformed JSON"),
        (0, "[]", "non-object"),
        (
            0,
            '{"id":"123","login":"reviewer[bot]","type":"Bot"}',
            "invalid numeric user ID",
        ),
        (
            0,
            '{"id":123,"login":"other[bot]","type":"Bot"}',
            "does not match",
        ),
        (
            0,
            '{"id":123,"login":"reviewer[bot]","type":"User"}',
            "bot account",
        ),
    ],
)
def test_rejects_invalid_bot_user_lookup_results(
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stdout: str,
    message: str,
) -> None:
    monkeypatch.setattr(
        reviewer_config.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(
            args,
            returncode,
            stdout=stdout,
            stderr="sensitive error omitted",
        ),
    )

    with pytest.raises(reviewer_config.ReviewerConfigError, match=message):
        reviewer_config.resolve_reviewer_identity("reviewer")


def test_workflow_prepares_config_before_build_and_verifies_both_services() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert workflow.index("Prepare integration reviewer config") < workflow.index(
        "Build test stack image"
    )
    assert "steps.testbed-reviewer-token.outputs.app-slug" in workflow
    assert "for service in web-test daemon-test" in workflow
    configured = yaml.safe_load(
        Path("config.test.yml").read_text(encoding="utf-8")
    )["daemon"]["trusted_reviewer_identities"]
    assert configured == [
        {
            "user_id": 199175422,
            "login": "chatgpt-codex-connector[bot]",
        }
    ]
