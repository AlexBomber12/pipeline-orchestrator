"""Tests for the guardrail notification helpers in src/daemon/notifications.py."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from src.daemon.notifications import (
    _parse_guardrail_cause_for_notification,
    send_guardrail_notification,
)

_DEFAULT_KWARGS: dict[str, Any] = {
    "webhook_url": "https://example.test/hook",
    "repo_name": "example",
    "pr_id": "PR-7",
    "pr_number": 42,
    "owner_repo": "acme/example",
    "tier": 2,
    "category": "large_diff_threshold",
    "excerpt": "excerpt body",
    "rule": "large_diff_threshold",
}


def _send(**overrides: Any) -> None:
    kwargs = {**_DEFAULT_KWARGS, **overrides}
    asyncio.run(send_guardrail_notification(**kwargs))


class _CapturingClient:
    """Stand-in for httpx.AsyncClient that records posts."""

    posted: list[tuple[str, dict[str, Any], float]] = []

    def __init__(self, timeout: float) -> None:
        self._timeout = timeout

    async def __aenter__(self) -> _CapturingClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def post(self, url: str, json: dict[str, Any]) -> None:
        type(self).posted.append((url, json, self._timeout))


def _make_raising_client(exc: Exception) -> type:
    class _Raising:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> _Raising: return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> None: raise exc
    return _Raising


@pytest.fixture
def capturing_client(monkeypatch: pytest.MonkeyPatch) -> type[_CapturingClient]:
    _CapturingClient.posted = []
    monkeypatch.setattr(
        "src.daemon.notifications.httpx.AsyncClient", _CapturingClient
    )
    return _CapturingClient


def test_send_guardrail_notification_no_webhook_url_returns_silently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed: list[Any] = []

    class _ShouldNotConstruct:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            constructed.append((args, kwargs))

    monkeypatch.setattr(
        "src.daemon.notifications.httpx.AsyncClient", _ShouldNotConstruct
    )
    _send(webhook_url=None)
    assert constructed == []


def test_send_guardrail_notification_payload_shape(
    capturing_client: type[_CapturingClient],
) -> None:
    _send(timeout_seconds=3.0, dashboard_base_url="https://orchestrator.lan")
    assert len(capturing_client.posted) == 1
    url, payload, timeout = capturing_client.posted[0]
    assert url == _DEFAULT_KWARGS["webhook_url"]
    assert timeout == 3.0
    expected_keys = {
        "event", "repo_name", "pr_id", "tier", "category",
        "excerpt", "rule", "github_pr_url", "dashboard_url", "text",
    }
    assert expected_keys.issubset(payload.keys())
    assert payload["event"] == "guardrail_escalation"
    assert payload["github_pr_url"] == "https://github.com/acme/example/pull/42"
    assert payload["dashboard_url"] == "https://orchestrator.lan/repo/example"


def test_send_guardrail_notification_text_summary_starts_with_guardrail_tier(
    capturing_client: type[_CapturingClient],
) -> None:
    _send()
    assert capturing_client.posted[0][1]["text"].startswith("GUARDRAIL Tier 2:")


def test_send_guardrail_notification_text_includes_dashboard_link_when_base_set(
    capturing_client: type[_CapturingClient],
) -> None:
    _send(dashboard_base_url="https://orchestrator.lan/")
    payload = capturing_client.posted[0][1]
    assert "Dashboard: https://orchestrator.lan/repo/example" in payload["text"]


def test_send_guardrail_notification_text_omits_dashboard_when_base_none(
    capturing_client: type[_CapturingClient],
) -> None:
    _send(dashboard_base_url=None)
    payload = capturing_client.posted[0][1]
    assert "Dashboard:" not in payload["text"]
    assert payload["dashboard_url"] is None


def test_send_guardrail_notification_text_omits_github_when_pr_number_none(
    capturing_client: type[_CapturingClient],
) -> None:
    _send(pr_number=None)
    payload = capturing_client.posted[0][1]
    assert "GitHub:" not in payload["text"]
    assert payload["github_pr_url"] is None


def test_send_guardrail_notification_excerpt_truncated_to_500_in_payload(
    capturing_client: type[_CapturingClient],
) -> None:
    _send(excerpt="Q" * 1000)
    payload = capturing_client.posted[0][1]
    assert payload["excerpt"] == "Q" * 500
    assert payload["text"].count("Q") == 200


def test_send_guardrail_notification_propagates_httpx_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.daemon.notifications.httpx.AsyncClient",
        _make_raising_client(httpx.TimeoutException("slow")),
    )
    with pytest.raises(httpx.TimeoutException):
        _send()


def test_send_guardrail_notification_propagates_httpx_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.daemon.notifications.httpx.AsyncClient",
        _make_raising_client(httpx.ConnectError("refused")),
    )
    with pytest.raises(httpx.ConnectError):
        _send()


@pytest.mark.parametrize(
    ("message", "expected_tier", "expected_category"),
    [
        (
            "GUARDRAIL: governance_file_tampering: AGENTS.md modified",
            1,
            "governance_file_tampering",
        ),
        (
            "GUARDRAIL: secret_in_diff: src/example.py:42 (github_pat_classic)",
            1,
            "secret_in_diff",
        ),
        (
            "GUARDRAIL: mass_deletion: 25 files deleted",
            1,
            "mass_deletion",
        ),
        (
            "GUARDRAIL: large_diff_threshold: +1535 LOC across 4 files",
            2,
            "large_diff_threshold",
        ),
        (
            "GUARDRAIL: novel_unmapped_category: some context",
            2,
            "novel_unmapped_category",
        ),
        (
            "GUARDRAIL: tier=2 governance_file_tampering: AGENTS.md modified",
            2,
            "governance_file_tampering",
        ),
    ],
)
def test_parse_guardrail_cause_classification(
    message: str, expected_tier: int, expected_category: str
) -> None:
    parsed = _parse_guardrail_cause_for_notification(message)
    assert parsed is not None
    assert parsed["tier"] == expected_tier
    assert parsed["category"] == expected_category
    assert parsed["rule"] == expected_category


def test_parse_guardrail_cause_extracts_excerpt() -> None:
    parsed = _parse_guardrail_cause_for_notification(
        "GUARDRAIL: large_diff_threshold: +1535 LOC across 4 files"
    )
    assert parsed is not None
    assert parsed["excerpt"] == "+1535 LOC across 4 files"


@pytest.mark.parametrize(
    "message",
    [
        "FIX iteration cap reached",
        "",
        "GUARDRAIL: nocolonhere",
        "GUARDRAIL: : excerpt",
    ],
)
def test_parse_guardrail_cause_returns_none_for_invalid_inputs(
    message: str,
) -> None:
    assert _parse_guardrail_cause_for_notification(message) is None
