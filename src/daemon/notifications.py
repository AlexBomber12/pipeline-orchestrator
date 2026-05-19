"""Operator notification helpers for daemon guardrails."""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from src.audit.webhook_log import write_webhook_audit

_GUARDRAIL_PREFIXES = ("GUARDRAIL:", "[GUARDRAIL]")
_TIER_TOKEN_RE = re.compile(r"^tier=(\d+)\s+")

# PR-307a spec names overlap partly with src/daemon/guardrails.py emitted
# category strings; a follow-up PR can replace string parsing with
# structured cause-payload fields and reconcile the taxonomy.
_TIER1_CATEGORIES: frozenset[str] = frozenset({
    "secret_in_diff", "mass_deletion", "governance_file_tampering",
    "repo_create", "repo_delete", "branch_delete_main",
    "branch_protection_modification", "dangerous_action_external_install",
    "permissions_escalation", "workflow_destruction",
})


async def _post_json_with_audit(
    *,
    event_type: str,
    webhook_url: str,
    payload: dict[str, Any],
    timeout_seconds: float,
    attempt_number: int = 1,
) -> httpx.Response:
    """POST JSON to a webhook and audit the delivery attempt."""
    payload_size_bytes = len(json.dumps(payload).encode("utf-8"))
    status: int | None = None
    response_excerpt = ""
    retry_scheduled_at: datetime | None = None
    start = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
            response = await client.post(webhook_url, json=payload)
        status = getattr(response, "status_code", None)
        response_excerpt = str(getattr(response, "text", ""))[:200]
        if status is not None and status >= 500:
            retry_scheduled_at = datetime.now(timezone.utc)
        response.raise_for_status()
        return response
    except httpx.RequestError as exc:
        response_excerpt = f"request_error: {type(exc).__name__}: {str(exc)[:100]}"
        raise
    finally:
        elapsed_ms = (time.monotonic() - start) * 1000
        write_webhook_audit(
            event_type=event_type,
            webhook_url=webhook_url,
            payload_size_bytes=payload_size_bytes,
            attempt_number=attempt_number,
            http_status=status,
            response_excerpt=response_excerpt,
            elapsed_ms=elapsed_ms,
            retry_scheduled_at=retry_scheduled_at,
        )


async def send_spend_ceiling_warning(
    *,
    webhook_url: str,
    coder_name: str,
    limit_kind: str,
    current_percent: int,
    cap_percent: int,
    warning_percent: int,
    timeout_seconds: float,
) -> None:
    """POST a spend-ceiling warning to the operator webhook."""
    payload = {
        "event": "spend_ceiling_warning",
        "coder_name": coder_name,
        "limit_kind": limit_kind,
        "current_percent": current_percent,
        "cap_percent": cap_percent,
        "text": (
            f"SPEND CEILING WARNING: {coder_name} {limit_kind} usage at "
            f"{current_percent}% (cap: {cap_percent}%, warn at "
            f"{warning_percent}% of cap)."
        ),
    }
    await _post_json_with_audit(
        event_type="spend_ceiling_warning",
        webhook_url=webhook_url,
        payload=payload,
        timeout_seconds=timeout_seconds,
    )


async def send_guardrail_notification(
    *,
    webhook_url: str | None,
    repo_name: str,
    pr_id: str,
    pr_number: int | None,
    owner_repo: str,
    tier: int,
    category: str,
    excerpt: str,
    rule: str,
    timeout_seconds: float = 5.0,
    dashboard_base_url: str | None = None,
) -> None:
    """Best-effort POST of guardrail escalation to operator webhook.

    Failures propagate to the caller for logging — daemon must not
    crash on notification delivery problems.
    """
    if webhook_url is None:
        return
    dashboard_link = (
        f"{dashboard_base_url.rstrip('/')}/repo/{repo_name}"
        if dashboard_base_url
        else None
    )
    github_link = (
        f"https://github.com/{owner_repo}/pull/{pr_number}"
        if pr_number is not None
        else None
    )
    text_summary = (
        f"GUARDRAIL Tier {tier}: {category} on {repo_name} {pr_id}. "
        f"{excerpt[:200]}"
    )
    if dashboard_link:
        text_summary += f"\nDashboard: {dashboard_link}"
    if github_link:
        text_summary += f"\nGitHub: {github_link}"
    payload: dict[str, Any] = {
        "event": "guardrail_escalation",
        "repo_name": repo_name,
        "pr_id": pr_id,
        "tier": tier,
        "category": category,
        "excerpt": excerpt[:500],
        "rule": rule,
        "github_pr_url": github_link,
        "dashboard_url": dashboard_link,
        "text": text_summary,
    }
    await _post_json_with_audit(
        event_type="guardrail_violation",
        webhook_url=webhook_url,
        payload=payload,
        timeout_seconds=timeout_seconds,
    )


def _parse_guardrail_cause_for_notification(
    cause_message: str,
) -> dict[str, Any] | None:
    """Parse a GUARDRAIL cause string into tier/category/rule/excerpt.

    Returns None when the message does not look like a guardrail cause.
    Inferred tier comes from a hardcoded category-to-tier map; unknown
    categories default to Tier 2. Optional ``tier=N`` token in the
    message overrides the map.
    """
    body: str | None = None
    for prefix in _GUARDRAIL_PREFIXES:
        if cause_message.startswith(prefix):
            body = cause_message[len(prefix) :].lstrip()
            break
    if body is None:
        return None
    tier_from_token: int | None = None
    tier_match = _TIER_TOKEN_RE.match(body)
    if tier_match:
        tier_from_token = int(tier_match.group(1))
        body = body[tier_match.end() :]
    colon_idx = body.find(":")
    if colon_idx == -1:
        return None
    category = body[:colon_idx].strip()
    excerpt = body[colon_idx + 1 :].strip()
    if not category:
        return None
    if tier_from_token is not None:
        tier = tier_from_token
    else:
        tier = 1 if category in _TIER1_CATEGORIES else 2
    return {
        "tier": tier,
        "category": category,
        "excerpt": excerpt,
        "rule": category,
    }
