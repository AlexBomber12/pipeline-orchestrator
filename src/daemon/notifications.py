"""Operator notification helpers for daemon guardrails."""

from __future__ import annotations

import httpx


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
    async with httpx.AsyncClient(timeout=timeout_seconds) as client:
        response = await client.post(webhook_url, json=payload)
        response.raise_for_status()
