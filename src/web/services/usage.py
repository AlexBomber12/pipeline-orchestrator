"""Read daemon-owned quota observations for settings, without provider calls."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

from src.daemon.github_rate_limit import read_graphql_budget, read_rest_budget
from src.keyspace import coder_usage
from src.usage import UsageObservation


async def usage_settings_context(redis_client: Any, coder_names: list[str], cache_ttl: int) -> dict[str, Any]:
    now = time.time()

    async def read_coder(name: str) -> tuple[str, dict[str, Any]]:
        view: dict[str, Any] = {"supported": None, "status": "unavailable", "windows": [], "updated_at": None}
        try:
            raw = await redis_client.get(coder_usage(name)) if redis_client is not None else None
            if raw is None:
                return name, view
            if len(raw) > 16384:
                raise ValueError("Oversized usage observation")
            observation = UsageObservation.model_validate_json(raw)
        except Exception:
            return name, view
        view["supported"] = observation.status != "unsupported"
        view["status"] = observation.status
        if observation.last_successful_at is not None:
            view["updated_at"] = datetime.fromtimestamp(
                observation.last_successful_at, tz=timezone.utc,
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
        snapshot = observation.snapshot
        if snapshot is not None:
            if now - snapshot.fetched_at > max(120, cache_ttl * 3) or snapshot.fetched_at > now + 60:
                view["status"] = "stale"
            for label, percent, reset in (
                ("Session", snapshot.session_percent, snapshot.session_resets_at),
                ("Weekly", snapshot.weekly_percent, snapshot.weekly_resets_at),
            ):
                if percent is not None and reset is not None:
                    view["windows"].append({
                        "label": label, "percent": percent if reset > now else None,
                        "reset_at": reset if reset > now else None,
                    })
            if view["windows"] and all(window["percent"] is None for window in view["windows"]):
                view["status"] = "stale"
        return name, view

    rows = await asyncio.gather(*(read_coder(name) for name in coder_names))
    budgets = await asyncio.gather(read_rest_budget(redis_client), read_graphql_budget(redis_client))
    github = []
    for label, budget in zip(("REST API", "GraphQL API"), budgets, strict=True):
        current = budget is not None and budget.reset_at.timestamp() > now
        github.append({
            "label": label,
            "status": "available" if current else "unavailable",
            "remaining": budget.remaining if current else None,
            "limit": budget.limit if current else None,
            "reset_at": int(budget.reset_at.timestamp()) if current else None,
        })
    return {"coder_usage": dict(rows), "github_usage": github}
