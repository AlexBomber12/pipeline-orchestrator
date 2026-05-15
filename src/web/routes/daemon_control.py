"""Daemon-level operator control endpoints.

PR-308b: ``POST /daemon/panic/resume`` clears the cascade panic state
so dispatch resumes without waiting for the auto-resume cooldown.
Lives in its own router because the surface is daemon-wide, not
per-repo — the existing ``repo_control`` module strictly handles
per-repo mutations.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import Response

from src.keyspace import daemon_panic_state

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/daemon/panic/resume", status_code=204)
async def resume_panic(request: Request) -> Response:
    """Clear the cascade ESCALATE panic state.

    Idempotent: if no panic state is active (Redis missing the key, or
    Redis itself unavailable), still returns 204 so the dashboard's
    HTMX button remains a no-op on repeat clicks. Logs the operator
    intervention so audit trails capture who broke the panic.
    """
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        return Response(status_code=204)
    try:
        existed = bool(await redis_client.delete(daemon_panic_state()))
    except Exception:
        logger.warning("Failed to clear cascade panic state", exc_info=True)
        return Response(status_code=204)
    if existed:
        logger.info("Operator cleared cascade panic state")
    return Response(status_code=204)
