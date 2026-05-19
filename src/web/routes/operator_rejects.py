"""Read-only operator-reject cancellation history page."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from src.cancellation.storage import get_cancellation_cause, index_key
from src.utils import repo_slug_from_url
from src.web import app as _app

router = APIRouter()


def _decode_task_id(raw: object) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    return str(raw)


async def _read_operator_rejects(
    redis_client: Any,
    repo_name: str,
    *,
    limit: int,
) -> list[dict[str, str | None]]:
    task_ids = await redis_client.zrevrange(index_key(repo_name), 0, -1)
    rejects: list[dict[str, str | None]] = []
    for raw_task_id in task_ids or []:
        task_id = _decode_task_id(raw_task_id)
        try:
            cause = await get_cancellation_cause(
                redis_client,
                repo_name,
                task_id,
                refresh_ttl=False,
            )
        except Exception:
            continue
        if cause is None or not isinstance(cause.payload, dict):
            continue
        if cause.payload.get("subsource") != "operator_reject":
            continue
        excerpt = cause.payload.get("operator_reject_excerpt", "")
        if not isinstance(excerpt, str):
            excerpt = str(excerpt)
        rule = cause.payload.get("operator_reject_rule")
        if rule is not None and not isinstance(rule, str):
            rule = str(rule)
        rejects.append(
            {
                "task_id": task_id,
                "canceled_at": cause.created_at,
                "rule": rule,
                "excerpt": excerpt[:200],
            }
        )
        if len(rejects) >= limit:
            break
    return rejects


@router.get("/repo/{name}/rejects", response_class=HTMLResponse)
async def operator_reject_history(
    request: Request,
    name: str,
    limit: int = Query(50, ge=1, le=500),
) -> HTMLResponse:
    redis_client = request.app.state.redis
    cfg = _app.load_config(_app.CONFIG_PATH)
    repo = next((r for r in cfg.repositories if repo_slug_from_url(r.url) == name), None)
    if repo is None:
        raise HTTPException(status_code=404, detail=f"Unknown repo: {name}")

    rejects = await _read_operator_rejects(redis_client, name, limit=limit)
    return _app.templates.TemplateResponse(
        request,
        "operator_rejects.html",
        {
            "repo_name": name,
            "rejects": rejects,
            "limit": limit,
        },
    )
