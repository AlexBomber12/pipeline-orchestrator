"""Read-only main-commit audit findings pages."""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from src.daemon.main_commit_audit import _branch_key_part, _findings_key
from src.utils import repo_slug_from_url
from src.web import app as _app

router = APIRouter()


def _decode_text(raw: object) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, bytes):
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return str(raw)


def _parse_finding(raw: object) -> dict[str, Any] | None:
    text = _decode_text(raw)
    if text is None:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _parse_findings_payload(raw: object) -> list[dict[str, Any]]:
    text = _decode_text(raw)
    if text is None:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    return []


async def _read_audit_findings(
    redis_client: Any,
    repo_name: str,
    branch: str,
) -> list[dict[str, Any]]:
    key = _findings_key(repo_name, branch)
    try:
        entries = await redis_client.lrange(key, 0, -1)
    except Exception:
        entries = None
    if isinstance(entries, list):
        return [
            finding
            for entry in entries
            if (finding := _parse_finding(entry)) is not None
        ]

    try:
        raw = await redis_client.get(key)
    except Exception:
        return []
    return _parse_findings_payload(raw)


async def _read_last_audit_timestamp(
    redis_client: Any,
    repo_name: str,
    branch: str,
) -> str | None:
    key = f"audit:main_commits:{repo_name}:{_branch_key_part(branch)}:last_audit_at"
    try:
        return _decode_text(await redis_client.get(key))
    except Exception:
        return None


def _github_owner_repo(repo_url: str, repo_name: str) -> str:
    slug = repo_slug_from_url(repo_url)
    if "__" in slug:
        owner, name = slug.split("__", 1)
        return f"{owner}/{name}"
    if "__" in repo_name:
        owner, name = repo_name.split("__", 1)
        return f"{owner}/{name}"
    return repo_name


@router.get("/repo/{name}/audit", response_class=HTMLResponse)
async def audit_findings(request: Request, name: str) -> HTMLResponse:
    redis_client = request.app.state.redis
    cfg = _app.load_config(_app.CONFIG_PATH)
    repo = next((r for r in cfg.repositories if repo_slug_from_url(r.url) == name), None)
    if repo is None:
        raise HTTPException(status_code=404, detail=f"Unknown repo: {name}")

    branch = repo.branch or "main"
    findings = await _read_audit_findings(redis_client, name, branch)
    last_audit_at = await _read_last_audit_timestamp(redis_client, name, branch)
    return _app.templates.TemplateResponse(
        request,
        "audit_findings.html",
        {
            "repo_name": name,
            "branch": branch,
            "findings": findings,
            "last_audit_at": last_audit_at,
            "github_owner_repo": _github_owner_repo(repo.url, name),
        },
    )
