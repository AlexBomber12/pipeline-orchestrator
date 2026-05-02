"""Onboarding routes that reconcile a repo's AGENTS.md.

Operators trigger reconciliation from the dashboard by clicking
"Onboard" on a repo card; that hits ``/onboarding/preview`` first to
diff the proposed file body and then ``/onboarding/apply`` to write.
The endpoints share a path-traversal sandbox in
:func:`_resolve_onboarding_target` to keep apply from escaping
``REPOS_DIR`` via a malformed slug or planted symlink.
"""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, Form
from fastapi.responses import JSONResponse

from src.config import load_config
from src.onboarding.markdown_sections import MarkerError
from src.onboarding.reconciliation import reconcile_agents_md
from src.utils import repo_slug_from_url

# ``router`` MUST be bound before ``src.web.app`` is imported. The app
# module loads this one via ``from src.web.routes import onboarding as
# _onboarding_routes`` and immediately calls ``app.include_router(
# _onboarding_routes.router)``; if onboarding.py is the entry point of
# the import, app.py runs while this module is still partial and would
# fail without ``router`` already on the partial module.
router = APIRouter()

from src.web import app as _app  # noqa: E402 — must follow ``router = APIRouter()``

_REPO_SLUG_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$"
)


def _resolve_onboarding_target(repo_name: str) -> Path | None:
    """Return the AGENTS.md path for ``repo_name`` if it is safe to touch.

    Returns ``None`` when ``repo_name`` fails the slug regex, is not
    listed in ``config.yml``, would resolve outside ``REPOS_DIR``, or
    the on-disk repo directory is not an existing git checkout (no
    ``.git`` entry). The combination of regex, config-membership check,
    and ``relative_to`` resolution is the path-traversal sandbox: any
    single layer alone would be insufficient because a malformed config
    entry, a permissive regex, or a symlink under ``REPOS_DIR`` could
    each individually allow escape. The ``.git`` check additionally
    prevents apply from creating a fresh non-git directory under
    ``REPOS_DIR`` — that would later trip ``ensure_repo_cloned`` into
    running ``git fetch`` against a non-repo and parking the daemon in
    an error state.
    """
    if not _REPO_SLUG_PATTERN.fullmatch(repo_name):
        return None
    cfg = load_config(_app.CONFIG_PATH)
    known_slugs = {repo_slug_from_url(repo.url) for repo in cfg.repositories}
    if repo_name not in known_slugs:
        return None
    repos_root = Path(_app.REPOS_DIR).resolve()
    repo_dir = (Path(_app.REPOS_DIR) / repo_name).resolve()
    target = repo_dir / "AGENTS.md"
    try:
        target.relative_to(repos_root)
    except ValueError:
        return None
    if not repo_dir.is_dir() or not (repo_dir / ".git").exists():
        return None
    # ``target.relative_to`` only validates the textual path; if AGENTS.md
    # itself is a symlink, ``read_text``/``write_text`` would follow it and
    # could read or overwrite a file outside REPOS_DIR. Reject symlinked
    # AGENTS.md outright so reconciliation only ever touches a regular
    # file under operator control.
    if target.is_symlink():
        return None
    # ``read_text`` on a directory or other non-regular path raises
    # ``IsADirectoryError`` / ``OSError`` rather than ``FileNotFoundError``,
    # which would bubble up as a 500. A repo can legitimately contain an
    # ``AGENTS.md/`` directory, so reject any non-regular existing target
    # at the resolver to keep the endpoints' 4xx contract intact.
    if target.exists() and not target.is_file():
        return None
    return target


@router.post("/onboarding/preview")
async def onboarding_preview(repo_name: str = Form(...)) -> JSONResponse:
    """Return what onboarding reconciliation would change in AGENTS.md.

    Form field ``repo_name`` is the repo slug (``owner__repo``). The
    endpoint never writes; the response payload contains the full
    proposed file body and a unified diff so the operator can decide
    whether to call :func:`onboarding_apply`.
    """
    target = _resolve_onboarding_target(repo_name)
    if target is None:
        return JSONResponse(
            {"error": "Unknown or invalid repo_name"}, status_code=422
        )
    try:
        proposed, diff = reconcile_agents_md(target, dry_run=True)
    except MarkerError as exc:
        return JSONResponse(
            {"error": f"Malformed managed markers in AGENTS.md: {exc}"},
            status_code=422,
        )
    return JSONResponse(
        {
            "applied": False,
            "diff": diff,
            "proposed_content": proposed,
        }
    )


@router.post("/onboarding/apply")
async def onboarding_apply(repo_name: str = Form(...)) -> JSONResponse:
    """Write the reconciled AGENTS.md for ``repo_name`` to disk."""
    target = _resolve_onboarding_target(repo_name)
    if target is None:
        return JSONResponse(
            {"error": "Unknown or invalid repo_name"}, status_code=422
        )
    try:
        final, diff = reconcile_agents_md(target, dry_run=False)
    except MarkerError as exc:
        return JSONResponse(
            {"error": f"Malformed managed markers in AGENTS.md: {exc}"},
            status_code=422,
        )
    return JSONResponse(
        {
            "applied": True,
            "diff": diff,
            "proposed_content": final,
        }
    )
