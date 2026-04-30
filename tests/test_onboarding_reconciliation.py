"""Tests for src/onboarding/reconciliation.py and the onboarding endpoints."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.onboarding.agents_md_template import MANAGED_SECTIONS
from src.onboarding.reconciliation import reconcile_agents_md
from src.web import app as web_app
from src.web.app import app

USER_INTRO = (
    "# AGENTS\n\n"
    "## Mission\n"
    "Ship the thing.\n\n"
    "## Security policy\n"
    "- Never log PII.\n"
)


def _all_sections_present(content: str) -> bool:
    return all(
        f"managed BEGIN {name} " in content
        and f"managed END {name} " in content
        for name in MANAGED_SECTIONS
    )


def test_dry_run_on_missing_agents_md_proposes_adding_daemon_sections(
    tmp_path: Path,
) -> None:
    target = tmp_path / "AGENTS.md"
    proposed, diff = reconcile_agents_md(target, dry_run=True)

    assert not target.exists(), "dry-run must not write to disk"
    assert _all_sections_present(proposed)
    assert diff, "diff must be non-empty when daemon sections are added"
    for name in MANAGED_SECTIONS:
        assert f"+<!-- pipeline-orchestrator: managed BEGIN {name} -->" in diff


def test_dry_run_on_existing_agents_md_without_markers_appends_and_preserves(
    tmp_path: Path,
) -> None:
    target = tmp_path / "AGENTS.md"
    target.write_text(USER_INTRO)

    proposed, diff = reconcile_agents_md(target, dry_run=True)

    assert target.read_text() == USER_INTRO, "dry-run must not write to disk"
    assert proposed.startswith(USER_INTRO), "user content must remain at top"
    assert _all_sections_present(proposed)
    assert "## Mission" in proposed and "## Security policy" in proposed
    assert "+## Mission" not in diff, "user lines must not be re-added"


def test_dry_run_on_existing_markers_updates_in_place(tmp_path: Path) -> None:
    target = tmp_path / "AGENTS.md"
    stale_body = (
        "# AGENTS\n\n"
        "## User intro\n\n"
        "<!-- pipeline-orchestrator: managed BEGIN work_modes -->\n"
        "stale work modes content\n"
        "<!-- pipeline-orchestrator: managed END work_modes -->\n\n"
        "## Tail user notes\n"
    )
    target.write_text(stale_body)

    proposed, diff = reconcile_agents_md(target, dry_run=True)

    assert "stale work modes content" not in proposed
    assert "## User intro" in proposed
    assert "## Tail user notes" in proposed
    assert "## Work Modes" in proposed
    assert "-stale work modes content" in diff


def test_apply_writes_file_to_disk(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "AGENTS.md"

    final, diff = reconcile_agents_md(target, dry_run=False)

    assert target.exists()
    assert target.read_text() == final
    assert _all_sections_present(target.read_text())
    assert diff, "first apply produces a non-empty diff"


def test_apply_is_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "AGENTS.md"
    target.write_text(USER_INTRO)

    first_final, first_diff = reconcile_agents_md(target, dry_run=False)
    second_final, second_diff = reconcile_agents_md(target, dry_run=False)

    assert first_final == second_final
    assert first_diff
    assert second_diff == "", "second apply must be a no-op"
    assert target.read_text() == first_final


def test_dry_run_returns_empty_diff_when_already_reconciled(
    tmp_path: Path,
) -> None:
    target = tmp_path / "AGENTS.md"
    reconcile_agents_md(target, dry_run=False)

    _, diff = reconcile_agents_md(target, dry_run=True)

    assert diff == "", "dry-run on a reconciled file must show no changes"


# ---------------------------------------------------------------------------
# Web endpoint tests
# ---------------------------------------------------------------------------


class _StubAioredisClient:
    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    @staticmethod
    def from_url(
        url: str, decode_responses: bool = True
    ) -> _StubAioredisClient:
        return _StubAioredisClient()


def _stub_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, slug: str = "example__alpha"
) -> Path:
    cfg = tmp_path / "config.yml"
    owner, repo = slug.split("__", 1)
    cfg.write_text(
        f"repositories:\n  - url: https://github.com/{owner}/{repo}.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / slug
    repo_dir.mkdir(parents=True)
    return repo_dir


def test_preview_endpoint_returns_diff_without_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _stub_repo(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["applied"] is False
    assert _all_sections_present(payload["proposed_content"])
    assert payload["diff"]
    assert not (repo_dir / "AGENTS.md").exists()


def test_apply_endpoint_writes_agents_md(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _stub_repo(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["applied"] is True
    target = repo_dir / "AGENTS.md"
    assert target.exists()
    assert target.read_text() == payload["proposed_content"]


def test_preview_rejects_invalid_slug(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_repo(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "../etc"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}


def test_apply_rejects_unknown_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _stub_repo(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "other__repo"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}


def test_preview_rejects_path_traversal_via_symlink_escape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defense-in-depth: even a config-listed slug whose on-disk
    directory is a symlink pointing outside ``REPOS_DIR`` must be
    rejected so a malicious symlink cannot smuggle arbitrary writes
    past the regex + config-membership checks."""
    _stub_repo(tmp_path, monkeypatch, slug="example__alpha")
    repos_dir = tmp_path / "repos"
    # Replace the slug directory with a symlink that escapes REPOS_DIR.
    target_outside = tmp_path / "outside"
    target_outside.mkdir()
    (repos_dir / "example__alpha").rmdir()
    (repos_dir / "example__alpha").symlink_to(target_outside)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert not (target_outside / "AGENTS.md").exists()
