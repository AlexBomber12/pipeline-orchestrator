"""Tests for src/onboarding/reconciliation.py and the onboarding endpoints."""

from __future__ import annotations

import shutil
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


def test_reconcile_agents_md_propagates_forbidden_actions_section(
    tmp_path: Path,
) -> None:
    target = tmp_path / "AGENTS.md"
    target.write_text(USER_INTRO)

    proposed, _ = reconcile_agents_md(target, dry_run=True)

    assert (
        "<!-- pipeline-orchestrator: managed BEGIN forbidden_actions -->"
        in proposed
    )
    assert (
        "<!-- pipeline-orchestrator: managed END forbidden_actions -->"
        in proposed
    )


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
# Legacy unmarked "Quick rules" migration. Pre-PR-271 onboarded repos (and
# repos freshly scaffolded from the bundled template) carry an unmarked
# "Quick rules" block; without migration ``apply_managed_regions`` would
# leave it in place and append a second managed copy at EOF, producing two
# conflicting copies. The migration strips the legacy block before regions
# are applied so reconciliation lands a single managed ``quick_rules``
# section.
# ---------------------------------------------------------------------------


_LEGACY_QUICK_RULES_BLOCK = (
    "Quick rules\n"
    "- Always choose a work mode using an exact trigger phrase.\n"
    "- Never commit secrets.\n"
    "- Always run the local gate `scripts/ci.sh` until it exits with code 0.\n"
)


def test_legacy_unmarked_quick_rules_is_stripped(tmp_path: Path) -> None:
    target = tmp_path / "AGENTS.md"
    target.write_text(
        "# AGENTS\n\n"
        "These rules apply to every PR and every task in this repo.\n\n"
        + _LEGACY_QUICK_RULES_BLOCK
        + "\n## Some user section\n\nUser content stays.\n"
    )

    proposed, _ = reconcile_agents_md(target, dry_run=True)

    assert proposed.count("Always choose a work mode") == 0, (
        "legacy unmarked Quick rules block must be removed"
    )
    assert proposed.count(
        "<!-- pipeline-orchestrator: managed BEGIN quick_rules -->"
    ) == 1, "exactly one managed quick_rules section must remain"
    assert "## Some user section" in proposed
    assert "User content stays." in proposed


def test_legacy_unmarked_quick_rules_alongside_existing_markers(
    tmp_path: Path,
) -> None:
    """A repo that already has the managed quick_rules markers AND a stale
    unmarked legacy block (from an earlier reconcile run before this fix
    landed) is not left with duplicate Quick rules content."""
    target = tmp_path / "AGENTS.md"
    target.write_text(
        "# AGENTS\n\n"
        + _LEGACY_QUICK_RULES_BLOCK
        + "\n<!-- pipeline-orchestrator: managed BEGIN quick_rules -->\n"
        "## Quick rules\n- existing managed bullet\n"
        "<!-- pipeline-orchestrator: managed END quick_rules -->\n"
    )

    proposed, _ = reconcile_agents_md(target, dry_run=True)

    assert "Always choose a work mode" not in proposed, (
        "legacy bullet must be stripped"
    )
    assert proposed.count(
        "<!-- pipeline-orchestrator: managed BEGIN quick_rules -->"
    ) == 1, "managed quick_rules must remain a single section"


def test_managed_quick_rules_heading_inside_markers_is_preserved(
    tmp_path: Path,
) -> None:
    """The migration must not touch the ``## Quick rules`` heading that
    lives inside the new managed region — only unmarked legacy blocks."""
    target = tmp_path / "AGENTS.md"
    reconcile_agents_md(target, dry_run=False)

    final, _ = reconcile_agents_md(target, dry_run=False)

    assert "## Quick rules" in final, (
        "managed Quick rules heading must survive a re-reconcile"
    )
    assert final.count(
        "<!-- pipeline-orchestrator: managed BEGIN quick_rules -->"
    ) == 1


def test_user_authored_quick_rules_without_bullets_is_preserved(
    tmp_path: Path,
) -> None:
    """A user-written ``Quick rules`` section that does not match the
    template form (no dash bullets) is left alone."""
    target = tmp_path / "AGENTS.md"
    target.write_text(
        "# AGENTS\n\n"
        "Quick rules\n\n"
        "Read the rules carefully and follow them.\n"
    )

    proposed, _ = reconcile_agents_md(target, dry_run=True)

    assert "Read the rules carefully and follow them." in proposed
    assert proposed.count(
        "<!-- pipeline-orchestrator: managed BEGIN quick_rules -->"
    ) == 1


def test_user_authored_quick_rules_with_bullets_is_preserved(
    tmp_path: Path,
) -> None:
    """A user-authored ``Quick rules`` section that uses the same heading
    + dash-bullet structure as the legacy template — but whose first
    bullet is NOT the canonical "Always choose a work mode" signature —
    must be preserved. Otherwise reconciliation silently deletes user
    content that happens to share the common Markdown shape."""
    target = tmp_path / "AGENTS.md"
    target.write_text(
        "# AGENTS\n\n"
        "## Quick rules\n"
        "- Use the linter before pushing.\n"
        "- Document new env vars in the README.\n"
        "- Tag the on-call before merging schema changes.\n"
        "\n## Other section\n\nOther content.\n"
    )

    proposed, _ = reconcile_agents_md(target, dry_run=True)

    assert "Use the linter before pushing." in proposed
    assert "Document new env vars in the README." in proposed
    assert "Tag the on-call before merging schema changes." in proposed
    assert "## Other section" in proposed
    # The managed quick_rules block is still appended, so two "Quick
    # rules" headings coexist: the user's and the managed one.
    assert proposed.count(
        "<!-- pipeline-orchestrator: managed BEGIN quick_rules -->"
    ) == 1


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
    # The onboarding endpoints only operate on existing git checkouts, so
    # the stub clone needs a ``.git`` marker for the resolver to accept
    # it. A directory marker is enough — the endpoints never invoke git.
    (repo_dir / ".git").mkdir()
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
    (target_outside / ".git").mkdir()
    shutil.rmtree(repos_dir / "example__alpha")
    (repos_dir / "example__alpha").symlink_to(target_outside)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert not (target_outside / "AGENTS.md").exists()


def test_preview_rejects_symlinked_agents_md(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An AGENTS.md symlinked to a file outside REPOS_DIR must be
    rejected. Otherwise read_text/write_text would follow the symlink
    and let preview surface, or apply overwrite, an external file."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    outside = tmp_path / "outside.md"
    outside.write_text("secret outside content\n")
    (repo_dir / "AGENTS.md").symlink_to(outside)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}
    assert outside.read_text() == "secret outside content\n"


def test_apply_rejects_symlinked_agents_md(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply must refuse to write through a symlinked AGENTS.md so an
    attacker cannot overwrite an external file by planting a symlink in
    a clone."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    outside = tmp_path / "outside.md"
    outside.write_text("secret outside content\n")
    (repo_dir / "AGENTS.md").symlink_to(outside)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}
    assert outside.read_text() == "secret outside content\n"


def test_apply_rejects_when_repo_directory_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply must not create a brand-new directory under REPOS_DIR for a
    config-listed slug that has not been cloned yet. Doing so would
    leave a non-git path that ``ensure_repo_cloned`` later trips on."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    shutil.rmtree(repo_dir)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}
    assert not repo_dir.exists()


def test_apply_rejects_when_repo_directory_is_not_a_git_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply must reject a slug whose directory exists but lacks
    ``.git`` — writing AGENTS.md there would still leave a non-repo
    that breaks subsequent daemon cycles."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    shutil.rmtree(repo_dir / ".git")

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}
    assert not (repo_dir / "AGENTS.md").exists()


def test_preview_rejects_agents_md_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A repo can contain an ``AGENTS.md/`` directory. ``read_text`` on a
    directory raises ``IsADirectoryError`` (not ``FileNotFoundError``),
    which would bubble up as a 500. The resolver must reject the target
    so preview returns a controlled 4xx instead."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    (repo_dir / "AGENTS.md").mkdir()

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}
    assert (repo_dir / "AGENTS.md").is_dir()


def test_apply_rejects_agents_md_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply must also refuse when AGENTS.md is a directory; otherwise
    ``write_text`` would raise ``IsADirectoryError`` and surface as a
    500 rather than a structured operator-facing error."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    (repo_dir / "AGENTS.md").mkdir()

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    assert response.json() == {"error": "Unknown or invalid repo_name"}
    assert (repo_dir / "AGENTS.md").is_dir()


_MALFORMED_AGENTS_MD = (
    "# AGENTS\n\n"
    "<!-- pipeline-orchestrator: managed BEGIN work_modes -->\n"
    "stale body without an END marker\n"
)


def test_preview_returns_client_error_on_malformed_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A target AGENTS.md with an unmatched managed marker must yield a
    structured 4xx response, not a 500. Operators rely on the JSON
    contract to surface what to fix."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    target = repo_dir / "AGENTS.md"
    target.write_text(_MALFORMED_AGENTS_MD)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/preview", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    payload = response.json()
    assert "Malformed managed markers" in payload["error"]
    assert "work_modes" in payload["error"]
    assert target.read_text() == _MALFORMED_AGENTS_MD


def test_apply_returns_client_error_on_malformed_markers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply must also surface a 4xx (not crash) so operators can
    reconcile a repo whose AGENTS.md already drifted into a malformed
    marker state."""
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    target = repo_dir / "AGENTS.md"
    target.write_text(_MALFORMED_AGENTS_MD)

    with TestClient(app) as client:
        response = client.post(
            "/onboarding/apply", data={"repo_name": "example__alpha"}
        )

    assert response.status_code == 422
    payload = response.json()
    assert "Malformed managed markers" in payload["error"]
    assert target.read_text() == _MALFORMED_AGENTS_MD


def test_onboarding_helpers_resolve_via_module_getattr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Onboarding helpers stay importable as ``web_app.X`` after PR-225b.

    The helpers now live in ``src.web.routes.onboarding`` and are
    re-exported via :func:`src.web.app.__getattr__`; touching one of
    them keeps that proxy branch exercised.
    """
    repo_dir = _stub_repo(tmp_path, monkeypatch)
    target = web_app._resolve_onboarding_target("example__alpha")
    assert target == repo_dir / "AGENTS.md"
    assert web_app._REPO_SLUG_PATTERN.fullmatch("example__alpha") is not None
