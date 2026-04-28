"""Tests for the tasks viewer endpoints in src/web/app.py."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.web import app as web_app
from src.web.app import app


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


def _write_alpha_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / "example__alpha"
    (repo_dir / "tasks").mkdir(parents=True)
    return repo_dir


def test_list_repo_tasks_returns_grouped_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-001: First done task\n- Status: DONE\n- Branch: pr-001\n\n"
        "## PR-002: Second queued task\n- Status: TODO\n- Branch: pr-002\n\n"
        "## PR-003: In-flight task\n- Status: DOING\n- Branch: pr-003\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "<!DOCTYPE" not in body  # fragment, not full page
    assert "Tasks queue" in body
    assert "3 total" in body
    assert "PR-001" in body
    assert "First done task" in body
    assert "PR-002" in body
    assert "Second queued task" in body
    assert "PR-003" in body
    assert "In-flight task" in body
    assert "In progress" in body
    assert 'hx-get="/repos/example__alpha/tasks/PR-001"' in body
    assert 'hx-get="/repos/example__alpha/tasks/PR-002"' in body
    assert 'hx-get="/repos/example__alpha/tasks/PR-003"' in body


def test_list_repo_tasks_slugifies_pr_id_with_dots_for_dom_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``.`` in ``pr_id`` would break CSS selector lookup of ``hx-target``.

    The id attribute is HTML5-valid with a dot, but CSS selectors interpret
    ``.`` as a class delimiter, so ``#task-content-todo-PR-1.2`` would never
    match the corresponding element. The macro must slugify the id token
    consistently for both the rendered ``id`` and the ``hx-target`` selector.
    """
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-1.2: Dotted task\n- Status: TODO\n- Branch: pr-1-2\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    # The hx-get URL keeps the raw pr_id (it is path-safe and the server
    # validates it), but the DOM id and hx-target selector use a slugified
    # form so the CSS selector still matches the element.
    assert 'hx-get="/repos/example__alpha/tasks/PR-1.2"' in body
    assert 'hx-target="#task-content-todo-PR-1-2"' in body
    assert 'id="task-content-todo-PR-1-2"' in body
    assert "task-content-todo-PR-1.2" not in body


def test_list_repo_tasks_omits_doing_section_when_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-100: Only TODO\n- Status: TODO\n- Branch: pr-100\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "PR-100" in body
    assert "In progress" not in body


def test_list_repo_tasks_returns_friendly_message_when_no_queue_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_alpha_config(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "0 total" in body
    assert "No tasks found" in body


def test_list_repo_tasks_renders_empty_status_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-200: Done only\n- Status: DONE\n- Branch: pr-200\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "No queued tasks." in body


def test_list_repo_tasks_empty_done_section_renders_placeholder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-300: Only TODO\n- Status: TODO\n- Branch: pr-300\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert "No completed tasks yet." in body


def test_list_repo_tasks_returns_404_for_unknown_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_alpha_config(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/repos/example__missing/tasks")

    assert response.status_code == 404
    assert "Repository not found" in response.text


def test_view_repo_task_returns_file_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    task_body = (
        "# PR-042: Sample task\n"
        "Branch: pr-042-sample\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 3\n"
        "- Coder: any\n\n"
        "## Problem\n"
        "Render <script>alert('xss')</script> safely.\n"
    )
    (repo_dir / "tasks" / "PR-042.md").write_text(task_body, encoding="utf-8")

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-042")

    assert response.status_code == 200
    body = response.text
    assert "<pre" in body
    assert "PR-042.md" in body
    assert "Sample task" in body
    # Markdown content must be rendered as escaped text, not executed HTML.
    assert "<script>alert" not in body
    assert "&lt;script&gt;alert" in body


def test_view_repo_task_returns_404_for_missing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_alpha_config(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-999")

    assert response.status_code == 404
    assert "Task file not found" in response.text


def test_view_repo_task_returns_404_for_unknown_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_alpha_config(tmp_path, monkeypatch)

    with TestClient(app) as client:
        response = client.get("/repos/example__missing/tasks/PR-001")

    assert response.status_code == 404
    assert "Repository not found" in response.text


def test_view_repo_task_rejects_invalid_pr_id_with_400(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_alpha_config(tmp_path, monkeypatch)

    with TestClient(app) as client:
        # No ``PR-`` prefix → fails the pr_id regex; cannot escape the
        # tasks directory because path-separator characters are not in
        # the allowed character class.
        response = client.get("/repos/example__alpha/tasks/etc-passwd")

    assert response.status_code == 400
    assert "Invalid task identifier" in response.text


def test_repo_detail_page_includes_tasks_panel_lazy_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert 'hx-get="/repos/example__alpha/tasks"' in body
    assert "Tasks queue" in body
