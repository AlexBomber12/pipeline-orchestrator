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


def test_list_repo_tasks_uses_collision_free_target_for_dotted_pr_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``.`` in ``pr_id`` would break ``#id`` CSS selector lookup.

    The id attribute is HTML5-valid with a dot, but ``#task-content-todo-PR-1.2``
    is parsed as ``#task-content-todo-PR-1`` plus a ``.2`` class. The macro
    keeps the literal pr_id in the DOM id and uses an attribute selector for
    ``hx-target`` so the dot is matched as data, not as a class delimiter.
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
    assert 'hx-get="/repos/example__alpha/tasks/PR-1.2"' in body
    assert "hx-target=\"[id='task-content-todo-PR-1.2']\"" in body
    assert 'id="task-content-todo-PR-1.2"' in body


def test_list_repo_tasks_target_ids_are_collision_free_across_pr_ids(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``PR-1.2`` and ``PR-1-2`` must render distinct DOM ids and hx-targets.

    A naive ``replace('.', '-')`` slugifier collapses both into the same
    ``task-content-<status>-PR-1-2`` token, producing duplicate ids in the
    same status bucket and making ``hx-target`` non-deterministic.
    """
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-1.2: Dotted task\n- Status: TODO\n- Branch: pr-1-dot-2\n\n"
        "## PR-1-2: Dashed task\n- Status: TODO\n- Branch: pr-1-2\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    body = response.text
    assert body.count('id="task-content-todo-PR-1.2"') == 1
    assert body.count('id="task-content-todo-PR-1-2"') == 1
    assert "hx-target=\"[id='task-content-todo-PR-1.2']\"" in body
    assert "hx-target=\"[id='task-content-todo-PR-1-2']\"" in body


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


def test_view_repo_task_returns_404_when_tasks_dir_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the on-disk repo has no ``tasks/`` dir, the viewer returns 404."""
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-001")

    assert response.status_code == 404
    assert "Task file not found" in response.text


def test_view_repo_task_uses_queued_tasks_file_when_filename_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A queue entry whose ``Tasks file:`` deviates from ``{pr_id}.md``
    must drive the lookup; otherwise the viewer incorrectly reports
    ``Task file not found`` for tasks the runner accepts.
    """
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-555: Custom-name task\n"
        "- Status: TODO\n"
        "- Branch: pr-555\n"
        "- Tasks file: tasks/custom-name.md\n",
        encoding="utf-8",
    )
    (repo_dir / "tasks" / "custom-name.md").write_text(
        "# PR-555: Custom-name task\n\nbody from custom file\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-555")

    assert response.status_code == 200
    body = response.text
    assert "body from custom file" in body


def test_view_repo_task_rejects_symlink_under_tasks_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A symlink in ``tasks/`` pointing at a host file must not be read.

    Without this guard, a configured repo containing
    ``tasks/PR-666.md -> /data/secrets/...`` would exfiltrate the symlink
    target through the dashboard.
    """
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    secret = tmp_path / "host-secret.txt"
    secret.write_text("super secret host content", encoding="utf-8")
    (repo_dir / "tasks" / "PR-666.md").symlink_to(secret)

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-666")

    assert response.status_code == 404
    assert "Task file not found" in response.text
    assert "super secret" not in response.text


def test_view_repo_task_rejects_queue_file_escaping_tasks_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A queue entry whose ``Tasks file:`` walks out of ``tasks/`` is rejected.

    The runner only follows queue entries written by the daemon, but the
    dashboard must defend against a tampered QUEUE.md regardless.
    """
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    sibling = repo_dir / "secret.md"
    sibling.write_text("repo-level secret", encoding="utf-8")
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-777: Escape task\n"
        "- Status: TODO\n"
        "- Branch: pr-777\n"
        "- Tasks file: tasks/../secret.md\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-777")

    assert response.status_code == 404
    assert "Task file not found" in response.text
    assert "repo-level secret" not in response.text


def test_view_repo_task_rejects_queue_file_with_absolute_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An absolute ``Tasks file:`` value must not be honored."""
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    elsewhere = tmp_path / "elsewhere.md"
    elsewhere.write_text("absolute path target", encoding="utf-8")
    (repo_dir / "tasks" / "QUEUE.md").write_text(
        "## PR-888: Absolute path task\n"
        "- Status: TODO\n"
        "- Branch: pr-888\n"
        f"- Tasks file: {elsewhere}\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-888")

    assert response.status_code == 404
    assert "Task file not found" in response.text
    assert "absolute path target" not in response.text


def test_view_repo_task_rejects_directory_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the candidate path resolves to a directory, return 404."""
    repo_dir = _write_alpha_config(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "PR-999.md").mkdir()

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks/PR-999")

    assert response.status_code == 404
    assert "Task file not found" in response.text


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
