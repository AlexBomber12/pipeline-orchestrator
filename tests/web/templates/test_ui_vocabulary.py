"""Shared operator vocabulary is rendered from the same observed state."""
import re
from pathlib import Path

import pytest
from src.models import CIStatus, PipelineState, PRInfo, QueueTask, ReviewStatus, TaskStatus
from src.web.app import templates

from tests.web.templates.test_state_styles_canonical import (
    _render_repo_cards,
    _render_repo_summary,
    _repo,
)

RENDERERS = (_render_repo_cards, _render_repo_summary)


def _text(html):
    return " ".join(re.sub(r"<[^>]+>", " ", html).split())


@pytest.mark.parametrize("ci,label", zip(CIStatus, (
    "CI pending", "CI passed", "CI failed", "CI infra failure",
)))
@pytest.mark.parametrize("review,label_review", zip(ReviewStatus, (
    "Review pending", "Review in review", "Review approved", "Review changes requested",
)))
def test_same_ci_and_review_labels(ci, label, review, label_review):
    repo = _repo(PipelineState.WATCH)
    repo.current_pr = PRInfo(number=1, branch="pr-1", ci_status=ci, review_status=review)
    for render in RENDERERS:
        text = _text(render(repo))
        assert label in text
        assert label_review in text
        assert not any(emoji in text for emoji in ("👍", "👀", "✏"))
        if review == ReviewStatus.APPROVED and ci == CIStatus.PENDING:
            assert "Review approved · waiting for CI" in text
        if ci == CIStatus.SUCCESS:
            expected = {ReviewStatus.APPROVED: "merge checks",
                        ReviewStatus.CHANGES_REQUESTED: "fixes"}.get(review, "review")
            assert f"waiting for {expected}" in text


@pytest.mark.parametrize("queue,expected", [
    (None, "Queue snapshot unavailable."),
    ([], "No eligible tasks in the queue snapshot."),
    ([QueueTask(pr_id="PR-1", title="Blocked", status=TaskStatus.TODO,
                unresolved_deps=["PR-0"])], "Tasks blocked by dependencies remain in the queue."),
    ([QueueTask(pr_id="PR-1", title="Retained", status=TaskStatus.ERROR)],
     "Error tasks retained; use Retry where available."),
    ([QueueTask(pr_id="PR-1", title="Verified", status=TaskStatus.DONE)], "Queue 1 / 1"),
])
def test_queue_states_are_distinct(queue, expected):
    repo = _repo(PipelineState.IDLE)
    repo.current_queue = queue
    # Counters alone cannot establish completion or snapshot availability.
    repo.queue_done = repo.queue_total = 999
    for render in RENDERERS:
        text = _text(render(repo))
        assert expected in text
        assert "All tasks completed" not in text
        assert "999" not in text
        assert "No active task." in text


def test_watch_without_pr_and_non_watch_states():
    for render in RENDERERS:
        assert "Waiting for PR status" in render(_repo(PipelineState.WATCH))
        assert "data-watch-status" not in render(_repo(PipelineState.PAUSED))
        assert "⏸" in render(_repo(PipelineState.PAUSED))


def test_task_headings_and_retry_hint_keep_blocked_and_error_visible():
    grouped = {key: [dict(pr_id=f"PR-{key}", title=key, retry_count=0,
                         unresolved_deps=["PR-missing"] if key == "todo" else [])]
               for key in ("doing", "todo", "error", "done")}
    template = templates.get_template("components/tasks_panel.html")
    text = _text(template.render(repo_name="example__repo", tasks_by_status=grouped,
                                 tasks_total=4, retry_cap=3))
    for heading in ("Doing", "Todo", "Error", "Done"):
        assert heading in text
    assert "Blocked by: PR-missing" in text
    assert "Use Retry where available, or edit and re-upload the task file." in text
    assert "PR-error" in text
    assert "No eligible tasks" not in text
    empty = _text(template.render(tasks_total=0))
    assert "No eligible tasks in the queue snapshot." in empty


def test_templates_have_no_decorative_emoji():
    for name in ("repo_cards", "repo_summary", "tasks_panel", "_status_vocabulary"):
        source = Path(f"src/web/templates/components/{name}.html").read_text()
        assert not any(emoji in source for emoji in ("👍", "👀", "✏", "✅", "❌"))
