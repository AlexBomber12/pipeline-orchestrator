from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from src.models import PipelineState, RepoState
from src.web.app import templates


COMPONENT_DIR = Path("src/web/templates/components")
CHANGED_COMPONENTS = (
    COMPONENT_DIR / "repo_list.html",
    COMPONENT_DIR / "activity_feed.html",
    COMPONENT_DIR / "event_list.html",
)
WARN_BADGE = "bg-warn/10 text-warn border-warn/30"


def _source(path: Path) -> str:
    return path.read_text()


def _repo(state: PipelineState) -> RepoState:
    return RepoState(
        url="https://github.com/example/repo.git",
        name="example__repo",
        state=state,
        last_updated=datetime(2026, 5, 23, tzinfo=timezone.utc),
    )


def _render_repo_cards(repo: RepoState) -> str:
    return templates.get_template("components/repo_cards.html").render(
        repos=[repo],
        resources=None,
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress={},
        inhibitor_labels={},
        css_escape=lambda value: value,
        upload_feedback_target=lambda _name: "",
        utcnow=lambda: datetime.now(timezone.utc),
    )


def _render_repo_list(repo: RepoState) -> str:
    return templates.get_template("components/repo_list.html").render(repos=[repo])


def _render_repo_summary(repo: RepoState) -> str:
    return templates.get_template("components/repo_summary.html").render(
        repo=repo,
        resources=None,
        show_rate_limit_badge=False,
        selected_repo_coder="any",
        selected_repo_coder_label="Any",
        active_repo_coder=None,
        active_repo_coder_label="",
        inherit_coder="claude",
        guardrail_pending=[],
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        css_escape=lambda value: value,
        utcnow=lambda: datetime.now(timezone.utc),
    )


def _render_activity_feed(state: PipelineState) -> str:
    return templates.get_template("components/activity_feed.html").render(
        feed=[
            {
                "time": "12:00:00",
                "repo_name": "example__repo",
                "repo_style": "bg-gray-500/10 text-gray-300 border-gray-500/30",
                "repo_abbrev": "repo",
                "state": state.value,
                "event": "state update",
            }
        ]
    )


def _render_event_list(state: PipelineState) -> str:
    return templates.get_template("components/event_list.html").render(
        events=[
            {
                "time": "12:00:00",
                "state": state.value,
                "message": "state update",
            }
        ],
        repo={"name": "example__repo"},
    )


def test_no_local_state_styles_in_repo_list() -> None:
    assert "set state_styles =" not in _source(COMPONENT_DIR / "repo_list.html")


def test_no_local_state_styles_in_activity_feed() -> None:
    assert "set state_styles =" not in _source(COMPONENT_DIR / "activity_feed.html")


def test_no_local_state_styles_in_event_list() -> None:
    assert "set state_styles =" not in _source(COMPONENT_DIR / "event_list.html")


def test_paused_badge_consistent_across_components() -> None:
    repo = _repo(PipelineState.PAUSED)

    for html in (
        _render_repo_cards(repo),
        _render_repo_list(repo),
        _render_repo_summary(repo),
    ):
        assert WARN_BADGE in html


def test_each_component_imports_canonical() -> None:
    for path in CHANGED_COMPONENTS:
        assert 'import "components/_repo_state_styles.html"' in _source(path)


def test_all_pipeline_states_render_in_each_component() -> None:
    for state in PipelineState:
        repo = _repo(state)
        rendered = (
            _render_repo_list(repo),
            _render_activity_feed(state),
            _render_event_list(state),
        )

        for html in rendered:
            assert state.value in html
            assert re.search(
                r"bg-[\w-]+(?:/\d+)? text-[\w-]+ border-[\w-]+(?:/\d+)?",
                html,
            )
