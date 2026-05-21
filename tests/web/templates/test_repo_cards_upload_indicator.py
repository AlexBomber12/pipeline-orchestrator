"""Template checks for per-card upload request indicators."""

from __future__ import annotations

from html.parser import HTMLParser

from src.models import RepoState
from src.web.app import templates
from src.web.services.upload_validation import (
    _escape_css_identifier,
    _upload_feedback_target,
)


class _ElementParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.stack: list[tuple[str, dict[str, str | None]]] = []
        self.elements: list[tuple[str, dict[str, str | None], list[str]]] = []
        self.text_by_id: dict[str, list[str]] = {}

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        attr_map = dict(attrs)
        ancestors = [
            ancestor_attrs["id"]
            for _tag, ancestor_attrs in self.stack
            if ancestor_attrs.get("id")
        ]
        self.elements.append((tag, attr_map, ancestors))
        self.stack.append((tag, attr_map))

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                return

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if not text:
            return
        for _tag, attrs in reversed(self.stack):
            element_id = attrs.get("id")
            if element_id:
                self.text_by_id.setdefault(element_id, []).append(text)
                return


def _repo(name: str) -> RepoState:
    return RepoState(url=f"https://github.com/example/{name}.git", name=name)


def _render(repos: list[RepoState]) -> str:
    return templates.get_template("components/repo_cards.html").render(
        repos=repos,
        resources=None,
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress={},
        inhibitor_labels={},
        css_escape=_escape_css_identifier,
        upload_feedback_target=_upload_feedback_target,
    )


def _parse(html: str) -> _ElementParser:
    parser = _ElementParser()
    parser.feed(html)
    return parser


def _elements_by_id(
    parser: _ElementParser, element_id: str
) -> list[tuple[str, dict[str, str | None], list[str]]]:
    return [
        element
        for element in parser.elements
        if element[1].get("id") == element_id
    ]


def _upload_form_attrs(
    parser: _ElementParser, repo_name: str
) -> dict[str, str | None]:
    for tag, attrs, _ancestors in parser.elements:
        if tag == "form" and attrs.get("hx-post") == f"/repos/{repo_name}/upload-tasks":
            return attrs
    raise AssertionError(f"upload form for {repo_name} not found")


def test_per_card_indicator_id_present() -> None:
    html = _render([_repo("pipeline-orchestrator")])

    assert 'id="upload-indicator-pipeline-orchestrator"' in html


def test_form_hx_indicator_matches_per_card_id() -> None:
    repo_name = "pipeline-orchestrator"
    parser = _parse(_render([_repo(repo_name)]))

    attrs = _upload_form_attrs(parser, repo_name)

    assert attrs["hx-indicator"] == "#upload-indicator-pipeline-orchestrator"


def test_multiple_repos_render_separate_indicators() -> None:
    repo_names = ["pipeline-orchestrator", "alpha", "beta"]
    parser = _parse(_render([_repo(name) for name in repo_names]))
    ids = [
        attrs["id"]
        for tag, attrs, _ancestors in parser.elements
        if tag == "div" and attrs.get("id", "").startswith("upload-indicator-")
    ]

    assert ids == [f"upload-indicator-{name}" for name in repo_names]
    assert len(ids) == len(set(ids)) == 3


def test_global_spinner_not_referenced_in_upload_form() -> None:
    parser = _parse(_render([_repo("pipeline-orchestrator")]))

    attrs = _upload_form_attrs(parser, "pipeline-orchestrator")

    assert attrs["hx-indicator"] != "#global-spinner"


def test_indicator_has_htmx_indicator_class() -> None:
    parser = _parse(_render([_repo("pipeline-orchestrator")]))
    [indicator] = _elements_by_id(parser, "upload-indicator-pipeline-orchestrator")

    assert "htmx-indicator" in (indicator[1].get("class") or "").split()


def test_indicator_element_outside_feedback_target() -> None:
    parser = _parse(_render([_repo("pipeline-orchestrator")]))
    [indicator] = _elements_by_id(parser, "upload-indicator-pipeline-orchestrator")

    assert "upload-feedback-pipeline-orchestrator" not in indicator[2]


def test_indicator_text_says_uploading() -> None:
    parser = _parse(_render([_repo("pipeline-orchestrator")]))
    text = " ".join(parser.text_by_id["upload-indicator-pipeline-orchestrator"])

    assert "uploading" in text.lower()
