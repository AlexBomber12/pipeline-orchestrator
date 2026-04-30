"""Tests for src/onboarding/markdown_sections.py."""

from __future__ import annotations

import pytest
from src.onboarding.markdown_sections import (
    MarkerError,
    apply_managed_regions,
    extract_managed_regions,
    validate_no_user_content_inside_markers,
)


def test_extract_returns_empty_dict_for_content_with_no_markers() -> None:
    assert extract_managed_regions("# Title\n\nNo markers here.\n") == {}


def test_extract_returns_single_section() -> None:
    content = (
        "Intro paragraph.\n"
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\n"
        "ci body\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
    )
    assert extract_managed_regions(content) == {"ci": "\nci body\n"}


def test_extract_returns_multiple_sections() -> None:
    content = (
        "# AGENTS\n\n"
        "<!-- pipeline-orchestrator: managed BEGIN one -->\nA\n"
        "<!-- pipeline-orchestrator: managed END one -->\n\n"
        "user notes\n\n"
        "<!-- pipeline-orchestrator: managed BEGIN two -->\nB\nB2\n"
        "<!-- pipeline-orchestrator: managed END two -->\n"
    )
    assert extract_managed_regions(content) == {
        "one": "\nA\n",
        "two": "\nB\nB2\n",
    }


def test_apply_replaces_existing_region_in_place() -> None:
    content = (
        "leading\n"
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\n"
        "old body\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
        "trailing\n"
    )
    updated = apply_managed_regions(content, {"ci": "\nnew body\n"})
    assert updated == (
        "leading\n"
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\n"
        "new body\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
        "trailing\n"
    )


def test_apply_appends_new_region_at_end() -> None:
    content = "# AGENTS\n\nUser-authored intro.\n"
    updated = apply_managed_regions(content, {"escalate": "\nbody\n"})
    assert updated == (
        "# AGENTS\n\nUser-authored intro.\n"
        "<!-- pipeline-orchestrator: managed BEGIN escalate -->\n"
        "body\n"
        "<!-- pipeline-orchestrator: managed END escalate -->\n"
    )


def test_apply_preserves_all_user_content_outside_markers() -> None:
    user_prefix = "# AGENTS\n\nMission: do the thing.\n\n"
    user_middle = "\n## Workflow\n\nstep 1\nstep 2\n\n"
    user_suffix = "\n## Security\n\nbe careful\n"
    content = (
        user_prefix
        + "<!-- pipeline-orchestrator: managed BEGIN one -->\nold one\n"
        + "<!-- pipeline-orchestrator: managed END one -->\n"
        + user_middle
        + "<!-- pipeline-orchestrator: managed BEGIN two -->\nold two\n"
        + "<!-- pipeline-orchestrator: managed END two -->\n"
        + user_suffix
    )
    updated = apply_managed_regions(
        content,
        {"one": "\nfresh one\n", "two": "\nfresh two\n"},
    )
    assert user_prefix in updated
    assert user_middle in updated
    assert user_suffix in updated
    assert "old one" not in updated
    assert "old two" not in updated
    assert "fresh one" in updated
    assert "fresh two" in updated


def test_validate_raises_on_unmatched_start_marker() -> None:
    content = (
        "intro\n"
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\n"
        "body without end marker\n"
    )
    with pytest.raises(MarkerError, match="no matching END"):
        validate_no_user_content_inside_markers(content)


def test_validate_raises_on_nested_markers() -> None:
    content = (
        "<!-- pipeline-orchestrator: managed BEGIN outer -->\n"
        "<!-- pipeline-orchestrator: managed BEGIN inner -->\n"
        "<!-- pipeline-orchestrator: managed END inner -->\n"
        "<!-- pipeline-orchestrator: managed END outer -->\n"
    )
    with pytest.raises(MarkerError, match="nested marker"):
        validate_no_user_content_inside_markers(content)


def test_validate_raises_on_end_without_begin() -> None:
    content = (
        "intro\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
    )
    with pytest.raises(MarkerError, match="does not close"):
        validate_no_user_content_inside_markers(content)


def test_validate_raises_on_duplicate_section_name() -> None:
    content = (
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\nfirst\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
        "user notes\n"
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\nsecond\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
    )
    with pytest.raises(MarkerError, match="duplicate managed section name"):
        validate_no_user_content_inside_markers(content)


def test_extract_raises_on_duplicate_section_name() -> None:
    content = (
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\nfirst\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
        "<!-- pipeline-orchestrator: managed BEGIN ci -->\nsecond\n"
        "<!-- pipeline-orchestrator: managed END ci -->\n"
    )
    with pytest.raises(MarkerError, match="duplicate managed section name"):
        extract_managed_regions(content)


def test_apply_appends_when_content_lacks_trailing_newline() -> None:
    updated = apply_managed_regions(
        "no newline here", {"sec": "\nbody\n"}
    )
    assert updated == (
        "no newline here\n"
        "<!-- pipeline-orchestrator: managed BEGIN sec -->\n"
        "body\n"
        "<!-- pipeline-orchestrator: managed END sec -->\n"
    )


def test_apply_round_trip_is_identity() -> None:
    content = (
        "preamble\n"
        "<!-- pipeline-orchestrator: managed BEGIN a -->\nA\n"
        "<!-- pipeline-orchestrator: managed END a -->\n"
        "between\n"
        "<!-- pipeline-orchestrator: managed BEGIN b -->\nB\n"
        "<!-- pipeline-orchestrator: managed END b -->\n"
        "tail\n"
    )
    assert apply_managed_regions(content, extract_managed_regions(content)) == content
