"""Persisted outcome severity for the remaining FIX helper events."""

import asyncio
from unittest.mock import Mock

import pytest
from src.daemon import fix_codex_trigger, fix_push_verify

from tests.runner import _helpers as h


@pytest.mark.parametrize("operation", ["fetch", "rev-parse", "merge-base"])
@pytest.mark.parametrize("context", ["after FIX stop", "after FIX exit"])
def test_failed_push_verification_records_error(monkeypatch, operation, context):
    def git(repo_path, command, *args, **kwargs):
        if command == operation:
            raise OSError("verification unavailable")
        return Mock(stdout="remote-ahead", returncode=0)

    monkeypatch.setattr(fix_push_verify.git_ops, "_git", git)
    runner = h._make_runner()
    assert fix_push_verify.verify_pushes_since(
        runner, "feature", "old-head", "new-head", context=context,
    ) is None
    event = runner.state.history[-1]
    assert operation in event["event"]
    assert context in event["event"]
    assert event["severity"] == "error"


def test_duplicate_review_skip_records_ordinary_progress():
    runner = h._make_runner()
    runner._should_skip_codex_review_post = Mock(return_value=True)
    runner._post_codex_review = Mock()
    assert asyncio.run(fix_codex_trigger.maybe_post_codex_review_after_push(
        runner, 42, "after FIX push",
    ))
    runner._post_codex_review.assert_not_called()
    event = runner.state.history[-1]
    assert "skipping duplicate" in event["event"]
    assert event["severity"] == "info"
