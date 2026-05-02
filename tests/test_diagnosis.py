"""Tests for src/diagnosis.py."""

from __future__ import annotations

from src.diagnosis import build_diagnosis_prompt, parse_diagnosis


def test_parse_diagnosis_fix_with_plan() -> None:
    assert parse_diagnosis("FIX\ndo something") == "FIX"


def test_parse_diagnosis_skip() -> None:
    assert parse_diagnosis("SKIP") == "SKIP"


def test_parse_diagnosis_escalate_with_suffix() -> None:
    assert parse_diagnosis("ESCALATE: too complex") == "ESCALATE"


def test_parse_diagnosis_unknown_defaults_to_escalate() -> None:
    assert parse_diagnosis("I don't know") == "ESCALATE"


def test_parse_diagnosis_empty_defaults_to_escalate() -> None:
    assert parse_diagnosis("") == "ESCALATE"


def test_parse_diagnosis_whitespace_only_defaults_to_escalate() -> None:
    assert parse_diagnosis("   \n\t  ") == "ESCALATE"


def test_build_diagnosis_prompt_contains_context_and_verdicts() -> None:
    prompt = build_diagnosis_prompt("/data/repos/demo", "git push failed: 403")
    assert "Error context: git push failed: 403" in prompt
    assert "FIX, SKIP, or ESCALATE" in prompt
    assert prompt.startswith("You are the pipeline orchestrator.")


def test_build_diagnosis_prompt_byte_identical_to_legacy_template() -> None:
    """Guard the exact wording — PR-215 is a pure code move and behavior
    must remain byte-identical for both ``claude`` and ``codex`` CLIs.
    """
    expected = (
        "You are the pipeline orchestrator. An infrastructure error occurred. "
        "Error context: boom "
        "Respond with exactly one word on the first line: FIX, SKIP, or ESCALATE. "
        "If FIX, include a brief action plan on subsequent lines."
    )
    assert build_diagnosis_prompt("/data/repos/demo", "boom") == expected
