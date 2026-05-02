"""Coder-neutral diagnosis prompt builder and verdict parser.

The pipeline orchestrator routes infrastructure-error diagnosis through the
selected coder CLI (``claude`` or ``codex``). Both CLIs produce the same
``FIX``/``SKIP``/``ESCALATE`` verdict and consume the same prompt; this
module hosts the shared prompt template and verdict parser so the per-CLI
wrappers only own their subprocess invocation details and so callers in
``src/daemon/handlers/error.py`` need not import a coder-specific module to
parse a diagnosis verdict.
"""

from __future__ import annotations


def build_diagnosis_prompt(repo_path: str, context: str) -> str:
    """Return the diagnosis prompt for ``context`` to send to a coder CLI.

    ``repo_path`` is accepted for symmetry with the CLI invocation surface
    even though the prompt body does not embed it; keeping the parameter
    here lets future prompt revisions reference repo state without
    touching every call site.
    """
    del repo_path
    return (
        "You are the pipeline orchestrator. An infrastructure error occurred. "
        f"Error context: {context} "
        "Respond with exactly one word on the first line: FIX, SKIP, or ESCALATE. "
        "If FIX, include a brief action plan on subsequent lines."
    )


def parse_diagnosis(stdout: str) -> str:
    """Return ``FIX``, ``SKIP``, or ``ESCALATE`` from a diagnosis stdout.

    Anything that does not clearly start with one of those tokens is treated
    as ``ESCALATE`` so ambiguous responses never silently trigger a fix.
    """
    tokens = stdout.split()
    if not tokens:
        return "ESCALATE"

    first = tokens[0].upper()
    for verdict in ("FIX", "SKIP", "ESCALATE"):
        if first.startswith(verdict):
            return verdict
    return "ESCALATE"
