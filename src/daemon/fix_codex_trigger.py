"""@codex review retrigger helper extracted from ``handlers/fix.py`` (PR-230).

After every productive FIX push the daemon must request a fresh Codex
review unless the auto-trigger already fired (the OBS-Z race window). The
same skip-or-post-or-error pattern appeared inline three times in
``handle_fix`` (breach-cancel, late-breach, productive-push); this module
collapses it into one helper.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.daemon.runner import PipelineRunner


async def maybe_post_codex_review_after_push(
    runner: "PipelineRunner",
    pr_number: int,
    failure_detail: str,
) -> bool:
    """Post ``@codex review`` for ``pr_number`` after a fix push.

    Skips when ``_should_skip_codex_review_post`` reports a fresh EYES
    reaction (PR-189 OBS-Z race). On post failure, transitions the
    runner to ``ERROR`` via ``_transition_to_error`` so the next idle
    cycle does not loop back into FIX without ever re-requesting a
    review (Codex P1 on PR #190).

    ``failure_detail`` is appended to the ERROR ``error_message`` so an
    operator reading the dashboard can distinguish breach-cancel,
    late-breach, and productive-push failure paths.

    Returns ``True`` when posting succeeded (or was correctly skipped).
    Returns ``False`` after an ERROR transition: the caller must return
    immediately because ``state.state`` is no longer ``FIX``.
    """
    if runner._should_skip_codex_review_post(pr_number):
        runner.log_event(
            "[FIX] Codex auto-trigger detected, skipping duplicate "
            "@codex review post."
        )
        return True
    if runner._post_codex_review(pr_number):
        return True
    await runner._transition_to_error(
        (
            f"Failed to post @codex review on PR #{pr_number} "
            f"{failure_detail}"
        ),
        save_run_record_as=None,
        publish=False,
        log_prefix="[FIX]",
    )
    return False
