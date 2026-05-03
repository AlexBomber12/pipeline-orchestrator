"""Handler mixins for PipelineRunner state transitions."""

from __future__ import annotations


class CoderUnavailable(Exception):
    """Raised when coder invocation cannot proceed (rate-limit, etc.).

    The caller's runner state has already been adjusted (PAUSED, run record
    saved) before this exception is raised; ``handle_coding`` returns
    without further side effects when it sees this.
    """
