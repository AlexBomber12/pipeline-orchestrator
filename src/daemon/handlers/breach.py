"""In-flight rate-limit breach monitoring for CLI tasks.

Mixin methods:
    _monitor_inflight_breach — cancel task on statusline breach marker
    _breach_env             — return (breach_dir, run_id)
    _check_late_breach      — synchronous post-check for breach marker
    _cleanup_breach_marker  — remove breach marker file
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Directory where the statusline hook writes breach marker files.
_BREACH_DIR = "/tmp/pipeline-breach"

# Poll interval for the in-flight breach monitor (seconds).
_BREACH_POLL_SEC = 2


class BreachMixin:
    """In-flight rate-limit breach monitoring."""

    async def _monitor_inflight_breach(
        self,
        breach_dir: str,
        run_id: str,
        claude_task: asyncio.Task,  # type: ignore[type-arg]
        breach_flag: dict[str, bool],
    ) -> None:
        """Cancel *claude_task* if the statusline hook writes a breach marker."""
        marker = Path(breach_dir) / f"{run_id}.breach"
        while not claude_task.done():
            if marker.is_file():
                try:
                    data = json.loads(marker.read_text())
                except (OSError, json.JSONDecodeError):
                    await asyncio.sleep(_BREACH_POLL_SEC)
                    continue
                resets_at = data.get("resets_at", 0)
                if resets_at:
                    self.state.rate_limited_until = datetime.fromtimestamp(
                        resets_at, tz=timezone.utc
                    )
                else:
                    self.state.rate_limited_until = (
                        datetime.now(timezone.utc) + timedelta(minutes=30)
                    )
                self.state.rate_limit_reactive = True
                self.state.rate_limit_reactive_coder = "claude"
                breach_type = data.get("type", "session")
                pct_key = "session_pct" if breach_type == "session" else "weekly_pct"
                pct_val = data.get(pct_key, "?")
                self.log_event(
                    f"In-flight breach: {breach_type} at {pct_val}%, "
                    f"killing Claude CLI"
                )
                breach_flag["breached"] = True
                claude_task.cancel()
                return
            await asyncio.sleep(_BREACH_POLL_SEC)

    def _breach_env(self) -> tuple[str, str]:
        """Return ``(breach_dir, run_id)`` for an in-flight breach monitor."""
        breach_dir = _BREACH_DIR
        Path(breach_dir).mkdir(parents=True, exist_ok=True)
        run_id = uuid.uuid4().hex[:12]
        return breach_dir, run_id

    def _check_late_breach(
        self, breach_dir: str, run_id: str, breach_flag: dict[str, bool],
    ) -> None:
        """Final synchronous check for a breach marker the poll loop missed."""
        if breach_flag["breached"]:
            return
        marker = Path(breach_dir) / f"{run_id}.breach"
        if not marker.is_file():
            return
        data = None
        for _ in range(3):
            try:
                data = json.loads(marker.read_text())
                break
            except (OSError, json.JSONDecodeError):
                time.sleep(0.1)
        if data is None:
            return
        resets_at = data.get("resets_at", 0)
        if resets_at:
            self.state.rate_limited_until = datetime.fromtimestamp(
                resets_at, tz=timezone.utc
            )
        else:
            self.state.rate_limited_until = (
                datetime.now(timezone.utc) + timedelta(minutes=30)
            )
        self.state.rate_limit_reactive = True
        self.state.rate_limit_reactive_coder = "claude"
        breach_type = data.get("type", "session")
        pct_key = "session_pct" if breach_type == "session" else "weekly_pct"
        pct_val = data.get(pct_key, "?")
        self.log_event(
            f"Late in-flight breach detected: {breach_type} at {pct_val}%"
        )
        breach_flag["breached"] = True

    def _cleanup_breach_marker(self, breach_dir: str, run_id: str) -> None:
        """Remove the breach marker file for a completed run."""
        marker = Path(breach_dir) / f"{run_id}.breach"
        try:
            marker.unlink(missing_ok=True)
        except OSError:
            pass
