"""Rate-limit detection and proactive usage checks.

Mixin methods:
    _proactive_usage_check — check usage API before CLI invocation
    _check_rate_limit      — unified rate-limit gate (proactive + reactive)
    _detect_rate_limit     — parse stderr for rate-limit signals
"""

from __future__ import annotations

import asyncio
import math
import re
from datetime import datetime, timedelta, timezone

from src.models import PipelineState


class RateLimitMixin:
    """Rate-limit detection and proactive usage checks."""

    async def _proactive_usage_check(self, proactive_coder: str | None = None) -> bool:
        """Return True if CLI calls are allowed, False if usage threshold breached.

        Fail-open: returns True when the provider cannot reach the endpoint,
        deferring to the reactive _detect_rate_limit on stderr after the CLI run.

        When *proactive_coder* is set it overrides the configured coder so
        callers that always use a specific CLI (e.g. ``handle_error`` →
        ``claude_cli``) check the correct provider's quota.
        """
        coder_name = proactive_coder or self._get_coder()[0]
        provider = (
            self._claude_usage_provider
            if coder_name == "claude"
            else self._codex_usage_provider
        )
        snapshot = await asyncio.to_thread(provider.fetch)
        if snapshot is None:
            if (
                provider.consecutive_failures >= 10
                and not self._usage_degraded_logged
            ):
                self._usage_degraded_logged = True
                self.log_event(
                    f"[{coder_name}] Usage API degraded (10 consecutive failures), "
                    "falling back to reactive rate-limit detection"
                )
            return True
        self._usage_degraded_logged = False
        session_threshold = self.app_config.daemon.rate_limit_session_pause_percent
        weekly_threshold = self.app_config.daemon.rate_limit_weekly_pause_percent
        breached = None
        resets_at = 0
        if snapshot.session_percent >= session_threshold:
            breached = "session"
            resets_at = snapshot.session_resets_at
        elif snapshot.weekly_percent >= weekly_threshold:
            breached = "weekly"
            resets_at = snapshot.weekly_resets_at
        if breached is None:
            return True
        self.state.rate_limited_until = datetime.fromtimestamp(resets_at, tz=timezone.utc)
        self.state.rate_limit_reactive_coder = coder_name
        # Only preserve error_message when pausing from ERROR state so
        # handle_paused correctly resumes to ERROR; clear stale error
        # context from non-ERROR states to avoid incorrect ERROR resume.
        if self.state.state != PipelineState.ERROR:
            self.state.error_message = None
        self.state.state = PipelineState.PAUSED
        self.log_event(
            f"[{coder_name}] Proactive pause: {breached} usage at "
            f"{snapshot.session_percent if breached == 'session' else snapshot.weekly_percent}%, "
            f"resumes at {self.state.rate_limited_until.isoformat()}"
        )
        return False

    async def _check_rate_limit(self, proactive_coder: str | None = None) -> bool:
        """Return True if CLI calls are allowed, False if rate-limited.

        *proactive_coder* is forwarded to ``_proactive_usage_check`` so
        callers that always invoke a specific CLI can check the right quota.
        """
        coder = self.repo_config.coder or self.app_config.daemon.coder
        effective_coder = proactive_coder or coder.value
        if self.state.rate_limited_until is not None:
            # Legacy pauses (pre-PR-066) have no coder attribution;
            # treat them as Claude since that was the only coder.
            pause_coder = self.state.rate_limit_reactive_coder or "claude"
            # Diagnosis pauses always use Claude — honour regardless of
            # the repo's configured coder.
            diagnosis_pause = (
                self.state.error_message is not None
                and pause_coder == "claude"
            )
            # A pause from a *different* effective coder doesn't apply.
            # When proactive_coder is set (e.g. "claude" for merge/diagnosis),
            # only pauses matching that coder block; otherwise the repo's
            # configured coder is used.
            other_coder = (
                not diagnosis_pause
                and pause_coder != effective_coder
            )
            clearable = other_coder
            if clearable:
                self.state.rate_limited_until = None
                self.state.rate_limit_reactive = False
                self.state.rate_limit_reactive_coder = None
                self._claude_usage_provider.invalidate_cache()
                self._codex_usage_provider.invalidate_cache()
                if self.state.state == PipelineState.PAUSED:
                    if (
                        self.state.current_pr is not None
                        and self.state.current_task is not None
                        and self.state.current_pr.branch == self.state.current_task.branch
                    ):
                        self.state.state = PipelineState.WATCH
                    else:
                        self.state.state = PipelineState.IDLE
                self.log_event(
                    f"{coder.value.capitalize()} active, clearing other-coder rate-limit pause"
                )
                # Fall through so the proactive usage check still runs
                # to avoid launching work that exceeds usage thresholds.
            elif datetime.now(timezone.utc) < self.state.rate_limited_until:
                if self.state.state != PipelineState.PAUSED:
                    self.state.state = PipelineState.PAUSED
                remaining = (self.state.rate_limited_until - datetime.now(timezone.utc)).total_seconds()
                self.log_event(f"Rate limited, resuming in {int(remaining)}s")
                return False
            else:
                self.state.rate_limited_until = None
                self.state.rate_limit_reactive = False
                self.state.rate_limit_reactive_coder = None
                self._claude_usage_provider.invalidate_cache()
                self._codex_usage_provider.invalidate_cache()
                self.log_event("Rate limit window expired, resuming")
        return await self._proactive_usage_check(proactive_coder=proactive_coder)

    def _detect_rate_limit(self, stderr: str, coder_name: str | None = None) -> None:
        """Set rate-limit pause if stderr contains rate-limit signals.

        ``coder_name`` identifies the CLI that produced *stderr*.  When
        omitted the configured coder is used, but callers that always
        invoke a specific CLI (e.g. ``handle_error`` → ``claude_cli``)
        should pass the name explicitly so reactive pauses are attributed
        to the correct provider.
        """
        if coder_name is None:
            coder = self.repo_config.coder or self.app_config.daemon.coder
            coder_name = coder.value
        session_threshold = self.app_config.daemon.rate_limit_session_pause_percent
        weekly_threshold = self.app_config.daemon.rate_limit_weekly_pause_percent
        lower = stderr.lower()
        triggered = False
        limit_type = "session"
        pause_min = 30

        if re.search(r"\b429\b", stderr):
            triggered = True
            limit_type = "session"

        # Anthropic percentage-based pattern (Claude only)
        m_anthropic = re.search(
            r"(\d{1,3})%\s*(?:of\s+)?(?:your\s+)?(?:(weekly|week|session|5-hour)\s+)?rate\s*limit"
            r"|(?:(weekly|week|session|5-hour)\s+)?rate\s*limit\s+(?:at\s+)?(\d{1,3})%",
            lower,
        )
        if not triggered and m_anthropic and coder_name == "claude":
            pct = int(m_anthropic.group(1) or m_anthropic.group(4))
            qualifier = m_anthropic.group(2) or m_anthropic.group(3) or ""
            if qualifier in ("weekly", "week"):
                limit_type = "weekly"
                triggered = pct >= weekly_threshold
            else:
                limit_type = "session"
                triggered = pct >= session_threshold

        # Codex "try again in X days Y hours Z minutes/seconds" pattern
        m_codex_retry = re.search(
            r"try again in\s+"
            r"(?:(\d+)\s*days?)?\s*"
            r"(?:(\d+)\s*hours?)?\s*"
            r"(?:(\d+)\s*minutes?)?\s*"
            r"(?:(\d+(?:\.\d+)?)\s*(?:seconds?|secs?|s))?",
            lower,
        )
        codex_retry_parsed = False
        if not triggered and m_codex_retry and coder_name == "codex":
            days = int(m_codex_retry.group(1) or 0)
            hours = int(m_codex_retry.group(2) or 0)
            minutes = int(m_codex_retry.group(3) or 0)
            seconds = float(m_codex_retry.group(4) or 0)
            total_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
            if total_seconds > 0:
                codex_retry_parsed = True
                triggered = True
                pause_min = max(1, math.ceil(total_seconds / 60))
                limit_type = "weekly" if days > 0 or hours > 12 else "session"

        # Codex "You've hit your usage limit"
        if not triggered and "you've hit your usage limit" in lower:
            triggered = True
            limit_type = "session"

        # Generic "rate limit" fallback for non-Codex stderr only.
        anthropic_handled = m_anthropic and coder_name == "claude"
        codex_retry_handled = codex_retry_parsed
        if (
            not triggered
            and not anthropic_handled
            and not codex_retry_handled
            and "rate limit" in lower
            and coder_name != "codex"
        ):
            if "weekly" in lower or "week" in lower:
                limit_type = "weekly"
            triggered = True

        # Codex "usage limit" fallback (without "try again")
        if not triggered and "usage limit" in lower:
            limit_type = "session"
            triggered = True

        if triggered:
            self.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=pause_min)
            self.state.rate_limit_reactive = True
            self.state.rate_limit_reactive_coder = coder_name
            self.log_event(
                f"[{coder_name}] Rate limit detected ({limit_type}), "
                f"pausing for {pause_min} min"
            )
