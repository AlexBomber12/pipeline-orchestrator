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
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.daemon.notifications import send_spend_ceiling_warning
from src.models import PipelineState
from src.usage import UsageSnapshot


@dataclass(frozen=True)
class PauseVerdict:
    """Pure pause decision for one coder at one instant."""

    coder_name: str
    until: datetime | None
    active: bool
    expired: bool


@dataclass(frozen=True)
class GlobalPauseVerdict:
    """Pure decision for the process-wide pause marker."""

    pause_coder: str
    until: datetime
    active: bool
    expired: bool
    diagnosis_pause: bool
    legacy_pause: bool


RATE_LIMIT_BRANCH_MAP = (
    "effective-coder: proactive_coder overrides repo and selector coder",
    "effective-coder: repo_config.coder overrides selector coder",
    "effective-coder: selector coder is used when repo coder is unset",
    "effective-pause: no per-coder pause is present",
    "effective-pause: per-coder pause is active",
    "effective-pause: per-coder pause is expired and clearable",
    "global-pause: no process-wide pause is present",
    "legacy-pause: process-wide pause without coder attribution maps to claude",
    "global-pause: attributed process-wide pause uses its recorded coder",
    "global-pause: per-coder window overrides process-wide until for pause coder",
    "global-pause: process-wide until is used when no per-coder window exists",
    "diagnosis-pause: claude pause with error_message blocks any effective coder",
    "global-expired: expired process-wide pause clears pause coder metadata",
    "global-expired: legacy process-wide fields are cleared when attribution is absent",
    "global-expired: provider caches are invalidated before fallback",
    "global-expired: active effective-coder pause is reapplied",
    "global-expired: no effective-coder pause falls through to proactive check",
    "cross-coder: diagnosis pauses are not clearable",
    "cross-coder: matching pause coder is not clearable",
    "cross-coder: other coder pause is clearable",
    "cross-coder: clearable pause still blocks when effective coder has active pause",
    "cross-coder: paused state returns to WATCH when current PR branch matches task",
    "cross-coder: paused state returns to IDLE without matching watch context",
    "cross-coder: non-PAUSED state is preserved while clearing other coder pause",
    "cross-coder: legacy other-coder pause is copied into per-coder metadata",
    "cross-coder: existing per-coder metadata is preserved",
    "cross-coder: process-wide pause fields are cleared",
    "cross-coder: provider caches are invalidated before fallback",
    "cross-coder: clearable pause falls through to proactive check",
    "active-global: active matching pause transitions state to PAUSED",
    "active-global: already PAUSED state remains PAUSED",
    "active-global: active matching pause logs remaining seconds",
    "effective-only: active effective-coder pause without global pause is applied",
    "effective-only: effective-coder pause sets process-wide until",
    "effective-only: effective-coder pause sets reactive coder attribution",
    "effective-only: effective-coder pause transitions state to PAUSED",
    "no-pause: no active pause falls through to proactive usage check",
)


class RateLimitMixin:
    """Rate-limit detection and proactive usage checks."""

    async def _fetch_usage_snapshot(self, coder_name: str) -> UsageSnapshot | None:
        provider = (
            self._claude_usage_provider
            if coder_name == "claude"
            else self._codex_usage_provider
        )
        return await asyncio.to_thread(provider.fetch)

    async def _check_spend_ceiling(self, coder_name: str) -> bool:
        """Return True if dispatch may proceed under configured spend ceilings."""
        config = self.app_config.daemon
        session_cap = config.spend_ceiling_session_percent
        weekly_cap = config.spend_ceiling_weekly_percent
        if session_cap is None and weekly_cap is None:
            return True

        snapshot = await self._fetch_usage_snapshot(coder_name)
        if snapshot is None:
            return True

        if (
            session_cap is not None
            and snapshot.session_percent >= session_cap
        ):
            await self._enter_spend_ceiling_paused(
                coder_name=coder_name,
                limit_kind="session",
                current_percent=snapshot.session_percent,
                cap_percent=session_cap,
                resets_at=snapshot.session_resets_at,
            )
            return False
        if weekly_cap is not None and snapshot.weekly_percent >= weekly_cap:
            await self._enter_spend_ceiling_paused(
                coder_name=coder_name,
                limit_kind="weekly",
                current_percent=snapshot.weekly_percent,
                cap_percent=weekly_cap,
                resets_at=snapshot.weekly_resets_at,
            )
            return False

        await self._maybe_send_ceiling_warning(coder_name, snapshot)
        return True

    async def _enter_spend_ceiling_paused(
        self,
        *,
        coder_name: str,
        limit_kind: str,
        current_percent: int,
        cap_percent: int,
        resets_at: int,
    ) -> None:
        """Transition to PAUSED using the same reset machinery as rate limits."""
        until = datetime.fromtimestamp(resets_at, tz=timezone.utc)
        self._record_rate_limit(coder_name, until, reactive=False)
        self.state.error_message = None
        self.state.state = PipelineState.PAUSED
        self.log_event(
            f"[RATE-LIMIT] [SPEND-CEILING] {coder_name} {limit_kind} cap reached "
            f"({current_percent}%/{cap_percent}%); paused until reset."
        )
        await self.publish_state()

    async def _maybe_send_ceiling_warning(
        self,
        coder_name: str,
        snapshot: UsageSnapshot,
    ) -> None:
        """Send a best-effort warning once per coder/kind/reset window."""
        config = self.app_config.daemon
        warning_pct = config.spend_ceiling_warning_percent
        webhook_url = config.guardrail_notification_webhook_url
        if not webhook_url:
            return

        for kind, current, cap, resets_at in (
            (
                "session",
                snapshot.session_percent,
                config.spend_ceiling_session_percent,
                snapshot.session_resets_at,
            ),
            (
                "weekly",
                snapshot.weekly_percent,
                config.spend_ceiling_weekly_percent,
                snapshot.weekly_resets_at,
            ),
        ):
            if cap is None:
                continue
            if current * 100 < cap * warning_pct:
                continue
            dedup_key = f"warn:spend_ceiling:{coder_name}:{kind}:{resets_at}"
            ttl_seconds = max(60, resets_at - int(time.time()))
            try:
                ok = await self.redis.set(
                    dedup_key,
                    "1",
                    ex=ttl_seconds,
                    nx=True,
                )
            except Exception:
                ok = False
            if not ok:
                continue
            try:
                await send_spend_ceiling_warning(
                    webhook_url=webhook_url,
                    coder_name=coder_name,
                    limit_kind=kind,
                    current_percent=current,
                    cap_percent=cap,
                    warning_percent=warning_pct,
                    timeout_seconds=config.guardrail_notification_timeout_seconds,
                )
            except Exception as exc:
                try:
                    await self.redis.delete(dedup_key)
                except Exception as cleanup_exc:
                    self.log_event(
                        "[RATE-LIMIT] [SPEND-CEILING] warn dedup cleanup failed: "
                        f"{cleanup_exc}"
                    )
                self.log_event(
                    f"[RATE-LIMIT] [SPEND-CEILING] warn notification failed: {exc}"
                )

    def _record_rate_limit(
        self,
        coder_name: str,
        until: datetime,
        *,
        reactive: bool,
    ) -> None:
        self.state.rate_limited_until = until
        self.state.rate_limit_reactive = reactive
        self.state.rate_limit_reactive_coder = coder_name
        self.state.rate_limited_coders.add(coder_name)
        self.state.rate_limited_coder_until[coder_name] = until

    def _clear_rate_limit(self, coder_name: str) -> None:
        self.state.rate_limited_coders.discard(coder_name)
        self.state.rate_limited_coder_until.pop(coder_name, None)
        if self.state.rate_limit_reactive_coder == coder_name:
            self.state.rate_limited_until = None
            self.state.rate_limit_reactive = False
            self.state.rate_limit_reactive_coder = None

    def _rate_limit_until_for(self, coder_name: str) -> datetime | None:
        until = self.state.rate_limited_coder_until.get(coder_name)
        if until is not None:
            return until
        if self.state.rate_limit_reactive_coder == coder_name:
            return self.state.rate_limited_until
        return None

    def _effective_rate_limit_coder(self, proactive_coder: str | None) -> str:
        if proactive_coder is not None:
            return proactive_coder
        if self.repo_config.coder is not None:
            return self.repo_config.coder.value
        return self._get_coder()[0]

    def _legacy_pause_active(self, now: datetime) -> bool:
        until = self.state.rate_limited_until
        return (
            until is not None
            and self.state.rate_limit_reactive_coder is None
            and now < until
        )

    def _effective_coder_pause(self, coder_name: str, now: datetime) -> PauseVerdict:
        until = self._rate_limit_until_for(coder_name)
        if until is None:
            return PauseVerdict(
                coder_name=coder_name,
                until=None,
                active=False,
                expired=False,
            )
        return PauseVerdict(
            coder_name=coder_name,
            until=until,
            active=now < until,
            expired=now >= until,
        )

    def _global_pause_verdict(self, now: datetime) -> GlobalPauseVerdict | None:
        if self.state.rate_limited_until is None:
            return None
        # Legacy pauses (pre-PR-066) have no coder attribution; treat them
        # as Claude since that was the only coder.
        pause_coder = self.state.rate_limit_reactive_coder or "claude"
        pause_until = (
            self._rate_limit_until_for(pause_coder)
            or self.state.rate_limited_until
        )
        diagnosis_pause = (
            self.state.error_message is not None
            and pause_coder == "claude"
        )
        return GlobalPauseVerdict(
            pause_coder=pause_coder,
            until=pause_until,
            active=now < pause_until,
            expired=now >= pause_until,
            diagnosis_pause=diagnosis_pause,
            legacy_pause=self.state.rate_limit_reactive_coder is None,
        )

    def _cross_coder_clearable(
        self,
        effective_coder: str,
        pause_coder: str,
        now: datetime,
        *,
        diagnosis_pause: bool,
    ) -> bool:
        del now
        return not diagnosis_pause and pause_coder != effective_coder

    def _apply_pause_state(
        self,
        *,
        coder_name: str | None,
        until: datetime,
        now: datetime,
        update_global: bool,
    ) -> None:
        if update_global:
            self.state.rate_limited_until = until
            self.state.rate_limit_reactive_coder = coder_name
        if self.state.state != PipelineState.PAUSED:
            self.state.state = PipelineState.PAUSED
        remaining = (until - now).total_seconds()
        self.log_event(
            f"[RATE-LIMIT] Rate limited, resuming in "
            f"{int(remaining)}s."
        )

    def _restore_state_after_cross_coder_clear(self) -> None:
        if self.state.state != PipelineState.PAUSED:
            return
        if (
            self.state.current_pr is not None
            and self.state.current_task is not None
            and self.state.current_pr.branch == self.state.current_task.branch
        ):
            self.state.state = PipelineState.WATCH
        else:
            self.state.state = PipelineState.IDLE

    def _preserve_pause_coder_window(
        self,
        pause_coder: str,
        pause_until: datetime,
    ) -> None:
        if pause_coder not in self.state.rate_limited_coder_until:
            self.state.rate_limited_coders.add(pause_coder)
            self.state.rate_limited_coder_until[pause_coder] = pause_until

    def _clear_global_pause_fields(self) -> None:
        self.state.rate_limited_until = None
        self.state.rate_limit_reactive = False
        self.state.rate_limit_reactive_coder = None

    def _invalidate_usage_caches(self) -> None:
        self._claude_usage_provider.invalidate_cache()
        self._codex_usage_provider.invalidate_cache()

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
                    f"[RATE-LIMIT] [{coder_name}] Usage API degraded "
                    f"(10 consecutive failures), falling back to "
                    f"reactive rate-limit detection."
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
        until = datetime.fromtimestamp(resets_at, tz=timezone.utc)
        self._record_rate_limit(coder_name, until, reactive=False)
        # Only preserve error_message when pausing from ERROR state so
        # handle_paused correctly resumes to ERROR; clear stale error
        # context from non-ERROR states to avoid incorrect ERROR resume.
        if self.state.state != PipelineState.ERROR:
            self.state.error_message = None
        self.state.state = PipelineState.PAUSED
        self.log_event(
            f"[RATE-LIMIT] [{coder_name}] Proactive pause: {breached} "
            f"usage at "
            f"{snapshot.session_percent if breached == 'session' else snapshot.weekly_percent}%, "
            f"resumes at {until.isoformat()}."
        )
        return False

    async def _check_rate_limit(self, proactive_coder: str | None = None) -> bool:
        """Return True if CLI calls are allowed, False if rate-limited.

        *proactive_coder* is forwarded to ``_proactive_usage_check`` so
        callers that always invoke a specific CLI can check the right quota.
        """
        effective_coder = self._effective_rate_limit_coder(proactive_coder)
        now = datetime.now(timezone.utc)
        effective_pause = self._effective_coder_pause(effective_coder, now)
        if effective_pause.expired:
            self._clear_rate_limit(effective_coder)
            effective_pause = self._effective_coder_pause(effective_coder, now)

        global_pause = self._global_pause_verdict(now)
        if global_pause is not None:
            if global_pause.expired:
                self._clear_rate_limit(global_pause.pause_coder)
                if self.state.rate_limit_reactive_coder is None:
                    self.state.rate_limited_until = None
                    self.state.rate_limit_reactive = False
                effective_pause = self._effective_coder_pause(effective_coder, now)
                self._invalidate_usage_caches()
                self.log_event(
                    "[RATE-LIMIT] Rate limit window expired, resuming."
                )
                if effective_pause.until is not None:
                    self._apply_pause_state(
                        coder_name=effective_coder,
                        until=effective_pause.until,
                        now=now,
                        update_global=True,
                    )
                    return False
                return await self._proactive_usage_check(proactive_coder=proactive_coder)

            # A pause from a *different* effective coder doesn't apply.
            # When proactive_coder is set (e.g. "claude" for merge/diagnosis),
            # only pauses matching that coder block; otherwise the repo's
            # configured coder is used.
            if self._cross_coder_clearable(
                effective_coder,
                global_pause.pause_coder,
                now,
                diagnosis_pause=global_pause.diagnosis_pause,
            ):
                if effective_pause.until is not None:
                    self._apply_pause_state(
                        coder_name=effective_coder,
                        until=effective_pause.until,
                        now=now,
                        update_global=True,
                    )
                    return False
                self._restore_state_after_cross_coder_clear()
                self._preserve_pause_coder_window(
                    global_pause.pause_coder,
                    global_pause.until,
                )
                self._clear_global_pause_fields()
                self._invalidate_usage_caches()
                self.log_event(
                    f"[RATE-LIMIT] {effective_coder.capitalize()} active "
                    f"while {global_pause.pause_coder} remains rate-limited until "
                    f"{global_pause.until.isoformat()}."
                )
                return await self._proactive_usage_check(proactive_coder=proactive_coder)

            if global_pause.active:
                self._apply_pause_state(
                    coder_name=None,
                    until=global_pause.until,
                    now=now,
                    update_global=False,
                )
                return False

        if effective_pause.until is not None:
            self._apply_pause_state(
                coder_name=effective_coder,
                until=effective_pause.until,
                now=now,
                update_global=True,
            )
            return False
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

        # Codex error fallback for unmatched rate-limit failures that still
        # carry concrete retry language, while ignoring progress-only stderr.
        if (
            not triggered
            and coder_name == "codex"
            and "rate limit" in lower
            and (
                "rate limit exceeded" in lower
                or "please try again" in lower
                or "try again later" in lower
                or "retry later" in lower
            )
        ):
            triggered = True
            limit_type = "weekly" if "weekly" in lower or "week" in lower else "session"

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
        if not triggered and "usage limit" in lower and coder_name != "codex":
            limit_type = "session"
            triggered = True

        if triggered:
            until = datetime.now(timezone.utc) + timedelta(minutes=pause_min)
            self._record_rate_limit(coder_name, until, reactive=True)
            self.log_event(
                f"[RATE-LIMIT] [{coder_name}] Rate limit detected "
                f"({limit_type}), pausing for {pause_min} min."
            )
