"""Per-repository pipeline state machine.

One ``PipelineRunner`` instance exists per connected repository. The daemon
main loop calls ``run_cycle`` once per poll interval; each cycle clones or
fetches the repo, runs a preflight check, and dispatches on the persisted
state (``IDLE``, ``WATCH``, ``PAUSED``, or ``ERROR``). Transient states
(``CODING``, ``FIX``, ``MERGE``) are resolved within a single cycle and
never persisted across cycles.

After PR-057 decomposition, handler logic lives in mixin classes under
``src.daemon.handlers.*`` and supporting modules (``git_ops``, ``recovery``,
``preflight``, ``rate_limit``, ``repo_ops``). ``PipelineRunner`` inherits
from all mixins and keeps only the core lifecycle methods here.

Mixin resolution order (left-to-right, less-dependent first):
    RecoveryMixin, PreflightMixin, RateLimitMixin, RepoOpsMixin,
    CodingMixin, WatchMixin, FixMixin (→BreachMixin), MergeMixin,
    ErrorMixin, HungMixin, IdleMixin
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from src.cancellation import (
    TTL_SECONDS as TASK_SPEC_HASH_TTL_SECONDS,
)
from src.cancellation import (
    CancellationCause,
    get_cancellation_cause,
    safe_record_cancellation_cause,
    truncate_for_payload,
)
from src.cancellation.availability import (
    ActiveHoursSource,
    AvailabilityState,
    HeartbeatSource,
    ManualOverrideSource,
    is_operator_available,
)
from src.coder_registry import CoderPlugin, CoderRegistry
from src.coders import build_coder_registry
from src.config import AppConfig, CoderType, RepoConfig, load_config
from src.daemon import (
    error_rate_tracker,
    git_ops,
    scaffolder,  # noqa: F401 — tests reference runner_module.scaffolder
)
from src.daemon.git_ops import _repo_looks_scaffolded, repo_owner_from_url
from src.daemon.github_rate_limit import (
    RateLimitBudget,
    clear_graphql_budget,
    clear_rest_budget,
    read_budget,
    record_cycle_burn,
    release_refresh_lock,
    try_claim_refresh_lock,
    write_budget,
    write_graphql_budget,
    write_rest_budget,
)
from src.daemon.handlers.coding import CodingMixin
from src.daemon.handlers.error import (  # noqa: F401 — re-exported for tests
    INFRA_ERROR_PATTERNS,
    ErrorCategory,
    ErrorMixin,
    _classify_error,
    _is_infra_error,
)
from src.daemon.handlers.fix import FixMixin
from src.daemon.handlers.hung import HungMixin
from src.daemon.handlers.idle import IdleMixin
from src.daemon.handlers.merge import MergeMixin
from src.daemon.handlers.watch import WatchMixin
from src.daemon.preflight import PreflightMixin
from src.daemon.rate_limit import RateLimitMixin
from src.daemon.recovery import RecoveryMixin
from src.daemon.recovery_policy import BoundedRecoveryPolicy
from src.daemon.repo_ops import RepoOpsMixin
from src.daemon.selector import (
    SelectionContext,
    select_auxiliary_coder,
    select_coder,
)
from src.events import publish_repo_event
from src.github import comments as gh_comments
from src.github import gh_runner
from src.github import prs as gh_prs
from src.github import rate_limit as gh_rate_limit
from src.keyspace import (
    cli_log_history,
    cli_log_latest,
    control_config_dirty,
    control_stop,
    legacy_recovered_tasks,
    pipeline_state,
    status_write_failed_tasks,
    upload_pending,
)
from src.metrics import MetricsStore, RunRecord
from src.models import PipelineState, RepoState
from src.queue_parser import (
    TYPE_SYNONYMS,
    QueueValidationError,
    parse_task_header,
    write_frontmatter_status,
)
from src.usage import UsageProvider
from src.utils import repo_slug_from_url

logger = logging.getLogger(__name__)

_TRANSIENT_STATES = {
    PipelineState.CODING,
    PipelineState.FIX,
    PipelineState.MERGE,
}

_EXIT_REASON_TO_RUN_OUTCOME: dict[str, tuple[str, str | None]] = {
    "success_merged": ("merged", None),
    "closed_unmerged": ("superseded", None),
    "coding_complete": ("superseded", None),
    "rate_limit": ("paused", None),
    "paused": ("paused", None),
    "stopped": ("paused", None),
    "cancelled": ("paused", None),
    "timeout": ("failed", "TIMEOUT"),
    "escalated": ("failed", "ESCALATE"),
    "error": ("failed", "CRASH"),
    "crash": ("failed", "CRASH"),
}

_HISTORY_LIMIT = 100
_STOP_POLL_INTERVAL_SEC = 0.5
_IDLE_STREAK_CAP = 100

# Sentinel for ``_escalate_and_skip``'s ``error_message_override``: when
# the caller passes the sentinel (the default), ``error_message`` is set
# to ``message``. ``None`` and any string value override that default.
_USE_MESSAGE_AS_ERROR: object = object()

# A ``PR #<number>`` token is a semantic identifier (different PRs are
# distinct events) and is preserved verbatim. The alternation tries the
# ``PR #N`` form first so the full PR id is consumed before generic ``\d+``
# can match its digits.
_PR_ID_OR_NUMERIC_RE = re.compile(r"PR #\d+|\d+")


def _normalize_for_dedup(event: str) -> str:
    """Return ``event`` with numeric runs replaced by ``#`` for fuzzy matching.

    ``PR #<number>`` PR-identifier tokens are preserved so that switching
    from one PR to another never collapses into a single history row.
    """
    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        return token if token.startswith("PR #") else "#"

    return _PR_ID_OR_NUMERIC_RE.sub(_replace, event)

# Timeout for ``scripts/ci.sh`` on the auto-commit path.
_CI_SCRIPT_TIMEOUT_SEC = 1800
_EXTENSION_LANGUAGE_MAP = {
    ".c": "c",
    ".cc": "c++",
    ".cpp": "c++",
    ".cs": "csharp",
    ".css": "css",
    ".go": "go",
    ".html": "html",
    ".java": "java",
    ".js": "javascript",
    ".jsx": "javascript",
    ".kt": "kotlin",
    ".md": "markdown",
    ".php": "php",
    ".py": "python",
    ".rb": "ruby",
    ".rs": "rust",
    ".sh": "shell",
    ".sql": "sql",
    ".swift": "swift",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".yml": "yaml",
    ".yaml": "yaml",
}


class PipelineRunner(
    RecoveryMixin,
    PreflightMixin,
    RateLimitMixin,
    RepoOpsMixin,
    CodingMixin,
    WatchMixin,
    FixMixin,
    MergeMixin,
    ErrorMixin,
    HungMixin,
    IdleMixin,
):
    """State machine for one repository."""

    def __init__(
        self,
        repo_config: RepoConfig,
        app_config: AppConfig,
        redis_client: aioredis.Redis,
        claude_usage_provider: UsageProvider,
        codex_usage_provider: UsageProvider,
        registry: CoderRegistry | None = None,
    ) -> None:
        self.repo_config = repo_config
        self._app_config = app_config
        self.redis = redis_client
        self._registry = registry or build_coder_registry()
        self.name = repo_slug_from_url(repo_config.url)
        self.owner_repo = repo_owner_from_url(repo_config.url)
        self.repo_path = f"/data/repos/{self.name}"
        # Migrate clone path from old basename-only format to owner__repo.
        old_basename = repo_config.url.rstrip("/").rsplit("/", 1)[-1]
        if old_basename.endswith(".git"):
            old_basename = old_basename[:-4]
        old_path = Path(f"/data/repos/{old_basename}")
        new_path = Path(self.repo_path)
        if old_basename != self.name and old_path.exists() and not new_path.exists():
            try:
                result = subprocess.run(
                    ["git", "-C", str(old_path), "remote", "get-url", "origin"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                old_origin = result.stdout.strip()
                if repo_slug_from_url(old_origin) != self.name:
                    logger.warning(
                        "Legacy clone %s has origin %s, expected %s — skipping migration",
                        old_path,
                        old_origin,
                        repo_config.url,
                    )
                else:
                    import shutil
                    shutil.move(str(old_path), str(new_path))
                    logger.info("Migrated clone path %s -> %s", old_path, new_path)
            except Exception:
                logger.warning("Could not verify origin for %s — skipping migration", old_path)
        if new_path.exists():
            if not (new_path / ".git").exists():
                logger.warning("Removing non-git directory %s", new_path)
                import shutil
                shutil.rmtree(new_path, ignore_errors=True)
            else:
                try:
                    result = subprocess.run(
                        ["git", "-C", str(new_path), "remote", "get-url", "origin"],
                        capture_output=True,
                        text=True,
                        timeout=5,
                        check=True,
                    )
                    current_origin = result.stdout.strip()
                    if repo_slug_from_url(current_origin) != self.name:
                        logger.warning(
                            "Clone %s has origin %s, expected %s — removing stale clone",
                            new_path,
                            current_origin,
                            repo_config.url,
                        )
                        import shutil
                        shutil.rmtree(new_path)
                except Exception:
                    logger.warning("Could not verify origin for %s", new_path)
        self._old_basename = old_basename
        self.state = RepoState(
            url=repo_config.url,
            name=self.name,
            last_updated=datetime.now(timezone.utc),
        )
        self._recovered = False
        self._scaffolded = _repo_looks_scaffolded(self.repo_path)
        self._consecutive_dirty_cycles = 0
        self._error_diagnose_count = 0
        self._error_skip_context: str | None = None
        self._error_skip_count = 0
        self._error_skip_active = False
        self._last_push_at: datetime | None = None
        self._last_push_at_pr_number: int | None = None
        self._last_codex_review_pr: int | None = None
        self._last_codex_review_head_sha: str | None = None
        self._queue_progress_dirty = False
        self._last_published_queue_progress: tuple[int, int] | None = None
        self._last_published_state_signature: (
            tuple[str, tuple, tuple] | None
        ) = None
        self._pending_event_log_entries: list[dict[str, object]] = []
        self._usage_degraded_logged = False
        self._claude_usage_provider = claude_usage_provider
        self._codex_usage_provider = codex_usage_provider
        self._metrics_store = MetricsStore(redis_client)
        self._current_run_record: RunRecord | None = None
        self._selector_rng = random.Random()
        self._auth_status_cache: dict[str, dict[str, str]] = {}
        self._auth_status_cache_expires_at: datetime | None = None
        self._current_coder_process: asyncio.subprocess.Process | None = None
        self._stop_requested = False
        self._user_stopped_task_pr_ids: set[str] = set()
        # PR-186: Tasks marked ERROR by recovery after a crash. The next
        # IDLE cycle treats these as ERROR and skips them so the same
        # task is not re-picked into a crash loop. Cleared when the user
        # re-uploads the task file.
        self._crashed_task_pr_ids: set[str] = set()
        # Tasks explicitly escalated/canceled where the best-effort
        # task-file status commit failed. Unlike crashed tasks, these
        # must stay parked even if their old PR remains visible.
        self._status_write_failed_task_pr_ids: set[str] = set()
        self._status_write_failed_task_pr_ids_persist_failed = False
        self._user_pause_logged = False
        self._pending_repo_config: RepoConfig | None = None
        self._pending_app_config: AppConfig | None = None
        self._pending_usage_providers: (
            tuple[UsageProvider, UsageProvider] | None
        ) = None
        # True iff the staged change is one that intentionally needs to
        # land on an IDLE boundary (currently: coder-only swaps mid-PR).
        # False marks staging that exists purely because a cycle was in
        # flight at reload time — those are safe to apply as soon as the
        # cycle drains, regardless of the resulting state. Without this
        # split, a non-coder change like ``active=False`` staged during
        # an in-flight cycle would be left pending forever if the cycle
        # finished in WATCH/FIX/MERGE/CODING.
        self._pending_requires_idle_boundary: bool = False
        # GitHub API rate-limit budget tracking. The budget is refreshed
        # at most once per minute; counters drive the BoundedRecoveryPolicy
        # transitions for the slowdown/pause threshold actions.
        self._github_api_budget_cache: RateLimitBudget | None = None
        self._github_api_budget_last_fetched: datetime | None = None
        self._github_api_pause_attempts = 0
        self._github_api_slowdown_attempts = 0
        self._github_api_slowdown_cycle = 0
        # PR-184: count consecutive cycles ending in IDLE with no PR in
        # flight. Once the streak reaches ``idle_extended_after_cycles``
        # the daemon main loop polls this runner on the slower
        # ``idle_extended_poll_interval_sec`` cadence; the streak is
        # cleared by either a Redis wake event or any state transition
        # out of the no-work IDLE shape.
        self._idle_streak = 0
        # Set by ``handle_idle`` when the cycle ended without a clean
        # IDLE verdict — either ``process_pending_uploads`` deferred
        # work to the next cycle (returning ``None``) or a GitHub read
        # (``get_open_prs``) raised, leaving queue/PR status unknown.
        # Suppresses the streak increment so a transient outage or an
        # outstanding upload retry is not folded into the slower
        # extended-idle cadence.
        self._idle_dispatch_deferred = False
        # PR-202: WATCH adaptive polling. ``_watch_entered_at`` is set
        # at the moment the state transitions into WATCH, either by
        # ``_run_cycle_body`` (handler-driven transitions) or by
        # ``recover_state`` (startup recovery). Anchoring at transition
        # time — not on the first WATCH cycle — ensures the daemon's
        # *next* poll interval already reflects the slow cadence. Each
        # WATCH cycle records the polled PR signature; when that
        # signature changes between cycles a real GitHub event arrived
        # and ``_watch_last_event_at`` advances. All three are cleared
        # by ``_reset_watch_polling`` on transition out of WATCH so a
        # stale anchor cannot leak into the next WATCH session.
        self._watch_entered_at: datetime | None = None
        self._watch_last_event_at: datetime | None = None
        self._watch_last_event_signature: tuple[Any, ...] | None = None
        self._github_api_pause_policy: BoundedRecoveryPolicy[
            "PipelineRunner"
        ] = BoundedRecoveryPolicy(
            name="github_api_pause",
            max_attempts=1,
            counter_getter=lambda r: r._github_api_pause_attempts,
            counter_setter=lambda r, n: setattr(r, "_github_api_pause_attempts", n),
            on_threshold=lambda r: r._enter_github_api_pause(),
        )
        self._github_api_slowdown_policy: BoundedRecoveryPolicy[
            "PipelineRunner"
        ] = BoundedRecoveryPolicy(
            name="github_api_slowdown",
            max_attempts=1,
            counter_getter=lambda r: r._github_api_slowdown_attempts,
            counter_setter=lambda r, n: setattr(
                r, "_github_api_slowdown_attempts", n
            ),
            on_threshold=lambda r: r._enter_github_api_slowdown(),
        )
        # PR-223: route the open-coded ``_error_skip_count`` and
        # ``_error_diagnose_count`` ceilings through ``BoundedRecoveryPolicy``
        # so all 5 threshold sites share one shape. ``max_attempts=4``
        # preserves the legacy ``count > 3`` semantic: ``maybe_escalate``
        # fires when the counter reaches ``max_attempts``, which after
        # increment from 3 to 4 matches the original gate.
        self._error_skip_policy: BoundedRecoveryPolicy[
            "PipelineRunner"
        ] = BoundedRecoveryPolicy(
            name="error_skip",
            max_attempts=4,
            counter_getter=lambda r: r._error_skip_count,
            counter_setter=lambda r, n: setattr(r, "_error_skip_count", n),
            on_threshold=lambda r: r._on_error_skip_threshold(),
        )
        self._error_diagnose_policy: BoundedRecoveryPolicy[
            "PipelineRunner"
        ] = BoundedRecoveryPolicy(
            name="error_diagnose",
            max_attempts=4,
            counter_getter=lambda r: r._error_diagnose_count,
            counter_setter=lambda r, n: setattr(r, "_error_diagnose_count", n),
            on_threshold=lambda r: r._on_error_diagnose_threshold(),
        )

    def _on_error_skip_threshold(self) -> None:
        """Log when the soft-skip ceiling fires; stays ERROR.

        The dynamic ``Skipping AI diagnosis: <Coder> rate limited`` line
        is logged inline by ``handle_error`` before increment, so the
        threshold callback only owns the ceiling message asserted by
        the PR-210 baseline.
        """
        self.log_event(
            "[ERROR] max soft-skip retries (3) reached, staying ERROR."
        )

    def _on_error_diagnose_threshold(self) -> None:
        """Log when the diagnose-attempt ceiling fires; stays ERROR."""
        self.log_event(
            "[ERROR] diagnose_error: max attempts (3) reached, staying ERROR."
        )

    @property
    def app_config(self) -> AppConfig:
        return self._app_config

    @app_config.setter
    def app_config(self, value: AppConfig) -> None:
        self._app_config = value

    def set_usage_providers(
        self,
        claude_usage_provider: UsageProvider,
        codex_usage_provider: UsageProvider,
    ) -> None:
        """Swap in the shared daemon-level usage providers."""
        self._claude_usage_provider = claude_usage_provider
        self._codex_usage_provider = codex_usage_provider

    def _availability_sources(self) -> list[Any]:
        cfg = self.app_config.daemon
        return [
            ManualOverrideSource(redis_client=self.redis),
            HeartbeatSource(redis_client=self.redis),
            ActiveHoursSource(
                start_hour=cfg.operator_active_hours_start,
                end_hour=cfg.operator_active_hours_end,
                timezone_name=cfg.operator_timezone,
            ),
        ]

    async def _auto_pause_repo(self, reason: str) -> None:
        """Pause the repo until an operator explicitly resumes it."""
        await error_rate_tracker.mark_auto_pause(self.redis, self.name)
        self.state.state = PipelineState.PAUSED
        self.state.user_paused = True
        await self._clear_error_message_on_recovery(
            log_prefix="[AUTO-PAUSE]",
            reason="error-rate threshold auto-pause",
        )
        self.log_event(
            f"[AUTO-PAUSE] ERROR rate exceeded threshold; manual Resume "
            f"required ({reason})."
        )
        await self.redis.set(pipeline_state(self.name), self.state.model_dump_json())
        await self.publish_state()

    async def _maybe_auto_pause_for_error_rate(self) -> bool:
        """Return True when AVAILABLE-mode ERROR rate pauses this repo.

        AWAY mode intentionally absorbs individual skip-and-record
        failures without repo-level attention while still pruning the
        bounded ERROR-rate tracker; AVAILABLE mode converts a sustained
        ERROR burst into the same PAUSED state as a manual operator pause
        so Resume remains the only way forward.
        """
        cfg = self.app_config.daemon
        try:
            count = await error_rate_tracker.count_recent(
                self.redis,
                self.name,
                cfg.error_rate_window_min,
            )
        except Exception as exc:
            logger.warning(
                "Failed to read ERROR-rate tracker for %s: %s",
                self.name,
                exc,
            )
            return False
        if not cfg.error_rate_auto_pause_enabled:
            return False
        try:
            availability = await is_operator_available(self._availability_sources())
        except Exception:
            availability = AvailabilityState.AVAILABLE
        if availability is not AvailabilityState.AVAILABLE:
            return False
        if count < cfg.error_rate_threshold:
            return False
        if not await error_rate_tracker.has_records_after_last_auto_pause(
            self.redis,
            self.name,
        ):
            return False
        await self._auto_pause_repo(
            f"{count}/{cfg.error_rate_threshold} in "
            f"{cfg.error_rate_window_min}min"
        )
        return True

    def stage_config_reload(
        self,
        repo_config: RepoConfig,
        app_config: AppConfig,
        claude_usage_provider: UsageProvider,
        codex_usage_provider: UsageProvider,
        *,
        requires_idle_boundary: bool = False,
    ) -> None:
        """Queue config changes to apply at the next safe task-pickup boundary.

        ``requires_idle_boundary=True`` means the change must wait for an
        IDLE-equivalent state before swapping (e.g. a mid-PR coder swap).
        ``False`` means staging is just an in-flight-cycle precaution and
        the post-cycle drain may apply the change regardless of state.

        The IDLE-boundary requirement is sticky across overlapping calls:
        if a prior call within the same staging window flagged the swap as
        coder-sensitive, a later ``False`` reload (e.g. an unrelated
        daemon-setting change reloaded while the cycle is still in flight)
        must not downgrade the deferral. The flag is cleared only when the
        staged config is consumed via ``_apply_staged_config_reload`` or
        dropped via ``clear_staged_config_reload``.
        """
        self._pending_repo_config = repo_config
        self._pending_app_config = app_config
        self._pending_usage_providers = (
            claude_usage_provider,
            codex_usage_provider,
        )
        self._pending_requires_idle_boundary = (
            self._pending_requires_idle_boundary or requires_idle_boundary
        )

    def _reset_runner_local_task_counters(self) -> None:
        """Reset PipelineRunner-private counters tied to the active task.

        Call this from every code path that clears
        ``state.current_task``. The ``RepoState.__setattr__`` hook
        already releases ``current_pr`` and ``error_message`` when the
        triggering write is ``current_task = None``; this helper covers
        the runner-instance fields that the persisted state does not
        carry: the SKIP/diagnose retry counters and the IDLE soft-defer
        flag. Keeping them on the runner avoids either bloating the
        persisted RepoState blob or holding a runner reference inside
        the Pydantic model.

        After PR-218, no production code path may build a "task clear"
        from ``state.current_task = None`` plus a hand-rolled set of
        field resets. Use this helper next to the assignment so the
        recovery.py:371-375 superset stays the universal contract.
        """
        self._error_skip_active = False
        self._error_skip_policy.reset(self)
        self._error_skip_context = None
        self._error_diagnose_policy.reset(self)
        self._idle_dispatch_deferred = False

    def _build_usage_providers_for_app_config(
        self,
        app_config: AppConfig,
    ) -> tuple[UsageProvider, UsageProvider]:
        """Rebuild shared usage providers from the active config snapshot."""
        claude_provider = self._registry.get("claude").create_usage_provider(
            config=app_config
        )
        codex_provider = self._registry.get("codex").create_usage_provider(
            config=app_config
        )
        return (
            claude_provider or self._claude_usage_provider,
            codex_provider or self._codex_usage_provider,
        )

    def _apply_staged_config_reload(self) -> None:
        """Apply any queued config changes now that the runner is safe to swap."""
        if self._pending_repo_config is None or self._pending_app_config is None:
            return
        self.repo_config = self._pending_repo_config
        self.app_config = self._pending_app_config
        if self._pending_usage_providers is not None:
            self.set_usage_providers(*self._pending_usage_providers)
        self.clear_staged_config_reload()

    def clear_staged_config_reload(self) -> None:
        """Drop any queued config swap once a newer config is in effect."""
        self._pending_repo_config = None
        self._pending_app_config = None
        self._pending_usage_providers = None
        self._pending_requires_idle_boundary = False

    async def reload_repo_config_if_dirty(self) -> None:
        """Hot-reload repo config at the idle boundary when flagged by the web UI."""
        dirty_key = control_config_dirty(self.name)
        dirty_exists = False
        try:
            if hasattr(self.redis, "exists"):
                dirty_exists = bool(await self.redis.exists(dirty_key))
            elif hasattr(self.redis, "get"):
                dirty_exists = (await self.redis.get(dirty_key)) is not None
        except RedisError as exc:
            logger.warning(
                "Skipping config dirty check for %s while Redis is unavailable: %s",
                self.name,
                exc,
            )
            self._apply_staged_config_reload()
            return
        if not dirty_exists:
            self._apply_staged_config_reload()
            return

        config = load_config()
        for repo in config.repositories:
            if repo_slug_from_url(repo.url) == self.name:
                self.repo_config = repo
                self.app_config = config
                self.set_usage_providers(
                    *self._build_usage_providers_for_app_config(config)
                )
                self.clear_staged_config_reload()
                await self.redis.delete(dirty_key)
                self.log_event(
                    "[INFRA] Reloaded repo config from config.yml."
                )
                return
        await self.redis.delete(dirty_key)
        self._apply_staged_config_reload()

    def _active_task_coder_pin(self) -> str | None:
        """Return the active task's ``Coder:`` header value if parseable.

        Treat ``ValueError`` (which covers ``UnicodeDecodeError`` from a
        non-UTF-8 task file) the same as ``OSError``: the pin lookup is
        best-effort, and any read or decode failure must degrade to "no
        pin" rather than escape the selector. Otherwise the same bad bytes
        that ``handle_coding`` is now guarded against would crash here
        before the handler ever runs.
        """
        task = self.state.current_task
        if task is None or not task.task_file:
            return None
        task_path = Path(self.repo_path) / task.task_file
        try:
            header = parse_task_header(task_path)
        except (QueueValidationError, OSError, ValueError):
            return None
        return header.coder

    def _select_coder(
        self, *, allow_exploration: bool = True
    ) -> tuple[str, CoderPlugin] | None:
        """Return the active selector choice without default fallback."""
        app_config = self.app_config
        if not allow_exploration and app_config.daemon.exploration_epsilon != 0:
            daemon_config = app_config.daemon.model_copy(
                update={"exploration_epsilon": 0.0}
            )
            app_config = app_config.model_copy(update={"daemon": daemon_config})
        ctx = SelectionContext(
            registry=self._registry,
            repo_config=self.repo_config,
            app_config=app_config,
            state=self.state,
            rng=self._selector_rng,
            auth_statuses=self._auth_status_cache or None,
            task_coder_pin=self._active_task_coder_pin(),
        )
        return select_coder(ctx)

    def _select_auxiliary_coder(self) -> tuple[str, CoderPlugin] | None:
        """Return the best eligible coder for daemon helper workflows."""
        ctx = SelectionContext(
            registry=self._registry,
            repo_config=self.repo_config,
            app_config=self.app_config,
            state=self.state,
            rng=self._selector_rng,
            auth_statuses=self._auth_status_cache or None,
        )
        return select_auxiliary_coder(ctx)

    def _get_coder(
        self, *, allow_exploration: bool = True
    ) -> tuple[str, CoderPlugin]:
        """Return ``(coder_name, coder_plugin)`` for the active coder.

        When the active task pins a specific coder via ``Coder:`` and no
        eligible coder is available, fall back to the pinned coder rather
        than the repo/global default to preserve the hard-pin guarantee.
        """
        result = self._select_coder(allow_exploration=allow_exploration)
        if result is not None:
            self.state.coder = result[0]
            return result
        pin = self._active_task_coder_pin()
        if pin in ("claude", "codex"):
            self.state.coder = pin
            return pin, self._registry.get(pin)
        coder = self.repo_config.coder or self.app_config.daemon.coder
        coder_name = coder.value if isinstance(coder, CoderType) else str(coder)
        self.state.coder = coder_name
        return coder_name, self._registry.get(coder_name)

    def _get_auxiliary_coder(self) -> tuple[str, CoderPlugin] | None:
        """Return the eligible coder for diagnosis/merge helper workflows."""
        result = self._select_auxiliary_coder()
        if result is not None:
            self.state.coder = result[0]
        return result

    async def _refresh_auth_status_cache(self) -> None:
        """Refresh cached coder auth state off the event loop."""
        now = datetime.now(timezone.utc)
        if (
            self._auth_status_cache
            and self._auth_status_cache_expires_at is not None
            and now < self._auth_status_cache_expires_at
        ):
            return

        def _probe() -> dict[str, dict[str, str]]:
            statuses: dict[str, dict[str, str]] = {}
            for name in self._registry.coder_names():
                try:
                    statuses[name] = self._registry.get(name).check_auth()
                except Exception:
                    statuses[name] = {"status": "error"}
            return statuses

        self._auth_status_cache = await asyncio.to_thread(_probe)
        self._auth_status_cache_expires_at = now + timedelta(minutes=5)

    def _load_current_task_metadata(self) -> tuple[str, str]:
        """Return ``(task_type, complexity)`` for the active task if available."""
        task = self.state.current_task
        if task is None or not task.task_file:
            return ("unknown", "unknown")
        task_path = Path(self.repo_path) / task.task_file
        try:
            lines = task_path.read_text(encoding="utf-8").splitlines()
        except (OSError, ValueError):
            # ``ValueError`` covers ``UnicodeDecodeError`` from a non-UTF-8
            # task file. The metadata lookup is best-effort, so degrade to
            # ``("unknown", "unknown")`` rather than crash the cycle — the
            # handler-side body read is the canonical failure surface for
            # decode errors and routes through ``_transition_to_error``.
            return ("unknown", "unknown")

        task_type = "unknown"
        complexity = "unknown"
        for raw_line in lines:
            line = raw_line.strip()
            lower = line.lower()
            if lower.startswith("- type:"):
                value = line.split(":", 1)[1].strip()
                if value:
                    task_type = TYPE_SYNONYMS.get(value, value)
            elif lower.startswith("- complexity:"):
                value = line.split(":", 1)[1].strip()
                if value:
                    complexity = value
        return (task_type, complexity)

    def _start_current_run_record(self, coder_name: str, model: str) -> None:
        """Initialize the in-memory run record for the current CODING pass."""
        task = self.state.current_task
        if task is None:
            self._current_run_record = None
            return
        task_type, complexity = self._load_current_task_metadata()
        self._current_run_record = RunRecord(
            run_id=str(uuid.uuid4()),
            task_id=task.pr_id,
            profile_id=f"{coder_name}:{model}:container",
            task_type=task_type,
            complexity=complexity,
            started_at=datetime.now(timezone.utc).isoformat(),
            ended_at=None,
            duration_ms=None,
            fix_iterations=0,
            tokens_in=0,
            tokens_out=0,
            exit_reason="",
            operator_intervention=False,
            outcome="failed",
            cause="CRASH",
            run_phase="coding",
            coder_session_id=str(uuid.uuid4()),
            repo_name=self.name,
            stage="coder",
        )

    def _git_rev_parse(self, ref: str) -> str:
        try:
            result = subprocess.run(
                ["git", "rev-parse", ref],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
        except (subprocess.SubprocessError, OSError):
            return ""
        if result.returncode != 0:
            return ""
        return result.stdout.strip()

    def _canonical_push_timestamp(self, pr_number: int) -> datetime:
        """Return a monotonic baseline timestamp for ``_last_push_at``.

        Reads the GitHub committer date of the PR head via
        ``gh_prs.get_pr_metadata``. Centralizes writes to
        ``_last_push_at`` so daemon wall clock drift cannot push the
        baseline past a Codex review submitted seconds after the actual
        git push (PR #407 incident).

        To guard against rebases or cherry-picks where the committer
        date is older than the actual push event, the result never
        moves below the existing baseline for the same PR — preventing
        old Codex comments from re-appearing as "new" feedback (Codex
        P1 on PR #408).

        Falls back to ``datetime.now(timezone.utc)`` only when no
        canonical value AND no prior baseline are available, preserving
        liveness of the WATCH cycle when GitHub is unreachable on the
        first observation.
        """
        previous: datetime | None = None
        if (
            self._last_push_at is not None
            and self._last_push_at_pr_number == pr_number
        ):
            previous = self._last_push_at
        try:
            metadata = gh_prs.get_pr_metadata(self.owner_repo, pr_number)
            head_iso = metadata.get("head_commit_date", "")
        except Exception:
            head_iso = ""
        head_time = gh_runner._parse_iso(head_iso) if head_iso else None
        if head_time is not None and head_time.tzinfo is None:
            head_time = head_time.replace(tzinfo=timezone.utc)
        if head_time is None:
            return previous if previous is not None else datetime.now(timezone.utc)
        if previous is not None and head_time < previous:
            return previous
        return head_time

    @staticmethod
    def _attempt_count_key(repo_name: str, task_id: str) -> str:
        return f"metrics:attempt_count:{repo_name}:{task_id}"

    async def _prepare_current_run_record_dispatch_metadata(
        self,
        *,
        task_hash: str,
        previous_task_hash: str | None,
    ) -> None:
        """Populate dispatch-time metadata on the active run record."""
        record = self._current_run_record
        task = self.state.current_task
        if record is None or task is None:
            return
        key = self._attempt_count_key(self.name, task.pr_id)
        if previous_task_hash is not None and previous_task_hash != task_hash:
            try:
                await self.redis.delete(key)
            except Exception:
                pass
        try:
            raw = await self.redis.get(key)
            current = int(raw) if raw is not None else 0
        except Exception:
            current = 0
        next_attempt = current + 1
        try:
            await self.redis.set(key, str(next_attempt), ex=TASK_SPEC_HASH_TTL_SECONDS)
        except Exception:
            pass
        record.attempt_index = next_attempt
        record.task_spec_hash = task_hash
        record.base_sha = self._git_rev_parse(
            f"origin/{self.repo_config.branch or 'main'}"
        )

    @staticmethod
    def _ext_to_language(path: str) -> str | None:
        """Map a file extension to a stable language label for metrics."""
        suffix = Path(path).suffix.lower()
        return _EXTENSION_LANGUAGE_MAP.get(suffix)

    def _set_queue_progress(self, done: int, total: int) -> None:
        """Update queue counters and mark progress publishing as needed."""
        changed = (
            self.state.queue_done != done
            or self.state.queue_total != total
        )
        self.state.queue_done = done
        self.state.queue_total = total
        if changed:
            self._queue_progress_dirty = True

    async def _publish_progress_updated_if_needed(self) -> None:
        """Publish debounced queue progress updates after state is saved."""
        if not self._queue_progress_dirty:
            return
        progress = (self.state.queue_done, self.state.queue_total)
        if progress == self._last_published_queue_progress:
            self._queue_progress_dirty = False
            return
        await publish_repo_event(
            self.name,
            "progress_updated",
            {
                "queue_done": self.state.queue_done,
                "queue_total": self.state.queue_total,
            },
            redis_client=self.redis,
        )
        self._last_published_queue_progress = progress
        self._queue_progress_dirty = False

    def _summary_pr_signature(self) -> tuple:
        """Return the visible PR fingerprint shown in the repo summary.

        Captures the fields that ``handle_watch`` mutates while the
        runner stays in WATCH for many cycles (CI conclusion, review
        status, push and commit counters, plus the PR identity used in
        the header). Without including these, the dashboard summary
        would only refresh on ``state.state`` transitions and operator
        history events — leaving CI/review status visibly stale until
        an unrelated transition or a manual reload.
        """
        pr = self.state.current_pr
        if pr is None:
            return ()
        return (
            pr.number,
            pr.branch,
            pr.ci_status.value,
            pr.review_status.value,
            pr.push_count,
            pr.commits_count,
        )

    async def _publish_state_change_if_needed(self) -> None:
        """Publish a state_change event when the visible repo state changes.

        Drives SSE-driven dashboard refreshes; replaces the legacy 5s
        repo-summary poll that caused the OBS-AM badge stutter. Beyond
        ``state.state`` itself this also fires when the visible PR
        metadata (CI conclusion, review status, push/commit counters)
        changes — those mutate inside ``handle_watch`` while the runner
        stays in WATCH for many cycles, and without a publish here the
        summary card would stay stale until a state transition or an
        unrelated history event arrived. The signature also tracks the
        coder usage fields (session/weekly percent, API degraded flag)
        rendered in the summary's rate-limit badge — ``publish_state``
        refreshes them every cycle, so without inclusion here the
        badges would stay stale through long WATCH/IDLE stretches with
        an unchanged PR signature until an unrelated transition fired.

        The published payload mirrors what ``_serialize_latest_state``
        writes to Redis: inactive repos surface as ``IDLE`` regardless
        of ``self.state.state`` so SSE-only views (repo.html) reflect
        the deactivation immediately instead of staying on the last
        live state until a manual reload.
        """
        if self.repo_config.active:
            current_state = self.state.state.value
        else:
            current_state = PipelineState.IDLE.value
        usage_signature = (
            self.state.usage_session_percent,
            self.state.usage_weekly_percent,
            self.state.usage_api_degraded,
        )
        signature = (
            current_state,
            self._summary_pr_signature(),
            usage_signature,
        )
        if signature == self._last_published_state_signature:
            return
        # SSE notification is best-effort: a Redis pub/sub or list-op
        # failure here must not abort ``publish_state``. The authoritative
        # state was already persisted above, and re-raising would short-
        # circuit ``_publish_pending_event_log_entries`` plus stall the
        # runner cycle on every transient pub/sub blip. Leaving the
        # signature unchanged on failure means the next cycle retries
        # automatically while it still differs.
        try:
            await publish_repo_event(
                self.name,
                "state_change",
                {"state": current_state},
                redis_client=self.redis,
            )
        except Exception as exc:
            logger.warning(
                "[%s] state_change publish failed; will retry next cycle: %s",
                self.name,
                exc,
            )
            return
        self._last_published_state_signature = signature

    async def _publish_pending_event_log_entries(self) -> None:
        """Drain queued log entries as event_log_append SSE events.

        Each entry is removed only after the publish succeeds. If
        ``publish_repo_event`` raises (transient Redis outage), the
        failed entry and any not-yet-attempted ones are kept at the head
        of the queue so a later cycle can retry — otherwise subscribers
        would silently miss live updates while history was already
        persisted in ``state.history``. The exception is swallowed so a
        pub/sub blip never aborts ``publish_state`` (state was already
        persisted) and the runner cycle can keep progressing while the
        queued entries wait for the next opportunity.
        """
        if not self._pending_event_log_entries:
            return
        pending = self._pending_event_log_entries
        self._pending_event_log_entries = []
        for index, entry in enumerate(pending):
            try:
                await publish_repo_event(
                    self.name,
                    "event_log_append",
                    {"entry": entry},
                    redis_client=self.redis,
                )
            except Exception:
                self._pending_event_log_entries = (
                    list(pending[index:]) + self._pending_event_log_entries
                )
                logger.warning(
                    "[%s] event_log_append publish failed; will retry later",
                    self.name,
                )
                return

    def _compute_diff_stats(self, base_branch: str) -> dict[str, object]:
        """Compute file/language/line stats for the current branch vs base."""
        try:
            numstat = subprocess.run(
                ["git", "diff", "--numstat", f"origin/{base_branch}...HEAD"],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        except (subprocess.TimeoutExpired, OSError):
            return {}
        if numstat.returncode != 0:
            return {}
        added = 0
        deleted = 0
        files: list[str] = []
        for line in numstat.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) != 3:
                continue
            try:
                added += int(parts[0]) if parts[0] != "-" else 0
                deleted += int(parts[1]) if parts[1] != "-" else 0
            except ValueError:
                continue
            files.append(parts[2])
        languages = sorted(
            {
                language
                for file_path in files
                if (language := self._ext_to_language(file_path))
            }
        )
        test_files = sum(
            1
            for file_path in files
            if "test" in file_path.lower() or file_path.startswith("tests/")
        )
        ratio = (test_files / len(files)) if files else 0.0
        return {
            "files_touched_count": len(files),
            "languages_touched": languages,
            "diff_lines_added": added,
            "diff_lines_deleted": deleted,
            "test_file_ratio": round(ratio, 3),
        }

    @staticmethod
    def _apply_diff_stats(
        record: RunRecord,
        stats: dict[str, object],
        base_branch: str,
    ) -> None:
        """Copy diff-enrichment fields onto a run record."""
        record.files_touched_count = int(stats.get("files_touched_count", 0))
        record.languages_touched = list(stats.get("languages_touched", []))
        record.diff_lines_added = int(stats.get("diff_lines_added", 0))
        record.diff_lines_deleted = int(stats.get("diff_lines_deleted", 0))
        record.test_file_ratio = float(stats.get("test_file_ratio", 0.0))
        record.base_branch = base_branch

    async def _checkpoint_current_run_record(self) -> None:
        """Persist the active run record without finalizing it."""
        record = self._current_run_record
        if record is None:
            return
        await self._metrics_store.save(record)

    def _run_phase_for_metrics(self, exit_reason: str) -> str:
        if self.state.state == PipelineState.FIX:
            return "fix"
        if self.state.state == PipelineState.MERGE:
            return "merge"
        if self.state.state == PipelineState.CODING:
            return "coding"
        if exit_reason == "coding_complete":
            return "coding"
        if exit_reason == "success_merged":
            return "merge"
        if self.state.current_pr is not None:
            return "fix"
        return "coding"

    @staticmethod
    def _run_phase_from_state(state: PipelineState) -> str:
        if state == PipelineState.FIX:
            return "fix"
        if state == PipelineState.MERGE:
            return "merge"
        if state == PipelineState.ERROR:
            return "recovery"
        return "coding"

    async def _restore_current_run_record(self) -> None:
        """Reload the latest persisted record for the active task."""
        task = self.state.current_task
        if task is None:
            self._current_run_record = None
            return
        try:
            recent = await self._metrics_store.recent(
                task_id=task.pr_id,
                limit=20,
                repo_name=self.name,
            )
        except Exception as exc:
            self._current_run_record = None
            self.log_event(
                f"[INFRA] restore_current_run_record failed for "
                f"{task.pr_id}: {exc}."
            )
            return
        self._current_run_record = next(
            (record for record in recent if record.task_id == task.pr_id),
            None,
        )

    async def _save_current_run_record(
        self,
        exit_reason: str,
        *,
        diff_stats: dict[str, object] | None = None,
        base_branch: str | None = None,
        run_phase: str | None = None,
        cause: str | None = None,
    ) -> None:
        """Finalize and persist the active run record."""
        record = self._current_run_record
        if record is None:
            return
        ended_at = datetime.now(timezone.utc)
        record.ended_at = ended_at.isoformat()
        try:
            started_at = datetime.fromisoformat(record.started_at)
        except ValueError:
            record.duration_ms = None
        else:
            record.duration_ms = max(
                int((ended_at - started_at).total_seconds() * 1000),
                0,
            )
        record.exit_reason = exit_reason
        outcome, mapped_cause = _EXIT_REASON_TO_RUN_OUTCOME.get(
            exit_reason,
            ("failed", "CRASH"),
        )
        record.outcome = outcome  # type: ignore[assignment]
        record.cause = cause or mapped_cause  # type: ignore[assignment]
        record.run_phase = run_phase or self._run_phase_for_metrics(exit_reason)
        record.head_sha = self._git_rev_parse("HEAD")
        if exit_reason in ("success_merged", "coding_complete", "closed_unmerged"):
            resolved_base_branch = base_branch or self.repo_config.branch or "main"
            stats = diff_stats
            if stats is None:
                stats = self._compute_diff_stats(resolved_base_branch)
            self._apply_diff_stats(record, stats, resolved_base_branch)
        await self._metrics_store.save(record)
        # SSE notification is best-effort: a Redis pub/sub or list-op
        # failure here must not abort the cycle, otherwise a transient
        # outage would short-circuit downstream post-save logic in the
        # caller (state transitions, review trigger, etc.) after the run
        # record is already persisted.
        try:
            await publish_repo_event(
                self.name,
                "pr_metrics_update",
                {"task_id": record.task_id, "exit_reason": record.exit_reason},
                redis_client=self.redis,
            )
        except Exception as exc:
            logger.warning(
                "[%s] pr_metrics_update publish failed for %s: %s",
                self.name,
                record.task_id,
                exc,
            )

    async def _clear_error_message_on_recovery(
        self,
        *,
        log_prefix: str,
        reason: str,
        publish: bool = False,
    ) -> None:
        """Atomic clear of ``state.error_message`` at recovery transitions.

        Logs the clearing event so operators can reconstruct dashboard
        state changes from the event log. Mirrors ``_transition_to_error``:
        every ``error_message`` lifecycle write goes through a primitive
        with logging.

        Args:
            log_prefix: prefix matching the calling handler.
            reason: human-readable cause of the recovery.
            publish: when True, immediately publishes the cleared state.
        """
        if self.state.error_message is None:
            return
        previous = self.state.error_message
        self.state.error_message = None
        self.log_event(
            f"{log_prefix} cleared error_message ({reason}): "
            f"{previous[:120]}"
        )
        if publish:
            await self._publish_state_for_repo()

    async def _publish_state_for_repo(self) -> None:
        """Compatibility wrapper for repo-scoped state publication."""
        await self.publish_state()

    # All ERROR transitions must use this primitive. Direct writes to
    # ``state.state = PipelineState.ERROR`` are forbidden after PR-219b.
    async def _transition_to_error(
        self,
        message: str,
        *,
        save_run_record_as: str | None = "error",
        publish: bool = True,
        log_prefix: str = "[ERROR]",
        log_message: str | None = None,
        cancellation_cause: CancellationCause | None = None,
    ) -> None:
        """Atomic transition to ERROR with consistent telemetry.

        Sets state.state, error_message, logs an [ERROR]-prefixed event,
        optionally saves a run record, and optionally publishes state. All
        transitions to PipelineState.ERROR must use this primitive after
        PR-219b ships; direct ``state.state = PipelineState.ERROR`` writes
        are forbidden.

        ``log_message`` overrides the body of the log line when callers
        need the operator-visible log to differ from ``error_message``
        (e.g. ``watch.py``'s ``[WATCH] {exc}.`` vs. its more verbose
        ``error_message``). Defaults to ``message`` so the common case
        keeps a one-line callsite.

        ``cancellation_cause`` (PR-253) overrides the default CRASH cause
        record written to Redis. Callers that already classified the
        failure as TIMEOUT or INFRA pass the structured cause directly so
        the dashboard surfaces the specific category. The write is
        best-effort — Redis errors never block the ERROR transition.

        First-cause-wins (PR-253 fix): if a cause is already recorded for
        this ``task_id``, do not overwrite it. Retry-heavy flows
        (ERROR → IDLE → retry → ERROR) would otherwise replace the
        original failure category, corrupting OBS-BE attribution. The
        matching delete in ``handle_error`` clears the slot when a retry
        succeeds, so the next genuine failure can record again.
        """
        prior_state = self.state.state
        self.state.state = PipelineState.ERROR
        self.state.error_message = message
        log_body = message if log_message is None else log_message
        self.log_event(f"{log_prefix} {log_body}.")
        if save_run_record_as:
            await self._save_current_run_record(
                save_run_record_as,
                run_phase=self._run_phase_from_state(prior_state),
                cause=(
                    cancellation_cause.category
                    if cancellation_cause is not None
                    else None
                ),
            )
        task = self.state.current_task
        if task is not None:
            existing: CancellationCause | None
            try:
                existing = await get_cancellation_cause(
                    self.redis, self.name, task.pr_id
                )
            except Exception:
                existing = None
            if existing is None:
                cause = cancellation_cause or CancellationCause(
                    category="CRASH",
                    payload={"error_message": truncate_for_payload(message)},
                )
                await safe_record_cancellation_cause(
                    self.redis,
                    self.name,
                    task.pr_id,
                    cause,
                    log=self.log_event,
                )
        if publish:
            await self.publish_state()

    # All daemon escalation-and-skip transitions must use this primitive so
    # the queue can continue while the PR keeps its durable escalation signal.
    async def _escalate_and_skip(
        self,
        message: str,
        *,
        target_state: PipelineState = PipelineState.IDLE,
        error_message_override: str | None | object = _USE_MESSAGE_AS_ERROR,
        apply_escalated_label: bool = True,
        label_create_log_prefix: str = "escalate",
        post_comment_on_pr: str | None = None,
        set_pr_escalated_flag: bool = True,
        log_message: str | None = None,
    ) -> bool:
        """Escalate the active PR with consistent telemetry.

        By default sets state=IDLE, applies the ``escalated`` label on
        the current PR via ``_ensure_escalated_label`` (FixMixin),
        marks ``PRInfo.is_escalated=True``, logs an ``[ESCALATE]``
        event and publishes state.

        Returns ``True`` when the ``escalated`` label was applied
        successfully (or label-apply was skipped); ``False`` when
        ``_ensure_escalated_label`` reported a soft-failure on the
        ``pr edit --add-label`` step. Callers that route to a state
        which depends on the GitHub label for durability inspect this
        return to downgrade when the upstream apply failed.

        Args:
            message: Becomes ``error_message`` and the default log
                payload. Callers that need a different log body pass
                ``log_message``; callers that need to clear or replace
                ``error_message`` pass ``error_message_override``.
            target_state: Final state. Default ``IDLE``. ``ERROR`` may be
                passed when the escalation should also act as a durable
                parking error.
            error_message_override: Sentinel default uses ``message``.
                Pass ``None`` to clear ``state.error_message``. Pass a
                string to replace it (e.g.
                ``_escalate_fix_coder_initiated`` expands the failure
                context when label-apply fails).
            apply_escalated_label: When True, calls
                ``_ensure_escalated_label`` so the GitHub label is
                created (idempotent) and applied to the PR. Default
                True. Returns the apply outcome via the function's
                return value.
            label_create_log_prefix: Forwarded to
                ``_ensure_escalated_label`` so existing label-create
                soft-fail log prefixes (``"FIX no-push"``, ``"FIX
                coder ESCALATE"``, ...) survive the migration.
            post_comment_on_pr: When non-None, posts the supplied text
                via ``comments.post_comment``. Failure is logged
                with a generic ``[INFRA] Warning:`` prefix; callers
                that need a custom failure-log body (e.g. fix.py
                wrappers asserted on by regression tests) post the
                comment themselves before invoking the primitive.
            set_pr_escalated_flag: When True, sets
                ``self.state.current_pr.is_escalated = True``. Default
                True. Set False at sites where the in-memory flag is
                explicitly NOT meant to mark the PR as escalated
                (e.g. ``watch.py``'s review-timeout HUNG fall-through,
                where the PR is parked but recoverable).
            log_message: Overrides the body after ``[ESCALATE] `` when
                the operator-visible log differs from
                ``error_message``. Defaults to ``message``.
        """
        pr = self.state.current_pr

        if post_comment_on_pr is not None and pr is not None:
            try:
                gh_comments.post_comment(
                    self.owner_repo, pr.number, post_comment_on_pr
                )
            except Exception as exc:
                self.log_event(
                    f"[INFRA] Warning: failed to post escalation comment "
                    f"on PR #{pr.number}: {exc}."
                )

        label_applied = True
        if apply_escalated_label and pr is not None:
            label_applied = self._ensure_escalated_label(
                pr.number, label_create_log_prefix
            )

        if set_pr_escalated_flag and pr is not None:
            pr.is_escalated = True

        prior_state = self.state.state
        current_task = self.state.current_task
        if current_task is not None:
            existing_cause: CancellationCause | None
            try:
                existing_cause = await get_cancellation_cause(
                    self.redis,
                    self.name,
                    current_task.pr_id,
                )
            except Exception:
                existing_cause = None
            if existing_cause is None:
                await safe_record_cancellation_cause(
                    self.redis,
                    self.name,
                    current_task.pr_id,
                    CancellationCause(
                        category="ESCALATE",
                        payload={
                            "subsource": "daemon",
                            "reason_text": message,
                            "previous_state": prior_state.value,
                        },
                    ),
                    log=self.log_event,
                )
            if set_pr_escalated_flag:
                try:
                    status_written = await self._commit_task_status_change(
                        current_task,
                        "ERROR",
                        message,
                    )
                except Exception as exc:
                    self.log_event(
                        f"[ERROR] Failed to write status:ERROR to "
                        f"{current_task.task_file}: {exc}"
                    )
                    status_written = False
                if not status_written:
                    await self._mark_status_write_failed_task(current_task)

        if target_state == PipelineState.IDLE and current_task is not None:
            self.state.current_task = None
            self._reset_runner_local_task_counters()

        self.state.state = target_state
        if error_message_override is _USE_MESSAGE_AS_ERROR:
            self.state.error_message = message
        else:
            self.state.error_message = error_message_override  # type: ignore[assignment]
        log_body = log_message if log_message is not None else message
        self.log_event(f"[ESCALATE] {log_body}")
        await self.publish_state()
        return label_applied

    async def _commit_task_status_change(
        self,
        current_task: Any,
        status: str,
        reason: str,
    ) -> bool:
        """Best-effort commit of daemon-written task frontmatter status."""
        task_file = getattr(current_task, "task_file", None)
        pr_id = getattr(current_task, "pr_id", "")
        if not task_file:
            return False

        task_path = Path(task_file)
        if task_path.is_absolute() or ".." in task_path.parts:
            self.log_event(
                f"[INFRA] Warning: refusing to commit unsafe task path "
                f"{task_file!r}."
            )
            return False
        repo_root = Path(self.repo_path).resolve()
        resolved_task_path = (repo_root / task_path).resolve()
        try:
            resolved_task_path.relative_to(repo_root)
        except ValueError:
            self.log_event(
                f"[INFRA] Warning: refusing to commit task path outside repo "
                f"{task_file!r}."
            )
            return False

        short_reason = " ".join(reason.split())
        if len(short_reason) > 80:
            short_reason = f"{short_reason[:77]}..."
        display_pr_id = pr_id if str(pr_id).startswith("PR-") else f"PR-{pr_id}"
        subject = f"[STATUS] {display_pr_id} marked {status}: {short_reason}"
        message = f"{subject}\n\n[skip ci]"
        base = self.repo_config.branch

        try:
            git_ops._git(self.repo_path, "fetch", "origin", base, timeout=60)
            git_ops._git(self.repo_path, "checkout", "-f", base, timeout=60)
            git_ops._git(
                self.repo_path,
                "reset",
                "--hard",
                f"origin/{base}",
                timeout=60,
            )
            write_frontmatter_status(Path(self.repo_path) / task_file, status)
            git_ops._git(self.repo_path, "add", "--", task_file, timeout=30)
            diff = git_ops._git(
                self.repo_path,
                "diff",
                "--cached",
                "--quiet",
                "--",
                task_file,
                check=False,
            )
            if diff.returncode == 0:
                self.log_event(
                    f"[MERGE] No task status change to commit for {task_file}."
                )
                return True
            git_ops._git(
                self.repo_path,
                "commit",
                "-m",
                message,
                timeout=60,
            )
            git_ops._git(self.repo_path, "push", "origin", base, timeout=60)
            return True
        except Exception as exc:
            self.log_event(
                f"[INFRA] Warning: failed to commit {status} status for "
                f"{task_file}: {exc}."
            )
            return False

    async def _persist_status_write_failed_task_pr_ids(self) -> None:
        """Persist status-write fallback markers across daemon restarts."""
        key = status_write_failed_tasks(self.name)
        try:
            if self._status_write_failed_task_pr_ids:
                await self.redis.set(
                    key,
                    json.dumps(
                        sorted(self._status_write_failed_task_pr_ids),
                        separators=(",", ":"),
                    ),
                )
            else:
                await self.redis.delete(key)
            self._status_write_failed_task_pr_ids_persist_failed = False
        except Exception as exc:
            self._status_write_failed_task_pr_ids_persist_failed = True
            self.log_event(
                f"[INFRA] Warning: failed to persist status-write "
                f"fallback markers: {exc}."
            )

    async def _hydrate_status_write_failed_task_pr_ids(self) -> None:
        """Load status-write fallback markers persisted by a prior process."""
        decoded_ok, decoded_found, decoded = await self._read_status_write_failed_task_ids(
            status_write_failed_tasks(self.name)
        )
        (
            legacy_ok,
            legacy_found,
            legacy_decoded,
        ) = await self._read_status_write_failed_task_ids(
            legacy_recovered_tasks(self.name)
        )
        if (decoded_ok and decoded_found) or (legacy_ok and legacy_found):
            self._status_write_failed_task_pr_ids = decoded | legacy_decoded
            self._status_write_failed_task_pr_ids_persist_failed = False
        elif (
            decoded_ok
            and legacy_ok
            and not self._status_write_failed_task_pr_ids_persist_failed
        ):
            self._status_write_failed_task_pr_ids = set()

    async def _clear_status_write_failed_task_ids(
        self,
        uploaded_pr_ids: set[str],
    ) -> None:
        """Clear fallback markers for task files the operator re-uploaded."""
        await self._hydrate_status_write_failed_task_pr_ids()
        self._status_write_failed_task_pr_ids.difference_update(uploaded_pr_ids)
        await self._persist_status_write_failed_task_pr_ids()
        legacy_key = legacy_recovered_tasks(self.name)
        try:
            await self.redis.set(
                legacy_key,
                json.dumps(
                    sorted(self._status_write_failed_task_pr_ids),
                    separators=(",", ":"),
                ),
            )
            await self.redis.delete(legacy_key)
        except Exception as exc:
            self.log_event(
                f"[INFRA] Warning: failed to clear legacy status-write "
                f"fallback markers: {exc}."
            )

    async def _read_status_write_failed_task_ids(
        self, key: str
    ) -> tuple[bool, bool, set[str]]:
        """Return a persisted fallback marker set from one Redis key."""
        try:
            raw = await self.redis.get(key)
        except Exception:
            return False, False, set()
        if raw is None:
            return True, False, set()
        if not raw:
            return True, True, set()
        try:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")
            decoded = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return True, True, set()
        if not isinstance(decoded, list):
            return True, True, set()
        return True, True, {
            item for item in decoded if isinstance(item, str) and item
        }

    async def _mark_status_write_failed_task(self, current_task: Any) -> None:
        """Keep an explicitly parked task skipped when status commit fails."""
        pr_id = getattr(current_task, "pr_id", "")
        if not pr_id:
            return
        self._status_write_failed_task_pr_ids.add(pr_id)
        self.log_event(
            f"[INFRA] Warning: using in-memory ERROR fallback for "
            f"{pr_id}; task file re-upload is required to retry."
        )
        await self._persist_status_write_failed_task_pr_ids()

    def _track_current_coder_process(
        self, proc: asyncio.subprocess.Process
    ) -> None:
        """Remember the active coder subprocess for user-triggered stop."""
        self._current_coder_process = proc

    async def _refresh_user_paused_from_redis(self) -> None:
        """Merge the persisted ``user_paused`` flag into in-memory state."""
        try:
            raw = await self.redis.get(pipeline_state(self.name))
        except Exception:
            return
        if not raw:
            return
        try:
            persisted = RepoState.model_validate_json(raw)
        except Exception:
            return
        self.state.user_paused = persisted.user_paused

    async def _pop_stop_request(self) -> bool:
        """Return True when a pending stop control signal exists."""
        key = control_stop(self.name)
        try:
            raw = await self.redis.get(key)
        except Exception:
            return False
        if not raw:
            return False
        try:
            await self.redis.delete(key)
        except Exception:
            pass
        return True

    async def _terminate_current_coder(self) -> None:
        """Terminate the active coder subprocess with TERM then KILL."""
        proc = self._current_coder_process
        if proc is None or proc.returncode is not None:
            self._current_coder_process = None
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            self._current_coder_process = None
            return
        grace = self.app_config.daemon.coder_terminate_grace_sec
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace)
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
        finally:
            self._current_coder_process = None

    async def _monitor_stop_request(
        self, cli_task: asyncio.Task[tuple[int, str, str]]
    ) -> None:
        """Watch Redis for user stop commands while CODING is active."""
        while not cli_task.done():
            if await self._pop_stop_request():
                self._stop_requested = True
                self.state.user_paused = True
                self.log_event(
                    "[INFRA] User stop requested; terminating current "
                    "coder."
                )
                await self._terminate_current_coder()
                cli_task.cancel()
                return
            await asyncio.sleep(_STOP_POLL_INTERVAL_SEC)

    async def publish_state(self) -> None:
        """Serialize ``self.state`` and write it to Redis."""
        self.state.active = self.repo_config.active
        configured_coder = self.repo_config.coder or self.app_config.daemon.coder
        active_coder = self.state.coder or configured_coder.value
        self.state.coder = active_coder
        if self.repo_config.active:
            provider = (
                self._claude_usage_provider
                if active_coder != CoderType.CODEX.value
                else self._codex_usage_provider
            )
            snap = await asyncio.to_thread(provider.fetch)
            if snap is not None:
                self.state.usage_session_percent = snap.session_percent
                self.state.usage_session_resets_at = snap.session_resets_at
                self.state.usage_weekly_percent = snap.weekly_percent
                self.state.usage_weekly_resets_at = snap.weekly_resets_at
            else:
                self.state.usage_session_percent = None
                self.state.usage_session_resets_at = None
                self.state.usage_weekly_percent = None
                self.state.usage_weekly_resets_at = None
            self.state.usage_api_degraded = provider.consecutive_failures >= 10
        self.state.last_updated = datetime.now(timezone.utc)
        state_key = pipeline_state(self.name)

        async def _serialize_latest_state() -> str:
            await self._refresh_user_paused_from_redis()
            if not self.repo_config.active:
                data = self.state.model_dump()
                data["state"] = PipelineState.IDLE.value
                return RepoState(**data).model_dump_json()
            return self.state.model_dump_json()

        if hasattr(self.redis, "transaction"):
            async def _transaction(pipe: Any) -> None:
                raw = await pipe.get(state_key)
                if raw:
                    try:
                        persisted = RepoState.model_validate_json(raw)
                    except Exception:
                        pass
                    else:
                        self.state.user_paused = persisted.user_paused
                payload = await _serialize_latest_state()
                pipe.multi()
                pipe.set(state_key, payload)

            await self.redis.transaction(_transaction, state_key)
        else:
            payload = await _serialize_latest_state()
            await self.redis.set(state_key, payload)
        if self._old_basename != self.name:
            try:
                old_key = pipeline_state(self._old_basename)
                old_data = await self.redis.get(old_key)
                owns_old_key = False
                if old_data:
                    old_state = json.loads(old_data)
                    old_url = old_state.get("url", "")
                    if repo_slug_from_url(old_url) == self.name:
                        await self.redis.delete(old_key)
                        owns_old_key = True
                if owns_old_key:
                    old_upload = upload_pending(self._old_basename)
                    new_upload = upload_pending(self.name)
                    if await self.redis.exists(old_upload):
                        await self.redis.renamenx(old_upload, new_upload)
            except Exception:
                pass
        await self._publish_progress_updated_if_needed()
        await self._publish_state_change_if_needed()
        await self._publish_pending_event_log_entries()

    async def _save_cli_log(self, stdout: str, stderr: str, label: str) -> None:
        _MAX_CLI_LOG_BYTES = 64 * 1024  # 64 KB cap per entry
        ts = datetime.now(timezone.utc).isoformat()
        key_latest = cli_log_latest(self.name)
        key_history = cli_log_history(self.name, ts)
        marker = "[truncated]\n"
        combined = f"=== STDOUT ===\n{stdout}\n\n=== STDERR ===\n{stderr}"
        raw = combined.encode("utf-8", errors="replace")
        if len(raw) > _MAX_CLI_LOG_BYTES:
            tail_budget = _MAX_CLI_LOG_BYTES - len(marker.encode("utf-8"))
            raw = raw[-tail_budget:]
            combined = marker + raw.decode("utf-8", errors="replace")
        try:
            await self.redis.set(key_latest, combined, ex=3600)
            await self.redis.set(key_history, combined, ex=86400)
        except Exception:
            logger.warning("Failed to save CLI log for %s", self.name)
        if combined.strip():
            first_lines = combined.strip()[:200]
            self.log_event(f"[INFRA] {label}: {first_lines}.")

    def log_event(self, event: str) -> None:
        """Append an event to ``state.history`` (capped) and log it.

        Consecutive events whose only difference is a numeric counter
        (e.g. ``"PR #5 waiting (1/20m)"`` vs ``"PR #5 waiting (2/20m)"``)
        are deduped: the existing entry's ``count`` increments, its
        ``event`` is replaced with the latest text so updated counter
        values stay visible, and ``last_seen_at`` is refreshed.
        """
        now = datetime.now(timezone.utc).isoformat()
        state = self.state.state.value
        last_entry = self.state.history[-1] if self.state.history else None
        if (
            last_entry is not None
            and last_entry.get("state") == state
            and _normalize_for_dedup(last_entry.get("event", ""))
            == _normalize_for_dedup(event)
        ):
            last_entry["count"] = int(last_entry.get("count", 1)) + 1
            last_entry["event"] = event
            last_entry["last_seen_at"] = now
            self._pending_event_log_entries.append(dict(last_entry))
        else:
            entry = {
                "time": now,
                "state": state,
                "event": event,
                "count": 1,
                "last_seen_at": now,
            }
            self.state.history.append(entry)
            self._pending_event_log_entries.append(dict(entry))
        if len(self.state.history) > _HISTORY_LIMIT:
            self.state.history = self.state.history[-_HISTORY_LIMIT:]
        logger.info("[%s] %s", self.name, event)

    async def _publish_while_waiting(self, label: str) -> None:
        """Publish state every 30s while a long-running CLI call is active."""
        while True:
            await asyncio.sleep(30)
            try:
                await self.publish_state()
            except Exception:
                logger.warning("[%s] heartbeat publish failed, will retry", self.name)

    async def _refresh_github_api_budget(self) -> RateLimitBudget | None:
        """Refresh the installation budget, sharing one probe across runners.

        The local TTL guards repeated ``gh api rate_limit`` probes from the
        same runner. The cross-runner Redis lock (``try_claim_refresh_lock``)
        guarantees that among all ``PipelineRunner`` instances, only one
        actually invokes ``gh api rate_limit`` per TTL window; the rest read
        the persisted observation. Without this, probe traffic scales linearly
        with repo count and can itself exhaust the rate limit.

        The shared Redis snapshot is consulted on every refresh, even within
        the local TTL window. Otherwise a runner can keep returning a stale
        "healthy" cached value for up to a minute after a sibling has already
        published a "critical" update — exactly the window where multi-repo
        deployments would keep spending GitHub quota during the most sensitive
        moment.

        The TTL is only advanced once a snapshot is actually in hand. On
        concurrent startup a non-lock holder may see ``read_budget()`` return
        ``None`` before the lock holder has finished probing; in that case
        the next cycle must retry rather than treating "no data" as a fresh
        observation, otherwise budget protection silently disengages for
        the full TTL window.

        When this runner wins the lock but the probe itself returns ``None``
        (transient ``gh api rate_limit`` failure), the lock is released so a
        sibling can retry on its next cycle. Holding it for the full TTL on
        a failed probe would suppress every other runner's probe attempt
        during exactly the conditions the protection exists to cover.
        """
        now = datetime.now(timezone.utc)
        shared = await read_budget(self.redis)
        if shared is not None:
            self._github_api_budget_cache = shared
        if (
            self._github_api_budget_last_fetched is not None
            and (now - self._github_api_budget_last_fetched).total_seconds() < 60
        ):
            return self._github_api_budget_cache
        if await try_claim_refresh_lock(self.redis, ttl_seconds=60):
            rest, graphql = await asyncio.to_thread(
                gh_rate_limit.fetch_rate_limit_buckets
            )
            if rest is not None:
                await write_rest_budget(self.redis, rest)
            else:
                await clear_rest_budget(self.redis)
            if graphql is not None:
                await write_graphql_budget(self.redis, graphql)
            else:
                await clear_graphql_budget(self.redis)
            candidates = [b for b in (rest, graphql) if b is not None]
            budget = (
                min(candidates, key=lambda b: b.remaining_percent)
                if candidates
                else None
            )
            if budget is not None:
                self._github_api_budget_cache = budget
                self._github_api_budget_last_fetched = now
                await write_budget(self.redis, budget)
                return budget
            await release_refresh_lock(self.redis)
        if self._github_api_budget_cache is not None:
            self._github_api_budget_last_fetched = now
        return self._github_api_budget_cache

    def _enter_github_api_pause(self) -> None:
        """Threshold action for ``_github_api_pause_policy``.

        Only the transition cycle invokes ``maybe_escalate``, so this
        runs at most once per low-budget window.
        """
        budget = self._github_api_budget_cache
        if budget is None:  # pragma: no cover - guarded by caller
            return
        reset_iso = budget.reset_at.isoformat()
        remaining_min = max(
            0,
            int((budget.reset_at - datetime.now(timezone.utc)).total_seconds() // 60),
        )
        self.log_event(
            f"[RATE-LIMIT] GitHub API budget critical "
            f"({budget.remaining}/{budget.limit}), pausing until "
            f"{reset_iso} ({remaining_min} min)."
        )

    def _enter_github_api_slowdown(self) -> None:
        """Threshold action for ``_github_api_slowdown_policy``."""
        budget = self._github_api_budget_cache
        if budget is None:  # pragma: no cover - guarded by caller
            return
        multiplier = self.app_config.daemon.github_api_slowdown_multiplier
        effective_interval = self.repo_config.poll_interval_sec * multiplier
        self.log_event(
            f"[RATE-LIMIT] GitHub API budget low "
            f"({budget.remaining}/{budget.limit}), slowing polling to "
            f"{effective_interval}s."
        )

    def _is_extended_idle_active(self) -> bool:
        """Return whether the runner is on the slower extended-idle cadence."""
        return (
            self._idle_streak
            >= self.app_config.daemon.idle_extended_after_cycles
        )

    @property
    def effective_idle_poll_interval(self) -> int:
        """Return the IDLE poll interval after the adaptive slow-down.

        Returns the configured base unless ``_idle_streak`` has reached
        ``idle_extended_after_cycles``, in which case the longer
        ``idle_extended_poll_interval_sec`` cadence applies. When the
        rate-limit slowdown is also active, returns the larger of the
        two slowdowns (``max(extended, base * multiplier)``) rather
        than letting ``_check_github_api_budget``'s skip-every-Nth
        logic stack on top — which would compound the two into
        ``extended * multiplier`` between real cycles. The skip logic
        is suppressed in that branch so spacing equals this interval.
        """
        base = self.repo_config.poll_interval_sec
        if not self._is_extended_idle_active():
            return base
        target = max(
            base,
            self.app_config.daemon.idle_extended_poll_interval_sec,
        )
        if self._github_api_slowdown_attempts > 0:
            multiplier = max(
                1, self.app_config.daemon.github_api_slowdown_multiplier
            )
            target = max(target, base * multiplier)
        return target

    def reset_idle_streak(self) -> None:
        """Reset the adaptive IDLE-polling streak (e.g. on wake)."""
        self._idle_streak = 0

    def _update_idle_streak_after_cycle(
        self, pre_state: PipelineState | None = None
    ) -> None:
        """Bump or clear ``_idle_streak`` based on the cycle outcome.

        A cycle counts toward the streak only when the runner both
        STARTED and ENDED the cycle in IDLE with no PR pinned
        (``current_pr is None``) AND the cycle produced a clean idle
        verdict (``_idle_dispatch_deferred`` is false). Any other
        outcome — a transition into IDLE from an active state
        (WATCH/FIX/MERGE/CODING/etc.), attaching to an open PR for
        manual work, a pending upload retry, or a GitHub read
        failure that left queue/PR status unknown — resets the
        streak so the next cycle polls on the fast cadence again
        and recovers quickly from transient outages.
        """
        dispatch_deferred = self._idle_dispatch_deferred
        self._idle_dispatch_deferred = False
        if (
            pre_state == PipelineState.IDLE
            and self.state.state == PipelineState.IDLE
            and self.state.current_pr is None
            and not dispatch_deferred
        ):
            cap = max(
                _IDLE_STREAK_CAP,
                self.app_config.daemon.idle_extended_after_cycles,
            )
            if self._idle_streak < cap:
                self._idle_streak += 1
        else:
            self._idle_streak = 0

    async def _check_github_api_budget(self) -> bool:
        """Return ``True`` if the cycle may proceed; ``False`` to skip it.

        Implements three threshold paths driven by ``app_config.daemon``:
        critical → pause until ``reset_at``; warning → run only one in
        ``github_api_slowdown_multiplier`` cycles; otherwise normal.
        Both threshold branches are gated on ``now < budget.reset_at`` so a
        stale snapshot whose reset has elapsed never throttles polling.
        Both threshold transitions are mediated by ``BoundedRecoveryPolicy``
        instances so the bookkeeping matches the dirty-tree and FIX
        iteration-cap recovery sites.
        """
        budget = await self._refresh_github_api_budget()
        if budget is None:
            return True

        pct = budget.remaining_percent
        pause_pct = self.app_config.daemon.github_api_pause_threshold_percent
        slowdown_pct = self.app_config.daemon.github_api_slowdown_threshold_percent
        multiplier = max(1, self.app_config.daemon.github_api_slowdown_multiplier)
        now = datetime.now(timezone.utc)

        if pct < pause_pct and now < budget.reset_at:
            was_zero = self._github_api_pause_attempts == 0
            self._github_api_pause_policy.increment(self)
            if was_zero:
                await self._github_api_pause_policy.maybe_escalate(self)
            return False

        if self._github_api_pause_attempts > 0:
            self._github_api_pause_policy.reset(self)

        if pct < slowdown_pct and now < budget.reset_at:
            was_zero = self._github_api_slowdown_attempts == 0
            self._github_api_slowdown_policy.increment(self)
            if was_zero:
                await self._github_api_slowdown_policy.maybe_escalate(self)
            # When the runner is already polling on the extended-idle
            # cadence, ``effective_idle_poll_interval`` has folded the
            # slowdown into the sleep duration. Skipping cycles here
            # would compound the two slowdowns; instead, let every
            # cycle proceed and rely on the longer interval. The same
            # applies to WATCH: ``effective_watch_poll_interval`` takes
            # ``max(target, base * multiplier)``, so an additional
            # one-in-N skip would space WATCH polls at
            # ``effective_watch_poll_interval * multiplier`` and could
            # delay merge/fix transitions by an hour or more.
            if (
                self._is_extended_idle_active()
                or self.state.state == PipelineState.WATCH
            ):
                return True
            proceed = self._github_api_slowdown_cycle % multiplier == 0
            self._github_api_slowdown_cycle += 1
            return proceed

        if self._github_api_slowdown_attempts > 0:
            self._github_api_slowdown_policy.reset(self)
            self._github_api_slowdown_cycle = 0
        return True

    async def _capture_budget_remaining_for_burn(self) -> int | None:
        """Return the latest known remaining budget for cycle-burn tracking.

        Reads the shared snapshot when available so multi-runner
        deployments still observe deltas across the same lock-holder
        probes; falls back to the runner-local cache otherwise. Returns
        ``None`` when no observation has been seen yet — the cycle-burn
        recorder treats that as ``0``.
        """
        budget = await read_budget(self.redis)
        if budget is None:
            budget = self._github_api_budget_cache
        return budget.remaining if budget is not None else None

    async def run_cycle(self) -> None:
        """Advance the state machine by one step."""
        before_remaining = await self._capture_budget_remaining_for_burn()
        try:
            await self._run_cycle_body()
        finally:
            await self._record_cycle_burn(before_remaining)

    async def _record_cycle_burn(self, before_remaining: int | None) -> None:
        """Persist GraphQL points consumed by this cycle for the dashboard.

        Reads the budget cache after the cycle has finished, computes the
        delta against ``before_remaining``, and forwards the result to
        :func:`record_cycle_burn`. A negative delta (the rate-limit window
        reset between the two observations) is normalised to ``0`` so a
        reset never appears as a spurious "burn". Both reads use the
        cached snapshot, so cycles where the cache was not refreshed in
        this window record ``0`` — the metric tracks observed deltas, not
        attributed-per-call consumption.
        """
        try:
            after_remaining = await self._capture_budget_remaining_for_burn()
            if before_remaining is None or after_remaining is None:
                delta = 0
            else:
                delta = max(0, before_remaining - after_remaining)
            await record_cycle_burn(self.redis, self.name, delta)
        except Exception:
            logger.warning(
                "Failed to record GraphQL cycle burn for %s",
                self.name,
                exc_info=True,
            )

    async def _run_cycle_body(self) -> None:
        """Inner state-machine step; ``run_cycle`` wraps it for burn tracking."""
        try:
            await self.ensure_repo_cloned()
        except RuntimeError as exc:
            await self._transition_to_error(
                str(exc),
                log_prefix="[INFRA]",
                log_message=f"ensure_repo_cloned failed: {exc}",
                save_run_record_as=None,
                publish=True,
            )
            return

        if not await self._check_github_api_budget():
            await self.publish_state()
            return

        await self._refresh_user_paused_from_redis()
        if not self.state.user_paused:
            self._user_pause_logged = False
        if not self._recovered:
            recovery_complete = await self.recover_state()
            if not recovery_complete:
                has_pending = False
                try:
                    raw = await self.redis.get(upload_pending(self.name))
                    has_pending = bool(raw)
                except Exception:
                    pass
                if has_pending:
                    branch = self.repo_config.branch
                    on_base = False
                    try:
                        head_ref = git_ops._git(
                            self.repo_path, "rev-parse", "--abbrev-ref",
                            "HEAD",
                        ).stdout.strip()
                        if head_ref == branch:
                            on_base = True
                        else:
                            git_ops._git(self.repo_path, "checkout", branch)
                            on_base = True
                    except Exception:
                        pass
                    if on_base:
                        await self.process_pending_uploads(_safe=True)
                await self.publish_state()
                return
            self._recovered = True
            await self.publish_state()
            return

        if (
            self.state.user_paused
            and self.state.state in (
                PipelineState.IDLE,
                PipelineState.PAUSED,
                PipelineState.WATCH,
                PipelineState.MERGE,
            )
        ):
            if not self._user_pause_logged:
                self.log_event("[INFRA] Paused. Press Play to resume.")
                self._user_pause_logged = True
            await self.publish_state()
            return

        if not await self.preflight():
            await self.publish_state()
            return

        pre_state = self.state.state
        if self.state.state in _TRANSIENT_STATES:
            self.log_event(
                f"[INFRA] resetting stale transient state "
                f"{self.state.state.value} -> IDLE."
            )
            self.state.state = PipelineState.IDLE

        if self.state.state == PipelineState.IDLE:
            await self._refresh_user_paused_from_redis()
            if self.state.user_paused:
                if not self._user_pause_logged:
                    self.log_event("[INFRA] Paused. Press Play to resume.")
                    self._user_pause_logged = True
                await self.publish_state()
                return
            await self.reload_repo_config_if_dirty()
            if not self.repo_config.active:
                await self.publish_state()
                return
            self._user_pause_logged = False
            if await self._maybe_auto_pause_for_error_rate():
                return

        current = self.state.state
        if current == PipelineState.IDLE:
            await self.handle_idle()
        elif current == PipelineState.WATCH:
            await self.handle_watch()
        elif current == PipelineState.PAUSED:
            await self.handle_paused()
        elif current == PipelineState.ERROR:
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.log_event(
                    "[RATE-LIMIT] Legacy ERROR + rate_limited_until "
                    "-> PAUSED."
                )
            elif self.app_config.daemon.error_handler_use_ai:
                await self.handle_error()

        if (
            current != PipelineState.ERROR
            and self._error_skip_active
            and self.state.state != PipelineState.ERROR
        ):
            self._error_skip_context = None
            self._error_skip_policy.reset(self)
            self._error_skip_active = False

        self._update_idle_streak_after_cycle(pre_state)
        if (
            pre_state == PipelineState.WATCH
            and self.state.state != PipelineState.WATCH
        ):
            self._reset_watch_polling()
        elif (
            pre_state != PipelineState.WATCH
            and self.state.state == PipelineState.WATCH
        ):
            # PR-202: anchor the slow-start window at the moment of the
            # state transition. The daemon main loop computes the next
            # poll interval *before* the next ``run_cycle`` runs, so
            # deferring this to ``handle_watch`` would leave the first
            # interval after WATCH entry on the fast base cadence and
            # waste the quota the slow-start is meant to save.
            self._watch_entered_at = datetime.now(timezone.utc)
        await self.publish_state()
