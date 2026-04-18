"""Per-repository pipeline state machine.

One ``PipelineRunner`` instance exists per connected repository. The daemon
main loop calls ``run_cycle`` once per poll interval; each cycle clones or
fetches the repo, runs a preflight check, and dispatches on the persisted
state (``IDLE``, ``WATCH``, ``HUNG``, ``PAUSED``, or ``ERROR``). Transient states
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
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis

from src import github_client  # noqa: F401 — tests reference runner_module.github_client
from src.coder_registry import CoderPlugin, CoderRegistry
from src.coders import build_coder_registry
from src.config import AppConfig, CoderType, RepoConfig
from src.daemon import (
    git_ops,
    scaffolder,  # noqa: F401 — tests reference runner_module.scaffolder
)
from src.daemon.git_ops import _repo_looks_scaffolded, repo_owner_from_url
from src.daemon.handlers.coding import CodingMixin
from src.daemon.handlers.error import ErrorCategory, ErrorMixin, _classify_error  # noqa: F401 — re-exported for tests
from src.daemon.handlers.fix import FixMixin
from src.daemon.handlers.hung import HungMixin
from src.daemon.handlers.idle import IdleMixin
from src.daemon.handlers.merge import MergeMixin
from src.daemon.handlers.watch import WatchMixin
from src.daemon.preflight import PreflightMixin
from src.daemon.rate_limit import RateLimitMixin
from src.daemon.recovery import RecoveryMixin
from src.daemon.repo_ops import RepoOpsMixin
from src.metrics import MetricsStore, RunRecord
from src.models import PipelineState, RepoState
from src.usage import UsageProvider
from src.utils import repo_slug_from_url

logger = logging.getLogger(__name__)

_TRANSIENT_STATES = {
    PipelineState.CODING,
    PipelineState.FIX,
    PipelineState.MERGE,
}

_HISTORY_LIMIT = 100

# Timeout for ``scripts/ci.sh`` on the auto-commit path.
_CI_SCRIPT_TIMEOUT_SEC = 1800


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
        self._usage_degraded_logged = False
        self._claude_usage_provider = claude_usage_provider
        self._codex_usage_provider = codex_usage_provider
        self._metrics_store = MetricsStore(redis_client)
        self._current_run_record: RunRecord | None = None

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

    def _get_coder(self) -> tuple[str, CoderPlugin]:
        """Return ``(coder_name, coder_plugin)`` for the active coder."""
        coder = self.repo_config.coder or self.app_config.daemon.coder
        coder_name = (
            coder.value if isinstance(coder, CoderType) else str(coder)
        )
        return coder_name, self._registry.get(coder_name)

    def _load_current_task_metadata(self) -> tuple[str, str]:
        """Return ``(task_type, complexity)`` for the active task if available."""
        task = self.state.current_task
        if task is None or not task.task_file:
            return ("unknown", "unknown")
        task_path = Path(self.repo_path) / task.task_file
        try:
            lines = task_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return ("unknown", "unknown")

        task_type = "unknown"
        complexity = "unknown"
        for raw_line in lines:
            line = raw_line.strip()
            lower = line.lower()
            if lower.startswith("- type:"):
                value = line.split(":", 1)[1].strip()
                if value:
                    task_type = value
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
            repo_name=self.name,
        )

    async def _save_current_run_record(self, exit_reason: str) -> None:
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
        await self._metrics_store.save(record)

    async def publish_state(self) -> None:
        """Serialize ``self.state`` and write it to Redis."""
        self.state.active = self.repo_config.active
        self.state.last_updated = datetime.now(timezone.utc)
        coder = self.repo_config.coder or self.app_config.daemon.coder
        self.state.coder = coder.value
        if self.repo_config.active:
            provider = (
                self._claude_usage_provider
                if coder != CoderType.CODEX
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
        if not self.repo_config.active:
            data = self.state.model_dump()
            data["state"] = PipelineState.IDLE.value
            payload = RepoState(**data).model_dump_json()
        else:
            payload = self.state.model_dump_json()
        await self.redis.set(f"pipeline:{self.name}", payload)
        if self._old_basename != self.name:
            try:
                old_key = f"pipeline:{self._old_basename}"
                old_data = await self.redis.get(old_key)
                owns_old_key = False
                if old_data:
                    old_state = json.loads(old_data)
                    old_url = old_state.get("url", "")
                    if repo_slug_from_url(old_url) == self.name:
                        await self.redis.delete(old_key)
                        owns_old_key = True
                if owns_old_key:
                    old_upload = f"upload:{self._old_basename}:pending"
                    new_upload = f"upload:{self.name}:pending"
                    if await self.redis.exists(old_upload):
                        await self.redis.renamenx(old_upload, new_upload)
            except Exception:
                pass

    async def _save_cli_log(self, stdout: str, stderr: str, label: str) -> None:
        _MAX_CLI_LOG_BYTES = 64 * 1024  # 64 KB cap per entry
        ts = datetime.now(timezone.utc).isoformat()
        key_latest = f"cli_log:{self.name}:latest"
        key_history = f"cli_log:{self.name}:{ts}"
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
            self.log_event(f"{label}: {first_lines}")

    def log_event(self, event: str) -> None:
        """Append an event to ``state.history`` (capped) and log it."""
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "state": self.state.state.value,
            "event": event,
        }
        self.state.history.append(entry)
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

    async def run_cycle(self) -> None:
        """Advance the state machine by one step."""
        try:
            await self.ensure_repo_cloned()
        except RuntimeError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = str(exc)
            self.log_event(f"ensure_repo_cloned failed: {exc}")
            await self.publish_state()
            return

        if not self._recovered:
            recovery_complete = await self.recover_state()
            if not recovery_complete:
                has_pending = False
                try:
                    raw = await self.redis.get(f"upload:{self.name}:pending")
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

        if not self.preflight():
            await self.publish_state()
            return

        if self.state.state in _TRANSIENT_STATES:
            self.log_event(
                f"resetting stale transient state {self.state.state.value} -> IDLE"
            )
            self.state.state = PipelineState.IDLE

        current = self.state.state
        if current == PipelineState.IDLE:
            await self.handle_idle()
        elif current == PipelineState.WATCH:
            await self.handle_watch()
        elif current == PipelineState.HUNG:
            await self.handle_hung()
        elif current == PipelineState.PAUSED:
            await self.handle_paused()
        elif current == PipelineState.ERROR:
            if self.state.rate_limited_until is not None:
                self.state.state = PipelineState.PAUSED
                self.log_event("Legacy ERROR + rate_limited_until -> PAUSED")
            elif self.app_config.daemon.error_handler_use_ai:
                await self.handle_error()

        if (
            current != PipelineState.ERROR
            and self._error_skip_active
            and self.state.state != PipelineState.ERROR
        ):
            self._error_skip_context = None
            self._error_skip_count = 0
            self._error_skip_active = False

        await self.publish_state()
