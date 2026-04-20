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
import random
import subprocess
import uuid
from datetime import datetime, timedelta, timezone
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
from src.daemon.selector import SelectionContext, select_coder
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
_STOP_WAIT_TIMEOUT_SEC = 5
_STOP_POLL_INTERVAL_SEC = 0.5

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
        self._user_pause_logged = False

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
        )
        return select_coder(ctx)

    def _get_coder(
        self, *, allow_exploration: bool = True
    ) -> tuple[str, CoderPlugin]:
        """Return ``(coder_name, coder_plugin)`` for the active coder."""
        result = self._select_coder(allow_exploration=allow_exploration)
        if result is not None:
            self.state.coder = result[0]
            return result
        coder = self.repo_config.coder or self.app_config.daemon.coder
        coder_name = coder.value if isinstance(coder, CoderType) else str(coder)
        self.state.coder = coder_name
        return coder_name, self._registry.get(coder_name)

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
            stage="coder",
        )

    @staticmethod
    def _ext_to_language(path: str) -> str | None:
        """Map a file extension to a stable language label for metrics."""
        suffix = Path(path).suffix.lower()
        return _EXTENSION_LANGUAGE_MAP.get(suffix)

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
                f"restore_current_run_record failed for {task.pr_id}: {exc}"
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
        if exit_reason in ("success_merged", "coding_complete", "closed_unmerged"):
            resolved_base_branch = base_branch or self.repo_config.branch or "main"
            stats = diff_stats
            if stats is None:
                stats = self._compute_diff_stats(resolved_base_branch)
            self._apply_diff_stats(record, stats, resolved_base_branch)
        await self._metrics_store.save(record)

    def _track_current_coder_process(
        self, proc: asyncio.subprocess.Process
    ) -> None:
        """Remember the active coder subprocess for user-triggered stop."""
        self._current_coder_process = proc

    async def _refresh_user_paused_from_redis(self) -> None:
        """Merge the persisted ``user_paused`` flag into in-memory state."""
        try:
            raw = await self.redis.get(f"pipeline:{self.name}")
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
        key = f"control:{self.name}:stop"
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
        try:
            await asyncio.wait_for(proc.wait(), timeout=_STOP_WAIT_TIMEOUT_SEC)
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
                self.log_event("User stop requested; terminating current coder")
                await self._terminate_current_coder()
                cli_task.cancel()
                return
            await asyncio.sleep(_STOP_POLL_INTERVAL_SEC)

    async def publish_state(self) -> None:
        """Serialize ``self.state`` and write it to Redis."""
        await self._refresh_user_paused_from_redis()
        self.state.active = self.repo_config.active
        self.state.last_updated = datetime.now(timezone.utc)
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

        await self._refresh_user_paused_from_redis()
        if not self.state.user_paused:
            self._user_pause_logged = False
        if self.state.state == PipelineState.IDLE and self.state.user_paused:
            if not self._user_pause_logged:
                self.log_event("Paused by user, not picking up new tasks")
                self._user_pause_logged = True
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
        if current == PipelineState.IDLE and self.state.user_paused:
            if not self._user_pause_logged:
                self.log_event("Paused by user, not picking up new tasks")
                self._user_pause_logged = True
            await self.publish_state()
            return
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
