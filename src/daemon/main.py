"""Daemon entry point.

Boots a :class:`~src.daemon.runner.PipelineRunner` for every repository in
``config.yml`` and drives them in a single poll loop. This is what
``docker compose up daemon`` executes via ``python -m src.daemon.main``.

The loop is deliberately simple: each cycle walks every runner in order,
calls ``run_cycle`` inside a try/except so that one repo's failure cannot
take down the others, then sleeps ``daemon.poll_interval_sec`` before
iterating again. Running with an empty repository list is valid: the
daemon logs a warning and keeps polling so that a future ``config.yml``
edit has somewhere to land.

Every ``CONFIG_RELOAD_CYCLES`` daemon-interval-lengths the loop re-reads
``config.yml`` and reconciles the live set of runners with the new
configuration: repositories that have been added get a fresh runner,
repositories that have been removed are dropped, and settings changes
are propagated onto existing runners without restarting the process.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from src.coder_registry import CoderRegistry
from src.coders import build_coder_registry
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin
from src.config import AppConfig, RepoConfig, load_config, normalize_repo_url
from src.daemon.config_watcher import watch_config_file_changes
from src.daemon.runner import PipelineRunner
from src.events.wake import repo_from_channel, subscribe_wake
from src.models import PipelineState
from src.usage import UsageProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://localhost:6379/0"

#: Re-read ``config.yml`` roughly every this many loop cycles (in terms
#: of the daemon-level poll interval). The actual reload cadence is
#: ``CONFIG_RELOAD_CYCLES * daemon.poll_interval_sec`` seconds, so it
#: adapts to both fast and slow deployments.
CONFIG_RELOAD_CYCLES = 5
_DEFERRED_RUNNER_CONFIG_STATES = {
    PipelineState.CODING,
    PipelineState.WATCH,
    PipelineState.FIX,
    PipelineState.MERGE,
    PipelineState.PAUSED,
    PipelineState.HUNG,
}


def _runner_requires_idle_boundary(runner: Any) -> bool:
    """Return whether this runner should defer config changes to IDLE."""
    state = getattr(getattr(runner, "state", None), "state", None)
    return state in _DEFERRED_RUNNER_CONFIG_STATES


def _runner_poll_interval(runner: Any) -> int:
    """Return the next-cycle poll interval for ``runner``.

    IDLE runners use the adaptive ``effective_idle_poll_interval`` so a
    repo that has been quiet long enough drops to the slower cadence
    configured by ``daemon.idle_extended_poll_interval_sec``. WATCH
    runners use ``effective_watch_poll_interval`` so the slow-start
    window after entering WATCH avoids burning GitHub quota on the
    typical Codex/CI response gap (PR-202). Every other state keeps
    the static ``poll_interval_sec`` so active work is not artificially
    throttled. Falls back to the static base if the runner is missing
    the property (e.g. test stubs predating PR-184/PR-202).
    """
    state = getattr(getattr(runner, "state", None), "state", None)
    if state == PipelineState.IDLE and hasattr(
        runner, "effective_idle_poll_interval"
    ):
        return runner.effective_idle_poll_interval
    if state == PipelineState.WATCH and hasattr(
        runner, "effective_watch_poll_interval"
    ):
        return runner.effective_watch_poll_interval
    return runner.repo_config.poll_interval_sec


def _repo_config_differs_only_in_coder(current: RepoConfig, updated: RepoConfig) -> bool:
    """Return whether the repo delta is limited to the coder selection."""
    if current.coder == updated.coder:
        return False
    updated_payload = updated.model_dump()
    updated_payload["coder"] = current.coder
    return current.model_dump() == updated_payload


def _app_config_differs_only_in_repo_coder(
    current: AppConfig,
    updated: AppConfig,
    repo_key: str,
) -> bool:
    """Return whether the app-config delta is limited to one repo's coder field."""
    current_repo = _find_repo_config(current, repo_key)
    updated_repo = _find_repo_config(updated, repo_key)
    if current_repo is None or updated_repo is None:
        return False
    if not _repo_config_differs_only_in_coder(current_repo, updated_repo):
        return False

    current_payload = current.model_dump(mode="json")
    updated_payload = updated.model_dump(mode="json")

    current_repos = current_payload.get("repositories", [])
    updated_repos = updated_payload.get("repositories", [])
    if len(current_repos) != len(updated_repos):
        return False

    for current_repo_payload, updated_repo_payload in zip(current_repos, updated_repos):
        if normalize_repo_url(current_repo_payload["url"]) != normalize_repo_url(
            updated_repo_payload["url"]
        ):
            return False
        if normalize_repo_url(current_repo_payload["url"]) == repo_key:
            current_repo_payload = {
                **current_repo_payload,
                "coder": updated_repo_payload.get("coder"),
            }
        if current_repo_payload != updated_repo_payload:
            return False

    current_payload["repositories"] = updated_repos
    return current_payload == updated_payload


def _setup_git_auth() -> None:
    """Run ``gh auth setup-git`` so git clone/push works automatically."""
    try:
        result = subprocess.run(
            ["gh", "auth", "setup-git"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode == 0:
            logger.info("gh auth setup-git succeeded")
        else:
            logger.warning(
                "gh auth setup-git exited %d: %s",
                result.returncode,
                result.stderr.strip(),
            )
    except subprocess.TimeoutExpired:
        logger.warning("gh auth setup-git timed out after 30s")
    except Exception:
        logger.warning("gh auth setup-git failed", exc_info=True)


def _validate_auth() -> dict[str, bool]:
    """Check whether ``claude`` and ``gh`` CLIs are authenticated."""
    checks: dict[str, bool] = {}
    for name, cmd in [
        ("claude", ["claude", "auth", "status"]),
        ("gh", ["gh", "auth", "status"]),
    ]:
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=5, check=True)
            checks[name] = True
        except Exception:
            checks[name] = False
    logger.info("Auth status: %s", checks)
    return checks


_BREACH_DIR = "/tmp/pipeline-breach"


def _clean_breach_dir() -> None:
    """Remove all stale breach markers on daemon startup."""
    breach_path = Path(_BREACH_DIR)
    if breach_path.is_symlink() or breach_path.exists():
        if breach_path.is_dir() and not breach_path.is_symlink():
            shutil.rmtree(breach_path, ignore_errors=True)
        else:
            breach_path.unlink()
    breach_path.mkdir(parents=True, exist_ok=True)


def _install_statusline_hook(claude_config_dir: str) -> None:
    """Register the statusline hook in Claude CLI settings.

    Merges with existing settings. If a non-default statusLine command is
    already present, logs a warning and preserves it.
    """
    settings_path = Path(claude_config_dir) / "settings.json"
    try:
        existing = json.loads(settings_path.read_text()) if settings_path.is_file() else {}
    except (OSError, json.JSONDecodeError):
        existing = {}

    hook_path = str(Path(__file__).resolve().parent.parent.parent / "scripts" / "statusline_hook.py")
    expected_command = f"python3 {hook_path}"

    current_sl = existing.get("statusLine")
    if isinstance(current_sl, dict):
        current_cmd = current_sl.get("command", "")
        if current_cmd and current_cmd != expected_command:
            logger.warning(
                "statusLine already configured to %r; not overwriting "
                "(set daemon.install_statusline_hook=false to suppress)",
                current_cmd,
            )
            return

    existing["statusLine"] = {
        "type": "command",
        "command": expected_command,
        "padding": 0,
    }
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(json.dumps(existing, indent=2))
    logger.info("Installed statusline hook at %s", settings_path)


def _build_runner(
    repo: RepoConfig,
    config: AppConfig,
    redis_client: Any,
    claude_usage_provider: UsageProvider,
    codex_usage_provider: UsageProvider,
    registry: CoderRegistry,
) -> PipelineRunner | None:
    """Construct a runner, logging and swallowing init failures."""
    try:
        kwargs: dict[str, Any] = {
            "repo_config": repo,
            "app_config": config,
            "redis_client": redis_client,
            "claude_usage_provider": claude_usage_provider,
            "codex_usage_provider": codex_usage_provider,
        }
        if "registry" in inspect.signature(PipelineRunner).parameters:
            kwargs["registry"] = registry
        return PipelineRunner(**kwargs)
    except Exception:
        logger.error(
            "Failed to initialize runner for %s; skipping",
            repo.url,
            exc_info=True,
        )
        return None


def _create_usage_providers(config: AppConfig) -> tuple[UsageProvider, UsageProvider]:
    """Create the shared daemon-level usage providers for the current config."""
    return (
        ClaudePlugin().create_usage_provider(config=config),
        CodexPlugin().create_usage_provider(config=config),
    )


def _sync_runners(
    runners: dict[str, PipelineRunner],
    config: AppConfig,
    redis_client: Any,
    claude_usage_provider: UsageProvider,
    codex_usage_provider: UsageProvider,
    registry: CoderRegistry,
    in_flight: dict[str, asyncio.Task[None]] | None = None,
) -> None:
    """Reconcile ``runners`` with ``config.repositories`` in place.

    * New URLs get a freshly constructed :class:`PipelineRunner`.
    * Removed URLs are dropped from the dict.
    * Surviving URLs whose runner has no cycle in flight have their
      ``repo_config`` and ``app_config`` swapped in place so settings
      changes take effect on the next cycle.
    * Surviving URLs whose runner is mid-cycle route the change through
      ``stage_config_reload`` instead, so the running cycle cannot
      observe a mixed old/new config across awaits. The staged config
      is applied by ``_drain_finished_cycle`` once the cycle completes.

    Runners are keyed by normalized URL so that equivalent forms of the
    same GitHub URL (``.git`` suffix, trailing slash) do not create or
    destroy runners across reloads.
    """
    desired: dict[str, RepoConfig] = {}
    for repo in config.repositories:
        desired[normalize_repo_url(repo.url)] = repo

    # Drop runners whose repos are no longer in the config.
    for key in list(runners.keys()):
        if key not in desired:
            logger.info("Removing runner for %s (no longer in config)", key)
            del runners[key]

    # Add new runners and refresh configs on existing ones.
    for key, repo in desired.items():
        if key in runners:
            runner = runners[key]
            cycle_in_flight = (
                in_flight is not None
                and key in in_flight
                and not in_flight[key].done()
            )
            active_changed = runner.repo_config.active != repo.active
            defer_coder_only_reload = (
                _runner_requires_idle_boundary(runner)
                and _repo_config_differs_only_in_coder(runner.repo_config, repo)
                and _app_config_differs_only_in_repo_coder(
                    runner.app_config,
                    config,
                    key,
                )
            )
            needs_idle_boundary_defer = (
                runner.repo_config.active
                and not active_changed
                and defer_coder_only_reload
            )
            must_defer = cycle_in_flight or needs_idle_boundary_defer
            if must_defer and hasattr(runner, "stage_config_reload"):
                runner.stage_config_reload(
                    repo,
                    config,
                    claude_usage_provider,
                    codex_usage_provider,
                )
            else:
                runner.repo_config = repo
                runner.app_config = config
                runner.set_usage_providers(
                    claude_usage_provider,
                    codex_usage_provider,
                )
                if hasattr(runner, "clear_staged_config_reload"):
                    runner.clear_staged_config_reload()
            continue
        runner = _build_runner(
            repo,
            config,
            redis_client,
            claude_usage_provider,
            codex_usage_provider,
            registry,
        )
        if runner is not None:
            runners[key] = runner
            logger.info("Added runner for %s", repo.url)


def _configs_differ(a: AppConfig, b: AppConfig) -> bool:
    """Return True iff ``a`` and ``b`` serialize to different JSON."""
    return a.model_dump_json() != b.model_dump_json()


def _find_repo_config(config: AppConfig, url: str) -> RepoConfig | None:
    """Return the repo config matching ``url`` in ``config`` if present."""
    needle = normalize_repo_url(url)
    for repo in config.repositories:
        if normalize_repo_url(repo.url) == needle:
            return repo
    return None


async def _close_pubsub(pubsub: Any) -> None:
    """Best-effort close of a pubsub object, ignoring teardown errors."""
    if pubsub is None:
        return
    try:
        await pubsub.aclose()
    except Exception:
        pass


def _apply_pending_in_flight_config(runner: PipelineRunner) -> None:
    """Apply any config staged while a cycle was in flight.

    ``_sync_runners`` defers the in-place ``repo_config``/``app_config``
    swap when a cycle is still running, routing the new config through
    ``stage_config_reload`` so the running cycle cannot observe a mixed
    old/new snapshot across awaits. The staged config is then applied
    once the cycle finishes — either here, after ``_drain_finished_cycle``
    pops the task, or at the next IDLE handler entry via
    ``reload_repo_config_if_dirty``. The two paths are idempotent because
    ``_apply_staged_config_reload`` clears the pending fields after the
    swap.
    """
    apply = getattr(runner, "_apply_staged_config_reload", None)
    if apply is None:
        return
    try:
        apply()
    except Exception:
        logger.error(
            "Failed to apply deferred config for %s",
            getattr(runner, "name", "?"),
            exc_info=True,
        )


def _drain_finished_cycle(
    key: str,
    in_flight: dict[str, asyncio.Task[None]],
    runners: dict[str, PipelineRunner],
) -> bool:
    """Drain ``key``'s completed cycle if present. Return whether it was popped."""
    task = in_flight.get(key)
    if task is None or not task.done():
        return False
    runner = runners.get(key)
    runner_name = runner.name if runner is not None else key
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.error(
            "run_cycle failed for %s", runner_name, exc_info=True
        )
    del in_flight[key]
    if runner is not None:
        _apply_pending_in_flight_config(runner)
    return True


def _drain_finished_cycles(
    in_flight: dict[str, asyncio.Task[None]],
    runners: dict[str, PipelineRunner],
) -> None:
    """Pop completed cycle tasks from ``in_flight`` and log exceptions.

    Cancellations are intentional (config reload removed the runner) and
    are not logged; any other exception is reported with the runner name
    so a crash inside a per-runner task is not silently swallowed. Any
    config that was staged while the cycle was running is applied here
    so the next cycle starts with the new snapshot.
    """
    for key in list(in_flight.keys()):
        _drain_finished_cycle(key, in_flight, runners)


async def _cleanup_in_flight_for_removed(
    in_flight: dict[str, asyncio.Task[None]],
    removed_keys: set[str],
) -> None:
    """Cancel still-running cycles for removed runners and drain finished ones.

    Called after ``_sync_runners`` drops a repo from the live set: a long
    CODING/FIX cycle on the removed runner must not keep a stale task alive
    on the loop. Cancellation propagates ``asyncio.CancelledError`` into the
    coder subprocess wait, which existing handlers already catch.

    Caller cancellation (SIGINT/SIGTERM landing on the daemon's main task
    while we await the cancelled cycle) is re-raised. Catching it here
    would swallow the shutdown signal and leave the daemon unresponsive
    to the first ``Ctrl-C`` whenever it arrived during a config-reload
    cleanup.
    """
    cur = asyncio.current_task()
    initial_cancelling = cur.cancelling() if cur is not None else 0
    for key in list(removed_keys):
        task = in_flight.pop(key, None)
        if task is None:
            continue
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                if (
                    cur is not None
                    and cur.cancelling() > initial_cancelling
                ):
                    raise
            except Exception:
                logger.error(
                    "run_cycle failed for %s (runner removed)",
                    key,
                    exc_info=True,
                )
            continue
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.error(
                "run_cycle failed for %s (runner removed)",
                key,
                exc_info=True,
            )


def _apply_wake_message(
    message: dict[str, Any],
    last_run: dict[str, float],
    slug_to_key: dict[str, str],
    runners: dict[str, PipelineRunner] | None = None,
) -> None:
    """Reset ``last_run`` for the repo named on the wake channel.

    Also clears the runner's adaptive ``_idle_streak`` so the next
    cycle on a long-idle repo polls on the fast cadence again — without
    this, a wake event would set ``last_run`` to 0 but the next sleep
    would still use ``effective_idle_poll_interval`` reflecting the
    prior streak.
    """
    channel = message.get("channel")
    if isinstance(channel, bytes):
        channel = channel.decode("utf-8")
    if not isinstance(channel, str):
        return
    slug = repo_from_channel(channel)
    if slug is None:
        return
    key = slug_to_key.get(slug)
    if key is not None:
        last_run[key] = 0.0
        if runners is not None:
            runner = runners.get(key)
            if runner is not None and hasattr(runner, "reset_idle_streak"):
                runner.reset_idle_streak()


async def _drain_wake_messages(
    pubsub: Any,
    last_run: dict[str, float],
    slug_to_key: dict[str, str],
    runners: dict[str, PipelineRunner] | None = None,
) -> None:
    """Apply any queued wake messages without blocking."""
    while True:
        try:
            extra = await pubsub.get_message(
                ignore_subscribe_messages=True, timeout=0.0
            )
        except Exception:
            return
        if extra is None:
            return
        _apply_wake_message(extra, last_run, slug_to_key, runners)


async def _wait_or_wake(
    pubsub: Any,
    tick: float,
    last_run: dict[str, float],
    slug_to_key: dict[str, str],
    runners: dict[str, PipelineRunner] | None = None,
) -> bool:
    """Sleep ``tick`` seconds or wake early on a wake-channel message.

    Returns True when the pubsub stayed healthy, False when the subscriber
    raised and the caller should rebuild it. When the subscriber errors
    before the tick has elapsed, the function still finishes the tick
    before returning so the caller cannot rebuild the subscriber faster
    than the configured cadence — otherwise a Redis disconnect would
    drive a tight reconnect loop. Falls back to a pure sleep when
    ``pubsub`` is None so the daemon never blocks on a missing subscriber.
    """
    if pubsub is None:
        await asyncio.sleep(tick)
        return True

    sleep_task = asyncio.create_task(asyncio.sleep(tick))
    wake_task = asyncio.create_task(
        pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
    )
    done, pending = await asyncio.wait(
        {sleep_task, wake_task},
        return_when=asyncio.FIRST_COMPLETED,
    )

    healthy = True
    if wake_task in done:
        wake_exc = wake_task.exception()
        if wake_exc is not None:
            healthy = False
        else:
            msg = wake_task.result()
            if msg is not None:
                _apply_wake_message(msg, last_run, slug_to_key, runners)
                await _drain_wake_messages(
                    pubsub, last_run, slug_to_key, runners
                )

    if not healthy and not sleep_task.done():
        # Subscriber errored early; finish the tick so the caller observes
        # the same backoff as a healthy cycle and cannot drive a tight
        # reconnect loop while Redis is unreachable.
        try:
            await sleep_task
        except BaseException:
            pass

    for task in pending:
        if task.done():
            continue
        task.cancel()
        try:
            await task
        except BaseException:
            pass

    if sleep_task.done() and not sleep_task.cancelled():
        sleep_exc = sleep_task.exception()
        if sleep_exc is not None:
            raise sleep_exc

    return healthy


async def main() -> None:
    """Initialize runners and drive the poll loop forever."""
    gh_dir = os.environ.get("GH_CONFIG_DIR")
    if gh_dir:
        os.environ["GH_CONFIG_HOME"] = gh_dir  # pragma: no cover

    _setup_git_auth()
    auth = _validate_auth()
    if not auth.get("claude") and not auth.get("gh"):
        logger.error(
            "No auth configured. Run: docker compose run --rm daemon bash"
        )

    config = load_config()
    registry = build_coder_registry()
    claude_usage_provider, codex_usage_provider = _create_usage_providers(config)

    _clean_breach_dir()
    if config.daemon.install_statusline_hook:
        try:
            _install_statusline_hook(config.auth.claude_config_dir)
        except Exception:
            logger.warning("Failed to install statusline hook", exc_info=True)

    redis_url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)
    redis_client = aioredis.from_url(redis_url, decode_responses=True)

    logger.info(
        "Daemon starting with %d repositories", len(config.repositories)
    )
    if not config.repositories:
        logger.warning(
            "No repositories configured; daemon will idle until config.yml is updated"
        )

    runners: dict[str, PipelineRunner] = {}
    in_flight: dict[str, asyncio.Task[None]] = {}
    _sync_runners(
        runners,
        config,
        redis_client,
        claude_usage_provider,
        codex_usage_provider,
        registry,
        in_flight,
    )

    # Keep a strong reference: the event loop only holds weak references
    # to tasks, so a discarded handle can be garbage-collected mid-await.
    _background_tasks: set[asyncio.Task[None]] = set()
    watcher_task = asyncio.create_task(
        watch_config_file_changes(
            redis_client,
            get_repo_names=lambda: [runner.name for runner in runners.values()],
        )
    )
    _background_tasks.add(watcher_task)
    watcher_task.add_done_callback(_background_tasks.discard)

    last_run: dict[str, float] = {}
    last_config_check = time.monotonic()
    pubsub: Any | None = None
    subscribed_slugs: tuple[str, ...] = ()
    while True:
        now_mono = time.monotonic()
        reload_interval = CONFIG_RELOAD_CYCLES * config.daemon.poll_interval_sec
        if now_mono - last_config_check >= reload_interval:
            last_config_check = now_mono
            try:
                new_config = load_config()
            except Exception:
                logger.error("Failed to reload config.yml", exc_info=True)
            else:
                if _configs_differ(new_config, config):
                    logger.info(
                        "Config change detected; reconciling runners"
                    )
                    config = new_config
                    claude_usage_provider, codex_usage_provider = _create_usage_providers(config)
                    prev_keys = set(runners.keys())
                    _sync_runners(
                        runners,
                        config,
                        redis_client,
                        claude_usage_provider,
                        codex_usage_provider,
                        registry,
                        in_flight,
                    )
                    removed_keys = prev_keys - set(runners.keys())
                    if removed_keys:
                        await _cleanup_in_flight_for_removed(
                            in_flight, removed_keys
                        )

        for key, runner in list(runners.items()):
            if not runner.repo_config.active:
                last_run.pop(key, None)
                try:
                    await runner.publish_state()
                except Exception:
                    logger.error(
                        "publish paused state failed for %s",
                        runner.name,
                        exc_info=True,
                    )
                continue
            existing = in_flight.get(key)
            if existing is not None:
                if not existing.done():
                    # Previous cycle still running; do not pile up another
                    # one on top of it. last_run is left untouched so the
                    # new cycle will be scheduled as soon as the in-flight
                    # task finishes and the next interval is due.
                    continue
                # Drain a cycle that finished between iterations (e.g.
                # during ``await runner.publish_state()`` for an inactive
                # peer earlier in this loop) before scheduling the next
                # one, so any config staged during the cycle is applied
                # and the new cycle starts from the latest snapshot.
                _drain_finished_cycle(key, in_flight, runners)
            now = time.monotonic()
            interval = _runner_poll_interval(runner)
            if key in last_run and now - last_run[key] < interval:
                continue
            # last_run is stamped at scheduling time, not completion time.
            # Otherwise a 30-minute CODING cycle would re-schedule itself
            # the moment it returns and the per-runner interval would be
            # ignored on long jobs.
            last_run[key] = now
            in_flight[key] = asyncio.create_task(runner.run_cycle())

        # Clean up last_run entries for removed runners.
        for key in list(last_run.keys()):
            if key not in runners:
                del last_run[key]

        now_after = time.monotonic()
        remaining: list[float] = []
        slug_to_key: dict[str, str] = {}
        for key, runner in runners.items():
            if not runner.repo_config.active:
                continue
            slug_to_key[runner.name] = key
            due_in = (last_run.get(key, 0.0) + _runner_poll_interval(runner)) - now_after
            remaining.append(max(due_in, 0.0))
        tick = min(remaining) if remaining else config.daemon.poll_interval_sec
        tick = min(tick, config.daemon.poll_interval_sec)

        desired_slugs = tuple(sorted(slug_to_key.keys()))
        if desired_slugs != subscribed_slugs:
            await _close_pubsub(pubsub)
            pubsub = await subscribe_wake(redis_client, desired_slugs)
            if pubsub is None and desired_slugs:
                # Subscribe failed transiently; clear subscribed_slugs so the
                # next iteration retries instead of falling back permanently
                # to timed polling.
                subscribed_slugs = ()
            else:
                subscribed_slugs = desired_slugs

        try:
            healthy = await _wait_or_wake(
                pubsub, max(tick, 1), last_run, slug_to_key, runners
            )
        finally:
            # Wait_or_wake yields to the event loop, giving any newly
            # spawned per-runner cycle tasks a chance to run. Drain whatever
            # finished during the wait so a runner crash is logged in the
            # same iteration it occurred — even if wait_or_wake propagated
            # an exception (e.g. the test sentinel).
            _drain_finished_cycles(in_flight, runners)
        if not healthy:
            await _close_pubsub(pubsub)
            pubsub = None
            subscribed_slugs = ()


if __name__ == "__main__":  # pragma: no cover  # entry point invoked via python -m
    asyncio.run(main())
