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
from src.daemon.runner import PipelineRunner
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
) -> None:
    """Reconcile ``runners`` with ``config.repositories`` in place.

    * New URLs get a freshly constructed :class:`PipelineRunner`.
    * Removed URLs are dropped from the dict.
    * Surviving URLs have their ``repo_config`` and ``app_config``
      swapped in place so settings changes take effect on the next cycle.

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
            runners[key].repo_config = repo
            runners[key].app_config = config
            runners[key].set_usage_providers(
                claude_usage_provider,
                codex_usage_provider,
            )
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


async def main() -> None:
    """Initialize runners and drive the poll loop forever."""
    gh_dir = os.environ.get("GH_CONFIG_DIR")
    if gh_dir:
        os.environ["GH_CONFIG_HOME"] = gh_dir

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
    _sync_runners(
        runners,
        config,
        redis_client,
        claude_usage_provider,
        codex_usage_provider,
        registry,
    )

    last_run: dict[str, float] = {}
    last_config_check = time.monotonic()
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
                    _sync_runners(
                        runners,
                        config,
                        redis_client,
                        claude_usage_provider,
                        codex_usage_provider,
                        registry,
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
            now = time.monotonic()
            interval = runner.repo_config.poll_interval_sec
            if key in last_run and now - last_run[key] < interval:
                continue
            last_run[key] = now
            try:
                await runner.run_cycle()
            except Exception:
                logger.error(
                    "run_cycle failed for %s", runner.name, exc_info=True
                )

        # Clean up last_run entries for removed runners.
        for key in list(last_run.keys()):
            if key not in runners:
                del last_run[key]

        now_after = time.monotonic()
        remaining: list[float] = []
        for key, runner in runners.items():
            if not runner.repo_config.active:
                continue
            due_in = (last_run.get(key, 0.0) + runner.repo_config.poll_interval_sec) - now_after
            remaining.append(max(due_in, 0.0))
        tick = min(remaining) if remaining else config.daemon.poll_interval_sec
        tick = min(tick, config.daemon.poll_interval_sec)
        await asyncio.sleep(max(tick, 1))


if __name__ == "__main__":
    asyncio.run(main())
