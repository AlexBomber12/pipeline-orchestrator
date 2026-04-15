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

Every ``CONFIG_RELOAD_EVERY_CYCLES`` iterations the loop re-reads
``config.yml`` and reconciles the live set of runners with the new
configuration: repositories that have been added get a fresh runner,
repositories that have been removed are dropped, and settings changes
are propagated onto existing runners without restarting the process.
"""

from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import time
from typing import Any

import redis.asyncio as aioredis

from src.config import AppConfig, RepoConfig, load_config, normalize_repo_url
from src.daemon.runner import PipelineRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://localhost:6379/0"

#: Re-read ``config.yml`` every N poll cycles. At the default
#: ``poll_interval_sec=60`` this is roughly one reload per five minutes,
#: which is frequent enough for settings-page edits to take effect without
#: thrashing the filesystem each poll.
CONFIG_RELOAD_EVERY_CYCLES = 5


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


def _build_runner(
    repo: RepoConfig, config: AppConfig, redis_client: Any
) -> PipelineRunner | None:
    """Construct a runner, logging and swallowing init failures."""
    try:
        return PipelineRunner(
            repo_config=repo,
            app_config=config,
            redis_client=redis_client,
        )
    except Exception:
        logger.error(
            "Failed to initialize runner for %s; skipping",
            repo.url,
            exc_info=True,
        )
        return None


def _sync_runners(
    runners: dict[str, PipelineRunner],
    config: AppConfig,
    redis_client: Any,
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
            continue
        runner = _build_runner(repo, config, redis_client)
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
    _sync_runners(runners, config, redis_client)

    last_run: dict[str, float] = {}
    cycle = 0
    while True:
        # Check for config changes every N cycles. We skip the check on
        # the very first iteration because runners were just built from
        # the current config above.
        if cycle > 0 and cycle % CONFIG_RELOAD_EVERY_CYCLES == 0:
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
                    _sync_runners(runners, config, redis_client)

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

        await asyncio.sleep(config.daemon.poll_interval_sec)
        cycle += 1


if __name__ == "__main__":
    asyncio.run(main())
