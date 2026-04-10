"""Daemon entry point.

Boots a :class:`~src.daemon.runner.PipelineRunner` for every repository in
``config.yml`` and drives them in a single poll loop. This is what
``docker compose up daemon`` executes via ``python -m src.daemon.main``.

The loop is deliberately simple: each cycle walks every runner in order,
calls ``run_cycle`` inside a try/except so that one repo's failure cannot
take down the others, then sleeps ``daemon.poll_interval_sec`` before
iterating again. Running with an empty repository list is valid: the
daemon logs a warning and keeps polling so that a future ``config.yml``
edit (re-read on restart) has somewhere to land.
"""

from __future__ import annotations

import asyncio
import logging
import os

import redis.asyncio as aioredis

from src.config import load_config
from src.daemon.runner import PipelineRunner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

DEFAULT_REDIS_URL = "redis://localhost:6379/0"


async def main() -> None:
    """Initialize runners and drive the poll loop forever."""
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

    runners: list[PipelineRunner] = []
    for repo in config.repositories:
        # Per-repo try/except: an invalid URL makes PipelineRunner.__init__
        # raise via get_repo_full_name, which must not take the daemon down.
        try:
            runner = PipelineRunner(
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
            continue
        runners.append(runner)

    while True:
        for runner in runners:
            try:
                await runner.run_cycle()
            except Exception:
                logger.error(
                    "run_cycle failed for %s", runner.name, exc_info=True
                )
        await asyncio.sleep(config.daemon.poll_interval_sec)


if __name__ == "__main__":
    asyncio.run(main())
