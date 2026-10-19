"""Chromium verifies the exact-observation HTMX confirmation control."""

import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import redis.asyncio as aioredis
from playwright.sync_api import Page, expect
from src.keyspace import pipeline_state
from src.models import PRInfo, PublicationMigration, RepoState
from src.publication_migration import (
    MigrationChanged,
    MigrationCommand,
    command_key,
    enqueue,
    migration_binding,
)
from src.web import app as web_app


def test_publication_migration_confirmation_posts_displayed_binding(
    page: Page, dashboard_url: str,
) -> None:
    observation = PublicationMigration(
        repo="example/alpha", pr_number=620, head_repo="example/alpha",
        ref="refs/heads/pr-459", head_sha="a" * 40,
        published_at=datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc),
        provenance="github-activity:push", reason="Operator trust anchor required.",
    )
    state = RepoState(url="https://github.com/example/alpha.git",
                      name="example__alpha", publication_migration=observation)
    binding = migration_binding(observation)
    panel = web_app.templates.get_template(
        "components/_publication_migration_panel.html"
    ).render(repo=state, publication_migration_binding=binding,
             publication_migration_confirmable=True, publication_migration_command=None)
    response = page.goto(dashboard_url, wait_until="domcontentloaded")
    assert response is not None and response.status == 200
    page.wait_for_function("Boolean(window.htmx)")
    page.evaluate(
        """html => {
            document.body.insertAdjacentHTML('beforeend', html);
            htmx.process(document.body.lastElementChild);
        }""",
        panel,
    )
    requests = []

    def respond(route) -> None:
        requests.append(route.request.post_data or "")
        route.fulfill(status=202, body='<section data-publication-migration>Queued</section>',
                      content_type="text/html")

    page.route("**/repos/example__alpha/publication-migration/confirm", respond)
    page.on("dialog", lambda dialog: dialog.accept())
    page.get_by_role("button", name="Confirm initial publication floor").click()
    expect(page.locator("[data-publication-migration]")).to_have_text("Queued")
    assert len(requests) == 1
    assert binding in requests[0]


def test_publication_migration_command_is_atomic_in_isolated_redis() -> None:
    """Use DB 15 of the e2e Redis container, never the daemon's DB 0."""
    address = subprocess.run(
        ["docker", "inspect", "--format",
         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
         "pipeline-orchestrator-redis-test"],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout.strip()
    assert address

    async def exercise() -> None:
        redis = aioredis.Redis(host=address, port=6379, db=15, decode_responses=True)
        try:
            await redis.ping()
            await redis.flushdb()
            observation = PublicationMigration(
                repo="example/alpha", pr_number=620, head_repo="example/alpha",
                ref="refs/heads/pr-459", head_sha="a" * 40,
                published_at=datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc),
                provenance="github-activity:push", reason="Operator confirmation required.",
            )
            state = RepoState(url="https://github.com/example/alpha.git",
                              name="example__alpha", publication_migration=observation,
                              current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40))
            await redis.set(pipeline_state(state.name), state.model_dump_json())
            binding = migration_binding(observation)
            first, second = await asyncio.gather(
                enqueue(redis, state.name, binding), enqueue(redis, state.name, binding)
            )
            assert first == second
            assert MigrationCommand.model_validate_json(
                await redis.get(command_key(state.name, binding))
            ).status == "pending"
            changed = observation.model_copy(update={"head_sha": "b" * 40})
            state.publication_migration = changed
            state.current_pr.head_sha = "b" * 40
            await redis.set(pipeline_state(state.name), state.model_dump_json())
            try:
                await enqueue(redis, state.name, migration_binding(changed))
            except MigrationChanged:
                pass
            else:
                raise AssertionError("Changed target bypassed the pending exact-observation command")
        finally:
            await redis.flushdb()
            await redis.aclose()

    # Playwright's synchronous browser fixture owns a loop in pytest's thread.
    # Give the isolated Redis client its own loop and thread for this test.
    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(lambda: asyncio.run(exercise())).result(timeout=30)
