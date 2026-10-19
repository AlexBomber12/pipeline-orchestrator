"""Durable, exact-observation command for a parent-version publication floor."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field

from src.keyspace import pipeline_state
from src.models import PublicationMigration, RepoState


class MigrationChanged(ValueError):
    """The displayed migration target or evidence changed."""


class MigrationCommand(BaseModel):
    binding: str
    observation: PublicationMigration
    status: Literal["pending", "deferred", "applied", "failed"] = "pending"
    reason: str = "Waiting for daemon verification."
    requested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def command_key(repo: str, binding: str) -> str:
    return f"control:{repo}:publication_migration:command:{binding}"


def pending_key(repo: str) -> str:
    return f"control:{repo}:publication_migration:pending"


def migration_binding(observation: PublicationMigration) -> str:
    data = observation.model_dump(mode="json", exclude={"reason", "status"})
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def confirmable(observation: PublicationMigration) -> bool:
    return bool(
        observation.status == "legacy" and observation.published_at is not None
        and observation.published_at.tzinfo is not None
        and observation.provenance.startswith("github-activity:")
        and re.fullmatch(r"[^/]+/[^/]+", observation.head_repo) is not None
        and observation.ref.startswith("refs/heads/")
        and re.fullmatch(r"[0-9a-fA-F]{40}", observation.head_sha) is not None
    )


def matches_state(observation: PublicationMigration, state: RepoState) -> bool:
    pr = state.current_pr
    return bool(
        pr is not None and state.name == observation.repo.replace("/", "__")
        and pr.number == observation.pr_number
        and f"refs/heads/{pr.branch}" == observation.ref
        and pr.head_sha.lower() == observation.head_sha.lower()
    )


async def enqueue(redis: Any, repo: str, binding: str) -> MigrationCommand:
    state_key = pipeline_state(repo)
    pointer = pending_key(repo)
    key = command_key(repo, binding)

    async def transaction(pipe: Any) -> MigrationCommand:
        existing = await pipe.get(key)
        if existing:
            receipt = MigrationCommand.model_validate_json(existing)
            if receipt.status != "failed":
                return receipt
        raw = await pipe.get(state_key)
        if not raw:
            raise MigrationChanged("Daemon state is unavailable; refresh the page.")
        state = RepoState.model_validate_json(raw)
        observation = state.publication_migration
        if observation is None or not confirmable(observation) or not matches_state(observation, state):
            raise MigrationChanged("Publication observation cannot be confirmed; refresh the page.")
        if migration_binding(observation) != binding or state.name != repo:
            raise MigrationChanged("Displayed publication observation changed; refresh the page.")
        prior = await pipe.get(pointer)
        if prior:
            raise MigrationChanged("Another publication migration command is pending.")
        command = MigrationCommand(binding=binding, observation=observation.model_copy(deep=True))
        pipe.multi()
        pipe.set(key, command.model_dump_json())
        pipe.set(pointer, binding)
        return command

    return await redis.transaction(transaction, state_key, pointer, key, value_from_callable=True)
