"""Explicit, reviewed completion records for work merged outside its task branch."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from src.queue_parser import _PR_ID_RE


class CompletionEvidenceUnavailable(RuntimeError):
    """A recorded completion cannot currently be verified safely."""


class CompletionRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    task_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    merge_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    pull_request: int = Field(gt=0)
    reason: str = Field(min_length=1)


class CompletionManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal[1]
    repository: str
    base_branch: str
    completions: dict[
        Annotated[str, Field(pattern=_PR_ID_RE.pattern)], CompletionRecord
    ]


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate completion manifest field: {key}")
        result[key] = value
    return result


def get_recorded_completions(
    repo_path: str,
    base_branch: str,
    owner_repo: str,
    candidate_pr_ids: set[str],
    *,
    accepted_digests: dict[str, str] | None = None,
) -> set[str]:
    """Verify matching task bytes and commit ancestry, without network or writes.

    The reviewed manifest attests which PR implemented the task. Runtime
    verifies that the exact task is still present and the recorded commit
    belongs to the configured base. A changed task does not inherit a receipt.
    """
    manifest_path = Path(repo_path) / "tasks" / "completions.json"
    try:
        raw = manifest_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return set()
    except (OSError, UnicodeError) as exc:
        raise CompletionEvidenceUnavailable(
            "Cannot read tasks/completions.json"
        ) from exc
    try:
        manifest = CompletionManifest.model_validate(
            json.loads(raw, object_pairs_hook=_unique_fields)
        )
    except ValueError as exc:
        raise CompletionEvidenceUnavailable(
            "Invalid tasks/completions.json; review its schema"
        ) from exc
    if (
        manifest.repository.casefold() != owner_repo.casefold()
        or manifest.base_branch != base_branch
    ):
        raise CompletionEvidenceUnavailable(
            "Completion manifest repository/base does not match this runner"
        )

    completed: set[str] = set()
    for pr_id, record in manifest.completions.items():
        if pr_id not in candidate_pr_ids:
            continue
        task_path = Path(repo_path) / "tasks" / f"{pr_id}.md"
        if accepted_digests is not None and pr_id in accepted_digests:
            digest = accepted_digests[pr_id]
        else:
            try:
                digest = hashlib.sha256(task_path.read_bytes()).hexdigest()
            except OSError as exc:
                raise CompletionEvidenceUnavailable(
                    f"Cannot read task for completion record {pr_id}"
                ) from exc
        if digest != record.task_sha256:
            continue
        try:
            base = _base_commit(repo_path, base_branch)
            result = _git(
                repo_path, "merge-base", "--is-ancestor", record.merge_commit, base
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise CompletionEvidenceUnavailable(
                f"Cannot verify completion commit for {pr_id}"
            ) from exc
        if result.returncode != 0:
            raise CompletionEvidenceUnavailable(
                f"Completion commit for {pr_id} is unavailable or not in {base_branch}; "
                "refresh repository history or review the completion record"
            )
        completed.add(pr_id)
    return completed


def _base_commit(repo_path: str, base_branch: str) -> str:
    for ref in (f"refs/remotes/origin/{base_branch}", f"refs/heads/{base_branch}"):
        result = _git(repo_path, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}")
        if result.returncode == 0:
            return result.stdout.strip()
    raise CompletionEvidenceUnavailable("Cannot resolve base for completion records")


def _git(repo_path: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", repo_path, *args],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
