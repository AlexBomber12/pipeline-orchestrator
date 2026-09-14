"""Repository operations: clone, fetch, scaffold, sync, queue parsing, uploads.

Mixin methods:
    ensure_repo_cloned       — clone or fetch; retry scaffolding
    sync_to_main             — hard-sync working tree to origin/{branch}
    process_pending_uploads  — commit and push uploaded task files
    _delete_upload_if_unchanged — atomic CAS delete for Redis keys
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
import subprocess
from pathlib import Path

from src.cancellation.storage import task_spec_content_hash
from src.daemon import git_ops, scaffolder
from src.daemon.git_ops import (
    _FETCH_MISSING_REF_NEEDLE,
    _base_branch_ahead_of_origin,
    _working_tree_dirty,
)
from src.keyspace import upload_pending, upload_pending_count
from src.models import TaskStatus
from src.retry import retry_transient
from src.task_admission import invalid_upload_graph_members, validate_admission_graph
from src.task_attempts import AdmissionRejected, TaskAttempt, attempt_key, load_attempt

logger = logging.getLogger(__name__)


def _uploaded_repo_path(filename: str) -> Path:
    """Return the repository-relative path for an uploaded dashboard file."""
    if filename in {"AGENTS.md", "CLAUDE.md"}:
        return Path(filename)
    return Path("tasks") / filename


def _matches_pruned_upload_receipt(
    raw_attempt: str | bytes | None,
    task_id: str,
    filename: str,
    fingerprint: str | None,
) -> bool:
    """Return whether a pending receipt belongs to a pruned upload member."""
    if not raw_attempt or not isinstance(fingerprint, str):
        return False
    attempt = TaskAttempt.model_validate_json(raw_attempt)
    return (
        attempt.task.pr_id == task_id
        and Path(attempt.task.task_file).name == filename
        and attempt.fingerprint == fingerprint
        and attempt.admission_pending
        and not attempt.started
        and not attempt.completed
        and attempt.pr_number is None
        and not attempt.pr_creation_pending
        and not attempt.rejection
    )


class RepoOpsMixin:
    """Repository clone, fetch, scaffold, sync, queue parsing, and uploads."""

    async def ensure_repo_cloned(self) -> None:
        """Clone the repo if missing, otherwise fetch ``origin/{branch}``.

        Also retries scaffolding on every cycle until ``_scaffolded`` is
        set. See ``_scaffolded`` in ``__init__`` for the reasoning.
        """
        path = Path(self.repo_path)
        if not path.exists():
            # ``git clone`` runs before ``self.repo_path`` exists, so it
            # cannot use ``_git`` (which sets ``cwd=repo_path`` and would
            # fail with ``FileNotFoundError`` before git is even invoked).
            def _do_clone() -> None:
                # Remove any partial clone left by a previous failed attempt
                # so git doesn't error with "destination path already exists".
                if path.exists():
                    shutil.rmtree(path)
                subprocess.run(
                    ["git", "clone", self.repo_config.url, self.repo_path],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=True,
                )
            try:
                retry_transient(_do_clone, operation_name="git clone")
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip()
                raise RuntimeError(f"git clone failed: {detail}") from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("git clone timed out") from exc
        else:
            fetch_missing_ref = False
            try:
                retry_transient(
                    lambda: git_ops._git(
                        self.repo_path,
                        "fetch",
                        "--prune",
                        "origin",
                        self.repo_config.branch,
                        timeout=60,
                    ),
                    operation_name=f"git fetch origin {self.repo_config.branch}",
                )
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip()
                if _FETCH_MISSING_REF_NEEDLE in detail.lower():
                    fetch_missing_ref = True
                    self.log_event(
                        f"[INFRA] git fetch: {detail}; will retry "
                        f"scaffold."
                    )
                else:
                    raise RuntimeError(
                        f"git fetch failed: {detail}"
                    ) from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("git fetch timed out") from exc

            if fetch_missing_ref:
                self._scaffolded = False
            elif self._scaffolded and _base_branch_ahead_of_origin(
                self.repo_path, self.repo_config.branch
            ):
                self._scaffolded = False
                self.log_event(
                    f"[INFRA] local {self.repo_config.branch} ahead of "
                    f"origin, re-running scaffold to re-push stranded "
                    f"commits."
                )

        if not self._scaffolded:
            if not path.exists() or not _working_tree_dirty(self.repo_path):
                try:
                    actions = scaffolder.scaffold_repo(
                        self.repo_path, self.repo_config.branch
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"scaffold_repo failed: {exc}"
                    ) from exc
                self._scaffolded = True
                if actions:
                    self.log_event(
                        f"[INFRA] scaffold_repo created: "
                        f"{', '.join(actions)}."
                    )
            else:
                self.log_event(
                    "[INFRA] scaffold_repo deferred: working tree dirty, "
                    "letting recover_state and preflight run first."
                )

        if self._scaffolded and not _working_tree_dirty(self.repo_path):
            try:
                if scaffolder.ensure_claude_md(
                    self.repo_path, self.repo_config.branch
                ):
                    self.log_event(
                        "[INFRA] backfilled CLAUDE.md for legacy repo."
                    )
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                OSError,
            ) as exc:
                raise RuntimeError(
                    f"CLAUDE.md backfill failed: {exc}"
                ) from exc

    def sync_to_main(self) -> None:
        """Hard-sync the working tree to ``origin/{branch}``.

        Only safe to call when the runner is IDLE (no active Claude working
        branch to clobber). Uses ``git reset --hard`` instead of ``git pull``
        so that any stray local modifications from a prior crashed cycle are
        discarded deterministically, guaranteeing tasks/ reflects the tip
        of the base branch before the IDLE selector reads it.

        Raises the underlying ``subprocess`` exception on failure so the
        caller can translate it into ERROR state with appropriate context.
        ``OSError`` (missing git binary, missing cwd) is translated to
        ``RuntimeError`` so it cannot escape to ``daemon.main``'s generic
        handler without the runner's state being updated to ERROR by the
        caller.
        """
        branch = self.repo_config.branch
        try:
            retry_transient(
                lambda: git_ops._git(
                    self.repo_path, "fetch", "--prune", "origin", branch, timeout=60
                ),
                operation_name=f"git fetch origin {branch}",
            )
            git_ops._git(self.repo_path, "checkout", branch)
            git_ops._git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            git_ops._git(self.repo_path, "clean", "-fd")
        except OSError as exc:
            raise RuntimeError(f"sync_to_main OS error: {exc}") from exc

    _DELETE_IF_UNCHANGED_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    if KEYS[2] then
        redis.call("del", KEYS[2])
    end
    return redis.call("del", KEYS[1])
end
return 0
"""

    _DELETE_PENDING_COUNT_IF_MANIFEST_MATCHES_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[2])
end
return 0
"""

    async def _delete_upload_if_unchanged(
        self,
        key: str,
        expected: bytes | str,
        *,
        also_delete_key: str | None = None,
    ) -> bool:
        """Delete ``key`` only if its value still matches ``expected``."""
        try:
            keys = (key,) if also_delete_key is None else (key, also_delete_key)
            result = await self.redis.eval(
                self._DELETE_IF_UNCHANGED_LUA,
                len(keys),
                *keys,
                expected,
            )
            return bool(result)
        except Exception:
            logger.warning("%s: CAS delete failed for %s, falling back", self.name, key)
            try:
                current = await self.redis.get(key)
                if current == expected:
                    if also_delete_key is not None:
                        await self.redis.delete(also_delete_key)
                    await self.redis.delete(key)
                    return True
            except Exception:
                pass
            return False

    async def _clear_upload_pending_count_if_manifest_matches(
        self, manifest_key: str, expected_manifest: bytes | str
    ) -> None:
        """Clear the visible pending count unless a newer upload replaced it."""
        try:
            await self.redis.eval(
                self._DELETE_PENDING_COUNT_IF_MANIFEST_MATCHES_LUA,
                2,
                manifest_key,
                upload_pending_count(self.name),
                expected_manifest,
            )
        except Exception:
            logger.warning("%s: failed clearing upload pending count", self.name)

    async def _delete_upload_pending_count(self) -> None:
        """Best-effort cleanup for the dashboard-only pending count key."""
        try:
            await self.redis.delete(upload_pending_count(self.name))
        except Exception:
            logger.warning("%s: failed deleting upload pending count", self.name)

    async def _discard_invalid_upload(
        self, key: str, raw: bytes | str, staging_dir: Path, reason: str
    ) -> bool | None:
        """Retire only the rejected submission, never a concurrently staged batch."""
        async def discard(pipe):
            if await pipe.get(key) != raw:
                return False
            pipe.multi()
            pipe.delete(key, upload_pending_count(self.name))
            return True

        try:
            discarded = await self.redis.transaction(discard, key, value_from_callable=True)
        except Exception as exc:
            self.log_event(f"[INFRA] Invalid upload acknowledgement deferred ({type(exc).__name__}).")
            return None
        if not discarded:
            return None
        shutil.rmtree(str(staging_dir), ignore_errors=True)
        self.log_event(f"[INFRA] Discarded invalid upload batch: {reason}. Submit corrected files again.")
        return False

    async def _discard_invalid_upload_member(
        self,
        key: str,
        raw: bytes | str,
        staging_dir: Path,
        manifest: dict,
        filename: str,
        reason: str,
    ) -> bool | None:
        """Remove one invalid staged file while preserving unrelated pending uploads."""
        files = [name for name in manifest.get("files", []) if name != filename]
        task_id = Path(filename).stem
        updated = dict(manifest)
        updated["files"] = files
        for field in ("task_hashes", "rejection_tokens", "prior_spec_files"):
            values = updated.get(field)
            if isinstance(values, dict):
                values = dict(values)
                values.pop(task_id, None)
                updated[field] = values
        updated_raw = json.dumps(updated)
        task_hashes = manifest.get("task_hashes", {})
        upload_fingerprint = (
            task_hashes.get(task_id) if isinstance(task_hashes, dict) else None
        )
        receipt_key = attempt_key(self.name, task_id)

        async def discard(pipe):
            if await pipe.get(key) != raw:
                return "changed"
            receipt_raw = await pipe.get(receipt_key)
            delete_receipt = _matches_pruned_upload_receipt(
                receipt_raw,
                task_id,
                filename,
                upload_fingerprint,
            )
            pipe.multi()
            if files:
                pipe.set(key, updated_raw)
                pipe.set(upload_pending_count(self.name), str(len(files)))
            if delete_receipt:
                pipe.delete(receipt_key)
            if files:
                return "retained"
            pipe.delete(key, upload_pending_count(self.name))
            return "discarded"

        try:
            result = await self.redis.transaction(
                discard,
                key,
                receipt_key,
                value_from_callable=True,
            )
        except Exception as exc:
            self.log_event(f"[INFRA] Invalid upload acknowledgement deferred ({type(exc).__name__}).")
            return None
        if result == "changed":
            return None
        try:
            (staging_dir / filename).unlink(missing_ok=True)
        except Exception:
            logger.warning("%s: failed removing invalid upload member %s", self.name, filename)
        if result == "discarded":
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            self.log_event(f"[INFRA] Discarded invalid upload batch: {reason}. Submit corrected files again.")
            return False
        self.log_event(
            f"[INFRA] Discarded invalid upload member {filename}: {reason}. "
            "Remaining staged files stay pending."
        )
        return None

    async def _discard_invalid_upload_members(
        self,
        key: str,
        raw: bytes | str,
        staging_dir: Path,
        manifest: dict,
        filenames: set[str],
        reason: str,
    ) -> bool | None:
        """Remove invalid staged task files while preserving unrelated uploads."""
        files = [name for name in manifest.get("files", []) if name not in filenames]
        task_ids = {Path(filename).stem for filename in filenames}
        updated = dict(manifest)
        updated["files"] = files
        for field in ("task_hashes", "rejection_tokens", "prior_spec_files"):
            values = updated.get(field)
            if isinstance(values, dict):
                values = dict(values)
                for task_id in task_ids:
                    values.pop(task_id, None)
                updated[field] = values
        updated_raw = json.dumps(updated)
        task_hashes = manifest.get("task_hashes", {})
        receipt_keys = {
            task_id: attempt_key(self.name, task_id) for task_id in task_ids
        }

        async def discard(pipe):
            if await pipe.get(key) != raw:
                return "changed"
            delete_receipt_keys = []
            for task_id, receipt_key in sorted(receipt_keys.items()):
                upload_fingerprint = (
                    task_hashes.get(task_id)
                    if isinstance(task_hashes, dict)
                    else None
                )
                receipt_raw = await pipe.get(receipt_key)
                if _matches_pruned_upload_receipt(
                    receipt_raw,
                    task_id,
                    f"{task_id}.md",
                    upload_fingerprint,
                ):
                    delete_receipt_keys.append(receipt_key)
            pipe.multi()
            if files:
                pipe.set(key, updated_raw)
                pipe.set(upload_pending_count(self.name), str(len(files)))
            if delete_receipt_keys:
                pipe.delete(*delete_receipt_keys)
            if files:
                return "retained"
            pipe.delete(key, upload_pending_count(self.name))
            return "discarded"

        try:
            result = await self.redis.transaction(
                discard,
                key,
                *receipt_keys.values(),
                value_from_callable=True,
            )
        except Exception as exc:
            self.log_event(f"[INFRA] Invalid upload acknowledgement deferred ({type(exc).__name__}).")
            return None
        if result == "changed":
            return None
        for filename in filenames:
            try:
                (staging_dir / filename).unlink(missing_ok=True)
            except Exception:
                logger.warning("%s: failed removing invalid upload member %s", self.name, filename)
        if result == "discarded":
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            self.log_event(f"[INFRA] Discarded invalid upload batch: {reason}. Submit corrected files again.")
            return False
        self.log_event(
            f"[INFRA] Discarded invalid upload members {', '.join(sorted(filenames))}: {reason}. "
            "Remaining staged files stay pending."
        )
        return None

    async def process_pending_uploads(
        self, *, _safe: bool = False,
    ) -> bool | None:
        """Commit and push any files staged by the web upload endpoint.

        Returns ``True`` if an upload was pushed, ``False`` if there was
        nothing pending or an invalid batch was discarded, or ``None`` if a pending upload failed (caller
        should skip task dispatch so it retries next cycle).

        When *_safe* is ``True`` the error handler skips the destructive
        ``git reset --hard origin/{branch}`` cleanup.  This is used by
        the recovery-failure path where the working tree may contain
        uncommitted crash-recovery work that must not be discarded.
        """
        key = upload_pending(self.name)
        try:
            raw = await self.redis.get(key)
        except Exception:
            logger.warning("%s: Redis error checking pending uploads", self.name)
            return None
        if not raw:
            return False

        try:
            manifest = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.error("%s: corrupt upload manifest, discarding", self.name)
            await self.redis.delete(key)
            await self._delete_upload_pending_count()
            return False

        staging_dir = Path(manifest["staging_dir"]) if "staging_dir" in manifest else Path("/data/uploads") / self.name
        filenames: list[str] = manifest.get("files", [])
        commit_subject = manifest.get("commit_subject")
        include_commit_body = isinstance(commit_subject, str) and bool(
            commit_subject.strip()
        )
        if not isinstance(commit_subject, str) or not commit_subject.strip():
            commit_subject = "chore: upload sprint tasks via dashboard"
        else:
            commit_subject = commit_subject.strip()
        if not filenames or not staging_dir.is_dir():
            logger.warning("%s: upload manifest has no files or staging dir missing", self.name)
            await self.redis.delete(key)
            await self._delete_upload_pending_count()
            return False

        # tasks/QUEUE.md is gitignored (PR-181) and regenerated on each
        # IDLE cycle from PR-*.md headers. Drop any uploaded copy from
        # the manifest so we never try to ``git add`` an ignored path,
        # which would otherwise abort the whole upload and block the
        # rest of the dashboard's task files from landing.
        stageable_filenames = [fn for fn in filenames if fn != "QUEUE.md"]
        if len(stageable_filenames) != len(filenames):
            self.log_event(
                "[INFRA] Skipping QUEUE.md from upload: gitignored, "
                "regenerated by daemon from task headers."
            )
        if not stageable_filenames:
            await self._delete_upload_if_unchanged(
                key,
                raw,
                also_delete_key=upload_pending_count(self.name),
            )
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            return False

        branch = self.repo_config.branch
        try:
            tasks_dir = Path(self.repo_path) / "tasks"
            tasks_dir.mkdir(exist_ok=True)
            prior_files = manifest.get("prior_spec_files", {})
            for fname in stageable_filenames:
                if fname.startswith("PR-") and fname.endswith(".md") and Path(fname).stem in prior_files:
                    task_id = Path(fname).stem
                    target = tasks_dir / fname
                    current = await load_attempt(self.redis, self.name, task_id)
                    current_hash = task_spec_content_hash(target.read_text()) if target.is_file() else None
                    incoming_hash = task_spec_content_hash((staging_dir / fname).read_text())
                    if current_hash != prior_files[task_id] and not (
                        current and current.fingerprint == incoming_hash and current_hash == incoming_hash
                        and current.previous_rejection == manifest.get("rejection_tokens", {}).get(task_id)
                    ):
                        return await self._discard_invalid_upload_member(
                            key,
                            raw,
                            staging_dir,
                            manifest,
                            fname,
                            "Task changed or was deleted after upload; submit it again.",
                        )
            task_uploads = [
                staging_dir / name for name in stageable_filenames
                if name.startswith("PR-") and name.endswith(".md")
            ]
            try:
                validate_admission_graph(Path(self.repo_path), task_uploads)
            except AdmissionRejected as exc:
                invalid_graph_members = invalid_upload_graph_members(Path(self.repo_path), task_uploads)
                if invalid_graph_members:
                    return await self._discard_invalid_upload_members(
                        key,
                        raw,
                        staging_dir,
                        manifest,
                        invalid_graph_members,
                        str(exc),
                    )
                raise
            await self._snapshot_accepted_specs()
            admissions = []
            validated = []
            available_ids = {path.stem for path in tasks_dir.glob("PR-*.md")} | {
                Path(name).stem for name in stageable_filenames if name.startswith("PR-")
            }
            for fname in stageable_filenames:
                if fname.startswith("PR-") and fname.endswith(".md"):
                    task_id = Path(fname).stem
                    target = tasks_dir / fname
                    current = await load_attempt(self.redis, self.name, task_id)
                    current_hash = task_spec_content_hash(target.read_text()) if target.is_file() else None
                    incoming_hash = task_spec_content_hash((staging_dir / fname).read_text())
                    if task_id in prior_files:
                        if current_hash != prior_files[task_id] and not (
                            current and current.fingerprint == incoming_hash and current_hash == incoming_hash
                            and current.previous_rejection == manifest.get("rejection_tokens", {}).get(task_id)
                        ):
                            return await self._discard_invalid_upload_member(
                                key,
                                raw,
                                staging_dir,
                                manifest,
                                fname,
                                "Task changed or was deleted after upload; submit it again.",
                            )
                    elif current and not target.is_file():
                        raise AdmissionRejected("Task was deleted; an old pending upload cannot recreate it.")
                    try:
                        validated.append(
                            await self._validate_admission(
                                staging_dir / fname,
                                token=manifest.get("rejection_tokens", {}).get(Path(fname).stem),
                                upload=True,
                                available_ids=available_ids,
                            )
                        )
                    except AdmissionRejected as exc:
                        return await self._discard_invalid_upload_member(
                            key,
                            raw,
                            staging_dir,
                            manifest,
                            fname,
                            str(exc),
                        )
            # Validate the whole batch before reserving any replacement. An
            # invalid later file must not orphan an earlier pending receipt.
            for previous, candidate in validated:
                admission = await self._reserve_validated_admission(previous, candidate, upload=True)
                if admission:
                    admissions.append(admission)
            for fname in stageable_filenames:
                src = staging_dir / fname
                if src.is_file():
                    dest = Path(self.repo_path) / _uploaded_repo_path(fname)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.exists():
                        old_hash = hashlib.sha256(dest.read_bytes()).hexdigest()
                        new_hash = hashlib.sha256(src.read_bytes()).hexdigest()
                        warning = (
                            "Upload overwrite warning: "
                            f"{_uploaded_repo_path(fname)} existing_sha256={old_hash} "
                            f"new_sha256={new_hash}"
                        )
                        logger.warning("%s: %s", self.name, warning)
                        self.log_event(f"[INFRA] {warning}.")
                    shutil.copy2(str(src), str(dest))

            git_ops._git(
                self.repo_path,
                "add",
                *[str(_uploaded_repo_path(fn)) for fn in stageable_filenames],
            )
            commit_body = "\n".join(
                _uploaded_repo_path(fn).as_posix()
                for fn in sorted(stageable_filenames)
            )
            commit_args = ["commit", "-m", commit_subject]
            if include_commit_body and commit_body:
                commit_args.extend(["-m", commit_body])
            commit_result = git_ops._git(
                self.repo_path,
                *commit_args,
                check=False,
            )
            if commit_result.returncode != 0:
                combined = f"{commit_result.stderr}\n{commit_result.stdout}"
                if "nothing to commit" not in combined:
                    raise RuntimeError(combined.strip())
            retry_transient(
                lambda: git_ops._git(self.repo_path, "push", "origin", branch, timeout=60),
                operation_name=f"git push origin {branch}",
            )
            for attempt in admissions:
                await self._finish_admission(attempt)
            task_count = len(
                {
                    name
                    for name in stageable_filenames
                    if name.startswith("PR-") and name.endswith(".md")
                }
            )
            self.log_event(
                f"[INFRA] Uploaded {task_count} task files to tasks/ "
                f"and pushed to {branch}."
            )
            self._clear_canceled_in_snapshot({attempt.task.pr_id for attempt in admissions})
        except AdmissionRejected as exc:
            return await self._discard_invalid_upload(key, raw, staging_dir, str(exc))
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, RuntimeError, ValueError) as exc:
            logger.error("%s: upload git operations failed: %s", self.name, exc)
            self.log_event(f"[INFRA] Upload push failed: {exc}.")
            if not _safe:
                try:
                    git_ops._git(
                        self.repo_path,
                        "reset",
                        "--hard",
                        f"origin/{branch}",
                        check=False,
                    )
                except Exception:
                    pass
            await self._clear_upload_pending_count_if_manifest_matches(key, raw)
            return None

        deleted = await self._delete_upload_if_unchanged(
            key,
            raw,
            also_delete_key=upload_pending_count(self.name),
        )
        if deleted:
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            return True

        # The upload has already been committed, pushed, and admitted. A newer
        # manifest only means the cleanup CAS lost a race; leaving it queued
        # must not make IDLE skip dispatch from the committed base branch.
        self.log_event(
            "[INFRA] Newer upload pending; completed current upload "
            "and leaving newer upload queued."
        )
        return True

    def _clear_canceled_in_snapshot(self, uploaded_pr_ids: set[str]) -> None:
        """Flip ERROR → TODO in ``state.current_queue`` for re-uploads.

        The user re-uploads a task file to retry a previously-crashed
        task. ``crashed_task_pr_ids`` and ``recovered_task_pr_ids`` are
        already pruned by the caller; mirroring the change in the
        in-memory snapshot keeps the dashboard consistent until the
        next IDLE cycle rebuilds the snapshot from headers.

        Reassigns ``state.current_queue`` after the mutation so the
        ``RepoState.__setattr__`` hook re-stamps
        ``current_queue_snapshot_at``; without that, the
        ``/api/repo/{name}/queue`` ``snapshot_at`` change token would
        stay pinned to the pre-upload time and clients could miss the
        ERROR→TODO transition until the next IDLE rebuild.
        """
        snapshot = self.state.current_queue
        if not snapshot:
            return
        changed = False
        for index, queued in enumerate(snapshot):
            if (
                queued.pr_id in uploaded_pr_ids
                and queued.status == TaskStatus.ERROR
            ):
                snapshot[index] = queued.model_copy(
                    update={"status": TaskStatus.TODO}
                )
                changed = True
        if changed:
            self.state.current_queue = snapshot
