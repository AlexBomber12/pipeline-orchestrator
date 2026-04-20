"""Repository operations: clone, fetch, scaffold, sync, queue parsing, uploads.

Mixin methods:
    ensure_repo_cloned       — clone or fetch; retry scaffolding
    sync_to_main             — hard-sync working tree to origin/{branch}
    _parse_base_queue        — parse QUEUE.md from origin/{branch}
    process_pending_uploads  — commit and push uploaded task files
    _delete_upload_if_unchanged — atomic CAS delete for Redis keys
"""

from __future__ import annotations

import json
import logging
import shutil
import subprocess
from pathlib import Path

from src.daemon import git_ops, scaffolder
from src.daemon.git_ops import (
    _FETCH_MISSING_REF_NEEDLE,
    _base_branch_ahead_of_origin,
    _working_tree_dirty,
)
from src.models import QueueTask
from src.queue_parser import parse_queue_text
from src.retry import retry_transient

logger = logging.getLogger(__name__)


def _uploaded_repo_path(filename: str) -> Path:
    """Return the repository-relative path for an uploaded dashboard file."""
    if filename in {"AGENTS.md", "CLAUDE.md"}:
        return Path(filename)
    return Path("tasks") / filename


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
                        f"git fetch: {detail}; will retry scaffold"
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
                    f"local {self.repo_config.branch} ahead of "
                    "origin, re-running scaffold to re-push stranded "
                    "commits"
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
                        f"scaffold_repo created: {', '.join(actions)}"
                    )
            else:
                self.log_event(
                    "scaffold_repo deferred: working tree dirty, letting "
                    "recover_state and preflight run first"
                )

        if self._scaffolded and not _working_tree_dirty(self.repo_path):
            try:
                if scaffolder.ensure_claude_md(
                    self.repo_path, self.repo_config.branch
                ):
                    self.log_event("backfilled CLAUDE.md for legacy repo")
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
        discarded deterministically, guaranteeing QUEUE.md and tasks/ reflect
        the tip of the base branch before ``parse_queue`` reads them.

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
                lambda: git_ops._git(self.repo_path, "fetch", "origin", branch, timeout=60),
                operation_name=f"git fetch origin {branch}",
            )
            git_ops._git(self.repo_path, "checkout", branch)
            git_ops._git(self.repo_path, "reset", "--hard", f"origin/{branch}")
            git_ops._git(self.repo_path, "clean", "-fd")
        except OSError as exc:
            raise RuntimeError(f"sync_to_main OS error: {exc}") from exc

    def _parse_base_queue(
        self, *, strict: bool = False
    ) -> list[QueueTask] | None:
        """Return QUEUE.md parsed from ``origin/{branch}``, or ``None``.

        ``recover_state`` runs before ``preflight``, so the working tree
        may be dirty or checked out on a different branch than
        ``repo_config.branch``:

        - A fresh ``git clone`` lands HEAD on the remote's default
          branch (``origin/HEAD``), which may not match the configured
          base branch. ``ensure_repo_cloned`` does not checkout after
          clone, so an un-guarded ``parse_queue`` would read the default
          branch's QUEUE.md and miss in-flight tasks tracked on a
          different configured branch.
        - A crashed prior cycle may have left the tree on a feature
          branch with uncommitted edits.

        Reading via ``git show origin/{branch}:tasks/QUEUE.md`` sidesteps
        both: it yields the authoritative queue snapshot from the
        configured base branch without touching the working tree, so
        recovery stays non-destructive. Returns ``None`` when the read
        fails (ref missing, timeout, tasks/QUEUE.md absent on base),
        letting the caller translate the failure into a retryable ERROR.

        When *strict* is ``True``, ``parse_queue_text`` runs the full
        validation suite (duplicate IDs/branches, missing deps, cycles).
        A ``QueueValidationError`` is propagated to the caller so
        recovery can transition to ``ERROR`` instead of driving
        execution on a malformed queue.
        """
        branch = self.repo_config.branch
        try:
            result = git_ops._git(
                self.repo_path,
                "show",
                f"origin/{branch}:tasks/QUEUE.md",
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            return None
        return parse_queue_text(result.stdout, strict=strict)

    _DELETE_IF_UNCHANGED_LUA = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

    async def _delete_upload_if_unchanged(self, key: str, expected: bytes | str) -> bool:
        """Delete ``key`` only if its value still matches ``expected``."""
        try:
            result = await self.redis.eval(
                self._DELETE_IF_UNCHANGED_LUA, 1, key, expected,
            )
            return bool(result)
        except Exception:
            logger.warning("%s: CAS delete failed for %s, falling back", self.name, key)
            try:
                current = await self.redis.get(key)
                if current == expected:
                    await self.redis.delete(key)
                    return True
            except Exception:
                pass
            return False

    async def process_pending_uploads(
        self, *, _safe: bool = False,
    ) -> bool | None:
        """Commit and push any files staged by the web upload endpoint.

        Returns ``True`` if an upload was pushed, ``False`` if there was
        nothing pending, or ``None`` if a pending upload failed (caller
        should skip task dispatch so it retries next cycle).

        When *_safe* is ``True`` the error handler skips the destructive
        ``git reset --hard origin/{branch}`` cleanup.  This is used by
        the recovery-failure path where the working tree may contain
        uncommitted crash-recovery work that must not be discarded.
        """
        key = f"upload:{self.name}:pending"
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
            return False

        staging_dir = Path(manifest["staging_dir"]) if "staging_dir" in manifest else Path("/data/uploads") / self.name
        filenames: list[str] = manifest.get("files", [])
        if not filenames or not staging_dir.is_dir():
            logger.warning("%s: upload manifest has no files or staging dir missing", self.name)
            await self.redis.delete(key)
            return False

        branch = self.repo_config.branch
        try:
            tasks_dir = Path(self.repo_path) / "tasks"
            tasks_dir.mkdir(exist_ok=True)
            for fname in filenames:
                src = staging_dir / fname
                if src.is_file():
                    dest = Path(self.repo_path) / _uploaded_repo_path(fname)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(src), str(dest))

            git_ops._git(
                self.repo_path,
                "add",
                *[str(_uploaded_repo_path(fn)) for fn in filenames],
            )
            commit_result = git_ops._git(
                self.repo_path,
                "commit",
                "-m",
                "chore: upload sprint tasks via dashboard",
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
            task_count = sum(1 for name in filenames if name.startswith("PR-") and name.endswith(".md"))
            self.log_event(
                f"Uploaded {task_count} task files to tasks/ and pushed to {branch}"
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, RuntimeError) as exc:
            logger.error("%s: upload git operations failed: %s", self.name, exc)
            self.log_event(f"Upload push failed: {exc}")
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
            return None

        deleted = await self._delete_upload_if_unchanged(key, raw)
        if deleted:
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            return True

        self.log_event("Newer upload pending; blocking dispatch to process it next cycle")
        return None
