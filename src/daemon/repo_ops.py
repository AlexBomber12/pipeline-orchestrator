"""Repository operations: clone, fetch, scaffold, sync, queue parsing, uploads.

Mixin methods:
    ensure_repo_cloned       — clone or fetch; retry scaffolding
    sync_to_main             — hard-sync working tree to origin/{branch}
    _parse_base_queue        — parse QUEUE.md from the local working tree
    process_pending_uploads  — commit and push uploaded task files
    _delete_upload_if_unchanged — atomic CAS delete for Redis keys
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
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

    def _origin_queue_md_tracked(self) -> bool | None:
        """Return ``True``/``False`` for tracked-on-origin, ``None`` on probe failure.

        PR-181 untracks ``tasks/QUEUE.md`` in this repo, but managed
        repos that have not yet adopted that migration still carry the
        file in tree. ``.gitignore`` does not retroactively untrack
        files, so the daemon must detect tracked-QUEUE repos and steer
        clear of the local-write / working-tree-read paths that PR-181's
        design otherwise relies on.

        Tristate on purpose. A transient ``cat-file`` failure (timeout,
        OSError) is genuinely indeterminate: collapsing it to ``False``
        would make legacy repos look post-PR-181 and route recovery into
        the working-tree path, where a feature-branch checkout (or a
        missing ``tasks/QUEUE.md``) would yield a stale/empty queue and
        detach the daemon from real in-flight DOING work. Returning
        ``None`` lets each caller pick its own conservative fallback:
        recovery escalates to ERROR and retries; the IDLE/merge handlers
        skip their writes (treat as tracked) and self-heal next cycle.

        A non-zero ``returncode`` (file genuinely absent on origin) is
        still a definitive ``False``.
        """
        branch = self.repo_config.branch
        try:
            result = git_ops._git(
                self.repo_path,
                "cat-file",
                "-e",
                f"origin/{branch}:tasks/QUEUE.md",
                check=False,
                timeout=10,
            )
        except (subprocess.TimeoutExpired, OSError):
            return None
        return result.returncode == 0

    def _parse_base_queue(
        self, *, strict: bool = False, queue_from_origin: bool | None = None,
    ) -> list[QueueTask] | None:
        """Return QUEUE.md parsed for the active base ref, or ``None``.

        Post-PR-181 repos have ``tasks/QUEUE.md`` gitignored: the daemon
        regenerates it from structured task headers each IDLE cycle and
        never pushes it, so the only authoritative copy is the local
        working tree. Pre-PR-181 (legacy) repos still track the file on
        ``origin/{branch}``, and recovery may run while the working tree
        is checked out on a feature branch left behind by an interrupted
        cycle. Reading from the working tree there would return a
        branch-local snapshot and could resurrect or misclassify
        in-flight work; instead we read directly from
        ``origin/{branch}`` whenever the file is tracked there.

        Returns ``None`` when no snapshot can be parsed (fresh clone,
        unreadable file, ``git show`` failure). The caller translates
        that into a retryable ERROR so the next cycle's regenerate /
        sync brings the snapshot back into reach.

        When *strict* is ``True``, ``parse_queue_text`` runs the full
        validation suite and a ``QueueValidationError`` is propagated.

        When *queue_from_origin* is provided, the caller has already
        run ``_origin_queue_md_tracked()`` and wants both the parse
        source and any downstream ghost-filter decision to come from
        the same probe result. This avoids a transient probe failure
        between two independent probes from desyncing the two
        decisions (e.g., reading from origin while the caller still
        applies the local-existence ghost filter, which would drop
        real DOING/DONE entries on a feature-branch checkout). When
        ``None``, the helper probes internally; an indeterminate probe
        result (``None``) is treated as "no snapshot can be parsed" so
        the helper does not silently fall back to a possibly-stale
        working-tree copy on legacy repos.
        """
        if queue_from_origin is None:
            queue_from_origin = self._origin_queue_md_tracked()
            if queue_from_origin is None:
                return None
        if queue_from_origin:
            branch = self.repo_config.branch
            try:
                result = git_ops._git(
                    self.repo_path,
                    "show",
                    f"origin/{branch}:tasks/QUEUE.md",
                    check=False,
                    timeout=15,
                )
            except (subprocess.TimeoutExpired, OSError):
                return None
            if result.returncode != 0:
                return None
            return parse_queue_text(result.stdout, strict=strict)

        queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
        try:
            content = queue_path.read_text(encoding="utf-8")
        except OSError:
            return None
        return parse_queue_text(content, strict=strict)

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

        # tasks/QUEUE.md is gitignored (PR-181) and regenerated on each
        # IDLE cycle from PR-*.md headers. Drop any uploaded copy from
        # the manifest so we never try to ``git add`` an ignored path,
        # which would otherwise abort the whole upload and block the
        # rest of the dashboard's task files from landing.
        stageable_filenames = [fn for fn in filenames if fn != "QUEUE.md"]
        if len(stageable_filenames) != len(filenames):
            self.log_event(
                "Skipping QUEUE.md from upload: gitignored, "
                "regenerated by daemon from task headers"
            )
        if not stageable_filenames:
            await self._delete_upload_if_unchanged(key, raw)
            shutil.rmtree(str(staging_dir), ignore_errors=True)
            return False

        branch = self.repo_config.branch
        try:
            tasks_dir = Path(self.repo_path) / "tasks"
            tasks_dir.mkdir(exist_ok=True)
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
                        self.log_event(warning)
                    shutil.copy2(str(src), str(dest))

            git_ops._git(
                self.repo_path,
                "add",
                *[str(_uploaded_repo_path(fn)) for fn in stageable_filenames],
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
            task_count = len(
                {
                    name
                    for name in stageable_filenames
                    if name.startswith("PR-") and name.endswith(".md")
                }
            )
            self.log_event(
                f"Uploaded {task_count} task files to tasks/ and pushed to {branch}"
            )
            # PR-186: Re-uploading a task file is the user's signal to retry
            # a previously-crashed task. Clear the in-memory CANCELED mark
            # for any uploaded PR-id so the next IDLE cycle picks the task
            # again instead of treating it as still crashed. Also flip the
            # working-tree ``tasks/QUEUE.md`` row from CANCELED to TODO for
            # those PR-ids: the next IDLE regenerates the queue from
            # headers, but a daemon restart between this upload and that
            # regeneration would otherwise see the stale CANCELED row,
            # rehydrate ``_crashed_task_pr_ids`` from it, and re-cancel the
            # task — losing the retry signal until the user uploads again.
            uploaded_pr_ids = {
                Path(name).stem
                for name in stageable_filenames
                if name.startswith("PR-") and name.endswith(".md")
            }
            crashed_pr_ids = getattr(self, "_crashed_task_pr_ids", None)
            if crashed_pr_ids:
                crashed_pr_ids.difference_update(uploaded_pr_ids)
            if uploaded_pr_ids:
                self._clear_canceled_queue_rows(uploaded_pr_ids)
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

    def _clear_canceled_queue_rows(self, uploaded_pr_ids: set[str]) -> None:
        """Flip CANCELED → TODO in the working-tree QUEUE.md for re-uploads.

        Bridges the gap between an upload landing (which the user uses to
        retry a previously-crashed task) and the next IDLE regeneration of
        ``tasks/QUEUE.md``. Without this, a daemon restart in that window
        would re-read the stale CANCELED row from QUEUE.md and re-add the
        PR-id to ``_crashed_task_pr_ids`` via ``recover_state``'s
        rehydrate, silently dropping the retry signal.

        Best-effort: missing or unreadable QUEUE.md is a no-op (the next
        IDLE cycle regenerates from scratch and there is no stale row to
        clear). Legacy tracked-QUEUE repos see only a working-tree change
        here; recovery on those repos still parses ``origin/{branch}``,
        so the user must also keep the upstream queue in sync as before.
        """
        queue_path = Path(self.repo_path) / "tasks" / "QUEUE.md"
        try:
            text = queue_path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return

        new_lines: list[str] = []
        current_pr_id: str | None = None
        changed = False
        header_re = re.compile(r"^##\s+(PR-[A-Za-z0-9_.-]+)\b")
        status_re = re.compile(r"^(\s*-\s*Status\s*:\s*)CANCELED(\s*)$")
        for line in text.splitlines(keepends=True):
            stripped = line.rstrip("\n").rstrip("\r")
            header_match = header_re.match(stripped)
            if header_match:
                current_pr_id = header_match.group(1)
                new_lines.append(line)
                continue
            if current_pr_id in uploaded_pr_ids:
                status_match = status_re.match(stripped)
                if status_match:
                    eol = line[len(stripped):]
                    new_lines.append(
                        f"{status_match.group(1)}TODO{status_match.group(2)}{eol}"
                    )
                    changed = True
                    continue
            new_lines.append(line)

        if changed:
            try:
                queue_path.write_text("".join(new_lines), encoding="utf-8")
            except OSError as exc:
                self.log_event(
                    f"Failed to clear stale CANCELED rows in QUEUE.md: {exc}"
                )
