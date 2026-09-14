"""Daemon admission receipts reconcile Git writes and Redis independently."""

from __future__ import annotations

import hashlib
import subprocess
import tempfile
from pathlib import Path

from src.cancellation.storage import CancellationCause, cause_key, index_key, task_spec_content_hash, task_spec_hash_key
from src.daemon import git_ops
from src.daemon.approval_commands import checkout_process_blocker
from src.daemon.rejection_commands import rejection_pr_details
from src.github import gh_runner
from src.models import QueueTask, TaskStatus
from src.queue_parser import QueueValidationError, UnstructuredLegacyTaskError, parse_existing_task_header
from src.rejection_commands import load_rejection
from src.task_admission import admission_candidate, validate_admission_graph, verify_unfinished
from src.task_attempts import (
    AdmissionRejected,
    AttemptChanged,
    TaskAttempt,
    attempt_key,
    load_attempt,
    new_attempt,
    save_attempt,
)
from src.task_status import MergeStatusUnavailable


def _parse_snapshot_header(filename: str, content: str):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / Path(filename).name
        path.write_text(content, encoding="utf-8")
        try:
            return parse_existing_task_header(path)
        except UnstructuredLegacyTaskError:
            return None
        except QueueValidationError as exc:
            raise QueueValidationError([issue.replace(str(path), filename) for issue in exc.issues]) from exc


class TaskAdmissionMixin:
    def _has_historical_operator_reject_marker(self, base: str, filename: str, fingerprint: str) -> bool:
        result = git_ops._git(self.repo_path, "log", "--format=%H", base, "--", filename, check=False)
        if result.returncode != 0:
            return False
        for commit in result.stdout.splitlines():
            blob = git_ops._git_bytes(self.repo_path, "show", f"{commit}:{filename}", check=False)
            if blob.returncode != 0:
                continue
            try:
                content = blob.stdout.decode("utf-8")
            except UnicodeError:
                continue
            if task_spec_content_hash(content) != fingerprint:
                continue
            try:
                header = _parse_snapshot_header(filename, content)
            except QueueValidationError:
                continue
            if header and header.blocked_reason == "operator_reject":
                return True
        return False

    async def _snapshot_accepted_specs(self) -> None:
        """Capture prior local-base bytes before synchronization overwrites them.

        These receipts preserve the identity needed to verify an alternative
        completion record even when a later Git edit changes the task hash.
        """
        if not (Path(self.repo_path) / ".git").exists():
            return  # No local-base snapshot exists before the first clone.
        base = self.repo_config.branch
        local_base = git_ops._git(
            self.repo_path,
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/heads/{base}^{{commit}}",
            check=False,
        )
        if local_base.returncode != 0:
            self.log_event(f"[RECOVERY] Accepted-spec snapshot deferred until local base {base!r} is synchronized.")
            return
        try:
            result = git_ops._git(self.repo_path, "ls-tree", "-r", "--name-only", base, "tasks")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
            raise MergeStatusUnavailable(f"Accepted-spec snapshot unavailable for {base}: {exc}") from exc
        for filename in result.stdout.splitlines():
            if not Path(filename).match("tasks/PR-*.md"):
                continue
            task_id = Path(filename).stem
            if await load_attempt(self.redis, self.name, task_id):
                continue
            content_bytes = git_ops._git_bytes(self.repo_path, "show", f"{base}:{filename}").stdout
            content = content_bytes.decode("utf-8")
            header = _parse_snapshot_header(filename, content)
            if header is None:
                continue
            fingerprint = task_spec_content_hash(content)
            task = QueueTask(
                pr_id=header.pr_id,
                title=header.title,
                task_file=filename,
                branch=header.branch,
                status=TaskStatus.DONE if header.frontmatter_status == "done" else TaskStatus.TODO,
            )
            receipt = new_attempt(
                self.repo_config.url,
                task,
                content,
                started=header.frontmatter_status not in (None, "todo"),
                file_sha256=hashlib.sha256(content_bytes).hexdigest(),
            )
            if header.blocked_reason == "operator_reject" or self._has_historical_operator_reject_marker(
                base,
                filename,
                fingerprint,
            ):
                receipt.rejection = "legacy-missing-identity"
            await save_attempt(self.redis, self.name, receipt, expected=None)

    async def _validate_admission(
        self, path: Path, *, token: str | None = None, upload: bool = False, available_ids: set[str] | None = None
    ) -> tuple[TaskAttempt | None, TaskAttempt | None]:
        return await admission_candidate(
            self.redis,
            self.name,
            self.repo_config.url,
            self.repo_config.branch,
            Path(self.repo_path),
            path,
            expected_rejection=token,
            upload=upload,
            available_ids=available_ids,
        )

    async def _reserve_admission(
        self, path: Path, *, token: str | None = None, upload: bool = False, available_ids: set[str] | None = None
    ) -> TaskAttempt | None:
        previous, candidate = await self._validate_admission(
            path, token=token, upload=upload, available_ids=available_ids,
        )
        return await self._reserve_validated_admission(previous, candidate, upload=upload)

    async def _reserve_validated_admission(
        self, previous: TaskAttempt | None, candidate: TaskAttempt | None, *, upload: bool
    ) -> TaskAttempt | None:
        if candidate is None:
            return previous if previous and previous.admission_pending else None
        current = await load_attempt(self.redis, self.name, candidate.task.pr_id)
        if current and current != previous:
            raise AttemptChanged("Accepted task changed during admission.")
        if previous and previous.rejection and not upload:
            rejection = await load_rejection(self.redis, self.name, previous.rejection)
            old = git_ops._git(
                self.repo_path,
                "show",
                f"{rejection.base_commit}:{candidate.task.task_file}",
                check=False,
            )
            if old.returncode == 0 and task_spec_content_hash(old.stdout) == candidate.fingerprint:
                raise AttemptChanged("This Git input predates rejection; commit a later specification rewrite.")
        candidate.base_commit = git_ops._git(
            self.repo_path,
            "rev-parse",
            f"origin/{self.repo_config.branch}",
        ).stdout.strip()
        return await save_attempt(self.redis, self.name, candidate, expected=current)

    async def _finish_admission(self, attempt: TaskAttempt) -> None:
        key = attempt_key(self.name, attempt.task.pr_id)
        task_id = attempt.task.pr_id
        path = Path(self.repo_path) / attempt.task.task_file
        if task_spec_content_hash(path.read_bytes().decode("utf-8")) != attempt.fingerprint:
            raise AttemptChanged("Accepted specification is not present in the checkout.")
        header = parse_existing_task_header(path)
        if header.frontmatter_status == "error":
            if not await self._commit_task_status_change(attempt.task, "TODO", "accept rewritten unfinished task"):
                raise AttemptChanged("Rewritten specification is pending its TODO status commit.")
        origin = git_ops._git_bytes(
            self.repo_path,
            "show",
            f"origin/{self.repo_config.branch}:{attempt.task.task_file}",
        )
        if task_spec_content_hash(origin.stdout.decode("utf-8")) != attempt.fingerprint:
            raise AttemptChanged("Accepted specification is not confirmed on the configured base.")

        async def transaction(pipe):
            current = TaskAttempt.model_validate_json(await pipe.get(key))
            if current.attempt_id != attempt.attempt_id or current.rejection:
                raise AttemptChanged("Admission was superseded by another decision.")
            if not current.admission_pending:
                return
            current.admission_pending = False
            pipe.multi()
            pipe.set(key, current.model_dump_json())
            pipe.delete(
                cause_key(self.name, task_id),
                f"diagnose_exhausted:{self.name}:{task_id}",
                f"metrics:retry_count:{self.name}:{task_id}",
                f"metrics:retry_fingerprint:{self.name}:{task_id}",
                f"metrics:attempt_count:{self.name}:{task_id}",
                f"current_run_started_at:{self.name}:{task_id}",
            )
            pipe.zrem(index_key(self.name), task_id)
            pipe.set(task_spec_hash_key(self.name, task_id), current.fingerprint)

        await self.redis.transaction(transaction, key)
        self._crashed_task_pr_ids.discard(task_id)
        self._user_stopped_task_pr_ids.discard(task_id)
        self._status_write_failed_task_pr_ids.discard(task_id)
        await self._persist_status_write_failed_task_pr_ids()
        if self.state.current_task and self.state.current_task.pr_id == task_id:
            self.state.current_task = None
            self.state.current_pr = None
            self._reset_runner_local_task_counters()
            self._current_run_record = None
        self.log_event(
            f"[RECOVERY] {task_id}: rewritten task accepted as attempt {attempt.attempt_id}; "
            "scheduling subject to controls and dependencies."
        )

    async def _reconcile_git_admissions(self) -> set[str]:
        """Hold invalid/partial inputs individually, leaving independent tasks eligible."""
        held = set()
        try:
            validate_admission_graph(Path(self.repo_path))
        except AdmissionRejected as exc:
            held = {path.stem for path in (Path(self.repo_path) / "tasks").glob("PR-*.md")}
            self._admission_held_task_ids = held
            self.log_event(f"[RECOVERY] Task set cannot be admitted: {exc}")
            return held
        for path in sorted((Path(self.repo_path) / "tasks").glob("PR-*.md")):
            prior = await load_attempt(self.redis, self.name, path.stem)
            if prior is None:
                raw = await self.redis.get(cause_key(self.name, path.stem))
                if raw and CancellationCause.from_redis(raw).payload.get("subsource") == "operator_reject":
                    held.add(path.stem)
                continue  # normal first admission is recorded at dispatch
            if task_spec_content_hash(path.read_text(encoding="utf-8")) == prior.fingerprint:
                if prior.completed:
                    continue
                if prior.rejection:
                    held.add(path.stem)
                elif prior.admission_pending:
                    try:
                        await self._finish_admission(prior)
                    except Exception as exc:
                        held.add(path.stem)
                        self.log_event(f"[RECOVERY] {path.stem}: admission pending ({type(exc).__name__}).")
                continue
            try:
                candidate = await self._reserve_admission(path)
                if candidate:
                    await self._finish_admission(candidate)
            except Exception as exc:
                held.add(path.stem)
                message = str(exc) if isinstance(exc, AttemptChanged) else type(exc).__name__
                self.log_event(f"[RECOVERY] {path.stem}: {message}")
        self._admission_held_task_ids = held
        return held

    async def _fence_recovery_tasks(self, tasks: list[QueueTask]) -> list[QueueTask]:
        result = []
        for task in tasks:
            path = Path(self.repo_path) / (task.task_file or f"tasks/{task.pr_id}.md")
            try:
                attempt = await load_attempt(self.redis, self.name, task.pr_id)
            except Exception as exc:
                raise MergeStatusUnavailable("Attempt ownership unavailable; recovery is deferred.") from exc
            if attempt and not path.is_file():
                continue
            raw = await self.redis.get(cause_key(self.name, task.pr_id))
            if raw and CancellationCause.from_redis(raw).payload.get("subsource") == "operator_reject":
                task.status = TaskStatus.ERROR
            if attempt:
                task.attempt_id = attempt.attempt_id
                if attempt.completed:
                    task.status = TaskStatus.DONE
                elif (
                    attempt.rejection
                    or attempt.admission_pending
                    or attempt.fingerprint != task_spec_content_hash(path.read_text())
                ):
                    task.status = TaskStatus.ERROR
            result.append(task)
        return result

    async def _prepare_task_attempt(self, content: str, *, file_sha256: str | None = None) -> bool:
        """Bind a coder dispatch and remove only proven abandoned branch refs."""
        task = self.state.current_task
        if task is None:
            return False
        try:
            attempt = await load_attempt(self.redis, self.name, task.pr_id)
            if attempt is None:
                attempt = new_attempt(self.repo_config.url, task, content, file_sha256=file_sha256)
                attempt = await save_attempt(self.redis, self.name, attempt, expected=None)
            if (
                attempt.rejection
                or attempt.admission_pending
                or attempt.completed
                or attempt.pr_creation_pending
                or attempt.fingerprint != task_spec_content_hash(content)
            ):
                raise AttemptChanged("Specification/attempt is not admitted for coding.")
            if not attempt.started and (Path(self.repo_path) / "tasks/completions.json").is_file():
                # First startup may have no older Redis receipt. An unresolved
                # historical digest must not turn an edited completion into TODO.
                verify_unfinished(
                    Path(self.repo_path),
                    self.repo_config.branch,
                    self.repo_config.url,
                    attempt,
                )
            self.state.current_task.attempt_id = attempt.attempt_id
            if self._current_run_record is not None:
                self._current_run_record.attempt_id = attempt.attempt_id
            if attempt.previous_rejection and not attempt.branch_prepared:
                await self._prepare_reused_branch(attempt)
                updated = attempt.model_copy(update={"branch_prepared": True})
                attempt = await save_attempt(self.redis, self.name, updated, expected=attempt)
            if not attempt.started or attempt.coder_dispatched is not True:
                updated = attempt.model_copy(update={"started": True, "coder_dispatched": True})
                attempt = await save_attempt(self.redis, self.name, updated, expected=attempt)
            await self.publish_state()
            return True
        except Exception as exc:
            message = str(exc) if isinstance(exc, AttemptChanged) else type(exc).__name__
            self.log_event(f"[RECOVERY] {task.pr_id}: coder dispatch deferred: {message}")
            return False

    async def _prepare_reused_branch(self, attempt: TaskAttempt) -> None:
        command = await load_rejection(self.redis, self.name, attempt.previous_rejection)
        if command is None or command.status != "rejected" or not command.released:
            raise AttemptChanged("Prior rejection is not confirmed.")
        branch, base = attempt.task.branch, self.repo_config.branch
        if branch == base or attempt.repo_url != command.repo_url:
            raise AttemptChanged("Branch or repository ownership differs from the abandoned attempt.")
        if self._current_coder_process is not None or checkout_process_blocker(self.repo_path):
            raise AttemptChanged("Checkout process quiescence is not established.")
        origin = git_ops._git(self.repo_path, "remote", "get-url", "origin").stdout.strip()
        if gh_runner.get_repo_full_name(origin).casefold() != self.owner_repo.casefold():
            raise AttemptChanged("Origin does not belong to the configured repository.")
        if command.pr:
            details = rejection_pr_details(self.owner_repo, command, base)
            if details["state"] != "closed" or details["merged_at"]:
                raise AttemptChanged("Rejected PR is no longer closed without merge.")
        prs = gh_runner.run_gh(
            ["pr", "list", "--state", "open", "--head", branch, "--json", "number"],
            self.owner_repo,
        )
        if prs != []:
            raise AttemptChanged("Another PR may own the reused branch.")
        if git_ops._git(self.repo_path, "status", "--porcelain").stdout.strip():
            raise AttemptChanged("Checkout has unrelated uncommitted files.")
        remote = git_ops._git(self.repo_path, "ls-remote", "--heads", "origin", f"refs/heads/{branch}").stdout.strip()
        remote_sha = remote.split()[0] if remote else None
        local = git_ops._git(self.repo_path, "rev-parse", "--verify", f"refs/heads/{branch}", check=False)
        local_sha = local.stdout.strip() if local.returncode == 0 else None
        expected = command.branch_head if branch == command.task.branch else None
        if (remote_sha and remote_sha != expected) or (local_sha and local_sha != expected):
            raise AttemptChanged("Abandoned branch has an unexpected update; reconcile ownership before cleanup.")
        git_ops._git(self.repo_path, "fetch", "origin", base)
        git_ops._git(self.repo_path, "checkout", base)
        # The normal IDLE sync has already established a clean current base.
        # A new base update is consumed by the next IDLE cycle, not hard-reset
        # over a potentially unrelated local base commit here.
        if (
            git_ops._git(self.repo_path, "rev-parse", "HEAD").stdout
            != git_ops._git(self.repo_path, "rev-parse", f"origin/{base}").stdout
        ):
            raise AttemptChanged("Configured base advanced; synchronize before preparing the new attempt.")
        if remote_sha:
            git_ops._git(
                self.repo_path,
                "push",
                f"--force-with-lease=refs/heads/{branch}:{remote_sha}",
                "origin",
                f":refs/heads/{branch}",
            )
        if local_sha:
            git_ops._git(self.repo_path, "update-ref", "-d", f"refs/heads/{branch}", local_sha)
        # Coder's normal AUTO PR creates the now-absent branch from origin/base.
        # Retry/Approve never call this abandoned-attempt cleanup.
