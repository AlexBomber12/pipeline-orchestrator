"""Per-repository pipeline state machine.

One ``PipelineRunner`` instance exists per connected repository. The daemon
main loop calls ``run_cycle`` once per poll interval; each cycle clones or
fetches the repo, runs a preflight check, and dispatches on the persisted
state (``IDLE``, ``WATCH``, ``HUNG``, or ``ERROR``). Transient states
(``CODING``, ``FIX``, ``MERGE``) are resolved within a single cycle and
never persisted across cycles.
"""

from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import redis.asyncio as aioredis

from src import claude_cli, github_client
from src.config import AppConfig, RepoConfig
from src.models import (
    CIStatus,
    PipelineState,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.queue_parser import get_next_task, parse_queue

logger = logging.getLogger(__name__)

_TRANSIENT_STATES = {
    PipelineState.CODING,
    PipelineState.FIX,
    PipelineState.MERGE,
}

_HISTORY_LIMIT = 100


def repo_name_from_url(url: str) -> str:
    """Return the repo name (last URL segment without ``.git``)."""
    cleaned = url.rstrip("/")
    last = cleaned.rsplit("/", 1)[-1]
    if last.endswith(".git"):
        last = last[: -len(".git")]
    return last


def repo_owner_from_url(url: str) -> str:
    """Return ``owner/repo`` for a GitHub URL."""
    return github_client.get_repo_full_name(url)


class PipelineRunner:
    """State machine for one repository."""

    def __init__(
        self,
        repo_config: RepoConfig,
        app_config: AppConfig,
        redis_client: aioredis.Redis,
    ) -> None:
        self.repo_config = repo_config
        self.app_config = app_config
        self.redis = redis_client
        self.name = repo_name_from_url(repo_config.url)
        self.owner_repo = repo_owner_from_url(repo_config.url)
        self.repo_path = f"/data/repos/{self.name}"
        self.state = RepoState(
            url=repo_config.url,
            name=self.name,
            last_updated=datetime.now(timezone.utc),
        )
        # One-shot guard so recover_state runs exactly once per process, on
        # the first cycle after startup. Reconstructing state from QUEUE.md +
        # GitHub on every cycle would clobber in-memory progress made in
        # earlier cycles of the same run.
        self._recovered = False

    async def publish_state(self) -> None:
        """Serialize ``self.state`` and write it to Redis."""
        self.state.last_updated = datetime.now(timezone.utc)
        payload = self.state.model_dump_json()
        await self.redis.set(f"pipeline:{self.name}", payload)

    def log_event(self, event: str) -> None:
        """Append an event to ``state.history`` (capped) and log it."""
        entry = {
            "time": datetime.now(timezone.utc).isoformat(),
            "state": self.state.state.value,
            "event": event,
        }
        self.state.history.append(entry)
        if len(self.state.history) > _HISTORY_LIMIT:
            self.state.history = self.state.history[-_HISTORY_LIMIT:]
        logger.info("[%s] %s", self.name, event)

    async def ensure_repo_cloned(self) -> None:
        """Clone the repo if missing, otherwise fetch ``origin/{branch}``."""
        path = Path(self.repo_path)
        if not path.exists():
            try:
                subprocess.run(
                    ["git", "clone", self.repo_config.url, self.repo_path],
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=True,
                )
            except subprocess.CalledProcessError as exc:
                detail = (exc.stderr or exc.stdout or "").strip()
                raise RuntimeError(f"git clone failed: {detail}") from exc
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError("git clone timed out") from exc
            return

        try:
            subprocess.run(
                ["git", "fetch", "origin", self.repo_config.branch],
                capture_output=True,
                text=True,
                timeout=60,
                check=True,
                cwd=self.repo_path,
            )
        except subprocess.CalledProcessError as exc:
            detail = (exc.stderr or exc.stdout or "").strip()
            raise RuntimeError(f"git fetch failed: {detail}") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("git fetch timed out") from exc

    def sync_to_main(self) -> None:
        """Hard-sync the working tree to ``origin/{branch}``.

        Only safe to call when the runner is IDLE (no active Claude working
        branch to clobber). Uses ``git reset --hard`` instead of ``git pull``
        so that any stray local modifications from a prior crashed cycle are
        discarded deterministically, guaranteeing QUEUE.md and tasks/ reflect
        the tip of the base branch before ``parse_queue`` reads them.

        Raises the underlying ``subprocess`` exception on failure so the
        caller can translate it into ERROR state with appropriate context.
        """
        branch = self.repo_config.branch
        subprocess.run(
            ["git", "fetch", "origin", branch],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
            cwd=self.repo_path,
        )
        subprocess.run(
            ["git", "checkout", branch],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
            cwd=self.repo_path,
        )
        subprocess.run(
            ["git", "reset", "--hard", f"origin/{branch}"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
            cwd=self.repo_path,
        )

    async def recover_state(self) -> None:
        """Reconstruct state from QUEUE.md + GitHub on daemon startup.

        Decision tree:

        1. If QUEUE.md has a DOING task:
           - Matching open PR on that branch -> WATCH (runner resumes
             polling the existing PR).
           - No matching PR -> CODING + re-run ``handle_coding()``
             (Claude CLI run was interrupted before pushing).
        2. If no DOING task but an open PR exists on this repo:
           - Attach as an orphan -> WATCH, and if a DONE task in QUEUE.md
             matches the PR's branch set that as ``current_task`` (the task
             was marked DONE but the PR has not yet merged).
        3. Otherwise, stay IDLE.

        Runs before ``preflight`` so that even a dirty working tree left
        behind by a crashed cycle does not block recovery. Runs exactly
        once per process (see ``_recovered`` in ``__init__``).
        """
        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        tasks = parse_queue(queue_path)
        doing = next((t for t in tasks if t.status == TaskStatus.DOING), None)

        try:
            prs = github_client.get_open_prs(self.owner_repo)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"recover_state: get_open_prs failed: {exc}"
            self.log_event(f"recover_state failed: {exc}")
            return

        if doing is not None:
            self.state.current_task = doing
            matching = (
                next((p for p in prs if p.branch == doing.branch), None)
                if doing.branch
                else None
            )
            if matching is not None:
                self.state.current_pr = matching
                self.state.state = PipelineState.WATCH
                self.log_event(
                    f"Recovered: DOING task {doing.pr_id} "
                    f"-> WATCH PR #{matching.number}"
                )
                return

            self.state.state = PipelineState.CODING
            self.log_event(
                f"Recovered: DOING task {doing.pr_id}, no PR "
                "-> re-running CODING"
            )
            await self.handle_coding()
            return

        if prs:
            # No DOING task but an open PR exists: attach to it. The runner
            # only ever drives one PR at a time, so picking prs[0] is safe
            # under the one-task-at-a-time invariant enforced by
            # get_next_task.
            orphan = prs[0]
            self.state.current_pr = orphan
            done_match = next(
                (
                    t
                    for t in tasks
                    if t.status == TaskStatus.DONE and t.branch == orphan.branch
                ),
                None,
            )
            if done_match is not None:
                self.state.current_task = done_match
            self.state.state = PipelineState.WATCH
            self.log_event(f"Recovered: orphan PR #{orphan.number} -> WATCH")
            return

        self.log_event("Recovered: no DOING tasks, no open PRs -> IDLE")

    def preflight(self) -> bool:
        """Return ``True`` iff the working tree is clean."""
        try:
            result = subprocess.run(
                ["git", "status", "--porcelain"],
                capture_output=True,
                text=True,
                timeout=30,
                check=True,
                cwd=self.repo_path,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"preflight failed: {exc}"
            self.log_event(f"preflight failed: {exc}")
            return False

        dirty = result.stdout.strip()
        if dirty:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"working tree dirty: {dirty}"
            self.log_event("preflight: dirty working tree")
            return False
        return True

    async def run_cycle(self) -> None:
        """Advance the state machine by one step."""
        try:
            await self.ensure_repo_cloned()
        except RuntimeError as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = str(exc)
            self.log_event(f"ensure_repo_cloned failed: {exc}")
            await self.publish_state()
            return

        if not self._recovered:
            # Run before preflight: a crashed cycle may have left a dirty
            # working tree, and recover_state needs to reconstruct the
            # in-memory state from QUEUE.md + GitHub regardless.
            await self.recover_state()
            self._recovered = True

        if not self.preflight():
            await self.publish_state()
            return

        if self.state.state in _TRANSIENT_STATES:
            self.log_event(
                f"resetting stale transient state {self.state.state.value} -> IDLE"
            )
            self.state.state = PipelineState.IDLE

        current = self.state.state
        if current == PipelineState.IDLE:
            await self.handle_idle()
        elif current == PipelineState.WATCH:
            await self.handle_watch()
        elif current == PipelineState.HUNG:
            await self.handle_hung()
        elif current == PipelineState.ERROR:
            if self.app_config.daemon.error_handler_use_ai:
                await self.handle_error()

        await self.publish_state()

    async def handle_idle(self) -> None:
        """Hard-sync to ``origin/{branch}``, pick the next task, hand off."""
        # sync_to_main is only safe in IDLE state: it runs git reset --hard
        # on the base branch, which would destroy any in-flight Claude work
        # on a feature branch. We are IDLE here, so that's fine.
        try:
            self.sync_to_main()
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"sync_to_main failed: {exc}"
            self.log_event(f"sync_to_main failed: {exc}")
            return

        queue_path = str(Path(self.repo_path) / "tasks" / "QUEUE.md")
        tasks = parse_queue(queue_path)
        task = get_next_task(tasks)
        if task is None:
            self.log_event("No tasks available")
            return

        self.state.current_task = task
        self.state.state = PipelineState.CODING
        self.log_event(f"Picked task {task.pr_id}: {task.title}")
        await self.publish_state()
        await self.handle_coding()

    async def handle_coding(self) -> None:
        """Run ``PLANNED PR`` via the claude CLI and hand off to WATCH."""
        code, _stdout, stderr = claude_cli.run_planned_pr(self.repo_path)
        if code != 0:
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"claude exit {code}"
            self.log_event(f"claude CLI failed: {self.state.error_message}")
            return

        try:
            prs = github_client.get_open_prs(self.owner_repo)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"get_open_prs failed: {exc}"
            self.log_event(str(exc))
            return

        target_branch = (
            self.state.current_task.branch if self.state.current_task else None
        )
        if not target_branch:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                "Current task has no branch; cannot identify PR"
            )
            self.log_event(self.state.error_message)
            return

        # Match strictly by branch. Falling back to the newest open PR would
        # attach the runner to an unrelated PR if the PLANNED PR run failed
        # to open the expected branch, which could then trigger unintended
        # WATCH/FIX/MERGE actions on someone else's work.
        candidate = next((pr for pr in prs if pr.branch == target_branch), None)
        if candidate is None:
            self.state.state = PipelineState.ERROR
            self.state.error_message = (
                f"Claude CLI succeeded but no PR found for branch {target_branch!r}"
            )
            self.log_event(self.state.error_message)
            return

        self.state.current_pr = candidate
        self.state.state = PipelineState.WATCH
        self.log_event(f"Opened PR #{candidate.number} -> WATCH")

    async def handle_watch(self) -> None:
        """Poll PR status and decide whether to merge, fix, hang, or wait."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            self.log_event("WATCH without current_pr -> IDLE")
            return

        try:
            prs = github_client.get_open_prs(self.owner_repo)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"get_open_prs failed: {exc}"
            self.log_event(str(exc))
            return

        current_number = self.state.current_pr.number
        found = next((p for p in prs if p.number == current_number), None)
        if found is None:
            self.log_event(f"PR #{current_number} no longer open -> IDLE")
            self.state.current_pr = None
            self.state.current_task = None
            self.state.state = PipelineState.IDLE
            return

        self.state.current_pr = found

        ci = found.ci_status
        review = found.review_status
        if ci == CIStatus.SUCCESS and review == ReviewStatus.APPROVED:
            if self.repo_config.auto_merge:
                await self.handle_merge()
            else:
                # Honor repositories configured with auto_merge: false.
                # Stay in WATCH so a human can merge manually; handle_watch
                # will return to IDLE on the next cycle once the PR closes.
                self.log_event(
                    f"PR #{found.number} green but auto_merge disabled; "
                    "awaiting manual merge"
                )
            return
        if review == ReviewStatus.CHANGES_REQUESTED or ci == CIStatus.FAILURE:
            await self.handle_fix()
            return

        # Any remaining combination is a waiting state that should still be
        # subject to the review timeout. This explicitly includes
        # ``APPROVED + ci PENDING``: previously that pair matched none of the
        # branches above and the runner could block on permanently pending CI
        # without ever transitioning to ``HUNG``.
        last_activity = found.last_activity or self.state.last_updated
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        elapsed_min = (now - last_activity).total_seconds() / 60
        timeout_min = self.repo_config.review_timeout_min
        if elapsed_min >= timeout_min:
            self.state.state = PipelineState.HUNG
            self.log_event(
                f"PR #{found.number} hung after {elapsed_min:.0f}m "
                f"(review={review.value}, ci={ci.value})"
            )
        else:
            self.log_event(
                f"PR #{found.number} waiting "
                f"(review={review.value}, ci={ci.value}, "
                f"{elapsed_min:.0f}/{timeout_min}m)"
            )

    async def handle_fix(self) -> None:
        """Run ``FIX REVIEW`` via the claude CLI and return to WATCH."""
        self.state.state = PipelineState.FIX
        self.log_event("entering FIX")
        await self.publish_state()

        code, _stdout, stderr = claude_cli.fix_review(self.repo_path)
        if code != 0:
            self.state.state = PipelineState.ERROR
            self.state.error_message = stderr.strip() or f"claude exit {code}"
            self.log_event(f"fix_review failed: {self.state.error_message}")
            return

        if self.state.current_pr is not None:
            self.state.current_pr.push_count += 1
            self.state.current_pr.last_activity = datetime.now(timezone.utc)
            iteration = self.state.current_pr.push_count
        else:
            iteration = 0

        self.state.state = PipelineState.WATCH
        self.log_event(f"Fix pushed, iteration #{iteration}")

    async def handle_merge(self) -> None:
        """Merge the current PR and return to IDLE."""
        if self.state.current_pr is None:
            self.state.state = PipelineState.IDLE
            return

        number = self.state.current_pr.number
        self.log_event(f"Merging PR #{number}")
        try:
            github_client.merge_pr(self.owner_repo, number)
        except Exception as exc:
            self.state.state = PipelineState.ERROR
            self.state.error_message = f"merge_pr failed: {exc}"
            self.log_event(str(exc))
            return

        self.state.current_pr = None
        self.state.current_task = None
        self.state.state = PipelineState.IDLE
        self.log_event(f"Merged PR #{number} -> IDLE")

    async def handle_hung(self) -> None:
        """Nudge the reviewer with ``@codex review`` or give up, per config."""
        if (
            self.app_config.daemon.hung_fallback_codex_review
            and self.state.current_pr is not None
        ):
            try:
                github_client.post_comment(
                    self.owner_repo,
                    self.state.current_pr.number,
                    "@codex review",
                )
            except Exception as exc:
                self.state.state = PipelineState.ERROR
                self.state.error_message = f"post_comment failed: {exc}"
                self.log_event(str(exc))
                return
            self.state.current_pr.last_activity = datetime.now(timezone.utc)
            self.state.state = PipelineState.WATCH
            self.log_event("posted @codex review -> WATCH")
            return

        self.log_event("hung fallback disabled, skipping")
        self.state.current_pr = None
        self.state.current_task = None
        self.state.state = PipelineState.IDLE

    async def handle_error(self, error_context: str | None = None) -> None:
        """Ask the claude CLI whether to FIX, SKIP, or ESCALATE the error."""
        context = error_context or self.state.error_message or "Unknown error"
        code, stdout, stderr = claude_cli.diagnose_error(self.repo_path, context)
        if code != 0:
            self.log_event(
                f"diagnose_error CLI failed: {stderr.strip() or f'exit {code}'}"
            )
            return

        verdict = claude_cli.parse_diagnosis(stdout)
        if verdict == "SKIP":
            self.state.current_task = None
            self.state.current_pr = None
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            self.log_event("diagnose_error: SKIP -> IDLE")
        elif verdict == "FIX":
            self.state.error_message = None
            self.state.state = PipelineState.IDLE
            summary = stdout.strip().splitlines()[-1] if stdout.strip() else ""
            self.log_event(f"diagnose_error: FIX -> IDLE ({summary[:80]})")
        else:  # ESCALATE
            self.log_event("diagnose_error: ESCALATE, keeping ERROR state")
