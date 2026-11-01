"""GitHub review helper for the independent testbed reviewer App."""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Callable
from urllib.parse import quote

import yaml

REVIEWER_TOKEN_ENV = "TESTBED_REVIEWER_TOKEN"
MARKER_PATH = "tests/e2e-shim-marker.txt"
_HUNK_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


class ReviewApiError(AssertionError):
    pass


def _parse_json(raw: str) -> object | None:
    raw = raw.strip()
    if not raw:
        return None
    candidates = [raw]
    start, end = raw.find("{"), raw.rfind("}")
    if start >= 0 and end > start:
        candidates.append(raw[start : end + 1])
    for candidate in candidates:
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            pass
    return None


def _error_details(result: subprocess.CompletedProcess[str]) -> str:
    payload = _parse_json(result.stdout) or _parse_json(result.stderr)
    safe: dict[str, object] = {"message": "structured response unavailable", "errors": []}
    if isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str):
            safe["message"] = message[:500]
        errors = payload.get("errors")
        if isinstance(errors, list):
            safe_errors = []
            for item in errors[:5]:
                if isinstance(item, dict):
                    safe_errors.append({
                        key: value[:300] if isinstance(value, str) else value
                        for key, value in item.items()
                        if key in {"resource", "field", "code", "message"}
                        and isinstance(value, (str, int, bool))
                    })
                elif isinstance(item, str):
                    safe_errors.append(item[:300])
            safe["errors"] = safe_errors
    return json.dumps(safe, sort_keys=True)[:2000]


def _first_added_line(patch: str) -> int:
    new_line: int | None = None
    for line in patch.splitlines():
        match = _HUNK_RE.match(line)
        if match:
            new_line = int(match.group(1))
        elif new_line is not None and line.startswith("+"):
            return new_line
        elif new_line is not None and not line.startswith("-"):
            new_line += 1
    raise ReviewApiError(f"no added line found in {MARKER_PATH} patch")


class ReviewApi:
    def __init__(
        self,
        token: str,
        *,
        repo: str = "AlexBomber12/pipeline-orchestrator-testbed",
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> None:
        self.token = token
        self.repo = repo
        self._runner = runner or subprocess.run

    @classmethod
    def from_environment(
        cls,
        *,
        environ: dict[str, str] | None = None,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
    ) -> "ReviewApi":
        env = os.environ if environ is None else environ
        token = env.get(REVIEWER_TOKEN_ENV, "").strip()
        if not token:
            raise ReviewApiError(
                "missing TESTBED_REVIEWER_TOKEN; configure "
                "TESTBED_REVIEWER_APP_ID and TESTBED_REVIEWER_APP_PRIVATE_KEY "
                "per docs/ci-setup.md (the author GH_TOKEN is never used)"
            )
        return cls(token, runner=runner)

    def _call(
        self,
        operation: str,
        pr_number: int,
        path: str,
        *,
        method: str = "GET",
        payload: dict | None = None,
    ) -> object:
        command = ["gh", "api", "-X", method, path]
        input_text = json.dumps(payload) if payload is not None else None
        if payload is not None:
            command.extend(["--input", "-"])
        env = {**os.environ, "GH_TOKEN": self.token}
        result = self._runner(
            command,
            input=input_text,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode != 0:
            raise ReviewApiError(
                f"GitHub review API failure: operation={operation}, "
                f"pr_number={pr_number}, return_code={result.returncode}, "
                f"response={_error_details(result)}"
            )
        parsed = _parse_json(result.stdout)
        if parsed is None:
            raise ReviewApiError(
                f"GitHub review API returned non-JSON: operation={operation}, "
                f"pr_number={pr_number}, return_code={result.returncode}"
            )
        return parsed

    def _wait_after(self, pr_number: int, floor: datetime) -> None:
        """Use GitHub's clock for GitHub evidence, independent of runner skew."""
        deadline = time.monotonic() + 10
        while True:
            result = self._runner(
                ["gh", "api", "--include", "-H", "Cache-Control: no-cache",
                 f"repos/{self.repo}/pulls/{pr_number}"],
                env={**os.environ, "GH_TOKEN": self.token},
                capture_output=True, text=True, check=False, timeout=30,
            )
            if result.returncode != 0:
                raise ReviewApiError(
                    f"GitHub clock read failed for PR #{pr_number}: {_error_details(result)}"
                )
            # Only inspect the HTTP headers, never a Date-like line in JSON.
            headers = result.stdout.replace("\r\n", "\n").split("\n\n", 1)[0]
            try:
                stamp = next(line.split(":", 1)[1].strip() for line in headers.splitlines()
                             if line.lower().startswith("date:"))
                now = parsedate_to_datetime(stamp)
                if now.tzinfo is None:
                    raise ValueError("timezone missing")
            except (StopIteration, ValueError, TypeError) as exc:
                raise ReviewApiError(f"missing or invalid GitHub Date header for PR #{pr_number}") from exc
            if now > floor + timedelta(seconds=1):
                return
            if time.monotonic() >= deadline:
                raise ReviewApiError(f"timed out waiting for GitHub review clock on PR #{pr_number}")
            time.sleep(0.25)

    def approve_head(self, pr_number: int, head: str, publication_at: str) -> dict:
        """Post real, independent success evidence after proven publication."""
        pull = self._call("get_pull", pr_number, f"repos/{self.repo}/pulls/{pr_number}")
        assert pull["head"]["sha"] == head and re.fullmatch(r"[0-9a-f]{40}", head), pull
        floor = datetime.fromisoformat(publication_at.replace("Z", "+00:00"))
        self._wait_after(pr_number, floor)
        review = self._call(
            "create_success_review", pr_number, f"repos/{self.repo}/pulls/{pr_number}/reviews",
            method="POST", payload={
                "event": "APPROVE", "commit_id": head,
                "body": "Codex Review: Didn't find any major issues.",
            },
        )
        assert review["commit_id"] == head and review["state"] == "APPROVED", review
        comments = self._call("get_anchor", pr_number, f"repos/{self.repo}/issues/{pr_number}/comments")
        anchors = [item for item in comments if item.get("body") == "@codex review"
                   and item["user"]["login"] == pull["user"]["login"]]
        anchor = anchors[-1]
        assert datetime.fromisoformat(anchor["created_at"].replace("Z", "+00:00")) >= floor
        reaction = self._call(
            "approve_anchor", pr_number, f"repos/{self.repo}/issues/comments/{anchor['id']}/reactions",
            method="POST", payload={"content": "+1"},
        )
        assert reaction["content"] == "+1", reaction
        return review

    def post_changes_requested(self, pr_number: int) -> dict:
        pull = self._call("get_pull", pr_number, f"repos/{self.repo}/pulls/{pr_number}")
        if not isinstance(pull, dict):
            raise ReviewApiError(f"get_pull returned invalid payload for PR #{pr_number}")
        author = ((pull.get("user") or {}).get("login") or "")
        head_sha = ((pull.get("head") or {}).get("sha") or "")

        comments = self._call(
            "get_review_anchor", pr_number, f"repos/{self.repo}/issues/{pr_number}/comments"
        )
        if not isinstance(comments, list):
            raise ReviewApiError(f"get_review_anchor returned invalid payload for PR #{pr_number}")
        anchors = [
            item for item in comments if isinstance(item, dict)
            and (item.get("user") or {}).get("login") == author
            and "@codex review" in (item.get("body") or "").lower()
        ]
        if not anchors:
            raise ReviewApiError(f"author review anchor not found for PR #{pr_number}")
        anchor_at = anchors[-1].get("created_at") or ""

        commit = self._call("get_head_commit", pr_number, f"repos/{self.repo}/commits/{head_sha}")
        commit_data = (commit.get("commit") or {}) if isinstance(commit, dict) else {}
        committed_at = ((commit_data.get("committer") or {}).get("date") or "")
        files = self._call("get_pull_files", pr_number, f"repos/{self.repo}/pulls/{pr_number}/files")
        if not isinstance(files, list):
            raise ReviewApiError(f"get_pull_files returned invalid payload for PR #{pr_number}")
        marker = next(
            (item for item in files if isinstance(item, dict) and item.get("filename") == MARKER_PATH),
            None,
        )
        if marker is None:
            raise ReviewApiError(f"{MARKER_PATH} is not changed by PR #{pr_number}")
        line = _first_added_line(marker.get("patch") or "")

        try:
            floor = max(datetime.fromisoformat(value.replace("Z", "+00:00")) for value in (anchor_at, committed_at))
        except ValueError as exc:
            raise ReviewApiError(
                f"missing push or review-anchor timestamp for PR #{pr_number}"
            ) from exc
        self._wait_after(pr_number, floor)

        review = self._call(
            "create_request_changes_review",
            pr_number,
            f"repos/{self.repo}/pulls/{pr_number}/reviews",
            method="POST",
            payload={
                "event": "REQUEST_CHANGES",
                "body": "e2e: please address the inline review feedback",
                "commit_id": head_sha,
                "comments": [{"path": MARKER_PATH, "line": line, "side": "RIGHT", "body": "P1: e2e review feedback"}],
            },
        )
        recorded = review if isinstance(review, dict) else {}
        review_id = recorded.get("id")
        if not isinstance(review_id, int):
            raise ReviewApiError(f"create review response missing integer id for PR #{pr_number}")
        deadline = time.monotonic() + 10
        feedback = None
        while feedback is None:
            review_comments = self._call(
                "get_created_review_comment",
                pr_number,
                f"repos/{self.repo}/pulls/{pr_number}/comments",
            )
            if isinstance(review_comments, list):
                feedback = next(
                    (
                        item for item in review_comments
                        if isinstance(item, dict)
                        and item.get("pull_request_review_id") == review_id
                        and item.get("path") == MARKER_PATH
                    ),
                    None,
                )
            if feedback is None:
                if time.monotonic() >= deadline:
                    raise ReviewApiError(f"inline review comment not recorded for PR #{pr_number}")
                time.sleep(0.25)
        feedback_at = datetime.fromisoformat((feedback.get("created_at") or "").replace("Z", "+00:00"))
        if feedback_at <= floor:
            raise ReviewApiError(f"inline review feedback is not fresh for PR #{pr_number}")
        return {
            "state": recorded.get("state"), "reviewer": (recorded.get("user") or {}).get("login"),
            "author": author,
            "commit_id": recorded.get("commit_id"),
            "head_sha": head_sha,
        }


class MergeOutcomeApi:
    """Read exact publication and merge evidence with the testbed author token."""

    def __init__(self, repo: str, base: str, *, runner=None):
        self.repo, self.base = repo, base
        self._runner = runner or subprocess.run

    def _get(self, path: str):
        result = self._runner(
            ["gh", "api", f"repos/{self.repo}/{path}"],
            capture_output=True, text=True, check=True, timeout=30,
        )
        return json.loads(result.stdout)

    def _content(self, path: str, ref: str) -> str:
        payload = self._get(f"contents/{path}?ref={quote(ref, safe='')}")
        return base64.b64decode(payload["content"]).decode()

    def assert_pull(self, number: int, branch: str, head: str) -> dict:
        assert re.fullmatch(r"[0-9a-f]{40}", head), head
        pull = self._get(f"pulls/{number}")
        assert pull["number"] == number, pull
        assert pull["head"]["ref"] == branch and pull["head"]["sha"] == head, pull
        assert pull["head"]["repo"]["full_name"] == self.repo, pull
        assert pull["base"]["ref"] == self.base, pull
        assert pull["base"]["repo"]["full_name"] == self.repo, pull
        return pull

    def published_marker(self, number: int, branch: str, head: str, task_id: str) -> str:
        self.assert_pull(number, branch, head)
        markers = [
            line for line in self._content(MARKER_PATH, head).splitlines()
            if line.startswith(f"shim marker for {task_id} at ")
        ]
        assert len(markers) == 1, markers
        return markers[0]

    def fixed_marker(
        self, number: int, branch: str, head: str, task_id: str, old_head: str, old_marker: str,
    ) -> str:
        assert head != old_head, "FIX did not publish a new HEAD"
        marker = self.published_marker(number, branch, head, task_id)
        assert marker != old_marker, "FIX did not change the task marker"
        remote = self._get(f"git/ref/heads/{quote(branch, safe='')}")
        assert remote["object"]["sha"] == head, remote
        return marker

    def assert_merged(
        self, number: int, branch: str, head: str, task_id: str, marker: str,
    ) -> dict:
        pull = self.assert_pull(number, branch, head)
        assert pull["merged"] is True and pull["state"] == "closed", pull
        merge = pull.get("merge_commit_sha")
        assert isinstance(merge, str) and re.fullmatch(r"[0-9a-f]{40}", merge), pull
        # Pin both reads to one configured-base snapshot after bookkeeping.
        base_sha = self._get(f"commits/{quote(self.base, safe='')}")["sha"]
        comparison = self._get(f"compare/{merge}...{base_sha}")
        assert comparison["status"] in {"ahead", "identical"}, comparison
        assert marker in self._content(MARKER_PATH, merge).splitlines(), marker
        assert marker in self._content(MARKER_PATH, base_sha).splitlines(), marker
        task = self._content(f"tasks/{task_id}.md", base_sha)
        metadata = yaml.safe_load(task.split("---", 2)[1])
        assert metadata["status"] == "DONE", task[:300]
        assert f"# {task_id}:" in task and f"Branch: {branch}\n" in task, task[:500]
        return {"number": number, "head_sha": head, "merge_commit_sha": merge, "base_sha": base_sha}


def task_released_and_done(state: dict | None, task_id: str, branch: str) -> bool:
    """IDLE requires released identities and the exact positive queue outcome."""
    if not state or state.get("state") != "IDLE":
        return False
    if state.get("current_task") is not None or state.get("current_pr") is not None:
        return False
    matches = [task for task in state.get("current_queue", []) if task.get("pr_id") == task_id]
    return len(matches) == 1 and matches[0].get("branch") == branch and matches[0].get("status") == "DONE"
