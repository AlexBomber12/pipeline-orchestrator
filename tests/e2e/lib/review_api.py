"""GitHub review helper for the independent testbed reviewer App."""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

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
        deadline = time.monotonic() + 10
        while datetime.now(timezone.utc) <= floor + timedelta(seconds=1):
            if time.monotonic() >= deadline:
                raise ReviewApiError(f"timed out waiting for a fresh review timestamp on PR #{pr_number}")
            time.sleep(0.1)

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
