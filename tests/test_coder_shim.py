import subprocess
from pathlib import Path

SHIM = Path(__file__).parent / "e2e" / "lib" / "coder_shim.sh"


def _write_task(
    repo: Path,
    pr_id: str,
    *,
    branch: str,
    status: str | None = "TODO",
) -> None:
    tasks = repo / "tasks"
    tasks.mkdir(parents=True, exist_ok=True)
    status_line = "" if status is None else f"- Status: {status}\n"
    (tasks / f"{pr_id}.md").write_text(
        f"# {pr_id}: Sample\n\n"
        f"Branch: {branch}\n"
        f"{status_line}",
        encoding="utf-8",
    )


def _parse_doing_task(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            f"source {SHIM}; parse_doing_task {repo}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )


def test_shim_reads_active_pr_from_runtime_file(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-004", branch="pr-004", status="DOING")
    _write_task(tmp_path, "PR-005", branch="pr-005", status="TODO")
    runtime_dir = tmp_path / ".daemon-runtime"
    runtime_dir.mkdir()
    (runtime_dir / "active-pr-id").write_text("PR-005\n", encoding="utf-8")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_falls_back_to_status_doing_in_pr_md(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status="DOING")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_falls_back_to_statusless_pr_md(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status=None)

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_ignores_stale_runtime_file_before_fallback(
    tmp_path: Path,
) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status=None)
    runtime_dir = tmp_path / ".daemon-runtime"
    runtime_dir.mkdir()
    (runtime_dir / "active-pr-id").write_text("PR-999\n", encoding="utf-8")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-005\tpr-005\n"


def test_shim_returns_nothing_when_no_doing_task(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-005", branch="pr-005", status="TODO")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 1
    assert result.stdout == ""


def test_shim_handles_multiple_doing_status(tmp_path: Path) -> None:
    _write_task(tmp_path, "PR-010", branch="pr-010", status="DOING")
    _write_task(tmp_path, "PR-002", branch="pr-002", status="DOING")

    result = _parse_doing_task(tmp_path)

    assert result.returncode == 0
    assert result.stdout == "PR-002\tpr-002\n"
