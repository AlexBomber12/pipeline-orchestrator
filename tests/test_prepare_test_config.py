import os
import shlex
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from scripts.prepare_test_config import prepare_test_config


def test_installs_reviewer_prepared_config_in_shared_directory(tmp_path: Path) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text(
        "daemon:\n  trusted_reviewer_identities:\n"
        "    - user_id: 123\n      login: reviewer[bot]\n",
        encoding="utf-8",
    )
    destination = tmp_path / "data" / "config.test.yml"

    prepare_test_config(source, destination)

    assert destination.read_bytes() == source.read_bytes()
    assert yaml.safe_load(destination.read_text())["daemon"]["trusted_reviewer_identities"] == [
        {"user_id": 123, "login": "reviewer[bot]"}
    ]
    assert destination.parent.is_dir()
    assert list(destination.parent.iterdir()) == [destination]


def test_replaces_old_disposable_config_without_changing_source(tmp_path: Path) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text("daemon:\n  trusted_reviewer_identities:\n    - user_id: 1\n", encoding="utf-8")
    destination = tmp_path / "data" / "config.test.yml"
    destination.parent.mkdir()
    destination.write_text("stale: true\n", encoding="utf-8")
    original = source.read_bytes()

    prepare_test_config(source, destination)

    assert destination.read_bytes() == original
    assert source.read_bytes() == original


def test_installs_trusted_testbed_status_producer(tmp_path: Path) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text(
        "repositories:\n  - url: https://github.com/example/testbed.git\n"
        "daemon:\n  trusted_reviewer_identities:\n    - user_id: 1\n",
        encoding="utf-8",
    )
    destination = tmp_path / "shared.yml"

    prepare_test_config(source, destination, status_app_slug="testbed-app")

    installed = yaml.safe_load(destination.read_text(encoding="utf-8"))
    assert installed["repositories"][0]["ci_fix_trusted_statuses"] == {
        "e2e/watch-merge-gate": "user:testbed-app[bot]",
    }
    assert "ci_fix_trusted_statuses" not in source.read_text(encoding="utf-8")


@pytest.mark.parametrize("slug", ["", "bad slug"])
def test_rejects_invalid_testbed_status_app_slug(tmp_path: Path, slug: str) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text(
        "daemon:\n  trusted_reviewer_identities:\n    - user_id: 1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="status App slug"):
        prepare_test_config(source, tmp_path / "shared.yml", status_app_slug=slug)


def test_requires_one_repository_for_testbed_status_policy(tmp_path: Path) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text(
        "daemon:\n  trusted_reviewer_identities:\n    - user_id: 1\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="one testbed repository"):
        prepare_test_config(source, tmp_path / "shared.yml", status_app_slug="testbed-app")


@pytest.mark.parametrize("content", ["null\n", "daemon: {}\n"])
def test_rejects_config_without_reviewer_policy(tmp_path: Path, content: str) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text(content, encoding="utf-8")
    destination = tmp_path / "data" / "config.test.yml"

    with pytest.raises(ValueError, match="trusted reviewer"):
        prepare_test_config(source, destination)

    assert not destination.exists()


def test_rejects_same_source_and_destination(tmp_path: Path) -> None:
    source = tmp_path / "config.test.yml"
    source.write_text("daemon: {}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="must differ"):
        prepare_test_config(source, source)


def test_local_wrapper_prepares_shared_config_before_stack_start(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("test-e2e.sh", "prepare_test_config.py", "init-test-volume.sh"):
        shutil.copy(root / "scripts" / name, scripts / name)
    shutil.copy(root / "config.test.yml", tmp_path / "config.test.yml")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker = fake_bin / "docker"
    docker.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"compose\" ] && [ \"$4\" = \"up\" ]; then\n"
        f"  test -f {shlex.quote(str(tmp_path / 'tests/e2e/data/config.test.yml'))}\n"
        "fi\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    python = fake_bin / "python3"
    python.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = \"-m\" ]; then exit 0; fi\n"
        f"exec {shlex.quote(sys.executable)} \"$@\"\n",
        encoding="utf-8",
    )
    python.chmod(0o755)

    result = subprocess.run(
        ["bash", str(scripts / "test-e2e.sh")],
        cwd=tmp_path,
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    destination = tmp_path / "tests/e2e/data/config.test.yml"
    assert destination.read_bytes() == (tmp_path / "config.test.yml").read_bytes()
    assert stat.S_IMODE(destination.parent.stat().st_mode) == 0o777
