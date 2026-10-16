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
