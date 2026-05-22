"""Feature flag defaults and committed config coverage."""

from __future__ import annotations

from pathlib import Path

import yaml
from src.config import FeatureFlags, load_config


def test_use_unified_inhibitor_check_default_is_true() -> None:
    assert FeatureFlags().use_unified_inhibitor_check is True


def test_use_single_error_exit_default_is_false() -> None:
    assert FeatureFlags().use_single_error_exit is False


def test_config_yml_documents_flag() -> None:
    config_path = Path("config.yml")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))

    values = [
        repo["feature_flags"]["use_unified_inhibitor_check"]
        for repo in raw["repositories"]
    ]
    assert values
    assert all(value is True for value in values)

    text = config_path.read_text(encoding="utf-8")
    assert "use_unified_inhibitor_check: true" in text
    assert "unified WorkInhibitor helper" in text
    assert "targeted rollback" in text
    single_error_values = [
        repo["feature_flags"]["use_single_error_exit"]
        for repo in raw["repositories"]
    ]
    assert all(value is False for value in single_error_values)
    assert "use_single_error_exit: false" in text


def test_per_repo_override_to_false_respected(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
repositories:
  - url: https://github.com/example/repo.git
    feature_flags:
      use_unified_inhibitor_check: false
""",
        encoding="utf-8",
    )

    cfg = load_config(str(config_path))

    assert cfg.repositories[0].feature_flags.use_unified_inhibitor_check is False


def test_per_repo_override_to_true_redundant_but_works(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
repositories:
  - url: https://github.com/example/repo.git
    feature_flags:
      use_unified_inhibitor_check: true
""",
        encoding="utf-8",
    )

    cfg = load_config(str(config_path))

    assert cfg.repositories[0].feature_flags.use_unified_inhibitor_check is True


def test_single_error_exit_override_to_true_respected(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yml"
    config_path.write_text(
        """
repositories:
  - url: https://github.com/example/repo.git
    feature_flags:
      use_single_error_exit: true
""",
        encoding="utf-8",
    )

    cfg = load_config(str(config_path))

    assert cfg.repositories[0].feature_flags.use_single_error_exit is True
