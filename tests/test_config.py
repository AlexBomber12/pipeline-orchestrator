"""Tests for src/config.py."""

from __future__ import annotations

from pathlib import Path

import pytest
import src.config as config_module
from src.config import (
    AppConfig,
    RepoConfig,
    add_repository,
    load_config,
    normalize_repo_url,
    remove_repository,
    save_config,
    update_daemon_config,
    update_repository,
)


def test_load_config_missing_file_returns_defaults(tmp_path: Path) -> None:
    cfg = load_config(str(tmp_path / "does-not-exist.yml"))

    assert isinstance(cfg, AppConfig)
    assert cfg.repositories == []
    assert cfg.daemon.poll_interval_sec == 60
    assert cfg.daemon.review_timeout_min == 20
    assert cfg.daemon.hung_fallback_codex_review is True
    assert cfg.daemon.error_handler_use_ai is True
    assert cfg.daemon.claude_model == "opus"
    assert cfg.web.host == "0.0.0.0"
    assert cfg.web.port == 8000
    assert cfg.auth.claude_config_dir == "/data/auth/claude"
    assert cfg.auth.gh_config_dir == "/data/auth/gh"


def test_load_config_missing_file_applies_env_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PO_FIX_ITERATION_CAP", "4")
    monkeypatch.setenv("PO_STALE_REVIEW_THRESHOLD_MIN", "12")

    cfg = load_config(str(tmp_path / "does-not-exist.yml"))

    assert cfg.daemon.fix_iteration_cap == 4
    assert cfg.daemon.stale_review_threshold_min == 12


def test_load_config_raw_omits_runtime_env_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PO_FIX_ITERATION_CAP", "4")

    raw = config_module._load_config_raw(str(tmp_path / "does-not-exist.yml"))

    assert raw == {}


def test_load_config_honors_po_config_path_env_var(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.test.yml"
    cfg_path.write_text(
        """
repositories:
  - url: https://github.com/example/test-repo.git
    branch: main
daemon:
  poll_interval_sec: 2
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("PO_CONFIG_PATH", str(cfg_path))

    cfg = load_config()

    assert len(cfg.repositories) == 1
    assert cfg.repositories[0].url == "https://github.com/example/test-repo.git"
    assert cfg.daemon.poll_interval_sec == 2


def test_load_config_explicit_path_ignores_po_config_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    explicit_cfg = tmp_path / "explicit.yml"
    explicit_cfg.write_text(
        """
repositories:
  - url: https://github.com/example/explicit-repo.git
""",
        encoding="utf-8",
    )
    env_cfg = tmp_path / "env.yml"
    env_cfg.write_text(
        """
repositories:
  - url: https://github.com/example/env-repo.git
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("PO_CONFIG_PATH", str(env_cfg))

    cfg = load_config(str(explicit_cfg))

    assert [r.url for r in cfg.repositories] == [
        "https://github.com/example/explicit-repo.git"
    ]


def test_load_config_unset_po_config_path_defaults_to_config_yml(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("PO_CONFIG_PATH", raising=False)
    monkeypatch.chdir(tmp_path)

    cfg = load_config()

    assert isinstance(cfg, AppConfig)
    assert cfg.repositories == []


def test_daemon_config_claude_model_default() -> None:
    from src.config import DaemonConfig

    assert DaemonConfig().claude_model == "opus"


def test_daemon_config_selector_defaults() -> None:
    from src.config import DaemonConfig

    cfg = DaemonConfig()

    assert cfg.auto_fallback is True
    assert cfg.coder_priority == {"codex": 81, "claude": 76}
    assert cfg.exploration_epsilon == 0.15


def test_load_config_valid_yaml(tmp_path: Path) -> None:
    yaml_text = """
repositories:
  - url: https://github.com/example/repo.git
    branch: develop
    auto_merge: false
    review_timeout_min: 30

daemon:
  poll_interval_sec: 90
  review_timeout_min: 45
  hung_fallback_codex_review: false
  error_handler_use_ai: false

web:
  host: 127.0.0.1
  port: 9000

auth:
  claude_config_dir: /tmp/claude
  gh_config_dir: /tmp/gh
"""
    config_file = tmp_path / "config.yml"
    config_file.write_text(yaml_text, encoding="utf-8")

    cfg = load_config(str(config_file))

    assert len(cfg.repositories) == 1
    repo = cfg.repositories[0]
    assert repo.url == "https://github.com/example/repo.git"
    assert repo.branch == "develop"
    assert repo.auto_merge is False
    assert repo.review_timeout_min == 30

    assert cfg.daemon.poll_interval_sec == 90
    assert cfg.daemon.review_timeout_min == 45
    assert cfg.daemon.hung_fallback_codex_review is False
    assert cfg.daemon.error_handler_use_ai is False

    assert cfg.web.host == "127.0.0.1"
    assert cfg.web.port == 9000

    assert cfg.auth.claude_config_dir == "/tmp/claude"
    assert cfg.auth.gh_config_dir == "/tmp/gh"


def test_repo_config_defaults() -> None:
    repo = RepoConfig(url="https://github.com/example/repo.git")

    assert repo.url == "https://github.com/example/repo.git"
    assert repo.branch == "main"
    assert repo.auto_merge is True
    # ``review_timeout_min`` is an optional override that defaults to
    # ``None``; when unset, the runner falls back to
    # ``daemon.review_timeout_min`` so a repo added through the Settings
    # UI without a custom timeout inherits whatever PR-016's daemon
    # control is set to.
    assert repo.review_timeout_min is None
    assert repo.disabled_coders is None


def test_normalize_repo_url_strips_git_and_slash() -> None:
    assert (
        normalize_repo_url("https://github.com/o/r.git")
        == "https://github.com/o/r"
    )
    assert (
        normalize_repo_url("https://github.com/o/r/")
        == "https://github.com/o/r"
    )
    assert (
        normalize_repo_url("https://github.com/o/r.git/")
        == "https://github.com/o/r"
    )
    assert (
        normalize_repo_url("  https://github.com/o/r  ")
        == "https://github.com/o/r"
    )


def test_save_config_round_trip(tmp_path: Path) -> None:
    config = AppConfig(
        repositories=[
            RepoConfig(
                url="https://github.com/octo/alpha.git",
                branch="dev",
                auto_merge=False,
                review_timeout_min=30,
            ),
        ],
    )
    config.daemon.poll_interval_sec = 90
    config.daemon.error_handler_use_ai = False
    config.web.port = 9000
    config.auth.claude_config_dir = "/tmp/claude"

    path = tmp_path / "config.yml"
    save_config(config, str(path))

    assert path.is_file()
    loaded = load_config(str(path))
    assert loaded.model_dump() == config.model_dump()


def test_save_config_atomic_overwrites_existing(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    path.write_text("repositories: []\n", encoding="utf-8")

    new_cfg = AppConfig(
        repositories=[RepoConfig(url="https://github.com/o/r.git")]
    )
    save_config(new_cfg, str(path))

    reloaded = load_config(str(path))
    assert len(reloaded.repositories) == 1
    assert reloaded.repositories[0].url == "https://github.com/o/r.git"
    # No leftover tmp files next to the target.
    siblings = [p.name for p in tmp_path.iterdir()]
    assert siblings == ["config.yml"], siblings


def test_save_config_cleans_up_temp_file_on_os_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "config.yml"

    def fail_replace(src: str, dst: Path) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(config_module.os, "replace", fail_replace)

    with pytest.raises(OSError, match="replace failed"):
        save_config(AppConfig(), str(path))

    assert not list(tmp_path.glob("config.yml.*.tmp"))


def test_save_config_cleanup_handles_missing_tmp_file_gracefully(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "config.yml"
    attempted_unlinks: list[str] = []
    original_unlink = config_module.os.unlink

    def fail_replace(src: str, dst: Path) -> None:
        original_unlink(src)
        raise OSError("replace failed")

    def missing_unlink(path_to_unlink: str) -> None:
        attempted_unlinks.append(path_to_unlink)
        raise FileNotFoundError(path_to_unlink)

    monkeypatch.setattr(config_module.os, "replace", fail_replace)
    monkeypatch.setattr(config_module.os, "unlink", missing_unlink)

    with pytest.raises(OSError, match="replace failed"):
        save_config(AppConfig(), str(path))

    assert attempted_unlinks
    assert not list(tmp_path.glob("config.yml.*.tmp"))


def test_add_repository_appends_and_persists(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = add_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        branch="dev",
        auto_merge=False,
    )
    assert len(cfg.repositories) == 1
    assert cfg.repositories[0].branch == "dev"
    assert cfg.repositories[0].auto_merge is False

    reloaded = load_config(str(path))
    assert len(reloaded.repositories) == 1
    assert reloaded.repositories[0].url == "https://github.com/octo/alpha.git"


def test_add_repository_rejects_duplicate_normalized_url(
    tmp_path: Path,
) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))

    with pytest.raises(ValueError, match="already configured"):
        add_repository("https://github.com/octo/alpha", str(path))
    with pytest.raises(ValueError, match="already configured"):
        add_repository("https://github.com/octo/alpha/", str(path))


def test_add_repository_rejects_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Unknown repository fields"):
        add_repository(
            "https://github.com/o/r.git", str(path), nonsense=True
        )


def test_remove_repository_removes_and_persists(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))
    add_repository("https://github.com/octo/beta.git", str(path))

    cfg = remove_repository("https://github.com/octo/alpha/", str(path))

    urls = [r.url for r in cfg.repositories]
    assert urls == ["https://github.com/octo/beta.git"]
    reloaded = load_config(str(path))
    assert [r.url for r in reloaded.repositories] == [
        "https://github.com/octo/beta.git"
    ]


def test_remove_repository_raises_on_missing(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Repository not found"):
        remove_repository("https://github.com/octo/missing.git", str(path))


def test_update_repository_updates_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        branch="main",
        auto_merge=True,
    )

    cfg = update_repository(
        "https://github.com/octo/alpha",
        str(path),
        branch="release",
        review_timeout_min=15,
    )
    repo = cfg.repositories[0]
    assert repo.branch == "release"
    assert repo.review_timeout_min == 15
    assert repo.auto_merge is True  # untouched
    assert repo.url == "https://github.com/octo/alpha.git"  # url untouched


def test_update_repository_raises_on_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))

    with pytest.raises(ValueError, match="Unknown repository fields"):
        update_repository(
            "https://github.com/octo/alpha.git", str(path), bogus=1
        )


def test_update_repository_raises_on_missing_repo(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Repository not found"):
        update_repository("https://github.com/octo/missing.git", str(path))


def test_update_repository_validates_patch_types(tmp_path: Path) -> None:
    """Malformed patches must raise and leave config.yml untouched."""
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        review_timeout_min=30,
    )
    before = path.read_text(encoding="utf-8")

    with pytest.raises(Exception):
        update_repository(
            "https://github.com/octo/alpha.git",
            str(path),
            review_timeout_min="not-an-int",
        )

    assert path.read_text(encoding="utf-8") == before
    reloaded = load_config(str(path))
    assert reloaded.repositories[0].review_timeout_min == 30


def test_update_daemon_config_updates_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = update_daemon_config(
        str(path),
        poll_interval_sec=120,
        error_handler_use_ai=False,
    )
    assert cfg.daemon.poll_interval_sec == 120
    assert cfg.daemon.error_handler_use_ai is False
    # Unchanged fields keep their previous values.
    assert cfg.daemon.review_timeout_min == 20
    assert cfg.daemon.hung_fallback_codex_review is True

    reloaded = load_config(str(path))
    assert reloaded.daemon.poll_interval_sec == 120
    assert reloaded.daemon.error_handler_use_ai is False


def test_update_daemon_config_accepts_large_diff_thresholds(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = update_daemon_config(
        str(path),
        large_diff_addition_threshold=2000,
        large_diff_files_threshold=40,
    )

    assert cfg.daemon.large_diff_addition_threshold == 2000
    assert cfg.daemon.large_diff_files_threshold == 40
    reloaded = load_config(str(path))
    assert reloaded.daemon.large_diff_addition_threshold == 2000
    assert reloaded.daemon.large_diff_files_threshold == 40


def test_update_daemon_config_selector_fields(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = update_daemon_config(
        str(path),
        auto_fallback=False,
        exploration_epsilon=0.25,
        coder_priority={"claude": 10, "codex": 20},
    )

    assert cfg.daemon.auto_fallback is False
    assert cfg.daemon.exploration_epsilon == 0.25
    assert cfg.daemon.coder_priority == {"claude": 10, "codex": 20}


def test_update_daemon_config_does_not_persist_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "config.yml"
    path.write_text("daemon:\n  review_timeout_min: 60\n", encoding="utf-8")
    monkeypatch.setenv("PO_FIX_ITERATION_CAP", "4")

    updated = update_daemon_config(str(path), poll_interval_sec=45)

    assert updated.daemon.poll_interval_sec == 45
    assert "fix_iteration_cap: 4" not in path.read_text(encoding="utf-8")
    assert load_config(str(path)).daemon.fix_iteration_cap == 4

    monkeypatch.delenv("PO_FIX_ITERATION_CAP")
    reloaded = load_config(str(path))
    assert reloaded.daemon.fix_iteration_cap == 25


def test_daemon_config_rejects_exploration_epsilon_out_of_range() -> None:
    from pydantic import ValidationError
    from src.config import DaemonConfig

    with pytest.raises(ValidationError):
        DaemonConfig(exploration_epsilon=-0.01)
    with pytest.raises(ValidationError):
        DaemonConfig(exploration_epsilon=0.51)


def test_update_daemon_config_rejects_unknown_field(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    with pytest.raises(ValueError, match="Unknown daemon fields"):
        update_daemon_config(str(path), bogus=True)


def test_update_daemon_config_accepts_operator_availability_fields(
    tmp_path: Path,
) -> None:
    """PR-255 active-hours fields must be tunable via update_daemon_config().

    The heartbeat composition reads ``operator_active_hours_*`` and
    ``operator_timezone`` from ``DaemonConfig``. If these aren't on the
    update allowlist, settings/API mutations raise ``ValueError`` and
    operators are forced to hand-edit YAML.
    """
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))

    cfg = update_daemon_config(
        str(path),
        operator_active_hours_start=8,
        operator_active_hours_end=20,
        operator_timezone="America/New_York",
    )
    assert cfg.daemon.operator_active_hours_start == 8
    assert cfg.daemon.operator_active_hours_end == 20
    assert cfg.daemon.operator_timezone == "America/New_York"

    reloaded = load_config(str(path))
    assert reloaded.daemon.operator_active_hours_start == 8
    assert reloaded.daemon.operator_active_hours_end == 20
    assert reloaded.daemon.operator_timezone == "America/New_York"


def test_update_daemon_config_validates_patch_types(tmp_path: Path) -> None:
    """Malformed daemon patches must raise and leave config.yml untouched."""
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    update_daemon_config(str(path), poll_interval_sec=45)
    before = path.read_text(encoding="utf-8")

    with pytest.raises(Exception):
        update_daemon_config(str(path), poll_interval_sec="nope")

    assert path.read_text(encoding="utf-8") == before
    reloaded = load_config(str(path))
    assert reloaded.daemon.poll_interval_sec == 45


def test_fix_idle_timeout_default() -> None:
    from src.config import DaemonConfig

    assert DaemonConfig().fix_idle_timeout_sec == 1800


def test_fix_iteration_cap_default() -> None:
    from src.config import DaemonConfig

    assert DaemonConfig().fix_iteration_cap == 25


def test_fix_review_timeout_removed() -> None:
    from src.config import DaemonConfig

    assert not hasattr(DaemonConfig(), "fix_review_timeout_sec")


def test_daemon_config_planned_pr_timeout_default() -> None:
    from src.config import DaemonConfig

    assert DaemonConfig().planned_pr_timeout_sec == 3600


def test_config_rejects_negative_review_timeout(tmp_path: Path) -> None:
    from pydantic import ValidationError

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  review_timeout_min: -1\n", encoding="utf-8"
    )

    with pytest.raises(ValidationError):
        load_config(str(cfg_path))


def test_config_rejects_zero_planned_pr_timeout(tmp_path: Path) -> None:
    from pydantic import ValidationError

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  planned_pr_timeout_sec: 0\n", encoding="utf-8"
    )

    with pytest.raises(ValidationError):
        load_config(str(cfg_path))


def test_update_daemon_config_accepts_timeouts(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon: {}\n", encoding="utf-8")
    updated = update_daemon_config(
        path=str(cfg_path),
        fix_idle_timeout_sec=2000,
        fix_iteration_cap=12,
        planned_pr_timeout_sec=1200,
    )
    assert updated.daemon.fix_idle_timeout_sec == 2000
    assert updated.daemon.fix_iteration_cap == 12
    assert updated.daemon.planned_pr_timeout_sec == 1200


def test_fix_idle_timeout_rejects_zero_or_negative() -> None:
    from pydantic import ValidationError
    from src.config import DaemonConfig

    with pytest.raises(ValidationError):
        DaemonConfig(fix_idle_timeout_sec=0)
    with pytest.raises(ValidationError):
        DaemonConfig(fix_idle_timeout_sec=-5)


def test_fix_iteration_cap_rejects_zero_or_negative() -> None:
    from pydantic import ValidationError
    from src.config import DaemonConfig

    with pytest.raises(ValidationError):
        DaemonConfig(fix_iteration_cap=0)
    with pytest.raises(ValidationError):
        DaemonConfig(fix_iteration_cap=-1)


def test_daemon_config_rate_limit_defaults() -> None:
    from src.config import DaemonConfig

    assert DaemonConfig().rate_limit_session_pause_percent == 95
    assert DaemonConfig().rate_limit_weekly_pause_percent == 100


def test_config_rejects_rate_limit_over_100(tmp_path: Path) -> None:
    from pydantic import ValidationError

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  rate_limit_session_pause_percent: 101\n",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError):
        load_config(str(cfg_path))


def test_update_daemon_config_rate_limit_session(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon: {}\n", encoding="utf-8")
    updated = update_daemon_config(
        path=str(cfg_path),
        rate_limit_session_pause_percent=75,
    )
    assert updated.daemon.rate_limit_session_pause_percent == 75


def test_update_daemon_config_rate_limit_weekly(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon: {}\n", encoding="utf-8")
    updated = update_daemon_config(
        path=str(cfg_path),
        rate_limit_weekly_pause_percent=90,
    )
    assert updated.daemon.rate_limit_weekly_pause_percent == 90


def test_deprecated_rate_limit_pause_percent(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  rate_limit_pause_percent: 80\n", encoding="utf-8"
    )
    from src.config import load_config

    cfg = load_config(str(cfg_path))
    assert cfg.daemon.rate_limit_session_pause_percent == 80
    assert cfg.daemon.rate_limit_weekly_pause_percent == 100


def test_load_config_migrates_fix_review_timeout_sec_to_fix_idle_timeout_sec(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  fix_review_timeout_sec: 120\n", encoding="utf-8"
    )

    cfg = load_config(str(cfg_path))

    assert cfg.daemon.fix_idle_timeout_sec == 120


def test_load_config_does_not_override_existing_fix_idle_timeout_sec(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n"
        "  fix_review_timeout_sec: 120\n"
        "  fix_idle_timeout_sec: 240\n",
        encoding="utf-8",
    )

    cfg = load_config(str(cfg_path))

    assert cfg.daemon.fix_idle_timeout_sec == 240


def test_load_config_applies_fix_iteration_cap_env_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n"
        "  fix_iteration_cap: 15\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PO_FIX_ITERATION_CAP", "5")

    cfg = load_config(str(cfg_path))

    assert cfg.daemon.fix_iteration_cap == 5


def test_apply_daemon_env_overrides_creates_daemon_section(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw: dict[str, object] = {}
    monkeypatch.setenv("PO_FIX_ITERATION_CAP", "4")

    config_module._apply_daemon_env_overrides(raw)

    assert raw == {"daemon": {"fix_iteration_cap": "4"}}


def test_apply_daemon_env_overrides_ignores_non_mapping_daemon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw: dict[str, object] = {"daemon": "invalid"}
    monkeypatch.setenv("PO_FIX_ITERATION_CAP", "6")

    config_module._apply_daemon_env_overrides(raw)

    assert raw == {"daemon": "invalid"}


def test_repo_poll_interval_default() -> None:
    repo = RepoConfig(url="https://github.com/example/repo")
    assert repo.poll_interval_sec == 60


def test_poll_interval_validator_defaults_to_60_when_none() -> None:
    repo = RepoConfig(
        url="https://github.com/example/repo", poll_interval_sec=None
    )
    assert repo.poll_interval_sec == 60


def test_repo_poll_interval_rejects_zero() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        RepoConfig(url="https://github.com/example/repo", poll_interval_sec=0)


def test_repo_poll_interval_rejects_negative() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        RepoConfig(url="https://github.com/example/repo", poll_interval_sec=-5)


def test_repo_poll_interval_rejects_float() -> None:
    with pytest.raises(ValueError, match="must be an integer"):
        RepoConfig(url="https://github.com/example/repo", poll_interval_sec=1.9)


def test_repo_allow_merge_without_checks_default() -> None:
    repo = RepoConfig(url="https://github.com/example/repo")
    assert repo.allow_merge_without_checks is False
    assert repo.allow_merge_without_review is False


def test_repo_allow_merge_without_checks_loads_from_yaml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/repo\n"
        "    allow_merge_without_checks: true\n",
        encoding="utf-8",
    )
    cfg = load_config(str(cfg_path))
    assert cfg.repositories[0].allow_merge_without_checks is True


def test_repo_allow_merge_without_review_loads_from_yaml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/repo\n"
        "    allow_merge_without_review: true\n",
        encoding="utf-8",
    )
    cfg = load_config(str(cfg_path))
    assert cfg.repositories[0].allow_merge_without_review is True


def test_config_rejects_invalid_port(tmp_path: Path) -> None:
    from pydantic import ValidationError

    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("web:\n  port: 70000\n", encoding="utf-8")

    with pytest.raises(ValidationError):
        load_config(str(cfg_path))


def test_update_repository_allow_merge_without_checks(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))

    cfg = update_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        allow_merge_without_checks=True,
    )
    assert cfg.repositories[0].allow_merge_without_checks is True

    reloaded = load_config(str(path))
    assert reloaded.repositories[0].allow_merge_without_checks is True


def test_update_daemon_config_accepts_strict_queue_validation(
    tmp_path: Path,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon: {}\n", encoding="utf-8")
    updated = update_daemon_config(
        path=str(cfg_path),
        strict_queue_validation=False,
    )
    assert updated.daemon.strict_queue_validation is False


def test_coder_type_enum_values() -> None:
    from src.config import CoderType

    assert CoderType.CLAUDE.value == "claude"
    assert CoderType.CODEX.value == "codex"


def test_daemon_config_default_coder_is_claude() -> None:
    from src.config import CoderType, DaemonConfig

    d = DaemonConfig()
    assert d.coder == CoderType.CLAUDE
    assert d.codex_model == ""


def test_repo_config_coder_override_none_inherits_daemon() -> None:
    repo = RepoConfig(url="https://github.com/example/repo")
    assert repo.coder is None


def test_repo_config_coder_override_codex() -> None:
    from src.config import CoderType

    repo = RepoConfig(url="https://github.com/example/repo", coder="codex")
    assert repo.coder == CoderType.CODEX


def test_update_daemon_config_coder(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon: {}\n", encoding="utf-8")
    updated = update_daemon_config(path=str(cfg_path), coder="codex")
    assert updated.daemon.coder.value == "codex"


def test_update_daemon_config_codex_model(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon: {}\n", encoding="utf-8")
    updated = update_daemon_config(path=str(cfg_path), codex_model="o4-mini")
    assert updated.daemon.codex_model == "o4-mini"


def test_update_repository_coder_override(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))

    cfg = update_repository(
        "https://github.com/octo/alpha.git",
        str(path),
        coder="codex",
    )
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"


def test_update_repository_coder_clear(tmp_path: Path) -> None:
    path = tmp_path / "config.yml"
    save_config(AppConfig(), str(path))
    add_repository("https://github.com/octo/alpha.git", str(path))
    update_repository(
        "https://github.com/octo/alpha.git", str(path), coder="codex"
    )

    cfg = update_repository(
        "https://github.com/octo/alpha.git", str(path), coder=None
    )
    assert cfg.repositories[0].coder is None


def test_load_config_no_overlay_uses_base(tmp_path: Path) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "daemon:\n  poll_interval_sec: 45\n", encoding="utf-8"
    )

    cfg = load_config(str(base))

    assert cfg.daemon.poll_interval_sec == 45
    # Defaults remain intact for fields the base does not pin.
    assert cfg.daemon.fix_iteration_cap == 25


def test_load_config_overlay_merges_over_base(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "daemon:\n"
        "  poll_interval_sec: 45\n"
        "  fix_iteration_cap: 8\n"
        "web:\n"
        "  host: 127.0.0.1\n"
        "  port: 9000\n",
        encoding="utf-8",
    )
    overlay = tmp_path / "config.production.yml"
    overlay.write_text(
        "daemon:\n"
        "  fix_iteration_cap: 25\n"
        "  rate_limit_session_pause_percent: 90\n"
        "web:\n"
        "  port: 9100\n",
        encoding="utf-8",
    )

    with caplog.at_level("INFO", logger="src.config"):
        cfg = load_config(str(base))

    # Overlay wins per-field; base values for un-overridden fields survive.
    assert cfg.daemon.fix_iteration_cap == 25
    assert cfg.daemon.rate_limit_session_pause_percent == 90
    assert cfg.daemon.poll_interval_sec == 45
    assert cfg.web.host == "127.0.0.1"
    assert cfg.web.port == 9100

    info_messages = [
        rec.getMessage() for rec in caplog.records if rec.levelname == "INFO"
    ]
    applied_msg = next(
        (m for m in info_messages if "overlay fields" in m), None
    )
    assert applied_msg is not None, info_messages
    assert "daemon.fix_iteration_cap" in applied_msg
    assert "web.port" in applied_msg


def test_load_config_overlay_unknown_field_warns_and_ignored(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "daemon:\n  poll_interval_sec: 45\n", encoding="utf-8"
    )
    overlay = tmp_path / "config.production.yml"
    overlay.write_text(
        "daemon:\n"
        "  poll_interval_sec: 90\n"
        "  future_only_field: 1\n"
        "totally_unknown_section:\n"
        "  whatever: true\n",
        encoding="utf-8",
    )

    with caplog.at_level("INFO", logger="src.config"):
        cfg = load_config(str(base))

    # Known overlay keys still apply; pydantic's default extra="ignore"
    # silently drops the unknown ones during validation.
    assert cfg.daemon.poll_interval_sec == 90
    warnings = [
        rec.getMessage() for rec in caplog.records if rec.levelname == "WARNING"
    ]
    assert any("daemon.future_only_field" in m for m in warnings), warnings
    assert any("totally_unknown_section" in m for m in warnings), warnings

    # The "Applied overlay fields" log must NOT advertise a key that was
    # just warned as unknown — operators verifying a deploy rely on this
    # line to see what actually took effect.
    info_messages = [
        rec.getMessage() for rec in caplog.records if rec.levelname == "INFO"
    ]
    applied_msg = next(
        (m for m in info_messages if "overlay fields" in m), None
    )
    assert applied_msg is not None, info_messages
    assert "daemon.poll_interval_sec" in applied_msg
    assert "future_only_field" not in applied_msg
    assert "totally_unknown_section" not in applied_msg


def test_load_config_overlay_warns_unknown_field_inside_list_item(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    base = tmp_path / "config.yml"
    base.write_text("repositories: []\n", encoding="utf-8")
    overlay = tmp_path / "config.production.yml"
    overlay.write_text(
        "repositories:\n"
        "  - url: https://github.com/o/r.git\n"
        "    made_up_field: true\n",
        encoding="utf-8",
    )

    with caplog.at_level("WARNING", logger="src.config"):
        cfg = load_config(str(base))

    # The unknown sub-key is dropped by pydantic, but the rest of the
    # repository entry still validates and the overlay still replaces the
    # base list — operators must not be misled into thinking the typo
    # took effect.
    assert len(cfg.repositories) == 1
    assert cfg.repositories[0].url == "https://github.com/o/r.git"
    warnings = [
        rec.getMessage() for rec in caplog.records if rec.levelname == "WARNING"
    ]
    assert any(
        "repositories[0].made_up_field" in m for m in warnings
    ), warnings


def test_load_config_overlay_must_be_sibling_of_base(tmp_path: Path) -> None:
    nested = tmp_path / "etc"
    nested.mkdir()
    base = nested / "config.yml"
    base.write_text(
        "daemon:\n  fix_iteration_cap: 8\n", encoding="utf-8"
    )
    # An overlay placed outside the base directory must NOT be picked up,
    # so an operator cannot accidentally point at the wrong file from a
    # different working directory.
    far_overlay = tmp_path / "config.production.yml"
    far_overlay.write_text(
        "daemon:\n  fix_iteration_cap: 25\n", encoding="utf-8"
    )

    cfg = load_config(str(base))

    assert cfg.daemon.fix_iteration_cap == 8

    # The sibling overlay IS picked up.
    sibling_overlay = nested / "config.production.yml"
    sibling_overlay.write_text(
        "daemon:\n  fix_iteration_cap: 25\n", encoding="utf-8"
    )

    cfg = load_config(str(base))
    assert cfg.daemon.fix_iteration_cap == 25


def test_load_config_overlay_empty_file_is_noop(tmp_path: Path) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "daemon:\n  poll_interval_sec: 45\n", encoding="utf-8"
    )
    overlay = tmp_path / "config.production.yml"
    overlay.write_text("", encoding="utf-8")

    cfg = load_config(str(base))

    assert cfg.daemon.poll_interval_sec == 45


def test_load_config_overlay_replaces_lists(tmp_path: Path) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "repositories:\n"
        "  - url: https://github.com/octo/alpha.git\n"
        "  - url: https://github.com/octo/beta.git\n",
        encoding="utf-8",
    )
    overlay = tmp_path / "config.production.yml"
    overlay.write_text(
        "repositories:\n"
        "  - url: https://github.com/octo/gamma.git\n",
        encoding="utf-8",
    )

    cfg = load_config(str(base))

    assert [r.url for r in cfg.repositories] == [
        "https://github.com/octo/gamma.git"
    ]


def test_resolve_nested_model_handles_optional_and_list() -> None:
    from src.config import (
        AppConfig,
        DaemonConfig,
        RepoConfig,
        _resolve_nested_model,
    )

    daemon_field = AppConfig.model_fields["daemon"]
    assert _resolve_nested_model(daemon_field.annotation) is DaemonConfig

    coder_field = RepoConfig.model_fields["coder"]
    # Optional[CoderType] is not a BaseModel — must return None, not a guess.
    assert _resolve_nested_model(coder_field.annotation) is None

    repos_field = AppConfig.model_fields["repositories"]
    # ``list[RepoConfig]`` is handled by ``_list_item_model`` instead, so
    # ``_resolve_nested_model`` returns None for list annotations.
    assert _resolve_nested_model(repos_field.annotation) is None

    # Optional[BaseModel] / ``Model | None`` must unwrap to the model. None
    # of the existing schema fields use this shape, so build it inline to
    # exercise the union-arg branch.
    assert (
        _resolve_nested_model(DaemonConfig | None) is DaemonConfig
    )


def test_list_item_model_resolves_repositories() -> None:
    from src.config import AppConfig, RepoConfig, _list_item_model

    repos_field = AppConfig.model_fields["repositories"]
    assert _list_item_model(repos_field.annotation) is RepoConfig

    daemon_field = AppConfig.model_fields["daemon"]
    # Non-list annotations have no list item model.
    assert _list_item_model(daemon_field.annotation) is None

    # ``list[scalar]`` annotations (e.g. ``RepoConfig.disabled_coders``)
    # are lists, but their item type is not a BaseModel — the function
    # must return None rather than guessing.
    assert _list_item_model(list[str]) is None


def test_collect_unknown_overlay_keys_walks_nested_models() -> None:
    from src.config import AppConfig, _collect_unknown_overlay_keys

    overlay = {
        "daemon": {
            "poll_interval_sec": 45,
            "made_up_field": True,
        },
        "extras": {"x": 1},
    }
    paths = _collect_unknown_overlay_keys(overlay, AppConfig)

    assert "daemon.made_up_field" in paths
    assert "extras" in paths
    assert "daemon.poll_interval_sec" not in paths


def test_collect_unknown_overlay_keys_walks_list_items() -> None:
    from src.config import AppConfig, _collect_unknown_overlay_keys

    overlay = {
        "repositories": [
            {
                "url": "https://github.com/o/r.git",
                "made_up_field": 1,
                # ``disabled_coders`` is ``list[str]`` — a list whose item
                # type is not a BaseModel. The collector must skip the
                # descent silently rather than treat scalar items as typos.
                "disabled_coders": ["claude"],
            },
            {"url": "https://github.com/o/s.git", "branch": "main"},
            "scalar-not-a-dict",
        ],
    }
    paths = _collect_unknown_overlay_keys(overlay, AppConfig)

    assert "repositories[0].made_up_field" in paths
    # Items that are entirely valid (or non-dict scalars that pydantic
    # will reject for its own reasons) must not produce phantom warnings.
    assert all(not p.startswith("repositories[1]") for p in paths), paths
    assert all(not p.startswith("repositories[2]") for p in paths), paths
    # ``disabled_coders`` itself is a known field and its scalar items
    # must not be flagged as unknown.
    assert all(
        "disabled_coders" not in p for p in paths
    ), paths


def test_collect_unknown_overlay_keys_handles_non_dict_input() -> None:
    from src.config import AppConfig, _collect_unknown_overlay_keys

    assert _collect_unknown_overlay_keys("not-a-dict", AppConfig) == []  # type: ignore[arg-type]


def test_applied_overlay_paths_flattens_nested_known_keys() -> None:
    from src.config import AppConfig, _applied_overlay_paths

    overlay = {
        "daemon": {
            "poll_interval_sec": 45,
            "coder_priority": {"claude": 10, "codex": 20},
        },
        "web": {"port": 9100},
        "repositories": [{"url": "https://github.com/o/r.git"}],
    }
    paths = sorted(_applied_overlay_paths(overlay, AppConfig))

    # ``coder_priority`` is ``dict[str, int]`` — not a nested BaseModel —
    # so the function does not descend; it appears as the field name.
    # ``repositories`` is a list field so the wholesale-replaced list
    # surfaces as the field name itself, not per-item paths.
    assert paths == [
        "daemon.coder_priority",
        "daemon.poll_interval_sec",
        "repositories",
        "web.port",
    ]


def test_applied_overlay_paths_skips_unknown_keys() -> None:
    from src.config import AppConfig, _applied_overlay_paths

    overlay = {
        "daemon": {
            "poll_interval_sec": 45,
            "future_only_field": 1,
        },
        "totally_unknown_section": {"whatever": True},
    }
    paths = _applied_overlay_paths(overlay, AppConfig)

    assert "daemon.poll_interval_sec" in paths
    assert "daemon.future_only_field" not in paths
    assert "totally_unknown_section" not in paths
    assert all("totally_unknown_section" not in p for p in paths)


def test_applied_overlay_paths_handles_non_dict_input() -> None:
    from src.config import AppConfig, _applied_overlay_paths

    assert _applied_overlay_paths("not-a-dict", AppConfig) == []  # type: ignore[arg-type]


def test_load_config_overlay_top_level_list_raises(tmp_path: Path) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "daemon:\n  poll_interval_sec: 45\n", encoding="utf-8"
    )
    overlay = tmp_path / "config.production.yml"
    overlay.write_text(
        "- daemon:\n    poll_interval_sec: 90\n", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="must be a YAML mapping"):
        load_config(str(base))


def test_load_config_overlay_top_level_scalar_raises(tmp_path: Path) -> None:
    base = tmp_path / "config.yml"
    base.write_text(
        "daemon:\n  poll_interval_sec: 45\n", encoding="utf-8"
    )
    overlay = tmp_path / "config.production.yml"
    overlay.write_text("just-a-string\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a YAML mapping"):
        load_config(str(base))


def test_deep_merge_overlay_replaces_scalar_with_dict(tmp_path: Path) -> None:
    from src.config import _deep_merge

    base = {"daemon": "scalar-base"}
    overlay = {"daemon": {"poll_interval_sec": 45}}

    merged = _deep_merge(base, overlay)

    assert merged == {"daemon": {"poll_interval_sec": 45}}


def test_committed_config_yml_uses_production_defaults() -> None:
    """The committed ``config.yml`` must ship operator-validated production
    values. Loss of `usage_api_beta_header` on 2026-05-01 is the cautionary
    tale — divergence between upstream defaults and the operator's running
    daemon caused an outage on a routine deploy.

    Reads the raw committed YAML and validates it against ``AppConfig``
    directly. ``load_config`` would deep-merge a sibling
    ``config.production.yml`` overlay and apply ``PO_*`` env overrides,
    which makes the assertion non-hermetic on developer/ops machines.

    Critical security invariants (e.g. usage_api_beta_header) use
    exact-equality checks because their loss caused real incidents.
    Operator-tunable policy fields (timeouts, iteration caps, rate-limit
    thresholds) use range-check assertions instead — the operator may
    legitimately tune them, but values outside sane ranges still signal a
    config bug or merge accident.
    """
    raw = config_module._load_config_raw("config.yml")
    cfg = AppConfig.model_validate(raw)

    assert 5 <= cfg.daemon.review_timeout_min <= 120
    assert 300 <= cfg.daemon.planned_pr_timeout_sec <= 86400
    assert 1 <= cfg.daemon.fix_iteration_cap <= 100
    assert cfg.daemon.usage_api_beta_header == "oauth-2025-04-20"
    assert 50 <= cfg.daemon.rate_limit_session_pause_percent <= 100
    assert 50 <= cfg.daemon.rate_limit_weekly_pause_percent <= 100
