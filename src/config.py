"""Configuration loader and Pydantic models for the pipeline orchestrator."""

from __future__ import annotations

import hashlib
import logging
import os
import stat as stat_module
import tempfile
import threading
import typing
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

OVERLAY_FILENAME = "config.production.yml"


class CoderType(str, Enum):
    CLAUDE = "claude"
    CODEX = "codex"

logger = logging.getLogger(__name__)

_REPO_FIELDS = {
    "url",
    "branch",
    "auto_merge",
    "review_timeout_min",
    "active",
    "poll_interval_sec",
    "allow_merge_without_checks",
    "allow_merge_without_review",
    "coder",
    "disabled_coders",
    "governance_scan_enabled",
    "feature_flags",
}

_DAEMON_FIELDS = {
    "poll_interval_sec",
    "review_timeout_min",
    "stale_review_threshold_min",
    "stale_review_threshold_eyes_min",
    "hung_fallback_codex_review",
    "error_handler_use_ai",
    "claude_model",
    "fix_idle_timeout_sec",
    "fix_iteration_cap",
    "fix_no_push_cap",
    "retry_button_cap",
    "fix_poll_interval_sec",
    "coder_terminate_grace_sec",
    "planned_pr_timeout_sec",
    "rate_limit_session_pause_percent",
    "rate_limit_weekly_pause_percent",
    "spend_ceiling_session_percent",
    "spend_ceiling_weekly_percent",
    "spend_ceiling_warning_percent",
    "strict_queue_validation",
    "upload_staging_max_age_hours",
    "usage_api_user_agent",
    "usage_api_beta_header",
    "usage_api_cache_ttl_sec",
    "install_statusline_hook",
    "auto_fallback",
    "coder_priority",
    "exploration_epsilon",
    "coder",
    "codex_model",
    "github_api_pause_threshold_percent",
    "github_api_slowdown_threshold_percent",
    "github_api_slowdown_multiplier",
    "idle_extended_poll_interval_sec",
    "idle_extended_after_cycles",
    "watch_slow_window_sec",
    "watch_slow_poll_interval_sec",
    "watch_fast_poll_interval_sec",
    "watch_retrigger_cap",
    "ci_pending_max_min",
    "error_rate_threshold",
    "error_rate_window_min",
    "error_rate_auto_pause_enabled",
    "cascade_escalate_threshold",
    "cascade_escalate_window_min",
    "cascade_escalate_auto_resume_min",
    "operator_active_hours_start",
    "operator_active_hours_end",
    "operator_timezone",
    "guardrail_notification_webhook_url",
    "guardrail_notification_min_tier",
    "guardrail_notification_timeout_seconds",
    "dashboard_base_url",
    "large_diff_addition_threshold",
    "large_diff_files_threshold",
    "mass_deletion_threshold",
    "test_deletion_threshold",
    "governance_scan_enabled",
    "main_commit_audit_interval_idle_cycles",
    "main_commit_audit_lookback_n",
    "git_bundle_backup_enabled",
    "git_bundle_backup_dir",
    "git_bundle_backup_interval_hours",
    "git_bundle_backup_daily_retention",
    "git_bundle_backup_weekly_retention",
    "coder_filesystem_isolation",
}

_DAEMON_ENV_OVERRIDES = {
    "PO_FIX_ITERATION_CAP": "fix_iteration_cap",
    "PO_STALE_REVIEW_THRESHOLD_MIN": "stale_review_threshold_min",
}


_EnvOverrideFingerprint = tuple[tuple[str, str | None], ...]


@dataclass(frozen=True)
class _ConfigFileSignature:
    mtime_ns: int
    ctime_ns: int
    size: int
    content_hash: str


@dataclass(frozen=True)
class _ConfigCacheEntry:
    config: "AppConfig"
    base_signature: _ConfigFileSignature
    overlay_signature: _ConfigFileSignature
    env_fingerprint: _EnvOverrideFingerprint


# Guards cache dict mutation only (microsecond hold). Disk reads and Pydantic
# validation run outside the lock so async web callers never queue behind a
# slow parse.
_config_cache_lock = threading.Lock()
_config_cache: dict[str, _ConfigCacheEntry] = {}


class FeatureFlags(BaseModel):
    # PR-329: dispatcher migration to ``is_work_inhibited`` is gated per
    # repo so canary rollout (PR-330) can flip one repo at a time without
    # touching the rest. Default True completes the WorkInhibitor cutover;
    # per-repo overrides to False remain available for targeted rollback.
    use_unified_inhibitor_check: bool = True
    # PR-380: single operator-clearable ERROR exit is canaried per repo.
    # Default False preserves the legacy ERROR branch until explicitly flipped.
    use_single_error_exit: bool = False


class RepoConfig(BaseModel):
    url: str
    branch: str = "main"
    auto_merge: bool = True
    # Optional per-repo override. ``None`` means "inherit
    # ``daemon.review_timeout_min``": the runner's hung-detection logic
    # falls back to the daemon-level setting whenever the repo itself
    # does not pin a timeout, so PR-016's "Default review timeout" UI
    # control actually steers every repo that has not opted into a
    # custom value.
    review_timeout_min: int | None = None
    active: bool = True
    poll_interval_sec: int = 60
    allow_merge_without_checks: bool = False
    allow_merge_without_review: bool = False
    coder: CoderType | None = None
    disabled_coders: list[str] | None = None
    governance_scan_enabled: bool | None = None
    feature_flags: FeatureFlags = Field(default_factory=FeatureFlags)

    @field_validator("poll_interval_sec", mode="before")
    @classmethod
    def _poll_interval_at_least_one(cls, v: Any) -> int:
        if v is None:
            return 60
        if not isinstance(v, int) or isinstance(v, bool):
            raise ValueError("poll_interval_sec must be an integer")
        if v < 1:
            raise ValueError("poll_interval_sec must be at least 1")
        return v


class DaemonConfig(BaseModel):
    poll_interval_sec: int = 60
    review_timeout_min: int = Field(default=20, ge=1)
    stale_review_threshold_min: int = Field(default=10, ge=1)
    stale_review_threshold_eyes_min: int = Field(default=5, ge=1)
    hung_fallback_codex_review: bool = True
    error_handler_use_ai: bool = True
    claude_model: str = "opus"
    fix_idle_timeout_sec: int = Field(default=1800, ge=1)
    fix_iteration_cap: int = Field(default=25, ge=1)
    fix_no_push_cap: int = Field(default=3, ge=1)
    retry_button_cap: int = Field(default=3, ge=1, le=20)
    fix_poll_interval_sec: int = Field(default=30, ge=1)
    coder_terminate_grace_sec: int = Field(default=5, ge=1)
    planned_pr_timeout_sec: int = Field(default=3600, ge=60)
    # Unified usage gate pause predicates. These legacy field names remain
    # parseable for the production overlay; internally the usage gate reads
    # through the ``usage_gate_*`` properties below.
    rate_limit_session_pause_percent: int = Field(default=95, ge=0, le=100)
    rate_limit_weekly_pause_percent: int = Field(default=100, ge=0, le=100)
    # Deprecated aliases for the usage gate's spend-ceiling pause predicate.
    spend_ceiling_session_percent: int | None = Field(default=None, ge=1, le=100)
    spend_ceiling_weekly_percent: int | None = Field(default=None, ge=1, le=100)
    spend_ceiling_warning_percent: int = Field(default=80, ge=1, le=100)
    strict_queue_validation: bool = True
    upload_staging_max_age_hours: int = Field(default=24, ge=1)
    usage_api_user_agent: str = "claude-code/2.1.104"
    usage_api_beta_header: str = "oauth-2025-04-20"
    usage_api_cache_ttl_sec: int = Field(default=60, ge=5, le=3600)
    install_statusline_hook: bool = True
    auto_fallback: bool = True
    coder_priority: dict[str, int] = Field(
        default_factory=lambda: {
            "codex": 81,
            "claude": 76,
        }
    )
    exploration_epsilon: float = Field(default=0.15, ge=0.0, le=0.5)
    coder: CoderType = CoderType.CLAUDE
    codex_model: str = ""
    github_api_pause_threshold_percent: int = Field(default=5, ge=0, le=100)
    github_api_slowdown_threshold_percent: int = Field(default=20, ge=0, le=100)
    github_api_slowdown_multiplier: int = Field(default=5, ge=1, le=60)
    idle_extended_poll_interval_sec: int = Field(default=300, ge=1)
    idle_extended_after_cycles: int = Field(default=3, ge=1)
    watch_slow_window_sec: int = Field(default=300, ge=1)
    watch_slow_poll_interval_sec: int = Field(default=300, ge=1)
    watch_fast_poll_interval_sec: int = Field(default=45, ge=1)
    watch_retrigger_cap: int = Field(default=3, ge=1)
    ci_pending_max_min: int = Field(default=30, ge=1)
    error_rate_threshold: int = Field(default=5, ge=1)
    error_rate_window_min: int = Field(default=60, ge=1)
    error_rate_auto_pause_enabled: bool = True
    cascade_escalate_threshold: int = Field(default=3, ge=0, le=20)
    cascade_escalate_window_min: int = Field(default=15, ge=1)
    cascade_escalate_auto_resume_min: int = Field(default=60, ge=0)
    operator_active_hours_start: int = Field(default=9, ge=0, le=23)
    operator_active_hours_end: int = Field(default=21, ge=1, le=24)
    operator_timezone: str = "Europe/Rome"
    guardrail_notification_webhook_url: str | None = Field(default=None)
    guardrail_notification_min_tier: int = Field(default=1, ge=1, le=2)
    guardrail_notification_timeout_seconds: float = Field(default=5.0, ge=1.0, le=30.0)
    dashboard_base_url: str | None = Field(default=None)
    large_diff_addition_threshold: int = Field(default=1500, ge=100)
    large_diff_files_threshold: int = Field(default=30, ge=2)
    mass_deletion_threshold: int = Field(default=20, ge=1)
    test_deletion_threshold: int = Field(default=5, ge=1)
    governance_scan_enabled: bool = True
    main_commit_audit_interval_idle_cycles: int = Field(default=20, ge=1)
    main_commit_audit_lookback_n: int = Field(default=10, ge=1, le=50)
    git_bundle_backup_enabled: bool = Field(default=False)
    git_bundle_backup_dir: str | None = Field(default=None)
    git_bundle_backup_interval_hours: int = Field(default=24, ge=1)
    git_bundle_backup_daily_retention: int = Field(default=7, ge=1)
    git_bundle_backup_weekly_retention: int = Field(default=4, ge=0)
    coder_filesystem_isolation: bool = Field(default=False)

    @property
    def usage_gate_rate_limit_session_pause_percent(self) -> int:
        return self.rate_limit_session_pause_percent

    @property
    def usage_gate_rate_limit_weekly_pause_percent(self) -> int:
        return self.rate_limit_weekly_pause_percent

    @property
    def usage_gate_spend_ceiling_session_percent(self) -> int | None:
        return self.spend_ceiling_session_percent

    @property
    def usage_gate_spend_ceiling_weekly_percent(self) -> int | None:
        return self.spend_ceiling_weekly_percent


class WebConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = Field(default=8000, ge=1, le=65535)


class AuthConfig(BaseModel):
    claude_config_dir: str = "/data/auth/claude"
    gh_config_dir: str = "/data/auth/gh"
    codex_home_dir: str = "/data/auth"


class AppConfig(BaseModel):
    repositories: list[RepoConfig] = Field(default_factory=list)
    daemon: DaemonConfig = Field(default_factory=DaemonConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)


def _load_config_raw(path: str = "config.yml") -> dict[str, Any]:
    """Return the parsed config mapping from ``path`` or an empty mapping."""
    config_path = Path(path)
    if not config_path.is_file():
        return {}

    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    daemon = raw.get("daemon")
    if isinstance(daemon, dict):
        legacy = daemon.pop("fix_review_timeout_sec", None)
        if legacy is not None and "fix_idle_timeout_sec" not in daemon:
            daemon["fix_idle_timeout_sec"] = legacy

        legacy_rate = daemon.pop("rate_limit_pause_percent", None)
        if legacy_rate is not None:
            logger.warning(
                "Deprecated config field 'rate_limit_pause_percent' — "
                "use 'rate_limit_session_pause_percent' and "
                "'rate_limit_weekly_pause_percent' instead"
            )
            if "rate_limit_session_pause_percent" not in daemon:
                daemon["rate_limit_session_pause_percent"] = legacy_rate

    return raw


def invalidate_config_cache() -> None:
    """Clear cached ``load_config`` results after a known config write."""
    with _config_cache_lock:
        _config_cache.clear()


def _config_file_signature(path: Path) -> _ConfigFileSignature:
    """Return cache-relevant file metadata, treating a missing file as absent."""
    try:
        stat = path.stat()
    except OSError:
        return _ConfigFileSignature(mtime_ns=0, ctime_ns=0, size=0, content_hash="")
    if not stat_module.S_ISREG(stat.st_mode):
        return _ConfigFileSignature(mtime_ns=0, ctime_ns=0, size=0, content_hash="")
    content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    return _ConfigFileSignature(
        mtime_ns=stat.st_mtime_ns,
        ctime_ns=stat.st_ctime_ns,
        size=stat.st_size,
        content_hash=content_hash,
    )


def _daemon_env_override_fingerprint() -> _EnvOverrideFingerprint:
    """Return the runtime env values that affect ``load_config`` output."""
    return tuple(
        (env_name, os.environ.get(env_name))
        for env_name in sorted(_DAEMON_ENV_OVERRIDES)
    )


def load_config(path: str | None = None) -> AppConfig:
    """Read a YAML config file and return an AppConfig.

    If the file is missing, return an AppConfig populated with defaults.
    Env overrides apply to this runtime view only.

    When ``path`` is omitted, the path is resolved from the
    ``PO_CONFIG_PATH`` environment variable, falling back to
    ``"config.yml"`` when the variable is unset. Explicit paths are
    honored as-is so callers that pin a config file (e.g. unit tests,
    settings writers) keep their existing semantics.

    If a sibling ``config.production.yml`` exists next to the resolved
    base path, it is deep-merged on top of the base (overlay wins for
    scalars and lists; nested mappings merge recursively). The overlay
    is gitignored by convention so production overrides survive
    ``git reset`` without polluting the committed config.
    """
    selected_path = (
        path if path is not None else os.environ.get("PO_CONFIG_PATH", "config.yml")
    )
    base_path = Path(selected_path).absolute()
    overlay_path = base_path.parent / OVERLAY_FILENAME
    cache_key = str(base_path)
    base_signature = _config_file_signature(base_path)
    overlay_signature = _config_file_signature(overlay_path)
    env_fingerprint = _daemon_env_override_fingerprint()

    with _config_cache_lock:
        cached = _config_cache.get(cache_key)
        if (
            cached is not None
            and cached.base_signature == base_signature
            and cached.overlay_signature == overlay_signature
            and cached.env_fingerprint == env_fingerprint
        ):
            return cached.config.model_copy(deep=True)

    raw = _load_config_raw(str(base_path))

    overlay = _load_overlay_raw(base_path)
    if overlay:
        unknown = _collect_unknown_overlay_keys(overlay, AppConfig)
        for key in unknown:
            logger.warning(
                "Overlay %s contains unknown field '%s' — ignored",
                OVERLAY_FILENAME,
                key,
            )
        raw = _deep_merge(raw, overlay)
        applied = _applied_overlay_paths(overlay, AppConfig)
        if applied:
            logger.info(
                "Applied %s overlay fields: %s",
                OVERLAY_FILENAME,
                ", ".join(sorted(applied)),
            )

    _apply_daemon_env_overrides(raw)

    config = AppConfig.model_validate(raw)
    with _config_cache_lock:
        _config_cache[cache_key] = _ConfigCacheEntry(
            config=config,
            base_signature=base_signature,
            overlay_signature=overlay_signature,
            env_fingerprint=env_fingerprint,
        )
    return config.model_copy(deep=True)


def _load_overlay_raw(base_path: Path) -> dict[str, Any]:
    """Return overlay mapping if sibling ``config.production.yml`` exists.

    Resolves the overlay strictly as a sibling of ``base_path`` so
    operators cannot accidentally point the daemon at an overlay in an
    unrelated directory. Returns an empty mapping when the overlay does
    not exist or is empty.

    Raises ``ValueError`` if the overlay file is present but its
    top-level YAML is not a mapping. ``yaml.safe_load`` happily returns
    a list or scalar for a malformed file; without this guard the merge
    would later trip an ``AttributeError`` on ``.items()`` and take down
    daemon startup with an opaque traceback instead of a clear error.
    """
    overlay_path = base_path.parent / OVERLAY_FILENAME
    if not overlay_path.is_file():
        return {}

    with overlay_path.open("r", encoding="utf-8") as fh:
        loaded = yaml.safe_load(fh)

    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError(
            f"Overlay {OVERLAY_FILENAME} must be a YAML mapping at the "
            f"top level, got {type(loaded).__name__}"
        )
    return loaded


def _deep_merge(
    base: dict[str, Any], overlay: dict[str, Any]
) -> dict[str, Any]:
    """Return ``overlay`` merged into ``base``.

    Nested dicts merge recursively. Lists in the overlay replace lists
    in the base (no concat) so an operator can shrink a list without
    fighting accumulated entries.
    """
    merged: dict[str, Any] = dict(base)
    for key, value in overlay.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_nested_model(annotation: Any) -> type[BaseModel] | None:
    """Return the BaseModel subclass embedded in ``annotation`` if any.

    Recognizes plain ``Model`` and ``Optional[Model]`` / ``Model | None``
    forms. ``list[Model]`` is handled separately by ``_list_item_model``
    so the dict-recursion path stays free of list semantics.
    """
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    if typing.get_origin(annotation) is list:
        return None
    for arg in typing.get_args(annotation):
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return arg
    return None


def _list_item_model(annotation: Any) -> type[BaseModel] | None:
    """Return the item BaseModel subclass for a ``list[Model]`` annotation."""
    if typing.get_origin(annotation) is not list:
        return None
    for arg in typing.get_args(annotation):
        if isinstance(arg, type) and issubclass(arg, BaseModel):
            return arg
    return None


def _collect_unknown_overlay_keys(
    overlay: dict[str, Any],
    model_cls: type[BaseModel],
    prefix: str = "",
) -> list[str]:
    """Return dotted overlay paths that do not match ``model_cls`` schema.

    Descends into nested mappings AND list-of-model fields so that typos
    inside list items (e.g. ``repositories[0].made_up_field``) surface as
    warnings instead of being silently dropped at validation time.
    """
    unknown: list[str] = []
    if not isinstance(overlay, dict):
        return unknown
    for key, value in overlay.items():
        path = f"{prefix}.{key}" if prefix else key
        field = model_cls.model_fields.get(key)
        if field is None:
            unknown.append(path)
            continue
        if isinstance(value, dict):
            inner = _resolve_nested_model(field.annotation)
            if inner is not None:
                unknown.extend(
                    _collect_unknown_overlay_keys(value, inner, path)
                )
        elif isinstance(value, list):
            item_model = _list_item_model(field.annotation)
            if item_model is None:
                continue
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    unknown.extend(
                        _collect_unknown_overlay_keys(
                            item, item_model, f"{path}[{idx}]"
                        )
                    )
    return unknown


def _applied_overlay_paths(
    overlay: dict[str, Any],
    model_cls: type[BaseModel],
    prefix: str = "",
) -> list[str]:
    """Flatten ``overlay`` to dotted paths for the info log.

    Skips keys that ``model_cls`` does not know about — those keys were
    already warned as "unknown" and pydantic drops them at validation, so
    logging them as "Applied" would mislead operators verifying that an
    override actually took effect.
    """
    paths: list[str] = []
    if not isinstance(overlay, dict):
        return paths
    for key, value in overlay.items():
        path = f"{prefix}.{key}" if prefix else key
        field = model_cls.model_fields.get(key)
        if field is None:
            continue
        if isinstance(value, dict) and value:
            inner = _resolve_nested_model(field.annotation)
            if inner is not None:
                paths.extend(_applied_overlay_paths(value, inner, path))
            else:
                paths.append(path)
        else:
            paths.append(path)
    return paths


def _apply_daemon_env_overrides(raw: dict[str, Any]) -> None:
    """Apply supported daemon env-var overrides onto ``raw`` config."""
    daemon = raw.get("daemon")
    overrides = {
        field: os.environ.get(env_name)
        for env_name, field in _DAEMON_ENV_OVERRIDES.items()
        if os.environ.get(env_name) not in (None, "")
    }
    if not overrides:
        return
    if daemon is None:
        daemon = {}
        raw["daemon"] = daemon
    if not isinstance(daemon, dict):
        return
    daemon.update(overrides)


def normalize_repo_url(url: str) -> str:
    """Return a canonical form of ``url`` for equality comparisons.

    Strips trailing slashes and a ``.git`` suffix so that
    ``https://github.com/o/r``, ``https://github.com/o/r/`` and
    ``https://github.com/o/r.git`` all compare equal.
    """
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[: -len(".git")]
    return cleaned


def save_config(config: AppConfig, path: str = "config.yml") -> None:
    """Serialize ``config`` to YAML at ``path`` atomically.

    Writes to a temporary file in the target directory first and then
    renames it into place via ``os.replace`` so a crash mid-write cannot
    leave a half-written ``config.yml`` behind.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # ``exclude_none=True`` keeps optional fields (``RepoConfig.review_timeout_min``)
    # out of the on-disk YAML when they are unset. Otherwise they would be
    # serialized as ``null``, which is both ugly and ambiguous on re-read.
    payload = config.model_dump(mode="json", exclude_none=True)
    yaml_text = yaml.dump(payload, default_flow_style=False, sort_keys=False)

    fd, tmp_path = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(yaml_text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, target)
        invalidate_config_cache()
    except Exception:
        # Best-effort cleanup of the tmp file if the replace never happened.
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _find_repo_index(config: AppConfig, url: str) -> int:
    """Return the index of ``url`` in ``config.repositories`` or ``-1``."""
    needle = normalize_repo_url(url)
    for idx, repo in enumerate(config.repositories):
        if normalize_repo_url(repo.url) == needle:
            return idx
    return -1


def add_repository(
    url: str, path: str = "config.yml", **overrides: Any
) -> AppConfig:
    """Append a repository to ``config.yml`` and return the updated config.

    Raises ``ValueError`` if ``url`` (normalized) is already configured or
    if ``overrides`` contains an unknown field.
    """
    unknown = set(overrides) - (_REPO_FIELDS - {"url"})
    if unknown:
        raise ValueError(f"Unknown repository fields: {sorted(unknown)}")

    config = AppConfig.model_validate(_load_config_raw(path))
    if _find_repo_index(config, url) >= 0:
        raise ValueError(f"Repository already configured: {url}")

    repo = RepoConfig(url=url, **overrides)
    config.repositories.append(repo)
    save_config(config, path)
    return config


def remove_repository(url: str, path: str = "config.yml") -> AppConfig:
    """Remove a repository from ``config.yml`` and return the updated config.

    Raises ``ValueError`` if ``url`` (normalized) is not configured.
    """
    config = AppConfig.model_validate(_load_config_raw(path))
    idx = _find_repo_index(config, url)
    if idx < 0:
        raise ValueError(f"Repository not found: {url}")

    config.repositories.pop(idx)
    save_config(config, path)
    return config


def update_repository(
    url: str, path: str = "config.yml", **updates: Any
) -> AppConfig:
    """Update fields on an existing repository and return the updated config.

    Only known fields on :class:`RepoConfig` may be updated; ``url`` itself
    is immutable here. Raises ``ValueError`` if the repo does not exist or
    an unknown field is supplied.
    """
    unknown = set(updates) - (_REPO_FIELDS - {"url"})
    if unknown:
        raise ValueError(f"Unknown repository fields: {sorted(unknown)}")

    config = AppConfig.model_validate(_load_config_raw(path))
    idx = _find_repo_index(config, url)
    if idx < 0:
        raise ValueError(f"Repository not found: {url}")

    existing = config.repositories[idx]
    # model_copy(update=...) does NOT re-run validators in Pydantic v2, so
    # rebuild via model_validate to reject malformed patches before writing
    # anything to disk.
    merged = RepoConfig.model_validate({**existing.model_dump(), **updates})
    config.repositories[idx] = merged
    save_config(config, path)
    return config


def update_daemon_config(
    path: str = "config.yml", **updates: Any
) -> AppConfig:
    """Update fields on ``daemon:`` and return the updated config.

    Raises ``ValueError`` if ``updates`` contains an unknown field.
    """
    unknown = set(updates) - _DAEMON_FIELDS
    if unknown:
        raise ValueError(f"Unknown daemon fields: {sorted(unknown)}")

    config = AppConfig.model_validate(_load_config_raw(path))
    # Same reasoning as update_repository: go through model_validate so a
    # malformed patch raises instead of corrupting the on-disk config.
    config.daemon = DaemonConfig.model_validate(
        {**config.daemon.model_dump(), **updates}
    )
    save_config(config, path)
    return config
