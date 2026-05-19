"""Round-trip YAML writes for single ``daemon:`` fields.

``src.config.save_config`` uses PyYAML, which strips comments on
re-serialization. For operator-facing edits on ``config.yml`` (the
Spending controls section is the first consumer), comments are part of
the operator's mental model — losing them on every UI write would
silently delete documentation the next time anyone re-reads the file.

This helper reads ``config.yml`` with ``ruamel.yaml`` in round-trip
mode, mutates a single key under ``daemon:`` (or deletes it for a
reset), and writes it back atomically. Comments, key ordering, and
unrelated fields are preserved verbatim by ruamel's CommentedMap.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap


def _load(path: Path) -> CommentedMap:
    yaml = YAML()
    yaml.preserve_quotes = True
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.load(fh)
    if data is None:
        return CommentedMap()
    if not isinstance(data, CommentedMap):
        raise ValueError(f"config.yml root must be a mapping, got {type(data)!r}")
    return data


def _dump_atomic(path: Path, data: CommentedMap) -> None:
    """Serialize ``data`` to ``path`` via tmp-file + ``os.replace``.

    Matches the atomicity guarantee of ``src.config.save_config``: a crash
    mid-write cannot leave a half-written ``config.yml`` behind, and the
    inotify watcher (PR-342) sees exactly one rename event instead of a
    sequence of incremental writes.
    """
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.default_flow_style = False
    yaml.indent(mapping=2, sequence=4, offset=2)

    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            yaml.dump(data, fh)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def write_daemon_field(path: str | Path, field: str, value: Any) -> None:
    """Set ``daemon.<field> = value`` in ``config.yml``, preserving comments."""
    target = Path(path)
    data = _load(target)
    daemon = data.get("daemon")
    if not isinstance(daemon, CommentedMap):
        daemon = CommentedMap()
        data["daemon"] = daemon
    daemon[field] = value
    _dump_atomic(target, data)


def delete_daemon_fields(path: str | Path, fields: list[str]) -> None:
    """Remove ``daemon.<field>`` keys, falling back to Pydantic defaults on load."""
    target = Path(path)
    data = _load(target)
    daemon = data.get("daemon")
    if not isinstance(daemon, CommentedMap):
        return
    for field in fields:
        if field in daemon:
            del daemon[field]
    _dump_atomic(target, data)
