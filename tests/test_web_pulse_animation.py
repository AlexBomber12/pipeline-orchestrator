"""Tests for the pulse-dot CSS technique in base.html."""

from __future__ import annotations

import re
from pathlib import Path

BASE_HTML = Path(__file__).resolve().parents[1] / "src" / "web" / "templates" / "base.html"


def test_pulse_dot_uses_transition_not_keyframe() -> None:
    body = BASE_HTML.read_text(encoding="utf-8")
    assert "@keyframes pulse-dot" not in body
    assert re.search(r"\.pulse-dot\s*\{[^}]*transition:\s*opacity[^}]*\}", body) is not None
    assert re.search(r"\.pulse-dot\.dim\s*\{[^}]*opacity:\s*0\.3", body) is not None


def test_pulse_dot_global_timer_toggles_dim_class() -> None:
    body = BASE_HTML.read_text(encoding="utf-8")
    assert "document.querySelectorAll('.pulse-dot')" in body
    assert "classList.toggle('dim')" in body
    assert re.search(r"setInterval\([^,]+,\s*1000\)", body) is not None
