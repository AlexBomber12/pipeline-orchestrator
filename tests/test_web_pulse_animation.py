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


def test_pulse_dot_global_timer_runs_at_one_second_cadence() -> None:
    body = BASE_HTML.read_text(encoding="utf-8")
    pulse_interval = re.search(
        r"setInterval\(\s*\(\)\s*=>\s*\{(?P<body>[^{}]*)\},\s*1000\s*\)",
        body,
        re.DOTALL,
    )
    assert pulse_interval is not None, "Expected pulse-dot setInterval with 1000ms cadence"
    pulse_body = pulse_interval.group("body")
    assert "dim = !dim" in pulse_body
    assert "syncPulseDots()" in pulse_body


def test_pulse_dot_uses_shared_phase_not_per_dot_toggle() -> None:
    """All dots must derive their dim state from a single shared phase variable.

    Per-dot ``classList.toggle('dim')`` (no second argument) preserves
    per-element parity, so a dot inserted by HTMX mid-cycle stays out of phase
    forever. Forcing the boolean from a shared variable keeps every dot in sync.
    """

    body = BASE_HTML.read_text(encoding="utf-8")
    assert re.search(r"let\s+dim\s*=\s*false", body) is not None
    assert "dim = !dim" in body
    assert re.search(
        r"\.pulse-dot'\)\.forEach\(dot\s*=>\s*dot\.classList\.toggle\('dim',\s*dim\)\)",
        body,
    ) is not None


def test_pulse_dot_syncs_immediately_on_htmx_swap() -> None:
    """HTMX-swapped dots must be synced to the current dim phase immediately.

    Without an htmx:afterSwap handler, a dot inserted between interval ticks
    starts at the default opacity 1 even when the rest of the page is dimmed
    to 0.3, recreating the visible jump this change is meant to remove. The
    sync function must be the same one driven by the interval so both paths
    apply the identical phase.
    """

    body = BASE_HTML.read_text(encoding="utf-8")
    assert re.search(
        r"addEventListener\(\s*'htmx:afterSwap'\s*,\s*syncPulseDots\s*\)",
        body,
    ) is not None
