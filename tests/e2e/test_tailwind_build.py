"""Real pinned compiler and runtime UID, only in the existing isolated stack."""

import subprocess


def test_web_compiles_current_css_and_rejects_partial_output():
    script = r"""
import os
from pathlib import Path
import tempfile
from scripts.build_tailwind import build, is_current

# This verifies the startup path, not a fallback-only dashboard response.
published = Path('/data/static')
assert os.getuid() == 1000  # Dockerfile USER runner; production Compose user 1000:1000.
assert is_current(), 'startup CSS is missing or stale'
assert (published / 'app.css').stat().st_uid == os.getuid()
assert '.text-ok' in (published / 'app.css').read_text()
assert r'.bg-ok\/15' in (published / 'app.css').read_text()
with tempfile.TemporaryDirectory(prefix='tailwind-check-', dir='/data') as name:
    output = Path(name)
    assert build(output=output)
    assert is_current(output=output)
    original = (output / 'app.css').read_bytes()
    fake = output / 'compiler'
    fake.write_text('#!/bin/sh\nwhile [ "$1" != "-o" ]; do shift; done\nprintf partial > "$2"\nexit 1\n')
    fake.chmod(0o755)
    assert not build(output=output, compiler=fake)
    assert (output / 'app.css').read_bytes() == original
    assert not is_current(output=output)
    assert not (output / 'app.css.manifest.json').exists()
    assert not list(output.glob('.tailwind-*'))
print('verified startup CSS, UID, source/compiler/CSS identity and partial-output failure')
"""
    result = subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", "exec", "-T", "web-test", "python", "-c", script],
        capture_output=True,
        text=True,
        timeout=150,
    )
    assert result.returncode == 0, result.stdout + result.stderr
