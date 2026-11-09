import json
import os
import shutil
import subprocess

import pytest
from scripts.build_tailwind import ROOT, build, is_current


@pytest.fixture
def workspace(tmp_path):
    root = tmp_path / "app"
    for name in ("tailwind.config.js", "src/web/static/tailwind.src.css", "scripts/build_tailwind.py"):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / name, target)
    (root / "src/web/templates").mkdir()
    (root / "src/web/templates/base.html").write_text('<p class="text-ok">test</p>')
    compiler = tmp_path / "tailwindcss"
    compiler.write_text('#!/bin/sh\nwhile [ "$1" != "-o" ]; do shift; done\nprintf ".text-ok{}" > "$2"\n')
    compiler.chmod(0o755)
    return root, tmp_path / "static", compiler


def test_build_identity_and_source_invalidation(workspace):
    root, output, compiler = workspace
    assert not is_current(root, output, compiler)
    assert build(root, output, compiler)
    assert is_current(root, output, compiler)
    assert (output / "app.css").stat().st_uid == os.getuid()
    assert (output / "app.css").stat().st_mode & 0o777 == 0o644
    manifest = json.loads((output / "app.css.manifest.json").read_text())
    assert "src/web/templates/base.html" in manifest["sources"]
    nested = root / "src/web/templates/optional"
    nested.mkdir()
    template = nested / "later.html"
    template.write_text("<p>later</p>")
    assert not is_current(root, output, compiler)
    assert build(root, output, compiler)
    template.unlink()
    assert not is_current(root, output, compiler)
    assert build(root, output, compiler)
    js = root / "src/web/later.js"
    js.write_text('"text-ok"')
    assert not is_current(root, output, compiler)
    assert build(root, output, compiler)
    compiler.write_text(compiler.read_text() + "# changed compiler\n")
    assert not is_current(root, output, compiler)


@pytest.mark.parametrize("mode", ["partial", "empty", "missing", "changed"])
def test_failure_retains_complete_css_but_invalidates_success(workspace, mode, capsys):
    root, output, compiler = workspace
    assert build(root, output, compiler)
    original = (output / "app.css").read_bytes()
    if mode == "missing":
        compiler.unlink()
    else:
        action = {
            "partial": 'printf partial > "$2"; exit 1',
            "empty": ': > "$2"',
            "changed": f'printf partial > "$2"; echo changed >> "{root}/src/web/templates/base.html"',
        }[mode]
        compiler.write_text('#!/bin/sh\nwhile [ "$1" != "-o" ]; do shift; done\n' + action + "\n")
    assert not build(root, output, compiler)
    assert "ERROR" in capsys.readouterr().err
    assert (output / "app.css").read_bytes() == original
    assert not (output / "app.css.manifest.json").exists()
    assert not is_current(root, output, compiler)
    assert not list(output.glob(".tailwind-*"))


def test_corrupt_asset_and_unwritable_directory(workspace):
    root, output, compiler = workspace
    assert build(root, output, compiler)
    (output / "app.css").write_text("corrupt")
    assert not is_current(root, output, compiler)
    (output / "app.css.manifest.json").write_text("invalid json")
    assert not is_current(root, output, compiler)
    blocker = output / "file"
    blocker.touch()
    assert not build(root, blocker, compiler)


@pytest.mark.parametrize(
    "command,expected",
    [
        (["uvicorn", "src.web.app:app"], True),
        (["python", "-m", "uvicorn", "src.web.app:app"], True),
        (["python", "-m", "src.daemon.main"], False),
    ],
)
@pytest.mark.parametrize("fail_build", [False, True])
def test_entrypoint_only_builds_for_web(tmp_path, command, expected, fail_build):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # Stub unrelated auth/config setup and final command; log the builder call.
    for name in ("git", "gh", "python", "uvicorn"):
        script = bin_dir / name
        script.write_text(
            '#!/bin/sh\nprintf "%s\\n" "$*" >> "$CALLS"\n'
            'if [ "$1" = scripts/build_tailwind.py ] && [ "$FAIL_BUILD" = 1 ]; then exit 1; fi\n'
            "exit 0\n"
        )
        script.chmod(0o755)
    calls = tmp_path / "calls"
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/entrypoint.sh"), *command],
        env={
            **os.environ,
            "HOME": str(tmp_path),
            "PATH": f"{bin_dir}:/usr/bin:/bin",
            "CALLS": str(calls),
            "FAIL_BUILD": str(int(fail_build)),
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert ("scripts/build_tailwind.py" in calls.read_text()) is expected

    assert ("ERROR" in result.stderr) is (expected and fail_build)
