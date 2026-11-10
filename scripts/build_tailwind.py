"""Transactional startup build. Consumers must call is_current, never trust mtime.

The manifest binds the complete source set, builder, compiler bytes and CSS.
Editing/adding/removing a mounted source invalidates it without a rebuild.
Failure removes the manifest but retains the last complete CSS for diagnosis.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(root, compiler):
    sources = {
        root / "tailwind.config.js",
        root / "src/web/static/tailwind.src.css",
        root / "scripts/build_tailwind.py",
    }
    sources.update((root / "src/web/templates").rglob("*.html"))
    sources.update((root / "src/web").rglob("*.js"))
    return {
        "version": 1,
        "compiler_sha256": digest(Path(compiler)),
        "sources": {str(p.relative_to(root)): digest(p) for p in sorted(sources)},
    }


def is_current(root=ROOT, output=Path("/data/static"), compiler=None):
    """Fail closed on stale, missing, corrupt or mismatched build evidence."""
    try:
        compiler = compiler or shutil.which("tailwindcss")
        manifest = json.loads((output / "app.css.manifest.json").read_text())
        return manifest == {**identity(root, compiler), "css_sha256": digest(output / "app.css")} and bool(
            (output / "app.css").stat().st_size
        )
    except (OSError, ValueError, TypeError):
        return False


def build(root=ROOT, output=Path("/data/static"), compiler=None):
    temporary = []
    manifest = output / "app.css.manifest.json"
    try:
        output.mkdir(parents=True, exist_ok=True)
        # Invalidate previous success before invoking a compiler that might fail.
        manifest.unlink(missing_ok=True)
        compiler = compiler or shutil.which("tailwindcss")
        before = identity(root, compiler)
        for suffix in (".css", ".json"):
            fd, name = tempfile.mkstemp(prefix=".tailwind-", suffix=suffix, dir=output)
            os.close(fd)
            temporary.append(Path(name))
        css, metadata = temporary
        subprocess.run(
            [
                str(compiler),
                "-c",
                "tailwind.config.js",
                "-i",
                "src/web/static/tailwind.src.css",
                "-o",
                str(css),
                "--minify",
            ],
            cwd=root,
            check=True,
            timeout=120,
        )
        if not css.stat().st_size or identity(root, compiler) != before:
            raise ValueError("empty output or sources changed during compilation")
        metadata.write_text(json.dumps({**before, "css_sha256": digest(css)}, sort_keys=True))
        css.chmod(0o644)
        metadata.chmod(0o644)
        # A crash between replacements leaves no success manifest.
        css.replace(output / "app.css")
        metadata.replace(manifest)
        print("Tailwind: published CSS and source/build manifest", flush=True)
        return True
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        print(f"ERROR: Tailwind build failed ({exc}); existing CDN fallback remains active", file=sys.stderr)
        return False
    finally:
        for path in temporary:
            path.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(0 if build() else 1)
