"""Exercise the shell installer offline with real hashing and installation."""

import hashlib
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "scripts/install_tailwind.sh"
PINS = {
    "amd64": ("x64", "4af3198c015616ea7d6617974ec3d70d987ecc00c1ca8463b0a30fd65cc7c06e"),
    "arm64": ("arm64", "e5b2d27694daa80cc52ec29553ba2c6bd43d86bd51a9d633ed24058b9c05a676"),
}
PAYLOAD = b"#!/bin/sh\nprintf 'fixture tailwindcss v3.4.19\\n'\n"


@pytest.fixture
def installer(tmp_path, monkeypatch):
    commands = tmp_path / "commands"
    commands.mkdir()
    downloads = tmp_path / "downloads"
    downloads.mkdir()
    destination = tmp_path / "bin"
    destination.mkdir()
    payload = tmp_path / "payload"
    payload.write_bytes(PAYLOAD)
    for name, body in {
        "dpkg": 'test "$1" = --print-architecture || exit 2\necho "$FAKE_ARCH"\n',
        "curl": '''printf '%s\\n' "$@" > "$CURL_LOG"
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output) output=$2; shift ;;
    esac
    shift
done
cp "$PAYLOAD" "$output"
exit "$DOWNLOAD_EXIT"
''',
    }.items():
        command = commands / name
        command.write_text("#!/bin/sh\nset -eu\n" + body)
        command.chmod(0o755)
    monkeypatch.setenv("PATH", f"{commands}:{os.environ['PATH']}")
    monkeypatch.setenv("TMPDIR", str(downloads))
    monkeypatch.setenv("CURL_LOG", str(tmp_path / "curl.log"))
    monkeypatch.setenv("PAYLOAD", str(payload))
    monkeypatch.setenv("DOWNLOAD_EXIT", "0")

    def run(arch="amd64", fixture_checksum=False, download_exit=0):
        monkeypatch.setenv("FAKE_ARCH", arch)
        monkeypatch.setenv("DOWNLOAD_EXIT", str(download_exit))
        script = INSTALLER.read_text()
        if fixture_checksum:
            # Only substitute the artifact digest: real sha256sum checks the
            # controlled payload, while pin/URL assertions cover the originals.
            for _, digest in PINS.values():
                assert digest in script
                script = script.replace(digest, hashlib.sha256(PAYLOAD).hexdigest())
        local_script = tmp_path / "install.sh"
        local_script.write_text(script)
        result = subprocess.run(
            ["sh", str(local_script), str(destination)], capture_output=True, text=True,
        )
        assert list(downloads.iterdir()) == [], "temporary download must be removed"
        return result, destination / "tailwindcss", tmp_path / "curl.log"

    return run


@pytest.mark.parametrize("arch", PINS)
def test_supported_install_is_executable_and_uses_exact_release(installer, arch):
    result, binary, log = installer(arch, fixture_checksum=True)
    assert result.returncode == 0, result.stderr
    assert binary.read_bytes() == PAYLOAD
    assert binary.stat().st_mode & 0o777 == 0o755
    assert subprocess.check_output([str(binary)], text=True) == "fixture tailwindcss v3.4.19\n"
    args = log.read_text().splitlines()
    artifact, _ = PINS[arch]
    assert args[-1] == (
        f"https://github.com/tailwindlabs/tailwindcss/releases/download/v3.4.19/tailwindcss-linux-{artifact}"
    )
    assert args[:8] == [
        "--fail", "--silent", "--show-error", "--location", "--proto", "=https", "--proto-redir", "=https",
    ]


@pytest.mark.parametrize("arch", ["armhf", "riscv64", "unknown"])
def test_unsupported_architecture_does_not_download(installer, arch):
    result, binary, log = installer(arch)
    assert result.returncode != 0
    assert "Unsupported Tailwind architecture" in result.stderr
    assert not log.exists()
    assert not binary.exists()


@pytest.mark.parametrize("arch", PINS)
def test_real_pinned_digest_rejects_fixture_and_preserves_existing_binary(installer, arch, tmp_path):
    binary = tmp_path / "bin" / "tailwindcss"
    binary.write_text("previous installation")
    result, binary, _ = installer(arch)
    assert result.returncode != 0
    assert binary.read_text() == "previous installation"


@pytest.mark.parametrize("download_exit", [22, 60])
def test_failed_download_cannot_install_even_valid_partial_payload(installer, download_exit):
    result, binary, _ = installer(fixture_checksum=True, download_exit=download_exit)
    assert result.returncode == download_exit
    assert not binary.exists()


def test_image_installs_before_nonroot_runtime_outside_source_mount():
    dockerfile = (ROOT / "Dockerfile").read_text()
    copy = "COPY scripts/install_tailwind.sh /usr/local/libexec/install_tailwind.sh"
    run = "RUN sh /usr/local/libexec/install_tailwind.sh"
    assert dockerfile.index(copy) < dockerfile.index(run) < dockerfile.index("USER runner")
    assert 'install_dir=${1:-/usr/local/bin}' in INSTALLER.read_text()
