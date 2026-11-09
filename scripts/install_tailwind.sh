#!/bin/sh
# Standalone v3 preserves the v3 config and semantic RGB utility contract.
# Digests verified against the official release assets and sha256sums.txt:
# https://github.com/tailwindlabs/tailwindcss/releases/tag/v3.4.19
set -eu

version=v3.4.19
case "$(dpkg --print-architecture)" in
    amd64)
        artifact=tailwindcss-linux-x64
        checksum=4af3198c015616ea7d6617974ec3d70d987ecc00c1ca8463b0a30fd65cc7c06e
        ;;
    arm64)
        artifact=tailwindcss-linux-arm64
        checksum=e5b2d27694daa80cc52ec29553ba2c6bd43d86bd51a9d633ed24058b9c05a676
        ;;
    *)
        echo "Unsupported Tailwind architecture (requires amd64 or arm64)" >&2
        exit 1
        ;;
esac

# Optional destination directory permits isolated installer verification.
install_dir=${1:-/usr/local/bin}
temp_dir=$(mktemp -d)
trap 'rm -rf "$temp_dir"' EXIT
trap 'exit 1' HUP INT TERM
curl --fail --silent --show-error --location \
    --proto '=https' --proto-redir '=https' \
    --output "$temp_dir/tailwindcss" \
    "https://github.com/tailwindlabs/tailwindcss/releases/download/$version/$artifact"
printf '%s  %s\n' "$checksum" "$temp_dir/tailwindcss" | sha256sum --check --status
install -m 0755 "$temp_dir/tailwindcss" "$install_dir/tailwindcss"
