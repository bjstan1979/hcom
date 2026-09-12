#!/usr/bin/env bash
set -euo pipefail

[[ $# -eq 1 ]] || { echo 'Usage: build-context.sh OUTPUT_DIR' >&2; exit 2; }
out=$1
source_dir=$(cd "$(dirname "$0")" && pwd)
rm -rf "$out"
mkdir -p "$out/rootfs/usr/local/bin"
cp "$source_dir/Containerfile" "$out/Containerfile"
cp "$source_dir/pi-container.sh" "$out/rootfs/usr/local/bin/pi"
cp "$source_dir/pi-container-entry.sh" "$out/rootfs/usr/local/bin/pi-container-entry"
cp "$source_dir/flowus-sandbox-shim.py" "$out/rootfs/usr/local/bin/flowus"
chmod 0755 "$out/rootfs/usr/local/bin/"*
printf 'PODMAN_BASE_CONTEXT_OK path=%s\n' "$out"
