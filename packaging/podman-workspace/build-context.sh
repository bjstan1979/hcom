#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: build-context.sh OUTPUT_DIR

Generates (but does not commit) an image context from the live host Pi/HCOM
installation. Override HCOM_BIN, PI_PACKAGE_DIR, and PI_CODING_AGENT_DIR as
needed. Then build with:
  podman build -t localhost/hcom-pi-workspace:live OUTPUT_DIR
EOF
}

[[ $# -eq 1 ]] || { usage >&2; exit 2; }
out=$1
hcom_bin=${HCOM_BIN:-$(command -v hcom || true)}
pi_bin=$(command -v pi || true)
pi_dir=${PI_CODING_AGENT_DIR:-${HOME:?}/.pi/agent}
[[ -x "$hcom_bin" ]] || { echo "hcom executable not found" >&2; exit 1; }
[[ -n "$pi_bin" ]] || { echo "pi executable not found" >&2; exit 1; }
pi_real=$(readlink -f "$pi_bin")
pi_package=${PI_PACKAGE_DIR:-}
if [[ -z "$pi_package" ]]; then
  cursor=$(dirname "$pi_real")
  while [[ "$cursor" != / && ! -f "$cursor/package.json" ]]; do cursor=$(dirname "$cursor"); done
  pi_package=$cursor
fi
[[ -f "$pi_package/package.json" ]] || { echo "Pi npm package root not found" >&2; exit 1; }

rm -rf "$out"
mkdir -p "$out/rootfs/usr/local/bin" "$out/rootfs/usr/local/lib" "$out/rootfs/home/pi/.pi/agent"
cp "$(dirname "$0")/Containerfile" "$out/Containerfile"
cp -L "$hcom_bin" "$out/rootfs/usr/local/bin/hcom"
cp -a "$pi_package" "$out/rootfs/usr/local/lib/pi-coding-agent"
cat > "$out/rootfs/usr/local/bin/pi" <<'EOF'
#!/bin/sh
exec node /usr/local/lib/pi-coding-agent/dist/bundle/cli.js "$@"
EOF
cat > "$out/rootfs/usr/local/bin/pi-container-entry" <<'EOF'
#!/bin/sh
set -eu
seed=/opt/pi-agent-seed
runtime=${PI_CODING_AGENT_DIR:?}
mkdir -p "$runtime"
for entry in extensions agents npm; do
  if [ ! -e "$runtime/$entry" ] && [ -e "$seed/$entry" ]; then
    mkdir -p "$runtime/$entry"
    cp -R --no-preserve=ownership "$seed/$entry/." "$runtime/$entry/"
  fi
done
for entry in settings.json APPEND_SYSTEM.md schedule-prompts-settings.json; do
  if [ ! -e "$runtime/$entry" ] && [ -f "$seed/$entry" ]; then
    cp --no-preserve=ownership "$seed/$entry" "$runtime/$entry"
  fi
done
exec /usr/local/bin/pi "$@"
EOF
chmod 0755 "$out/rootfs/usr/local/bin/pi" "$out/rootfs/usr/local/bin/pi-container-entry"
mkdir -p "$out/rootfs/opt/pi-agent-seed"
for entry in extensions agents npm settings.json APPEND_SYSTEM.md schedule-prompts-settings.json; do
  [[ -e "$pi_dir/$entry" ]] && cp -a "$pi_dir/$entry" "$out/rootfs/opt/pi-agent-seed/$entry"
done
find "$out/rootfs/opt/pi-agent-seed" -type d -name .git -prune -exec rm -rf {} +

# Materialize extension package links that resolve outside the copied seed and
# recursively copy only their production dependency closure. This preserves
# ordinary in-tree links such as node_modules/.bin while avoiding host-specific
# links such as extensions/tool/node_modules/pkg -> ~/node_modules/pkg.
python3 - "$pi_dir/extensions" "$out/rootfs/opt/pi-agent-seed/extensions" <<'PY'
import json, os, shutil, sys
from pathlib import Path

source_root = Path(sys.argv[1]).resolve()
dest_root = Path(sys.argv[2])
if not source_root.is_dir():
    raise SystemExit(0)


def inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def resolve_dependency(package: Path, name: str) -> Path:
    cursor = package.parent
    while True:
        candidate = cursor / "node_modules" / name
        if candidate.is_dir():
            return candidate.resolve()
        if cursor == cursor.parent:
            raise RuntimeError(f"cannot resolve production dependency {name!r} from {package}")
        cursor = cursor.parent


def copy_closure(
    source: Path, destination: Path, install_root: Path, seen: set[tuple[str, str]]
) -> None:
    key = (str(source.resolve()), str(destination))
    if key in seen:
        return
    seen.add(key)
    if destination.is_symlink() or destination.exists():
        if destination.is_dir() and not destination.is_symlink():
            shutil.rmtree(destination)
        else:
            destination.unlink()
    shutil.copytree(source, destination, symlinks=True)
    manifest = source / "package.json"
    if not manifest.is_file():
        return
    dependencies = json.loads(manifest.read_text()).get("dependencies", {})
    for name in dependencies:
        dependency_source = resolve_dependency(source, name)
        dependency_destination = install_root / name
        copy_closure(dependency_source, dependency_destination, install_root, seen)


seen: set[tuple[str, str]] = set()
for extension in source_root.iterdir():
    modules = extension / "node_modules"
    if not modules.is_dir():
        continue
    for link in modules.rglob("*"):
        if not link.is_symlink():
            continue
        target = link.resolve(strict=True)
        if inside(target, source_root):
            continue
        relative = link.relative_to(source_root)
        destination = dest_root / relative
        install_root = dest_root / extension.name / "node_modules"
        copy_closure(target, destination, install_root, seen)
PY

if find -L "$out/rootfs/opt/pi-agent-seed" -type l -print -quit | grep -q .; then
  echo "image seed contains a dangling symlink" >&2
  exit 1
fi
printf 'Generated %s\n' "$out"
