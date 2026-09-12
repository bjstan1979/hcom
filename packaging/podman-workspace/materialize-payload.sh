#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo 'Usage: materialize-payload.sh BUNDLE_ROOT PAYLOAD_ROOT' >&2
  exit 2
}

[[ $# -eq 2 ]] || usage
source_root=$(realpath -e -- "$1")
payload_root=$2
manifest=$source_root/MANIFEST.txt

for path in \
  "$manifest" \
  "$source_root/pi-runtime/package.json" \
  "$source_root/pi-extensions" \
  "$source_root/pi-packages/node_modules" \
  "$source_root/pi-config/settings.json" \
  "$source_root/pi-config/managed-package-closure.txt" \
  "$source_root/scripts/configure-vetted-pi-packages.mjs"; do
  [[ -e $path && ! -L $path ]] || { echo "payload source missing or symlinked: $path" >&2; exit 1; }
done

version=$(awk -F= '$1 == "bundle_version" { print $2; exit }' "$manifest")
[[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "invalid bundle_version: $version" >&2; exit 1; }
manifest_sha=$(sha256sum "$manifest" | cut -d' ' -f1)
payload_id=$version-${manifest_sha:0:20}
mkdir -p "$payload_root/versions"
chmod 700 "$payload_root" "$payload_root/versions"
exec 9>"$payload_root/install.lock"
flock 9

destination=$payload_root/versions/$payload_id
if [[ -e $destination ]]; then
  [[ -d $destination && ! -L $destination ]] || { echo "invalid existing payload: $destination" >&2; exit 1; }
  [[ $(cat "$destination/.manifest-sha256" 2>/dev/null) == "$manifest_sha" ]] \
    || { echo "existing payload marker mismatch: $destination" >&2; exit 1; }
  [[ $(cat "$destination/.payload-id" 2>/dev/null) == "$payload_id" ]] \
    || { echo "existing payload ID mismatch: $destination" >&2; exit 1; }
  [[ $(sha256sum "$destination/MANIFEST.txt" 2>/dev/null | cut -d' ' -f1) == "$manifest_sha" ]] \
    || { echo "existing payload Manifest mismatch: $destination" >&2; exit 1; }
  mode=$(stat -c %a "$destination")
  (( (8#$mode & 8#222) == 0 )) \
    || { echo "existing payload is writable: $destination" >&2; exit 1; }
else
  stage=$(mktemp -d "$payload_root/versions/.${payload_id}.XXXXXX")
  trap 'chmod -R u+w "$stage" 2>/dev/null || true; rm -rf "$stage"' EXIT
  for directory in pi-runtime pi-extensions pi-packages pi-config; do
    cp -a --reflink=auto "$source_root/$directory" "$stage/$directory"
  done
  mkdir -p "$stage/pi-skills" "$stage/scripts" "$stage/managed-git"
  for skill in flowus-cli flowus-markdown-upload; do
    cp -a --reflink=auto "$source_root/pi-skills/$skill" "$stage/pi-skills/$skill"
  done
  mapfile -t ponytail_bundles < <(find "$source_root/pi-packages/vendor" -maxdepth 1 -type f -name 'ponytail-*.bundle' -print)
  mapfile -t themes_bundles < <(find "$source_root/pi-packages/vendor" -maxdepth 1 -type f -name 'pi-community-themes-*.bundle' -print)
  [[ ${#ponytail_bundles[@]} -eq 1 && ${#themes_bundles[@]} -eq 1 ]] \
    || { echo 'payload requires exactly one Ponytail and one theme Git bundle' >&2; exit 1; }
  cp -a --reflink=auto "${ponytail_bundles[0]}" "$stage/managed-git/ponytail.bundle"
  cp -a --reflink=auto "${themes_bundles[0]}" "$stage/managed-git/pi-community-themes.bundle"
  cp -a "$source_root/scripts/configure-vetted-pi-packages.mjs" "$stage/scripts/"
  cp -a "$manifest" "$stage/MANIFEST.txt"
  find "$stage/pi-skills" -type d -name __pycache__ -prune -exec rm -rf {} +
  find "$stage/pi-skills" -type f -name '*.pyc' -delete
  while IFS= read -r -d '' link; do
    target=$(realpath -e -- "$link") || { echo "payload contains dangling symlink: $link" >&2; exit 1; }
    case "$target" in "$stage"|"$stage"/*) ;; *)
      echo "payload symlink escapes version root: $link -> $target" >&2
      exit 1 ;;
    esac
  done < <(find "$stage" -type l -print0)
  if find "$stage" \( -name .git -o -name auth.json -o -name credentials.json -o -name hcom.db \
      -o -name control.key -o -name '*.jsonl' \) -print -quit | grep -q .; then
    echo 'payload contains forbidden private state' >&2
    exit 1
  fi
  printf '%s\n' "$payload_id" > "$stage/.payload-id"
  printf '%s\n' "$manifest_sha" > "$stage/.manifest-sha256"
  chmod -R a+rX,a-w "$stage"
  mv "$stage" "$destination"
  trap - EXIT
fi

link=$payload_root/.current.$$
ln -s "versions/$payload_id" "$link"
mv -Tf "$link" "$payload_root/current"
printf 'PODMAN_PAYLOAD_OK id=%s path=%s\n' "$payload_id" "$(realpath -e -- "$payload_root/current")"
