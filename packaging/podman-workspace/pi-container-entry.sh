#!/bin/sh
set -eu
payload=${PI_FRAMEWORK_PAYLOAD:?}
runtime=${PI_CODING_AGENT_DIR:?}
configurator=${PI_CONFIGURATOR:-$payload/scripts/configure-vetted-pi-packages.mjs}
pi_bin=${PI_CONTAINER_PI_BIN:-/usr/local/bin/pi}
manifest=$payload/MANIFEST.txt
PATH="$runtime/bin:$PATH"
XDG_CONFIG_HOME=${XDG_CONFIG_HOME:-$runtime/.config}
XDG_CACHE_HOME=${XDG_CACHE_HOME:-$runtime/.cache}
XDG_DATA_HOME=${XDG_DATA_HOME:-$runtime/.local/share}
XDG_STATE_HOME=${XDG_STATE_HOME:-$runtime/.local/state}
export PATH XDG_CONFIG_HOME XDG_CACHE_HOME XDG_DATA_HOME XDG_STATE_HOME

for path in "$manifest" "$payload/pi-runtime/package.json" "$payload/pi-extensions" \
  "$payload/pi-packages/node_modules" "$payload/pi-config/managed-package-closure.txt" "$configurator"; do
  [ -e "$path" ] && [ ! -L "$path" ] || { echo "invalid Pi payload path: $path" >&2; exit 1; }
done
mkdir -p "$runtime"
exec 9>"$runtime/.managed-package-refresh.lock"
flock 9
swap_stage=
swap_backup=
swap_target=
restore_swap() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ -n "$swap_target" ] && [ ! -e "$swap_target" ] && [ ! -L "$swap_target" ] \
      && { [ -e "$swap_backup" ] || [ -L "$swap_backup" ]; }; then
    mv "$swap_backup" "$swap_target" || true
  fi
  [ -z "$swap_stage" ] || rm -rf "$swap_stage"
  exit "$status"
}
trap restore_swap EXIT HUP INT TERM
recover_swap() {
  recover_target=$1
  recover_parent=$(dirname "$recover_target")
  recover_name=$(basename "$recover_target")
  recover_backup=$(find "$recover_parent" -maxdepth 1 -name "$recover_name.backup.*" -print -quit 2>/dev/null || true)
  if [ ! -e "$recover_target" ] && [ ! -L "$recover_target" ] && [ -n "$recover_backup" ]; then
    mv "$recover_backup" "$recover_target"
  fi
  if [ -e "$recover_target" ] || [ -L "$recover_target" ]; then
    find "$recover_parent" -maxdepth 1 -name "$recover_name.backup.*" -exec rm -rf {} + 2>/dev/null || true
  fi
}
publish_dir() {
  swap_stage=$1
  swap_target=$2
  swap_backup=$3
  recover_swap "$swap_target"
  rm -rf "$swap_backup"
  if [ -e "$swap_target" ] || [ -L "$swap_target" ]; then mv "$swap_target" "$swap_backup"; fi
  mv "$swap_stage" "$swap_target"
  swap_stage=
  swap_target=
  rm -rf "$swap_backup"
  swap_backup=
}
publish_file() {
  file_source=$1
  file_target=$2
  file_stage="${file_target}.stage.$$"
  rm -f "$file_stage"
  cp --no-preserve=ownership "$file_source" "$file_stage"
  mv -f "$file_stage" "$file_target"
}
mkdir -p "$XDG_CONFIG_HOME" "$XDG_CACHE_HOME" "$XDG_DATA_HOME" "$XDG_STATE_HOME"
if [ ! -e "$runtime/extensions" ] && [ ! -L "$runtime/extensions" ]; then
  stage="$runtime/extensions.stage.$$"
  rm -rf "$stage"
  mkdir -p "$stage"
  cp -R --no-preserve=ownership "$payload/pi-extensions/." "$stage/"
  publish_dir "$stage" "$runtime/extensions" "$runtime/extensions.backup.$$"
fi
if [ ! -e "$runtime/npm" ] && [ ! -L "$runtime/npm" ]; then
  stage="$runtime/npm.stage.$$"
  rm -rf "$stage"
  mkdir -p "$stage"
  cp -R --no-preserve=ownership "$payload/pi-packages/." "$stage/"
  publish_dir "$stage" "$runtime/npm" "$runtime/npm.backup.$$"
fi
for entry in settings.json APPEND_SYSTEM.md schedule-prompts-settings.json; do
  if [ ! -e "$runtime/$entry" ] && [ ! -L "$runtime/$entry" ] && [ -f "$payload/pi-config/$entry" ]; then
    publish_file "$payload/pi-config/$entry" "$runtime/$entry"
  fi
done

node "$configurator" "$runtime/settings.json" >&2
while IFS= read -r package; do
  case "$package" in
    ""|/*|*..*) echo "invalid managed package path: $package" >&2; exit 1 ;;
  esac
  source="$payload/pi-packages/node_modules/$package"
  target="$runtime/npm/node_modules/$package"
  stage="${target}.stage.$$"
  backup="${target}.backup.$$"
  [ -d "$source" ] || { echo "managed package missing from payload: $package" >&2; exit 1; }
  rm -rf "$stage" "$backup"
  mkdir -p "$stage"
  cp -R --no-preserve=ownership "$source/." "$stage/"
  publish_dir "$stage" "$target" "$backup"
done < "$payload/pi-config/managed-package-closure.txt"

manifest_value() {
  awk -F= -v key="$1" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$manifest"
}
refresh_managed_git() {
  package_label=$1
  package_bundle=$2
  package_relative=$3
  package_expected=$4
  package_remote=$5
  package_target="$runtime/git/$package_relative"
  package_stage="${package_target}.stage.$$"
  package_backup="${package_target}.backup.$$"
  [ -f "$package_bundle" ] && [ ! -L "$package_bundle" ] || { echo "managed $package_label bundle missing" >&2; exit 1; }
  mkdir -p "$(dirname "$package_target")"
  rm -rf "$package_stage" "$package_backup"
  git clone -q "$package_bundle" "$package_stage"
  [ "$(git -C "$package_stage" rev-parse HEAD)" = "$package_expected" ] \
    || { echo "managed $package_label payload commit mismatch" >&2; exit 1; }
  git -C "$package_stage" remote set-url origin "$package_remote"
  publish_dir "$package_stage" "$package_target" "$package_backup"
}
refresh_managed_git Ponytail \
  "$payload/managed-git/ponytail.bundle" \
  github.com/DietrichGebert/ponytail "$(manifest_value ponytail_commit)" \
  https://github.com/DietrichGebert/ponytail
refresh_managed_git pi-community-themes \
  "$payload/managed-git/pi-community-themes.bundle" \
  github.com/hasit/pi-community-themes "$(manifest_value pi_community_themes_commit)" \
  https://github.com/hasit/pi-community-themes

for skill in ponytail ponytail-audit ponytail-debt ponytail-gain ponytail-help ponytail-review; do
  user_skill="$runtime/skills/$skill"
  package_skill="$runtime/git/github.com/DietrichGebert/ponytail/skills/$skill/SKILL.md"
  rollback_skill="$runtime/.release-rollback/0.2.20/duplicate-ponytail-skills/$skill"
  if [ -d "$user_skill" ] && [ ! -L "$user_skill" ] \
    && [ -f "$user_skill/SKILL.md" ] && [ ! -L "$user_skill/SKILL.md" ] \
    && [ "$(find "$user_skill" -mindepth 1 -maxdepth 1 -printf x | wc -c)" -eq 1 ] \
    && cmp -s "$user_skill/SKILL.md" "$package_skill"; then
    mkdir -p "$(dirname "$rollback_skill")"
    if [ -e "$rollback_skill" ]; then rm -rf "$user_skill"; else mv "$user_skill" "$rollback_skill"; fi
  fi
done

if [ -f "$payload/pi-extensions/hcom.ts" ]; then
  mkdir -p "$runtime/extensions"
  publish_file "$payload/pi-extensions/hcom.ts" "$runtime/extensions/hcom.ts"
fi
if [ -f "$payload/pi-extensions/agentmemory/index.ts" ]; then
  mkdir -p "$runtime/extensions/agentmemory"
  publish_file "$payload/pi-extensions/agentmemory/index.ts" "$runtime/extensions/agentmemory/index.ts"
fi
for extension in anysearch.ts mmx.ts; do
  if [ -f "$payload/pi-extensions/$extension" ]; then
    mkdir -p "$runtime/extensions"
    publish_file "$payload/pi-extensions/$extension" "$runtime/extensions/$extension"
  fi
done
for skill in flowus-cli flowus-markdown-upload; do
  if [ -d "$payload/pi-skills/$skill" ]; then
    mkdir -p "$runtime/skills"
    stage="$runtime/skills/$skill.stage.$$"
    rm -rf "$stage"
    mkdir -p "$stage"
    cp -R --no-preserve=ownership "$payload/pi-skills/$skill/." "$stage/"
    publish_dir "$stage" "$runtime/skills/$skill" "$runtime/skills/$skill.backup.$$"
  fi
done
trap - EXIT HUP INT TERM
exec 9>&-
exec "$pi_bin" "$@"
