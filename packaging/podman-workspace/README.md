# Podman workspace base and Pi payload

The Podman workspace is split into two artifacts:

- **Stable base image:** Ubuntu 24.04, Node 22, tini, Git, bridge shim, thin `pi` launcher, and workspace bootstrap.
- **Versioned payload:** Pi runtime, extensions, managed npm packages, configuration, FlowUs skills, and pinned local Git bundles.

Package, extension, skill, and Pi runtime updates only create a new payload. They do not rebuild the base image.

## Build the stable base

```bash
context=$(mktemp -d)
podman-workspace/build-context.sh "$context"
base_sha=$(tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
  -C "$context" -cf - . | sha256sum | cut -d' ' -f1)
podman build \
  --build-arg HCOM_IMAGE_RELEASE=development \
  --build-arg HCOM_BASE_CONTEXT_SHA256="$base_sha" \
  -t localhost/hcom-pi-workspace:live "$context"
rm -rf "$context"
```

Rebuild only when `Containerfile`, a launcher/bootstrap, or the FlowUs shim changes. `install.sh` and release preparation reuse any image whose base-context SHA and payload-mode label match; Bundle release-label changes alone never rebuild it.

## Materialize a payload

From an installed or extracted Pi Framework Bundle:

```bash
bundle=$HOME/.local/share/pi-framework
payload_root=${HCOM_PODMAN_PAYLOAD_ROOT:-$HOME/.local/share/hcom-sandbox/payloads}
"$bundle/podman-workspace/materialize-payload.sh" "$bundle" "$payload_root"
readlink "$payload_root/current"
```

The materializer:

- fingerprints `MANIFEST.txt` as `<bundle-version>-<20-char-manifest-sha>`;
- writes `versions/<payload-id>` under an exclusive `flock`;
- rejects credentials, HCOM state, Git metadata, dangling links, and links escaping the payload;
- makes the completed tree read-only;
- atomically replaces only the `current` symlink;
- never changes or deletes an existing version directory.

## Container lifecycle

`hcom-podman-sandbox` exports the payload root. HCOM resolves and validates `current` while holding the workspace creation lock. A new `hcs-<workspace-id>` container binds that exact version directory read-only at `/opt/pi-payload` and records its host path in `io.hcom.payload-root`. New containers fail closed if `current` is missing or the selected image lacks `io.hcom.payload-mode=readonly-host-bind`.

Existing containers remain pinned to their recorded payload even after `current` changes. Existing legacy containers without the label continue using their image-baked runtime; they are not stopped or replaced automatically. To migrate a workspace, stop its agents and remove only its `hcs-*` container. The next launch preserves host-backed workspace state and pins the then-current payload.

Do not delete an old `payloads/versions/*` directory while either `current` or any container's `io.hcom.payload-root` label references it. Payload garbage collection is intentionally manual.

On each Pi start, `pi-container-entry` takes the workspace refresh `flock` before any mutation, copies first-use configuration, and refreshes managed package, Git, integration, and FlowUs skill trees through staged swaps with signal rollback and next-start crash recovery. Unrelated user packages and settings are preserved. The host canonical `~/.pi/agent/APPEND_SYSTEM.md` is still synchronized by HCOM with no-follow atomic writes.

## Security boundary

The existing baseline is unchanged:

- rootless Podman;
- read-only container rootfs and payload mount;
- `--cap-drop=ALL` and `no-new-privileges`;
- tini as PID 1;
- only the canonical workspace and workspace-private Pi/cache/HCOM-client state are writable;
- host HCOM database, control key, credentials, and session history are never mounted in the payload.

Optional settings: `HCOM_PODMAN_IMAGE`, `HCOM_PODMAN_PAYLOAD_ROOT`, `HCOM_PODMAN_STATE_ROOT`, `HCOM_PODMAN_PIDS_LIMIT`, `HCOM_PODMAN_MEMORY`, and `HCOM_PODMAN_CPUS`.

Happy and `pi-acp` remain host-side; only Pi RPC runs in the same rootless Podman workspace.
