# Podman workspace worker image

This directory contains source-only image assets. It does not alter the release bundle.

Generate a context from the installed live Pi/HCOM runtime:

```sh
packaging/podman-workspace/build-context.sh /tmp/hcom-pi-image
podman build -t localhost/hcom-pi-workspace:live /tmp/hcom-pi-image
```

The generator copies the live `pi` and `hcom` executables plus the Pi
`extensions`, `agents`, `npm`, and settings seed. Extension package links that
resolve outside the Pi directory are materialized with their production
dependency closure. Git metadata is removed. Override `HCOM_BIN`,
`PI_PACKAGE_DIR`, or `PI_CODING_AGENT_DIR` when the live installation is
elsewhere. Credentials, sessions, HCOM state, and mutable runtime state are not
baked into the image.

Install the launchers and run:

```sh
install -m 0755 scripts/hcom-podman-sandbox ~/.local/bin/
install -m 0755 scripts/hcom-sandbox ~/.local/bin/
install -m 0755 scripts/hcom-happy-sandbox scripts/hcom-happy-pi \
  scripts/hcom-happy-pi-rpc ~/.local/bin/
hcom-sandbox --workspace /path/to/project 1 pi
```

`hcom-sandbox` defaults to `podman-workspace`; use `--mode workspace` for the
legacy bubblewrap sandbox or `--mode off` explicitly. Podman mode creates one
persistent container per canonical workspace, seeds workspace-private
`auth.json` and `models.json` once with mode 0600, and uses a user-systemd
broker service. Host HCOM DB/key are never mounted. Rootless namespace uid 0
maps to the launching host user; all capabilities are dropped, no-new-privileges
is set, and the root filesystem is read-only.

## Happy mobile + HCOM

Keep Happy and `pi-acp` on the host while the Pi RPC process runs in the same
Podman workspace sandbox and binds to HCOM through the Pi extension:

```sh
hcom-happy-sandbox --workspace /path/to/project
hcom-happy-sandbox --workspace /path/to/project \
  --session 01a0387b-6ec0-7421-afd1-4fe665227c50
```

The second form opens the requested existing Pi session in Happy. It is distinct
from `happy resume <happy-session-id>`: `--session` selects the Pi transcript
and lets HCOM restore that transcript's canonical identity. Do not concurrently
open the same Pi session through both the interactive TUI and Happy. Happy's ACP path stays on the host
as the encrypted mobile transport and does not engage its Claude/Codex sandbox
manager; the Pi process remains inside the rootless, capability-free,
read-only-rootfs HCOM Podman sandbox. HCOM delivery is plugin-only in this mode,
so messages are sent
to the same Pi RPC session rather than injected into Happy's host terminal.

Optional runtime settings: `HCOM_PODMAN_IMAGE`, `HCOM_PODMAN_STATE_ROOT`,
`HCOM_PODMAN_PIDS_LIMIT`, `HCOM_PODMAN_MEMORY`, and `HCOM_PODMAN_CPUS`.

Workspace containers intentionally survive agent exits and image rebuilds. To
move a workspace to a rebuilt image, stop its agents and remove only the
`hcs-<workspace-id>` container; the next launch recreates it while preserving
the host backing state under `HCOM_PODMAN_STATE_ROOT`.
