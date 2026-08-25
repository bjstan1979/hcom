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
hcom-sandbox --workspace /path/to/project 1 pi
```

`hcom-sandbox` defaults to `podman-workspace`; use `--mode workspace` for the
legacy bubblewrap sandbox or `--mode off` explicitly. Podman mode creates one
persistent container per canonical workspace, seeds workspace-private
`auth.json` and `models.json` once with mode 0600, and uses a user-systemd
broker service. Host HCOM DB/key are never mounted. Rootless namespace uid 0
maps to the launching host user; all capabilities are dropped, no-new-privileges
is set, and the root filesystem is read-only.

Optional runtime settings: `HCOM_PODMAN_IMAGE`, `HCOM_PODMAN_STATE_ROOT`,
`HCOM_PODMAN_PIDS_LIMIT`, `HCOM_PODMAN_MEMORY`, and `HCOM_PODMAN_CPUS`.

Workspace containers intentionally survive agent exits and image rebuilds. To
move a workspace to a rebuilt image, stop its agents and remove only the
`hcs-<workspace-id>` container; the next launch recreates it while preserving
the host backing state under `HCOM_PODMAN_STATE_ROOT`.
