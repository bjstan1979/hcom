//! Explicit OS-level sandboxing for HCOM-launched workers.
//!
//! The HCOM PTY proxy remains on the host. Only the final AI tool process and
//! its descendants are wrapped in bubblewrap, so delivery and wakeup state do
//! not need broad writable access inside the sandbox.

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const MODE_ENV: &str = "HCOM_WORKER_SANDBOX";
const ROOT_ENV: &str = "HCOM_WORKER_SANDBOX_ROOT";
const WORKSPACE_MODE: &str = "workspace";
const PODMAN_WORKSPACE_MODE: &str = "podman-workspace";
const PODMAN_IMAGE_ENV: &str = "HCOM_PODMAN_IMAGE";
const PODMAN_STATE_ROOT_ENV: &str = "HCOM_PODMAN_STATE_ROOT";
const PODMAN_PIDS_LIMIT_ENV: &str = "HCOM_PODMAN_PIDS_LIMIT";
const PODMAN_MEMORY_ENV: &str = "HCOM_PODMAN_MEMORY";
const PODMAN_CPUS_ENV: &str = "HCOM_PODMAN_CPUS";
const AGENTMEMORY_SOCKET_ENV: &str = "AGENTMEMORY_SOCKET";
const ANYSEARCH_SOCKET_ENV: &str = "ANYSEARCH_SOCKET";
const MMX_SOCKET_ENV: &str = "MMX_SOCKET";
const FLOWUS_SOCKET_ENV: &str = "FLOWUS_SOCKET";
const DEFAULT_PODMAN_IMAGE: &str = "localhost/hcom-pi-workspace:live";

pub struct WorkerCommand {
    pub command: String,
    pub args: Vec<String>,
    // Keep the source fd alive until bubblewrap has consumed --ro-bind-data.
    // The fd is opened read-only from the host's trusted /usr/bin/flock.
    pub(crate) _flock_helper: Option<File>,
}

pub fn wrap_worker_command(
    command: String,
    args: Vec<String>,
    instance_name: Option<&str>,
) -> Result<WorkerCommand> {
    if std::env::var("HCOM_HAPPY_HOST_RUNNER").as_deref() == Ok("1") {
        let expected = std::env::var("HCOM_PI_BIN").unwrap_or_default();
        let command_path = Path::new(&command).canonicalize().ok();
        let expected_path = Path::new(&expected).canonicalize().ok();
        if !expected.is_empty() && command_path.is_some() && command_path == expected_path {
            return Ok(WorkerCommand {
                command,
                args,
                _flock_helper: None,
            });
        }
        // A stale/foreign Happy override must never become a host bypass. Fall
        // through to the ordinary sandbox path; this also keeps independent
        // launches safe when process-wide test/config environments overlap.
    }
    wrap_worker_command_with_stdio(command, args, instance_name, true)
}

fn wrap_worker_command_with_stdio(
    command: String,
    args: Vec<String>,
    instance_name: Option<&str>,
    terminal: bool,
) -> Result<WorkerCommand> {
    let mode = std::env::var(MODE_ENV).unwrap_or_default();
    if mode.is_empty() || mode == "off" {
        return Ok(WorkerCommand {
            command,
            args,
            _flock_helper: None,
        });
    }
    if mode != WORKSPACE_MODE && mode != PODMAN_WORKSPACE_MODE {
        bail!("Unsupported HCOM worker sandbox mode: {mode}");
    }
    if !cfg!(target_os = "linux") {
        bail!("HCOM worker sandbox requires Linux");
    }

    let workspace = match std::env::var(ROOT_ENV) {
        Ok(root) if !root.trim().is_empty() => PathBuf::from(root),
        _ => std::env::current_dir().context("Cannot resolve sandbox workspace")?,
    };
    let workspace = workspace
        .canonicalize()
        .with_context(|| format!("Cannot resolve sandbox workspace: {}", workspace.display()))?;
    if !workspace.is_dir() {
        bail!(
            "Sandbox workspace is not a directory: {}",
            workspace.display()
        );
    }

    let hcom_dir = crate::paths::hcom_dir()
        .canonicalize()
        .context("Cannot resolve HCOM state directory for sandbox")?;
    let home = dirs::home_dir().and_then(|path| path.canonicalize().ok());
    if workspace == Path::new("/") || home.as_deref() == Some(workspace.as_path()) {
        bail!(
            "Refusing unsafe sandbox workspace {}; choose a project directory",
            workspace.display()
        );
    }
    if hcom_dir.starts_with(&workspace) {
        bail!(
            "Refusing sandbox workspace {} because it contains HCOM state {}",
            workspace.display(),
            hcom_dir.display()
        );
    }

    if mode == PODMAN_WORKSPACE_MODE {
        return build_podman_workspace_command(command, args, &workspace, terminal);
    }

    let instance = instance_name
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow::anyhow!("Sandbox worker requires an HCOM instance name"))?;
    let bwrap = crate::terminal::which_bin("bwrap")
        .ok_or_else(|| anyhow::anyhow!("bubblewrap (bwrap) is required for sandbox workers"))?;

    build_workspace_command(bwrap, command, args, &workspace, &hcom_dir, instance)
}

fn podman_output(podman: &str, args: &[String]) -> Result<Output> {
    Command::new(podman)
        .args(args)
        .output()
        .with_context(|| format!("Failed to execute rootless Podman: {podman}"))
}

fn podman_success(podman: &str, args: &[String], action: &str) -> Result<Output> {
    let output = podman_output(podman, args)?;
    if !output.status.success() {
        bail!(
            "Podman {action} failed (status {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn workspace_id(workspace: &Path) -> String {
    let digest = Sha256::digest(workspace.as_os_str().as_encoded_bytes());
    digest[..10]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn bind_mount_arg(source: &Path, destination: &Path, read_only: bool) -> String {
    format!(
        "{}:{}:{}",
        source.to_string_lossy(),
        destination.to_string_lossy(),
        if read_only { "ro" } else { "rw" }
    )
}

fn inspect_value(podman: &str, container: &str, format: &str) -> Result<String> {
    let args = vec![
        "container".into(),
        "inspect".into(),
        "--format".into(),
        format.into(),
        container.into(),
    ];
    let output = podman_success(podman, &args, "container inspection")?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn lock_file(file: &File) -> Result<()> {
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    if result != 0 {
        bail!(
            "Failed to lock Podman workspace state: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}

fn build_podman_workspace_command(
    worker_command: String,
    worker_args: Vec<String>,
    workspace: &Path,
    terminal: bool,
) -> Result<WorkerCommand> {
    if unsafe { libc::geteuid() } == 0 {
        bail!("Refusing to run Podman workspace sandbox as root");
    }
    let podman = crate::terminal::which_bin("podman")
        .ok_or_else(|| anyhow::anyhow!("podman is required for podman-workspace workers"))?;
    let info_args = vec![
        "info".into(),
        "--format".into(),
        "{{.Host.Security.Rootless}}".into(),
    ];
    let info = podman_success(&podman, &info_args, "rootless verification")?;
    if String::from_utf8_lossy(&info.stdout).trim() != "true" {
        bail!("Podman is not running rootless; refusing podman-workspace sandbox");
    }

    let id = workspace_id(workspace);
    let container = format!("hcs-{id}");
    let state_root = match env_nonempty(PODMAN_STATE_ROOT_ENV) {
        Some(root) => PathBuf::from(root).join(&id),
        None => dirs::home_dir()
            .ok_or_else(|| anyhow::anyhow!("Cannot resolve HOME for Podman workspace state"))?
            .join(".local/share/hcom-sandbox/workspaces")
            .join(&id),
    };
    let pi_state = state_root.join("pi-agent");
    let cache_state = state_root.join("cache");
    let client_state = state_root.join("hcom-client");
    for path in [&state_root, &pi_state, &cache_state, &client_state] {
        ensure_private_dir(path)?;
    }
    seed_workspace_pi_credentials(&pi_state)?;
    let lock_path = state_root.join("create.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)
        .context("Failed to open Podman workspace lock")?;
    crate::sys::fs::set_private(&lock_path)?;
    lock_file(&lock)?;

    let exists_args = vec!["container".into(), "exists".into(), container.clone()];
    let exists = podman_output(&podman, &exists_args)?;
    match exists.status.code() {
        Some(0) => {
            let label_id = inspect_value(
                &podman,
                &container,
                "{{ index .Config.Labels \"io.hcom.workspace\" }}",
            )?;
            let label_path = inspect_value(
                &podman,
                &container,
                "{{ index .Config.Labels \"io.hcom.workspace-path\" }}",
            )?;
            if label_id != id || label_path != workspace.to_string_lossy() {
                bail!("Existing container {container} does not match workspace identity");
            }
        }
        Some(1) => {
            let image =
                env_nonempty(PODMAN_IMAGE_ENV).unwrap_or_else(|| DEFAULT_PODMAN_IMAGE.to_string());
            let mut create = vec![
                "create".into(),
                "--name".into(),
                container.clone(),
                "--label".into(),
                format!("io.hcom.workspace={id}"),
                "--label".into(),
                format!("io.hcom.workspace-path={}", workspace.to_string_lossy()),
                "--read-only".into(),
                "--cap-drop=ALL".into(),
                "--security-opt=no-new-privileges".into(),
                // Rootless container uid 0 maps to the launching host user, not
                // host root. Avoid keep-id: it forces Podman to chown-map the
                // entire large image for every workspace and can exceed HCOM's
                // launch deadline. Namespace root also preserves strict helper
                // ownership checks while capabilities remain fully dropped.
                "--user".into(),
                "0:0".into(),
                "--network=slirp4netns".into(),
                "--pids-limit".into(),
                env_nonempty(PODMAN_PIDS_LIMIT_ENV).unwrap_or_else(|| "512".into()),
                "--memory".into(),
                env_nonempty(PODMAN_MEMORY_ENV).unwrap_or_else(|| "4g".into()),
                "--cpus".into(),
                env_nonempty(PODMAN_CPUS_ENV).unwrap_or_else(|| "2".into()),
            ];
            for target in ["/tmp", "/run", "/var/tmp"] {
                create.extend(["--tmpfs".into(), format!("{target}:rw,nosuid,nodev")]);
            }
            for mount in [
                bind_mount_arg(workspace, workspace, false),
                bind_mount_arg(&pi_state, &pi_state, false),
                bind_mount_arg(&cache_state, &cache_state, false),
                bind_mount_arg(&client_state, &client_state, false),
            ] {
                create.extend(["--volume".into(), mount]);
            }
            for env_name in [
                AGENTMEMORY_SOCKET_ENV,
                ANYSEARCH_SOCKET_ENV,
                MMX_SOCKET_ENV,
                FLOWUS_SOCKET_ENV,
            ] {
                if let Some(socket) = env_nonempty(env_name).map(PathBuf::from) {
                    if !socket.is_absolute() || !socket.exists() {
                        bail!("{env_name} must be an existing absolute path");
                    }
                    let parent = socket.parent().context("bridge socket has no parent")?;
                    create.extend(["--volume".into(), bind_mount_arg(parent, parent, true)]);
                }
            }
            match (
                env_nonempty("HCOM_BROKER_SOCKET").map(PathBuf::from),
                env_nonempty("HCOM_BROKER_TOKEN_FILE").map(PathBuf::from),
            ) {
                (Some(socket), Some(token)) => {
                    if !socket.is_absolute() || !socket.exists() {
                        bail!("HCOM_BROKER_SOCKET must be an existing absolute path");
                    }
                    if !token.is_absolute() || !token.is_file() {
                        bail!("HCOM_BROKER_TOKEN_FILE must be an existing absolute file");
                    }
                    let socket_parent = socket.parent().context("broker socket has no parent")?;
                    if token.parent() != Some(socket_parent) {
                        bail!("broker socket and token must share a directory");
                    }
                    // Mount the directory, not the socket inode, so a supervised
                    // broker restart is visible inside an existing container.
                    // Read-only prevents the worker replacing either credential.
                    create.extend([
                        "--volume".into(),
                        bind_mount_arg(socket_parent, socket_parent, true),
                    ]);
                }
                (None, None) => {}
                _ => bail!("broker socket and token must be configured together"),
            }
            // Run a real init as PID 1. Long-lived workspace containers spawn
            // multiprocessing searches and detached helpers; plain `sleep`
            // neither forwards signals nor reaps orphaned children, eventually
            // filling the pids limit with zombies and making the Pi TUI sluggish.
            create.extend([
                image,
                "/usr/bin/tini".into(),
                "--".into(),
                "sleep".into(),
                "infinity".into(),
            ]);
            podman_success(&podman, &create, "container creation")?;
        }
        _ => bail!(
            "Podman container existence check failed (status {}): {}",
            exists.status,
            String::from_utf8_lossy(&exists.stderr).trim()
        ),
    }

    if inspect_value(&podman, &container, "{{.State.Running}}")? != "true" {
        podman_success(
            &podman,
            &["start".into(), container.clone()],
            "container start",
        )?;
    }
    drop(lock);

    let container_command = match Path::new(&worker_command)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some("pi") => "/usr/local/bin/pi-container-entry".to_string(),
        Some("hcom") => "/usr/local/bin/hcom".to_string(),
        _ => worker_command,
    };
    let mut args = vec!["exec".into(), "--interactive".into()];
    if terminal {
        args.push("--tty".into());
    }
    args.extend([
        "--workdir".into(),
        workspace.to_string_lossy().into_owned(),
        "--env".into(),
        "HOME=/home/pi".into(),
        "--env".into(),
        format!("PI_CODING_AGENT_DIR={}", pi_state.to_string_lossy()),
        "--env".into(),
        format!("HCOM_CLIENT_DIR={}", client_state.to_string_lossy()),
        "--env".into(),
        // Extensions such as zz-hcom-supervisor need durable, writable HCOM-
        // adjacent state. Keep it workspace-private rather than exposing the
        // host-global HCOM database/key or making the container HOME writable.
        format!("HCOM_DIR={}", client_state.to_string_lossy()),
        "--env".into(),
        format!("{MODE_ENV}={PODMAN_WORKSPACE_MODE}"),
        "--env".into(),
        format!("{ROOT_ENV}={}", workspace.to_string_lossy()),
    ]);
    for name in [
        "HCOM_PROCESS_ID",
        "HCOM_LAUNCHED",
        "HCOM_TAG",
        "HCOM_BROKER_SOCKET",
        "HCOM_BROKER_TOKEN_FILE",
        AGENTMEMORY_SOCKET_ENV,
        ANYSEARCH_SOCKET_ENV,
        MMX_SOCKET_ENV,
        FLOWUS_SOCKET_ENV,
    ] {
        args.extend(["--env".into(), name.into()]);
    }
    args.extend([container, container_command]);
    args.extend(worker_args);
    Ok(WorkerCommand {
        command: podman,
        args,
        _flock_helper: None,
    })
}

pub fn run_sandbox_pi_rpc(args: &[String]) -> Result<i32> {
    validate_pi_rpc_environment()?;
    validate_pi_rpc_args(args)?;
    let worker = wrap_worker_command_with_stdio("pi".into(), args.to_vec(), None, false)?;
    let status = Command::new(&worker.command)
        .args(&worker.args)
        .status()
        .with_context(|| format!("Failed to execute sandboxed Pi RPC: {}", worker.command))?;
    Ok(status.code().unwrap_or(1))
}

fn validate_pi_rpc_environment() -> Result<()> {
    if std::env::var("HCOM_LAUNCHED").as_deref() != Ok("1")
        || env_nonempty("HCOM_PROCESS_ID").is_none()
    {
        bail!("sandbox-pi-rpc requires a managed HCOM launch");
    }
    if std::env::var(MODE_ENV).as_deref() != Ok(PODMAN_WORKSPACE_MODE) {
        bail!("sandbox-pi-rpc requires podman-workspace mode");
    }
    if env_nonempty(ROOT_ENV).is_none() {
        bail!("sandbox-pi-rpc requires an explicit workspace root");
    }
    if env_nonempty("HCOM_BROKER_SOCKET").is_none()
        || env_nonempty("HCOM_BROKER_TOKEN_FILE").is_none()
    {
        bail!("sandbox-pi-rpc requires the authenticated workspace broker");
    }
    Ok(())
}

fn validate_pi_rpc_args(args: &[String]) -> Result<()> {
    let mut mode_rpc = false;
    let mut no_themes = false;
    let mut session = false;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--mode" if args.get(i + 1).map(String::as_str) == Some("rpc") && !mode_rpc => {
                mode_rpc = true;
                i += 2;
            }
            "--no-themes" if !no_themes => {
                no_themes = true;
                i += 1;
            }
            "--session" if !session && args.get(i + 1).is_some_and(|v| !v.is_empty()) => {
                session = true;
                i += 2;
            }
            arg => bail!("unsupported sandbox Pi RPC argument: {arg}"),
        }
    }
    if !mode_rpc {
        bail!("sandbox Pi RPC requires --mode rpc");
    }
    Ok(())
}

fn seed_workspace_pi_credentials(pi_state: &Path) -> Result<()> {
    let host_pi = std::env::var_os("PI_CODING_AGENT_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".pi/agent")))
        .ok_or_else(|| anyhow::anyhow!("Cannot resolve host Pi state directory"))?;
    for name in ["auth.json", "models.json"] {
        sync_newer_private_file(&host_pi.join(name), &pi_state.join(name))?;
    }

    let host_credentials = host_pi.join("credentials");
    if host_credentials.is_dir() {
        let sandbox_credentials = pi_state.join("credentials");
        ensure_private_dir(&sandbox_credentials)?;
        for entry in fs::read_dir(&host_credentials).with_context(|| {
            format!(
                "Failed to read host Pi credentials {}",
                host_credentials.display()
            )
        })? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let source = entry.path();
            // Provider credentials are flat regular Markdown files. Refuse
            // symlinks and nested trees so this sync cannot become an
            // arbitrary host-file copier.
            if !file_type.is_file() || source.extension().and_then(|ext| ext.to_str()) != Some("md")
            {
                continue;
            }
            sync_newer_private_file(&source, &sandbox_credentials.join(entry.file_name()))?;
        }
    }
    Ok(())
}

fn sync_newer_private_file(source: &Path, destination: &Path) -> Result<()> {
    if !source.is_file() {
        return Ok(());
    }
    // Workspace credentials are private copies, not mounts. Refresh only when
    // the host copy is newer, so host login/config updates reach an existing
    // persistent sandbox without overwriting newer sandbox-local changes.
    let host_updated = match (source.metadata()?.modified(), destination.metadata()) {
        (Ok(source_time), Ok(destination_meta)) => match destination_meta.modified() {
            Ok(destination_time) => source_time > destination_time,
            Err(_) => true,
        },
        _ => true,
    };
    if host_updated {
        copy_private_file(source, destination)?;
    }
    Ok(())
}

fn ensure_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("Failed to create sandbox directory {}", path.display()))?;
    crate::sys::fs::set_private_dir(path)
        .with_context(|| format!("Failed to secure sandbox directory {}", path.display()))?;
    Ok(())
}

fn ensure_private_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        ensure_private_dir(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to prepare sandbox file {}", path.display()))?;
    crate::sys::fs::set_private(path)
        .with_context(|| format!("Failed to secure sandbox file {}", path.display()))?;
    Ok(())
}

fn copy_private_file(source: &Path, destination: &Path) -> Result<()> {
    if !source.is_file() {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        ensure_private_dir(parent)?;
    }
    fs::copy(source, destination).with_context(|| {
        format!(
            "Failed to copy sandbox state {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    crate::sys::fs::set_private(destination)?;
    Ok(())
}

fn copy_private_settings(
    source: &Path,
    destination: &Path,
    host_pi: &Path,
    sandbox_pi: &Path,
) -> Result<()> {
    let contents = fs::read_to_string(source)
        .with_context(|| format!("Failed to read Pi settings {}", source.display()))?;
    let host_extensions = host_pi.join("extensions").to_string_lossy().into_owned();
    let sandbox_extensions = sandbox_pi.join("extensions").to_string_lossy().into_owned();
    let rewritten = contents.replace(&host_extensions, &sandbox_extensions);
    if let Some(parent) = destination.parent() {
        ensure_private_dir(parent)?;
    }
    fs::write(destination, rewritten).with_context(|| {
        format!(
            "Failed to write sandbox Pi settings {}",
            destination.display()
        )
    })?;
    crate::sys::fs::set_private(destination)?;
    Ok(())
}

fn copy_private_tree(source: &Path, destination: &Path) -> Result<()> {
    ensure_private_dir(destination)?;
    for entry in fs::read_dir(source)
        .with_context(|| format!("Failed to read sandbox tree {}", source.display()))?
    {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_private_tree(&source_path, &destination_path)?;
        } else if entry.file_type()?.is_file() {
            copy_private_file(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

fn push_bind(args: &mut Vec<String>, source: &Path, destination: &Path) {
    args.push("--bind".into());
    args.push(source.to_string_lossy().into_owned());
    args.push(destination.to_string_lossy().into_owned());
}

/// Encode a canonical working directory exactly as Pi does for its native
/// `PI_CODING_AGENT_DIR/sessions/<cwd>` layout.
fn pi_session_directory_name(cwd: &Path) -> String {
    let text = cwd.to_string_lossy();
    let without_root = text.trim_start_matches(['/', '\\']);
    let encoded: String = without_root
        .chars()
        .map(|character| match character {
            '/' | '\\' | ':' => '-',
            other => other,
        })
        .collect();
    format!("--{encoded}--")
}

fn build_workspace_command(
    bwrap: String,
    worker_command: String,
    worker_args: Vec<String>,
    workspace: &Path,
    hcom_dir: &Path,
    instance: &str,
) -> Result<WorkerCommand> {
    let sandbox_root = hcom_dir.join("sandboxes").join(instance);
    let sandbox_hcom = sandbox_root.join("hcom-state");
    let sandbox_pi = sandbox_root.join("pi-agent");
    for dir in [&sandbox_root, &sandbox_hcom, &sandbox_pi] {
        ensure_private_dir(dir)?;
    }
    ensure_private_dir(&sandbox_hcom.join(".tmp/logs"))?;
    ensure_private_dir(&sandbox_hcom.join("pi-delivery"))?;

    // HCOM gets a writable private root, while its shared SQLite files are
    // mounted individually below. This lets hooks create logs and enforce
    // directory permissions without exposing the host HCOM tree to rm -rf.
    for name in ["config.toml", "env", "shell_env.json"] {
        copy_private_file(&hcom_dir.join(name), &sandbox_hcom.join(name))?;
    }

    let pi_agent = std::env::var_os("PI_CODING_AGENT_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".pi/agent")))
        .ok_or_else(|| anyhow::anyhow!("Cannot resolve Pi agent directory for sandbox"))?
        .canonicalize()
        .context("Cannot resolve Pi agent directory for sandbox")?;
    for entry in fs::read_dir(&pi_agent)
        .with_context(|| format!("Cannot read Pi agent directory {}", pi_agent.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_file() && !entry.file_name().to_string_lossy().ends_with(".lock") {
            let destination = sandbox_pi.join(entry.file_name());
            if entry.file_name() == "settings.json" {
                copy_private_settings(&entry.path(), &destination, &pi_agent, &sandbox_pi)?;
            } else {
                copy_private_file(&entry.path(), &destination)?;
            }
        }
    }
    // Pi's package manager installs enabled packages into this directory.
    // Seed a private copy so npm never needs write access to the host tree.
    let host_npm = pi_agent.join("npm");
    if host_npm.is_dir() {
        copy_private_tree(&host_npm, &sandbox_pi.join("npm"))?;
    }
    // Pi discovers user extensions and custom agent definitions relative to
    // PI_CODING_AGENT_DIR. Expose both host trees at their private paths
    // read-only; do not copy them into the writable runtime, which would make
    // policy-bearing agent definitions mutable from inside the worker.
    let host_extensions = pi_agent.join("extensions");
    let host_agents = pi_agent.join("agents");
    for (source, name) in [(&host_extensions, "extensions"), (&host_agents, "agents")] {
        if source.is_dir() {
            ensure_private_dir(&sandbox_pi.join(name))?;
        }
    }

    // Preserve Pi's normal session discovery semantics. The full native
    // session catalog is visible read-only, while only this workspace's native
    // cwd-scoped directory is writable. HCOM worker names therefore do not
    // change what `/resume` can see or where Pi stores the selected session.
    let host_sessions = pi_agent.join("sessions");
    ensure_private_dir(&host_sessions)?;
    let host_workspace_sessions = host_sessions.join(pi_session_directory_name(workspace));
    ensure_private_dir(&host_workspace_sessions)?;
    ensure_private_dir(&sandbox_pi.join("sessions"))?;

    let db = hcom_dir.join("hcom.db");
    let db_wal = hcom_dir.join("hcom.db-wal");
    let db_shm = hcom_dir.join("hcom.db-shm");
    for file in [&db, &db_wal, &db_shm] {
        ensure_private_file(file)?;
    }
    for file in ["hcom.db", "hcom.db-wal", "hcom.db-shm"] {
        ensure_private_file(&sandbox_hcom.join(file))?;
    }

    // bubblewrap's user namespace maps namespace root to the launching user;
    // it is not host root. We use namespace root so --ro-bind-data can create
    // a root-owned, non-writable helper inode in the private /run tmpfs for
    // plugins that perform fail-closed helper ownership checks.
    let flock_helper = File::open("/usr/bin/flock")
        .context("Cannot open trusted /usr/bin/flock for sandbox injection")?;
    // The descriptor is passed as a numeric bwrap argument, so it must survive
    // exec. std::fs::File otherwise carries close-on-exec on Unix.
    let flock_fd_raw = flock_helper.as_raw_fd();
    let flags = unsafe { libc::fcntl(flock_fd_raw, libc::F_GETFD) };
    if flags < 0
        || unsafe { libc::fcntl(flock_fd_raw, libc::F_SETFD, flags & !libc::FD_CLOEXEC) } < 0
    {
        bail!("Cannot prepare /usr/bin/flock descriptor for sandbox injection");
    }
    let flock_fd = flock_fd_raw.to_string();

    let mut args = vec![
        "--die-with-parent".into(),
        "--new-session".into(),
        "--unshare-pid".into(),
        "--unshare-user".into(),
        "--uid".into(),
        "0".into(),
        "--gid".into(),
        "0".into(),
        "--ro-bind".into(),
        "/".into(),
        "/".into(),
        "--tmpfs".into(),
        "/tmp".into(),
        "--tmpfs".into(),
        "/run".into(),
        "--proc".into(),
        "/proc".into(),
        "--dev-bind".into(),
        "/dev".into(),
        "/dev".into(),
        "--perms".into(),
        "0755".into(),
        "--file".into(),
        flock_fd,
        "/run/flock".into(),
        "--remount-ro".into(),
        "/run".into(),
        "--setenv".into(),
        "PI_POSITION_FLOCK_HELPER".into(),
        "/run/flock".into(),
    ];

    // The whole host starts read-only. Re-open only the project and this
    // worker's private runtime state as writable mounts.
    push_bind(&mut args, workspace, workspace);
    push_bind(&mut args, &sandbox_root, &sandbox_root);
    args.push("--ro-bind".into());
    args.push(host_sessions.to_string_lossy().into_owned());
    args.push(sandbox_pi.join("sessions").to_string_lossy().into_owned());
    push_bind(
        &mut args,
        &host_workspace_sessions,
        &sandbox_pi
            .join("sessions")
            .join(pi_session_directory_name(workspace)),
    );
    for (source, name) in [(&host_extensions, "extensions"), (&host_agents, "agents")] {
        if source.is_dir() {
            args.push("--ro-bind".into());
            args.push(source.to_string_lossy().into_owned());
            args.push(sandbox_pi.join(name).to_string_lossy().into_owned());
        }
    }
    for (source, destination) in [
        (&db, sandbox_hcom.join("hcom.db")),
        (&db_wal, sandbox_hcom.join("hcom.db-wal")),
        (&db_shm, sandbox_hcom.join("hcom.db-shm")),
    ] {
        push_bind(&mut args, source, &destination);
    }

    args.extend([
        "--chdir".into(),
        workspace.to_string_lossy().into_owned(),
        "--setenv".into(),
        MODE_ENV.into(),
        WORKSPACE_MODE.into(),
        "--setenv".into(),
        ROOT_ENV.into(),
        workspace.to_string_lossy().into_owned(),
        "--setenv".into(),
        "PI_WORKER_SANDBOX".into(),
        WORKSPACE_MODE.into(),
        "--setenv".into(),
        "PI_WORKER_SANDBOX_ROOT".into(),
        workspace.to_string_lossy().into_owned(),
        "--setenv".into(),
        "HCOM_DIR".into(),
        sandbox_hcom.to_string_lossy().into_owned(),
        "--setenv".into(),
        "PI_CODING_AGENT_DIR".into(),
        sandbox_pi.to_string_lossy().into_owned(),
        "--".into(),
        worker_command,
    ]);
    args.extend(worker_args);

    Ok(WorkerCommand {
        command: bwrap,
        args,
        _flock_helper: Some(flock_helper),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::EnvGuard;
    use std::os::unix::fs::PermissionsExt;

    struct VarGuard(Vec<(String, Option<std::ffi::OsString>)>);

    impl VarGuard {
        fn set(values: &[(&str, Option<&std::ffi::OsStr>)]) -> Self {
            let saved = values
                .iter()
                .map(|(name, _)| ((*name).to_string(), std::env::var_os(name)))
                .collect();
            for (name, value) in values {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
            Self(saved)
        }
    }

    impl Drop for VarGuard {
        fn drop(&mut self) {
            for (name, value) in &self.0 {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    fn fake_podman(temp: &Path) -> PathBuf {
        let bin = temp.join("bin");
        fs::create_dir_all(&bin).unwrap();
        let script = bin.join("podman");
        fs::write(
            &script,
            r#"#!/bin/sh
set -eu
printf '%s\t' "$@" >> "$FAKE_PODMAN_LOG"
printf '\n' >> "$FAKE_PODMAN_LOG"
if [ "${FAKE_PODMAN_ROOTLESS:-true}" != true ] && [ "$1" = info ]; then
  printf 'false\n'
  exit 0
fi
if [ "$1" = info ]; then printf 'true\n'; exit 0; fi
if [ "$1" = container ] && [ "$2" = exists ]; then
  test -d "$FAKE_PODMAN_STATE/$3"
  exit
fi
if [ "$1" = container ] && [ "$2" = inspect ]; then
  format=$4
  name=$5
  case "$format" in
    *workspace-path*) cat "$FAKE_PODMAN_STATE/$name/path" ;;
    *workspace*) cat "$FAKE_PODMAN_STATE/$name/id" ;;
    *Running*) test -f "$FAKE_PODMAN_STATE/$name/running" && printf 'true\n' || printf 'false\n' ;;
    *) exit 44 ;;
  esac
  exit 0
fi
if [ "$1" = create ]; then
  shift
  name=
  id=
  path=
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --name) name=$2; shift 2 ;;
      --label)
        case "$2" in
          io.hcom.workspace=*) id=${2#*=} ;;
          io.hcom.workspace-path=*) path=${2#*=} ;;
        esac
        shift 2 ;;
      *) shift ;;
    esac
  done
  mkdir -p "$FAKE_PODMAN_STATE/$name"
  printf '%s\n' "$id" > "$FAKE_PODMAN_STATE/$name/id"
  printf '%s\n' "$path" > "$FAKE_PODMAN_STATE/$name/path"
  exit 0
fi
if [ "$1" = start ]; then touch "$FAKE_PODMAN_STATE/$2/running"; exit 0; fi
exit 45
"#,
        )
        .unwrap();
        let mut permissions = fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).unwrap();
        bin
    }

    #[test]
    fn podman_workspace_is_scoped_persistent_and_minimally_mounted() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let home = temp.path().join("home");
        let hcom = home.join(".hcom");
        let pi = home.join("host-pi-agent");
        let workspace_a = temp.path().join("workspace-a");
        let workspace_b = temp.path().join("workspace-b");
        let state = temp.path().join("podman-state");
        let log = temp.path().join("podman.log");
        let broker_socket = temp.path().join("broker.sock");
        let broker_token = temp.path().join("broker.token");
        let mmx_bridge = temp.path().join("mmx-bridge");
        let mmx_socket = mmx_bridge.join("mmx.sock");
        let flowus_bridge = temp.path().join("flowus-bridge");
        let flowus_socket = flowus_bridge.join("flowus.sock");
        for path in [
            &home,
            &hcom,
            &pi,
            &workspace_a,
            &workspace_b,
            &state,
            &mmx_bridge,
            &flowus_bridge,
        ] {
            fs::create_dir_all(path).unwrap();
        }
        fs::write(&broker_socket, "socket placeholder").unwrap();
        fs::write(&broker_token, "token").unwrap();
        fs::write(&mmx_socket, "socket placeholder").unwrap();
        fs::write(&flowus_socket, "socket placeholder").unwrap();
        fs::write(pi.join("auth.json"), "host-auth").unwrap();
        fs::write(pi.join("models.json"), "host-models").unwrap();
        fs::create_dir_all(pi.join("credentials")).unwrap();
        fs::write(pi.join("credentials/provider.md"), "host-provider-key").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            pi.join("auth.json"),
            pi.join("credentials/refused-symlink.md"),
        )
        .unwrap();
        let bin = fake_podman(temp.path());
        let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
        let vars = [
            ("HOME", Some(home.as_os_str())),
            ("HCOM_DIR", Some(hcom.as_os_str())),
            ("PI_CODING_AGENT_DIR", Some(pi.as_os_str())),
            (MODE_ENV, Some(std::ffi::OsStr::new(PODMAN_WORKSPACE_MODE))),
            (ROOT_ENV, Some(workspace_a.as_os_str())),
            ("PATH", Some(std::ffi::OsStr::new(&path))),
            ("FAKE_PODMAN_LOG", Some(log.as_os_str())),
            ("FAKE_PODMAN_STATE", Some(state.as_os_str())),
            ("FAKE_PODMAN_ROOTLESS", Some(std::ffi::OsStr::new("true"))),
            ("HCOM_BROKER_SOCKET", Some(broker_socket.as_os_str())),
            ("HCOM_BROKER_TOKEN_FILE", Some(broker_token.as_os_str())),
            (MMX_SOCKET_ENV, Some(mmx_socket.as_os_str())),
            (FLOWUS_SOCKET_ENV, Some(flowus_socket.as_os_str())),
        ];
        let _vars = VarGuard::set(&vars);

        let first =
            wrap_worker_command("pi".into(), vec!["--model".into(), "x".into()], Some("one"))
                .unwrap();
        let second = wrap_worker_command("pi".into(), vec![], Some("two")).unwrap();
        unsafe { std::env::set_var(ROOT_ENV, &workspace_b) };
        let third = wrap_worker_command("pi".into(), vec![], Some("one")).unwrap();

        let id_a = workspace_id(&workspace_a.canonicalize().unwrap());
        let id_b = workspace_id(&workspace_b.canonicalize().unwrap());
        assert_eq!(first.command, bin.join("podman").to_string_lossy());
        assert!(first.args.iter().any(|arg| arg == &format!("hcs-{id_a}")));
        assert!(second.args.iter().any(|arg| arg == &format!("hcs-{id_a}")));
        assert!(third.args.iter().any(|arg| arg == &format!("hcs-{id_b}")));
        assert_ne!(id_a, id_b);
        assert!(first._flock_helper.is_none());
        assert!(first.args.iter().any(|arg| arg == "--interactive"));
        assert!(first.args.iter().any(|arg| arg == "--tty"));
        assert!(
            first
                .args
                .iter()
                .any(|arg| arg == "/usr/local/bin/pi-container-entry")
        );
        let rpc = build_podman_workspace_command(
            "pi".into(),
            vec!["--mode".into(), "rpc".into(), "--no-themes".into()],
            &workspace_a.canonicalize().unwrap(),
            false,
        )
        .unwrap();
        assert!(rpc.args.iter().any(|arg| arg == "--interactive"));
        assert!(!rpc.args.iter().any(|arg| arg == "--tty"));
        assert!(rpc.args.iter().any(|arg| arg == "--mode"));
        assert!(rpc.args.iter().any(|arg| arg == "rpc"));
        assert!(
            first
                .args
                .windows(2)
                .any(|w| w == ["--workdir", workspace_a.to_string_lossy().as_ref()])
        );
        let root = home
            .join(".local/share/hcom-sandbox/workspaces")
            .join(&id_a);
        for env in [
            "HOME=/home/pi",
            &format!("PI_CODING_AGENT_DIR={}", root.join("pi-agent").display()),
            &format!("HCOM_CLIENT_DIR={}", root.join("hcom-client").display()),
            &format!("HCOM_DIR={}", root.join("hcom-client").display()),
            "HCOM_PROCESS_ID",
            "HCOM_LAUNCHED",
            "HCOM_TAG",
            "HCOM_BROKER_SOCKET",
            "HCOM_BROKER_TOKEN_FILE",
            MMX_SOCKET_ENV,
            FLOWUS_SOCKET_ENV,
        ] {
            assert!(first.args.windows(2).any(|w| w == ["--env", env]));
        }
        assert!(!first.args.iter().any(|arg| arg == "HCOM_DIR"));
        assert!(
            !first
                .args
                .iter()
                .any(|arg| arg == "PI_CODING_AGENT_SESSION_DIR")
        );

        let contents = fs::read_to_string(&log).unwrap();
        let create_lines: Vec<_> = contents
            .lines()
            .filter(|line| line.starts_with("create\t"))
            .collect();
        assert_eq!(create_lines.len(), 2, "{contents}");
        let create_a = create_lines
            .iter()
            .find(|line| line.contains(&format!("hcs-{id_a}")))
            .unwrap();
        assert!(create_a.contains("--user\t0:0"));
        assert!(!create_a.contains("--userns=keep-id"));
        assert!(create_a.contains("--cap-drop=ALL"));
        assert!(create_a.contains("--security-opt=no-new-privileges"));
        assert!(create_a.contains("/usr/bin/tini\t--\tsleep\tinfinity"));
        assert!(create_a.contains(&format!(
            "{}:{}:rw",
            workspace_a.display(),
            workspace_a.display()
        )));
        for mount in [
            format!("{0}:{0}:rw", root.join("pi-agent").display()),
            format!("{0}:{0}:rw", root.join("cache").display()),
            format!("{0}:{0}:rw", root.join("hcom-client").display()),
        ] {
            assert!(create_a.contains(&mount), "missing {mount} in {create_a}");
        }
        let broker_root = broker_socket.parent().unwrap();
        assert!(create_a.contains(&format!(
            "{}:{}:ro",
            broker_root.display(),
            broker_root.display()
        )));
        assert!(!create_a.contains(&format!(
            "{}:{}:rw",
            broker_socket.display(),
            broker_socket.display()
        )));
        assert!(create_a.contains(&format!(
            "{}:{}:ro",
            mmx_bridge.display(),
            mmx_bridge.display()
        )));
        assert!(!create_a.contains(&format!(
            "{}:{}:rw",
            mmx_bridge.display(),
            mmx_bridge.display()
        )));
        assert!(create_a.contains(&format!(
            "{}:{}:ro",
            flowus_bridge.display(),
            flowus_bridge.display()
        )));
        assert!(!create_a.contains(&format!(
            "{}:{}:rw",
            flowus_bridge.display(),
            flowus_bridge.display()
        )));
        assert!(!create_a.contains("hcom.db"));
        assert!(!create_a.contains("control.key"));
        assert!(!create_a.contains(pi.to_string_lossy().as_ref()));
        assert_eq!(
            contents
                .lines()
                .filter(|line| line.starts_with("start\t"))
                .count(),
            2
        );
        assert_eq!(
            fs::read(root.join("pi-agent/auth.json")).unwrap(),
            b"host-auth"
        );
        assert_eq!(
            fs::read(root.join("pi-agent/models.json")).unwrap(),
            b"host-models"
        );
        assert_eq!(
            fs::read(root.join("pi-agent/credentials/provider.md")).unwrap(),
            b"host-provider-key"
        );
        assert!(
            !root
                .join("pi-agent/credentials/refused-symlink.md")
                .exists()
        );
        assert_eq!(
            fs::metadata(root.join("pi-agent/credentials/provider.md"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        assert_eq!(
            fs::metadata(root.join("pi-agent/auth.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        fs::write(root.join("pi-agent/auth.json"), "workspace-auth").unwrap();
        fs::write(
            root.join("pi-agent/credentials/provider.md"),
            "workspace-provider-key",
        )
        .unwrap();
        unsafe { std::env::set_var(ROOT_ENV, &workspace_a) };
        wrap_worker_command("pi".into(), vec![], Some("three")).unwrap();
        assert_eq!(
            fs::read(root.join("pi-agent/auth.json")).unwrap(),
            b"workspace-auth"
        );
        assert_eq!(
            fs::read(root.join("pi-agent/credentials/provider.md")).unwrap(),
            b"workspace-provider-key"
        );
        // A later host /login is propagated on the next worker start without
        // mounting the host credential file into the container.
        std::thread::sleep(std::time::Duration::from_millis(2));
        fs::write(pi.join("auth.json"), "new-host-auth").unwrap();
        fs::write(pi.join("credentials/provider.md"), "new-host-provider-key").unwrap();
        wrap_worker_command("pi".into(), vec![], Some("four")).unwrap();
        assert_eq!(
            fs::read(root.join("pi-agent/auth.json")).unwrap(),
            b"new-host-auth"
        );
        assert_eq!(
            fs::read(root.join("pi-agent/credentials/provider.md")).unwrap(),
            b"new-host-provider-key"
        );
        for path in [
            root.clone(),
            root.join("pi-agent"),
            root.join("cache"),
            root.join("hcom-client"),
        ] {
            assert_eq!(
                fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn podman_workspace_fails_closed_when_not_rootless() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let bin = fake_podman(temp.path());
        let log = temp.path().join("podman.log");
        let state = temp.path().join("state");
        fs::create_dir_all(&state).unwrap();
        let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap());
        let _vars = VarGuard::set(&[
            ("PATH", Some(std::ffi::OsStr::new(&path))),
            ("FAKE_PODMAN_LOG", Some(log.as_os_str())),
            ("FAKE_PODMAN_STATE", Some(state.as_os_str())),
            ("FAKE_PODMAN_ROOTLESS", Some(std::ffi::OsStr::new("false"))),
        ]);
        let workspace = temp.path().join("workspace");
        fs::create_dir_all(&workspace).unwrap();
        let error = match build_podman_workspace_command("pi".into(), vec![], &workspace, true) {
            Ok(_) => panic!("non-rootless Podman unexpectedly accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("not running rootless"));
        assert!(!fs::read_to_string(log).unwrap().contains("create\t"));
    }

    #[test]
    fn workspace_command_wraps_only_worker_and_limits_writable_mounts() {
        let _guard = EnvGuard::new();
        unsafe { std::env::remove_var("PI_CODING_AGENT_DIR") };
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let hcom_dir = temp.path().join(".hcom");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hcom_dir).unwrap();

        let wrapped = build_workspace_command(
            "/usr/bin/bwrap".into(),
            "/usr/bin/pi".into(),
            vec!["--model".into(), "test".into()],
            &workspace,
            &hcom_dir,
            "luna",
        )
        .unwrap();

        assert_eq!(wrapped.command, "/usr/bin/bwrap");
        assert!(
            wrapped
                .args
                .windows(3)
                .any(|w| w == ["--ro-bind", "/", "/"])
        );
        assert!(wrapped.args.windows(3).any(|w| {
            w[0] == "--bind"
                && w[1] == workspace.to_string_lossy()
                && w[2] == workspace.to_string_lossy()
        }));
        for name in ["extensions", "agents"] {
            assert!(wrapped.args.windows(3).any(|w| {
                w == [
                    "--ro-bind",
                    dirs::home_dir()
                        .unwrap()
                        .join(".pi/agent")
                        .join(name)
                        .to_string_lossy()
                        .as_ref(),
                    hcom_dir
                        .join("sandboxes/luna/pi-agent")
                        .join(name)
                        .to_string_lossy()
                        .as_ref(),
                ]
            }));
        }
        assert!(
            wrapped
                .args
                .windows(3)
                .any(|w| w == ["--unshare-user", "--uid", "0"])
        );
        assert!(
            wrapped
                .args
                .windows(3)
                .any(|w| w == ["--gid", "0", "--ro-bind"])
        );
        assert!(
            wrapped
                .args
                .windows(3)
                .any(|w| { w[0] == "--file" && w[2] == "/run/flock" })
        );
        assert!(wrapped._flock_helper.is_some());
        assert!(
            wrapped
                .args
                .windows(3)
                .any(|w| w == ["--setenv", "PI_POSITION_FLOCK_HELPER", "/run/flock"])
        );
        assert!(
            !wrapped
                .args
                .iter()
                .any(|arg| arg == "PI_CODING_AGENT_SESSION_DIR")
        );
        let host_sessions = dirs::home_dir().unwrap().join(".pi/agent/sessions");
        let sandbox_sessions = hcom_dir.join("sandboxes/luna/pi-agent/sessions");
        assert!(wrapped.args.windows(3).any(|w| {
            w == [
                "--ro-bind",
                host_sessions.to_string_lossy().as_ref(),
                sandbox_sessions.to_string_lossy().as_ref(),
            ]
        }));
        let cwd_sessions = pi_session_directory_name(&workspace);
        assert!(wrapped.args.windows(3).any(|w| {
            w == [
                "--bind",
                host_sessions.join(&cwd_sessions).to_string_lossy().as_ref(),
                sandbox_sessions
                    .join(&cwd_sessions)
                    .to_string_lossy()
                    .as_ref(),
            ]
        }));
        assert!(wrapped.args.ends_with(&[
            "--".to_string(),
            "/usr/bin/pi".to_string(),
            "--model".to_string(),
            "test".to_string(),
        ]));
    }

    #[test]
    fn bubblewrap_enforces_workspace_boundary_for_descendants() {
        let _guard = EnvGuard::new();
        unsafe { std::env::remove_var("PI_CODING_AGENT_DIR") };
        let Some(bwrap) = crate::terminal::which_bin("bwrap") else {
            return;
        };
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let hcom_dir = temp.path().join("hcom-state");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hcom_dir).unwrap();
        let home = dirs::home_dir().unwrap();
        let outside_dir = tempfile::Builder::new()
            .prefix("hcom-sandbox-outside.")
            .tempdir_in(home)
            .unwrap();
        let outside = outside_dir.path().join("outside.txt");
        fs::write(&outside, "host").unwrap();

        let script = r#"
set -eu
printf sandbox > workspace.txt
/bin/bash -c 'printf child > child.txt'
{ printf escaped > "$1"; } 2>/dev/null && exit 90 || :
printf private > /tmp/private.txt
test "$(cat /tmp/private.txt)" = private
test -f "$PI_CODING_AGENT_DIR/agents/Explore.md"
grep -q '^name: Explore$' "$PI_CODING_AGENT_DIR/agents/Explore.md"
{ printf tampered >> "$PI_CODING_AGENT_DIR/agents/Explore.md"; } 2>/dev/null && exit 91 || :
"#;
        let wrapped = build_workspace_command(
            bwrap,
            "/bin/bash".into(),
            vec![
                "-c".into(),
                script.into(),
                "bash".into(),
                outside.to_string_lossy().into_owned(),
            ],
            &workspace,
            &hcom_dir,
            "test",
        )
        .unwrap();
        let output = std::process::Command::new(&wrapped.command)
            .args(&wrapped.args)
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "bwrap failed with {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            fs::read_to_string(workspace.join("workspace.txt")).unwrap(),
            "sandbox"
        );
        assert_eq!(
            fs::read_to_string(workspace.join("child.txt")).unwrap(),
            "child"
        );
        assert_eq!(fs::read_to_string(outside).unwrap(), "host");
    }

    #[test]
    fn native_pi_sessions_are_shared_for_current_cwd_and_other_projects_are_read_only() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let hcom_dir = temp.path().join("hcom-state");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hcom_dir).unwrap();

        let host_pi = temp.path().join("pi-agent");
        let sessions = host_pi.join("sessions");
        let current_sessions = sessions.join(pi_session_directory_name(&workspace));
        let other_sessions = sessions.join("--other-project--");
        fs::create_dir_all(&current_sessions).unwrap();
        fs::create_dir_all(&other_sessions).unwrap();
        let historical = current_sessions.join("historical.jsonl");
        let other = other_sessions.join("other.jsonl");
        fs::write(&historical, "history\n").unwrap();
        fs::write(&other, "other\n").unwrap();
        unsafe { std::env::set_var("PI_CODING_AGENT_DIR", &host_pi) };

        let Some(bwrap) = crate::terminal::which_bin("bwrap") else {
            return;
        };
        let fake_bin = workspace.join("bin");
        fs::create_dir_all(&fake_bin).unwrap();
        let fake_pi = fake_bin.join("pi");
        fs::write(&fake_pi, "#!/bin/bash\nexec /bin/bash \"$@\"\n").unwrap();
        let mut permissions = fs::metadata(&fake_pi).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
        fs::set_permissions(&fake_pi, permissions).unwrap();
        let sandbox_sessions = hcom_dir.join("sandboxes/sana/pi-agent/sessions");
        let sandbox_current = sandbox_sessions.join(pi_session_directory_name(&workspace));
        let sandbox_other = sandbox_sessions.join("--other-project--");
        let script = r#"
set -eu
test "${PI_CODING_AGENT_SESSION_DIR+x}" != x
cat "$1/historical.jsonl" >/dev/null
printf resumed >> "$1/historical.jsonl"
printf new > "$1/new.jsonl"
cat "$2/other.jsonl" >/dev/null
{ printf other-tamper >> "$2/other.jsonl"; } 2>/dev/null && exit 92 || :
{ printf other-new > "$2/new.jsonl"; } 2>/dev/null && exit 93 || :
"#;
        let wrapped = build_workspace_command(
            bwrap,
            fake_pi.to_string_lossy().into_owned(),
            vec![
                "-c".into(),
                script.into(),
                "bash".into(),
                sandbox_current.to_string_lossy().into_owned(),
                sandbox_other.to_string_lossy().into_owned(),
            ],
            &workspace,
            &hcom_dir,
            "sana",
        )
        .unwrap();

        assert!(
            !wrapped
                .args
                .iter()
                .any(|arg| arg == "PI_CODING_AGENT_SESSION_DIR")
        );

        let output = std::process::Command::new(&wrapped.command)
            .args(&wrapped.args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "bwrap resume probe failed with {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_to_string(&historical).unwrap(), "history\nresumed");
        assert_eq!(
            fs::read_to_string(current_sessions.join("new.jsonl")).unwrap(),
            "new"
        );
        assert_eq!(fs::read_to_string(&other).unwrap(), "other\n");
        assert!(!other_sessions.join("new.jsonl").exists());
    }

    #[test]
    fn sandbox_pi_rpc_environment_is_fail_closed() {
        let _guard = EnvGuard::new();
        let vars = [
            ("HCOM_LAUNCHED", Some(std::ffi::OsStr::new("1"))),
            ("HCOM_PROCESS_ID", Some(std::ffi::OsStr::new("process-id"))),
            (MODE_ENV, Some(std::ffi::OsStr::new(PODMAN_WORKSPACE_MODE))),
            (ROOT_ENV, Some(std::ffi::OsStr::new("/workspace"))),
            (
                "HCOM_BROKER_SOCKET",
                Some(std::ffi::OsStr::new("/broker/socket")),
            ),
            (
                "HCOM_BROKER_TOKEN_FILE",
                Some(std::ffi::OsStr::new("/broker/token")),
            ),
        ];
        let _vars = VarGuard::set(&vars);
        assert!(validate_pi_rpc_environment().is_ok());
        unsafe { std::env::set_var(MODE_ENV, "off") };
        assert!(
            validate_pi_rpc_environment()
                .unwrap_err()
                .to_string()
                .contains("podman-workspace")
        );
        unsafe { std::env::set_var(MODE_ENV, PODMAN_WORKSPACE_MODE) };
        unsafe { std::env::remove_var("HCOM_BROKER_TOKEN_FILE") };
        assert!(
            validate_pi_rpc_environment()
                .unwrap_err()
                .to_string()
                .contains("authenticated workspace broker")
        );
    }

    #[test]
    fn sandbox_pi_rpc_arguments_are_fail_closed() {
        assert!(validate_pi_rpc_args(&["--mode".into(), "rpc".into()]).is_ok());
        assert!(
            validate_pi_rpc_args(&[
                "--mode".into(),
                "rpc".into(),
                "--no-themes".into(),
                "--session".into(),
                "01a0387b-6ec0-7421-afd1-4fe665227c50".into(),
            ])
            .is_ok()
        );
        assert!(validate_pi_rpc_args(&["--no-themes".into()]).is_err());
        assert!(
            validate_pi_rpc_args(&[
                "--mode".into(),
                "rpc".into(),
                "--model".into(),
                "unsafe".into(),
            ])
            .is_err()
        );
        assert!(
            validate_pi_rpc_args(&[
                "--mode".into(),
                "rpc".into(),
                "--session".into(),
                "one".into(),
                "--session".into(),
                "two".into(),
            ])
            .is_err()
        );
    }

    #[test]
    fn happy_host_runner_must_match_pi_override() {
        let _guard = EnvGuard::new();
        let temp = tempfile::tempdir().unwrap();
        let runner = temp.path().join("hcom-happy-pi");
        fs::write(&runner, "#!/bin/sh\n").unwrap();
        let vars = [
            ("HCOM_HAPPY_HOST_RUNNER", Some(std::ffi::OsStr::new("1"))),
            ("HCOM_PI_BIN", Some(runner.as_os_str())),
        ];
        let _vars = VarGuard::set(&vars);
        let worker = wrap_worker_command(
            runner.to_string_lossy().into_owned(),
            vec!["--session".into(), "session-id".into()],
            Some("happy"),
        )
        .unwrap();
        assert_eq!(worker.command, runner.to_string_lossy());
        let mismatch = wrap_worker_command("/bin/sh".into(), vec![], Some("happy")).unwrap();
        assert_eq!(mismatch.command, "/bin/sh");
        assert_ne!(mismatch.command, worker.command);
    }

    #[test]
    fn private_runtime_is_outside_workspace() {
        let _guard = EnvGuard::new();
        unsafe { std::env::remove_var("PI_CODING_AGENT_DIR") };
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let hcom_dir = temp.path().join("hcom-state");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hcom_dir).unwrap();

        build_workspace_command(
            "bwrap".into(),
            "pi".into(),
            vec![],
            &workspace,
            &hcom_dir,
            "nori",
        )
        .unwrap();

        assert!(hcom_dir.join("sandboxes/nori/pi-agent/sessions").is_dir());
        assert!(!hcom_dir.join("sandboxes/nori/pi-sessions").exists());
        assert!(!workspace.join(".hcom-sandbox").exists());
    }
}
