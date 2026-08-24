//! Explicit OS-level sandboxing for HCOM-launched workers.
//!
//! The HCOM PTY proxy remains on the host. Only the final AI tool process and
//! its descendants are wrapped in bubblewrap, so delivery and wakeup state do
//! not need broad writable access inside the sandbox.

use anyhow::{Context, Result, bail};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

const MODE_ENV: &str = "HCOM_WORKER_SANDBOX";
const ROOT_ENV: &str = "HCOM_WORKER_SANDBOX_ROOT";
const WORKSPACE_MODE: &str = "workspace";

pub struct WorkerCommand {
    pub command: String,
    pub args: Vec<String>,
}

pub fn wrap_worker_command(
    command: String,
    args: Vec<String>,
    instance_name: Option<&str>,
) -> Result<WorkerCommand> {
    let mode = std::env::var(MODE_ENV).unwrap_or_default();
    if mode.is_empty() || mode == "off" {
        return Ok(WorkerCommand { command, args });
    }
    if mode != WORKSPACE_MODE {
        bail!("Unsupported HCOM worker sandbox mode: {mode}");
    }
    if !cfg!(target_os = "linux") {
        bail!("HCOM worker sandbox requires Linux bubblewrap");
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

    let instance = instance_name
        .filter(|name| !name.is_empty())
        .ok_or_else(|| anyhow::anyhow!("Sandbox worker requires an HCOM instance name"))?;
    let bwrap = crate::terminal::which_bin("bwrap")
        .ok_or_else(|| anyhow::anyhow!("bubblewrap (bwrap) is required for sandbox workers"))?;

    build_workspace_command(bwrap, command, args, &workspace, &hcom_dir, instance)
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
    let sessions = sandbox_root.join("pi-sessions");
    for dir in [&sandbox_root, &sandbox_hcom, &sandbox_pi, &sessions] {
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
            copy_private_file(&entry.path(), &sandbox_pi.join(entry.file_name()))?;
        }
    }
    // Pi's package manager installs enabled packages into this directory.
    // Seed a private copy so npm never needs write access to the host tree.
    let host_npm = pi_agent.join("npm");
    if host_npm.is_dir() {
        copy_private_tree(&host_npm, &sandbox_pi.join("npm"))?;
    }
    let db = hcom_dir.join("hcom.db");
    let db_wal = hcom_dir.join("hcom.db-wal");
    let db_shm = hcom_dir.join("hcom.db-shm");
    for file in [&db, &db_wal, &db_shm] {
        ensure_private_file(file)?;
    }
    for file in ["hcom.db", "hcom.db-wal", "hcom.db-shm"] {
        ensure_private_file(&sandbox_hcom.join(file))?;
    }

    let mut args = vec![
        "--die-with-parent".into(),
        "--new-session".into(),
        "--unshare-pid".into(),
        "--ro-bind".into(),
        "/".into(),
        "/".into(),
        "--tmpfs".into(),
        "/tmp".into(),
        "--proc".into(),
        "/proc".into(),
        "--dev-bind".into(),
        "/dev".into(),
        "/dev".into(),
    ];

    // The whole host starts read-only. Re-open only the project and this
    // worker's private runtime state as writable mounts.
    push_bind(&mut args, workspace, workspace);
    push_bind(&mut args, &sandbox_root, &sandbox_root);
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
        "--setenv".into(),
        "PI_CODING_AGENT_SESSION_DIR".into(),
        sessions.to_string_lossy().into_owned(),
        "--".into(),
        worker_command,
    ]);
    args.extend(worker_args);

    Ok(WorkerCommand {
        command: bwrap,
        args,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_command_wraps_only_worker_and_limits_writable_mounts() {
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
        assert!(wrapped.args.windows(3).any(|w| {
            w == [
                "--setenv",
                "PI_CODING_AGENT_SESSION_DIR",
                hcom_dir
                    .join("sandboxes/luna/pi-sessions")
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
if printf escaped > "$1" 2>/dev/null; then exit 90; fi
printf private > /tmp/private.txt
test "$(cat /tmp/private.txt)" = private
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
    fn private_runtime_is_outside_workspace() {
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

        assert!(hcom_dir.join("sandboxes/nori/pi-sessions").is_dir());
        assert!(!workspace.join(".hcom-sandbox").exists());
    }
}
