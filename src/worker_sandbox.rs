//! Explicit OS-level sandboxing for HCOM-launched workers.
//!
//! The HCOM PTY proxy remains on the host. Only the final AI tool process and
//! its descendants are wrapped in bubblewrap, so delivery and wakeup state do
//! not need broad writable access inside the sandbox.

use anyhow::{Context, Result, bail};
use std::fs::{self, File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

const MODE_ENV: &str = "HCOM_WORKER_SANDBOX";
const ROOT_ENV: &str = "HCOM_WORKER_SANDBOX_ROOT";
const WORKSPACE_MODE: &str = "workspace";

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
    let mode = std::env::var(MODE_ENV).unwrap_or_default();
    if mode.is_empty() || mode == "off" {
        return Ok(WorkerCommand {
            command,
            args,
            _flock_helper: None,
        });
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

fn option_value(args: &[String], name: &str) -> Option<String> {
    args.iter().enumerate().find_map(|(index, arg)| {
        if arg == name {
            args.get(index + 1).cloned()
        } else {
            arg.strip_prefix(&format!("{name}=")).map(str::to_owned)
        }
    })
}

fn replace_option_value(args: &mut [String], name: &str, value: &Path) {
    let replacement = value.to_string_lossy().into_owned();
    let equals_prefix = format!("{name}=");
    let mut index = 0;
    while index < args.len() {
        if args[index] == name {
            if let Some(next) = args.get_mut(index + 1) {
                *next = replacement.clone();
            }
            index += 2;
        } else if args[index].starts_with(&equals_prefix) {
            args[index] = format!("{name}={replacement}");
            index += 1;
        } else {
            index += 1;
        }
    }
}

/// Migrate a cross-worker Pi resume into the new worker's private session
/// directory. Arbitrary source directories are rejected: the source must be
/// exactly another HCOM sandbox's `pi-sessions` directory and contain one
/// regular transcript matching the explicit session id. Copying avoids making
/// any part of the former worker's runtime writable in the new namespace.
fn migrate_cross_worker_resume(
    worker_command: &str,
    worker_args: &mut [String],
    hcom_dir: &Path,
    own_sessions: &Path,
) -> Result<Option<(PathBuf, PathBuf)>> {
    let command_name = Path::new(worker_command)
        .file_name()
        .and_then(|name| name.to_str());
    if !matches!(command_name, Some("pi" | "pi-agent")) {
        return Ok(None);
    }

    let (Some(session_dir), Some(session_id)) = (
        option_value(worker_args, "--session-dir"),
        option_value(worker_args, "--session"),
    ) else {
        return Ok(None);
    };
    if session_id.is_empty()
        || session_id.contains('/')
        || session_id.contains('\\')
        || session_id == "."
        || session_id == ".."
    {
        bail!("Refusing unsafe Pi resume session id: {session_id}");
    }

    let session_dir = PathBuf::from(session_dir)
        .canonicalize()
        .context("Cannot resolve Pi resume session directory")?;
    let own_sessions = own_sessions
        .canonicalize()
        .context("Cannot resolve worker Pi session directory")?;
    if session_dir == own_sessions {
        return Ok(None);
    }

    let sandboxes = hcom_dir
        .join("sandboxes")
        .canonicalize()
        .context("Cannot resolve HCOM sandbox directory while validating Pi resume")?;
    let relative = session_dir.strip_prefix(&sandboxes).map_err(|_| {
        anyhow::anyhow!(
            "Refusing Pi resume directory outside HCOM sandboxes: {}",
            session_dir.display()
        )
    })?;
    let components: Vec<_> = relative.components().collect();
    if components.len() != 2
        || components[1].as_os_str() != "pi-sessions"
        || components[0].as_os_str().is_empty()
    {
        bail!(
            "Refusing non-standard HCOM Pi resume directory: {}",
            session_dir.display()
        );
    }

    let suffix = format!("_{session_id}.jsonl");
    let mut matches = Vec::new();
    for entry in fs::read_dir(&session_dir)
        .with_context(|| format!("Cannot read Pi resume directory {}", session_dir.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.file_name().to_string_lossy().ends_with(&suffix) {
            matches.push(entry.path());
        }
    }
    if matches.len() != 1 {
        bail!(
            "Expected exactly one transcript for Pi session {session_id} in {}, found {}",
            session_dir.display(),
            matches.len()
        );
    }
    let source = matches.pop().expect("one transcript was validated");
    let file_name = source
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("Pi resume transcript has no file name"))?;
    let destination = own_sessions.join(file_name);
    if destination.exists() {
        bail!(
            "Refusing to overwrite existing migrated Pi transcript: {}",
            destination.display()
        );
    }
    copy_private_file(&source, &destination)?;
    replace_option_value(worker_args, "--session-dir", &own_sessions);
    Ok(Some((source, destination)))
}

fn build_workspace_command(
    bwrap: String,
    worker_command: String,
    mut worker_args: Vec<String>,
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
    migrate_cross_worker_resume(&worker_command, &mut worker_args, hcom_dir, &sessions)?;
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
        _flock_helper: Some(flock_helper),
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
test -f "$PI_CODING_AGENT_DIR/agents/Explore.md"
grep -q '^name: Explore$' "$PI_CODING_AGENT_DIR/agents/Explore.md"
if printf tampered >> "$PI_CODING_AGENT_DIR/agents/Explore.md" 2>/dev/null; then exit 91; fi
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
    fn cross_worker_pi_resume_migrates_only_selected_transcript() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let hcom_dir = temp.path().join("hcom-state");
        let former_sessions = hcom_dir.join("sandboxes/vera/pi-sessions");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&former_sessions).unwrap();
        let selected = former_sessions.join("2026-08-25T00-00-00Z_session-123.jsonl");
        let sibling = former_sessions.join("2026-08-25T00-00-01Z_session-456.jsonl");
        fs::write(&selected, "selected\n").unwrap();
        fs::write(&sibling, "sibling\n").unwrap();

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
        let migrated = hcom_dir
            .join("sandboxes/sana/pi-sessions")
            .join(selected.file_name().unwrap());
        let script = r#"
set -eu
printf resumed >> "$1"
if printf source-tamper >> "$2" 2>/dev/null; then exit 92; fi
if printf sibling-tamper >> "$3" 2>/dev/null; then exit 93; fi
if printf directory-tamper > "$4/new.jsonl" 2>/dev/null; then exit 94; fi
"#;
        let wrapped = build_workspace_command(
            bwrap,
            fake_pi.to_string_lossy().into_owned(),
            vec![
                "-c".into(),
                script.into(),
                "bash".into(),
                migrated.to_string_lossy().into_owned(),
                selected.to_string_lossy().into_owned(),
                sibling.to_string_lossy().into_owned(),
                former_sessions.to_string_lossy().into_owned(),
                "--session-dir".into(),
                former_sessions.to_string_lossy().into_owned(),
                "--session".into(),
                "session-123".into(),
            ],
            &workspace,
            &hcom_dir,
            "sana",
        )
        .unwrap();

        assert!(wrapped.args.windows(2).any(|w| {
            w[0] == "--session-dir"
                && w[1]
                    == hcom_dir
                        .join("sandboxes/sana/pi-sessions")
                        .to_string_lossy()
        }));
        assert!(!wrapped.args.windows(3).any(|w| {
            w[0] == "--bind"
                && (w[1] == selected.to_string_lossy()
                    || w[1] == former_sessions.to_string_lossy()
                    || w[1] == sibling.to_string_lossy())
        }));

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
        assert_eq!(fs::read_to_string(&migrated).unwrap(), "selected\nresumed");
        assert_eq!(fs::read_to_string(&selected).unwrap(), "selected\n");
        assert_eq!(fs::read_to_string(&sibling).unwrap(), "sibling\n");
        assert!(!former_sessions.join("new.jsonl").exists());
    }

    #[test]
    fn cross_worker_pi_resume_rejects_arbitrary_session_directory() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let hcom_dir = temp.path().join("hcom-state");
        let arbitrary = temp.path().join("arbitrary");
        fs::create_dir_all(&workspace).unwrap();
        fs::create_dir_all(&hcom_dir).unwrap();
        fs::create_dir_all(&arbitrary).unwrap();
        fs::write(arbitrary.join("session-123.jsonl"), "unsafe\n").unwrap();

        let result = build_workspace_command(
            "bwrap".into(),
            "pi".into(),
            vec![
                format!("--session-dir={}", arbitrary.display()),
                "--session=session-123".into(),
            ],
            &workspace,
            &hcom_dir,
            "sana",
        );

        let error = match result {
            Ok(_) => panic!("arbitrary Pi session directory was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("outside HCOM sandboxes"));
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
