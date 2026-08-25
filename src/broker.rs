//! Host-global HCOM execution broker.

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const PROTOCOL_VERSION: u32 = 1;
const MAX_REQUEST: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
const MAX_HEADER: usize = 64 * 1024;
const CLIENT_ENV_ALLOWLIST: &[&str] = &["HCOM_PROCESS_ID", "HCOM_LAUNCHED", "HCOM_TAG"];

#[derive(Debug, Serialize, Deserialize)]
struct Request {
    version: u32,
    token: String,
    argv: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct ResponseHeader {
    version: u32,
    status: i32,
    stdout_len: usize,
    stderr_len: usize,
    error: Option<String>,
}

pub fn maybe_serve(argv: &[String]) -> Result<bool> {
    if argv.first().map(String::as_str) != Some("broker-serve") {
        return Ok(false);
    }
    #[cfg(not(unix))]
    bail!("hcom broker-serve is only supported on Unix");
    #[cfg(unix)]
    {
        serve(
            Path::new(required_flag(argv, "--socket")?),
            Path::new(required_flag(argv, "--token-file")?),
            Path::new(required_flag(argv, "--workspace")?),
        )?;
        Ok(true)
    }
}

pub fn maybe_forward(argv: &[String]) -> Result<Option<i32>> {
    if env::var_os("HCOM_BROKER_DIRECT").as_deref() == Some(std::ffi::OsStr::new("1"))
        || is_local_control_command(argv)
    {
        return Ok(None);
    }
    let (Some(socket), Some(token_file)) = (
        env::var_os("HCOM_BROKER_SOCKET"),
        env::var_os("HCOM_BROKER_TOKEN_FILE"),
    ) else {
        return Ok(None);
    };
    #[cfg(not(unix))]
    bail!("HCOM broker forwarding is only supported on Unix");
    #[cfg(unix)]
    {
        let request = Request {
            version: PROTOCOL_VERSION,
            token: read_token(Path::new(&token_file))?,
            argv: argv.to_vec(),
            env: CLIENT_ENV_ALLOWLIST
                .iter()
                .filter_map(|key| env::var(key).ok().map(|value| ((*key).to_string(), value)))
                .collect(),
            cwd: env::current_dir().context("determine broker client cwd")?,
        };
        let (status, stdout, stderr, error) = client_round_trip(Path::new(&socket), &request)?;
        std::io::stdout().write_all(&stdout)?;
        std::io::stderr().write_all(&stderr)?;
        if let Some(error) = error {
            bail!("broker rejected request: {error}");
        }
        Ok(Some(status))
    }
}

fn is_local_control_command(argv: &[String]) -> bool {
    let mut index = 0;
    while index < argv.len() {
        match argv[index].as_str() {
            "--go" => index += 1,
            "--name" => index += 2,
            _ => break,
        }
    }
    let Some(command) = argv.get(index).map(String::as_str) else {
        return true;
    };
    command == "pty"
        || matches!(command, "launch" | "resume" | "fork" | "r" | "f")
        || command.parse::<u32>().is_ok()
        || is_released_tool(command)
}

fn required_flag<'a>(argv: &'a [String], flag: &str) -> Result<&'a str> {
    let index = argv
        .iter()
        .position(|arg| arg == flag)
        .ok_or_else(|| anyhow!("broker-serve requires {flag} PATH"))?;
    argv.get(index + 1)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("broker-serve requires {flag} PATH"))
}

pub fn authorize(argv: &[String]) -> bool {
    let mut index = 0;
    while index < argv.len() {
        match argv[index].as_str() {
            "--go" => index += 1,
            "--name" => index += 2,
            _ => break,
        }
    }
    let Some(command) = argv.get(index).map(String::as_str) else {
        return false;
    };
    if matches!(
        command,
        "pi-start"
            | "pi-status"
            | "pi-read"
            | "pi-beforetool"
            | "pi-stop"
            | "send"
            | "list"
            | "events"
            | "listen"
            | "status"
            | "start"
            | "stop"
            | "transcript"
            | "bundle"
            | "kill"
            | "term"
    ) {
        return true;
    }
    false
}

fn is_released_tool(name: &str) -> bool {
    name.parse::<crate::tool::Tool>()
        .is_ok_and(|tool| tool.spec().released)
}

fn read_token(path: &Path) -> Result<String> {
    reject_symlink(path, "token file")?;
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("read token file metadata: {}", path.display()))?;
    if !metadata.is_file() {
        bail!("token file is not a regular file: {}", path.display());
    }
    let token = std::fs::read_to_string(path)
        .with_context(|| format!("read broker token: {}", path.display()))?;
    let token = token.trim_end_matches(['\r', '\n']).to_string();
    if token.is_empty() {
        bail!("broker token is empty");
    }
    Ok(token)
}

fn reject_symlink(path: &Path, kind: &str) -> Result<()> {
    if std::fs::symlink_metadata(path)
        .with_context(|| format!("inspect {kind}: {}", path.display()))?
        .file_type()
        .is_symlink()
    {
        bail!("{kind} must not be a symlink: {}", path.display());
    }
    Ok(())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}

#[cfg(unix)]
fn serve(socket: &Path, token_file: &Path, workspace: &Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    use std::os::unix::net::UnixListener;
    let token = read_token(token_file)?;
    let workspace = workspace
        .canonicalize()
        .with_context(|| format!("canonicalize workspace: {}", workspace.display()))?;
    if !workspace.is_dir() {
        bail!(
            "broker workspace is not a directory: {}",
            workspace.display()
        );
    }
    let parent = socket
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| anyhow!("broker socket must have a parent directory"))?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("create socket directory: {}", parent.display()))?;
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
    if let Ok(metadata) = std::fs::symlink_metadata(socket) {
        if metadata.file_type().is_symlink() {
            bail!(
                "broker socket path must not be a symlink: {}",
                socket.display()
            );
        }
        if !metadata.file_type().is_socket() {
            bail!("refusing to remove non-socket path: {}", socket.display());
        }
        std::fs::remove_file(socket)
            .with_context(|| format!("remove stale broker socket: {}", socket.display()))?;
    }
    let listener = UnixListener::bind(socket)
        .with_context(|| format!("bind broker socket: {}", socket.display()))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let _cleanup = SocketCleanup(socket.to_path_buf());
    let executable = env::current_exe().context("locate current HCOM executable")?;
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let (token, workspace, executable) =
                    (token.clone(), workspace.clone(), executable.clone());
                std::thread::spawn(move || {
                    if let Err(error) = handle_connection(stream, &token, &workspace, &executable) {
                        eprintln!("hcom broker connection error: {error:#}");
                    }
                });
            }
            Err(error) => eprintln!("hcom broker accept error: {error}"),
        }
    }
    Ok(())
}

#[cfg(unix)]
struct SocketCleanup(PathBuf);
#[cfg(unix)]
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(unix)]
fn handle_connection(
    mut stream: std::os::unix::net::UnixStream,
    token: &str,
    workspace: &Path,
    executable: &Path,
) -> Result<()> {
    let request: Request = read_json_frame(&mut stream, MAX_REQUEST)?;
    match process_request(request, token, workspace, executable) {
        Ok((status, stdout, stderr)) => write_response(&mut stream, status, &stdout, &stderr, None),
        Err(error) => write_response(&mut stream, 1, &[], &[], Some(format!("{error:#}"))),
    }
}

#[cfg(unix)]
fn process_request(
    request: Request,
    token: &str,
    workspace: &Path,
    executable: &Path,
) -> Result<(i32, Vec<u8>, Vec<u8>)> {
    use std::process::{Command, Stdio};
    if request.version != PROTOCOL_VERSION {
        bail!("unsupported broker protocol version {}", request.version);
    }
    if !constant_time_eq(request.token.as_bytes(), token.as_bytes()) {
        bail!("broker authentication failed");
    }
    if !authorize(&request.argv) {
        bail!("broker command is not authorized");
    }
    if request
        .env
        .keys()
        .any(|key| !CLIENT_ENV_ALLOWLIST.contains(&key.as_str()))
    {
        bail!("request contains a disallowed environment variable");
    }
    let cwd = request
        .cwd
        .canonicalize()
        .with_context(|| format!("canonicalize client cwd: {}", request.cwd.display()))?;
    if !cwd.starts_with(workspace) {
        bail!("client cwd is outside broker workspace");
    }
    let mut command = Command::new(executable);
    command
        .args(&request.argv)
        .current_dir(workspace)
        .env("HCOM_BROKER_DIRECT", "1")
        .envs(request.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().context("spawn host HCOM process")?;
    let stdout = child.stdout.take().context("capture broker stdout")?;
    let stderr = child.stderr.take().context("capture broker stderr")?;
    let stdout_thread = std::thread::spawn(move || read_capped(stdout, MAX_OUTPUT));
    let stderr_thread = std::thread::spawn(move || read_capped(stderr, MAX_OUTPUT));
    let status = child.wait().context("wait for host HCOM process")?;
    let stdout = stdout_thread
        .join()
        .map_err(|_| anyhow!("stdout reader panicked"))??;
    let stderr = stderr_thread
        .join()
        .map_err(|_| anyhow!("stderr reader panicked"))??;
    let code = status.code().unwrap_or_else(|| {
        use std::os::unix::process::ExitStatusExt;
        128 + status.signal().unwrap_or(1)
    });
    Ok((code, stdout, stderr))
}

fn read_capped(mut reader: impl Read, limit: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut exceeded = false;
    let mut buffer = [0_u8; 8192];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = limit.saturating_sub(output.len());
        if count > remaining {
            exceeded = true;
        }
        output.extend_from_slice(&buffer[..count.min(remaining)]);
    }
    if exceeded {
        bail!("broker child output exceeds {limit} bytes");
    }
    Ok(output)
}

fn read_json_frame<T: for<'de> Deserialize<'de>>(
    reader: &mut impl Read,
    limit: usize,
) -> Result<T> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > limit {
        bail!("broker frame exceeds {limit} bytes");
    }
    let mut bytes = vec![0_u8; length];
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes).context("decode broker JSON frame")
}

fn write_json_frame(writer: &mut impl Write, value: &impl Serialize, limit: usize) -> Result<()> {
    let bytes = serde_json::to_vec(value).context("encode broker JSON frame")?;
    if bytes.len() > limit {
        bail!("broker frame exceeds {limit} bytes");
    }
    writer.write_all(&(bytes.len() as u32).to_be_bytes())?;
    writer.write_all(&bytes)?;
    Ok(())
}

fn write_response(
    writer: &mut impl Write,
    status: i32,
    stdout: &[u8],
    stderr: &[u8],
    error: Option<String>,
) -> Result<()> {
    write_json_frame(
        writer,
        &ResponseHeader {
            version: PROTOCOL_VERSION,
            status,
            stdout_len: stdout.len(),
            stderr_len: stderr.len(),
            error,
        },
        MAX_HEADER,
    )?;
    writer.write_all(stdout)?;
    writer.write_all(stderr)?;
    Ok(())
}

#[cfg(unix)]
fn client_round_trip(
    socket: &Path,
    request: &Request,
) -> Result<(i32, Vec<u8>, Vec<u8>, Option<String>)> {
    use std::os::unix::fs::FileTypeExt;
    use std::os::unix::net::UnixStream;
    let metadata = std::fs::symlink_metadata(socket)
        .with_context(|| format!("inspect broker socket: {}", socket.display()))?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_socket() {
        bail!(
            "broker socket path is not a Unix socket: {}",
            socket.display()
        );
    }
    let mut stream = UnixStream::connect(socket)
        .with_context(|| format!("connect broker socket: {}", socket.display()))?;
    write_json_frame(&mut stream, request, MAX_REQUEST)?;
    let header: ResponseHeader = read_json_frame(&mut stream, MAX_HEADER)?;
    if header.version != PROTOCOL_VERSION {
        bail!("unsupported broker response version {}", header.version);
    }
    if header.stdout_len > MAX_OUTPUT || header.stderr_len > MAX_OUTPUT {
        bail!("broker response exceeds output limit");
    }
    let mut stdout = vec![0_u8; header.stdout_len];
    let mut stderr = vec![0_u8; header.stderr_len];
    stream.read_exact(&mut stdout)?;
    stream.read_exact(&mut stderr)?;
    Ok((header.status, stdout, stderr, header.error))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_string()).collect()
    }

    fn request(workspace: &Path) -> Request {
        Request {
            version: PROTOCOL_VERSION,
            token: "secret".into(),
            argv: strings(&["status"]),
            env: BTreeMap::new(),
            cwd: workspace.to_path_buf(),
        }
    }

    #[test]
    fn authorization_is_allowlist_only() {
        for argv in [
            strings(&["send", "@a", "--", "hello"]),
            strings(&["pi-beforetool", "--name", "a"]),
            strings(&["--name", "a", "status"]),
        ] {
            assert!(authorize(&argv), "should authorize {argv:?}");
        }
        for argv in [
            vec![],
            strings(&["broker-serve"]),
            strings(&["config"]),
            strings(&["hooks"]),
            strings(&["archive"]),
            strings(&["reset"]),
            strings(&["relay"]),
            strings(&["run"]),
            strings(&["update"]),
            strings(&["pty", "claude"]),
            strings(&["resume", "a"]),
            strings(&["claude"]),
            strings(&["3", "pi"]),
            strings(&["unknown"]),
            strings(&["2", "unknown"]),
        ] {
            assert!(!authorize(&argv), "should deny {argv:?}");
        }
    }
    #[test]
    fn launch_and_pty_commands_stay_local() {
        for argv in [
            vec![],
            strings(&["pty", "pi"]),
            strings(&["resume", "a"]),
            strings(&["r", "a"]),
            strings(&["fork", "a"]),
            strings(&["pi"]),
            strings(&["3", "pi"]),
        ] {
            assert!(
                is_local_control_command(&argv),
                "should stay local {argv:?}"
            );
        }
        assert!(!is_local_control_command(&strings(&["send", "@a", "hi"])));
        assert!(!is_local_control_command(&strings(&["status"])));
    }

    #[test]
    fn protocol_frames_are_bounded_and_round_trip() {
        let request = Request {
            version: PROTOCOL_VERSION,
            token: "secret".into(),
            argv: strings(&["status", "--json"]),
            env: BTreeMap::new(),
            cwd: PathBuf::from("/tmp"),
        };
        let mut wire = Vec::new();
        write_json_frame(&mut wire, &request, MAX_REQUEST).unwrap();
        let decoded: Request = read_json_frame(&mut wire.as_slice(), MAX_REQUEST).unwrap();
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert_eq!(decoded.argv, request.argv);
        let mut framed = Vec::new();
        framed.extend_from_slice(&5_u32.to_be_bytes());
        framed.extend_from_slice(&[0_u8; 5]);
        assert!(read_json_frame::<Request>(&mut framed.as_slice(), 4).is_err());
    }
    #[test]
    fn token_comparison_checks_content_and_length() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreu"));
        assert!(!constant_time_eq(b"secret", b"secret-extra"));
    }

    #[cfg(unix)]
    #[test]
    fn authentication_environment_and_workspace_are_enforced() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let child = workspace.join("child");
        std::fs::create_dir_all(&child).unwrap();
        let executable = temp.path().join("fake-hcom");
        std::fs::write(
            &executable,
            "#!/bin/sh\nprintf 'out:%s:%s' \"$PWD\" \"$HCOM_PROCESS_ID\"\nprintf 'err' >&2\nexit 7\n",
        )
        .unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        let canonical_workspace = workspace.canonicalize().unwrap();

        let mut valid = request(&child);
        valid.env.insert("HCOM_PROCESS_ID".into(), "agent-1".into());
        let (status, stdout, stderr) =
            process_request(valid, "secret", &canonical_workspace, &executable).unwrap();
        assert_eq!(status, 7);
        assert_eq!(
            stdout,
            format!("out:{}:agent-1", canonical_workspace.display()).as_bytes()
        );
        assert_eq!(stderr, b"err");

        let mut bad_token = request(&workspace);
        bad_token.token = "wrong".into();
        assert!(
            process_request(bad_token, "secret", &canonical_workspace, &executable)
                .unwrap_err()
                .to_string()
                .contains("authentication")
        );

        let outside = tempfile::tempdir().unwrap();
        assert!(
            process_request(
                request(outside.path()),
                "secret",
                &canonical_workspace,
                &executable
            )
            .unwrap_err()
            .to_string()
            .contains("outside broker workspace")
        );

        let mut bad_env = request(&workspace);
        bad_env.env.insert("HOME".into(), "/attacker".into());
        assert!(
            process_request(bad_env, "secret", &canonical_workspace, &executable)
                .unwrap_err()
                .to_string()
                .contains("disallowed environment")
        );
    }

    #[cfg(unix)]
    #[test]
    fn client_connection_preserves_output_and_status() {
        use std::os::unix::net::UnixListener;

        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("broker.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let received: Request = read_json_frame(&mut stream, MAX_REQUEST).unwrap();
            assert_eq!(received.argv, strings(&["status"]));
            write_response(&mut stream, 23, b"exact\0stdout\n", b"exact stderr\n", None).unwrap();
        });

        let response = client_round_trip(&socket, &request(temp.path())).unwrap();
        assert_eq!(response.0, 23);
        assert_eq!(response.1, b"exact\0stdout\n");
        assert_eq!(response.2, b"exact stderr\n");
        assert!(response.3.is_none());
        server.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn token_symlinks_and_oversized_output_are_rejected() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let token = temp.path().join("token");
        let link = temp.path().join("token-link");
        std::fs::write(&token, "secret\n").unwrap();
        symlink(&token, &link).unwrap();
        assert!(
            read_token(&link)
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
        assert!(read_capped(vec![b'x'; 9].as_slice(), 8).is_err());
    }
}
