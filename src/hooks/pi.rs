//! Pi Coding Agent hook handlers — argv-based lifecycle plus TypeScript plugin.

use std::time::Instant;

use serde_json::Value;

use crate::bootstrap;
use crate::db::HcomDb;
use crate::instance_binding;
use crate::instance_lifecycle as lifecycle;
use crate::instances;
use crate::log::{log_error, log_info};
use crate::shared::ST_LISTENING;
use crate::shared::context::HcomContext;

use super::common;
use super::common::finalize_session;

fn parse_flag(argv: &[String], flag: &str) -> Option<String> {
    argv.iter()
        .position(|a| a == flag)
        .and_then(|i| argv.get(i + 1))
        .cloned()
}

fn has_flag(argv: &[String], flag: &str) -> bool {
    argv.iter().any(|a| a == flag)
}
fn parse_endpoint_epoch(argv: &[String]) -> Result<Option<String>, String> {
    let Some(raw) = parse_flag(argv, "--endpoint-epoch") else {
        return Ok(None);
    };
    if raw.len() != 36 || raw.len() > 64 {
        return Err("--endpoint-epoch must be a 36-character UUID".to_string());
    }
    let parsed = uuid::Uuid::parse_str(&raw)
        .map_err(|_| "--endpoint-epoch must be a valid UUID".to_string())?;
    Ok(Some(parsed.to_string()))
}

fn verified_hook_name(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> Result<String, String> {
    let process_id = ctx
        .process_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "HCOM_PROCESS_ID not set".to_string())?;
    let bound_name = db
        .get_process_binding(process_id)
        .map_err(|error| format!("process binding lookup failed: {error}"))?
        .ok_or_else(|| "No instance bound to this process".to_string())?;
    if db
        .get_instance_full(&bound_name)
        .map_err(|error| format!("instance lookup failed: {error}"))?
        .is_none()
    {
        return Err("Verified process binding has no live instance".to_string());
    }
    if let Some(asserted_name) = parse_flag(argv, "--name") {
        let asserted = crate::identity::resolve_from_name(db, &asserted_name)
            .map_err(|error| error.to_string())?;
        if asserted.name != bound_name {
            return Err(format!(
                "Explicit --name '{}' conflicts with verified Pi actor '{}'",
                asserted_name, bound_name
            ));
        }
    }
    Ok(bound_name)
}

fn upsert_plugin_notify_endpoint(db: &HcomDb, instance_name: &str, port: u16) {
    if let Err(e) = db.upsert_notify_endpoint(instance_name, "plugin", port) {
        log_error(
            "native",
            "pi.register_notify_fail",
            &format!(
                "Failed to register plugin notify port for {}: {}",
                instance_name, e
            ),
        );
        return;
    }

    // Pi launch readiness is authoritative on this plugin bind. Wake the PTY
    // delivery loop immediately instead of waiting for its periodic poll.
    crate::notify::wake(db, instance_name, crate::notify::WakeKind::DELIVERY_LOOPS);
}

fn initialize_last_event_id(db: &HcomDb, instance_name: &str) {
    if let Ok(Some(existing)) = db.get_instance_full(instance_name)
        && existing.last_event_id == 0
    {
        let launch_event_id: Option<i64> = std::env::var("HCOM_LAUNCH_EVENT_ID")
            .ok()
            .and_then(|s| s.parse().ok());
        let current_max = db.get_last_event_id();
        let new_id = match launch_event_id {
            Some(lei) if lei <= current_max => lei,
            _ => current_max,
        };
        let mut updates = serde_json::Map::new();
        updates.insert("last_event_id".into(), serde_json::json!(new_id));
        instances::update_instance_position(db, instance_name, &updates);
    }
}

fn bootstrap_for(ctx: &HcomContext, db: &HcomDb, instance_name: &str) -> String {
    let tag = db
        .get_instance_full(instance_name)
        .ok()
        .flatten()
        .and_then(|d| d.tag.clone())
        .unwrap_or_default();
    let hcom_config = crate::config::HcomConfig::load(None).unwrap_or_default();
    let relay_enabled = crate::relay::is_relay_enabled(&hcom_config);
    let effective_tag = if tag.is_empty() {
        &hcom_config.tag
    } else {
        &tag
    };
    bootstrap::get_bootstrap(
        db,
        &ctx.hcom_dir,
        instance_name,
        "pi",
        ctx.is_background,
        ctx.is_launched,
        &ctx.notes,
        effective_tag,
        relay_enabled,
        ctx.background_name.as_deref(),
    )
}

fn handle_start(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    // Plugin RPC returns JSON errors on exit 0 so the extension can handle
    // setup failures without Pi treating the hook itself as failed.
    let session_id = match parse_flag(argv, "--session-id") {
        Some(sid) => sid,
        None => return (0, r#"{"error":"Missing --session-id"}"#.to_string()),
    };
    let transcript_path = parse_flag(argv, "--transcript-path");
    let cwd = parse_flag(argv, "--cwd");
    let notify_port: Option<u16> = parse_flag(argv, "--notify-port").and_then(|s| s.parse().ok());
    let endpoint_epoch = match parse_endpoint_epoch(argv) {
        Ok(epoch) => epoch,
        Err(error) => return (0, serde_json::json!({"error": error}).to_string()),
    };

    let process_id = match &ctx.process_id {
        Some(pid) => pid.clone(),
        None => return (0, r#"{"error":"HCOM_PROCESS_ID not set"}"#.to_string()),
    };

    let instance_name =
        match instance_binding::bind_session_to_process(db, &session_id, Some(&process_id)) {
            Some(name) => name,
            None => {
                return (
                    0,
                    r#"{"error":"No instance bound to this process"}"#.to_string(),
                );
            }
        };
    if let Some(asserted_name) = parse_flag(argv, "--name") {
        let asserted = match crate::identity::resolve_from_name(db, &asserted_name) {
            Ok(asserted) => asserted,
            Err(error) => {
                return (
                    0,
                    serde_json::json!({"error": error.to_string()}).to_string(),
                );
            }
        };
        if asserted.name != instance_name {
            return (
                0,
                serde_json::json!({
                    "error": format!(
                        "Explicit --name '{}' conflicts with verified Pi actor '{}'",
                        asserted_name, instance_name
                    )
                })
                .to_string(),
            );
        }
    }

    initialize_last_event_id(db, &instance_name);
    lifecycle::set_status(
        db,
        &instance_name,
        ST_LISTENING,
        "start",
        Default::default(),
    );
    instance_binding::capture_and_store_launch_context(db, &instance_name);

    let mut updates = serde_json::Map::new();
    updates.insert("tool".into(), serde_json::json!("pi"));
    updates.insert("session_id".into(), serde_json::json!(&session_id));
    if let Some(epoch) = endpoint_epoch.as_ref() {
        updates.insert("endpoint_epoch".into(), serde_json::json!(epoch));
    }
    if let Some(path) = transcript_path.as_ref().filter(|p| !p.is_empty()) {
        updates.insert("transcript_path".into(), serde_json::json!(path));
    }
    let cwd_value = cwd
        .as_deref()
        .filter(|p| !p.is_empty())
        .or_else(|| ctx.cwd.to_str());
    if let Some(cwd) = cwd_value {
        updates.insert("directory".into(), serde_json::json!(cwd));
    }
    instances::update_instance_position(db, &instance_name, &updates);
    let effective_endpoint_epoch = db
        .get_instance_full(&instance_name)
        .ok()
        .flatten()
        .map(|instance| instance.endpoint_epoch)
        .unwrap_or_default();
    upsert_plugin_notify_endpoint(db, &instance_name, notify_port.unwrap_or(0));
    log_info(
        "hooks",
        "pi-start.bind",
        &format!("instance={} session_id={}", instance_name, session_id),
    );
    crate::relay::worker::ensure_worker(true);

    let response = serde_json::json!({
        "name": instance_name,
        "session_id": session_id,
        "endpoint_epoch": effective_endpoint_epoch,
        "bootstrap": bootstrap_for(ctx, db, &instance_name),
    });
    (0, response.to_string())
}

fn handle_status(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match verified_hook_name(ctx, db, argv) {
        Ok(name) => name,
        Err(error) => return (1, serde_json::json!({"error": error}).to_string()),
    };
    let status = match parse_flag(argv, "--status") {
        Some(s) => s,
        None => return (0, r#"{"error":"Missing --name or --status"}"#.to_string()),
    };
    let context = parse_flag(argv, "--context").unwrap_or_default();
    let detail = parse_flag(argv, "--detail").unwrap_or_default();
    if let Some(presence) = parse_flag(argv, "--presence")
        && let Err(error) = instances::update_instance_presence(db, &name, &presence)
    {
        return (1, serde_json::json!({"error": error}).to_string());
    }
    let was_listening = db
        .get_instance_full(&name)
        .ok()
        .flatten()
        .is_some_and(|inst| inst.status == ST_LISTENING);

    lifecycle::set_status(
        db,
        &name,
        &status,
        &context,
        lifecycle::StatusUpdate {
            detail: &detail,
            ..Default::default()
        },
    );
    if status == ST_LISTENING && !was_listening {
        crate::notify::wake(db, &name, &[]);
    }
    (0, r#"{"ok":true}"#.to_string())
}

fn handle_read(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match verified_hook_name(ctx, db, argv) {
        Ok(name) => name,
        Err(error) => return (1, serde_json::json!({"error": error}).to_string()),
    };
    let format_mode = has_flag(argv, "--format");
    let check_mode = has_flag(argv, "--check");
    let ack_mode = has_flag(argv, "--ack");
    if ack_mode {
        let (ack_id, legacy_count) = if let Some(up_to) = parse_flag(argv, "--up-to") {
            let Ok(ack_id) = up_to.parse::<i64>() else {
                return (
                    1,
                    serde_json::json!({"error": format!("Invalid --up-to: {}", up_to)}).to_string(),
                );
            };
            (ack_id, None)
        } else {
            let unread = db.get_unread_messages(&name);
            if unread.is_empty() {
                return (0, r#"{"acked":0}"#.to_string());
            }
            let ack_id = unread
                .iter()
                .filter_map(|message| message.event_id)
                .max()
                .filter(|id| *id > 0)
                .unwrap_or_else(|| db.get_last_event_id());
            (ack_id, Some(unread.len()))
        };
        match db.acknowledge_inbox_and_advance_cursor(&name, ack_id) {
            Ok(receipt_transitions) => {
                return (
                    0,
                    serde_json::json!({
                        "acked_to": ack_id,
                        "acked": legacy_count,
                        "receipt_transitions": receipt_transitions,
                    })
                    .to_string(),
                );
            }
            Err(error) => {
                return (
                    1,
                    serde_json::json!({"error": format!("Pi ACK commit failed: {error}")})
                        .to_string(),
                );
            }
        }
    }

    let raw_messages = db.get_pi_unresolved_messages(&name);
    let projected_ids: Vec<String> = raw_messages
        .iter()
        .filter_map(|message| message.message_id.clone())
        .collect();
    let claimed = match db.claim_inbox_received(&name, &projected_ids) {
        Ok(claimed) => claimed,
        Err(error) => {
            return (
                1,
                serde_json::json!({"error": format!("Inbox claim failed: {error}")}).to_string(),
            );
        }
    };
    let messages: Vec<Value> = raw_messages
        .iter()
        .filter(|message| {
            message.message_id.as_ref().is_none_or(|message_id| {
                claimed.contains(message_id)
                    || db.projected_inbox_delivery(message_id, &name).is_none()
            })
        })
        .map(common::message_to_value)
        .collect();

    if format_mode {
        if messages.is_empty() {
            return (0, String::new());
        }
        let deliver = common::limit_delivery_messages(&messages);
        return (
            0,
            common::format_messages_json_for_instance(db, &deliver, &name),
        );
    }
    if check_mode {
        return (
            0,
            if messages.is_empty() { "false" } else { "true" }.to_string(),
        );
    }
    (
        0,
        serde_json::to_string(&messages).unwrap_or_else(|_| "[]".to_string()),
    )
}

fn handle_beforetool(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match verified_hook_name(ctx, db, argv) {
        Ok(name) => name,
        Err(error) => return (1, serde_json::json!({"error": error}).to_string()),
    };
    let tool_name = parse_flag(argv, "--tool").unwrap_or_default();
    let input = parse_flag(argv, "--input-json")
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .unwrap_or_else(|| serde_json::json!({}));
    if !tool_name.is_empty() {
        common::update_tool_status(db, &name, "pi", &tool_name, &input);
    }
    (0, r#"{"decision":"allow"}"#.to_string())
}

fn handle_stop(ctx: &HcomContext, db: &HcomDb, argv: &[String]) -> (i32, String) {
    let name = match verified_hook_name(ctx, db, argv) {
        Ok(name) => name,
        Err(error) => return (1, serde_json::json!({"error": error}).to_string()),
    };
    let reason = parse_flag(argv, "--reason").unwrap_or_else(|| "unknown".to_string());
    finalize_session(db, &name, &reason, None);
    (0, r#"{"ok":true}"#.to_string())
}

pub fn dispatch_pi_hook(hook_name: &str, argv: &[String]) -> (i32, String) {
    let start = Instant::now();
    let ctx = HcomContext::from_os();
    crate::paths::ensure_hcom_directories_at(&ctx.hcom_dir);
    let db = match HcomDb::open() {
        Ok(db) => db,
        Err(e) => {
            log_error(
                "hooks",
                "hook.error",
                &format!("hook={} op=db_open err={}", hook_name, e),
            );
            return (
                0,
                serde_json::json!({"error": format!("DB open failed: {}", e)}).to_string(),
            );
        }
    };
    if !common::hook_gate_check(&ctx, &db) {
        return (0, String::new());
    }
    let handler_argv: Vec<String> = if !argv.is_empty() && argv[0] == hook_name {
        argv[1..].to_vec()
    } else {
        argv.to_vec()
    };
    let hook_name_owned = hook_name.to_string();
    let handler_start = Instant::now();
    let (exit_code, output) = common::dispatch_with_panic_guard(
        "pi",
        &hook_name_owned,
        (
            0,
            serde_json::json!({"error": "internal panic"}).to_string(),
        ),
        || match hook_name_owned.as_str() {
            "pi-start" => handle_start(&ctx, &db, &handler_argv),
            "pi-status" => handle_status(&ctx, &db, &handler_argv),
            "pi-read" => handle_read(&ctx, &db, &handler_argv),
            "pi-beforetool" => handle_beforetool(&ctx, &db, &handler_argv),
            "pi-stop" => handle_stop(&ctx, &db, &handler_argv),
            _ => (
                0,
                serde_json::json!({"error": format!("Unknown Pi hook: {}", hook_name_owned)})
                    .to_string(),
            ),
        },
    );
    log_info(
        "hooks",
        "pi.dispatch.timing",
        &format!(
            "hook={} handler_ms={:.2} total_ms={:.2} exit_code={}",
            hook_name,
            handler_start.elapsed().as_secs_f64() * 1000.0,
            start.elapsed().as_secs_f64() * 1000.0,
            exit_code
        ),
    );
    (exit_code, output)
}

pub const PLUGIN_SOURCE: &str = include_str!("../pi_plugin/hcom.ts");
const PLUGIN_FILENAME: &str = "hcom.ts";

fn current_home_dir() -> std::path::PathBuf {
    std::env::var("HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| dirs::home_dir().unwrap_or_default())
}

fn pi_plugin_dir() -> std::path::PathBuf {
    let tool_root = crate::runtime_env::tool_config_root();
    let home = current_home_dir();
    if tool_root == home {
        if let Ok(dir) = std::env::var("PI_CODING_AGENT_DIR")
            && !dir.is_empty()
        {
            return std::path::PathBuf::from(dir).join("extensions");
        }
        home.join(".pi").join("agent").join("extensions")
    } else {
        tool_root.join(".pi").join("extensions")
    }
}

pub fn get_pi_plugin_path() -> std::path::PathBuf {
    pi_plugin_dir().join(PLUGIN_FILENAME)
}

fn plugin_matches_source(path: &std::path::Path) -> bool {
    match std::fs::read_to_string(path) {
        Ok(content) => content == PLUGIN_SOURCE,
        Err(_) => false,
    }
}

pub fn verify_pi_plugin_installed() -> bool {
    plugin_matches_source(&get_pi_plugin_path())
}

pub fn install_pi_plugin() -> std::io::Result<bool> {
    let target_dir = pi_plugin_dir();
    let target = target_dir.join(PLUGIN_FILENAME);
    std::fs::create_dir_all(&target_dir)?;
    if target.is_symlink() || target.exists() {
        std::fs::remove_file(&target)?;
    }
    std::fs::write(&target, PLUGIN_SOURCE)?;
    Ok(true)
}

pub fn ensure_pi_plugin_installed() -> bool {
    if verify_pi_plugin_installed() {
        return true;
    }
    install_pi_plugin().unwrap_or(false)
}

pub fn remove_pi_plugin() -> std::io::Result<()> {
    let path = get_pi_plugin_path();
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::{ST_ACTIVE, ST_LISTENING};
    use std::net::TcpListener;
    use std::path::PathBuf;
    use std::time::Duration;

    fn setup_test_db() -> (HcomDb, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_pi_hooks_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path)
    }

    fn cleanup(path: PathBuf) {
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    fn save_test_instance(db: &HcomDb, name: &str, status: &str) {
        let mut row = serde_json::Map::new();
        row.insert("name".into(), serde_json::json!(name));
        row.insert("tool".into(), serde_json::json!("pi"));
        row.insert("status".into(), serde_json::json!(status));
        row.insert("status_context".into(), serde_json::json!(""));
        row.insert("status_detail".into(), serde_json::json!(""));
        row.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named(name, &row).unwrap();
    }
    fn bound_pi_context(db: &HcomDb, name: &str, process_id: &str) -> HcomContext {
        db.conn()
            .execute(
                "INSERT INTO process_bindings (process_id, instance_name, updated_at)
                 VALUES (?1, ?2, 1.0)",
                rusqlite::params![process_id, name],
            )
            .unwrap();
        let env = std::collections::HashMap::from([(
            "HCOM_PROCESS_ID".to_string(),
            process_id.to_string(),
        )]);
        HcomContext::from_env(&env, std::env::temp_dir())
    }

    #[test]
    fn plugin_bootstraps_via_hidden_message() {
        assert!(PLUGIN_SOURCE.contains("before_agent_start"));
        assert!(PLUGIN_SOURCE.contains("customType: \"hcom-bootstrap\""));
        assert!(PLUGIN_SOURCE.contains("display: false"));
        assert!(!PLUGIN_SOURCE.contains("text: `${bootstrapText}\\n\\n${event.text}`"));
    }

    #[test]
    fn plugin_skips_pi_subagent_child_sessions_before_registering_handlers() {
        assert!(PLUGIN_SOURCE.contains("Symbol.for(\"pi-subagents:child-session-context\")"));
        assert!(PLUGIN_SOURCE.contains("if (isPiSubagentChildSession())"));
        assert!(PLUGIN_SOURCE.contains("plugin.bind_skipped_nested"));
        let guard = PLUGIN_SOURCE
            .find("if (isPiSubagentChildSession())")
            .unwrap();
        let first_handler = PLUGIN_SOURCE.find("pi.on(\"session_start\"").unwrap();
        assert!(guard < first_handler);
    }

    #[test]
    fn plugin_reconcile_keeps_idle_workers_alive_without_active_polling() {
        assert!(!PLUGIN_SOURCE.contains(
            "reportStatus(currentCtx, currentCtx.isIdle() ? \"listening\" : \"active\")"
        ));
        assert!(PLUGIN_SOURCE.contains("pi.on(\"agent_end\""));
        assert!(PLUGIN_SOURCE.contains("IDLE_DEBOUNCE_MS"));
        assert!(PLUGIN_SOURCE.contains("LISTENING_HEARTBEAT_MS = 5_000"));
        assert!(PLUGIN_SOURCE.contains("10s no-TCP stale threshold"));
        assert!(PLUGIN_SOURCE.contains("lastListeningHeartbeatAt"));
        assert!(PLUGIN_SOURCE.contains("heartbeatDue"));
        assert!(PLUGIN_SOURCE.contains("currentCtx?.isIdle()"));
        assert!(!PLUGIN_SOURCE.contains("pi.on(\"turn_end\", async (_event, ctx) => {\n\t\tcurrentCtx = ctx;\n\t\tawait reportStatus(ctx, \"listening\");"));
    }

    #[test]
    fn plugin_preserves_identity_across_in_process_session_replacement() {
        assert!(
            PLUGIN_SOURCE.contains("[\"reload\", \"new\", \"resume\", \"fork\"].includes(reason)")
        );
        assert!(PLUGIN_SOURCE.contains("if (instanceName && !replacingSession)"));
        assert!(PLUGIN_SOURCE.contains("plugin.session_replacement"));
        assert!(
            PLUGIN_SOURCE.contains(
                "await hcom([\"pi-stop\", \"--name\", instanceName, \"--reason\", reason])"
            )
        );
    }

    #[test]
    fn plugin_cleans_reconcile_timer_between_session_runtimes() {
        assert!(PLUGIN_SOURCE.contains("function stopReconcileTimer(): void"));
        assert!(PLUGIN_SOURCE.contains("clearInterval(reconcileTimer)"));
        assert!(
            PLUGIN_SOURCE.contains("function resetBinding(): void {\n\t\tstopReconcileTimer();")
        );
    }

    #[test]
    fn plugin_delivery_reports_active_edge() {
        assert!(PLUGIN_SOURCE.contains("reportStatus(ctx, \"active\""));
        assert!(PLUGIN_SOURCE.contains("`deliver:${sender}`"));
    }

    #[test]
    fn plugin_reports_ui_prompt_waiting_without_status_races() {
        assert!(PLUGIN_SOURCE.contains("pi.on(\"ui_prompt_start\""));
        assert!(PLUGIN_SOURCE.contains("pi.on(\"ui_prompt_end\""));
        assert!(
            PLUGIN_SOURCE.contains("reportStatus(ctx, \"blocked\", \"ui_prompt\", event.kind)")
        );
        assert!(PLUGIN_SOURCE.contains("epoch !== uiPromptEpoch || !uiPromptActive"));
        assert!(PLUGIN_SOURCE.contains("if (ctx.isIdle() && !uiPromptActive)"));
        assert!(PLUGIN_SOURCE.contains("if (!uiPromptActive) await pollPendingIfDue(ctx)"));
        assert!(PLUGIN_SOURCE.contains("await deliverPending(ctx)"));
    }

    #[test]
    fn plugin_waits_for_real_prompt_acceptance_before_ack() {
        assert!(!PLUGIN_SOURCE.contains("ctx.ui.notify(formatted)"));
        assert!(PLUGIN_SOURCE.contains("onAccepted: resolve"));
        assert!(PLUGIN_SOURCE.contains("if (!accepted)"));
        assert!(PLUGIN_SOURCE.contains("rememberDelivered(pending.messages)"));
        assert!(PLUGIN_SOURCE.contains("await ackPending(idle ?"));
        assert!(!PLUGIN_SOURCE.contains("await pi.sendUserMessage(formatted)"));
        assert!(
            !PLUGIN_SOURCE.contains("event.source === \"extension\") {\n\t\t\tawait ackPending")
        );
    }

    #[test]
    fn plugin_persists_delivery_ids_across_reload() {
        assert!(PLUGIN_SOURCE.contains("pi-delivery"));
        assert!(PLUGIN_SOURCE.contains("closed-workers.json"));
        assert!(PLUGIN_SOURCE.contains("isClosedSupervisionWorker"));
        assert!(PLUGIN_SOURCE.contains("deliveredMessageIds"));
        assert!(PLUGIN_SOURCE.contains("loadDeliveryLedger("));
        assert!(PLUGIN_SOURCE.contains("ctx.sessionManager.getEntries()"));
        assert!(PLUGIN_SOURCE.contains("collectDeliveredIdsFromMessage"));
        assert!(PLUGIN_SOURCE.contains("ENOENT is a normal fresh-session state"));
        assert!(PLUGIN_SOURCE.contains("rememberDelivered(pending.messages)"));
        assert!(PLUGIN_SOURCE.contains("!deliveredMessageIds.has(Number(m.event_id))"));
        assert!(PLUGIN_SOURCE.contains("--ack\", \"--up-to"));
    }

    #[test]
    fn plugin_keeps_ack_gate_until_command_succeeds() {
        let idx = PLUGIN_SOURCE
            .find("async function ackPending")
            .expect("ackPending present");
        let ack = &PLUGIN_SOURCE[idx..];
        let command = ack.find("const result = await hcom").expect("ack command");
        let clear = ack.find("pendingAckId = null").expect("pending ack clear");
        assert!(command < clear);
        assert!(ack.contains("if (result.code !== 0)"));
        assert!(ack.contains("plugin.deferred_ack_failed"));
        assert!(PLUGIN_SOURCE.contains("await ackPending(\"reconcile\")"));
    }

    #[test]
    fn status_handler_wakes_plugin_only_when_entering_listening() {
        let (db, path) = setup_test_db();
        save_test_instance(&db, "luna", ST_LISTENING);
        let ctx = bound_pi_context(&db, "luna", "pi-status-test");
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        db.upsert_notify_endpoint("luna", "plugin", port).unwrap();

        let argv = vec![
            "--name".to_string(),
            "luna".to_string(),
            "--status".to_string(),
            ST_LISTENING.to_string(),
        ];
        let (code, _) = handle_status(&ctx, &db, &argv);
        assert_eq!(code, 0);
        std::thread::sleep(Duration::from_millis(20));
        assert!(listener.accept().is_err());

        let mut updates = serde_json::Map::new();
        updates.insert("status".into(), serde_json::json!(ST_ACTIVE));
        instances::update_instance_position(&db, "luna", &updates);

        let (code, _) = handle_status(&ctx, &db, &argv);
        assert_eq!(code, 0);
        let mut accepted = false;
        for _ in 0..10 {
            if listener.accept().is_ok() {
                accepted = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(accepted);

        cleanup(path);
    }
    #[test]
    fn pi_status_piggybacks_bounded_rich_presence() {
        let (db, path) = setup_test_db();
        save_test_instance(&db, "luna", ST_LISTENING);
        let ctx = bound_pi_context(&db, "luna", "pi-presence-test");
        let presence = serde_json::json!({
            "provider": "openai-codex",
            "model": "gpt-5.6-sol",
            "endpointEpoch": "00000000-0000-4000-8000-000000000000"
        })
        .to_string();
        let argv = vec![
            "--name".to_string(),
            "luna".to_string(),
            "--status".to_string(),
            ST_LISTENING.to_string(),
            "--presence".to_string(),
            presence.clone(),
        ];
        assert_eq!(handle_status(&ctx, &db, &argv).0, 0);
        assert_eq!(
            db.get_instance_full("luna").unwrap().unwrap().presence_json,
            presence
        );
        cleanup(path);
    }

    #[test]
    fn pi_plugin_exposes_native_message_lifecycle_contract() {
        assert!(PLUGIN_SOURCE.contains("const endpointEpoch = randomUUID()"));
        assert!(PLUGIN_SOURCE.contains("--endpoint-epoch"));
        assert!(PLUGIN_SOURCE.contains("pi.registerTool({"));
        assert!(PLUGIN_SOURCE.contains("name: \"hcom\""));
        assert!(PLUGIN_SOURCE.contains("Use hcom when"));
        assert!(PLUGIN_SOURCE.contains("Only one active HCOM ask is allowed"));
        assert!(PLUGIN_SOURCE.contains("\"wait\""));
        assert!(PLUGIN_SOURCE.contains("\"cancel\""));
        assert!(PLUGIN_SOURCE.contains("pi.registerCommand(\"hcom\""));
        assert!(PLUGIN_SOURCE.contains("pi.registerShortcut(\"alt+m\""));
        assert!(PLUGIN_SOURCE.contains("CURSOR_MARKER"));
        assert!(PLUGIN_SOURCE.contains("requestRender()"));
        assert!(PLUGIN_SOURCE.contains("plugin.ui_overlay_failed"));
        assert!(PLUGIN_SOURCE.contains("stale HCOM session generation"));
        assert!(PLUGIN_SOURCE.contains("child.kill(\"SIGTERM\")"));
        assert!(PLUGIN_SOURCE.contains("child.kill(\"SIGKILL\")"));
        assert!(PLUGIN_SOURCE.contains("pi-rich-presence-v1"));
        assert!(PLUGIN_SOURCE.contains("message_id"));
        assert!(PLUGIN_SOURCE.contains("attachment?.sha256"));
    }

    #[test]
    fn pi_plugin_child_bridge_is_scoped_and_parent_only() {
        assert!(PLUGIN_SOURCE.contains("pi-subagents:supervisor-bridge-context"));
        assert!(PLUGIN_SOURCE.contains("pi-subagents:supervisor-bridge-provider"));
        assert!(PLUGIN_SOURCE.contains("name: \"contact_supervisor\""));
        assert!(PLUGIN_SOURCE.contains("progress_update"));
        assert!(PLUGIN_SOURCE.contains("need_decision"));
        assert!(PLUGIN_SOURCE.contains("interview_request"));
        assert!(PLUGIN_SOURCE.contains("Only one active HCOM ask is allowed"));
        assert!(PLUGIN_SOURCE.contains("bridge.expiresAt <= Date.now()"));
        assert!(PLUGIN_SOURCE.contains("runtimeGeneration === expectedGeneration"));
        assert!(PLUGIN_SOURCE.contains("Parent HCOM session changed"));
        assert!(
            PLUGIN_SOURCE
                .contains("never ask for or infer a target agent name or capability token")
        );
        assert!(!PLUGIN_SOURCE.contains("target_name: seed"));
    }

    #[test]
    fn pi_lifecycle_name_is_only_a_consistency_assertion() {
        let (db, path) = setup_test_db();
        save_test_instance(&db, "alice", ST_LISTENING);
        save_test_instance(&db, "bob", ST_LISTENING);
        let ctx = bound_pi_context(&db, "alice", "pi-alice");

        let read = vec!["--name".to_string(), "bob".to_string()];
        assert_eq!(handle_read(&ctx, &db, &read).0, 1);
        let status = vec![
            "--name".to_string(),
            "bob".to_string(),
            "--status".to_string(),
            ST_LISTENING.to_string(),
        ];
        assert_eq!(handle_status(&ctx, &db, &status).0, 1);
        let beforetool = vec![
            "--name".to_string(),
            "bob".to_string(),
            "--tool".to_string(),
            "bash".to_string(),
        ];
        assert_eq!(handle_beforetool(&ctx, &db, &beforetool).0, 1);
        let stop = vec!["--name".to_string(), "bob".to_string()];
        assert_eq!(handle_stop(&ctx, &db, &stop).0, 1);
        assert_eq!(
            db.get_instance_full("bob").unwrap().unwrap().status,
            ST_LISTENING
        );
        cleanup(path);
    }
    #[test]
    fn pi_fetch_claims_received_and_ack_failure_keeps_cursor_and_state() {
        let (db, path) = setup_test_db();
        save_test_instance(&db, "alice", ST_LISTENING);
        save_test_instance(&db, "bob", ST_LISTENING);
        db.conn()
            .execute(
                "UPDATE instances SET endpoint_epoch = 'epoch-alice' WHERE name = 'alice'",
                [],
            )
            .unwrap();
        let ctx = bound_pi_context(&db, "alice", "pi-ack-alice");
        let recipients = vec!["alice".to_string()];
        let data = serde_json::json!({
            "protocol": crate::db::MESSAGE_PROTOCOL_V1,
            "message_id": "pi-atomic-ack",
            "correlation_id": "pi-atomic-ack",
            "from": "bob",
            "sender_kind": "instance",
            "scope": "mentions",
            "mentions": recipients,
            "delivered_to": recipients,
            "text": "claim me",
            "intent": "inform",
            "expects_reply": false,
            "delivery_endpoint": "inbox",
        });
        let persisted = db
            .persist_message_v1(&crate::db::MessagePersistInput {
                routing_instance: "bob",
                sender_name: "bob",
                sender_session_id: None,
                data: &data,
                message_id: "pi-atomic-ack",
                correlation_id: "pi-atomic-ack",
                intent: Some("inform"),
                expects_reply: false,
                in_reply_to: None,
                legacy_reply_to: None,
                reply_endpoint: None,
                delivery_endpoint: "inbox",
                supersedes: None,
                retry_of: None,
                attachments_json: "[]",
                recipients: &recipients,
            })
            .unwrap();

        let fetch = vec!["--name".to_string(), "alice".to_string()];
        let (fetch_code, fetch_output) = handle_read(&ctx, &db, &fetch);
        assert_eq!(fetch_code, 0);
        assert!(fetch_output.contains("pi-atomic-ack"));
        assert_eq!(
            db.inspect_message_v1("pi-atomic-ack").unwrap()["deliveries"][0]["state"],
            "received"
        );
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_pi_hook_ack
                 BEFORE UPDATE OF state ON message_deliveries
                 WHEN OLD.message_id = 'pi-atomic-ack' AND NEW.state = 'accepted'
                 BEGIN SELECT RAISE(ABORT, 'forced hook ack failure'); END;",
            )
            .unwrap();
        let ack = vec![
            "--name".to_string(),
            "alice".to_string(),
            "--ack".to_string(),
            "--up-to".to_string(),
            persisted.event_id.to_string(),
        ];
        let (failed_code, failed_output) = handle_read(&ctx, &db, &ack);
        assert_eq!(failed_code, 1, "{failed_output}");
        assert!(failed_output.contains("Pi ACK commit failed"));
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .last_event_id,
            0
        );
        assert_eq!(
            db.inspect_message_v1("pi-atomic-ack").unwrap()["deliveries"][0]["state"],
            "received"
        );

        db.conn()
            .execute_batch("DROP TRIGGER fail_pi_hook_ack;")
            .unwrap();
        let (success_code, success_output) = handle_read(&ctx, &db, &ack);
        assert_eq!(success_code, 0, "{success_output}");
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .last_event_id,
            persisted.event_id
        );
        assert_eq!(
            db.inspect_message_v1("pi-atomic-ack").unwrap()["deliveries"][0]["state"],
            "acknowledged"
        );
        cleanup(path);
    }

    #[test]
    fn pi_start_endpoint_epoch_retries_are_idempotent_and_explicit_changes_rotate() {
        let (db, path) = setup_test_db();
        save_test_instance(&db, "alice", ST_LISTENING);
        let ctx = bound_pi_context(&db, "alice", "pi-endpoint");
        let first_epoch = "11111111-1111-4111-8111-111111111111";
        let second_epoch = "22222222-2222-4222-8222-222222222222";
        let invoke = |epoch: &str| {
            handle_start(
                &ctx,
                &db,
                &[
                    "--session-id".to_string(),
                    "pi-session".to_string(),
                    "--endpoint-epoch".to_string(),
                    epoch.to_string(),
                ],
            )
        };

        for _ in 0..2 {
            let (code, output) = invoke(first_epoch);
            assert_eq!(code, 0);
            let response: Value = serde_json::from_str(&output).unwrap();
            assert_eq!(response["endpoint_epoch"], first_epoch);
            assert_eq!(
                db.get_instance_full("alice")
                    .unwrap()
                    .unwrap()
                    .endpoint_epoch,
                first_epoch
            );
        }
        let plugin_port: i64 = db
            .conn()
            .query_row(
                "SELECT port FROM notify_endpoints WHERE instance = 'alice' AND kind = 'plugin'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            plugin_port, 0,
            "pi-start without TCP notify must still mark plugin readiness"
        );

        let (_, output) = invoke(second_epoch);
        let response: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(response["endpoint_epoch"], second_epoch);
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .endpoint_epoch,
            second_epoch
        );
        let (_, invalid) = invoke("not-a-uuid");
        assert!(invalid.contains("36-character UUID"));
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .endpoint_epoch,
            second_epoch
        );
        cleanup(path);
    }
    #[test]
    fn start_handler_restores_stopped_identity_after_resume_deleted_placeholder() {
        let (db, path) = setup_test_db();
        let temp = tempfile::TempDir::new().unwrap();

        let mut canonical = serde_json::Map::new();
        canonical.insert("name".into(), serde_json::json!("gona"));
        canonical.insert("tool".into(), serde_json::json!("pi"));
        canonical.insert("session_id".into(), serde_json::json!("sid-existing"));
        canonical.insert("status".into(), serde_json::json!(ST_LISTENING));
        canonical.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named("gona", &canonical).unwrap();
        db.log_life_event(
            "gona",
            "stopped",
            "test",
            "exit:quit",
            Some(serde_json::json!({ "session_id": "sid-existing", "tool": "pi" })),
        )
        .unwrap();
        db.delete_instance("gona").unwrap();

        let mut placeholder = serde_json::Map::new();
        placeholder.insert("name".into(), serde_json::json!("zora"));
        placeholder.insert("tool".into(), serde_json::json!("pi"));
        placeholder.insert("session_id".into(), serde_json::json!("sid-temporary"));
        placeholder.insert("status".into(), serde_json::json!(ST_LISTENING));
        placeholder.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named("zora", &placeholder).unwrap();
        db.set_process_binding("pid-resume", "sid-temporary", "zora")
            .unwrap();
        db.delete_instance("zora").unwrap();

        let env = std::collections::HashMap::from([
            ("HCOM_PROCESS_ID".to_string(), "pid-resume".to_string()),
            ("HCOM_LAUNCHED".to_string(), "1".to_string()),
            ("HCOM_TOOL".to_string(), "pi".to_string()),
        ]);
        let ctx = HcomContext::from_env(&env, temp.path().to_path_buf());
        let (code, output) = handle_start(
            &ctx,
            &db,
            &[
                "--session-id".to_string(),
                "sid-existing".to_string(),
                "--cwd".to_string(),
                temp.path().to_string_lossy().to_string(),
            ],
        );

        assert_eq!(code, 0);
        let response: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(
            response.get("error"),
            None,
            "unexpected bind error: {output}"
        );
        assert_eq!(
            response.get("name").and_then(|value| value.as_str()),
            Some("gona")
        );
        assert_eq!(
            db.get_session_binding("sid-existing").unwrap().as_deref(),
            Some("gona")
        );
        assert_eq!(
            db.get_process_binding("pid-resume").unwrap().as_deref(),
            Some("gona")
        );
        assert_eq!(
            db.get_instance_full("gona")
                .unwrap()
                .unwrap()
                .session_id
                .as_deref(),
            Some("sid-existing")
        );

        cleanup(path);
    }

    #[test]
    fn start_handler_uses_central_binding_for_existing_session() {
        let (db, path) = setup_test_db();
        let temp = tempfile::TempDir::new().unwrap();

        let mut canonical = serde_json::Map::new();
        canonical.insert("name".into(), serde_json::json!("miso"));
        canonical.insert("tool".into(), serde_json::json!("pi"));
        canonical.insert("session_id".into(), serde_json::json!("sid-123"));
        canonical.insert("status".into(), serde_json::json!(ST_LISTENING));
        canonical.insert("status_context".into(), serde_json::json!(""));
        canonical.insert("status_detail".into(), serde_json::json!(""));
        canonical.insert("last_event_id".into(), serde_json::json!(42));
        canonical.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named("miso", &canonical).unwrap();
        db.rebind_session("sid-123", "miso").unwrap();

        let mut placeholder = serde_json::Map::new();
        placeholder.insert("name".into(), serde_json::json!("temp"));
        placeholder.insert("tool".into(), serde_json::json!("pi"));
        placeholder.insert("status".into(), serde_json::json!("pending"));
        placeholder.insert("status_context".into(), serde_json::json!("new"));
        placeholder.insert("status_detail".into(), serde_json::json!(""));
        placeholder.insert("created_at".into(), serde_json::json!(1.0));
        db.save_instance_named("temp", &placeholder).unwrap();
        db.set_process_binding("pid-123", "", "temp").unwrap();

        let env = std::collections::HashMap::from([
            ("HCOM_PROCESS_ID".to_string(), "pid-123".to_string()),
            ("HCOM_LAUNCHED".to_string(), "1".to_string()),
            ("HCOM_TOOL".to_string(), "pi".to_string()),
        ]);
        let ctx = HcomContext::from_env(&env, temp.path().to_path_buf());

        let (code, output) = handle_start(
            &ctx,
            &db,
            &[
                "--session-id".to_string(),
                "sid-123".to_string(),
                "--cwd".to_string(),
                temp.path().to_string_lossy().to_string(),
            ],
        );
        assert_eq!(code, 0);
        let response: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(response.get("name").and_then(|v| v.as_str()), Some("miso"));
        assert!(db.get_instance_full("temp").unwrap().is_none());
        assert_eq!(
            db.get_process_binding("pid-123").unwrap(),
            Some("miso".to_string())
        );

        let rebound = db.get_instance_full("miso").unwrap().unwrap();
        assert_eq!(rebound.last_event_id, 42);
        assert_eq!(rebound.directory, temp.path().to_string_lossy());

        cleanup(path);
    }
}
