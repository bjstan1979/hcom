//! Incoming message handling — route state vs control topics, apply remote state.
//!
//! Handles MQTT messages received from remote devices:
//! - State messages: upsert remote instances, import events
//! - Control messages: process stop/kill commands
//! - Authenticated null state: device gone (graceful cleanup)

use crate::db::HcomDb;
use crate::log;
use anyhow::Result;
use rusqlite::{OptionalExtension, params};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashSet};

use super::crypto;
use super::replay::ReplayGuard;
use super::{device_short_id_for_db, remember_device_short_id, safe_kv_get, safe_kv_set};

/// Crypto + replay context shared by all inbound message handlers.
pub struct InboundContext<'a> {
    pub psk: &'a [u8; 32],
    pub relay_id: &'a str,
    pub topic: &'a str,
    pub replay_guard: &'a mut ReplayGuard,
}

struct OpenedEnvelope {
    plaintext: Vec<u8>,
    ts_secs: u64,
}

enum ReplayPolicy {
    ControlFreshness,
    State { min_accepted_ts: Option<u64> },
}

fn state_ts_key(device_id: &str) -> String {
    format!("relay_state_ts_{}", device_id)
}

fn state_ts_watermark(db: &HcomDb, device_id: &str) -> Option<u64> {
    safe_kv_get(db, &state_ts_key(device_id)).and_then(|s| s.parse().ok())
}

fn record_state_ts_watermark(db: &HcomDb, device_id: &str, ts_secs: u64) {
    let current = state_ts_watermark(db, device_id).unwrap_or(0);
    if ts_secs > current {
        safe_kv_set(db, &state_ts_key(device_id), Some(&ts_secs.to_string()));
    }
}

/// Decrypt + replay-check an envelope coming off the wire. Returns the inner
/// JSON bytes ready for `serde_json::from_slice`. Errors are logged inline so
/// caller sites stay short.
fn open_envelope_for_handler(
    ctx: &mut InboundContext<'_>,
    sender_short: &str,
    payload: &[u8],
    replay_policy: ReplayPolicy,
) -> Option<OpenedEnvelope> {
    let parsed = match crypto::parse_envelope(payload) {
        Ok(p) => p,
        Err(e) => {
            log::log_warn("relay", "relay.bad_envelope", &format!("{}", e));
            return None;
        }
    };
    let plaintext = match crypto::open(ctx.psk, ctx.relay_id, ctx.topic, payload) {
        Ok(pt) => pt,
        Err(e) => {
            log::log_warn("relay", "relay.decrypt_fail", &format!("{}", e));
            return None;
        }
    };

    let now_secs = crate::shared::time::now_epoch_f64() as u64;
    let replay_result = match replay_policy {
        ReplayPolicy::ControlFreshness => {
            ctx.replay_guard
                .check(sender_short, parsed.nonce, parsed.ts_secs, now_secs)
        }
        ReplayPolicy::State { min_accepted_ts } => ctx.replay_guard.check_state(
            sender_short,
            parsed.nonce,
            parsed.ts_secs,
            now_secs,
            min_accepted_ts,
        ),
    };
    if let Err(e) = replay_result {
        log::log_warn("relay", "relay.replay", &format!("{}", e));
        return None;
    }
    if let Err(e) = ctx
        .replay_guard
        .record_nonce(sender_short, parsed.nonce, now_secs)
    {
        log::log_warn("relay", "relay.replay", &format!("{}", e));
        return None;
    }

    Some(OpenedEnvelope {
        plaintext,
        ts_secs: parsed.ts_secs,
    })
}

/// Handle an authenticated null state from a departing device.
/// Removes all instances belonging to the disconnected device.
pub fn handle_device_gone(db: &HcomDb, device_id: &str) {
    if let Err(e) = db.conn().execute(
        "DELETE FROM instances WHERE origin_device_id = ?",
        params![device_id],
    ) {
        log::log_error("relay", "relay.device_gone_err", &format!("{}", e));
        return;
    }
    let short_id = resolve_short_id(db, device_id);
    safe_kv_set(db, &format!("relay_sync_time_{}", device_id), None);
    safe_kv_set(db, &format!("relay_caps_{}", device_id), None);
    safe_kv_set(db, &format!("relay_ctrl_{}", device_id), None);
    safe_kv_set(db, &state_ts_key(device_id), None);
    if let Some(ref short) = short_id {
        safe_kv_set(db, &format!("relay_short_{}", short), None);
    }
    safe_kv_set(db, &format!("relay_uuid_short_{}", device_id), None);
    let prefix = super::device_id_prefix(device_id);
    let label = short_id.as_deref().unwrap_or(prefix);
    emit_device_event(
        db,
        super::ACTION_DEVICE_LEAVE,
        label,
        prefix,
        &format!("device {} left the relay", label),
        false,
    );
    log::log_info("relay", "relay.device_gone", &format!("device={}", prefix));
}

/// Handle a control message from the control topic.
pub fn handle_control_message(
    db: &HcomDb,
    payload: &[u8],
    own_device: &str,
    ctx: &mut InboundContext<'_>,
) -> bool {
    let opened =
        match open_envelope_for_handler(ctx, "control", payload, ReplayPolicy::ControlFreshness) {
            Some(p) => p,
            None => return false,
        };

    let data: Value = match serde_json::from_slice(&opened.plaintext) {
        Ok(v) => v,
        Err(e) => {
            log::log_warn("relay", "relay.bad_payload", &format!("{}", e));
            return false;
        }
    };

    let source_device = data
        .get("from_device")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    // Ignore own control messages
    if source_device == own_device {
        return false;
    }

    let own_short_id = device_short_id_for_db(db, own_device);
    let events = if let Some(arr) = data.get("events").and_then(|v| v.as_array()) {
        arr.clone()
    } else if data.get("type").and_then(|v| v.as_str()) == Some("control") {
        vec![data.clone()]
    } else {
        vec![]
    };

    super::control::handle_control_events(db, &events, &own_short_id, source_device)
}

/// Handle a state message from a remote device.
pub fn handle_state_message(
    db: &HcomDb,
    device_id: &str,
    payload: &[u8],
    own_device: &str,
    ctx: &mut InboundContext<'_>,
) -> bool {
    let t0 = std::time::Instant::now();

    let opened = match open_envelope_for_handler(
        ctx,
        device_id,
        payload,
        ReplayPolicy::State {
            min_accepted_ts: state_ts_watermark(db, device_id),
        },
    ) {
        Some(p) => p,
        None => return false,
    };

    let data: Value = match serde_json::from_slice(&opened.plaintext) {
        Ok(v) => v,
        Err(e) => {
            log::log_warn("relay", "relay.bad_payload", &format!("{}", e));
            return false;
        }
    };

    if data.get("state").is_some() && data["state"].is_null() {
        handle_device_gone(db, device_id);
        return false;
    }

    let state = data
        .get("state")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let events = data
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let short_id = state
        .get("short_id")
        .and_then(|v| v.as_str())
        .unwrap_or(&device_id[..4.min(device_id.len())])
        .to_uppercase();
    let reset_ts = state
        .get("reset_ts")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    // Check short_id collision (two different devices with same short_id)
    let cached_device = safe_kv_get(db, &format!("relay_short_{}", short_id));
    if let Some(ref cached) = cached_device {
        if cached != device_id {
            log::log_warn(
                "relay",
                "relay.collision",
                &format!(
                    "short_id={} existing={} incoming={}",
                    short_id,
                    super::device_id_prefix(cached),
                    super::device_id_prefix(device_id)
                ),
            );
            return false; // Skip to prevent data corruption
        }
        // Known device — check if it's a reconnect (was offline, now back)
        let last_sync: f64 = safe_kv_get(db, &format!("relay_sync_time_{}", device_id))
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        let now = crate::shared::time::now_epoch_f64();
        if last_sync > 0.0 && (now - last_sync) > super::DEVICE_STALE_SECS {
            let prefix = super::device_id_prefix(device_id);
            emit_device_event(
                db,
                super::ACTION_DEVICE_JOIN,
                &short_id,
                prefix,
                &format!("device {} reconnected", short_id),
                true,
            );
        }
    } else {
        safe_kv_set(db, &format!("relay_short_{}", short_id), Some(device_id));
        let prefix = super::device_id_prefix(device_id);
        emit_device_event(
            db,
            super::ACTION_DEVICE_JOIN,
            &short_id,
            prefix,
            &format!("new device {} joined the relay", short_id),
            false,
        );
    }
    remember_device_short_id(db, device_id, &short_id);
    // Cache the peer's advertised capabilities. Distinguish three states:
    //   - "null"  → peer state arrived without a `capabilities` field at all
    //               (legacy / pre-capability peer); treated as unknown by the
    //               capability check so we don't hard-block it.
    //   - "[]"    → peer explicitly advertised an empty list (e.g. remote
    //               control disabled); capability check blocks every action.
    //   - "[...]" → explicit advertisement.
    // Missing KV key means "no state received yet" and is handled separately.
    if let Some(caps) = state.get("capabilities").and_then(|v| v.as_array()) {
        let serialized = serde_json::to_string(caps).unwrap_or_else(|_| "[]".to_string());
        safe_kv_set(db, &format!("relay_caps_{}", device_id), Some(&serialized));
    } else {
        safe_kv_set(db, &format!("relay_caps_{}", device_id), Some("null"));
    }

    // Check for device reset — clean old data before importing
    let cached_reset: f64 = safe_kv_get(db, &format!("relay_reset_{}", device_id))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    if reset_ts > cached_reset {
        if let Err(error) = cleanup_imported_events_for_device(db, device_id) {
            log::log_error(
                "relay",
                "pull.reset_events",
                &format!("failed to clean relay projections for device {device_id}: {error}"),
            );
            return false;
        }
        if let Err(error) = db.conn().execute(
            "DELETE FROM instances WHERE origin_device_id = ?1",
            params![device_id],
        ) {
            log::log_warn(
                "relay",
                "pull.reset_instances",
                &format!("failed to delete instances for device {device_id}: {error}"),
            );
        }
        safe_kv_set(
            db,
            &format!("relay_reset_{}", device_id),
            Some(&reset_ts.to_string()),
        );
        safe_kv_set(db, &format!("relay_events_{}", device_id), Some("0"));
        log::log_info("relay", "relay.reset", &format!("device={}", short_id));
    }

    // Get local reset timestamp for filtering stale data.
    // Check KV first, then fall back to events table.
    let mut local_reset_ts: f64 = safe_kv_get(db, "relay_local_reset_ts")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);

    if local_reset_ts == 0.0 {
        // Fallback: query events table for last reset event
        let ts_opt = db
            .conn()
            .query_row(
                "SELECT timestamp FROM events
             WHERE type='life' AND instance='_device'
               AND json_extract(data, '$.action')='reset'
               AND json_extract(data, '$._relay') IS NULL
             ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, Option<String>>(0),
            )
            .ok()
            .flatten();

        if let Some(ts_str) = ts_opt {
            let ts = parse_ts(Some(&serde_json::Value::String(ts_str)));
            if ts > 0.0 {
                local_reset_ts = ts;
                // Cache in KV for future calls
                safe_kv_set(db, "relay_local_reset_ts", Some(&ts.to_string()));
            }
        }
    }

    // Upsert remote instances
    let own_short_id = device_short_id_for_db(db, own_device);
    let instances = state
        .get("instances")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    let mut seen_instances = std::collections::HashSet::new();

    for (name, inst) in &instances {
        let status_time = inst
            .get("status_time")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);
        let status_time_i64 = status_time as i64;

        // Local reset wins: ignore remote snapshots older than our reset so
        // cleared instances don't reappear from broker-retained state.
        if local_reset_ts > 0.0 && status_time < local_reset_ts {
            continue;
        }

        let namespaced = super::add_device_suffix(name, &short_id);
        seen_instances.insert(namespaced.clone());

        let parent = inst
            .get("parent")
            .and_then(|v| v.as_str())
            .map(|p| super::add_device_suffix(p, &short_id));

        let now = crate::shared::time::now_epoch_f64();

        let _ = db.conn().execute(
            "INSERT INTO instances (
                name, origin_device_id, status, status_context, status_detail, status_time,
                parent_name, directory, transcript_path, created_at,
                session_id, parent_session_id, agent_id, wait_timeout, last_stop, tcp_mode,
                tag, tool, background, endpoint_epoch
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                status = excluded.status,
                status_context = excluded.status_context, status_detail = excluded.status_detail,
                status_time = excluded.status_time,
                parent_name = excluded.parent_name,
                directory = excluded.directory, transcript_path = excluded.transcript_path,
                session_id = excluded.session_id, parent_session_id = excluded.parent_session_id,
                agent_id = excluded.agent_id, wait_timeout = excluded.wait_timeout,
                last_stop = excluded.last_stop, tcp_mode = excluded.tcp_mode,
                tag = excluded.tag, tool = excluded.tool, background = excluded.background,
                endpoint_epoch = excluded.endpoint_epoch",
            params![
                namespaced,
                device_id,
                inst.get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown"),
                inst.get("context").and_then(|v| v.as_str()).unwrap_or(""),
                inst.get("detail").and_then(|v| v.as_str()).unwrap_or(""),
                status_time_i64,
                parent,
                inst.get("directory").and_then(|v| v.as_str()),
                inst.get("transcript").and_then(|v| v.as_str()),
                now,
                Option::<String>::None,
                Option::<String>::None,
                Option::<String>::None,
                inst.get("wait_timeout")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(86400),
                inst.get("last_stop")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.0),
                inst.get("tcp_mode")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                inst.get("tag").and_then(|v| v.as_str()),
                inst.get("tool")
                    .and_then(|v| v.as_str())
                    .unwrap_or("claude"),
                inst.get("background")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
                inst.get("endpoint_epoch")
                    .and_then(|v| v.as_str())
                    .unwrap_or(""),
            ],
        );
    }

    // Remove stale instances (no longer in remote state)
    let current_remote: Vec<String> = db
        .conn()
        .prepare("SELECT name FROM instances WHERE origin_device_id = ?")
        .ok()
        .map(|mut stmt| {
            stmt.query_map(params![device_id], |row| row.get::<_, String>(0))
                .ok()
                .map(|rows| rows.filter_map(|r| r.ok()).collect())
                .unwrap_or_default()
        })
        .unwrap_or_default();

    for name in &current_remote {
        if !seen_instances.contains(name) {
            let _ = db
                .conn()
                .execute("DELETE FROM instances WHERE name = ?", params![name]);
        }
    }

    // Handle control events in the events payload
    let should_push = super::control::handle_control_events(db, &events, &own_short_id, device_id);

    // Import remote events with dedup
    import_remote_events(
        db,
        device_id,
        &short_id,
        &events,
        local_reset_ts,
        &own_short_id,
    );

    // Update sync timestamp
    let now = crate::shared::time::now_epoch_f64();
    safe_kv_set(
        db,
        &format!("relay_sync_time_{}", device_id),
        Some(&now.to_string()),
    );

    // Update relay_device_count and relay_last_sync
    let device_count: i64 = db
        .conn()
        .query_row(
            "SELECT COUNT(DISTINCT origin_device_id) FROM instances \
             WHERE origin_device_id IS NOT NULL AND origin_device_id != ''",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    safe_kv_set(db, "relay_device_count", Some(&device_count.to_string()));
    safe_kv_set(db, "relay_last_sync", Some(&now.to_string()));
    record_state_ts_watermark(db, device_id, opened.ts_secs);

    let apply_ms = t0.elapsed().as_millis();
    log::log_with_fields(
        "INFO",
        "relay",
        "relay.recv",
        "",
        &[
            ("device", &short_id),
            ("events", &events.len().to_string()),
            ("instances", &instances.len().to_string()),
            ("apply_ms", &apply_ms.to_string()),
            ("payload_bytes", &payload.len().to_string()),
        ],
    );

    // Wake local TCP instances so they see new messages immediately.
    crate::notify::wake_all(db);

    should_push
}

/// Result of safely handling one protocol-aware relay event.
#[derive(Debug)]
enum RelayV1Import {
    Applied(i64),
    Duplicate,
    Rejected(String),
}

fn relay_source_digest(event: &Value) -> String {
    let bytes = serde_json::to_vec(event).unwrap_or_default();
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn map_v1_remote_name(name: &str, source_short: &str, own_short: &str) -> String {
    let local = strip_device_suffix(name, own_short);
    if local != name {
        local
    } else if name.contains(':') {
        name.to_string()
    } else {
        super::add_device_suffix(name, source_short)
    }
}

fn prepare_remote_event(
    db: &HcomDb,
    event: &Value,
    device_id: &str,
    short_id: &str,
    own_short_id: &str,
    event_id: i64,
) -> (String, String, Value) {
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_string();
    let instance = event.get("instance").and_then(Value::as_str).unwrap_or("");
    let namespaced_instance =
        if !instance.is_empty() && !instance.contains(':') && !instance.starts_with('_') {
            super::add_device_suffix(instance, short_id)
        } else {
            instance.to_string()
        };
    let mut data = event
        .get("data")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let is_v1 = data.get("protocol").and_then(Value::as_str)
        == Some(crate::db::MESSAGE_PROTOCOL_V1)
        && matches!(event_type.as_str(), "message" | "message_state");
    let digest = relay_source_digest(event);

    if let Some(obj) = data.as_object_mut() {
        if let Some(from) = obj.get("from").and_then(Value::as_str).map(String::from)
            && !from.contains(':')
        {
            obj.insert(
                "from".to_string(),
                Value::String(super::add_device_suffix(&from, short_id)),
            );
        }

        for field in ["mentions", "delivered_to"] {
            if let Some(values) = obj.get(field).and_then(Value::as_array).cloned() {
                let fixed = values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|name| {
                        Value::String(if is_v1 {
                            map_v1_remote_name(name, short_id, own_short_id)
                        } else {
                            strip_device_suffix(name, own_short_id)
                        })
                    })
                    .collect();
                obj.insert(field.to_string(), Value::Array(fixed));
            }
        }

        if is_v1 && event_type == "message_state" {
            for field in ["actor", "recipient"] {
                if let Some(name) = obj.get(field).and_then(Value::as_str).map(String::from) {
                    obj.insert(
                        field.to_string(),
                        Value::String(map_v1_remote_name(&name, short_id, own_short_id)),
                    );
                }
            }
        }

        if is_v1 && event_type == "message" {
            if let Some(reply_to) = obj
                .get("reply_to")
                .and_then(Value::as_str)
                .map(String::from)
                && !reply_to.contains(':')
            {
                obj.insert(
                    "reply_to".to_string(),
                    Value::String(format!("{reply_to}:{short_id}")),
                );
            }
            if let Some(parent_id) = obj.get("in_reply_to").and_then(Value::as_str)
                && let Ok(local_parent_event) = db.conn().query_row(
                    "SELECT event_id FROM message_records WHERE message_id = ?1",
                    params![parent_id],
                    |row| row.get::<_, i64>(0),
                )
            {
                obj.insert(
                    "reply_to_local".to_string(),
                    Value::from(local_parent_event),
                );
            }
        }

        let mut marker = serde_json::json!({
            "device": device_id,
            "short": short_id,
            "id": event_id,
        });
        if is_v1 {
            marker["source_digest"] = Value::String(digest);
        }
        obj.insert("_relay".to_string(), marker);
    }
    (event_type, namespaced_instance, data)
}

fn bounded_imported_attachments(data: &Value) -> std::result::Result<String, String> {
    let Some(attachments) = data.get("attachments") else {
        return Ok("[]".to_string());
    };
    let values = attachments
        .as_array()
        .ok_or_else(|| "message attachments must be an array".to_string())?;
    let raw = values
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| format!("invalid attachment JSON: {error}"))?;
    let normalized = crate::messages::normalize_attachments(&raw)?;
    let normalized_value = serde_json::to_value(&normalized)
        .map_err(|error| format!("invalid attachment metadata: {error}"))?;
    if normalized_value != *attachments {
        return Err("attachment metadata or digest does not match bounded content".to_string());
    }
    serde_json::to_string(&normalized)
        .map_err(|error| format!("invalid attachment metadata: {error}"))
}

fn valid_stable_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value.chars().any(|character| character.is_control())
}

fn snapshot_recipient_epoch(tx: &rusqlite::Transaction<'_>, recipient: &str) -> Result<String> {
    Ok(tx
        .query_row(
            "SELECT endpoint_epoch FROM instances WHERE name = ?1",
            params![recipient],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_default())
}

fn project_imported_message(
    tx: &rusqlite::Transaction<'_>,
    local_event_id: i64,
    timestamp: &str,
    data: &Value,
) -> Result<Option<String>> {
    let Some(message_id) = data.get("message_id").and_then(Value::as_str) else {
        return Ok(Some("v1 message is missing message_id".to_string()));
    };
    let Some(correlation_id) = data.get("correlation_id").and_then(Value::as_str) else {
        return Ok(Some("v1 message is missing correlation_id".to_string()));
    };
    let Some(sender) = data.get("from").and_then(Value::as_str) else {
        return Ok(Some("v1 message is missing sender".to_string()));
    };
    if !valid_stable_id(message_id) || !valid_stable_id(correlation_id) {
        return Ok(Some(
            "v1 message has an invalid stable identity".to_string(),
        ));
    }
    let Some(recipient_values) = data.get("delivered_to").and_then(Value::as_array) else {
        return Ok(Some("v1 message is missing delivered_to".to_string()));
    };
    let recipients = recipient_values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(String::from)
                .ok_or_else(|| "v1 recipient must be a string".to_string())
        })
        .collect::<std::result::Result<Vec<_>, _>>();
    let recipients = match recipients {
        Ok(recipients) => recipients,
        Err(error) => return Ok(Some(error)),
    };
    let unique: BTreeSet<&str> = recipients.iter().map(String::as_str).collect();
    if recipients.is_empty() || unique.len() != recipients.len() {
        return Ok(Some(
            "v1 message recipients must be non-empty and unique".to_string(),
        ));
    }
    let delivery_endpoint = data
        .get("delivery_endpoint")
        .and_then(Value::as_str)
        .unwrap_or("inbox");
    if delivery_endpoint != "inbox" && !delivery_endpoint.starts_with("request:") {
        return Ok(Some("unsupported logical delivery endpoint".to_string()));
    }
    let attachments_json = match bounded_imported_attachments(data) {
        Ok(value) => value,
        Err(error) => return Ok(Some(error)),
    };

    let existing_event = tx
        .query_row(
            "SELECT event_id FROM message_records WHERE message_id = ?1",
            params![message_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()?;
    if existing_event.is_some_and(|event_id| event_id != local_event_id) {
        return Ok(Some(format!(
            "stable message_id collision for {message_id}"
        )));
    }

    let intent = data.get("intent").and_then(Value::as_str);
    let expects_reply = data
        .get("expects_reply")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let in_reply_to = data.get("in_reply_to").and_then(Value::as_str);
    let legacy_reply_to = data.get("reply_to").and_then(Value::as_str);
    let reply_endpoint = data.get("reply_endpoint").and_then(Value::as_str);
    let supersedes = data.get("supersedes").and_then(Value::as_str);
    let retry_of = data.get("retry_of").and_then(Value::as_str);

    if existing_event.is_none() {
        tx.execute(
            "INSERT INTO message_records (
                message_id, event_id, correlation_id, sender_name, sender_session_id,
                intent, expects_reply, in_reply_to, legacy_reply_to, reply_endpoint,
                supersedes, retry_of, attachments_json, created_at
             ) VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                message_id,
                local_event_id,
                correlation_id,
                sender,
                intent,
                i64::from(expects_reply),
                in_reply_to,
                legacy_reply_to,
                reply_endpoint,
                supersedes,
                retry_of,
                attachments_json,
                timestamp,
            ],
        )?;
    } else {
        tx.execute(
            "UPDATE message_records
             SET correlation_id = ?1, sender_name = ?2, sender_session_id = NULL,
                 intent = ?3, expects_reply = ?4, in_reply_to = ?5,
                 legacy_reply_to = ?6, reply_endpoint = ?7, supersedes = ?8,
                 retry_of = ?9, attachments_json = ?10, created_at = ?11
             WHERE message_id = ?12 AND event_id = ?13",
            params![
                correlation_id,
                sender,
                intent,
                i64::from(expects_reply),
                in_reply_to,
                legacy_reply_to,
                reply_endpoint,
                supersedes,
                retry_of,
                attachments_json,
                timestamp,
                message_id,
                local_event_id,
            ],
        )?;
    }

    let attempt = if retry_of.is_some() {
        tx.query_row(
            "SELECT COALESCE(MAX(md.attempt), 0) + 1
             FROM message_records mr
             JOIN message_deliveries md ON md.message_id = mr.message_id
             WHERE mr.correlation_id = ?1 AND mr.sender_name = ?2
               AND mr.message_id != ?3",
            params![correlation_id, sender, message_id],
            |row| row.get::<_, i64>(0),
        )?
    } else {
        1
    };
    for recipient in &recipients {
        let existing = tx
            .query_row(
                "SELECT delivery_endpoint FROM message_deliveries
                 WHERE message_id = ?1 AND recipient_name = ?2",
                params![message_id, recipient],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing != delivery_endpoint {
                tx.execute(
                    "UPDATE message_deliveries SET delivery_endpoint = ?1
                     WHERE message_id = ?2 AND recipient_name = ?3",
                    params![delivery_endpoint, message_id, recipient],
                )?;
            }
            continue;
        }
        let epoch = snapshot_recipient_epoch(tx, recipient)?;
        tx.execute(
            "INSERT INTO message_deliveries (
                message_id, recipient_name, endpoint_epoch, delivery_endpoint,
                state, attempt, state_event_id, last_actor, updated_at
             ) VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?6, ?7, ?8)",
            params![
                message_id,
                recipient,
                epoch,
                delivery_endpoint,
                attempt,
                local_event_id,
                sender,
                timestamp,
            ],
        )?;
    }

    if let Some(parent_id) = in_reply_to {
        let parent = tx
            .query_row(
                "SELECT sender_name, correlation_id, intent, expects_reply
                 FROM message_records WHERE message_id = ?1",
                params![parent_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, i64>(3)? != 0,
                    ))
                },
            )
            .optional()?;
        let Some((parent_sender, parent_correlation, parent_intent, parent_expects_reply)) = parent
        else {
            return Ok(Some("reply target projection not found".to_string()));
        };
        if intent != Some("ack")
            || parent_intent.as_deref() != Some("request")
            || !parent_expects_reply
            || parent_correlation != correlation_id
            || recipients != [parent_sender]
            || delivery_endpoint != format!("request:{parent_id}")
        {
            return Ok(Some("imported reply lineage is invalid".to_string()));
        }
        let parent_delivery = tx
            .query_row(
                "SELECT state FROM message_deliveries
                 WHERE message_id = ?1 AND recipient_name = ?2",
                params![parent_id, sender],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(parent_state) = parent_delivery else {
            return Ok(Some("reply sender is not a request recipient".to_string()));
        };
        if matches!(
            parent_state.as_str(),
            "queued" | "received" | "accepted" | "acknowledged"
        ) {
            tx.execute(
                "UPDATE message_deliveries
                 SET state = 'replied', state_event_id = ?1, last_actor = ?2, updated_at = ?3
                 WHERE message_id = ?4 AND recipient_name = ?2 AND state = ?5",
                params![local_event_id, sender, timestamp, parent_id, parent_state],
            )?;
        } else if parent_state != "replied" {
            return Ok(Some(format!(
                "request is no longer replyable ({parent_state})"
            )));
        }
    }
    Ok(None)
}

fn state_transition_allowed(prior: &str, next: &str) -> bool {
    matches!(
        (prior, next),
        ("queued", "received")
            | ("received", "accepted")
            | ("accepted", "acknowledged")
            | (
                "queued" | "received" | "accepted" | "acknowledged",
                "replied"
            )
            | ("queued", "cancelled" | "superseded" | "delivery_failed")
            | (
                "received",
                "cancellation_requested" | "superseded" | "delivery_failed"
            )
            | (
                "accepted" | "acknowledged",
                "cancellation_requested" | "supersession_requested" | "delivery_failed"
            )
    )
}

fn project_imported_state(
    tx: &rusqlite::Transaction<'_>,
    local_event_id: i64,
    timestamp: &str,
    data: &Value,
    device_id: &str,
    short_id: &str,
) -> Result<Option<String>> {
    let Some(message_id) = data.get("message_id").and_then(Value::as_str) else {
        return Ok(Some("v1 message_state is missing message_id".to_string()));
    };
    if !valid_stable_id(message_id) {
        return Ok(Some("v1 message_state has invalid message_id".to_string()));
    }
    let Some(actor) = data.get("actor").and_then(Value::as_str) else {
        return Ok(Some("v1 message_state is missing actor".to_string()));
    };
    let Some(new_state) = data.get("new_state").and_then(Value::as_str) else {
        return Ok(Some("v1 message_state is missing new_state".to_string()));
    };
    let record = tx
        .query_row(
            "SELECT mr.sender_name, json_extract(events.data, '$._relay.device')
             FROM message_records mr JOIN events ON events.id = mr.event_id
             WHERE mr.message_id = ?1",
            params![message_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()?;
    let Some((sender, message_origin)) = record else {
        return Ok(Some(
            "message_state target projection not found".to_string(),
        ));
    };

    if new_state == "ask_timed_out" {
        if actor != sender || message_origin.as_deref() != Some(device_id) {
            return Ok(Some("unauthorized ask timeout origin".to_string()));
        }
        return Ok(None);
    }

    let Some(recipient) = data.get("recipient").and_then(Value::as_str) else {
        return Ok(Some("v1 message_state is missing recipient".to_string()));
    };
    let delivery = tx
        .query_row(
            "SELECT state, endpoint_epoch, attempt FROM message_deliveries
             WHERE message_id = ?1 AND recipient_name = ?2",
            params![message_id, recipient],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()?;
    let Some((current, snapshot_epoch, attempt)) = delivery else {
        return Ok(Some(
            "message_state recipient projection not found".to_string(),
        ));
    };

    let authorized_origin = if let Some(origin) = message_origin.as_deref() {
        origin == device_id
    } else {
        let suffix = format!(":{short_id}");
        recipient.len() > suffix.len()
            && recipient[recipient.len() - suffix.len()..].eq_ignore_ascii_case(&suffix)
    };
    if !authorized_origin {
        return Ok(Some(
            "peer cannot mutate an unrelated message projection".to_string(),
        ));
    }

    let actor_is_sender = actor == sender;
    let actor_is_recipient = actor == recipient;
    let actor_allowed = match new_state {
        "queued"
        | "cancelled"
        | "cancellation_requested"
        | "superseded"
        | "supersession_requested"
        | "delivery_failed" => actor_is_sender,
        "received" | "accepted" | "acknowledged" | "replied" => actor_is_recipient,
        _ => false,
    };
    if !actor_allowed {
        return Ok(Some("message_state actor is not authorized".to_string()));
    }
    let event_epoch = data
        .get("endpoint_epoch")
        .and_then(Value::as_str)
        .unwrap_or("");
    let event_attempt = data.get("attempt").and_then(Value::as_i64).unwrap_or(1);
    if event_epoch != snapshot_epoch || event_attempt != attempt {
        return Ok(Some("stale endpoint epoch or delivery attempt".to_string()));
    }
    let current_epoch = snapshot_recipient_epoch(tx, recipient)?;
    if !current_epoch.is_empty() && current_epoch != snapshot_epoch {
        return Ok(Some("destination endpoint epoch has rotated".to_string()));
    }
    if current == new_state {
        return Ok(None);
    }
    let prior = data.get("prior_state").and_then(Value::as_str);
    if prior != Some(current.as_str()) || !state_transition_allowed(&current, new_state) {
        return Ok(Some(format!(
            "invalid message_state transition {current}->{new_state}"
        )));
    }
    let changed = tx.execute(
        "UPDATE message_deliveries
         SET state = ?1, state_event_id = ?2, last_actor = ?3, updated_at = ?4
         WHERE message_id = ?5 AND recipient_name = ?6 AND state = ?7
           AND endpoint_epoch = ?8 AND attempt = ?9",
        params![
            new_state,
            local_event_id,
            actor,
            timestamp,
            message_id,
            recipient,
            current,
            snapshot_epoch,
            attempt,
        ],
    )?;
    if changed != 1 {
        return Ok(Some(
            "concurrent imported message_state transition".to_string(),
        ));
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)] // mirrors the authenticated relay event envelope
fn import_relay_v1_event(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    remote_event_id: i64,
    timestamp: &str,
    event_type: &str,
    instance: &str,
    data: &Value,
) -> Result<RelayV1Import> {
    let mut inserted = false;
    let outcome = db.with_immediate_transaction(|tx| {
        tx.execute_batch("SAVEPOINT relay_v1_import")?;
        let existing = tx
            .query_row(
                "SELECT id, timestamp, type, instance, data FROM events
                 WHERE json_extract(data, '$._relay.device') = ?1
                   AND CAST(json_extract(data, '$._relay.id') AS INTEGER) = ?2
                 ORDER BY id LIMIT 1",
                params![device_id, remote_event_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?;
        let local_event_id =
            if let Some((id, stored_ts, stored_type, stored_instance, stored_data)) = existing {
                let stored: Value = serde_json::from_str(&stored_data)?;
                let stored_digest = stored
                    .get("_relay")
                    .and_then(|relay| relay.get("source_digest"))
                    .and_then(Value::as_str);
                let incoming_digest = data
                    .get("_relay")
                    .and_then(|relay| relay.get("source_digest"))
                    .and_then(Value::as_str);
                let same = stored_type == event_type
                    && stored_instance == instance
                    && stored_ts == timestamp
                    && match (stored_digest, incoming_digest) {
                        (Some(left), Some(right)) => left == right,
                        _ => stored == *data,
                    };
                if !same {
                    return Ok(RelayV1Import::Rejected(format!(
                        "remote event identity collision device={device_id} id={remote_event_id}"
                    )));
                }
                id
            } else {
                tx.execute(
                    "INSERT INTO events (timestamp, type, instance, data) VALUES (?1, ?2, ?3, ?4)",
                    params![
                        timestamp,
                        event_type,
                        instance,
                        serde_json::to_string(data)?
                    ],
                )?;
                inserted = true;
                tx.last_insert_rowid()
            };

        let rejection = match event_type {
            "message" => project_imported_message(tx, local_event_id, timestamp, data)?,
            "message_state" => {
                project_imported_state(tx, local_event_id, timestamp, data, device_id, short_id)?
            }
            _ => Some("unsupported protocol-aware relay event".to_string()),
        };
        if let Some(reason) = rejection {
            tx.execute_batch("ROLLBACK TO relay_v1_import; RELEASE relay_v1_import")?;
            return Ok(RelayV1Import::Rejected(reason));
        }
        tx.execute_batch("RELEASE relay_v1_import")?;
        Ok(if inserted {
            RelayV1Import::Applied(local_event_id)
        } else {
            RelayV1Import::Duplicate
        })
    })?;

    if let RelayV1Import::Applied(event_id) = outcome {
        crate::db::subscriptions::process_logged_event(db, event_id, event_type, instance, data);
    }
    Ok(outcome)
}

/// Import remote events with cursor-based dedup. Protocol-aware events are
/// repaired even when their remote cursor is already behind our watermark.
fn import_remote_events(
    db: &HcomDb,
    device_id: &str,
    short_id: &str,
    events: &[Value],
    local_reset_ts: f64,
    own_short_id: &str,
) {
    let mut last_event_id: i64 = safe_kv_get(db, &format!("relay_events_{device_id}"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);

    if !events.is_empty() && last_event_id > 0 {
        let remote_max_id = events
            .iter()
            .filter(|event| event.get("type").and_then(Value::as_str) != Some("control"))
            .filter_map(|event| event.get("id").and_then(Value::as_i64))
            .max()
            .unwrap_or(0);
        if remote_max_id > 0 && remote_max_id < last_event_id {
            log::log_info(
                "relay",
                "relay.reset",
                &format!(
                    "device={} reason=id_regression:{}<{}",
                    short_id, remote_max_id, last_event_id
                ),
            );
            if let Err(error) = cleanup_imported_events_for_device(db, device_id) {
                log::log_error("relay", "pull.reset_events", &error.to_string());
                return;
            }
            let _ = db.conn().execute(
                "DELETE FROM instances WHERE origin_device_id = ?1",
                params![device_id],
            );
            last_event_id = 0;
            safe_kv_set(db, &format!("relay_events_{device_id}"), Some("0"));
        }
    }

    let mut max_event_id = last_event_id;
    for event in events {
        if event.get("type").and_then(Value::as_str) == Some("control")
            || event.get("instance").and_then(Value::as_str) == Some("_device")
        {
            continue;
        }
        let Some(event_id) = event.get("id").and_then(Value::as_i64) else {
            log::log_warn(
                "relay",
                "relay.bad_event_id",
                &format!("Skipping event with bad/missing id: {:?}", event.get("id")),
            );
            continue;
        };
        let event_ts = parse_ts(event.get("ts"));
        if local_reset_ts > 0.0 && event_ts > 0.0 && event_ts < local_reset_ts {
            continue;
        }
        let timestamp = match event.get("ts") {
            Some(Value::String(value)) => value.clone(),
            Some(Value::Number(value)) => value.to_string(),
            _ => String::new(),
        };
        let (event_type, instance, data) =
            prepare_remote_event(db, event, device_id, short_id, own_short_id, event_id);
        let protocol_aware = data.get("protocol").and_then(Value::as_str)
            == Some(crate::db::MESSAGE_PROTOCOL_V1)
            && matches!(event_type.as_str(), "message" | "message_state");

        if event_id <= last_event_id && !protocol_aware {
            continue;
        }
        if protocol_aware {
            match import_relay_v1_event(
                db,
                device_id,
                short_id,
                event_id,
                &timestamp,
                &event_type,
                &instance,
                &data,
            ) {
                Ok(RelayV1Import::Applied(_)) | Ok(RelayV1Import::Duplicate) => {}
                Ok(RelayV1Import::Rejected(reason)) => {
                    log::log_warn(
                        "relay",
                        "relay.v1_rejected",
                        &format!("device={short_id} remote_event={event_id} {reason}"),
                    );
                }
                Err(error) => {
                    log::log_error(
                        "relay",
                        "relay.v1_import_error",
                        &format!("device={short_id} remote_event={event_id} {error}"),
                    );
                    break;
                }
            }
        } else {
            let _ = db.log_event_with_ts(&event_type, &instance, &data, Some(&timestamp));
        }

        if event_id > last_event_id {
            max_event_id = max_event_id.max(event_id);
        }
        if event_type == "message" && event_ts > 0.0 {
            let latency_ms = ((crate::shared::time::now_epoch_f64() - event_ts) * 1000.0) as i64;
            log::log_with_fields(
                "INFO",
                "relay",
                "relay.msg_recv",
                "",
                &[
                    ("device", short_id),
                    ("instance", &instance),
                    ("latency_ms", &latency_ms.to_string()),
                ],
            );
        }
    }

    if max_event_id > last_event_id {
        safe_kv_set(
            db,
            &format!("relay_events_{device_id}"),
            Some(&max_event_id.to_string()),
        );
    }
}

/// Delete one peer's imported history without touching local-origin message
/// records, then rebuild every source-side delivery projection affected by the
/// removed state events from the retained event stream.
fn cleanup_imported_events_for_device(db: &HcomDb, device_id: &str) -> Result<()> {
    db.with_immediate_transaction(|tx| {
        let mut affected: HashSet<(String, String)> = HashSet::new();
        {
            let mut stmt = tx.prepare(
                "SELECT json_extract(data, '$.message_id'), json_extract(data, '$.recipient')
                 FROM events
                 WHERE type = 'message_state'
                   AND json_extract(data, '$.protocol') = ?1
                   AND json_extract(data, '$._relay.device') = ?2
                   AND json_extract(data, '$.recipient') IS NOT NULL",
            )?;
            let rows = stmt
                .query_map(params![crate::db::MESSAGE_PROTOCOL_V1, device_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?;
            for row in rows {
                affected.insert(row?);
            }
        }
        {
            let mut stmt = tx.prepare(
                "SELECT json_extract(events.data, '$.in_reply_to'),
                        json_extract(events.data, '$.from')
                 FROM message_records mr
                 JOIN events ON events.id = mr.event_id
                 WHERE json_extract(events.data, '$._relay.device') = ?1
                   AND json_extract(events.data, '$.in_reply_to') IS NOT NULL",
            )?;
            let rows = stmt.query_map(params![device_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                affected.insert(row?);
            }
        }

        tx.execute(
            "DELETE FROM message_records
             WHERE event_id IN (
                 SELECT id FROM events
                 WHERE json_extract(data, '$._relay.device') = ?1
             )",
            params![device_id],
        )?;
        tx.execute(
            "DELETE FROM events WHERE json_extract(data, '$._relay.device') = ?1",
            params![device_id],
        )?;

        for (message_id, recipient) in affected {
            let retained = tx
                .query_row(
                    "SELECT id, timestamp,
                            json_extract(data, '$.new_state'),
                            json_extract(data, '$.endpoint_epoch'),
                            json_extract(data, '$.actor')
                     FROM events
                     WHERE type = 'message_state'
                       AND json_extract(data, '$.protocol') = ?1
                       AND json_extract(data, '$.message_id') = ?2
                       AND json_extract(data, '$.recipient') = ?3
                     ORDER BY id DESC LIMIT 1",
                    params![crate::db::MESSAGE_PROTOCOL_V1, message_id, recipient],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((event_id, timestamp, state, epoch, actor)) = retained {
                tx.execute(
                    "UPDATE message_deliveries
                     SET state = ?1, endpoint_epoch = ?2, state_event_id = ?3,
                         last_actor = ?4, updated_at = ?5
                     WHERE message_id = ?6 AND recipient_name = ?7",
                    params![
                        state, epoch, event_id, actor, timestamp, message_id, recipient
                    ],
                )?;
            } else {
                tx.execute(
                    "UPDATE message_deliveries
                     SET state = 'queued', state_event_id = (
                             SELECT event_id FROM message_records WHERE message_id = ?1
                         ),
                         last_actor = (
                             SELECT sender_name FROM message_records WHERE message_id = ?1
                         ),
                         updated_at = (
                             SELECT created_at FROM message_records WHERE message_id = ?1
                         )
                     WHERE message_id = ?1 AND recipient_name = ?2",
                    params![message_id, recipient],
                )?;
            }
        }
        Ok(())
    })
}

/// Reverse lookup: find short_id for a device UUID.
fn resolve_short_id(db: &HcomDb, device_id: &str) -> Option<String> {
    if let Some(short_id) = safe_kv_get(db, &format!("relay_uuid_short_{}", device_id)) {
        return Some(short_id);
    }
    if let Ok(entries) = db.kv_prefix("relay_short_") {
        for (key, val) in entries {
            if val == device_id {
                return Some(key.trim_start_matches("relay_short_").to_string());
            }
        }
    }
    None
}

/// Emit a relay device lifecycle event.
fn emit_device_event(
    db: &HcomDb,
    action: &str,
    short_id: &str,
    device_id_prefix: &str,
    text: &str,
    reconnect: bool,
) {
    let mut data = serde_json::json!({
        "action": action,
        "short_id": short_id,
        "device_id": device_id_prefix,
        "text": text,
    });
    if reconnect {
        data["reconnect"] = serde_json::json!(true);
    }
    let _ = db.log_event("life", "", &data);
}

/// Strip own device suffix from a name (case-insensitive).
/// e.g. "nuvi:RIVA" with own_short_id="RIVA" → "nuvi"
fn strip_device_suffix(name: &str, own_short_id: &str) -> String {
    let suffix = format!(":{}", own_short_id);
    if name.len() > suffix.len() && name[name.len() - suffix.len()..].eq_ignore_ascii_case(&suffix)
    {
        name[..name.len() - suffix.len()].to_string()
    } else {
        name.to_string()
    }
}

/// Parse timestamp (float or ISO string) to f64 epoch seconds.
fn parse_ts(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
        Some(Value::String(s)) => chrono::DateTime::parse_from_rfc3339(s)
            .or_else(|_| chrono::DateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ"))
            .ok()
            .map(|dt| dt.timestamp() as f64)
            .unwrap_or(0.0),
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::test_helpers::isolated_test_env;
    use serde_json::json;
    use serial_test::serial;

    fn fixture_psk() -> [u8; 32] {
        [0x33; 32]
    }

    fn seal_for_test(payload: &serde_json::Value, topic: &str, relay_id: &str) -> Vec<u8> {
        let psk = fixture_psk();
        let bytes = serde_json::to_vec(payload).unwrap();
        let now = crate::shared::time::now_epoch_f64() as u64;
        crate::relay::crypto::seal(&psk, relay_id, topic, &bytes, now).unwrap()
    }

    #[test]
    #[serial]
    fn test_handle_state_message_drops_remote_unique_identity_fields() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {
                    "orla": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": serde_json::Value::Null,
                        "directory": "/tmp/demo-parent",
                        "transcript": "/tmp/demo-parent/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": "demo",
                        "tool": "codex",
                        "background": false
                    },
                    "luna": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": "orla",
                        "directory": "/tmp/demo",
                        "transcript": "/tmp/demo/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": "demo",
                        "tool": "codex",
                        "background": false
                    }
                }
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let envelope = seal_for_test(&payload, topic, "relay-test");
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        );

        let row = db
            .get_instance_full("luna:ABCD")
            .unwrap()
            .expect("remote row");
        assert_eq!(row.parent_name.as_deref(), Some("orla:ABCD"));
        assert_eq!(row.session_id, None);
        assert_eq!(row.parent_session_id, None);
        assert_eq!(row.agent_id, None);
        assert_eq!(row.tool, "codex");
    }

    #[test]
    #[serial]
    fn test_handle_state_message_caches_remote_capabilities() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "capabilities": ["launch", "resume"],
                "instances": {}
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let envelope = seal_for_test(&payload, topic, "relay-test");
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert_eq!(
            safe_kv_get(&db, "relay_caps_device-1234").as_deref(),
            Some(r#"["launch","resume"]"#)
        );
    }

    #[test]
    #[serial]
    fn test_handle_state_message_caches_legacy_peer_without_capabilities() {
        // Peers that predate the `capabilities` advertisement must be cached
        // with the "null" sentinel, not "[]". The capability check in
        // relay::control reads this sentinel as `CachedCapabilities::Legacy`
        // and lets requests through optimistically so rolling upgrades don't
        // break remote actions against older peers.
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {}
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let envelope = seal_for_test(&payload, topic, "relay-test");
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert_eq!(
            safe_kv_get(&db, "relay_caps_device-1234").as_deref(),
            Some("null"),
            "legacy peer (no capabilities field) must be cached as the \"null\" sentinel"
        );
    }

    #[test]
    #[serial]
    fn test_handle_state_message_accepts_sender_clock_skew_in_both_directions() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();
        let now = crate::shared::time::now_epoch_f64() as i64;

        for (device_id, short_id, offset) in [
            ("device-past", "PAST", -61_i64),
            ("device-future", "FUTR", 61_i64),
        ] {
            let payload = json!({
                "state": {
                    "short_id": short_id,
                    "reset_ts": 0.0,
                    "instances": {
                        "luna": {
                            "status": "active",
                            "context": "",
                            "detail": "",
                            "status_time": crate::shared::time::now_epoch_f64(),
                            "parent": serde_json::Value::Null,
                            "directory": "/tmp/demo",
                            "transcript": "/tmp/demo/transcript.jsonl",
                            "wait_timeout": 42,
                            "last_stop": 0.0,
                            "tcp_mode": false,
                            "tag": serde_json::Value::Null,
                            "tool": "codex",
                            "background": false
                        }
                    }
                },
                "events": []
            });
            let topic = format!("relay-test/{device_id}");
            let bytes = serde_json::to_vec(&payload).unwrap();
            let envelope = crate::relay::crypto::seal(
                &psk,
                "relay-test",
                &topic,
                &bytes,
                (now + offset) as u64,
            )
            .unwrap();

            assert!(!handle_state_message(
                &db,
                device_id,
                &envelope,
                "own-device-5678",
                &mut InboundContext {
                    psk: &psk,
                    relay_id: "relay-test",
                    topic: &topic,
                    replay_guard: &mut guard,
                },
            ));
            assert!(
                db.get_instance_full(&format!("luna:{short_id}"))
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    #[serial]
    fn test_handle_state_message_rejects_rollback_behind_watermark() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        safe_kv_set(&db, "relay_state_ts_device-1234", Some("1500"));

        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {
                    "luna": {
                        "status": "active",
                        "context": "",
                        "detail": "",
                        "status_time": crate::shared::time::now_epoch_f64(),
                        "parent": serde_json::Value::Null,
                        "directory": "/tmp/demo",
                        "transcript": "/tmp/demo/transcript.jsonl",
                        "wait_timeout": 42,
                        "last_stop": 0.0,
                        "tcp_mode": false,
                        "tag": serde_json::Value::Null,
                        "tool": "codex",
                        "background": false
                    }
                }
            },
            "events": []
        });

        let topic = "relay-test/device-1234";
        let bytes = serde_json::to_vec(&payload).unwrap();
        let envelope =
            crate::relay::crypto::seal(&fixture_psk(), "relay-test", topic, &bytes, 1000).unwrap();
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert!(db.get_instance_full("luna:ABCD").unwrap().is_none());
    }

    #[test]
    #[serial]
    fn test_decrypt_failure_does_not_consume_replay_slot() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        let topic = "relay-test/device-1234";
        let payload = json!({
            "state": {
                "short_id": "ABCD",
                "reset_ts": 0.0,
                "instances": {}
            },
            "events": []
        });

        let good_envelope = seal_for_test(&payload, topic, "relay-test");
        let mut bad_psk = fixture_psk();
        bad_psk[0] ^= 0x55;
        let bad_bytes = serde_json::to_vec(&payload).unwrap();
        let bad_envelope =
            crate::relay::crypto::seal(&bad_psk, "relay-test", topic, &bad_bytes, 1234).unwrap();

        let mut guard = ReplayGuard::new(1, 600, crate::relay::replay::MAX_SKEW_SECS);
        let psk = fixture_psk();

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &bad_envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));
        assert_eq!(
            guard.len(),
            0,
            "failed decrypt must not record replay nonce"
        );

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &good_envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));
        assert_eq!(guard.len(), 1);
    }

    #[test]
    #[serial]
    fn test_handle_state_message_authenticated_null_state_cleans_up_device_and_watermark() {
        let (_dir, _hcom_dir, _home, _guard) = isolated_test_env();
        let db = HcomDb::open().unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (name, origin_device_id, created_at) VALUES (?1, ?2, ?3)",
                rusqlite::params!["luna:ABCD", "device-1234", 1.0],
            )
            .unwrap();
        safe_kv_set(&db, "relay_state_ts_device-1234", Some("1500"));

        let payload = json!({
            "state": serde_json::Value::Null,
            "events": [],
        });
        let topic = "relay-test/device-1234";
        let bytes = serde_json::to_vec(&payload).unwrap();
        let envelope =
            crate::relay::crypto::seal(&fixture_psk(), "relay-test", topic, &bytes, 2000).unwrap();
        let mut guard = ReplayGuard::default();
        let psk = fixture_psk();

        assert!(!handle_state_message(
            &db,
            "device-1234",
            &envelope,
            "own-device-5678",
            &mut InboundContext {
                psk: &psk,
                relay_id: "relay-test",
                topic,
                replay_guard: &mut guard,
            },
        ));

        assert!(db.get_instance_full("luna:ABCD").unwrap().is_none());
        assert_eq!(safe_kv_get(&db, "relay_state_ts_device-1234"), None);
    }
    fn add_test_instance(db: &HcomDb, name: &str, epoch: &str, origin_device_id: Option<&str>) {
        db.conn()
            .execute(
                "INSERT INTO instances (
                    name, status, endpoint_epoch, origin_device_id, created_at
                 ) VALUES (?1, 'active', ?2, ?3, 1.0)",
                params![name, epoch, origin_device_id],
            )
            .unwrap();
    }

    #[allow(clippy::too_many_arguments)] // explicit fixture fields make lineage assertions readable
    fn persist_test_message(
        db: &HcomDb,
        sender: &str,
        recipients: &[String],
        message_id: &str,
        correlation_id: &str,
        intent: &str,
        expects_reply: bool,
        in_reply_to: Option<&str>,
        legacy_reply_to: Option<&str>,
        delivery_endpoint: &str,
    ) -> crate::db::MessagePersistResult {
        let mut data = json!({
            "protocol": crate::db::MESSAGE_PROTOCOL_V1,
            "message_id": message_id,
            "correlation_id": correlation_id,
            "from": sender,
            "sender_kind": "instance",
            "scope": "mentions",
            "mentions": recipients,
            "delivered_to": recipients,
            "text": format!("text-{message_id}"),
            "intent": intent,
            "expects_reply": expects_reply,
            "delivery_endpoint": delivery_endpoint,
        });
        if expects_reply {
            data["reply_endpoint"] = json!(format!("request:{message_id}"));
            data["reply_mode"] = json!("wait");
        }
        if let Some(parent) = in_reply_to {
            data["in_reply_to"] = json!(parent);
        }
        if let Some(legacy) = legacy_reply_to {
            data["reply_to"] = json!(legacy);
            data["reply_to_local"] = json!(legacy.parse::<i64>().unwrap());
        }
        db.persist_message_v1(&crate::db::MessagePersistInput {
            routing_instance: sender,
            sender_name: sender,
            sender_session_id: None,
            data: &data,
            message_id,
            correlation_id,
            intent: Some(intent),
            expects_reply,
            in_reply_to,
            legacy_reply_to,
            reply_endpoint: expects_reply.then_some("request:test-request"),
            delivery_endpoint,
            supersedes: None,
            retry_of: None,
            attachments_json: "[]",
            recipients,
        })
        .unwrap()
    }

    fn local_push_events(db: &HcomDb, device_id: &str) -> Vec<Value> {
        crate::relay::push::build_push_payload(db, device_id).1
    }

    fn delivery_state(db: &HcomDb, message_id: &str, recipient: &str) -> String {
        db.conn()
            .query_row(
                "SELECT state FROM message_deliveries
                 WHERE message_id = ?1 AND recipient_name = ?2",
                params![message_id, recipient],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn two_device_v1_request_reply_state_replay_collision_and_reset() {
        const DEVICE_A: &str = "device-aaaaaaaa";
        const DEVICE_B: &str = "device-bbbbbbbb";
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let db_a = HcomDb::open_at(&dir_a.path().join("a.db")).unwrap();
        let db_b = HcomDb::open_at(&dir_b.path().join("b.db")).unwrap();
        remember_device_short_id(&db_a, DEVICE_A, "AAAA");
        remember_device_short_id(&db_a, DEVICE_B, "BBBB");
        remember_device_short_id(&db_b, DEVICE_A, "AAAA");
        remember_device_short_id(&db_b, DEVICE_B, "BBBB");
        add_test_instance(&db_a, "alice", "epoch-a", None);
        add_test_instance(&db_a, "bob:BBBB", "epoch-b", Some(DEVICE_B));
        add_test_instance(&db_b, "bob", "epoch-b", None);
        add_test_instance(&db_b, "alice:AAAA", "epoch-a", Some(DEVICE_A));
        db_b.log_event("status", "bob", &json!({"status": "active"}))
            .unwrap();
        let request = persist_test_message(
            &db_a,
            "alice",
            &["bob:BBBB".to_string()],
            "test-request",
            "test-correlation",
            "request",
            true,
            None,
            None,
            "inbox",
        );
        let from_a = local_push_events(&db_a, DEVICE_A);
        import_remote_events(&db_b, DEVICE_A, "AAAA", &from_a, 0.0, "BBBB");

        let imported = db_b.inspect_message_v1("test-request").unwrap();
        assert_eq!(imported["message"]["message_id"], "test-request");
        assert_eq!(imported["message"]["correlation_id"], "test-correlation");
        assert_eq!(imported["message"]["from"], "alice:AAAA");
        assert_eq!(imported["deliveries"][0]["recipient_name"], "bob");
        assert_eq!(imported["deliveries"][0]["endpoint_epoch"], "epoch-b");
        assert_eq!(imported["deliveries"][0]["state"], "queued");
        assert_eq!(db_b.pending_messages_v1("bob").unwrap().len(), 1);

        let imported_event_id = imported["message"]["event_id"].as_i64().unwrap();
        assert_ne!(imported_event_id, request.event_id);
        let event_count_before: i64 = db_b
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        import_remote_events(&db_b, DEVICE_A, "AAAA", &from_a, 0.0, "BBBB");
        let event_count_after: i64 = db_b
            .conn()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(event_count_after, event_count_before);
        assert_eq!(
            db_b.inspect_message_v1("test-request").unwrap()["message"]["event_id"],
            imported_event_id
        );

        db_b.conn()
            .execute(
                "DELETE FROM message_records WHERE message_id = 'test-request'",
                [],
            )
            .unwrap();
        assert!(db_b.inspect_message_v1("test-request").is_err());
        import_remote_events(&db_b, DEVICE_A, "AAAA", &from_a, 0.0, "BBBB");
        let repaired = db_b.inspect_message_v1("test-request").unwrap();
        assert_eq!(repaired["message"]["event_id"], imported_event_id);
        assert_eq!(repaired["deliveries"][0]["recipient_name"], "bob");
        assert_eq!(repaired["deliveries"][0]["endpoint_epoch"], "epoch-b");
        db_b.claim_inbox_received("bob", &["test-request".to_string()])
            .unwrap();
        let receipt_events = local_push_events(&db_b, DEVICE_B);
        import_remote_events(&db_a, DEVICE_B, "BBBB", &receipt_events, 0.0, "AAAA");
        assert_eq!(
            delivery_state(&db_a, "test-request", "bob:BBBB"),
            "received"
        );

        let forged = json!([{
            "id": 9001,
            "ts": "2026-01-01T00:00:00Z",
            "type": "message_state",
            "instance": "mallory",
            "data": {
                "protocol": crate::db::MESSAGE_PROTOCOL_V1,
                "message_id": "test-request",
                "actor": "mallory",
                "recipient": "bob:BBBB",
                "endpoint_epoch": "epoch-b",
                "prior_state": "received",
                "new_state": "accepted",
                "attempt": 1,
            }
        }]);
        import_remote_events(
            &db_a,
            "device-cccccccc",
            "CCCC",
            forged.as_array().unwrap(),
            0.0,
            "AAAA",
        );
        assert_eq!(
            delivery_state(&db_a, "test-request", "bob:BBBB"),
            "received"
        );
        let forged_count: i64 = db_a
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events
             WHERE json_extract(data, '$._relay.device') = 'device-cccccccc'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(forged_count, 0);

        let collision = json!([{
            "id": 1,
            "ts": "2026-01-01T00:00:01Z",
            "type": "message",
            "instance": "mallory",
            "data": {
                "protocol": crate::db::MESSAGE_PROTOCOL_V1,
                "message_id": "test-request",
                "correlation_id": "collision",
                "from": "mallory",
                "scope": "mentions",
                "mentions": ["alice:AAAA"],
                "delivered_to": ["alice:AAAA"],
                "text": "collision",
                "intent": "inform",
                "expects_reply": false,
                "delivery_endpoint": "inbox",
            }
        }]);
        import_remote_events(
            &db_a,
            "device-cccccccc",
            "CCCC",
            collision.as_array().unwrap(),
            0.0,
            "AAAA",
        );
        let request_records: i64 = db_a
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM message_records WHERE message_id = 'test-request'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(request_records, 1);

        let parent_local_on_b =
            db_b.inspect_message_v1("test-request").unwrap()["message"]["event_id"]
                .as_i64()
                .unwrap();
        persist_test_message(
            &db_b,
            "bob",
            &["alice:AAAA".to_string()],
            "test-reply",
            "test-correlation",
            "ack",
            false,
            Some("test-request"),
            Some(&parent_local_on_b.to_string()),
            "request:test-request",
        );
        let from_b = local_push_events(&db_b, DEVICE_B);
        import_remote_events(&db_a, DEVICE_B, "BBBB", &from_b, 0.0, "AAAA");

        let reply = db_a.inspect_message_v1("test-reply").unwrap();
        assert_eq!(reply["message"]["message_id"], "test-reply");
        assert_eq!(reply["message"]["in_reply_to"], "test-request");
        assert_eq!(reply["message"]["from"], "bob:BBBB");
        assert_eq!(
            reply["message"]["reply_to"],
            format!("{parent_local_on_b}:BBBB")
        );
        assert_eq!(reply["message"]["reply_to_local"], request.event_id);
        assert_eq!(reply["deliveries"][0]["recipient_name"], "alice");
        assert_eq!(
            reply["deliveries"][0]["delivery_endpoint"],
            "request:test-request"
        );
        assert!(
            !db_a
                .get_unread_messages("alice")
                .iter()
                .any(|message| message.message_id.as_deref() == Some("test-reply"))
        );

        let claimed = db_a
            .claim_reply_v1("test-request", "alice")
            .unwrap()
            .unwrap();
        assert_eq!(claimed["message"]["message_id"], "test-reply");
        assert!(
            db_a.claim_reply_v1("test-request", "alice")
                .unwrap()
                .is_none()
        );

        let before_duplicate: i64 = db_a
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events
             WHERE json_extract(data, '$._relay.device') = ?1",
                params![DEVICE_B],
                |row| row.get(0),
            )
            .unwrap();
        import_remote_events(&db_a, DEVICE_B, "BBBB", &from_b, 0.0, "AAAA");
        let after_duplicate: i64 = db_a
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events
             WHERE json_extract(data, '$._relay.device') = ?1",
                params![DEVICE_B],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after_duplicate, before_duplicate);

        cleanup_imported_events_for_device(&db_a, DEVICE_B).unwrap();
        assert!(db_a.inspect_message_v1("test-reply").is_err());
        assert_eq!(delivery_state(&db_a, "test-request", "bob:BBBB"), "queued");
        assert!(db_a.inspect_message_v1("test-request").is_ok());
    }

    #[test]
    fn legacy_relay_and_remote_event_short_reference_remain_compatible() {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("legacy.db")).unwrap();
        let events = json!([{
            "id": 42,
            "ts": "2026-01-01T00:00:00Z",
            "type": "message",
            "instance": "legacy",
            "data": {
                "from": "legacy",
                "scope": "mentions",
                "mentions": ["bob:BBBB"],
                "delivered_to": ["bob:BBBB"],
                "text": "legacy",
                "reply_to": "7:AAAA"
            }
        }]);
        import_remote_events(
            &db,
            "device-aaaaaaaa",
            "AAAA",
            events.as_array().unwrap(),
            0.0,
            "BBBB",
        );
        import_remote_events(
            &db,
            "device-aaaaaaaa",
            "AAAA",
            events.as_array().unwrap(),
            0.0,
            "BBBB",
        );
        let (count, from, recipient, reply_to): (i64, String, String, String) = db
            .conn()
            .query_row(
                "SELECT COUNT(*), json_extract(data, '$.from'),
                    json_extract(data, '$.delivered_to[0]'), json_extract(data, '$.reply_to')
             FROM events WHERE json_extract(data, '$._relay.device') = 'device-aaaaaaaa'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(from, "legacy:AAAA");
        assert_eq!(recipient, "bob");
        assert_eq!(reply_to, "7:AAAA");
        let projections: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM message_records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(projections, 0, "legacy imports must remain legacy");
    }
}
