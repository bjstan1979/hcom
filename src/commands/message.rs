//! `hcom message` — broker-authorized per-message lifecycle operations.

use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use serde_json::{Value, json};

use crate::commands::send::send_message;
use crate::db::HcomDb;
use crate::messages::{MessageEnvelope, normalize_attachments};
use crate::notify::NotifyServer;
use crate::shared::{CommandContext, SenderIdentity};

#[derive(Parser, Debug)]
#[command(
    name = "message",
    about = "Inspect and control durable message lifecycle"
)]
pub struct MessageArgs {
    #[command(subcommand)]
    pub command: MessageCommand,
}

#[derive(Subcommand, Debug)]
pub enum MessageCommand {
    /// Wait for the one exactly-correlated reply to a request
    Wait {
        message_id: String,
        #[arg(long, default_value_t = 86400)]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
    /// List unresolved inbound requests addressed to this actor
    Pending {
        #[arg(long)]
        json: bool,
    },
    /// Reply to a request as its verified recipient
    Reply {
        message_id: String,
        #[arg(long = "attachment")]
        attachments: Vec<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        quiet: bool,
        #[arg(last = true, required = true)]
        text: Vec<String>,
    },
    /// Cancel an unaccepted request, or record a cancellation request
    Cancel {
        message_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Send a replacement and atomically supersede the earlier message
    Supersede {
        message_id: String,
        #[arg(long = "attachment")]
        attachments: Vec<String>,
        #[arg(long)]
        json: bool,
        #[arg(last = true, required = true)]
        text: Vec<String>,
    },
    /// Explicitly retry a prior message as a new attempt
    Retry {
        message_id: String,
        #[arg(long)]
        json: bool,
    },
    /// Append a recipient-authenticated delivery receipt
    Receipt {
        message_id: String,
        #[arg(long)]
        state: String,
        #[arg(long)]
        endpoint_epoch: String,
        #[arg(long)]
        reason: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Inspect canonical message data and per-recipient projection state
    Inspect {
        message_id: String,
        #[arg(long)]
        json: bool,
    },
}
impl MessageArgs {
    pub fn suppresses_post_command_delivery(&self) -> bool {
        match &self.command {
            MessageCommand::Wait { json, .. }
            | MessageCommand::Pending { json }
            | MessageCommand::Cancel { json, .. }
            | MessageCommand::Supersede { json, .. }
            | MessageCommand::Retry { json, .. }
            | MessageCommand::Receipt { json, .. }
            | MessageCommand::Inspect { json, .. } => *json,
            MessageCommand::Reply { json, quiet, .. } => *json || *quiet,
        }
    }
}
fn actor(ctx: Option<&CommandContext>) -> Result<SenderIdentity, String> {
    ctx.and_then(|context| context.identity.clone())
        .ok_or_else(|| "message operations require a verified instance identity".to_string())
}

fn print_value(value: &Value, json_mode: bool) {
    if json_mode {
        println!("{}", serde_json::to_string(value).unwrap_or_default());
    } else {
        println!(
            "{}",
            serde_json::to_string_pretty(value).unwrap_or_default()
        );
    }
}

fn inspected_message<'a>(inspection: &'a Value, field: &str) -> Result<&'a Value, String> {
    inspection
        .get("message")
        .and_then(|message| message.get(field))
        .ok_or_else(|| format!("message projection missing {field}"))
}

fn inspection_targets(inspection: &Value) -> Result<Vec<String>, String> {
    inspection
        .get("deliveries")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.get("recipient_name").and_then(Value::as_str))
                .map(String::from)
                .collect::<Vec<_>>()
        })
        .filter(|targets| !targets.is_empty())
        .ok_or_else(|| "message has no recipient deliveries".to_string())
}

fn authorize_inspection(inspection: &Value, actor: &str) -> Result<(), String> {
    if inspected_message(inspection, "from")?.as_str() == Some(actor)
        || inspection
            .get("deliveries")
            .and_then(Value::as_array)
            .is_some_and(|rows| {
                rows.iter()
                    .any(|row| row.get("recipient_name").and_then(Value::as_str) == Some(actor))
            })
    {
        Ok(())
    } else {
        Err("message is not addressed to this actor".to_string())
    }
}

fn send_result_value(result: &crate::db::MessagePersistResult) -> Value {
    json!({
        "event_id": result.event_id,
        "message_id": result.message_id,
        "delivered_to": result.delivered_to,
        "recipient_endpoint_epochs": result.recipient_endpoint_epochs,
    })
}

fn send_reply(
    db: &HcomDb,
    actor: &SenderIdentity,
    request_id: &str,
    text: &str,
    attachments: &[String],
) -> Result<crate::db::MessagePersistResult, String> {
    let inspection = db
        .inspect_message_v1(request_id)
        .map_err(|error| error.to_string())?;
    authorize_inspection(&inspection, &actor.name)?;
    if inspected_message(&inspection, "intent")?.as_str() != Some("request")
        || inspected_message(&inspection, "expects_reply")?.as_bool() != Some(true)
    {
        return Err("reply target must be a request that expects a reply".to_string());
    }
    let request_sender = inspected_message(&inspection, "from")?
        .as_str()
        .ok_or_else(|| "request sender is invalid".to_string())?
        .to_string();
    let correlation_id = inspected_message(&inspection, "correlation_id")?
        .as_str()
        .ok_or_else(|| "request correlation is invalid".to_string())?
        .to_string();
    let parent_event_id = inspected_message(&inspection, "event_id")?
        .as_i64()
        .ok_or_else(|| "request event ID is invalid".to_string())?;
    let reply_mode = inspection
        .get("message")
        .and_then(|message| message.get("reply_mode"))
        .and_then(Value::as_str)
        .unwrap_or("inbox");
    let delivery_endpoint = match reply_mode {
        "inbox" => "inbox".to_string(),
        "wait" => format!("request:{request_id}"),
        other => return Err(format!("request has invalid reply mode: {other}")),
    };
    let mut envelope = MessageEnvelope {
        intent: "ack".parse().ok(),
        reply_to: Some(parent_event_id.to_string()),
        correlation_id: Some(correlation_id),
        in_reply_to: Some(request_id.to_string()),
        delivery_endpoint: Some(delivery_endpoint),
        attachments: normalize_attachments(attachments)?,
        ..Default::default()
    };
    envelope.thread = inspection
        .get("message")
        .and_then(|message| message.get("thread"))
        .and_then(Value::as_str)
        .map(String::from);
    send_message(db, actor, text, Some(&envelope), Some(&[request_sender]))
}

fn wait_for_reply(
    db: &HcomDb,
    actor: &SenderIdentity,
    message_id: &str,
    timeout: u64,
) -> Result<Option<Value>, String> {
    let inspection = db
        .inspect_message_v1(message_id)
        .map_err(|error| error.to_string())?;
    authorize_inspection(&inspection, &actor.name)?;
    if inspected_message(&inspection, "from")?.as_str() != Some(actor.name.as_str()) {
        return Err("wait is allowed only for the original sender".to_string());
    }
    if inspected_message(&inspection, "intent")?.as_str() != Some("request")
        || inspected_message(&inspection, "expects_reply")?.as_bool() != Some(true)
    {
        return Err("message is not a request that expects a reply".to_string());
    }
    let targets = inspection_targets(&inspection)?;
    if targets.len() != 1 {
        return Err("blocking wait requires exactly one request recipient".to_string());
    }
    if let Some(reply) = db
        .claim_reply_v1(message_id, &actor.name)
        .map_err(|error| error.to_string())?
    {
        return Ok(Some(reply));
    }
    if let Some((_, device_short_id)) = targets[0].rsplit_once(':') {
        crate::relay::control::require_remote_feature(db, device_short_id, "message-lifecycle-v1")?;
    }
    let server = NotifyServer::new().map_err(|error| format!("notify setup failed: {error}"))?;
    db.upsert_notify_endpoint(&actor.name, "message_wait", server.port())
        .map_err(|error| format!("notify registration failed: {error}"))?;
    let started = Instant::now();
    let outcome = loop {
        if let Some(reply) = db
            .claim_reply_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())?
        {
            break Ok(Some(reply));
        }
        let Some(remaining) = Duration::from_secs(timeout).checked_sub(started.elapsed()) else {
            db.append_ask_timeout_v1(message_id, &actor.name)
                .map_err(|error| error.to_string())?;
            break Ok(None);
        };
        if remaining.is_zero() {
            db.append_ask_timeout_v1(message_id, &actor.name)
                .map_err(|error| error.to_string())?;
            break Ok(None);
        }
        server.wait(remaining.min(Duration::from_secs(30)));
    };
    let _ = db.delete_notify_endpoint(&actor.name, "message_wait");
    outcome
}

fn preserved_envelope(
    inspection: &Value,
    parent_id: &str,
    lineage: &str,
) -> Result<MessageEnvelope, String> {
    let message = inspection
        .get("message")
        .and_then(Value::as_object)
        .ok_or_else(|| "message projection is invalid".to_string())?;
    let correlation_id = message
        .get("correlation_id")
        .and_then(Value::as_str)
        .ok_or_else(|| "message correlation is invalid".to_string())?
        .to_string();
    let intent = message
        .get("intent")
        .and_then(Value::as_str)
        .map(str::parse)
        .transpose()
        .map_err(|error: String| error)?;
    let attachments = message
        .get("attachments")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| format!("stored attachments are invalid: {error}"))?
        .unwrap_or_default();
    let deliveries = inspection
        .get("deliveries")
        .and_then(Value::as_array)
        .ok_or_else(|| "message deliveries are invalid".to_string())?;
    let mut endpoints = deliveries
        .iter()
        .filter_map(|row| row.get("delivery_endpoint").and_then(Value::as_str));
    let delivery_endpoint = endpoints
        .next()
        .ok_or_else(|| "message has no delivery endpoint".to_string())?
        .to_string();
    if endpoints.any(|endpoint| endpoint != delivery_endpoint) {
        return Err("message recipients have inconsistent delivery endpoints".to_string());
    }
    let mut envelope = MessageEnvelope {
        intent,
        thread: message
            .get("thread")
            .and_then(Value::as_str)
            .map(String::from),
        bundle_id: message
            .get("bundle_id")
            .and_then(Value::as_str)
            .map(String::from),
        correlation_id: Some(correlation_id),
        expects_reply: message
            .get("expects_reply")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        delivery_endpoint: Some(delivery_endpoint),
        attachments,
        ..Default::default()
    };
    match lineage {
        "supersedes" => envelope.supersedes = Some(parent_id.to_string()),
        "retry_of" => envelope.retry_of = Some(parent_id.to_string()),
        _ => return Err("invalid message lineage".to_string()),
    }
    Ok(envelope)
}

pub fn cmd_message(db: &HcomDb, args: &MessageArgs, ctx: Option<&CommandContext>) -> i32 {
    let actor = match actor(ctx) {
        Ok(actor) => actor,
        Err(error) => {
            eprintln!("Error: {error}");
            return 1;
        }
    };

    let result: Result<(Value, bool, bool), String> = match &args.command {
        MessageCommand::Wait {
            message_id,
            timeout,
            json,
        } => db
            .resolve_message_reference_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())
            .and_then(|canonical_id| wait_for_reply(db, &actor, &canonical_id, *timeout))
            .and_then(|reply| {
                reply
                    .map(|value| (value, *json, false))
                    .ok_or_else(|| "timed out waiting for reply".to_string())
            }),
        MessageCommand::Pending { json } => db
            .pending_messages_v1(&actor.name)
            .map(|rows| (json!(rows), *json, false))
            .map_err(|error| error.to_string()),
        MessageCommand::Reply {
            message_id,
            attachments,
            json,
            quiet,
            text,
        } => db
            .resolve_message_reference_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())
            .and_then(|canonical_id| {
                send_reply(db, &actor, &canonical_id, &text.join(" "), attachments)
            })
            .map(|sent| (send_result_value(&sent), *json, *quiet)),
        MessageCommand::Cancel { message_id, json } => db
            .resolve_message_reference_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())
            .and_then(|canonical_id| {
                db.cancel_message_v1(&canonical_id, &actor.name)
                    .map_err(|error| error.to_string())
            })
            .map(|value| (value, *json, false)),
        MessageCommand::Supersede {
            message_id,
            attachments,
            json,
            text,
        } => {
            db.resolve_message_reference_v1(message_id, &actor.name)
                .map_err(|error| error.to_string())
                .and_then(|canonical_id| {
                    let inspection = db
                        .inspect_message_v1(&canonical_id)
                        .map_err(|error| error.to_string())?;
                    authorize_inspection(&inspection, &actor.name)?;
                    if inspected_message(&inspection, "from")?.as_str()
                        != Some(actor.name.as_str())
                    {
                        return Err(
                            "supersede is allowed only for the original sender".to_string(),
                        );
                    }
                    if !attachments.is_empty() {
                        return Err("supersede preserves the original attachments; new attachments are not allowed".to_string());
                    }
                    let targets = inspection_targets(&inspection)?;
                    let envelope =
                        preserved_envelope(&inspection, &canonical_id, "supersedes")?;
                    send_message(db, &actor, &text.join(" "), Some(&envelope), Some(&targets))
                        .map(|sent| (send_result_value(&sent), *json, false))
                })
        }
        MessageCommand::Retry { message_id, json } => db
            .resolve_message_reference_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())
            .and_then(|canonical_id| {
                let inspection = db
                    .inspect_message_v1(&canonical_id)
                    .map_err(|error| error.to_string())?;
                authorize_inspection(&inspection, &actor.name)?;
                if inspected_message(&inspection, "from")?.as_str()
                    != Some(actor.name.as_str())
                {
                    return Err("retry is allowed only for the original sender".to_string());
                }
                let targets = inspection_targets(&inspection)?;
                let message = inspected_message(&inspection, "text")?
                    .as_str()
                    .ok_or_else(|| "message text is invalid".to_string())?;
                let envelope = preserved_envelope(&inspection, &canonical_id, "retry_of")?;
                send_message(db, &actor, message, Some(&envelope), Some(&targets))
                    .map(|sent| (send_result_value(&sent), *json, false))
            }),
        MessageCommand::Receipt {
            message_id,
            state,
            endpoint_epoch,
            reason,
            json,
        } => db
            .resolve_message_reference_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())
            .and_then(|canonical_id| {
                db.record_delivery_receipt(
                    &canonical_id,
                    &actor.name,
                    &actor.name,
                    endpoint_epoch,
                    state,
                    reason.as_deref().unwrap_or("explicit receipt"),
                )
                .map_err(|error| error.to_string())
            })
            .map(|receipt| {
                (
                    json!({
                        "message_id": receipt.message_id,
                        "recipient": receipt.recipient,
                        "prior_state": receipt.prior_state,
                        "state": receipt.state,
                        "state_event_id": receipt.state_event_id,
                        "idempotent": receipt.idempotent,
                    }),
                    *json,
                    false,
                )
            }),
        MessageCommand::Inspect { message_id, json } => db
            .resolve_message_reference_v1(message_id, &actor.name)
            .map_err(|error| error.to_string())
            .and_then(|canonical_id| {
                db.inspect_message_v1(&canonical_id)
                    .map_err(|error| error.to_string())
            })
            .and_then(|inspection| {
                authorize_inspection(&inspection, &actor.name)?;
                Ok((inspection, *json, false))
            }),
    };

    match result {
        Ok((value, json_mode, quiet)) => {
            if json_mode || !quiet {
                print_value(&value, json_mode);
            }
            crate::relay::worker::ensure_worker(true);
            0
        }
        Err(error) => {
            eprintln!("Error: {error}");
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn lifecycle_commands_require_verified_identity() {
        assert!(actor(None).is_err());
        let parsed = MessageArgs::try_parse_from(["message", "pending", "--json"]).unwrap();
        assert!(matches!(
            parsed.command,
            MessageCommand::Pending { json: true }
        ));
    }

    #[test]
    fn retry_and_supersede_envelopes_preserve_protocol_semantics() {
        let attachments = normalize_attachments(&[json!({
            "type": "context",
            "name": "context.txt",
            "content": "bounded context"
        })
        .to_string()])
        .unwrap();
        let inspection = json!({
            "message": {
                "message_id": "original",
                "correlation_id": "correlation",
                "intent": "request",
                "expects_reply": true,
                "thread": "thread-1",
                "bundle_id": "bundle-1",
                "attachments": attachments,
                "text": "original text"
            },
            "deliveries": [{
                "recipient_name": "bob",
                "delivery_endpoint": "inbox"
            }]
        });

        let retry = preserved_envelope(&inspection, "original", "retry_of").unwrap();
        assert_eq!(retry.intent.map(|value| value.as_str()), Some("request"));
        assert!(retry.expects_reply);
        assert_eq!(retry.thread.as_deref(), Some("thread-1"));
        assert_eq!(retry.bundle_id.as_deref(), Some("bundle-1"));
        assert_eq!(retry.attachments, attachments);
        assert_eq!(retry.delivery_endpoint.as_deref(), Some("inbox"));
        assert_eq!(retry.retry_of.as_deref(), Some("original"));
        assert!(
            retry.reply_endpoint.is_none(),
            "new request ID must own its endpoint"
        );

        let supersede = preserved_envelope(&inspection, "original", "supersedes").unwrap();
        assert_eq!(supersede.supersedes.as_deref(), Some("original"));
        assert_eq!(supersede.correlation_id.as_deref(), Some("correlation"));
        assert_eq!(supersede.attachments, attachments);
    }

    #[test]
    fn blocking_wait_feature_gates_legacy_remote_peer_before_sleeping() {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("message.db")).unwrap();
        db.conn()
            .execute(
                "INSERT INTO instances (
                    name, status, endpoint_epoch, origin_device_id, created_at
                 ) VALUES
                    ('alice', 'active', 'epoch-a', NULL, 1.0),
                    ('bob:OLDP', 'active', 'epoch-b', 'device-old', 1.0)",
                [],
            )
            .unwrap();
        crate::relay::safe_kv_set(&db, "relay_short_OLDP", Some("device-old"));
        crate::relay::safe_kv_set(&db, "relay_caps_device-old", Some("null"));
        let recipients = vec!["bob:OLDP".to_string()];
        let data = json!({
            "protocol": crate::db::MESSAGE_PROTOCOL_V1,
            "message_id": "legacy-wait",
            "correlation_id": "legacy-wait",
            "from": "alice",
            "scope": "mentions",
            "mentions": recipients,
            "delivered_to": recipients,
            "text": "question",
            "intent": "request",
            "expects_reply": true,
            "reply_endpoint": "request:legacy-wait",
            "delivery_endpoint": "inbox",
        });
        db.persist_message_v1(&crate::db::MessagePersistInput {
            routing_instance: "alice",
            sender_name: "alice",
            sender_session_id: None,
            data: &data,
            message_id: "legacy-wait",
            correlation_id: "legacy-wait",
            intent: Some("request"),
            expects_reply: true,
            in_reply_to: None,
            legacy_reply_to: None,
            reply_endpoint: Some("request:legacy-wait"),
            delivery_endpoint: "inbox",
            supersedes: None,
            retry_of: None,
            attachments_json: "[]",
            recipients: &recipients,
        })
        .unwrap();
        let actor = SenderIdentity {
            name: "alice".to_string(),
            kind: crate::shared::SenderKind::Instance,
            session_id: None,
            instance_data: None,
        };
        let started = Instant::now();
        let error = wait_for_reply(&db, &actor, "legacy-wait", 60).unwrap_err();
        assert!(error.contains("ordinary send/reply semantics"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
