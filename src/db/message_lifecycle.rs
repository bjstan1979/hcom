//! Durable per-message identity, delivery projections, and lifecycle transitions.

use anyhow::{Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

use super::{HcomDb, chrono_now_iso, subscriptions};

pub const MESSAGE_PROTOCOL_V1: &str = "hcom-message/v1";
pub const MESSAGE_FEATURES: &[&str] = &[
    "message-lifecycle-v1",
    "message-attachments-v1",
    "pi-rich-presence-v1",
];

/// Preserve upstream's "every ordinary message wakes its recipient" contract
/// while retaining an exclusive endpoint for explicit blocking waits. New
/// senders route ordinary replies directly to `inbox`; this compatibility path
/// also projects replies produced by older lifecycle peers from `request:<id>`
/// into the inbox unless the parent request explicitly selected `wait`.
fn delivery_routes_to_inbox(
    conn: &Connection,
    message_id: &str,
    delivery_endpoint: &str,
) -> Result<bool> {
    if delivery_endpoint == "inbox" {
        return Ok(true);
    }
    let Some(request_id) = delivery_endpoint.strip_prefix("request:") else {
        return Ok(false);
    };
    let reply_mode: Option<String> = conn
        .query_row(
            "SELECT COALESCE(json_extract(parent_event.data, '$.reply_mode'), 'inbox')
             FROM message_records reply
             JOIN message_records parent ON parent.message_id = reply.in_reply_to
             JOIN events parent_event ON parent_event.id = parent.event_id
             WHERE reply.message_id = ?1 AND reply.in_reply_to = ?2",
            params![message_id, request_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(reply_mode.as_deref() == Some("inbox"))
}

#[derive(Debug, Clone)]
pub struct MessagePersistInput<'a> {
    pub routing_instance: &'a str,
    pub sender_name: &'a str,
    pub sender_session_id: Option<&'a str>,
    pub data: &'a Value,
    pub message_id: &'a str,
    pub correlation_id: &'a str,
    pub intent: Option<&'a str>,
    pub expects_reply: bool,
    pub in_reply_to: Option<&'a str>,
    pub legacy_reply_to: Option<&'a str>,
    pub reply_endpoint: Option<&'a str>,
    pub delivery_endpoint: &'a str,
    pub supersedes: Option<&'a str>,
    pub retry_of: Option<&'a str>,
    pub attachments_json: &'a str,
    pub recipients: &'a [String],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessagePersistResult {
    pub event_id: i64,
    pub message_id: String,
    pub delivered_to: Vec<String>,
    pub recipient_endpoint_epochs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryReceiptResult {
    pub message_id: String,
    pub recipient: String,
    pub prior_state: String,
    pub state: String,
    pub state_event_id: Option<i64>,
    pub idempotent: bool,
}

fn insert_event(
    tx: &Transaction<'_>,
    timestamp: &str,
    event_type: &str,
    instance: &str,
    data: &Value,
) -> Result<i64> {
    tx.execute(
        "INSERT INTO events (timestamp, type, instance, data) VALUES (?1, ?2, ?3, ?4)",
        params![
            timestamp,
            event_type,
            instance,
            serde_json::to_string(data)?
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

#[allow(clippy::too_many_arguments)]
fn append_state_transition(
    tx: &Transaction<'_>,
    message_id: &str,
    recipient: &str,
    endpoint_epoch: &str,
    prior_state: Option<&str>,
    new_state: &str,
    attempt: i64,
    actor: &str,
    reason: &str,
    timestamp: &str,
    event_instance: &str,
    emitted: &mut Vec<(i64, String, Value)>,
) -> Result<i64> {
    let data = json!({
        "protocol": MESSAGE_PROTOCOL_V1,
        "message_id": message_id,
        "actor": actor,
        "recipient": recipient,
        "endpoint_epoch": endpoint_epoch,
        "prior_state": prior_state,
        "new_state": new_state,
        "attempt": attempt,
        "timestamp": timestamp,
        "reason": reason,
    });
    let event_id = insert_event(tx, timestamp, "message_state", event_instance, &data)?;
    emitted.push((event_id, event_instance.to_string(), data));
    Ok(event_id)
}

fn snapshot_epoch(tx: &Transaction<'_>, recipient: &str) -> Result<String> {
    Ok(tx
        .query_row(
            "SELECT endpoint_epoch FROM instances WHERE name = ?1",
            params![recipient],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_default())
}

fn supersession_state(prior: &str) -> Option<&'static str> {
    match prior {
        "queued" | "received" => Some("superseded"),
        "accepted" | "acknowledged" => Some("supersession_requested"),
        _ => None,
    }
}

fn cancellation_state(prior: &str) -> Option<&'static str> {
    match prior {
        "queued" => Some("cancelled"),
        "received" | "accepted" | "acknowledged" => Some("cancellation_requested"),
        _ => None,
    }
}

impl HcomDb {
    /// Persist the canonical message event and both projections in one immediate
    /// transaction. Subscription processing is intentionally deferred until the
    /// commit has released SQLite's write lock.
    pub fn persist_message_v1(
        &self,
        input: &MessagePersistInput<'_>,
    ) -> Result<MessagePersistResult> {
        let timestamp = chrono_now_iso();
        let mut emitted_states: Vec<(i64, String, Value)> = Vec::new();
        let mut endpoint_epochs = BTreeMap::new();
        let mut message_event_id = 0i64;

        self.with_immediate_transaction(|tx| {
            let recipient_set: BTreeSet<&str> = input.recipients.iter().map(String::as_str).collect();
            if recipient_set.len() != input.recipients.len() {
                bail!("message recipients must be unique");
            }
            if let Some(old_id) = input.retry_of {
                let old: Option<(String, Option<String>)> = tx
                    .query_row(
                        "SELECT sender_name, in_reply_to FROM message_records WHERE message_id = ?1",
                        params![old_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((old_sender, old_in_reply_to)) = old else {
                    bail!("retry target not found");
                };
                if old_sender != input.sender_name {
                    bail!("retry is allowed only for the original sender");
                }
                if old_in_reply_to.is_some() {
                    bail!("replies cannot be retried in message lifecycle V1");
                }
            }
            if let Some(old_id) = input.supersedes {
                let old: Option<(String, Option<String>)> = tx
                    .query_row(
                        "SELECT sender_name, in_reply_to FROM message_records WHERE message_id = ?1",
                        params![old_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((old_sender, old_in_reply_to)) = old else {
                    bail!("supersede target not found");
                };
                if old_sender != input.sender_name {
                    bail!("supersede is allowed only for the original sender");
                }
                if old_in_reply_to.is_some() {
                    bail!("replies cannot be superseded in message lifecycle V1");
                }
                let mut stmt = tx.prepare(
                    "SELECT recipient_name FROM message_deliveries
                     WHERE message_id = ?1 ORDER BY recipient_name",
                )?;
                let old_recipients = stmt
                    .query_map(params![old_id], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<BTreeSet<_>>>()?;
                drop(stmt);
                let new_recipients: BTreeSet<String> = input.recipients.iter().cloned().collect();
                if old_recipients != new_recipients {
                    bail!("supersede must preserve the identical recipient set");
                }
            }
            if let Some(target_id) = input.in_reply_to {
                let target: Option<(String, String, i64, Option<String>, bool, String)> = tx
                    .query_row(
                        "SELECT mr.sender_name, mr.correlation_id, mr.event_id, mr.intent,
                                mr.expects_reply,
                                COALESCE(json_extract(e.data, '$.reply_mode'), 'inbox')
                         FROM message_records mr
                         JOIN events e ON e.id = mr.event_id
                         WHERE mr.message_id = ?1",
                        params![target_id],
                        |row| {
                            Ok((
                                row.get(0)?,
                                row.get(1)?,
                                row.get(2)?,
                                row.get(3)?,
                                row.get::<_, i64>(4)? != 0,
                                row.get(5)?,
                            ))
                        },
                    )
                    .optional()?;
                let Some((
                    target_sender,
                    target_correlation,
                    target_event_id,
                    target_intent,
                    target_expects_reply,
                    target_reply_mode,
                )) = target
                else {
                    bail!("reply target not found");
                };
                if target_intent.as_deref() != Some("request") || !target_expects_reply {
                    bail!("reply target must be a request that expects a reply");
                }
                if input.intent != Some("ack") {
                    bail!("request replies must use ack intent");
                }
                if target_correlation != input.correlation_id {
                    bail!("reply correlation does not match request");
                }
                if input.recipients != [target_sender] {
                    bail!("reply must address exactly the original sender");
                }
                let endpoint_matches = match target_reply_mode.as_str() {
                    "inbox" => {
                        input.delivery_endpoint == "inbox"
                            || input.delivery_endpoint == format!("request:{target_id}")
                    }
                    "wait" => input.delivery_endpoint == format!("request:{target_id}"),
                    other => bail!("reply target has invalid reply mode: {other}"),
                };
                if !endpoint_matches {
                    bail!("reply delivery endpoint does not match request reply mode");
                }
                if input.legacy_reply_to != Some(target_event_id.to_string().as_str()) {
                    bail!("reply must retain the parent event ID as legacy reply_to");
                }
                let delivery: Option<(String, String, i64)> = tx
                    .query_row(
                        "SELECT state, endpoint_epoch, attempt FROM message_deliveries
                         WHERE message_id = ?1 AND recipient_name = ?2",
                        params![target_id, input.sender_name],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .optional()?;
                let Some((prior, endpoint_epoch, attempt)) = delivery else {
                    bail!("reply actor is not a recipient of the request");
                };
                if matches!(
                    prior.as_str(),
                    "replied" | "cancelled" | "superseded" | "delivery_failed"
                ) {
                    bail!("request is no longer replyable ({prior})");
                }
                let state_event_id = append_state_transition(
                    tx,
                    target_id,
                    input.sender_name,
                    &endpoint_epoch,
                    Some(&prior),
                    "replied",
                    attempt,
                    input.sender_name,
                    "reply committed",
                    &timestamp,
                    input.sender_name,
                    &mut emitted_states,
                )?;
                let changed = tx.execute(
                    "UPDATE message_deliveries
                     SET state = 'replied', state_event_id = ?1, last_actor = ?2, updated_at = ?3
                     WHERE message_id = ?4 AND recipient_name = ?2 AND state = ?5",
                    params![state_event_id, input.sender_name, timestamp, target_id, prior],
                )?;
                if changed != 1 {
                    bail!("concurrent reply state transition");
                }
            }

            message_event_id = insert_event(
                tx,
                &timestamp,
                "message",
                input.routing_instance,
                input.data,
            )?;
            tx.execute(
                "INSERT INTO message_records (
                    message_id, event_id, correlation_id, sender_name, sender_session_id,
                    intent, expects_reply, in_reply_to, legacy_reply_to, reply_endpoint,
                    supersedes, retry_of, attachments_json, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    input.message_id,
                    message_event_id,
                    input.correlation_id,
                    input.sender_name,
                    input.sender_session_id,
                    input.intent,
                    i64::from(input.expects_reply),
                    input.in_reply_to,
                    input.legacy_reply_to,
                    input.reply_endpoint,
                    input.supersedes,
                    input.retry_of,
                    input.attachments_json,
                    timestamp,
                ],
            )?;

            let attempt = if input.retry_of.is_some() {
                let mut max_attempt = 0i64;
                for recipient in input.recipients {
                    let recipient_max = tx.query_row(
                        "SELECT COALESCE(MAX(md.attempt), 0)
                         FROM message_records mr
                         JOIN message_deliveries md ON md.message_id = mr.message_id
                         WHERE mr.correlation_id = ?1
                           AND mr.sender_name = ?2
                           AND md.recipient_name = ?3",
                        params![input.correlation_id, input.sender_name, recipient],
                        |row| row.get::<_, i64>(0),
                    )?;
                    max_attempt = max_attempt.max(recipient_max);
                }
                max_attempt + 1
            } else {
                1
            };

            for recipient in input.recipients {
                let epoch = snapshot_epoch(tx, recipient)?;
                endpoint_epochs.insert(recipient.clone(), epoch.clone());
                tx.execute(
                    "INSERT INTO message_deliveries (
                        message_id, recipient_name, endpoint_epoch, delivery_endpoint,
                        state, attempt, state_event_id, last_actor, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, 'queued', ?5, NULL, ?6, ?7)",
                    params![
                        input.message_id,
                        recipient,
                        epoch,
                        input.delivery_endpoint,
                        attempt,
                        input.sender_name,
                        timestamp,
                    ],
                )?;
                let state_event_id = append_state_transition(
                    tx,
                    input.message_id,
                    recipient,
                    &epoch,
                    None,
                    "queued",
                    attempt,
                    input.sender_name,
                    "message committed",
                    &timestamp,
                    input.routing_instance,
                    &mut emitted_states,
                )?;
                tx.execute(
                    "UPDATE message_deliveries SET state_event_id = ?1
                     WHERE message_id = ?2 AND recipient_name = ?3",
                    params![state_event_id, input.message_id, recipient],
                )?;
            }

            if let Some(old_id) = input.supersedes {
                let mut valid_transitions = 0usize;
                let mut stmt = tx.prepare(
                    "SELECT recipient_name, endpoint_epoch, state, attempt
                     FROM message_deliveries WHERE message_id = ?1",
                )?;
                let rows = stmt
                    .query_map(params![old_id], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                drop(stmt);
                for (recipient, epoch, prior, attempt) in rows {
                    let Some(next) = supersession_state(&prior) else {
                        continue;
                    };
                    valid_transitions += 1;
                    let state_event_id = append_state_transition(
                        tx,
                        old_id,
                        &recipient,
                        &epoch,
                        Some(&prior),
                        next,
                        attempt,
                        input.sender_name,
                        &format!("superseded by {}", input.message_id),
                        &timestamp,
                        input.sender_name,
                        &mut emitted_states,
                    )?;
                    tx.execute(
                        "UPDATE message_deliveries SET state = ?1, state_event_id = ?2,
                         last_actor = ?3, updated_at = ?4
                         WHERE message_id = ?5 AND recipient_name = ?6 AND state = ?7",
                        params![
                            next,
                            state_event_id,
                            input.sender_name,
                            timestamp,
                            old_id,
                            recipient,
                            prior,
                        ],
                    )?;
                }
                if valid_transitions == 0 {
                    bail!("supersede requires at least one active delivery transition");
                }
            }
            Ok(())
        })?;

        if let Some(old_id) = input.supersedes {
            self.clear_request_watches_v1(old_id, None)?;
        }

        subscriptions::process_logged_event(
            self,
            message_event_id,
            "message",
            input.routing_instance,
            input.data,
        );
        for (event_id, instance, data) in emitted_states {
            subscriptions::process_logged_event(self, event_id, "message_state", &instance, &data);
        }

        Ok(MessagePersistResult {
            event_id: message_event_id,
            message_id: input.message_id.to_string(),
            delivered_to: input.recipients.to_vec(),
            recipient_endpoint_epochs: endpoint_epochs,
        })
    }

    /// Return whether a projected delivery belongs to the ordinary inbox. A
    /// missing projection is `None` so callers can retain legacy cursor routing.
    pub fn projected_inbox_delivery(&self, message_id: &str, recipient: &str) -> Option<bool> {
        self.conn()
            .query_row(
                "SELECT delivery_endpoint, state FROM message_deliveries
                 WHERE message_id = ?1 AND recipient_name = ?2",
                params![message_id, recipient],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .ok()
            .flatten()
            .map(|(endpoint, state)| {
                delivery_routes_to_inbox(self.conn(), message_id, &endpoint).unwrap_or(false)
                    && !matches!(
                        state.as_str(),
                        "accepted"
                            | "acknowledged"
                            | "replied"
                            | "cancelled"
                            | "superseded"
                            | "delivery_failed"
                    )
            })
    }

    /// Pi owns a durable fetch/accept/ACK transaction. During plugin reload it must
    /// be able to recover both fetched-but-not-injected (`received`) deliveries and
    /// accepted-but-not-ACKed deliveries. Ordinary unread consumers intentionally
    /// continue to hide `accepted` messages.
    pub fn projected_pi_recoverable_inbox_delivery(
        &self,
        message_id: &str,
        recipient: &str,
    ) -> Option<bool> {
        self.conn()
            .query_row(
                "SELECT delivery_endpoint, state FROM message_deliveries
                 WHERE message_id = ?1 AND recipient_name = ?2",
                params![message_id, recipient],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .ok()
            .flatten()
            .map(|(endpoint, state)| {
                delivery_routes_to_inbox(self.conn(), message_id, &endpoint).unwrap_or(false)
                    && matches!(state.as_str(), "queued" | "received" | "accepted")
            })
    }

    fn transition_delivery_chain(
        &self,
        message_id: &str,
        recipient: &str,
        actor: &str,
        endpoint_epoch: &str,
        requested_state: &str,
        reason: &str,
    ) -> Result<DeliveryReceiptResult> {
        let timestamp = chrono_now_iso();
        let mut emitted = Vec::new();
        let mut result = None;
        self.with_immediate_transaction(|tx| {
            let row: Option<(String, String, i64)> = tx
                .query_row(
                    "SELECT state, endpoint_epoch, attempt FROM message_deliveries
                     WHERE message_id = ?1 AND recipient_name = ?2",
                    params![message_id, recipient],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()?;
            let Some((mut state, snapshot, attempt)) = row else {
                bail!("message delivery not found");
            };
            if actor != recipient {
                bail!("delivery receipts may only be written by the recipient");
            }
            if snapshot != endpoint_epoch {
                bail!("stale endpoint epoch");
            }
            let current_epoch = snapshot_epoch(tx, recipient)?;
            if current_epoch != snapshot {
                bail!("stale endpoint epoch");
            }
            let target_rank = match requested_state {
                "received" => 1,
                "accepted" => 2,
                "acknowledged" => 3,
                _ => bail!("unsupported receipt state: {requested_state}"),
            };
            let current_rank = match state.as_str() {
                "queued" => 0,
                "received" => 1,
                "accepted" => 2,
                "acknowledged" => 3,
                terminal => bail!("delivery is not receiptable ({terminal})"),
            };
            if current_rank >= target_rank {
                result = Some(DeliveryReceiptResult {
                    message_id: message_id.to_string(),
                    recipient: recipient.to_string(),
                    prior_state: state.clone(),
                    state: state.clone(),
                    state_event_id: None,
                    idempotent: true,
                });
                return Ok(());
            }
            let initial = state.clone();
            let chain = ["received", "accepted", "acknowledged"];
            let mut last_event_id = None;
            for next in chain.iter().take(target_rank).skip(current_rank) {
                let event_id = append_state_transition(
                    tx,
                    message_id,
                    recipient,
                    &snapshot,
                    Some(&state),
                    next,
                    attempt,
                    actor,
                    reason,
                    &timestamp,
                    recipient,
                    &mut emitted,
                )?;
                let changed = tx.execute(
                    "UPDATE message_deliveries SET state = ?1, state_event_id = ?2,
                     last_actor = ?3, updated_at = ?4
                     WHERE message_id = ?5 AND recipient_name = ?6 AND state = ?7",
                    params![
                        next, event_id, actor, timestamp, message_id, recipient, state
                    ],
                )?;
                if changed != 1 {
                    bail!("concurrent delivery state transition");
                }
                state = (*next).to_string();
                last_event_id = Some(event_id);
            }
            result = Some(DeliveryReceiptResult {
                message_id: message_id.to_string(),
                recipient: recipient.to_string(),
                prior_state: initial,
                state,
                state_event_id: last_event_id,
                idempotent: false,
            });
            Ok(())
        })?;
        for (event_id, instance, data) in emitted {
            subscriptions::process_logged_event(self, event_id, "message_state", &instance, &data);
        }
        result.ok_or_else(|| anyhow!("delivery transition produced no result"))
    }

    pub fn record_delivery_receipt(
        &self,
        message_id: &str,
        recipient: &str,
        actor: &str,
        endpoint_epoch: &str,
        state: &str,
        reason: &str,
    ) -> Result<DeliveryReceiptResult> {
        self.transition_delivery_chain(message_id, recipient, actor, endpoint_epoch, state, reason)
    }

    /// Claim projected inbox deliveries before exposing them to the current endpoint.
    /// Missing projections are legacy messages and remain visible to the caller.
    pub fn claim_inbox_received(
        &self,
        recipient: &str,
        message_ids: &[String],
    ) -> Result<BTreeSet<String>> {
        let timestamp = chrono_now_iso();
        let mut emitted = Vec::new();
        let mut rebound = Vec::new();
        let mut visible = BTreeSet::new();
        self.with_immediate_transaction(|tx| {
            let current_epoch: String = tx
                .query_row(
                    "SELECT endpoint_epoch FROM instances WHERE name = ?1",
                    params![recipient],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| anyhow!("verified inbox recipient not found"))?;
            for message_id in message_ids {
                let projection: Option<(String, String, String, i64)> = tx
                    .query_row(
                        "SELECT delivery_endpoint, state, endpoint_epoch, attempt
                         FROM message_deliveries
                         WHERE message_id = ?1 AND recipient_name = ?2",
                        params![message_id, recipient],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?;
                let Some((endpoint, state, snapshot_epoch, attempt)) = projection else {
                    continue;
                };
                if !delivery_routes_to_inbox(tx, message_id, &endpoint)? {
                    continue;
                }
                if state == "queued" {
                    let event_id = append_state_transition(
                        tx,
                        message_id,
                        recipient,
                        &current_epoch,
                        Some("queued"),
                        "received",
                        attempt,
                        recipient,
                        if snapshot_epoch == current_epoch {
                            "inbox fetch"
                        } else {
                            "inbox fetch rebound queued delivery to current endpoint"
                        },
                        &timestamp,
                        recipient,
                        &mut emitted,
                    )?;
                    let changed = tx.execute(
                        "UPDATE message_deliveries
                         SET state = 'received', endpoint_epoch = ?1, state_event_id = ?2,
                             last_actor = ?3, updated_at = ?4
                         WHERE message_id = ?5 AND recipient_name = ?3 AND state = 'queued'",
                        params![current_epoch, event_id, recipient, timestamp, message_id],
                    )?;
                    if changed != 1 {
                        bail!("concurrent inbox claim");
                    }
                    visible.insert(message_id.clone());
                    continue;
                }
                if matches!(state.as_str(), "received" | "accepted")
                    && snapshot_epoch != current_epoch
                {
                    let changed = tx.execute(
                        "UPDATE message_deliveries
                         SET endpoint_epoch = ?1, last_actor = ?2, updated_at = ?3
                         WHERE message_id = ?4 AND recipient_name = ?2
                           AND state = ?5 AND endpoint_epoch = ?6",
                        params![
                            current_epoch,
                            recipient,
                            timestamp,
                            message_id,
                            state,
                            snapshot_epoch,
                        ],
                    )?;
                    if changed != 1 {
                        bail!("concurrent inbox recovery rebind");
                    }
                    rebound.push((
                        message_id.clone(),
                        state.clone(),
                        snapshot_epoch,
                        current_epoch.clone(),
                    ));
                    visible.insert(message_id.clone());
                    continue;
                }
                if snapshot_epoch == current_epoch
                    && !matches!(
                        state.as_str(),
                        "cancelled" | "superseded" | "delivery_failed"
                    )
                {
                    visible.insert(message_id.clone());
                }
            }
            Ok(())
        })?;
        for (message_id, state, old_epoch, new_epoch) in rebound {
            crate::log::log_info(
                "delivery",
                "message.endpoint_rebound",
                &format!(
                    "message_id={message_id} recipient={recipient} state={state} old_epoch={old_epoch} new_epoch={new_epoch}"
                ),
            );
        }
        for (event_id, instance, data) in emitted {
            subscriptions::process_logged_event(self, event_id, "message_state", &instance, &data);
        }
        Ok(visible)
    }

    /// Atomically append Pi inbox receipt transitions and advance its event cursor.
    /// Subscription processing happens only after the write transaction commits.
    pub fn acknowledge_inbox_and_advance_cursor(
        &self,
        recipient: &str,
        event_id: i64,
    ) -> Result<usize> {
        let timestamp = chrono_now_iso();
        let mut emitted = Vec::new();
        let mut changed = 0usize;
        self.with_immediate_transaction(|tx| {
            let current_epoch: String = tx
                .query_row(
                    "SELECT endpoint_epoch FROM instances WHERE name = ?1",
                    params![recipient],
                    |row| row.get(0),
                )
                .optional()?
                .ok_or_else(|| anyhow!("verified inbox recipient not found"))?;
            let rows = {
                let mut stmt = tx.prepare(
                    "SELECT mr.message_id, md.state, md.endpoint_epoch, md.attempt,
                            md.delivery_endpoint
                     FROM message_records mr
                     JOIN message_deliveries md ON md.message_id = mr.message_id
                     WHERE md.recipient_name = ?1
                       AND mr.event_id <= ?2
                     ORDER BY mr.event_id",
                )?;
                stmt.query_map(params![recipient, event_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (message_id, mut state, snapshot_epoch, attempt, delivery_endpoint) in rows {
                if !delivery_routes_to_inbox(tx, &message_id, &delivery_endpoint)? {
                    continue;
                }
                if matches!(
                    state.as_str(),
                    "cancelled"
                        | "superseded"
                        | "delivery_failed"
                        | "cancellation_requested"
                        | "supersession_requested"
                        | "replied"
                ) {
                    continue;
                }
                if snapshot_epoch != current_epoch {
                    if state == "queued" {
                        bail!("queued delivery belongs to a stale endpoint; fetch before ACK");
                    }
                    continue;
                }
                let current_rank = match state.as_str() {
                    "queued" => 0,
                    "received" => 1,
                    "accepted" => 2,
                    "acknowledged" => 3,
                    terminal => bail!("delivery is not inbox-acknowledgeable ({terminal})"),
                };
                if current_rank == 3 {
                    continue;
                }
                for next in ["received", "accepted", "acknowledged"]
                    .iter()
                    .skip(current_rank)
                {
                    let transition_id = append_state_transition(
                        tx,
                        &message_id,
                        recipient,
                        &current_epoch,
                        Some(&state),
                        next,
                        attempt,
                        recipient,
                        "Pi accepted inbox delivery",
                        &timestamp,
                        recipient,
                        &mut emitted,
                    )?;
                    let updated = tx.execute(
                        "UPDATE message_deliveries
                         SET state = ?1, state_event_id = ?2, last_actor = ?3, updated_at = ?4
                         WHERE message_id = ?5 AND recipient_name = ?3 AND state = ?6
                           AND endpoint_epoch = ?7 AND delivery_endpoint = ?8",
                        params![
                            next,
                            transition_id,
                            recipient,
                            timestamp,
                            message_id,
                            state,
                            current_epoch,
                            delivery_endpoint,
                        ],
                    )?;
                    if updated != 1 {
                        bail!("concurrent inbox acknowledgement");
                    }
                    state = (*next).to_string();
                    changed += 1;
                }
            }
            let cursor_updated = tx.execute(
                "UPDATE instances
                 SET last_event_id = MAX(COALESCE(last_event_id, 0), ?1)
                 WHERE name = ?2",
                params![event_id, recipient],
            )?;
            if cursor_updated != 1 {
                bail!("verified inbox recipient not found");
            }
            Ok(())
        })?;
        for (transition_id, instance, data) in emitted {
            subscriptions::process_logged_event(
                self,
                transition_id,
                "message_state",
                &instance,
                &data,
            );
        }
        Ok(changed)
    }
    fn clear_request_watches_v1(&self, message_id: &str, recipient: Option<&str>) -> Result<usize> {
        let event_id: Option<i64> = self
            .conn()
            .query_row(
                "SELECT event_id FROM message_records WHERE message_id = ?1",
                params![message_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(event_id) = event_id else {
            return Ok(0);
        };
        let deleted = if let Some(recipient) = recipient {
            self.conn().execute(
                "DELETE FROM kv
                 WHERE key LIKE 'events_sub:reqwatch-%'
                   AND CAST(json_extract(value, '$.filters.request_id') AS INTEGER) = ?1
                   AND json_extract(value, '$.filters.target') = ?2",
                params![event_id, recipient],
            )?
        } else {
            self.conn().execute(
                "DELETE FROM kv
                 WHERE key LIKE 'events_sub:reqwatch-%'
                   AND CAST(json_extract(value, '$.filters.request_id') AS INTEGER) = ?1",
                params![event_id],
            )?
        };
        Ok(deleted)
    }

    pub fn cancel_message_v1(&self, message_id: &str, actor: &str) -> Result<Value> {
        self.sender_transition(message_id, actor, "cancel")
    }

    fn sender_transition(&self, message_id: &str, actor: &str, operation: &str) -> Result<Value> {
        let timestamp = chrono_now_iso();
        let mut emitted = Vec::new();
        let mut outcomes = Vec::new();
        let mut terminal_recipients = Vec::new();
        self.with_immediate_transaction(|tx| {
            let sender: Option<String> = tx
                .query_row(
                    "SELECT sender_name FROM message_records WHERE message_id = ?1",
                    params![message_id],
                    |row| row.get(0),
                )
                .optional()?;
            if sender.as_deref() != Some(actor) {
                bail!("{operation} is allowed only for the original sender");
            }
            let mut stmt = tx.prepare(
                "SELECT recipient_name, endpoint_epoch, state, attempt
                 FROM message_deliveries WHERE message_id = ?1 ORDER BY recipient_name",
            )?;
            let rows = stmt
                .query_map(params![message_id], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(stmt);
            for (recipient, epoch, prior, attempt) in rows {
                let next = match operation {
                    "cancel" => cancellation_state(&prior),
                    _ => None,
                };
                let Some(next) = next else {
                    outcomes
                        .push(json!({"recipient": recipient, "state": prior, "changed": false}));
                    continue;
                };
                let event_id = append_state_transition(
                    tx,
                    message_id,
                    &recipient,
                    &epoch,
                    Some(&prior),
                    next,
                    attempt,
                    actor,
                    operation,
                    &timestamp,
                    actor,
                    &mut emitted,
                )?;
                let changed = tx.execute(
                    "UPDATE message_deliveries SET state = ?1, state_event_id = ?2,
                     last_actor = ?3, updated_at = ?4
                     WHERE message_id = ?5 AND recipient_name = ?6 AND state = ?7",
                    params![
                        next, event_id, actor, timestamp, message_id, recipient, prior
                    ],
                )?;
                if changed != 1 {
                    bail!("concurrent delivery state transition");
                }
                if next == "cancelled" {
                    terminal_recipients.push(recipient.clone());
                }
                outcomes.push(json!({"recipient": recipient, "state": next, "changed": true}));
            }
            Ok(())
        })?;
        for recipient in &terminal_recipients {
            self.clear_request_watches_v1(message_id, Some(recipient))?;
        }
        for (event_id, instance, data) in emitted {
            subscriptions::process_logged_event(self, event_id, "message_state", &instance, &data);
        }
        Ok(json!({"message_id": message_id, "operation": operation, "deliveries": outcomes}))
    }

    pub fn inspect_message_v1(&self, message_id: &str) -> Result<Value> {
        let record: Option<(i64, String)> = self
            .conn()
            .query_row(
                "SELECT event_id, data FROM message_records
                 JOIN events ON events.id = message_records.event_id
                 WHERE message_records.message_id = ?1",
                params![message_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((event_id, data)) = record else {
            bail!("message not found");
        };
        let mut message: Value = serde_json::from_str(&data)?;
        message["event_id"] = json!(event_id);
        let mut stmt = self.conn().prepare(
            "SELECT recipient_name, endpoint_epoch, delivery_endpoint, state, attempt,
                    state_event_id, last_actor, updated_at
             FROM message_deliveries WHERE message_id = ?1 ORDER BY recipient_name",
        )?;
        let deliveries = stmt
            .query_map(params![message_id], |row| {
                Ok(json!({
                    "recipient_name": row.get::<_, String>(0)?,
                    "endpoint_epoch": row.get::<_, String>(1)?,
                    "delivery_endpoint": row.get::<_, String>(2)?,
                    "state": row.get::<_, String>(3)?,
                    "attempt": row.get::<_, i64>(4)?,
                    "state_event_id": row.get::<_, Option<i64>>(5)?,
                    "last_actor": row.get::<_, String>(6)?,
                    "updated_at": row.get::<_, String>(7)?,
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(json!({"message": message, "deliveries": deliveries}))
    }
    /// Resolve a lifecycle command reference to its canonical stable message ID.
    /// Stable IDs take precedence; a local numeric event ID is accepted only when
    /// no stable ID with that exact text exists. Missing and unauthorized records
    /// deliberately share one error to avoid leaking message existence.
    pub fn resolve_message_reference_v1(&self, reference: &str, actor: &str) -> Result<String> {
        let exact: Option<(String, bool)> = self
            .conn()
            .query_row(
                "SELECT mr.message_id,
                        (mr.sender_name = ?2 OR EXISTS (
                            SELECT 1 FROM message_deliveries md
                            WHERE md.message_id = mr.message_id
                              AND md.recipient_name = ?2
                        ))
                 FROM message_records mr WHERE mr.message_id = ?1",
                params![reference, actor],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?;
        if let Some((message_id, authorized)) = exact {
            if authorized {
                return Ok(message_id);
            }
            bail!("message not found");
        }

        let Ok(event_id) = reference.parse::<i64>() else {
            bail!("message not found");
        };
        let by_event: Option<(String, bool)> = self
            .conn()
            .query_row(
                "SELECT mr.message_id,
                        (mr.sender_name = ?2 OR EXISTS (
                            SELECT 1 FROM message_deliveries md
                            WHERE md.message_id = mr.message_id
                              AND md.recipient_name = ?2
                        ))
                 FROM message_records mr WHERE mr.event_id = ?1",
                params![event_id, actor],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?;
        match by_event {
            Some((message_id, true)) => Ok(message_id),
            _ => bail!("message not found"),
        }
    }

    pub fn pending_messages_v1(&self, actor: &str) -> Result<Vec<Value>> {
        let mut stmt = self.conn().prepare(
            "SELECT mr.message_id
             FROM message_records mr
             JOIN message_deliveries md ON md.message_id = mr.message_id
             WHERE md.recipient_name = ?1
               AND mr.intent = 'request'
               AND mr.expects_reply = 1
               AND md.state NOT IN ('replied', 'cancelled', 'superseded', 'delivery_failed')
               AND NOT EXISTS (
                   SELECT 1 FROM message_records reply
                   WHERE reply.in_reply_to = mr.message_id
                     AND reply.sender_name = ?1
               )
             ORDER BY mr.event_id",
        )?;
        let ids = stmt
            .query_map(params![actor], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt);
        ids.into_iter()
            .map(|id| self.inspect_message_v1(&id))
            .collect()
    }

    pub fn find_reply_v1(&self, request_id: &str, actor: &str) -> Result<Option<Value>> {
        let sender: Option<String> = self
            .conn()
            .query_row(
                "SELECT sender_name FROM message_records WHERE message_id = ?1",
                params![request_id],
                |row| row.get(0),
            )
            .optional()?;
        if sender.as_deref() != Some(actor) {
            bail!("wait is allowed only for the original sender");
        }
        let reply_id: Option<String> = self
            .conn()
            .query_row(
                "SELECT reply.message_id
                 FROM message_records reply
                 JOIN message_deliveries delivery ON delivery.message_id = reply.message_id
                 WHERE reply.in_reply_to = ?1
                   AND delivery.recipient_name = ?2
                   AND delivery.delivery_endpoint IN ('inbox', ('request:' || ?1))
                   AND delivery.state NOT IN (
                       'acknowledged', 'replied', 'cancelled', 'superseded', 'delivery_failed'
                   )
                 ORDER BY reply.event_id LIMIT 1",
                params![request_id, actor],
                |row| row.get(0),
            )
            .optional()?;
        reply_id.map(|id| self.inspect_message_v1(&id)).transpose()
    }

    pub fn claim_reply_v1(&self, request_id: &str, actor: &str) -> Result<Option<Value>> {
        let Some(reply) = self.find_reply_v1(request_id, actor)? else {
            return Ok(None);
        };
        let message_id = reply["message"]["message_id"]
            .as_str()
            .ok_or_else(|| anyhow!("reply projection missing message_id"))?;
        let endpoint_epoch = reply["deliveries"]
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row["endpoint_epoch"].as_str())
            .unwrap_or("");
        self.record_delivery_receipt(
            message_id,
            actor,
            actor,
            endpoint_epoch,
            "acknowledged",
            "request waiter claimed reply",
        )?;
        self.inspect_message_v1(message_id).map(Some)
    }

    pub fn append_ask_timeout_v1(&self, message_id: &str, actor: &str) -> Result<i64> {
        let timestamp = chrono_now_iso();
        let data = json!({
            "protocol": MESSAGE_PROTOCOL_V1,
            "message_id": message_id,
            "actor": actor,
            "new_state": "ask_timed_out",
            "timestamp": timestamp,
            "reason": "wait timeout",
        });
        let event_id = self.with_immediate_transaction(|tx| {
            let sender: Option<String> = tx
                .query_row(
                    "SELECT sender_name FROM message_records WHERE message_id = ?1",
                    params![message_id],
                    |row| row.get(0),
                )
                .optional()?;
            if sender.as_deref() != Some(actor) {
                bail!("wait is allowed only for the original sender");
            }
            insert_event(tx, &timestamp, "message_state", actor, &data)
        })?;
        subscriptions::process_logged_event(self, event_id, "message_state", actor, &data);
        Ok(event_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (HcomDb, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = HcomDb::open_at(&dir.path().join("hcom.db")).unwrap();
        for (name, epoch) in [
            ("alice", "epoch-alice"),
            ("bob", "epoch-bob"),
            ("carol", "epoch-carol"),
        ] {
            db.conn()
                .execute(
                    "INSERT INTO instances (name, status, endpoint_epoch, created_at)
                     VALUES (?1, 'active', ?2, 1.0)",
                    params![name, epoch],
                )
                .unwrap();
        }
        (db, dir)
    }

    #[allow(clippy::too_many_arguments)]
    fn persist(
        db: &HcomDb,
        message_id: &str,
        sender: &str,
        recipients: &[&str],
        intent: Option<&str>,
        expects_reply: bool,
        correlation_id: &str,
        in_reply_to: Option<&str>,
        legacy_reply_to: Option<&str>,
        delivery_endpoint: &str,
        reply_endpoint: Option<&str>,
        supersedes: Option<&str>,
        retry_of: Option<&str>,
    ) -> Result<MessagePersistResult> {
        let recipients: Vec<String> = recipients
            .iter()
            .map(|value| (*value).to_string())
            .collect();
        let mut data = json!({
            "protocol": MESSAGE_PROTOCOL_V1,
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
        // Lifecycle unit fixtures model explicit blocking wait requests unless a
        // test overwrites this field. Public `hcom send --intent request`
        // defaults to ordinary inbox delivery.
        if intent == Some("request") && expects_reply {
            data["reply_mode"] = json!("wait");
        }
        if let Some(value) = in_reply_to {
            data["in_reply_to"] = json!(value);
        }
        if let Some(value) = legacy_reply_to {
            data["reply_to"] = json!(value);
            if let Ok(event_id) = value.parse::<i64>() {
                data["reply_to_local"] = json!(event_id);
            }
        }
        if let Some(value) = reply_endpoint {
            data["reply_endpoint"] = json!(value);
        }
        if let Some(value) = supersedes {
            data["supersedes"] = json!(value);
        }
        if let Some(value) = retry_of {
            data["retry_of"] = json!(value);
        }
        db.persist_message_v1(&MessagePersistInput {
            routing_instance: sender,
            sender_name: sender,
            sender_session_id: None,
            data: &data,
            message_id,
            correlation_id,
            intent,
            expects_reply,
            in_reply_to,
            legacy_reply_to,
            reply_endpoint,
            delivery_endpoint,
            supersedes,
            retry_of,
            attachments_json: "[]",
            recipients: &recipients,
        })
    }

    fn state(db: &HcomDb, message_id: &str, recipient: &str) -> String {
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
    fn lifecycle_references_accept_authorized_local_event_ids_without_leaking() {
        let (db, _dir) = setup();
        let first = persist(
            &db,
            "stable-alpha",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "stable-alpha",
            None,
            None,
            "inbox",
            Some("request:stable-alpha"),
            None,
            None,
        )
        .unwrap();
        let event_reference = first.event_id.to_string();

        assert_eq!(
            db.resolve_message_reference_v1("stable-alpha", "bob")
                .unwrap(),
            "stable-alpha"
        );
        assert_eq!(
            db.resolve_message_reference_v1(&event_reference, "alice")
                .unwrap(),
            "stable-alpha"
        );
        assert_eq!(
            db.resolve_message_reference_v1(&event_reference, "bob")
                .unwrap(),
            "stable-alpha"
        );
        assert_eq!(
            db.resolve_message_reference_v1(&event_reference, "carol")
                .unwrap_err()
                .to_string(),
            "message not found"
        );
        assert_eq!(
            db.resolve_message_reference_v1("missing", "bob")
                .unwrap_err()
                .to_string(),
            "message not found"
        );

        // An exact stable ID must never fall through to an unrelated local event.
        persist(
            &db,
            &event_reference,
            "alice",
            &["carol"],
            Some("inform"),
            false,
            &event_reference,
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            db.resolve_message_reference_v1(&event_reference, "bob")
                .unwrap_err()
                .to_string(),
            "message not found"
        );
        assert_eq!(
            db.resolve_message_reference_v1(&event_reference, "carol")
                .unwrap(),
            event_reference
        );
    }

    #[test]
    fn pending_defaults_to_unresolved_inbound_oldest_first() {
        let (db, _dir) = setup();
        persist(
            &db,
            "request-1",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "request-1",
            None,
            None,
            "inbox",
            Some("request:request-1"),
            None,
            None,
        )
        .unwrap();
        persist(
            &db,
            "request-2",
            "carol",
            &["bob"],
            Some("request"),
            true,
            "request-2",
            None,
            None,
            "inbox",
            Some("request:request-2"),
            None,
            None,
        )
        .unwrap();
        persist(
            &db,
            "outbound",
            "bob",
            &["alice"],
            Some("request"),
            true,
            "outbound",
            None,
            None,
            "inbox",
            Some("request:outbound"),
            None,
            None,
        )
        .unwrap();

        let pending = db.pending_messages_v1("bob").unwrap();
        let ids: Vec<&str> = pending
            .iter()
            .filter_map(|row| row["message"]["message_id"].as_str())
            .collect();
        assert_eq!(ids, ["request-1", "request-2"]);

        db.cancel_message_v1("request-1", "alice").unwrap();
        db.conn()
            .execute(
                "UPDATE message_deliveries SET state = 'delivery_failed'
                 WHERE message_id = 'request-2' AND recipient_name = 'bob'",
                [],
            )
            .unwrap();
        assert!(db.pending_messages_v1("bob").unwrap().is_empty());
    }

    #[test]
    fn reply_uses_stable_and_legacy_ids_finalizes_watch_and_claims_immediately() {
        let (db, _dir) = setup();
        let request = persist(
            &db,
            "request",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "correlation",
            None,
            None,
            "inbox",
            Some("request:request"),
            None,
            None,
        )
        .unwrap();
        let watch_key = format!("events_sub:reqwatch-{}-bob", request.event_id);
        db.kv_set(
            &watch_key,
            Some(
                &json!({
                    "id": "watch",
                    "caller": "alice",
                    "filters": {
                        "request_watch": true,
                        "request_id": request.event_id,
                        "target": "bob"
                    },
                    "last_id": request.event_id,
                    "once": true,
                    "sql": "0"
                })
                .to_string(),
            ),
        )
        .unwrap();
        let legacy = request.event_id.to_string();
        let reply = persist(
            &db,
            "reply",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "correlation",
            Some("request"),
            Some(&legacy),
            "request:request",
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(state(&db, "request", "bob"), "replied");
        assert!(db.kv_get(&watch_key).unwrap().is_none());
        let inspection = db.inspect_message_v1("reply").unwrap();
        assert_eq!(inspection["message"]["message_id"], "reply");
        assert_eq!(inspection["message"]["in_reply_to"], "request");
        assert_eq!(inspection["message"]["reply_to"], legacy);
        assert_eq!(
            inspection["deliveries"][0]["delivery_endpoint"],
            "request:request"
        );
        assert_eq!(reply.message_id, "reply");

        let claimed = db.claim_reply_v1("request", "alice").unwrap().unwrap();
        assert_eq!(claimed["message"]["message_id"], "reply");
        assert_eq!(state(&db, "reply", "alice"), "acknowledged");
    }

    #[test]
    fn invalid_reply_rolls_back_without_orphan_events_or_parent_transition() {
        let (db, _dir) = setup();
        let parent = persist(
            &db,
            "inform",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "inform",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        let before_events = db.get_last_event_id();
        let legacy = parent.event_id.to_string();
        let error = persist(
            &db,
            "invalid-reply",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "inform",
            Some("inform"),
            Some(&legacy),
            "request:inform",
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(error.to_string().contains("expects a reply"));
        assert_eq!(db.get_last_event_id(), before_events);
        assert_eq!(state(&db, "inform", "bob"), "queued");
        let records: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM message_records WHERE message_id = 'invalid-reply'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(records, 0);

        let duplicate_error = persist(
            &db,
            "inform",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "inform",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(duplicate_error.to_string().contains("UNIQUE"));
        assert_eq!(db.get_last_event_id(), before_events);
        let duplicate_events: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM events
                 WHERE type = 'message' AND json_extract(data, '$.message_id') = 'inform'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(duplicate_events, 1);
    }

    #[test]
    fn cancel_distinguishes_pre_and_post_acceptance() {
        let (db, _dir) = setup();
        for id in ["pre", "post"] {
            persist(
                &db,
                id,
                "alice",
                &["bob"],
                Some("request"),
                true,
                id,
                None,
                None,
                "inbox",
                Some(&format!("request:{id}")),
                None,
                None,
            )
            .unwrap();
        }
        db.cancel_message_v1("pre", "alice").unwrap();
        assert_eq!(state(&db, "pre", "bob"), "cancelled");

        db.record_delivery_receipt(
            "post",
            "bob",
            "bob",
            "epoch-bob",
            "accepted",
            "Pi accepted input",
        )
        .unwrap();
        db.cancel_message_v1("post", "alice").unwrap();
        assert_eq!(state(&db, "post", "bob"), "cancellation_requested");
    }

    #[test]
    fn supersede_is_atomic_and_requires_identical_recipients() {
        let (db, _dir) = setup();
        persist(
            &db,
            "old",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "correlation",
            None,
            None,
            "inbox",
            Some("request:old"),
            None,
            None,
        )
        .unwrap();
        let before = db.get_last_event_id();
        assert!(
            persist(
                &db,
                "bad-new",
                "alice",
                &["carol"],
                Some("request"),
                true,
                "correlation",
                None,
                None,
                "inbox",
                Some("request:bad-new"),
                Some("old"),
                None,
            )
            .is_err()
        );
        assert_eq!(db.get_last_event_id(), before);
        assert_eq!(state(&db, "old", "bob"), "queued");

        persist(
            &db,
            "new",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "correlation",
            None,
            None,
            "inbox",
            Some("request:new"),
            Some("old"),
            None,
        )
        .unwrap();
        assert_eq!(state(&db, "old", "bob"), "superseded");
        assert_eq!(state(&db, "new", "bob"), "queued");
        let endpoint: String = db
            .conn()
            .query_row(
                "SELECT reply_endpoint FROM message_records WHERE message_id = 'new'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(endpoint, "request:new");
    }

    #[test]
    fn retry_lineage_increments_attempt_and_stale_receipts_fail_closed() {
        let (db, _dir) = setup();
        persist(
            &db,
            "attempt-1",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "correlation",
            None,
            None,
            "inbox",
            Some("request:attempt-1"),
            None,
            None,
        )
        .unwrap();
        persist(
            &db,
            "attempt-2",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "correlation",
            None,
            None,
            "inbox",
            Some("request:attempt-2"),
            None,
            Some("attempt-1"),
        )
        .unwrap();
        let third = persist(
            &db,
            "attempt-3",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "correlation",
            None,
            None,
            "inbox",
            Some("request:attempt-3"),
            None,
            Some("attempt-2"),
        )
        .unwrap();
        assert_eq!(
            third
                .recipient_endpoint_epochs
                .get("bob")
                .map(String::as_str),
            Some("epoch-bob")
        );
        assert_eq!(
            db.inspect_message_v1("attempt-2").unwrap()["deliveries"][0]["attempt"],
            2
        );
        assert_eq!(
            db.inspect_message_v1("attempt-3").unwrap()["deliveries"][0]["attempt"],
            3
        );

        db.record_delivery_receipt(
            "attempt-3",
            "bob",
            "bob",
            "epoch-bob",
            "received",
            "endpoint received",
        )
        .unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET endpoint_epoch = 'rotated' WHERE name = 'bob'",
                [],
            )
            .unwrap();
        let error = db
            .record_delivery_receipt(
                "attempt-3",
                "bob",
                "bob",
                "epoch-bob",
                "accepted",
                "stale endpoint",
            )
            .unwrap_err();
        assert!(error.to_string().contains("stale endpoint epoch"));
        assert_eq!(state(&db, "attempt-3", "bob"), "received");
    }

    #[test]
    fn replies_cannot_be_retried_or_superseded_in_v1() {
        let (db, _dir) = setup();
        let request = persist(
            &db,
            "reply-parent",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "reply-correlation",
            None,
            None,
            "inbox",
            Some("request:reply-parent"),
            None,
            None,
        )
        .unwrap();
        let legacy = request.event_id.to_string();
        persist(
            &db,
            "reply-child",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "reply-correlation",
            Some("reply-parent"),
            Some(&legacy),
            "request:reply-parent",
            None,
            None,
            None,
        )
        .unwrap();

        let retry = persist(
            &db,
            "reply-retry",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "reply-correlation",
            None,
            None,
            "request:reply-parent",
            None,
            None,
            Some("reply-child"),
        )
        .unwrap_err();
        assert!(retry.to_string().contains("replies cannot be retried"));

        let supersede = persist(
            &db,
            "reply-replacement",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "reply-correlation",
            None,
            None,
            "request:reply-parent",
            None,
            Some("reply-child"),
            None,
        )
        .unwrap_err();
        assert!(
            supersede
                .to_string()
                .contains("replies cannot be superseded")
        );
    }

    #[test]
    fn sibling_retries_use_unique_attempts_across_correlation_recipient_lineage() {
        let (db, _dir) = setup();
        persist(
            &db,
            "sibling-original",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "sibling-correlation",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        persist(
            &db,
            "sibling-a",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "sibling-correlation",
            None,
            None,
            "inbox",
            None,
            None,
            Some("sibling-original"),
        )
        .unwrap();
        persist(
            &db,
            "sibling-b",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "sibling-correlation",
            None,
            None,
            "inbox",
            None,
            None,
            Some("sibling-original"),
        )
        .unwrap();

        assert_eq!(
            db.inspect_message_v1("sibling-a").unwrap()["deliveries"][0]["attempt"],
            2
        );
        assert_eq!(
            db.inspect_message_v1("sibling-b").unwrap()["deliveries"][0]["attempt"],
            3
        );
    }

    #[test]
    fn replied_is_terminal_and_second_replacement_requires_an_active_transition() {
        let (db, _dir) = setup();
        let request = persist(
            &db,
            "mixed-original",
            "alice",
            &["bob", "carol"],
            Some("request"),
            true,
            "mixed-correlation",
            None,
            None,
            "inbox",
            Some("request:mixed-original"),
            None,
            None,
        )
        .unwrap();
        let legacy = request.event_id.to_string();
        persist(
            &db,
            "mixed-reply",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "mixed-correlation",
            Some("mixed-original"),
            Some(&legacy),
            "request:mixed-original",
            None,
            None,
            None,
        )
        .unwrap();

        let cancelled = db.cancel_message_v1("mixed-original", "alice").unwrap();
        assert_eq!(state(&db, "mixed-original", "bob"), "replied");
        assert_eq!(state(&db, "mixed-original", "carol"), "cancelled");
        let bob = cancelled["deliveries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["recipient"] == "bob")
            .unwrap();
        assert_eq!(bob["state"], "replied");
        assert_eq!(bob["changed"], false);

        let terminal_supersede = persist(
            &db,
            "mixed-replacement",
            "alice",
            &["bob", "carol"],
            Some("request"),
            true,
            "mixed-correlation",
            None,
            None,
            "inbox",
            Some("request:mixed-replacement"),
            Some("mixed-original"),
            None,
        )
        .unwrap_err();
        assert!(
            terminal_supersede
                .to_string()
                .contains("at least one active delivery transition")
        );
        assert_eq!(state(&db, "mixed-original", "bob"), "replied");
        assert_eq!(state(&db, "mixed-original", "carol"), "cancelled");
        let original = persist(
            &db,
            "replace-original",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "replace-correlation",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        persist(
            &db,
            "replace-first",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "replace-correlation",
            None,
            None,
            "inbox",
            None,
            Some("replace-original"),
            None,
        )
        .unwrap();
        assert_eq!(state(&db, "replace-original", "bob"), "superseded");
        let before_second = db.get_last_event_id();
        let second = persist(
            &db,
            "replace-second",
            "alice",
            &["bob"],
            Some("inform"),
            false,
            "replace-correlation",
            None,
            None,
            "inbox",
            None,
            Some("replace-original"),
            None,
        )
        .unwrap_err();
        assert!(
            second
                .to_string()
                .contains("at least one active delivery transition")
        );
        assert_eq!(db.get_last_event_id(), before_second);
        assert!(db.inspect_message_v1("replace-second").is_err());
        assert!(original.event_id > 0);
    }

    #[test]
    fn inbox_fetch_rebinds_only_queued_delivery_and_cancellation_becomes_advisory() {
        let (db, _dir) = setup();
        persist(
            &db,
            "fetch-message",
            "bob",
            &["alice"],
            Some("inform"),
            false,
            "fetch-message",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        db.conn()
            .execute(
                "UPDATE instances SET endpoint_epoch = 'epoch-alice-new' WHERE name = 'alice'",
                [],
            )
            .unwrap();

        let claimed = db
            .claim_inbox_received("alice", &["fetch-message".to_string()])
            .unwrap();
        assert!(claimed.contains("fetch-message"));
        let inspection = db.inspect_message_v1("fetch-message").unwrap();
        assert_eq!(inspection["deliveries"][0]["state"], "received");
        assert_eq!(
            inspection["deliveries"][0]["endpoint_epoch"],
            "epoch-alice-new"
        );
        let stale = db
            .record_delivery_receipt(
                "fetch-message",
                "alice",
                "alice",
                "epoch-alice",
                "accepted",
                "stale endpoint",
            )
            .unwrap_err();
        assert!(stale.to_string().contains("stale endpoint epoch"));

        db.cancel_message_v1("fetch-message", "bob").unwrap();
        assert_eq!(
            state(&db, "fetch-message", "alice"),
            "cancellation_requested"
        );
        assert_eq!(
            db.projected_inbox_delivery("fetch-message", "alice"),
            Some(true)
        );
    }

    #[test]
    fn pi_recovery_rebinds_received_and_accepted_deliveries_after_endpoint_rotation() {
        let (db, _dir) = setup();
        let received = persist(
            &db,
            "recover-received",
            "bob",
            &["alice"],
            Some("inform"),
            false,
            "recover-received",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        let accepted = persist(
            &db,
            "recover-accepted",
            "bob",
            &["alice"],
            Some("inform"),
            false,
            "recover-accepted",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        db.claim_inbox_received(
            "alice",
            &[
                "recover-received".to_string(),
                "recover-accepted".to_string(),
            ],
        )
        .unwrap();
        db.record_delivery_receipt(
            "recover-accepted",
            "alice",
            "alice",
            "epoch-alice",
            "accepted",
            "Pi accepted input before reload",
        )
        .unwrap();

        let ordinary: Vec<String> = db
            .get_unread_messages("alice")
            .into_iter()
            .filter_map(|message| message.message_id)
            .collect();
        assert!(ordinary.contains(&"recover-received".to_string()));
        assert!(!ordinary.contains(&"recover-accepted".to_string()));

        db.conn()
            .execute(
                "UPDATE instances SET endpoint_epoch = 'epoch-alice-reloaded' WHERE name = 'alice'",
                [],
            )
            .unwrap();
        let unresolved = db.get_pi_unresolved_messages("alice");
        let unresolved_ids: Vec<String> = unresolved
            .iter()
            .filter_map(|message| message.message_id.clone())
            .collect();
        assert!(unresolved_ids.contains(&"recover-received".to_string()));
        assert!(unresolved_ids.contains(&"recover-accepted".to_string()));

        let rebound = db.claim_inbox_received("alice", &unresolved_ids).unwrap();
        assert!(rebound.contains("recover-received"));
        assert!(rebound.contains("recover-accepted"));
        for message_id in ["recover-received", "recover-accepted"] {
            let endpoint_epoch: String = db
                .conn()
                .query_row(
                    "SELECT endpoint_epoch FROM message_deliveries
                     WHERE message_id = ?1 AND recipient_name = 'alice'",
                    params![message_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(endpoint_epoch, "epoch-alice-reloaded");
        }

        db.acknowledge_inbox_and_advance_cursor("alice", received.event_id.max(accepted.event_id))
            .unwrap();
        assert_eq!(state(&db, "recover-received", "alice"), "acknowledged");
        assert_eq!(state(&db, "recover-accepted", "alice"), "acknowledged");
    }

    #[test]
    fn atomic_pi_ack_rolls_back_receipts_and_cursor_then_commits_together() {
        let (db, _dir) = setup();
        let sent = persist(
            &db,
            "atomic-ack",
            "bob",
            &["alice"],
            Some("inform"),
            false,
            "atomic-ack",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        db.claim_inbox_received("alice", &["atomic-ack".to_string()])
            .unwrap();
        let events_before = db.get_last_event_id();
        db.conn()
            .execute_batch(
                "CREATE TRIGGER fail_atomic_ack
                 BEFORE UPDATE OF state ON message_deliveries
                 WHEN OLD.message_id = 'atomic-ack' AND NEW.state = 'accepted'
                 BEGIN SELECT RAISE(ABORT, 'forced ack failure'); END;",
            )
            .unwrap();

        let error = db
            .acknowledge_inbox_and_advance_cursor("alice", sent.event_id)
            .unwrap_err();
        assert!(error.to_string().contains("forced ack failure"));
        assert_eq!(state(&db, "atomic-ack", "alice"), "received");
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .last_event_id,
            0
        );
        assert_eq!(db.get_last_event_id(), events_before);

        db.conn()
            .execute_batch("DROP TRIGGER fail_atomic_ack;")
            .unwrap();
        let transitions = db
            .acknowledge_inbox_and_advance_cursor("alice", sent.event_id)
            .unwrap();
        assert_eq!(transitions, 2);
        assert_eq!(state(&db, "atomic-ack", "alice"), "acknowledged");
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .last_event_id,
            sent.event_id
        );
    }
    #[test]
    fn logical_request_endpoint_is_excluded_from_unread_and_inbox_ack() {
        let (db, _dir) = setup();
        let inbox = persist(
            &db,
            "inbox-message",
            "bob",
            &["alice"],
            Some("inform"),
            false,
            "inbox-message",
            None,
            None,
            "inbox",
            None,
            None,
            None,
        )
        .unwrap();
        let logical = persist(
            &db,
            "logical-message",
            "bob",
            &["alice"],
            Some("inform"),
            false,
            "logical-message",
            None,
            None,
            "request:parent",
            None,
            None,
            None,
        )
        .unwrap();
        let legacy_event = db
            .log_event(
                "message",
                "bob",
                &json!({
                    "from": "bob",
                    "sender_kind": "instance",
                    "scope": "mentions",
                    "mentions": ["alice"],
                    "delivered_to": ["alice"],
                    "text": "legacy readable",
                    "_relay": {"id": 7, "short": "BOXE"}
                }),
            )
            .unwrap();

        let unread = db.get_unread_messages("alice");
        assert!(
            unread
                .iter()
                .any(|message| message.text == "legacy readable" && message.relay)
        );
        let texts: Vec<&str> = unread.iter().map(|message| message.text.as_str()).collect();
        assert!(texts.contains(&"text-inbox-message"));
        assert!(texts.contains(&"legacy readable"));
        assert!(!texts.contains(&"text-logical-message"));
        assert_eq!(state(&db, "inbox-message", "alice"), "queued");
        assert_eq!(state(&db, "logical-message", "alice"), "queued");

        db.acknowledge_inbox_and_advance_cursor(
            "alice",
            legacy_event.max(logical.event_id).max(inbox.event_id),
        )
        .unwrap();
        assert_eq!(state(&db, "inbox-message", "alice"), "acknowledged");
        assert_eq!(state(&db, "logical-message", "alice"), "queued");
        assert_eq!(
            db.get_instance_full("alice")
                .unwrap()
                .unwrap()
                .last_event_id,
            legacy_event.max(logical.event_id).max(inbox.event_id)
        );
    }

    #[test]
    fn legacy_request_endpoint_reply_preserves_upstream_inbox_wake() {
        let (db, _dir) = setup();
        let request = persist(
            &db,
            "legacy-request",
            "alice",
            &["bob"],
            Some("request"),
            true,
            "legacy-request",
            None,
            None,
            "inbox",
            Some("request:legacy-request"),
            None,
            None,
        )
        .unwrap();
        db.conn()
            .execute(
                "UPDATE events SET data = json_remove(data, '$.reply_mode') WHERE id = ?1",
                params![request.event_id],
            )
            .unwrap();
        let legacy = request.event_id.to_string();
        let reply = persist(
            &db,
            "legacy-reply",
            "bob",
            &["alice"],
            Some("ack"),
            false,
            "legacy-request",
            Some("legacy-request"),
            Some(&legacy),
            "request:legacy-request",
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(
            db.projected_inbox_delivery("legacy-reply", "alice"),
            Some(true)
        );
        let unread = db.get_unread_messages("alice");
        assert!(
            unread
                .iter()
                .any(|message| message.message_id.as_deref() == Some("legacy-reply"))
        );
        let claimed = db
            .claim_inbox_received("alice", &["legacy-reply".to_string()])
            .unwrap();
        assert!(claimed.contains("legacy-reply"));
        assert_eq!(state(&db, "legacy-reply", "alice"), "received");
        db.acknowledge_inbox_and_advance_cursor("alice", reply.event_id)
            .unwrap();
        assert_eq!(state(&db, "legacy-reply", "alice"), "acknowledged");
        assert_eq!(
            db.projected_inbox_delivery("legacy-reply", "alice"),
            Some(false),
            "a reply claimed or acknowledged through any consumer must not be reinjected"
        );
        assert!(
            db.get_unread_messages("alice")
                .iter()
                .all(|message| message.message_id.as_deref() != Some("legacy-reply"))
        );
    }
}
