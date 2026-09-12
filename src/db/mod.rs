//! SQLite database access for hcom
//!
//! Three loosely-coupled state planes live in a single DB:
//! - `instances`: live per-agent state (TUI display, gating, delivery cursors)
//! - `events`: append-only history / message log / relay replication source
//! - `process_bindings`, `session_bindings`, `notify_endpoints`, `kv`: routing
//!   and control-plane state
//!
//! Callers typically write an event, advance per-instance cursors separately,
//! and touch bindings/endpoints/kv for delivery, identity resolution, relay
//! cursors, request-watch bookkeeping, and other control-plane state.
//!
//! Includes:
//! - Reading unread messages from `events`
//! - Updating cursor position (instances.last_event_id)
//! - Reading instance status
//! - Registering notify endpoints

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{Connection, Transaction, TransactionBehavior};

use crate::shared::time::now_epoch_f64;

mod claude_actors;
mod events;
mod instances;
mod kv;
pub mod message_lifecycle;
mod notify;
pub(crate) mod reqwatch_policy;
mod sessions;
pub(crate) mod subscriptions;

pub use events::Message;
pub use instances::InstanceRow;
#[allow(unused_imports)]
pub use instances::InstanceStatus;
pub use message_lifecycle::{
    MESSAGE_FEATURES, MESSAGE_PROTOCOL_V1, MessagePersistInput, MessagePersistResult,
};

/// Schema version - bump on any schema change.
const SCHEMA_VERSION: i32 = 19;
pub const DEV_ROOT_KV_KEY: &str = "config:dev_root";
const MIGRATIONS: &[(i32, &str)] = &[
    (
        17,
        "ALTER TABLE instances ADD COLUMN terminal_preset_requested TEXT DEFAULT '';
         ALTER TABLE instances ADD COLUMN terminal_preset_effective TEXT DEFAULT '';
         UPDATE instances
         SET terminal_preset_effective = json_extract(launch_context, '$.terminal_preset')
         WHERE launch_context != '' AND json_valid(launch_context) AND json_extract(launch_context, '$.terminal_preset') IS NOT NULL;",
    ),
    (
        18,
        "ALTER TABLE instances ADD COLUMN last_seen INTEGER DEFAULT 0;
         CREATE TABLE IF NOT EXISTS claude_actor_capabilities (
             token TEXT PRIMARY KEY,
             session_id TEXT NOT NULL,
             tool_use_id TEXT NOT NULL,
             agent_id TEXT NOT NULL DEFAULT '',
             instance_name TEXT NOT NULL,
             created_at INTEGER NOT NULL,
             expires_at INTEGER NOT NULL,
             last_seen INTEGER NOT NULL,
             UNIQUE(session_id, tool_use_id, agent_id)
         );
         CREATE INDEX IF NOT EXISTS idx_claude_actor_expiry
             ON claude_actor_capabilities(expires_at);
         CREATE INDEX IF NOT EXISTS idx_claude_actor_session
             ON claude_actor_capabilities(session_id);",
    ),
    (
        19,
        "CREATE TABLE IF NOT EXISTS message_records (
             message_id TEXT PRIMARY KEY,
             event_id INTEGER NOT NULL UNIQUE,
             correlation_id TEXT NOT NULL,
             sender_name TEXT NOT NULL,
             sender_session_id TEXT,
             intent TEXT,
             expects_reply INTEGER NOT NULL DEFAULT 0,
             in_reply_to TEXT,
             legacy_reply_to TEXT,
             reply_endpoint TEXT,
             supersedes TEXT,
             retry_of TEXT,
             attachments_json TEXT NOT NULL DEFAULT '[]',
             created_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS message_deliveries (
             message_id TEXT NOT NULL,
             recipient_name TEXT NOT NULL,
             endpoint_epoch TEXT NOT NULL DEFAULT '',
             delivery_endpoint TEXT NOT NULL DEFAULT 'inbox',
             state TEXT NOT NULL,
             attempt INTEGER NOT NULL DEFAULT 1,
             state_event_id INTEGER,
             last_actor TEXT NOT NULL DEFAULT '',
             updated_at TEXT NOT NULL,
             PRIMARY KEY(message_id, recipient_name),
             FOREIGN KEY(message_id) REFERENCES message_records(message_id) ON DELETE CASCADE
         );
         CREATE INDEX IF NOT EXISTS idx_message_records_event ON message_records(event_id);
         CREATE INDEX IF NOT EXISTS idx_message_records_correlation ON message_records(correlation_id);
         CREATE INDEX IF NOT EXISTS idx_message_records_sender ON message_records(sender_name, created_at);
         CREATE INDEX IF NOT EXISTS idx_message_records_reply_target ON message_records(in_reply_to);
         CREATE INDEX IF NOT EXISTS idx_message_records_supersedes ON message_records(supersedes);
         CREATE INDEX IF NOT EXISTS idx_message_records_retry_of ON message_records(retry_of);
         CREATE INDEX IF NOT EXISTS idx_message_deliveries_recipient_state
             ON message_deliveries(recipient_name, state, delivery_endpoint);",
    ),
];
#[derive(Clone, Copy)]
struct ColumnShape {
    not_null: bool,
    primary_key_position: i64,
}

const MESSAGE_RECORD_COLUMNS: &[&str] = &[
    "message_id",
    "event_id",
    "correlation_id",
    "sender_name",
    "sender_session_id",
    "intent",
    "expects_reply",
    "in_reply_to",
    "legacy_reply_to",
    "reply_endpoint",
    "supersedes",
    "retry_of",
    "attachments_json",
    "created_at",
];
const MESSAGE_DELIVERY_COLUMNS: &[&str] = &[
    "message_id",
    "recipient_name",
    "endpoint_epoch",
    "delivery_endpoint",
    "state",
    "attempt",
    "state_event_id",
    "last_actor",
    "updated_at",
];
const MESSAGE_INDEXES: &[(&str, &str, &[&str])] = &[
    (
        "idx_message_records_event",
        "CREATE INDEX idx_message_records_event ON message_records(event_id)",
        &["event_id"],
    ),
    (
        "idx_message_records_correlation",
        "CREATE INDEX idx_message_records_correlation ON message_records(correlation_id)",
        &["correlation_id"],
    ),
    (
        "idx_message_records_sender",
        "CREATE INDEX idx_message_records_sender ON message_records(sender_name, created_at)",
        &["sender_name", "created_at"],
    ),
    (
        "idx_message_records_reply_target",
        "CREATE INDEX idx_message_records_reply_target ON message_records(in_reply_to)",
        &["in_reply_to"],
    ),
    (
        "idx_message_records_supersedes",
        "CREATE INDEX idx_message_records_supersedes ON message_records(supersedes)",
        &["supersedes"],
    ),
    (
        "idx_message_records_retry_of",
        "CREATE INDEX idx_message_records_retry_of ON message_records(retry_of)",
        &["retry_of"],
    ),
    (
        "idx_message_deliveries_recipient_state",
        "CREATE INDEX idx_message_deliveries_recipient_state ON message_deliveries(recipient_name, state, delivery_endpoint)",
        &["recipient_name", "state", "delivery_endpoint"],
    ),
];

fn table_shape(
    conn: &Connection,
    table: &str,
) -> rusqlite::Result<std::collections::HashMap<String, ColumnShape>> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                ColumnShape {
                    not_null: row.get::<_, i64>(3)? != 0,
                    primary_key_position: row.get(5)?,
                },
            ))
        })?
        .collect()
}

fn table_exists(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
        [table],
        |row| row.get(0),
    )
}

fn index_columns(conn: &Connection, index: &str) -> rusqlite::Result<Vec<String>> {
    let mut statement = conn.prepare(&format!("PRAGMA index_info({index})"))?;
    statement
        .query_map([], |row| row.get::<_, String>(2))?
        .collect()
}

fn index_matches(
    conn: &Connection,
    index: &str,
    expected_columns: &[&str],
) -> rusqlite::Result<bool> {
    if !conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
        [index],
        |row| row.get(0),
    )? {
        return Ok(false);
    }
    Ok(index_columns(conn, index)? == expected_columns)
}

fn has_unique_event_id(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    let mut statement = conn.prepare(&format!("PRAGMA index_list({table})"))?;
    let indexes = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(2)? != 0))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (name, unique) in indexes {
        if unique && index_columns(conn, &name)? == ["event_id"] {
            return Ok(true);
        }
    }
    Ok(false)
}

fn has_message_delivery_fk(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    let mut statement = conn.prepare(&format!("PRAGMA foreign_key_list({table})"))?;
    let foreign_keys = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(6)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(foreign_keys.iter().any(|(parent, from, to, on_delete)| {
        parent == "message_records"
            && from == "message_id"
            && to == "message_id"
            && on_delete.eq_ignore_ascii_case("CASCADE")
    }))
}

fn message_schema_v19_complete_checked(conn: &Connection) -> rusqlite::Result<bool> {
    let records = table_shape(conn, "message_records")?;
    let deliveries = table_shape(conn, "message_deliveries")?;
    let records_required_not_null = [
        "event_id",
        "correlation_id",
        "sender_name",
        "expects_reply",
        "attachments_json",
        "created_at",
    ];
    let deliveries_required_not_null = [
        "message_id",
        "recipient_name",
        "endpoint_epoch",
        "delivery_endpoint",
        "state",
        "attempt",
        "last_actor",
        "updated_at",
    ];
    Ok(MESSAGE_RECORD_COLUMNS
        .iter()
        .all(|column| records.contains_key(*column))
        && records
            .get("message_id")
            .map(|column| column.primary_key_position)
            == Some(1)
        && records_required_not_null
            .iter()
            .all(|name| records.get(*name).is_some_and(|column| column.not_null))
        && has_unique_event_id(conn, "message_records")?
        && MESSAGE_DELIVERY_COLUMNS
            .iter()
            .all(|column| deliveries.contains_key(*column))
        && deliveries
            .get("message_id")
            .map(|column| column.primary_key_position)
            == Some(1)
        && deliveries
            .get("recipient_name")
            .map(|column| column.primary_key_position)
            == Some(2)
        && deliveries_required_not_null
            .iter()
            .all(|name| deliveries.get(*name).is_some_and(|column| column.not_null))
        && has_message_delivery_fk(conn, "message_deliveries")?)
}

#[cfg(test)]
fn message_schema_v19_complete(conn: &Connection) -> bool {
    message_schema_v19_complete_checked(conn).unwrap_or(false)
}

fn message_indexes_v19_complete_checked(conn: &Connection) -> rusqlite::Result<bool> {
    for (name, _, columns) in MESSAGE_INDEXES {
        if !index_matches(conn, name, columns)? {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
fn message_indexes_v19_complete(conn: &Connection) -> bool {
    message_indexes_v19_complete_checked(conn).unwrap_or(false)
}

fn create_message_records(conn: &Connection, table: &str) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE TABLE {table} (
             message_id TEXT PRIMARY KEY,
             event_id INTEGER NOT NULL UNIQUE,
             correlation_id TEXT NOT NULL,
             sender_name TEXT NOT NULL,
             sender_session_id TEXT,
             intent TEXT,
             expects_reply INTEGER NOT NULL DEFAULT 0,
             in_reply_to TEXT,
             legacy_reply_to TEXT,
             reply_endpoint TEXT,
             supersedes TEXT,
             retry_of TEXT,
             attachments_json TEXT NOT NULL DEFAULT '[]',
             created_at TEXT NOT NULL
         );"
    ))?;
    Ok(())
}

fn create_message_deliveries(conn: &Connection, table: &str, parent_table: &str) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE TABLE {table} (
             message_id TEXT NOT NULL,
             recipient_name TEXT NOT NULL,
             endpoint_epoch TEXT NOT NULL DEFAULT '',
             delivery_endpoint TEXT NOT NULL DEFAULT 'inbox',
             state TEXT NOT NULL,
             attempt INTEGER NOT NULL DEFAULT 1,
             state_event_id INTEGER,
             last_actor TEXT NOT NULL DEFAULT '',
             updated_at TEXT NOT NULL,
             PRIMARY KEY(message_id, recipient_name),
             FOREIGN KEY(message_id) REFERENCES {parent_table}(message_id) ON DELETE CASCADE
         );"
    ))?;
    Ok(())
}

fn repair_message_indexes_v19(conn: &Connection) -> Result<()> {
    for (name, sql, columns) in MESSAGE_INDEXES {
        if index_matches(conn, name, columns)? {
            continue;
        }
        conn.execute_batch(&format!("DROP INDEX IF EXISTS {name}; {sql};"))?;
    }
    Ok(())
}

fn copy_projection(
    columns: &std::collections::HashMap<String, ColumnShape>,
    row_count: i64,
    specs: &[(&str, Option<&str>, &str)],
) -> Result<String> {
    let mut projection = Vec::with_capacity(specs.len());
    for (name, missing_default, empty_placeholder) in specs {
        if columns.contains_key(*name) {
            projection.push((*name).to_string());
        } else if let Some(default) = missing_default {
            projection.push((*default).to_string());
        } else if row_count == 0 {
            projection.push((*empty_placeholder).to_string());
        } else {
            anyhow::bail!(
                "lossless v19 lifecycle repair cannot be proven: non-empty table is missing required column {name}"
            );
        }
    }
    Ok(projection.join(", "))
}

fn foreign_key_violations(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    let mut statement = conn.prepare(&format!("PRAGMA foreign_key_check({table})"))?;
    Ok(statement.query([])?.next()?.is_some())
}

fn reconstruct_message_schema_v19(conn: &Connection) -> Result<()> {
    const RECORD_NEW: &str = "message_records_v19_new";
    const DELIVERY_NEW: &str = "message_deliveries_v19_new";
    const RECORD_OLD: &str = "message_records_v19_old";
    const DELIVERY_OLD: &str = "message_deliveries_v19_old";
    for reserved in [RECORD_NEW, DELIVERY_NEW, RECORD_OLD, DELIVERY_OLD] {
        if table_exists(conn, reserved)? {
            anyhow::bail!(
                "lossless v19 lifecycle repair cannot be proven: reserved table {reserved} already exists"
            );
        }
    }

    let record_columns = table_shape(conn, "message_records")?;
    let delivery_columns = table_shape(conn, "message_deliveries")?;
    if record_columns
        .keys()
        .any(|column| !MESSAGE_RECORD_COLUMNS.contains(&column.as_str()))
        || delivery_columns
            .keys()
            .any(|column| !MESSAGE_DELIVERY_COLUMNS.contains(&column.as_str()))
    {
        anyhow::bail!(
            "lossless v19 lifecycle repair cannot be proven: lifecycle table has unknown columns"
        );
    }

    let record_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM message_records", [], |row| row.get(0))?;
    let delivery_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM message_deliveries", [], |row| {
            row.get(0)
        })?;
    let record_projection = copy_projection(
        &record_columns,
        record_count,
        &[
            ("message_id", None, "''"),
            ("event_id", None, "0"),
            ("correlation_id", None, "''"),
            ("sender_name", None, "''"),
            ("sender_session_id", Some("NULL"), "NULL"),
            ("intent", Some("NULL"), "NULL"),
            ("expects_reply", Some("0"), "0"),
            ("in_reply_to", Some("NULL"), "NULL"),
            ("legacy_reply_to", Some("NULL"), "NULL"),
            ("reply_endpoint", Some("NULL"), "NULL"),
            ("supersedes", Some("NULL"), "NULL"),
            ("retry_of", Some("NULL"), "NULL"),
            ("attachments_json", Some("'[]'"), "'[]'"),
            ("created_at", None, "''"),
        ],
    )?;
    let delivery_projection = copy_projection(
        &delivery_columns,
        delivery_count,
        &[
            ("message_id", None, "''"),
            ("recipient_name", None, "''"),
            ("endpoint_epoch", Some("''"), "''"),
            ("delivery_endpoint", Some("'inbox'"), "'inbox'"),
            ("state", None, "''"),
            ("attempt", Some("1"), "1"),
            ("state_event_id", Some("NULL"), "NULL"),
            ("last_actor", Some("''"), "''"),
            ("updated_at", None, "''"),
        ],
    )?;

    create_message_records(conn, RECORD_NEW)?;
    create_message_deliveries(conn, DELIVERY_NEW, RECORD_NEW)?;
    conn.execute_batch(&format!(
        "INSERT INTO {RECORD_NEW} ({}) SELECT {record_projection} FROM message_records;
         INSERT INTO {DELIVERY_NEW} ({}) SELECT {delivery_projection} FROM message_deliveries;",
        MESSAGE_RECORD_COLUMNS.join(", "),
        MESSAGE_DELIVERY_COLUMNS.join(", "),
    ))
    .context("lossless v19 lifecycle copy failed constraint validation")?;

    let copied_records: i64 =
        conn.query_row(&format!("SELECT COUNT(*) FROM {RECORD_NEW}"), [], |row| {
            row.get(0)
        })?;
    let copied_deliveries: i64 =
        conn.query_row(&format!("SELECT COUNT(*) FROM {DELIVERY_NEW}"), [], |row| {
            row.get(0)
        })?;
    if copied_records != record_count || copied_deliveries != delivery_count {
        anyhow::bail!(
            "lossless v19 lifecycle repair row-count mismatch: records {record_count}/{copied_records}, deliveries {delivery_count}/{copied_deliveries}"
        );
    }
    if foreign_key_violations(conn, DELIVERY_NEW)? {
        anyhow::bail!("lossless v19 lifecycle repair found foreign-key violations");
    }

    conn.execute_batch(&format!(
        "ALTER TABLE message_deliveries RENAME TO {DELIVERY_OLD};
         ALTER TABLE message_records RENAME TO {RECORD_OLD};
         ALTER TABLE {RECORD_NEW} RENAME TO message_records;
         ALTER TABLE {DELIVERY_NEW} RENAME TO message_deliveries;
         DROP TABLE {DELIVERY_OLD};
         DROP TABLE {RECORD_OLD};"
    ))?;
    if conn.query_row("SELECT COUNT(*) FROM message_records", [], |row| {
        row.get::<_, i64>(0)
    })? != record_count
        || conn.query_row("SELECT COUNT(*) FROM message_deliveries", [], |row| {
            row.get::<_, i64>(0)
        })? != delivery_count
        || foreign_key_violations(conn, "message_deliveries")?
        || !message_schema_v19_complete_checked(conn)?
    {
        anyhow::bail!("lossless v19 lifecycle repair failed post-swap validation");
    }
    Ok(())
}

fn repair_message_schema_v19(conn: &Connection) -> Result<()> {
    if !table_exists(conn, "message_records")? {
        create_message_records(conn, "message_records")?;
    }
    if !table_exists(conn, "message_deliveries")? {
        create_message_deliveries(conn, "message_deliveries", "message_records")?;
    }

    let additive_record_columns = [
        ("sender_session_id", "TEXT"),
        ("intent", "TEXT"),
        ("expects_reply", "INTEGER NOT NULL DEFAULT 0"),
        ("in_reply_to", "TEXT"),
        ("legacy_reply_to", "TEXT"),
        ("reply_endpoint", "TEXT"),
        ("supersedes", "TEXT"),
        ("retry_of", "TEXT"),
        ("attachments_json", "TEXT NOT NULL DEFAULT '[]'"),
    ];
    let additive_delivery_columns = [
        ("endpoint_epoch", "TEXT NOT NULL DEFAULT ''"),
        ("delivery_endpoint", "TEXT NOT NULL DEFAULT 'inbox'"),
        ("attempt", "INTEGER NOT NULL DEFAULT 1"),
        ("state_event_id", "INTEGER"),
        ("last_actor", "TEXT NOT NULL DEFAULT ''"),
    ];
    let mut records = table_shape(conn, "message_records")?;
    for (name, definition) in additive_record_columns {
        if !records.contains_key(name) {
            conn.execute_batch(&format!(
                "ALTER TABLE message_records ADD COLUMN {name} {definition}"
            ))?;
            records = table_shape(conn, "message_records")?;
        }
    }
    let mut deliveries = table_shape(conn, "message_deliveries")?;
    for (name, definition) in additive_delivery_columns {
        if !deliveries.contains_key(name) {
            conn.execute_batch(&format!(
                "ALTER TABLE message_deliveries ADD COLUMN {name} {definition}"
            ))?;
            deliveries = table_shape(conn, "message_deliveries")?;
        }
    }

    if !message_schema_v19_complete_checked(conn)? {
        reconstruct_message_schema_v19(conn)?;
    }
    repair_message_indexes_v19(conn)?;
    if !message_schema_v19_complete_checked(conn)? || !message_indexes_v19_complete_checked(conn)? {
        anyhow::bail!("v19 lifecycle schema repair did not produce the canonical schema");
    }
    Ok(())
}

/// Schema compatibility check result
enum SchemaCompat {
    /// Schema is compatible (or fresh DB) — proceed with init_db
    Ok,
    /// Schema is incompatible — archive, reconnect, reinit
    NeedsArchive(String, Option<i32>),
    /// DB is newer than code — stale process, work with existing schema
    StaleProcess,
}

enum LockedSchemaResult {
    Ready,
    Archive(String),
}

fn is_transient_schema_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let Some(rusqlite::Error::SqliteFailure(sqlite, _)) =
            cause.downcast_ref::<rusqlite::Error>()
        else {
            return false;
        };
        matches!(
            sqlite.code,
            rusqlite::ffi::ErrorCode::DatabaseBusy
                | rusqlite::ffi::ErrorCode::DatabaseLocked
                | rusqlite::ffi::ErrorCode::SchemaChanged
                | rusqlite::ffi::ErrorCode::FileLockingProtocolFailed
        )
    })
}

/// Database handle for hcom operations
pub struct HcomDb {
    conn: Connection,
    db_path: std::path::PathBuf,
    db_inode: u64,
}

fn get_inode(path: &std::path::Path) -> u64 {
    crate::sys::fs::file_id(path)
}

/// Reject filesystem-backed unit-test databases that are not disposable state.
/// This is a last-resort tripwire for code paths that bypass Config entirely.
///
/// A path is disposable if a fixture registered its root, or if it sits under
/// the system temp tree — the backstop for ad-hoc `tempfile` DBs opened by
/// explicit path. Unlike the Config redirect, this stays lenient about temp
/// geography because tests only ever hand `open_raw` their own throwaway paths;
/// the inherited-real-DB threat flows through Config, which is registry-gated.
#[cfg(test)]
fn assert_isolated_db_path(db_path: &std::path::Path) {
    if db_path == std::path::Path::new(":memory:") {
        return;
    }

    if crate::paths::test_roots::is_registered(db_path) {
        return;
    }

    assert!(
        crate::paths::is_test_temp_path(db_path),
        "test refused to open a DB at {} (not a registered or temp-tree path).\n\
         This path is not disposable test state, so open_raw fails closed.\n\
         Tests must install an isolated environment first:\n    \
         let (_dir, _hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();",
        db_path.display(),
    );
}

impl HcomDb {
    /// Open a hardened connection: secure the directory and database files to
    /// owner-only modes (see `paths::ensure_private_db`), then open with the
    /// standard hcom PRAGMAs. The single write path for opening the DB.
    fn open_connection(db_path: &std::path::Path) -> Result<Connection> {
        crate::paths::ensure_private_db(db_path)
            .with_context(|| format!("Failed to secure database: {}", db_path.display()))?;

        let conn = Connection::open(db_path)
            .with_context(|| format!("Failed to open database: {}", db_path.display()))?;
        // busy_timeout first: converting a fresh db to WAL takes a brief
        // exclusive lock, so with a 0 timeout a concurrent first-open (or heavy
        // load) fails instantly with SQLITE_BUSY. Setting the timeout up front
        // makes the WAL conversion retry instead.
        conn.execute_batch(
            "PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;",
        )?;

        Ok(conn)
    }

    /// Access the underlying SQLite connection (for direct queries).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }

    /// Run `f` inside a `BEGIN IMMEDIATE` transaction and commit on success.
    ///
    /// The immediate write lock makes read-modify-write sequences atomic
    /// across the separate database connections used by hook processes.
    /// Queries inside `f` must use the provided transaction.
    pub fn with_immediate_transaction<T>(
        &self,
        f: impl FnOnce(&Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let txn = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)?;
        let result = f(&txn)?;
        txn.commit()?;
        Ok(result)
    }

    /// Access the filesystem path backing this DB handle.
    pub fn path(&self) -> &std::path::Path {
        &self.db_path
    }

    /// Open the hcom database at ~/.hcom/hcom.db with schema migration/compat.
    pub fn open() -> Result<Self> {
        let hcom_dir = crate::paths::hcom_dir();
        crate::paths::ensure_private_directory(&hcom_dir)
            .with_context(|| format!("Failed to secure hcom directory: {}", hcom_dir.display()))?;
        Self::open_at(&hcom_dir.join("hcom.db"))
    }

    /// Open the hcom database at a specific path with schema migration/compat.
    pub fn open_at(db_path: &std::path::Path) -> Result<Self> {
        let mut db = Self::open_raw(db_path)?;
        db.ensure_schema()?;
        Ok(db)
    }

    /// Open DB connection without schema checks (for testing only).
    pub fn open_raw(db_path: &std::path::Path) -> Result<Self> {
        #[cfg(test)]
        assert_isolated_db_path(db_path);
        let conn = Self::open_connection(db_path)?;

        let inode = get_inode(db_path);

        Ok(Self {
            conn,
            db_path: db_path.to_path_buf(),
            db_inode: inode,
        })
    }

    /// Reconnect if the DB file was replaced (e.g., by hcom reset / schema bump).
    /// Long-lived threads (PTY delivery, listeners) hold an open connection to the
    /// old inode; this moves them onto the new DB file.
    /// Returns true if reconnection happened.
    pub fn reconnect_if_stale(&mut self) -> bool {
        let current_inode = get_inode(&self.db_path);
        if current_inode == 0 || current_inode == self.db_inode {
            return false;
        }
        // DB file replaced — reconnect
        use crate::log::{log_error, log_info};
        // Best-effort re-harden: the replacement was written by another hcom
        // process (reset/archive) that already secured it, so failing here must
        // not wedge a live delivery/listener loop — log and continue.
        if let Err(e) = crate::paths::ensure_private_db(&self.db_path) {
            use crate::log::log_warn;
            log_warn(
                "native",
                "db.secure_fail",
                &format!("Failed to re-secure DB after replacement: {}", e),
            );
        }
        match Connection::open(&self.db_path) {
            Ok(new_conn) => {
                if let Err(e) = new_conn.execute_batch(
                    "PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;",
                ) {
                    use crate::log::log_warn;
                    log_warn(
                        "native",
                        "db.pragma_fail",
                        &format!("PRAGMA setup failed after reconnect: {}", e),
                    );
                }
                log_info(
                    "native",
                    "db.reconnect",
                    &format!(
                        "DB file replaced (inode {} -> {}), reconnected",
                        self.db_inode, current_inode
                    ),
                );
                self.conn = new_conn;
                self.db_inode = current_inode;
                true
            }
            Err(e) => {
                log_error(
                    "native",
                    "db.reconnect_fail",
                    &format!("Failed to reconnect: {}", e),
                );
                false
            }
        }
    }

    /// Initialize database schema. Idempotent (IF NOT EXISTS).
    /// Creates all tables, indexes, events_v view, FTS5 virtual table + trigger,
    /// and sets PRAGMA user_version.
    pub fn init_db(&self) -> Result<()> {
        // Skip if already at current version (avoids DROP VIEW race with concurrent readers)
        let current: i32 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if current == SCHEMA_VERSION {
            return Ok(());
        }

        self.conn.execute_batch(
            "
            -- Events table
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                type TEXT NOT NULL,
                instance TEXT NOT NULL,
                data TEXT NOT NULL
            );

            -- Notify endpoints
            CREATE TABLE IF NOT EXISTS notify_endpoints (
                instance TEXT NOT NULL,
                kind TEXT NOT NULL,
                port INTEGER NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY (instance, kind)
            );
            CREATE INDEX IF NOT EXISTS idx_notify_endpoints_instance ON notify_endpoints(instance);
            CREATE INDEX IF NOT EXISTS idx_notify_endpoints_port ON notify_endpoints(port);

            -- Process bindings
            CREATE TABLE IF NOT EXISTS process_bindings (
                process_id TEXT PRIMARY KEY,
                session_id TEXT,
                instance_name TEXT,
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_process_bindings_instance ON process_bindings(instance_name);
            CREATE INDEX IF NOT EXISTS idx_process_bindings_session ON process_bindings(session_id);

            -- Session bindings
            CREATE TABLE IF NOT EXISTS session_bindings (
                session_id TEXT PRIMARY KEY,
                instance_name TEXT NOT NULL,
                created_at REAL NOT NULL,
                FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_session_bindings_instance ON session_bindings(instance_name);

            -- Instances table
            CREATE TABLE IF NOT EXISTS instances (
                name TEXT PRIMARY KEY,
                session_id TEXT UNIQUE,
                parent_session_id TEXT,
                parent_name TEXT,
                tag TEXT,
                last_event_id INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                status_time INTEGER DEFAULT 0,
                last_seen INTEGER DEFAULT 0,
                status_context TEXT DEFAULT '',
                status_detail TEXT DEFAULT '',
                last_stop INTEGER DEFAULT 0,
                directory TEXT,
                created_at REAL NOT NULL,
                transcript_path TEXT DEFAULT '',
                tcp_mode INTEGER DEFAULT 0,
                wait_timeout INTEGER,
                background INTEGER DEFAULT 0,
                background_log_file TEXT DEFAULT '',
                name_announced INTEGER DEFAULT 0,
                agent_id TEXT UNIQUE,
                running_tasks TEXT DEFAULT '',
                origin_device_id TEXT DEFAULT '',
                hints TEXT DEFAULT '',
                subagent_timeout INTEGER,
                tool TEXT DEFAULT 'claude',
                launch_args TEXT DEFAULT '',
                terminal_preset_requested TEXT DEFAULT '',
                terminal_preset_effective TEXT DEFAULT '',
                idle_since TEXT DEFAULT '',
                pid INTEGER DEFAULT NULL,
                launch_context TEXT DEFAULT '',
                endpoint_epoch TEXT NOT NULL DEFAULT '',
                presence_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (parent_session_id) REFERENCES instances(session_id) ON DELETE SET NULL
            );

            -- Claude shell actor capabilities
            CREATE TABLE IF NOT EXISTS claude_actor_capabilities (
                token TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                tool_use_id TEXT NOT NULL,
                agent_id TEXT NOT NULL DEFAULT '',
                instance_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                last_seen INTEGER NOT NULL,
                UNIQUE(session_id, tool_use_id, agent_id)
            );
            CREATE INDEX IF NOT EXISTS idx_claude_actor_expiry ON claude_actor_capabilities(expires_at);
            CREATE INDEX IF NOT EXISTS idx_claude_actor_session ON claude_actor_capabilities(session_id);
            -- Per-message lifecycle projections
            CREATE TABLE IF NOT EXISTS message_records (
                message_id TEXT PRIMARY KEY,
                event_id INTEGER NOT NULL UNIQUE,
                correlation_id TEXT NOT NULL,
                sender_name TEXT NOT NULL,
                sender_session_id TEXT,
                intent TEXT,
                expects_reply INTEGER NOT NULL DEFAULT 0,
                in_reply_to TEXT,
                legacy_reply_to TEXT,
                reply_endpoint TEXT,
                supersedes TEXT,
                retry_of TEXT,
                attachments_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS message_deliveries (
                message_id TEXT NOT NULL,
                recipient_name TEXT NOT NULL,
                endpoint_epoch TEXT NOT NULL DEFAULT '',
                delivery_endpoint TEXT NOT NULL DEFAULT 'inbox',
                state TEXT NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                state_event_id INTEGER,
                last_actor TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY(message_id, recipient_name),
                FOREIGN KEY(message_id) REFERENCES message_records(message_id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_message_records_event ON message_records(event_id);
            CREATE INDEX IF NOT EXISTS idx_message_records_correlation ON message_records(correlation_id);
            CREATE INDEX IF NOT EXISTS idx_message_records_sender ON message_records(sender_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_message_records_reply_target ON message_records(in_reply_to);
            CREATE INDEX IF NOT EXISTS idx_message_records_supersedes ON message_records(supersedes);
            CREATE INDEX IF NOT EXISTS idx_message_records_retry_of ON message_records(retry_of);
            CREATE INDEX IF NOT EXISTS idx_message_deliveries_recipient_state
                ON message_deliveries(recipient_name, state, delivery_endpoint);

            -- KV table
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT);

            -- Event indexes
            CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp);
            CREATE INDEX IF NOT EXISTS idx_type ON events(type);
            CREATE INDEX IF NOT EXISTS idx_instance ON events(instance);
            CREATE INDEX IF NOT EXISTS idx_type_instance ON events(type, instance);

            -- Instance indexes
            CREATE INDEX IF NOT EXISTS idx_session_id ON instances(session_id);
            CREATE INDEX IF NOT EXISTS idx_parent_session_id ON instances(parent_session_id);
            CREATE INDEX IF NOT EXISTS idx_parent_name ON instances(parent_name);
            CREATE INDEX IF NOT EXISTS idx_created_at ON instances(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_status ON instances(status);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_id_unique ON instances(agent_id) WHERE agent_id IS NOT NULL;
            CREATE INDEX IF NOT EXISTS idx_instances_origin ON instances(origin_device_id);

            -- Flattened events view (DROP first to apply schema changes)
            DROP VIEW IF EXISTS events_v;
            CREATE VIEW IF NOT EXISTS events_v AS
            SELECT
                id, timestamp, type, instance, data,
                json_extract(data, '$.from') as msg_from,
                json_extract(data, '$.text') as msg_text,
                json_extract(data, '$.scope') as msg_scope,
                json_extract(data, '$.sender_kind') as msg_sender_kind,
                json_extract(data, '$.delivered_to') as msg_delivered_to,
                json_extract(data, '$.mentions') as msg_mentions,
                json_extract(data, '$.intent') as msg_intent,
                json_extract(data, '$.thread') as msg_thread,
                json_extract(data, '$.reply_to') as msg_reply_to,
                json_extract(data, '$.reply_to_local') as msg_reply_to_local,
                json_extract(data, '$.protocol') as msg_protocol,
                json_extract(data, '$.message_id') as msg_message_id,
                json_extract(data, '$.correlation_id') as msg_correlation_id,
                json_extract(data, '$.in_reply_to') as msg_in_reply_to,
                json_extract(data, '$.expects_reply') as msg_expects_reply,
                json_extract(data, '$.reply_endpoint') as msg_reply_endpoint,
                json_extract(data, '$.delivery_endpoint') as msg_delivery_endpoint,
                json_extract(data, '$.supersedes') as msg_supersedes,
                json_extract(data, '$.retry_of') as msg_retry_of,
                json_extract(data, '$.attachments') as msg_attachments,
                json_extract(data, '$.bundle_id') as bundle_id,
                json_extract(data, '$.title') as bundle_title,
                json_extract(data, '$.description') as bundle_description,
                json_extract(data, '$.extends') as bundle_extends,
                json_extract(data, '$.refs.events') as bundle_events,
                json_extract(data, '$.refs.files') as bundle_files,
                json_extract(data, '$.refs.transcript') as bundle_transcript,
                json_extract(data, '$.created_by') as bundle_created_by,
                json_extract(data, '$.status') as status_val,
                json_extract(data, '$.context') as status_context,
                json_extract(data, '$.detail') as status_detail,
                json_extract(data, '$.action') as life_action,
                json_extract(data, '$.by') as life_by,
                json_extract(data, '$.batch_id') as life_batch_id,
                json_extract(data, '$.reason') as life_reason
            FROM events;

            -- FTS5 full-text search index
            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                searchable,
                tokenize='unicode61'
            );
            CREATE TRIGGER IF NOT EXISTS events_fts_insert
            AFTER INSERT ON events BEGIN
                INSERT INTO events_fts(rowid, searchable) VALUES (
                    new.id,
                    COALESCE(json_extract(new.data, '$.text'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.from'), '') || ' ' ||
                    COALESCE(new.instance, '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.context'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.detail'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.action'), '') || ' ' ||
                    COALESCE(json_extract(new.data, '$.reason'), '')
                );
            END;
            ",
        )?;

        // Set schema version
        self.conn
            .execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION))?;

        Ok(())
    }

    /// Full schema bootstrap: check version, archive if mismatched, reconnect, init.
    ///
    /// Checks schema version, archives DB if mismatched, reconnects, and reinitializes.
    /// Call after open() for production use.
    pub fn ensure_schema(&mut self) -> Result<()> {
        match self.check_schema_compat()? {
            SchemaCompat::Ok => return self.init_db(),
            SchemaCompat::StaleProcess => return Ok(()),
            SchemaCompat::NeedsArchive(_, _) => {}
        }

        const MAX_RECONCILE_ATTEMPTS: usize = 3;
        let mut attempt = 0usize;
        let archive_reason = loop {
            attempt += 1;
            match self.reconcile_schema_locked() {
                Ok(LockedSchemaResult::Ready) => return Ok(()),
                Ok(LockedSchemaResult::Archive(reason)) => break reason,
                Err(error)
                    if is_transient_schema_error(&error) && attempt < MAX_RECONCILE_ATTEMPTS =>
                {
                    self.reconnect_schema_retry(attempt)?;
                    continue;
                }
                Err(error) => {
                    return Err(error).context(
                        "schema migration failed closed; database was not archived or deleted",
                    );
                }
            }
        };

        eprintln!("hcom: {}, archiving...", archive_reason);
        // This path is reachable only after a BEGIN IMMEDIATE holder re-read the
        // version and schema and proved there is no lossless migration path.
        self.snapshot_running_to_pidtrack();
        self.conn = Connection::open_in_memory()?;
        let archive_path = Self::archive_db_at(&self.db_path)?;
        if let Some(ref path) = archive_path {
            eprintln!("hcom: Archived to {}", path);
            eprintln!("       Query with: hcom archive 1");
        }

        self.conn = Self::open_connection(&self.db_path).with_context(|| {
            format!(
                "Failed to reopen DB after archive: {}",
                self.db_path.display()
            )
        })?;
        self.db_inode = get_inode(&self.db_path);
        self.init_db()?;
        self.log_reset_event()?;
        Ok(())
    }

    fn reconnect_schema_retry(&mut self, attempt: usize) -> Result<()> {
        std::thread::sleep(std::time::Duration::from_millis(10 * attempt as u64));
        self.conn = Self::open_connection(&self.db_path).with_context(|| {
            format!(
                "failed to reconnect for bounded schema migration retry: {}",
                self.db_path.display()
            )
        })?;
        self.db_inode = get_inode(&self.db_path);
        Ok(())
    }

    fn reconcile_schema_locked(&self) -> Result<LockedSchemaResult> {
        let transaction = Transaction::new_unchecked(&self.conn, TransactionBehavior::Immediate)
            .context("failed to acquire immediate schema migration lock")?;
        let result = match Self::check_schema_compat_on(&transaction)? {
            SchemaCompat::Ok | SchemaCompat::StaleProcess => LockedSchemaResult::Ready,
            SchemaCompat::NeedsArchive(reason, None) => LockedSchemaResult::Archive(reason),
            SchemaCompat::NeedsArchive(reason, Some(version)) => {
                if !table_exists(&transaction, "instances")? {
                    LockedSchemaResult::Archive(reason)
                } else {
                    let migrate_from = Self::repair_migrate_from(&transaction, version)?;
                    if Self::try_apply_migrations(&transaction, migrate_from)? {
                        match Self::check_schema_compat_on(&transaction)? {
                            SchemaCompat::Ok | SchemaCompat::StaleProcess => {
                                LockedSchemaResult::Ready
                            }
                            SchemaCompat::NeedsArchive(revalidated_reason, _) => {
                                LockedSchemaResult::Archive(revalidated_reason)
                            }
                        }
                    } else {
                        LockedSchemaResult::Archive(reason)
                    }
                }
            }
        };
        match result {
            LockedSchemaResult::Ready => {
                transaction.commit()?;
                Ok(LockedSchemaResult::Ready)
            }
            LockedSchemaResult::Archive(reason) => {
                transaction.rollback()?;
                Ok(LockedSchemaResult::Archive(reason))
            }
        }
    }

    /// Internal: check schema compatibility without taking action.
    fn check_schema_compat(&self) -> Result<SchemaCompat> {
        Self::check_schema_compat_on(&self.conn)
    }

    fn check_schema_compat_on(conn: &Connection) -> Result<SchemaCompat> {
        let version: i32 = conn.query_row("PRAGMA user_version", [], |row| row.get(0))?;

        // Check what tables exist
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let tables: std::collections::HashSet<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;

        let required: std::collections::HashSet<&str> = [
            "events",
            "instances",
            "kv",
            "notify_endpoints",
            "session_bindings",
            "claude_actor_capabilities",
            "message_records",
            "message_deliveries",
        ]
        .into_iter()
        .collect();

        if version == 0 {
            // Race handling: another process may be initializing
            if !tables.is_empty() && required.iter().any(|t| tables.contains(*t)) {
                let mut resolved_version = 0i32;
                for _ in 0..20 {
                    let v2: i32 = conn
                        .query_row("PRAGMA user_version", [], |row| row.get(0))
                        .unwrap_or(0);
                    if v2 != 0 {
                        resolved_version = v2;
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
                if resolved_version == SCHEMA_VERSION {
                    return Ok(SchemaCompat::Ok);
                }
                if resolved_version > SCHEMA_VERSION {
                    crate::log::log_warn(
                        "db",
                        "schema.stale_process",
                        &format!(
                            "DB v{} > code v{}, working with newer schema",
                            resolved_version, SCHEMA_VERSION
                        ),
                    );
                    return Ok(SchemaCompat::StaleProcess);
                }
                // Timeout exhausted — another process is still initializing.
                // Return Ok rather than falling through to NeedsArchive which
                // would incorrectly archive a valid in-progress DB.
                if resolved_version == 0 {
                    crate::log::log_warn(
                        "db",
                        "schema.init_timeout",
                        "Concurrent init poll timed out, assuming OK",
                    );
                    return Ok(SchemaCompat::Ok);
                }
            }
            // Fresh DB (no tables) - safe to initialize
            if tables.is_empty() {
                return Ok(SchemaCompat::Ok);
            }
            // Pre-versioned DB with our tables - needs archive
            if required.iter().any(|t| tables.contains(*t)) {
                return Ok(SchemaCompat::NeedsArchive(
                    "Pre-versioned DB found".to_string(),
                    None,
                ));
            }
            // Has tables but not ours - fresh enough
            return Ok(SchemaCompat::Ok);
        }

        if version != SCHEMA_VERSION {
            if version > SCHEMA_VERSION {
                // DB newer than code - stale process, work with it
                crate::log::log_warn(
                    "db",
                    "schema.stale_process",
                    &format!(
                        "DB v{} > code v{}, working with newer schema",
                        version, SCHEMA_VERSION
                    ),
                );
                return Ok(SchemaCompat::StaleProcess);
            }
            // DB older - needs archive
            return Ok(SchemaCompat::NeedsArchive(
                format!(
                    "DB version mismatch (DB v{}, code v{})",
                    version, SCHEMA_VERSION
                ),
                Some(version),
            ));
        }

        // Verify required tables exist
        let have_all = required.iter().all(|t| tables.contains(*t));
        if !have_all {
            let missing: Vec<&&str> = required.iter().filter(|t| !tables.contains(**t)).collect();
            return Ok(SchemaCompat::NeedsArchive(
                format!("DB missing tables {:?}", missing),
                Some(version),
            ));
        }

        // Column guard: verify all expected columns exist (catches partial schema from
        // version bump before migration was written)
        let instance_columns = table_shape(conn, "instances")?;
        let required_instance_columns = [
            "tool",
            "terminal_preset_requested",
            "terminal_preset_effective",
            "last_seen",
            "endpoint_epoch",
            "presence_json",
        ];
        let missing_col = required_instance_columns
            .iter()
            .find(|column| !instance_columns.contains_key(**column))
            .map(|column| (*column).to_string());
        if let Some(col) = missing_col {
            return Ok(SchemaCompat::NeedsArchive(
                format!("DB schema missing instances.{}", col),
                Some(version),
            ));
        }
        if !message_schema_v19_complete_checked(conn)? {
            return Ok(SchemaCompat::NeedsArchive(
                "DB schema has incomplete v19 message projections".to_string(),
                Some(version),
            ));
        }
        if !message_indexes_v19_complete_checked(conn)? {
            return Ok(SchemaCompat::NeedsArchive(
                "DB schema has incomplete v19 message indexes".to_string(),
                Some(version),
            ));
        }

        Ok(SchemaCompat::Ok)
    }

    /// Decide the version to migrate *from* when repairing a DB that may have
    /// been stamped without actually running its migrations.
    ///
    /// Migrations only ADD COLUMN, so an existing column proves its migration
    /// ran. We walk back to the migration that adds the earliest column still
    /// missing; a genuinely up-to-date-but-one-behind DB (all older columns
    /// present) just re-runs the remaining steps.
    fn repair_migrate_from(conn: &Connection, version: i32) -> Result<i32> {
        let mut statement = conn.prepare("PRAGMA table_info(instances)")?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;

        // (migration version, a column that migration introduces)
        const COLUMN_MIGRATIONS: &[(i32, &str)] = &[
            (17, "terminal_preset_requested"),
            (18, "last_seen"),
            (19, "endpoint_epoch"),
            (19, "presence_json"),
        ];
        for (migration, column) in COLUMN_MIGRATIONS {
            if !columns.contains(*column) {
                return Ok(migration - 1);
            }
        }

        // All migration-added columns present: ordinary version-behind DB, or a
        // current stamp flagged for some other reason — re-run just the last step.
        Ok(if version >= SCHEMA_VERSION {
            SCHEMA_VERSION - 1
        } else {
            version
        })
    }

    /// Try in-place migration for consecutive schema versions inside the caller's
    /// already-acquired `BEGIN IMMEDIATE` transaction.
    ///
    /// Returns `Ok(false)` only when the migration chain is genuinely unavailable.
    /// SQL/schema errors are returned so callers fail closed instead of archiving.
    fn try_apply_migrations(tx: &Transaction<'_>, old_version: i32) -> Result<bool> {
        if old_version <= 0 || old_version >= SCHEMA_VERSION {
            return Ok(false);
        }
        if ((old_version + 1)..=SCHEMA_VERSION).any(|version| {
            !MIGRATIONS
                .iter()
                .any(|(candidate, _)| *candidate == version)
        }) {
            return Ok(false);
        }
        for next_version in (old_version + 1)..=SCHEMA_VERSION {
            if next_version == 17 {
                let has_launch_context = tx
                    .prepare("PRAGMA table_info(instances)")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .filter_map(|row| row.ok())
                    .any(|column| column == "launch_context");
                if !has_launch_context {
                    tx.execute(
                        "ALTER TABLE instances ADD COLUMN launch_context TEXT DEFAULT ''",
                        [],
                    )?;
                }
            }
            if next_version == 19 {
                let columns: std::collections::HashSet<String> = tx
                    .prepare("PRAGMA table_info(instances)")?
                    .query_map([], |row| row.get::<_, String>(1))?
                    .filter_map(|row| row.ok())
                    .collect();
                if !columns.contains("endpoint_epoch") {
                    tx.execute(
                        "ALTER TABLE instances ADD COLUMN endpoint_epoch TEXT NOT NULL DEFAULT ''",
                        [],
                    )?;
                }
                if !columns.contains("presence_json") {
                    tx.execute(
                        "ALTER TABLE instances ADD COLUMN presence_json TEXT NOT NULL DEFAULT '{}'",
                        [],
                    )?;
                }
                repair_message_schema_v19(tx)?;
            }
            let (_, sql) = MIGRATIONS
                .iter()
                .find(|(version, _)| *version == next_version)
                .expect("migration chain was validated before applying");
            let has_status_time = if next_version == 18 {
                let mut statement = tx.prepare("PRAGMA table_info(instances)")?;
                let columns: Vec<String> = statement
                    .query_map([], |row| row.get::<_, String>(1))?
                    .filter_map(|row| row.ok())
                    .collect();
                columns.iter().any(|column| column == "status_time")
            } else {
                false
            };
            tx.execute_batch(sql)?;
            if next_version == 18 && has_status_time {
                tx.execute(
                    "UPDATE instances SET last_seen = status_time WHERE last_seen = 0",
                    [],
                )?;
            }
            tx.execute_batch(&format!("PRAGMA user_version = {next_version}"))?;
        }
        Ok(true)
    }

    /// Archive current database at a given path.
    /// WAL checkpoint, copy to archive dir (sibling archive/ directory), delete original.
    ///
    /// Known, deliberately deferred limitation on Windows: the `remove_file`
    /// below can fail even though the caller already released its own
    /// connection (see `ensure_schema`). Windows only allows deleting a file
    /// while other handles remain open if *every* one of those handles was
    /// opened with `FILE_SHARE_DELETE` — and SQLite's Windows VFS (and thus
    /// rusqlite's default `Connection::open`) does not request that flag.
    /// Unix has no equivalent restriction; `unlink` on an open file always
    /// succeeds there, which is why this asymmetry doesn't show up in the
    /// Unix path at all.
    ///
    /// In practice this only bites when a schema-version mismatch forces an
    /// archive-and-reset (rare) while some other hcom process — another agent
    /// instance, a relay worker, a hook invocation — still has the same DB
    /// file open anywhere on the machine. When that happens, this call
    /// returns a real, un-recoverable-in-place `Err`; there is no retry that
    /// helps within this function. A proper fix would need a different
    /// strategy entirely — e.g. copying the live file's contents into a fresh
    /// DB and resetting schema in place, rather than deleting the original —
    /// so no cross-process handle-closing coordination is required. That is a
    /// larger change than this narrow Windows-support pass and is deferred
    /// given how rare schema mismatches are in practice.
    fn archive_db_at(db_path: &std::path::Path) -> Result<Option<String>> {
        if !db_path.exists() {
            return Ok(None);
        }

        let db_wal = db_path.with_extension("db-wal");
        let db_shm = db_path.with_extension("db-shm");

        // WAL checkpoint before archive
        if let Ok(temp_conn) = Connection::open(db_path) {
            let _ = temp_conn.execute_batch("PRAGMA wal_checkpoint(PASSIVE)");
        }

        // Create archive directory next to the DB file
        let parent = db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let timestamp = Utc::now().format("%Y-%m-%d_%H%M%S").to_string();
        let archive_dir = parent
            .join("archive")
            .join(format!("session-{}", timestamp));
        std::fs::create_dir_all(&archive_dir)?;

        // Copy DB files to archive
        let db_name = db_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("hcom.db"));
        std::fs::copy(db_path, archive_dir.join(db_name))?;
        if db_wal.exists() {
            let wal_name = format!("{}-wal", db_name.to_string_lossy());
            let _ = std::fs::copy(&db_wal, archive_dir.join(wal_name));
        }
        if db_shm.exists() {
            let shm_name = format!("{}-shm", db_name.to_string_lossy());
            let _ = std::fs::copy(&db_shm, archive_dir.join(shm_name));
        }

        // Delete original
        std::fs::remove_file(db_path)?;
        let _ = std::fs::remove_file(&db_wal);
        let _ = std::fs::remove_file(&db_shm);

        Ok(Some(archive_dir.to_string_lossy().to_string()))
    }

    /// Snapshot running instances to pidtrack before DB archive.
    ///
    /// Writes live instances (with their PIDs) to ~/.hcom/.tmp/launched_pids.json
    /// so orphan recovery can re-register them into the fresh DB after schema bump.
    fn snapshot_running_to_pidtrack(&self) {
        let Ok(mut stmt) = self.conn.prepare(
            "SELECT i.name, i.pid, i.tool, i.directory, i.session_id, p.process_id, \
                    n_pty.port AS notify_port, n_inj.port AS inject_port \
             FROM instances i \
             LEFT JOIN process_bindings p ON i.name = p.instance_name \
             LEFT JOIN notify_endpoints n_pty ON i.name = n_pty.instance AND n_pty.kind = 'pty' \
             LEFT JOIN notify_endpoints n_inj ON i.name = n_inj.instance AND n_inj.kind = 'inject' \
             WHERE i.pid IS NOT NULL",
        ) else {
            return;
        };

        let Ok(rows) = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,         // name
                row.get::<_, i64>(1)?,            // pid
                row.get::<_, Option<String>>(2)?, // tool
                row.get::<_, Option<String>>(3)?, // directory
                row.get::<_, Option<String>>(4)?, // session_id
                row.get::<_, Option<String>>(5)?, // process_id
                row.get::<_, Option<i64>>(6)?,    // notify_port
                row.get::<_, Option<i64>>(7)?,    // inject_port
            ))
        }) else {
            return;
        };

        let pidfile_path = self
            .db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(".tmp")
            .join("launched_pids.json");

        // Read existing pidfile
        let mut piddata: serde_json::Map<String, serde_json::Value> =
            std::fs::read_to_string(&pidfile_path)
                .ok()
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or_default();

        for row in rows.flatten() {
            let (name, pid, tool, directory, session_id, process_id, notify_port, inject_port) =
                row;
            let alive = crate::pidtrack::is_alive(pid as u32);
            if !alive {
                continue;
            }

            piddata.insert(
                pid.to_string(),
                serde_json::json!({
                    "tool": tool.unwrap_or_else(|| "claude".to_string()),
                    "names": [name],
                    "directory": directory.unwrap_or_default(),
                    "process_id": process_id.unwrap_or_default(),
                    "session_id": session_id.unwrap_or_default(),
                    "notify_port": notify_port.unwrap_or(0),
                    "inject_port": inject_port.unwrap_or(0),
                    "launched_at": now_epoch_f64(),
                }),
            );
        }

        if let Ok(json) = serde_json::to_string(&piddata) {
            let _ = std::fs::write(&pidfile_path, json);
        }
    }

    /// Log _device reset event + set relay timestamp. Call after any DB archive/reset.
    pub fn log_reset_event(&self) -> Result<()> {
        // Derive hcom_dir from db_path (db is at hcom_dir/hcom.db)
        let hcom_dir = self
            .db_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let device_id = std::fs::read_to_string(hcom_dir.join(".tmp").join("device_uuid"))
            .unwrap_or_else(|_| "unknown".to_string())
            .trim()
            .to_string();

        self.log_event(
            "life",
            "_device",
            &serde_json::json!({"action": "reset", "device": device_id}),
        )?;

        self.kv_set("relay_local_reset_ts", Some(&now_epoch_f64().to_string()))?;

        Ok(())
    }

    /// Remove all event subscriptions owned by an instance.
    ///
    /// Subscriptions are stored as kv entries with key 'events_sub:sub-{hash}'
    /// and a JSON value containing a "caller" field.
    pub fn cleanup_subscriptions(&self, name: &str) -> Result<u32> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::cleanup_subscriptions(self, name)
    }

    /// Remove delivery-only thread memberships for an instance.
    ///
    /// This is used when a stopped name is being reused by a fresh instance:
    /// normal stop/resume should preserve memberships, but identity replacement
    /// must not inherit old thread state.
    pub fn cleanup_thread_memberships_for_name_reuse(&self, name: &str) -> Result<u32> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::cleanup_thread_memberships_for_name_reuse(self, name)
    }

    /// Return active members of a thread in join order.
    pub fn get_thread_members(&self, thread: &str) -> Vec<String> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::get_thread_members(self, thread)
    }

    /// Upsert memberships for recipients of a thread message.
    pub fn add_thread_memberships(
        &self,
        thread: &str,
        sender: Option<&str>,
        recipients: &[String],
    ) {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::add_thread_memberships(self, thread, sender, recipients);
    }

    /// Send a system notification message (simplified inline version).
    /// Parses @mentions, computes scope, inserts message event.
    pub fn send_system_message(&self, sender_name: &str, message: &str) -> Result<Vec<String>> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::send_system_message(self, sender_name, message)
    }

    /// Like `send_system_message` but lets the caller specify `sender_kind`
    /// ("instance" | "external" | "system"). Used by subscription on-hit to
    /// preserve the sub caller's real identity on the event.
    pub fn send_message_as(
        &self,
        sender_name: &str,
        sender_kind: &str,
        message: &str,
    ) -> Result<Vec<String>> {
        // Delegates to db::subscriptions; events_sub: kv ownership lives there.
        subscriptions::send_message_as(self, sender_name, sender_kind, message)
    }
}

/// Generate ISO timestamp for current time.
pub(super) fn chrono_now_iso() -> String {
    crate::shared::time::now_iso()
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use rusqlite::{Connection, params};
    use std::path::PathBuf;

    /// Clean up test database
    pub(super) fn cleanup_test_db(path: PathBuf) {
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(PathBuf::from(format!("{}-wal", path.display())));
        let _ = std::fs::remove_file(PathBuf::from(format!("{}-shm", path.display())));
    }

    #[cfg(unix)]
    fn mode(path: &std::path::Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[cfg(unix)]
    #[test]
    fn open_raw_creates_private_database_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("hcom.db");

        let db = HcomDb::open_raw(&db_path).unwrap();
        db.conn()
            .execute("CREATE TABLE permission_probe (id INTEGER)", [])
            .unwrap();
        db.conn()
            .execute("INSERT INTO permission_probe VALUES (1)", [])
            .unwrap();

        assert_eq!(mode(&db_path), 0o600);
        assert_eq!(mode(&crate::paths::sidecar_path(&db_path, "-wal")), 0o600);
        assert_eq!(mode(&crate::paths::sidecar_path(&db_path, "-shm")), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn open_raw_restricts_existing_database_files() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("hcom.db");
        let first = HcomDb::open_raw(&db_path).unwrap();
        first
            .conn()
            .execute("CREATE TABLE permission_probe (id INTEGER)", [])
            .unwrap();
        first
            .conn()
            .execute("INSERT INTO permission_probe VALUES (1)", [])
            .unwrap();

        for path in [
            db_path.clone(),
            crate::paths::sidecar_path(&db_path, "-wal"),
            crate::paths::sidecar_path(&db_path, "-shm"),
        ] {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644)).unwrap();
        }

        let _second = HcomDb::open_raw(&db_path).unwrap();

        assert_eq!(mode(&db_path), 0o600);
        assert_eq!(mode(&crate::paths::sidecar_path(&db_path, "-wal")), 0o600);
        assert_eq!(mode(&crate::paths::sidecar_path(&db_path, "-shm")), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn open_raw_restricts_sidecars_for_non_db_filename() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("state.sqlite");

        let db = HcomDb::open_raw(&db_path).unwrap();
        db.conn()
            .execute("CREATE TABLE permission_probe (id INTEGER)", [])
            .unwrap();
        db.conn()
            .execute("INSERT INTO permission_probe VALUES (1)", [])
            .unwrap();

        let wal_path = tmp.path().join("state.sqlite-wal");
        let shm_path = tmp.path().join("state.sqlite-shm");
        std::fs::set_permissions(&wal_path, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::set_permissions(&shm_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        let _second = HcomDb::open_raw(&db_path).unwrap();

        assert_eq!(mode(&wal_path), 0o600);
        assert_eq!(mode(&shm_path), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn open_restricts_the_configured_hcom_directory_and_database() {
        use std::os::unix::fs::PermissionsExt;

        let (_tmp, hcom_dir, _home, _guard) = crate::hooks::test_helpers::isolated_test_env();
        std::fs::set_permissions(&hcom_dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        let db = HcomDb::open().unwrap();

        assert_eq!(mode(&hcom_dir), 0o700);
        assert_eq!(mode(db.path()), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn reconnect_if_stale_resecures_replaced_database() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::TempDir::new().unwrap();
        let db_path = tmp.path().join("hcom.db");
        let mut db = HcomDb::open_raw(&db_path).unwrap();

        // Simulate another process replacing the DB with a broad-mode file
        // (new inode), as reset/schema-archive does.
        std::fs::remove_file(&db_path).unwrap();
        let _ = std::fs::remove_file(crate::paths::sidecar_path(&db_path, "-wal"));
        let _ = std::fs::remove_file(crate::paths::sidecar_path(&db_path, "-shm"));
        drop(HcomDb::open_raw(&db_path).unwrap());
        std::fs::set_permissions(&db_path, std::fs::Permissions::from_mode(0o644)).unwrap();

        assert!(db.reconnect_if_stale());
        assert_eq!(mode(&db_path), 0o600);
    }

    #[test]
    #[should_panic(expected = "not a registered or temp-tree path")]
    fn test_open_raw_rejects_non_temp_path() {
        let db_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(".hcom-unsafe-test")
            .join("hcom.db");
        let _ = HcomDb::open_raw(&db_path);
    }

    #[test]
    fn test_open_raw_allows_temp_path() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("allowed.db");

        let db = HcomDb::open_raw(&db_path).unwrap();

        assert_eq!(db.path(), db_path);
    }

    #[cfg(unix)]
    #[test]
    #[should_panic(expected = "not a registered or temp-tree path")]
    fn test_open_raw_rejects_temp_symlink_to_non_temp_path() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().unwrap();
        let link = temp.path().join("outside");
        symlink(env!("CARGO_MANIFEST_DIR"), &link).unwrap();
        let db_path = link.join(".hcom").join("hcom.db");

        let _ = HcomDb::open_raw(&db_path);
    }

    #[test]
    fn test_all_methods_return_ok_none_when_not_found() {
        let (db, db_path) = setup_full_test_db();

        // All these should return Ok(None) for non-existent data
        assert!(db.get_instance_status("nonexistent").unwrap().is_none());
        assert!(db.get_status("nonexistent").unwrap().is_none());
        assert!(db.get_process_binding("nonexistent").unwrap().is_none());
        assert!(db.get_transcript_path("nonexistent").unwrap().is_none());
        assert!(db.get_instance_snapshot("nonexistent").unwrap().is_none());

        cleanup_test_db(db_path);
    }

    /// Create a test DB with full init_db() schema
    pub(super) fn setup_full_test_db() -> (HcomDb, PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_full_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let db = HcomDb::open_at(&db_path).unwrap();
        (db, db_path)
    }

    #[test]
    fn test_init_db_creates_all_tables() {
        let (db, db_path) = setup_full_test_db();

        let tables: Vec<String> = db
            .conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"events".to_string()));
        assert!(tables.contains(&"instances".to_string()));
        assert!(tables.contains(&"kv".to_string()));
        assert!(tables.contains(&"notify_endpoints".to_string()));
        assert!(tables.contains(&"process_bindings".to_string()));
        assert!(tables.contains(&"session_bindings".to_string()));

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_sets_schema_version() {
        let (db, db_path) = setup_full_test_db();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_idempotent() {
        let (db, db_path) = setup_full_test_db();

        // Call init_db again - should be no-op
        db.init_db().unwrap();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_creates_events_v_view() {
        let (db, db_path) = setup_full_test_db();

        // Check view exists
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name='events_v'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_creates_fts5_table() {
        let (db, db_path) = setup_full_test_db();

        // FTS5 tables show up as 'table' in sqlite_master
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='events_fts'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count > 0, "events_fts should exist");

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_init_db_fts_trigger_indexes_on_insert() {
        let (db, db_path) = setup_full_test_db();

        // Insert an event
        db.conn
            .execute(
                "INSERT INTO events (timestamp, type, instance, data) VALUES ('2026-01-01T00:00:00Z', 'message', 'luna', ?)",
                params![serde_json::json!({"from": "luna", "text": "hello world"}).to_string()],
            )
            .unwrap();

        // Search FTS
        let count: i64 = db
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events_fts WHERE searchable MATCH 'hello'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_check_schema_compat_fresh_db() {
        let (db, db_path) = setup_full_test_db();
        match db.check_schema_compat().unwrap() {
            SchemaCompat::Ok => {} // expected
            other => panic!(
                "Expected SchemaCompat::Ok, got {:?}",
                match other {
                    SchemaCompat::NeedsArchive(r, v) => format!("NeedsArchive({}, {:?})", r, v),
                    SchemaCompat::StaleProcess => "StaleProcess".to_string(),
                    SchemaCompat::Ok => unreachable!(),
                }
            ),
        }
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_fresh_db() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_ensure_{}_{}.db",
            std::process::id(),
            test_id
        ));

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        // Should have full schema
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_archives_old_version() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(2000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_archive_{}_{}.db",
            std::process::id(),
            test_id
        ));

        // Create a DB with old schema version
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (name TEXT PRIMARY KEY, created_at REAL NOT NULL);
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 PRAGMA user_version = 5;",
            )
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        // Should have been archived and recreated at current version
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // Archive directory should exist
        let archive_dir = temp_dir.join("archive");
        if archive_dir.exists() {
            let _ = std::fs::remove_dir_all(&archive_dir);
        }

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_migrates_v16_to_v18_in_place_without_status_time() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(2500);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_migrate_{}_{}.db",
            std::process::id(),
            test_id
        ));

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (
                     name TEXT PRIMARY KEY,
                     tool TEXT DEFAULT 'claude',
                     created_at REAL NOT NULL,
                     launch_context TEXT DEFAULT ''
                 );
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 PRAGMA user_version = 16;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO instances (name, tool, created_at, launch_context) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params![
                    "luna",
                    "claude",
                    1.0f64,
                    r#"{"terminal_preset":"ghostty-tab"}"#
                ],
            )
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        let preset: String = db
            .conn
            .query_row(
                "SELECT terminal_preset_effective FROM instances WHERE name = ?",
                params!["luna"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(preset, "ghostty-tab");
        let launch_context: String = db
            .conn
            .query_row(
                "SELECT launch_context FROM instances WHERE name = ?",
                params!["luna"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(launch_context, r#"{"terminal_preset":"ghostty-tab"}"#);
        let last_seen: i64 = db
            .conn
            .query_row(
                "SELECT last_seen FROM instances WHERE name = ?",
                params!["luna"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_seen, 0);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_migrates_v17_to_v18_using_status_time() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(2750);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_migrate_status_time_{}_{}.db",
            std::process::id(),
            test_id
        ));

        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (
                     name TEXT PRIMARY KEY,
                     tool TEXT DEFAULT 'claude',
                     status_time INTEGER DEFAULT 0,
                     created_at REAL NOT NULL,
                     launch_context TEXT DEFAULT '',
                     terminal_preset_requested TEXT DEFAULT '',
                     terminal_preset_effective TEXT DEFAULT ''
                 );
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 PRAGMA user_version = 17;",
            )
            .unwrap();
            conn.execute(
                "INSERT INTO instances (name, tool, status_time, created_at) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params!["luna", "claude", 123i64, 1.0f64],
            )
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        let last_seen: i64 = db
            .conn
            .query_row(
                "SELECT last_seen FROM instances WHERE name = ?",
                params!["luna"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_seen, 123);

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_ensure_schema_column_guard() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(3000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_colguard_{}_{}.db",
            std::process::id(),
            test_id
        ));

        // Create a DB at current version but missing 'tool' column
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(&format!(
                "CREATE TABLE events (id INTEGER PRIMARY KEY, timestamp TEXT, type TEXT, instance TEXT, data TEXT);
                 CREATE TABLE instances (name TEXT PRIMARY KEY, created_at REAL NOT NULL);
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT, kind TEXT, port INTEGER, updated_at REAL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 CREATE TABLE claude_actor_capabilities (
                     token TEXT PRIMARY KEY,
                     session_id TEXT NOT NULL,
                     tool_use_id TEXT NOT NULL,
                     agent_id TEXT NOT NULL DEFAULT '',
                     instance_name TEXT NOT NULL,
                     created_at INTEGER NOT NULL,
                     expires_at INTEGER NOT NULL,
                     last_seen INTEGER NOT NULL,
                     UNIQUE(session_id, tool_use_id, agent_id)
                 );
                 CREATE TABLE message_records (
                     message_id TEXT PRIMARY KEY,
                     event_id INTEGER NOT NULL UNIQUE,
                     correlation_id TEXT NOT NULL,
                     sender_name TEXT NOT NULL,
                     sender_session_id TEXT,
                     intent TEXT,
                     expects_reply INTEGER NOT NULL DEFAULT 0,
                     in_reply_to TEXT,
                     legacy_reply_to TEXT,
                     reply_endpoint TEXT,
                     supersedes TEXT,
                     retry_of TEXT,
                     attachments_json TEXT NOT NULL DEFAULT '[]',
                     created_at TEXT NOT NULL
                 );
                 CREATE TABLE message_deliveries (
                     message_id TEXT NOT NULL,
                     recipient_name TEXT NOT NULL,
                     endpoint_epoch TEXT NOT NULL DEFAULT '',
                     delivery_endpoint TEXT NOT NULL DEFAULT 'inbox',
                     state TEXT NOT NULL,
                     attempt INTEGER NOT NULL DEFAULT 1,
                     state_event_id INTEGER,
                     last_actor TEXT NOT NULL DEFAULT '',
                     updated_at TEXT NOT NULL,
                     PRIMARY KEY(message_id, recipient_name)
                 );
                 PRAGMA user_version = {};",
                SCHEMA_VERSION
            ))
            .unwrap();
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();

        // check_schema_compat should detect missing column
        match db.check_schema_compat().unwrap() {
            SchemaCompat::NeedsArchive(reason, _) => {
                assert!(reason.contains("instances.tool"), "reason: {}", reason);
            }
            _ => panic!("Expected NeedsArchive for missing tool column"),
        }

        // ensure_schema should fix it
        db.ensure_schema().unwrap();

        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        cleanup_test_db(db_path);
    }

    /// Regression test for issue #16: init_db() stamped user_version=17 without
    /// actually adding the terminal_preset_* columns. ensure_schema must repair
    /// this via migration instead of archiving (which would lose data).
    #[test]
    fn test_ensure_schema_repairs_stamped_but_not_migrated_db() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(4000);

        let temp_dir = std::env::temp_dir();
        let test_id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let db_path = temp_dir.join(format!(
            "test_hcom_repair_{}_{}.db",
            std::process::id(),
            test_id
        ));

        // Simulate the bug: create a v16-style DB but stamp it as v17
        // (this is what init_db() did — CREATE IF NOT EXISTS is a no-op on
        // existing tables, then it unconditionally set user_version = 17)
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute_batch(
                "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL, type TEXT NOT NULL, instance TEXT NOT NULL, data TEXT NOT NULL);
                 CREATE TABLE instances (
                     name TEXT PRIMARY KEY,
                     session_id TEXT UNIQUE,
                     parent_session_id TEXT,
                     parent_name TEXT,
                     tag TEXT,
                     last_event_id INTEGER DEFAULT 0,
                     status TEXT DEFAULT 'active',
                     status_time INTEGER DEFAULT 0,
                     status_context TEXT DEFAULT '',
                     status_detail TEXT DEFAULT '',
                     last_stop INTEGER DEFAULT 0,
                     directory TEXT,
                     created_at REAL NOT NULL,
                     transcript_path TEXT DEFAULT '',
                     tcp_mode INTEGER DEFAULT 0,
                     wait_timeout INTEGER DEFAULT 86400,
                     background INTEGER DEFAULT 0,
                     background_log_file TEXT DEFAULT '',
                     name_announced INTEGER DEFAULT 0,
                     agent_id TEXT UNIQUE,
                     running_tasks TEXT DEFAULT '',
                     origin_device_id TEXT DEFAULT '',
                     hints TEXT DEFAULT '',
                     subagent_timeout INTEGER,
                     tool TEXT DEFAULT 'claude',
                     launch_args TEXT DEFAULT '',
                     idle_since TEXT DEFAULT '',
                     pid INTEGER DEFAULT NULL,
                     launch_context TEXT DEFAULT ''
                 );
                 CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT);
                 CREATE TABLE notify_endpoints (instance TEXT NOT NULL, kind TEXT NOT NULL, port INTEGER NOT NULL, updated_at REAL NOT NULL, PRIMARY KEY(instance, kind));
                 CREATE TABLE session_bindings (session_id TEXT PRIMARY KEY, instance_name TEXT NOT NULL, created_at REAL NOT NULL);
                 CREATE TABLE process_bindings (process_id TEXT PRIMARY KEY, session_id TEXT, instance_name TEXT, updated_at REAL NOT NULL);
                 PRAGMA user_version = 17;",
            )
            .unwrap();
            // Insert test data that should survive the repair
            conn.execute(
                "INSERT INTO instances (name, tool, created_at) VALUES ('luna', 'claude', 1.0)",
                [],
            )
            .unwrap();
        }

        // Verify columns are missing before repair
        {
            let conn = Connection::open(&db_path).unwrap();
            let cols: Vec<String> = conn
                .prepare("PRAGMA table_info(instances)")
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect();
            assert!(
                !cols.contains(&"terminal_preset_requested".to_string()),
                "column should be missing before repair"
            );
        }

        let mut db = HcomDb::open_raw(&db_path).unwrap();
        db.ensure_schema().unwrap();

        // Should be at current version
        let version: i32 = db
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);

        // Columns should now exist
        let cols: Vec<String> = db
            .conn
            .prepare("PRAGMA table_info(instances)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        assert!(
            cols.contains(&"terminal_preset_requested".to_string()),
            "terminal_preset_requested column should exist after repair"
        );
        assert!(
            cols.contains(&"terminal_preset_effective".to_string()),
            "terminal_preset_effective column should exist after repair"
        );

        // Test data should have survived (not archived)
        let name: String = db
            .conn
            .query_row(
                "SELECT name FROM instances WHERE name = 'luna'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(name, "luna");

        cleanup_test_db(db_path);
    }

    #[test]
    fn test_schema_18_to_19_migrates_in_place_preserving_events_instances_and_cursor() {
        let (mut db, db_path) = setup_full_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, last_event_id, created_at)
                 VALUES ('luna', 41, 1.0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO events (id, timestamp, type, instance, data)
                 VALUES (42, '2026-01-01T00:00:00Z', 'message', 'luna', '{\"text\":\"preserve\"}')",
                [],
            )
            .unwrap();
        db.conn()
            .execute_batch(
                "DROP TABLE message_deliveries;
                 DROP TABLE message_records;
                 PRAGMA user_version = 18;",
            )
            .unwrap();

        db.ensure_schema().unwrap();
        assert_eq!(
            db.conn()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            19
        );
        assert!(message_schema_v19_complete(db.conn()));
        assert!(message_indexes_v19_complete(db.conn()));
        let cursor: i64 = db
            .conn()
            .query_row(
                "SELECT last_event_id FROM instances WHERE name = 'luna'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(cursor, 41);
        let event: String = db
            .conn()
            .query_row("SELECT data FROM events WHERE id = 42", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert!(event.contains("preserve"));
        cleanup_test_db(db_path);
    }

    #[test]
    fn test_incomplete_v19_repairs_idempotently_without_losing_state_planes() {
        let (mut db, db_path) = setup_full_test_db();
        db.conn()
            .execute(
                "INSERT INTO instances (name, last_event_id, created_at)
                 VALUES ('nova', 77, 1.0)",
                [],
            )
            .unwrap();
        db.conn()
            .execute(
                "INSERT INTO events (id, timestamp, type, instance, data)
                 VALUES (78, '2026-01-01T00:00:00Z', 'status', 'nova', '{\"status\":\"active\"}')",
                [],
            )
            .unwrap();
        db.conn()
            .execute_batch(
                "DROP TABLE message_deliveries;
                 DROP TABLE message_records;
                 CREATE TABLE message_records (message_id TEXT PRIMARY KEY);
                 PRAGMA user_version = 19;",
            )
            .unwrap();

        db.ensure_schema().unwrap();
        db.ensure_schema().unwrap();
        assert!(message_schema_v19_complete(db.conn()));
        assert!(message_indexes_v19_complete(db.conn()));
        let cursor: i64 = db
            .conn()
            .query_row(
                "SELECT last_event_id FROM instances WHERE name = 'nova'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(cursor, 77);
        let event_count: i64 = db
            .conn()
            .query_row("SELECT COUNT(*) FROM events WHERE id = 78", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(event_count, 1);
        let instance_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM instances WHERE name = 'nova'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(instance_count, 1);
        cleanup_test_db(db_path);
    }
    #[test]
    fn test_concurrent_18_to_19_migration_is_lossless_and_never_archives() {
        use std::sync::{Arc, Barrier};

        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("hcom.db");
        {
            let db = HcomDb::open_at(&db_path).unwrap();
            db.conn()
                .execute(
                    "INSERT INTO instances (name, last_event_id, created_at)
                     VALUES ('sentinel', 901, 1.0)",
                    [],
                )
                .unwrap();
            db.conn()
                .execute(
                    "INSERT INTO events (id, timestamp, type, instance, data)
                     VALUES (902, '2026-01-01T00:00:00Z', 'message', 'sentinel',
                             '{\"text\":\"concurrent-preserve\"}')",
                    [],
                )
                .unwrap();
            db.conn()
                .execute(
                    "INSERT INTO kv (key, value) VALUES ('sentinel_cursor', 'cursor-901')",
                    [],
                )
                .unwrap();
            db.conn()
                .execute_batch(
                    "DROP TABLE message_deliveries;
                     DROP TABLE message_records;
                     PRAGMA user_version = 18;",
                )
                .unwrap();
        }

        let worker_count = 12usize;
        let barrier = Arc::new(Barrier::new(worker_count + 1));
        let mut workers = Vec::new();
        for _ in 0..worker_count {
            let path = db_path.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || -> Result<()> {
                let mut db = HcomDb::open_raw(&path)?;
                barrier.wait();
                db.ensure_schema()
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }

        assert!(!temp.path().join("archive").exists());
        let db = HcomDb::open_raw(&db_path).unwrap();
        assert_eq!(
            db.conn()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        assert!(message_schema_v19_complete(db.conn()));
        assert!(message_indexes_v19_complete(db.conn()));
        let sentinel: (i64, String, String) = db
            .conn()
            .query_row(
                "SELECT i.last_event_id, e.data, k.value
                 FROM instances i
                 JOIN events e ON e.id = 902
                 JOIN kv k ON k.key = 'sentinel_cursor'
                 WHERE i.name = 'sentinel'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(sentinel.0, 901);
        assert!(sentinel.1.contains("concurrent-preserve"));
        assert_eq!(sentinel.2, "cursor-901");
        assert_eq!(
            db.conn()
                .query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        let scratch_tables: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name GLOB 'message_*_v19_*'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(scratch_tables, 0);
    }

    #[test]
    fn test_incomplete_v19_reconstruction_preserves_accepted_delivery() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("hcom.db");
        let mut db = HcomDb::open_at(&db_path).unwrap();
        db.conn()
            .execute_batch(
                "INSERT INTO events (id, timestamp, type, instance, data)
                 VALUES (500, '2026-01-01T00:00:00Z', 'message', 'sender', '{}');
                 DROP TABLE message_deliveries;
                 DROP TABLE message_records;
                 CREATE TABLE message_records (
                     message_id TEXT, event_id INTEGER, correlation_id TEXT, sender_name TEXT,
                     sender_session_id TEXT, intent TEXT, expects_reply INTEGER,
                     in_reply_to TEXT, legacy_reply_to TEXT, reply_endpoint TEXT,
                     supersedes TEXT, retry_of TEXT, attachments_json TEXT, created_at TEXT
                 );
                 CREATE TABLE message_deliveries (
                     message_id TEXT, recipient_name TEXT, endpoint_epoch TEXT,
                     delivery_endpoint TEXT, state TEXT, attempt INTEGER,
                     state_event_id INTEGER, last_actor TEXT, updated_at TEXT
                 );
                 INSERT INTO message_records VALUES (
                     'msg-accepted', 500, 'corr-500', 'sender', 'session-s', 'request', 1,
                     NULL, NULL, 'request:msg-accepted', NULL, NULL, '[]',
                     '2026-01-01T00:00:00Z'
                 );
                 INSERT INTO message_deliveries VALUES (
                     'msg-accepted', 'recipient', 'epoch-7', 'inbox', 'accepted', 2,
                     501, 'recipient', '2026-01-01T00:00:01Z'
                 );
                 PRAGMA user_version = 19;",
            )
            .unwrap();

        db.ensure_schema().unwrap();
        assert!(!temp.path().join("archive").exists());
        assert!(message_schema_v19_complete(db.conn()));
        assert!(message_indexes_v19_complete(db.conn()));
        let delivery: (String, String, String, i64, Option<i64>, String, String) = db
            .conn()
            .query_row(
                "SELECT message_id, endpoint_epoch, state, attempt, state_event_id,
                        last_actor, updated_at
                 FROM message_deliveries WHERE recipient_name = 'recipient'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            delivery,
            (
                "msg-accepted".to_string(),
                "epoch-7".to_string(),
                "accepted".to_string(),
                2,
                Some(501),
                "recipient".to_string(),
                "2026-01-01T00:00:01Z".to_string(),
            )
        );
        assert_eq!(
            db.conn()
                .query_row("SELECT COUNT(*) FROM message_records", [], |row| {
                    row.get::<_, i64>(0)
                })
                .unwrap(),
            1
        );
        assert!(!foreign_key_violations(db.conn(), "message_deliveries").unwrap());
    }

    #[test]
    fn test_impossible_v19_lossless_repair_fails_closed_without_archive() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("hcom.db");
        let mut db = HcomDb::open_at(&db_path).unwrap();
        db.conn()
            .execute_batch(
                "DROP TABLE message_deliveries;
                 DROP TABLE message_records;
                 CREATE TABLE message_records (
                     message_id TEXT PRIMARY KEY,
                     correlation_id TEXT NOT NULL,
                     sender_name TEXT NOT NULL,
                     created_at TEXT NOT NULL
                 );
                 CREATE TABLE message_deliveries (
                     message_id TEXT NOT NULL,
                     recipient_name TEXT NOT NULL,
                     endpoint_epoch TEXT NOT NULL DEFAULT '',
                     delivery_endpoint TEXT NOT NULL DEFAULT 'inbox',
                     state TEXT NOT NULL,
                     attempt INTEGER NOT NULL DEFAULT 1,
                     state_event_id INTEGER,
                     last_actor TEXT NOT NULL DEFAULT '',
                     updated_at TEXT NOT NULL,
                     PRIMARY KEY(message_id, recipient_name),
                     FOREIGN KEY(message_id) REFERENCES message_records(message_id) ON DELETE CASCADE
                 );
                 INSERT INTO message_records
                     (message_id, correlation_id, sender_name, created_at)
                 VALUES ('cannot-prove', 'corr-x', 'sender', '2026-01-01T00:00:00Z');
                 PRAGMA user_version = 19;",
            )
            .unwrap();

        let error = db.ensure_schema().unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("lossless v19 lifecycle repair cannot be proven"));
        assert!(message.contains("missing required column event_id"));
        assert!(message.contains("database was not archived or deleted"));
        assert!(!temp.path().join("archive").exists());
        assert!(db_path.exists());
        assert_eq!(
            db.conn()
                .query_row("SELECT message_id FROM message_records", [], |row| row
                    .get::<_, String>(
                    0
                ),)
                .unwrap(),
            "cannot-prove"
        );
        assert_eq!(
            db.conn()
                .query_row("PRAGMA user_version", [], |row| row.get::<_, i32>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let scratch_tables: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'table' AND name GLOB 'message_*_v19_*'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(scratch_tables, 0);
    }
}
