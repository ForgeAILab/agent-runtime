//! A bundled SQLite sink (opt-in via the `sqlite` feature).
//!
//! This is a batteries-included consumer of the [`ObsRow`] projection for hosts
//! that want durable event history without wiring their own store. Consumers
//! that already have a database use [`ObsRow`] with their own driver instead and
//! leave this feature off, keeping `rusqlite`/bundled SQLite out of the graph.

use std::fmt;
use std::path::Path;
use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::Connection;

use agent_runtime_core::event::EventEnvelope;

use crate::row::ObsRow;
use crate::{EventSink, ObsError};

/// The default table name used by [`SqliteSink::open`].
pub const DEFAULT_TABLE: &str = "runtime_events";

/// Persists each event as a row in a SQLite table.
pub struct SqliteSink {
    conn: Mutex<Connection>,
    table: String,
}

impl SqliteSink {
    /// Opens (creating if needed) a database at `path` using the default table.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ObsError> {
        let conn = Connection::open(path).map_err(sqlite_err)?;
        Self::with_connection(conn, DEFAULT_TABLE)
    }

    /// Opens an in-memory database (useful for tests).
    pub fn in_memory() -> Result<Self, ObsError> {
        let conn = Connection::open_in_memory().map_err(sqlite_err)?;
        Self::with_connection(conn, DEFAULT_TABLE)
    }

    /// Uses an existing connection and a custom table name.
    ///
    /// `table` is embedded in DDL/DML unquoted, so it must be a trusted
    /// identifier (it is validated to be alphanumeric/underscore).
    pub fn with_connection(conn: Connection, table: impl Into<String>) -> Result<Self, ObsError> {
        let table = table.into();
        if table.is_empty() || !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(ObsError::Sink(format!("invalid table name: {table:?}")));
        }
        let sink = Self {
            conn: Mutex::new(conn),
            table,
        };
        sink.ensure_table()?;
        Ok(sink)
    }

    fn ensure_table(&self) -> Result<(), ObsError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
                seq INTEGER NOT NULL, \
                session TEXT NOT NULL, \
                turn TEXT, \
                timestamp_ms INTEGER NOT NULL, \
                event_type TEXT NOT NULL, \
                payload TEXT NOT NULL)",
            self.table
        );
        self.conn
            .lock()
            .expect("poisoned")
            .execute(&sql, [])
            .map_err(sqlite_err)?;
        Ok(())
    }

    /// The number of rows currently stored (test/inspection helper).
    pub fn row_count(&self) -> Result<u64, ObsError> {
        let sql = format!("SELECT COUNT(*) FROM {}", self.table);
        let conn = self.conn.lock().expect("poisoned");
        let count: i64 = conn
            .query_row(&sql, [], |row| row.get(0))
            .map_err(sqlite_err)?;
        Ok(count as u64)
    }
}

impl fmt::Debug for SqliteSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteSink")
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl EventSink for SqliteSink {
    async fn emit(&self, event: &EventEnvelope) -> Result<(), ObsError> {
        let row = ObsRow::from_envelope(event)?;
        let sql = format!(
            "INSERT INTO {} (seq, session, turn, timestamp_ms, event_type, payload) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            self.table
        );
        self.conn
            .lock()
            .expect("poisoned")
            .execute(
                &sql,
                rusqlite::params![
                    row.seq as i64,
                    row.session,
                    row.turn,
                    row.timestamp_ms as i64,
                    row.event_type,
                    row.payload,
                ],
            )
            .map_err(sqlite_err)?;
        Ok(())
    }
}

fn sqlite_err(err: rusqlite::Error) -> ObsError {
    ObsError::Sink(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::sample_event;

    #[tokio::test]
    async fn inserts_rows_into_the_table() {
        let sink = SqliteSink::in_memory().unwrap();
        sink.emit(&sample_event(0)).await.unwrap();
        sink.emit(&sample_event(1)).await.unwrap();
        assert_eq!(sink.row_count().unwrap(), 2);
    }

    #[test]
    fn rejects_untrusted_table_names() {
        let conn = Connection::open_in_memory().unwrap();
        let err = SqliteSink::with_connection(conn, "events; DROP TABLE x").unwrap_err();
        assert!(matches!(err, ObsError::Sink(_)));
    }
}
