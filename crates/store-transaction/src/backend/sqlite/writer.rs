// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Async background writer for SQLite backend using tokio-rusqlite.
//!
//! Uses tokio-rusqlite for async SQLite operations, allowing the writer
//! to be a regular tokio task without spawn_blocking.

use reifydb_type::{Result, diagnostic::internal::internal, error};
use rusqlite::params;
use tokio::sync::{mpsc, oneshot};
use tokio_rusqlite::Connection;
use tracing::{debug, info, instrument};

/// Commands for the background writer.
pub(super) enum WriteCommand {
	PutBatch {
		table_name: String,
		entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
		respond_to: oneshot::Sender<Result<()>>,
	},
	ClearTable {
		table_name: String,
		respond_to: oneshot::Sender<Result<()>>,
	},
	EnsureTable {
		table_name: String,
		respond_to: oneshot::Sender<Result<()>>,
	},
	Shutdown,
}

/// Sender type for write commands.
pub(super) type WriterSender = mpsc::UnboundedSender<WriteCommand>;

/// Spawn the background writer task.
///
/// Returns a sender that can be used to send commands to the writer.
/// The task owns the tokio-rusqlite Connection which provides async SQLite access.
pub(super) fn spawn_writer(conn: Connection) -> WriterSender {
	let (sender, receiver) = mpsc::unbounded_channel();

	tokio::spawn(async move {
		run_writer(receiver, conn).await;
	});

	sender
}

/// Run the background writer (async).
async fn run_writer(mut rx: mpsc::UnboundedReceiver<WriteCommand>, conn: Connection) {
	debug!(name: "sqlite_writer", "background writer task started");

	while let Some(cmd) = rx.recv().await {
		match cmd {
			WriteCommand::PutBatch {
				table_name,
				entries,
				respond_to,
			} => {
				let result = execute_put_batch(&conn, table_name, entries).await;
				if let Err(ref e) = result {
					tracing::error!(err = %e, "PutBatch failed");
				}
				let _ = respond_to.send(result);
			}
			WriteCommand::ClearTable {
				table_name,
				respond_to,
			} => {
				debug!(table = %table_name, "received ClearTable command");
				let result = conn
					.call(move |conn| -> rusqlite::Result<()> {
						conn.execute(&format!("DELETE FROM \"{}\"", table_name), []).map(|_| ())
					})
					.await
					.map_err(|e| error!(internal(format!("Failed to clear table: {}", e))));
				let _ = respond_to.send(result);
			}
			WriteCommand::EnsureTable {
				table_name,
				respond_to,
			} => {
				let result = create_table_if_not_exists(&conn, table_name).await;
				let _ = respond_to.send(result);
			}
			WriteCommand::Shutdown => {
				info!(name: "sqlite_writer", "background writer task shutting down");
				break;
			}
		}
	}
}

/// Create a table if it doesn't exist.
#[instrument(name = "store::sqlite::ensure_table", level = "trace", skip(conn), fields(table = %table_name))]
pub(super) async fn create_table_if_not_exists(conn: &Connection, table_name: String) -> Result<()> {
	conn.call(move |conn| -> rusqlite::Result<()> {
		conn.execute(
			&format!(
				"CREATE TABLE IF NOT EXISTS \"{}\" (
                key   BLOB NOT NULL PRIMARY KEY,
                value BLOB
            ) WITHOUT ROWID",
				table_name
			),
			[],
		)
		.map(|_| ())
	})
	.await
	.map_err(|e| error!(internal(format!("Failed to create table: {}", e))))
}

/// Execute a batch of put operations in a transaction.
#[instrument(name = "store::sqlite::put_batch", level = "debug", skip(conn, entries), fields(table = %table_name, entry_count = entries.len()))]
async fn execute_put_batch(
	conn: &Connection,
	table_name: String,
	entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
) -> Result<()> {
	conn.call(move |conn| -> rusqlite::Result<()> {
		// Ensure table exists before writing
		conn.execute(
			&format!(
				"CREATE TABLE IF NOT EXISTS \"{}\" (
                key   BLOB NOT NULL PRIMARY KEY,
                value BLOB
            ) WITHOUT ROWID",
				table_name
			),
			[],
		)?;

		// Use a transaction for atomicity
		let tx = conn.unchecked_transaction()?;

		for (key, value) in entries {
			tx.execute(
				&format!("INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?1, ?2)", table_name),
				params![key, value.as_deref()],
			)?;
		}

		tx.commit()?;
		Ok(())
	})
	.await
	.map_err(|e| error!(internal(format!("Failed to execute put batch: {}", e))))
}
