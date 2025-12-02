// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Background writer thread for SQLite backend.

use std::sync::mpsc;

use reifydb_type::{Result, diagnostic::internal::internal, error};
use rusqlite::{Connection, params};

/// Commands for the background writer thread.
pub(super) enum WriteCommand {
	PutBatch {
		table_name: String,
		entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
		respond_to: mpsc::Sender<Result<()>>,
	},
	ClearTable {
		table_name: String,
		respond_to: mpsc::Sender<Result<()>>,
	},
	EnsureTable {
		table_name: String,
		respond_to: mpsc::Sender<Result<()>>,
	},
	Shutdown,
}

/// Run the background writer thread.
pub(super) fn run_writer(receiver: mpsc::Receiver<WriteCommand>, conn: Connection) {
	while let Ok(cmd) = receiver.recv() {
		match cmd {
			WriteCommand::PutBatch {
				table_name,
				entries,
				respond_to,
			} => {
				let result = execute_put_batch(&conn, &table_name, &entries);
				let _ = respond_to.send(result);
			}
			WriteCommand::ClearTable {
				table_name,
				respond_to,
			} => {
				let result = conn
					.execute(&format!("DELETE FROM \"{}\"", table_name), [])
					.map(|_| ())
					.map_err(|e| error!(internal(format!("Failed to clear table: {}", e))));
				let _ = respond_to.send(result);
			}
			WriteCommand::EnsureTable {
				table_name,
				respond_to,
			} => {
				let result = create_table_if_not_exists(&conn, &table_name);
				let _ = respond_to.send(result);
			}
			WriteCommand::Shutdown => break,
		}
	}
}

/// Create a table if it doesn't exist.
pub(super) fn create_table_if_not_exists(conn: &Connection, table_name: &str) -> Result<()> {
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
	.map_err(|e| error!(internal(format!("Failed to create table {}: {}", table_name, e))))
}

/// Execute a batch of put operations in a transaction.
fn execute_put_batch(conn: &Connection, table_name: &str, entries: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<()> {
	// Ensure table exists before writing
	create_table_if_not_exists(conn, table_name)?;

	// Use a transaction for atomicity
	let tx = conn
		.unchecked_transaction()
		.map_err(|e| error!(internal(format!("Failed to start transaction: {}", e))))?;

	for (key, value) in entries {
		tx.execute(
			&format!("INSERT OR REPLACE INTO \"{}\" (key, value) VALUES (?1, ?2)", table_name),
			params![key, value.as_deref()],
		)
		.map_err(|e| error!(internal(format!("Failed to insert: {}", e))))?;
	}

	tx.commit().map_err(|e| error!(internal(format!("Failed to commit: {}", e))))
}
