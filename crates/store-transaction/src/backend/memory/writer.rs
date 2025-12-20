// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Background writer task for memory backend using tokio.

use std::sync::Arc;

use parking_lot::RwLock;
use reifydb_type::Result;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info};

use super::tables::Tables;
use crate::backend::primitive::TableId;

/// Commands for the background writer task.
pub(super) enum WriteCommand {
	PutBatch {
		table: TableId,
		entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
		respond_to: oneshot::Sender<Result<()>>,
	},
	ClearTable {
		table: TableId,
		respond_to: oneshot::Sender<Result<()>>,
	},
	Shutdown,
}

/// Run the background writer task (async).
pub(super) async fn run_writer(mut receiver: mpsc::Receiver<WriteCommand>, tables: Arc<RwLock<Tables>>) {
	debug!(name: "memory_writer", "background writer task started");
	while let Some(cmd) = receiver.recv().await {
		match cmd {
			WriteCommand::PutBatch {
				table,
				entries,
				respond_to,
			} => {
				let mut guard = tables.write();
				let table_data = guard.get_table_mut(table);
				for (key, value) in entries {
					table_data.insert(key, value);
				}
				let _ = respond_to.send(Ok(()));
			}
			WriteCommand::ClearTable {
				table,
				respond_to,
			} => {
				debug!(table = ?table, "received ClearTable command");
				let mut guard = tables.write();
				let table_data = guard.get_table_mut(table);
				table_data.clear();
				let _ = respond_to.send(Ok(()));
			}
			WriteCommand::Shutdown => {
				info!(name: "memory_writer", "background writer task shutting down");
				break;
			}
		}
	}
}
