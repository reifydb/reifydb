// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Background writer thread for memory backend.

use std::sync::{Arc, mpsc};

use parking_lot::RwLock;
use reifydb_type::Result;
use tracing::{debug, info};

use super::tables::Tables;
use crate::backend::primitive::TableId;

/// Commands for the background writer thread.
pub(super) enum WriteCommand {
	PutBatch {
		table: TableId,
		entries: Vec<(Vec<u8>, Option<Vec<u8>>)>,
		respond_to: mpsc::Sender<Result<()>>,
	},
	ClearTable {
		table: TableId,
		respond_to: mpsc::Sender<Result<()>>,
	},
	Shutdown,
}

/// Run the background writer thread.
pub(super) fn run_writer(receiver: mpsc::Receiver<WriteCommand>, tables: Arc<RwLock<Tables>>) {
	debug!(name: "memory_writer", "background writer thread started");
	while let Ok(cmd) = receiver.recv() {
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
				info!(name: "memory_writer", "background writer thread shutting down");
				break;
			}
		}
	}
}
