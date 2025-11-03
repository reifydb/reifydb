// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::{CommitVersion, CowVec, Result, delta::Delta, util::now_millis};
use reifydb_type::Error;

use super::StorageType;
use crate::{
	backend::{
		memory::{multi::classify_key, write::WriteCommand},
		multi::BackendMultiVersionCommit,
	},
	memory::MemoryBackend,
	storage_internal_error,
};

impl BackendMultiVersionCommit for MemoryBackend {
	fn commit(&self, delta: CowVec<Delta>, version: CommitVersion) -> Result<()> {
		// Extract affected operators BEFORE sending commit (to avoid cloning delta)
		let affected_operators: Vec<_> = delta
			.iter()
			.filter_map(|d| {
				if let StorageType::Operator(flow_node_id) = classify_key(d.key()) {
					Some(flow_node_id)
				} else {
					None
				}
			})
			.collect();

		let (respond_to, response) = mpsc::channel();

		self.writer
			.send(WriteCommand::MultiVersionCommit {
				deltas: delta,
				version,
				timestamp: now_millis(),
				respond_to,
			})
			.map_err(|_| Error(storage_internal_error!("Memory writer disconnected")))?;

		match response.recv() {
			Ok(result) => {
				// If commit succeeded, send cleanup command for affected operators
				if result.is_ok() && !affected_operators.is_empty() {
					let (tx, _rx) = mpsc::channel();
					let _ = self.writer.send(WriteCommand::CleanupOperatorRetention {
						operators: affected_operators,
						respond_to: tx,
					});
				}
				result
			}
			Err(_) => Err(Error(storage_internal_error!("Memory writer failed to respond"))),
		}
	}
}
