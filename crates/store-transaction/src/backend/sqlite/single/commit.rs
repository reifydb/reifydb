// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use reifydb_core::{CowVec, Result, delta::Delta};

use crate::backend::{
	single::BackendSingleVersionCommit,
	sqlite::{SqliteBackend, write::WriteCommand},
};

impl BackendSingleVersionCommit for SqliteBackend {
	fn commit(&self, deltas: CowVec<Delta>) -> Result<()> {
		let (tx, rx) = mpsc::channel();
		self.writer
			.send(WriteCommand::SingleVersionCommit {
				operations: convert_deltas_to_operations(deltas),
				response: tx,
			})
			.map_err(|_| {
				reifydb_type::Error(crate::storage_internal_error!("Writer actor disconnected"))
			})?;
		match rx.recv() {
			Ok(result) => result,
			Err(_) => {
				Err(reifydb_type::Error(crate::storage_internal_error!("Writer actor response failed")))
			}
		}
	}
}

/// Convert deltas to SQL operations for single storage
fn convert_deltas_to_operations(deltas: CowVec<Delta>) -> Vec<(String, Vec<rusqlite::types::Value>)> {
	let mut operations = Vec::new();
	for delta in deltas.as_ref() {
		match delta {
			Delta::Set {
				key,
				values: bytes,
			} => {
				operations.push((
					"INSERT OR REPLACE INTO single (key,value) VALUES (?1, ?2)".to_string(),
					vec![
						rusqlite::types::Value::Blob(key.to_vec()),
						rusqlite::types::Value::Blob(bytes.to_vec()),
					],
				));
			}
			Delta::Remove {
				key,
			} => {
				operations.push((
					"INSERT OR REPLACE INTO single (key, value) VALUES (?1, NULL)".to_string(),
					vec![rusqlite::types::Value::Blob(key.to_vec())],
				));
			}
		}
	}
	operations
}
