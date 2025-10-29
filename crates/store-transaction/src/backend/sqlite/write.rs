// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{HashMap, HashSet},
	path::Path,
	sync::mpsc,
	thread,
};

use mpsc::Sender;
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey,
	delta::Delta,
};
use reifydb_type::{Error, Result};
use rusqlite::{Connection, OpenFlags, Transaction, params_from_iter, types::Value};

use super::diagnostic::{from_rusqlite_error, transaction_failed};
use crate::{
	backend::{
		commit::CommitBuffer,
		diagnostic::connection_failed,
		sqlite::{
			cdc::store_internal_cdc,
			multi::{ensure_source_exists, source_name, fetch_pre_version},
		},
	},
	cdc::{process_deltas_for_cdc, InternalCdc, InternalCdcSequencedChange},
};

pub enum WriteCommand {
	SingleVersionCommit {
		operations: Vec<(String, Vec<Value>)>,
		response: Sender<Result<()>>,
	},
	MultiVersionCommit {
		deltas: CowVec<Delta>,
		version: CommitVersion,
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	},
	Shutdown,
}

pub struct Writer {
	receiver: mpsc::Receiver<WriteCommand>,
	conn: Connection,
	ensured_sources: HashSet<String>,
	commit_buffer: CommitBuffer,
	pending_responses: HashMap<CommitVersion, Sender<Result<()>>>,
}

impl Writer {
	pub fn spawn(db_path: &Path, flags: OpenFlags) -> Result<Sender<WriteCommand>> {
		let conn = Connection::open_with_flags(db_path, flags)
			.map_err(|e| Error(connection_failed(db_path.display().to_string(), e.to_string())))?;

		let (sender, receiver) = mpsc::channel();

		thread::spawn(move || {
			let mut actor = Writer {
				receiver,
				conn,
				ensured_sources: HashSet::new(),
				commit_buffer: CommitBuffer::new(),
				pending_responses: HashMap::new(),
			};

			actor.run();
		});

		Ok(sender)
	}

	fn run(&mut self) {
		while let Ok(cmd) = self.receiver.recv() {
			match cmd {
				WriteCommand::SingleVersionCommit {
					operations,
					response,
				} => {
					let result = self.handle_single_commit(operations);

					let _ = response.send(result);
				}
				WriteCommand::MultiVersionCommit {
					deltas,
					version,
					timestamp,
					respond_to,
				} => {
					// Buffer the commit and process any that are ready
					self.buffer_and_apply_commit(
						deltas,
						version,
						timestamp,
						respond_to,
					);
				}
				WriteCommand::Shutdown => break,
			}
		}
	}

	fn handle_single_commit(&mut self, operations: Vec<(String, Vec<Value>)>) -> Result<()> {
		let tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		for (rql, params) in operations {
			tx.execute(&rql, params_from_iter(params)).map_err(|e| Error(from_rusqlite_error(e)))?;
		}

		tx.commit().map_err(|e| Error(transaction_failed(e.to_string())))
	}

	fn buffer_and_apply_commit(
		&mut self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		timestamp: u64,
		respond_to: Sender<Result<()>>,
	) {
		// Store the response sender for later
		self.pending_responses.insert(version, respond_to);

		// Add to buffer
		self.commit_buffer.add_commit(version, deltas, timestamp);

		// Process all ready commits
		let ready_commits = self.commit_buffer.drain_ready();
		for commit in ready_commits {
			let result = self.apply_multi_commit(
				commit.deltas,
				commit.version,
				commit.timestamp,
			);

			// Send response for this commit if we have one pending
			if let Some(sender) = self.pending_responses.remove(&commit.version) {
				let _ = sender.send(result);
			}
		}
	}

	fn apply_multi_commit(
		&mut self,
		deltas: CowVec<Delta>,
		version: CommitVersion,
		timestamp: u64,
	) -> Result<()> {
		let mut tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		let cdc_changes = Self::apply_deltas(&mut tx, &deltas, version, &mut self.ensured_sources)?;

		if !cdc_changes.is_empty() {
			Self::store_cdc_changes(&tx, version, timestamp, cdc_changes)?;
		}

		tx.commit().map_err(|e| Error(transaction_failed(e.to_string())))?;

		Ok(())
	}

	fn apply_deltas(
		tx: &mut Transaction,
		deltas: &[Delta],
		version: CommitVersion,
		ensured_sources: &mut HashSet<String>,
	) -> Result<Vec<InternalCdcSequencedChange>> {
		// Capture pre-versions BEFORE applying deltas
		let mut pre_versions = std::collections::HashMap::new();
		for delta in deltas {
			let key = delta.key();
			if !pre_versions.contains_key(key) {
				if let Ok(source) = source_name(key) {
					if let Ok(Some(pre_version)) = fetch_pre_version(tx, key, source) {
						pre_versions.insert(key.clone(), pre_version);
					}
				}
			}
		}

		// Apply all deltas to storage
		for delta in deltas {
			Self::apply_single_delta(tx, delta, version, ensured_sources)?;
		}

		// Process CDC changes using the shared function
		process_deltas_for_cdc(
			deltas.iter().cloned(),
			version,
			|key| {
				// Return the pre-version we captured before applying deltas
				pre_versions.get(key).copied()
			},
		)
	}

	fn apply_single_delta(
		tx: &Transaction,
		delta: &Delta,
		version: CommitVersion,
		ensured_sources: &mut HashSet<String>,
	) -> Result<()> {
		match delta {
			Delta::Set {
				key,
				values,
			} => Self::apply_delta_set(tx, key, values, version, ensured_sources),
			Delta::Remove {
				key,
			} => Self::apply_delta_remove(tx, key, version, ensured_sources),
		}
	}

	fn apply_delta_set(
		tx: &Transaction,
		key: &[u8],
		values: &[u8],
		version: CommitVersion,
		ensured_sources: &mut HashSet<String>,
	) -> Result<()> {
		let encoded_key = EncodedKey::new(key.to_vec());
		let source = source_name(&encoded_key)?;
		Self::ensure_source_if_needed(tx, source, ensured_sources)?;

		let query = format!("INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)", source);

		tx.execute(&query, rusqlite::params![key.to_vec(), version.0, values.to_vec()])
			.map_err(|e| Error(from_rusqlite_error(e)))?;

		Ok(())
	}

	fn apply_delta_remove(
		tx: &Transaction,
		key: &[u8],
		version: CommitVersion,
		ensured_sources: &mut HashSet<String>,
	) -> Result<()> {
		let encoded_key = EncodedKey::new(key.to_vec());
		let source = source_name(&encoded_key)?;
		Self::ensure_source_if_needed(tx, source, ensured_sources)?;

		let query = format!("INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, NULL)", source);

		tx.execute(&query, rusqlite::params![key.to_vec(), version.0])
			.map_err(|e| Error(from_rusqlite_error(e)))?;

		Ok(())
	}

	fn ensure_source_if_needed(
		tx: &Transaction,
		source: &str,
		ensured_sources: &mut HashSet<String>,
	) -> Result<()> {
		if source != "multi" && !ensured_sources.contains(source) {
			ensure_source_exists(tx, source);
			ensured_sources.insert(source.to_string());
		}
		Ok(())
	}

	fn store_cdc_changes(
		tx: &Transaction,
		version: CommitVersion,
		timestamp: u64,
		cdc_changes: Vec<InternalCdcSequencedChange>,
	) -> Result<()> {
		store_internal_cdc(tx, InternalCdc { version, timestamp, changes: cdc_changes })
			.map_err(|e| Error(from_rusqlite_error(e)))
	}
}
