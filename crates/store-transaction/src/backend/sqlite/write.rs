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
		gc::GcStats,
		sqlite::{
			cdc::store_internal_cdc,
			multi::{as_flow_node_state_key, ensure_source_exists, operator_name, source_name, fetch_pre_version},
		},
	},
	cdc::{process_deltas_for_cdc, InternalCdc, InternalCdcSequencedChange},
};

/// Helper function to get the appropriate table name for a given key
fn get_table_name(key: &EncodedKey) -> Result<&'static str> {
	// Check if it's a FlowNodeStateKey first
	if as_flow_node_state_key(key).is_some() {
		operator_name(key)
	} else {
		// Use source_name for everything else (RowKey or multi)
		source_name(key)
	}
}

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
	GarbageCollect {
		deletions: Vec<(String, EncodedKey, CommitVersion)>, // (table_name, key, version)
		respond_to: Sender<Result<GcStats>>,
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
				WriteCommand::GarbageCollect {
					deletions,
					respond_to,
				} => {
					let result = self.handle_garbage_collect(deletions);
					let _ = respond_to.send(result);
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

	fn handle_garbage_collect(
		&mut self,
		deletions: Vec<(String, EncodedKey, CommitVersion)>,
	) -> Result<GcStats> {
		let mut stats = GcStats::default();

		let tx = self.conn.transaction()
			.map_err(|e| Error(from_rusqlite_error(e)))?;

		// Group deletions by table for efficiency
		let mut by_table: HashMap<String, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		for (table, key, version) in deletions {
			by_table.entry(table).or_default().push((key, version));
		}

		// Execute deletions per table
		for (table, entries) in by_table {
			let query = format!("DELETE FROM {} WHERE key = ? AND version = ?", table);
			let mut stmt = tx.prepare(&query)
				.map_err(|e| Error(from_rusqlite_error(e)))?;

			for (key, version) in entries {
				stmt.execute(rusqlite::params![key.to_vec(), version.0])
					.map_err(|e| Error(from_rusqlite_error(e)))?;
				stats.versions_removed += 1;
			}

			stats.tables_cleaned += 1;
		}

		tx.commit().map_err(|e| Error(transaction_failed(e.to_string())))?;

		Ok(stats)
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
				if let Ok(table) = get_table_name(key) {
					if let Ok(Some(pre_version)) = fetch_pre_version(tx, key, table) {
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
		let table = get_table_name(&encoded_key)?;
		Self::ensure_source_if_needed(tx, table, ensured_sources)?;

		let query = format!("INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, ?3)", table);

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
		let table = get_table_name(&encoded_key)?;
		Self::ensure_source_if_needed(tx, table, ensured_sources)?;

		let query = format!("INSERT OR REPLACE INTO {} (key, version, value) VALUES (?1, ?2, NULL)", table);

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
