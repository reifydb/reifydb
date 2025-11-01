// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	collections::{HashMap, HashSet},
	sync::mpsc,
	thread,
};

use mpsc::Sender;
use reifydb_core::{CommitVersion, CowVec, EncodedKey, delta::Delta};
use reifydb_type::{Error, Result};
use rusqlite::{Connection, Transaction, params_from_iter, types::Value};

use super::diagnostic::{from_rusqlite_error, transaction_failed};
use crate::{
	backend::{
		commit::{BufferedCommit, CommitBuffer},
		delta_optimizer::optimize_deltas_cow,
		gc::GcStats,
		sqlite::{
			cdc::store_cdc_changes,
			multi::{
				as_flow_node_state_key, ensure_source_exists, fetch_pre_versions, operator_name,
				source_name,
			},
		},
	},
	cdc::{InternalCdc, InternalCdcSequencedChange, process_deltas_for_cdc},
};

// Batch size is now defined in apply_batched_deltas_for_table as BATCH_SIZE_NEW

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
	pub fn spawn(conn: Connection) -> Result<(Sender<WriteCommand>, thread::JoinHandle<()>)> {
		let (sender, receiver) = mpsc::channel();

		let handle = thread::spawn(move || {
			let actor = Writer {
				receiver,
				conn,
				ensured_sources: HashSet::new(),
				commit_buffer: CommitBuffer::new(),
				pending_responses: HashMap::new(),
			};

			actor.run();
		});

		Ok((sender, handle))
	}

	fn run(mut self) {
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
					self.buffer_and_apply_commit(deltas, version, timestamp, respond_to);
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

		// Cleanup on shutdown: ensure no locks are held
		self.cleanup_on_shutdown();
	}

	fn cleanup_on_shutdown(mut self) {
		// Fail all pending responses that are waiting for commits
		for (version, sender) in self.pending_responses.drain() {
			let diagnostic = transaction_failed(format!(
				"Commit version {} abandoned due to storage shutdown",
				version.0
			));
			let _ = sender.send(Err(Error(diagnostic)));
		}

		let _ = self.conn.close();
	}

	fn handle_single_commit(&mut self, operations: Vec<(String, Vec<Value>)>) -> Result<()> {
		let tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		for (rql, params) in operations {
			tx.execute(&rql, params_from_iter(params)).map_err(|e| Error(from_rusqlite_error(e)))?;
		}

		tx.commit().map_err(|e| Error(transaction_failed(e.to_string())))
	}

	fn handle_garbage_collect(&mut self, deletions: Vec<(String, EncodedKey, CommitVersion)>) -> Result<GcStats> {
		let mut stats = GcStats::default();

		let tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		// Group deletions by table for efficiency
		let mut by_table: HashMap<String, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		for (table, key, version) in deletions {
			by_table.entry(table).or_default().push((key, version));
		}

		// Execute deletions per table
		for (table, entries) in by_table {
			let query = format!("DELETE FROM {} WHERE key = ? AND version = ?", table);
			let mut stmt = tx.prepare(&query).map_err(|e| Error(from_rusqlite_error(e)))?;

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

		// Process all ready commits in a single batched transaction
		let ready_commits = self.commit_buffer.drain_ready();
		if ready_commits.is_empty() {
			return;
		}

		// Collect versions for response handling
		let commit_versions: Vec<CommitVersion> = ready_commits.iter().map(|c| c.version).collect();

		// Apply all commits in a single batched transaction
		let result = self.apply_multi_commits(ready_commits);

		// Send result to all commits in the batch
		match result {
			Ok(()) => {
				// Send success to all
				for version in commit_versions {
					if let Some(sender) = self.pending_responses.remove(&version) {
						let _ = sender.send(Ok(()));
					}
				}
			}
			Err(e) => {
				// Send error to all (recreate error for each sender)
				for version in commit_versions {
					if let Some(sender) = self.pending_responses.remove(&version) {
						let _ = sender.send(Err(Error(transaction_failed(format!(
							"Batched commit failed: {}",
							e.0.message
						)))));
					}
				}
			}
		}
	}

	fn apply_multi_commits(&mut self, commits: Vec<BufferedCommit>) -> Result<()> {
		if commits.is_empty() {
			return Ok(());
		}

		let mut tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		// Apply all deltas and collect all CDC changes
		let mut all_cdc_entries = Vec::new();

		for commit in commits {
			let cdc_changes =
				Self::apply_deltas(&mut tx, &commit.deltas, commit.version, &mut self.ensured_sources)?;

			if !cdc_changes.is_empty() {
				all_cdc_entries.push(InternalCdc {
					version: commit.version,
					timestamp: commit.timestamp,
					changes: cdc_changes,
				});
			}
		}
		// Batch insert all CDC entries
		if !all_cdc_entries.is_empty() {
			store_cdc_changes(&tx, all_cdc_entries).map_err(|e| Error(from_rusqlite_error(e)))?;
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
		// Group keys by table for batched pre-version fetching
		let mut keys_by_table: HashMap<&'static str, Vec<&EncodedKey>> = HashMap::new();
		let mut seen_keys: HashSet<&EncodedKey> = HashSet::new();

		for delta in deltas {
			let key = delta.key();
			if seen_keys.insert(key) {
				// Only process each unique key once
				if let Ok(table) = get_table_name(key) {
					keys_by_table.entry(table).or_default().push(key);
				}
			}
		}

		// Batch fetch pre-versions for each table
		let mut pre_versions: HashMap<EncodedKey, CommitVersion> = HashMap::new();
		for (table, keys) in keys_by_table {
			let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
			if let Ok(table_versions) = fetch_pre_versions(tx, &key_bytes, table) {
				for (key_bytes, version) in table_versions {
					pre_versions.insert(EncodedKey(CowVec::new(key_bytes)), version);
				}
			}
		}

		// Optimize deltas BEFORE database writes to skip unnecessary operations
		let optimized_deltas = optimize_deltas_cow(CowVec::new(deltas.to_vec()), |key| {
			// Key exists in storage if we have a pre-version for it
			pre_versions.contains_key(key)
		});

		// Group optimized deltas by table for batched inserts
		let mut by_table: HashMap<&'static str, Vec<&Delta>> = HashMap::new();
		for delta in optimized_deltas.iter() {
			let key = delta.key();
			let table = get_table_name(key)?;
			by_table.entry(table).or_default().push(delta);
		}

		// Apply batched optimized deltas for each table
		for (table, table_deltas) in by_table {
			Self::ensure_source_if_needed(tx, table, ensured_sources)?;
			Self::apply_batched_deltas_for_table(tx, table, &table_deltas, version)?;
		}

		// Process CDC changes using the OPTIMIZED deltas (optimization already done at delta level)
		process_deltas_for_cdc(optimized_deltas, version, |key| {
			// Return the pre-version we captured before applying deltas
			pre_versions.get(key).copied()
		})
	}

	fn apply_batched_deltas_for_table(
		tx: &Transaction,
		table: &str,
		deltas: &[&Delta],
		version: CommitVersion,
	) -> Result<()> {
		const BATCH_SIZE: usize = 249; // 999 params / 4 columns (SQLite max)

		for chunk in deltas.chunks(BATCH_SIZE) {
			if chunk.is_empty() {
				continue;
			}

			let placeholders: Vec<String> = (0..chunk.len())
				.map(|i| {
					let base = i * 4;
					format!("(?{}, ?{}, ?{}, ?{})", base + 1, base + 2, base + 3, base + 4)
				})
				.collect();

			let query = format!(
				"INSERT OR REPLACE INTO {} (key, version, value, is_tombstone) VALUES {}",
				table,
				placeholders.join(", ")
			);

			let mut params: Vec<Value> = Vec::with_capacity(chunk.len() * 4);
			for delta in chunk {
				match delta {
					Delta::Set {
						key,
						values,
					} => {
						params.push(Value::Blob(key.to_vec()));
						params.push(Value::Integer(version.0 as i64));
						params.push(Value::Blob(values.to_vec()));
						params.push(Value::Integer(0)); // is_tombstone = 0 for Set
					}
					Delta::Remove {
						key,
					} => {
						params.push(Value::Blob(key.to_vec()));
						params.push(Value::Integer(version.0 as i64));
						params.push(Value::Null); // NULL value for tombstone
						params.push(Value::Integer(1)); // is_tombstone = 1 for Remove
					}
				}
			}

			tx.execute(&query, params_from_iter(params)).map_err(|e| Error(from_rusqlite_error(e)))?;
		}

		Ok(())
	}

	fn ensure_source_if_needed(
		tx: &Transaction,
		source: &str,
		ensured_sources: &mut HashSet<String>,
	) -> Result<()> {
		if source != "multi" && !ensured_sources.contains(source) {
			ensure_source_exists(tx, source).map_err(|e| Error(from_rusqlite_error(e)))?;
			ensured_sources.insert(source.to_string());
		}
		Ok(())
	}
}
