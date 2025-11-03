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
	/// Delete mode cleanup - creates tombstones and CDC entries
	_CleanupRetentionDelete {
		keys: Vec<EncodedKey>,
		version: CommitVersion,
		respond_to: Sender<Result<()>>,
	},
	/// Drop mode cleanup - silent removal from storage
	_CleanupRetentionDrop {
		keys: Vec<EncodedKey>,
		max_version: CommitVersion,
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
				WriteCommand::_CleanupRetentionDelete {
					keys,
					version,
					respond_to,
				} => {
					let result = self.handle_retention_delete(keys, version);
					let _ = respond_to.send(result);
				}
				WriteCommand::_CleanupRetentionDrop {
					keys,
					max_version,
					respond_to,
				} => {
					let result = self.handle_retention_drop(keys, max_version);
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
		// pre_versions stores: key -> (version, is_latest_tombstone)
		// - version: latest non-tombstone version (for fetching pre-value)
		// - is_latest_tombstone: whether the absolute latest version is a tombstone
		let mut pre_versions: HashMap<EncodedKey, (CommitVersion, bool)> = HashMap::new();
		for (table, keys) in keys_by_table {
			let key_bytes: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
			if let Ok(table_versions) = fetch_pre_versions(tx, &key_bytes, table) {
				for (key_bytes, version_info) in table_versions {
					pre_versions.insert(EncodedKey(CowVec::new(key_bytes)), version_info);
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
			// For CDC, we need to determine if there's a "live" pre-version
			// If the latest version is a tombstone, the key was deleted, so no pre-version
			// If the latest version is not a tombstone, return the version
			match pre_versions.get(key) {
				Some((version, is_tombstone)) if !is_tombstone => Some(*version),
				_ => None, // Either no version exists, or latest is tombstone (key was deleted)
			}
		})
	}

	fn apply_batched_deltas_for_table(
		tx: &Transaction,
		table: &str,
		deltas: &[&Delta],
		version: CommitVersion,
	) -> Result<()> {
		const BATCH_SIZE: usize = 249; // 999 params / 4 columns

		if deltas.is_empty() {
			return Ok(());
		}

		// Pre-generate placeholder string for full batches (done once)
		let full_batch_placeholders: String = (0..BATCH_SIZE)
			.map(|i| {
				let base = i * 4;
				format!("(?{}, ?{}, ?{}, ?{})", base + 1, base + 2, base + 3, base + 4)
			})
			.collect::<Vec<_>>()
			.join(", ");

		// Pre-build query for full batches
		let full_batch_query = format!(
			"INSERT OR REPLACE INTO {} (key, version, value, is_tombstone) VALUES {}",
			table, full_batch_placeholders
		);

		// Prepare statement once for full batches
		let mut full_batch_stmt =
			tx.prepare_cached(&full_batch_query).map_err(|e| Error(from_rusqlite_error(e)))?;

		// Pre-allocate parameter vector with max capacity (reused across batches)
		let mut params: Vec<Value> = Vec::with_capacity(BATCH_SIZE * 4);

		// Process full batches
		let mut chunks = deltas.chunks_exact(BATCH_SIZE);
		for chunk in &mut chunks {
			params.clear();

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

			full_batch_stmt
				.execute(params_from_iter(&params))
				.map_err(|e| Error(from_rusqlite_error(e)))?;
		}

		// Handle remainder (if any)
		let remainder = chunks.remainder();
		if !remainder.is_empty() {
			let remainder_placeholders: String = (0..remainder.len())
				.map(|i| {
					let base = i * 4;
					format!("(?{}, ?{}, ?{}, ?{})", base + 1, base + 2, base + 3, base + 4)
				})
				.collect::<Vec<_>>()
				.join(", ");

			let remainder_query = format!(
				"INSERT OR REPLACE INTO {} (key, version, value, is_tombstone) VALUES {}",
				table, remainder_placeholders
			);

			params.clear();
			for delta in remainder {
				match delta {
					Delta::Set {
						key,
						values,
					} => {
						params.push(Value::Blob(key.to_vec()));
						params.push(Value::Integer(version.0 as i64));
						params.push(Value::Blob(values.to_vec()));
						params.push(Value::Integer(0));
					}
					Delta::Remove {
						key,
					} => {
						params.push(Value::Blob(key.to_vec()));
						params.push(Value::Integer(version.0 as i64));
						params.push(Value::Null);
						params.push(Value::Integer(1));
					}
				}
			}

			tx.execute(&remainder_query, params_from_iter(&params))
				.map_err(|e| Error(from_rusqlite_error(e)))?;
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

	/// Handle retention delete - creates tombstones and CDC entries
	fn handle_retention_delete(&mut self, keys: Vec<EncodedKey>, version: CommitVersion) -> Result<()> {
		let tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		let mut deletable_keys = Vec::new();
		for key in keys {
			// Check if key is already tombstoned
			let is_tombstoned: bool = tx
				.query_row(
					"SELECT is_tombstone FROM multi WHERE key = ? ORDER BY version DESC LIMIT 1",
					[&key.as_bytes()],
					|row| row.get(0),
				)
				.unwrap_or(false);

			if !is_tombstoned {
				deletable_keys.push(key);
			}
		}

		if deletable_keys.is_empty() {
			return Ok(());
		}

		// Create deltas for deletion
		let deltas: Vec<Delta> = deletable_keys
			.into_iter()
			.map(|key| Delta::Remove {
				key,
			})
			.collect();

		// Process as CDC changes
		let cdc_changes = process_deltas_for_cdc(deltas.iter().cloned(), version, |_key| {
			// For retention deletion, there's no pre-version since we're deleting
			None
		})?;

		// Wrap in InternalCdc and store
		let cdc_entry = InternalCdc {
			version,
			timestamp: 0, // TODO: Should we track timestamp for retention deletions?
			changes: cdc_changes,
		};

		store_cdc_changes(&tx, vec![cdc_entry]).map_err(|e| Error(from_rusqlite_error(e)))?;

		for delta in &deltas {
			if let Delta::Remove {
				key,
			} = delta
			{
				tx.execute(
					"INSERT INTO multi (key, version, value, is_tombstone) VALUES (?, ?, NULL, 1)",
					params_from_iter([Value::Blob(key.to_vec()), Value::Integer(version.0 as i64)]),
				)
				.map_err(|e| Error(from_rusqlite_error(e)))?;
			}
		}

		tx.commit().map_err(|e| Error(from_rusqlite_error(e)))?;
		Ok(())
	}

	/// Handle retention drop - silent removal without CDC
	fn handle_retention_drop(&mut self, keys: Vec<EncodedKey>, max_version: CommitVersion) -> Result<()> {
		let tx = self.conn.transaction().map_err(|e| Error(from_rusqlite_error(e)))?;

		for key in keys {
			let query = "DELETE FROM multi
				 WHERE key = ?
				   AND version < ?
				   AND version != (SELECT MAX(version) FROM multi WHERE key = ?)";

			tx.execute(
				query,
				params_from_iter([
					Value::Blob(key.as_bytes().to_vec()),
					Value::Integer(max_version.0 as i64),
					Value::Blob(key.as_bytes().to_vec()),
				]),
			)
			.map_err(|e| Error(from_rusqlite_error(e)))?;
		}

		tx.commit().map_err(|e| Error(from_rusqlite_error(e)))?;
		Ok(())
	}
}
