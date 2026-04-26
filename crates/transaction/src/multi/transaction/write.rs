// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::mem;
use std::{cmp::Ordering, collections::HashSet, iter, ops::RangeBounds, sync::Arc, vec};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::transaction::PostCommitEvent,
	interface::store::{
		MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow,
	},
};
use reifydb_type::{
	Result,
	util::{cowvec::CowVec, hex},
};
use tracing::instrument;

use super::{MultiTransaction, version::StandardVersionProvider};
use crate::{
	TransactionId,
	delta::optimize_deltas,
	error::TransactionError,
	multi::{
		conflict::ConflictManager,
		marker::Marker,
		oracle::{CreateCommitResult, Oracle},
		pending::PendingWrites,
		types::{Pending, TransactionValue},
	},
};

/// Snapshot of write transaction state for savepoint/restore.
pub struct WriteSavepoint {
	pub(crate) pending_writes: PendingWrites,
	pub(crate) count: u64,
	pub(crate) size: u64,
	pub(crate) duplicates: Vec<Pending>,
	// `delta_log` is the source of truth at commit time; it is append-only,
	// so a length is enough to roll back to the savepoint.
	pub(crate) delta_log_len: usize,
	pub(crate) conflicts: ConflictManager,
	pub(crate) preexisting_keys: HashSet<Vec<u8>>,
}

pub struct MultiWriteTransaction {
	engine: MultiTransaction,

	pub(crate) id: TransactionId,
	pub(crate) version: CommitVersion,
	// Separate read version for as_of queries.
	pub(crate) read_version: Option<CommitVersion>,
	pub(crate) size: u64,
	pub(crate) count: u64,
	pub(crate) oracle: Arc<Oracle<StandardVersionProvider>>,
	pub(crate) conflicts: ConflictManager,
	// Stores any writes done by tx (used for read-your-own-writes; last value per key wins).
	pub(crate) pending_writes: PendingWrites,
	pub(crate) duplicates: Vec<Pending>,
	// Append-only history of every modify() call in issuance order. Source of truth
	// for the deltas fed to `optimize_deltas` at commit time so that Set+Unset of the
	// same key (and any other multi-touch sequences) are visible in their original
	// order. `pending_writes` collapses to one entry per key and so cannot serve this.
	pub(crate) delta_log: Vec<Pending>,
	// Keys that existed in committed storage before this transaction started.
	// Populated by Update operations (where the prior row was read). The optimizer
	// uses this to distinguish a true Insert+Delete (cancellable) from an
	// Update+Delete (must keep tombstone). See `optimize_deltas`.
	pub(crate) preexisting_keys: HashSet<Vec<u8>>,

	pub(crate) discarded: bool,
	pub(crate) done_query: bool,
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::new", level = "debug", skip(engine))]
	pub fn new(engine: MultiTransaction) -> Result<Self> {
		let oracle = engine.tm.oracle().clone();
		let version = oracle.version()?;
		// Register the read snapshot with the query watermark so cleanup
		// of conflict-detection windows knows this version is still in
		// flight. The matching `done` is called from `discard()` /
		// `new_commit` once the transaction terminates.
		oracle.query.begin(version);

		let id = TransactionId::generate(oracle.metrics_clock(), oracle.rng());
		Ok(Self {
			engine,
			id,
			version,
			read_version: None,
			size: 0,
			count: 0,
			oracle,
			conflicts: ConflictManager::new(),
			pending_writes: PendingWrites::new(),
			duplicates: Vec::new(),
			delta_log: Vec::new(),
			preexisting_keys: HashSet::new(),
			discarded: false,
			done_query: false,
		})
	}
}

impl Drop for MultiWriteTransaction {
	fn drop(&mut self) {
		if !self.discarded {
			self.discard();
		}
	}
}

impl MultiWriteTransaction {
	/// Returns the unique ID of the transaction.
	pub fn id(&self) -> TransactionId {
		self.id
	}

	/// Returns the version for reading (uses read_version if set, otherwise base version).
	pub fn version(&self) -> CommitVersion {
		self.read_version.unwrap_or(self.version)
	}

	/// Returns the base version for writes and conflict detection.
	pub fn base_version(&self) -> CommitVersion {
		self.version
	}

	/// Sets the read version for as-of queries without affecting write/commit version.
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.read_version = Some(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.read_as_of_version_exclusive(CommitVersion(version.0 + 1));
		Ok(())
	}

	pub fn pending_writes(&self) -> &PendingWrites {
		&self.pending_writes
	}

	pub fn conflicts(&self) -> &ConflictManager {
		&self.conflicts
	}

	pub fn mark_preexisting(&mut self, key: &EncodedKey) {
		self.preexisting_keys.insert(key.as_ref().to_vec());
	}

	pub fn preexisting_keys(&self) -> &HashSet<Vec<u8>> {
		&self.preexisting_keys
	}
}

impl MultiWriteTransaction {
	/// Snapshot pending writes for later restore.
	pub fn savepoint(&self) -> WriteSavepoint {
		WriteSavepoint {
			pending_writes: self.pending_writes.clone(),
			count: self.count,
			size: self.size,
			duplicates: self.duplicates.clone(),
			delta_log_len: self.delta_log.len(),
			conflicts: self.conflicts.clone(),
			preexisting_keys: self.preexisting_keys.clone(),
		}
	}

	/// Restore pending writes from a savepoint.
	pub fn restore_savepoint(&mut self, sp: WriteSavepoint) {
		self.pending_writes = sp.pending_writes;
		self.count = sp.count;
		self.size = sp.size;
		self.duplicates = sp.duplicates;
		self.delta_log.truncate(sp.delta_log_len);
		self.conflicts = sp.conflicts;
		self.preexisting_keys = sp.preexisting_keys;
	}
}

impl MultiWriteTransaction {
	/// Marker for keys that are operated. Used by iterator implementations
	/// to register conflict tracking from inside an iterator that already
	/// borrows the transaction.
	pub fn marker(&mut self) -> Marker<'_> {
		Marker::new(&mut self.conflicts)
	}

	/// Returns a marker for keys that are operated and the pending writes manager.
	pub fn marker_with_pending_writes(&mut self) -> (Marker<'_>, &PendingWrites) {
		(Marker::new(&mut self.conflicts), &self.pending_writes)
	}

	pub fn mark_read(&mut self, k: &EncodedKey) {
		self.conflicts.mark_read(k);
	}

	pub fn mark_write(&mut self, k: &EncodedKey) {
		self.conflicts.mark_write(k);
	}

	/// Reserve capacity for `additional` more write keys ahead of a known-size bulk write.
	pub fn reserve_writes(&mut self, additional: usize) {
		self.conflicts.reserve_writes(additional);
	}

	/// See `Engine::bulk_insert_unchecked` for the safety contract.
	pub(crate) fn disable_conflict_tracking(&mut self) {
		self.conflicts.disable();
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::set", level = "debug", skip(self, row), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref()),
		value_len = row.len()
	))]
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(Pending {
			delta: Delta::Set {
				key: key.clone(),
				row,
			},
			version: self.base_version(),
		})
	}

	/// Removes a key.
	///
	/// This is done by adding a delete marker for the key at commit
	/// timestamp.  Any reads happening before this timestamp would be
	/// unaffected. Any reads after this commit would see the deletion.
	///
	/// The `row` parameter contains the deleted row for CDC and metrics.
	#[instrument(name = "transaction::command::unset", level = "debug", skip(self, row), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref()),
		value_len = row.len()
	))]
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(Pending {
			delta: Delta::Unset {
				key: key.clone(),
				row,
			},
			version: self.base_version(),
		})
	}

	/// Remove an entry without preserving deleted values.
	/// Use when only the key matters (e.g., index entries, catalog metadata).
	#[instrument(name = "transaction::command::remove", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_len = key.len()
	))]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(Pending {
			delta: Delta::Remove {
				key: key.clone(),
			},
			version: self.base_version(),
		})
	}

	/// Rolls back the transaction.
	#[instrument(name = "transaction::command::rollback", level = "debug", skip(self), fields(txn_id = %self.id))]
	pub fn rollback(&mut self) -> Result<()> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}

		self.pending_writes.rollback();
		self.conflicts.rollback();
		self.delta_log.clear();
		self.duplicates.clear();
		Ok(())
	}

	/// Returns `true` if the pending writes contains the key.
	#[instrument(name = "transaction::command::contains_key", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref())
	))]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let version = self.version();
		match self.pending_writes.get(key) {
			Some(pending) => {
				if pending.was_removed() {
					return Ok(false);
				}
				Ok(true)
			}
			None => {
				self.conflicts.mark_read(key);
				MultiVersionContains::contains(&self.engine.store, key, version)
			}
		}
	}

	/// Looks for the key in the pending writes, if such key is not in the
	/// pending writes, the end user can read the key from the database.
	#[instrument(name = "transaction::command::get", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref())
	))]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let version = self.version();
		if let Some(v) = self.pending_writes.get(key) {
			if v.row().is_some() {
				return Ok(Some(Pending {
					delta: match v.row() {
						Some(row) => Delta::Set {
							key: key.clone(),
							row: row.clone(),
						},
						None => Delta::Remove {
							key: key.clone(),
						},
					},
					version: v.version,
				}
				.into()));
			}
			return Ok(None);
		}
		self.conflicts.mark_read(key);
		Ok(MultiVersionGet::get(&self.engine.store, key, version)?.map(Into::into))
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::modify", level = "trace", skip(self, pending), fields(
		txn_id = %self.id,
		key_hex = %hex::display(pending.key().as_ref()),
		is_remove = pending.was_removed()
	))]
	fn modify(&mut self, pending: Pending) -> Result<()> {
		// Caller has already checked `discarded`.
		let cnt = self.count + 1;
		let size = self.size + self.pending_writes.estimate_size(&pending);
		if cnt >= self.pending_writes.max_batch_entries() || size >= self.pending_writes.max_batch_size() {
			return Err(TransactionError::TooLarge.into());
		}

		self.count = cnt;
		self.size = size;

		self.conflicts.mark_write(pending.key());

		// If a duplicate entry was inserted in managed mode, move it to
		// the duplicate writes slice. Add the entry to
		// duplicateWrites only if both the entries have different
		// versions. For same versions, we will overwrite the existing
		// entry.
		let key = pending.key();
		let row = pending.row();
		let version = pending.version;

		if let Some((old_key, old_value)) = self.pending_writes.remove_entry(key)
			&& old_value.version != version
		{
			self.duplicates.push(Pending {
				delta: match row {
					Some(row) => Delta::Set {
						key: old_key,
						row: row.clone(),
					},
					None => Delta::Remove {
						key: old_key,
					},
				},
				version,
			})
		}
		// Append to delta_log BEFORE moving into pending_writes so commit can replay
		// the full issuance order (including same-key sequences like Set+Unset).
		self.delta_log.push(pending.clone());
		self.pending_writes.insert(key.clone(), pending);

		Ok(())
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::commit_pending", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	fn commit_pending(&mut self) -> Result<(CommitVersion, Vec<Pending>)> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let conflict_manager = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		match self.oracle.new_commit(&mut self.done_query, base_version, conflict_manager)? {
			CreateCommitResult::Conflict(conflicts) => {
				self.conflicts = conflicts;
				Err(TransactionError::Conflict.into())
			}
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => {
				let _ = mem::take(&mut self.pending_writes);
				let duplicate_writes = mem::take(&mut self.duplicates);
				let mut all = mem::take(&mut self.delta_log);
				all.reserve(duplicate_writes.len());

				// Stamp commit version onto every delta. Issuance order is
				// preserved by delta_log's append-only construction.
				for pending in all.iter_mut() {
					pending.version = version;
				}

				for mut pending in duplicate_writes {
					pending.version = version;
					all.push(pending);
				}

				debug_assert_ne!(version, 0);

				Ok((version, all))
			}
		}
	}

	/// See `Engine::bulk_insert_unchecked` for the safety contract.
	#[instrument(name = "transaction::command::commit_pending_unchecked", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	fn commit_pending_unchecked(&mut self) -> Result<(CommitVersion, Vec<Pending>)> {
		if self.discarded {
			return Err(TransactionError::RolledBack.into());
		}

		// Drop the conflict manager without consulting it - this commit
		// is not registered in the oracle's conflict index, so the
		// previously-collected read/write keys are not needed.
		let _ = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		match self.oracle.advance_unchecked(&mut self.done_query, base_version)? {
			CreateCommitResult::Conflict(_) => {
				unreachable!("advance_unchecked never reports a conflict")
			}
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => {
				let _ = mem::take(&mut self.pending_writes);
				let duplicate_writes = mem::take(&mut self.duplicates);
				let mut all = mem::take(&mut self.delta_log);
				all.reserve(duplicate_writes.len());

				for pending in all.iter_mut() {
					pending.version = version;
				}
				for mut pending in duplicate_writes {
					pending.version = version;
					all.push(pending);
				}

				debug_assert_ne!(version, 0);

				Ok((version, all))
			}
		}
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::commit", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub fn commit(&mut self) -> Result<CommitVersion> {
		// For read-only transactions (no pending writes), skip conflict detection
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}

		// Use commit_pending to allocate the commit version via oracle BEFORE writing to storage
		// This ensures entries have the correct commit version
		let (commit_version, entries) = self.commit_pending()?;

		if entries.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}

		// Collect and optimize deltas for storage commit
		let mut raw_deltas = CowVec::with_capacity(entries.len());
		for pending in &entries {
			raw_deltas.push(pending.delta.clone());
		}
		let optimized = optimize_deltas(raw_deltas.iter().cloned(), self.preexisting_keys());
		let deltas = CowVec::new(optimized);

		MultiVersionCommit::commit(&self.engine.store, deltas.clone(), commit_version)?;

		self.discard();

		// Emit PostCommitEvent BEFORE marking the watermark as done.
		// The CDC poll actor uses done_until() as the safe upper bound for
		// fetching CDC entries from the store. If done_commit runs first,
		// there is a window where done_until >= V but V's CDC has not been
		// written to the store yet (because the CDC producer processes
		// PostCommitEvents asynchronously). A concurrent commit on another
		// thread could then produce a CDC entry at V+1 that the poll actor
		// sees, causing it to advance its checkpoint past V and permanently
		// skip V's CDC.
		self.engine.event_bus.emit(PostCommitEvent::new(deltas, commit_version));
		self.oracle.done_commit(commit_version);

		Ok(commit_version)
	}

	/// See `Engine::bulk_insert_unchecked` for the safety contract.
	#[instrument(name = "transaction::command::commit_unchecked", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub(crate) fn commit_unchecked(&mut self) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}

		let (commit_version, entries) = self.commit_pending_unchecked()?;

		if entries.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}

		let mut raw_deltas = CowVec::with_capacity(entries.len());
		for pending in &entries {
			raw_deltas.push(pending.delta.clone());
		}
		let optimized = optimize_deltas(raw_deltas.iter().cloned(), self.preexisting_keys());
		let deltas = CowVec::new(optimized);

		MultiVersionCommit::commit(&self.engine.store, deltas.clone(), commit_version)?;

		self.discard();

		// Order matters: emit PostCommitEvent before done_commit.
		// See `commit` above for the CDC checkpoint race this avoids.
		self.engine.event_bus.emit(PostCommitEvent::new(deltas, commit_version));
		self.oracle.done_commit(commit_version);

		Ok(commit_version)
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::done_query", level = "trace", skip(self), fields(txn_id = %self.id))]
	fn finish_query(&mut self) {
		if !self.done_query {
			self.done_query = true;
			self.oracle.query.done(self.version);
		}
	}

	/// Discards a created transaction. This method is very important and
	/// must be called. `commit*` methods calls this internally, however,
	/// calling this multiple times doesn't cause any issues.
	#[instrument(name = "transaction::command::discard", level = "trace", skip(self), fields(txn_id = %self.id))]
	pub fn discard(&mut self) {
		if self.discarded {
			return;
		}
		self.discarded = true;
		self.finish_query();
	}

	/// Returns true if the transaction is discarded.
	pub fn is_discard(&self) -> bool {
		self.discarded
	}
}

impl MultiWriteTransaction {
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self.range(EncodedKeyRange::prefix(prefix), 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> =
			self.range_rev(EncodedKeyRange::prefix(prefix), 1024).collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	/// Create a streaming iterator for forward range queries, merging pending writes.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// and maintains cursor state internally. Pending writes are merged with
	/// committed storage data.
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let version = self.version();
		let (mut marker, pw) = self.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		// Collect pending writes in range as owned data
		let pending: Vec<(EncodedKey, Pending)> =
			pw.range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	/// Create a streaming iterator for reverse range queries, merging pending writes.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let version = self.version();
		let (mut marker, pw) = self.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		// Collect pending writes in range as owned data (reversed)
		let pending: Vec<(EncodedKey, Pending)> =
			pw.range((start, end)).rev().map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range_rev(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}
}

/// Iterator that merges pending writes with storage iterator.
pub(crate) struct MergePendingIterator<I> {
	pending_iter: iter::Peekable<vec::IntoIter<(EncodedKey, Pending)>>,
	storage_iter: I,
	next_storage: Option<MultiVersionRow>,
	reverse: bool,
}

impl<I> MergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	pub(crate) fn new(pending: Vec<(EncodedKey, Pending)>, storage_iter: I, reverse: bool) -> Self {
		Self {
			pending_iter: pending.into_iter().peekable(),
			storage_iter,
			next_storage: None,
			reverse,
		}
	}
}

impl<I> Iterator for MergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			// Fetch next storage item if needed
			if self.next_storage.is_none() {
				self.next_storage = match self.storage_iter.next() {
					Some(Ok(v)) => Some(v),
					Some(Err(e)) => return Some(Err(e)),
					None => None,
				};
			}

			match (self.pending_iter.peek(), &self.next_storage) {
				(Some((pending_key, _)), Some(storage_val)) => {
					let cmp = pending_key.cmp(&storage_val.key);
					let should_yield_pending = if self.reverse {
						// Reverse: larger keys first
						matches!(cmp, Ordering::Greater)
					} else {
						// Forward: smaller keys first
						matches!(cmp, Ordering::Less)
					};

					if should_yield_pending {
						// Pending key comes first
						let (key, value) = self.pending_iter.next().unwrap();
						if let Some(row) = value.row() {
							return Some(Ok(MultiVersionRow {
								key,
								row: row.clone(),
								version: value.version,
							}));
						}
						// Tombstone: skip (continue loop)
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = self.pending_iter.next().unwrap();
						self.next_storage = None; // Consume storage entry
						if let Some(row) = value.row() {
							return Some(Ok(MultiVersionRow {
								key,
								row: row.clone(),
								version: value.version,
							}));
						}
						// Tombstone: skip (continue loop)
					} else {
						// Storage key comes first
						return Some(Ok(self.next_storage.take().unwrap()));
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = self.pending_iter.next().unwrap();
					if let Some(row) = value.row() {
						return Some(Ok(MultiVersionRow {
							key,
							row: row.clone(),
							version: value.version,
						}));
					}
					// Tombstone: skip (continue loop)
				}
				(None, Some(_)) => {
					// Only storage left
					return Some(Ok(self.next_storage.take().unwrap()));
				}
				(None, None) => return None,
			}
		}
	}
}
