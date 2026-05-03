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
		types::{DeltaEntry, TransactionValue},
	},
};

pub struct WriteSavepoint {
	pub(crate) pending_writes: PendingWrites,
	pub(crate) count: u64,
	pub(crate) size: u64,
	pub(crate) duplicates: Vec<DeltaEntry>,
	pub(crate) delta_log_len: usize,
	pub(crate) conflicts: ConflictManager,
	pub(crate) preexisting_keys: HashSet<Vec<u8>>,
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Lifecycle {
	Active,
	QueryDone,
	Discarded,
}

pub struct MultiWriteTransaction {
	engine: MultiTransaction,

	pub(crate) id: TransactionId,
	pub(crate) version: CommitVersion,
	pub(crate) read_version: Option<CommitVersion>,
	pub(crate) size: u64,
	pub(crate) count: u64,
	pub(crate) oracle: Arc<Oracle<StandardVersionProvider>>,
	pub(crate) conflicts: ConflictManager,
	pub(crate) pending_writes: PendingWrites,
	pub(crate) duplicates: Vec<DeltaEntry>,

	pub(crate) delta_log: Vec<DeltaEntry>,

	pub(crate) preexisting_keys: HashSet<Vec<u8>>,

	pub(crate) lifecycle: Lifecycle,
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::new", level = "debug", skip(engine))]
	pub fn new(engine: MultiTransaction) -> Result<Self> {
		let oracle = engine.tm.oracle().clone();
		let version = oracle.version()?;
		oracle.query.register_in_flight(version);

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
			lifecycle: Lifecycle::Active,
		})
	}

	fn transition_to(&mut self, next: Lifecycle) {
		debug_assert!(matches!(
			(self.lifecycle, next),
			(Lifecycle::Active, Lifecycle::QueryDone)
				| (Lifecycle::Active, Lifecycle::Discarded)
				| (Lifecycle::QueryDone, Lifecycle::Discarded)
		));
		self.lifecycle = next;
	}
}

impl Drop for MultiWriteTransaction {
	fn drop(&mut self) {
		if self.lifecycle != Lifecycle::Discarded {
			self.discard();
		}
	}
}

impl MultiWriteTransaction {
	pub fn id(&self) -> TransactionId {
		self.id
	}

	pub fn version(&self) -> CommitVersion {
		self.read_version.unwrap_or(self.version)
	}

	pub fn base_version(&self) -> CommitVersion {
		self.version
	}

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
	pub fn marker(&mut self) -> Marker<'_> {
		Marker::new(&mut self.conflicts)
	}

	pub fn marker_with_pending_writes(&mut self) -> (Marker<'_>, &PendingWrites) {
		(Marker::new(&mut self.conflicts), &self.pending_writes)
	}

	pub fn mark_read(&mut self, k: &EncodedKey) {
		self.conflicts.mark_read(k);
	}

	pub fn mark_write(&mut self, k: &EncodedKey) {
		self.conflicts.mark_write(k);
	}

	pub fn reserve_writes(&mut self, additional: usize) {
		self.conflicts.reserve_writes(additional);
	}

	pub(crate) fn disable_conflict_tracking(&mut self) {
		self.conflicts.set_disabled();
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::set", level = "debug", skip(self, row), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref()),
		value_len = row.len()
	))]
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::Set {
				key: key.clone(),
				row,
			},
			version: self.base_version(),
		})
	}

	#[instrument(name = "transaction::command::unset", level = "debug", skip(self, row), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref()),
		value_len = row.len()
	))]
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::Unset {
				key: key.clone(),
				row,
			},
			version: self.base_version(),
		})
	}

	#[instrument(name = "transaction::command::remove", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_len = key.len()
	))]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::Remove {
				key: key.clone(),
			},
			version: self.base_version(),
		})
	}

	#[instrument(name = "transaction::command::rollback", level = "debug", skip(self), fields(txn_id = %self.id))]
	pub fn rollback(&mut self) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}

		self.pending_writes.rollback();
		self.conflicts.rollback();
		self.delta_log.clear();
		self.duplicates.clear();
		Ok(())
	}

	#[instrument(name = "transaction::command::contains_key", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref())
	))]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		if self.lifecycle == Lifecycle::Discarded {
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

	#[instrument(name = "transaction::command::get", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref())
	))]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let version = self.version();
		if let Some(v) = self.pending_writes.get(key) {
			if v.row().is_some() {
				return Ok(Some(DeltaEntry {
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

	#[instrument(name = "transaction::command::get_committed", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::display(key.as_ref())
	))]
	pub fn get_committed(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let version = self.version();
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
	fn modify(&mut self, pending: DeltaEntry) -> Result<()> {
		let cnt = self.count + 1;
		let size = self.size + self.pending_writes.estimate_size(&pending);
		if cnt >= self.pending_writes.max_batch_entries() || size >= self.pending_writes.max_batch_size() {
			return Err(TransactionError::TooLarge.into());
		}

		self.count = cnt;
		self.size = size;

		self.conflicts.mark_write(pending.key());

		let key = pending.key();
		let row = pending.row();
		let version = pending.version;

		if let Some((old_key, old_value)) = self.pending_writes.remove_entry(key)
			&& old_value.version != version
		{
			self.duplicates.push(DeltaEntry {
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
	fn commit_pending(&mut self) -> Result<(CommitVersion, Vec<DeltaEntry>)> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let conflict_manager = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		let result = self.oracle.new_commit(base_version, conflict_manager);
		self.release_read_snapshot(base_version);

		match result? {
			CreateCommitResult::Conflict(conflicts) => {
				self.conflicts = conflicts;
				Err(TransactionError::Conflict.into())
			}
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => Ok((version, self.assemble_committed_deltas(version))),
		}
	}

	#[instrument(name = "transaction::command::commit_pending_unchecked", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	fn commit_pending_unchecked(&mut self) -> Result<(CommitVersion, Vec<DeltaEntry>)> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let _ = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		let result = self.oracle.advance_unchecked(base_version);
		self.release_read_snapshot(base_version);

		match result? {
			CreateCommitResult::Conflict(_) => unreachable!("advance_unchecked never reports a conflict"),
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => Ok((version, self.assemble_committed_deltas(version))),
		}
	}

	#[inline]
	fn release_read_snapshot(&mut self, base_version: CommitVersion) {
		if self.lifecycle == Lifecycle::Active {
			self.oracle.query.mark_finished(base_version);
			self.transition_to(Lifecycle::QueryDone);
		}
	}

	#[inline]
	fn assemble_committed_deltas(&mut self, version: CommitVersion) -> Vec<DeltaEntry> {
		debug_assert_ne!(version, 0);
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
		all
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::commit", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub fn commit(&mut self) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let (commit_version, entries) = self.commit_pending()?;
		self.finalize_commit(commit_version, entries)
	}

	#[instrument(name = "transaction::command::commit_unchecked", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub(crate) fn commit_unchecked(&mut self) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let (commit_version, entries) = self.commit_pending_unchecked()?;
		self.finalize_commit(commit_version, entries)
	}

	#[inline]
	fn finalize_commit(
		&mut self,
		commit_version: CommitVersion,
		entries: Vec<DeltaEntry>,
	) -> Result<CommitVersion> {
		if entries.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let deltas = self.optimize_for_storage(&entries);
		MultiVersionCommit::commit(&self.engine.store, deltas.clone(), commit_version)?;
		self.discard();
		self.publish(commit_version, deltas);
		Ok(commit_version)
	}

	#[inline]
	fn optimize_for_storage(&self, entries: &[DeltaEntry]) -> CowVec<Delta> {
		let mut raw_deltas = CowVec::with_capacity(entries.len());
		for pending in entries {
			raw_deltas.push(pending.delta.clone());
		}
		let optimized = optimize_deltas(raw_deltas.iter().cloned(), self.preexisting_keys());
		CowVec::new(optimized)
	}

	#[inline]
	fn publish(&self, commit_version: CommitVersion, deltas: CowVec<Delta>) {
		self.engine.event_bus.emit(PostCommitEvent::new(deltas, commit_version));
		self.oracle.done_commit(commit_version);
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::discard", level = "trace", skip(self), fields(txn_id = %self.id))]
	pub fn discard(&mut self) {
		match self.lifecycle {
			Lifecycle::Discarded => return,
			Lifecycle::Active => self.oracle.query.mark_finished(self.version),
			Lifecycle::QueryDone => {}
		}
		self.transition_to(Lifecycle::Discarded);
	}

	pub fn is_discard(&self) -> bool {
		self.lifecycle == Lifecycle::Discarded
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

		let pending: Vec<(EncodedKey, DeltaEntry)> =
			pw.range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

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

		let pending: Vec<(EncodedKey, DeltaEntry)> =
			pw.range((start, end)).rev().map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range_rev(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}
}

pub(crate) struct MergePendingIterator<I> {
	pending_iter: iter::Peekable<vec::IntoIter<(EncodedKey, DeltaEntry)>>,
	storage_iter: I,
	next_storage: Option<MultiVersionRow>,
	reverse: bool,
}

impl<I> MergePendingIterator<I>
where
	I: Iterator<Item = Result<MultiVersionRow>>,
{
	pub(crate) fn new(pending: Vec<(EncodedKey, DeltaEntry)>, storage_iter: I, reverse: bool) -> Self {
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
						matches!(cmp, Ordering::Greater)
					} else {
						matches!(cmp, Ordering::Less)
					};

					if should_yield_pending {
						let (key, value) = self.pending_iter.next().unwrap();
						if let Some(row) = value.row() {
							return Some(Ok(MultiVersionRow {
								key,
								row: row.clone(),
								version: value.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.next_storage = None;
						if let Some(row) = value.row() {
							return Some(Ok(MultiVersionRow {
								key,
								row: row.clone(),
								version: value.version,
							}));
						}
					} else {
						return Some(Ok(self.next_storage.take().unwrap()));
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let Some(row) = value.row() {
						return Some(Ok(MultiVersionRow {
							key,
							row: row.clone(),
							version: value.version,
						}));
					}
				}
				(None, Some(_)) => {
					return Some(Ok(self.next_storage.take().unwrap()));
				}
				(None, None) => return None,
			}
		}
	}
}
