// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::mem;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{encoded::EncodedValues, key::EncodedKey},
};
use reifydb_type::{return_error, util::hex};
use reifydb_core::error::diagnostic::transaction;
use tracing::instrument;

use crate::{
	TransactionId,
	multi::{
		conflict::ConflictManager,
		marker::Marker,
		pending::PendingWrites,
		transaction::{version::VersionProvider, *},
		types::Pending,
	},
};

pub enum TransactionKind {
	Current(CommitVersion),
	TimeTravel(CommitVersion),
}

/// TransactionManagerRx is a read-only transaction manager.
pub struct TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	id: TransactionId,
	engine: TransactionManager<L>,
	transaction: TransactionKind,
}

impl<L> TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	pub fn new_current(id: TransactionId, engine: TransactionManager<L>, version: CommitVersion) -> Self {
		Self {
			id,
			engine,
			transaction: TransactionKind::Current(version),
		}
	}

	pub fn new_time_travel(id: TransactionId, engine: TransactionManager<L>, version: CommitVersion) -> Self {
		Self {
			id,
			engine,
			transaction: TransactionKind::TimeTravel(version),
		}
	}

	pub fn id(&self) -> TransactionId {
		self.id
	}

	pub fn version(&self) -> CommitVersion {
		match self.transaction {
			TransactionKind::Current(version) => version,
			TransactionKind::TimeTravel(version) => version,
		}
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.transaction = TransactionKind::TimeTravel(version);
	}
}

impl<L> Drop for TransactionManagerQuery<L>
where
	L: VersionProvider,
{
	fn drop(&mut self) {
		// time travel transaction have no effect on multi
		if let TransactionKind::Current(version) = self.transaction {
			self.engine.inner.done_query(version);
		}
	}
}

pub struct TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	pub(super) id: TransactionId,
	pub(super) version: CommitVersion,
	pub(super) read_version: Option<CommitVersion>, // Separate read version for as_of queries
	pub(super) size: u64,
	pub(super) count: u64,
	pub(super) oracle: Arc<Oracle<L>>,
	pub(super) conflicts: ConflictManager,
	// stores any writes done by tx
	pub(super) pending_writes: PendingWrites,
	pub(super) duplicates: Vec<Pending>,

	pub(super) discarded: bool,
	pub(super) done_query: bool,
}

impl<L> Drop for TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	fn drop(&mut self) {
		if !self.discarded {
			self.discard();
		}
	}
}

impl<L> TransactionManagerCommand<L>
where
	L: VersionProvider,
{
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

	/// Returns the pending writes
	pub fn pending_writes(&self) -> &PendingWrites {
		&self.pending_writes
	}

	/// Returns the conflict manager.
	pub fn conflicts(&self) -> &ConflictManager {
		&self.conflicts
	}
}

impl<L> TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	/// This method is used to create a marker for the keys that are
	/// operated. It must be used to mark keys when end user is
	/// implementing iterators to make sure the transaction manager works
	/// correctly.
	pub fn marker(&mut self) -> Marker<'_> {
		Marker::new(&mut self.conflicts)
	}

	/// Returns a marker for the keys that are operated and the pending
	/// writes manager. As Rust's borrow checker does not allow to borrow
	/// mutable marker and the immutable pending writes manager at the same
	pub fn marker_with_pending_writes(&mut self) -> (Marker<'_>, &PendingWrites) {
		(Marker::new(&mut self.conflicts), &self.pending_writes)
	}

	/// Marks a key is read.
	pub fn mark_read(&mut self, k: &EncodedKey) {
		self.conflicts.mark_read(k);
	}

	/// Marks a key as written.
	pub fn mark_write(&mut self, k: &EncodedKey) {
		self.conflicts.mark_write(k);
	}
}

impl<L> TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	/// Set a key-value pair to the transaction.
	#[instrument(name = "transaction::command::set", level = "debug", skip(self, values), fields(
		txn_id = %self.id,
		key_hex = %hex::encode(key.as_ref()),
		value_len = values.as_ref().len()
	))]
	pub fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}

		self.set_internal(key, values)
	}

	/// Removes a key.
	///
	/// This is done by adding a delete marker for the key at commit
	/// timestamp.  Any reads happening before this timestamp would be
	/// unaffected. Any reads after this commit would see the deletion.
	///
	/// The `values` parameter contains the deleted values for CDC and metrics.
	#[instrument(name = "transaction::command::unset", level = "debug", skip(self, values), fields(
		txn_id = %self.id,
		key_hex = %hex::encode(key.as_ref()),
		value_len = values.len()
	))]
	pub fn unset(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}
		self.modify(Pending {
			delta: Delta::Unset {
				key: key.clone(),
				values,
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
			return_error!(transaction::transaction_rolled_back());
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
			return_error!(transaction::transaction_rolled_back());
		}

		self.pending_writes.rollback();
		self.conflicts.rollback();
		Ok(())
	}

	/// Returns `true` if the pending writes contains the key.
	#[instrument(name = "transaction::command::contains_key", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::encode(key.as_ref())
	))]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<Option<bool>> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}

		match self.pending_writes.get(key) {
			Some(pending) => {
				if pending.was_removed() {
					return Ok(Some(false));
				}
				// Fulfill from buffer.
				Ok(Some(true))
			}
			None => {
				// track reads. No need to track read if txn
				// serviced it internally.
				self.conflicts.mark_read(key);
				Ok(None)
			}
		}
	}

	/// Looks for the key in the pending writes, if such key is not in the
	/// pending writes, the end user can read the key from the database.
	#[instrument(name = "transaction::command::get", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_hex = %hex::encode(key.as_ref())
	))]
	pub fn get<'a, 'b: 'a>(&'a mut self, key: &'b EncodedKey) -> Result<Option<Pending>> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}

		if let Some(v) = self.pending_writes.get(key) {
			if v.was_removed() {
				return Ok(None);
			}

			Ok(Some(Pending {
				delta: match v.values() {
					Some(values) => Delta::Set {
						key: key.clone(),
						values: values.clone(),
					},
					None => Delta::Remove {
						key: key.clone(),
					},
				},
				version: v.version,
			}))
		} else {
			self.conflicts.mark_read(key);
			Ok(None)
		}
	}
}

impl<L> TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	#[instrument(name = "transaction::command::set_internal", level = "trace", skip(self, values), fields(
		txn_id = %self.id,
		key_hex = %hex::encode(key.as_ref())
	))]
	fn set_internal(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}

		self.modify(Pending {
			delta: Delta::Set {
				key: key.clone(),
				values,
			},
			version: self.base_version(),
		})
	}

	#[instrument(name = "transaction::command::modify", level = "trace", skip(self, pending), fields(
		txn_id = %self.id,
		key_hex = %hex::encode(pending.key().as_ref()),
		is_remove = pending.was_removed()
	))]
	fn modify(&mut self, pending: Pending) -> Result<()> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}

		let pending_writes = &mut self.pending_writes;

		let cnt = self.count + 1;
		// Extra encoded for the version in key.
		let size = self.size + pending_writes.estimate_size(&pending);
		if cnt >= pending_writes.max_batch_entries() || size >= pending_writes.max_batch_size() {
			return_error!(transaction::transaction_too_large());
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
		let values = pending.values();
		let version = pending.version;

		if let Some((old_key, old_value)) = pending_writes.remove_entry(key) {
			if old_value.version != version {
				self.duplicates.push(Pending {
					delta: match values {
						Some(values) => Delta::Set {
							key: old_key,
							values: values.clone(),
						},
						None => Delta::Remove {
							key: old_key,
						},
					},
					version,
				})
			}
		}
		pending_writes.insert(key.clone(), pending);

		Ok(())
	}
}

impl<L> TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	#[instrument(name = "transaction::command::commit_pending", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	pub(crate) fn commit_pending(&mut self) -> Result<(CommitVersion, Vec<Pending>)> {
		if self.discarded {
			return_error!(transaction::transaction_rolled_back());
		}

		let conflict_manager = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		match self.oracle.new_commit(&mut self.done_query, base_version, conflict_manager)? {
			CreateCommitResult::Conflict(conflicts) => {
				// If there is a conflict, we should not send
				// the updates to the write channel.
				// Instead, we should return the conflict error
				// to the user.
				self.conflicts = conflicts;
				return_error!(transaction::transaction_conflict())
			}
			CreateCommitResult::Success(version) => {
				let pending_writes = mem::take(&mut self.pending_writes);
				let duplicate_writes = mem::take(&mut self.duplicates);
				// Pre-allocate exact capacity to avoid
				// reallocations
				let mut all = Vec::with_capacity(pending_writes.len() + duplicate_writes.len());

				let process = |entries: &mut Vec<Pending>, mut pending: Pending| {
					pending.version = version;
					entries.push(pending);
				};

				pending_writes.into_iter_insertion_order().for_each(|(_k, v)| {
					let (ver, delta) = v.into_components();
					process(
						&mut all,
						Pending {
							delta,
							version: ver,
						},
					)
				});

				duplicate_writes.into_iter().for_each(|item| process(&mut all, item));

				// version should not be zero if we're inserting
				// transaction markers.
				debug_assert_ne!(version, 0);

				Ok((version, all))
			}
		}
	}
}

impl<L> TransactionManagerCommand<L>
where
	L: VersionProvider,
{
	#[instrument(name = "transaction::command::done", level = "trace", skip(self), fields(txn_id = %self.id))]
	fn done_query(&mut self) {
		if !self.done_query {
			self.done_query = true;
			self.oracle().query.done(self.version);
		}
	}

	fn oracle(&self) -> &Oracle<L> {
		&self.oracle
	}

	/// Discards a created transaction. This method is very important and
	/// must be called. `commit*` methods calls this internally, however,
	/// calling this multiple times doesn't cause any issues
	#[instrument(name = "transaction::command::discard", level = "trace", skip(self), fields(txn_id = %self.id))]
	pub fn discard(&mut self) {
		if self.discarded {
			return;
		}
		self.discarded = true;
		self.done_query();
	}

	/// Returns true if the transaction is discarded.
	pub fn is_discard(&self) -> bool {
		self.discarded
	}
}
