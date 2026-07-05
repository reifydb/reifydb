// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::mem;
use std::{cmp::Ordering, collections::HashSet, iter, ops::RangeBounds, sync::Arc, vec};

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::transaction::PostCommitEvent,
	interface::{
		change::Change,
		store::{
			MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow,
		},
	},
	key::{Key, kind::KeyKind},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sub_raft::message::Command;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	reifydb_assertions,
	util::{cowvec::CowVec, hex},
};
use tracing::instrument;

use super::{MultiTransaction, version::StandardVersionProvider};
use crate::{
	TransactionId,
	delta::optimize_deltas,
	error::TransactionError,
	multi::{
		RangeScope,
		conflict::ConflictManager,
		lease::VersionLeaseGuard,
		marker::Marker,
		oracle::{CreateCommitResult, Oracle},
		pending::PendingWrites,
		types::{DeltaEntry, TransactionValue},
	},
};

pub struct WriteSavepoint {
	pub(crate) pending_writes: PendingWrites,
	pub(crate) count: u64,
	pub(crate) size: ByteSize,
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
	pub(crate) size: ByteSize,
	pub(crate) count: u64,
	pub(crate) oracle: Arc<Oracle<StandardVersionProvider>>,
	pub(crate) conflicts: ConflictManager,
	pub(crate) pending_writes: PendingWrites,
	pub(crate) duplicates: Vec<DeltaEntry>,

	pub(crate) delta_log: Vec<DeltaEntry>,

	pub(crate) preexisting_keys: HashSet<Vec<u8>>,

	pub(crate) lifecycle: Lifecycle,

	pub(crate) self_lease: Option<VersionLeaseGuard>,

	pending_query_pin: Option<CommitVersion>,
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
			size: ByteSize::ZERO,
			count: 0,
			oracle,
			conflicts: ConflictManager::new(),
			pending_writes: PendingWrites::new(),
			duplicates: Vec::new(),
			delta_log: Vec::new(),
			preexisting_keys: HashSet::new(),
			lifecycle: Lifecycle::Active,
			self_lease: None,
			pending_query_pin: None,
		})
	}

	fn transition_to(&mut self, next: Lifecycle) {
		reifydb_assertions! {
			assert!(matches!(
				(self.lifecycle, next),
				(Lifecycle::Active, Lifecycle::QueryDone)
					| (Lifecycle::Active, Lifecycle::Discarded)
					| (Lifecycle::QueryDone, Lifecycle::Discarded)
			));
		}
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
	#[instrument(name = "transaction::command::set", level = "trace", skip(self, row), fields(
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

	#[instrument(name = "transaction::command::unset", level = "trace", skip(self, row), fields(
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

	#[instrument(name = "transaction::command::drop_key", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_len = key.len()
	))]
	pub fn drop_key(&mut self, key: &EncodedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::Drop {
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

		if !matches!(Key::kind(pending.key()), Some(KeyKind::DictionaryEntry | KeyKind::DictionaryEntryIndex)) {
			self.conflicts.mark_write(pending.key());
		}

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
			CreateCommitResult::Success(version) => {
				self.pending_query_pin = Some(version);
				Ok((version, self.assemble_committed_deltas(version)))
			}
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
			CreateCommitResult::Success(version) => {
				self.pending_query_pin = Some(version);
				Ok((version, self.assemble_committed_deltas(version)))
			}
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
		reifydb_assertions! {
			assert_ne!(version, 0);
		}
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
	pub fn commit(&mut self, flow_changes: Vec<Change>) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let (commit_version, entries) = self.commit_pending()?;
		self.finalize_commit(commit_version, entries, flow_changes)
	}

	#[instrument(name = "transaction::command::commit_unchecked", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub(crate) fn commit_unchecked(&mut self, flow_changes: Vec<Change>) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let (commit_version, entries) = self.commit_pending_unchecked()?;
		self.finalize_commit(commit_version, entries, flow_changes)
	}

	#[inline]
	fn finalize_commit(
		&mut self,
		commit_version: CommitVersion,
		entries: Vec<DeltaEntry>,
		flow_changes: Vec<Change>,
	) -> Result<CommitVersion> {
		if entries.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		reifydb_assertions! {
			assert_ne!(
				commit_version, 0,
				"finalize_commit reached with commit_version=0 but {} non-empty entries; \
				 CommitVersion(0) is the empty/discarded sentinel callers read as 'nothing \
				 committed', so committing real deltas at it would silently drop them",
				entries.len()
			);
		}
		let self_lease = self.oracle.leases.try_acquire(commit_version, self.oracle.query.done_until()).ok();
		reifydb_assertions! {
			assert!(
				self_lease.is_some(),
				"self-version lease on freshly-committed version {} must succeed: it is the newest \
				 version so query.done_until() < it; failing means the historical-GC cutoff passed our \
				 own commit version before its post-commit hooks ran",
				commit_version.0
			);
		}
		self.self_lease = self_lease;
		if let Some(v) = self.pending_query_pin.take() {
			self.oracle.query.mark_finished(v);
		}
		let deltas = self.optimize_for_storage(&entries);
		let flow_changes = match self.propose_to_raft(commit_version, &deltas, flow_changes)? {
			Ok(version) => return Ok(version),
			Err(flow_changes) => flow_changes,
		};
		MultiVersionCommit::commit(&self.engine.store, deltas.clone(), commit_version)?;
		self.discard();
		self.publish(commit_version, deltas, flow_changes);
		Ok(commit_version)
	}

	#[cfg(not(target_arch = "wasm32"))]
	#[inline]
	fn propose_to_raft(
		&mut self,
		commit_version: CommitVersion,
		deltas: &CowVec<Delta>,
		flow_changes: Vec<Change>,
	) -> Result<core::result::Result<CommitVersion, Vec<Change>>> {
		let raft_handle = self.engine.raft.read().clone();
		let Some(raft) = raft_handle else {
			return Ok(Err(flow_changes));
		};
		let cmd = Command::WriteMulti {
			deltas: deltas.to_vec(),
			version: commit_version,
			changes: flow_changes,
		};
		let propose_result = raft.propose(cmd);
		self.oracle.done_commit(commit_version);
		self.discard();
		match propose_result {
			Ok(_) => Ok(Ok(commit_version)),
			Err(e) => Err(TransactionError::RaftProposeFailed {
				message: e.to_string(),
			}
			.into()),
		}
	}

	#[cfg(target_arch = "wasm32")]
	#[inline]
	fn propose_to_raft(
		&mut self,
		_commit_version: CommitVersion,
		_deltas: &CowVec<Delta>,
		flow_changes: Vec<Change>,
	) -> Result<core::result::Result<CommitVersion, Vec<Change>>> {
		Ok(Err(flow_changes))
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
	fn publish(&self, commit_version: CommitVersion, deltas: CowVec<Delta>, flow_changes: Vec<Change>) {
		self.engine.event_bus.emit(PostCommitEvent::new(deltas, commit_version, flow_changes));
		self.oracle.done_commit(commit_version);
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::discard", level = "trace", skip(self), fields(txn_id = %self.id))]
	pub fn discard(&mut self) {
		if let Some(v) = self.pending_query_pin.take() {
			self.oracle.query.mark_finished(v);
		}
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

	pub(crate) fn take_self_lease(&mut self) -> Option<VersionLeaseGuard> {
		self.self_lease.take()
	}
}

impl MultiWriteTransaction {
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self
			.range(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		let items: Vec<_> = self
			.range_rev(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let (mut marker, pw) = self.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		let pending: Vec<(EncodedKey, DeltaEntry)> =
			pw.range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range(range, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let (mut marker, pw) = self.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		let pending: Vec<(EncodedKey, DeltaEntry)> =
			pw.range((start, end)).rev().map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range_rev(range, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::serialize;
	use reifydb_core::common::CommitVersion;
	use reifydb_value::{util::cowvec::CowVec, value::duration::Duration};

	use super::*;
	use crate::multi::transaction::MultiTransaction;

	fn test_key(s: &str) -> EncodedKey {
		EncodedKey::new(serialize(&s))
	}

	fn test_row(s: &str) -> EncodedRow {
		EncodedRow(CowVec::new(serialize(&s.to_string())))
	}

	/// Regression test for the self-lease race in `finalize_commit`.
	///
	/// `commit_version` is allocated in `Oracle::allocate_commit_version` and, before the fix,
	/// registered only on the `command` watermark - nothing protects it on the `query` watermark
	/// until `finalize_commit` runs. A concurrent transaction finishing at a higher, registered
	/// query version can then race `query.done_until()` past our own commit_version before we get
	/// there, tripping the `reifydb_assertions!` in `finalize_commit`.
	///
	/// This test does not rely on real thread timing. It calls the private `commit_pending()`
	/// directly to obtain `commit_version` exactly as `commit()` would, then sends a racing
	/// transaction's begin+finish through the *real* async watermark mechanism
	/// (`register_in_flight`/`mark_finished`) and uses a bounded `wait_for_mark_timeout` to
	/// deterministically observe whether the query watermark could advance past `commit_version`.
	#[test]
	fn commit_version_stays_protected_from_query_watermark_race_until_finalized() {
		let engine = MultiTransaction::testing();
		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("race-key"), test_row("race-value")).unwrap();

		// Allocate commit_version exactly as commit() would, without finalizing it yet.
		let (commit_version, entries) = txn.commit_pending().unwrap();
		assert_ne!(commit_version, CommitVersion(0));

		// Simulate an unrelated, concurrent transaction finishing at a HIGHER version - the
		// real-world trigger for the flake (any other thread's begin_command()/commit() or
		// begin_query() completing while ours is still mid-flight).
		let racer = CommitVersion(commit_version.0 + 1);
		txn.oracle.query.register_in_flight(racer);
		txn.oracle.query.mark_finished(racer);

		// Bounded, deterministic check: before the fix, commit_version was never registered on
		// the query watermark, so nothing blocks the racer's Done from advancing done_until past
		// it - this resolves almost immediately (well under the bound). After the fix,
		// commit_version is registered-but-unfinished on the query watermark, so done_until can
		// never reach `racer` while it's open - this reliably times out. Either outcome is
		// reached deterministically within the bound; it is not a best-effort sleep.
		let racer_observed =
			txn.oracle.query.wait_for_mark_timeout(racer, Duration::from_milliseconds(300).unwrap());
		assert!(
			!racer_observed,
			"query watermark advanced to {} before commit_version {} was finalized - the \
			 historical-GC cutoff raced past our own not-yet-leased commit version",
			racer.0, commit_version.0
		);

		let result = txn.finalize_commit(commit_version, entries, vec![]);
		assert_eq!(
			result.unwrap(),
			commit_version,
			"commit of our own freshly-allocated version must succeed even under a racing query watermark"
		);
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
