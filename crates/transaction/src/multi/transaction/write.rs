// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::mem;
use std::{cmp::Ordering, collections::HashSet, iter, ops::RangeBounds, sync::Arc, vec};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
#[cfg(reifydb_assertions)]
use reifydb_core::key::{EncodableKey, operator_state::OperatorStateKey};
use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, RemoveAnnounce},
	event::transaction::PostCommitEvent,
	interface::{
		change::Change,
		store::{
			MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow,
		},
	},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sub_raft::message::Command;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	reifydb_assertions,
	util::{cowvec::CowVec, hex::display as hex_display},
};
use tracing::{instrument, warn};

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
	pub(crate) preexisting_keys: HashSet<EncodedKey>,
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

	pub(crate) preexisting_keys: HashSet<EncodedKey>,

	pub(crate) lifecycle: Lifecycle,

	pub(crate) self_lease: Option<VersionLeaseGuard>,

	pending_query_pin: Option<CommitVersion>,
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::new", level = "debug", skip(engine))]
	pub fn new(engine: MultiTransaction) -> Result<Self> {
		let oracle = engine.tm.oracle().clone();
		let version = oracle.query.register_in_flight_with(|| oracle.version())?;

		let applied = oracle.command.wait_for_mark(version.0);
		if !applied {
			warn!(
				version = version.0,
				"command transaction opened before the commit watermark reached its snapshot; reads at this version may miss commits that are still being applied"
			);
		}
		reifydb_assertions! {
			assert!(
				applied,
				"waiting for the commit watermark to reach snapshot {} timed out; opening the \
				 transaction anyway reads a snapshot whose commits are not yet all applied",
				version.0
			);
		}

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
		self.read_version = Some(CommitVersion(version.0.saturating_sub(1)));
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
		self.preexisting_keys.insert(key.clone());
	}

	pub fn preexisting_keys(&self) -> &HashSet<EncodedKey> {
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
	#[instrument(name = "transaction::command::set", level = "trace", skip(self, bytes), fields(
		txn_id = %self.id,
		key_hex = %hex_display(key.as_ref()),
		value_len = bytes.len()
	))]
	pub fn set(&mut self, key: &EncodedKey, bytes: EncodedBytes) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::Set {
				key: key.clone(),
				bytes,
			},
			version: self.base_version(),
		})
	}

	#[instrument(name = "transaction::command::remove_with_pre", level = "trace", skip(self, pre), fields(
		txn_id = %self.id,
		key_hex = %hex_display(key.as_ref()),
		value_len = pre.len()
	))]
	pub fn remove_with_pre(&mut self, key: &EncodedKey, pre: EncodedBytes) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::remove_announced(key.clone(), pre),
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
		let announce = match self.get(key)? {
			Some(found) => RemoveAnnounce::Announced {
				pre: found.bytes().clone(),
			},
			None => RemoveAnnounce::Silent,
		};
		self.modify(DeltaEntry {
			delta: Delta::Remove {
				key: key.clone(),
				announce,
			},
			version: self.base_version(),
		})
	}

	#[instrument(name = "transaction::command::remove_silent", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key_len = key.len()
	))]
	pub fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		self.modify(DeltaEntry {
			delta: Delta::remove_silent(key.clone()),
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
		key_hex = %hex_display(key.as_ref())
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
		key_hex = %hex_display(key.as_ref())
	))]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let version = self.version();
		if let Some(v) = self.pending_writes.get(key) {
			if let Some(bytes) = v.bytes() {
				return Ok(Some(DeltaEntry {
					delta: Delta::Set {
						key: key.clone(),
						bytes: bytes.clone(),
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
		key_hex = %hex_display(key.as_ref())
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
		key_hex = %hex_display(pending.key().as_ref()),
		is_remove = pending.was_removed()
	))]
	fn modify(&mut self, pending: DeltaEntry) -> Result<()> {
		reifydb_assertions! {
			assert!(
				OperatorStateKey::decode(pending.key()).is_none(),
				"operator state must reach the arena through the committer split, never the \
				 multi store: {}",
				hex_display(pending.key().as_ref())
			);
		}

		let cnt = self.count + 1;
		let size = self.size + self.pending_writes.estimate_size(&pending);
		if cnt >= self.pending_writes.max_batch_entries() || size >= self.pending_writes.max_batch_size() {
			return Err(TransactionError::TooLarge.into());
		}

		self.count = cnt;
		self.size = size;

		self.conflicts.mark_write(pending.key());

		let key = pending.key();
		let version = pending.version;

		let superseded = self
			.pending_writes
			.get_entry(key)
			.filter(|(_, old_value)| old_value.version != version)
			.map(|(old_key, _)| old_key.clone());

		if let Some(old_key) = superseded {
			self.duplicates.push(DeltaEntry {
				delta: match &pending.delta {
					Delta::Set {
						bytes,
						..
					} => Delta::Set {
						key: old_key,
						bytes: bytes.clone(),
					},
					Delta::Remove {
						announce,
						..
					} => Delta::Remove {
						key: old_key,
						announce: announce.clone(),
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

		let proposed = match self.propose_to_raft(commit_version, &deltas, flow_changes) {
			Ok(proposed) => proposed,
			Err(err) => {
				self.oracle.done_commit(commit_version);
				return Err(err);
			}
		};
		let flow_changes = match proposed {
			Ok(version) => return Ok(version),
			Err(flow_changes) => flow_changes,
		};
		if let Err(err) = MultiVersionCommit::commit(&self.engine.store, deltas.clone(), commit_version) {
			self.oracle.done_commit(commit_version);
			return Err(err);
		}
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
		CowVec::new(optimize_deltas(
			entries.iter().map(|pending| pending.delta.clone()),
			self.preexisting_keys(),
		))
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

	fn test_bytes(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(serialize(&s.to_string())))
	}

	#[test]
	fn commit_version_stays_protected_from_query_watermark_race_until_finalized() {
		// An allocated but unfinalized commit version must hold the query watermark down. If a
		// racing higher version can advance done_until past it, the historical-GC cutoff crosses
		// a commit version whose own post-commit hooks have not run.
		let engine = MultiTransaction::testing();
		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("race-key"), test_bytes("race-value")).unwrap();

		// Allocate commit_version exactly as commit() would, without finalizing it yet.
		let (commit_version, entries) = txn.commit_pending().unwrap();
		assert_ne!(commit_version, CommitVersion(0));

		// An unrelated transaction finishing at a higher version is the real-world trigger.
		let racer = CommitVersion(commit_version.0 + 1);
		txn.oracle.query.register_in_flight(racer);
		txn.oracle.query.mark_finished(racer);

		// Bounded wait, not a sleep: while commit_version is open done_until can never reach the
		// racer, so both outcomes resolve deterministically inside the bound.
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
						if let Some(bytes) = value.bytes() {
							return Some(Ok(MultiVersionRow {
								key,
								bytes: bytes.clone(),
								version: value.version,
							}));
						}
					} else if matches!(cmp, Ordering::Equal) {
						let (key, value) = self.pending_iter.next().unwrap();
						self.next_storage = None;
						if let Some(bytes) = value.bytes() {
							return Some(Ok(MultiVersionRow {
								key,
								bytes: bytes.clone(),
								version: value.version,
							}));
						}
					} else {
						return Some(Ok(self.next_storage.take().unwrap()));
					}
				}
				(Some(_), None) => {
					let (key, value) = self.pending_iter.next().unwrap();
					if let Some(bytes) = value.bytes() {
						return Some(Ok(MultiVersionRow {
							key,
							bytes: bytes.clone(),
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
