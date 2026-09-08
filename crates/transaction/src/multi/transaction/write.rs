// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::mem;
use std::{
	cmp::Ordering,
	collections::BTreeSet,
	iter,
	ops::{Bound, RangeBounds},
	sync::Arc,
	vec,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	common::CommitVersion,
	delta::{Delta, RemoveAnnounce},
	event::transaction::PostCommitEvent,
	interface::{
		catalog::{object::ObjectId, storage::StorageId},
		change::Change,
		store::{MultiVersionBatch, MultiVersionContains, MultiVersionGet, MultiVersionRow},
	},
	key::{
		any::TaggedKey,
		bound::{TaggedKeyBound, TaggedKeyBoundRange, object_fields},
		row::{PartitionedRowKey, RowKey, StoragePartitionedRowKey, StorageRowKey},
		tag::KeyTag,
	},
};
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
	pub(crate) preexisting_keys: BTreeSet<TaggedKey>,
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

	pub(crate) preexisting_keys: BTreeSet<TaggedKey>,

	pub(crate) lifecycle: Lifecycle,

	pub(crate) self_lease: Option<VersionLeaseGuard>,

	pending_query_pin: Option<CommitVersion>,
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::new", level = "debug", skip(engine))]
	pub fn new(engine: MultiTransaction) -> Result<Self> {
		let oracle = engine.tm.oracle().clone();
		let version = oracle.query.register_in_flight_with(|| oracle.version())?;

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
			preexisting_keys: BTreeSet::new(),
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

	pub fn mark_preexisting<K: Into<TaggedKey> + Clone>(&mut self, key: &K) {
		self.preexisting_keys.insert(key.clone().into());
	}

	pub fn preexisting_keys(&self) -> &BTreeSet<TaggedKey> {
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

	pub fn mark_read(&mut self, k: &TaggedKey) {
		self.conflicts.mark_read(k);
	}

	pub fn mark_write(&mut self, k: &TaggedKey) {
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
		key = ?key
	))]
	fn set_any(&mut self, key: TaggedKey, bytes: EncodedBytes) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let encoded = key.encode();
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::Set {
					key,
					bytes,
				},
				version: self.base_version(),
			},
		)
	}

	pub fn set<K: Into<TaggedKey> + Clone>(&mut self, key: &K, bytes: impl Into<EncodedBytes>) -> Result<()> {
		self.set_any(key.clone().into(), bytes.into())
	}

	#[instrument(name = "transaction::command::remove_with_pre", level = "trace", skip(self, pre), fields(
		txn_id = %self.id,
		key = ?key,
		value_len = pre.len()
	))]
	fn remove_with_pre_any(&mut self, key: TaggedKey, pre: EncodedBytes) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let encoded = key.encode();
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::remove_announced(key, pre),
				version: self.base_version(),
			},
		)
	}

	pub fn remove_with_pre<K: Into<TaggedKey> + Clone>(&mut self, key: &K, pre: EncodedBytes) -> Result<()> {
		self.remove_with_pre_any(key.clone().into(), pre)
	}

	#[instrument(name = "transaction::command::remove", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key = ?key
	))]
	fn remove_any(&mut self, key: TaggedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let encoded = key.encode();
		let announce = match self.get(&key)? {
			Some(found) => RemoveAnnounce::Announced {
				pre: found.bytes().clone(),
			},
			None => RemoveAnnounce::Silent,
		};
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::Remove {
					key,
					announce,
				},
				version: self.base_version(),
			},
		)
	}

	pub fn remove<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<()> {
		self.remove_any(key.clone().into())
	}

	#[instrument(name = "transaction::command::remove_unobserved", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key = ?key
	))]
	fn remove_unobserved_any(&mut self, key: TaggedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let encoded = key.encode();
		let announce = match self.get(&key)? {
			Some(found) => RemoveAnnounce::Unobserved {
				pre: found.bytes().clone(),
			},
			None => RemoveAnnounce::Silent,
		};
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::Remove {
					key,
					announce,
				},
				version: self.base_version(),
			},
		)
	}

	pub fn remove_unobserved<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<()> {
		self.remove_unobserved_any(key.clone().into())
	}

	#[instrument(name = "transaction::command::remove_unobserved_with_pre", level = "trace", skip(self, pre), fields(
		txn_id = %self.id,
		key = ?key,
		value_len = pre.len()
	))]
	fn remove_unobserved_with_pre_any(&mut self, key: TaggedKey, pre: EncodedBytes) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let encoded = key.encode();
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::remove_unobserved(key, pre),
				version: self.base_version(),
			},
		)
	}

	pub fn remove_unobserved_with_pre<K: Into<TaggedKey> + Clone>(
		&mut self,
		key: &K,
		pre: EncodedBytes,
	) -> Result<()> {
		self.remove_unobserved_with_pre_any(key.clone().into(), pre)
	}

	#[instrument(name = "transaction::command::remove_silent", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key = ?key
	))]
	fn remove_silent_any(&mut self, key: TaggedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let encoded = key.encode();
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::remove_silent(key),
				version: self.base_version(),
			},
		)
	}

	pub fn remove_silent<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<()> {
		self.remove_silent_any(key.clone().into())
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

	#[instrument(name = "transaction::command::contains_key", level = "trace", skip(self, key), fields(
		txn_id = %self.id
	))]
	pub fn contains<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<bool> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let key = key.clone().into();
		let version = self.version();
		match self.pending_writes.get(&key) {
			Some(pending) => {
				if pending.was_removed() {
					return Ok(false);
				}
				Ok(true)
			}
			None => {
				self.conflicts.mark_read(&key);
				MultiVersionContains::contains(&self.engine.store, &key, version)
			}
		}
	}

	#[instrument(name = "transaction::command::get", level = "trace", skip(self, key), fields(
		txn_id = %self.id
	))]
	pub fn get<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<Option<TransactionValue>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}

		let key = key.clone().into();
		let version = self.version();
		if let Some(v) = self.pending_writes.get(&key) {
			if let Some(bytes) = v.bytes() {
				return Ok(Some(DeltaEntry {
					delta: Delta::Set {
						key: v.key().clone(),
						bytes: bytes.clone(),
					},
					version: v.version,
				}
				.into()));
			}
			return Ok(None);
		}
		self.conflicts.mark_read(&key);
		Ok(MultiVersionGet::get(&self.engine.store, &key, version)?.map(Into::into))
	}

	#[instrument(name = "transaction::command::get_committed", level = "trace", skip(self, key), fields(
		txn_id = %self.id
	))]
	pub fn get_committed<K: Into<TaggedKey> + Clone>(&mut self, key: &K) -> Result<Option<TransactionValue>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let key = key.clone().into();
		let version = self.version();
		self.conflicts.mark_read(&key);
		Ok(MultiVersionGet::get(&self.engine.store, &key, version)?.map(Into::into))
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::modify", level = "trace", skip(self, pending), fields(
		txn_id = %self.id,
		key_hex = %hex_display(encoded.as_ref()),
		is_remove = pending.was_removed()
	))]
	fn modify(&mut self, encoded: EncodedKey, pending: DeltaEntry) -> Result<()> {
		reifydb_assertions! {
			assert!(
				!matches!(pending.key(), TaggedKey::OperatorState(_)),
				"operator state must reach the operator store through the committer split, never the \
				 multi store: {}",
				hex_display(encoded.as_ref())
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

		let version = pending.version;

		let superseded = self
			.pending_writes
			.get_entry(pending.key())
			.filter(|(_, old_value)| old_value.version != version)
			.map(|(_, old_value)| old_value.key().clone());

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
		self.pending_writes.insert(pending);

		Ok(())
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::commit_pending", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	fn commit_pending(&mut self, deltas: CowVec<Delta>) -> Result<CommitVersion> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let conflict_manager = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		let result = self.oracle.new_commit(base_version, conflict_manager, deltas);
		self.release_read_snapshot(base_version);

		match result? {
			CreateCommitResult::Conflict(conflicts) => {
				self.conflicts = conflicts;
				Err(TransactionError::Conflict.into())
			}
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => {
				self.pending_query_pin = Some(version);
				self.clear_pending_state();
				Ok(version)
			}
		}
	}

	#[instrument(name = "transaction::command::commit_pending_unchecked", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	fn commit_pending_unchecked(&mut self, deltas: CowVec<Delta>) -> Result<CommitVersion> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let _ = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		let result = self.oracle.advance_unchecked(base_version, deltas);
		self.release_read_snapshot(base_version);

		match result? {
			CreateCommitResult::Conflict(_) => unreachable!("advance_unchecked never reports a conflict"),
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => {
				self.pending_query_pin = Some(version);
				self.clear_pending_state();
				Ok(version)
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
	fn build_deltas(&self) -> CowVec<Delta> {
		CowVec::new(optimize_deltas(
			self.delta_log.iter().chain(self.duplicates.iter()).map(|pending| pending.delta.clone()),
			self.preexisting_keys(),
		))
	}

	#[inline]
	fn clear_pending_state(&mut self) {
		let _ = mem::take(&mut self.pending_writes);
		let _ = mem::take(&mut self.duplicates);
		let _ = mem::take(&mut self.delta_log);
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::commit", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub fn commit(&mut self, flow_changes: Vec<Change>) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let deltas = self.build_deltas();
		let commit_version = self.commit_pending(deltas.clone())?;
		self.finalize_commit(commit_version, deltas, flow_changes)
	}

	#[instrument(name = "transaction::command::commit_unchecked", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub(crate) fn commit_unchecked(&mut self, flow_changes: Vec<Change>) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		let deltas = self.build_deltas();
		let commit_version = self.commit_pending_unchecked(deltas.clone())?;
		self.finalize_commit(commit_version, deltas, flow_changes)
	}

	#[inline]
	fn finalize_commit(
		&mut self,
		commit_version: CommitVersion,
		deltas: CowVec<Delta>,
		flow_changes: Vec<Change>,
	) -> Result<CommitVersion> {
		reifydb_assertions! {
			assert_ne!(
				commit_version, 0,
				"finalize_commit reached with commit_version=0 but {} non-empty deltas; \
				 CommitVersion(0) is the empty/discarded sentinel callers read as 'nothing \
				 committed', so committing real deltas at it would silently drop them",
				deltas.len()
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
		self.discard();
		self.publish(commit_version, deltas, flow_changes);
		Ok(commit_version)
	}

	#[inline]
	fn publish(&self, commit_version: CommitVersion, deltas: CowVec<Delta>, flow_changes: Vec<Change>) {
		self.oracle.done_commit(commit_version);
		self.engine.event_bus.emit(PostCommitEvent::new(deltas, commit_version, flow_changes));
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
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch<TaggedKey>> {
		let items: Vec<_> = self
			.range_encoded(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch<TaggedKey>> {
		let items: Vec<_> = self
			.range_encoded_rev(EncodedKeyRange::prefix(prefix), RangeScope::All, 1024)
			.collect::<Result<Vec<_>>>()?;
		Ok(MultiVersionBatch {
			items,
			has_more: false,
		})
	}

	fn range_encoded(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range_encoded(range.clone());

		let pending: Vec<(TaggedKey, DeltaEntry)> = pw
			.iter()
			.filter(|(k, _)| range.contains(&k.encode()))
			.map(|(_, v)| (v.delta.key().clone(), v.clone()))
			.collect();

		let storage_iter = self.engine.store.range(range, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	fn range_encoded_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range_encoded(range.clone());

		let mut pending: Vec<(TaggedKey, DeltaEntry)> = pw
			.iter()
			.filter(|(k, _)| range.contains(&k.encode()))
			.map(|(_, v)| (v.delta.key().clone(), v.clone()))
			.collect();
		pending.reverse();

		let storage_iter = self.engine.store.range_rev(range, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}

	pub fn range(
		&mut self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let encoded = range.encode();
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range(range.clone());

		let pending: Vec<(TaggedKey, DeltaEntry)> = pw
			.range((range.start.as_ref(), range.end.as_ref()))
			.map(|(_, v)| (v.delta.key().clone(), v.clone()))
			.collect();

		let storage_iter = self.engine.store.range(encoded, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	pub fn range_row(
		&mut self,
		storage: StorageId,
		start: Bound<StorageRowKey>,
		end: Bound<StorageRowKey>,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<StorageRowKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let range = row_bounds_to_typed(storage, &start, &end);
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range(range.clone());

		let pending: Vec<(StorageRowKey, DeltaEntry)> = pw
			.iter()
			.filter_map(|(k, v)| {
				let TaggedKeyBound::Key(TaggedKey::Row(decoded)) = k else {
					return None;
				};
				(decoded.storage == storage && range.contains(k))
					.then(|| (StorageRowKey::new(decoded.row), v.clone()))
			})
			.collect();

		let storage_iter = self.engine.store.range_row(storage, start, end, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	pub fn range_partitioned_row(
		&mut self,
		storage: StorageId,
		start: Bound<StoragePartitionedRowKey>,
		end: Bound<StoragePartitionedRowKey>,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<StoragePartitionedRowKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let range = partitioned_row_bounds_to_typed(storage, &start, &end);
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range(range.clone());

		let pending: Vec<(StoragePartitionedRowKey, DeltaEntry)> = pw
			.iter()
			.filter_map(|(k, v)| {
				let TaggedKeyBound::Key(TaggedKey::PartitionedRow(decoded)) = k else {
					return None;
				};
				(decoded.storage == storage && range.contains(k)).then(|| {
					(StoragePartitionedRowKey::new(decoded.partition, decoded.row), v.clone())
				})
			})
			.collect();

		let storage_iter =
			self.engine.store.range_partitioned_row(storage, start, end, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	pub fn range_persistence(
		&mut self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let encoded = range.encode();
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range(range.clone());

		let pending: Vec<(TaggedKey, DeltaEntry)> = pw
			.range((range.start.as_ref(), range.end.as_ref()))
			.map(|(_, v)| (v.delta.key().clone(), v.clone()))
			.collect();

		let storage_iter = self.engine.store.range_persistence(encoded, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	pub fn range_rev(
		&mut self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let encoded = range.encode();
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range(range.clone());

		let pending: Vec<(TaggedKey, DeltaEntry)> = pw
			.range((range.start.as_ref(), range.end.as_ref()))
			.rev()
			.map(|(_, v)| (v.delta.key().clone(), v.clone()))
			.collect();

		let storage_iter = self.engine.store.range_rev(encoded, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}

	pub fn range_rev_persistence(
		&mut self,
		range: TaggedKeyBoundRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow<TaggedKey>>> + Send + '_> {
		let multi_scope = scope.into_multi(self.version());
		let encoded = range.encode();
		let (mut marker, pw) = self.marker_with_pending_writes();

		marker.mark_range(range.clone());

		let pending: Vec<(TaggedKey, DeltaEntry)> = pw
			.range((range.start.as_ref(), range.end.as_ref()))
			.rev()
			.map(|(_, v)| (v.delta.key().clone(), v.clone()))
			.collect();

		let storage_iter = self.engine.store.range_rev_persistence(encoded, multi_scope, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_core::{common::CommitVersion, interface::catalog::id::QueueId, key::queue::QueueDeduplicationKey};
	use reifydb_value::{
		util::cowvec::CowVec,
		value::{duration::Duration, partition::Partition, row_number::RowNumber},
	};

	use super::*;
	use crate::multi::transaction::MultiTransaction;

	fn test_key(s: &str) -> QueueDeduplicationKey {
		QueueDeduplicationKey::new(QueueId(1), s.as_bytes().iter().map(|b| !b).collect::<Vec<u8>>())
	}

	fn test_bytes(s: &str) -> EncodedBytes {
		let mut ser = KeySerializer::new();
		ser.extend_str(s);
		EncodedBytes(CowVec::new(ser.finish().as_slice().to_vec()))
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
		let deltas = txn.build_deltas();
		let commit_version = txn.commit_pending(deltas.clone()).unwrap();
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

		let result = txn.finalize_commit(commit_version, deltas, vec![]);
		assert_eq!(
			result.unwrap(),
			commit_version,
			"commit of our own freshly-allocated version must succeed even under a racing query watermark"
		);
	}

	fn storage() -> StorageId {
		StorageId::table(7)
	}

	fn row_range_bytes(start: Bound<StorageRowKey>, end: Bound<StorageRowKey>) -> EncodedKeyRange {
		row_bounds_to_typed(storage(), &start, &end).encode()
	}

	fn partitioned_range_bytes(
		start: Bound<StoragePartitionedRowKey>,
		end: Bound<StoragePartitionedRowKey>,
	) -> EncodedKeyRange {
		partitioned_row_bounds_to_typed(storage(), &start, &end).encode()
	}

	#[test]
	fn an_unbounded_row_range_still_spans_exactly_the_storages_own_bytes() {
		// The conflict manager now tracks these ranges typed, so the typed spelling of an
		// open end has to land on the same byte as the storage_end producer it replaced.
		// Anything wider silently reports conflicts that are not there; anything narrower
		// silently misses real ones, and neither shows up as a failure anywhere else.
		let range = row_range_bytes(Bound::Unbounded, Bound::Unbounded);
		assert_eq!(range.start, Bound::Included(RowKey::storage_start(storage())));
		assert_eq!(range.end, Bound::Included(RowKey::storage_end(storage())));

		let range = partitioned_range_bytes(Bound::Unbounded, Bound::Unbounded);
		assert_eq!(range.start, Bound::Included(PartitionedRowKey::storage_start(storage())));
		assert_eq!(range.end, Bound::Included(PartitionedRowKey::storage_end(storage())));
	}

	#[test]
	fn a_bounded_row_range_encodes_the_keys_it_names() {
		let low = StorageRowKey::new(RowNumber(1));
		let high = StorageRowKey::new(RowNumber(9));
		let range = row_range_bytes(Bound::Included(low), Bound::Excluded(high));
		assert_eq!(range.start, Bound::Included(RowKey::encoded(storage(), RowNumber(1))));
		assert_eq!(range.end, Bound::Excluded(RowKey::encoded(storage(), RowNumber(9))));

		let low = StoragePartitionedRowKey::new(Partition(3), RowNumber(1));
		let high = StoragePartitionedRowKey::new(Partition(3), RowNumber(9));
		let range = partitioned_range_bytes(Bound::Excluded(low), Bound::Included(high));
		assert_eq!(
			range.start,
			Bound::Excluded(PartitionedRowKey::encoded(storage(), Partition(3), RowNumber(1)))
		);
		assert_eq!(
			range.end,
			Bound::Included(PartitionedRowKey::encoded(storage(), Partition(3), RowNumber(9)))
		);
	}

	#[test]
	fn a_row_range_contains_the_same_keys_typed_as_its_bytes_do() {
		// The pending-writes filter reads containment off the typed bounds while the storage
		// iterator reads it off the bytes; a disagreement hides a transaction's own writes.
		// Row numbers encode descending, so a non-empty span runs from the higher number to
		// the lower one. Spelling it 3..9 would make every case empty and let the end bound
		// answer alone, which hides whatever the start bound does.
		let rows: Vec<RowNumber> = (0u64..12).map(RowNumber).collect();
		let low = StorageRowKey::new(RowNumber(9));
		let high = StorageRowKey::new(RowNumber(3));
		let cases = [
			(Bound::Unbounded, Bound::Unbounded),
			(Bound::Included(low), Bound::Unbounded),
			(Bound::Excluded(low), Bound::Unbounded),
			(Bound::Excluded(low), Bound::Excluded(high)),
			(Bound::Included(low), Bound::Included(high)),
			(Bound::Unbounded, Bound::Excluded(high)),
			(Bound::Unbounded, Bound::Included(high)),
		];

		for (start, end) in cases {
			let typed = row_bounds_to_typed(storage(), &start, &end);
			let bytes = typed.encode();
			for row in &rows {
				let key = RowKey::new(storage(), *row);
				let encoded = RowKey::encoded(storage(), *row);
				assert_eq!(
					typed.contains(&TaggedKeyBound::Key(TaggedKey::Row(key))),
					bytes.contains(&encoded),
					"row {row:?} in {start:?}..{end:?}"
				);
			}
		}
	}
}

pub(crate) struct MergePendingIterator<I, K = EncodedKey> {
	pending_iter: iter::Peekable<vec::IntoIter<(K, DeltaEntry)>>,
	storage_iter: I,
	next_storage: Option<MultiVersionRow<K>>,
	reverse: bool,
}

impl<I, K> MergePendingIterator<I, K>
where
	K: Ord,
	I: Iterator<Item = Result<MultiVersionRow<K>>>,
{
	pub(crate) fn new(pending: Vec<(K, DeltaEntry)>, storage_iter: I, reverse: bool) -> Self {
		Self {
			pending_iter: pending.into_iter().peekable(),
			storage_iter,
			next_storage: None,
			reverse,
		}
	}
}

impl<I, K> Iterator for MergePendingIterator<I, K>
where
	K: Ord,
	I: Iterator<Item = Result<MultiVersionRow<K>>>,
{
	type Item = Result<MultiVersionRow<K>>;

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

fn row_bounds_to_typed(
	storage: StorageId,
	start: &Bound<StorageRowKey>,
	end: &Bound<StorageRowKey>,
) -> TaggedKeyBoundRange {
	let bound = |k: &StorageRowKey| TaggedKeyBound::Key(TaggedKey::Row(RowKey::new(storage, k.row())));
	let lower = match start {
		Bound::Included(k) => Bound::Included(bound(k)),
		Bound::Excluded(k) => Bound::Excluded(bound(k)),
		Bound::Unbounded => Bound::Included(storage_span_start(KeyTag::Row, storage)),
	};
	let upper = match end {
		Bound::Included(k) => Bound::Included(bound(k)),
		Bound::Excluded(k) => Bound::Excluded(bound(k)),
		Bound::Unbounded => Bound::Included(storage_span_end(KeyTag::Row, storage)),
	};
	TaggedKeyBoundRange {
		start: lower,
		end: upper,
	}
}

fn partitioned_row_bounds_to_typed(
	storage: StorageId,
	start: &Bound<StoragePartitionedRowKey>,
	end: &Bound<StoragePartitionedRowKey>,
) -> TaggedKeyBoundRange {
	let bound = |k: &StoragePartitionedRowKey| {
		TaggedKeyBound::Key(TaggedKey::PartitionedRow(PartitionedRowKey::new(storage, k.partition(), k.row())))
	};
	let lower = match start {
		Bound::Included(k) => Bound::Included(bound(k)),
		Bound::Excluded(k) => Bound::Excluded(bound(k)),
		Bound::Unbounded => Bound::Included(storage_span_start(KeyTag::PartitionedRow, storage)),
	};
	let upper = match end {
		Bound::Included(k) => Bound::Included(bound(k)),
		Bound::Excluded(k) => Bound::Excluded(bound(k)),
		Bound::Unbounded => Bound::Included(storage_span_end(KeyTag::PartitionedRow, storage)),
	};
	TaggedKeyBoundRange {
		start: lower,
		end: upper,
	}
}

fn storage_span_start(kind: KeyTag, storage: StorageId) -> TaggedKeyBound {
	TaggedKeyBound::prefix(kind, object_fields(ObjectId::from(storage)))
}

fn storage_span_end(kind: KeyTag, storage: StorageId) -> TaggedKeyBound {
	TaggedKeyBound::prefix(kind, object_fields(ObjectId::from(storage).prev()))
}
