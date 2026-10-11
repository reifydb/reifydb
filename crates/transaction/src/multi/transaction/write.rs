// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::mem;
use std::{
	cmp::Ordering,
	collections::{BTreeMap, BTreeSet},
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
	common::{ChangeVersion, CommitVersion, SourceVersion},
	delta::{Delta, RemoveAnnounce, SetPre},
	event::transaction::PostCommitEvent,
	interface::{
		catalog::{object::ObjectId, storage::StorageId},
		store::{MultiVersionBatch, MultiVersionContains, MultiVersionGet, MultiVersionRow},
	},
	key::{
		any::TaggedKey,
		bound::{TaggedKeyBound, TaggedKeyBoundRange, object_fields},
		row::{PartitionedRowKey, RowKey, StoragePartitionedRowKey, StorageRowKey},
		tag::KeyTag,
	},
};
use reifydb_store_commit::VersionedGetResult;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	reifydb_assertions,
	util::{cowvec::CowVec, hex::display as hex_display},
};
use tracing::{instrument, warn};

use super::{MultiTransaction, version::StandardVersionProvider};
#[cfg(feature = "testing")]
use crate::counting::OpCounts;
use crate::{
	TransactionId,
	delta::optimize_deltas,
	error::TransactionError,
	multi::{
		RangeScope,
		conflict::ConflictManager,
		lease::VersionLeaseGuard,
		marker::Marker,
		oracle::{CommitRebase, CreateCommitResult, Oracle},
		pending::PendingWrites,
		types::{DeltaEntry, TransactionValue},
	},
};

pub type Rebase = Arc<dyn Fn(&EncodedBytes) -> Result<EncodedBytes> + Send + Sync>;

pub struct WriteSavepoint {
	pub(crate) pending_writes: PendingWrites,
	pub(crate) count: u64,
	pub(crate) size: ByteSize,
	pub(crate) duplicates: Vec<DeltaEntry>,
	pub(crate) delta_log_len: usize,
	pub(crate) removed_after_set_len: usize,
	pub(crate) conflicts: ConflictManager,
	pub(crate) rebases: BTreeMap<TaggedKey, (EncodedBytes, Vec<Rebase>)>,
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
	source: Option<SourceVersion>,
	pub(crate) size: ByteSize,
	pub(crate) count: u64,
	pub(crate) oracle: Arc<Oracle<StandardVersionProvider>>,
	pub(crate) conflicts: ConflictManager,
	pub(crate) pending_writes: PendingWrites,
	pub(crate) duplicates: Vec<DeltaEntry>,

	pub(crate) delta_log: Vec<DeltaEntry>,

	removed_after_set: Vec<TaggedKey>,

	committed_pres: BTreeMap<TaggedKey, EncodedBytes>,

	rebases: BTreeMap<TaggedKey, (EncodedBytes, Vec<Rebase>)>,

	pub(crate) lifecycle: Lifecycle,

	pub(crate) self_lease: Option<VersionLeaseGuard>,

	#[cfg(feature = "testing")]
	pub(crate) op_counts: OpCounts,

	pending_query_pin: Option<CommitVersion>,
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::multi::new", level = "debug", skip(engine))]
	pub fn new(engine: MultiTransaction) -> Result<Self> {
		let oracle = engine.tm.oracle().clone();
		let version = oracle.query.register_in_flight_with(|| oracle.version())?;

		let id = TransactionId::generate(oracle.metrics_clock(), oracle.rng());
		Ok(Self {
			engine,
			id,
			version,
			read_version: None,
			source: None,
			size: ByteSize::ZERO,
			count: 0,
			oracle,
			conflicts: ConflictManager::new(),
			pending_writes: PendingWrites::new(),
			duplicates: Vec::new(),
			delta_log: Vec::new(),
			removed_after_set: Vec::new(),
			committed_pres: BTreeMap::new(),
			rebases: BTreeMap::new(),
			lifecycle: Lifecycle::Active,
			self_lease: None,
			pending_query_pin: None,
			#[cfg(feature = "testing")]
			op_counts: OpCounts::default(),
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

	pub fn stamp_source(&mut self, source: SourceVersion) {
		self.source = Some(source);
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
}

impl MultiWriteTransaction {
	pub fn savepoint(&self) -> WriteSavepoint {
		WriteSavepoint {
			pending_writes: self.pending_writes.clone(),
			count: self.count,
			size: self.size,
			duplicates: self.duplicates.clone(),
			delta_log_len: self.delta_log.len(),
			removed_after_set_len: self.removed_after_set.len(),
			conflicts: self.conflicts.clone(),
			rebases: self.rebases.clone(),
		}
	}

	pub fn restore_savepoint(&mut self, sp: WriteSavepoint) {
		self.pending_writes = sp.pending_writes;
		self.count = sp.count;
		self.size = sp.size;
		self.duplicates = sp.duplicates;
		self.delta_log.truncate(sp.delta_log_len);
		self.removed_after_set.truncate(sp.removed_after_set_len);
		self.conflicts = sp.conflicts;
		self.rebases = sp.rebases;
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).sets += 1;
		}
		let encoded = key.encode();
		self.modify(
			encoded,
			DeltaEntry {
				delta: Delta::Set {
					key,
					bytes,
					pre: SetPre::Unknown,
				},
				version: self.base_version(),
			},
		)
	}

	pub fn set<K: Into<TaggedKey> + Clone>(&mut self, key: &K, bytes: impl Into<EncodedBytes>) -> Result<()> {
		self.set_any(key.clone().into(), bytes.into())
	}

	pub fn set_rebased(&mut self, key: &TaggedKey, bytes: EncodedBytes, rebase: Rebase) -> Result<()> {
		let pending = self.pending_writes.get(key).map(|entry| entry.bytes().cloned());
		match (pending, self.rebases.get_mut(key)) {
			(None, _) => {
				self.rebases.insert(key.clone(), (bytes.clone(), vec![rebase]));
			}
			(Some(Some(pending)), Some((written, steps))) if pending == *written => {
				*written = bytes.clone();
				steps.push(rebase);
			}
			_ => {
				self.rebases.remove(key);
			}
		}
		self.set_any(key.clone(), bytes)
	}

	#[instrument(name = "transaction::command::set_many", level = "trace", skip_all, fields(
		txn_id = %self.id,
		entries = entries.len()
	))]
	pub fn set_many(&mut self, entries: Vec<(TaggedKey, EncodedBytes)>) -> Result<()> {
		self.set_many_with_pre(entries.into_iter().map(|(key, bytes)| (key, bytes, SetPre::Unknown)).collect())
	}

	#[instrument(name = "transaction::command::set_many_with_pre", level = "trace", skip_all, fields(
		txn_id = %self.id,
		entries = entries.len()
	))]
	pub fn set_many_with_pre(&mut self, entries: Vec<(TaggedKey, EncodedBytes, SetPre)>) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		if entries.is_empty() {
			return Ok(());
		}
		#[cfg(feature = "testing")]
		{
			self.op_counts.set_many_call();
			for (key, _, _) in &entries {
				self.op_counts.key(key.kind()).batched += 1;
			}
		}
		let version = self.base_version();
		let pending: Vec<DeltaEntry> = entries
			.into_iter()
			.map(|(key, bytes, pre)| DeltaEntry {
				delta: Delta::Set {
					key,
					bytes,
					pre,
				},
				version,
			})
			.collect();
		reifydb_assertions! {
			for entry in &pending {
				assert!(
					!matches!(entry.key(), TaggedKey::OperatorState(_)),
					"operator state must reach the operator store through the committer split, never \
					 the multi store: {}",
					hex_display(entry.key().encode().as_ref())
				);
			}
		}
		let size = pending
			.iter()
			.fold(ByteSize::ZERO, |total, entry| total + self.pending_writes.estimate_size(entry));
		self.admit(pending.len() as u64, size)?;
		self.conflicts.reserve_writes(pending.len());
		for entry in pending {
			self.apply(entry);
		}
		Ok(())
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).removes += 1;
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).removes += 1;
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

	#[instrument(name = "transaction::command::remove_many", level = "trace", skip(self, entries), fields(
		txn_id = %self.id,
		keys = entries.len()
	))]
	pub fn remove_many(&mut self, entries: Vec<(TaggedKey, Option<EncodedBytes>)>) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		if entries.is_empty() {
			return Ok(());
		}
		#[cfg(feature = "testing")]
		{
			self.op_counts.remove_many_call();
			for (key, _) in &entries {
				self.op_counts.key(key.kind()).batched_removes += 1;
			}
		}
		let version = self.base_version();
		let mut removed: BTreeSet<EncodedKey> = BTreeSet::new();
		let first: Vec<bool> = entries.iter().map(|(key, _)| removed.insert(key.encode())).collect();
		let unread: Vec<TaggedKey> = entries
			.iter()
			.zip(&first)
			.filter(|((_, pre), first)| **first && pre.is_none())
			.map(|((key, _), _)| key.clone())
			.collect();
		let mut read = self.get_many(&unread)?.into_iter();
		let mut pending: Vec<DeltaEntry> = Vec::with_capacity(entries.len());
		for ((key, pre), first) in entries.into_iter().zip(first) {
			let announce = match pre {
				Some(pre) => RemoveAnnounce::Announced {
					pre,
				},
				None if !first => RemoveAnnounce::Silent,
				None => match read.next().flatten() {
					Some(found) => RemoveAnnounce::Announced {
						pre: found.bytes().clone(),
					},
					None => RemoveAnnounce::Silent,
				},
			};
			pending.push(DeltaEntry {
				delta: Delta::Remove {
					key,
					announce,
				},
				version,
			});
		}
		reifydb_assertions! {
			for entry in &pending {
				assert!(
					!matches!(entry.key(), TaggedKey::OperatorState(_)),
					"operator state must reach the operator store through the committer split, never \
					 the multi store: {}",
					hex_display(entry.key().encode().as_ref())
				);
			}
		}
		let size = pending
			.iter()
			.fold(ByteSize::ZERO, |total, entry| total + self.pending_writes.estimate_size(entry));
		self.admit(pending.len() as u64, size)?;
		self.conflicts.reserve_writes(pending.len());
		for entry in pending {
			self.apply(entry);
		}
		Ok(())
	}

	#[instrument(name = "transaction::command::remove_unobserved", level = "trace", skip(self), fields(
		txn_id = %self.id,
		key = ?key
	))]
	fn remove_unobserved_any(&mut self, key: TaggedKey) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).removes += 1;
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).removes += 1;
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).removes += 1;
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

	#[instrument(name = "transaction::multi::rollback", level = "debug", skip(self), fields(txn_id = %self.id))]
	pub fn rollback(&mut self) -> Result<()> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}

		self.pending_writes.rollback();
		self.conflicts.rollback();
		self.delta_log.clear();
		self.duplicates.clear();
		self.removed_after_set.clear();
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).contains += 1;
		}
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).gets += 1;
		}
		let version = self.version();
		if let Some(v) = self.pending_writes.get(&key) {
			if let Some(bytes) = v.bytes() {
				return Ok(Some(DeltaEntry {
					delta: Delta::Set {
						key: v.key().clone(),
						bytes: bytes.clone(),
						pre: SetPre::Unknown,
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
		#[cfg(feature = "testing")]
		{
			self.op_counts.key(key.kind()).committed_gets += 1;
		}
		let version = self.version();
		self.conflicts.mark_read(&key);
		Ok(MultiVersionGet::get(&self.engine.store, &key, version)?.map(Into::into))
	}

	#[instrument(name = "transaction::command::get_committed_many", level = "trace", skip(self, keys), fields(
		txn_id = %self.id,
		keys = keys.len()
	))]
	pub fn get_committed_many(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<TransactionValue>>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		#[cfg(feature = "testing")]
		self.op_counts.get_committed_many_call();
		let version = self.version();
		let encoded: Vec<EncodedKey> = keys.iter().map(TaggedKey::encode).collect();
		for key in keys {
			self.conflicts.mark_read(key);
		}
		let found = self.engine.store.get_many_versioned(&encoded, version)?;
		Ok(keys.iter()
			.zip(&encoded)
			.map(|(key, encoded)| match found.get(encoded) {
				Some(VersionedGetResult::Value {
					value,
					version,
				}) => Some(MultiVersionRow {
					key: key.clone(),
					bytes: EncodedBytes(value.clone()),
					version: *version,
				}
				.into()),
				_ => None,
			})
			.collect())
	}

	#[instrument(name = "transaction::command::get_many", level = "trace", skip(self, keys), fields(
		txn_id = %self.id,
		keys = keys.len()
	))]
	pub fn get_many(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<TransactionValue>>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		#[cfg(feature = "testing")]
		{
			self.op_counts.get_many_call();
			for key in keys {
				self.op_counts.key(key.kind()).batched_gets += 1;
			}
		}
		self.read_many(keys)
	}

	#[instrument(name = "transaction::command::contains_many", level = "trace", skip(self, keys), fields(
		txn_id = %self.id,
		keys = keys.len()
	))]
	pub fn contains_many(&mut self, keys: &[TaggedKey]) -> Result<Vec<bool>> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		if keys.is_empty() {
			return Ok(Vec::new());
		}
		#[cfg(feature = "testing")]
		{
			self.op_counts.contains_many_call();
			for key in keys {
				self.op_counts.key(key.kind()).batched_contains += 1;
			}
		}
		Ok(self.read_many(keys)?.iter().map(Option::is_some).collect())
	}

	fn read_many(&mut self, keys: &[TaggedKey]) -> Result<Vec<Option<TransactionValue>>> {
		let version = self.version();
		let mut found: Vec<Option<TransactionValue>> = Vec::with_capacity(keys.len());
		let mut unread: Vec<usize> = Vec::new();
		for (slot, key) in keys.iter().enumerate() {
			match self.pending_writes.get(key) {
				Some(pending) => found.push(pending.bytes().map(|bytes| {
					DeltaEntry {
						delta: Delta::Set {
							key: pending.key().clone(),
							bytes: bytes.clone(),
							pre: SetPre::Unknown,
						},
						version: pending.version,
					}
					.into()
				})),
				None => {
					self.conflicts.mark_read(key);
					unread.push(slot);
					found.push(None);
				}
			}
		}
		if unread.is_empty() {
			return Ok(found);
		}
		let snapshot: Vec<TaggedKey> = unread.iter().map(|&slot| keys[slot].clone()).collect();
		for (slot, committed) in unread.into_iter().zip(self.engine.get_many(&snapshot, version)?) {
			found[slot] = committed.map(Into::into);
		}
		Ok(found)
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

		let size = self.pending_writes.estimate_size(&pending);
		self.admit(1, size)?;
		self.apply(pending);
		Ok(())
	}

	fn admit(&mut self, entries: u64, size: ByteSize) -> Result<()> {
		let cnt = self.count + entries;
		let size = self.size + size;
		if cnt >= self.pending_writes.max_batch_entries() || size >= self.pending_writes.max_batch_size() {
			return Err(TransactionError::TooLarge.into());
		}

		self.count = cnt;
		self.size = size;
		Ok(())
	}

	fn apply(&mut self, pending: DeltaEntry) {
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
						pre: SetPre::Unknown,
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

		if matches!(pending.delta, Delta::Remove { .. })
			&& matches!(
				self.pending_writes.get_entry(pending.key()),
				Some((
					_,
					DeltaEntry {
						delta: Delta::Set { .. },
						..
					}
				))
			) {
			self.removed_after_set.push(pending.key().clone());
		}

		self.delta_log.push(pending.clone());
		self.pending_writes.insert(pending);
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::command::commit_pending", level = "debug", skip(self), fields(
		txn_id = %self.id,
		pending_count = self.pending_writes.len()
	))]
	fn commit_pending(&mut self, deltas: CowVec<Delta>) -> Result<(CommitVersion, CowVec<Delta>)> {
		if self.lifecycle == Lifecycle::Discarded {
			return Err(TransactionError::RolledBack.into());
		}
		let conflict_manager = mem::take(&mut self.conflicts);
		let base_version = self.base_version();

		let result = match self.take_rebase() {
			None => self
				.oracle
				.new_commit(base_version, conflict_manager, deltas.clone())
				.map(|result| (result, deltas)),
			Some(rebase) => self.oracle.new_commit_rebased(base_version, conflict_manager, deltas, rebase),
		};
		self.release_read_snapshot(base_version);
		let (result, deltas) = result?;

		match result {
			CreateCommitResult::Conflict(conflicts) => {
				self.conflicts = conflicts;
				Err(TransactionError::Conflict.into())
			}
			CreateCommitResult::TooOld => Err(TransactionError::TooOld.into()),
			CreateCommitResult::Success(version) => {
				self.pending_query_pin = Some(version);
				self.clear_pending_state();
				Ok((version, deltas))
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
			&self.committed_pres,
		))
	}

	fn resolve_removed_after_set(&mut self) -> Result<()> {
		let mut keys = mem::take(&mut self.removed_after_set);
		keys.sort();
		keys.dedup();
		let mut first: BTreeSet<&TaggedKey> = BTreeSet::new();
		let mut known: BTreeSet<&TaggedKey> = BTreeSet::new();
		for entry in &self.delta_log {
			if first.insert(entry.key())
				&& matches!(
					entry.delta,
					Delta::Set {
						pre: SetPre::Absent | SetPre::Present(_),
						..
					}
				) {
				known.insert(entry.key());
			}
		}
		let keys: Vec<TaggedKey> = keys.into_iter().filter(|key| !known.contains(key)).collect();
		if keys.is_empty() {
			return Ok(());
		}
		let found = self.get_committed_many(&keys)?;
		self.committed_pres = keys
			.into_iter()
			.zip(found)
			.filter_map(|(key, found)| Some((key, found?.bytes().clone())))
			.collect();
		Ok(())
	}

	fn take_rebase(&mut self) -> Option<CommitRebase> {
		let rebases: Vec<(TaggedKey, EncodedBytes, Vec<Rebase>)> = mem::take(&mut self.rebases)
			.into_iter()
			.filter(|(key, (written, _))| {
				self.pending_writes.get(key).and_then(|entry| entry.bytes()) == Some(written)
			})
			.map(|(key, (written, steps))| (key, written, steps))
			.collect();
		if rebases.is_empty() {
			return None;
		}
		let store = self.engine.store.clone();
		Some(Box::new(move |deltas: &mut CowVec<Delta>, latest: CommitVersion| {
			for (key, written, steps) in rebases {
				let Some(found) = MultiVersionGet::get(&store, &key, latest)? else {
					continue;
				};
				let mut rebased = found.bytes;
				for step in &steps {
					rebased = step(&rebased)?;
				}
				if rebased == written {
					continue;
				}
				for delta in deltas.make_mut() {
					if let Delta::Set {
						key: set_key,
						bytes,
						..
					} = delta && *set_key == key
					{
						*bytes = rebased.clone();
					}
				}
			}
			Ok(())
		}))
	}

	fn forget_present_tags(&mut self) {
		for entry in &mut self.delta_log {
			if let Delta::Set {
				pre: pre @ SetPre::Present(_),
				..
			} = &mut entry.delta
			{
				*pre = SetPre::Unknown;
			}
		}
	}

	#[inline]
	fn clear_pending_state(&mut self) {
		let _ = mem::take(&mut self.pending_writes);
		let _ = mem::take(&mut self.duplicates);
		let _ = mem::take(&mut self.delta_log);
		let _ = mem::take(&mut self.removed_after_set);
		let _ = mem::take(&mut self.committed_pres);
	}
}

impl MultiWriteTransaction {
	#[instrument(name = "transaction::multi::commit", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub fn commit(&mut self) -> Result<CommitVersion> {
		self.commit_with(|_| {})
	}

	pub fn commit_with(&mut self, before_publish: impl FnOnce(CommitVersion)) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			before_publish(CommitVersion(0));
			return Ok(CommitVersion(0));
		}
		let deltas = self.prepare_commit()?;
		self.commit_prepared(deltas, before_publish)
	}

	fn prepare_commit(&mut self) -> Result<CowVec<Delta>> {
		self.resolve_removed_after_set()?;
		Ok(self.build_deltas())
	}

	fn commit_prepared(
		&mut self,
		deltas: CowVec<Delta>,
		before_publish: impl FnOnce(CommitVersion),
	) -> Result<CommitVersion> {
		let (commit_version, deltas) = self.commit_pending(deltas)?;
		self.finalize_commit(commit_version, deltas, before_publish)
	}

	#[instrument(name = "transaction::multi::commit_unchecked", level = "debug", skip(self), fields(pending_count = self.pending_writes().len()))]
	pub(crate) fn commit_unchecked(&mut self) -> Result<CommitVersion> {
		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(CommitVersion(0));
		}
		self.forget_present_tags();
		self.resolve_removed_after_set()?;
		let deltas = self.build_deltas();
		let commit_version = self.commit_pending_unchecked(deltas.clone())?;
		self.finalize_commit(commit_version, deltas, |_| {})
	}

	#[inline]
	fn finalize_commit(
		&mut self,
		commit_version: CommitVersion,
		deltas: CowVec<Delta>,
		before_publish: impl FnOnce(CommitVersion),
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
		before_publish(commit_version);
		self.publish(commit_version, deltas);
		Ok(commit_version)
	}

	#[inline]
	fn publish(&self, commit_version: CommitVersion, deltas: CowVec<Delta>) {
		self.oracle.done_commit(commit_version);
		let version = ChangeVersion {
			commit: commit_version,
			source: self.source.unwrap_or(SourceVersion::from(commit_version)),
		};
		self.engine.event_bus.emit(PostCommitEvent::new(deltas, version));
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

#[cfg(test)]
mod tests {
	use reifydb_codec::key::serializer::KeySerializer;
	use reifydb_core::{
		common::CommitVersion, event::EventListener, interface::catalog::id::QueueId,
		key::queue::QueueDeduplicationKey,
	};
	use reifydb_runtime::sync::mutex::Mutex;
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
		let (commit_version, _) = txn.commit_pending(deltas.clone()).unwrap();
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

		let result = txn.finalize_commit(commit_version, deltas, |_| {});
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

	fn committed_with(engine: &MultiTransaction, writes: &[(&str, &str)]) {
		let mut txn = engine.begin_command().unwrap();
		for (key, bytes) in writes {
			txn.set(&test_key(key), test_bytes(bytes)).unwrap();
		}
		txn.commit().unwrap();
	}

	fn stored(engine: &MultiTransaction, key: &str) -> Option<EncodedBytes> {
		let mut txn = engine.begin_command().unwrap();
		txn.get(&test_key(key)).unwrap().map(|found| found.bytes().clone())
	}

	fn resolved_deltas(txn: &mut MultiWriteTransaction) -> Vec<Delta> {
		txn.resolve_removed_after_set().unwrap();
		txn.build_deltas().iter().cloned().collect()
	}

	#[test]
	fn a_committed_key_set_then_removed_without_a_mark_is_deleted_from_storage() {
		// The writer removed the row it overwrote; a merge that cancels the pair leaves the committed row
		// readable at every later version.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("committed", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("committed"), test_bytes("v2")).unwrap();
		txn.remove(&test_key("committed")).unwrap();
		txn.set(&test_key("fresh"), test_bytes("f1")).unwrap();
		txn.remove(&test_key("fresh")).unwrap();
		txn.commit().unwrap();

		assert_eq!((stored(&engine, "committed"), stored(&engine, "fresh")), (None, None));
	}

	#[test]
	fn the_unchecked_commit_resolves_set_then_remove_the_same_way() {
		// Unchecked commits carry flow and bulk writes; resolving only the checked path leaves their
		// cancelled deletes in storage.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("committed", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("committed"), test_bytes("v2")).unwrap();
		txn.remove(&test_key("committed")).unwrap();
		txn.commit_unchecked().unwrap();

		assert_eq!(stored(&engine, "committed"), None);
	}

	#[test]
	fn set_then_remove_publishes_one_remove_with_the_committed_pre_image() {
		// The delta is what the producer publishes: a cancelled pair publishes nothing, and an overwrite
		// pre-image publishes bytes no consumer ever saw.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("committed", "v1"), ("again", "a1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("committed"), test_bytes("v2")).unwrap();
		txn.remove(&test_key("committed")).unwrap();
		txn.set(&test_key("fresh"), test_bytes("f1")).unwrap();
		txn.remove(&test_key("fresh")).unwrap();
		txn.remove(&test_key("again")).unwrap();
		txn.set(&test_key("again"), test_bytes("a2")).unwrap();
		txn.remove(&test_key("again")).unwrap();

		assert_eq!(
			resolved_deltas(&mut txn),
			vec![
				Delta::remove_announced(test_key("committed").into(), test_bytes("v1")),
				Delta::remove_announced(test_key("again").into(), test_bytes("a1")),
			]
		);
	}

	#[test]
	fn a_key_with_a_known_pre_image_is_neither_read_nor_changed() {
		// A writer that tags its set already resolves the pair without a read; reading it again costs a read
		// per removed row and would replace the pre-image it publishes.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("known", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_many_with_pre(vec![(
			test_key("known").into(),
			test_bytes("v2"),
			SetPre::Present(test_bytes("v1")),
		)])
		.unwrap();
		txn.remove(&test_key("known")).unwrap();

		assert_eq!(
			resolved_deltas(&mut txn),
			vec![Delta::remove_announced(test_key("known").into(), test_bytes("v1"))]
		);
		#[cfg(feature = "testing")]
		assert_eq!(txn.op_counts.get_committed_many_calls(), 0);
	}

	#[cfg(feature = "testing")]
	#[test]
	fn an_unchecked_commit_reads_a_tagged_set_then_remove_instead_of_trusting_the_tag() {
		// An unchecked commit sits outside the conflict window, so a pre-image tagged at its snapshot can be
		// stale when it commits; it must fall back to the store like an untagged write.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("tagged", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_many_with_pre(vec![(
			test_key("tagged").into(),
			test_bytes("v2"),
			SetPre::Present(test_bytes("v1")),
		)])
		.unwrap();
		txn.remove(&test_key("tagged")).unwrap();
		txn.commit_unchecked().unwrap();

		assert_eq!(txn.op_counts.get_committed_many_calls(), 1);
	}

	#[cfg(feature = "testing")]
	#[test]
	fn every_set_then_remove_of_a_commit_is_resolved_by_one_read() {
		// The resolution must stay one batched read per commit, never one per removed row, and a commit
		// without such a pair must not read at all.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("a", "a1"), ("b", "b1")]);

		let mut txn = engine.begin_command().unwrap();
		for key in ["a", "b", "c"] {
			txn.set(&test_key(key), test_bytes("new")).unwrap();
			txn.remove(&test_key(key)).unwrap();
		}
		txn.set(&test_key("a"), test_bytes("again")).unwrap();
		txn.remove(&test_key("a")).unwrap();
		resolved_deltas(&mut txn);
		assert_eq!(txn.op_counts.get_committed_many_calls(), 1);

		let mut plain = engine.begin_command().unwrap();
		plain.set(&test_key("d"), test_bytes("d1")).unwrap();
		plain.remove(&test_key("a")).unwrap();
		resolved_deltas(&mut plain);
		assert_eq!(plain.op_counts.get_committed_many_calls(), 0);
	}

	#[test]
	fn a_remove_rolled_back_to_a_savepoint_is_not_resolved() {
		// A remove undone by a savepoint is not part of the commit; resolving it would delete a row the
		// transaction kept.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("kept", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("kept"), test_bytes("v2")).unwrap();
		let savepoint = txn.savepoint();
		txn.remove(&test_key("kept")).unwrap();
		txn.restore_savepoint(savepoint);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "kept"), Some(test_bytes("v2")));
	}

	#[test]
	fn the_committed_read_behind_a_delete_is_seen_by_conflict_detection() {
		// The delete is decided from the committed row; a concurrent commit that replaces that row after the
		// read must fail this transaction instead of leaving the replacement behind a delete of the old one.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("contested", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("contested"), test_bytes("v2")).unwrap();
		txn.remove(&test_key("contested")).unwrap();
		txn.resolve_removed_after_set().unwrap();
		committed_with(&engine, &[("contested", "v3")]);
		let deltas = txn.build_deltas();

		assert!(txn.commit_pending(deltas).is_err(), "the committed read was invalidated");
	}

	#[test]
	fn a_set_then_remove_rolled_back_to_a_savepoint_is_neither_read_nor_conflicted() {
		// The rolled-back remove is not part of the commit; reading its key costs a read and marks it, so a
		// concurrent write to a key this transaction no longer touches would abort the commit.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("rolled_back", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		let savepoint = txn.savepoint();
		txn.set(&test_key("rolled_back"), test_bytes("v2")).unwrap();
		txn.remove(&test_key("rolled_back")).unwrap();
		txn.restore_savepoint(savepoint);
		txn.set(&test_key("other"), test_bytes("o1")).unwrap();
		committed_with(&engine, &[("rolled_back", "v3")]);

		assert!(txn.commit().is_ok(), "the rolled-back key must not take part in conflict detection");
		#[cfg(feature = "testing")]
		assert_eq!(txn.op_counts.get_committed_many_calls(), 0);
		assert_eq!(stored(&engine, "rolled_back"), Some(test_bytes("v3")));
	}

	fn unchecked_with(engine: &MultiTransaction, writes: &[(&str, &str)]) {
		let mut txn = engine.begin_command().unwrap();
		for (key, bytes) in writes {
			txn.set(&test_key(key), test_bytes(bytes)).unwrap();
		}
		txn.commit_unchecked().unwrap();
	}

	fn appended(bytes: &EncodedBytes, tag: &str) -> EncodedBytes {
		let mut out = bytes.to_vec();
		out.extend_from_slice(tag.as_bytes());
		EncodedBytes(CowVec::new(out))
	}

	fn appending(tag: &'static str) -> Rebase {
		Arc::new(move |bytes: &EncodedBytes| Ok(appended(bytes, tag)))
	}

	#[test]
	fn a_rebased_write_is_replayed_on_what_an_unchecked_commit_left() {
		// An unchecked commit lands outside the conflict window; writing back the value derived from the
		// snapshot would silently erase it.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(appended(&test_bytes("v2"), "+d")));
	}

	#[test]
	fn a_rebased_write_nobody_raced_commits_once_as_written() {
		// Without a racing commit the replay reproduces the written bytes, so the published change must be
		// exactly what was written.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		let written = appended(&test_bytes("v1"), "+d");
		txn.set_rebased(&test_key("meta").into(), written.clone(), appending("+d")).unwrap();
		let deltas = txn.prepare_commit().unwrap();
		let (_, published) = txn.commit_pending(deltas).unwrap();

		assert_eq!(
			published.iter().cloned().collect::<Vec<_>>(),
			vec![Delta::Set {
				key: test_key("meta").into(),
				bytes: written,
				pre: SetPre::Unknown,
			}]
		);
	}

	#[test]
	fn rebased_writes_to_one_key_replay_in_order() {
		// Every statement's change must survive the replay, in the order the statements ran.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		let first = appended(&test_bytes("v1"), "+a");
		txn.set_rebased(&test_key("meta").into(), first.clone(), appending("+a")).unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&first, "+b"), appending("+b")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(appended(&appended(&test_bytes("v2"), "+a"), "+b")));
	}

	#[test]
	fn a_plain_write_after_a_rebased_one_commits_as_written() {
		// The plain write is not a replayable change; replaying the earlier one would throw it away.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		txn.set(&test_key("meta"), test_bytes("plain")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(test_bytes("plain")));
	}

	#[test]
	fn a_rebased_write_over_an_earlier_plain_write_commits_as_written() {
		// The rebased change was derived from this transaction's own plain write; replaying it on the stored
		// value would drop that write.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("meta"), test_bytes("plain")).unwrap();
		let written = appended(&test_bytes("plain"), "+d");
		txn.set_rebased(&test_key("meta").into(), written.clone(), appending("+d")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(written));
	}

	#[test]
	fn a_plain_write_between_rebased_ones_stops_the_replay() {
		// The later rebased change was derived from the plain write; replaying the chain on the stored value
		// would drop the plain write.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+a"), appending("+a")).unwrap();
		txn.set(&test_key("meta"), test_bytes("plain")).unwrap();
		let written = appended(&test_bytes("plain"), "+b");
		txn.set_rebased(&test_key("meta").into(), written.clone(), appending("+b")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(written));
	}

	#[test]
	fn a_plain_write_restoring_earlier_bytes_does_not_revive_a_stopped_replay() {
		// Equal bytes do not make the chain replayable again: the rebased change before the plain writes would
		// be replayed under statements that never saw it.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		let first = appended(&test_bytes("v1"), "+a");
		txn.set_rebased(&test_key("meta").into(), first.clone(), appending("+a")).unwrap();
		txn.set(&test_key("meta"), test_bytes("plain")).unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("plain"), "+b"), appending("+b"))
			.unwrap();
		txn.set(&test_key("meta"), first.clone()).unwrap();
		let written = appended(&first, "+c");
		txn.set_rebased(&test_key("meta").into(), written.clone(), appending("+c")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(written));
	}

	#[test]
	fn a_rebased_write_rolled_back_to_a_savepoint_is_not_replayed() {
		// A rolled-back statement is not part of the commit; its change must neither be replayed nor stop the
		// replay of the statements that stayed.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		let first = appended(&test_bytes("v1"), "+a");
		txn.set_rebased(&test_key("meta").into(), first.clone(), appending("+a")).unwrap();
		let savepoint = txn.savepoint();
		txn.set_rebased(&test_key("meta").into(), appended(&first, "+b"), appending("+b")).unwrap();
		txn.restore_savepoint(savepoint);
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(appended(&test_bytes("v2"), "+a")));
	}

	#[test]
	fn a_rebased_write_of_a_key_no_commit_holds_commits_as_written() {
		// There is no stored value to replay on, so the written bytes are the only truth.
		let engine = MultiTransaction::testing();

		let mut txn = engine.begin_command().unwrap();
		let written = appended(&test_bytes("fresh"), "+d");
		txn.set_rebased(&test_key("meta").into(), written.clone(), appending("+d")).unwrap();
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "meta"), Some(written));
	}

	#[test]
	fn an_unchecked_commit_landing_after_the_commit_is_prepared_is_still_replayed_on() {
		// Until the commit version is taken another unchecked commit can install; a replay read before that
		// point writes back a value that erases it.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		let deltas = txn.prepare_commit().unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit_prepared(deltas, |_| {}).unwrap();

		assert_eq!(stored(&engine, "meta"), Some(appended(&test_bytes("v2"), "+d")));
	}

	#[test]
	fn the_published_change_carries_the_bytes_replayed_under_the_commit() {
		// Subscribers must see the value that was installed, not the one written before the race.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		let deltas = txn.prepare_commit().unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		let (_, published) = txn.commit_pending(deltas).unwrap();

		assert_eq!(
			published.iter().cloned().collect::<Vec<_>>(),
			vec![Delta::Set {
				key: test_key("meta").into(),
				bytes: appended(&test_bytes("v2"), "+d"),
				pre: SetPre::Unknown,
			}]
		);
	}

	struct Published(Arc<Mutex<Vec<Delta>>>);

	impl EventListener<PostCommitEvent> for Published {
		fn on(&self, event: &PostCommitEvent) {
			self.0.lock().extend(event.deltas().iter().cloned());
		}
	}

	#[test]
	fn a_committed_rebased_write_publishes_what_it_installed() {
		// Change data built from the pre-replay bytes would tell subscribers a value storage never held.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		let published = Arc::new(Mutex::new(Vec::new()));
		engine.event_bus.register::<PostCommitEvent, _>(Published(published.clone()));
		txn.commit().unwrap();
		engine.event_bus.wait_for_completion();

		assert_eq!(
			*published.lock(),
			vec![Delta::Set {
				key: test_key("meta").into(),
				bytes: appended(&test_bytes("v2"), "+d"),
				pre: SetPre::Unknown,
			}]
		);
	}

	#[test]
	fn a_replay_rewrites_only_its_own_key() {
		// Other writes of the same commit must install as written while the rebased key is replayed.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.set(&test_key("other"), test_bytes("o1")).unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		unchecked_with(&engine, &[("meta", "v2")]);
		txn.commit().unwrap();

		assert_eq!(stored(&engine, "other"), Some(test_bytes("o1")));
		assert_eq!(stored(&engine, "meta"), Some(appended(&test_bytes("v2"), "+d")));
	}

	#[test]
	fn a_checked_commit_after_the_read_still_conflicts_a_rebased_write() {
		// The replay covers only commits outside the conflict window; a checked writer must still abort it.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.get(&test_key("meta")).unwrap();
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), appending("+d")).unwrap();
		committed_with(&engine, &[("meta", "v2")]);

		assert!(txn.commit().is_err(), "the checked write after the read must conflict");
		assert_eq!(stored(&engine, "meta"), Some(test_bytes("v2")));
	}

	#[test]
	fn a_conflicting_commit_reports_the_conflict_before_running_its_replay() {
		// A conflict must abort before the replay reads storage; a replay error would hide the retryable
		// conflict.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		txn.get(&test_key("meta")).unwrap();
		let failing: Rebase = Arc::new(|_: &EncodedBytes| Err(TransactionError::RolledBack.into()));
		txn.set_rebased(&test_key("meta").into(), appended(&test_bytes("v1"), "+d"), failing).unwrap();
		committed_with(&engine, &[("meta", "v2")]);

		assert_eq!(txn.commit().unwrap_err().0.code, "TXN_001");
	}

	#[test]
	fn a_replay_that_reproduces_the_written_bytes_leaves_the_deltas_untouched() {
		// An unraced replay must not copy the change set or swap in fresh bytes under the commit lock.
		let engine = MultiTransaction::testing();
		committed_with(&engine, &[("meta", "v1")]);

		let mut txn = engine.begin_command().unwrap();
		let written = appended(&test_bytes("v1"), "+d");
		txn.set_rebased(&test_key("meta").into(), written.clone(), appending("+d")).unwrap();
		let deltas = txn.prepare_commit().unwrap();
		let input = deltas.clone();
		let (_, published) = txn.commit_pending(deltas).unwrap();

		assert_eq!(published.as_slice().as_ptr(), input.as_slice().as_ptr());
		let Delta::Set {
			bytes,
			..
		} = &published.as_slice()[0]
		else {
			panic!("the rebased key must publish a set");
		};
		assert_eq!(bytes.as_slice().as_ptr(), written.as_slice().as_ptr());
	}
}
