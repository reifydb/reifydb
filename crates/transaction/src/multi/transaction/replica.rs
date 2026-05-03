// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::mem;
use std::{collections::HashSet, ops::RangeBounds, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::store::{
		MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRow,
	},
};
use reifydb_type::{Result, util::cowvec::CowVec};
use tracing::instrument;

use super::{MultiTransaction, version::StandardVersionProvider};
use crate::{
	TransactionId,
	delta::optimize_deltas,
	error::TransactionError,
	multi::{
		conflict::ConflictManager,
		marker::Marker,
		oracle::Oracle,
		pending::PendingWrites,
		transaction::write::{Lifecycle, MergePendingIterator},
		types::{DeltaEntry, TransactionValue},
	},
};

pub struct MultiReplicaTransaction {
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

	commit_version: CommitVersion,
}

impl MultiReplicaTransaction {
	#[instrument(name = "transaction::replica::new", level = "debug", skip(engine), fields(version = %version.0))]
	pub fn new(engine: MultiTransaction, version: CommitVersion) -> Result<Self> {
		let oracle = engine.tm.oracle().clone();
		let snapshot = oracle.version()?;
		oracle.query.register_in_flight(snapshot);

		let id = TransactionId::generate(oracle.metrics_clock(), oracle.rng());
		Ok(Self {
			engine,
			id,
			version: snapshot,
			read_version: Some(version),
			size: 0,
			count: 0,
			oracle,
			conflicts: ConflictManager::new(),
			pending_writes: PendingWrites::new(),
			duplicates: Vec::new(),
			delta_log: Vec::new(),
			preexisting_keys: HashSet::new(),
			lifecycle: Lifecycle::Active,
			commit_version: version,
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

impl Drop for MultiReplicaTransaction {
	fn drop(&mut self) {
		if self.lifecycle != Lifecycle::Discarded {
			self.discard();
		}
	}
}

impl MultiReplicaTransaction {
	pub fn id(&self) -> TransactionId {
		self.id
	}

	pub fn version(&self) -> CommitVersion {
		self.read_version.unwrap_or(self.version)
	}

	pub fn pending_writes(&self) -> &PendingWrites {
		&self.pending_writes
	}

	pub fn mark_preexisting(&mut self, key: &EncodedKey) {
		self.preexisting_keys.insert(key.as_ref().to_vec());
	}

	pub fn preexisting_keys(&self) -> &HashSet<Vec<u8>> {
		&self.preexisting_keys
	}

	pub fn marker(&mut self) -> Marker<'_> {
		Marker::new(&mut self.conflicts)
	}

	pub fn marker_with_pending_writes(&mut self) -> (Marker<'_>, &PendingWrites) {
		(Marker::new(&mut self.conflicts), &self.pending_writes)
	}

	pub fn base_version(&self) -> CommitVersion {
		self.version
	}
}

impl MultiReplicaTransaction {
	#[instrument(name = "transaction::replica::commit", level = "debug", skip(self), fields(version = %self.commit_version.0, pending_count = self.pending_writes.len()))]
	pub fn commit_at_version(&mut self) -> Result<()> {
		let version = self.commit_version;

		if self.pending_writes.is_empty() {
			self.discard();
			return Ok(());
		}

		let deltas = self.drain_deltas();

		self.engine.tm.begin_commit(version);
		MultiVersionCommit::commit(&self.engine.store, deltas, version)?;
		self.engine.tm.done_commit(version);
		self.engine.tm.advance_clock_to(version);
		self.discard();

		Ok(())
	}

	#[inline]
	fn drain_deltas(&mut self) -> CowVec<Delta> {
		let pending_writes = mem::take(&mut self.pending_writes);
		let duplicate_writes = mem::take(&mut self.duplicates);

		let mut raw_deltas = Vec::with_capacity(pending_writes.len() + duplicate_writes.len());
		pending_writes.into_iter_insertion_order().for_each(|(_k, v)| {
			let (_ver, delta) = v.into_components();
			raw_deltas.push(delta);
		});
		duplicate_writes.into_iter().for_each(|item| raw_deltas.push(item.delta));

		CowVec::new(optimize_deltas(raw_deltas, &self.preexisting_keys))
	}
}

impl MultiReplicaTransaction {
	#[instrument(name = "transaction::replica::set", level = "trace", skip(self, row))]
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

	#[instrument(name = "transaction::replica::unset", level = "trace", skip(self, row))]
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

	#[instrument(name = "transaction::replica::remove", level = "trace", skip(self))]
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

	#[instrument(name = "transaction::replica::rollback", level = "debug", skip(self))]
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

	#[instrument(name = "transaction::replica::contains_key", level = "trace", skip(self))]
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

	#[instrument(name = "transaction::replica::get", level = "trace", skip(self))]
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
}

impl MultiReplicaTransaction {
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

impl MultiReplicaTransaction {
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
		let start = RangeBounds::start_bound(&range);
		let end = RangeBounds::end_bound(&range);

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
		let start = RangeBounds::start_bound(&range);
		let end = RangeBounds::end_bound(&range);

		marker.mark_range(range.clone());

		let pending: Vec<(EncodedKey, DeltaEntry)> =
			pw.range((start, end)).rev().map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range_rev(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}
}
