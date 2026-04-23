// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, ops::RangeBounds};

use reifydb_core::{
	common::CommitVersion,
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

use super::{MultiTransaction, TransactionManagerCommand, version::StandardVersionProvider};
use crate::{
	delta::optimize_deltas,
	multi::{
		pending::PendingWrites,
		transaction::write::MergePendingIterator,
		types::{Pending, TransactionValue},
	},
};

/// A multi-version write transaction for replica use.
///
/// Unlike `MultiWriteTransaction`, this type commits at a specific version
/// provided by the primary, bypassing oracle conflict detection and version
/// allocation entirely. It is the only write path on a replica node.
pub struct MultiReplicaTransaction {
	engine: MultiTransaction,
	pub(crate) tm: TransactionManagerCommand<StandardVersionProvider>,
	version: CommitVersion,
}

impl MultiReplicaTransaction {
	#[instrument(name = "transaction::replica::new", level = "debug", skip(engine), fields(version = %version.0))]
	pub fn new(engine: MultiTransaction, version: CommitVersion) -> Result<Self> {
		let mut tm = engine.tm.write()?;
		// Set read_version so that version() returns the primary's exact version
		// and reads see committed data up to (but not including) this version.
		// Pending writes within this transaction are always visible via MergePendingIterator.
		tm.read_version = Some(version);
		Ok(Self {
			engine,
			tm,
			version,
		})
	}
}

impl MultiReplicaTransaction {
	/// Commit pending writes at the primary's exact version.
	///
	/// Bypasses oracle conflict detection and version allocation.
	/// Registers with the command watermark so the system doesn't stall.
	/// Does NOT emit PostCommitEvent - replicas do not generate CDC.
	#[instrument(name = "transaction::replica::commit", level = "debug", skip(self), fields(version = %self.version.0, pending_count = self.tm.pending_writes().len()))]
	pub fn commit_at_version(&mut self) -> Result<()> {
		let version = self.version;

		if self.tm.pending_writes().is_empty() {
			self.tm.discard();
			return Ok(());
		}

		// Take pending writes directly - bypass oracle entirely
		let pending_writes = mem::take(&mut self.tm.pending_writes);
		let duplicate_writes = mem::take(&mut self.tm.duplicates);

		let mut raw_deltas = Vec::with_capacity(pending_writes.len() + duplicate_writes.len());
		pending_writes.into_iter_insertion_order().for_each(|(_k, v)| {
			let (_ver, delta) = v.into_components();
			raw_deltas.push(delta);
		});
		duplicate_writes.into_iter().for_each(|item| raw_deltas.push(item.delta));

		let optimized = optimize_deltas(raw_deltas.into_iter());
		let deltas = CowVec::new(optimized);

		// Register with command watermark BEFORE storage write
		self.engine.tm.begin_commit(version);

		// Write to multi-version store at the primary's exact version
		MultiVersionCommit::commit(&self.engine.store, deltas, version)?;

		// Signal watermark advancement AFTER storage write
		self.engine.tm.done_commit(version);

		// Advance the oracle's clock so clock.current() returns the right
		// snapshot for subsequent query transactions
		self.engine.tm.advance_clock_to(version);

		// Release query watermark
		self.tm.discard();

		// NO PostCommitEvent - replica does not generate CDC
		Ok(())
	}
}

impl MultiReplicaTransaction {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn pending_writes(&self) -> &PendingWrites {
		self.tm.pending_writes()
	}

	#[instrument(name = "transaction::replica::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<()> {
		self.tm.rollback()
	}

	#[instrument(name = "transaction::replica::contains_key", level = "trace", skip(self))]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		let version = self.tm.version();
		match self.tm.contains_key(key)? {
			Some(true) => Ok(true),
			Some(false) => Ok(false),
			None => MultiVersionContains::contains(&self.engine.store, key, version),
		}
	}

	#[instrument(name = "transaction::replica::get", level = "trace", skip(self))]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>> {
		let version = self.tm.version();
		match self.tm.get(key)? {
			Some(v) => {
				if v.row().is_some() {
					Ok(Some(v.into()))
				} else {
					Ok(None)
				}
			}
			None => Ok(MultiVersionGet::get(&self.engine.store, key, version)?.map(Into::into)),
		}
	}

	#[instrument(name = "transaction::replica::set", level = "trace", skip(self, row))]
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.tm.set(key, row)
	}

	#[instrument(name = "transaction::replica::unset", level = "trace", skip(self, row))]
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.tm.unset(key, row)
	}

	#[instrument(name = "transaction::replica::remove", level = "trace", skip(self))]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.tm.remove(key)
	}

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
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = RangeBounds::start_bound(&range);
		let end = RangeBounds::end_bound(&range);

		marker.mark_range(range.clone());

		let pending: Vec<(EncodedKey, Pending)> =
			pw.range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, false))
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = RangeBounds::start_bound(&range);
		let end = RangeBounds::end_bound(&range);

		marker.mark_range(range.clone());

		let pending: Vec<(EncodedKey, Pending)> =
			pw.range((start, end)).rev().map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_iter = self.engine.store.range_rev(range, version, batch_size);

		Box::new(MergePendingIterator::new(pending, storage_iter, true))
	}
}
