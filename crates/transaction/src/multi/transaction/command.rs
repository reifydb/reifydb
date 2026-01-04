// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{ops::RangeBounds, pin::Pin};

use async_stream::try_stream;
use futures_util::{Stream, StreamExt};
use reifydb_core::{
	CommitVersion, CowVec, EncodedKey, EncodedKeyRange, event::transaction::PostCommitEvent,
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange,
	MultiVersionRangeRev,
};
use reifydb_type::{Error, util::hex};
use tokio::spawn;
use tracing::instrument;

use super::{TransactionManagerCommand, TransactionMulti, version::StandardVersionProvider};
use crate::multi::types::TransactionValue;

pub struct CommandTransaction {
	engine: TransactionMulti,
	pub(crate) tm: TransactionManagerCommand<StandardVersionProvider>,
}

impl CommandTransaction {
	#[instrument(name = "transaction::command::new", level = "debug", skip(engine))]
	pub async fn new(engine: TransactionMulti) -> crate::Result<Self> {
		let tm = engine.tm.write().await?;
		Ok(Self {
			engine,
			tm,
		})
	}
}

impl CommandTransaction {
	#[instrument(name = "transaction::command::commit", level = "info", skip(self), fields(pending_count = self.tm.pending_writes().len()))]
	pub async fn commit(&mut self) -> Result<CommitVersion, Error> {
		// For read-only transactions (no pending writes), skip conflict detection
		if self.tm.pending_writes().is_empty() {
			self.tm.discard();
			return Ok(CommitVersion(0));
		}

		// Use commit_pending to allocate the commit version via oracle BEFORE writing to storage
		// This ensures entries have the correct commit version
		let (commit_version, entries) = self.tm.commit_pending().await?;

		if entries.is_empty() {
			self.tm.discard();
			return Ok(CommitVersion(0));
		}

		// Collect deltas for storage commit
		let mut deltas = CowVec::with_capacity(entries.len());
		for pending in &entries {
			deltas.push(pending.delta.clone());
		}

		MultiVersionCommit::commit(&self.engine.store, deltas.clone(), commit_version).await?;

		self.tm.oracle.done_commit(commit_version);
		self.tm.discard();

		let event_bus = self.engine.event_bus.clone();
		let version = commit_version;

		spawn(async move {
			event_bus
				.emit(PostCommitEvent {
					deltas,
					version,
				})
				.await;
		});

		Ok(commit_version)
	}
}

impl CommandTransaction {
	pub fn version(&self) -> CommitVersion {
		self.tm.version()
	}

	pub fn pending_writes(&self) -> &crate::multi::pending::PendingWrites {
		self.tm.pending_writes()
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) {
		self.tm.read_as_of_version_exclusive(version);
	}

	pub fn read_as_of_version_inclusive(&mut self, version: CommitVersion) -> Result<(), Error> {
		self.read_as_of_version_exclusive(CommitVersion(version.0 + 1));
		Ok(())
	}

	#[instrument(name = "transaction::command::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<(), Error> {
		self.tm.rollback()
	}

	#[instrument(name = "transaction::command::contains_key", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, reifydb_type::Error> {
		let version = self.tm.version();
		match self.tm.contains_key(key)? {
			Some(true) => Ok(true),
			Some(false) => Ok(false),
			None => MultiVersionContains::contains(&self.engine.store, key, version).await,
		}
	}

	#[instrument(name = "transaction::command::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub async fn get(&mut self, key: &EncodedKey) -> Result<Option<TransactionValue>, Error> {
		let version = self.tm.version();
		match self.tm.get(key)? {
			Some(v) => {
				if v.values().is_some() {
					Ok(Some(v.into()))
				} else {
					Ok(None)
				}
			}
			None => Ok(MultiVersionGet::get(&self.engine.store, key, version).await?.map(Into::into)),
		}
	}

	#[instrument(name = "transaction::command::set", level = "trace", skip(self, values), fields(key_hex = %hex::encode(key.as_ref()), value_len = values.as_ref().len()))]
	pub fn set(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<(), reifydb_type::Error> {
		self.tm.set(key, values)
	}

	#[instrument(name = "transaction::command::remove", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<(), Error> {
		self.tm.remove(key)
	}

	/// Fetch a batch of values for the given range, merging pending writes with committed data.
	pub async fn range_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<MultiVersionBatch, reifydb_type::Error> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		// Get pending writes in the range
		let pending: Vec<_> = pw.range((start, end)).collect();

		// Get committed batch
		let committed = MultiVersionRange::range_batch(&self.engine.store, range, version, batch_size).await?;

		// Merge pending writes with committed data
		let merged = merge_pending_with_committed(pending, committed);

		Ok(merged)
	}

	pub async fn range(&mut self, range: EncodedKeyRange) -> Result<MultiVersionBatch, reifydb_type::Error> {
		self.range_batch(range, 1024).await
	}

	/// Fetch a batch of values in reverse order, merging pending writes with committed data.
	pub async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> Result<MultiVersionBatch, reifydb_type::Error> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		// Get pending writes in the range (reversed)
		let pending: Vec<_> = pw.range((start, end)).rev().collect();

		// Get committed batch in reverse
		let committed =
			MultiVersionRangeRev::range_rev_batch(&self.engine.store, range, version, batch_size).await?;

		// Merge pending writes with committed data (in reverse order)
		let merged = merge_pending_with_committed_rev(pending, committed);

		Ok(merged)
	}

	pub async fn range_rev(&mut self, range: EncodedKeyRange) -> Result<MultiVersionBatch, reifydb_type::Error> {
		self.range_rev_batch(range, 1024).await
	}

	pub async fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch, reifydb_type::Error> {
		self.range(EncodedKeyRange::prefix(prefix)).await
	}

	pub async fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch, reifydb_type::Error> {
		self.range_rev(EncodedKeyRange::prefix(prefix)).await
	}

	/// Create a streaming iterator for forward range queries, merging pending writes.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// and maintains cursor state internally. Pending writes are merged with
	/// committed storage data.
	pub fn range_stream(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Pin<Box<dyn Stream<Item = crate::Result<MultiVersionValues>> + Send + '_>> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		// Collect pending writes in range as owned data
		let pending: Vec<(EncodedKey, Pending)> =
			pw.range((start, end)).map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_stream = self.engine.store.range_stream(range, version, batch_size);

		Box::pin(merge_pending_with_stream(pending, storage_stream, false))
	}

	/// Create a streaming iterator for reverse range queries, merging pending writes.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The stream yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev_stream(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Pin<Box<dyn Stream<Item = crate::Result<MultiVersionValues>> + Send + '_>> {
		let version = self.tm.version();
		let (mut marker, pw) = self.tm.marker_with_pending_writes();
		let start = range.start_bound();
		let end = range.end_bound();

		marker.mark_range(range.clone());

		// Collect pending writes in range as owned data (reversed)
		let pending: Vec<(EncodedKey, Pending)> =
			pw.range((start, end)).rev().map(|(k, v)| (k.clone(), v.clone())).collect();

		let storage_stream = self.engine.store.range_rev_stream(range, version, batch_size);

		Box::pin(merge_pending_with_stream(pending, storage_stream, true))
	}
}

use reifydb_core::interface::MultiVersionValues;

use crate::multi::types::Pending;

/// Merge pending writes with committed data in ascending key order.
fn merge_pending_with_committed(
	pending: Vec<(&EncodedKey, &Pending)>,
	committed: MultiVersionBatch,
) -> MultiVersionBatch {
	use std::cmp::Ordering;

	let mut result = Vec::new();
	let mut pending_iter = pending.into_iter().peekable();
	let mut committed_iter = committed.items.into_iter().peekable();

	loop {
		match (pending_iter.peek(), committed_iter.peek()) {
			(Some((pending_key, _)), Some(committed)) => {
				match (*pending_key).cmp(&committed.key) {
					Ordering::Less => {
						// Pending key is smaller, yield it
						let (key, value) = pending_iter.next().unwrap();
						if let Some(values) = value.values() {
							result.push(MultiVersionValues {
								key: key.clone(),
								values: values.clone(),
								version: value.version,
							});
						}
					}
					Ordering::Equal => {
						// Keys match, prefer pending (skip committed)
						committed_iter.next();
						let (key, value) = pending_iter.next().unwrap();
						if let Some(values) = value.values() {
							result.push(MultiVersionValues {
								key: key.clone(),
								values: values.clone(),
								version: value.version,
							});
						}
					}
					Ordering::Greater => {
						// Committed key is smaller, yield it
						result.push(committed_iter.next().unwrap());
					}
				}
			}
			(Some(_), None) => {
				// Only pending left
				let (key, value) = pending_iter.next().unwrap();
				if let Some(values) = value.values() {
					result.push(MultiVersionValues {
						key: key.clone(),
						values: values.clone(),
						version: value.version,
					});
				}
			}
			(None, Some(_)) => {
				// Only committed left
				result.push(committed_iter.next().unwrap());
			}
			(None, None) => break,
		}
	}

	MultiVersionBatch {
		items: result,
		has_more: committed.has_more,
	}
}

/// Merge pending writes with committed data in descending key order.
fn merge_pending_with_committed_rev(
	pending: Vec<(&EncodedKey, &Pending)>,
	committed: MultiVersionBatch,
) -> MultiVersionBatch {
	use std::cmp::Ordering;

	let mut result = Vec::new();
	let mut pending_iter = pending.into_iter().peekable();
	let mut committed_iter = committed.items.into_iter().peekable();

	loop {
		match (pending_iter.peek(), committed_iter.peek()) {
			(Some((pending_key, _)), Some(committed)) => {
				match (*pending_key).cmp(&committed.key) {
					Ordering::Greater => {
						// Pending key is larger (comes first in reverse), yield it
						let (key, value) = pending_iter.next().unwrap();
						if let Some(values) = value.values() {
							result.push(MultiVersionValues {
								key: key.clone(),
								values: values.clone(),
								version: value.version,
							});
						}
					}
					Ordering::Equal => {
						// Keys match, prefer pending (skip committed)
						committed_iter.next();
						let (key, value) = pending_iter.next().unwrap();
						if let Some(values) = value.values() {
							result.push(MultiVersionValues {
								key: key.clone(),
								values: values.clone(),
								version: value.version,
							});
						}
					}
					Ordering::Less => {
						// Committed key is larger (comes first in reverse), yield it
						result.push(committed_iter.next().unwrap());
					}
				}
			}
			(Some(_), None) => {
				// Only pending left
				let (key, value) = pending_iter.next().unwrap();
				if let Some(values) = value.values() {
					result.push(MultiVersionValues {
						key: key.clone(),
						values: values.clone(),
						version: value.version,
					});
				}
			}
			(None, Some(_)) => {
				// Only committed left
				result.push(committed_iter.next().unwrap());
			}
			(None, None) => break,
		}
	}

	MultiVersionBatch {
		items: result,
		has_more: committed.has_more,
	}
}

/// Merge pending writes with a storage stream.
fn merge_pending_with_stream<'a, S>(
	pending: Vec<(EncodedKey, Pending)>,
	storage_stream: S,
	reverse: bool,
) -> impl Stream<Item = crate::Result<MultiVersionValues>> + Send + 'a
where
	S: Stream<Item = reifydb_type::Result<MultiVersionValues>> + Send + 'a,
{
	try_stream! {
		use std::cmp::Ordering;

		let mut storage_stream = std::pin::pin!(storage_stream);
		let mut pending_iter = pending.into_iter().peekable();
		let mut next_storage: Option<MultiVersionValues> = None;

		loop {
			// Fetch next storage item if needed
			if next_storage.is_none() {
				next_storage = match storage_stream.next().await {
					Some(Ok(v)) => Some(v),
					Some(Err(e)) => { Err(e)?; None }
					None => None,
				};
			}

			match (pending_iter.peek(), &next_storage) {
				(Some((pending_key, _)), Some(storage_val)) => {
					let cmp = pending_key.cmp(&storage_val.key);
					let should_yield_pending = if reverse {
						// Reverse: larger keys first
						matches!(cmp, Ordering::Greater)
					} else {
						// Forward: smaller keys first
						matches!(cmp, Ordering::Less)
					};

					if should_yield_pending {
						// Pending key comes first
						let (key, value) = pending_iter.next().unwrap();
						if let Some(values) = value.values() {
							yield MultiVersionValues {
								key,
								values: values.clone(),
								version: value.version,
							};
						}
						// Tombstone: skip (don't yield)
					} else if matches!(cmp, Ordering::Equal) {
						// Same key - pending shadows storage
						let (key, value) = pending_iter.next().unwrap();
						next_storage = None; // Consume storage entry
						if let Some(values) = value.values() {
							yield MultiVersionValues {
								key,
								values: values.clone(),
								version: value.version,
							};
						}
					} else {
						// Storage key comes first
						yield next_storage.take().unwrap();
					}
				}
				(Some(_), None) => {
					// Only pending left
					let (key, value) = pending_iter.next().unwrap();
					if let Some(values) = value.values() {
						yield MultiVersionValues {
							key,
							values: values.clone(),
							version: value.version,
						};
					}
				}
				(None, Some(_)) => {
					// Only storage left
					yield next_storage.take().unwrap();
				}
				(None, None) => break,
			}
		}
	}
}
