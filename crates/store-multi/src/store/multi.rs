// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	ops::{Bound, RangeBounds},
};

use drop::find_keys_to_drop;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::metric::{StorageDelete, StorageDrop, StorageStatsRecordedEvent, StorageWrite},
	interface::store::{
		MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionGetPrevious,
		MultiVersionStore, MultiVersionValues,
	},
	value::encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
};
use reifydb_type::util::{cowvec::CowVec, hex};
use tracing::instrument;

use super::{
	StandardMultiStore, drop,
	router::{classify_key, is_single_version_semantics_key},
	version::{VersionedGetResult, encode_versioned_key, get_at_version},
};
use crate::tier::{EntryKind, EntryKind::Multi, RangeCursor, TierStorage};

/// Fixed chunk size for internal tier scans.
/// This is the number of versioned entries fetched per tier per iteration.
const TIER_SCAN_CHUNK_SIZE: usize = 4096;

impl MultiVersionGet for StandardMultiStore {
	#[instrument(name = "store::multi::get", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0))]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<Option<MultiVersionValues>> {
		let table = classify_key(key);

		// Try hot tier first
		if let Some(hot) = &self.hot {
			match get_at_version(hot, table, key.as_ref(), version)? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionValues {
						key: key.clone(),
						values: EncodedValues(value),
						version: v,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		// Try warm tier
		if let Some(warm) = &self.warm {
			match get_at_version(warm, table, key.as_ref(), version)? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionValues {
						key: key.clone(),
						values: EncodedValues(value),
						version: v,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		// Try cold tier
		if let Some(cold) = &self.cold {
			match get_at_version(cold, table, key.as_ref(), version)? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionValues {
						key: key.clone(),
						values: EncodedValues(value),
						version: v,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		Ok(None)
	}
}

impl MultiVersionContains for StandardMultiStore {
	#[instrument(name = "store::multi::contains", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref()), version = version.0), ret)]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> crate::Result<bool> {
		Ok(MultiVersionGet::get(self, key, version)?.is_some())
	}
}

impl MultiVersionCommit for StandardMultiStore {
	#[instrument(name = "store::multi::commit", level = "info", skip(self, deltas), fields(delta_count = deltas.len(), version = version.0))]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> crate::Result<()> {
		// Get the hot storage tier (warm and cold are placeholders for now)
		let Some(storage) = &self.hot else {
			return Ok(());
		};

		// For flow state keys (single-version semantics), track pending Set operations.
		// Drops are queued to a background worker after the commit.
		let pending_set_keys: HashSet<Vec<u8>> = deltas
			.iter()
			.filter_map(|delta| {
				if let Delta::Set {
					key,
					..
				} = delta
				{
					if is_single_version_semantics_key(key) {
						return Some(key.as_ref().to_vec());
					}
				}
				None
			})
			.collect();

		// Collect storage statistics for batch sending.
		// Stats are emitted via EventBus for the metrics worker to process.
		// CDC stats are collected by the async CDC shard workers.
		let mut writes: Vec<StorageWrite> = Vec::new();
		let mut deletes: Vec<StorageDelete> = Vec::new();
		let mut drops: Vec<StorageDrop> = Vec::new();

		for delta in deltas.iter() {
			let key = delta.key();

			match delta {
				Delta::Set {
					values,
					..
				} => {
					writes.push(StorageWrite {
						key: key.clone(),
						value_bytes: values.len() as u64,
					});
				}
				Delta::Unset {
					values,
					..
				} => {
					deletes.push(StorageDelete {
						key: key.clone(),
						value_bytes: values.len() as u64,
					});
				}
				Delta::Remove {
					..
				}
				| Delta::Drop {
					..
				} => {}
			}
		}

		// Batch deltas by table for efficient storage writes
		let mut batches: HashMap<EntryKind, Vec<(CowVec<u8>, Option<CowVec<u8>>)>> = HashMap::new();

		for delta in deltas.iter() {
			let table = classify_key(delta.key());

			match delta {
				Delta::Set {
					key,
					values,
				} => {
					let versioned_key = CowVec::new(encode_versioned_key(key.as_ref(), version));
					batches.entry(table)
						.or_default()
						.push((versioned_key, Some(CowVec::new(values.as_ref().to_vec()))));
				}
				Delta::Unset {
					key,
					..
				}
				| Delta::Remove {
					key,
				} => {
					let versioned_key = CowVec::new(encode_versioned_key(key.as_ref(), version));
					batches.entry(table).or_default().push((versioned_key, None));
				}
				Delta::Drop {
					key,
					up_to_version,
					keep_last_versions,
				} => {
					// Drop scans for versioned entries and deletes them based on constraints.
					// For single-version keys with a pending Set in this commit, pass the version
					// so find_keys_to_drop knows about the new version being written.
					let pending_version = if pending_set_keys.contains(key.as_ref()) {
						Some(version)
					} else {
						None
					};
					let entries_to_drop = find_keys_to_drop(
						storage,
						table,
						key.as_ref(),
						*up_to_version,
						*keep_last_versions,
						pending_version,
					)?;
					for entry in entries_to_drop {
						// Collect stats for each dropped entry
						drops.push(StorageDrop {
							key: key.clone(),
							value_bytes: entry.value_bytes,
						});
						batches.entry(table).or_default().push((entry.versioned_key, None));
					}
				}
			}
		}

		// Queue deferred drops for single-version-semantics keys to background worker
		let drop_worker = self.drop_worker.lock();
		for key_bytes in pending_set_keys.iter() {
			let key = CowVec::new(key_bytes.clone());
			let table = classify_key(&EncodedKey(key.clone()));
			drop_worker.queue_drop(
				table,
				key,
				None,          // up_to_version
				Some(1),       // keep_last_versions
				Some(version), // pending_version
			);
		}
		drop(drop_worker);

		// Write versioned entries to storage
		storage.set(batches)?;

		// Emit storage stats event for this commit
		if !writes.is_empty() || !deletes.is_empty() || !drops.is_empty() {
			self.event_bus.emit(StorageStatsRecordedEvent {
				writes,
				deletes,
				drops,
				version,
			});
		}

		Ok(())
	}
}

/// Cursor state for multi-version range streaming.
///
/// Tracks position in each tier independently, allowing the scan to continue
/// until enough unique logical keys are collected.
#[derive(Debug, Clone, Default)]
pub struct MultiVersionRangeCursor {
	/// Cursor for hot tier
	pub hot: RangeCursor,
	/// Cursor for warm tier
	pub warm: RangeCursor,
	/// Cursor for cold tier
	pub cold: RangeCursor,
	/// Whether all tiers are exhausted
	pub exhausted: bool,
}

impl MultiVersionRangeCursor {
	/// Create a new cursor at the start.
	pub fn new() -> Self {
		Self::default()
	}

	/// Check if all tiers are exhausted.
	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

impl StandardMultiStore {
	/// Fetch the next batch of entries, continuing from cursor position.
	///
	/// This properly handles high version density by scanning until `batch_size`
	/// unique logical keys are collected OR all tiers are exhausted.
	pub fn range_next(
		&self,
		cursor: &mut MultiVersionRangeCursor,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		if cursor.exhausted {
			return Ok(MultiVersionBatch {
				items: Vec::new(),
				has_more: false,
			});
		}

		let table = classify_key_range(&range);
		let (start, end) = make_versioned_range_bounds(&range);
		let batch_size = batch_size as usize;

		// Collected entries: logical_key -> (version, value)
		let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		// Keep scanning until we have batch_size unique logical keys OR all tiers exhausted
		while collected.len() < batch_size {
			let mut any_progress = false;

			// Scan chunk from hot tier
			if let Some(hot) = &self.hot {
				if !cursor.hot.exhausted {
					let progress = Self::scan_tier_chunk(
						hot,
						table,
						&mut cursor.hot,
						&start,
						&end,
						version,
						&range,
						&mut collected,
					)?;
					any_progress |= progress;
				}
			}

			// Scan chunk from warm tier
			if let Some(warm) = &self.warm {
				if !cursor.warm.exhausted {
					let progress = Self::scan_tier_chunk(
						warm,
						table,
						&mut cursor.warm,
						&start,
						&end,
						version,
						&range,
						&mut collected,
					)?;
					any_progress |= progress;
				}
			}

			// Scan chunk from cold tier
			if let Some(cold) = &self.cold {
				if !cursor.cold.exhausted {
					let progress = Self::scan_tier_chunk(
						cold,
						table,
						&mut cursor.cold,
						&start,
						&end,
						version,
						&range,
						&mut collected,
					)?;
					any_progress |= progress;
				}
			}

			if !any_progress {
				// All tiers exhausted
				cursor.exhausted = true;
				break;
			}
		}

		// Convert to MultiVersionValues in sorted key order, filtering out tombstones
		let items: Vec<MultiVersionValues> = collected
			.into_iter()
			.take(batch_size)
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionValues {
					key: EncodedKey(CowVec::new(key_bytes)),
					values: EncodedValues(val),
					version: v,
				})
			})
			.collect();

		let has_more = items.len() >= batch_size || !cursor.exhausted;

		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}

	/// Scan a chunk from a single tier and merge into collected entries.
	/// Returns true if any entries were processed (i.e., made progress).
	fn scan_tier_chunk<S: TierStorage>(
		storage: &S,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: &[u8],
		end: &[u8],
		version: CommitVersion,
		range: &EncodedKeyRange,
		collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
	) -> crate::Result<bool> {
		use super::version::{extract_key, extract_version};

		let batch = storage.range_next(
			table,
			cursor,
			Bound::Included(start),
			Bound::Included(end),
			TIER_SCAN_CHUNK_SIZE,
		)?;

		if batch.entries.is_empty() {
			return Ok(false);
		}

		for entry in batch.entries {
			if let (Some(original_key), Some(entry_version)) =
				(extract_key(&entry.key), extract_version(&entry.key))
			{
				// Skip if version is greater than requested
				if entry_version > version {
					continue;
				}

				// Skip if key is not within the requested logical range
				let original_key_encoded = EncodedKey(CowVec::new(original_key.clone()));
				if !range.contains(&original_key_encoded) {
					continue;
				}

				// Update if no entry exists or this is a higher version
				let should_update = match collected.get(&original_key) {
					None => true,
					Some((existing_version, _)) => entry_version > *existing_version,
				};

				if should_update {
					collected.insert(original_key, (entry_version, entry.value));
				}
			}
		}

		Ok(true)
	}

	/// Create an iterator for forward range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The iterator yields individual entries
	/// and maintains cursor state internally.
	pub fn range(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: usize,
	) -> MultiVersionRangeIter {
		MultiVersionRangeIter {
			store: self.clone(),
			cursor: MultiVersionRangeCursor::new(),
			range,
			version,
			batch_size,
			current_batch: Vec::new(),
			current_index: 0,
		}
	}

	/// Create an iterator for reverse range queries.
	///
	/// This properly handles high version density by scanning until batch_size
	/// unique logical keys are collected. The iterator yields individual entries
	/// in reverse key order and maintains cursor state internally.
	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: usize,
	) -> MultiVersionRangeRevIter {
		MultiVersionRangeRevIter {
			store: self.clone(),
			cursor: MultiVersionRangeCursor::new(),
			range,
			version,
			batch_size,
			current_batch: Vec::new(),
			current_index: 0,
		}
	}

	/// Fetch the next batch of entries in reverse order, continuing from cursor position.
	///
	/// This properly handles high version density by scanning until `batch_size`
	/// unique logical keys are collected OR all tiers are exhausted.
	fn range_rev_next(
		&self,
		cursor: &mut MultiVersionRangeCursor,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		if cursor.exhausted {
			return Ok(MultiVersionBatch {
				items: Vec::new(),
				has_more: false,
			});
		}

		let table = classify_key_range(&range);
		let (start, end) = make_versioned_range_bounds(&range);
		let batch_size = batch_size as usize;

		// Collected entries: logical_key -> (version, value)
		let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		// Keep scanning until we have batch_size unique logical keys OR all tiers exhausted
		while collected.len() < batch_size {
			let mut any_progress = false;

			// Scan chunk from hot tier (reverse)
			if let Some(hot) = &self.hot {
				if !cursor.hot.exhausted {
					let progress = Self::scan_tier_chunk_rev(
						hot,
						table,
						&mut cursor.hot,
						&start,
						&end,
						version,
						&range,
						&mut collected,
					)?;
					any_progress |= progress;
				}
			}

			// Scan chunk from warm tier (reverse)
			if let Some(warm) = &self.warm {
				if !cursor.warm.exhausted {
					let progress = Self::scan_tier_chunk_rev(
						warm,
						table,
						&mut cursor.warm,
						&start,
						&end,
						version,
						&range,
						&mut collected,
					)?;
					any_progress |= progress;
				}
			}

			// Scan chunk from cold tier (reverse)
			if let Some(cold) = &self.cold {
				if !cursor.cold.exhausted {
					let progress = Self::scan_tier_chunk_rev(
						cold,
						table,
						&mut cursor.cold,
						&start,
						&end,
						version,
						&range,
						&mut collected,
					)?;
					any_progress |= progress;
				}
			}

			if !any_progress {
				// All tiers exhausted
				cursor.exhausted = true;
				break;
			}
		}

		// Convert to MultiVersionValues in REVERSE sorted key order, filtering out tombstones
		let items: Vec<MultiVersionValues> = collected
			.into_iter()
			.rev()
			.take(batch_size)
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionValues {
					key: EncodedKey(CowVec::new(key_bytes)),
					values: EncodedValues(val),
					version: v,
				})
			})
			.collect();

		let has_more = items.len() >= batch_size || !cursor.exhausted;

		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}

	/// Scan a chunk from a single tier in reverse and merge into collected entries.
	/// Returns true if any entries were processed (i.e., made progress).
	fn scan_tier_chunk_rev<S: TierStorage>(
		storage: &S,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: &[u8],
		end: &[u8],
		version: CommitVersion,
		range: &EncodedKeyRange,
		collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
	) -> crate::Result<bool> {
		use super::version::{extract_key, extract_version};

		let batch = storage.range_rev_next(
			table,
			cursor,
			Bound::Included(start),
			Bound::Included(end),
			TIER_SCAN_CHUNK_SIZE,
		)?;

		if batch.entries.is_empty() {
			return Ok(false);
		}

		for entry in batch.entries {
			if let (Some(original_key), Some(entry_version)) =
				(extract_key(&entry.key), extract_version(&entry.key))
			{
				// Skip if version is greater than requested
				if entry_version > version {
					continue;
				}

				// Skip if key is not within the requested logical range
				let original_key_encoded = EncodedKey(CowVec::new(original_key.clone()));
				if !range.contains(&original_key_encoded) {
					continue;
				}

				// Update if no entry exists or this is a higher version
				let should_update = match collected.get(&original_key) {
					None => true,
					Some((existing_version, _)) => entry_version > *existing_version,
				};

				if should_update {
					collected.insert(original_key, (entry_version, entry.value));
				}
			}
		}

		Ok(true)
	}
}

impl MultiVersionGetPrevious for StandardMultiStore {
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> crate::Result<Option<MultiVersionValues>> {
		if before_version.0 == 0 {
			return Ok(None);
		}

		// Hot storage must be available for version lookups
		let storage = self.hot.as_ref().expect("hot storage required for version lookups");

		let table = classify_key(key);
		let prev_version = CommitVersion(before_version.0 - 1);

		match get_at_version(storage, table, key.as_ref(), prev_version) {
			Ok(VersionedGetResult::Value {
				value,
				version,
			}) => Ok(Some(MultiVersionValues {
				key: key.clone(),
				values: EncodedValues(CowVec::new(value.to_vec())),
				version,
			})),
			Ok(VersionedGetResult::Tombstone) | Ok(VersionedGetResult::NotFound) => Ok(None),
			Err(e) => Err(e),
		}
	}
}

impl MultiVersionStore for StandardMultiStore {}

/// Iterator for forward multi-version range queries.
pub struct MultiVersionRangeIter {
	store: StandardMultiStore,
	cursor: MultiVersionRangeCursor,
	range: EncodedKeyRange,
	version: CommitVersion,
	batch_size: usize,
	current_batch: Vec<MultiVersionValues>,
	current_index: usize,
}

impl Iterator for MultiVersionRangeIter {
	type Item = crate::Result<MultiVersionValues>;

	fn next(&mut self) -> Option<Self::Item> {
		// If we have items in the current batch, return them
		if self.current_index < self.current_batch.len() {
			let item = self.current_batch[self.current_index].clone();
			self.current_index += 1;
			return Some(Ok(item));
		}

		// If cursor is exhausted, we're done
		if self.cursor.exhausted {
			return None;
		}

		// Fetch the next batch
		match self.store.range_next(&mut self.cursor, self.range.clone(), self.version, self.batch_size as u64)
		{
			Ok(batch) => {
				if batch.items.is_empty() {
					return None;
				}
				self.current_batch = batch.items;
				self.current_index = 0;
				self.next()
			}
			Err(e) => Some(Err(e)),
		}
	}
}

/// Iterator for reverse multi-version range queries.
pub struct MultiVersionRangeRevIter {
	store: StandardMultiStore,
	cursor: MultiVersionRangeCursor,
	range: EncodedKeyRange,
	version: CommitVersion,
	batch_size: usize,
	current_batch: Vec<MultiVersionValues>,
	current_index: usize,
}

impl Iterator for MultiVersionRangeRevIter {
	type Item = crate::Result<MultiVersionValues>;

	fn next(&mut self) -> Option<Self::Item> {
		// If we have items in the current batch, return them
		if self.current_index < self.current_batch.len() {
			let item = self.current_batch[self.current_index].clone();
			self.current_index += 1;
			return Some(Ok(item));
		}

		// If cursor is exhausted, we're done
		if self.cursor.exhausted {
			return None;
		}

		// Fetch the next batch
		match self.store.range_rev_next(
			&mut self.cursor,
			self.range.clone(),
			self.version,
			self.batch_size as u64,
		) {
			Ok(batch) => {
				if batch.items.is_empty() {
					return None;
				}
				self.current_batch = batch.items;
				self.current_index = 0;
				self.next()
			}
			Err(e) => Some(Err(e)),
		}
	}
}

/// Classify a range to determine which table it belongs to.
fn classify_key_range(range: &EncodedKeyRange) -> EntryKind {
	use super::router::classify_range;

	classify_range(range).unwrap_or(Multi)
}

/// - Version MAX encodes to 0x00..00 (smallest bytes)
/// - Version 0 encodes to 0xFF..FF (largest bytes)
///
/// For range queries, we need start <= end in byte order:
/// - Start uses version MAX to get the smallest encoded value
/// - End uses version 0 to get the largest encoded value
/// The actual key range and version filtering happens after retrieval.
fn make_versioned_range_bounds(range: &EncodedKeyRange) -> (Vec<u8>, Vec<u8>) {
	let start = match &range.start {
		// Version MAX encodes smallest, capturing all versions of this key
		Bound::Included(key) => encode_versioned_key(key.as_ref(), CommitVersion(u64::MAX)),
		Bound::Excluded(key) => encode_versioned_key(key.as_ref(), CommitVersion(u64::MAX)),
		Bound::Unbounded => encode_versioned_key(&[], CommitVersion(u64::MAX)),
	};

	let end = match &range.end {
		// Version 0 encodes largest, capturing all versions of this key
		Bound::Included(key) => encode_versioned_key(key.as_ref(), CommitVersion(0)),
		Bound::Excluded(key) => encode_versioned_key(key.as_ref(), CommitVersion(0)),
		Bound::Unbounded => encode_versioned_key(&[0xFFu8; 256], CommitVersion(0)),
	};

	(start, end)
}
