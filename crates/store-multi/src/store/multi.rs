// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	ops::{Bound, RangeBounds},
};

use reifydb_core::{
	actors::drop::{DropMessage, DropRequest},
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::metric::{MultiCommittedEvent, MultiDelete, MultiWrite},
	interface::store::{
		EntryKind, MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet,
		MultiVersionGetPrevious, MultiVersionRow, MultiVersionStore,
	},
};
use reifydb_type::util::{cowvec::CowVec, hex};
use tracing::{instrument, warn};

use super::{
	StandardMultiStore,
	router::{classify_key, classify_range, is_single_version_semantics_key},
	version::{VersionedGetResult, get_at_version},
};
use crate::{
	Result,
	cold::ColdStorage,
	hot::storage::HotStorage,
	tier::{RangeBatch, RangeCursor, TierBatch, TierStorage},
	warm::WarmStorage,
};

/// Fixed chunk size for internal tier scans.
/// This is the number of versioned entries fetched per tier per iteration.
const TIER_SCAN_CHUNK_SIZE: usize = 32;

impl MultiVersionGet for StandardMultiStore {
	#[instrument(name = "store::multi::get", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref()), version = version.0))]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		let table = classify_key(key);

		// Try hot tier first
		if let Some(hot) = &self.hot {
			match get_at_version(hot, table, key.as_ref(), version)? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					return Ok(Some(MultiVersionRow {
						key: key.clone(),
						row: EncodedRow(value),
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
					return Ok(Some(MultiVersionRow {
						key: key.clone(),
						row: EncodedRow(value),
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
					return Ok(Some(MultiVersionRow {
						key: key.clone(),
						row: EncodedRow(value),
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
	#[instrument(name = "store::multi::contains", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref()), version = version.0), ret)]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		Ok(MultiVersionGet::get(self, key, version)?.is_some())
	}
}

impl MultiVersionCommit for StandardMultiStore {
	#[instrument(name = "store::multi::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len(), version = version.0))]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()> {
		// Get the hot storage tier (warm and cold are placeholders for now)
		let Some(storage) = &self.hot else {
			return Ok(());
		};

		let classified = classify_deltas(&deltas);
		let drop_batch = build_drop_batch(classified.explicit_drops, &classified.pending_set_keys, version);
		self.dispatch_drops(drop_batch);

		storage.set(version, classified.batches)?;
		self.emit_commit_metrics(classified.writes, classified.deletes, version);
		Ok(())
	}
}

/// `commit`'s per-delta classification: Set/Unset go to `batches` (and emit
/// metric entries), Remove goes to `batches` only, Drop is queued for the
/// drop-actor with optional pending-version tagging if the same key was Set
/// in this commit (single-version-semantics keys).
struct ClassifiedDeltas {
	pending_set_keys: HashSet<CowVec<u8>>,
	writes: Vec<MultiWrite>,
	deletes: Vec<MultiDelete>,
	batches: TierBatch,
	explicit_drops: Vec<(EntryKind, EncodedKey)>,
}

#[inline]
fn classify_deltas(deltas: &CowVec<Delta>) -> ClassifiedDeltas {
	let mut pending_set_keys: HashSet<CowVec<u8>> = HashSet::new();
	let mut writes: Vec<MultiWrite> = Vec::new();
	let mut deletes: Vec<MultiDelete> = Vec::new();
	let mut batches: TierBatch = HashMap::new();
	let mut explicit_drops: Vec<(EntryKind, EncodedKey)> = Vec::new();

	for delta in deltas.iter() {
		let key = delta.key();
		let table = classify_key(key);
		let is_single_version = is_single_version_semantics_key(key);

		match delta {
			Delta::Set {
				key,
				row,
			} => {
				if is_single_version {
					pending_set_keys.insert(key.0.clone());
				}
				writes.push(MultiWrite {
					key: key.clone(),
					value_bytes: row.len() as u64,
				});
				batches.entry(table).or_default().push((key.0.clone(), Some(row.0.clone())));
			}
			Delta::Unset {
				key,
				row,
			} => {
				deletes.push(MultiDelete {
					key: key.clone(),
					value_bytes: row.len() as u64,
				});
				batches.entry(table).or_default().push((key.0.clone(), None));
			}
			Delta::Remove {
				key,
			} => {
				deletes.push(MultiDelete {
					key: key.clone(),
					value_bytes: 0,
				});
				batches.entry(table).or_default().push((key.0.clone(), None));
			}
			Delta::Drop {
				key,
			} => {
				explicit_drops.push((table, key.clone()));
			}
		}
	}

	ClassifiedDeltas {
		pending_set_keys,
		writes,
		deletes,
		batches,
		explicit_drops,
	}
}

/// Combine explicit `Delta::Drop` requests with implicit drops for
/// single-version-semantics keys that were also Set in this commit. Both kinds
/// share the same commit version; explicit drops carry a pending_version only
/// when the same key was Set in this commit (overlap case).
#[inline]
fn build_drop_batch(
	explicit_drops: Vec<(EntryKind, EncodedKey)>,
	pending_set_keys: &HashSet<CowVec<u8>>,
	version: CommitVersion,
) -> Vec<DropRequest> {
	let mut drop_batch = Vec::with_capacity(explicit_drops.len() + pending_set_keys.len());
	for (table, key) in explicit_drops {
		let pending_version = if pending_set_keys.contains(key.as_ref()) {
			Some(version)
		} else {
			None
		};
		drop_batch.push(DropRequest {
			table,
			key: key.0.clone(),
			commit_version: version,
			pending_version,
		});
	}
	for key in pending_set_keys.iter() {
		let table = classify_key(&EncodedKey(key.clone()));
		drop_batch.push(DropRequest {
			table,
			key: key.clone(),
			commit_version: version,
			pending_version: Some(version),
		});
	}
	drop_batch
}

impl StandardMultiStore {
	#[inline]
	fn dispatch_drops(&self, drop_batch: Vec<DropRequest>) {
		if !drop_batch.is_empty() && self.drop_actor.send_blocking(DropMessage::Batch(drop_batch)).is_err() {
			warn!("Failed to send drop batch");
		}
	}

	#[inline]
	fn emit_commit_metrics(&self, writes: Vec<MultiWrite>, deletes: Vec<MultiDelete>, version: CommitVersion) {
		if writes.is_empty() && deletes.is_empty() {
			return;
		}
		self.event_bus.emit(MultiCommittedEvent::new(writes, deletes, vec![], version));
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

pub struct TierScanQuery<'a> {
	pub table: EntryKind,
	pub start: &'a [u8],
	pub end: &'a [u8],
	pub version: CommitVersion,
	pub range: &'a EncodedKeyRange,
}

pub fn scan_tier_chunk<S: TierStorage>(
	storage: &S,
	cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
	let batch = storage.range_next(
		scan.table,
		cursor,
		Bound::Included(scan.start),
		Bound::Included(scan.end),
		scan.version,
		TIER_SCAN_CHUNK_SIZE,
	)?;
	merge_tier_batch(batch, scan.range, collected)
}

pub fn scan_tier_chunk_rev<S: TierStorage>(
	storage: &S,
	cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
	let batch = storage.range_rev_next(
		scan.table,
		cursor,
		Bound::Included(scan.start),
		Bound::Included(scan.end),
		scan.version,
		TIER_SCAN_CHUNK_SIZE,
	)?;
	merge_tier_batch(batch, scan.range, collected)
}

#[inline]
fn merge_tier_batch(
	batch: RangeBatch,
	range: &EncodedKeyRange,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
	if batch.entries.is_empty() {
		return Ok(false);
	}

	for entry in batch.entries {
		let original_key = entry.key.as_slice().to_vec();
		let entry_version = entry.version;

		let original_key_encoded = EncodedKey(CowVec::new(original_key.clone()));
		if !range.contains(&original_key_encoded) {
			continue;
		}

		let should_update = match collected.get(&original_key) {
			None => true,
			Some((existing_version, _)) => entry_version > *existing_version,
		};

		if should_update {
			collected.insert(original_key, (entry_version, entry.value));
		}
	}

	Ok(true)
}

#[inline]
pub fn collected_to_batch(
	collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
	has_more: bool,
) -> MultiVersionBatch {
	let items: Vec<MultiVersionRow> = collected
		.into_iter()
		.filter_map(|(key_bytes, (v, value))| {
			value.map(|val| MultiVersionRow {
				key: EncodedKey(CowVec::new(key_bytes)),
				row: EncodedRow(val),
				version: v,
			})
		})
		.collect();

	MultiVersionBatch {
		items,
		has_more,
	}
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn step_all_tiers(
	hot: Option<&HotStorage>,
	hot_cursor: &mut RangeCursor,
	warm: Option<&WarmStorage>,
	warm_cursor: &mut RangeCursor,
	cold: Option<&ColdStorage>,
	cold_cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
	let mut any_progress = false;
	if let Some(s) = hot
		&& !hot_cursor.exhausted
	{
		any_progress |= scan_tier_chunk(s, hot_cursor, scan, collected)?;
	}
	if let Some(s) = warm
		&& !warm_cursor.exhausted
	{
		any_progress |= scan_tier_chunk(s, warm_cursor, scan, collected)?;
	}
	if let Some(s) = cold
		&& !cold_cursor.exhausted
	{
		any_progress |= scan_tier_chunk(s, cold_cursor, scan, collected)?;
	}
	Ok(any_progress)
}

pub fn scan_tiers_latest(
	hot: Option<&HotStorage>,
	warm: Option<&WarmStorage>,
	cold: Option<&ColdStorage>,
	range: EncodedKeyRange,
	version: CommitVersion,
	max_keys: usize,
) -> Result<MultiVersionBatch> {
	let table = classify_key_range(&range);
	let (start, end) = make_range_bounds(&range);
	let scan = TierScanQuery {
		table,
		start: &start,
		end: &end,
		version,
		range: &range,
	};

	let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();
	let mut hot_cursor = RangeCursor::default();
	let mut warm_cursor = RangeCursor::default();
	let mut cold_cursor = RangeCursor::default();
	let mut exhausted = false;

	while collected.len() < max_keys {
		let progress = step_all_tiers(
			hot,
			&mut hot_cursor,
			warm,
			&mut warm_cursor,
			cold,
			&mut cold_cursor,
			&scan,
			&mut collected,
		)?;
		if !progress {
			exhausted = true;
			break;
		}
	}

	Ok(collected_to_batch(collected, !exhausted))
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
	) -> Result<MultiVersionBatch> {
		if cursor.exhausted {
			return Ok(MultiVersionBatch {
				items: Vec::new(),
				has_more: false,
			});
		}

		// An unconfigured tier has nothing to contribute; treat it as exhausted so
		// the horizon helpers below don't see a phantom "non-exhausted, no
		// last_key" cursor and refuse to filter.
		mark_unconfigured_exhausted(self, cursor);

		let table = classify_key_range(&range);
		let (start, end) = make_range_bounds(&range);
		let batch_size = batch_size as usize;
		let scan = TierScanQuery {
			table,
			start: &start,
			end: &end,
			version,
			range: &range,
		};

		let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		while collected.len() < batch_size {
			let progress = step_all_tiers(
				self.hot.as_ref(),
				&mut cursor.hot,
				self.warm.as_ref(),
				&mut cursor.warm,
				self.cold.as_ref(),
				&mut cursor.cold,
				&scan,
				&mut collected,
			)?;
			if !progress {
				cursor.exhausted = true;
				break;
			}
		}

		// Per-tier cursors can be at different positions after this iteration:
		// tiers with smaller chunks advanced their last_key less far than tiers with
		// larger chunks. Emitting everything would let the lagging tier re-emit
		// keys above its own last_key on the next call. Compute horizon = smallest
		// last_key among non-exhausted tiers, drop entries beyond it from
		// `collected`, and rewind any over-advanced tier so the next call resumes
		// from the same horizon for every tier.
		apply_forward_horizon(cursor, &mut collected);

		// Convert to MultiVersionRow in sorted key order, filtering out tombstones.
		let items: Vec<MultiVersionRow> = collected
			.into_iter()
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionRow {
					key: EncodedKey(CowVec::new(key_bytes)),
					row: EncodedRow(val),
					version: v,
				})
			})
			.collect();

		let has_more = !cursor.exhausted;

		Ok(MultiVersionBatch {
			items,
			has_more,
		})
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
	) -> Result<MultiVersionBatch> {
		if cursor.exhausted {
			return Ok(MultiVersionBatch {
				items: Vec::new(),
				has_more: false,
			});
		}

		mark_unconfigured_exhausted(self, cursor);

		let table = classify_key_range(&range);
		let (start, end) = make_range_bounds(&range);
		let batch_size = batch_size as usize;
		let scan = TierScanQuery {
			table,
			start: &start,
			end: &end,
			version,
			range: &range,
		};

		let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		while collected.len() < batch_size {
			let mut any_progress = false;

			if let Some(hot) = &self.hot
				&& !cursor.hot.exhausted
			{
				any_progress |= scan_tier_chunk_rev(hot, &mut cursor.hot, &scan, &mut collected)?;
			}

			if let Some(warm) = &self.warm
				&& !cursor.warm.exhausted
			{
				any_progress |= scan_tier_chunk_rev(warm, &mut cursor.warm, &scan, &mut collected)?;
			}

			if let Some(cold) = &self.cold
				&& !cursor.cold.exhausted
			{
				any_progress |= scan_tier_chunk_rev(cold, &mut cursor.cold, &scan, &mut collected)?;
			}

			if !any_progress {
				cursor.exhausted = true;
				break;
			}
		}

		// Per-tier cursors can be at different positions after this iteration. In
		// reverse iteration `last_key` is the smallest key emitted; the tier whose
		// chunk reached the smallest key has consumed more than its peers. Emitting
		// everything would let the trailing tier re-emit keys below its own
		// last_key on the next call. Compute horizon = largest last_key among
		// non-exhausted tiers, drop entries < horizon from `collected`, and rewind
		// any over-advanced tier so the next call resumes from the same horizon for
		// every tier.
		apply_reverse_horizon(cursor, &mut collected);

		// Convert to MultiVersionRow in REVERSE sorted key order, filtering out tombstones.
		let items: Vec<MultiVersionRow> = collected
			.into_iter()
			.rev()
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionRow {
					key: EncodedKey(CowVec::new(key_bytes)),
					row: EncodedRow(val),
					version: v,
				})
			})
			.collect();

		let has_more = !cursor.exhausted;

		Ok(MultiVersionBatch {
			items,
			has_more,
		})
	}
}

/// Mark per-tier cursors as exhausted for any tier the store does not have
/// configured. The cursor type carries a slot for every tier regardless of
/// whether the storage is wired up; without this, the horizon helpers see a
/// phantom non-exhausted cursor with `last_key=None` and refuse to filter.
fn mark_unconfigured_exhausted(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor) {
	if store.hot.is_none() {
		cursor.hot.exhausted = true;
	}
	if store.warm.is_none() {
		cursor.warm.exhausted = true;
	}
	if store.cold.is_none() {
		cursor.cold.exhausted = true;
	}
}

/// Drop collected entries past the slowest non-exhausted tier's last_key, and
/// rewind any over-advanced tier cursor to that horizon. After this returns,
/// every non-exhausted tier's `last_key` is identical, so the next call resumes
/// from a consistent position across tiers and the BTreeMap dedupe in
/// `scan_tier_chunk` covers any re-emission.
fn apply_forward_horizon(
	cursor: &mut MultiVersionRangeCursor,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) {
	let horizon = forward_horizon(cursor);
	if let Some(h) = horizon {
		collected.retain(|k, _| k.as_slice() <= h.as_slice());
		rewind_over_advanced_forward(cursor, &h);
	}
}

fn apply_reverse_horizon(
	cursor: &mut MultiVersionRangeCursor,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) {
	let horizon = reverse_horizon(cursor);
	if let Some(h) = horizon {
		collected.retain(|k, _| k.as_slice() >= h.as_slice());
		rewind_over_advanced_reverse(cursor, &h);
	}
}

fn forward_horizon(cursor: &MultiVersionRangeCursor) -> Option<CowVec<u8>> {
	let mut horizon: Option<CowVec<u8>> = None;
	for tier in [&cursor.hot, &cursor.warm, &cursor.cold] {
		if tier.exhausted {
			continue;
		}
		let last = match &tier.last_key {
			Some(k) => k.clone(),
			// A non-exhausted tier with no last_key emitted nothing this iteration
			// but may still have keys to return; horizon must be the start of the
			// range, which we represent as "no horizon" so nothing is dropped.
			None => return None,
		};
		horizon = Some(match horizon {
			None => last,
			Some(prev) => {
				if last.as_slice() < prev.as_slice() {
					last
				} else {
					prev
				}
			}
		});
	}
	horizon
}

fn reverse_horizon(cursor: &MultiVersionRangeCursor) -> Option<CowVec<u8>> {
	let mut horizon: Option<CowVec<u8>> = None;
	for tier in [&cursor.hot, &cursor.warm, &cursor.cold] {
		if tier.exhausted {
			continue;
		}
		let last = match &tier.last_key {
			Some(k) => k.clone(),
			None => return None,
		};
		horizon = Some(match horizon {
			None => last,
			Some(prev) => {
				if last.as_slice() > prev.as_slice() {
					last
				} else {
					prev
				}
			}
		});
	}
	horizon
}

fn rewind_over_advanced_forward(cursor: &mut MultiVersionRangeCursor, horizon: &CowVec<u8>) {
	for tier in [&mut cursor.hot, &mut cursor.warm, &mut cursor.cold] {
		if tier.exhausted {
			continue;
		}
		if let Some(last) = &tier.last_key
			&& last.as_slice() > horizon.as_slice()
		{
			tier.last_key = Some(horizon.clone());
		}
	}
}

fn rewind_over_advanced_reverse(cursor: &mut MultiVersionRangeCursor, horizon: &CowVec<u8>) {
	for tier in [&mut cursor.hot, &mut cursor.warm, &mut cursor.cold] {
		if tier.exhausted {
			continue;
		}
		if let Some(last) = &tier.last_key
			&& last.as_slice() < horizon.as_slice()
		{
			tier.last_key = Some(horizon.clone());
		}
	}
}

impl MultiVersionGetPrevious for StandardMultiStore {
	fn get_previous_version(
		&self,
		key: &EncodedKey,
		before_version: CommitVersion,
	) -> Result<Option<MultiVersionRow>> {
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
			}) => Ok(Some(MultiVersionRow {
				key: key.clone(),
				row: EncodedRow(CowVec::new(value.to_vec())),
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
	current_batch: Vec<MultiVersionRow>,
	current_index: usize,
}

impl Iterator for MultiVersionRangeIter {
	type Item = Result<MultiVersionRow>;

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
					if self.cursor.exhausted {
						return None;
					}
					return self.next();
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
	current_batch: Vec<MultiVersionRow>,
	current_index: usize,
}

impl Iterator for MultiVersionRangeRevIter {
	type Item = Result<MultiVersionRow>;

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
					if self.cursor.exhausted {
						return None;
					}
					return self.next();
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
	classify_range(range).unwrap_or(EntryKind::Multi)
}

/// Create range bounds from an EncodedKeyRange.
/// Returns the start and end byte slices for the range query.
fn make_range_bounds(range: &EncodedKeyRange) -> (Vec<u8>, Vec<u8>) {
	let start = match &range.start {
		Bound::Included(key) => key.as_ref().to_vec(),
		Bound::Excluded(key) => key.as_ref().to_vec(),
		Bound::Unbounded => vec![],
	};

	let end = match &range.end {
		Bound::Included(key) => key.as_ref().to_vec(),
		Bound::Excluded(key) => key.as_ref().to_vec(),
		Bound::Unbounded => vec![0xFFu8; 256],
	};

	(start, end)
}
