// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, btree_map::Entry},
	ops::{Bound, RangeBounds},
	sync::atomic::Ordering,
	vec,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::metric::{MultiCommittedEvent, MultiDelete, MultiWrite},
	interface::store::{
		EntryKind, MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet,
		MultiVersionGetPrevious, MultiVersionRow, MultiVersionStore, StorageKey, classify_key, classify_range,
		storage_key,
	},
	key::typed::TypedKey,
};
use reifydb_store_commit::{
	MultiVersionScope, RangeBatch, RangeCursor, RangeStop, TierBatch, VersionedGetResult, store::CommitStore,
};
use reifydb_value::{
	reifydb_assertions,
	util::{cowvec::CowVec, hex},
};
use tracing::instrument;

use super::StandardMultiStore;
use crate::{
	Result,
	tier::{TierStorage, persistent::MultiPersistentTier, range::ServedChunk},
};

const TIER_SCAN_CHUNK_SIZE: usize = 32;

#[derive(Clone, Copy)]
struct ClassifiedKey<'a> {
	key: &'a EncodedKey,
	storage_key: Option<StorageKey>,
}

impl MultiVersionGet for StandardMultiStore {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		let (table, storage_key) = storage_key(key);
		match table {
			EntryKind::Source(_) => self.get_source(table, storage_key, key, version),
			_ => self.get_multi(table, storage_key, key, version),
		}
	}
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::get::source", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_source(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<MultiVersionRow>> {
		self.get_impl(table, storage_key, key, version)
	}

	#[instrument(name = "store::multi::get::multi", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_multi(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<MultiVersionRow>> {
		self.get_impl(table, storage_key, key, version)
	}

	#[inline]
	fn get_impl(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<MultiVersionRow>> {
		if let Some(found) = self.get_probe_commit(table, key, version)? {
			return Ok(found);
		}
		if let Some(found) = self.get_probe_read(table, storage_key, key, version) {
			return Ok(found);
		}
		if let Some(found) = self.get_probe_persistent(table, storage_key, key, version)? {
			return Ok(found);
		}

		Ok(None)
	}
}

impl StandardMultiStore {
	#[inline]
	fn get_probe_commit(
		&self,
		table: EntryKind,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<Option<MultiVersionRow>>> {
		Ok(match self.commit.get(table, key.as_ref(), version)? {
			VersionedGetResult::Value {
				value,
				version: v,
			} => Some(Some(MultiVersionRow {
				key: key.clone(),
				bytes: EncodedBytes(value),
				version: v,
			})),
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
		})
	}

	#[inline]
	fn get_probe_read(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Option<Option<MultiVersionRow>> {
		let point = self.point.as_ref()?;
		match point.get(table, storage_key, key, version) {
			VersionedGetResult::Value {
				value,
				version: v,
			} => Some(Some(MultiVersionRow {
				key: key.clone(),
				bytes: EncodedBytes(value),
				version: v,
			})),
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
		}
	}

	#[inline]
	fn get_probe_persistent(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<Option<MultiVersionRow>>> {
		let Some(persistent) = &self.persistent else {
			return Ok(None);
		};
		if !persistent.filter().may_contain((table, key)) {
			return Ok(None);
		}
		self.persistent_probes.fetch_add(1, Ordering::Relaxed);
		Ok(match persistent.get(table, key.as_ref(), version)? {
			VersionedGetResult::Value {
				value,
				version: v,
			} => {
				if let Some(point) = &self.point {
					point.insert(table, storage_key, key.clone(), v, Some(value.clone()));
				}
				Some(Some(MultiVersionRow {
					key: key.clone(),
					bytes: EncodedBytes(value),
					version: v,
				}))
			}
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => {
				self.persistent_absent.fetch_add(1, Ordering::Relaxed);
				None
			}
		})
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
		let classified = classify_deltas(&deltas);

		self.update_read_cache_on_commit(&classified.batches);

		self.write_batches(version, classified.batches)?;

		self.emit_commit_metrics(classified.writes, classified.deletes, version);

		Ok(())
	}
}

struct ClassifiedDeltas {
	writes: Vec<MultiWrite>,
	deletes: Vec<MultiDelete>,
	batches: TierBatch,
}

#[inline]
fn classify_deltas(deltas: &CowVec<Delta>) -> ClassifiedDeltas {
	let mut writes: Vec<MultiWrite> = Vec::new();
	let mut deletes: Vec<MultiDelete> = Vec::new();
	let mut batches: TierBatch = HashMap::new();

	for delta in deltas.iter() {
		let key = delta.key();
		let table = classify_key(key);

		match delta {
			Delta::Set {
				key,
				bytes,
			} => {
				writes.push(MultiWrite {
					key: key.clone(),
					value_bytes: bytes.len() as u64,
				});
				batches.entry(table).or_default().push((key.clone(), Some(bytes.0.clone())));
			}
			Delta::Remove {
				key,
				..
			} => {
				deletes.push(MultiDelete {
					key: key.clone(),
				});
				batches.entry(table).or_default().push((key.clone(), None));
			}
		}
	}

	ClassifiedDeltas {
		writes,
		deletes,
		batches,
	}
}

impl StandardMultiStore {
	pub fn get_many(
		&self,
		keys: &[EncodedKey],
		version: CommitVersion,
	) -> Result<HashMap<EncodedKey, MultiVersionRow>> {
		let mut by_table: HashMap<EntryKind, Vec<ClassifiedKey<'_>>> = HashMap::new();
		for key in keys {
			let (table, storage_key) = storage_key(key);
			by_table.entry(table).or_default().push(ClassifiedKey {
				key,
				storage_key,
			});
		}

		let mut out: HashMap<EncodedKey, MultiVersionRow> = HashMap::new();
		for (table, table_keys) in by_table {
			self.get_many_for_table(table, &table_keys, version, &mut out)?;
		}

		Ok(out)
	}

	pub fn get_many_versioned(
		&self,
		keys: &[EncodedKey],
		version: CommitVersion,
	) -> Result<HashMap<EncodedKey, VersionedGetResult>> {
		let mut by_table: HashMap<EntryKind, Vec<ClassifiedKey<'_>>> = HashMap::new();
		for key in keys {
			let (table, storage_key) = storage_key(key);
			by_table.entry(table).or_default().push(ClassifiedKey {
				key,
				storage_key,
			});
		}

		let mut out: HashMap<EncodedKey, VersionedGetResult> = HashMap::new();
		for (table, table_keys) in by_table {
			let (commit_results, read_aligned, persistent_aligned) =
				self.probe_tiers(table, &table_keys, version)?;
			for (i, routed) in table_keys.iter().enumerate() {
				let resolved = match &commit_results[i] {
					VersionedGetResult::NotFound => match &read_aligned[i] {
						VersionedGetResult::NotFound => &persistent_aligned[i],
						found => found,
					},
					found => found,
				};
				out.insert(routed.key.clone(), resolved.clone());
			}
		}

		Ok(out)
	}

	#[inline]
	fn probe_tiers(
		&self,
		table: EntryKind,
		table_keys: &[ClassifiedKey<'_>],
		version: CommitVersion,
	) -> Result<(Vec<VersionedGetResult>, Vec<VersionedGetResult>, Vec<VersionedGetResult>)> {
		let key_slices: Vec<&[u8]> = table_keys.iter().map(|routed| routed.key.as_ref()).collect();

		let commit_results = self.probe_commit_batch(table, &key_slices, version)?;
		let (read_aligned, persistent_aligned) = self.resolve_misses_through_read_and_persistent(
			table,
			table_keys,
			&key_slices,
			&commit_results,
			version,
		)?;

		Ok((commit_results, read_aligned, persistent_aligned))
	}

	#[inline]
	fn get_many_for_table(
		&self,
		table: EntryKind,
		table_keys: &[ClassifiedKey<'_>],
		version: CommitVersion,
		out: &mut HashMap<EncodedKey, MultiVersionRow>,
	) -> Result<()> {
		let (commit_results, read_aligned, persistent_aligned) =
			self.probe_tiers(table, table_keys, version)?;

		reifydb_assertions! {
			let n = table_keys.len();
			assert!(
				commit_results.len() == n && read_aligned.len() == n && persistent_aligned.len() == n,
				"per-tier result vectors must stay index-aligned with the table's keys, otherwise collect_resolved_rows \
				 reads a tier result for the wrong key and returns mismatched rows (keys={n}, commit={}, read={}, persistent={})",
				commit_results.len(),
				read_aligned.len(),
				persistent_aligned.len()
			);
		}

		self.collect_resolved_rows(table_keys, &commit_results, &read_aligned, &persistent_aligned, out);
		Ok(())
	}

	#[inline]
	fn probe_commit_batch(
		&self,
		table: EntryKind,
		key_slices: &[&[u8]],
		version: CommitVersion,
	) -> Result<Vec<VersionedGetResult>> {
		self.commit.get_many(table, key_slices, version)
	}

	#[inline]
	fn resolve_misses_through_read_and_persistent(
		&self,
		table: EntryKind,
		table_keys: &[ClassifiedKey<'_>],
		key_slices: &[&[u8]],
		commit_results: &[VersionedGetResult],
		version: CommitVersion,
	) -> Result<(Vec<VersionedGetResult>, Vec<VersionedGetResult>)> {
		let mut read_aligned = vec![VersionedGetResult::NotFound; key_slices.len()];
		let mut persistent_idx: Vec<usize> = Vec::new();
		let mut persistent_slices: Vec<&[u8]> = Vec::new();
		for (i, result) in commit_results.iter().enumerate() {
			if !matches!(result, VersionedGetResult::NotFound) {
				continue;
			}
			let read_hit = self
				.point
				.as_ref()
				.map(|c| c.get(table, table_keys[i].storage_key, table_keys[i].key, version))
				.unwrap_or(VersionedGetResult::NotFound);
			match read_hit {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					read_aligned[i] = VersionedGetResult::Value {
						value,
						version: v,
					};
				}
				VersionedGetResult::Tombstone => {
					read_aligned[i] = VersionedGetResult::Tombstone;
				}
				VersionedGetResult::NotFound => {
					let maybe = match &self.persistent {
						Some(persistent) => {
							persistent.filter().may_contain((table, table_keys[i].key))
						}
						None => false,
					};
					if maybe {
						persistent_idx.push(i);
						persistent_slices.push(key_slices[i]);
					}
				}
			}
		}

		let mut persistent_aligned = vec![VersionedGetResult::NotFound; key_slices.len()];
		if !persistent_slices.is_empty()
			&& let Some(persistent) = &self.persistent
		{
			self.persistent_probes.fetch_add(persistent_slices.len() as u64, Ordering::Relaxed);
			let persistent_results = persistent.get_many(table, &persistent_slices, version)?;
			for (slot, result) in persistent_idx.into_iter().zip(persistent_results) {
				if matches!(result, VersionedGetResult::NotFound) {
					self.persistent_absent.fetch_add(1, Ordering::Relaxed);
				}
				if let VersionedGetResult::Value {
					value,
					version: v,
				} = &result && let Some(point) = &self.point
				{
					point.insert(
						table,
						table_keys[slot].storage_key,
						table_keys[slot].key.clone(),
						*v,
						Some(value.clone()),
					);
				}
				persistent_aligned[slot] = result;
			}
		}

		Ok((read_aligned, persistent_aligned))
	}

	#[inline]
	fn collect_resolved_rows(
		&self,
		table_keys: &[ClassifiedKey<'_>],
		commit_results: &[VersionedGetResult],
		read_aligned: &[VersionedGetResult],
		persistent_aligned: &[VersionedGetResult],
		out: &mut HashMap<EncodedKey, MultiVersionRow>,
	) {
		for (i, routed) in table_keys.iter().enumerate() {
			let resolved = match &commit_results[i] {
				VersionedGetResult::Value {
					value,
					version: v,
				} => Some((value.clone(), *v)),
				VersionedGetResult::Tombstone => None,
				VersionedGetResult::NotFound => match &read_aligned[i] {
					VersionedGetResult::Value {
						value,
						version: v,
					} => Some((value.clone(), *v)),
					VersionedGetResult::Tombstone => None,
					VersionedGetResult::NotFound => match &persistent_aligned[i] {
						VersionedGetResult::Value {
							value,
							version: v,
						} => Some((value.clone(), *v)),
						_ => None,
					},
				},
			};

			if let Some((value, v)) = resolved {
				out.insert(
					routed.key.clone(),
					MultiVersionRow {
						key: routed.key.clone(),
						bytes: EncodedBytes(value),
						version: v,
					},
				);
			}
		}
	}

	#[inline]
	fn update_read_cache_on_commit(&self, batches: &TierBatch) {
		if self.point.is_none() && self.range.is_none() {
			return;
		}
		for (table, entries) in batches.iter() {
			for (key, _) in entries {
				if let Some(range) = &self.range {
					range.invalidate(*table, key);
				}
				if let Some(point) = &self.point {
					point.invalidate(*table, storage_key(key).1, key);
				}
			}
		}
	}

	#[inline]
	fn write_batches(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		self.commit.set(version, batches)
	}

	#[inline]
	fn emit_commit_metrics(&self, writes: Vec<MultiWrite>, deletes: Vec<MultiDelete>, version: CommitVersion) {
		if writes.is_empty() && deletes.is_empty() {
			return;
		}
		self.event_bus.emit(MultiCommittedEvent::new(writes, deletes, version));
	}
}

#[derive(Debug, Clone, Default)]
pub struct MultiVersionRangeCursor {
	pub commit: RangeCursor,

	pub persistent: RangeCursor,

	pub exhausted: bool,

	persistent_recheck_spent: bool,

	materialize: bool,
}

impl MultiVersionRangeCursor {
	pub fn new() -> Self {
		Self {
			materialize: true,
			..Default::default()
		}
	}

	pub fn cold() -> Self {
		Self {
			materialize: false,
			..Default::default()
		}
	}

	pub fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

pub struct TierScanQuery<'a> {
	pub table: EntryKind,
	pub start: &'a [u8],
	pub end: &'a [u8],
	pub scope: MultiVersionScope,
	pub range: &'a EncodedKeyRange,
}

pub fn scan_tier_chunk<S: TierStorage>(
	storage: &S,
	cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<()> {
	let batch = storage.range_next(
		scan.table,
		cursor,
		Bound::Included(scan.start),
		Bound::Included(scan.end),
		scan.scope,
		TIER_SCAN_CHUNK_SIZE,
	)?;
	merge_tier_batch(batch, scan.range, collected)
}

pub fn scan_tier_chunk_rev<S: TierStorage>(
	storage: &S,
	cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<()> {
	let batch = storage.range_rev_next(
		scan.table,
		cursor,
		Bound::Included(scan.start),
		Bound::Included(scan.end),
		scan.scope,
		TIER_SCAN_CHUNK_SIZE,
	)?;
	merge_tier_batch(batch, scan.range, collected)
}

#[inline]
fn merge_tier_batch(
	batch: RangeBatch,
	range: &EncodedKeyRange,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<()> {
	for entry in batch.entries {
		if !range.contains(&entry.key) {
			continue;
		}

		match collected.entry(entry.key) {
			Entry::Vacant(slot) => {
				slot.insert((entry.version, entry.value));
			}
			Entry::Occupied(mut slot) => {
				if entry.version > slot.get().0 {
					slot.insert((entry.version, entry.value));
				}
			}
		}
	}

	Ok(())
}

#[inline]
pub fn collected_to_batch(
	collected: BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
	has_more: bool,
) -> MultiVersionBatch {
	let items: Vec<MultiVersionRow> = collected
		.into_iter()
		.filter_map(|(key, (v, value))| {
			value.map(|val| MultiVersionRow {
				key,
				bytes: EncodedBytes(val),
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
fn step_all_tiers(
	buffer: Option<&CommitStore>,
	buffer_cursor: &mut RangeCursor,
	persistent: Option<&MultiPersistentTier>,
	persistent_cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<()> {
	if let Some(s) = buffer
		&& !buffer_cursor.is_exhausted()
	{
		scan_tier_chunk(s, buffer_cursor, scan, collected)?;
	}
	if let Some(s) = persistent
		&& !persistent_cursor.is_exhausted()
	{
		scan_tier_chunk(s, persistent_cursor, scan, collected)?;
	}
	Ok(())
}

pub fn scan_tiers_latest(
	buffer: Option<&CommitStore>,
	persistent: Option<&MultiPersistentTier>,
	range: EncodedKeyRange,
	scope: MultiVersionScope,
	max_keys: usize,
) -> Result<MultiVersionBatch> {
	let table = classify_key_range(&range);
	let (start, end) = make_range_bounds(&range);
	let scan = TierScanQuery {
		table,
		start: &start,
		end: &end,
		scope,
		range: &range,
	};

	let mut collected: BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();
	let mut buffer_cursor = if buffer.is_none() {
		RangeCursor::start_exhausted()
	} else {
		RangeCursor::new()
	};
	let mut persistent_cursor = if persistent.is_none() {
		RangeCursor::start_exhausted()
	} else {
		RangeCursor::new()
	};
	let mut exhausted = false;

	while collected.len() < max_keys {
		step_all_tiers(buffer, &mut buffer_cursor, persistent, &mut persistent_cursor, &scan, &mut collected)?;
		if buffer_cursor.is_exhausted() && persistent_cursor.is_exhausted() {
			exhausted = true;
			break;
		}
	}

	Ok(collected_to_batch(collected, !exhausted))
}

impl StandardMultiStore {
	pub fn range_next(
		&self,
		cursor: &mut MultiVersionRangeCursor,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
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
			scope,
			range: &range,
		};

		let mut collected: BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		while collected.len() < batch_size {
			if !cursor.commit.is_exhausted() {
				scan_tier_chunk(&self.commit, &mut cursor.commit, &scan, &mut collected)?;
			}

			if self.persistent.is_some() && !cursor.persistent.is_exhausted() {
				self.step_persistent_cached(&scan, cursor, &mut collected, false)?;
			}

			if cursor.commit.is_exhausted() && cursor.persistent.is_exhausted() {
				if reread_persistent_absent_before_its_table(cursor) {
					continue;
				}
				cursor.exhausted = true;
				break;
			}
		}

		apply_forward_horizon(cursor, &mut collected);

		let items: Vec<MultiVersionRow> = collected
			.into_iter()
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionRow {
					key: EncodedKey::new(key_bytes),
					bytes: EncodedBytes(val),
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

	pub fn range(
		&self,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> MultiVersionRangeIter {
		MultiVersionRangeIter {
			store: self.clone(),
			cursor: MultiVersionRangeCursor::new(),
			range,
			scope,
			batch_size,
			current_batch: Vec::new().into_iter(),
		}
	}

	pub fn range_persistence(
		&self,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> MultiVersionRangeIter {
		MultiVersionRangeIter {
			store: self.clone(),
			cursor: MultiVersionRangeCursor::cold(),
			range,
			scope,
			batch_size,
			current_batch: Vec::new().into_iter(),
		}
	}

	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> MultiVersionRangeRevIter {
		MultiVersionRangeRevIter {
			store: self.clone(),
			cursor: MultiVersionRangeCursor::new(),
			range,
			scope,
			batch_size,
			current_batch: Vec::new().into_iter(),
		}
	}

	pub fn range_rev_persistence(
		&self,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> MultiVersionRangeRevIter {
		MultiVersionRangeRevIter {
			store: self.clone(),
			cursor: MultiVersionRangeCursor::cold(),
			range,
			scope,
			batch_size,
			current_batch: Vec::new().into_iter(),
		}
	}

	fn range_rev_next(
		&self,
		cursor: &mut MultiVersionRangeCursor,
		range: EncodedKeyRange,
		scope: MultiVersionScope,
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
			scope,
			range: &range,
		};

		let mut collected: BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		while collected.len() < batch_size {
			if !cursor.commit.is_exhausted() {
				scan_tier_chunk_rev(&self.commit, &mut cursor.commit, &scan, &mut collected)?;
			}

			if self.persistent.is_some() && !cursor.persistent.is_exhausted() {
				self.step_persistent_cached(&scan, cursor, &mut collected, true)?;
			}

			if cursor.commit.is_exhausted() && cursor.persistent.is_exhausted() {
				if reread_persistent_absent_before_its_table(cursor) {
					continue;
				}
				cursor.exhausted = true;
				break;
			}
		}

		apply_reverse_horizon(cursor, &mut collected);

		let items: Vec<MultiVersionRow> = collected
			.into_iter()
			.rev()
			.filter_map(|(key_bytes, (v, value))| {
				value.map(|val| MultiVersionRow {
					key: EncodedKey::new(key_bytes),
					bytes: EncodedBytes(val),
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

	fn step_persistent_cached(
		&self,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
		descending: bool,
	) -> Result<()> {
		let Some(persistent) = &self.persistent else {
			return Ok(());
		};

		match self.serve_from_read_cache(scan, cursor, collected, descending) {
			Some(served) => served?,
			None => self.scan_persistent_chunk(persistent, scan, cursor, collected, descending)?,
		}

		Ok(())
	}

	#[inline]
	fn serve_from_read_cache(
		&self,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
		descending: bool,
	) -> Option<Result<()>> {
		let (Some(range), true) = (&self.range, scan.table.cache_tiers().caches_ranges()) else {
			return None;
		};
		match range.serve_persistent_chunk(
			scan.table,
			&mut cursor.persistent,
			scan.start,
			scan.end,
			scan.scope,
			TIER_SCAN_CHUNK_SIZE,
			descending,
		) {
			ServedChunk::Served(batch) => Some(merge_tier_batch(batch, scan.range, collected)),
			ServedChunk::Gap => None,
		}
	}

	#[inline]
	fn scan_persistent_chunk(
		&self,
		persistent: &MultiPersistentTier,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
		descending: bool,
	) -> Result<()> {
		let resumed_at = cursor.persistent.last_key().cloned();
		let batch = if descending {
			persistent.range_rev_next(
				scan.table,
				&mut cursor.persistent,
				Bound::Included(scan.start),
				Bound::Included(scan.end),
				scan.scope,
				TIER_SCAN_CHUNK_SIZE,
			)?
		} else {
			persistent.range_next(
				scan.table,
				&mut cursor.persistent,
				Bound::Included(scan.start),
				Bound::Included(scan.end),
				scan.scope,
				TIER_SCAN_CHUNK_SIZE,
			)?
		};
		if !descending && cursor.materialize {
			self.materialize_scanned_chunk(
				persistent,
				scan,
				resumed_at.as_ref(),
				&cursor.persistent,
				&batch,
			)?;
		}
		merge_tier_batch(batch, scan.range, collected)
	}

	#[inline]
	fn materialize_scanned_chunk(
		&self,
		persistent: &MultiPersistentTier,
		scan: &TierScanQuery,
		resumed_at: Option<&EncodedKey>,
		cursor: &RangeCursor,
		batch: &RangeBatch,
	) -> Result<()> {
		let (Some(range), true) = (&self.range, scan.table.cache_tiers().caches_ranges()) else {
			return Ok(());
		};
		let MultiVersionScope::AsOf {
			read: at,
		} = scan.scope
		else {
			return Ok(());
		};
		if at < persistent.install_floor()? {
			return Ok(());
		}
		let range_start = EncodedKey::new(scan.start);
		let lo = match resumed_at {
			Some(last) => match last.successor() {
				Some(next) => next.max(range_start),
				None => return Ok(()),
			},
			None => range_start,
		};
		let through = match (cursor.scanned_to_end(), cursor.is_exhausted(), cursor.last_key()) {
			(true, _, _) => EncodedKey::new(scan.end),
			(false, true, _) => return Ok(()),
			(false, false, Some(last)) => last.clone(),
			(false, false, None) => return Ok(()),
		};
		range.materialize_scanned_chunk(scan.table, &lo, &through, &batch.entries);
		Ok(())
	}
}

fn reread_persistent_absent_before_its_table(cursor: &mut MultiVersionRangeCursor) -> bool {
	if cursor.persistent_recheck_spent || cursor.persistent.stop() != Some(&RangeStop::AbsentTable) {
		return false;
	}
	cursor.persistent_recheck_spent = true;
	cursor.persistent.reopen();
	true
}

fn mark_unconfigured_exhausted(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor) {
	if store.persistent.is_none() {
		cursor.persistent.finish();
	}
}

fn apply_forward_horizon(
	cursor: &mut MultiVersionRangeCursor,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) {
	let horizon = forward_horizon(cursor);
	if let Some(h) = horizon {
		collected.retain(|k, _| k.as_slice() <= h.as_slice());
		rewind_over_advanced_forward(cursor, &h);
	}
}

fn apply_reverse_horizon(
	cursor: &mut MultiVersionRangeCursor,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) {
	let horizon = reverse_horizon(cursor);
	if let Some(h) = horizon {
		collected.retain(|k, _| k.as_slice() >= h.as_slice());
		rewind_over_advanced_reverse(cursor, &h);
	}
}

fn forward_horizon(cursor: &MultiVersionRangeCursor) -> Option<EncodedKey> {
	let mut horizon: Option<EncodedKey> = None;
	for tier in [&cursor.commit, &cursor.persistent] {
		if tier.is_exhausted() {
			continue;
		}
		let last = match tier.last_key() {
			Some(k) => k.clone(),

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

fn reverse_horizon(cursor: &MultiVersionRangeCursor) -> Option<EncodedKey> {
	let mut horizon: Option<EncodedKey> = None;
	for tier in [&cursor.commit, &cursor.persistent] {
		if tier.is_exhausted() {
			continue;
		}
		let last = match tier.last_key() {
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

fn rewind_over_advanced_forward(cursor: &mut MultiVersionRangeCursor, horizon: &EncodedKey) {
	for tier in [&mut cursor.commit, &mut cursor.persistent] {
		if let Some(last) = tier.last_key()
			&& last.as_slice() > horizon.as_slice()
		{
			tier.resume(horizon.clone());
		}
	}
}

fn rewind_over_advanced_reverse(cursor: &mut MultiVersionRangeCursor, horizon: &EncodedKey) {
	for tier in [&mut cursor.commit, &mut cursor.persistent] {
		if let Some(last) = tier.last_key()
			&& last.as_slice() < horizon.as_slice()
		{
			tier.resume(horizon.clone());
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

		let (table, storage_key) = storage_key(key);
		reifydb_assertions! {
			assert!(
				before_version.0 >= 1,
				"the before_version==0 guard must precede this subtraction, otherwise before_version.0 - 1 \
				 wraps to u64::MAX and the probe reads the latest version instead of the previous one \
				 (before_version={})",
				before_version.0
			);
		}
		let prev_version = CommitVersion(before_version.0 - 1);

		if let Some(found) = self.previous_probe_commit(table, key, prev_version)? {
			return Ok(found);
		}
		if let Some(found) = self.previous_probe_read(table, storage_key, key, prev_version) {
			return Ok(found);
		}
		if let Some(found) = self.previous_probe_persistent(table, storage_key, key, prev_version)? {
			return Ok(found);
		}

		Ok(None)
	}
}

impl StandardMultiStore {
	#[inline]
	fn previous_probe_commit(
		&self,
		table: EntryKind,
		key: &EncodedKey,
		prev_version: CommitVersion,
	) -> Result<Option<Option<MultiVersionRow>>> {
		Ok(match self.commit.get(table, key.as_ref(), prev_version)? {
			VersionedGetResult::Value {
				value,
				version,
			} => Some(Some(MultiVersionRow {
				key: key.clone(),
				bytes: EncodedBytes(CowVec::new(value.to_vec())),
				version,
			})),
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
		})
	}

	#[inline]
	fn previous_probe_read(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		prev_version: CommitVersion,
	) -> Option<Option<MultiVersionRow>> {
		let point = self.point.as_ref()?;
		match point.get(table, storage_key, key, prev_version) {
			VersionedGetResult::Value {
				value,
				version,
			} => Some(Some(MultiVersionRow {
				key: key.clone(),
				bytes: EncodedBytes(CowVec::new(value.to_vec())),
				version,
			})),
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
		}
	}

	#[inline]
	fn previous_probe_persistent(
		&self,
		table: EntryKind,
		storage_key: Option<StorageKey>,
		key: &EncodedKey,
		prev_version: CommitVersion,
	) -> Result<Option<Option<MultiVersionRow>>> {
		let Some(persistent) = &self.persistent else {
			return Ok(None);
		};
		if !persistent.filter().may_contain((table, key)) {
			return Ok(None);
		}
		self.persistent_probes.fetch_add(1, Ordering::Relaxed);
		Ok(match persistent.get(table, key.as_ref(), prev_version)? {
			VersionedGetResult::Value {
				value,
				version,
			} => {
				if let Some(point) = &self.point {
					point.insert(table, storage_key, key.clone(), version, Some(value.clone()));
				}
				Some(Some(MultiVersionRow {
					key: key.clone(),
					bytes: EncodedBytes(CowVec::new(value.to_vec())),
					version,
				}))
			}
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => {
				self.persistent_absent.fetch_add(1, Ordering::Relaxed);
				None
			}
		})
	}
}

impl MultiVersionStore for StandardMultiStore {}

pub struct MultiVersionRangeIter {
	store: StandardMultiStore,
	cursor: MultiVersionRangeCursor,
	range: EncodedKeyRange,
	scope: MultiVersionScope,
	batch_size: usize,
	current_batch: vec::IntoIter<MultiVersionRow>,
}

impl Iterator for MultiVersionRangeIter {
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(item) = self.current_batch.next() {
			return Some(Ok(item));
		}

		if self.cursor.exhausted {
			return None;
		}

		match self.store.range_next(&mut self.cursor, self.range.clone(), self.scope, self.batch_size as u64) {
			Ok(batch) => {
				if batch.items.is_empty() {
					if self.cursor.exhausted {
						return None;
					}
					return self.next();
				}
				self.current_batch = batch.items.into_iter();
				self.next()
			}
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct MultiVersionRangeRevIter {
	store: StandardMultiStore,
	cursor: MultiVersionRangeCursor,
	range: EncodedKeyRange,
	scope: MultiVersionScope,
	batch_size: usize,
	current_batch: vec::IntoIter<MultiVersionRow>,
}

impl Iterator for MultiVersionRangeRevIter {
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some(item) = self.current_batch.next() {
			return Some(Ok(item));
		}

		if self.cursor.exhausted {
			return None;
		}

		match self.store.range_rev_next(
			&mut self.cursor,
			self.range.clone(),
			self.scope,
			self.batch_size as u64,
		) {
			Ok(batch) => {
				if batch.items.is_empty() {
					if self.cursor.exhausted {
						return None;
					}
					return self.next();
				}
				self.current_batch = batch.items.into_iter();
				self.next()
			}
			Err(e) => Some(Err(e)),
		}
	}
}

fn classify_key_range(range: &EncodedKeyRange) -> EntryKind {
	classify_range(range).unwrap_or(EntryKind::Multi)
}

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

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod cache_tests {
	use std::collections::HashMap;

	use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
	use reifydb_core::{
		common::CommitVersion,
		delta::Delta,
		interface::{
			catalog::{flow::OperatorId, id::TableId, storage::StorageId},
			store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key, storage_key},
		},
		key::{
			EncodableKey,
			operator::state::{GroupId, KeyspaceId, OperatorStateKey},
			row::RowKey,
			typed::key::Key,
		},
	};
	use reifydb_store_commit::{MultiVersionScope, RangeStop, RawEntry, VersionedGetResult};
	use reifydb_value::{byte_size::ByteSize, cow_vec, util::cowvec::CowVec};

	use super::MultiVersionRangeCursor;
	use crate::{
		store::StandardMultiStore,
		tier::{
			TierStorage,
			point::MultiPointConfig,
			range::{MultiRangeConfig, PartitionId},
		},
	};

	const STORAGE: StorageId = StorageId::Table(TableId(1));

	fn commit_row(store: &StandardMultiStore, n: u64, version: u64) {
		MultiVersionCommit::commit(
			store,
			cow_vec![Delta::Set {
				key: RowKey::encoded(STORAGE, n),
				bytes: EncodedBytes(CowVec::new(format!("v{n}").into_bytes())),
			}],
			CommitVersion(version),
		)
		.unwrap();
	}

	fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
		let commit = store.commit();
		for kind in commit.list_all_entry_kinds().unwrap() {
			let (to_persist, to_compact, _consumed, _more) =
				commit.collect_evictable_below(kind, cutoff, ByteSize::from_bytes(u64::MAX));
			if to_compact.is_empty() {
				continue;
			}
			if !to_persist.is_empty() {
				let persistent = store.persistent().expect("persistent tier");
				let mut by_version: HashMap<
					CommitVersion,
					HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>>,
				> = HashMap::new();
				for (key, version, value) in to_persist {
					by_version
						.entry(version)
						.or_default()
						.entry(kind)
						.or_default()
						.push((key, value));
				}
				for (version, batch) in by_version {
					persistent.set(version, batch).unwrap();
				}
			}
			for evicted in &to_compact {
				store.invalidate_read_key(kind, &evicted.key);
			}
			commit.compact(HashMap::from([(
				kind,
				to_compact.into_iter().map(|e| (e.key, e.version)).collect(),
			)]))
			.unwrap();
		}
	}

	#[test]
	fn a_full_scan_claims_every_bucket_it_walks_to_the_edge() {
		const HEAVY: u64 = 192;
		const LIGHT: u64 = 20;
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		for n in 1..=HEAVY {
			commit_row(&store, n, 1);
		}
		for n in 0..LIGHT {
			commit_row(&store, (1u64 << 16) + n, 1);
		}
		flush(&store, CommitVersion(1));

		let range = store.range.clone().expect("range tier configured");
		let kind = EntryKind::Source(STORAGE);
		let heavy = PartitionId::of(kind, &RowKey::encoded(STORAGE, 1)).expect("a row key names a bucket");
		let light =
			PartitionId::of(kind, &RowKey::encoded(STORAGE, 1u64 << 16)).expect("a row key names a bucket");
		assert_ne!(heavy, light, "the two row groups must land in different buckets");
		assert_eq!(range.complete_partitions().iter().sum::<usize>(), 0, "nothing is claimed before the scan");

		let scanned = store
			.range(
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				32,
			)
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(scanned.len() as u64, HEAVY + LIGHT, "the scan returns every row");

		assert_eq!(
			range.complete_partitions().iter().sum::<usize>(),
			2,
			"both buckets the scan walked to their edge must be claimed"
		);
	}

	#[test]
	fn operator_state_commit_does_not_populate_the_point_tier() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let point = store.point.clone().expect("point tier configured");

		let opkey = OperatorStateKey::new(
			OperatorId(7),
			GroupId::ROOT,
			KeyspaceId::CUSTOM_NOT_CACHED,
			vec![1, 2, 3],
		)
		.encode();
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: opkey.clone(),
				bytes: EncodedBytes(CowVec::new(b"state-v10".to_vec())),
			}],
			CommitVersion(10),
		)
		.unwrap();

		assert!(
			matches!(
				point.get(classify_key(&opkey), storage_key(&opkey).1, &opkey, CommitVersion(10)),
				VersionedGetResult::NotFound
			),
			"an operator commit must not write through into the point tier"
		);
		assert_eq!(
			store.point_shard_metrics().iter().map(|shard| shard.entries).sum::<usize>(),
			0,
			"no operator row may become resident on commit"
		);

		let row = MultiVersionGet::get(&store, &opkey, CommitVersion(10))
			.unwrap()
			.expect("the committed operator state must still be readable through the store");
		assert_eq!(row.bytes.as_slice(), b"state-v10");
		assert_eq!(row.version, CommitVersion(10));

		assert!(
			matches!(
				point.get(classify_key(&opkey), storage_key(&opkey).1, &opkey, CommitVersion(10)),
				VersionedGetResult::NotFound
			),
			"a store-level operator read must not back-populate the point tier"
		);
	}

	#[test]
	fn source_row_write_clears_coverage_on_its_partition() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let range = store.range.clone().expect("range tier configured");

		let neighbor = RowKey::encoded(STORAGE, 1);
		let kind = classify_key(&neighbor);
		let partition = PartitionId::of(kind, &neighbor).expect("a source row key must name a partition");
		assert_eq!(
			PartitionId::of(kind, &RowKey::encoded(STORAGE, 2)),
			Some(partition),
			"both source rows must share a partition for this test to exercise the retraction"
		);
		assert!(
			range.materialize_scanned_chunk(
				kind,
				&RowKey::storage_start(STORAGE),
				&RowKey::storage_end(STORAGE),
				&[RawEntry {
					key: neighbor,
					version: CommitVersion(1),
					value: Some(CowVec::new(b"neighbor".to_vec())),
				}],
			),
			"the seeding chunk must publish its claim"
		);
		assert_eq!(
			range.complete_partitions().iter().sum::<usize>(),
			1,
			"the partition must start range-complete, or the retraction below proves nothing"
		);

		commit_row(&store, 2, 5);

		assert_eq!(
			range.complete_partitions().iter().sum::<usize>(),
			0,
			"writing a source row into a covered partition must retract the claim, or a later read is \
			 answered from a partition that no longer holds every row it claims"
		);
	}

	fn drain_forward(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor, read: u64) -> Vec<u64> {
		let mut seen = Vec::new();
		loop {
			let batch = store
				.range_next(
					cursor,
					RowKey::full_scan(STORAGE),
					MultiVersionScope::AsOf {
						read: CommitVersion(read),
					},
					2,
				)
				.unwrap();
			seen.extend(batch.items.iter().map(|row| row.key.clone()));
			if !batch.has_more {
				return rows_of(seen);
			}
		}
	}

	fn drain_reverse(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor, read: u64) -> Vec<u64> {
		let mut seen = Vec::new();
		loop {
			let batch = store
				.range_rev_next(
					cursor,
					RowKey::full_scan(STORAGE),
					MultiVersionScope::AsOf {
						read: CommitVersion(read),
					},
					2,
				)
				.unwrap();
			seen.extend(batch.items.iter().map(|row| row.key.clone()));
			if !batch.has_more {
				return rows_of(seen);
			}
		}
	}

	fn rows_of(keys: Vec<EncodedKey>) -> Vec<u64> {
		let mut rows: Vec<u64> = keys.iter().map(|key| RowKey::decode(key).expect("a row key").row.0).collect();
		rows.sort_unstable();
		rows.dedup();
		rows
	}

	const ROWS: u64 = 200;

	fn store_without_read_tier() -> (StandardMultiStore, impl Drop) {
		StandardMultiStore::testing_memory_with_persistent_sqlite_tiers(
			MultiPointConfig {
				shard_bytes: None,
				..MultiPointConfig::testing()
			},
			MultiRangeConfig {
				shard_bytes: None,
				..MultiRangeConfig::testing()
			},
		)
	}

	const PERSISTED: u64 = 200;
	const BUFFERED: u64 = 200;

	fn seed_both_tiers(store: &StandardMultiStore, persist_the_low_rows: bool) {
		let (persist, buffer) = if persist_the_low_rows {
			(1..=PERSISTED, PERSISTED + 1..=PERSISTED + BUFFERED)
		} else {
			(PERSISTED + 1..=PERSISTED + BUFFERED, 1..=BUFFERED)
		};
		for n in persist {
			commit_row(store, n, 1);
		}
		flush(store, CommitVersion(1));
		for n in buffer {
			commit_row(store, n, 2);
		}
	}

	#[test]
	fn a_scan_reading_both_tiers_loses_nothing_when_a_flush_lands_under_it() {
		let (store, _g) = store_without_read_tier();
		seed_both_tiers(&store, true);

		let mut cursor = MultiVersionRangeCursor::new();
		let first = store
			.range_next(
				&mut cursor,
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(2),
				},
				2,
			)
			.unwrap();
		assert!(first.has_more, "four hundred rows cannot fit in one tier chunk, so the scan must continue");
		assert_ne!(
			cursor.persistent.stop(),
			Some(&RangeStop::AbsentTable),
			"the persistent tier must hold a table here, or this is the empty-database case again"
		);
		assert!(
			cursor.persistent.last_key().is_some(),
			"the persistent tier contributed nothing, so the scan is not merging two live tiers"
		);

		flush(&store, CommitVersion(2));

		let mut seen = rows_of(first.items.iter().map(|row| row.key.clone()).collect());
		seen.extend(drain_forward(&store, &mut cursor, 2));
		seen.sort_unstable();
		seen.dedup();

		assert_eq!(
			seen,
			(1..=PERSISTED + BUFFERED).collect::<Vec<_>>(),
			"the merge dropped rows the flush moved from the buffer into a tier the scan had already read past"
		);
	}

	#[test]
	fn a_reverse_scan_reading_both_tiers_loses_nothing_when_a_flush_lands_under_it() {
		let (store, _g) = store_without_read_tier();
		seed_both_tiers(&store, false);

		let mut cursor = MultiVersionRangeCursor::new();
		let first = store
			.range_rev_next(
				&mut cursor,
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(2),
				},
				2,
			)
			.unwrap();
		assert!(first.has_more, "four hundred rows cannot fit in one tier chunk, so the scan must continue");
		assert_ne!(
			cursor.persistent.stop(),
			Some(&RangeStop::AbsentTable),
			"the persistent tier must hold a table here, or this is the empty-database case again"
		);
		assert!(
			cursor.persistent.last_key().is_some(),
			"the persistent tier contributed nothing, so the scan is not merging two live tiers"
		);

		flush(&store, CommitVersion(2));

		let mut seen = rows_of(first.items.iter().map(|row| row.key.clone()).collect());
		seen.extend(drain_reverse(&store, &mut cursor, 2));
		seen.sort_unstable();
		seen.dedup();

		assert_eq!(
			seen,
			(1..=PERSISTED + BUFFERED).collect::<Vec<_>>(),
			"the reverse merge dropped rows the flush moved from the buffer into a tier the scan had already read past"
		);
	}

	#[test]
	fn a_scan_that_asked_persistent_before_its_table_existed_reads_it_again_once_the_buffer_drains() {
		let (store, _g) = store_without_read_tier();
		for n in 1..=ROWS {
			commit_row(&store, n, 1);
		}

		let mut cursor = MultiVersionRangeCursor::new();
		let first = store
			.range_next(
				&mut cursor,
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(1),
				},
				2,
			)
			.unwrap();
		assert!(first.has_more, "two hundred rows cannot fit in one tier chunk, so the scan must continue");
		assert_eq!(
			cursor.persistent.stop(),
			Some(&RangeStop::AbsentTable),
			"nothing is flushed yet, so this test is not exercising the interleaving it exists for"
		);

		flush(&store, CommitVersion(1));

		let mut seen = rows_of(first.items.iter().map(|row| row.key.clone()).collect());
		seen.extend(drain_forward(&store, &mut cursor, 1));
		seen.sort_unstable();
		seen.dedup();

		assert_eq!(
			seen,
			(1..=ROWS).collect::<Vec<_>>(),
			"the scan dropped rows the flush moved out from under it"
		);
	}

	#[test]
	fn a_reverse_scan_that_asked_persistent_before_its_table_existed_reads_it_again_too() {
		let (store, _g) = store_without_read_tier();
		for n in 1..=ROWS {
			commit_row(&store, n, 1);
		}

		let mut cursor = MultiVersionRangeCursor::new();
		let first = store
			.range_rev_next(
				&mut cursor,
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(1),
				},
				2,
			)
			.unwrap();
		assert!(first.has_more, "two hundred rows cannot fit in one tier chunk, so the scan must continue");
		assert_eq!(
			cursor.persistent.stop(),
			Some(&RangeStop::AbsentTable),
			"nothing is flushed yet, so this test is not exercising the interleaving it exists for"
		);

		flush(&store, CommitVersion(1));

		let mut seen = rows_of(first.items.iter().map(|row| row.key.clone()).collect());
		seen.extend(drain_reverse(&store, &mut cursor, 1));
		seen.sort_unstable();
		seen.dedup();

		assert_eq!(
			seen,
			(1..=ROWS).collect::<Vec<_>>(),
			"the reverse scan dropped rows the flush moved out from under it"
		);
	}
}

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod probe_tests {
	use std::collections::HashMap;

	use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
	use reifydb_core::{
		common::CommitVersion,
		delta::Delta,
		event::EventBus,
		interface::{
			catalog::{id::TableId, storage::StorageId},
			store::{MultiVersionCommit, MultiVersionGet, MultiVersionGetPrevious, classify_key},
		},
		key::row::{PartitionedRowKey, RowKey},
	};
	use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock, shutdown::Shutdown};
	use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
	use reifydb_store_commit::{MultiVersionScope, TierBatch, store::CommitStore};
	use reifydb_value::{
		cow_vec,
		util::cowvec::CowVec,
		value::{partition::Partition, row_number::RowNumber},
	};

	use crate::{
		config::{CommitStoreConfig, MultiStoreConfig, PersistentConfig},
		store::StandardMultiStore,
		tier::{
			persistent::sqlite::storage::SqlitePersistentStorage, point::MultiPointConfig,
			range::MultiRangeConfig,
		},
	};

	const STORAGE: StorageId = StorageId::Table(TableId(1));

	fn probes(store: &StandardMultiStore) -> (u64, u64) {
		let m = store.persistent_probe_metrics().expect("persistent tier configured");
		(m.persistent_probes.as_u64(), m.persistent_absent.as_u64())
	}

	fn seed_persistent(store: &StandardMultiStore, entries: Vec<(EncodedKey, Option<CowVec<u8>>)>) {
		let mut batch: TierBatch = HashMap::new();
		for (key, value) in entries {
			batch.entry(classify_key(&key)).or_default().push((key, value));
		}
		store.persistent()
			.expect("persistent tier configured")
			.persist_sweep(vec![(CommitVersion(1), batch)])
			.unwrap();
	}

	fn value(text: &str) -> Option<CowVec<u8>> {
		Some(CowVec::new(text.as_bytes().to_vec()))
	}

	fn store_over_populated_persistent() -> (StandardMultiStore, SqliteTempPathGuard) {
		let (sqlite_config, guard) = SqliteConfig::in_memory();
		{
			let storage = SqlitePersistentStorage::new(sqlite_config.clone());
			let seeded = RowKey::encoded(STORAGE, 999);
			let mut batch: TierBatch = HashMap::new();
			batch.insert(classify_key(&seeded), vec![(seeded, value("preexisting"))]);
			storage.set_collecting_accepted(CommitVersion(1), batch).unwrap();
			storage.shutdown();
		}

		let clock = Clock::testing();
		let actor_system = ActorSystem::testing(clock.clone());
		let spawner = actor_system.spawner();
		let event_bus = EventBus::new(&spawner);
		let store = StandardMultiStore::new(MultiStoreConfig {
			commit: CommitStoreConfig {
				storage: CommitStore::new(),
			},
			persistent: Some(PersistentConfig::sqlite(sqlite_config)),
			point: Some(MultiPointConfig::testing()),
			range: Some(MultiRangeConfig::testing()),
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus,
			spawner,
			clock,
		})
		.unwrap();

		assert!(
			!store.persistent().expect("persistent tier configured").filter().metrics().enabled,
			"the filter came up armed over a populated database, so every lookup below is ruled out \
			 before it reaches sqlite and the probe counters measure nothing"
		);
		(store, guard)
	}

	#[test]
	fn a_read_the_commit_buffer_answers_never_counts_a_persistent_probe() {
		let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		let present = RowKey::encoded(STORAGE, 1);
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: present.clone(),
				bytes: EncodedBytes(CowVec::new(b"v1".to_vec())),
			}],
			CommitVersion(2),
		)
		.unwrap();
		let removed = RowKey::encoded(STORAGE, 2);
		MultiVersionCommit::commit(&store, cow_vec![Delta::remove_silent(removed.clone())], CommitVersion(3))
			.unwrap();

		let before = probes(&store);
		assert!(store.get(&present, CommitVersion(9)).unwrap().is_some());
		assert!(store.get(&removed, CommitVersion(9)).unwrap().is_none());

		assert_eq!(
			probes(&store),
			before,
			"the commit buffer answered both reads, including the tombstoned one. Counting them as \
			 persistent probes inflates the denominator with lookups no filter could ever have \
			 skipped, so the measured absent rate reads lower than the real ceiling"
		);
	}

	#[test]
	fn a_persistent_read_that_finds_a_row_counts_a_probe_but_no_absence() {
		let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		let k = RowKey::encoded(STORAGE, 1);
		seed_persistent(&store, vec![(k.clone(), value("resident"))]);

		let before = probes(&store);
		assert!(store.get(&k, CommitVersion(9)).unwrap().is_some());
		assert_eq!(
			probes(&store),
			(before.0 + 1, before.1),
			"a hit must raise the probe count and leave the absent count alone, otherwise the ratio \
			 claims a filter could skip reads that genuinely returned data"
		);
	}

	#[test]
	fn a_persistent_read_that_finds_nothing_counts_a_probe_and_an_absence() {
		let (store, _guard) = store_over_populated_persistent();

		let k = RowKey::encoded(STORAGE, 77);

		let before = probes(&store);
		assert!(store.get(&k, CommitVersion(9)).unwrap().is_none());
		assert_eq!(
			probes(&store),
			(before.0 + 1, before.1 + 1),
			"an absent point read must raise both counts, otherwise the ceiling on what a filter \
			 could save measures as zero and the decision is made on a number that cannot move"
		);
	}

	#[test]
	fn a_deleted_key_counts_a_probe_and_an_absence() {
		let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		let k = RowKey::encoded(STORAGE, 5);
		seed_persistent(&store, vec![(k.clone(), value("doomed"))]);
		seed_persistent(&store, vec![(k.clone(), None)]);

		let before = probes(&store);
		assert!(store.get(&k, CommitVersion(9)).unwrap().is_none(), "a deleted key reads as no row");
		assert_eq!(
			probes(&store),
			(before.0 + 1, before.1 + 1),
			"the delete removed the row, so sqlite came back with nothing and the read was wasted. \
			 Counting it as a hit would deny a filter a saving it can actually make"
		);
	}

	#[test]
	fn a_batched_read_counts_one_probe_per_key_that_reached_the_persistent_tier() {
		let (store, _guard) = store_over_populated_persistent();

		let resident = RowKey::encoded(STORAGE, 1);
		let deleted = RowKey::encoded(STORAGE, 2);
		let missing_a = RowKey::encoded(STORAGE, 3);
		let missing_b = RowKey::encoded(STORAGE, 4);
		let buffered = RowKey::encoded(STORAGE, 5);
		seed_persistent(
			&store,
			vec![(resident.clone(), value("resident")), (deleted.clone(), value("doomed"))],
		);
		seed_persistent(&store, vec![(deleted.clone(), None)]);
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: buffered.clone(),
				bytes: EncodedBytes(CowVec::new(b"buffered".to_vec())),
			}],
			CommitVersion(2),
		)
		.unwrap();

		let before = probes(&store);
		let found = store
			.get_many(
				&[
					resident.clone(),
					deleted.clone(),
					missing_a.clone(),
					missing_b.clone(),
					buffered.clone(),
				],
				CommitVersion(9),
			)
			.unwrap();
		assert_eq!(found.len(), 2, "only the resident and the buffered key carry a row");

		assert_eq!(
			probes(&store),
			(before.0 + 4, before.1 + 3),
			"four of the five keys fell through to sqlite and three of those came back with nothing. \
			 The buffered key must not count at all, and the deleted key must count as a wasted probe"
		);
	}

	#[test]
	fn a_previous_version_read_that_reaches_persistent_is_counted() {
		let (store, _guard) = store_over_populated_persistent();

		let k = RowKey::encoded(STORAGE, 42);

		let before = probes(&store);
		assert!(store.get_previous_version(&k, CommitVersion(9)).unwrap().is_none());
		assert_eq!(
			probes(&store),
			(before.0 + 1, before.1 + 1),
			"an absent previous-version probe is exactly the sqlite read a filter would skip"
		);
	}

	#[test]
	fn a_store_without_a_persistent_tier_reports_no_probe_metrics() {
		let store = StandardMultiStore::testing_memory();
		assert!(store.get(&RowKey::encoded(STORAGE, 1), CommitVersion(9)).unwrap().is_none());
		assert!(store.persistent_probe_metrics().is_none());
	}

	#[test]
	fn a_paginated_range_over_many_partitions_reaches_every_row() {
		let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		let mut entries = Vec::new();
		for p in 0u128..64 {
			let partition = Partition(p.wrapping_mul(0x9E3779B97F4A7C15) ^ 0xA5A5_A5A5_A5A5_A5A5);
			for r in 0u64..2 {
				let key = PartitionedRowKey::encoded(STORAGE, partition, RowNumber(r + 1));
				entries.push((key, value("v")));
			}
		}
		seed_persistent(&store, entries);

		let range = PartitionedRowKey::full_scan(STORAGE);
		let collected: Vec<_> = store
			.range(
				range,
				MultiVersionScope::AsOf {
					read: CommitVersion(9),
				},
				4,
			)
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(collected.len(), 128, "a paginated scan across 64 partitions must reach every row");
	}
}
