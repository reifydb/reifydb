// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, btree_map::Entry},
	ops::{Bound, RangeBounds},
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
		MultiVersionGetPrevious, MultiVersionRow, MultiVersionStore, classify_key, classify_range,
	},
};
use reifydb_store::row::page::PageId;
use reifydb_value::{
	reifydb_assertions,
	util::{cowvec::CowVec, hex},
};
use tracing::instrument;

use super::StandardMultiStore;
use crate::{
	MultiVersionScope, Result,
	tier::{
		DisplacedValues, RangeBatch, RangeCursor, TierBatch, TierStorage, VersionedGetResult,
		commit::buffer::MultiCommitBufferTier,
		persistent::MultiPersistentTier,
		read::{MultiReadBufferTier, ServedChunk},
	},
};

const TIER_SCAN_CHUNK_SIZE: usize = 32;

pub(crate) const WARM_THRESHOLD: u64 = 4 * TIER_SCAN_CHUNK_SIZE as u64;

impl MultiVersionGet for StandardMultiStore {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		match classify_key(key) {
			EntryKind::Source(_) => self.get_source(key, version),
			_ => self.get_multi(key, version),
		}
	}
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::get::source", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_source(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		self.get_impl(key, version)
	}

	#[instrument(name = "store::multi::get::multi", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_multi(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		self.get_impl(key, version)
	}

	#[inline]
	fn get_impl(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		let table = classify_key(key);

		if let Some(found) = self.get_probe_commit(table, key, version)? {
			return Ok(found);
		}
		if let Some(found) = self.get_probe_read(key, version) {
			return Ok(found);
		}
		if let Some(found) = self.get_probe_persistent(table, key, version)? {
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
	fn get_probe_read(&self, key: &EncodedKey, version: CommitVersion) -> Option<Option<MultiVersionRow>> {
		let read = self.read.as_ref()?;
		match read.get(key, version) {
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
		key: &EncodedKey,
		version: CommitVersion,
	) -> Result<Option<Option<MultiVersionRow>>> {
		let Some(persistent) = &self.persistent else {
			return Ok(None);
		};
		Ok(match persistent.get(table, key.as_ref(), version)? {
			VersionedGetResult::Value {
				value,
				version: v,
			} => {
				if let Some(read) = &self.read {
					read.insert(key.clone(), v, Some(value.clone()));
				}
				Some(Some(MultiVersionRow {
					key: key.clone(),
					bytes: EncodedBytes(value),
					version: v,
				}))
			}
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
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

		let displaced = self.write_batches(version, classified.batches)?;

		self.emit_commit_metrics(classified.writes, classified.deletes, displaced, version);

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
					value_bytes: 0,
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
		let mut by_table: HashMap<EntryKind, Vec<&EncodedKey>> = HashMap::new();
		for key in keys {
			by_table.entry(classify_key(key)).or_default().push(key);
		}

		let mut out: HashMap<EncodedKey, MultiVersionRow> = HashMap::new();
		for (table, table_keys) in by_table {
			self.get_many_for_table(table, &table_keys, version, &mut out)?;
		}

		Ok(out)
	}

	#[inline]
	fn get_many_for_table(
		&self,
		table: EntryKind,
		table_keys: &[&EncodedKey],
		version: CommitVersion,
		out: &mut HashMap<EncodedKey, MultiVersionRow>,
	) -> Result<()> {
		let key_slices: Vec<&[u8]> = table_keys.iter().map(|k| k.as_ref()).collect();

		let commit_results = self.probe_commit_batch(table, &key_slices, version)?;
		let (read_aligned, persistent_aligned) = self.resolve_misses_through_read_and_persistent(
			table,
			table_keys,
			&key_slices,
			&commit_results,
			version,
		)?;

		reifydb_assertions! {
			let n = key_slices.len();
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
		table_keys: &[&EncodedKey],
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
				.read
				.as_ref()
				.map(|c| c.get(table_keys[i], version))
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
					persistent_idx.push(i);
					persistent_slices.push(key_slices[i]);
				}
			}
		}

		let mut persistent_aligned = vec![VersionedGetResult::NotFound; key_slices.len()];
		if !persistent_slices.is_empty()
			&& let Some(persistent) = &self.persistent
		{
			let persistent_results = persistent.get_many(table, &persistent_slices, version)?;
			for (slot, result) in persistent_idx.into_iter().zip(persistent_results) {
				if let (
					Some(read),
					VersionedGetResult::Value {
						value,
						version: v,
					},
				) = (&self.read, &result)
				{
					read.insert(table_keys[slot].clone(), *v, Some(value.clone()));
				}
				persistent_aligned[slot] = result;
			}
		}

		Ok((read_aligned, persistent_aligned))
	}

	#[inline]
	fn collect_resolved_rows(
		&self,
		table_keys: &[&EncodedKey],
		commit_results: &[VersionedGetResult],
		read_aligned: &[VersionedGetResult],
		persistent_aligned: &[VersionedGetResult],
		out: &mut HashMap<EncodedKey, MultiVersionRow>,
	) {
		for (i, key) in table_keys.iter().enumerate() {
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
					(*key).clone(),
					MultiVersionRow {
						key: (*key).clone(),
						bytes: EncodedBytes(value),
						version: v,
					},
				);
			}
		}
	}

	#[inline]
	fn update_read_cache_on_commit(&self, batches: &TierBatch) {
		let Some(read) = &self.read else {
			return;
		};
		for entries in batches.values() {
			for (key, _) in entries {
				read.invalidate(key);
			}
		}
	}

	#[inline]
	fn write_batches(&self, version: CommitVersion, batches: TierBatch) -> Result<DisplacedValues> {
		self.commit.set(version, batches)
	}

	#[inline]
	fn emit_commit_metrics(
		&self,
		writes: Vec<MultiWrite>,
		mut deletes: Vec<MultiDelete>,
		displaced: DisplacedValues,
		version: CommitVersion,
	) {
		if writes.is_empty() && deletes.is_empty() {
			return;
		}
		if !deletes.is_empty() {
			let displaced: HashMap<&EncodedKey, u64> = displaced.iter().map(|(k, b)| (k, *b)).collect();
			for delete in deletes.iter_mut() {
				delete.value_bytes = displaced.get(&delete.key).copied().unwrap_or(0);
			}
		}
		self.event_bus.emit(MultiCommittedEvent::new(writes, deletes, version));
	}
}

#[derive(Debug, Clone, Default)]
pub struct MultiVersionRangeCursor {
	pub commit: RangeCursor,

	pub persistent: RangeCursor,

	pub exhausted: bool,

	warm: bool,

	warm_bucket: Option<PageId>,

	warm_consumed: u64,
}

impl MultiVersionRangeCursor {
	pub fn new() -> Self {
		Self {
			warm: true,
			..Default::default()
		}
	}

	pub fn cold() -> Self {
		Self {
			warm: false,
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
	buffer: Option<&MultiCommitBufferTier>,
	buffer_cursor: &mut RangeCursor,
	persistent: Option<&MultiPersistentTier>,
	persistent_cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<()> {
	if let Some(s) = buffer
		&& !buffer_cursor.exhausted
	{
		scan_tier_chunk(s, buffer_cursor, scan, collected)?;
	}
	if let Some(s) = persistent
		&& !persistent_cursor.exhausted
	{
		scan_tier_chunk(s, persistent_cursor, scan, collected)?;
	}
	Ok(())
}

pub fn scan_tiers_latest(
	buffer: Option<&MultiCommitBufferTier>,
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
	let mut buffer_cursor = RangeCursor {
		exhausted: buffer.is_none(),
		..Default::default()
	};
	let mut persistent_cursor = RangeCursor {
		exhausted: persistent.is_none(),
		..Default::default()
	};
	let mut exhausted = false;

	while collected.len() < max_keys {
		step_all_tiers(buffer, &mut buffer_cursor, persistent, &mut persistent_cursor, &scan, &mut collected)?;
		if buffer_cursor.exhausted && persistent_cursor.exhausted {
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
			if !cursor.commit.exhausted {
				scan_tier_chunk(&self.commit, &mut cursor.commit, &scan, &mut collected)?;
			}

			if self.persistent.is_some() && !cursor.persistent.exhausted {
				self.step_persistent_cached(&scan, cursor, &mut collected, false)?;
			}

			if cursor.commit.exhausted && cursor.persistent.exhausted {
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
			if !cursor.commit.exhausted {
				scan_tier_chunk_rev(&self.commit, &mut cursor.commit, &scan, &mut collected)?;
			}

			if self.persistent.is_some() && !cursor.persistent.exhausted {
				self.step_persistent_cached(&scan, cursor, &mut collected, true)?;
			}

			if cursor.commit.exhausted && cursor.persistent.exhausted {
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

		if let Some(served) = self.serve_from_read_cache(scan, cursor, collected, descending) {
			return served;
		}

		let consumed = self.scan_persistent_chunk(persistent, scan, cursor, collected, descending)?;
		self.warm_read_bucket_after_scan(persistent, scan, cursor, consumed)?;

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
		let (Some(read), EntryKind::Source(_)) = (&self.read, scan.table) else {
			return None;
		};
		match read.serve_persistent_chunk(
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
	) -> Result<usize> {
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
		let consumed = batch.entries.len();
		merge_tier_batch(batch, scan.range, collected)?;
		Ok(consumed)
	}

	#[inline]
	fn warm_read_bucket_after_scan(
		&self,
		persistent: &MultiPersistentTier,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		consumed: usize,
	) -> Result<()> {
		if !cursor.warm {
			return Ok(());
		}
		if let (Some(read), EntryKind::Source(_)) = (&self.read, scan.table) {
			maybe_warm_bucket(read, persistent, cursor, scan.table, consumed)?;
		}
		Ok(())
	}
}

fn maybe_warm_bucket(
	read: &MultiReadBufferTier,
	persistent: &MultiPersistentTier,
	cursor: &mut MultiVersionRangeCursor,
	table: EntryKind,
	consumed: usize,
) -> Result<()> {
	let page = {
		let Some(last) = cursor.persistent.last_key.as_ref() else {
			return Ok(());
		};
		read.page_of_key(last)
	};
	if !matches!(page.kind, EntryKind::Source(_)) {
		return Ok(());
	}

	if cursor.warm_bucket == Some(page) {
		cursor.warm_consumed = cursor.warm_consumed.saturating_add(consumed as u64);
	} else {
		cursor.warm_bucket = Some(page);
		cursor.warm_consumed = consumed as u64;
	}

	if cursor.warm_consumed <= WARM_THRESHOLD {
		return Ok(());
	}

	let settle = |cursor: &mut MultiVersionRangeCursor| {
		cursor.warm_bucket = None;
		cursor.warm_consumed = 0;
	};

	if read.page_is_complete(page) {
		settle(cursor);
		return Ok(());
	}

	let Some(range) = read.page_key_range(page) else {
		return Ok(());
	};
	let (Bound::Included(lo), Bound::Included(hi)) = (range.start, range.end) else {
		return Ok(());
	};

	if !read.begin_warm(page) {
		settle(cursor);
		return Ok(());
	}

	let loaded = persistent.load_range_consistent(
		table,
		Bound::Included(lo.as_slice()),
		Bound::Included(hi.as_slice()),
		CommitVersion(u64::MAX),
		None,
	);
	let entries = match loaded {
		Ok(entries) => entries,
		Err(e) => {
			read.abort_warm(page);
			settle(cursor);
			return Err(e);
		}
	};

	read.finish_warm(page, entries);
	settle(cursor);
	Ok(())
}

fn mark_unconfigured_exhausted(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor) {
	if store.persistent.is_none() {
		cursor.persistent.exhausted = true;
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

fn rewind_over_advanced_forward(cursor: &mut MultiVersionRangeCursor, horizon: &EncodedKey) {
	for tier in [&mut cursor.commit, &mut cursor.persistent] {
		if let Some(last) = &tier.last_key
			&& last.as_slice() > horizon.as_slice()
		{
			tier.last_key = Some(horizon.clone());
			tier.exhausted = false;
		}
	}
}

fn rewind_over_advanced_reverse(cursor: &mut MultiVersionRangeCursor, horizon: &EncodedKey) {
	for tier in [&mut cursor.commit, &mut cursor.persistent] {
		if let Some(last) = &tier.last_key
			&& last.as_slice() < horizon.as_slice()
		{
			tier.last_key = Some(horizon.clone());
			tier.exhausted = false;
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

		let table = classify_key(key);
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
		if let Some(found) = self.previous_probe_read(key, prev_version) {
			return Ok(found);
		}
		if let Some(found) = self.previous_probe_persistent(table, key, prev_version)? {
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
		key: &EncodedKey,
		prev_version: CommitVersion,
	) -> Option<Option<MultiVersionRow>> {
		let read = self.read.as_ref()?;
		match read.get(key, prev_version) {
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
		key: &EncodedKey,
		prev_version: CommitVersion,
	) -> Result<Option<Option<MultiVersionRow>>> {
		let Some(persistent) = &self.persistent else {
			return Ok(None);
		};
		Ok(match persistent.get(table, key.as_ref(), prev_version)? {
			VersionedGetResult::Value {
				value,
				version,
			} => {
				if let Some(read) = &self.read {
					read.insert(key.clone(), version, Some(value.clone()));
				}
				Some(Some(MultiVersionRow {
					key: key.clone(),
					bytes: EncodedBytes(CowVec::new(value.to_vec())),
					version,
				}))
			}
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
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
			store::{EntryKind, MultiVersionCommit, MultiVersionGet},
		},
		key::{
			EncodableKey,
			operator_state::{GroupId, Keyspace, OperatorStateKey},
			row::RowKey,
		},
	};
	use reifydb_value::{cow_vec, util::cowvec::CowVec};

	use crate::{
		MultiVersionScope,
		store::{StandardMultiStore, multi::WARM_THRESHOLD},
		tier::{RawEntry, TierStorage, VersionedGetResult, commit::buffer::MultiCommitBufferTier},
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
			let (to_persist, to_compact, _more) = match commit {
				MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff, usize::MAX),
			};
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
				store.invalidate_read_key(&evicted.key);
			}
			commit.compact(HashMap::from([(
				kind,
				to_compact.into_iter().map(|e| (e.key, e.version)).collect(),
			)]))
			.unwrap();
		}
	}

	#[test]
	fn warm_threshold_warms_only_buckets_above_threshold() {
		const HEAVY: u64 = WARM_THRESHOLD + 64;
		const LIGHT: u64 = 20;
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		for n in 1..=HEAVY {
			commit_row(&store, n, 1);
		}
		for n in 0..LIGHT {
			commit_row(&store, (1u64 << 16) + n, 1);
		}
		flush(&store, CommitVersion(1));

		let read = store.read.clone().expect("read tier configured");
		let heavy_bucket = read.page_of_key(&RowKey::encoded(STORAGE, 1));
		let light_bucket = read.page_of_key(&RowKey::encoded(STORAGE, 1u64 << 16));
		assert_ne!(heavy_bucket, light_bucket, "the two row groups must land in different buckets");
		assert!(!read.page_is_complete(heavy_bucket), "nothing is warm before the scan");

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
		assert_eq!(scanned.len() as u64, HEAVY + LIGHT, "the scan returns every row regardless of warming");

		assert!(read.page_is_complete(heavy_bucket), "a bucket scanned past the threshold must be warmed");
		assert!(
			!read.page_is_complete(light_bucket),
			"a bucket scanned below the threshold must not be warmed"
		);
	}

	#[test]
	fn operator_state_commit_does_not_populate_the_read_tier() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let read = store.read.clone().expect("read tier configured");

		let opkey =
			OperatorStateKey::new(OperatorId(7), GroupId::ROOT, Keyspace::CUSTOM_NOT_CACHED, vec![1, 2, 3])
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
			matches!(read.get(&opkey, CommitVersion(10)), VersionedGetResult::NotFound),
			"an operator commit must not write through into the read tier"
		);
		assert_eq!(read.resident_pages(), 0, "no operator page may become resident on commit");

		let row = MultiVersionGet::get(&store, &opkey, CommitVersion(10))
			.unwrap()
			.expect("the committed operator state must still be readable through the store");
		assert_eq!(row.bytes.as_slice(), b"state-v10");
		assert_eq!(row.version, CommitVersion(10));

		assert!(
			matches!(read.get(&opkey, CommitVersion(10)), VersionedGetResult::NotFound),
			"a store-level operator read must not back-populate the read tier"
		);
	}

	#[test]
	fn source_row_write_clears_range_complete_on_its_page() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let read = store.read.clone().expect("read tier configured");

		let neighbor = RowKey::encoded(STORAGE, 1);
		let page = read.page_of_key(&neighbor);
		assert_eq!(
			read.page_of_key(&RowKey::encoded(STORAGE, 2)),
			page,
			"both source rows must share a page for this test to exercise flag-clearing"
		);
		read.populate_page(
			page,
			vec![RawEntry {
				key: neighbor,
				version: CommitVersion(1),
				value: Some(CowVec::new(b"neighbor".to_vec())),
			}],
			true,
		);
		assert!(read.page_is_complete(page), "the page must start range-complete");

		commit_row(&store, 2, 5);

		assert!(
			!read.page_is_complete(page),
			"writing a source row into a range-complete page must clear the flag so the range cache re-warms"
		);
	}

	#[test]
	fn source_warm_does_not_publish_a_page_another_warm_has_claimed() {
		const HEAVY: u64 = WARM_THRESHOLD + 64;
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		for n in 1..=HEAVY {
			commit_row(&store, n, 1);
		}
		flush(&store, CommitVersion(1));

		let read = store.read.clone().expect("read tier configured");
		let page = read.page_of_key(&RowKey::encoded(STORAGE, 1));
		assert!(!read.page_is_complete(page), "nothing is warm before the scan");

		assert!(read.begin_warm(page), "the page is unclaimed, so this claim must succeed");

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
		assert_eq!(scanned.len() as u64, HEAVY, "the scan still returns every row");

		assert!(
			!read.page_is_complete(page),
			"a source range scan published a page that another warm had claimed. The operator warm path \
			 claims with begin_warm and publishes with finish_warm, which refuses a claim that a \
			 concurrent drop has dirtied; the source path claims nothing and publishes with \
			 populate_page, which sets range_complete unconditionally. So a drop landing during a source \
			 warm cannot invalidate it, and the stale pre-drop snapshot is republished as authoritative - \
			 resurrecting the dropped row in both point reads and range scans, permanently, because the \
			 persistent tier no longer holds anything to contradict the cache"
		);
	}

	#[test]
	fn source_warm_releases_its_claim_when_it_publishes() {
		const HEAVY: u64 = WARM_THRESHOLD + 64;
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();

		for n in 1..=HEAVY {
			commit_row(&store, n, 1);
		}
		flush(&store, CommitVersion(1));

		let read = store.read.clone().expect("read tier configured");
		let page = read.page_of_key(&RowKey::encoded(STORAGE, 1));

		let _ = store
			.range(
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(10),
				},
				32,
			)
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert!(read.page_is_complete(page), "a bucket scanned past the threshold must be warmed");

		assert!(
			read.begin_warm(page),
			"the source warm did not hand its claim back. Publishing through finish_warm consumes the \
			 claim; publishing through populate_page leaves it stranded in shard.warming, and then every \
			 later begin_warm on this page is refused - so once the page is invalidated it can never warm \
			 again for the life of the process"
		);
	}
}
