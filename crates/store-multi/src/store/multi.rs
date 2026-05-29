// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
use reifydb_value::util::{cowvec::CowVec, hex};
use tracing::{instrument, warn};

use super::{
	StandardMultiStore,
	router::{classify_key, classify_range, is_single_version_semantics_key},
};
use crate::{
	MultiVersionScope, Result,
	tier::{
		RangeBatch, RangeCursor, TierBatch, TierStorage, VersionedGetResult,
		commit::buffer::MultiCommitBufferTier, persistent::MultiPersistentTier,
	},
};

const TIER_SCAN_CHUNK_SIZE: usize = 32;

impl MultiVersionGet for StandardMultiStore {
	#[instrument(name = "store::multi::get", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref()), version = version.0))]
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		let table = classify_key(key);

		if let Some(commit) = &self.commit {
			match commit.get(table, key.as_ref(), version)? {
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

		if let Some(read) = &self.read {
			match read.get(key, version) {
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

		if let Some(persistent) = &self.persistent {
			match persistent.get(table, key.as_ref(), version)? {
				VersionedGetResult::Value {
					value,
					version: v,
				} => {
					if let Some(read) = &self.read {
						read.insert(key.clone(), v, Some(value.clone()));
					}
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
		let classified = classify_deltas(&deltas);

		let (operator_drops, source_drops): (Vec<_>, Vec<_>) = classified
			.explicit_drops
			.into_iter()
			.partition(|(table, _)| matches!(table, EntryKind::Operator(_)));

		let drop_batch = build_drop_batch(source_drops, &classified.pending_set_keys, version);
		self.dispatch_drops(drop_batch);

		if let Some(read) = &self.read {
			for write in &classified.writes {
				read.invalidate(&write.key);
			}
			for delete in &classified.deletes {
				read.invalidate(&delete.key);
			}
		}

		if let Some(commit) = &self.commit {
			commit.set(version, classified.batches)?;
		} else if let Some(persistent) = &self.persistent {
			persistent.set(version, classified.batches)?;
		} else {
			return Ok(());
		}

		self.evict_operator_state(&operator_drops)?;

		self.emit_commit_metrics(classified.writes, classified.deletes, version);

		Ok(())
	}
}

struct ClassifiedDeltas {
	pending_set_keys: HashSet<EncodedKey>,
	writes: Vec<MultiWrite>,
	deletes: Vec<MultiDelete>,
	batches: TierBatch,
	explicit_drops: Vec<(EntryKind, EncodedKey)>,
}

#[inline]
fn classify_deltas(deltas: &CowVec<Delta>) -> ClassifiedDeltas {
	let mut pending_set_keys: HashSet<EncodedKey> = HashSet::new();
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
					pending_set_keys.insert(key.clone());
				}
				writes.push(MultiWrite {
					key: key.clone(),
					value_bytes: row.len() as u64,
				});
				batches.entry(table).or_default().push((key.clone(), Some(row.0.clone())));
			}
			Delta::Unset {
				key,
				row,
			} => {
				deletes.push(MultiDelete {
					key: key.clone(),
					value_bytes: row.len() as u64,
				});
				batches.entry(table).or_default().push((key.clone(), None));
			}
			Delta::Remove {
				key,
			} => {
				deletes.push(MultiDelete {
					key: key.clone(),
					value_bytes: 0,
				});
				batches.entry(table).or_default().push((key.clone(), None));
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

#[inline]
fn build_drop_batch(
	explicit_drops: Vec<(EntryKind, EncodedKey)>,
	pending_set_keys: &HashSet<EncodedKey>,
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
			key,
			commit_version: version,
			pending_version,
		});
	}
	for key in pending_set_keys.iter() {
		let encoded = EncodedKey::new(key.to_vec());
		let table = classify_key(&encoded);
		drop_batch.push(DropRequest {
			table,
			key: encoded,
			commit_version: version,
			pending_version: Some(version),
		});
	}
	drop_batch
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
			let key_slices: Vec<&[u8]> = table_keys.iter().map(|k| k.as_ref()).collect();

			let commit_results = match &self.commit {
				Some(commit) => commit.get_many(table, &key_slices, version)?,
				None => vec![VersionedGetResult::NotFound; key_slices.len()],
			};

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

			for (i, key) in table_keys.into_iter().enumerate() {
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
						key.clone(),
						MultiVersionRow {
							key: key.clone(),
							row: EncodedRow(value),
							version: v,
						},
					);
				}
			}
		}

		Ok(out)
	}

	#[inline]
	fn dispatch_drops(&self, drop_batch: Vec<DropRequest>) {
		if drop_batch.is_empty() {
			return;
		}
		if let Some(actor) = &self.drop_actor
			&& actor.send_blocking(DropMessage::Batch(drop_batch)).is_err()
		{
			warn!("Failed to send drop batch");
		}
	}

	fn evict_operator_state(&self, drops: &[(EntryKind, EncodedKey)]) -> Result<()> {
		if drops.is_empty() {
			return Ok(());
		}

		if let Some(read) = &self.read {
			for (_, key) in drops {
				read.invalidate(key);
			}
		}

		if let Some(commit) = &self.commit {
			let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
			for (table, key) in drops {
				for (entry_version, _) in commit.get_all_versions(*table, key.as_ref())? {
					batches.entry(*table).or_default().push((key.clone(), entry_version));
				}
			}
			if !batches.is_empty() {
				commit.drop(batches)?;
			}
		}

		if let Some(persistent) = &self.persistent {
			let mut by_table: HashMap<EntryKind, Vec<EncodedKey>> = HashMap::new();
			for (table, key) in drops {
				by_table.entry(*table).or_default().push(key.clone());
			}
			for (table, keys) in by_table {
				persistent.delete_keys(table, &keys)?;
			}
		}

		Ok(())
	}

	#[inline]
	fn emit_commit_metrics(&self, writes: Vec<MultiWrite>, deletes: Vec<MultiDelete>, version: CommitVersion) {
		if writes.is_empty() && deletes.is_empty() {
			return;
		}
		self.event_bus.emit(MultiCommittedEvent::new(writes, deletes, vec![], version));
	}
}

#[derive(Debug, Clone, Default)]
pub struct MultiVersionRangeCursor {
	pub commit: RangeCursor,

	pub persistent: RangeCursor,

	pub exhausted: bool,
}

impl MultiVersionRangeCursor {
	pub fn new() -> Self {
		Self::default()
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
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
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
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
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
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
	if batch.entries.is_empty() {
		return Ok(false);
	}

	for entry in batch.entries {
		let original_key = entry.key.as_slice().to_vec();
		let entry_version = entry.version;

		let original_key_encoded = EncodedKey::new(original_key.clone());
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
				key: EncodedKey::new(key_bytes),
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
fn step_all_tiers(
	buffer: Option<&MultiCommitBufferTier>,
	buffer_cursor: &mut RangeCursor,
	persistent: Option<&MultiPersistentTier>,
	persistent_cursor: &mut RangeCursor,
	scan: &TierScanQuery,
	collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
) -> Result<bool> {
	let mut any_progress = false;
	if let Some(s) = buffer
		&& !buffer_cursor.exhausted
	{
		any_progress |= scan_tier_chunk(s, buffer_cursor, scan, collected)?;
	}
	if let Some(s) = persistent
		&& !persistent_cursor.exhausted
	{
		any_progress |= scan_tier_chunk(s, persistent_cursor, scan, collected)?;
	}
	Ok(any_progress)
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

	let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();
	let mut buffer_cursor = RangeCursor::default();
	let mut persistent_cursor = RangeCursor::default();
	let mut exhausted = false;

	while collected.len() < max_keys {
		let progress = step_all_tiers(
			buffer,
			&mut buffer_cursor,
			persistent,
			&mut persistent_cursor,
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

		let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		while collected.len() < batch_size {
			let progress = step_all_tiers(
				self.commit.as_ref(),
				&mut cursor.commit,
				self.persistent.as_ref(),
				&mut cursor.persistent,
				&scan,
				&mut collected,
			)?;
			if !progress {
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
			current_batch: Vec::new(),
			current_index: 0,
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
			current_batch: Vec::new(),
			current_index: 0,
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

		let mut collected: BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)> = BTreeMap::new();

		while collected.len() < batch_size {
			let mut any_progress = false;

			if let Some(commit) = &self.commit
				&& !cursor.commit.exhausted
			{
				any_progress |= scan_tier_chunk_rev(commit, &mut cursor.commit, &scan, &mut collected)?;
			}

			if let Some(persistent) = &self.persistent
				&& !cursor.persistent.exhausted
			{
				any_progress |=
					scan_tier_chunk_rev(persistent, &mut cursor.persistent, &scan, &mut collected)?;
			}

			if !any_progress {
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

fn mark_unconfigured_exhausted(store: &StandardMultiStore, cursor: &mut MultiVersionRangeCursor) {
	if store.commit.is_none() {
		cursor.commit.exhausted = true;
	}
	if store.persistent.is_none() {
		cursor.persistent.exhausted = true;
	}
}

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

fn rewind_over_advanced_reverse(cursor: &mut MultiVersionRangeCursor, horizon: &EncodedKey) {
	for tier in [&mut cursor.commit, &mut cursor.persistent] {
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

		let table = classify_key(key);
		let prev_version = CommitVersion(before_version.0 - 1);

		if let Some(commit) = &self.commit {
			match commit.get(table, key.as_ref(), prev_version)? {
				VersionedGetResult::Value {
					value,
					version,
				} => {
					return Ok(Some(MultiVersionRow {
						key: key.clone(),
						row: EncodedRow(CowVec::new(value.to_vec())),
						version,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		if let Some(read) = &self.read {
			match read.get(key, prev_version) {
				VersionedGetResult::Value {
					value,
					version,
				} => {
					return Ok(Some(MultiVersionRow {
						key: key.clone(),
						row: EncodedRow(CowVec::new(value.to_vec())),
						version,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		if let Some(persistent) = &self.persistent {
			match persistent.get(table, key.as_ref(), prev_version)? {
				VersionedGetResult::Value {
					value,
					version,
				} => {
					if let Some(read) = &self.read {
						read.insert(key.clone(), version, Some(value.clone()));
					}
					return Ok(Some(MultiVersionRow {
						key: key.clone(),
						row: EncodedRow(CowVec::new(value.to_vec())),
						version,
					}));
				}
				VersionedGetResult::Tombstone => return Ok(None),
				VersionedGetResult::NotFound => {}
			}
		}

		Ok(None)
	}
}

impl MultiVersionStore for StandardMultiStore {}

pub struct MultiVersionRangeIter {
	store: StandardMultiStore,
	cursor: MultiVersionRangeCursor,
	range: EncodedKeyRange,
	scope: MultiVersionScope,
	batch_size: usize,
	current_batch: Vec<MultiVersionRow>,
	current_index: usize,
}

impl Iterator for MultiVersionRangeIter {
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.current_index < self.current_batch.len() {
			let item = self.current_batch[self.current_index].clone();
			self.current_index += 1;
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
				self.current_batch = batch.items;
				self.current_index = 0;
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
	current_batch: Vec<MultiVersionRow>,
	current_index: usize,
}

impl Iterator for MultiVersionRangeRevIter {
	type Item = Result<MultiVersionRow>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.current_index < self.current_batch.len() {
			let item = self.current_batch[self.current_index].clone();
			self.current_index += 1;
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
				self.current_batch = batch.items;
				self.current_index = 0;
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
