// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	ops::{Bound, RangeBounds},
};

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	actors::drop::{DropMessage, DropRequest},
	common::CommitVersion,
	delta::Delta,
	event::metric::{MultiCommittedEvent, MultiDelete, MultiWrite},
	interface::store::{
		EntryKind, MultiVersionBatch, MultiVersionCommit, MultiVersionContains, MultiVersionGet,
		MultiVersionGetPrevious, MultiVersionRow, MultiVersionStore, classify_key, classify_range,
		is_single_version_semantics_key,
	},
};
use reifydb_store::row::page::PageId;
use reifydb_value::{
	reifydb_assertions,
	util::{cowvec::CowVec, hex},
};
use tracing::{Span, field, instrument, warn};

use super::StandardMultiStore;
use crate::{
	MultiVersionScope, Result,
	tier::{
		RangeBatch, RangeCursor, TierBatch, TierStorage, VersionedGetResult,
		commit::buffer::MultiCommitBufferTier,
		persistent::MultiPersistentTier,
		read::{MultiReadBufferTier, ServedChunk},
	},
};

const TIER_SCAN_CHUNK_SIZE: usize = 32;

const OPERATOR_PAGE_WARM_CAP: usize = 131_072;

pub(crate) const WARM_THRESHOLD: u64 = 4 * TIER_SCAN_CHUNK_SIZE as u64;

impl MultiVersionGet for StandardMultiStore {
	fn get(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		match classify_key(key) {
			EntryKind::Operator(_) => self.get_operator(key, version),
			EntryKind::OperatorInternal(_) => self.get_operator_internal(key, version),
			EntryKind::Source(_) => self.get_source(key, version),
			_ => self.get_multi(key, version),
		}
	}
}

impl StandardMultiStore {
	#[instrument(name = "store::multi::get::operator", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_operator(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		self.get_impl(key, version)
	}

	#[instrument(name = "store::multi::get::operator_internal", level = "trace", skip(self, key), fields(version = version.0))]
	fn get_operator_internal(&self, key: &EncodedKey, version: CommitVersion) -> Result<Option<MultiVersionRow>> {
		self.get_impl(key, version)
	}

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
			return Ok(self.unmask_dropped(key, found, version));
		}
		if matches!(table, EntryKind::Operator(_) | EntryKind::OperatorInternal(_))
			&& let Some(read) = &self.read
			&& self.warm_operator_page(read.page_of_key(key))?
			&& let Some(found) = self.get_probe_read(key, version)
		{
			return Ok(self.unmask_dropped(key, found, version));
		}
		if let Some(found) = self.get_probe_persistent(table, key, version)? {
			return Ok(self.unmask_dropped(key, found, version));
		}

		Ok(None)
	}

	#[inline]
	fn unmask_dropped(
		&self,
		key: &EncodedKey,
		found: Option<MultiVersionRow>,
		read: CommitVersion,
	) -> Option<MultiVersionRow> {
		match found {
			Some(row) if self.pending_drops.masks(key, row.version, read) => None,
			other => other,
		}
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
		let Some(commit) = &self.commit else {
			return Ok(None);
		};
		Ok(match commit.get(table, key.as_ref(), version)? {
			VersionedGetResult::Value {
				value,
				version: v,
			} => Some(Some(MultiVersionRow {
				key: key.clone(),
				row: EncodedRow(value),
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
				row: EncodedRow(value),
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
					row: EncodedRow(value),
					version: v,
				}))
			}
			VersionedGetResult::Tombstone => Some(None),
			VersionedGetResult::NotFound => None,
		})
	}

	#[instrument(name = "store::multi::warm_operator", level = "debug", skip(self), fields(node = ?page.kind, outcome = field::Empty, loaded = field::Empty))]
	fn warm_operator_page(&self, page: PageId) -> Result<bool> {
		let span = Span::current();
		let (Some(read), Some(persistent)) = (&self.read, &self.persistent) else {
			span.record("outcome", "no_tiers");
			return Ok(false);
		};
		if !matches!(page.kind, EntryKind::Operator(_) | EntryKind::OperatorInternal(_)) {
			span.record("outcome", "not_operator");
			return Ok(false);
		}
		if read.page_is_complete(page) {
			span.record("outcome", "already_complete");
			return Ok(true);
		}
		if !read.page_is_warm_candidate(page) {
			span.record("outcome", "blocked");
			return Ok(false);
		}
		let Some(range) = read.page_key_range(page) else {
			span.record("outcome", "no_range");
			return Ok(false);
		};
		if !read.begin_warm(page) {
			span.record("outcome", "busy");
			return Ok(false);
		}
		let loaded = persistent.load_range_consistent(
			page.kind,
			bound_as_slice(&range.start),
			bound_as_slice(&range.end),
			CommitVersion(u64::MAX),
			Some(OPERATOR_PAGE_WARM_CAP + 1),
		);
		let entries = match loaded {
			Ok(entries) => entries,
			Err(e) => {
				read.abort_warm(page);
				span.record("outcome", "load_error");
				return Err(e);
			}
		};
		span.record("loaded", entries.len());
		if entries.len() > OPERATOR_PAGE_WARM_CAP {
			read.abort_warm(page);
			read.set_warm_blocked(page);
			span.record("outcome", "over_cap");
			return Ok(false);
		}
		if read.finish_warm(page, entries) {
			span.record("outcome", "completed");
			Ok(true)
		} else {
			span.record("outcome", "dirty_abort");
			Ok(false)
		}
	}
}

#[inline]
fn bound_as_slice(bound: &Bound<EncodedKey>) -> Bound<&[u8]> {
	match bound {
		Bound::Included(k) => Bound::Included(k.as_slice()),
		Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
		Bound::Unbounded => Bound::Unbounded,
	}
}

impl MultiVersionContains for StandardMultiStore {
	#[instrument(name = "store::multi::contains", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref()), version = version.0), ret)]
	fn contains(&self, key: &EncodedKey, version: CommitVersion) -> Result<bool> {
		Ok(MultiVersionGet::get(self, key, version)?.is_some())
	}
}

impl MultiVersionCommit for StandardMultiStore {
	#[instrument(name = "store::multi::commit", level = "debug", skip(self, deltas), fields(delta_count = deltas.len(), version = version.0, drop_count = field::Empty))]
	fn commit(&self, deltas: CowVec<Delta>, version: CommitVersion) -> Result<()> {
		let classified = classify_deltas(&deltas);

		let (evictable_drops, actor_drops) = partition_drops(classified.explicit_drops);
		Span::current().record("drop_count", evictable_drops.len() + actor_drops.len());
		self.dispatch_drops(build_drop_batch(actor_drops, &classified.pending_set_keys, version));

		self.update_read_cache_on_commit(version, &classified.batches);

		if !self.write_batches(version, classified.batches)? {
			return Ok(());
		}

		self.evict_dropped_state(&evictable_drops, version)?;
		self.emit_commit_metrics(classified.writes, classified.deletes, version);

		Ok(())
	}
}

type DropPartition = (Vec<(EntryKind, EncodedKey)>, Vec<(EntryKind, EncodedKey)>);

#[inline]
fn partition_drops(explicit_drops: Vec<(EntryKind, EncodedKey)>) -> DropPartition {
	explicit_drops.into_iter().partition(|(table, _)| !matches!(table, EntryKind::Multi))
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
		match &self.commit {
			Some(commit) => commit.get_many(table, key_slices, version),
			None => Ok(vec![VersionedGetResult::NotFound; key_slices.len()]),
		}
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
					read_aligned[i] = if self.pending_drops.masks(table_keys[i], v, version) {
						VersionedGetResult::Tombstone
					} else {
						VersionedGetResult::Value {
							value,
							version: v,
						}
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

		if matches!(table, EntryKind::Operator(_) | EntryKind::OperatorInternal(_))
			&& !persistent_idx.is_empty()
			&& let Some(read) = &self.read
		{
			let mut pages: Vec<PageId> = Vec::new();
			for &i in &persistent_idx {
				let page = read.page_of_key(table_keys[i]);
				if !pages.contains(&page) {
					pages.push(page);
				}
			}
			let mut warmed_any = false;
			for page in pages {
				warmed_any |= self.warm_operator_page(page)?;
			}
			if warmed_any {
				let mut remaining_idx = Vec::new();
				let mut remaining_slices = Vec::new();
				for &i in &persistent_idx {
					match read.get(table_keys[i], version) {
						VersionedGetResult::Value {
							value,
							version: v,
						} => {
							read_aligned[i] = if self.pending_drops.masks(
								table_keys[i],
								v,
								version,
							) {
								VersionedGetResult::Tombstone
							} else {
								VersionedGetResult::Value {
									value,
									version: v,
								}
							};
						}
						VersionedGetResult::Tombstone => {
							read_aligned[i] = VersionedGetResult::Tombstone;
						}
						VersionedGetResult::NotFound => {
							remaining_idx.push(i);
							remaining_slices.push(key_slices[i]);
						}
					}
				}
				persistent_idx = remaining_idx;
				persistent_slices = remaining_slices;
			}
		}

		let mut persistent_aligned = vec![VersionedGetResult::NotFound; key_slices.len()];
		if !persistent_slices.is_empty()
			&& let Some(persistent) = &self.persistent
		{
			let persistent_results = persistent.get_many(table, &persistent_slices, version)?;
			for (slot, result) in persistent_idx.into_iter().zip(persistent_results) {
				if let VersionedGetResult::Value {
					version: v,
					..
				} = &result && self.pending_drops.masks(table_keys[slot], *v, version)
				{
					persistent_aligned[slot] = VersionedGetResult::Tombstone;
					continue;
				}
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
						row: EncodedRow(value),
						version: v,
					},
				);
			}
		}
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

	#[inline]
	fn update_read_cache_on_commit(&self, version: CommitVersion, batches: &TierBatch) {
		let Some(read) = &self.read else {
			return;
		};
		for (table, entries) in batches {
			match table {
				EntryKind::Operator(_) | EntryKind::OperatorInternal(_) => {
					for (key, value) in entries {
						match value {
							Some(value) => {
								read.insert(key.clone(), version, Some(value.clone()))
							}
							None => read.insert(key.clone(), version, None),
						}
					}
				}
				_ => {
					for (key, _) in entries {
						read.invalidate(key);
					}
				}
			}
		}
	}

	#[inline]
	fn write_batches(&self, version: CommitVersion, batches: TierBatch) -> Result<bool> {
		if let Some(commit) = &self.commit {
			commit.set(version, batches)?;
		} else if let Some(persistent) = &self.persistent {
			persistent.set(version, batches)?;
		} else {
			return Ok(false);
		}
		Ok(true)
	}

	#[instrument(name = "store::multi::evict_drops", level = "debug", skip_all, fields(drop_count = field::Empty))]
	fn evict_dropped_state(&self, drops: &[(EntryKind, EncodedKey)], version: CommitVersion) -> Result<()> {
		if drops.is_empty() {
			return Ok(());
		}
		Span::current().record("drop_count", drops.len());

		self.record_pending_drops(drops, version);
		self.evict_drops_from_commit(drops)?;
		self.remove_drops_from_read(drops);
		if !self.nudge_drop_purge() {
			self.pending_drops.purge(self.persistent.as_ref(), self.read.as_ref());
		}

		Ok(())
	}

	#[inline]
	fn nudge_drop_purge(&self) -> bool {
		if self.persistent.is_none() {
			return true;
		}
		let Some(actor) = &self.drop_actor else {
			return false;
		};
		if actor.send_blocking(DropMessage::PurgePending).is_err() {
			warn!("Failed to nudge drop purge, purging synchronously");
			return false;
		}
		true
	}

	#[inline]
	fn record_pending_drops(&self, drops: &[(EntryKind, EncodedKey)], version: CommitVersion) {
		if self.persistent.is_none() {
			return;
		}
		for (_, key) in drops {
			self.pending_drops.record(key.clone(), version);
		}
	}

	#[inline]
	fn remove_drops_from_read(&self, drops: &[(EntryKind, EncodedKey)]) {
		let Some(read) = &self.read else {
			return;
		};
		for (_, key) in drops {
			read.remove_dropped(key);
		}
	}

	#[inline]
	fn evict_drops_from_commit(&self, drops: &[(EntryKind, EncodedKey)]) -> Result<()> {
		let Some(commit) = &self.commit else {
			return Ok(());
		};
		let mut batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		for (table, key) in drops {
			for (entry_version, _) in commit.get_all_versions(*table, key.as_ref())? {
				batches.entry(*table).or_default().push((key.clone(), entry_version));
			}
		}
		if !batches.is_empty() {
			commit.drop(batches)?;
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

	warm_bucket: Option<PageId>,

	warm_consumed: u64,
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
			let mut any_progress = false;

			if let Some(commit) = &self.commit
				&& !cursor.commit.exhausted
			{
				any_progress |= scan_tier_chunk(commit, &mut cursor.commit, &scan, &mut collected)?;
			}

			if self.persistent.is_some() && !cursor.persistent.exhausted {
				any_progress |= self.step_persistent_cached(&scan, cursor, &mut collected, false)?;
			}

			if !any_progress {
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

			if self.persistent.is_some() && !cursor.persistent.exhausted {
				any_progress |= self.step_persistent_cached(&scan, cursor, &mut collected, true)?;
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

	fn step_persistent_cached(
		&self,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
		descending: bool,
	) -> Result<bool> {
		let Some(persistent) = &self.persistent else {
			return Ok(false);
		};

		if let Some(served) = self.serve_from_read_cache(scan, cursor, collected, descending) {
			return served;
		}

		if matches!(scan.table, EntryKind::Operator(_) | EntryKind::OperatorInternal(_))
			&& let Some(read) = &self.read
			&& self.warm_operator_page(read.page_of_key(&EncodedKey::new(scan.start.to_vec())))?
			&& let Some(served) = self.serve_from_read_cache(scan, cursor, collected, descending)
		{
			return served;
		}

		let (consumed, progressed) =
			self.scan_persistent_chunk(persistent, scan, cursor, collected, descending)?;
		self.warm_read_bucket_after_scan(persistent, scan, cursor, consumed)?;

		Ok(progressed)
	}

	#[inline]
	fn serve_from_read_cache(
		&self,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
		descending: bool,
	) -> Option<Result<bool>> {
		let (Some(read), EntryKind::Source(_) | EntryKind::Operator(_) | EntryKind::OperatorInternal(_)) =
			(&self.read, scan.table)
		else {
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
			ServedChunk::Served(batch) => {
				let batch = self.mask_dropped_persistent_rows(scan, batch);
				Some(merge_tier_batch(batch, scan.range, collected))
			}
			ServedChunk::Gap => None,
		}
	}

	#[inline]
	fn scan_persistent_chunk(
		&self,
		persistent: &MultiPersistentTier,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		collected: &mut BTreeMap<Vec<u8>, (CommitVersion, Option<CowVec<u8>>)>,
		descending: bool,
	) -> Result<(usize, bool)> {
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
		let batch = self.mask_dropped_persistent_rows(scan, batch);
		let progressed = merge_tier_batch(batch, scan.range, collected)?;
		Ok((consumed, progressed))
	}

	#[inline]
	fn mask_dropped_persistent_rows(&self, scan: &TierScanQuery, mut batch: RangeBatch) -> RangeBatch {
		if matches!(scan.table, EntryKind::Multi) || self.pending_drops.is_empty() {
			return batch;
		}
		for entry in batch.entries.iter_mut() {
			if entry.value.is_some()
				&& self.pending_drops.masks(&entry.key, entry.version, scan.scope.read())
			{
				entry.value = None;
			}
		}
		batch
	}

	#[inline]
	fn warm_read_bucket_after_scan(
		&self,
		persistent: &MultiPersistentTier,
		scan: &TierScanQuery,
		cursor: &mut MultiVersionRangeCursor,
		consumed: usize,
	) -> Result<()> {
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
		let Some(commit) = &self.commit else {
			return Ok(None);
		};
		Ok(match commit.get(table, key.as_ref(), prev_version)? {
			VersionedGetResult::Value {
				value,
				version,
			} => Some(Some(MultiVersionRow {
				key: key.clone(),
				row: EncodedRow(CowVec::new(value.to_vec())),
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
				row: EncodedRow(CowVec::new(value.to_vec())),
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
					row: EncodedRow(CowVec::new(value.to_vec())),
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

#[cfg(all(test, feature = "sqlite", not(target_arch = "wasm32")))]
mod cache_tests {
	use std::collections::HashMap;

	use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
	use reifydb_core::{
		common::CommitVersion,
		delta::Delta,
		interface::{
			catalog::{flow::FlowNodeId, id::TableId, shape::ShapeId},
			store::{EntryKind, MultiVersionCommit},
		},
		key::{
			EncodableKey, flow_node_internal_state::FlowNodeInternalStateKey,
			flow_node_state::FlowNodeStateKey, row::RowKey,
		},
	};
	use reifydb_value::{cow_vec, util::cowvec::CowVec};

	use crate::{
		MultiVersionScope,
		store::{StandardMultiStore, multi::WARM_THRESHOLD},
		tier::{RawEntry, TierStorage, VersionedGetResult, commit::buffer::MultiCommitBufferTier},
	};

	const SHAPE: ShapeId = ShapeId::Table(TableId(1));

	fn commit_row(store: &StandardMultiStore, n: u64, version: u64) {
		MultiVersionCommit::commit(
			store,
			cow_vec![Delta::Set {
				key: RowKey::encoded(SHAPE, n),
				row: EncodedRow(CowVec::new(format!("v{n}").into_bytes())),
			}],
			CommitVersion(version),
		)
		.unwrap();
	}

	fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
		let commit = store.commit().expect("commit tier");
		for kind in commit.list_all_entry_kinds().unwrap() {
			let (to_persist, to_drop) = match commit {
				MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff),
			};
			if to_drop.is_empty() {
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
			for (key, _) in &to_drop {
				store.invalidate_read_key(key);
			}
			commit.drop(HashMap::from([(kind, to_drop)])).unwrap();
		}
	}

	#[test]
	fn operator_drop_fully_removes_state_leaving_no_tombstone() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let node = FlowNodeId(7);
		let table = EntryKind::Operator(node);
		let internal_table = EntryKind::OperatorInternal(node);
		let data_key = FlowNodeStateKey::encoded(node, vec![1u8]);
		let internal_key = FlowNodeInternalStateKey::encoded(node, vec![2u8]);

		for v in [1u64, 2] {
			MultiVersionCommit::commit(
				&store,
				cow_vec![Delta::Set {
					key: data_key.clone(),
					row: EncodedRow(CowVec::new(vec![v as u8])),
				}],
				CommitVersion(v),
			)
			.unwrap();
		}
		for v in [3u64, 4] {
			MultiVersionCommit::commit(
				&store,
				cow_vec![Delta::Set {
					key: internal_key.clone(),
					row: EncodedRow(CowVec::new(vec![v as u8])),
				}],
				CommitVersion(v),
			)
			.unwrap();
		}

		let commit = store.commit().expect("commit tier");
		assert!(!commit.get_all_versions(table, data_key.as_ref()).unwrap().is_empty());
		assert!(!commit.get_all_versions(internal_table, internal_key.as_ref()).unwrap().is_empty());

		MultiVersionCommit::commit(
			&store,
			cow_vec![
				Delta::Drop {
					key: data_key.clone(),
				},
				Delta::Drop {
					key: internal_key.clone(),
				}
			],
			CommitVersion(5),
		)
		.unwrap();

		assert!(
			commit.get_all_versions(table, data_key.as_ref()).unwrap().is_empty(),
			"operator data-state Drop must remove every version, not leave a tombstone"
		);
		assert!(
			commit.get_all_versions(internal_table, internal_key.as_ref()).unwrap().is_empty(),
			"operator internal-state Drop must remove every version, not leave a tombstone"
		);
	}

	#[test]
	fn operator_remove_leaves_a_tombstone_in_commit_tier() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let node = FlowNodeId(8);
		let table = EntryKind::OperatorInternal(node);
		let key = FlowNodeInternalStateKey::encoded(node, vec![9u8]);

		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: key.clone(),
				row: EncodedRow(CowVec::new(vec![1u8])),
			}],
			CommitVersion(1),
		)
		.unwrap();
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Remove {
				key: key.clone(),
			}],
			CommitVersion(2),
		)
		.unwrap();

		let commit = store.commit().expect("commit tier");
		let versions = commit.get_all_versions(table, key.as_ref()).unwrap();
		assert!(
			versions.iter().any(|(_, value)| value.is_none()),
			"Remove leaves a tombstone in the commit tier (the path Drop must avoid); versions={versions:?}"
		);
	}

	#[test]
	fn operator_state_drop_keeps_keyspace_bounded_under_churn() {
		const ROUNDS: u64 = 200;

		fn current_count(store: &StandardMultiStore, table: EntryKind) -> u64 {
			match store.commit().expect("commit tier") {
				MultiCommitBufferTier::Memory(s) => s.count_current(table).unwrap(),
			}
		}

		fn churn(evict_with_drop: bool) -> u64 {
			let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
			let node = FlowNodeId(21);
			let table = EntryKind::OperatorInternal(node);
			let key_at = |round: u64| FlowNodeInternalStateKey::encoded(node, round.to_be_bytes().to_vec());

			let mut version = 0u64;
			for round in 0..ROUNDS {
				version += 1;
				MultiVersionCommit::commit(
					&store,
					cow_vec![Delta::Set {
						key: key_at(round),
						row: EncodedRow(CowVec::new(vec![1u8])),
					}],
					CommitVersion(version),
				)
				.unwrap();

				if round > 0 {
					version += 1;
					let prev = key_at(round - 1);
					let delta = if evict_with_drop {
						Delta::Drop {
							key: prev,
						}
					} else {
						Delta::Remove {
							key: prev,
						}
					};
					MultiVersionCommit::commit(&store, cow_vec![delta], CommitVersion(version))
						.unwrap();
				}
			}
			current_count(&store, table)
		}

		let drop_live = churn(true);
		let remove_live = churn(false);

		assert!(
			drop_live <= 2,
			"Drop must keep the operator keyspace bounded to the live set; got {drop_live}"
		);
		assert!(
			remove_live >= ROUNDS - 1,
			"Remove leaves a tombstone per round (the path Drop avoids); got {remove_live} after {ROUNDS} rounds"
		);
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
		let heavy_bucket = read.page_of_key(&RowKey::encoded(SHAPE, 1));
		let light_bucket = read.page_of_key(&RowKey::encoded(SHAPE, 1u64 << 16));
		assert_ne!(heavy_bucket, light_bucket, "the two row groups must land in different buckets");
		assert!(!read.page_is_complete(heavy_bucket), "nothing is warm before the scan");

		let scanned = store
			.range(
				RowKey::full_scan(SHAPE),
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
	fn operator_state_write_through_keeps_read_cache_warm() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let read = store.read.clone().expect("read tier configured");

		let opkey = FlowNodeStateKey::new(FlowNodeId(7), vec![1, 2, 3]).encode();
		MultiVersionCommit::commit(
			&store,
			cow_vec![Delta::Set {
				key: opkey.clone(),
				row: EncodedRow(CowVec::new(b"state-v10".to_vec())),
			}],
			CommitVersion(10),
		)
		.unwrap();

		match read.get(&opkey, CommitVersion(10)) {
			VersionedGetResult::Value {
				value,
				version,
			} => {
				assert_eq!(
					value.as_ref(),
					b"state-v10",
					"the cached operator state must be the committed value"
				);
				assert_eq!(
					version,
					CommitVersion(10),
					"the cached entry must carry the commit version"
				);
			}
			other => {
				panic!("operator state must be served from the read cache after commit, got {other:?}")
			}
		}

		assert!(
			matches!(read.get(&opkey, CommitVersion(9)), VersionedGetResult::NotFound),
			"a pre-write snapshot read must miss the write-through entry, not see the newer value"
		);
	}

	#[test]
	fn source_row_write_clears_range_complete_on_its_page() {
		let (store, _g) = StandardMultiStore::testing_memory_with_persistent_sqlite();
		let read = store.read.clone().expect("read tier configured");

		let neighbor = RowKey::encoded(SHAPE, 1);
		let page = read.page_of_key(&neighbor);
		assert_eq!(
			read.page_of_key(&RowKey::encoded(SHAPE, 2)),
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
		let page = read.page_of_key(&RowKey::encoded(SHAPE, 1));
		assert!(!read.page_is_complete(page), "nothing is warm before the scan");

		assert!(read.begin_warm(page), "the page is unclaimed, so this claim must succeed");

		let scanned = store
			.range(
				RowKey::full_scan(SHAPE),
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
		let page = read.page_of_key(&RowKey::encoded(SHAPE, 1));

		let _ = store
			.range(
				RowKey::full_scan(SHAPE),
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
