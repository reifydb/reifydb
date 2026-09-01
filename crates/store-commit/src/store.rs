// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BTreeSet, HashMap, HashSet, VecDeque},
	iter, mem,
	ops::Bound,
	sync::{Arc, atomic::Ordering},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::EntryKind,
	metrics::{collect::MetricsCollector, sample::MetricsSample},
};
use reifydb_value::{Result, byte_size::ByteSize, count::Count, util::cowvec::CowVec};
use tracing::{Span, field, instrument};

use crate::{
	HistoricalSweep, MultiVersionScope, RangeBatch, RangeCursor, RawEntry, TierBatch, VersionedGetResult,
	entry::{Entries, Entry, entry_bytes},
	rows::{ActiveRows, MergedRows, Removed, RowMap, lookup, newest_across},
};

type EvictablePersist = Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>;
type EvictableDrop = Vec<EvictedVersion>;

const CLOSE_BYTE_THRESHOLD: ByteSize = ByteSize::from_mib(16);

#[derive(Clone, Debug)]
pub struct EvictedVersion {
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub value_bytes: ByteSize,
	pub current: bool,
}

fn value_bytes_of(value: &Option<CowVec<u8>>) -> ByteSize {
	ByteSize::from_bytes(value.as_ref().map(|v| v.len() as u64).unwrap_or(0))
}

#[derive(Clone)]
pub struct CommitStore {
	inner: Arc<CommitStoreInner>,
}

struct CommitStoreInner {
	entries: Entries,
	close_threshold: u64,
}

impl Default for CommitStore {
	fn default() -> Self {
		Self::new()
	}
}

impl CommitStore {
	#[instrument(name = "store::multi::memory::new", level = "debug")]
	pub fn new() -> Self {
		Self::with_close_threshold(CLOSE_BYTE_THRESHOLD)
	}

	pub fn with_close_threshold(threshold: ByteSize) -> Self {
		Self {
			inner: Arc::new(CommitStoreInner {
				entries: Entries::default(),
				close_threshold: threshold.as_bytes().max(1),
			}),
		}
	}

	pub fn estimated_current_count(&self, table: EntryKind) -> Result<u64> {
		let Some(entry) = self.inner.entries.data.get(&table) else {
			return Ok(0);
		};
		Ok(entry.key_count.load(Ordering::Relaxed))
	}

	pub fn list_all_entry_kinds(&self) -> Result<Vec<EntryKind>> {
		Ok(self.inner.entries.data.keys())
	}

	fn collect_oldest_pending(&self) -> Vec<(EntryKind, CommitVersion)> {
		self.inner
			.entries
			.data
			.keys()
			.into_iter()
			.filter_map(|kind| Some((kind, self.oldest_pending_for(kind)?)))
			.collect()
	}

	pub fn list_entry_kinds_by_oldest_pending(&self) -> Result<Vec<EntryKind>> {
		let mut pending = self.collect_oldest_pending();
		pending.sort_by_key(|(_, version)| *version);
		Ok(pending.into_iter().map(|(kind, _)| kind).collect())
	}

	pub fn oldest_pending_for(&self, kind: EntryKind) -> Option<CommitVersion> {
		let entry = self.inner.entries.data.get(&kind)?;
		let active = entry.active.read().min_version();
		let closed = entry.closed.read().iter().map(|map| map.min_version()).min();
		match (active, closed) {
			(Some(a), Some(c)) => Some(a.min(c)),
			(Some(a), None) => Some(a),
			(None, closed) => closed,
		}
	}

	fn resident_bytes(&self, current: bool) -> ByteSize {
		let total = self
			.inner
			.entries
			.data
			.keys()
			.into_iter()
			.filter_map(|kind| self.inner.entries.data.get(&kind))
			.map(|entry| {
				let pick = |rows: &RowMap| {
					if current {
						rows.current_bytes()
					} else {
						rows.historical_bytes()
					}
				};
				let active = pick(entry.active.read().rows());
				let closed: u64 = entry.closed.read().iter().map(|map| pick(map.rows())).sum();
				active + closed
			})
			.sum();
		ByteSize::from_bytes(total)
	}

	pub fn current_resident_bytes(&self) -> ByteSize {
		self.resident_bytes(true)
	}

	pub fn historical_resident_bytes(&self) -> ByteSize {
		self.resident_bytes(false)
	}

	#[inline]
	#[instrument(name = "store::multi::memory::get_or_create_table", level = "trace", skip(self), fields(table = ?table))]
	fn get_or_create_table(&self, table: EntryKind) -> Arc<Entry> {
		self.inner.entries.data.get_or_insert_with(table, || Arc::new(Entry::new()))
	}

	fn close_active(entry: &Entry, active: &mut ActiveRows) {
		if active.is_empty() {
			return;
		}
		let closing = mem::take(active);
		entry.closed.write().push_back(Arc::new(closing.close()));
	}

	#[inline]
	#[instrument(name = "store::multi::memory::set::table", level = "trace", skip(self, entries), fields(
		table = ?table,
		entry_count = entries.len(),
	))]
	fn process_table(
		&self,
		table: EntryKind,
		version: CommitVersion,
		entries: Vec<(EncodedKey, Option<CowVec<u8>>)>,
	) {
		let table_entry = self.get_or_create_table(table);
		let mut active = table_entry.active_write();
		let keys_before = active.rows().key_count();
		let mut pending = table_entry.pending.lock();
		for (key, value) in entries {
			pending.insert(key.clone());
			active.insert(key, version, value);
		}
		drop(pending);
		let keys_added = active.rows().key_count() - keys_before;
		table_entry.key_count.fetch_add(keys_added as u64, Ordering::Relaxed);
		if active.bytes() >= self.inner.close_threshold {
			Self::close_active(&table_entry, &mut active);
		}
	}

	pub fn oldest_pending_version(&self) -> Option<CommitVersion> {
		self.collect_oldest_pending().into_iter().map(|(_, version)| version).min()
	}

	fn seal_for_flush(&self, table: EntryKind, cutoff: CommitVersion) {
		let Some(entry) = self.inner.entries.data.get(&table) else {
			return;
		};
		let mut active = entry.active_write();
		if active.min_version().is_some_and(|oldest| oldest <= cutoff) {
			Self::close_active(&entry, &mut active);
		}
	}

	pub fn collect_evictable_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		budget: ByteSize,
	) -> (EvictablePersist, EvictableDrop, ByteSize, bool) {
		self.seal_for_flush(table, cutoff);

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return (Vec::new(), Vec::new(), ByteSize::ZERO, false),
		};
		let closed = entry.closed_snapshot();

		let budget = budget.as_bytes();
		let mut consumed = 0u64;
		let mut selected = 0usize;
		let mut latest: HashMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)> = HashMap::new();
		let mut to_drop: EvictableDrop = Vec::new();
		let mut more = false;

		'select: for map in closed.iter() {
			if map.min_version() > cutoff {
				continue;
			}
			for (key, versions) in map.rows().iter() {
				if selected > 0 && consumed >= budget {
					more = true;
					break 'select;
				}
				let newest = versions.keys().next().map(|Reverse(v)| *v);
				let mut touched = false;
				for (Reverse(version), value) in versions.iter() {
					if *version > cutoff {
						continue;
					}
					touched = true;
					consumed += entry_bytes(key, value);
					to_drop.push(EvictedVersion {
						key: key.clone(),
						version: *version,
						value_bytes: value_bytes_of(value),
						current: newest == Some(*version),
					});
					match latest.get(key) {
						Some((best, _)) if *best >= *version => {}
						_ => {
							latest.insert(key.clone(), (*version, value.clone()));
						}
					}
				}
				if touched {
					selected += 1;
				}
			}
		}

		let to_persist = latest.into_iter().map(|(key, (v, val))| (key, v, val)).collect();
		(to_persist, to_drop, ByteSize::from_bytes(consumed), more)
	}
}

impl CommitStore {
	#[instrument(name = "store::multi::memory::get", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0))]
	pub fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return Ok(VersionedGetResult::NotFound),
		};

		let active = entry.active.read();
		let closed = entry.closed.read();
		let found = lookup(
			iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())),
			key,
			version,
		);

		Ok(match found {
			Some((found, Some(value))) => VersionedGetResult::Value {
				value: value.clone(),
				version: found,
			},
			Some((_, None)) => VersionedGetResult::Tombstone,
			None => VersionedGetResult::NotFound,
		})
	}

	#[instrument(name = "store::multi::memory::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0), ret)]
	pub fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return Ok(false),
		};

		let active = entry.active.read();
		let closed = entry.closed.read();
		let found = lookup(
			iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())),
			key,
			version,
		);

		Ok(found.is_some_and(|(_, value)| value.is_some()))
	}

	#[instrument(name = "store::multi::memory::set", level = "trace", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty,
		version = version.0
	))]
	pub fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();

		batches.into_iter().for_each(|(table, entries)| {
			self.process_table(table, version, entries);
		});

		Span::current().record("total_entry_count", total_entries);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, scope = ?scope))]
	pub fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.finish();
				return Ok(RangeBatch::empty());
			}
		};

		let cursor_key = cursor.last_key().cloned();
		let active = entry.active.read();
		let closed = entry.closed.read();

		let iter_start: Bound<&[u8]> = match &cursor_key {
			Some(last) => Bound::Excluded(last.as_slice()),
			None => start,
		};

		let mut merged = MergedRows::new(
			iter::once(active.rows())
				.chain(closed.iter().map(|map| map.rows()))
				.map(|rows| rows.range((iter_start, end)))
				.collect(),
			false,
		);

		let mut entries: Vec<RawEntry> = Vec::with_capacity(batch_size + 1);
		while entries.len() <= batch_size {
			let Some((key, group)) = merged.next_group() else {
				break;
			};
			if let Some((version, value)) = newest_across(group.iter().copied(), scope.read())
				&& scope.contains(version)
			{
				entries.push(RawEntry {
					key: key.clone(),
					version,
					value: value.clone(),
				});
			}
		}

		let has_more = entries.len() > batch_size;
		if has_more {
			entries.truncate(batch_size);
		}

		if let Some(last_entry) = entries.last() {
			cursor.advance(last_entry.key.clone());
		}
		if !has_more {
			cursor.finish();
		}

		Ok(RangeBatch {
			entries,
			has_more,
		})
	}

	#[instrument(name = "store::multi::memory::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, scope = ?scope))]
	pub fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.finish();
				return Ok(RangeBatch::empty());
			}
		};

		let cursor_key = cursor.last_key().cloned();
		let active = entry.active.read();
		let closed = entry.closed.read();

		let iter_end: Bound<&[u8]> = match &cursor_key {
			Some(last) => Bound::Excluded(last.as_slice()),
			None => end,
		};

		let mut merged = MergedRows::new(
			iter::once(active.rows())
				.chain(closed.iter().map(|map| map.rows()))
				.map(|rows| rows.range((start, iter_end)).rev())
				.collect(),
			true,
		);

		let mut entries: Vec<RawEntry> = Vec::with_capacity(batch_size + 1);
		while entries.len() <= batch_size {
			let Some((key, group)) = merged.next_group() else {
				break;
			};
			if let Some((version, value)) = newest_across(group.iter().copied(), scope.read())
				&& scope.contains(version)
			{
				entries.push(RawEntry {
					key: key.clone(),
					version,
					value: value.clone(),
				});
			}
		}

		let has_more = entries.len() > batch_size;
		if has_more {
			entries.truncate(batch_size);
		}

		if let Some(last_entry) = entries.last() {
			cursor.advance(last_entry.key.clone());
		}
		if !has_more {
			cursor.finish();
		}

		Ok(RangeBatch {
			entries,
			has_more,
		})
	}

	#[instrument(name = "store::multi::memory::ensure_table", level = "trace", skip(self), fields(table = ?table))]
	pub fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let _ = self.get_or_create_table(table);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::clear_table", level = "debug", skip(self), fields(table = ?table))]
	pub fn clear_table(&self, table: EntryKind) -> Result<()> {
		if let Some(entry) = self.inner.entries.data.get(&table) {
			let mut active = entry.active_write();
			*active = ActiveRows::new();
			*entry.closed.write() = VecDeque::new();
			entry.key_count.store(0, Ordering::Relaxed);
		}
		Ok(())
	}
}

impl CommitStore {
	#[instrument(name = "store::multi::memory::drop", level = "debug", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty
	))]
	pub fn compact(
		&self,
		batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>,
	) -> Result<Vec<EvictedVersion>> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();
		let mut removed: Vec<EvictedVersion> = Vec::with_capacity(total_entries);

		for (table, entries) in batches {
			let table_entry = self.get_or_create_table(table);
			let mut dropped: HashMap<EncodedKey, HashSet<CommitVersion>> = HashMap::new();
			let mut below = CommitVersion(0);
			for (key, version) in entries {
				below = below.max(version);
				dropped.entry(key).or_default().insert(version);
			}

			let mut active = table_entry.active_write();
			let mut closed = table_entry.closed.write();
			let keys_before = active.rows().key_count()
				+ closed.iter().map(|slot| slot.rows().key_count()).sum::<usize>();

			let newest: HashMap<EncodedKey, CommitVersion> = dropped
				.keys()
				.filter_map(|key| {
					lookup(
						iter::once(active.rows())
							.chain(closed.iter().rev().map(|slot| slot.rows())),
						key,
						CommitVersion(u64::MAX),
					)
					.map(|(version, _)| (key.clone(), version))
				})
				.collect();

			let mut record = |entry: &Removed| EvictedVersion {
				key: entry.key.clone(),
				version: entry.version,
				value_bytes: value_bytes_of(&entry.value),
				current: newest.get(&entry.key) == Some(&entry.version),
			};

			if active.min_version().is_some_and(|min| min <= below) {
				removed.extend(active.compact(&dropped).iter().map(&mut record));
			}

			for slot in closed.iter_mut() {
				if slot.min_version() > below {
					continue;
				}
				removed.extend(Arc::make_mut(slot).compact(&dropped).iter().map(&mut record));
			}
			closed.retain(|map| !map.rows().is_empty());
			let keys_after = active.rows().key_count()
				+ closed.iter().map(|slot| slot.rows().key_count()).sum::<usize>();
			table_entry.key_count.fetch_sub((keys_before - keys_after) as u64, Ordering::Relaxed);
		}

		Span::current().record("total_entry_count", total_entries);
		Ok(removed)
	}

	#[instrument(name = "store::multi::memory::get_all_versions", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	pub fn get_all_versions(
		&self,
		table: EntryKind,
		key: &[u8],
	) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return Ok(Vec::new()),
		};

		let active = entry.active.read();
		let closed = entry.closed.read();

		let mut versions: Vec<(CommitVersion, Option<CowVec<u8>>)> = iter::once(active.rows())
			.chain(closed.iter().map(|map| map.rows()))
			.filter_map(|rows| rows.versions_for(key))
			.flat_map(|found| found.iter().map(|(Reverse(v), value)| (*v, value.clone())))
			.collect();

		versions.sort_by(|a, b| b.0.cmp(&a.0));
		versions.dedup_by_key(|(version, _)| *version);

		Ok(versions)
	}

	#[instrument(name = "store::multi::memory::sweep_historical_below", level = "trace", skip(self), fields(table = ?table, cutoff = cutoff.0, batch_size = batch_size))]
	pub fn sweep_historical_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		batch_size: usize,
	) -> Result<HistoricalSweep> {
		let Some(entry) = self.inner.entries.data.get(&table) else {
			return Ok(HistoricalSweep::default());
		};

		let queued = mem::take(&mut *entry.pending.lock());
		let queued = if queued.is_empty() {
			mem::take(&mut *entry.retained.lock())
		} else {
			queued
		};
		let mut queued = queued.into_iter();
		let keys: Vec<EncodedKey> = queued.by_ref().take(batch_size).collect();
		let mut unexamined: BTreeSet<EncodedKey> = queued.collect();
		let remaining = unexamined.len() as u64;

		let mut entries: Vec<(EncodedKey, CommitVersion)> = Vec::new();
		let mut retained: Vec<EncodedKey> = Vec::new();
		let mut versions: Vec<CommitVersion> = Vec::new();
		for key in keys {
			versions.clear();
			{
				let active = entry.active.read();
				if let Some(found) = active.rows().versions_for(key.as_slice()) {
					versions.extend(found.keys().map(|Reverse(version)| *version));
				}
			}
			{
				let closed = entry.closed.read();
				for map in closed.iter() {
					if let Some(found) = map.rows().versions_for(key.as_slice()) {
						versions.extend(found.keys().map(|Reverse(version)| *version));
					}
				}
			}
			versions.sort_unstable();
			versions.dedup();
			let Some((_, older)) = versions.split_last() else {
				continue;
			};
			let mut above_cutoff = false;
			for version in older {
				if *version < cutoff {
					entries.push((key.clone(), *version));
				} else {
					above_cutoff = true;
				}
			}
			if above_cutoff {
				retained.push(key);
			}
		}

		entry.pending.lock().append(&mut unexamined);
		entry.retained.lock().extend(retained);

		Ok(HistoricalSweep {
			entries,
			remaining,
		})
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiCommitMetrics {
	pub current_bytes: ByteSize,
	pub historical_bytes: ByteSize,
	pub table_count: Count,
	pub current_entries: Count,
}

impl CommitStore {
	pub fn metrics(&self) -> MultiCommitMetrics {
		let kinds = self.list_all_entry_kinds().unwrap_or_default();
		let current_entries: u64 =
			kinds.iter().map(|kind| self.estimated_current_count(*kind).unwrap_or(0)).sum();
		MultiCommitMetrics {
			current_bytes: self.current_resident_bytes(),
			historical_bytes: self.historical_resident_bytes(),
			table_count: Count::new(kinds.len() as u64),
			current_entries: Count::new(current_entries),
		}
	}
}

impl MetricsCollector for CommitStore {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		out.push(MetricsSample::heap("commit_buffer", "current_bytes", self.current_resident_bytes()));
		out.push(MetricsSample::heap("commit_buffer", "historical_bytes", self.historical_resident_bytes()));
		let kinds = self.list_all_entry_kinds().unwrap_or_default();
		out.push(MetricsSample::count("commit_buffer", "table_count", kinds.len() as u64));
		let current_entries: u64 =
			kinds.iter().map(|kind| self.estimated_current_count(*kind).unwrap_or(0)).sum();
		out.push(MetricsSample::count("commit_buffer", "current_entries", current_entries));
	}
}

#[cfg(test)]
pub mod tests {
	use std::collections::BTreeMap;

	use reifydb_core::interface::catalog::{id::TableId, storage::StorageId};

	use super::*;

	const UNBOUNDED: ByteSize = ByteSize::from_bytes(u64::MAX);

	fn budget_of(entries: &[(EncodedKey, Option<CowVec<u8>>)]) -> ByteSize {
		// The budget is expressed in the same accounting the buffer's residency counters use, so a test
		// budget must be derived from entry_bytes rather than from the value length alone.
		ByteSize::from_bytes(entries.iter().map(|(key, value)| entry_bytes(key, value)).sum())
	}

	fn keyed(name: &str, value: &[u8]) -> (EncodedKey, Option<CowVec<u8>>) {
		(EncodedKey::new(name.as_bytes().to_vec()), Some(CowVec::new(value.to_vec())))
	}

	fn seed(storage: &CommitStore, version: u64, entries: &[(EncodedKey, Option<CowVec<u8>>)]) {
		for (key, value) in entries {
			storage.set(
				CommitVersion(version),
				HashMap::from([(EntryKind::Multi, vec![(key.clone(), value.clone())])]),
			)
			.unwrap();
		}
	}

	#[test]
	fn test_basic_operations() {
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key1");
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value1".to_vec())))])]),
		)
		.unwrap();

		let value = storage.get(EntryKind::Multi, &key, version).unwrap().value();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		assert!(storage.contains(EntryKind::Multi, &key, version).unwrap());

		assert!(!storage.contains(EntryKind::Multi, b"nonexistent", version).unwrap());

		let version2 = CommitVersion(2);
		storage.set(version2, HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();
		assert!(!storage.contains(EntryKind::Multi, &key, version2).unwrap());
	}

	#[test]
	fn test_source_tables() {
		let storage = CommitStore::new();

		let source1 = StorageId::Table(TableId(1));
		let source2 = StorageId::Table(TableId(2));

		let key = EncodedKey::new(b"key");
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source1),
				vec![(key.clone(), Some(CowVec::new(b"table1".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source2),
				vec![(key.clone(), Some(CowVec::new(b"table2".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Source(source1), &key, version).unwrap().value().as_deref(),
			Some(b"table1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Source(source2), &key, version).unwrap().value().as_deref(),
			Some(b"table2".as_slice())
		);
	}

	#[test]
	fn test_version_promotion_to_historical() {
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key1");

		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();

		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v2".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_insert_older_version() {
		// An out-of-order older commit must stay resolvable: a read takes the largest version <= the
		// snapshot, so the v2 snapshot resolves to v1.
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key1");

		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_range_next() {
		let storage = CommitStore::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.is_exhausted());

		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"c");
	}

	#[test]
	fn test_range_rev_next() {
		let storage = CommitStore::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.is_exhausted());

		assert_eq!(&*batch.entries[0].key, b"c");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"a");
	}

	#[test]
	fn test_range_streaming_pagination() {
		let storage = CommitStore::new();

		let version = CommitVersion(1);

		let entries: Vec<_> =
			(0..10u8).map(|i| (EncodedKey::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		let mut cursor = RangeCursor::new();

		let batch1 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		let batch2 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);

		let batch3 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch3.entries.len(), 3);
		assert!(batch3.has_more);
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch3.entries[0].key, &[6]);
		assert_eq!(&*batch3.entries[2].key, &[8]);

		let batch4 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch4.entries.len(), 1);
		assert!(!batch4.has_more);
		assert!(cursor.is_exhausted());

		assert_eq!(&*batch4.entries[0].key, &[9]);

		let batch5 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert!(batch5.entries.is_empty());
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = CommitStore::new();

		let version = CommitVersion(1);

		let entries: Vec<_> =
			(0..10u8).map(|i| (EncodedKey::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		let mut cursor = RangeCursor::new();

		let batch1 = storage
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		let batch2 = storage
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch2.entries[0].key, &[6]);
		assert_eq!(&*batch2.entries[2].key, &[4]);
	}

	fn sweep_all(storage: &CommitStore, cutoff: u64, batch: usize) -> Vec<(EncodedKey, CommitVersion)> {
		// Drives one full pass the way the actor does: keep sweeping while unexamined keys remain, compacting
		// each batch, so a bug in batching or requeueing shows up as missing or duplicated rows.
		let mut all = Vec::new();
		let mut calls = 0;
		loop {
			let sweep =
				storage.sweep_historical_below(EntryKind::Multi, CommitVersion(cutoff), batch).unwrap();
			calls += 1;
			assert!(calls <= 64, "a pass must terminate even when every key is retained");
			all.extend(sweep.entries.iter().cloned());
			if !sweep.entries.is_empty() {
				storage.compact(HashMap::from([(EntryKind::Multi, sweep.entries)])).unwrap();
			}
			if sweep.remaining == 0 {
				return all;
			}
		}
	}

	#[test]
	fn sweep_drops_versions_below_cutoff_across_closed_maps_and_forgets_finished_keys() {
		// Every set closes the active map, so each (key, version) sits in its own closed map and a key's
		// versions must be gathered across maps; a per-map view would see nothing to reclaim. The newest
		// version survives even when it is below the cutoff, and a key with nothing left to reclaim must
		// not be examined again on the next pass.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		for (name, version) in [
			("a", 1),
			("a", 2),
			("a", 3),
			("b", 1),
			("b", 2),
			("c", 1),
			("d", 1),
			("d", 2),
			("d", 4),
			("e", 3),
			("e", 2),
			("e", 1),
		] {
			seed(&storage, version, &[keyed(name, b"v")]);
		}
		let row =
			|name: &str, version: u64| (EncodedKey::new(name.as_bytes().to_vec()), CommitVersion(version));
		let expected =
			vec![row("a", 1), row("a", 2), row("b", 1), row("d", 1), row("d", 2), row("e", 1), row("e", 2)];

		assert_eq!(sweep_all(&storage, 4, 100), expected);
		for (name, survivors) in
			[("a", vec![3]), ("b", vec![2]), ("c", vec![1]), ("d", vec![4]), ("e", vec![3])]
		{
			let left: Vec<u64> = storage
				.get_all_versions(EntryKind::Multi, name.as_bytes())
				.unwrap()
				.into_iter()
				.map(|(v, _)| v.0)
				.collect();
			assert_eq!(left, survivors, "key {name}");
		}

		let again = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(100), 100).unwrap();
		assert!(again.entries.is_empty());
		assert_eq!(again.remaining, 0);
	}

	#[test]
	fn sweep_requeues_keys_whose_older_versions_are_still_above_cutoff() {
		// A version that is historical but not yet below the cutoff must be reclaimed by a later pass without
		// a new write to the key, so the key has to stay queued; but it must not count as backlog, or the
		// actor would spin re-examining it before the cutoff moves.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k".to_vec());
		for version in [1, 2, 3] {
			seed(&storage, version, &[keyed("k", b"v")]);
		}

		let first = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(2), 100).unwrap();
		assert_eq!(first.entries, vec![(key.clone(), CommitVersion(1))]);
		assert_eq!(first.remaining, 0, "a retained key is not backlog");
		storage.compact(HashMap::from([(EntryKind::Multi, first.entries)])).unwrap();

		let second = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(3), 100).unwrap();
		assert_eq!(
			second.entries,
			vec![(key.clone(), CommitVersion(2))],
			"requeued key reclaimed once the cutoff passed"
		);
		storage.compact(HashMap::from([(EntryKind::Multi, second.entries)])).unwrap();

		let third = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(100), 100).unwrap();
		assert!(third.entries.is_empty(), "nothing historical is left for the key");
	}

	#[test]
	fn a_retained_key_waits_for_the_next_pass_instead_of_displacing_unexamined_ones() {
		// Keys kept back because a version is still above the cutoff must not be handed to the next batch
		// ahead of keys nobody has looked at yet: they sort first, so re-queueing them straight away makes
		// every batch re-examine the same keys, drop nothing, and report the same backlog forever.
		let storage = CommitStore::new();
		let names = ["a", "b", "c", "d", "e"];
		for name in names {
			seed(&storage, 1, &[keyed(name, b"v1")]);
			seed(&storage, 2, &[keyed(name, b"v2")]);
			seed(&storage, 3, &[keyed(name, b"v3")]);
		}

		let dropped = |sweep: &HistoricalSweep| -> Vec<(String, u64)> {
			sweep.entries
				.iter()
				.map(|(key, version)| (String::from_utf8(key.as_slice().to_vec()).unwrap(), version.0))
				.collect()
		};

		let first = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(2), 2).unwrap();
		assert_eq!(dropped(&first), vec![("a".into(), 1), ("b".into(), 1)]);
		assert_eq!(first.remaining, 3);
		storage.compact(HashMap::from([(EntryKind::Multi, first.entries)])).unwrap();

		let second = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(2), 2).unwrap();
		assert_eq!(
			dropped(&second),
			vec![("c".into(), 1), ("d".into(), 1)],
			"a and b were retained, not re-batched"
		);
		assert_eq!(second.remaining, 1);
		storage.compact(HashMap::from([(EntryKind::Multi, second.entries)])).unwrap();

		let third = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(2), 2).unwrap();
		assert_eq!(dropped(&third), vec![("e".into(), 1)]);
		assert_eq!(third.remaining, 0, "the pass ends once every fresh key was examined");
		storage.compact(HashMap::from([(EntryKind::Multi, third.entries)])).unwrap();

		let next_pass = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(3), 2).unwrap();
		assert_eq!(
			dropped(&next_pass),
			vec![("a".into(), 2), ("b".into(), 2)],
			"retained keys come back once the queue is empty"
		);
		assert_eq!(next_pass.remaining, 3, "the rest of the retained keys are the new backlog");
	}

	#[test]
	fn sweep_examines_at_most_batch_keys_and_reports_the_rest_as_backlog() {
		// The batch bounds how many keys one call looks up, and the unexamined keys must be reported so the
		// actor keeps going instead of waiting a full interval; across the pass every key is examined
		// exactly once and no row is dropped twice.
		let storage = CommitStore::new();
		let names = ["k1", "k2", "k3", "k4", "k5"];
		for name in names {
			seed(&storage, 1, &[keyed(name, b"old")]);
			seed(&storage, 2, &[keyed(name, b"new")]);
		}
		seed(&storage, 2, &[keyed("z-single", b"single")]);

		let first = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(10), 2).unwrap();
		assert_eq!(first.entries.len(), 2);
		assert_eq!(first.remaining, 4, "six keys queued, two examined");
		storage.compact(HashMap::from([(EntryKind::Multi, first.entries.clone())])).unwrap();

		let mut all = first.entries;
		all.extend(sweep_all(&storage, 10, 2));
		let mut expected: Vec<(EncodedKey, CommitVersion)> = names
			.iter()
			.map(|name| (EncodedKey::new(name.as_bytes().to_vec()), CommitVersion(1)))
			.collect();
		expected.sort();
		all.sort();
		assert_eq!(all, expected);

		let after = storage.sweep_historical_below(EntryKind::Multi, CommitVersion(10), 100).unwrap();
		assert!(after.entries.is_empty());
		assert_eq!(after.remaining, 0, "a single-version key does not linger in the queue");
	}

	#[test]
	fn test_drop_from_historical() {
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));

		let key = EncodedKey::new(b"key1");

		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(1))])])).unwrap();

		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().is_none());

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);
	}
	#[test]
	fn compact_returns_each_removed_historical_version_flagged_as_not_current() {
		// The storage metric only ever decrements from what compact reports back, so a version removed
		// physically but omitted from the return value inflates historical_count forever.
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key1");

		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}
		storage.seal_for_flush(EntryKind::Multi, CommitVersion(u64::MAX));

		let removed = storage
			.compact(HashMap::from([(
				EntryKind::Multi,
				vec![(key.clone(), CommitVersion(1)), (key.clone(), CommitVersion(2))],
			)]))
			.unwrap();

		let mut versions: Vec<u64> = removed.iter().map(|entry| entry.version.0).collect();
		versions.sort_unstable();
		assert_eq!(versions, vec![1, 2]);
		assert!(removed.iter().all(|entry| !entry.current));
		assert!(removed.iter().all(|entry| entry.value_bytes == ByteSize::from_bytes(2)));
	}

	#[test]
	fn compact_reports_the_live_version_and_leaves_surviving_history_in_place() {
		// Dropping the live version does not promote the newest survivor, so the removal is only visible
		// to the metric through the returned record; the survivors must stay in historical, unreported
		// and still readable, which is what makes skipping the promotion safe.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));

		let key = EncodedKey::new(b"key1");

		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		let removed = storage
			.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(3))])]))
			.unwrap();

		assert_eq!(removed.len(), 1, "only the live version was dropped");
		assert_eq!(removed[0].version, CommitVersion(3));
		assert!(removed[0].current, "the dropped version was the live one");
		assert_eq!(removed[0].value_bytes, ByteSize::from_bytes(2));

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v2".as_slice()),
			"the newest survivor is still readable from historical without being promoted"
		);
	}

	#[test]
	fn compact_reports_the_live_entry_when_every_version_of_a_key_is_removed() {
		// The metric routes a current removal to the current counters and a historical one to the
		// historical counters, so mislabelling the live entry moves rows between the two columns
		// instead of clearing them.
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key1");

		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}
		storage.seal_for_flush(EntryKind::Multi, CommitVersion(u64::MAX));

		let removed = storage
			.compact(HashMap::from([(
				EntryKind::Multi,
				vec![
					(key.clone(), CommitVersion(1)),
					(key.clone(), CommitVersion(2)),
					(key.clone(), CommitVersion(3)),
				],
			)]))
			.unwrap();

		assert_eq!(removed.len(), 3);

		let live: Vec<u64> =
			removed.iter().filter(|entry| entry.current).map(|entry| entry.version.0).collect();
		assert_eq!(live, vec![3]);

		let mut historical: Vec<u64> =
			removed.iter().filter(|entry| !entry.current).map(|entry| entry.version.0).collect();
		historical.sort_unstable();
		assert_eq!(historical, vec![1, 2]);
	}

	#[test]
	fn test_tombstones() {
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key1");

		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();

		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().is_none());
		assert!(!storage.contains(EntryKind::Multi, &key, CommitVersion(2)).unwrap());

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"value".as_slice())
		);
	}

	#[test]
	fn test_collect_evictable_below_keeps_versions_above_cutoff() {
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// v2 is what a reader in [2, 3) resolves to, so it is the value that must be persisted.
		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(2), UNBOUNDED);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].0, key);
		assert_eq!(to_persist[0].1, CommitVersion(2));
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v2".as_slice()));
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(dropped, HashSet::from([CommitVersion(1), CommitVersion(2)]));

		storage.compact(HashMap::from([(
			EntryKind::Multi,
			to_drop.into_iter().map(|e| (e.key, e.version)).collect(),
		)]))
		.unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().is_none());
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().is_none());
	}

	#[test]
	fn test_collect_evictable_below_empty_when_all_above_cutoff() {
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(3), UNBOUNDED);
		assert!(to_persist.is_empty());
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_collect_evictable_below_persists_exactly_one_value_per_key() {
		// Only the latest-<=cutoff value may be persisted: it is the single value a reader at the cutoff
		// snapshot resolves to, so persisting an older one corrupts that resolution.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		for v in 1..=5u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(4), UNBOUNDED);
		assert_eq!(to_persist.len(), 1, "exactly one value persisted per key");
		assert_eq!(to_persist[0].1, CommitVersion(4), "the latest version <= cutoff");
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v4".as_slice()));

		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(
			dropped,
			HashSet::from([CommitVersion(1), CommitVersion(2), CommitVersion(3), CommitVersion(4)])
		);
	}

	#[test]
	fn test_collect_evictable_below_persists_tombstone_when_it_is_the_latest() {
		// A tombstone that is the latest-<=cutoff version must be carried to the persistent tier; dropping
		// it lets a later read resurrect the pre-delete value.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(2), UNBOUNDED);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(2), "the tombstone is the latest version");
		assert!(to_persist[0].2.is_none(), "the persisted latest value must be the tombstone, not v1");
		assert_eq!(to_drop.len(), 2, "both v1 and the tombstone are dropped from the buffer");
	}

	#[test]
	fn test_collect_evictable_below_only_drops_historical_when_current_is_above_cutoff() {
		// A key that is actively written while old snapshots age out: only the historical version may be
		// evicted, the current one is still hot and must not be persisted.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v5".to_vec())))])]),
		)
		.unwrap();

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(3), UNBOUNDED);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(2), "only the aged-out historical version is persisted");
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v2".as_slice()));
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(dropped, HashSet::from([CommitVersion(2)]), "v5 (current, > cutoff) is never dropped");

		storage.compact(HashMap::from([(
			EntryKind::Multi,
			to_drop.into_iter().map(|e| (e.key, e.version)).collect(),
		)]))
		.unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(5)).unwrap().value().as_deref(),
			Some(b"v5".as_slice())
		);
		assert!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().is_none(),
			"the v2 a reader at snapshot 3 used to see is gone from the buffer after eviction"
		);
	}

	#[test]
	fn test_collect_evictable_below_handles_multiple_keys_independently() {
		// The cutoff applies per version, not per key: a key whose only version is above it must stay
		// fully resident even while a sibling key is evicted.
		let storage = CommitStore::new();
		let cold = EncodedKey::new(b"cold");
		let hot = EncodedKey::new(b"hot");
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(cold.clone(), Some(CowVec::new(b"cold1".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(9),
			HashMap::from([(EntryKind::Multi, vec![(hot.clone(), Some(CowVec::new(b"hot9".to_vec())))])]),
		)
		.unwrap();

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(5), UNBOUNDED);
		assert_eq!(to_persist.len(), 1, "only the cold key is evictable below the cutoff");
		assert_eq!(to_persist[0].0, cold);
		assert!(to_drop.iter().all(|e| e.key == cold), "the hot key must not be scheduled for drop");
	}

	#[test]
	fn test_collect_evictable_below_bounds_to_budget_and_drains_across_calls() {
		// The budget bounds a flush slice so one transaction never persists the whole evictable set;
		// looping bounded calls must still drain exactly the below-cutoff set, no more, no less.
		let storage = CommitStore::new();
		let entries: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			(0..5u8).map(|i| keyed(&format!("k{i}"), &[i])).collect();
		seed(&storage, 1, &entries);
		let budget = budget_of(&entries[..2]);

		let (to_persist, to_drop, consumed, more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);
		assert_eq!(to_persist.len(), 2, "the budget only covers two entries' worth of bytes");
		assert_eq!(to_drop.len(), 2);
		assert_eq!(consumed, budget, "the reported spend must be the bytes actually selected");
		assert!(more, "three keys remain below the cutoff");

		let mut drained = to_persist.len();
		let mut compaction_batch: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		compaction_batch.insert(EntryKind::Multi, to_drop.into_iter().map(|e| (e.key, e.version)).collect());
		storage.compact(compaction_batch).unwrap();
		loop {
			let (p, d, _consumed, more) =
				storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);
			if p.is_empty() {
				assert!(!more, "an empty collect must not claim more remains");
				break;
			}
			drained += p.len();
			let mut batch: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
			batch.insert(EntryKind::Multi, d.into_iter().map(|e| (e.key, e.version)).collect());
			storage.compact(batch).unwrap();
			if !more {
				break;
			}
		}
		assert_eq!(drained, 5, "every below-cutoff key is drained exactly once");
	}

	#[test]
	fn collect_evictable_below_bounds_the_slice_by_bytes_not_by_key_count() {
		// A key-count budget lets one slice pull an unbounded number of bytes: N fat rows cost the same
		// as N thin ones. The same byte budget must therefore admit strictly fewer fat keys than thin
		// ones, which is exactly what a count-based cap cannot do.
		let thin: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			(0..8u8).map(|i| keyed(&format!("k{i}"), &[i])).collect();
		let fat: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			(0..8u8).map(|i| keyed(&format!("k{i}"), &vec![i; 4096])).collect();

		let budget = budget_of(&thin[..4]);

		let thin_storage = CommitStore::new();
		seed(&thin_storage, 1, &thin);
		let (thin_persist, _, _, thin_more) =
			thin_storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);

		let fat_storage = CommitStore::new();
		seed(&fat_storage, 1, &fat);
		let (fat_persist, _, fat_consumed, fat_more) =
			fat_storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);

		assert_eq!(thin_persist.len(), 4, "four thin entries is exactly what the budget buys");
		assert!(thin_more, "four of the eight thin keys are still pending");
		assert_eq!(fat_persist.len(), 1, "a single fat entry already exceeds the same byte budget");
		assert!(fat_more, "the remaining fat keys are still pending");
		assert!(
			fat_consumed.as_bytes() > budget.as_bytes(),
			"the one admitted fat entry is what pushed the slice over the budget"
		);
	}

	#[test]
	fn collect_evictable_below_always_admits_one_entry_even_when_the_budget_cannot_cover_it() {
		// A slice must never collect nothing while work is pending: an entry no budget can cover would
		// otherwise be skipped every slice forever, and the key it pins holds the durable frontier at its
		// commit version, which clamps the tombstone reap cutoff to zero.
		let storage = CommitStore::new();
		let entries = vec![keyed("huge-a", &vec![7u8; 65536]), keyed("huge-b", &vec![9u8; 65536])];
		seed(&storage, 1, &entries);

		let (to_persist, to_drop, consumed, more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), ByteSize::from_bytes(1));
		assert_eq!(to_persist.len(), 1, "the oversized entry must be admitted, not skipped");
		assert_eq!(to_drop.len(), 1);
		assert!(consumed.as_bytes() > 65536, "the whole oversized entry counts against the slice");
		assert!(more, "the second oversized entry is still pending");

		let (to_persist, _, consumed, more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), ByteSize::ZERO);
		assert_eq!(to_persist.len(), 1, "even a zero budget must still release exactly one entry");
		assert!(consumed.as_bytes() > 65536);
		assert!(more, "a zero budget yields after the one entry it was forced to take");
	}

	#[test]
	fn collect_evictable_below_counts_every_evicted_version_of_a_key_against_the_budget() {
		// A key's superseded versions leave the buffer in the same slice as its live one, so a budget
		// that only charged for the live version would let a deep version chain blow the slice's byte
		// ceiling without ever reporting it.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k".to_vec());
		for v in 1..=4u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(vec![v as u8; 512])))],
				)]),
			)
			.unwrap();
		}

		let (_, to_drop, consumed, _) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(4), UNBOUNDED);

		assert_eq!(to_drop.len(), 4, "all four versions leave the buffer");
		let expected: u64 = (1..=4u64).map(|v| entry_bytes(&key, &Some(CowVec::new(vec![v as u8; 512])))).sum();
		assert_eq!(
			consumed,
			ByteSize::from_bytes(expected),
			"the slice must charge itself for every version it evicted, not just the live one"
		);
	}

	fn walk_versions(storage: &CommitStore, table: EntryKind) -> BTreeMap<EncodedKey, Vec<CommitVersion>> {
		// Every version the buffer physically holds, gathered across the active map and every closed one,
		// so an assertion can be made against what is stored rather than what a counter claims.
		let entry = storage.inner.entries.data.get(&table).expect("table exists");
		let active = entry.active.read();
		let closed = entry.closed_snapshot();
		let mut walked: BTreeMap<EncodedKey, Vec<CommitVersion>> = BTreeMap::new();
		for rows in iter::once(active.rows()).chain(closed.iter().map(|map| map.rows())) {
			for (key, versions) in rows.iter() {
				walked.entry(key.clone())
					.or_default()
					.extend(versions.keys().map(|Reverse(version)| *version));
			}
		}
		walked
	}

	fn oldest_of(storage: &CommitStore, table: EntryKind, key: &EncodedKey) -> Option<CommitVersion> {
		walk_versions(storage, table).get(key).and_then(|versions| versions.iter().min().copied())
	}

	fn assert_oldest_pending_tracks_the_maps(storage: &CommitStore, table: EntryKind) {
		// A reported floor above the smallest stored version lets the reaper cut a version the buffer has
		// not flushed; one below it freezes the floor on a version nobody holds and the sweep never ends.
		let walked = walk_versions(storage, table);
		let smallest = walked.values().flat_map(|versions| versions.iter().copied()).min();
		assert_eq!(
			storage.oldest_pending_for(table),
			smallest,
			"the reported oldest pending version must equal the smallest version the maps still hold"
		);

		for (key, versions) in walked.iter() {
			let reported = storage
				.get_all_versions(table, key.as_ref())
				.unwrap()
				.last()
				.map(|(version, _)| *version);
			assert_eq!(
				reported,
				versions.iter().min().copied(),
				"a resident key must report its smallest stored version, not a newer one"
			);
		}
	}

	#[test]
	fn index_stays_consistent_across_new_monotonic_out_of_order_and_drops() {
		// The eviction index is what keeps collect_evictable_below O(evictable) instead of O(table);
		// drift from the maps either strands a key forever or churns a ghost, so every maintenance path
		// is cross-checked against a full walk of both maps.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		let kind = EntryKind::Multi;
		let a = EncodedKey::new(b"a");
		let b = EncodedKey::new(b"b");
		let c = EncodedKey::new(b"c");

		let set = |v: u64, key: &EncodedKey, val: &str| {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					kind,
					vec![(key.clone(), Some(CowVec::new(val.as_bytes().to_vec())))],
				)]),
			)
			.unwrap();
		};

		set(10, &a, "a10");
		set(20, &a, "a20");
		set(3, &a, "a3");
		set(5, &b, "b5");
		set(7, &c, "c7");
		assert_eq!(
			oldest_of(&storage, kind, &a),
			Some(CommitVersion(3)),
			"an out-of-order write below the current version must lower a's bucket to 3"
		);
		assert_oldest_pending_tracks_the_maps(&storage, kind);

		storage.compact(HashMap::from([(kind, vec![(a.clone(), CommitVersion(3))])])).unwrap();
		assert_eq!(
			oldest_of(&storage, kind, &a),
			Some(CommitVersion(10)),
			"dropping the oldest version must raise the bucket to the next-smallest stored version"
		);
		assert_oldest_pending_tracks_the_maps(&storage, kind);

		storage.compact(HashMap::from([(kind, vec![(b.clone(), CommitVersion(5))])])).unwrap();
		assert_eq!(oldest_of(&storage, kind, &b), None, "a fully dropped key must leave the index entirely");
		assert_oldest_pending_tracks_the_maps(&storage, kind);
	}

	#[test]
	fn out_of_order_landing_is_selected_for_eviction() {
		// A late or replayed commit landing below the current version becomes the key's oldest; an index
		// that tracked only first-seen versions would strand it in the buffer forever.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(20),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v20".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(5), UNBOUNDED);
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(
			dropped,
			HashSet::from([CommitVersion(3)]),
			"the out-of-order v3 must be selected; v20 stays resident"
		);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(3), "the aged-out v3 is the value persisted");
	}

	#[test]
	fn byte_tally_matches_a_full_walk_across_mixed_mutations() {
		// The tally is incremental, so any drift from the true map contents misreports memory forever
		// after; every mutation shape is exercised then compared against an exhaustive walk.
		let storage = CommitStore::new();
		let k1 = EncodedKey::new(b"key-one");
		let k2 = EncodedKey::new(b"key-two");

		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(k1.clone(), Some(CowVec::new(format!("value-{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(k2.clone(), Some(CowVec::new(b"x".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(k2.clone(), Some(CowVec::new(b"older".to_vec())))])]),
		)
		.unwrap();
		storage.set(CommitVersion(6), HashMap::from([(EntryKind::Multi, vec![(k2.clone(), None)])])).unwrap();

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(k1.clone(), CommitVersion(3))])])).unwrap();
		storage.compact(HashMap::from([(EntryKind::Multi, vec![(k2.clone(), CommitVersion(2))])])).unwrap();

		let entry = storage.inner.entries.data.get(&EntryKind::Multi).unwrap();
		let active = entry.active.read();
		let closed = entry.closed_snapshot();
		let mut walked_current = 0u64;
		let mut walked_historical = 0u64;
		for rows in iter::once(active.rows()).chain(closed.iter().map(|map| map.rows())) {
			for (key, versions) in rows.iter() {
				let newest = versions.keys().next().map(|Reverse(version)| *version);
				for (Reverse(version), value) in versions.iter() {
					if newest == Some(*version) {
						walked_current += entry_bytes(key, value);
					} else {
						walked_historical += entry_bytes(key, value);
					}
				}
			}
		}
		drop(active);

		assert!(walked_current > 0, "precondition: the scenario must leave current entries behind");
		assert!(walked_historical > 0, "precondition: the scenario must leave historical entries behind");
		assert_eq!(
			storage.current_resident_bytes().as_bytes(),
			walked_current,
			"the incremental current tally must equal an exhaustive walk of the newest version of every key"
		);
		assert_eq!(
			storage.historical_resident_bytes().as_bytes(),
			walked_historical,
			"the incremental historical tally must equal an exhaustive walk of every superseded version"
		);
	}

	#[test]
	fn byte_tally_nets_to_zero_when_the_buffer_is_fully_drained() {
		// Eviction drains the buffer continuously, so a leak in any release path accumulates into a
		// permanently inflated memory report; the live-version drop is included in the sequence.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		for v in 1..=4u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}
		storage.seal_for_flush(EntryKind::Multi, CommitVersion(u64::MAX));
		assert!(storage.current_resident_bytes().as_bytes() > 0);
		assert!(storage.historical_resident_bytes().as_bytes() > 0);

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(4))])])).unwrap();
		storage.compact(HashMap::from([(
			EntryKind::Multi,
			vec![
				(key.clone(), CommitVersion(1)),
				(key.clone(), CommitVersion(2)),
				(key.clone(), CommitVersion(3)),
			],
		)]))
		.unwrap();

		assert_eq!(
			storage.current_resident_bytes(),
			ByteSize::ZERO,
			"draining every entry must return the current tally to zero"
		);
		assert_eq!(
			storage.historical_resident_bytes(),
			ByteSize::ZERO,
			"draining every entry must return the historical tally to zero"
		);
	}

	#[test]
	fn clear_table_resets_the_byte_tally() {
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"w".to_vec())))])]),
		)
		.unwrap();
		assert!(storage.current_resident_bytes().as_bytes() > 0);
		assert!(storage.historical_resident_bytes().as_bytes() > 0);

		storage.clear_table(EntryKind::Multi).unwrap();
		assert_eq!(
			storage.current_resident_bytes(),
			ByteSize::ZERO,
			"clearing a table must zero its byte tally, not leak it"
		);
		assert_eq!(storage.historical_resident_bytes(), ByteSize::ZERO);
	}

	#[test]
	fn an_empty_buffer_has_no_oldest_pending_version() {
		// None means "nothing is waiting to be flushed", which is what lets a retention floor sit at
		// the permitted watermark. Reporting a version here would peg the floor to a write that
		// does not exist.
		let storage = CommitStore::new();

		assert_eq!(storage.oldest_pending_version(), None);
	}

	#[test]
	fn oldest_pending_version_is_the_minimum_across_every_entry_kind() {
		// The retention floor is global, so a single lagging keyspace has to hold it down. Taking a
		// per-kind minimum, or the newest version instead of the oldest, is what lets the tombstone
		// reaper delete rows an un-flushed write is about to rewrite.
		let storage = CommitStore::new();
		let key = EncodedKey::new(b"k");

		storage.set(
			CommitVersion(40),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"late".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(7),
			HashMap::from([(
				EntryKind::Source(StorageId::Table(TableId(1))),
				vec![(key.clone(), Some(CowVec::new(b"early".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			CommitVersion(19),
			HashMap::from([(
				EntryKind::Source(StorageId::Table(TableId(2))),
				vec![(key.clone(), Some(CowVec::new(b"mid".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(
			storage.oldest_pending_version(),
			Some(CommitVersion(7)),
			"the oldest un-flushed write in any keyspace is what bounds the durable frontier"
		);
	}

	#[test]
	fn the_oldest_bucket_in_a_keyspace_wins_over_its_newer_ones() {
		// One keyspace holds many keys, each indexed under its own oldest resident version. Reading
		// the newest bucket instead of the oldest reports a frontier above writes that are still
		// buffered - and the tombstone reaper deletes under exactly that frontier.
		let storage = CommitStore::new();

		storage.set(
			CommitVersion(11),
			HashMap::from([(
				EntryKind::Multi,
				vec![(EncodedKey::new(b"late"), Some(CowVec::new(b"v".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			CommitVersion(4),
			HashMap::from([(
				EntryKind::Multi,
				vec![(EncodedKey::new(b"early"), Some(CowVec::new(b"v".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(storage.oldest_pending_version(), Some(CommitVersion(4)));
	}

	#[test]
	fn a_superseded_version_still_counts_as_pending_until_it_is_compacted_away() {
		// Overwriting a key moves the old version into the historical map; it is still resident and
		// still un-flushed. If the index followed the current version instead, the frontier would
		// jump past a version the flusher has not written yet.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		let key = EncodedKey::new(b"k");

		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(9),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v9".to_vec())))])]),
		)
		.unwrap();

		assert_eq!(storage.oldest_pending_version(), Some(CommitVersion(3)));

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(3))])])).unwrap();

		assert_eq!(
			storage.oldest_pending_version(),
			Some(CommitVersion(9)),
			"once the sweep drains v3 the frontier may advance to the next un-flushed write"
		);
	}

	#[test]
	fn draining_the_buffer_clears_the_oldest_pending_version() {
		// A sweep that empties a keyspace has to retire its bucket from the index. A stale bucket
		// pins the retention floor at a version that is already durable, and reclamation stalls
		// forever with no failing symptom.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		let key = EncodedKey::new(b"k");

		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(5))])])).unwrap();

		assert_eq!(storage.oldest_pending_version(), None);
	}

	#[test]
	fn a_default_buffer_round_trips_a_write() {
		let storage = CommitStore::new();

		let key = EncodedKey::new(b"key");
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, version).unwrap().value().as_deref(),
			Some(b"value".as_slice())
		);
	}

	#[test]
	fn a_default_buffer_scans_every_key_at_the_written_version() {
		let storage = CommitStore::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.is_exhausted());
	}

	#[test]
	fn a_point_read_finds_the_newest_version_at_or_below_the_asked_one_across_closed_maps() {
		// Point reads skip maps whose version range cannot beat the best hit; a skip that fires one
		// version too early hands back an older row, and one that fires too late is only slow.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		let k = EncodedKey::new(b"k");
		for (version, value) in [(5u64, b"v5".as_slice()), (1, b"v1"), (9, b"v9")] {
			storage.set(
				CommitVersion(version),
				HashMap::from([(
					EntryKind::Multi,
					vec![(k.clone(), Some(CowVec::new(value.to_vec())))],
				)]),
			)
			.unwrap();
		}
		let entry = storage.inner.entries.data.get(&EntryKind::Multi).unwrap();
		assert_eq!(entry.closed.read().len(), 3, "precondition: one closed map per write");

		let read = |version: u64| match storage.get(EntryKind::Multi, b"k", CommitVersion(version)).unwrap() {
			VersionedGetResult::Value {
				value,
				version,
			} => Some((version.0, value.to_vec())),
			VersionedGetResult::Tombstone => panic!("no tombstone was written"),
			VersionedGetResult::NotFound => None,
		};
		assert_eq!(read(7), Some((5, b"v5".to_vec())));
		assert_eq!(
			read(5),
			Some((5, b"v5".to_vec())),
			"a read at exactly a map's oldest version keeps that map"
		);
		assert_eq!(read(3), Some((1, b"v1".to_vec())));
		assert_eq!(read(100), Some((9, b"v9".to_vec())));
		assert_eq!(read(0), None);
		assert!(storage.contains(EntryKind::Multi, b"k", CommitVersion(7)).unwrap());
		assert!(!storage.contains(EntryKind::Multi, b"k", CommitVersion(0)).unwrap());
	}

	#[test]
	fn the_key_count_follows_writes_closes_compaction_and_clearing() {
		// The flush sizes its backlog from this count without walking the maps; a count that drifts from
		// the stored keys makes the flush report work that is not there or hide work that is.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		let walked = |storage: &CommitStore| -> u64 {
			let entry = storage.inner.entries.data.get(&EntryKind::Multi).unwrap();
			let active = entry.active.read().rows().key_count();
			let closed: usize = entry.closed.read().iter().map(|map| map.rows().key_count()).sum();
			(active + closed) as u64
		};
		seed(&storage, 1, &[keyed("a", b"a1"), keyed("b", b"b1")]);
		assert_eq!(storage.estimated_current_count(EntryKind::Multi).unwrap(), 2);
		seed(&storage, 2, &[keyed("a", b"a2")]);
		assert_eq!(
			storage.estimated_current_count(EntryKind::Multi).unwrap(),
			3,
			"a key rewritten into a new map counts once per map"
		);
		assert_eq!(storage.estimated_current_count(EntryKind::Multi).unwrap(), walked(&storage));

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(EncodedKey::new(b"a"), CommitVersion(1))])]))
			.unwrap();
		assert_eq!(storage.estimated_current_count(EntryKind::Multi).unwrap(), 2);
		assert_eq!(storage.estimated_current_count(EntryKind::Multi).unwrap(), walked(&storage));

		storage.clear_table(EntryKind::Multi).unwrap();
		assert_eq!(storage.estimated_current_count(EntryKind::Multi).unwrap(), 0);

		let same_map = CommitStore::new();
		seed(&same_map, 1, &[keyed("a", b"a1")]);
		seed(&same_map, 2, &[keyed("a", b"a2")]);
		assert_eq!(
			same_map.estimated_current_count(EntryKind::Multi).unwrap(),
			1,
			"a rewrite inside the active map adds no key"
		);
	}

	#[test]
	fn compaction_never_alters_a_map_a_reader_still_holds() {
		// The flush walks a snapshot of the closed maps while the store keeps compacting; a drop that
		// mutated the shared map underneath would let the walk skip or double-read rows.
		let storage = CommitStore::with_close_threshold(ByteSize::from_bytes(1));
		seed(&storage, 1, &[keyed("a", b"a1")]);
		seed(&storage, 2, &[keyed("a", b"a2")]);
		let entry = storage.inner.entries.data.get(&EntryKind::Multi).unwrap();
		let held = entry.closed_snapshot();

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(EncodedKey::new(b"a"), CommitVersion(1))])]))
			.unwrap();

		let held_versions: usize = held
			.iter()
			.map(|map| map.rows().iter().map(|(_, versions)| versions.len()).sum::<usize>())
			.sum();
		assert_eq!(held_versions, 2, "the held snapshot still sees v1");
		assert_eq!(storage.get_all_versions(EntryKind::Multi, b"a").unwrap().len(), 1, "the store dropped v1");
	}

	#[test]
	fn the_oldest_pending_version_rises_when_the_oldest_row_leaves_a_map_that_keeps_others() {
		// Dropping a map's oldest row in place must move the map's oldest version to the next one it
		// still holds; a version range that stays put freezes the retention floor on a version nothing
		// holds any more.
		let storage = CommitStore::new();
		seed(&storage, 3, &[keyed("a", b"a3")]);
		seed(&storage, 5, &[keyed("b", b"b5")]);
		assert_eq!(storage.oldest_pending_version(), Some(CommitVersion(3)));

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(EncodedKey::new(b"a"), CommitVersion(3))])]))
			.unwrap();

		assert_eq!(storage.oldest_pending_version(), Some(CommitVersion(5)));
	}
}
